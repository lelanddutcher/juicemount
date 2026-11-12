/*
 * JuiceMount: pinned blocks (patch 0025).
 *
 * The app pins files for offline use ("pack for the trip"). Upstream JuiceFS
 * has one LRU-like disk cache and no notion of a pinned block, so:
 *   - a pin made by reading the file through FUSE took the ranged read path
 *     (patches 0018/0021) and depended on a background whole-block fetch whose
 *     queue drops requests when full: blocks went missing and the pin failed;
 *   - ordinary reads of other files evicted pinned blocks.
 *
 * This file adds:
 *   - jmPinSet: the cache keys pinned by each owner (an inode, chosen by the
 *     vfs layer). Disk-cache eviction (cleanupFull, cleanupExpire) skips them.
 *     Pinned bytes are capped at JMPinShare of the cache capacity, so
 *     unpinned reads always keep room; a fill past the cap fails with
 *     ErrJMPinFull and pins nothing new.
 *   - JMPinFill: downloads every missing block of the given slices whole (one
 *     GET per block, also on encrypted volumes), writes it to the disk cache
 *     synchronously and pins it. When it returns nil, every block is a file
 *     in the cache directory.
 *   - JMPinCheck: counts an owner's pinned bytes missing from disk, without
 *     network access.
 *
 * The pin set lives in memory and ends with the process; the app pins again
 * after a restart (JMPinFill of a present block costs one stat).
 */

package chunk

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// JMPinShare is the largest fraction of the block cache that pinned blocks
// may hold. The rest stays for ordinary reads.
const JMPinShare = 0.9

// jmPinBlockWorkers bounds the whole-block downloads of one fill, and
// jmPinDownloads all fills together.
const jmPinBlockWorkers = 4

var jmPinDownloads = make(chan struct{}, 8)

// jmPinRetries is how many times a block GET is attempted before a fill fails.
var jmPinRetries = 3
var jmPinRetryWait = time.Second

var (
	ErrJMPinFull        = errors.New("pinned blocks would exceed the pin share of the block cache")
	ErrJMPinUnsupported = errors.New("the block cache cannot pin (no disk cache)")
)

// JMPinSlice names one slice of a file: its id and its full length.
type JMPinSlice struct {
	ID   uint64
	Size uint32
}

// JMPinProgress is updated while a fill runs. Total and Done count the bytes
// of the owner's blocks; Fetched counts the bytes downloaded.
type JMPinProgress struct {
	Total, Done, Fetched atomic.Int64
}

type jmPinSet struct {
	mu     sync.Mutex
	limit  func() int64
	refs   map[cacheKey]int32
	owners map[uint64][]cacheKey
	bytes  int64
}

func newJMPinSet(limit func() int64) *jmPinSet {
	return &jmPinSet{limit: limit, refs: make(map[cacheKey]int32), owners: make(map[uint64][]cacheKey)}
}

func jmPinCost(k cacheKey) int64 { return int64(k.size) + 4096 }

// has reports whether k is pinned. Called by eviction under the store lock;
// the pin set never takes a store lock.
func (s *jmPinSet) has(k cacheKey) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refs[k] > 0
}

// replace makes keys the owner's pinned set. It fails with ErrJMPinFull, and
// changes nothing, when the new total would pass the cap.
func (s *jmPinSet) replace(owner uint64, keys []cacheKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	uniq := make(map[cacheKey]struct{}, len(keys))
	for _, k := range keys {
		uniq[k] = struct{}{}
	}
	old := make(map[cacheKey]struct{}, len(s.owners[owner]))
	for _, k := range s.owners[owner] {
		old[k] = struct{}{}
	}
	total := s.bytes
	for k := range old {
		if _, keep := uniq[k]; !keep && s.refs[k] == 1 {
			total -= jmPinCost(k)
		}
	}
	for k := range uniq {
		if _, had := old[k]; !had && s.refs[k] == 0 {
			total += jmPinCost(k)
		}
	}
	if total > s.bytes && total > s.limit() {
		return ErrJMPinFull
	}
	for k := range old {
		if _, keep := uniq[k]; !keep {
			s.unref(k)
		}
	}
	list := make([]cacheKey, 0, len(uniq))
	for k := range uniq {
		if _, had := old[k]; !had {
			if s.refs[k] == 0 {
				s.bytes += jmPinCost(k)
			}
			s.refs[k]++
		}
		list = append(list, k)
	}
	if len(list) == 0 {
		delete(s.owners, owner)
	} else {
		s.owners[owner] = list
	}
	return nil
}

func (s *jmPinSet) unref(k cacheKey) {
	if s.refs[k]--; s.refs[k] <= 0 {
		delete(s.refs, k)
		s.bytes -= jmPinCost(k)
	}
}

func (s *jmPinSet) drop(owner uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.owners[owner] {
		s.unref(k)
	}
	delete(s.owners, owner)
}

func (s *jmPinSet) keysOf(owner uint64) ([]cacheKey, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys, ok := s.owners[owner]
	return append([]cacheKey(nil), keys...), ok
}

func (s *jmPinSet) ownerList() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint64, 0, len(s.owners))
	for o := range s.owners {
		out = append(out, o)
	}
	return out
}

func (s *jmPinSet) usage() (int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes, s.limit()
}

// jmPins returns the manager's pin set, or nil for a memory cache.
func (m *cacheManager) jmPins() *jmPinSet { return m.pins }

func (m *cacheManager) jmPinLimit() int64 {
	m.Lock()
	defer m.Unlock()
	var total int64
	for _, s := range m.stores {
		if s.available() {
			total += s.capacity
		}
	}
	return int64(float64(total) * JMPinShare)
}

// jmOnDisk reports whether key is a complete file in the cache directory.
// A block that is only a page waiting for the background flush is not.
func (m *cacheManager) jmOnDisk(key string) bool {
	s := m.getStore(key)
	if s == nil {
		return false
	}
	var err error
	_ = s.checkErr(func() error {
		_, err = os.Stat(s.cachePath(key))
		return err
	})
	return err == nil
}

// jmCacheSync writes a whole block to the disk cache before it returns.
func (m *cacheManager) jmCacheSync(key string, p *Page) error {
	s := m.getStore(key)
	if s == nil || !s.enabled() {
		return errCacheDown
	}
	if err := s.flushPage(s.cachePath(key), p.Data, false); err != nil {
		return err
	}
	s.add(key, int32(len(p.Data)), uint32(time.Now().Unix()))
	return nil
}

func (store *cachedStore) jmPinManager() (*cacheManager, error) {
	m, ok := store.bcache.(*cacheManager)
	if !ok || m.pins == nil {
		return nil, ErrJMPinUnsupported
	}
	return m, nil
}

// JMPinFill pins every block of slices for owner (replacing what the owner
// pinned before) and downloads, whole, each block that is not on disk.
func (store *cachedStore) JMPinFill(ctx context.Context, owner uint64, slices []JMPinSlice, prog *JMPinProgress) error {
	m, err := store.jmPinManager()
	if err != nil {
		return err
	}
	if prog == nil {
		prog = &JMPinProgress{}
	}
	var keys []string
	var cks []cacheKey
	seen := make(map[uint64]struct{}, len(slices))
	for _, sl := range slices {
		if sl.ID == 0 || sl.Size == 0 {
			continue
		}
		if _, dup := seen[sl.ID]; dup {
			continue
		}
		seen[sl.ID] = struct{}{}
		for _, k := range sliceForRead(sl.ID, int(sl.Size), store).keys() {
			keys = append(keys, k)
			ck := m.stores[0].getCacheKey(k)
			cks = append(cks, ck)
			prog.Total.Add(int64(ck.size))
		}
	}
	if err := m.pins.replace(owner, cks); err != nil {
		return err
	}
	todo := make(chan int)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	for w := 0; w < jmPinBlockWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range todo {
				if e := store.jmPinBlock(ctx, m, keys[i], prog); e != nil {
					errOnce.Do(func() { firstErr = e })
				}
			}
		}()
	}
	for i := range keys {
		if ctx.Err() != nil {
			break
		}
		todo <- i
	}
	close(todo)
	wg.Wait()
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	return firstErr
}

func (store *cachedStore) jmPinBlock(ctx context.Context, m *cacheManager, key string, prog *JMPinProgress) error {
	size := parseObjOrigSize(key)
	if m.jmOnDisk(key) {
		prog.Done.Add(int64(size))
		return nil
	}
	if size <= 0 || size > store.conf.BlockSize {
		return errors.New("invalid block size in key " + key)
	}
	select {
	case jmPinDownloads <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-jmPinDownloads }()
	var err error
	for attempt := 0; attempt < jmPinRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(jmPinRetryWait * time.Duration(attempt)):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if m.jmOnDisk(key) { // a read cached it meanwhile
			prog.Done.Add(int64(size))
			return nil
		}
		p := NewOffPage(size)
		err = store.load(key, p, false, false)
		if err == nil {
			err = m.jmCacheSync(key, p)
		}
		p.Release()
		if err == nil {
			prog.Fetched.Add(int64(size))
			prog.Done.Add(int64(size))
			return nil
		}
		logger.Warnf("pin block %s (attempt %d): %s", key, attempt+1, err)
	}
	return err
}

// JMPinCheck reports the owner's pinned bytes and how many of them are not on
// disk. known is false when the owner has no pins in this process.
func (store *cachedStore) JMPinCheck(owner uint64) (known bool, total, missing int64, err error) {
	m, err := store.jmPinManager()
	if err != nil {
		return false, 0, 0, err
	}
	keys, known := m.pins.keysOf(owner)
	for _, k := range keys {
		total += int64(k.size)
		if !m.jmOnDisk(m.stores[0].getPathFromKey(k)) {
			missing += int64(k.size)
		}
	}
	return known, total, missing, nil
}

// JMPinDrop unpins every block of owner. Unpinned blocks stay cached and
// become ordinary eviction candidates.
func (store *cachedStore) JMPinDrop(owner uint64) {
	if m, err := store.jmPinManager(); err == nil {
		m.pins.drop(owner)
	}
}

// JMPinOwners lists the owners with pins.
func (store *cachedStore) JMPinOwners() []uint64 {
	if m, err := store.jmPinManager(); err == nil {
		return m.pins.ownerList()
	}
	return nil
}

// JMPinUsage returns the pinned bytes and the cap.
func (store *cachedStore) JMPinUsage() (pinned, limit int64) {
	if m, err := store.jmPinManager(); err == nil {
		return m.pins.usage()
	}
	return 0, 0
}
