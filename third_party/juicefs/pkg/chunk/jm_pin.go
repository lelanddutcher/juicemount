/*
 * JuiceMount: pinned blocks (patch 0026).
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
 *     vfs layer), with the file length and mtime the fill saw. Disk-cache
 *     eviction (cleanupFull, cleanupExpire) skips pinned keys. The set is
 *     persisted, one file per owner, in <first cache dir>/jmpins, and loaded
 *     before any cache store scans or evicts, so pins survive a restart with
 *     no metadata access.
 *   - A cap per cache store: pinned bytes may not pass JMPinShare of the
 *     store's capacity, nor what the disk can hold (used + free - the
 *     free-space floor). A fill past it fails (ErrJMPinFull, ErrJMPinNoSpace)
 *     and pins nothing new; a block is never written below the floor.
 *   - JMPinFill: downloads every missing block that holds visible file data,
 *     whole (one GET per block, also on encrypted volumes), through the same
 *     single flight as reads, after demand reads of other blocks (patch 0020),
 *     writes it to the disk cache before it returns, and pins it.
 *   - JMPinCheck / JMPinVerify: presence and recorded attributes, local only.
 *   - JMPinSetCapacity: grows the cache capacity at run time, so a pinned set
 *     larger than the mount's --cache-size can still fit.
 */

package chunk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// JMPinShare is the largest fraction of a cache store's capacity that pinned
// blocks may hold. The rest stays for ordinary reads.
const JMPinShare = 0.9

// jmPinMaxWorkers bounds the whole-block downloads of one fill, and
// jmPinDownloads all fills together: jmPinMaxDownloads, or 1 while the
// latest fill asked for one worker (the app's slow or metered link: one pin
// download at a time in the whole process, re-pins included).
const jmPinMaxWorkers = 4
const jmPinMaxDownloads = 8

var jmPinDownloads = newJMLimiter(jmPinMaxDownloads)

// jmLimiter is a counting semaphore whose limit can change.
type jmLimiter struct {
	mu    sync.Mutex
	n     int
	limit int
	wake  chan struct{}
}

func newJMLimiter(limit int) *jmLimiter {
	return &jmLimiter{limit: limit, wake: make(chan struct{})}
}

func (l *jmLimiter) acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		if l.n < l.limit {
			l.n++
			l.mu.Unlock()
			return nil
		}
		wake := l.wake
		l.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (l *jmLimiter) signal() { // with mu held
	close(l.wake)
	l.wake = make(chan struct{})
}

func (l *jmLimiter) release() {
	l.mu.Lock()
	l.n--
	l.signal()
	l.mu.Unlock()
}

func (l *jmLimiter) setLimit(limit int) {
	l.mu.Lock()
	if l.limit != limit {
		l.limit = limit
		l.signal()
	}
	l.mu.Unlock()
}

// jmPinRetries is how many times a block GET is attempted before a fill fails.
var jmPinRetries = 3
var jmPinRetryWait = time.Second

var (
	ErrJMPinFull        = errors.New("pinned blocks would exceed the pin share of the block cache")
	ErrJMPinNoSpace     = errors.New("not enough free disk space above the cache free-space floor for pinned blocks")
	ErrJMPinUnsupported = errors.New("the block cache cannot pin (no disk cache)")
)

// JMPinSlice is one slice of a file: its id, its full length, and the part of
// it the file shows (Off, Len, in slice bytes).
type JMPinSlice struct {
	ID       uint64
	Size     uint32
	Off, Len uint32
}

// JMPinAttr is the file length and mtime a fill saw.
type JMPinAttr struct {
	Length  uint64
	MtimeNS int64
}

// JMPinProgress is updated while a fill runs. Total and Done count the bytes
// of the owner's blocks; Fetched counts the bytes this fill downloaded.
type JMPinProgress struct {
	Total, Done, Fetched atomic.Int64
}

// JMPinVerifyItem asks whether an owner is pinned, complete, and was filled
// from a file with this length and mtime. When Slices is set (read from
// metadata now), the slices the fill saw must also be the same: compaction
// replaces a file's slices without changing its length or mtime.
type JMPinVerifyItem struct {
	Owner  uint64
	Attr   JMPinAttr
	Slices []JMPinSlice
}

// Reasons in JMPinVerify results.
const (
	JMPinUnknown    = 1 // no pins for the owner
	JMPinChanged    = 2 // pinned from a file with another length, mtime or slices
	JMPinIncomplete = 3 // pinned blocks missing from disk
)

type JMPinVerifyResult struct {
	Owner   uint64
	Reason  uint8
	Missing int64
}

type jmPinRecord struct {
	Keys    []string `json:"keys"`
	Length  uint64   `json:"length"`
	MtimeNS int64    `json:"mtime_ns"`
	// Slices is JMPinSliceHash of the slices the fill saw ("" in records
	// written before it existed: not compared).
	Slices string `json:"slices,omitempty"`
}

// JMPinSliceHash identifies a file's visible slices: their ids, sizes and
// the parts the file shows, in order.
func JMPinSliceHash(slices []JMPinSlice) string {
	h := sha256.New()
	var b [20]byte
	for _, sl := range slices {
		binary.BigEndian.PutUint64(b[0:], sl.ID)
		binary.BigEndian.PutUint32(b[8:], sl.Size)
		binary.BigEndian.PutUint32(b[12:], sl.Off)
		binary.BigEndian.PutUint32(b[16:], sl.Len)
		h.Write(b[:])
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

type jmRef struct {
	n     int32
	store string // cache store id; "" until bind
	cost  int64
}

type jmPinSet struct {
	pmu      sync.Mutex // serializes persistence; taken before mu
	mu       sync.Mutex
	dir      string
	refs     map[cacheKey]*jmRef
	owners   map[uint64]*jmPinRecord
	perStore map[string]int64
	bytes    int64
	m        *cacheManager

	// roomMu makes a block's free-space check and its reservation one
	// step; reserved counts blocks admitted but not yet on disk, per store.
	roomMu   sync.Mutex
	reserved map[string]int64
}

// jmPinsByDir hands the manager's pin set to newCacheStore, so a store skips
// pinned keys from its first scan.
var jmPinsByDir sync.Map

func jmPinsForDir(dir string) *jmPinSet {
	if v, ok := jmPinsByDir.Load(dir); ok {
		return v.(*jmPinSet)
	}
	return nil
}

// jmCacheKey parses an object key; getCacheKey uses no store state.
func jmCacheKey(key string) cacheKey { return (*cacheStore)(nil).getCacheKey(key) }

func jmPinCost(k cacheKey) int64 { return int64(k.size) + 4096 }

func newJMPinSet(dir string) *jmPinSet {
	s := &jmPinSet{dir: dir, refs: make(map[cacheKey]*jmRef), owners: make(map[uint64]*jmPinRecord), perStore: make(map[string]int64), reserved: make(map[string]int64)}
	s.load()
	return s
}

func (s *jmPinSet) ownerPath(owner uint64) string {
	return filepath.Join(s.dir, strconv.FormatUint(owner, 10)+".json")
}

// load reads the persisted owners. Stores are not bound yet: only has() is
// used until bind.
func (s *jmPinSet) load() {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		owner, err := strconv.ParseUint(strings.TrimSuffix(name, ".json"), 10, 64)
		if err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, name))
		var rec jmPinRecord
		if err != nil || json.Unmarshal(data, &rec) != nil {
			logger.Warnf("pinned blocks: unreadable %s: %v", name, err)
			continue
		}
		// Each key once: a duplicate would count its block twice and
		// keep it pinned after the owner drops it.
		valid := rec.Keys[:0]
		seen := make(map[string]struct{}, len(rec.Keys))
		for _, k := range rec.Keys {
			if _, dup := seen[k]; dup {
				continue
			}
			if strings.HasPrefix(k, "chunks/") && parseObjOrigSize(k) > 0 {
				seen[k] = struct{}{}
				valid = append(valid, k)
			}
		}
		rec.Keys = valid
		s.owners[owner] = &rec
		for _, k := range rec.Keys {
			ck := jmCacheKey(k)
			if r := s.refs[ck]; r != nil {
				r.n++
			} else {
				s.refs[ck] = &jmRef{n: 1, cost: jmPinCost(ck)}
				s.bytes += jmPinCost(ck)
			}
		}
	}
	if len(s.owners) > 0 {
		logger.Infof("pinned blocks: loaded %d files, %d bytes", len(s.owners), s.bytes)
	}
}

// bind assigns loaded keys to their cache store.
func (s *jmPinSet) bind(m *cacheManager) {
	s.mu.Lock()
	s.m = m
	var keys []string
	for _, rec := range s.owners {
		keys = append(keys, rec.Keys...)
	}
	s.mu.Unlock()
	ids := make([]string, len(keys))
	for i, k := range keys {
		ids[i] = m.jmStoreID(k)
	}
	s.mu.Lock()
	for i, k := range keys {
		if r := s.refs[jmCacheKey(k)]; r != nil && r.store == "" && ids[i] != "" {
			r.store = ids[i]
			s.perStore[r.store] += r.cost
		}
	}
	s.mu.Unlock()
}

// writeOwner persists one owner atomically; nil removes it.
func (s *jmPinSet) writeOwner(owner uint64, rec *jmPinRecord) {
	path := s.ownerPath(owner)
	if rec == nil {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logger.Warnf("pinned blocks: remove %s: %s", path, err)
		} else if err == nil {
			s.syncDir()
		}
		return
	}
	data, err := json.Marshal(rec)
	if err == nil {
		err = os.MkdirAll(s.dir, 0700)
	}
	if err == nil {
		tmp := path + ".tmp"
		var f *os.File
		if f, err = os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600); err == nil {
			if _, err = f.Write(data); err == nil {
				err = f.Sync()
			}
			if cerr := f.Close(); err == nil {
				err = cerr
			}
			if err == nil {
				if err = os.Rename(tmp, path); err == nil {
					s.syncDir() // the rename itself survives a power loss
				}
			} else {
				_ = os.Remove(tmp)
			}
		}
	}
	if err != nil {
		logger.Warnf("pinned blocks: persist %s: %s", path, err)
	}
}

// syncDir makes the folder's entries durable after a rename or remove.
func (s *jmPinSet) syncDir() {
	d, err := os.Open(s.dir)
	if err == nil {
		err = d.Sync()
		_ = d.Close()
	}
	if err != nil {
		logger.Warnf("pinned blocks: sync %s: %s", s.dir, err)
	}
}

// has reports whether k is pinned. Called by eviction under the store lock;
// the pin set never takes a store lock while it holds mu.
func (s *jmPinSet) has(k cacheKey) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refs[k] != nil
}

func (s *jmPinSet) storeBytes(id string) int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.perStore[id]
}

// presentBytes is the cost of the store's pinned keys that are in keys (the
// store's blocks on disk; the caller holds the store lock). A key that is
// pinned but was never downloaded (a failed or canceled fill) is not on disk
// and does not count: eviction must still free the rest of the cache.
func (s *jmPinSet) presentBytes(id string, keys map[cacheKey]cacheItem) int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.perStore[id] == 0 {
		return 0
	}
	var n int64
	for ck, r := range s.refs {
		if r.store != id {
			continue
		}
		if it, ok := keys[ck]; ok && it.size > 0 {
			n += r.cost
		}
	}
	return n
}

// replace makes keys the owner's pinned set. ids are the keys' store ids and
// limits the per-store caps, computed by the caller without mu. It fails, and
// changes nothing, when a store's pinned bytes would grow past its cap, or
// when ctx is done: a fill canceled for a newer one must not overwrite the
// newer fill's keys (the check is under pmu, which orders the two).
func (s *jmPinSet) replace(ctx context.Context, owner uint64, keys, ids []string, attr JMPinAttr, sliceHash string, limits map[string]jmLimit) error {
	s.pmu.Lock()
	defer s.pmu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	newIdx := make(map[cacheKey]int, len(keys))
	for i, k := range keys {
		newIdx[jmCacheKey(k)] = i
	}
	old := map[cacheKey]struct{}{}
	if rec := s.owners[owner]; rec != nil {
		for _, k := range rec.Keys {
			old[jmCacheKey(k)] = struct{}{}
		}
	}
	delta := map[string]int64{}
	for ck := range old {
		if _, keep := newIdx[ck]; !keep {
			if r := s.refs[ck]; r != nil && r.n == 1 {
				delta[r.store] -= r.cost
			}
		}
	}
	for ck, i := range newIdx {
		if _, had := old[ck]; !had && s.refs[ck] == nil {
			delta[ids[i]] += jmPinCost(ck)
		}
	}
	for id, d := range delta {
		if d <= 0 {
			continue
		}
		lim, ok := limits[id]
		if !ok || s.perStore[id]+d > lim.bytes {
			s.mu.Unlock()
			if ok && lim.disk {
				return ErrJMPinNoSpace
			}
			return ErrJMPinFull
		}
	}
	for ck := range old {
		if _, keep := newIdx[ck]; !keep {
			s.unref(ck)
		}
	}
	uniq := make([]string, 0, len(newIdx))
	for ck, i := range newIdx {
		if _, had := old[ck]; !had {
			if r := s.refs[ck]; r != nil {
				r.n++
			} else {
				s.refs[ck] = &jmRef{n: 1, store: ids[i], cost: jmPinCost(ck)}
				s.perStore[ids[i]] += jmPinCost(ck)
				s.bytes += jmPinCost(ck)
			}
		}
		uniq = append(uniq, keys[i])
	}
	// An owner with no blocks (an empty or all-hole file) stays known.
	rec := &jmPinRecord{Keys: uniq, Length: attr.Length, MtimeNS: attr.MtimeNS, Slices: sliceHash}
	s.owners[owner] = rec
	s.mu.Unlock()
	s.writeOwner(owner, rec)
	return nil
}

func (s *jmPinSet) unref(ck cacheKey) {
	r := s.refs[ck]
	if r == nil {
		return
	}
	if r.n--; r.n <= 0 {
		delete(s.refs, ck)
		s.perStore[r.store] -= r.cost
		if s.perStore[r.store] <= 0 {
			delete(s.perStore, r.store)
		}
		s.bytes -= r.cost
	}
}

func (s *jmPinSet) drop(owner uint64) bool {
	s.pmu.Lock()
	defer s.pmu.Unlock()
	s.mu.Lock()
	rec := s.owners[owner]
	if rec != nil {
		for _, k := range rec.Keys {
			s.unref(jmCacheKey(k))
		}
		delete(s.owners, owner)
	}
	s.mu.Unlock()
	if rec != nil {
		s.writeOwner(owner, nil)
	}
	return rec != nil
}

func (s *jmPinSet) record(owner uint64) (jmPinRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.owners[owner]
	if rec == nil {
		return jmPinRecord{}, false
	}
	return jmPinRecord{Keys: append([]string(nil), rec.Keys...), Length: rec.Length, MtimeNS: rec.MtimeNS, Slices: rec.Slices}, true
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

func (s *jmPinSet) total() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

// jmDiskRoom returns the free bytes of a store's disk and its free-space
// floor (both 0 when unknown). A variable for tests.
var jmDiskRoom = func(s *cacheStore) (free, floor int64) {
	usage := s.curFreeRatio()
	return int64(float64(usage.br) * float64(usage.spaceCap)), int64(float64(s.freeRatio) * float64(usage.spaceCap))
}

// jmLimit is a store's cap on pinned bytes; disk is set when free disk
// space, not the pin share, sets it.
type jmLimit struct {
	bytes int64
	disk  bool
}

func (m *cacheManager) jmStoreID(key string) string {
	if s := m.getStore(key); s != nil {
		return s.id
	}
	return ""
}

func (m *cacheManager) jmLiveStores() []*cacheStore {
	m.Lock()
	defer m.Unlock()
	out := make([]*cacheStore, 0, len(m.stores))
	for _, s := range m.stores {
		if s != nil && s.available() {
			out = append(out, s)
		}
	}
	return out
}

// jmStoreLimit: min(JMPinShare x capacity, used + free - floor).
func (m *cacheManager) jmStoreLimit(id string) jmLimit {
	m.Lock()
	s := m.storeMap[id]
	m.Unlock()
	if s == nil || !s.available() {
		return jmLimit{}
	}
	share := int64(float64(s.capacity+s.jmExtra.Load()) * JMPinShare)
	free, floor := jmDiskRoom(s)
	s.Lock()
	used := s.used
	s.Unlock()
	if free > 0 || floor > 0 {
		if disk := used + free - floor; disk < share {
			return jmLimit{bytes: max(disk, 0), disk: true}
		}
	}
	return jmLimit{bytes: share}
}

// jmRoomFor refuses a block that would take the disk below the cache's
// free-space floor: the spool and the app's databases share that disk.
// Ordinary cached blocks give way first: when the block does not fit, it
// evicts unpinned blocks for it once and checks again. On success the block
// is reserved until release is called (after it is written or failed), so
// parallel downloads cannot pass the floor together.
func (m *cacheManager) jmRoomFor(key string, size int) (release func(), err error) {
	s := m.getStore(key)
	if s == nil || !s.enabled() {
		return nil, errCacheDown
	}
	p := m.pins
	need := int64(size + 4096)
	p.roomMu.Lock()
	defer p.roomMu.Unlock()
	fits := func() bool {
		free, floor := jmDiskRoom(s)
		return (free == 0 && floor == 0) || free-p.reserved[s.id]-need >= floor
	}
	if s.rawFull.Load() || !fits() {
		if s.jmFreeUnpinned(need) == 0 || !fits() {
			return nil, ErrJMPinNoSpace
		}
		s.rawFull.Store(false) // checkFreeSpace sets it again if the disk is still short
	}
	p.reserved[s.id] += need
	var once sync.Once
	return func() {
		once.Do(func() {
			p.roomMu.Lock()
			if p.reserved[s.id] -= need; p.reserved[s.id] <= 0 {
				delete(p.reserved, s.id)
			}
			p.roomMu.Unlock()
		})
	}, nil
}

// jmFreeUnpinned evicts unpinned cached blocks, the older of each two seen
// first (as cleanupFull does), until need bytes are freed. It returns the
// bytes freed.
func (cache *cacheStore) jmFreeUnpinned(need int64) int64 {
	if !cache.available() || cache.eviction == "none" {
		return 0
	}
	var todel []cacheKey
	var freed int64
	var cnt int
	var lastK cacheKey
	var lastValue cacheItem
	cache.Lock()
	for k, value := range cache.keys {
		if value.size <= 0 || cache.jmPins.has(k) {
			continue // staging or pinned
		}
		if cnt == 0 || lastValue.atime > value.atime {
			lastK, lastValue = k, value
		}
		if cnt++; cnt > 1 {
			delete(cache.keys, lastK)
			freed += int64(lastValue.size + 4096)
			cache.used -= int64(lastValue.size + 4096)
			todel = append(todel, lastK)
			cache.m.cacheEvicts.Add(1)
			cnt = 0
			if freed >= need {
				break
			}
		}
	}
	if freed < need && cnt == 1 { // the last candidate left
		delete(cache.keys, lastK)
		freed += int64(lastValue.size + 4096)
		cache.used -= int64(lastValue.size + 4096)
		todel = append(todel, lastK)
		cache.m.cacheEvicts.Add(1)
	}
	cache.Unlock()
	for _, k := range todel {
		if !cache.available() {
			break
		}
		_ = cache.removeFile(cache.cachePath(cache.getPathFromKey(k)))
	}
	if len(todel) > 0 {
		logger.Debugf("pinned blocks: freed %d cached blocks (%d MB) in %s for a pinned block", len(todel), freed>>20, cache.dir)
	}
	return freed
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

// jmPinKeys lists the blocks that hold visible data of slices, once each.
func (store *cachedStore) jmPinKeys(slices []JMPinSlice) []string {
	bs := uint32(store.conf.BlockSize)
	seen := map[string]struct{}{}
	var keys []string
	for _, sl := range slices {
		if sl.ID == 0 || sl.Size == 0 || sl.Len == 0 || sl.Off >= sl.Size {
			continue
		}
		end := min(sl.Off+sl.Len, sl.Size)
		rs := sliceForRead(sl.ID, int(sl.Size), store)
		for i := sl.Off / bs; i <= (end-1)/bs; i++ {
			k := rs.key(int(i))
			if _, dup := seen[k]; !dup {
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
	}
	return keys
}

// JMPinFill pins the blocks of slices for owner (replacing what the owner
// pinned before) and downloads, whole, each block that is not on disk.
// workers bounds this fill's parallel downloads (1..4; 0 means 4); 1 also
// limits every fill of the process to one download at a time, until a fill
// asks for more. Only a one-worker fill waits for --demand-first's demand
// reads of other blocks: the app asks for one worker on a slow or metered
// read policy, where a pin download competes with the editor's reads. On a
// lan or fast link (four workers) demand-first is on too, and waiting there
// held each pin block up to deferMax while unpinned playback ran.
func (store *cachedStore) JMPinFill(ctx context.Context, owner uint64, slices []JMPinSlice, attr JMPinAttr, workers int, prog *JMPinProgress) error {
	m, err := store.jmPinManager()
	if err != nil {
		return err
	}
	if prog == nil {
		prog = &JMPinProgress{}
	}
	if workers <= 0 || workers > jmPinMaxWorkers {
		workers = jmPinMaxWorkers
	}
	if workers == 1 {
		jmPinDownloads.setLimit(1)
	} else {
		jmPinDownloads.setLimit(jmPinMaxDownloads)
	}
	waitDemand := workers == 1
	keys := store.jmPinKeys(slices)
	ids := make([]string, len(keys))
	limits := map[string]jmLimit{}
	for i, k := range keys {
		ids[i] = m.jmStoreID(k)
		if _, ok := limits[ids[i]]; !ok {
			limits[ids[i]] = m.jmStoreLimit(ids[i])
		}
		prog.Total.Add(int64(parseObjOrigSize(k)))
	}
	if err := m.pins.replace(ctx, owner, keys, ids, attr, JMPinSliceHash(slices), limits); err != nil {
		return err
	}
	todo := make(chan string)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range todo {
				if e := store.jmPinBlock(fctx, m, k, waitDemand, prog); e != nil {
					errOnce.Do(func() { firstErr = e; cancel() })
				}
			}
		}()
	}
	for _, k := range keys {
		if fctx.Err() != nil {
			break
		}
		todo <- k
	}
	close(todo)
	wg.Wait()
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	return firstErr
}

func (store *cachedStore) jmPinBlock(ctx context.Context, m *cacheManager, key string, waitDemand bool, prog *JMPinProgress) error {
	size := parseObjOrigSize(key)
	if m.jmOnDisk(key) {
		prog.Done.Add(int64(size))
		return nil
	}
	if size <= 0 || size > store.conf.BlockSize {
		return errors.New("invalid block size in key " + key)
	}
	if err := jmPinDownloads.acquire(ctx); err != nil {
		return err
	}
	defer jmPinDownloads.release()
	var err error
	for attempt := 0; attempt < jmPinRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(jmPinRetryWait * time.Duration(attempt)):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		done, e := store.jmPinAttempt(ctx, m, key, size, waitDemand, prog)
		if done { // success, or no room (the disk does not free itself)
			return e
		}
		err = e
		logger.Warnf("pin block %s (attempt %d): %s", key, attempt+1, err)
	}
	return err
}

// jmPinAttempt makes one attempt at a block. done is true when the attempt
// ended the block (its error, if any, is final).
func (store *cachedStore) jmPinAttempt(ctx context.Context, m *cacheManager, key string, size int, waitDemand bool, prog *JMPinProgress) (done bool, err error) {
	release, err := m.jmRoomFor(key, size)
	if err != nil {
		return true, err
	}
	defer release()
	// --demand-first: reads an editor waits for go first (patch 0020), on a
	// slow or metered link only (waitDemand; see JMPinFill).
	if waitDemand {
		store.demand.waitIdleCtx(ctx, key)
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if m.jmOnDisk(key) { // a read cached it meanwhile
		prog.Done.Add(int64(size))
		return true, nil
	}
	var fetched bool
	p := NewOffPage(size)
	defer p.Release()
	// The same single flight as reads: a demand read of this block
	// piggybacks on this download, and this fill on a read's.
	block, err := store.group.Execute(key, func() (*Page, error) {
		p.Acquire()
		endFull := store.demand.beginFull(key)
		err := store.load(key, p, false, false)
		endFull(err)
		fetched = true
		return p, err
	})
	if err == nil {
		err = m.jmCacheSync(key, block)
	}
	block.Release()
	if err != nil {
		return false, err
	}
	if fetched {
		prog.Fetched.Add(int64(size))
	}
	prog.Done.Add(int64(size))
	return true, nil
}

// JMPinCheck reports the owner's pinned bytes, how many of them are not on
// disk, and the attributes the fill saw. Local only. known is false when the
// owner has no pins.
func (store *cachedStore) JMPinCheck(owner uint64) (known bool, total, missing int64, attr JMPinAttr, err error) {
	m, err := store.jmPinManager()
	if err != nil {
		return false, 0, 0, attr, err
	}
	rec, known := m.pins.record(owner)
	for _, k := range rec.Keys {
		size := int64(parseObjOrigSize(k))
		total += size
		if !m.jmOnDisk(k) {
			missing += size
		}
	}
	return known, total, missing, JMPinAttr{Length: rec.Length, MtimeNS: rec.MtimeNS}, nil
}

// JMPinVerify returns the items that are not pinned, were pinned from another
// version of the file, or miss blocks. Local only.
func (store *cachedStore) JMPinVerify(items []JMPinVerifyItem) ([]JMPinVerifyResult, error) {
	m, err := store.jmPinManager()
	if err != nil {
		return nil, err
	}
	var out []JMPinVerifyResult
	for _, it := range items {
		known, _, missing, attr, err := store.JMPinCheck(it.Owner)
		if err != nil {
			return nil, err
		}
		sliceChanged := false
		if it.Slices != nil {
			if rec, ok := m.pins.record(it.Owner); ok && rec.Slices != "" {
				sliceChanged = rec.Slices != JMPinSliceHash(it.Slices)
			}
		}
		switch {
		case !known:
			out = append(out, JMPinVerifyResult{Owner: it.Owner, Reason: JMPinUnknown})
		case attr != it.Attr || sliceChanged:
			out = append(out, JMPinVerifyResult{Owner: it.Owner, Reason: JMPinChanged, Missing: missing})
		case missing > 0:
			out = append(out, JMPinVerifyResult{Owner: it.Owner, Reason: JMPinIncomplete, Missing: missing})
		}
	}
	return out, nil
}

// JMPinDrop unpins every block of owner. Unpinned blocks stay cached and
// become ordinary eviction candidates.
func (store *cachedStore) JMPinDrop(owner uint64) bool {
	if m, err := store.jmPinManager(); err == nil {
		return m.pins.drop(owner)
	}
	return false
}

// JMPinOwners lists the owners with pins.
func (store *cachedStore) JMPinOwners() []uint64 {
	if m, err := store.jmPinManager(); err == nil {
		return m.pins.ownerList()
	}
	return nil
}

// JMPinUsage returns the pinned bytes on disk (what eviction cannot free;
// pinned blocks a fill never got are not counted) and the sum of the stores'
// caps.
func (store *cachedStore) JMPinUsage() (pinned, limit int64) {
	m, err := store.jmPinManager()
	if err != nil {
		return 0, 0
	}
	for _, s := range m.jmLiveStores() {
		limit += m.jmStoreLimit(s.id).bytes
		s.Lock()
		pinned += m.pins.presentBytes(s.id, s.keys)
		s.Unlock()
	}
	return pinned, limit
}

// JMPinSetCapacity sets the total cache capacity to at least bytes (never
// below the mount's --cache-size) and returns the new total.
func (store *cachedStore) JMPinSetCapacity(bytes int64) int64 {
	m, err := store.jmPinManager()
	if err != nil {
		return 0
	}
	stores := m.jmLiveStores()
	if len(stores) == 0 {
		return 0
	}
	per := bytes / int64(len(stores))
	var total int64
	for _, s := range stores {
		s.jmExtra.Store(max(per-s.capacity, 0))
		total += s.capacity + s.jmExtra.Load()
	}
	return total
}
