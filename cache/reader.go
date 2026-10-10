package cache

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/slicegraph"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/juicefsbin"
)

const (
	DefaultBlockSize = 4 << 20  // 4MB
	ChunkSize        = 64 << 20 // 64MB (JuiceFS default)
	checksumBlock    = 32 << 10 // JuiceFS CsExtend checksum granularity
	mappingVersion   = 2
	mappingMaxBytes  = 64 << 20
)

var ErrCacheMiss = errors.New("cache miss")

var ErrSliceMapping = errors.New("invalid cache slice mapping")

var ErrMappingIdentity = errors.New("cache mapping snapshot identity mismatch")

var crc32c = crc32.MakeTable(crc32.Castagnoli)

// SliceInfo describes a JuiceFS slice within a chunk. The shared type keeps
// local observations and the final metadata comparison on one resolver.
type SliceInfo = slicegraph.Extent

// Reader reads JuiceFS cache blocks directly from the SSD,
// bypassing the JuiceFS FUSE mount for cached data.
type Reader struct {
	cacheDir  string // e.g. ~/.juicefs/cache/{uuid}/raw/chunks
	blockSize int64
	binding   VolumeBinding // immutable after construction
	rdb       atomic.Pointer[redis.Client]
	stopped   atomic.Bool
	stopOnce  sync.Once

	// Cached chunk→slice mappings: key = "inode_chunkIndex"
	sliceMu    sync.RWMutex
	sliceCache map[string][]SliceInfo
	// Each witness was observed in the same Redis EVAL as its chunk mapping.
	// Legacy snapshots have no witness and can serve bytes, never terminal EOF.
	attrWitness     map[string]ResidentAttr
	inodeAttr       map[uint64]ResidentAttr // O(1) cross-chunk coherence guard
	resetEpoch      uint64
	remoteSuspended bool
	remoteEpoch     uint64
	inodeEpoch      map[uint64]uint64

	// Only one Redis fetch per chunk may be in flight. Without this, the first
	// concurrent NFS reads after an invalidation stampede the remote metadata
	// link with identical LRANGE calls.
	flightMu sync.Mutex
	flights  map[string]*sliceFlight

	// NFS reads are normally 128 KiB–1 MiB. Reuse private all-or-nothing
	// buffers so CRC safety does not turn a 10GbE cached stream into equivalent
	// GC bandwidth. Buffers above 8 MiB are intentionally not retained.
	bufferPool sync.Pool

	// The JuiceFS chunk bytes survive an app restart, but the inode→slice map
	// needed to address them used to live only in RAM. Persist the verified
	// working set locally so a fresh offline launch can read already-cached
	// bytes without first asking Redis over the unavailable link.
	mappingPath string
	// Serialize every snapshot writer before it captures RAM state. A delayed
	// debounce write must never rename an older capture over a completed flush.
	mappingWriteMu sync.Mutex
	mappingPersist chan struct{}
	mappingStop    chan struct{}
	mappingWG      sync.WaitGroup
	// Test-only rendezvous after atomic remote observations, before publication.
	testBeforeRevalidatePublish func()
	// Test-only rendezvous when a foreground lookup joins a preparation flight.
	testJoinPreparationFlight func()
	// Test-only I/O rendezvous and directory sync failure injection.
	testAfterMappingSnapshotCapture func()
	testSyncMappingDirectory        func(*os.File) error
	testSyncResident                func(*os.File, bool) error
	testMappingSnapshotMaxBytes     int64
}

type sliceFlight struct {
	done        chan struct{}
	prepare     bool
	generation  cacheGeneration
	remoteEpoch uint64
	slices      []SliceInfo
	attr        ResidentAttr
	err         error
}

type cacheGeneration struct {
	reset uint64
	inode uint64
}

type mappingSnapshot struct {
	Version  uint32
	Binding  VolumeBinding
	CacheDir string
	Entries  map[string][]SliceInfo
	Attrs    map[string]ResidentAttr
}

// ResidentAttr is a same-observation JuiceFS inode attribute, not a claim
// that the backend has not changed since the last Redis observation.
type ResidentAttr struct {
	Size    int64
	MtimeNS int64
	Valid   bool
}

func decodeResidentAttr(raw string) ResidentAttr {
	b := []byte(raw)
	if len(b) != 72 && len(b) != 80 || uint8(binary.BigEndian.Uint16(b[1:3])>>12) != 1 {
		return ResidentAttr{}
	}
	sec := binary.BigEndian.Uint64(b[23:31])
	ns := binary.BigEndian.Uint32(b[31:35])
	size := binary.BigEndian.Uint64(b[51:59])
	const billion uint64 = 1_000_000_000
	if sec > uint64(math.MaxInt64)/billion || uint64(ns) >= billion || size > math.MaxInt64 ||
		sec == uint64(math.MaxInt64)/billion && uint64(ns) > uint64(math.MaxInt64)%billion {
		return ResidentAttr{}
	}
	return ResidentAttr{Size: int64(size), MtimeNS: int64(sec)*1e9 + int64(ns), Valid: true}
}

// RevalidateMappings compares every locally-persisted working-set mapping with
// Redis without reading any media bytes. Commands are pipelined in bounded
// groups, so a high-RTT link pays one round trip per group instead of one round
// trip per inode/chunk. Unchanged mappings remain hot; changed or removed
// mappings are discarded and will be fetched normally on their next read.
//
// The caller must keep remote fallthrough fail-closed until this returns. On a
// transport or decode error no local mapping is changed, allowing the caller to
// choose the conservative fallback (InvalidateAll) without leaving a partially
// validated set behind.
func (r *Reader) RevalidateMappings(ctx context.Context) (kept, discarded int, err error) {
	return r.revalidateMappingsWithVolumeCheck(ctx, r.ValidateVolume)
}

// Session uses its single finite qualification deadline for both this
// identity check and the subsequent mapping batches. The public method keeps
// its independent two-second initial volume check.
func (r *Reader) revalidateMappingsForSession(ctx context.Context) (kept, discarded int, err error) {
	return r.revalidateMappingsWithVolumeCheck(ctx, r.validateVolumeWithDeadline)
}

func (r *Reader) revalidateMappingsWithVolumeCheck(ctx context.Context, validate func(context.Context) error) (kept, discarded int, err error) {
	if r == nil {
		return 0, 0, nil
	}
	if err := validate(ctx); err != nil {
		return 0, 0, err
	}
	r.sliceMu.RLock()
	entries := make(map[string][]SliceInfo, len(r.sliceCache))
	attrs := make(map[string]ResidentAttr, len(r.attrWitness))
	starts := make(map[uint64]cacheGeneration)
	for key, slices := range r.sliceCache {
		entries[key] = append([]SliceInfo(nil), slices...)
		attrs[key] = r.attrWitness[key]
		if separator := strings.IndexByte(key, '_'); separator > 0 {
			if inode, parseErr := strconv.ParseUint(key[:separator], 10, 64); parseErr == nil {
				starts[inode] = r.generationLocked(inode)
			}
		}
	}
	r.sliceMu.RUnlock()
	if len(entries) == 0 {
		return 0, 0, nil
	}
	rdb := r.rdb.Load()
	if rdb == nil {
		return 0, 0, errors.New("no Redis client")
	}

	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Each JuiceFS slice record is only 24 bytes, but a heavily-overwritten
	// chunk can have many records. Keep both command count and response memory
	// bounded while collapsing the dominant WAN cost (RTT) by up to 256x.
	const batchSize = 256
	changed := make(map[string]struct{})
	observedAttrs := make(map[string]ResidentAttr, len(entries))
	for start := 0; start < len(keys); start += batchSize {
		end := min(start+batchSize, len(keys))
		pipe := rdb.Pipeline()
		cmds := make([]func() ([]string, error), 0, end-start)
		for _, key := range keys[start:end] {
			if r.binding == (VolumeBinding{}) {
				cmds = append(cmds, pipe.LRange(ctx, "c"+key, 0, -1).Result)
			} else {
				inodeText, _, _ := strings.Cut(key, "_")
				cmds = append(cmds, pipe.Eval(ctx, boundMappingAttrLua, []string{"setting", "c" + key, "i" + inodeText}, r.binding.UUID, r.binding.BlockSize/1024).StringSlice)
			}
		}
		if _, execErr := pipe.Exec(ctx); execErr != nil {
			return 0, 0, fmt.Errorf("revalidate slice mappings: %w", classifyMappingError(execErr))
		}
		for i, cmd := range cmds {
			items, cmdErr := cmd()
			if cmdErr != nil {
				return 0, 0, fmt.Errorf("revalidate slice mapping %q: %w", keys[start+i], classifyMappingError(cmdErr))
			}
			key := keys[start+i]
			if r.binding != (VolumeBinding{}) {
				if len(items) == 0 {
					return 0, 0, ErrSliceMapping
				}
				observedAttrs[key] = decodeResidentAttr(items[0])
				items = items[1:]
			}
			remote, decodeErr := decodeSliceRecords(items)
			if decodeErr != nil {
				return 0, 0, fmt.Errorf("%w: revalidate slice mapping %q: %v", ErrSliceMapping, keys[start+i], decodeErr)
			}
			if !sliceMappingsEqual(entries[key], remote) ||
				(attrs[key].Valid && attrs[key] != observedAttrs[key]) {
				changed[key] = struct{}{}
			}
		}
	}

	if r.testBeforeRevalidatePublish != nil {
		r.testBeforeRevalidatePublish()
	}
	r.sliceMu.Lock()
	changedInodes := make(map[uint64]struct{})
	conflictInodes := make(map[uint64]struct{})
	observedByInode := make(map[uint64]ResidentAttr)
	for _, key := range keys {
		inodeText, _, _ := strings.Cut(key, "_")
		inode, _ := strconv.ParseUint(inodeText, 10, 64)
		current, ok := r.sliceCache[key]
		// A mutation or a fresh fetch raced validation. Never delete a mapping
		// that is no longer the exact snapshot we compared.
		if !ok || r.generationLocked(inode) != starts[inode] ||
			!sliceMappingsEqual(current, entries[key]) || r.attrWitness[key] != attrs[key] {
			continue
		}
		if r.binding != (VolumeBinding{}) {
			observed := observedAttrs[key]
			if prior, seen := observedByInode[inode]; seen && prior != observed {
				conflictInodes[inode] = struct{}{}
			} else {
				observedByInode[inode] = observed
			}
			if attrs[key].Valid && attrs[key] != observed ||
				r.inodeAttr[inode].Valid && r.inodeAttr[inode] != observed {
				conflictInodes[inode] = struct{}{}
			}
		}
		if _, differs := changed[key]; differs {
			delete(r.sliceCache, key)
			delete(r.attrWitness, key)
			// A prepared resident range may already have copied bytes from
			// this mapping. Revoke its token when revalidation learns that
			// the remote slice changed, even if the mirror row is unchanged.
			changedInodes[inode] = struct{}{}
			discarded++
		} else {
			if r.attrWitness[key] != observedAttrs[key] {
				r.attrWitness[key] = observedAttrs[key]
				// An old snapshot gains an associated witness only after a
				// fresh atomic observation. Revoke any concurrent token.
				changedInodes[inode] = struct{}{}
			}
			kept++
		}
	}
	for inode := range changedInodes {
		r.inodeEpoch[inode]++
	}
	for inode := range conflictInodes {
		r.clearInodeWitnessLocked(inode)
		delete(r.inodeAttr, inode)
		if _, changed := changedInodes[inode]; !changed {
			r.inodeEpoch[inode]++
		}
	}
	for inode, attr := range observedByInode {
		if _, conflict := conflictInodes[inode]; !conflict && attr.Valid {
			r.inodeAttr[inode] = attr
		}
	}
	r.sliceMu.Unlock()
	if discarded > 0 || len(observedAttrs) > 0 {
		r.scheduleMappingPersist()
	}
	return kept, discarded, nil
}

func decodeSliceRecords(items []string) ([]SliceInfo, error) {
	raw := make([]SliceInfo, 0, len(items))
	for _, item := range items {
		data := []byte(item)
		if len(data) != 24 {
			return nil, fmt.Errorf("corrupt slice record: length %d", len(data))
		}
		raw = append(raw, SliceInfo{
			Pos:     binary.BigEndian.Uint32(data[0:4]),
			SliceID: binary.BigEndian.Uint64(data[4:12]),
			Size:    binary.BigEndian.Uint32(data[12:16]),
			Off:     binary.BigEndian.Uint32(data[16:20]),
			Len:     binary.BigEndian.Uint32(data[20:24]),
		})
	}
	return resolveSlices(raw)
}

func sliceMappingsEqual(a, b []SliceInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// NewReader creates a cache reader. cacheDir is the path to the JuiceFS
// cache chunks directory (e.g. ~/.juicefs/cache/{uuid}/raw/chunks).
// rdb is used to fetch chunk→slice mappings on demand.
func NewReader(cacheDir string, blockSize int64, rdb *redis.Client) *Reader {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}
	r := &Reader{
		cacheDir:    cacheDir,
		blockSize:   blockSize,
		sliceCache:  make(map[string][]SliceInfo),
		attrWitness: make(map[string]ResidentAttr),
		inodeAttr:   make(map[uint64]ResidentAttr),
		inodeEpoch:  make(map[uint64]uint64),
		flights:     make(map[string]*sliceFlight),
	}
	r.rdb.Store(rdb)
	return r
}

// SetMappingSnapshotPath enables durable local working-set metadata. Loading
// is best-effort but fail-closed: a corrupt, oversized, wrong-version, or
// different-cache-directory snapshot is ignored. The slice bytes themselves
// remain checksum-verified on every read, so this file can never turn a corrupt
// cache block into accepted content.
func (r *Reader) SetMappingSnapshotPath(snapshotPath string) error {
	if r == nil || strings.TrimSpace(snapshotPath) == "" {
		return nil
	}
	if r.stopped.Load() {
		return errors.New("cache reader is stopped")
	}
	if r.mappingPersist != nil {
		return errors.New("mapping snapshot path is already configured")
	}
	r.mappingPath = snapshotPath
	loaded, err := r.loadMappingSnapshot()
	if errors.Is(err, ErrMappingIdentity) {
		// A different library's valid snapshot, or an unqualified legacy map,
		// is not ours to replace on Stop. The caller can choose a new path.
		r.mappingPath = ""
		return err
	}
	if err != nil {
		// A bad snapshot is disposable cache metadata. Remove it so every later
		// launch does not repeat the same decode failure; live Redis/FUSE remains
		// the authoritative fallback.
		_ = os.Remove(snapshotPath)
	}
	r.mappingPersist = make(chan struct{}, 1)
	r.mappingStop = make(chan struct{})
	r.mappingWG.Add(1)
	go r.mappingPersistLoop()
	if loaded > 0 {
		log.Printf("cache: restored %d durable inode/chunk mappings from %s", loaded, snapshotPath)
	}
	return err
}

func (r *Reader) loadMappingSnapshot() (int, error) {
	f, err := os.Open(r.mappingPath)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() > mappingMaxBytes {
		return 0, fmt.Errorf("mapping snapshot is too large: %d bytes", info.Size())
	}
	var snapshot mappingSnapshot
	if err := gob.NewDecoder(bufio.NewReader(io.LimitReader(f, mappingMaxBytes+1))).Decode(&snapshot); err != nil {
		return 0, fmt.Errorf("decode mapping snapshot: %w", err)
	}
	if snapshot.Version != mappingVersion {
		return 0, fmt.Errorf("%w: snapshot version %d, want %d", ErrMappingIdentity, snapshot.Version, mappingVersion)
	}
	if snapshot.Binding != r.binding || filepath.Clean(snapshot.CacheDir) != filepath.Clean(r.cacheDir) {
		return 0, ErrMappingIdentity
	}
	validated := make(map[string][]SliceInfo, len(snapshot.Entries))
	for key, slices := range snapshot.Entries {
		if !validMappingKey(key) {
			return 0, fmt.Errorf("invalid mapping key %q", key)
		}
		resolved, err := resolveSlices(slices)
		if err != nil {
			return 0, fmt.Errorf("invalid mapping %q: %w", key, err)
		}
		validated[key] = resolved
	}
	r.sliceMu.Lock()
	// Loading onto an already-used Reader is allowed. Revoke all outstanding
	// resident tokens and replace every old pair, including witness-less keys.
	r.resetEpoch++
	clear(r.sliceCache)
	clear(r.attrWitness)
	clear(r.inodeAttr)
	conflicting := make(map[uint64]struct{})
	for key, slices := range validated {
		r.sliceCache[key] = slices
		if attr := snapshot.Attrs[key]; attr.Valid && attr.Size >= 0 && attr.MtimeNS >= 0 {
			inodeText, _, _ := strings.Cut(key, "_")
			inode, _ := strconv.ParseUint(inodeText, 10, 64)
			if prior, found := r.inodeAttr[inode]; found && prior != attr {
				conflicting[inode] = struct{}{}
			} else {
				r.inodeAttr[inode] = attr
				r.attrWitness[key] = attr
			}
		}
	}
	for inode := range conflicting {
		r.clearInodeWitnessLocked(inode)
		delete(r.inodeAttr, inode)
	}
	r.sliceMu.Unlock()
	return len(validated), nil
}

func validMappingKey(key string) bool {
	inodeText, chunkText, ok := strings.Cut(key, "_")
	if !ok || inodeText == "" || chunkText == "" {
		return false
	}
	inode, inodeErr := strconv.ParseUint(inodeText, 10, 64)
	chunk, chunkErr := strconv.ParseInt(chunkText, 10, 64)
	return inodeErr == nil && chunkErr == nil && inode > 0 && chunk >= 0
}

func (r *Reader) scheduleMappingPersist() {
	if r == nil || r.mappingPersist == nil || r.stopped.Load() {
		return
	}
	select {
	case r.mappingPersist <- struct{}{}:
	default:
	}
}

func (r *Reader) mappingPersistLoop() {
	defer r.mappingWG.Done()
	var timer *time.Timer
	var timerC <-chan time.Time
	for {
		select {
		case <-r.mappingPersist:
			if timer == nil {
				timer = time.NewTimer(time.Second)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(time.Second)
			}
			timerC = timer.C
		case <-timerC:
			if err := r.persistMappingSnapshot(); err != nil {
				log.Printf("cache: persist mapping snapshot: %v", err)
			}
			timerC = nil
		case <-r.mappingStop:
			if timer != nil {
				timer.Stop()
			}
			if err := r.persistMappingSnapshot(); err != nil {
				log.Printf("cache: final mapping snapshot persist: %v", err)
			}
			return
		}
	}
}

func (r *Reader) persistMappingSnapshot() error {
	r.mappingWriteMu.Lock()
	defer r.mappingWriteMu.Unlock()
	return r.writeMappingSnapshot(false)
}

// FlushMappingSnapshot durably publishes the current mapping and paired
// attribute witnesses. The caller owns this synchronous I/O through its
// return; no success is reported without an installed snapshot path. Its
// parent directory must already exist, so syncing that directory covers the
// snapshot's rename without an unflushed newly-created ancestor.
func (r *Reader) FlushMappingSnapshot() error {
	if r == nil {
		return errors.New("cache mapping snapshot path is not configured")
	}
	r.mappingWriteMu.Lock()
	defer r.mappingWriteMu.Unlock()
	if r.mappingPath == "" {
		return errors.New("cache mapping snapshot path is not configured")
	}
	if r.stopped.Load() {
		return errors.New("cache reader is stopped")
	}
	return r.writeMappingSnapshot(true)
}

func (r *Reader) writeMappingSnapshot(requireExistingParent bool) error {
	if r.mappingPath == "" {
		return nil
	}
	r.sliceMu.RLock()
	entries := make(map[string][]SliceInfo, len(r.sliceCache))
	attrs := make(map[string]ResidentAttr, len(r.attrWitness))
	for key, slices := range r.sliceCache {
		entries[key] = append([]SliceInfo(nil), slices...)
		if attr := r.attrWitness[key]; attr.Valid {
			attrs[key] = attr
		}
	}
	r.sliceMu.RUnlock()
	if r.testAfterMappingSnapshotCapture != nil {
		r.testAfterMappingSnapshotCapture()
	}

	dir := filepath.Dir(r.mappingPath)
	if requireExistingParent {
		info, err := os.Stat(dir)
		if err != nil {
			return fmt.Errorf("mapping snapshot parent: %w", err)
		}
		if !info.IsDir() {
			return fmt.Errorf("mapping snapshot parent is not a directory: %s", dir)
		}
	} else {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	parentFD, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	directory := os.NewFile(uintptr(parentFD), dir)
	defer directory.Close()
	parentInfo, err := directory.Stat()
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".cache-mappings-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if err := tmp.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	snapshot := mappingSnapshot{Version: mappingVersion, Binding: r.binding, CacheDir: r.cacheDir, Entries: entries, Attrs: attrs}
	writer := bufio.NewWriter(tmp)
	if err := gob.NewEncoder(writer).Encode(snapshot); err != nil {
		cleanup()
		return err
	}
	if err := writer.Flush(); err != nil {
		cleanup()
		return err
	}
	info, err := tmp.Stat()
	if err != nil {
		cleanup()
		return err
	}
	limit := int64(mappingMaxBytes)
	if r.testMappingSnapshotMaxBytes > 0 {
		limit = r.testMappingSnapshotMaxBytes
	}
	if info.Size() > limit {
		cleanup()
		return fmt.Errorf("mapping snapshot is too large: %d bytes", info.Size())
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	// Retain the exact file and parent through rename and directory sync.
	// A substituted mapping path cannot authorize a durable image handoff.
	defer tmp.Close()
	parentCurrent := func() bool {
		named, err := os.Lstat(dir)
		return err == nil && named.IsDir() && os.SameFile(named, parentInfo)
	}
	if !parentCurrent() {
		cleanup()
		return ErrMappingIdentity
	}
	if err := os.Rename(tmpName, r.mappingPath); err != nil {
		cleanup()
		return err
	}
	installed, err := residentDiskID(tmp, false)
	if err != nil {
		return err
	}
	installedCurrent := func() bool {
		if !parentCurrent() {
			return false
		}
		fd, err := unix.Openat(int(directory.Fd()), filepath.Base(r.mappingPath), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			return false
		}
		f := os.NewFile(uintptr(fd), r.mappingPath)
		id, statErr := residentDiskID(f, false)
		closeErr := f.Close()
		return statErr == nil && closeErr == nil && id == installed
	}
	if !installedCurrent() {
		return ErrMappingIdentity
	}
	if r.testSyncMappingDirectory != nil {
		err = r.testSyncMappingDirectory(directory)
	} else {
		err = directory.Sync()
	}
	if err != nil {
		return fmt.Errorf("sync mapping snapshot directory: %w", err)
	}
	if !installedCurrent() {
		return ErrMappingIdentity
	}
	return tmp.Close()
}

// Verify checks that the bundled JuiceFS version is compatible and the cache
// directory exists with the expected structure.
func (r *Reader) Verify() error {
	// Only the bundled juicefs beside this executable; never PATH or Homebrew.
	binary := juicefsbin.Resolve()
	if binary == "" {
		return errors.New("juicefs executable: no bundled juicefs beside this executable")
	}
	return r.VerifyWithBinary(context.Background(), binary)
}

// VerifyWithBinary checks the executable selected for the actual FUSE mount.
// A bounded owned version probe cannot hold startup indefinitely, and a GUI
// bundle need not have Homebrew installed to use its supported cache layout.
func (r *Reader) VerifyWithBinary(ctx context.Context, binary string) error {
	if r.cacheDir == "" {
		return fmt.Errorf("no cache directory configured")
	}
	if info, err := os.Stat(r.cacheDir); err != nil {
		// A qualified volume has a deterministic path before its first cache
		// fill. Keep the reader attached so subsequent blocks become usable.
		if !errors.Is(err, os.ErrNotExist) || !r.binding.valid() {
			return fmt.Errorf("cache dir not accessible: %w", err)
		}
	} else if !info.IsDir() {
		return errors.New("cache path is not a directory")
	}

	// Check JuiceFS version
	if binary == "" {
		return errors.New("no JuiceFS executable configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "--version")
	cmd.WaitDelay = 250 * time.Millisecond
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("juicefs version check: %w", err)
	}
	version := strings.TrimSpace(string(out))
	if !strings.Contains(version, "1.3.") {
		return fmt.Errorf("unsupported JuiceFS version: %s (expected 1.3.x)", version)
	}

	return nil
}

// ReadBlock reads data for the given inode at the specified file offset.
// Returns the data read and whether it came from cache.
// Returns ErrCacheMiss if the block is not in the SSD cache.
func (r *Reader) ReadBlock(ctx context.Context, inode uint64, fileOffset int64, buf []byte) (int, error) {
	return r.readBlock(ctx, inode, fileOffset, buf, 0, false, nil, nil, nil)
}

// ReadBlockWithMappingTimeout is ReadBlock with a bound applied only when a
// chunk→slice mapping is absent from RAM. The hot cached path creates no timer;
// this keeps 10GbE reads cheap while preventing a first remote LRANGE from
// parking an NFS connection indefinitely.
func (r *Reader) ReadBlockWithMappingTimeout(ctx context.Context, inode uint64, fileOffset int64, buf []byte, timeout time.Duration) (int, error) {
	return r.ReadBlockWithMappingTimeoutTrace(ctx, inode, fileOffset, buf, timeout, nil)
}

// ReadBlockWithMappingTimeoutTrace is the same read with optional, caller-owned
// synchronous attribution. The trace never escapes into a mapping flight.
func (r *Reader) ReadBlockWithMappingTimeoutTrace(ctx context.Context, inode uint64, fileOffset int64, buf []byte, timeout time.Duration, trace *ReadTrace) (int, error) {
	if trace != nil {
		*trace = ReadTrace{Outcome: "not_attempted"}
	}
	return r.readBlock(ctx, inode, fileOffset, buf, timeout, false, nil, nil, trace)
}

// ResidentToken pins the in-process mapping generation used to verify a range.
// A caller must check it again immediately before publishing the range.
type ResidentToken struct {
	reader     *Reader
	inode      uint64
	generation cacheGeneration
}

func (t ResidentToken) Valid() bool {
	if t.reader == nil || t.reader.stopped.Load() {
		return false
	}
	t.reader.sliceMu.RLock()
	valid := t.reader.generationLocked(t.inode) == t.generation
	t.reader.sliceMu.RUnlock()
	return valid
}

// ReadResidentRange reads an all-or-nothing CRC-verified range using only
// mappings already in RAM (including restored local mappings) and SSD blocks.
// It never asks Redis for a missing mapping. The returned token expires on any
// relevant local invalidation; an invalid token cannot authorize an NFS reply.
func (r *Reader) ReadResidentRange(inode uint64, fileOffset int64, buf []byte, expected ResidentAttr) (ResidentToken, error) {
	var token ResidentToken
	if r == nil || r.stopped.Load() || r.binding == (VolumeBinding{}) || !expected.Valid ||
		fileOffset < 0 || len(buf) == 0 || int64(len(buf)) > math.MaxInt64-fileOffset ||
		fileOffset > expected.Size || int64(len(buf)) > expected.Size-fileOffset {
		return token, ErrCacheMiss
	}
	_, err := r.readBlock(context.Background(), inode, fileOffset, buf, 0, true, &token, &expected, nil)
	return token, err
}

// ResidentEOF qualifies a zero-byte at/past-EOF reply from the terminal
// chunk's joint attr/mapping observation. It never manufactures a witness by
// iterating zero chunks and never scans unrelated library mappings.
func (r *Reader) ResidentEOF(inode uint64, expected ResidentAttr) (ResidentToken, error) {
	if r == nil || r.stopped.Load() || r.binding == (VolumeBinding{}) || !expected.Valid || expected.Size < 0 {
		return ResidentToken{}, ErrCacheMiss
	}
	chunk := int64(0)
	if expected.Size > 0 {
		chunk = (expected.Size - 1) / ChunkSize
	}
	key := fmt.Sprintf("%d_%d", inode, chunk)
	r.sliceMu.RLock()
	_, mapped := r.sliceCache[key]
	witness := r.attrWitness[key]
	gen := r.generationLocked(inode)
	r.sliceMu.RUnlock()
	if !mapped || witness != expected {
		return ResidentToken{}, ErrCacheMiss
	}
	return ResidentToken{reader: r, inode: inode, generation: gen}, nil
}

func (r *Reader) readBlock(ctx context.Context, inode uint64, fileOffset int64, buf []byte, mappingTimeout time.Duration, localOnly bool, token *ResidentToken, expected *ResidentAttr, trace *ReadTrace) (int, error) {
	if r.cacheDir == "" || fileOffset < 0 || len(buf) == 0 {
		if trace != nil {
			trace.Outcome = "not_attempted"
		}
		return 0, ErrCacheMiss
	}

	r.sliceMu.RLock()
	startGeneration := r.generationLocked(inode)
	r.sliceMu.RUnlock()

	// A direct-cache read is all-or-nothing. Reading into a private buffer means
	// a missing/corrupt tail can never leak a verified head into the caller and
	// be mistaken for EOF by NFS. The coherent FUSE path retries the whole read.
	verified := r.getBuffer(len(buf))
	defer r.putBuffer(verified)
	read := 0
	for read < len(verified) {
		absolute := fileOffset + int64(read)
		chunkIndex := absolute / ChunkSize
		offsetInChunk := absolute % ChunkSize

		var slices []SliceInfo
		var err error
		if localOnly {
			r.sliceMu.RLock()
			slices, _ = r.sliceCache[fmt.Sprintf("%d_%d", inode, chunkIndex)]
			r.sliceMu.RUnlock()
			if slices == nil {
				if trace != nil {
					trace.Outcome = "mapping_absent_or_hole"
				}
				return 0, ErrCacheMiss
			}
		} else {
			slices, err = r.getSlicesBoundedTrace(ctx, inode, chunkIndex, mappingTimeout, trace)
		}
		if err != nil {
			if trace != nil {
				switch {
				case errors.Is(err, context.DeadlineExceeded):
					trace.Outcome = "mapping_deadline"
				case errors.Is(err, context.Canceled):
					trace.Outcome = "mapping_canceled"
				case errors.Is(err, ErrCacheMiss):
					trace.Outcome = "mapping_absent_or_hole"
				default:
					trace.Outcome = "mapping_error"
				}
			}
			if errors.Is(err, ErrVolumeBinding) {
				return 0, err
			}
			return 0, ErrCacheMiss
		}
		s, ok := sliceAt(slices, offsetInChunk)
		if !ok || s.SliceID == 0 {
			if trace != nil {
				trace.Outcome = "mapping_absent_or_hole"
			}
			return 0, ErrCacheMiss
		}

		sliceStart := int64(s.Pos)
		sliceEnd := sliceStart + int64(s.Len)
		offsetInSlice := int64(s.Off) + offsetInChunk - sliceStart
		blockIndex := offsetInSlice / r.blockSize
		offsetInBlock := offsetInSlice % r.blockSize
		blockLen, ok := r.blockLength(s, blockIndex)
		if !ok || offsetInBlock >= blockLen {
			if trace != nil {
				trace.Outcome = "mapping_absent_or_hole"
			}
			return 0, ErrCacheMiss
		}

		want := int64(len(verified) - read)
		want = minInt64(want, sliceEnd-offsetInChunk)
		want = minInt64(want, blockLen-offsetInBlock)
		want = minInt64(want, ChunkSize-offsetInChunk)
		if want <= 0 {
			if trace != nil {
				trace.Outcome = "mapping_absent_or_hole"
			}
			return 0, ErrCacheMiss
		}

		blockStart := time.Time{}
		if trace != nil {
			blockStart = time.Now()
		}
		if err := r.readVerifiedBlock(s.SliceID, blockIndex, blockLen,
			offsetInBlock, verified[read:read+int(want)], trace); err != nil {
			if trace != nil {
				trace.VerifiedBlockNS += time.Since(blockStart).Nanoseconds()
				if trace.Outcome != "block_unavailable" {
					trace.Outcome = "block_unverified_or_read_error"
				}
			}
			return 0, ErrCacheMiss
		}
		if trace != nil {
			trace.VerifiedBlockNS += time.Since(blockStart).Nanoseconds()
		}
		read += int(want)
	}

	r.sliceMu.RLock()
	unchanged := r.generationLocked(inode) == startGeneration
	if unchanged && expected != nil {
		last := (fileOffset + int64(len(buf)) - 1) / ChunkSize
		for chunk := fileOffset / ChunkSize; chunk <= last; chunk++ {
			if r.attrWitness[fmt.Sprintf("%d_%d", inode, chunk)] != *expected {
				unchanged = false
				break
			}
		}
	}
	r.sliceMu.RUnlock()
	if !unchanged {
		if trace != nil {
			trace.Outcome = "generation_or_witness_invalidated"
		}
		return 0, ErrCacheMiss
	}
	copy(buf, verified)
	if trace != nil {
		trace.Outcome = "verified_hit"
	}
	if token != nil {
		*token = ResidentToken{reader: r, inode: inode, generation: startGeneration}
	}
	return len(buf), nil
}

// getSlices fetches the slice mapping for a chunk, caching it locally.
func (r *Reader) getSlices(ctx context.Context, inode uint64, chunkIndex int64) ([]SliceInfo, error) {
	return r.getSlicesBounded(ctx, inode, chunkIndex, 0)
}

func (r *Reader) getSlicesBounded(ctx context.Context, inode uint64, chunkIndex int64, timeout time.Duration) ([]SliceInfo, error) {
	return r.getSlicesBoundedTrace(ctx, inode, chunkIndex, timeout, nil)
}

// PrepareChunkMapping warms one bound chunk's mapping and same-observation
// inode attribute without opening media or transferring an unbounded list.
// A preparation never joins an existing foreground flight. Its own finite
// flight may be joined by foreground readers, which retain their own deadline.
func (r *Reader) PrepareChunkMapping(ctx context.Context, inode uint64, chunkIndex int64) error {
	if r == nil || r.stopped.Load() || r.binding == (VolumeBinding{}) || chunkIndex < 0 {
		return ErrCacheMiss
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrCacheMiss
	}
	_, err := r.lookupSlices(ctx, inode, chunkIndex, 0, nil, true)
	return err
}

func (r *Reader) getSlicesBoundedTrace(ctx context.Context, inode uint64, chunkIndex int64, timeout time.Duration, trace *ReadTrace) ([]SliceInfo, error) {
	return r.lookupSlices(ctx, inode, chunkIndex, timeout, trace, false)
}

func (r *Reader) lookupSlices(ctx context.Context, inode uint64, chunkIndex int64, timeout time.Duration, trace *ReadTrace, prepare bool) ([]SliceInfo, error) {
	if trace != nil {
		start := time.Now()
		trace.MappingLookupCount++
		defer trace.finishMappingLookup(start)
	}
	key := fmt.Sprintf("%d_%d", inode, chunkIndex)
	r.sliceMu.RLock()
	if slices, ok := r.sliceCache[key]; ok {
		r.sliceMu.RUnlock()
		return slices, nil
	}
	r.sliceMu.RUnlock()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	retriedPreparation := false
	for {
		r.sliceMu.RLock()
		if slices, ok := r.sliceCache[key]; ok {
			r.sliceMu.RUnlock()
			return slices, nil
		}
		if r.remoteSuspended || r.stopped.Load() {
			r.sliceMu.RUnlock()
			return nil, ErrCacheMiss
		}
		r.sliceMu.RUnlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if trace != nil {
			trace.MappingRemoteSharedCount++
		}
		r.flightMu.Lock()
		if flight := r.flights[key]; flight != nil {
			r.flightMu.Unlock()
			if prepare {
				return nil, ErrCacheMiss // leave the existing owner alone
			}
			if flight.prepare && r.testJoinPreparationFlight != nil {
				r.testJoinPreparationFlight()
			}
			select {
			case <-flight.done:
				r.sliceMu.RLock()
				current := r.generationLocked(inode)
				currentRemoteEpoch := r.remoteEpoch
				r.sliceMu.RUnlock()
				if current != flight.generation || currentRemoteEpoch != flight.remoteEpoch {
					return nil, errors.New("slice mapping invalidated during shared fetch")
				}
				if flight.prepare && flight.err != nil && !errors.Is(flight.err, ErrVolumeBinding) && !retriedPreparation {
					retriedPreparation = true
					continue // own normal fetch under the original absolute deadline
				}
				return flight.slices, flight.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		// Close the gap between the first RAM miss and flight reservation.
		// Lock order is flightMu then sliceMu; publication releases sliceMu
		// before its deferred flight completion takes flightMu.
		r.sliceMu.RLock()
		if slices, ok := r.sliceCache[key]; ok {
			r.sliceMu.RUnlock()
			r.flightMu.Unlock()
			return slices, nil
		}
		if r.remoteSuspended || r.stopped.Load() {
			r.sliceMu.RUnlock()
			r.flightMu.Unlock()
			return nil, ErrCacheMiss
		}
		fetchGeneration := r.generationLocked(inode)
		fetchRemoteEpoch := r.remoteEpoch
		r.sliceMu.RUnlock()
		flight := &sliceFlight{done: make(chan struct{}), generation: fetchGeneration, remoteEpoch: fetchRemoteEpoch, prepare: prepare}
		r.flights[key] = flight
		r.flightMu.Unlock()
		defer func() {
			r.flightMu.Lock()
			delete(r.flights, key)
			close(flight.done)
			r.flightMu.Unlock()
		}()
		rdb := r.rdb.Load()
		if rdb == nil {
			flight.err = errors.New("no Redis client")
			return nil, flight.err
		}
		redisKey := "c" + key
		var items []string
		var attr ResidentAttr
		var err error
		if prepare {
			items, attr, err = r.fetchMappingWithAttrBounded(ctx, rdb, redisKey, inode)
		} else {
			items, attr, err = r.fetchMappingWithAttr(ctx, rdb, redisKey, inode)
		}
		// A transport swap may retire the old client. Retry the same mode,
		// never substitute an unbounded foreground fetch for preparation.
		if err != nil && !errors.Is(err, ErrVolumeBinding) {
			if replacement := r.rdb.Load(); replacement != nil && replacement != rdb {
				if prepare {
					items, attr, err = r.fetchMappingWithAttrBounded(ctx, replacement, redisKey, inode)
				} else {
					items, attr, err = r.fetchMappingWithAttr(ctx, replacement, redisKey, inode)
				}
			}
		}
		if err != nil {
			flight.err = err
			return nil, err
		}
		slices, err := decodeSliceRecords(items)
		if err != nil {
			flight.err = err
			return nil, err
		}
		r.sliceMu.Lock()
		if r.generationLocked(inode) != fetchGeneration || r.remoteEpoch != fetchRemoteEpoch || r.stopped.Load() {
			r.sliceMu.Unlock()
			flight.err = errors.New("slice mapping invalidated during fetch")
			return nil, flight.err
		}
		// Both modes publish through the same map+attr authority transaction.
		if prior, known := r.inodeAttr[inode]; known && prior != attr {
			r.clearInodeWitnessLocked(inode)
			r.inodeEpoch[inode]++
		}
		r.sliceCache[key] = slices
		if attr.Valid {
			r.attrWitness[key] = attr
			r.inodeAttr[inode] = attr
		} else {
			delete(r.attrWitness, key)
			delete(r.inodeAttr, inode)
		}
		r.sliceMu.Unlock()
		r.scheduleMappingPersist()
		flight.slices = slices
		return slices, nil
	}
}

// resolveSlices reproduces JuiceFS meta.buildSlice's append-order overlay:
// each later Redis list item replaces the covered interval from all earlier
// slices. Returning raw LRANGE records directly is unsafe after overwrite or
// compaction because both old and new extents can cover the same file offset.
func resolveSlices(raw []SliceInfo) ([]SliceInfo, error) {
	return slicegraph.Resolve(raw)
}

func sliceAt(slices []SliceInfo, offset int64) (SliceInfo, bool) {
	i := sort.Search(len(slices), func(i int) bool {
		return int64(slices[i].Pos)+int64(slices[i].Len) > offset
	})
	if i >= len(slices) || offset < int64(slices[i].Pos) {
		return SliceInfo{}, false
	}
	return slices[i], true
}

// InvalidateSliceCache removes cached slice mappings for an inode.
// Call this when a file is modified.
func (r *Reader) InvalidateSliceCache(inode uint64) {
	r.sliceMu.Lock()
	r.inodeEpoch[inode]++
	delete(r.inodeAttr, inode)
	// Remove all chunk entries for this inode
	for key := range r.sliceCache {
		if strings.HasPrefix(key, fmt.Sprintf("%d_", inode)) {
			delete(r.sliceCache, key)
			delete(r.attrWitness, key)
		}
	}
	r.sliceMu.Unlock()
	r.scheduleMappingPersist()
}

// Caller holds sliceMu. Invalidation/fetch is rare; ordinary resident reads
// never scan the library or all of this inode's chunks.
func (r *Reader) clearInodeWitnessLocked(inode uint64) {
	prefix := strconv.FormatUint(inode, 10) + "_"
	for key := range r.attrWitness {
		if strings.HasPrefix(key, prefix) {
			delete(r.attrWitness, key)
		}
	}
}

// InvalidateAll drops every locally remembered chunk mapping. Full metadata
// reconciliation and directory-subtree replacement use this conservative path
// because they may repair changes whose pub/sub event was missed while offline.
func (r *Reader) InvalidateAll() {
	r.sliceMu.Lock()
	r.resetEpoch++
	clear(r.inodeEpoch)
	clear(r.sliceCache)
	clear(r.attrWitness)
	clear(r.inodeAttr)
	r.sliceMu.Unlock()
	r.scheduleMappingPersist()
}

// generationLocked returns the reset + inode-specific generation. A busy farm
// may publish content updates for many unrelated files while a cached read is
// in flight; only changes to this inode (or a coarse reset) should make it miss.
// Caller holds sliceMu for reading or writing.
func (r *Reader) generationLocked(inode uint64) cacheGeneration {
	return cacheGeneration{reset: r.resetEpoch, inode: r.inodeEpoch[inode]}
}

// blockPath returns the SSD cache file path for a given slice ID and block index.
// JuiceFS cache layout: chunks/{sliceID/1e6}/{sliceID/1000}/{id}_{index}_{actualSize}.
func (r *Reader) blockPath(sliceID uint64, blockIndex, blockLen int64) string {
	dir1 := sliceID / 1000 / 1000
	dir2 := sliceID / 1000
	return filepath.Join(
		r.cacheDir,
		fmt.Sprintf("%d", dir1),
		fmt.Sprintf("%d", dir2),
		fmt.Sprintf("%d_%d_%d", sliceID, blockIndex, blockLen),
	)
}

func (r *Reader) hashBlockPath(sliceID uint64, blockIndex, blockLen int64) string {
	return filepath.Join(
		r.cacheDir,
		fmt.Sprintf("%02X", sliceID%256),
		fmt.Sprintf("%d", sliceID/1000/1000),
		fmt.Sprintf("%d_%d_%d", sliceID, blockIndex, blockLen),
	)
}

func (r *Reader) blockLength(s SliceInfo, blockIndex int64) (int64, bool) {
	if blockIndex < 0 || s.Size == 0 {
		return 0, false
	}
	start := blockIndex * r.blockSize
	if start < 0 || start >= int64(s.Size) {
		return 0, false
	}
	return minInt64(r.blockSize, int64(s.Size)-start), true
}

// readVerifiedBlock opens a fresh immutable cache file, validates JuiceFS's
// CsExtend CRC32C trailer for every touched 32 KiB segment, then copies the
// requested range. Files without the trailer fail closed to coherent FUSE.
func (r *Reader) readVerifiedBlock(sliceID uint64, blockIndex, blockLen, offset int64, dst []byte, trace *ReadTrace) error {
	if offset < 0 || blockLen <= 0 || int64(len(dst)) > blockLen-offset {
		return ErrCacheMiss
	}
	paths := []string{
		r.blockPath(sliceID, blockIndex, blockLen),
		r.hashBlockPath(sliceID, blockIndex, blockLen),
	}
	var fd *os.File
	for _, p := range paths {
		if err := appisolation.ValidateCachePath(p, false); err != nil {
			continue
		}
		candidate, err := os.Open(p)
		if err == nil {
			fd = candidate
			break
		}
	}
	if fd == nil {
		if trace != nil {
			trace.Outcome = "block_unavailable"
		}
		return ErrCacheMiss
	}
	defer fd.Close()
	return r.readVerifiedBlockFD(fd, blockLen, offset, dst)
}

func (r *Reader) readVerifiedBlockFD(fd *os.File, blockLen, offset int64, dst []byte) error {
	info, err := fd.Stat()
	if err != nil {
		return err
	}
	checksumLen := ((blockLen-1)/checksumBlock + 1) * 4
	if info.Size() != blockLen+checksumLen {
		return fmt.Errorf("unverified cache file size %d, want %d", info.Size(), blockLen+checksumLen)
	}

	alignedStart := offset / checksumBlock * checksumBlock
	alignedEnd := offset + int64(len(dst))
	if alignedEnd%checksumBlock != 0 {
		alignedEnd = (alignedEnd/checksumBlock + 1) * checksumBlock
	}
	if alignedEnd > blockLen {
		alignedEnd = blockLen
	}
	data := dst
	pooled := false
	if alignedStart != offset || alignedEnd != offset+int64(len(dst)) {
		data = r.getBuffer(int(alignedEnd - alignedStart))
		pooled = true
	}
	if pooled {
		defer r.putBuffer(data)
	}
	if _, err := fd.ReadAt(data, alignedStart); err != nil {
		return err
	}
	checksumCount := (len(data)-1)/checksumBlock + 1
	checksums := make([]byte, checksumCount*4)
	checksumIndex := alignedStart / checksumBlock
	if _, err := fd.ReadAt(checksums, blockLen+checksumIndex*4); err != nil {
		return err
	}
	for start, index := 0, 0; start < len(data); start, index = start+checksumBlock, index+1 {
		end := start + checksumBlock
		if end > len(data) {
			end = len(data)
		}
		actual := crc32.Checksum(data[start:end], crc32c)
		expected := binary.BigEndian.Uint32(checksums[index*4 : index*4+4])
		if actual != expected {
			return fmt.Errorf("cache checksum mismatch: got %d want %d", actual, expected)
		}
	}
	start := offset - alignedStart
	if n := copy(dst, data[start:start+int64(len(dst))]); n != len(dst) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (r *Reader) getBuffer(size int) []byte {
	if candidate := r.bufferPool.Get(); candidate != nil {
		buf := candidate.([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
	}
	return make([]byte, size)
}

func (r *Reader) putBuffer(buf []byte) {
	if cap(buf) <= 8<<20 {
		r.bufferPool.Put(buf[:cap(buf)])
	}
}

// ResetRedisConnections swaps in a fresh go-redis pool and closes the previous
// one. Link calls this when the verified backend transport changes, aborting a
// socket whose proxy backend still points at the departed LAN instead of making
// the first cached read wait for the stalled-request watchdog.
func (r *Reader) ResetRedisConnections() bool {
	if r == nil || r.stopped.Load() {
		return false
	}
	old := r.rdb.Load()
	if old == nil {
		return false
	}
	opts := *old.Options()
	next := redis.NewClient(&opts)
	if !r.rdb.CompareAndSwap(old, next) {
		_ = next.Close()
		return false
	}
	_ = old.Close()
	return true
}

// Stop closes the active Redis client. Reads themselves use fresh cache-file
// descriptors and have no background goroutine to stop.
func (r *Reader) Stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		r.stopped.Store(true)
		if r.mappingStop != nil {
			close(r.mappingStop)
			r.mappingWG.Wait()
		}
		if client := r.rdb.Swap(nil); client != nil {
			_ = client.Close()
		}
	})
}

// DetectCacheDir finds the JuiceFS cache chunks directory.
func DetectCacheDir() string {
	paths, isolated, isolationErr := appisolation.ResolvePaths()
	if isolationErr != nil {
		return ""
	}
	// An explicit cache path makes multi-volume hosts and isolated integration
	// tests deterministic. Accept either the chunks directory itself or a
	// volume cache root containing raw/chunks; invalid overrides fail closed
	// instead of silently reading blocks from a different mounted volume.
	if configured := strings.TrimSpace(os.Getenv("JM_CACHE_DIR")); configured != "" {
		candidates := []string{filepath.Join(configured, "raw", "chunks")}
		if filepath.Base(filepath.Clean(configured)) == "chunks" {
			candidates = append(candidates, configured)
		}
		for _, candidate := range candidates {
			if err := appisolation.ValidateCachePath(candidate, true); err != nil {
				return ""
			}
			if info, err := os.Stat(candidate); err == nil && info.IsDir() {
				log.Printf("cache: using configured JuiceFS cache at %s", candidate)
				return candidate
			}
		}
		log.Printf("cache: JM_CACHE_DIR %q has no chunks directory", configured)
		return ""
	}
	cacheBase := paths.CacheBase
	if !isolated {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		cacheBase = filepath.Join(home, ".juicefs", "cache")
	}
	entries, err := os.ReadDir(cacheBase)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		chunksDir := filepath.Join(cacheBase, e.Name(), "raw", "chunks")
		if err := appisolation.ValidateCachePath(chunksDir, true); err != nil {
			return ""
		}
		if info, err := os.Stat(chunksDir); err == nil && info.IsDir() {
			log.Printf("cache: detected JuiceFS cache at %s", chunksDir)
			return chunksDir
		}
	}
	return ""
}

// SetRemoteAvailable retains local mappings while suspending network misses.
// Closing admission also invalidates actual in-flight fetch publication.
func (r *Reader) SetRemoteAvailable(available bool) {
	if r == nil {
		return
	}
	r.sliceMu.Lock()
	if !available && !r.remoteSuspended {
		r.remoteEpoch++
	}
	r.remoteSuspended = !available
	r.sliceMu.Unlock()
}
