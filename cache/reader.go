package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultBlockSize = 4 << 20  // 4MB
	ChunkSize        = 64 << 20 // 64MB (JuiceFS default)
	checksumBlock    = 32 << 10 // JuiceFS CsExtend checksum granularity
)

var ErrCacheMiss = errors.New("cache miss")

var crc32c = crc32.MakeTable(crc32.Castagnoli)

// SliceInfo describes a JuiceFS slice within a chunk.
type SliceInfo struct {
	Pos     uint32 // position within the chunk
	SliceID uint64
	Size    uint32 // total slice size
	Off     uint32 // offset within the slice
	Len     uint32 // length of data in this slice
}

// Reader reads JuiceFS cache blocks directly from the SSD,
// bypassing the JuiceFS FUSE mount for cached data.
type Reader struct {
	cacheDir  string // e.g. ~/.juicefs/cache/{uuid}/raw/chunks
	blockSize int64
	rdb       *redis.Client

	// Cached chunk→slice mappings: key = "inode_chunkIndex"
	sliceMu    sync.RWMutex
	sliceCache map[string][]SliceInfo
	resetEpoch uint64
	inodeEpoch map[uint64]uint64

	// Only one Redis fetch per chunk may be in flight. Without this, the first
	// concurrent NFS reads after an invalidation stampede the remote metadata
	// link with identical LRANGE calls.
	flightMu sync.Mutex
	flights  map[string]*sliceFlight

	// NFS reads are normally 128 KiB–1 MiB. Reuse private all-or-nothing
	// buffers so CRC safety does not turn a 10GbE cached stream into equivalent
	// GC bandwidth. Buffers above 8 MiB are intentionally not retained.
	bufferPool sync.Pool
}

type sliceFlight struct {
	done       chan struct{}
	generation cacheGeneration
	slices     []SliceInfo
	err        error
}

type cacheGeneration struct {
	reset uint64
	inode uint64
}

// NewReader creates a cache reader. cacheDir is the path to the JuiceFS
// cache chunks directory (e.g. ~/.juicefs/cache/{uuid}/raw/chunks).
// rdb is used to fetch chunk→slice mappings on demand.
func NewReader(cacheDir string, blockSize int64, rdb *redis.Client) *Reader {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}
	r := &Reader{
		cacheDir:   cacheDir,
		blockSize:  blockSize,
		rdb:        rdb,
		sliceCache: make(map[string][]SliceInfo),
		inodeEpoch: make(map[uint64]uint64),
		flights:    make(map[string]*sliceFlight),
	}
	return r
}

// Verify checks that the JuiceFS version is compatible and the cache directory
// exists with the expected structure.
func (r *Reader) Verify() error {
	if r.cacheDir == "" {
		return fmt.Errorf("no cache directory configured")
	}
	if _, err := os.Stat(r.cacheDir); err != nil {
		return fmt.Errorf("cache dir not accessible: %w", err)
	}

	// Check JuiceFS version
	out, err := exec.Command("/opt/homebrew/bin/juicefs", "--version").Output()
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
	return r.readBlock(ctx, inode, fileOffset, buf, 0)
}

// ReadBlockWithMappingTimeout is ReadBlock with a bound applied only when a
// chunk→slice mapping is absent from RAM. The hot cached path creates no timer;
// this keeps 10GbE reads cheap while preventing a first remote LRANGE from
// parking an NFS connection indefinitely.
func (r *Reader) ReadBlockWithMappingTimeout(ctx context.Context, inode uint64, fileOffset int64, buf []byte, timeout time.Duration) (int, error) {
	return r.readBlock(ctx, inode, fileOffset, buf, timeout)
}

func (r *Reader) readBlock(ctx context.Context, inode uint64, fileOffset int64, buf []byte, mappingTimeout time.Duration) (int, error) {
	if r.cacheDir == "" || fileOffset < 0 || len(buf) == 0 {
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

		slices, err := r.getSlicesBounded(ctx, inode, chunkIndex, mappingTimeout)
		if err != nil {
			return 0, ErrCacheMiss
		}
		s, ok := sliceAt(slices, offsetInChunk)
		if !ok || s.SliceID == 0 {
			return 0, ErrCacheMiss
		}

		sliceStart := int64(s.Pos)
		sliceEnd := sliceStart + int64(s.Len)
		offsetInSlice := int64(s.Off) + offsetInChunk - sliceStart
		blockIndex := offsetInSlice / r.blockSize
		offsetInBlock := offsetInSlice % r.blockSize
		blockLen, ok := r.blockLength(s, blockIndex)
		if !ok || offsetInBlock >= blockLen {
			return 0, ErrCacheMiss
		}

		want := int64(len(verified) - read)
		want = minInt64(want, sliceEnd-offsetInChunk)
		want = minInt64(want, blockLen-offsetInBlock)
		want = minInt64(want, ChunkSize-offsetInChunk)
		if want <= 0 {
			return 0, ErrCacheMiss
		}

		if err := r.readVerifiedBlock(s.SliceID, blockIndex, blockLen,
			offsetInBlock, verified[read:read+int(want)]); err != nil {
			return 0, ErrCacheMiss
		}
		read += int(want)
	}

	r.sliceMu.RLock()
	unchanged := r.generationLocked(inode) == startGeneration
	r.sliceMu.RUnlock()
	if !unchanged {
		return 0, ErrCacheMiss
	}
	copy(buf, verified)
	return len(buf), nil
}

// getSlices fetches the slice mapping for a chunk, caching it locally.
func (r *Reader) getSlices(ctx context.Context, inode uint64, chunkIndex int64) ([]SliceInfo, error) {
	return r.getSlicesBounded(ctx, inode, chunkIndex, 0)
}

func (r *Reader) getSlicesBounded(ctx context.Context, inode uint64, chunkIndex int64, timeout time.Duration) ([]SliceInfo, error) {
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
	r.sliceMu.RLock()
	fetchGeneration := r.generationLocked(inode)
	r.sliceMu.RUnlock()

	r.flightMu.Lock()
	if flight := r.flights[key]; flight != nil {
		r.flightMu.Unlock()
		select {
		case <-flight.done:
			r.sliceMu.RLock()
			current := r.generationLocked(inode)
			r.sliceMu.RUnlock()
			if current != flight.generation {
				return nil, errors.New("slice mapping invalidated during shared fetch")
			}
			return flight.slices, flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	flight := &sliceFlight{done: make(chan struct{}), generation: fetchGeneration}
	r.flights[key] = flight
	r.flightMu.Unlock()

	defer func() {
		r.flightMu.Lock()
		delete(r.flights, key)
		close(flight.done)
		r.flightMu.Unlock()
	}()

	if r.rdb == nil {
		flight.err = errors.New("no Redis client")
		return nil, flight.err
	}

	// Fetch from Redis
	redisKey := fmt.Sprintf("c%s", key)
	items, err := r.rdb.LRange(ctx, redisKey, 0, -1).Result()
	if err != nil {
		flight.err = err
		return nil, err
	}

	raw := make([]SliceInfo, 0, len(items))
	for _, item := range items {
		data := []byte(item)
		if len(data) != 24 {
			flight.err = fmt.Errorf("corrupt slice record: length %d", len(data))
			return nil, flight.err
		}
		raw = append(raw, SliceInfo{
			Pos:     binary.BigEndian.Uint32(data[0:4]),
			SliceID: binary.BigEndian.Uint64(data[4:12]),
			Size:    binary.BigEndian.Uint32(data[12:16]),
			Off:     binary.BigEndian.Uint32(data[16:20]),
			Len:     binary.BigEndian.Uint32(data[20:24]),
		})
	}
	slices, err := resolveSlices(raw)
	if err != nil {
		flight.err = err
		return nil, err
	}

	r.sliceMu.Lock()
	if r.generationLocked(inode) != fetchGeneration {
		r.sliceMu.Unlock()
		flight.err = errors.New("slice mapping invalidated during fetch")
		return nil, flight.err
	}
	r.sliceCache[key] = slices
	r.sliceMu.Unlock()

	flight.slices = slices
	return slices, nil
}

// resolveSlices reproduces JuiceFS meta.buildSlice's append-order overlay:
// each later Redis list item replaces the covered interval from all earlier
// slices. Returning raw LRANGE records directly is unsafe after overwrite or
// compaction because both old and new extents can cover the same file offset.
func resolveSlices(raw []SliceInfo) ([]SliceInfo, error) {
	visible := make([]SliceInfo, 0, len(raw))
	for _, next := range raw {
		if next.Len == 0 {
			continue
		}
		end := uint64(next.Pos) + uint64(next.Len)
		if end > ChunkSize {
			return nil, fmt.Errorf("slice interval overflows chunk: pos=%d len=%d", next.Pos, next.Len)
		}
		if next.SliceID != 0 && (uint64(next.Off)+uint64(next.Len) > uint64(next.Size)) {
			return nil, fmt.Errorf("slice extent exceeds source: id=%d size=%d off=%d len=%d",
				next.SliceID, next.Size, next.Off, next.Len)
		}

		nextStart, nextEnd := uint64(next.Pos), end
		updated := make([]SliceInfo, 0, len(visible)+1)
		for _, old := range visible {
			oldStart := uint64(old.Pos)
			oldEnd := oldStart + uint64(old.Len)
			if oldEnd <= nextStart || oldStart >= nextEnd {
				updated = append(updated, old)
				continue
			}
			if oldStart < nextStart {
				left := old
				left.Len = uint32(nextStart - oldStart)
				updated = append(updated, left)
			}
			if oldEnd > nextEnd {
				right := old
				delta := uint32(nextEnd - oldStart)
				right.Pos = uint32(nextEnd)
				right.Off += delta
				right.Len = uint32(oldEnd - nextEnd)
				updated = append(updated, right)
			}
		}
		updated = append(updated, next)
		sort.Slice(updated, func(i, j int) bool { return updated[i].Pos < updated[j].Pos })
		visible = updated
	}
	return visible, nil
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
	defer r.sliceMu.Unlock()
	r.inodeEpoch[inode]++
	// Remove all chunk entries for this inode
	for key := range r.sliceCache {
		if strings.HasPrefix(key, fmt.Sprintf("%d_", inode)) {
			delete(r.sliceCache, key)
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
	r.sliceMu.Unlock()
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
func (r *Reader) readVerifiedBlock(sliceID uint64, blockIndex, blockLen, offset int64, dst []byte) error {
	if offset < 0 || blockLen <= 0 || int64(len(dst)) > blockLen-offset {
		return ErrCacheMiss
	}
	paths := []string{
		r.blockPath(sliceID, blockIndex, blockLen),
		r.hashBlockPath(sliceID, blockIndex, blockLen),
	}
	var fd *os.File
	for _, p := range paths {
		candidate, err := os.Open(p)
		if err == nil {
			fd = candidate
			break
		}
	}
	if fd == nil {
		return ErrCacheMiss
	}
	defer fd.Close()

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

// Stop is retained for lifecycle compatibility. Reads use fresh descriptors,
// so there are no background goroutines or pooled files to close.
func (r *Reader) Stop() {}

// DetectCacheDir finds the JuiceFS cache chunks directory.
func DetectCacheDir() string {
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
			if info, err := os.Stat(candidate); err == nil && info.IsDir() {
				log.Printf("cache: using configured JuiceFS cache at %s", candidate)
				return candidate
			}
		}
		log.Printf("cache: JM_CACHE_DIR %q has no chunks directory", configured)
		return ""
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	cacheBase := filepath.Join(home, ".juicefs", "cache")
	entries, err := os.ReadDir(cacheBase)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		chunksDir := filepath.Join(cacheBase, e.Name(), "raw", "chunks")
		if info, err := os.Stat(chunksDir); err == nil && info.IsDir() {
			log.Printf("cache: detected JuiceFS cache at %s", chunksDir)
			return chunksDir
		}
	}
	return ""
}
