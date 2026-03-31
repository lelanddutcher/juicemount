package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultBlockSize = 4 << 20 // 4MB
	ChunkSize        = 64 << 20 // 64MB (JuiceFS default)

	fdTTL     = 5 * time.Minute
	evictTick = 30 * time.Second
)

var ErrCacheMiss = errors.New("cache miss")

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

	// Pooled file descriptors for cache block files
	fdMu  sync.Mutex
	fdMap map[string]*cachedFD

	stopCh chan struct{}
}

type cachedFD struct {
	fd       *os.File
	lastUsed time.Time
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
		fdMap:      make(map[string]*cachedFD),
		stopCh:     make(chan struct{}),
	}
	if cacheDir != "" {
		go r.evictLoop()
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
	if r.cacheDir == "" {
		return 0, ErrCacheMiss
	}

	// Determine chunk index and offset within chunk
	chunkIndex := fileOffset / ChunkSize
	offsetInChunk := fileOffset % ChunkSize

	// Get slice mapping for this chunk
	slices, err := r.getSlices(ctx, inode, chunkIndex)
	if err != nil {
		return 0, ErrCacheMiss
	}

	// Find which slice covers this offset
	for _, s := range slices {
		sliceStart := int64(s.Pos)
		sliceEnd := sliceStart + int64(s.Len)

		if offsetInChunk >= sliceStart && offsetInChunk < sliceEnd {
			// This slice covers our offset
			offsetInSlice := offsetInChunk - sliceStart + int64(s.Off)
			blockIndex := offsetInSlice / r.blockSize
			offsetInBlock := offsetInSlice % r.blockSize

			// Build cache file path
			blockPath := r.blockPath(s.SliceID, blockIndex)

			// Try to read from cache
			n, err := r.readFromCache(blockPath, buf, offsetInBlock)
			if err != nil {
				return 0, ErrCacheMiss
			}
			return n, nil
		}
	}

	return 0, ErrCacheMiss
}

// getSlices fetches the slice mapping for a chunk, caching it locally.
func (r *Reader) getSlices(ctx context.Context, inode uint64, chunkIndex int64) ([]SliceInfo, error) {
	key := fmt.Sprintf("%d_%d", inode, chunkIndex)

	r.sliceMu.RLock()
	if slices, ok := r.sliceCache[key]; ok {
		r.sliceMu.RUnlock()
		return slices, nil
	}
	r.sliceMu.RUnlock()

	// Fetch from Redis
	redisKey := fmt.Sprintf("c%s", key)
	items, err := r.rdb.LRange(ctx, redisKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var slices []SliceInfo
	for _, item := range items {
		data := []byte(item)
		if len(data) < 24 {
			continue
		}
		slices = append(slices, SliceInfo{
			Pos:     binary.BigEndian.Uint32(data[0:4]),
			SliceID: binary.BigEndian.Uint64(data[4:12]),
			Size:    binary.BigEndian.Uint32(data[12:16]),
			Off:     binary.BigEndian.Uint32(data[16:20]),
			Len:     binary.BigEndian.Uint32(data[20:24]),
		})
	}

	r.sliceMu.Lock()
	r.sliceCache[key] = slices
	r.sliceMu.Unlock()

	return slices, nil
}

// InvalidateSliceCache removes cached slice mappings for an inode.
// Call this when a file is modified.
func (r *Reader) InvalidateSliceCache(inode uint64) {
	r.sliceMu.Lock()
	defer r.sliceMu.Unlock()
	// Remove all chunk entries for this inode
	for key := range r.sliceCache {
		if strings.HasPrefix(key, fmt.Sprintf("%d_", inode)) {
			delete(r.sliceCache, key)
		}
	}
}

// blockPath returns the SSD cache file path for a given slice ID and block index.
// JuiceFS cache layout: chunks/{0}/{sliceID/1000}/{sliceID}_{blockIndex}_{blockSize}
func (r *Reader) blockPath(sliceID uint64, blockIndex int64) string {
	dir2 := sliceID / 1000
	return filepath.Join(
		r.cacheDir,
		"0",
		fmt.Sprintf("%d", dir2),
		fmt.Sprintf("%d_%d_%d", sliceID, blockIndex, r.blockSize),
	)
}

// readFromCache reads data from a cache block file.
func (r *Reader) readFromCache(blockPath string, buf []byte, offsetInBlock int64) (int, error) {
	fd := r.getFD(blockPath)
	if fd == nil {
		return 0, ErrCacheMiss
	}

	n, err := fd.ReadAt(buf, offsetInBlock)
	if err != nil && n > 0 {
		err = nil // partial read at end of block is OK
	}
	return n, err
}

func (r *Reader) getFD(path string) *os.File {
	r.fdMu.Lock()
	if cfd, ok := r.fdMap[path]; ok {
		cfd.lastUsed = time.Now()
		r.fdMu.Unlock()
		return cfd.fd
	}
	r.fdMu.Unlock()

	fd, err := os.Open(path)
	if err != nil {
		return nil
	}

	r.fdMu.Lock()
	if existing, ok := r.fdMap[path]; ok {
		r.fdMu.Unlock()
		fd.Close()
		existing.lastUsed = time.Now()
		return existing.fd
	}
	r.fdMap[path] = &cachedFD{fd: fd, lastUsed: time.Now()}
	r.fdMu.Unlock()
	return fd
}

func (r *Reader) evictLoop() {
	ticker := time.NewTicker(evictTick)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			r.fdMu.Lock()
			for p, cfd := range r.fdMap {
				if now.Sub(cfd.lastUsed) > fdTTL {
					cfd.fd.Close()
					delete(r.fdMap, p)
				}
			}
			r.fdMu.Unlock()
		}
	}
}

// Stop closes all cached fds and stops background goroutines.
func (r *Reader) Stop() {
	close(r.stopCh)
	r.fdMu.Lock()
	for _, cfd := range r.fdMap {
		cfd.fd.Close()
	}
	r.fdMap = nil
	r.fdMu.Unlock()
}

// DetectCacheDir finds the JuiceFS cache chunks directory.
func DetectCacheDir() string {
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
