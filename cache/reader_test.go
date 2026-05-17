package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const testRedisAddr = "127.0.0.1:6379"

func testRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr:        testRedisAddr,
		DB:          1,
		ReadTimeout: 10 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not reachable: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func TestDetectCacheDir(t *testing.T) {
	dir := DetectCacheDir()
	if dir == "" {
		t.Skip("No JuiceFS cache directory found")
	}
	t.Logf("Detected cache dir: %s", dir)

	// Verify it has the expected structure
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("cache dir is empty")
	}
	t.Logf("Cache dir has %d entries", len(entries))
}

func TestVerify(t *testing.T) {
	rdb := testRedisClient(t)
	dir := DetectCacheDir()
	if dir == "" {
		t.Skip("No JuiceFS cache directory found")
	}

	r := NewReader(dir, DefaultBlockSize, rdb)
	defer r.Stop()

	if err := r.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifyBadDir(t *testing.T) {
	rdb := testRedisClient(t)
	r := NewReader("/nonexistent/path", DefaultBlockSize, rdb)
	defer r.Stop()

	err := r.Verify()
	if err == nil {
		t.Fatal("expected error for nonexistent cache dir")
	}
}

func TestBlockPath(t *testing.T) {
	rdb := testRedisClient(t)
	r := NewReader("/fake/cache/chunks", DefaultBlockSize, rdb)
	defer r.Stop()

	tests := []struct {
		sliceID    uint64
		blockIndex int64
		want       string
	}{
		{995, 4, "/fake/cache/chunks/0/0/995_4_4194304"},
		{1000, 0, "/fake/cache/chunks/0/1/1000_0_4194304"},
		{1945, 3, "/fake/cache/chunks/0/1/1945_3_4194304"},
		{5500, 10, "/fake/cache/chunks/0/5/5500_10_4194304"},
	}

	for _, tt := range tests {
		got := r.blockPath(tt.sliceID, tt.blockIndex)
		if got != tt.want {
			t.Errorf("blockPath(%d, %d) = %q, want %q", tt.sliceID, tt.blockIndex, got, tt.want)
		}
	}
}

func TestGetSlicesFromRedis(t *testing.T) {
	rdb := testRedisClient(t)
	dir := DetectCacheDir()
	if dir == "" {
		t.Skip("No JuiceFS cache directory")
	}

	r := NewReader(dir, DefaultBlockSize, rdb)
	defer r.Stop()

	ctx := context.Background()

	// Find a chunk key to test with
	keys, _, err := rdb.Scan(ctx, 0, "c*_0", 5).Result()
	if err != nil || len(keys) == 0 {
		t.Skip("No chunk keys found in Redis")
	}

	// Parse inode from the first chunk key (format: c{inode}_{chunkIndex})
	var inode uint64
	fmt.Sscanf(keys[0], "c%d_0", &inode)
	t.Logf("Testing with chunk key %s (inode=%d)", keys[0], inode)

	slices, err := r.getSlices(ctx, inode, 0)
	if err != nil {
		t.Fatalf("getSlices: %v", err)
	}

	if len(slices) == 0 {
		t.Fatal("expected at least one slice")
	}

	for i, s := range slices {
		t.Logf("  slice[%d]: pos=%d sliceID=%d size=%d off=%d len=%d",
			i, s.Pos, s.SliceID, s.Size, s.Off, s.Len)
	}

	// Verify the cache is populated (second call should hit local cache)
	slices2, _ := r.getSlices(ctx, inode, 0)
	if len(slices2) != len(slices) {
		t.Fatalf("cached slices length mismatch: %d vs %d", len(slices2), len(slices))
	}
}

func TestReadCachedBlock(t *testing.T) {
	rdb := testRedisClient(t)
	dir := DetectCacheDir()
	if dir == "" {
		t.Skip("No JuiceFS cache directory")
	}

	r := NewReader(dir, DefaultBlockSize, rdb)
	defer r.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Strategy: pick a known inode from Redis, get its slices, check if any are cached.
	// This avoids the expensive reverse-map scan.
	var foundInode uint64
	var foundChunkIdx int64

	// Scan a small batch of chunk keys and check if their slices are in cache.
	// Limit iterations to avoid excessive round-trips on slow networks.
	var cursor uint64
	for attempt := 0; attempt < 5; attempt++ {
		keys, next, err := rdb.Scan(ctx, cursor, "c*", 100).Result()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}

		for _, key := range keys {
			var inode uint64
			var chunkIdx int64
			n, _ := fmt.Sscanf(key, "c%d_%d", &inode, &chunkIdx)
			if n != 2 {
				continue
			}

			items, _ := rdb.LRange(ctx, key, 0, -1).Result()
			for _, item := range items {
				data := []byte(item)
				if len(data) < 24 {
					continue
				}
				sliceID := binary.BigEndian.Uint64(data[4:12])
				// Check if block 0 of this slice exists in cache
				blockPath := r.blockPath(sliceID, 0)
				if _, err := os.Stat(blockPath); err == nil {
					foundInode = inode
					foundChunkIdx = chunkIdx
					t.Logf("Found cached slice: inode=%d chunkIdx=%d sliceID=%d",
						inode, chunkIdx, sliceID)
					goto found
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	t.Skip("No cached blocks found that match Redis chunk keys")

found:
	t.Logf("Testing read for inode=%d chunkIdx=%d", foundInode, foundChunkIdx)

	// Calculate the file offset (block 0 of the chunk)
	fileOffset := foundChunkIdx * ChunkSize

	// Read the block via our cache reader
	buf := make([]byte, DefaultBlockSize)
	n, err := r.ReadBlock(ctx, foundInode, fileOffset, buf)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}

	if n == 0 {
		t.Fatal("ReadBlock returned 0 bytes")
	}
	t.Logf("ReadBlock returned %d bytes from cache (direct SSD pread)", n)

	// Verify the data is non-zero (it's a real file)
	nonZero := 0
	for _, b := range buf[:n] {
		if b != 0 {
			nonZero++
		}
	}
	if nonZero == 0 {
		t.Fatal("all bytes are zero — likely read wrong data")
	}
	t.Logf("Data verification: %d/%d non-zero bytes", nonZero, n)
}

func TestReadCacheMiss(t *testing.T) {
	rdb := testRedisClient(t)
	dir := DetectCacheDir()
	if dir == "" {
		t.Skip("No JuiceFS cache directory")
	}

	r := NewReader(dir, DefaultBlockSize, rdb)
	defer r.Stop()

	ctx := context.Background()

	// Try to read from a non-existent inode — should miss
	buf := make([]byte, 4096)
	_, err := r.ReadBlock(ctx, 99999999, 0, buf)
	if err != ErrCacheMiss {
		t.Fatalf("expected ErrCacheMiss, got %v", err)
	}
}

func TestInvalidateSliceCache(t *testing.T) {
	rdb := testRedisClient(t)
	r := NewReader("/fake", DefaultBlockSize, rdb)
	defer r.Stop()

	// Manually populate slice cache
	r.sliceMu.Lock()
	r.sliceCache["42_0"] = []SliceInfo{{SliceID: 100}}
	r.sliceCache["42_1"] = []SliceInfo{{SliceID: 101}}
	r.sliceCache["99_0"] = []SliceInfo{{SliceID: 200}}
	r.sliceMu.Unlock()

	r.InvalidateSliceCache(42)

	r.sliceMu.RLock()
	defer r.sliceMu.RUnlock()

	if _, ok := r.sliceCache["42_0"]; ok {
		t.Fatal("42_0 should have been invalidated")
	}
	if _, ok := r.sliceCache["42_1"]; ok {
		t.Fatal("42_1 should have been invalidated")
	}
	if _, ok := r.sliceCache["99_0"]; !ok {
		t.Fatal("99_0 should still exist")
	}
}

func BenchmarkDirectSSDRead(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr, DB: 1})
	defer rdb.Close()

	dir := DetectCacheDir()
	if dir == "" {
		b.Skip("No JuiceFS cache directory")
	}

	// Find a cached block file
	var blockPath string
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if info.Size() >= DefaultBlockSize {
			blockPath = path
			return filepath.SkipAll
		}
		return nil
	})
	if blockPath == "" {
		b.Skip("No cached block files found")
	}

	fd, err := os.Open(blockPath)
	if err != nil {
		b.Fatal(err)
	}
	defer fd.Close()

	buf := make([]byte, DefaultBlockSize)

	b.SetBytes(DefaultBlockSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fd.ReadAt(buf, 0)
	}
}
