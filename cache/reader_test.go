package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultTestRedisAddr = "127.0.0.1:6379"

func testRedisAddr() string {
	if addr := os.Getenv("JM_TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return defaultTestRedisAddr
}

func testRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr:        testRedisAddr(),
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

func TestDetectCacheDirConfigured(t *testing.T) {
	root := t.TempDir()
	chunks := filepath.Join(root, "raw", "chunks")
	if err := os.MkdirAll(chunks, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_CACHE_DIR", root)
	if got := DetectCacheDir(); got != chunks {
		t.Fatalf("DetectCacheDir() = %q, want configured %q", got, chunks)
	}
}

func TestDetectCacheDirConfiguredInvalidFailsClosed(t *testing.T) {
	t.Setenv("JM_CACHE_DIR", filepath.Join(t.TempDir(), "missing"))
	if got := DetectCacheDir(); got != "" {
		t.Fatalf("DetectCacheDir() = %q for invalid override, want empty", got)
	}
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
	r := NewReader("/fake/cache/chunks", DefaultBlockSize, nil)
	defer r.Stop()

	tests := []struct {
		sliceID    uint64
		blockIndex int64
		blockLen   int64
		want       string
	}{
		{995, 4, DefaultBlockSize, "/fake/cache/chunks/0/0/995_4_4194304"},
		{1000, 0, DefaultBlockSize, "/fake/cache/chunks/0/1/1000_0_4194304"},
		{1945, 3, 221863, "/fake/cache/chunks/0/1/1945_3_221863"},
		{1000123, 0, DefaultBlockSize, "/fake/cache/chunks/1/1000/1000123_0_4194304"},
	}

	for _, tt := range tests {
		got := r.blockPath(tt.sliceID, tt.blockIndex, tt.blockLen)
		if got != tt.want {
			t.Errorf("blockPath(%d, %d, %d) = %q, want %q", tt.sliceID, tt.blockIndex, tt.blockLen, got, tt.want)
		}
	}
	if got, want := r.hashBlockPath(1000123, 2, 777), "/fake/cache/chunks/BB/1/1000123_2_777"; got != want {
		t.Fatalf("hashBlockPath = %q, want %q", got, want)
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
	var foundFusePath string
	var cursor uint64
	if inode, chunkIdx, fusePath, ok := warmCacheFixture(t, rdb, r); ok {
		foundInode = inode
		foundChunkIdx = chunkIdx
		foundFusePath = fusePath
		goto found
	}

	// Scan a small batch of chunk keys and check if their slices are in cache.
	// Limit iterations to avoid excessive round-trips on slow networks.
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
				sliceSize := binary.BigEndian.Uint32(data[12:16])
				// Check if block 0 of this slice exists in cache
				blockPath := r.blockPath(sliceID, 0, minInt64(DefaultBlockSize, int64(sliceSize)))
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
	if os.Getenv("JM_TEST_FUSE_PATH") != "" {
		t.Fatal("isolated JuiceFS cache fixture did not produce a Redis-matched cached block")
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
	if foundFusePath != "" {
		f, err := os.Open(foundFusePath)
		if err != nil {
			t.Fatalf("open coherent fixture: %v", err)
		}
		defer f.Close()
		want := make([]byte, n)
		if _, err := f.ReadAt(want, fileOffset); err != nil {
			t.Fatalf("read coherent fixture: %v", err)
		}
		if !bytes.Equal(buf[:n], want) {
			t.Fatal("direct verified-cache bytes differ from the coherent JuiceFS view")
		}
		// Exercise a cross-block read and the real partial final block too; the
		// historic corruption lived specifically at those boundaries.
		for _, tc := range []struct {
			off  int64
			size int
		}{
			{DefaultBlockSize - 257, 4096},
			{2*DefaultBlockSize + 17, 1024},
		} {
			direct := make([]byte, tc.size)
			if n, err := r.ReadBlock(ctx, foundInode, tc.off, direct); err != nil || n != len(direct) {
				t.Fatalf("direct boundary read off=%d = %d, %v", tc.off, n, err)
			}
			coherent := make([]byte, tc.size)
			if _, err := f.ReadAt(coherent, tc.off); err != nil {
				t.Fatalf("coherent boundary read off=%d: %v", tc.off, err)
			}
			if !bytes.Equal(direct, coherent) {
				t.Fatalf("direct bytes differ at live cache boundary off=%d", tc.off)
			}
		}
	}
}

// warmCacheFixture makes the live-cache integration deterministic when the
// caller supplied an isolated JuiceFS mount. Depending on arbitrary ambient
// cache contents made the RC gate skip after normal cache eviction or mount
// restarts. A write + read through the mount gives Redis metadata and the
// JuiceFS block cache a matching, real slice to verify.
func warmCacheFixture(t *testing.T, rdb *redis.Client, r *Reader) (uint64, int64, string, bool) {
	t.Helper()
	mountPath := os.Getenv("JM_TEST_FUSE_PATH")
	if mountPath == "" {
		return 0, 0, "", false
	}

	fixturePath := filepath.Join(mountPath, fmt.Sprintf(".jm-cache-integration-%d.bin", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(fixturePath) })
	data := make([]byte, 2*DefaultBlockSize+4096)
	for i := range data {
		data[i] = byte((i % 251) + 1)
	}
	f, err := os.OpenFile(fixturePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Logf("cache fixture create failed, falling back to ambient cache scan: %v", err)
		return 0, 0, "", false
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		t.Logf("cache fixture write failed, falling back to ambient cache scan: %v", err)
		return 0, 0, "", false
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Logf("cache fixture sync failed, falling back to ambient cache scan: %v", err)
		return 0, 0, "", false
	}
	if err := f.Close(); err != nil {
		t.Logf("cache fixture close failed, falling back to ambient cache scan: %v", err)
		return 0, 0, "", false
	}
	if _, err := os.ReadFile(fixturePath); err != nil {
		t.Logf("cache fixture warm read failed, falling back to ambient cache scan: %v", err)
		return 0, 0, "", false
	}
	if juicefsBin, err := exec.LookPath("juicefs"); err == nil {
		warmCtx, warmCancel := context.WithTimeout(context.Background(), 20*time.Second)
		warmErr := exec.CommandContext(warmCtx, juicefsBin, "warmup", fixturePath).Run()
		warmCancel()
		if warmErr != nil {
			t.Logf("juicefs warmup failed, checking the read-populated cache: %v", warmErr)
		}
	} else {
		t.Log("juicefs warmup binary unavailable, checking the read-populated cache")
	}
	info, err := os.Stat(fixturePath)
	if err != nil {
		t.Logf("cache fixture stat failed, falling back to ambient cache scan: %v", err)
		return 0, 0, "", false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Ino == 0 {
		t.Log("cache fixture inode unavailable, falling back to ambient cache scan")
		return 0, 0, "", false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := fmt.Sprintf("c%d_0", stat.Ino)
	for ctx.Err() == nil {
		items, err := rdb.LRange(ctx, key, 0, -1).Result()
		if err == nil {
			for _, item := range items {
				encoded := []byte(item)
				if len(encoded) < 24 {
					continue
				}
				sliceID := binary.BigEndian.Uint64(encoded[4:12])
				sliceSize := binary.BigEndian.Uint32(encoded[12:16])
				if _, err := os.Stat(r.blockPath(sliceID, 0, minInt64(DefaultBlockSize, int64(sliceSize)))); err == nil {
					t.Logf("Warmed deterministic cached slice: inode=%d chunkIdx=0 sliceID=%d", stat.Ino, sliceID)
					return stat.Ino, 0, fixturePath, true
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("cache fixture did not materialize before timeout, falling back to ambient cache scan")
	return 0, 0, "", false
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
	r := NewReader("/fake", DefaultBlockSize, nil)
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

func TestResetRedisConnectionsSwapsPoolAndStopsCleanly(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DB: 3})
	r := NewReader("/fake", DefaultBlockSize, client)
	old := r.rdb.Load()
	if !r.ResetRedisConnections() {
		t.Fatal("ResetRedisConnections returned false")
	}
	next := r.rdb.Load()
	if next == nil || next == old {
		t.Fatal("Redis client pool was not replaced")
	}
	if next.Options().Addr != old.Options().Addr || next.Options().DB != old.Options().DB {
		t.Fatal("replacement Redis client did not preserve options")
	}
	r.Stop()
	if r.rdb.Load() != nil {
		t.Fatal("Stop left an active Redis client")
	}
	if r.ResetRedisConnections() {
		t.Fatal("ResetRedisConnections succeeded after Stop")
	}
}

func TestResolveSlicesNewestWins(t *testing.T) {
	raw := []SliceInfo{
		{Pos: 0, SliceID: 10, Size: 100, Off: 0, Len: 100},
		{Pos: 20, SliceID: 20, Size: 40, Off: 0, Len: 40},
	}
	got, err := resolveSlices(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := []SliceInfo{
		{Pos: 0, SliceID: 10, Size: 100, Off: 0, Len: 20},
		{Pos: 20, SliceID: 20, Size: 40, Off: 0, Len: 40},
		{Pos: 60, SliceID: 10, Size: 100, Off: 60, Len: 40},
	}
	if len(got) != len(want) {
		t.Fatalf("resolved slices = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resolved[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestResolveSlicesRejectsCorruptExtent(t *testing.T) {
	_, err := resolveSlices([]SliceInfo{{Pos: 0, SliceID: 9, Size: 10, Off: 7, Len: 4}})
	if err == nil {
		t.Fatal("out-of-bounds slice extent was accepted")
	}
}

func writeVerifiedCacheFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	encoded := append([]byte(nil), data...)
	var sum [4]byte
	for start := 0; start < len(data); start += checksumBlock {
		end := start + checksumBlock
		if end > len(data) {
			end = len(data)
		}
		binary.BigEndian.PutUint32(sum[:], crc32.Checksum(data[start:end], crc32c))
		encoded = append(encoded, sum[:]...)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestReadBlockVerifiedAcrossPartialBlock(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()

	const inode = 42
	const sliceID = 1000123
	sliceSize := DefaultBlockSize + 4096
	r.sliceCache["42_0"] = []SliceInfo{{Pos: 0, SliceID: sliceID, Size: uint32(sliceSize), Len: uint32(sliceSize)}}

	first := make([]byte, DefaultBlockSize)
	second := make([]byte, 4096)
	for i := range first {
		first[i] = byte((i % 251) + 1)
	}
	for i := range second {
		second[i] = byte(255 - i%251)
	}
	writeVerifiedCacheFile(t, r.blockPath(sliceID, 0, DefaultBlockSize), first)
	// Put the final partial block in the alternate hash-prefix layout to prove
	// both supported JuiceFS cache layouts are discovered.
	writeVerifiedCacheFile(t, r.hashBlockPath(sliceID, 1, 4096), second)

	buf := make([]byte, 200)
	n, err := r.ReadBlock(context.Background(), inode, DefaultBlockSize-100, buf)
	if err != nil || n != len(buf) {
		t.Fatalf("ReadBlock = %d, %v; want %d, nil", n, err, len(buf))
	}
	want := append(append([]byte(nil), first[len(first)-100:]...), second[:100]...)
	if !bytes.Equal(buf, want) {
		t.Fatal("verified read crossing a partial block returned wrong bytes")
	}
}

func TestReadBlockFailsClosedOnChecksumMismatch(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	r.sliceCache["7_0"] = []SliceInfo{{Pos: 0, SliceID: 77, Size: 4096, Len: 4096}}
	data := bytes.Repeat([]byte{0x5a}, 4096)
	p := r.blockPath(77, 0, 4096)
	writeVerifiedCacheFile(t, p, data)
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, 17); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	buf := bytes.Repeat([]byte{0xa5}, 512)
	before := append([]byte(nil), buf...)
	if n, err := r.ReadBlock(context.Background(), 7, 0, buf); n != 0 || !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("corrupt cache read = %d, %v; want 0, ErrCacheMiss", n, err)
	}
	if !bytes.Equal(buf, before) {
		t.Fatal("failed direct read modified the caller buffer")
	}
}

func TestReadBlockFailsClosedWithoutChecksumOrPastSlice(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	r.sliceCache["8_0"] = []SliceInfo{{Pos: 0, SliceID: 88, Size: 4096, Len: 100}}
	p := r.blockPath(88, 0, 4096)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	if n, err := r.ReadBlock(context.Background(), 8, 0, make([]byte, 50)); n != 0 || !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("unverified cache read = %d, %v; want miss", n, err)
	}

	writeVerifiedCacheFile(t, p, make([]byte, 4096))
	if n, err := r.ReadBlock(context.Background(), 8, 0, make([]byte, 101)); n != 0 || !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("read past visible slice = %d, %v; want miss", n, err)
	}
}

func BenchmarkDirectSSDRead(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr(), DB: 1})
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

func BenchmarkVerifiedCacheRead(b *testing.B) {
	r := NewReader(b.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	const inode = 42
	const sliceID = 1000123
	r.sliceCache["42_0"] = []SliceInfo{{Pos: 0, SliceID: sliceID, Size: DefaultBlockSize, Len: DefaultBlockSize}}
	data := make([]byte, DefaultBlockSize)
	for i := range data {
		data[i] = byte(i)
	}
	p := r.blockPath(sliceID, 0, DefaultBlockSize)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		b.Fatal(err)
	}
	encoded := append([]byte(nil), data...)
	var sum [4]byte
	for start := 0; start < len(data); start += checksumBlock {
		end := start + checksumBlock
		binary.BigEndian.PutUint32(sum[:], crc32.Checksum(data[start:end], crc32c))
		encoded = append(encoded, sum[:]...)
	}
	if err := os.WriteFile(p, encoded, 0o600); err != nil {
		b.Fatal(err)
	}

	buf := make([]byte, 1<<20)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(i%4) * int64(len(buf))
		if _, err := r.ReadBlock(context.Background(), inode, off, buf); err != nil {
			b.Fatal(err)
		}
	}
}
