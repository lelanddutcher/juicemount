package nfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// offline_read_overhead_test.go — is an offline cached read actually at disk speed?
//
// The product claim is that offline, every cached read runs at disk speed. The
// offline branch in cachedFile.ReadAt does not read straight into the caller's
// buffer. Per read it does:
//
//	tmp := make([]byte, len(p))          // 1. a fresh ZEROED allocation
//	readAtBounded(...)                   // 2. a goroutine, 3. a chan, 4. a timer
//	copy(p, tmp[:bn])                    // 5. a full memcpy
//
// The private buffer is REQUIRED and must not be removed: when the bound is
// exceeded, readAtBounded returns while its goroutine is still running, and
// that goroutine would otherwise write into the caller's POOLED p after the
// caller moved on. That is a real data-corruption guard.
//
// What is not required is allocating it fresh every time. These benchmarks
// measure the gap so the fix is chosen from a number rather than from reasoning
// — the metadata-TTL change was the elegant hypothesis and measured zero.
//
// Deliberately run against a plain local file, NOT the FUSE mount: the quantity
// under test is OUR per-read overhead, and mixing in real disk variance would
// bury it.

func benchFile(b *testing.B, size int) *os.File {
	b.Helper()
	p := filepath.Join(b.TempDir(), "bench.bin")
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i)
	}
	if err := os.WriteFile(p, buf, 0o644); err != nil {
		b.Fatal(err)
	}
	f, err := os.Open(p)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { f.Close() })
	// Warm the page cache so the disk itself is not the variable.
	_, _ = f.ReadAt(buf, 0)
	return f
}

// BenchmarkOnlineReadPath is the baseline: read straight into the caller's
// buffer, which is what the online path does.
func BenchmarkOnlineReadPath(b *testing.B) {
	for _, size := range []int{64 << 10, 128 << 10, 1 << 20} {
		b.Run(sizeName(size), func(b *testing.B) {
			f := benchFile(b, size)
			p := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := f.ReadAt(p, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkOfflineReadPath is the offline branch as written: fresh private
// buffer, bounded read via goroutine+chan+timer, then a copy into the caller's
// buffer.
func BenchmarkOfflineReadPath(b *testing.B) {
	for _, size := range []int{64 << 10, 128 << 10, 1 << 20} {
		b.Run(sizeName(size), func(b *testing.B) {
			f := benchFile(b, size)
			p := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tmp := make([]byte, len(p))
				n, err, done := readAtBounded(f, tmp, 0, 1500*time.Millisecond)
				if !done || err != nil {
					b.Fatalf("done=%v err=%v", done, err)
				}
				copy(p, tmp[:n])
			}
		})
	}
}

// BenchmarkOfflineReadPathPooledBuffer keeps the goroutine+chan+timer and the
// copy — i.e. keeps the corruption guard intact — and only stops allocating the
// private buffer fresh. This isolates how much of the gap is the allocation.
func BenchmarkOfflineReadPathPooledBuffer(b *testing.B) {
	for _, size := range []int{64 << 10, 128 << 10, 1 << 20} {
		b.Run(sizeName(size), func(b *testing.B) {
			f := benchFile(b, size)
			p := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tmp := offlineReadBuf(len(p))
				n, err, done := readAtBounded(f, tmp, 0, 1500*time.Millisecond)
				if !done || err != nil {
					b.Fatalf("done=%v err=%v", done, err)
				}
				copy(p, tmp[:n])
				releaseOfflineReadBuf(tmp, done)
			}
		})
	}
}

func sizeName(n int) string {
	switch {
	case n >= 1<<20:
		return "1MiB"
	case n >= 128<<10:
		return "128KiB"
	default:
		return "64KiB"
	}
}
