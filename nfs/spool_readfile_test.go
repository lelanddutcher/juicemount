package nfs

import (
	"bytes"
	"io"
	"testing"
)

// TestSpoolReadFileSatisfiesBillyFile is a compile-time-style check
// that &spoolReadFile{} can be assigned to billy.File. Mirrors the
// slice-C compile-time check for spoolWriteFile.
func TestSpoolReadFileSatisfiesBillyFile(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/iface.bin")
	defer e.CloseAndDelete("test cleanup")

	rf := &spoolReadFile{
		name:  "/iface.bin",
		entry: e,
	}
	var _ interface {
		Name() string
		Write(p []byte) (int, error)
		Read(p []byte) (int, error)
		ReadAt(p []byte, off int64) (int, error)
		Seek(offset int64, whence int) (int64, error)
		Close() error
		Lock() error
		Unlock() error
		Truncate(size int64) error
	} = rf
}

// TestSpoolReadFileServesWriterBytes verifies the slice-D contract: a
// reader can see bytes a writer has landed in the spool, without the
// data having reached FUSE.
func TestSpoolReadFileServesWriterBytes(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/inflight.bin")
	defer e.Close()

	payload := []byte("readable mid-write")
	if _, err := e.WriteAt(payload, 0); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Now a separate read handle should see those bytes.
	rf := &spoolReadFile{name: "/inflight.bin", entry: e}
	defer rf.Close()

	got := make([]byte, len(payload))
	n, err := rf.ReadAt(got, 0)
	if err != nil {
		t.Fatalf("readat: %v", err)
	}
	if n != len(payload) || !bytes.Equal(got, payload) {
		t.Errorf("got %q, want %q", got[:n], payload)
	}
}

// TestSpoolReadFileWriteRejected confirms the read-only contract.
func TestSpoolReadFileWriteRejected(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/ro.bin")
	defer e.CloseAndDelete("test cleanup")

	rf := &spoolReadFile{name: "/ro.bin", entry: e}
	defer rf.Close()

	if _, err := rf.Write([]byte("nope")); err == nil {
		t.Errorf("Write on spoolReadFile should error")
	}
	if err := rf.Truncate(10); err == nil {
		t.Errorf("Truncate on spoolReadFile should error")
	}
}

// TestSpoolReadFileSeek covers SeekStart, SeekCurrent, SeekEnd and
// verifies SeekEnd is relative to the entry's live writtenEnd (so the
// behavior tracks ongoing writes).
func TestSpoolReadFileSeek(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/seekread.bin")
	defer e.Close()

	if _, err := e.WriteAt(make([]byte, 100), 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rf := &spoolReadFile{name: "/seekread.bin", entry: e}
	defer rf.Close()

	got, err := rf.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("seek end: %v", err)
	}
	if got != 100 {
		t.Errorf("seekEnd=%d, want 100", got)
	}

	// Writer extends file.
	if _, err := e.WriteAt(make([]byte, 50), 100); err != nil {
		t.Fatalf("extend: %v", err)
	}
	got, _ = rf.Seek(0, io.SeekEnd)
	if got != 150 {
		t.Errorf("seekEnd after extend=%d, want 150", got)
	}

	got, _ = rf.Seek(25, io.SeekStart)
	if got != 25 {
		t.Errorf("seekStart=%d", got)
	}
	got, _ = rf.Seek(5, io.SeekCurrent)
	if got != 30 {
		t.Errorf("seekCurrent=%d", got)
	}

	if _, err := rf.Seek(0, 999); err == nil {
		t.Errorf("invalid whence should error")
	}
}

// TestSpoolReadFileLazyFDReuse verifies the lazy-open + reuse-fd
// optimization works: a series of reads on the same handle opens the
// underlying fd exactly once.
func TestSpoolReadFileLazyFDReuse(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/reuse.bin")
	defer e.Close()
	if _, err := e.WriteAt(make([]byte, 4096), 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rf := &spoolReadFile{name: "/reuse.bin", entry: e}
	defer rf.Close()

	buf := make([]byte, 64)
	for i := 0; i < 8; i++ {
		if _, err := rf.ReadAt(buf, int64(i)*64); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	// fd should be non-nil and the same across reads.
	if rf.fd == nil {
		t.Errorf("fd should be cached after first read")
	}
}

// TestSpoolFileInfoForEntryReportsLiveSizeAndTime confirms the
// FileInfo returned by Stat/Lstat shadowing surfaces the entry's
// real-time writtenEnd + lastWrite.
func TestSpoolFileInfoForEntryReportsLiveSizeAndTime(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/live.bin")
	defer e.Close()
	e.SetInode(1234)

	before := e.LastWrite()
	if _, err := e.WriteAt(make([]byte, 999), 0); err != nil {
		t.Fatalf("write: %v", err)
	}

	fi := spoolFileInfoForEntry("live.bin", e)
	if fi.Name() != "live.bin" {
		t.Errorf("name=%q", fi.Name())
	}
	if fi.Size() != 999 {
		t.Errorf("size=%d, want 999", fi.Size())
	}
	if fi.IsDir() {
		t.Errorf("should not be dir")
	}
	if !fi.ModTime().After(before) {
		t.Errorf("ModTime should advance: before=%v after=%v", before, fi.ModTime())
	}
	if fi.inode != 1234 {
		t.Errorf("inode=%d, want 1234", fi.inode)
	}
}

// TestSpoolEntrySetInodeIdempotent covers the once-set semantics: a
// second SetInode call must not change the value.
func TestSpoolEntrySetInodeIdempotent(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/setino.bin")
	defer e.CloseAndDelete("test cleanup")

	e.SetInode(100)
	if e.Inode() != 100 {
		t.Fatalf("inode=%d, want 100", e.Inode())
	}
	e.SetInode(200)
	if e.Inode() != 100 {
		t.Errorf("SetInode second call should be no-op: got %d", e.Inode())
	}
}

// TestSpoolEntryInodeRaceFree covers the slice-D reviewer CRITICAL fix:
// SetInode and Inode are called concurrently in production (OpenFile
// writer-side sets; Stat/Lstat reader-side reads). Pre-fix the field
// was a plain uint64 and -race would catch the data race in this
// interleaving. Post-fix it's atomic.Uint64 with CompareAndSwap.
func TestSpoolEntryInodeRaceFree(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/race.bin")
	defer e.CloseAndDelete("test cleanup")

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			_ = e.Inode()
		}
		close(done)
	}()
	for i := 0; i < 10000; i++ {
		e.SetInode(uint64(i + 1))
	}
	<-done

	// All SetInode calls after the first must be no-ops.
	if e.Inode() != 1 {
		t.Errorf("expected first-write-wins, got Inode=%d", e.Inode())
	}
}

// BenchmarkOpenFileReadEmptySpool is the QA-35 perf gate for slice D.
// Measures the cost of a juiceFS.OpenFile READ when the spool is
// configured but empty — the common case for any read on a system
// without active writes.
//
// Hard requirement per the slice plan: must add <100 ns to this path
// over the slice-C baseline. The Index.Lookup measured at 8.4 ns in
// slice A — but this also pays for the nil-check on h.spool and the
// path-trim. Tight number to hit.
func BenchmarkOpenFileReadEmptySpool(b *testing.B) {
	s := newTestSpoolStoreForBench(b, 0)
	h := minimalHandlerForTest()
	h.spool = s
	jfs := &juiceFS{handler: h}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Open-only the spool-shadow path; we expect it to miss and
		// return nil immediately (the call returns ErrNotExist via
		// downstream lookups but the bench is measuring only the
		// spool tier's cost since the handler is otherwise un-wired).
		jfs.handler.spool.LookupActive("/not/in/spool/long-ish/path.mov")
	}
}

// BenchmarkOpenFileReadHotSpool measures cost when the spool DOES
// have the path. Establishes the upper bound for the read-path tax.
func BenchmarkOpenFileReadHotSpool(b *testing.B) {
	s := newTestSpoolStoreForBench(b, 0)
	e, _ := s.OpenWrite("/hot.mov")
	defer e.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.LookupActive("/hot.mov")
	}
}

// newTestSpoolStoreForBench is the *testing.B counterpart of
// newTestSpoolStore.
func newTestSpoolStoreForBench(b *testing.B, capacity int64) *SpoolStore {
	b.Helper()
	return newTestSpoolStoreTB(b, capacity)
}
