package nfs

import (
	"bytes"
	"io"
	"testing"
)

// TestSpoolWriteFileSatisfiesBillyFile is a compile-time-style check
// that &spoolWriteFile{} can be assigned to billy.File. If the
// interface signature drifts in a future go-billy upgrade, this test
// will fail to build before any other test runs.
func TestSpoolWriteFileSatisfiesBillyFile(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/iface.bin")
	defer e.CloseAndDelete("test cleanup")

	wf := &spoolWriteFile{
		name:    "/iface.bin",
		entry:   e,
		handler: nil, // not exercised in interface-shape test
	}

	// Compile-time assertions: billy.File + io.WriterAt.
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
	} = wf
	var _ io.WriterAt = wf
}

// TestSpoolWriteFileWriteAndRead covers the same-handle read-after-write
// case: write some bytes via WriteAt, then ReadAt at the same offset
// should return them.
func TestSpoolWriteFileWriteAndRead(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	handler := minimalHandlerForTest()
	defer handler.StopHandler()

	e, _ := s.OpenWrite("/rw.bin")
	wf := &spoolWriteFile{
		name:    "/rw.bin",
		entry:   e,
		handler: handler,
	}

	payload := []byte("the spool writes back")
	if n, err := wf.WriteAt(payload, 0); err != nil || n != len(payload) {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}

	got := make([]byte, len(payload))
	if n, err := wf.ReadAt(got, 0); err != nil || n != len(payload) {
		t.Fatalf("ReadAt: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("got %q, want %q", got, payload)
	}

	if err := wf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestSpoolWriteFileSeek covers all whence modes.
func TestSpoolWriteFileSeek(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	handler := minimalHandlerForTest()
	defer handler.StopHandler()

	e, _ := s.OpenWrite("/seek.bin")
	wf := &spoolWriteFile{
		name:    "/seek.bin",
		entry:   e,
		handler: handler,
	}
	defer wf.Close()

	if _, err := wf.WriteAt(make([]byte, 100), 0); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	cases := []struct {
		whence int
		offset int64
		want   int64
	}{
		{io.SeekStart, 50, 50},
		{io.SeekCurrent, 10, 60},
		{io.SeekEnd, -20, 80},
		{io.SeekStart, 0, 0},
	}
	for _, c := range cases {
		got, err := wf.Seek(c.offset, c.whence)
		if err != nil {
			t.Errorf("Seek(%d, %d): %v", c.offset, c.whence, err)
		}
		if got != c.want {
			t.Errorf("Seek(%d, %d): got %d, want %d", c.offset, c.whence, got, c.want)
		}
	}

	if _, err := wf.Seek(0, 99); err == nil {
		t.Errorf("Seek with invalid whence should error")
	}
}

// TestSpoolWriteFileWriteAdvancesPos verifies that sequential Write
// calls (the io.Writer surface) properly chain via the internal
// position counter.
func TestSpoolWriteFileWriteAdvancesPos(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	handler := minimalHandlerForTest()
	defer handler.StopHandler()

	e, _ := s.OpenWrite("/seq.bin")
	wf := &spoolWriteFile{
		name:    "/seq.bin",
		entry:   e,
		handler: handler,
	}
	defer wf.Close()

	if _, err := wf.Write([]byte("hello ")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if _, err := wf.Write([]byte("world")); err != nil {
		t.Fatalf("write 2: %v", err)
	}

	got := make([]byte, 11)
	if _, err := wf.ReadAt(got, 0); err != nil {
		t.Fatalf("readat: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("got %q, want %q", got, "hello world")
	}
}

// TestSpoolWriteFileTruncateZeroOnFreshIsNoop covers the truncate shapes on
// a fresh and a written entry. Phase 1 replaced slice C's truncate-to-0-on-
// fresh-only restriction with full ftruncate support (SETATTR{size} against
// spooled paths — fio preallocate, cp's final ftruncate); the broader resize
// coverage lives in nfs/spool_setattr_test.go.
func TestSpoolWriteFileTruncateZeroOnFreshIsNoop(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	handler := minimalHandlerForTest()
	defer handler.StopHandler()

	e, _ := s.OpenWrite("/trunc.bin")
	wf := &spoolWriteFile{
		name:    "/trunc.bin",
		entry:   e,
		handler: handler,
	}
	defer wf.Close()

	if err := wf.Truncate(0); err != nil {
		t.Errorf("Truncate(0) on empty: %v", err)
	}

	if _, err := wf.WriteAt([]byte("x"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := wf.Truncate(0); err != nil {
		t.Errorf("Truncate(0) on non-empty (O_TRUNC-style reset): %v", err)
	}
	if got := e.WrittenEnd(); got != 0 {
		t.Errorf("WrittenEnd after truncate-to-zero = %d, want 0", got)
	}
	if err := wf.Truncate(10); err != nil {
		t.Errorf("Truncate(nonzero) extend: %v", err)
	}
	if got := e.WrittenEnd(); got != 10 {
		t.Errorf("WrittenEnd after extend = %d, want 10", got)
	}
}

// TestSpoolWriteFileCloseDoesNotFinalize verifies the NFS-compatible
// lifecycle: a per-RPC spoolWriteFile.Close releases the handle but does
// NOT finalize the entry (NFS closes after every WRITE RPC, so finalizing
// on close would end the file after the first chunk). Finalize happens via
// the idle sweeper once the writer is quiescent.
func TestSpoolWriteFileCloseDoesNotFinalize(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	handler := minimalHandlerForTest()
	defer handler.StopHandler()

	e, _ := s.OpenWrite("/finalize.bin")
	wf := &spoolWriteFile{
		name:    "/finalize.bin",
		entry:   e,
		handler: handler,
	}

	if _, err := wf.WriteAt([]byte("done"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Per-RPC Close must NOT have finalized — the writer might still send
	// more WRITE RPCs to the same path.
	if e.SHA256() != nil {
		t.Errorf("per-RPC Close should not finalize the entry (SHA set prematurely)")
	}

	// The idle sweeper finalizes once the handle is released and the entry
	// is quiescent. sweepOnce(0) forces it deterministically.
	if n := s.sweepOnce(0); n != 1 {
		t.Fatalf("sweepOnce finalized %d entries, want 1", n)
	}
	if e.SHA256() == nil {
		t.Errorf("SHA should be finalized after the idle sweep")
	}
}

// TestSpoolWriteFileWriteTracksHandlerSize ensures the per-write
// trackWriteSize call lands so Stat/Lstat see the in-flight size while
// the entry is still being written.
func TestSpoolWriteFileWriteTracksHandlerSize(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	handler := minimalHandlerForTest()
	defer handler.StopHandler()

	e, _ := s.OpenWrite("/track.bin")
	wf := &spoolWriteFile{
		name:    "/track.bin",
		entry:   e,
		handler: handler,
	}
	defer wf.Close()

	if _, err := wf.WriteAt(make([]byte, 1234), 0); err != nil {
		t.Fatalf("write: %v", err)
	}

	handler.writeSizeMu.Lock()
	size := handler.writeSizes["/track.bin"]
	handler.writeSizeMu.Unlock()
	if size != 1234 {
		t.Errorf("handler.writeSizes=%d, want 1234", size)
	}
}

// minimalHandlerForTest constructs the smallest JuiceMountHandler that
// satisfies spoolWriteFile's dependencies: writeSizes + activeWriters
// maps and the fuse path. Avoids the heavy NewHandler ctor (which
// requires a *metadata.Store).
func minimalHandlerForTest() *JuiceMountHandler {
	return &JuiceMountHandler{
		writeSizes:    make(map[string]int64),
		activeWriters: make(map[string]int),
	}
}
