package nfs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestMemBufLoadCap is the regression guard for the 2026-06-14 thread-
// exhaustion crash: a burst of distinct small-file reads against a
// wedged/slow FUSE mount spawned one loadFile goroutine per file, each
// blocked in os.Open, until the process hit the macOS 8192-thread cap and
// the Go runtime aborted (8178 threads parked in open()).
//
// The fix bounds concurrent loaders with mb.loadSem. This test proves the
// bound holds even when every os.Open blocks forever: we point loadFile at
// a FIFO with no writer (a read-open of which blocks until a writer
// appears), fire far more Get calls than the cap, and assert that only
// `cap` loaders ever acquire a slot — the rest are declined (loadSkipped),
// so the caller falls through to the per-RPC fd-pool path.
func TestMemBufLoadCap(t *testing.T) {
	const cap = 4
	t.Setenv("JM_MEMBUF_LOADERS", "4")

	dir := t.TempDir()
	fifo := filepath.Join(dir, "wedged.fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	mb := NewMemoryBuffer(0, 0)
	defer mb.Stop()

	// Fire many more reads than the cap. Each distinct path is a fresh
	// entry; loadFile will block in os.Open(fifo) (no writer yet).
	const tries = 200
	for i := 0; i < tries; i++ {
		path := filepath.Join(dir, "file", string(rune('a'+i%26)), filepathInt(i))
		_ = mb.Get(path, 1024 /*fileSize*/, fifo)
	}

	// The loadSem acquire happens synchronously inside Get, so the counts
	// are deterministic the instant the loop returns — no sleep needed.
	if got := len(mb.loadSem); got != cap {
		t.Fatalf("concurrent loaders = %d, want exactly cap=%d (bound violated)", got, cap)
	}
	mb.statsMu.Lock()
	skipped := mb.loadSkipped
	mb.statsMu.Unlock()
	if skipped != tries-cap {
		t.Fatalf("loadSkipped = %d, want %d (tries-cap)", skipped, tries-cap)
	}

	// Drain: hold an O_RDWR end of the FIFO open for the whole drain window.
	// O_RDWR never blocks on a FIFO and keeps a writer present, so every
	// loader's blocked read-open completes (even ones whose goroutine
	// reaches os.Open slightly late). Their subsequent ReadAt on the
	// non-seekable FIFO returns ESPIPE, loadFile errors out and releases its
	// slot. Keep the fd open until fully drained, then close.
	rw, err := os.OpenFile(fifo, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open fifo rdwr: %v", err)
	}
	defer rw.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(mb.loadSem) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("loaders did not drain: len(loadSem)=%d", len(mb.loadSem))
}

// filepathInt avoids pulling strconv into the test's import set just for a
// unique path suffix.
func filepathInt(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
