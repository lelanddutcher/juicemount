package nfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func finishFixture(t *testing.T) (*SpoolEntry, string, string) {
	t.Helper()
	s := newTestSpoolStore(t, 64<<20)
	e, err := s.OpenWrite("/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	realPath := filepath.Join(dir, "clip.mov")
	tempPath, err := streamTempPath(realPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tempPath, []byte("streamed content"), 0o644); err != nil {
		t.Fatal(err)
	}
	e.SetStreamDest(tempPath)
	return e, tempPath, realPath
}

// The rename IS the publish. Until it happens the file is invisible everywhere;
// after it, it exists whole. No reader can observe a partially-written file at
// the real path, which is the entire reason for the temp name.
func TestFinishStreamPublishesAtomically(t *testing.T) {
	e, tempPath, realPath := finishFixture(t)

	if err := finishStream(e, tempPath, realPath); err != nil {
		t.Fatalf("finishStream: %v", err)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Error("temp path still exists after the publish")
	}
	got, err := os.ReadFile(realPath)
	if err != nil {
		t.Fatalf("real path missing after the publish: %v", err)
	}
	if string(got) != "streamed content" {
		t.Errorf("published content = %q, want %q", got, "streamed content")
	}
	// The read shadow must now point at the REAL file. Clearing it instead would
	// fail every punched read closed until the entry is evicted — and the spool's
	// copy of those bytes is holes, so there is nowhere else to serve them from.
	if got := e.StreamDestPath(); got != realPath {
		t.Errorf("streamDest = %q after publish, want the real path %q — punched "+
			"reads would fail closed with the bytes only available here", got, realPath)
	}
}

// A read landing in the rename window must be RETRYABLE, not an error the client
// gives up on. The window exists because holding e.mu across a FUSE rename would
// put an unbounded syscall under a hot lock.
func TestReadInTheRenameWindowIsRetryable(t *testing.T) {
	e, tempPath, _ := finishFixture(t)
	// Simulate the window: streamDest still names the temp path, which is gone.
	if err := os.Remove(tempPath); err != nil {
		t.Fatal(err)
	}
	if _, err := e.WriteAt(make([]byte, 64<<10), 0); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.punchedEnd = 32 << 10
	e.mu.Unlock()

	rf := &spoolReadFile{name: "/clip.mov", entry: e}
	defer rf.Close()

	_, err := rf.ReadAt(make([]byte, 4096), 0)
	if !errors.Is(err, pin.ErrSpoolDrained) {
		t.Errorf("read in the rename window returned %v, want ErrSpoolDrained — "+
			"the client must REOPEN onto the published file, not fail the copy", err)
	}
}

// A cancelled or failed stream must remove its partial in-process. The boot
// sweep is the crash backstop; leaving an ordinary failure to it would hold
// hundreds of gigabytes until the next restart.
func TestRollbackRemovesThePartial(t *testing.T) {
	e, tempPath, _ := finishFixture(t)

	if err := rollbackStream(e, tempPath); err != nil {
		t.Fatalf("rollbackStream: %v", err)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Error("partial survived rollback — nothing else can see it, so it would " +
			"hold backend space until the next boot sweep")
	}
	if got := e.StreamDestPath(); got != "" {
		t.Errorf("streamDest = %q after rollback, want empty — reads must not be "+
			"routed at a file we abandoned", got)
	}
}

// Rollback must NOT clear punchedEnd.
//
// The spool file's punched prefix is unrecoverable, so a streamed entry can
// never fall back to a whole-file drain. punchedEnd is the durable marker that
// tells boot recovery this spool file is not self-sufficient; clearing it here
// would re-arm the exact "resume from a holey file" hazard the punched_end
// column was added to prevent.
func TestRollbackDoesNotClearPunchedEnd(t *testing.T) {
	e, tempPath, _ := finishFixture(t)
	e.mu.Lock()
	e.punchedEnd = 32 << 10
	e.mu.Unlock()

	if err := rollbackStream(e, tempPath); err != nil {
		t.Fatal(err)
	}
	if got := e.PunchedEnd(); got != 32<<10 {
		t.Errorf("punchedEnd = %d after rollback, want %d — clearing it tells boot "+
			"recovery the spool file is intact when its prefix is holes, and it "+
			"would upload zeros over good backend bytes", got, 32<<10)
	}
}

// Rollback of an already-absent partial is not an error — a crash may have taken
// it, or the sweep may have got there first.
func TestRollbackToleratesAMissingPartial(t *testing.T) {
	e, tempPath, _ := finishFixture(t)
	if err := os.Remove(tempPath); err != nil {
		t.Fatal(err)
	}
	if err := rollbackStream(e, tempPath); err != nil {
		t.Errorf("rollback of a missing partial errored: %v", err)
	}
	if got := e.StreamDestPath(); got != "" {
		t.Errorf("streamDest = %q, want empty", got)
	}
}
