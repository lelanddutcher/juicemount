package nfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// TestReopenDuringDrainDefersToFuse is the regression test for the reopen
// corruption that gates #105. The production hazard: while a finalized entry is
// still DRAINING, LookupActive is true, so the handler routes a continuation
// WRITE to spool.OpenWrite; OpenWrite waits for the drain, and when the entry
// EVICTS mid-wait it used to create a FRESH spool entry — whose own drain
// os.Create-truncates the just-drained backend file and clobbers it (an earlier
// probe showed the drained file lost entirely). The fix: once OpenWrite has
// seen a closed entry, it must NEVER create a fresh entry on evict — it returns
// ErrSpoolBusy so the write reroutes (via NFS3ERR_JUKEBOX retry) to the
// in-place fdPool path, which appends to the durable FUSE file without truncating.
//
// This matters for #105 because shortening the large-file finalize window makes
// finalize+drain happen more often mid-workflow, so a continuation write is far
// more likely to land in this during-drain window.
func TestReopenDuringDrainDefersToFuse(t *testing.T) {
	_, spool, drainer, fuseRoot := newBatchWiredHandlerNoDispatch(t, 0)
	ctx := context.Background()
	part1 := bytes.Repeat([]byte{0xA1}, 64*1024)

	e1, err := spool.OpenWrite("/cont.bin")
	if err != nil {
		t.Fatalf("OpenWrite1: %v", err)
	}
	if _, err := e1.WriteAt(part1, 0); err != nil {
		t.Fatalf("write part1: %v", err)
	}
	if err := e1.Close(); err != nil { // finalize — NOT drained yet
		t.Fatalf("close1: %v", err)
	}
	// Entry is closed but still index-resident (the during-drain window).
	if _, active := spool.LookupActive("/cont.bin"); !active {
		t.Skip("entry not index-resident after Close (already drained) — window not reproducible on this harness")
	}

	type res struct {
		e   *SpoolEntry
		err error
	}
	ch := make(chan res, 1)
	go func() {
		e2, err := spool.OpenWrite("/cont.bin") // enters the reopen-wait (sawClosed)
		ch <- res{e2, err}
	}()

	// Let the goroutine enter the reopen-wait loop, then drain (which evicts).
	time.Sleep(80 * time.Millisecond)
	drainer.DrainOnceForTest(ctx)

	select {
	case r := <-ch:
		if r.e != nil || !pin.IsSpoolBusy(r.err) {
			t.Fatalf("reopen during drain: got entry!=nil=%v err=%v, want nil entry + ErrSpoolBusy (defer to in-place fdPool)", r.e != nil, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("OpenWrite did not return within 5s — stuck in reopen-wait?")
	}

	// The drained backend file must be intact part1 — never clobbered.
	got, err := os.ReadFile(filepath.Join(fuseRoot, "cont.bin"))
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if !bytes.Equal(got, part1) {
		t.Fatalf("dest not intact part1 after reopen-defer: len=%d want %d", len(got), len(part1))
	}
}
