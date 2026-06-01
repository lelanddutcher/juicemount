package nfs

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// QA-37 regression: deleting a spooled file before its drain settles must
// NOT resurrect it. Reproduces the 2026-06-01 10GbE finding — writing files
// then immediately deleting them while they were still spooled brought them
// back when the drainer ran, because nfs.Remove never cancelled the spool
// entry and the drainer happily wrote it into the FUSE mount afterward.
//
// Deterministic shape: copy a file (spool entry lands in `writing`), delete
// it BEFORE the sweep finalizes it to `ready`, then finalize + give the
// running drainer ample time to act. A correct delete cancels the spool
// entry, so there is nothing to drain and the file must stay gone. The
// pre-fix code left the entry intact and the drainer resurrected the file.
func TestSpoolDeleteOfPendingFileDoesNotResurrect(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandler(t)

	payload := deterministicPayload(7, 256*1024)
	if err := copyViaHandler(jfs, "race.bin", payload, 64*1024); err != nil {
		t.Fatalf("copy: %v", err)
	}

	// Delete the freshly-written, not-yet-drained file.
	if err := jfs.Remove("race.bin"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	// Finalize any quiescent entries and let the drainer run. With the bug
	// present, the still-indexed entry finalizes to `ready` here and the
	// drainer copies it into fuseRoot — resurrecting the deleted file.
	spool.sweepOnce(0)
	waitFor(t, 3*time.Second, func() bool {
		pf, _, _ := spool.Meta().PendingStats()
		return pf == 0
	})
	// Settle window for any in-flight FUSE write to land.
	time.Sleep(150 * time.Millisecond)

	if _, err := os.Stat(filepath.Join(fuseRoot, "race.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted file was resurrected by the drainer: stat err=%v (want not-exist)", err)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("spool capacity leaked after cancel: used=%d (want 0)", used)
	}
	if got := drainer.Metrics().DrainsSucceeded.Load(); got != 0 {
		t.Errorf("drainer drained %d file(s); a cancelled file must never drain", got)
	}
}

// QA-37 in-flight half: when the NFS layer cancels a spool entry while the
// drainer has ALREADY claimed it (draining state — exactly where the user's
// files were when they reappeared), the drainer's completion must be REFUSED
// so it undoes its FUSE write instead of resurrecting the deleted file. This
// exercises the writeMu-serialized DeleteActiveByPath ↔ MarkDone resolution
// directly, without depending on goroutine timing.
func TestSpoolMarkDrainCompleteRefusesCancelledRow(t *testing.T) {
	spoolDir := t.TempDir()
	db := openTestDB(t, filepath.Join(t.TempDir(), "m.db"))
	if err := metadata.InitSpoolSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	meta := metadata.NewSpoolStore(db)
	spool, err := NewSpoolStore(spoolDir, 0, meta)
	if err != nil {
		t.Fatalf("new spool: %v", err)
	}
	t.Cleanup(spool.Stop)

	// Build a ready entry (write + finalize), no drainer running.
	e, err := spool.OpenWrite("x.bin")
	if err != nil {
		t.Fatalf("openwrite: %v", err)
	}
	if _, err := e.WriteAt(deterministicPayload(3, 8192), 0); err != nil {
		t.Fatalf("writeat: %v", err)
	}
	e.ReleaseHandle()
	spool.sweepOnce(0) // finalize → ready

	// Drainer claims it: ready → draining (simulates an in-flight drain).
	claimed, err := meta.MarkDraining(e.ID())
	if err != nil || !claimed {
		t.Fatalf("claim: err=%v claimed=%v", err, claimed)
	}

	// The NFS delete races in while the drain is in flight.
	spool.CancelForDelete("x.bin")

	// Completion must be refused — otherwise the drained bytes resurrect the
	// file the user deleted.
	done, err := spool.MarkDrainComplete(e.ID(), "x.bin", e.SpoolFilePath(), e.WrittenEnd())
	if err != nil {
		t.Fatalf("MarkDrainComplete err: %v", err)
	}
	if done {
		t.Fatal("MarkDrainComplete returned done=true for a cancelled row — would resurrect a deleted file")
	}

	// Cancel cleaned up: index entry gone, capacity released, SQL row gone.
	if _, ok := spool.LookupActive("x.bin"); ok {
		t.Error("index entry survived CancelForDelete")
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("capacity leaked: used=%d (want 0)", used)
	}
	if _, gErr := meta.Get(e.ID()); !errors.Is(gErr, sql.ErrNoRows) {
		t.Errorf("spool row survived cancel: err=%v (want ErrNoRows)", gErr)
	}
}
