package nfs

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Phase-2 (LB-5 / item 2a+3) tests for /spool reporting: writing-row size
// overlay, stalled detection, failed-row relevance, recently-done tail,
// entry ages, and the pending-bytes overlay. Written failing-first.

// newTestSpoolStoreWithDB mirrors newTestSpoolStoreTB but also returns the
// raw *sql.DB so tests can backdate updated_at — the production code never
// rewinds clocks, so there's no store-level API for it (deliberately).
func newTestSpoolStoreWithDB(t *testing.T, capacity int64) (*SpoolStore, *sql.DB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "spool.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;`); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if err := metadata.InitSpoolSchema(db); err != nil {
		t.Fatalf("schema: %v", err)
	}
	store, err := NewSpoolStore(t.TempDir(), capacity, metadata.NewSpoolStore(db))
	if err != nil {
		t.Fatalf("new spool store: %v", err)
	}
	return store, db
}

// backdateRow rewinds a spool row's updated_at by `ago`.
func backdateRow(t *testing.T, db *sql.DB, id int64, ago time.Duration) {
	t.Helper()
	if _, err := db.Exec(`UPDATE spool_entries SET updated_at=? WHERE id=?`,
		time.Now().Add(-ago).Unix(), id); err != nil {
		t.Fatalf("backdate row %d: %v", id, err)
	}
}

// TestBuildSpoolStatusWritingSizeOverlay is item 3a/3c: a `writing` row
// persists size=0 in SQL until finalize, but the live index entry knows
// WrittenEnd. The status must overlay it — the UI must never render
// "Zero KB" for an entry holding real bytes, and pending_bytes must
// include those live bytes so pending_files>0 with pending_bytes=0
// can't recur.
func TestBuildSpoolStatusWritingSizeOverlay(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	e, err := s.OpenWrite("/edit/inflight.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := e.WriteAt(make([]byte, 1000), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Deliberately NOT closed — the row stays in `writing` with SQL size 0.

	resp, err := BuildSpoolStatus(s, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if resp.PendingFiles != 1 {
		t.Fatalf("PendingFiles=%d, want 1", resp.PendingFiles)
	}
	if resp.PendingBytes != 1000 {
		t.Errorf("PendingBytes=%d, want 1000 (live WrittenEnd overlay)", resp.PendingBytes)
	}
	if len(resp.Entries) != 1 {
		t.Fatalf("entries=%d, want 1", len(resp.Entries))
	}
	if resp.Entries[0].DrainState != "writing" {
		t.Errorf("drain_state=%q, want writing", resp.Entries[0].DrainState)
	}
	if resp.Entries[0].Size != 1000 {
		t.Errorf("entry size=%d, want 1000 (live WrittenEnd overlay)", resp.Entries[0].Size)
	}
	if resp.Entries[0].Stalled {
		t.Errorf("fresh writing entry must not be stalled")
	}
	_ = e.Close()
}

// TestBuildSpoolStatusStalledWriting is item 2a: a writing entry quiescent
// beyond the sweeper's escalation window is reported stalled (per entry +
// the stalled_files aggregate). A freshly-written entry is not.
func TestBuildSpoolStatusStalledWriting(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	// Default escalation window is 10 min; keep it.

	stuck, err := s.OpenWrite("/edit/stuck.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := stuck.WriteAt(make([]byte, 64), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Simulate a leaked handle that went quiescent 11 minutes ago. Same
	// package, so we can rewind the atomic directly (no API for this on
	// purpose — production never rewinds clocks).
	stuck.lastWrite.Store(time.Now().Add(-11 * time.Minute).UnixNano())

	fresh, err := s.OpenWrite("/edit/fresh.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := fresh.WriteAt(make([]byte, 64), 0); err != nil {
		t.Fatalf("write: %v", err)
	}

	resp, err := BuildSpoolStatus(s, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if resp.StalledFiles != 1 {
		t.Errorf("StalledFiles=%d, want 1", resp.StalledFiles)
	}
	byPath := map[string]SpoolEntryView{}
	for _, v := range resp.Entries {
		byPath[v.Path] = v
	}
	if !byPath["/edit/stuck.mov"].Stalled {
		t.Errorf("stuck entry not reported stalled")
	}
	if byPath["/edit/fresh.mov"].Stalled {
		t.Errorf("fresh entry wrongly reported stalled")
	}
	_ = fresh.Close()
}

// TestBuildSpoolStatusStalledAttemptsExhausted: a ready row whose
// drain_attempts hit the drainer's retry ceiling is stalled — it will
// only fail-permanent on its next claim, so the UI should already offer
// recovery.
func TestBuildSpoolStatusStalledAttemptsExhausted(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	d, err := NewDrainer(s, DrainerConfig{FuseRoot: t.TempDir()}) // MaxAttempts default 5
	if err != nil {
		t.Fatalf("new drainer: %v", err)
	}

	e, err := s.OpenWrite("/edit/exhausted.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := e.WriteAt(make([]byte, 32), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := s.Meta().IncrementAttempts(e.ID(), "copy to fuse: connection refused"); err != nil {
			t.Fatalf("incr attempts: %v", err)
		}
	}

	resp, err := BuildSpoolStatus(s, d)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Fatalf("entries=%d, want 1", len(resp.Entries))
	}
	if !resp.Entries[0].Stalled {
		t.Errorf("attempts-exhausted ready entry not reported stalled")
	}
	if resp.StalledFiles != 1 {
		t.Errorf("StalledFiles=%d, want 1", resp.StalledFiles)
	}
	if resp.Entries[0].LastError == "" {
		t.Errorf("LastError empty, want the drain error surfaced")
	}
}

// TestBuildSpoolStatusFailedRelevance is item 3b: failed rows are listed
// only while relevant — their spool file still exists (recoverable via
// retry-failed) or they failed recently (error feedback). Ancient failed
// rows with no data are history, not UI rows. failed_files counts the
// LISTED failed rows.
func TestBuildSpoolStatusFailedRelevance(t *testing.T) {
	s, db := newTestSpoolStoreWithDB(t, 0)

	mkFailed := func(path string) *SpoolEntry {
		t.Helper()
		e, err := s.OpenWrite(path)
		if err != nil {
			t.Fatalf("open write %s: %v", path, err)
		}
		if _, err := e.WriteAt(make([]byte, 16), 0); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		if err := e.Close(); err != nil {
			t.Fatalf("close %s: %v", path, err)
		}
		if _, err := s.Meta().MarkFailed(e.ID(), "drain failed: test"); err != nil {
			t.Fatalf("mark failed %s: %v", path, err)
		}
		return e
	}

	// A: failed long ago but spool file still on disk → relevant (actionable).
	a := mkFailed("/edit/failed-with-data.mov")
	backdateRow(t, db, a.ID(), 48*time.Hour)

	// B: failed long ago, file gone → history, not listed.
	b := mkFailed("/edit/failed-ancient.mov")
	if err := os.Remove(b.SpoolFilePath()); err != nil {
		t.Fatalf("remove spool file: %v", err)
	}
	backdateRow(t, db, b.ID(), 48*time.Hour)

	// C: failed seconds ago, file gone → still listed (recent error feedback).
	c := mkFailed("/edit/failed-recent.mov")
	if err := os.Remove(c.SpoolFilePath()); err != nil {
		t.Fatalf("remove spool file: %v", err)
	}

	resp, err := BuildSpoolStatus(s, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	paths := map[string]bool{}
	for _, v := range resp.Entries {
		paths[v.Path] = true
	}
	if !paths["/edit/failed-with-data.mov"] {
		t.Errorf("failed row with recoverable data not listed")
	}
	if paths["/edit/failed-ancient.mov"] {
		t.Errorf("ancient failed row with no data should not be listed")
	}
	if !paths["/edit/failed-recent.mov"] {
		t.Errorf("recent failed row not listed")
	}
	if resp.FailedFiles != 2 {
		t.Errorf("FailedFiles=%d, want 2 (the listed failed rows)", resp.FailedFiles)
	}
	if resp.PendingFiles != 0 {
		t.Errorf("PendingFiles=%d, want 0 (failed rows are not pending)", resp.PendingFiles)
	}
}

// TestBuildSpoolStatusDoneTail: done rows appear only as a short
// recently-finished tail (sorted after active rows), never as all-time
// history — the Phase-0 observation was 46 historical rows listed with
// pending=0.
func TestBuildSpoolStatusDoneTail(t *testing.T) {
	s, db := newTestSpoolStoreWithDB(t, 0)

	mkDone := func(path string) *SpoolEntry {
		t.Helper()
		e, err := s.OpenWrite(path)
		if err != nil {
			t.Fatalf("open write %s: %v", path, err)
		}
		if _, err := e.WriteAt(make([]byte, 16), 0); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		if err := e.Close(); err != nil {
			t.Fatalf("close %s: %v", path, err)
		}
		if _, err := s.Meta().MarkDone(e.ID()); err != nil {
			t.Fatalf("mark done %s: %v", path, err)
		}
		return e
	}

	recent := mkDone("/edit/just-done.mov")
	_ = recent
	old := mkDone("/edit/old-done.mov")
	backdateRow(t, db, old.ID(), 10*time.Minute)

	// One queued row so we can assert ordering (active first, done tail last).
	q, err := s.OpenWrite("/edit/queued.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := q.WriteAt(make([]byte, 16), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	resp, err := BuildSpoolStatus(s, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(resp.Entries) != 2 {
		t.Fatalf("entries=%d, want 2 (queued + recently-done), got %+v", len(resp.Entries), resp.Entries)
	}
	if resp.Entries[0].Path != "/edit/queued.mov" {
		t.Errorf("Entries[0]=%q, want the ACTIVE row first", resp.Entries[0].Path)
	}
	if resp.Entries[1].Path != "/edit/just-done.mov" || resp.Entries[1].DrainState != "done" {
		t.Errorf("Entries[1]=%+v, want the recently-done tail", resp.Entries[1])
	}
}

// TestBuildSpoolStatusAges: entries carry age_sec (now - updated_at) and
// the aggregate carries oldest_pending_age_sec so the UI can render
// "queued · 2h" without timestamp math.
func TestBuildSpoolStatusAges(t *testing.T) {
	s, db := newTestSpoolStoreWithDB(t, 0)

	e, err := s.OpenWrite("/edit/aged.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := e.WriteAt(make([]byte, 16), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	backdateRow(t, db, e.ID(), 90*time.Second)

	resp, err := BuildSpoolStatus(s, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Fatalf("entries=%d, want 1", len(resp.Entries))
	}
	if got := resp.Entries[0].AgeSec; got < 90 || got > 90+30 {
		t.Errorf("AgeSec=%d, want ~90", got)
	}
	if got := resp.OldestPendingAgeSec; got < 90 || got > 90+30 {
		t.Errorf("OldestPendingAgeSec=%d, want ~90", got)
	}
}

// TestRetryFailedRequeuesRowsWithData is item 2c (retry-failed action):
// failed rows whose spool file survives go back to ready with a FRESH
// attempt budget (otherwise the drainer's next claim re-fails them on
// the exhausted-budget check) and the drainer is woken. Failed rows
// with no data are skipped — nothing to retry, never delete user bytes.
func TestRetryFailedRequeuesRowsWithData(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	mkFailedExhausted := func(path string) *SpoolEntry {
		t.Helper()
		e, err := s.OpenWrite(path)
		if err != nil {
			t.Fatalf("open write %s: %v", path, err)
		}
		if _, err := e.WriteAt(make([]byte, 64), 0); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		if err := e.Close(); err != nil {
			t.Fatalf("close %s: %v", path, err)
		}
		for i := 0; i < 5; i++ {
			if err := s.Meta().IncrementAttempts(e.ID(), "copy to fuse: connection refused"); err != nil {
				t.Fatalf("incr attempts: %v", err)
			}
		}
		if _, err := s.Meta().MarkFailed(e.ID(), "retry budget exhausted (5 attempts)"); err != nil {
			t.Fatalf("mark failed %s: %v", path, err)
		}
		return e
	}

	withData := mkFailedExhausted("/edit/retryable.mov")
	noData := mkFailedExhausted("/edit/gone.mov")
	if err := os.Remove(noData.SpoolFilePath()); err != nil {
		t.Fatalf("remove spool file: %v", err)
	}

	woken := false
	s.SetDrainerWake(func() { woken = true })

	n, err := s.RetryFailed()
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if n != 1 {
		t.Errorf("RetryFailed=%d, want 1 (only the row with data)", n)
	}
	if !woken {
		t.Errorf("drainer not woken after requeue")
	}

	row, err := s.Meta().Get(withData.ID())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(row.DrainState) != "ready" {
		t.Errorf("retried row state=%q, want ready", row.DrainState)
	}
	if row.DrainAttempts != 0 {
		t.Errorf("retried row attempts=%d, want 0 (fresh budget)", row.DrainAttempts)
	}

	goneRow, err := s.Meta().Get(noData.ID())
	if err != nil {
		t.Fatalf("get gone: %v", err)
	}
	if string(goneRow.DrainState) != "failed" {
		t.Errorf("no-data row state=%q, want failed (untouched)", goneRow.DrainState)
	}
}

// TestRecoverStalledForceFinalizesLeakedEntry is item 2c (clear-stalled
// action): a writing entry with a leaked handle quiescent beyond the
// escalation window is force-finalized NOW via the Phase-1 escalation
// path — bytes preserved, row goes ready for the drainer. A fresh
// active writer is untouched.
func TestRecoverStalledForceFinalizesLeakedEntry(t *testing.T) {
	s := newTestSpoolStore(t, 0) // default 10-min escalation window

	stuck, err := s.OpenWrite("/edit/leaked.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := stuck.WriteAt(make([]byte, 128), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Leaked handle (refcount stays 1), quiescent 11 minutes.
	stuck.lastWrite.Store(time.Now().Add(-11 * time.Minute).UnixNano())

	fresh, err := s.OpenWrite("/edit/active.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := fresh.WriteAt(make([]byte, 16), 0); err != nil {
		t.Fatalf("write: %v", err)
	}

	if n := s.RecoverStalled(); n != 1 {
		t.Errorf("RecoverStalled=%d, want 1", n)
	}

	row, err := s.Meta().Get(stuck.ID())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(row.DrainState) != "ready" {
		t.Errorf("recovered row state=%q, want ready (bytes preserved → drain)", row.DrainState)
	}
	if row.Size != 128 {
		t.Errorf("recovered row size=%d, want 128", row.Size)
	}
	freshRow, err := s.Meta().Get(fresh.ID())
	if err != nil {
		t.Fatalf("get fresh: %v", err)
	}
	if string(freshRow.DrainState) != "writing" {
		t.Errorf("fresh row state=%q, want writing (untouched)", freshRow.DrainState)
	}
	_ = fresh.Close()
}

// TestFailPermanentReleasesCapacity: terminal rows must leave the
// capacity budget (QuarantineDrain and the boot scrubber already follow
// that invariant; RetryFailed re-reserves on requeue). Before Phase 2 a
// failed row pinned its bytes in `used` until restart.
func TestFailPermanentReleasesCapacity(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	d, err := NewDrainer(s, DrainerConfig{FuseRoot: t.TempDir()}) // MaxAttempts default 5
	if err != nil {
		t.Fatalf("new drainer: %v", err)
	}

	e, err := s.OpenWrite("/edit/doomed.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := e.WriteAt(make([]byte, 256), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if used, _ := s.Capacity(); used != 256 {
		t.Fatalf("pre: used=%d, want 256", used)
	}
	for i := 0; i < 5; i++ {
		if err := s.Meta().IncrementAttempts(e.ID(), "copy to fuse: connection refused"); err != nil {
			t.Fatalf("incr attempts: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.DrainOnceForTest(ctx) // exhausted budget → failPermanent

	row, err := s.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(row.DrainState) != "failed" {
		t.Fatalf("row state=%q, want failed", row.DrainState)
	}
	if used, _ := s.Capacity(); used != 0 {
		t.Errorf("used=%d after failPermanent, want 0 (reservation released)", used)
	}
}

// TestErrSpoolFullWrapsENOSPC pins the item-4 contract: the spool-full
// sentinel must carry syscall.ENOSPC so internal/nfs (which cannot
// import this package) maps it to NFS3ERR_NOSPC via errors.Is.
func TestErrSpoolFullWrapsENOSPC(t *testing.T) {
	if !errors.Is(ErrSpoolFull, syscall.ENOSPC) {
		t.Fatalf("ErrSpoolFull must wrap syscall.ENOSPC for the NFS3ERR_NOSPC mapping")
	}
	// And the error actually returned by a capacity-capped write is the
	// sentinel (identity), so both errors.Is checks hold downstream.
	s := newTestSpoolStore(t, 100)
	e, err := s.OpenWrite("/edit/full.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	defer func() { _ = e.Close() }()
	if _, err := e.WriteAt(make([]byte, 200), 0); !errors.Is(err, ErrSpoolFull) || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("over-cap WriteAt err=%v, want ErrSpoolFull (wrapping ENOSPC)", err)
	}
}
