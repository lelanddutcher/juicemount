package metadata

import (
	"database/sql"
	"errors"
	"hash/fnv"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// openStoreWithSpool opens a real entries Store and initializes the spool
// schema on the SAME DB, returning both the Store and a spool CRUD handle —
// the production wiring (bridge/cbridge.go: NewSpoolStore(store.DB())). Needed
// because BatchDrainComplete writes BOTH the entries table (size publish) and
// spool_entries (mark-done) in one cross-table transaction.
func openStoreWithSpool(t *testing.T) (*Store, *SpoolStore) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := InitSpoolSchema(store.DB()); err != nil {
		t.Fatalf("init spool schema: %v", err)
	}
	spool := NewSpoolStore(store.DB())
	// Mirror the production wiring (handler.SetSpool): BatchDrainComplete marks
	// spool_entries done under SpoolStore.writeMu (QA-37), so the store needs the
	// sibling SpoolStore reference or it fails closed.
	store.SetSpoolStore(spool)
	return store, spool
}

// seedReadyRow inserts an entries row (at Create-time size 0, the state a fresh
// spool-pending file is in) plus a matching ready→draining spool row, and
// returns the spool id. Mirrors the drain lifecycle at the point drainOne is
// about to publish the real size + mark done.
func seedReadyRow(t *testing.T, store *Store, spool *SpoolStore, nfsPath string, size int64) int64 {
	t.Helper()
	// Create-time entry: size 0 in cache + SQLite, exactly like a spooled file
	// before its drain publishes the real size (task #65 pre-condition). Inode
	// is derived per-path so two seeds don't collide on the inode cache (which
	// would evict the first path's entry via evictInodeOrphanLocked).
	h := fnv.New64a()
	_, _ = h.Write([]byte(nfsPath))
	e := MakeEntry(nfsPath, false, 0, time.Now(), h.Sum64())
	store.InsertToCache(e)
	if err := store.Insert(e); err != nil {
		t.Fatalf("entries insert %q: %v", nfsPath, err)
	}
	id, err := spool.Insert(nfsPath, "/spool/files/"+filepath.Base(nfsPath))
	if err != nil {
		t.Fatalf("spool insert %q: %v", nfsPath, err)
	}
	if err := spool.MarkReady(id, size, nil); err != nil {
		t.Fatalf("mark ready %d: %v", id, err)
	}
	if claimed, err := spool.MarkDraining(id); err != nil || !claimed {
		t.Fatalf("mark draining %d: claimed=%v err=%v", id, claimed, err)
	}
	return id
}

// TestBatchDrainCompletePublishesSizeAndMarksDone is the core correctness test:
// one transaction publishes each entry's authoritative size AND marks its spool
// row done, and a read AFTER the commit resolves the correct size (not the
// Create-time 0) — the task #65 size-publish-before-eviction guarantee, here
// proven at the SQL layer that the eviction cleanup keys off.
func TestBatchDrainCompletePublishesSizeAndMarksDone(t *testing.T) {
	store, spool := openStoreWithSpool(t)

	id1 := seedReadyRow(t, store, spool, "/Films/a.mov", 4096)
	id2 := seedReadyRow(t, store, spool, "/Films/b.mov", 8192)

	now := time.Now()
	results, err := store.BatchDrainComplete([]DrainCommitItem{
		{SpoolID: id1, NFSPath: "/Films/a.mov", SpoolFile: "/spool/files/" + filepath.Base("/Films/a.mov"), Size: 4096, Mtime: now},
		{SpoolID: id2, NFSPath: "/Films/b.mov", SpoolFile: "/spool/files/" + filepath.Base("/Films/b.mov"), Size: 8192, Mtime: now},
	})
	if err != nil {
		t.Fatalf("BatchDrainComplete: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results len=%d, want 2", len(results))
	}
	for _, r := range results {
		if !r.Done {
			t.Errorf("row %d: Done=false, want true", r.SpoolID)
		}
	}

	// Both spool rows must be `done` (the eviction trigger committed).
	for _, id := range []int64{id1, id2} {
		row, gerr := spool.Get(id)
		if gerr != nil {
			t.Fatalf("spool get %d: %v", id, gerr)
		}
		if row.DrainState != DrainDone {
			t.Errorf("row %d state=%q, want done", id, row.DrainState)
		}
	}

	// A read AFTER commit must resolve the published size, never the Create-time
	// 0 (task #65). Check BOTH the in-RAM cache (LookupByPath) and durable
	// SQLite (LookupByPathSQLite would go through serve accessors; use the
	// direct entries read via a fresh Store to be sure it's on disk).
	if e := store.LookupByPath("/Films/a.mov"); e == nil || e.Size != 4096 {
		t.Errorf("a.mov cached size = %v, want 4096", eSize(e))
	}
	if e := store.LookupByPath("/Films/b.mov"); e == nil || e.Size != 8192 {
		t.Errorf("b.mov cached size = %v, want 8192", eSize(e))
	}

	// Durable check: reopen the DB and confirm size persisted (proves the
	// entries UPDATE committed in the same tx as the mark-done, not just the
	// cache bump).
	if got := durableSize(t, store, "/Films/a.mov"); got != 4096 {
		t.Errorf("a.mov durable size = %d, want 4096", got)
	}
	if got := durableSize(t, store, "/Films/b.mov"); got != 8192 {
		t.Errorf("b.mov durable size = %d, want 8192", got)
	}
}

// TestBatchDrainCompleteSizeIsMaxOnly proves the entries size publish is
// MAX-only (idempotent), matching UpdateSize: a smaller batched size can never
// shrink an already-larger persisted size (a concurrent reconcile that already
// learned the real size must win over a stale/partial value).
func TestBatchDrainCompleteSizeIsMaxOnly(t *testing.T) {
	store, spool := openStoreWithSpool(t)

	nfsPath := "/Films/big.mov"
	id := seedReadyRow(t, store, spool, nfsPath, 100)
	// Pretend a reconcile already published the true (larger) size.
	if err := store.UpdateSize(nfsPath, 999999, time.Now()); err != nil {
		t.Fatalf("pre UpdateSize: %v", err)
	}

	if _, err := store.BatchDrainComplete([]DrainCommitItem{
		{SpoolID: id, NFSPath: nfsPath, SpoolFile: "/spool/files/" + filepath.Base(nfsPath), Size: 100, Mtime: time.Now()},
	}); err != nil {
		t.Fatalf("BatchDrainComplete: %v", err)
	}

	if e := store.LookupByPath(nfsPath); e == nil || e.Size != 999999 {
		t.Errorf("size = %v after batched smaller publish, want 999999 (MAX-only)", eSize(e))
	}
	if got := durableSize(t, store, nfsPath); got != 999999 {
		t.Errorf("durable size = %d, want 999999 (MAX-only)", got)
	}
}

// TestBatchDrainCompleteCancelledRowReportsNotDone proves the QA-37 cancel
// contract survives batching: a spool row DELETED out from under the drainer
// (the NFS delete path) is reported Done=false without changing the mirror, while sibling rows in the same batch still complete.
func TestBatchDrainCompleteCancelledRowReportsNotDone(t *testing.T) {
	store, spool := openStoreWithSpool(t)

	id1 := seedReadyRow(t, store, spool, "/Films/keep.mov", 4096)
	id2 := seedReadyRow(t, store, spool, "/Films/deleted.mov", 4096)

	// Cancel id2 out from under the drain (simulate CancelForDelete's DELETE of
	// active rows).
	if _, err := spool.DeleteActiveByPath("/Films/deleted.mov"); err != nil {
		t.Fatalf("delete active: %v", err)
	}

	results, err := store.BatchDrainComplete([]DrainCommitItem{
		{SpoolID: id1, NFSPath: "/Films/keep.mov", SpoolFile: "/spool/files/" + filepath.Base("/Films/keep.mov"), Size: 4096, Mtime: time.Now()},
		{SpoolID: id2, NFSPath: "/Films/deleted.mov", SpoolFile: "/spool/files/" + filepath.Base("/Films/deleted.mov"), Size: 4096, Mtime: time.Now()},
	})
	if err != nil {
		t.Fatalf("BatchDrainComplete: %v", err)
	}

	done := map[int64]bool{}
	for _, r := range results {
		done[r.SpoolID] = r.Done
	}
	if !done[id1] {
		t.Errorf("keep row %d: Done=false, want true", id1)
	}
	if done[id2] {
		t.Errorf("deleted row %d: Done=true, want false (must not resurrect a deleted file)", id2)
	}
}

// TestBatchDrainCompleteConcurrentCancelDoesNotResurrect is the QA-37
// regression test the batch-insert lever was MISSING: it drives the actual
// concurrent interleaving of a cancel (NFS delete → DeleteActiveByPath) against
// a BatchDrainComplete flush, not the trivial delete-before-batch ordering the
// sibling TestBatchDrainCompleteCancelledRowReportsNotDone covers.
//
// THE BUG IT LOCKS DOWN. Before the fix, BatchDrainComplete marked spool_entries
// done under Store.writeMu while DeleteActiveByPath ran under SpoolStore.writeMu
// — DIFFERENT mutexes — so the two could interleave: the cancel's SELECT sees a
// still-'draining' row and decides to delete it, the batch then commits
// drain_state='done' (RowsAffected>0 → Done=true), and the cancel's DELETE (which
// filters writing/ready/draining) now matches 0 rows. Result: Done=true drives
// BatchCompleteDrainCleanup + onDrainComplete → the user-deleted file is
// RESURRECTED as a committed backend/Redis entry.
//
// HOW THE INTERLEAVING IS FORCED (deterministic, not timing-based). A test hook
// fires inside DeleteActiveByPath at the precise "SELECT collected the row,
// DELETE not yet run" point. The cancel goroutine parks there — holding
// SpoolStore.writeMu — and signals. The main goroutine then launches the batch
// flush and confirms it has NOT completed (it is blocked on SpoolStore.writeMu,
// which the fix now makes it share with the cancel). Only then is the cancel's
// DELETE released. If the mutex were NOT shared (the pre-fix bug) the batch would
// sail through and mark the row done while the cancel was parked — exactly the
// resurrection window — and the Done=false assertion below would fail.
//
// INVARIANT ASSERTED. The cancelled row must never be BOTH deleted-away AND
// reported Done=true. Here the cancel wins the row (DELETE removes it), so the
// batch's mark-done must see 0 rows → Done=false, with no publication effects. A surviving (keep) row in the same batch must still commit Done=true,
// proving the batch isn't wholesale-poisoned by one cancel. Run under -race.
func TestBatchDrainCompleteConcurrentCancelDoesNotResurrect(t *testing.T) {
	store, spool := openStoreWithSpool(t)

	keepID := seedReadyRow(t, store, spool, "/Films/keep.mov", 4096)
	delID := seedReadyRow(t, store, spool, "/Films/deleted.mov", 8192)

	// selectDone fires once the cancel's SELECT has collected the draining row
	// (decision-to-delete made). releaseDelete unblocks its DELETE. The cancel
	// holds SpoolStore.writeMu across this whole window.
	selectDone := make(chan struct{})
	releaseDelete := make(chan struct{})
	var hookOnce sync.Once
	deleteActiveByPathSelectHook = func(nfsPath string) {
		if nfsPath != "/Films/deleted.mov" {
			return
		}
		hookOnce.Do(func() { close(selectDone) })
		<-releaseDelete
	}
	t.Cleanup(func() { deleteActiveByPathSelectHook = nil })

	// G1: the cancel. Runs SELECT (collects delID), fires the hook, parks holding
	// SpoolStore.writeMu until releaseDelete, then runs the DELETE.
	var cancelRows []*SpoolRow
	var cancelErr error
	var cancelWG sync.WaitGroup
	cancelWG.Add(1)
	go func() {
		defer cancelWG.Done()
		cancelRows, cancelErr = spool.DeleteActiveByPath("/Films/deleted.mov")
	}()

	// Wait until the cancel is parked mid-operation (SELECT done, DELETE pending,
	// SpoolStore.writeMu held).
	select {
	case <-selectDone:
	case <-time.After(5 * time.Second):
		close(releaseDelete)
		t.Fatal("cancel SELECT hook never fired — DeleteActiveByPath did not collect the draining row")
	}

	// G2: the batch flush, launched WHILE the cancel is parked. With the fix it
	// must block on SpoolStore.writeMu (held by the parked cancel).
	var results []DrainCommitResult
	var batchErr error
	batchDone := make(chan struct{})
	go func() {
		results, batchErr = store.BatchDrainComplete([]DrainCommitItem{
			{SpoolID: keepID, NFSPath: "/Films/keep.mov", SpoolFile: "/spool/files/" + filepath.Base("/Films/keep.mov"), Size: 4096, Mtime: time.Now()},
			{SpoolID: delID, NFSPath: "/Films/deleted.mov", SpoolFile: "/spool/files/" + filepath.Base("/Films/deleted.mov"), Size: 8192, Mtime: time.Now()},
		})
		close(batchDone)
	}()

	// The batch must NOT complete while the cancel holds SpoolStore.writeMu. If it
	// does, the mark-done ran unserialized — the pre-fix race — and would report
	// Done=true for the row the cancel is about to delete (resurrection).
	select {
	case <-batchDone:
		close(releaseDelete)
		t.Fatal("BatchDrainComplete completed while the cancel held SpoolStore.writeMu — mark-done ran unserialized (QA-37 race reopened)")
	case <-time.After(200 * time.Millisecond):
		// Expected: batch is blocked on the shared mutex. Release the cancel.
	}

	close(releaseDelete)
	cancelWG.Wait()
	<-batchDone

	if cancelErr != nil {
		t.Fatalf("cancel DeleteActiveByPath: %v", cancelErr)
	}
	if batchErr != nil {
		t.Fatalf("BatchDrainComplete: %v", batchErr)
	}
	if len(cancelRows) != 1 || cancelRows[0].ID != delID {
		t.Fatalf("cancel removed rows = %+v, want exactly the deleted.mov row (id %d)", cancelRows, delID)
	}

	done := map[int64]bool{}
	for _, r := range results {
		done[r.SpoolID] = r.Done
	}
	// The cancelled row: the DELETE ran after the cancel released the mutex, so
	// the batch's mark-done saw 0 rows → Done=false → no resurrection.
	if done[delID] {
		t.Errorf("deleted row %d: Done=true — the user-deleted file would be resurrected (QA-37)", delID)
	}
	// The surviving row still commits.
	if !done[keepID] {
		t.Errorf("keep row %d: Done=false, want true (a single cancel must not poison the whole batch)", keepID)
	}
	// The cancelled row is truly gone from spool_entries (DELETE won the row).
	if _, err := spool.Get(delID); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("deleted row %d still present in spool_entries (err=%v) — cancel DELETE did not remove it", delID, err)
	}
}

// TestBatchDrainCompleteEmptyIsNoop guards the trivial path.
func TestBatchDrainCompleteEmptyIsNoop(t *testing.T) {
	store, _ := openStoreWithSpool(t)
	results, err := store.BatchDrainComplete(nil)
	if err != nil {
		t.Fatalf("BatchDrainComplete(nil): %v", err)
	}
	if results != nil {
		t.Errorf("results = %v, want nil for empty batch", results)
	}
}

func eSize(e *Entry) any {
	if e == nil {
		return "<nil entry>"
	}
	return e.Size
}

// durableSize reads size straight from the entries table on the store's DB,
// bypassing the in-RAM cache, to prove the batched UPDATE actually committed.
func durableSize(t *testing.T, store *Store, path string) int64 {
	t.Helper()
	var size int64
	err := store.DB().QueryRow(`SELECT size FROM entries WHERE path = ?`, path).Scan(&size)
	if err != nil {
		t.Fatalf("durable size read %q: %v", path, err)
	}
	return size
}

// An exact production completion binds its final EOF and generation to the
// FULL hold. A high-bit inode must round-trip through SQLite's signed integer.
func TestBatchDrainCompleteExactBindsHighBitZeroImage(t *testing.T) {
	store, spool := openStoreWithSpool(t)
	const name = "exact/zero.mov" // FNV fixture inode has the high bit set.
	id := seedReadyRow(t, store, spool, name, 0)
	if before := store.LookupByPath(name); before == nil || before.Inode&(uint64(1)<<63) == 0 {
		t.Fatal("fixture did not create a high-bit inode")
	}
	if err := store.SetSizeExact(name, 8192, time.Now()); err != nil {
		t.Fatal(err)
	}
	item := DrainCommitItem{SpoolID: id, NFSPath: name, SpoolFile: "/spool/files/zero.mov", Size: 0,
		Mtime: time.Unix(1800000000, 0), BackingIdentity: "41:42"}
	results, err := store.BatchDrainCompleteExact([]DrainCommitItem{item})
	if err != nil || len(results) != 1 || !results[0].Done {
		t.Fatalf("exact completion: results=%+v err=%v", results, err)
	}
	r := results[0]
	if got := store.LookupByPath(name); got == nil || got.Size != 0 || got.Version != r.Published || got.Inode != r.Inode {
		t.Fatalf("exact RAM image: %+v receipt=%+v", got, r)
	}
	if got := durableSize(t, store, name); got != 0 {
		t.Fatalf("exact durable EOF=%d", got)
	}
	applied := 0
	if err := store.ApplyLegacyDrainEffects(item, r, func(DrainCommitResult) { applied++ }); err != nil || applied != 1 {
		t.Fatalf("live exact receipt refused: applied=%d err=%v", applied, err)
	}
	forged := r
	forged.BackingIdentity = "99:100"
	if err := store.ApplyLegacyDrainEffects(item, forged, func(DrainCommitResult) { applied++ }); err == nil || applied != 1 {
		t.Fatalf("forged identity applied: applied=%d err=%v", applied, err)
	}
	if err := spool.CleanupDrainCompletion(item, r, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyLegacyDrainEffects(item, r, func(DrainCommitResult) { applied++ }); err == nil || applied != 1 {
		t.Fatalf("retired hold replay applied: applied=%d err=%v", applied, err)
	}
}

func TestDrainCompleteExactRejectsCacheOnlySuccessorBeforeCommit(t *testing.T) {
	store, spool := openStoreWithSpool(t)
	const name = "cache/successor.mov"
	id := seedReadyRow(t, store, spool, name, 4096)
	before := store.LookupByPath(name)
	replacement := MakeEntry(name, false, 17, time.Unix(1900000000, 0), before.Inode+1)
	store.InsertToCache(replacement)
	item := DrainCommitItem{SpoolID: id, NFSPath: name, SpoolFile: "/spool/files/successor.mov", Size: 4096,
		Mtime: time.Unix(1800000000, 0), BackingIdentity: "41:42"}
	if _, err := store.DrainComplete(item); err == nil {
		t.Fatal("cache-only successor was overwritten")
	}
	if got := store.LookupByPath(name); got == nil || got.Inode != replacement.Inode || got.Size != replacement.Size {
		t.Fatalf("cache successor changed: %+v", got)
	}
	if got := durableSize(t, store, name); got != before.Size {
		t.Fatalf("durable predecessor changed: %d", got)
	}
	if row, err := spool.Get(id); err != nil || row.DrainState != DrainDraining {
		t.Fatalf("spool row changed: %+v err=%v", row, err)
	}
}

func TestLegacyDrainReceiptRejectsSameInodeNewRevisionWhileHeld(t *testing.T) {
	store, spool := openStoreWithSpool(t)
	const name = "revision/same-inode.mov"
	id := seedReadyRow(t, store, spool, name, 128)
	item := DrainCommitItem{SpoolID: id, NFSPath: name, SpoolFile: "/spool/files/same-inode.mov",
		Size: 128, Mtime: time.Unix(1800000000, 0), BackingIdentity: "41:42"}
	r, err := store.DrainComplete(item)
	if err != nil || !r.Done {
		t.Fatalf("receipt=%+v err=%v", r, err)
	}
	if _, err := store.DB().Exec(`UPDATE entries SET revision=revision+1 WHERE path=?`, name); err != nil {
		t.Fatal(err)
	}
	var inode int64
	if err := store.DB().QueryRow(`SELECT inode FROM entries WHERE path=?`, name).Scan(&inode); err != nil || uint64(inode) != r.Inode {
		t.Fatalf("same-inode fixture changed: inode=%d err=%v", inode, err)
	}
	applied := false
	if err := store.ApplyLegacyDrainEffects(item, r, func(DrainCommitResult) { applied = true }); err == nil || applied {
		t.Fatalf("same-inode newer revision accepted: applied=%t err=%v", applied, err)
	}
}
