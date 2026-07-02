package metadata

import (
	"hash/fnv"
	"path/filepath"
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
	return store, NewSpoolStore(store.DB())
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
		{SpoolID: id1, NFSPath: "/Films/a.mov", Size: 4096, Mtime: now},
		{SpoolID: id2, NFSPath: "/Films/b.mov", Size: 8192, Mtime: now},
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
		{SpoolID: id, NFSPath: nfsPath, Size: 100, Mtime: time.Now()},
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
// (the NFS delete path) is reported Done=false so the caller undoes the FUSE
// write, while sibling rows in the same batch still complete.
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
		{SpoolID: id1, NFSPath: "/Films/keep.mov", Size: 4096, Mtime: time.Now()},
		{SpoolID: id2, NFSPath: "/Films/deleted.mov", Size: 4096, Mtime: time.Now()},
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
