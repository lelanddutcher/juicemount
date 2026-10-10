//go:build h8_acceptance

package nfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// A rejected stale drain must preserve the replacement namespace binding.
// Exercise the real cross-table commit and the real disposal path together:
// checking Done=false alone misses both mirror clobber and backing removal.
func TestH8AcceptanceCancelledBatchPreservesReplacement(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool := metadata.NewSpoolStore(store.DB())
	store.SetSpoolStore(spool)
	const name = "reel.mov"
	stamp := time.Unix(1700000000, 0)
	if err := store.Insert(metadata.MakeEntry(name, false, 4096, stamp, 901)); err != nil {
		t.Fatal(err)
	}
	id, err := spool.Insert(name, filepath.Join(t.TempDir(), "old-spool"))
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.MarkReady(id, 4096, nil); err != nil {
		t.Fatal(err)
	}
	if ok, err := spool.MarkDraining(id); err != nil || !ok {
		t.Fatalf("claim old drain: ok=%v err=%v", ok, err)
	}
	oldRow, err := spool.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	// The completed copy is pending batch disposal. REMOVE cancels its row,
	// and CREATE replaces the pathname before the batch's timer flushes.
	if _, err := spool.DeleteActiveByPath(name); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(name); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(metadata.MakeEntry(name, false, 128, stamp, 902)); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), name)
	replacement := patternedPayload(128, 109)
	if err := os.WriteFile(dest, replacement, 0600); err != nil {
		t.Fatal(err)
	}
	d := &Drainer{onBatchDrainComplete: store.BatchDrainComplete}
	b := &drainBatcher{d: d}
	b.commitAndDispose([]pendingDrain{{row: oldRow, dest: dest, n: oldRow.Size, mtime: stamp}})

	if got := store.LookupByPath(name); got == nil || got.Size != 128 {
		t.Errorf("H8: canceled drain overwrote replacement mirror: %+v", got)
	}
	var size int64
	if err := store.DB().QueryRow(`SELECT size FROM entries WHERE path = ?`, name).Scan(&size); err != nil {
		t.Fatal(err)
	}
	if size != 128 {
		t.Errorf("H8: canceled drain overwrote replacement durable size: got %d, want 128", size)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("H8: canceled drain disposal removed replacement backing file: %v", err)
	}
	if string(got) != string(replacement) {
		t.Fatalf("H8: canceled drain disposal changed replacement backing bytes")
	}
}

// A cancel/replacement after verified copy but before the real inline
// completion transaction must preserve both replacement metadata and bytes.
func TestH8AcceptanceCancelledInlinePreservesReplacement(t *testing.T) {
	t.Setenv("JM_DRAIN_BATCH_INSERT", "0")
	t.Setenv("JM_FUSE_IDENTITY_GATE", "0")
	fuseRoot := t.TempDir()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spoolMeta := metadata.NewSpoolStore(store.DB())
	spool, err := NewSpoolStore(t.TempDir(), 0, spoolMeta)
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: fuseRoot})
	if err != nil {
		t.Fatal(err)
	}
	if d.batch != nil {
		t.Fatal("default inline path unexpectedly enabled batch drain")
	}
	h := NewHandler(store, fuseRoot)
	h.SetSpool(spool, nil)
	defer h.StopHandler()

	const name = "reel.mov"
	old := patternedPayload(4096, 37)
	replacement := patternedPayload(128, 109)
	stamp := time.Unix(1700000000, 0)
	if err := store.Insert(metadata.MakeEntry(name, false, int64(len(old)), stamp, 901)); err != nil {
		t.Fatal(err)
	}
	writeReadySpool(t, spool, name, old)
	dest := filepath.Join(fuseRoot, name)
	var callbackErr error
	callbackRan := false
	d.SetOnDrainCommit(func(item metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
		path, size := item.NFSPath, item.Size
		callbackRan = true
		if path != name || size != int64(len(old)) {
			callbackErr = fmt.Errorf("unexpected drain callback: path=%q size=%d", path, size)
			return metadata.DrainCommitResult{}, callbackErr
		}
		// The destination copy is complete. REMOVE cancels the claimed row,
		// then CREATE binds a new entry and new bytes to the same pathname.
		spool.CancelForDelete(name)
		if err := store.Delete(name); err != nil {
			callbackErr = err
			return metadata.DrainCommitResult{}, callbackErr
		}
		if err := store.Insert(metadata.MakeEntry(name, false, int64(len(replacement)), stamp, 902)); err != nil {
			callbackErr = err
			return metadata.DrainCommitResult{}, callbackErr
		}
		if err := os.WriteFile(dest, replacement, 0600); err != nil {
			callbackErr = err
			return metadata.DrainCommitResult{}, callbackErr
		}
		// Preserve the production transaction, not a test-only row check.
		return store.DrainComplete(item)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n := d.DrainOnceForTest(ctx); n != 1 {
		t.Fatalf("processed %d rows, want 1", n)
	}
	if !callbackRan {
		t.Fatal("inline completion transaction did not run")
	}
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}
	if got := store.LookupByPath(name); got == nil || got.Size != int64(len(replacement)) {
		t.Errorf("H8: canceled inline drain overwrote replacement mirror: %+v", got)
	}
	var size int64
	if err := store.DB().QueryRow(`SELECT size FROM entries WHERE path = ?`, name).Scan(&size); err != nil {
		t.Fatal(err)
	}
	if size != int64(len(replacement)) {
		t.Errorf("H8: canceled inline drain overwrote replacement durable size: got %d, want %d", size, len(replacement))
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("H8: canceled inline drain removed replacement backing file: %v", err)
	}
	if string(got) != string(replacement) {
		t.Fatal("H8: canceled inline drain changed replacement backing bytes")
	}
}

// Rejection containment does not fence a replacement that arrives after a
// successful transaction. Keep this separate acceptance oracle until legacy
// success callbacks carry operation identity end to end.
func TestH8AcceptancePostCommitReplacementPreserved(t *testing.T) {
	jfs, spool, d, root := newBatchWiredHandlerNoDispatch(t, 1)
	const name = "after-commit.mov"
	old := patternedPayload(4096, 37)
	replacement := patternedPayload(128, 109)
	stamp := time.Unix(1700000000, 0)
	if err := jfs.handler.store.Insert(metadata.MakeEntry(name, false, int64(len(old)), stamp, 901)); err != nil {
		t.Fatal(err)
	}
	writeReadySpool(t, spool, name, old)
	spool.sweepOnce(0)
	rows, err := spool.Meta().ListReady(1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	row := rows[0]
	if ok, err := spool.Meta().MarkDraining(row.ID); err != nil || !ok {
		t.Fatalf("claim=%v err=%v", ok, err)
	}
	dest := filepath.Join(root, name)
	if err := os.WriteFile(dest, old, 0600); err != nil {
		t.Fatal(err)
	}
	retained, err := os.OpenFile(dest, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	realCommit := d.onBatchDrainCompleteExact
	replaced := false
	var replacementVersion metadata.MirrorToken
	d.onBatchDrainCompleteExact = func(items []metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error) {
		results, err := realCommit(items)
		if err != nil || len(results) != 1 || !results[0].Done {
			t.Fatalf("real completion=%+v err=%v", results, err)
		}
		spool.CancelForDelete(name)
		if err := jfs.handler.store.Delete(name); err != nil {
			t.Fatal(err)
		}
		entry := metadata.MakeEntry(name, false, int64(len(replacement)), stamp, 902)
		entry.Mode = 0400
		if err := jfs.handler.store.Insert(entry); err != nil {
			t.Fatal(err)
		}
		replacementVersion = jfs.handler.store.LookupByPath(name).Version
		if err := os.Remove(dest); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dest, replacement, 0600); err != nil {
			t.Fatal(err)
		}
		replaced = true
		return results, nil
	}
	d.batch.commitAndDispose([]pendingDrain{{row: row, dest: dest, n: row.Size, mtime: time.Unix(1800000000, 0), fd: retained}})
	if !replaced {
		t.Fatal("postcommit replacement barrier did not run")
	}
	got := jfs.handler.store.LookupByPath(name)
	if got == nil || got.Size != int64(len(replacement)) || got.Version != replacementVersion || !got.Mtime.Equal(stamp) {
		t.Errorf("H8: successful old callback changed replacement mirror: %+v", got)
	}
	var size int64
	if err := jfs.handler.store.DB().QueryRow(`SELECT size FROM entries WHERE path=?`, name).Scan(&size); err != nil {
		t.Fatal(err)
	}
	if size != int64(len(replacement)) {
		t.Errorf("H8: replacement SQL size=%d want %d", size, len(replacement))
	}
	body, err := os.ReadFile(dest)
	if err != nil || string(body) != string(replacement) {
		t.Fatalf("H8: replacement backing changed: err=%v", err)
	}
	if info, err := os.Stat(dest); err != nil || info.Mode().Perm() != 0600 {
		t.Errorf("H8: old callback chmod changed replacement: info=%v err=%v", info, err)
	}
}
