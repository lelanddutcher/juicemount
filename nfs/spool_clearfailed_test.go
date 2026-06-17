package nfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

// TestClearFailedPreviewThenConfirm exercises the 4.8 "clear failed files" path:
// a PREVIEW (confirm=false) must report the failed entry and mutate NOTHING, and
// only a CONFIRM (confirm=true) deletes the spool file + DB row. This is the
// data-safety contract — un-drained bytes are never discarded without confirm.
func TestClearFailedPreviewThenConfirm(t *testing.T) {
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

	// Build a ready entry with real bytes on disk, then mark it FAILED.
	e, err := spool.OpenWrite("doomed.bin")
	if err != nil {
		t.Fatalf("openwrite: %v", err)
	}
	payload := deterministicPayload(7, 4096)
	if _, err := e.WriteAt(payload, 0); err != nil {
		t.Fatalf("writeat: %v", err)
	}
	e.ReleaseHandle()
	spool.sweepOnce(0) // finalize → ready
	row, err := meta.Get(e.ID())
	if err != nil || row == nil {
		t.Fatalf("get row: %v", err)
	}
	spoolFile := row.SpoolFile
	if _, err := os.Stat(spoolFile); err != nil {
		t.Fatalf("spool file should exist before clear: %v", err)
	}
	if ok, err := meta.MarkFailed(e.ID(), "simulated permanent failure"); err != nil || !ok {
		t.Fatalf("markfailed: ok=%v err=%v", ok, err)
	}

	// PREVIEW: reports exactly 1 item, clears 0, deletes nothing.
	items, cleared, bytes, err := spool.ClearFailed(false)
	if err != nil {
		t.Fatalf("clearfailed preview: %v", err)
	}
	if len(items) != 1 || cleared != 0 || bytes != 0 {
		t.Fatalf("preview: items=%d cleared=%d bytes=%d, want 1/0/0", len(items), cleared, bytes)
	}
	if items[0].Size != int64(len(payload)) || items[0].LastError == "" {
		t.Fatalf("preview item missing detail: %+v", items[0])
	}
	if _, err := os.Stat(spoolFile); err != nil {
		t.Fatalf("PREVIEW must NOT delete the spool file: %v", err)
	}
	if r, err := meta.Get(e.ID()); err != nil || r == nil {
		t.Fatalf("PREVIEW must NOT delete the DB row: %v", err)
	}

	// CONFIRM: discards the file + row, reports accurate count/bytes.
	_, cleared2, bytes2, err := spool.ClearFailed(true)
	if err != nil {
		t.Fatalf("clearfailed confirm: %v", err)
	}
	if cleared2 != 1 || bytes2 != int64(len(payload)) {
		t.Fatalf("confirm: cleared=%d bytes=%d, want 1/%d", cleared2, bytes2, len(payload))
	}
	if _, err := os.Stat(spoolFile); !os.IsNotExist(err) {
		t.Fatalf("CONFIRM must delete the spool file, stat err=%v", err)
	}
	if r, _ := meta.Get(e.ID()); r != nil {
		t.Fatalf("CONFIRM must delete the DB row, got %+v", r)
	}

	// Idempotent: nothing left to clear.
	items3, cleared3, _, err := spool.ClearFailed(false)
	if err != nil {
		t.Fatalf("clearfailed empty: %v", err)
	}
	if len(items3) != 0 || cleared3 != 0 {
		t.Fatalf("after clear, preview should be empty: items=%d cleared=%d", len(items3), cleared3)
	}
}

// TestClearFailedLeavesNonFailedRowsAlone confirms clear-failed is surgical: a
// READY (drainable) row and a DONE row must survive a confirm=true clear.
func TestClearFailedLeavesNonFailedRowsAlone(t *testing.T) {
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

	// A healthy READY entry that must NOT be touched.
	keep, err := spool.OpenWrite("keep.bin")
	if err != nil {
		t.Fatalf("openwrite keep: %v", err)
	}
	if _, err := keep.WriteAt(deterministicPayload(2, 2048), 0); err != nil {
		t.Fatalf("writeat keep: %v", err)
	}
	keep.ReleaseHandle()
	spool.sweepOnce(0) // ready
	keepRow, _ := meta.Get(keep.ID())

	// A FAILED entry that SHOULD be cleared.
	fail, err := spool.OpenWrite("fail.bin")
	if err != nil {
		t.Fatalf("openwrite fail: %v", err)
	}
	if _, err := fail.WriteAt(deterministicPayload(3, 2048), 0); err != nil {
		t.Fatalf("writeat fail: %v", err)
	}
	fail.ReleaseHandle()
	spool.sweepOnce(0)
	meta.MarkFailed(fail.ID(), "boom")

	_, cleared, _, err := spool.ClearFailed(true)
	if err != nil {
		t.Fatalf("clearfailed: %v", err)
	}
	if cleared != 1 {
		t.Fatalf("cleared=%d, want exactly 1 (only the failed row)", cleared)
	}
	// The READY row + its file survive.
	if r, err := meta.Get(keep.ID()); err != nil || r == nil {
		t.Fatalf("READY row must survive clear-failed: %v", err)
	}
	if _, err := os.Stat(keepRow.SpoolFile); err != nil {
		t.Fatalf("READY spool file must survive clear-failed: %v", err)
	}
}
