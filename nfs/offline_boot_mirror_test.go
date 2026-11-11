package nfs

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// TestOfflineBootServesSavedMirror pins what an offline start must serve: the
// folder listings and attributes saved by the previous session, read back from
// SQLite after a relaunch, with no backend and no FUSE behind them. The lab
// failure (2026-11-11, after-8) was a cold install with NO saved list, so the
// offline volume was empty; a Mac that has a saved list must never look empty.
func TestOfflineBootServesSavedMirror(t *testing.T) {
	pin.SetOffline(false)
	pin.SetAutoOffline(false, "")
	pin.ResetFUSEIdentityForTest()
	t.Cleanup(func() {
		pin.SetAutoOffline(false, "")
		pin.SetOffline(false)
		pin.ResetFUSEIdentityForTest()
	})

	dbPath := filepath.Join(t.TempDir(), "meta.db")
	mtime := time.Unix(1_700_000_000, 0)

	// Previous session: sync a small tree into the mirror, then quit.
	first, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	if err := first.BulkInsert([]*metadata.Entry{
		{Path: "media", Name: "media", ParentPath: ".", IsDir: true, Inode: 2, Mode: fs.ModeDir | 0o755, Mtime: mtime},
		{Path: "media/a.mov", Name: "a.mov", ParentPath: "media", Size: 4 << 20, Inode: 3, Mode: 0o644, Mtime: mtime},
	}, 100); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Relaunch offline: the boot path engages auto-offline before the first
	// RPC. fuseRoot is an empty plain directory: nothing behind the mirror.
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open (relaunch): %v", err)
	}
	if has, err := store.HasEntries(); err != nil || !has {
		t.Fatalf("HasEntries after relaunch = %v, %v; want true", has, err)
	}
	h := NewHandler(store, t.TempDir())
	t.Cleanup(func() {
		h.StopHandler()
		_ = store.Close()
	})
	jfs := &juiceFS{handler: h}
	pin.SetAutoOffline(true, "Started offline — backend unreachable at launch")

	root, err := jfs.ReadDir(".")
	if err != nil {
		t.Fatalf("offline ReadDir(.): %v", err)
	}
	if len(root) != 1 || root[0].Name() != "media" || !root[0].IsDir() {
		t.Fatalf("offline root listing = %v, want the saved folder media", offlineListingNames(root))
	}
	media, err := jfs.ReadDir("media")
	if err != nil {
		t.Fatalf("offline ReadDir(media): %v", err)
	}
	if len(media) != 1 || media[0].Name() != "a.mov" {
		t.Fatalf("offline media listing = %v, want [a.mov]", offlineListingNames(media))
	}
	info, err := jfs.Lstat("media/a.mov")
	if err != nil {
		t.Fatalf("offline Lstat(media/a.mov): %v (a saved entry must not be ENOENT)", err)
	}
	if info.Size() != 4<<20 || info.IsDir() || !info.ModTime().Equal(mtime) {
		t.Fatalf("offline attrs = size %d dir %v mtime %v; want the saved size %d and mtime %v",
			info.Size(), info.IsDir(), info.ModTime(), 4<<20, mtime)
	}
}

func offlineListingNames(infos []os.FileInfo) []string {
	out := make([]string, 0, len(infos))
	for _, info := range infos {
		out = append(out, info.Name())
	}
	return out
}
