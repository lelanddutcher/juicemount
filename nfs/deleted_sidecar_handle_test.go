package nfs

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
)

func TestFromHandleKeepsDeletedAppleDoubleHandleWithoutRelisting(t *testing.T) {
	store, err := metadata.OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	h := NewHandler(store, t.TempDir())
	defer h.StopHandler()

	const inode = uint64(0xabc123)
	const sidecarPath = "tree/._tree"
	entry := metadata.MakeEntry(sidecarPath, false, 4096, time.Now(), inode)
	store.InsertToCache(entry)
	store.DeleteFromCache(sidecarPath)

	handle := make([]byte, 8)
	binary.BigEndian.PutUint64(handle, inode)
	fs, parts, err := h.FromHandle(handle)
	if err != nil {
		t.Fatalf("FromHandle deleted sidecar: %v", err)
	}
	if _, ok := fs.(*deletedSidecarHandleFS); !ok {
		t.Fatalf("filesystem type = %T, want *deletedSidecarHandleFS", fs)
	}
	fullPath := fs.Join(parts...)
	info, err := fs.Lstat(fullPath)
	if err != nil {
		t.Fatalf("Lstat tombstone: %v", err)
	}
	if info.Name() != "._tree" || info.Size() != 4096 {
		t.Fatalf("tombstone info = %q size=%d", info.Name(), info.Size())
	}
	if err := h.Change(fs).Chmod(fullPath, 0600); err != nil {
		t.Fatalf("late Chmod: %v", err)
	}
	if got := store.LookupByPath(sidecarPath); got != nil {
		t.Fatalf("deleted sidecar was relisted in live mirror: %+v", got)
	}
}

func TestFromHandleDeletedPrincipalStillReturnsStale(t *testing.T) {
	store, err := metadata.OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	h := NewHandler(store, t.TempDir())
	defer h.StopHandler()

	const inode = uint64(0xabc124)
	entry := metadata.MakeEntry("tree/clip.mov", false, 1024, time.Now(), inode)
	store.InsertToCache(entry)
	store.DeleteFromCache(entry.Path)

	handle := make([]byte, 8)
	binary.BigEndian.PutUint64(handle, inode)
	_, _, err = h.FromHandle(handle)
	if err == nil {
		t.Fatal("FromHandle deleted principal succeeded, want STALE")
	}
	var statusErr *nfslib.NFSStatusError
	if !errors.As(err, &statusErr) || statusErr.NFSStatus != nfslib.NFSStatusStale {
		t.Fatalf("error = %v, want NFSStatusStale", err)
	}
	if _, statErr := os.Lstat(h.fusePath + "/tree/clip.mov"); !os.IsNotExist(statErr) {
		t.Fatalf("principal unexpectedly exists: %v", statErr)
	}
}
