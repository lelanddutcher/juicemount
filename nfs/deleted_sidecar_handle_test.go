package nfs

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
)

func TestDeletedSyntheticAppleDoubleHandleAcceptsLateOfflineWrite(t *testing.T) {
	store, err := metadata.OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	h := NewHandler(store, t.TempDir())
	defer h.StopHandler()
	jfs := &juiceFS{handler: h}

	wasOffline := pin.IsOffline()
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	// Offline-created entries use synthetic inodes. Finder can unlink the
	// AppleDouble name, keep its NFS handle, then send continuation WRITEs on
	// that old handle. The unlink must win in the namespace while the still-open
	// handle retains Unix-style write semantics.
	const inode = uint64(1<<63 | 0xabc126)
	const sidecarPath = "tree/._dir2"
	entry := metadata.MakeEntry(sidecarPath, false, 4096, time.Now(), inode)
	store.InsertToCache(entry)
	handle := h.ToHandle(jfs, splitPath(sidecarPath))

	if err := jfs.Remove(sidecarPath); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	fs, parts, err := h.FromHandle(handle)
	if err != nil {
		t.Fatalf("FromHandle after unlink: %v", err)
	}
	if _, ok := fs.(*deletedHandleFS); !ok {
		t.Fatalf("filesystem type = %T, want *deletedHandleFS", fs)
	}
	fullPath := fs.Join(parts...)
	f, err := fs.OpenFile(fullPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("late offline OpenFile: %v", err)
	}
	wa, ok := f.(io.WriterAt)
	if !ok {
		t.Fatalf("late-write file %T does not implement io.WriterAt", f)
	}
	if n, err := wa.WriteAt([]byte("finder-metadata"), 152); err != nil || n != len("finder-metadata") {
		t.Fatalf("late WriteAt = %d, %v", n, err)
	}
	if n, err := wa.WriteAt([]byte("cleanup"), 0); err != nil || n != len("cleanup") {
		t.Fatalf("cleanup WriteAt = %d, %v", n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("late Close: %v", err)
	}
	if got := store.LookupByPath(sidecarPath); got != nil {
		t.Fatalf("late write resurrected deleted sidecar: %+v", got)
	}
	if _, err := os.Lstat(h.fusePath + "/" + sidecarPath); !os.IsNotExist(err) {
		t.Fatalf("late write recreated deleted sidecar: %v", err)
	}
}

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
	if _, ok := fs.(*deletedHandleFS); !ok {
		t.Fatalf("filesystem type = %T, want *deletedHandleFS", fs)
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

func TestFromHandleKeepsDeletedDirectoryHandleWithoutRelisting(t *testing.T) {
	store, err := metadata.OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	h := NewHandler(store, t.TempDir())
	defer h.StopHandler()

	const inode = uint64(0xabc125)
	const dirPath = "tree/finished-copy"
	entry := metadata.MakeEntry(dirPath, true, 0, time.Now(), inode)
	store.InsertToCache(entry)
	store.DeleteFromCache(dirPath)

	handle := make([]byte, 8)
	binary.BigEndian.PutUint64(handle, inode)
	fs, parts, err := h.FromHandle(handle)
	if err != nil {
		t.Fatalf("FromHandle deleted directory: %v", err)
	}
	if _, ok := fs.(*deletedHandleFS); !ok {
		t.Fatalf("filesystem type = %T, want *deletedHandleFS", fs)
	}
	fullPath := fs.Join(parts...)
	info, err := fs.Lstat(fullPath)
	if err != nil {
		t.Fatalf("Lstat directory tombstone: %v", err)
	}
	if !info.IsDir() || info.Name() != "finished-copy" {
		t.Fatalf("tombstone info = %q dir=%v, want finished-copy dir", info.Name(), info.IsDir())
	}
	if got := store.LookupByPath(dirPath); got != nil {
		t.Fatalf("deleted directory was relisted in live mirror: %+v", got)
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
