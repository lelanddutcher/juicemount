package nfs

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestStrongSidecarCanPopulateAfterDurableMirrorReopen(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	const rel = "._clip.mp4"
	body := []byte("complete resource fork")
	full := filepath.Join(root, rel)
	if err := os.WriteFile(full, body, 0600); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(1779992968, 504304000)
	if err := os.Chtimes(full, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(full)
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().Nanosecond() == 0 {
		t.Fatal("fixture lacks fractional mtime")
	}
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := metadata.MakeEntry(rel, false, info.Size(), info.ModTime(), info.Sys().(*syscall.Stat_t).Ino)
	entry.MtimeNsecKnown = true
	if err := store.Insert(entry); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	t.Cleanup(func() { h.StopHandler(); store.Close() })
	jfs := &juiceFS{handler: h}
	f, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(body)+1)
	n, readErr := f.ReadAt(buf, 0)
	f.Close()
	if n != len(body) || !bytes.Equal(buf[:n], body) || (readErr != nil && readErr != io.EOF) {
		t.Fatalf("read=%d,%v", n, readErr)
	}
	if h.strongSidecarRead(rel) == nil {
		mirror := store.LookupByPath(rel)
		t.Fatalf("cold read cannot establish strong cache after reopen: descriptor_mtime=%s mirror_mtime=%s", info.ModTime().Format(time.RFC3339Nano), mirror.Mtime.Format(time.RFC3339Nano))
	}
}
