package nfs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// TestH8aMirrorVisibility checks the attributes the NFS serving layer exposes
// while a replacement's attributes are unavailable, and after correction.
func TestH8aMirrorVisibility(t *testing.T) {
	fuseRoot := t.TempDir()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	h := NewHandler(store, fuseRoot)
	t.Cleanup(func() {
		h.StopHandler()
		_ = store.Close()
	})
	jfs := &juiceFS{handler: h}

	const name = "replacement.mov"
	full := filepath.Join(fuseRoot, name)
	knownGood := metadata.MakeEntry(name, false, 7, time.Now(), 41001)
	if err := os.WriteFile(full, []byte("olddata"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(knownGood); err != nil {
		t.Fatalf("insert known-good row: %v", err)
	}

	// The backing inode has been replaced, but its attrs were unreadable to
	// reconciliation, so its last complete mirror row remains authoritative.
	replacement := []byte("replacement bytes")
	if err := os.WriteFile(full, replacement, 0o644); err != nil {
		t.Fatal(err)
	}
	assertVisible := func(label string, wantSize int64, wantInode uint64) {
		t.Helper()
		for _, stat := range []struct {
			name string
			fn   func(string) (os.FileInfo, error)
		}{{"Stat", jfs.Stat}, {"Lstat", jfs.Lstat}} {
			info, err := stat.fn(name)
			if err != nil {
				t.Fatalf("%s %s: %v", label, stat.name, err)
			}
			if info.Size() != wantSize {
				t.Errorf("%s %s size = %d, want %d", label, stat.name, info.Size(), wantSize)
			}
			if st, ok := info.Sys().(*syscall.Stat_t); !ok || uint64(st.Ino) != wantInode {
				t.Errorf("%s %s inode = %v, want %d", label, stat.name, info.Sys(), wantInode)
			}
		}
		entry := store.LookupByPath(name)
		if entry == nil || entry.Inode != wantInode || entry.Size != wantSize {
			t.Fatalf("%s mirror entry = %+v, want size=%d inode=%d", label, entry, wantSize, wantInode)
		}
		f, err := jfs.OpenFile(name, os.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("%s read-open: %v", label, err)
		}
		cf, ok := f.(*cachedFile)
		if !ok {
			_ = f.Close()
			t.Fatalf("%s read-open handle = %T, want *cachedFile", label, f)
		}
		if cf.fileSize != wantSize || cf.inode != wantInode {
			t.Errorf("%s read-open snapshot = (size=%d inode=%d), want (size=%d inode=%d)", label, cf.fileSize, cf.inode, wantSize, wantInode)
		}
		_ = f.Close()
	}
	assertVisible("unreadable replacement", 7, 41001)

	zero := metadata.MakeEntry("empty.dat", false, 0, time.Now(), 41002)
	if err := store.Insert(zero); err != nil {
		t.Fatalf("insert genuine zero row: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fuseRoot, "empty.dat"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := jfs.Stat("empty.dat")
	if err != nil || info.Size() != 0 {
		t.Fatalf("genuine zero Stat = (%v, %v), want size 0", info, err)
	}
	f, err := jfs.OpenFile("empty.dat", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("genuine zero read-open: %v", err)
	}
	zeroFile, ok := f.(*cachedFile)
	_ = f.Close()
	if !ok || zeroFile.fileSize != 0 || zeroFile.inode != 41002 {
		t.Fatalf("genuine zero read-open handle = %T; want cached size=0 inode=41002", f)
	}

	corrected := metadata.MakeEntry(name, false, int64(len(replacement)), time.Now(), 41003)
	if err := store.Insert(corrected); err != nil {
		t.Fatalf("durable correction: %v", err)
	}
	assertVisible("corrected replacement", int64(len(replacement)), 41003)
}
