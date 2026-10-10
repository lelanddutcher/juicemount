package nfs

import (
	"crypto/rand"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
)

func parentObservationFixture(t *testing.T) (*metadata.Store, *JuiceMountHandler, *juiceFS) {
	t.Helper()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	h := &JuiceMountHandler{store: store, export: newExportIdentity(rand.Reader)}
	return store, h, &juiceFS{handler: h}
}

func observedParentFileid(t *testing.T, jfs *juiceFS, name string) uint64 {
	t.Helper()
	info, err := jfs.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	attr := nfslib.ToFileAttribute(info, name)
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Ino != attr.Fileid {
		t.Fatalf("unproved parent file ID for %q: info=%T attr=%d", name, info, attr.Fileid)
	}
	return attr.Fileid
}

func TestRenameParentRealSyntheticPromotionBeforeAndAfterObservation(t *testing.T) {
	for _, cacheOnlyAtIssuance := range []bool{false, true} {
		t.Run(map[bool]string{false: "versioned-at-issue", true: "cache-only-at-issue"}[cacheOnlyAtIssuance], func(t *testing.T) {
			store, h, jfs := parentObservationFixture(t)
			const synthetic = uint64(1<<63 | 87)
			old := metadata.MakeEntry("parent", true, 0, time.Now(), synthetic)
			if cacheOnlyAtIssuance {
				store.InsertToCache(old)
			} else if err := store.Insert(old); err != nil {
				t.Fatal(err)
			}
			handle := h.ToHandle(jfs, []string{"parent"})
			if len(handle) == 0 {
				t.Fatal("ToHandle failed")
			}
			decoded, err := h.export.inode(handle)
			if err != nil || decoded != synthetic {
				t.Fatalf("issued handle inode=%d err=%v", decoded, err)
			}
			before := observedParentFileid(t, jfs, "parent")
			if before != synthetic {
				t.Fatalf("initial observed inode=%d", before)
			}
			if cacheOnlyAtIssuance {
				if err := store.Insert(old); err != nil {
					t.Fatal(err)
				}
			}
			// A real-inode publication changes the mirror incarnation in this
			// Store. The issued synthetic handle is redirected to the new owner.
			real := metadata.MakeEntry("parent", true, 0, time.Now(), 88)
			if err := store.Insert(real); err != nil {
				t.Fatal(err)
			}
			fromHandle, err := h.export.inode(h.ToHandle(jfs, nil))
			if err != nil {
				t.Fatal(err)
			}
			stale, err := store.RenameParentsMatchObservation(
				metadata.RenameParentObservation{Path: ".", HandleInode: fromHandle, ObservedFileid: 1},
				metadata.RenameParentObservation{Path: "parent", HandleInode: decoded, ObservedFileid: before},
			)
			if err != nil || stale {
				t.Fatalf("promotion after observation accepted: matched=%v err=%v", stale, err)
			}
			if _, path, err := h.FromHandle(handle); err != nil || len(path) != 1 || path[0] != "parent" {
				t.Fatalf("promoted handle did not resolve: path=%v err=%v", path, err)
			}
			after := observedParentFileid(t, jfs, "parent")
			if after != 88 {
				t.Fatalf("promoted observed inode=%d", after)
			}
			current, err := store.RenameParentsMatchObservation(
				metadata.RenameParentObservation{Path: ".", HandleInode: fromHandle, ObservedFileid: 1},
				metadata.RenameParentObservation{Path: "parent", HandleInode: decoded, ObservedFileid: after},
			)
			if err != nil || !current {
				t.Fatalf("promotion before observation refused: matched=%v err=%v", current, err)
			}
		})
	}
}

func TestRenameParentObservationRejectsMovedDeletedAndUnknownOwners(t *testing.T) {
	store, h, jfs := parentObservationFixture(t)
	parent := metadata.MakeEntry("parent", true, 0, time.Now(), 91)
	if err := store.Insert(parent); err != nil {
		t.Fatal(err)
	}
	handle := h.ToHandle(jfs, []string{"parent"})
	inode, err := h.export.inode(handle)
	if err != nil {
		t.Fatal(err)
	}
	observed := observedParentFileid(t, jfs, "parent")
	check := func() bool {
		t.Helper()
		ok, err := store.RenameParentsMatchObservation(metadata.RenameParentObservation{Path: "parent", HandleInode: inode, ObservedFileid: observed})
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !check() {
		t.Fatal("current owner refused")
	}
	moved := metadata.MakeEntry("retired", true, 0, time.Now(), inode)
	if err := store.Insert(moved); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(metadata.MakeEntry("parent", true, 0, time.Now(), 92)); err != nil {
		t.Fatal(err)
	}
	if check() {
		t.Fatal("moved original handle accepted on replacement path")
	}
	if err := store.Delete("parent"); err != nil {
		t.Fatal(err)
	}
	if check() {
		t.Fatal("deleted path accepted")
	}
	const synthetic = uint64(1<<63 | 93)
	unknown := metadata.MakeEntry("unknown", true, 0, time.Now(), synthetic)
	store.InsertToCache(unknown)
	issued := h.ToHandle(jfs, []string{"unknown"})
	if len(issued) == 0 {
		t.Fatal("synthetic ToHandle failed")
	}
	store.DeleteFromCache("unknown")
	ok, err := store.RenameParentsMatchObservation(metadata.RenameParentObservation{Path: "unknown", HandleInode: synthetic, ObservedFileid: synthetic})
	if err != nil || ok {
		t.Fatalf("path-only synthetic alias accepted: matched=%v err=%v", ok, err)
	}
}

func TestRenameParentObservationRejectsWrongExportAndRootMismatch(t *testing.T) {
	store, h, jfs := parentObservationFixture(t)
	if err := store.Insert(metadata.MakeEntry("parent", true, 0, time.Now(), 101)); err != nil {
		t.Fatal(err)
	}
	rootHandle := h.ToHandle(jfs, nil)
	parentHandle := h.ToHandle(jfs, []string{"parent"})
	from := nfslib.ParentRenameObservation{Handle: rootHandle, Fileid: 1, Proven: true}
	to := nfslib.ParentRenameObservation{Handle: parentHandle, Fileid: 101, Proven: true}
	other := &JuiceMountHandler{store: store, export: newExportIdentity(rand.Reader)}
	if err := (&juiceFS{handler: other}).RenameWithParentObservations("src", "parent/dst", from, to); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("wrong export not stale: %v", err)
	}
	from.Fileid = 2
	if err := jfs.RenameWithParentObservations("src", "parent/dst", from, to); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("root ID mismatch not retryable: %v", err)
	}
	from.Fileid, from.Proven = 1, false
	if err := jfs.RenameWithParentObservations("src", "parent/dst", from, to); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("unproved parent ID not retryable: %v", err)
	}
	// The valid observation remains rooted in a real filesystem inode.
	if observedParentFileid(t, jfs, ".") != 1 || observedParentFileid(t, jfs, "parent") != 101 {
		t.Fatal("fixture parent observations changed")
	}
}
