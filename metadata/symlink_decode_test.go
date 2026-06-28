package metadata

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// These tests cover the SYNC/decode gap the prior symlink tests missed: the
// WRITE path (juiceFS.Symlink) was tested, but every backend-resident symlink
// — and the just-created one after the next reconcile — was decoded as a 0644
// REGULAR file because the JuiceFS dentry filetype byte ft==3 (TypeSymlink)
// was thrown away (`isDir := ft == 2` then mode=0644 unless dir). The fix maps
// ft==3 → os.ModeSymlink at every decode/sync site via modeForFiletype.

// TestModeForFiletype is the direct unit test of the single-source-of-truth
// type→mode mapper. It FAILS against the old ft==3→0644 code (which had no
// symlink case at all): file=1 → regular, dir=2 → ModeDir, symlink=3 →
// ModeSymlink, and any other byte → regular.
func TestModeForFiletype(t *testing.T) {
	cases := []struct {
		name      string
		ft        byte
		wantType  fs.FileMode // os.ModeType-masked expectation (0 = regular)
		wantIsDir bool
	}{
		{"file", jfsTypeFile, 0, false},
		{"dir", jfsTypeDir, fs.ModeDir, true},
		{"symlink", jfsTypeSymlink, os.ModeSymlink, false},
		{"unknown_socket_byte", 6, 0, false}, // not a type we model → regular
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mode, isDir := modeForFiletype(c.ft)
			if got := mode & os.ModeType; got != c.wantType {
				t.Errorf("modeForFiletype(%d) type bits = %v, want %v", c.ft, got, c.wantType)
			}
			if isDir != c.wantIsDir {
				t.Errorf("modeForFiletype(%d) isDir = %v, want %v", c.ft, isDir, c.wantIsDir)
			}
			// Perm bits must be preserved (0644 for non-dir, 0755 for dir).
			wantPerm := fs.FileMode(0644)
			if c.wantIsDir {
				wantPerm = 0755
			}
			if got := mode.Perm(); got != wantPerm {
				t.Errorf("modeForFiletype(%d) perm = %v, want %v", c.ft, got, wantPerm)
			}
		})
	}

	// A symlink's Mode must survive the uint32(mode) SQLite round-trip: the
	// type bit os.ModeSymlink is 1<<27, which fits in uint32 and is restored by
	// fs.FileMode(mode) in scanEntry. Assert the bit survives a round-trip.
	symMode, _ := modeForFiletype(jfsTypeSymlink)
	roundTripped := fs.FileMode(uint32(symMode))
	if roundTripped&os.ModeSymlink == 0 {
		t.Errorf("ModeSymlink lost across uint32 round-trip: %v → %v", symMode, roundTripped)
	}
	if roundTripped&os.ModeDir != 0 {
		t.Errorf("symlink mode must NOT carry ModeDir: %v", roundTripped)
	}
}

// TestDecodeDirChildSymlinkEntry decodes a synthetic 9-byte dir-child value
// with filetype byte = 3 (the inverse of encodeDirChild used by the live
// tests) and asserts the resulting cache Entry carries os.ModeSymlink, exactly
// as the live keyspace dir-resync (reconcileDir) and the full SCAN now build
// it. dir=2 still maps to ModeDir, file=1 to regular. This is the decode site
// the bug lived at.
func TestDecodeDirChildSymlinkEntry(t *testing.T) {
	cases := []struct {
		name     string
		ft       byte
		wantType fs.FileMode
	}{
		{"file", jfsTypeFile, 0},
		{"dir", jfsTypeDir, fs.ModeDir},
		{"symlink", jfsTypeSymlink, os.ModeSymlink},
	}
	const childInode = 424242
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			val := encodeDirChild(childInode, c.ft)
			gotInode, gotFt, ok := decodeDirChild(val)
			if !ok || gotInode != childInode || gotFt != c.ft {
				t.Fatalf("decodeDirChild round-trip failed: inode=%d ft=%d ok=%v", gotInode, gotFt, ok)
			}
			// Build the Entry exactly as reconcileDir/syncMetadata do.
			mode, isDir := modeForFiletype(gotFt)
			e := &Entry{
				Path:  "bundle/Current",
				Name:  "Current",
				IsDir: isDir,
				Inode: gotInode,
				Mode:  mode,
			}
			if got := e.Mode & os.ModeType; got != c.wantType {
				t.Errorf("ft=%d → Entry.Mode type bits = %v, want %v", c.ft, got, c.wantType)
			}
			if c.ft == jfsTypeSymlink && e.IsDir {
				t.Errorf("symlink Entry must not be IsDir")
			}
		})
	}
}

// TestApplyEventSymlinkNoDowngrade exercises the per-key MetadataEvent /
// applyEvent path and the critical no-downgrade invariant: re-applying a synced
// symlink event over an existing ModeSymlink cache Entry must KEEP ModeSymlink.
// Before the fix, applyEvent only carried IsDir, so a symlink event re-derived
// a 0644 regular Mode and clobbered the locally-minted link.
func TestApplyEventSymlinkNoDowngrade(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "store.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	rc := &RedisClient{store: store}

	const linkPath = "Frameworks/Foo.framework/Versions/Current"
	now := time.Now()

	// 1) Locally-minted symlink Entry lands in the cache (mirrors
	//    juiceFS.Symlink: MakeEntry + set ModeSymlink type bit).
	seed := MakeEntry(linkPath, false, 7, now, 9001)
	seed.Mode = (seed.Mode &^ os.ModeType) | os.ModeSymlink
	store.InsertToCache(seed)
	if err := store.Insert(seed); err != nil {
		t.Fatalf("seed Insert: %v", err)
	}
	if got := store.LookupByPath(linkPath); got == nil || got.Mode&os.ModeSymlink == 0 {
		t.Fatalf("precondition: seeded entry is not ModeSymlink: %+v", got)
	}

	// 2) A reconcile/keyspace event re-applies the SAME symlink (different
	//    mtime/size so the live event genuinely overwrites). WITH the
	//    discriminator, applyEvent must preserve ModeSymlink.
	rc.applyEvent(MetadataEvent{
		Op:        "update",
		Path:      linkPath,
		Size:      11,
		Mtime:     now.Add(time.Minute).Unix(),
		Inode:     9001,
		IsSymlink: true,
	})

	after := store.LookupByPath(linkPath)
	if after == nil {
		t.Fatal("entry vanished after applyEvent")
	}
	if after.Mode&os.ModeSymlink == 0 {
		t.Errorf("DOWNGRADE BUG: symlink Entry degraded to non-symlink after re-apply: mode=%v", after.Mode)
	}
	if after.Mode&os.ModeDir != 0 {
		t.Errorf("symlink Entry must not carry ModeDir: %v", after.Mode)
	}

	// 3) Round-trip through SQLite must also preserve ModeSymlink via
	//    uint32(mode) → fs.FileMode (scanEntry). LookupByPath serves the cache,
	//    so reopen the DB from disk — Open()→rebuildCaches() reloads every row
	//    through scanEntry, proving the type bit survived the persisted write.
	if err := store.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}
	store2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { store2.Close() })
	dbEntry := store2.LookupByPath(linkPath)
	if dbEntry == nil || dbEntry.Mode&os.ModeSymlink == 0 {
		t.Errorf("SQLite round-trip lost ModeSymlink: %+v", dbEntry)
	}
}

// TestApplyEventDirVsFileUnchanged guards that the discriminator addition did
// not perturb the existing dir/file classification in applyEvent.
func TestApplyEventDirVsFileUnchanged(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	rc := &RedisClient{store: store}

	rc.applyEvent(MetadataEvent{Op: "create", Path: "movies", Inode: 1, IsDir: true})
	rc.applyEvent(MetadataEvent{Op: "create", Path: "clip.mov", Inode: 2})

	if d := store.LookupByPath("movies"); d == nil || d.Mode&os.ModeDir == 0 || d.Mode&os.ModeSymlink != 0 {
		t.Errorf("dir event misclassified: %+v", d)
	}
	if f := store.LookupByPath("clip.mov"); f == nil || f.Mode&os.ModeType != 0 {
		t.Errorf("file event misclassified (should be regular): %+v", f)
	}
}
