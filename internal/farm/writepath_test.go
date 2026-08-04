package farm

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// The farm has THREE mutually-exclusive entry points (cmd/jmfarm runs -proxy,
// -transcript and the blob pass as separate invocations, each returning before
// the others). A guard installed in only one of them protected only one of them,
// which is how a symlinked <inode> component still redirected real ffmpeg output
// and the manifest write out of the volume — on a host where the farm is root.
//
// This asserts the property directly on the shared primitive every entry point
// now goes through, so a fourth entry point cannot quietly skip it.
func TestDerivDirRefusesSymlinkedInodeComponent(t *testing.T) {
	mount, outside := t.TempDir(), t.TempDir()
	const inode = 990101

	// Baseline: a clean tree yields a usable descriptor.
	dir, err := derivDirFor(mount, inode)
	if err != nil {
		t.Fatalf("clean tree refused: %v", err)
	}
	if err := derivatives.WriteFileAt(dir, "manifest.json", []byte(`{"ok":1}`), 0o644); err != nil {
		t.Fatalf("write through descriptor: %v", err)
	}
	dir.Close()
	if _, err := os.Lstat(filepath.Join(mount, derivatives.DerivBlobRel(inode, "manifest.json"))); err != nil {
		t.Fatalf("file did not land inside the volume: %v", err)
	}

	// THE ATTACK: swap the per-inode directory for a link out of the tree.
	if err := os.RemoveAll(filepath.Join(mount, derivatives.DerivDirRel(inode))); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(mount, derivatives.DerivDirRel(inode))); err != nil {
		t.Fatal(err)
	}
	if d, err := derivDirFor(mount, inode); err == nil {
		d.Close()
		t.Fatal("derivDirFor traversed a symlinked <inode> component — " +
			"every farm write for this asset would land outside the volume, as root")
	}

	// And nothing was written through it.
	if ents, _ := os.ReadDir(outside); len(ents) != 0 {
		t.Errorf("files escaped the volume: %v", ents)
	}
}

// WriteFileAt must not be steerable by anything that happens to the PATH after
// the directory was validated — that check-then-use window is minutes wide
// during an encode.
func TestWriteFileAtIsNotPathResolved(t *testing.T) {
	mount, outside := t.TempDir(), t.TempDir()
	const inode = 990102

	dir, err := derivDirFor(mount, inode)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	// Swap the directory AFTER validation, exactly as an attacker would mid-encode.
	_ = os.RemoveAll(filepath.Join(mount, derivatives.DerivDirRel(inode)))
	if err := os.Symlink(outside, filepath.Join(mount, derivatives.DerivDirRel(inode))); err != nil {
		t.Fatal(err)
	}

	// The held descriptor still refers to the ORIGINAL directory, so the write
	// follows the descriptor rather than the swapped name. Whether it SUCCEEDS
	// depends on whether that directory still exists — here it was removed, so
	// openat fails with ENOENT. Either outcome is safe; the property under test
	// is that the bytes never reach the attacker's directory. Asserting success
	// would have been asserting the wrong thing.
	err = derivatives.WriteFileAt(dir, "manifest.json", []byte(`{"held":1}`), 0o644)
	if ents, _ := os.ReadDir(outside); len(ents) != 0 {
		t.Fatalf("post-validation swap redirected the write outside the volume: %v (write err %v)", ents, err)
	}

	// The same swap against a PATH-resolving write does land outside — that is
	// the bug this design removes, demonstrated rather than asserted in prose.
	victim := filepath.Join(mount, derivatives.DerivBlobRel(inode, "manifest.json"))
	if err := os.WriteFile(victim, []byte(`{"path":1}`), 0o644); err != nil {
		t.Fatalf("path-resolved control write failed unexpectedly: %v", err)
	}
	if ents, _ := os.ReadDir(outside); len(ents) == 0 {
		t.Error("control: expected the path-resolved write to follow the symlink outside")
	}
}

// A staged blob is committed through the descriptor, and a failed generation
// leaves nothing behind under the final name.
func TestStageAndCommitThroughDescriptor(t *testing.T) {
	mount := t.TempDir()
	const inode = 990103
	dir, err := derivDirFor(mount, inode)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	staged, abs, err := derivatives.StageNameAt(dir, "proxy.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(abs) != staged {
		t.Errorf("staged path %q does not end in the staged name %q", abs, staged)
	}
	// The staged name exists and is a REGULAR file we created — so it cannot
	// already be a symlink pointing elsewhere.
	fi, err := os.Lstat(abs)
	if err != nil || !fi.Mode().IsRegular() {
		t.Fatalf("staged entry is not a regular file: %v", err)
	}
	if err := os.WriteFile(abs, []byte("PROXY"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := derivatives.CommitStagedAt(dir, staged, "proxy.mp4"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(mount, derivatives.DerivBlobRel(inode, "proxy.mp4")))
	if err != nil || string(got) != "PROXY" {
		t.Fatalf("committed blob = %q (err %v)", got, err)
	}

	// Discard must leave no staged litter.
	staged2, _, err := derivatives.StageNameAt(dir, "poster.jpg")
	if err != nil {
		t.Fatal(err)
	}
	derivatives.DiscardStagedAt(dir, staged2)
	if _, err := os.Lstat(filepath.Join(mount, derivatives.DerivBlobRel(inode, staged2))); err == nil {
		t.Error("discarded stage still on disk")
	}
}

// WriteFileAt takes a FLAT name only: a separator would reintroduce the
// multi-component resolution these helpers exist to avoid.
func TestWriteFileAtRejectsNonFlatNames(t *testing.T) {
	mount := t.TempDir()
	dir, err := derivDirFor(mount, 990104)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	for _, bad := range []string{"../escape.json", "sub/dir.json", "/abs.json", "", ".", ".."} {
		if err := derivatives.WriteFileAt(dir, bad, []byte("x"), 0o644); err == nil {
			t.Errorf("accepted non-flat name %q", bad)
		}
	}
}
