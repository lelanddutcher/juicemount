package derivatives

import (
	"os"
	"path/filepath"
	"testing"
)

// TestEnsureDirUnderRefusesSymlinkedComponent is the write-side counterpart to
// the read-side ancestor-symlink case. os.MkdirAll FOLLOWS a symlinked
// component, so a planted .juicemount/derivatives/<inode> -> /elsewhere link
// silently redirects everything the farm writes — ffmpeg output included — out
// of the volume, on a host where the farm runs as root.
func TestEnsureDirUnderRefusesSymlinkedComponent(t *testing.T) {
	mount := t.TempDir()
	outside := t.TempDir()

	rel := DerivDirRel(720001)

	// Baseline: creates cleanly, and is idempotent.
	if err := EnsureDirUnder(mount, rel); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := EnsureDirUnder(mount, rel); err != nil {
		t.Fatalf("second call must be a no-op, got: %v", err)
	}
	if fi, err := os.Lstat(filepath.Join(mount, rel)); err != nil || !fi.IsDir() {
		t.Fatalf("directory not created: err=%v", err)
	}

	// THE ATTACK: replace the per-inode component with a symlink out of the tree.
	if err := os.RemoveAll(filepath.Join(mount, rel)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(mount, rel)); err != nil {
		t.Fatal(err)
	}
	if err := EnsureDirUnder(mount, rel); err == nil {
		t.Fatal("traversed a symlinked component — farm writes would land outside the mount")
	}

	// Demonstrate the contrast: MkdirAll accepts the same layout, which is why
	// the plain call could not stay.
	if err := os.MkdirAll(filepath.Join(mount, rel), 0o755); err != nil {
		t.Fatalf("expected MkdirAll to follow the symlink (that is the bug), got: %v", err)
	}
}

// An intermediate component matters as much as the leaf.
func TestEnsureDirUnderRefusesSymlinkedAncestor(t *testing.T) {
	mount := t.TempDir()
	outside := t.TempDir()
	if err := os.MkdirAll(filepath.Join(mount, ".juicemount"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(mount, ".juicemount", "derivatives")); err != nil {
		t.Fatal(err)
	}
	if err := EnsureDirUnder(mount, DerivDirRel(720002)); err == nil {
		t.Fatal("traversed a symlinked ANCESTOR component")
	}
}
