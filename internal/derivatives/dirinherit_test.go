package derivatives

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// The live bug (2026-08-11): the derivatives tree has two writers — the farm as
// root in a container, the Mac client as the logged-in user — and EnsureDirUnder
// created every level at a hardcoded 0755. Whoever got there first locked the
// other out. On the affected volume that was 12,016 root-owned directories
// against 2,780 client-owned, with 14 uploads stuck on `permission denied`
// while the PARENT `.juicemount/derivatives/` was 501:20 drwxrwxr-x the whole
// time. The tree was writable; only the new children were not.
//
// The chown half of the fix needs root and cannot be staged here. The MODE half
// is the half that unblocks a same-group client, and it is fully testable.

// setupParent makes root/<rel-parent> with an explicit mode, defeating umask.
func setupParent(t *testing.T, root, rel string, mode os.FileMode) string {
	t.Helper()
	dir := filepath.Join(root, rel)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, mode); err != nil {
		t.Fatal(err)
	}
	return dir
}

func modeOf(t *testing.T, p string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode().Perm()
}

// THE FIX. A per-inode directory must come out as writable as the tree it lives
// in, so the other producer can contribute into it.
func TestCreatedDirInheritsParentMode(t *testing.T) {
	root := t.TempDir()
	setupParent(t, root, ".juicemount/derivatives", 0o775)

	if err := EnsureDirUnder(root, ".juicemount/derivatives/2313833"); err != nil {
		t.Fatalf("EnsureDirUnder: %v", err)
	}

	got := modeOf(t, filepath.Join(root, ".juicemount/derivatives/2313833"))
	if got != 0o775 {
		t.Fatalf("new derivative dir is %04o, want 0775 inherited from its parent. "+
			"At 0755 the other producer cannot write into it — that is 12,016 "+
			"directories the client was locked out of", got)
	}
}

// Inheritance must not WIDEN access. Copying the parent's mode exactly means a
// locked-down derivatives tree stays locked down.
func TestCreatedDirDoesNotWidenBeyondParent(t *testing.T) {
	root := t.TempDir()
	setupParent(t, root, ".juicemount/derivatives", 0o700)

	if err := EnsureDirUnder(root, ".juicemount/derivatives/55"); err != nil {
		t.Fatalf("EnsureDirUnder: %v", err)
	}
	if got := modeOf(t, filepath.Join(root, ".juicemount/derivatives/55")); got != 0o700 {
		t.Errorf("new dir is %04o under a 0700 parent, want 0700 — inheritance must "+
			"never widen access", got)
	}
}

// An EXISTING directory must be left alone. Re-running the farm must not
// silently re-permission directories somebody deliberately changed.
func TestExistingDirIsNotRePermissioned(t *testing.T) {
	root := t.TempDir()
	setupParent(t, root, ".juicemount/derivatives", 0o775)
	child := filepath.Join(root, ".juicemount/derivatives/77")
	if err := os.MkdirAll(child, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(child, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := EnsureDirUnder(root, ".juicemount/derivatives/77"); err != nil {
		t.Fatalf("EnsureDirUnder: %v", err)
	}
	if got := modeOf(t, child); got != 0o700 {
		t.Errorf("existing dir was re-permissioned to %04o; only dirs we CREATE "+
			"may be adjusted", got)
	}
}

// Every level we create inherits, not just the last one.
func TestEveryCreatedLevelInherits(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o775); err != nil {
		t.Fatal(err)
	}
	if err := EnsureDirUnder(root, ".juicemount/derivatives/900"); err != nil {
		t.Fatalf("EnsureDirUnder: %v", err)
	}
	for _, rel := range []string{".juicemount", ".juicemount/derivatives", ".juicemount/derivatives/900"} {
		if got := modeOf(t, filepath.Join(root, rel)); got != 0o775 {
			t.Errorf("%s is %04o, want 0775 — an intermediate level that stays 0755 "+
				"blocks the other producer just as effectively as the leaf", rel, got)
		}
	}
}

// The symlink refusal that EnsureDirUnder exists for must survive the change.
func TestSymlinkComponentIsStillRefused(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	setupParent(t, root, ".juicemount", 0o775)
	link := filepath.Join(root, ".juicemount/derivatives")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}
	if err := EnsureDirUnder(root, ".juicemount/derivatives/1"); err == nil {
		t.Fatal("a symlinked component was traversed — this is the escape-the-volume " +
			"hole EnsureDirUnder exists to close")
	}
}

// Ownership is only adjustable when we have the privilege. Unprivileged, the
// call must still succeed — a best-effort chown that failed must never turn
// into a failed mkdir.
func TestUnprivilegedCreateStillSucceeds(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; this asserts the unprivileged path")
	}
	root := t.TempDir()
	setupParent(t, root, ".juicemount/derivatives", 0o775)
	if err := EnsureDirUnder(root, ".juicemount/derivatives/123"); err != nil {
		t.Fatalf("unprivileged EnsureDirUnder failed: %v — a best-effort chown must "+
			"never fail the directory creation", err)
	}
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(root, ".juicemount/derivatives/123"), &st); err != nil {
		t.Fatal(err)
	}
	if int(st.Uid) != os.Geteuid() {
		t.Errorf("dir uid = %d, want our own %d", st.Uid, os.Geteuid())
	}
}
