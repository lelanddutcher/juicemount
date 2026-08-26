package farm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// manifest.json is the file every sanitizer in this package runs on, and it was
// the last thing still read by bare name: os.ReadFile followed both the final
// component and every ancestor, with no size cap at all.
func TestManifestReadIsGuardedAndBounded(t *testing.T) {
	blob := "poster.jpg"
	good := ManifestSidecar{Inode: 730001, Derivatives: []derivatives.DerivRow{{
		Kind: "thumbnail", Status: "ready", Producer: "linux-farm",
		Version: 1, BlobRelPath: &blob,
	}}}
	body, _ := json.Marshal(good)

	t.Run("symlinked manifest is refused", func(t *testing.T) {
		mount, outside := t.TempDir(), t.TempDir()
		// The real manifest lives OUTSIDE the volume; the in-tree name is a link.
		target := filepath.Join(outside, "planted.json")
		if err := os.WriteFile(target, body, 0o644); err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(mount, derivatives.DerivDirRel(730001))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, "manifest.json")); err != nil {
			t.Fatal(err)
		}
		store, err := derivatives.Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if found, _ := ReconcileOneSidecar(store, mount, 730001); found {
			t.Fatal("ingested a manifest.json that is a SYMLINK out of the derivative tree")
		}
	})

	t.Run("ancestor symlink is refused", func(t *testing.T) {
		mount, outside := t.TempDir(), t.TempDir()
		if err := os.WriteFile(filepath.Join(outside, "manifest.json"), body, 0o644); err != nil {
			t.Fatal(err)
		}
		base := filepath.Join(mount, ".juicemount", "derivatives")
		if err := os.MkdirAll(base, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(base, "730001")); err != nil {
			t.Fatal(err)
		}
		store, _ := derivatives.Open(":memory:")
		defer store.Close()
		if found, _ := ReconcileOneSidecar(store, mount, 730001); found {
			t.Fatal("ingested through a symlinked per-inode DIRECTORY")
		}
	})

	t.Run("oversized manifest is refused", func(t *testing.T) {
		mount := t.TempDir()
		dir := filepath.Join(mount, derivatives.DerivDirRel(730002))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		// Valid JSON, just enormous: the attacker picks the size, and the parse
		// retry loop re-reads it up to four more times.
		huge := `{"inode":730002,"pad":"` + strings.Repeat("A", int(maxSidecarBytes)+1024) + `"}`
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(huge), 0o644); err != nil {
			t.Fatal(err)
		}
		store, _ := derivatives.Open(":memory:")
		defer store.Close()
		if found, _ := ReconcileOneSidecar(store, mount, 730002); found {
			t.Fatalf("ingested a manifest larger than the %d-byte cap", maxSidecarBytes)
		}
	})

	t.Run("a legitimate manifest still reconciles", func(t *testing.T) {
		mount := t.TempDir()
		dir := filepath.Join(mount, derivatives.DerivDirRel(730001))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), body, 0o644); err != nil {
			t.Fatal(err)
		}
		store, _ := derivatives.Open(":memory:")
		defer store.Close()
		found, err := ReconcileOneSidecar(store, mount, 730001)
		if err != nil || !found {
			t.Fatalf("legitimate manifest refused: found=%v err=%v", found, err)
		}
	})
}

// A refused manifest must be REPORTED, not silently indistinguishable from
// "this inode has no sidecar". A cap that hides what it dropped reads as
// "covered everything" when it did not.
func TestRefusedManifestIsCountedNotSilent(t *testing.T) {
	mount := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "m.json"), []byte(`{"inode":740001}`), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(mount, derivatives.DerivDirRel(740001))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "m.json"), filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	store, _ := derivatives.Open(":memory:")
	defer store.Close()

	res, err := reconcileOneSidecarInto(store, mount, 740001)
	if err != nil {
		t.Fatalf("security refusal was misclassified as a mount outage: %v", err)
	}
	if res.Errs == 0 {
		t.Error("a refused manifest was not counted as an error — it is indistinguishable from absent")
	}

	// An inode with genuinely no sidecar must stay silent (the full walk hits
	// this constantly; counting it would drown the signal).
	quiet, err := reconcileOneSidecarInto(store, mount, 740002)
	if err != nil {
		t.Fatalf("absent sidecar returned error: %v", err)
	}
	if quiet.Errs != 0 {
		t.Errorf("absent sidecar counted as an error (Errs=%d) — the walk would be all noise", quiet.Errs)
	}
}

func TestSidecarMountUnavailableClassification(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.ENXIO, syscall.ESTALE, syscall.ENOTCONN} {
		err := &os.PathError{Op: "open", Path: "/mount/manifest.json", Err: errno}
		if !sidecarMountUnavailable(err) {
			t.Errorf("%v was not classified as a mount-wide outage", errno)
		}
	}
	if sidecarMountUnavailable(os.ErrPermission) {
		t.Error("per-file permission refusal was misclassified as a mount-wide outage")
	}
}
