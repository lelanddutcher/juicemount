package farm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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
