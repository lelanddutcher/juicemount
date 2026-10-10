package derivatives

import (
	"os"
	"path/filepath"
	"testing"
)

// StatRegularUnder must hold exactly the guarantees OpenRegularUnder does — it
// only skips OPENING the leaf, not checking it. Covered separately because it
// takes a different syscall path (fstatat rather than openat) and so cannot
// inherit the open path's tests.
func TestStatRegularUnderGuarantees(t *testing.T) {
	mount := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "proxy.mp4"), []byte("PRIVATE"), 0o600); err != nil {
		t.Fatal(err)
	}

	rel := DerivBlobRel(710001, "proxy.mp4")
	dir := filepath.Dir(filepath.Join(mount, rel))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mount, rel), []byte("REAL-PROXY"), 0o644); err != nil {
		t.Fatal(err)
	}

	fi, err := StatRegularUnder(mount, rel)
	if err != nil {
		t.Fatalf("legitimate blob refused: %v", err)
	}
	if fi.Size() != int64(len("REAL-PROXY")) {
		t.Errorf("size = %d, want %d", fi.Size(), len("REAL-PROXY"))
	}

	t.Run("ancestor symlink", func(t *testing.T) {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, dir); err != nil {
			t.Fatal(err)
		}
		if fi, err := StatRegularUnder(mount, rel); err == nil {
			t.Fatalf("ancestor symlink followed — leaked size %d of an out-of-tree file", fi.Size())
		}
		_ = os.Remove(dir)
	})

	t.Run("final symlink", func(t *testing.T) {
		r := DerivBlobRel(710002, "proxy.mp4")
		if err := os.MkdirAll(filepath.Dir(filepath.Join(mount, r)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(outside, "proxy.mp4"), filepath.Join(mount, r)); err != nil {
			t.Fatal(err)
		}
		if _, err := StatRegularUnder(mount, r); err == nil {
			t.Fatal("final-component symlink followed")
		}
	})

	t.Run("directory", func(t *testing.T) {
		r := DerivBlobRel(710003, "proxy.mp4")
		if err := os.MkdirAll(filepath.Join(mount, r), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := StatRegularUnder(mount, r); err == nil {
			t.Fatal("stat'ed a DIRECTORY as a derivative blob")
		}
	})

	t.Run("escape", func(t *testing.T) {
		for _, r := range []string{"../" + filepath.Base(outside) + "/proxy.mp4", ""} {
			if _, err := StatRegularUnder(mount, r); err == nil {
				t.Errorf("escape %q permitted", r)
			}
		}
	})
}

// statInfo carries only PERMISSION bits from the raw stat, and Go's convention
// is that a FileMode with no type bits set means "regular" — so IsRegular() is
// true by construction rather than by intent. That is correct but accidental,
// and a caller checking Mode() would be silently wrong if the type bits were
// ever copied in without ModeType handling. Pinned so it cannot drift.
func TestStatRegularUnderModeReportsRegular(t *testing.T) {
	mount := t.TempDir()
	rel := DerivBlobRel(710009, "poster.jpg")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(mount, rel)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mount, rel), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := StatRegularUnder(mount, rel)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.Mode().IsRegular() {
		t.Errorf("Mode().IsRegular() = false for a regular file (mode %v) — "+
			"a caller checking it would reject every valid blob", fi.Mode())
	}
	if fi.IsDir() {
		t.Error("IsDir() = true for a regular file")
	}
}
