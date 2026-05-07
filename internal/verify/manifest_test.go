package verify

import (
	"path/filepath"
	"testing"
	"time"
)

func TestManifestRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	mp := filepath.Join(tmp, "manifest.json")

	m, err := NewManifest(mp)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().Truncate(time.Second)
	m.Record("Films/clip.r3d", TargetVerification{
		Target:     "local:/Volumes/zpool",
		Hash:       "abc",
		Size:       1234,
		ModTime:    now,
		VerifiedAt: now,
	})
	m.Record("Films/clip.r3d", TargetVerification{
		Target:     "local:/Volumes/backup",
		Hash:       "abc",
		Size:       1234,
		ModTime:    now,
		VerifiedAt: now,
	})

	if err := m.Save(); err != nil {
		t.Fatal(err)
	}

	// Reload from disk
	m2, err := NewManifest(mp)
	if err != nil {
		t.Fatal(err)
	}
	rec := m2.Get("Films/clip.r3d")
	if rec == nil {
		t.Fatal("expected record after reload")
	}
	if len(rec.Verifications) != 2 {
		t.Errorf("verifications = %d, want 2", len(rec.Verifications))
	}
	if rec.CanonicalHash != "abc" {
		t.Errorf("canonical hash = %q, want abc", rec.CanonicalHash)
	}
	for _, v := range rec.Verifications {
		if !v.OK {
			t.Errorf("expected OK=true for matching hash; got %+v", v)
		}
	}
}

func TestManifestMajorityHashWithMismatch(t *testing.T) {
	tmp := t.TempDir()
	m, _ := NewManifest(filepath.Join(tmp, "m.json"))

	now := time.Now()
	// Two targets agree on hash "abc", one is "xyz" (corruption)
	m.Record("foo.txt", TargetVerification{Target: "t1", Hash: "abc", VerifiedAt: now})
	m.Record("foo.txt", TargetVerification{Target: "t2", Hash: "abc", VerifiedAt: now})
	m.Record("foo.txt", TargetVerification{Target: "t3", Hash: "xyz", VerifiedAt: now})

	rec := m.Get("foo.txt")
	if rec.CanonicalHash != "abc" {
		t.Errorf("canonical = %q, want abc (majority)", rec.CanonicalHash)
	}
	if !rec.Verifications["t1"].OK || !rec.Verifications["t2"].OK {
		t.Error("t1 and t2 should be OK")
	}
	if rec.Verifications["t3"].OK {
		t.Error("t3 should be NOT OK (hash mismatch)")
	}
	// Status should be RED — one verified target is corrupted
	if statusOf(rec) != StatusRed {
		t.Errorf("status = %v, want red (one corrupt copy)", statusOf(rec))
	}
}

func TestManifestStatsBuckets(t *testing.T) {
	tmp := t.TempDir()
	m, _ := NewManifest(filepath.Join(tmp, "m.json"))

	now := time.Now()

	// File 1: green (3 verified copies)
	for _, tID := range []string{"t1", "t2", "t3"} {
		m.Record("green.txt", TargetVerification{Target: tID, Hash: "g", VerifiedAt: now})
	}
	// File 2: yellow (1 verified copy)
	m.Record("yellow.txt", TargetVerification{Target: "t1", Hash: "y", VerifiedAt: now})
	// File 3: red (1 verified, 1 mismatch)
	m.Record("red.txt", TargetVerification{Target: "t1", Hash: "r1", VerifiedAt: now})
	m.Record("red.txt", TargetVerification{Target: "t2", Hash: "r2", VerifiedAt: now})

	s := m.Stats()
	if s.TotalFiles != 3 {
		t.Errorf("total = %d, want 3", s.TotalFiles)
	}
	if s.GreenCount != 1 {
		t.Errorf("green = %d, want 1", s.GreenCount)
	}
	if s.YellowCount != 1 {
		t.Errorf("yellow = %d, want 1", s.YellowCount)
	}
	if s.RedCount != 1 {
		t.Errorf("red = %d, want 1", s.RedCount)
	}
}

func TestManifestAllPathsSorted(t *testing.T) {
	tmp := t.TempDir()
	m, _ := NewManifest(filepath.Join(tmp, "m.json"))

	now := time.Now()
	for _, p := range []string{"z.txt", "a.txt", "m.txt"} {
		m.Record(p, TargetVerification{Target: "t1", Hash: "h", VerifiedAt: now})
	}
	paths := m.AllPaths()
	if len(paths) != 3 {
		t.Fatalf("expected 3 paths, got %d", len(paths))
	}
	if paths[0] != "a.txt" || paths[1] != "m.txt" || paths[2] != "z.txt" {
		t.Errorf("paths not sorted: %v", paths)
	}
}

func TestManifestSaveAtomic(t *testing.T) {
	tmp := t.TempDir()
	mp := filepath.Join(tmp, "manifest.json")
	m, _ := NewManifest(mp)
	now := time.Now()
	m.Record("x", TargetVerification{Target: "t1", Hash: "h", VerifiedAt: now})
	if err := m.Save(); err != nil {
		t.Fatal(err)
	}
	// No leftover .tmp.* files
	matches, _ := filepath.Glob(filepath.Join(tmp, "*.tmp.*"))
	if len(matches) != 0 {
		t.Errorf("expected no .tmp.* leftovers, got %v", matches)
	}
}

func TestStatusOfNilAndEmpty(t *testing.T) {
	if statusOf(nil) != StatusUnknown {
		t.Error("nil record should be Unknown")
	}
	rec := &FileRecord{Path: "x", Verifications: map[string]TargetVerification{}}
	if statusOf(rec) != StatusUnknown {
		t.Error("empty record should be Unknown")
	}
}
