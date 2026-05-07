package verify

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Set up two local targets and verify them — round-trip through Manager.
func TestManagerVerifyAllTwoLocalTargets(t *testing.T) {
	src := t.TempDir()
	bak := t.TempDir()

	// Create matching files in both
	for _, p := range []string{"a.txt", "sub/b.txt", "sub/c.txt"} {
		mustWrite(t, filepath.Join(src, p), "content of "+p)
		mustWrite(t, filepath.Join(bak, p), "content of "+p)
	}
	// Backup sets back the mtime so the 60s skip-recent-write rule doesn't
	// drop the file from this test.
	resetMtimes(t, src, bak)

	srcT, _ := NewLocalTarget(src)
	bakT, _ := NewLocalTarget(bak)

	mp := filepath.Join(t.TempDir(), "manifest.json")
	m, _ := NewManifest(mp)
	mgr := NewManager(m, srcT, bakT)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := mgr.VerifyAll(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Both targets should report 3 files seen, 3 hashed, 0 failed
	for _, want := range []string{srcT.Identifier(), bakT.Identifier()} {
		s, ok := results[want]
		if !ok {
			t.Errorf("missing result for %s", want)
			continue
		}
		if s.FilesSeen != 3 {
			t.Errorf("%s seen = %d, want 3", want, s.FilesSeen)
		}
		if s.FilesHashed != 3 {
			t.Errorf("%s hashed = %d, want 3", want, s.FilesHashed)
		}
		if s.FilesFailed != 0 {
			t.Errorf("%s failed = %d, want 0", want, s.FilesFailed)
		}
	}

	// Status: each file should have 2 verified copies → yellow (need ≥3 for green)
	for _, p := range []string{"a.txt", "sub/b.txt", "sub/c.txt"} {
		got := mgr.Status(p)
		if got != StatusYellow {
			t.Errorf("Status(%s) = %v, want yellow", p, got)
		}
	}
}

// Add a third target so files reach green status.
func TestManagerVerifyAllThreeTargetsGreen(t *testing.T) {
	a := t.TempDir()
	b := t.TempDir()
	c := t.TempDir()

	mustWrite(t, filepath.Join(a, "x.txt"), "x")
	mustWrite(t, filepath.Join(b, "x.txt"), "x")
	mustWrite(t, filepath.Join(c, "x.txt"), "x")
	resetMtimes(t, a, b, c)

	at, _ := NewLocalTarget(a)
	bt, _ := NewLocalTarget(b)
	ct, _ := NewLocalTarget(c)

	mp := filepath.Join(t.TempDir(), "m.json")
	m, _ := NewManifest(mp)
	mgr := NewManager(m, at, bt, ct)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := mgr.VerifyAll(ctx); err != nil {
		t.Fatal(err)
	}

	if got := mgr.Status("x.txt"); got != StatusGreen {
		t.Errorf("Status with 3 targets = %v, want green", got)
	}
}

// Detect silent corruption: same file size on backup, different content.
func TestManagerDetectsSilentCorruption(t *testing.T) {
	src := t.TempDir()
	bak := t.TempDir()
	good := t.TempDir()

	mustWrite(t, filepath.Join(src, "f.txt"), "original-content")
	mustWrite(t, filepath.Join(bak, "f.txt"), "corrupted-content") // same length, different bytes
	mustWrite(t, filepath.Join(good, "f.txt"), "original-content")
	resetMtimes(t, src, bak, good)

	st, _ := NewLocalTarget(src)
	bt, _ := NewLocalTarget(bak)
	gt, _ := NewLocalTarget(good)
	mp := filepath.Join(t.TempDir(), "m.json")
	m, _ := NewManifest(mp)
	mgr := NewManager(m, st, bt, gt)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := mgr.VerifyAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Status should be RED — one corrupt copy detected
	if got := mgr.Status("f.txt"); got != StatusRed {
		t.Errorf("status with corrupt copy = %v, want red", got)
	}

	// Specifically: the bak target should have OK=false on its verification
	rec := m.Get("f.txt")
	bakV := rec.Verifications[bt.Identifier()]
	if bakV.OK {
		t.Errorf("backup target should be marked OK=false; got %+v", bakV)
	}
	srcV := rec.Verifications[st.Identifier()]
	if !srcV.OK {
		t.Errorf("source target should be OK=true; got %+v", srcV)
	}
}

// SafeToDelete should refuse when only the source has a verified copy.
func TestSafeToDeleteRefusesSingleCopy(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "lonely.txt"), "only copy")
	resetMtimes(t, src)

	st, _ := NewLocalTarget(src)
	mp := filepath.Join(t.TempDir(), "m.json")
	m, _ := NewManifest(mp)
	mgr := NewManager(m, st)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	mgr.VerifyAll(ctx)

	v := mgr.SafeToDelete("lonely.txt", st.Identifier())
	if v.Safe {
		t.Errorf("SafeToDelete should refuse single-copy file; got %+v", v)
	}
	if v.VerifiedCopies != 0 {
		t.Errorf("VerifiedCopies = %d, want 0", v.VerifiedCopies)
	}
}

// SafeToDelete should permit when ≥2 OTHER targets have verified copies.
func TestSafeToDeletePermitsWhenRedundant(t *testing.T) {
	a := t.TempDir()
	b := t.TempDir()
	c := t.TempDir()
	for _, dir := range []string{a, b, c} {
		mustWrite(t, filepath.Join(dir, "shared.txt"), "redundant")
	}
	resetMtimes(t, a, b, c)

	at, _ := NewLocalTarget(a)
	bt, _ := NewLocalTarget(b)
	ct, _ := NewLocalTarget(c)
	mp := filepath.Join(t.TempDir(), "m.json")
	m, _ := NewManifest(mp)
	mgr := NewManager(m, at, bt, ct)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	mgr.VerifyAll(ctx)

	// Try to delete from `a`. b and c still have it → safe.
	v := mgr.SafeToDelete("shared.txt", at.Identifier())
	if !v.Safe {
		t.Errorf("expected Safe=true with 2 other verified copies; got %+v", v)
	}
	if v.VerifiedCopies != 2 {
		t.Errorf("VerifiedCopies = %d, want 2", v.VerifiedCopies)
	}
}

// SafeToDelete on an unknown file should refuse.
func TestSafeToDeleteRefusesUnknown(t *testing.T) {
	mp := filepath.Join(t.TempDir(), "m.json")
	m, _ := NewManifest(mp)
	mgr := NewManager(m)
	v := mgr.SafeToDelete("/not/in/manifest", "any")
	if v.Safe {
		t.Error("unknown file should not be safe to delete")
	}
}

// Verifier should skip files modified within the last 60s
// (simulates an in-flight rsync write being stable enough to hash).
func TestManagerSkipsRecentlyModified(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "fresh.txt"), "just written")
	// Don't reset mtime — leave it as "now"

	st, _ := NewLocalTarget(src)
	mp := filepath.Join(t.TempDir(), "m.json")
	m, _ := NewManifest(mp)
	mgr := NewManager(m, st)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	mgr.VerifyAll(ctx)

	if rec := m.Get("fresh.txt"); rec != nil {
		t.Errorf("expected fresh.txt to be skipped (mtime within 60s); got record %+v", rec)
	}
}

// resetMtimes pushes mtimes back 5 minutes so the 60s skip-recent rule
// in verifyTarget doesn't drop test files.
func resetMtimes(t *testing.T, dirs ...string) {
	t.Helper()
	past := time.Now().Add(-5 * time.Minute)
	for _, dir := range dirs {
		filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			os.Chtimes(path, past, past)
			return nil
		})
	}
}
