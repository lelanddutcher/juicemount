package nfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The live incident these reproduce: 14 ClipLogger uploads, every one a name
// collision with a farm artifact, where the client's waveform.json was a
// 2,000-pixel preview and the farm's was 149,166-pixel full resolution under
// the same name. See deriv_clobber_guard.go.

// asOtherProducer makes derivClobberEUID report a uid that is NOT the one that
// owns files this test creates, standing in for the farm's root-in-a-container.
func asOtherProducer(t *testing.T) {
	t.Helper()
	real := os.Geteuid()
	derivClobberEUID = func() int { return real + 1 }
	t.Cleanup(func() { derivClobberEUID = os.Geteuid })
}

func writeBlob(t *testing.T, dir, name string, n int) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// THE MAIN EVENT: the exact live collision, with the real byte counts.
func TestForeignOwnedDerivativeIsNotOverwritten(t *testing.T) {
	asOtherProducer(t)
	root := t.TempDir()
	rel := ".juicemount/derivatives/2313833/waveform.json"
	dest := writeBlob(t, filepath.Join(root, ".juicemount/derivatives/2313833"), "waveform.json", 670104)

	err := checkDerivClobber(rel, dest, 11541)
	if err == nil {
		t.Fatal("a 11,541-byte preview was allowed to replace a 670,104-byte " +
			"full-resolution waveform belonging to another producer. Downsampling " +
			"149k->2k is not reversible, and this would have run 12,016 times")
	}
	for _, want := range []string{"670104", "11541", "refusing to overwrite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error text is missing %q, so an operator cannot see what was "+
				"preserved or what was refused: %v", want, err)
		}
	}
	// The existing blob must still be intact — the guard runs BEFORE os.Create,
	// which truncates.
	if fi, serr := os.Stat(dest); serr != nil || fi.Size() != 670104 {
		t.Errorf("existing blob was disturbed: %v (size now %v)", serr, fi)
	}
}

// A producer updating its OWN blob is normal and must not be blocked, or the
// farm could never re-derive and a client could never correct its own upload.
func TestOwnDerivativeIsStillOverwritable(t *testing.T) {
	root := t.TempDir()
	rel := ".juicemount/derivatives/999/poster.jpg"
	dest := writeBlob(t, filepath.Join(root, ".juicemount/derivatives/999"), "poster.jpg", 100)

	if err := checkDerivClobber(rel, dest, 200); err != nil {
		t.Fatalf("a producer was blocked from replacing its own blob: %v", err)
	}
}

// The guard is scoped to the derivatives tree. Ordinary volume files must keep
// ordinary overwrite behaviour — a user replacing a file is not a data-loss
// event, and widening this rule would break normal copies.
func TestOrdinaryFilesAreUntouchedByTheGuard(t *testing.T) {
	asOtherProducer(t)
	root := t.TempDir()
	for _, rel := range []string{
		"Footage/reel.mov",
		".juicemount/spool/whatever",
		"derivatives/notthetree.json",
		".juicemount/other/x.json",
	} {
		dest := writeBlob(t, filepath.Join(root, filepath.Dir(rel)), filepath.Base(rel), 10)
		if err := checkDerivClobber(rel, dest, 20); err != nil {
			t.Errorf("%s was guarded but is outside the derivatives tree: %v", rel, err)
		}
	}
}

// The overwhelmingly common case: nothing there yet. Must never fail.
func TestFirstContributionIsAllowed(t *testing.T) {
	asOtherProducer(t)
	root := t.TempDir()
	rel := ".juicemount/derivatives/4242/strip.jpg"
	dest := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := checkDerivClobber(rel, dest, 5000); err != nil {
		t.Fatalf("a contribution to a name nobody has taken was refused: %v", err)
	}
}

func TestIsDerivativeBlobPath(t *testing.T) {
	for _, tc := range []struct {
		p    string
		want bool
	}{
		{".juicemount/derivatives/1/waveform.json", true},
		{"/.juicemount/derivatives/1/waveform.json", true},
		{".juicemount/derivatives/1/._waveform.json", true},
		{".juicemount/derivatives/", false}, // the tree root itself holds no blobs
		{".juicemount/derivativesX/1/a.json", false},
		{"Footage/.juicemount/derivatives/1/a.json", false},
		{"reel.mov", false},
		{"", false},
	} {
		if got := isDerivativeBlobPath(tc.p); got != tc.want {
			t.Errorf("isDerivativeBlobPath(%q) = %v, want %v", tc.p, got, tc.want)
		}
	}
}

// A directory sharing a blob's name must not be reported as a clobber — that
// would turn a structural problem into a permanent per-file failure.
func TestDirectoryAtTheDestIsNotAClobber(t *testing.T) {
	asOtherProducer(t)
	root := t.TempDir()
	rel := ".juicemount/derivatives/7/waveform.json"
	dest := filepath.Join(root, rel)
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := checkDerivClobber(rel, dest, 10); err != nil {
		t.Errorf("a directory at the destination was reported as a clobber: %v", err)
	}
}
