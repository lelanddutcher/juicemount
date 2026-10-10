package nfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

// A derived pathname is not an ownership proof. Keep possible orphan stages
// until backend publication and cleanup can be fenced together.
func TestSweepDefersOrphanCleanup(t *testing.T) {
	spool, _, _ := newSpoolStoreForRecovery(t)
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "DCIM", "A001"), 0o755); err != nil {
		t.Fatal(err)
	}
	id, err := spool.Meta().Insert("DCIM/A001/clip.mov", filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := streamTempPath(filepath.Join(root, "DCIM/A001/clip.mov"), id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	var deferred *DeferredStreamCleanup
	if err := d.SweepStreamPartials(); !errors.As(err, &deferred) || deferred.Candidates != 1 {
		t.Fatalf("sweep = %v, want one deferred candidate", err)
	}
	if fi, err := os.Stat(tmp); err != nil || fi.Size() != 4096 {
		t.Fatalf("stage changed during deferred cleanup: (%v, %v)", fi, err)
	}
}

// The sweep must NEVER touch the real destination. Deleting finished footage
// would be catastrophically worse than leaking a partial.
func TestSweepNeverTouchesTheRealDestination(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "DCIM"), 0o755); err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(root, "DCIM", "clip.mov")
	if err := os.WriteFile(real, []byte("finished footage"), 0o644); err != nil {
		t.Fatal(err)
	}
	rows := []*metadata.SpoolRow{
		{ID: 7, NFSPath: "DCIM/clip.mov", DrainState: metadata.DrainReady},
	}

	candidates := streamPartialCandidates(rows, root)
	if len(candidates) != 1 || candidates[0] == real {
		t.Fatalf("stage candidates = %v, want only the private stage", candidates)
	}
	if _, err := os.Stat(real); err != nil {
		t.Fatalf("THE SWEEP DELETED THE REAL DESTINATION: %v", err)
	}
}

// A `done` row cannot have a live partial — completion IS the rename. Skipping
// done rows also means a GC'd audit row can never drive a deletion.
func TestSweepSkipsCompletedRows(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "DCIM"), 0o755); err != nil {
		t.Fatal(err)
	}
	rows := []*metadata.SpoolRow{
		{ID: 9, NFSPath: "DCIM/clip.mov", DrainState: metadata.DrainDone},
	}
	if got := streamPartialCandidates(rows, root); len(got) != 0 {
		t.Errorf("a done row produced sweep candidates %v — completion is the "+
			"rename, so no temp name survives it", got)
	}
}

// Selection must be bounded by live rows, never by a directory walk. A volume
// walk would be O(volume) over a tree that may hold millions of files, across a
// link that may be cellular.
func TestSweepCandidatesAreDerivedFromRowsOnly(t *testing.T) {
	root := t.TempDir()
	rows := []*metadata.SpoolRow{
		{ID: 1, NFSPath: "a/one.mov", DrainState: metadata.DrainReady},
		{ID: 2, NFSPath: "b/two.mov", DrainState: metadata.DrainWriting},
		{ID: 3, NFSPath: "", DrainState: metadata.DrainReady},          // no path: skipped
		{ID: 4, NFSPath: "c/four.mov", DrainState: metadata.DrainDone}, // done: skipped
	}
	got := streamPartialCandidates(rows, root)
	if len(got) != 2 {
		t.Fatalf("got %d candidates %v, want 2 (rows 1 and 2)", len(got), got)
	}
	for _, p := range got {
		if !isStreamTempName(filepath.Base(p)) {
			t.Errorf("candidate %q is not a stream temp name — the sweep must only "+
				"ever delete paths it constructed itself", p)
		}
	}
}

// An empty FUSE root must produce nothing. Joining onto "" would yield relative
// paths rooted at the process working directory — deleting files there would be
// both wrong and untraceable.
func TestSweepRefusesAnEmptyRoot(t *testing.T) {
	rows := []*metadata.SpoolRow{
		{ID: 1, NFSPath: "DCIM/clip.mov", DrainState: metadata.DrainReady},
	}
	if got := streamPartialCandidates(rows, ""); len(got) != 0 {
		t.Errorf("empty fuseRoot produced candidates %v — those are relative paths "+
			"under the process working directory", got)
	}
}
