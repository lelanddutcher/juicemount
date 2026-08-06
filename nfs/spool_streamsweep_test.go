package nfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

// An orphaned partial must be reclaimed, because NOTHING ELSE WILL.
//
// The three guards added for streaming each make a partial invisible in their
// own layer — the mirror rejects it, the farm skips it, and it is dot-prefixed
// so no user sees it. Correct individually, and together they mean no prune
// ladder, no derivative sweep and no human will ever notice a partial left by a
// crash. It can be hundreds of gigabytes, sitting on the very disk this feature
// exists to free.
func TestSweepReclaimsOrphanedPartials(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "DCIM", "A001"), 0o755); err != nil {
		t.Fatal(err)
	}
	rows := []*metadata.SpoolRow{
		{ID: 42, NFSPath: "DCIM/A001/clip.mov", DrainState: metadata.DrainReady},
	}
	tmp, err := streamTempPath(filepath.Join(root, "DCIM/A001/clip.mov"), 42)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	removed, bytes := sweepOrphanedStreamPartials(rows, root)
	if removed != 1 {
		t.Errorf("removed %d partials, want 1 — an unreclaimed partial is an "+
			"unbounded leak that nothing else can see", removed)
	}
	if bytes != 4096 {
		t.Errorf("reclaimed %d bytes, want 4096", bytes)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Error("partial still present after the sweep")
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

	removed, _ := sweepOrphanedStreamPartials(rows, root)
	if removed != 0 {
		t.Errorf("sweep removed %d files when only the real destination existed", removed)
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
