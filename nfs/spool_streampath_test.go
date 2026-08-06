package nfs

import (
	"path/filepath"
	"strings"
	"testing"
)

// A streamed partial must NEVER occupy the real path.
//
// The existing drainer writes its destination in place, which is safe only
// because it runs after finalize and lasts seconds. A streamed copy holds the
// destination open for the whole transfer — hours for a large file — and during
// that window everything that is not this client (another Mac, the farm,
// ClipLogger, keyspace push) would see a truncated file that looks finished.
// Our own reads come from the spool shadow, so the corruption is invisible from
// the machine doing the copy. That is what makes it dangerous.
func TestStreamTempPathNeverCollidesWithTheRealPath(t *testing.T) {
	const dest = "/Volumes/zpool/DCIM/A001/clip.mov"
	tmp, err := streamTempPath(dest, 42)
	if err != nil {
		t.Fatal(err)
	}
	if tmp == dest {
		t.Fatal("temp path IS the destination — a partial file would sit at the " +
			"real path for the whole transfer")
	}
	if filepath.Dir(tmp) != filepath.Dir(dest) {
		t.Errorf("temp path is in %q, destination in %q — the rename must stay "+
			"within one directory so it is pure metadata, with no data movement",
			filepath.Dir(tmp), filepath.Dir(dest))
	}
	if !strings.HasPrefix(filepath.Base(tmp), ".") {
		t.Errorf("temp name %q is not hidden — a visible partial shows up in Finder "+
			"and in anything walking the tree for content", filepath.Base(tmp))
	}
	if !isStreamTempName(filepath.Base(tmp)) {
		t.Errorf("isStreamTempName does not recognise %q, so partials cannot be "+
			"filtered out of listings or the mirror", filepath.Base(tmp))
	}
}

// Two concurrent streams to the same destination must not share a partial.
// A re-copy over an existing file, or a retry after a failed stream, both hit
// this — and two writers interleaving into one temp file corrupts both.
func TestStreamTempPathIsUniquePerEntry(t *testing.T) {
	const dest = "/Volumes/zpool/DCIM/A001/clip.mov"
	a, err := streamTempPath(dest, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := streamTempPath(dest, 2)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Errorf("entries 1 and 2 share the temp path %q — two concurrent streams "+
			"to the same destination would interleave into one file", a)
	}
}

// Stacking prefixes must be refused.
//
// A retry that built a temp name FROM a temp name would produce
// ".juicemount-streaming-2-.juicemount-streaming-1-clip.mov" and, worse, would
// no longer rename back onto the real path — the partial would be stranded
// under a name nothing ever cleans up.
func TestStreamTempPathRefusesToStackPrefixes(t *testing.T) {
	tmp, err := streamTempPath("/Volumes/zpool/A/clip.mov", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := streamTempPath(tmp, 2); err == nil {
		t.Error("building a temp path from an existing temp path succeeded — the " +
			"result could never be renamed back onto the real destination")
	}
}

// Degenerate inputs must error rather than produce a path.
func TestStreamTempPathRejectsPathsWithoutAFilename(t *testing.T) {
	for _, dest := range []string{"", "/", "."} {
		if got, err := streamTempPath(dest, 1); err == nil {
			t.Errorf("streamTempPath(%q) returned %q, want an error", dest, got)
		}
	}
}

// isStreamTempName must not classify ordinary user files as partials — a
// false positive would hide real content from listings and the mirror.
func TestIsStreamTempNameDoesNotMatchUserFiles(t *testing.T) {
	for _, name := range []string{
		"clip.mov",
		".DS_Store",
		"._clip.mov",
		".hidden",
		"juicemount-streaming-1-clip.mov", // no leading dot: a real user file
	} {
		if isStreamTempName(name) {
			t.Errorf("isStreamTempName(%q) = true — a user file classified as a "+
				"partial would be hidden from listings and the mirror", name)
		}
	}
}
