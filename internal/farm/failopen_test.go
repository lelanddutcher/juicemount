package farm

import (
	"os"
	"path/filepath"
	"testing"

	"encoding/json"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// A staging design must never publish bytes the generator did not write.
//
// StageNameAt creates the output file BEFORE the generator runs, so a generator
// that returns nil having written nothing — Waveform does exactly that when the
// source turns out to have no audio — would commit the 0-byte placeholder as a
// `ready` derivative. That inverts the fail-closed rule the whole derivative
// plane rests on: a MISSING blob 404s and the reader regenerates locally, while
// a 0-byte blob is served with 200 and looks like a real answer.
func TestCommitStagedRefusesEmptyPlaceholder(t *testing.T) {
	mount := t.TempDir()
	const inode = 950001
	dir, err := derivDirFor(mount, inode)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	staged, abs, err := derivatives.StageNameAt(dir, "waveform.json")
	if err != nil {
		t.Fatal(err)
	}
	// The generator "succeeds" without writing anything.
	if fi, serr := os.Stat(abs); serr != nil || fi.Size() != 0 {
		t.Fatalf("expected a 0-byte placeholder, got %v (err %v)", fi, serr)
	}
	if err := derivatives.CommitStagedAt(dir, staged, "waveform.json"); err == nil {
		t.Fatal("committed a 0-byte placeholder as a real derivative — " +
			"/blob would serve 200 with no bytes instead of 404ing and letting the reader regenerate")
	}
	// And it must not leave the empty file behind under either name.
	if _, err := os.Lstat(filepath.Join(mount, derivatives.DerivBlobRel(inode, "waveform.json"))); err == nil {
		t.Error("empty blob published under the final name")
	}
	if _, err := os.Lstat(abs); err == nil {
		t.Error("empty placeholder left behind")
	}

	// A generator that DID write commits normally.
	staged2, abs2, err := derivatives.StageNameAt(dir, "poster.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs2, []byte("REAL"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := derivatives.CommitStagedAt(dir, staged2, "poster.jpg"); err != nil {
		t.Fatalf("refused a real blob: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(mount, derivatives.DerivBlobRel(inode, "poster.jpg")))
	if string(got) != "REAL" {
		t.Errorf("committed blob = %q", got)
	}
}

// updated_at must land AFTER a consumer's cursor. Clamping to the asset's
// SourceMtime was stable across sweeps but put skewed farm rows BEFORE every
// cursor, making them permanently invisible on the changes feed with no
// attacker involved — one farm host with a fast clock was enough.
func TestUpdatedAtClampStaysVisibleToTheChangesFeed(t *testing.T) {
	blob := "poster.jpg"
	oldMtime := nowUnix() - 365*24*3600
	row := derivatives.DerivRow{
		Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1,
		BlobRelPath: &blob,
		SourceMtime: &oldMtime, SourceSize: i64(1024),
		UpdatedAt: nowUnix() + 600, // a farm host 10 minutes ahead
	}
	got, ok := sanitizeSidecarRow(row)
	if !ok {
		t.Fatal("row dropped")
	}
	cursor := nowUnix() - 3600 // a consumer that polled an hour ago
	if got.UpdatedAt <= cursor {
		t.Errorf("updated_at = %d is at or before a cursor of %d — this derivative "+
			"is invisible on /derivatives/changes and can never self-heal",
			got.UpdatedAt, cursor)
	}
	if got.UpdatedAt > nowUnix()+updatedAtSkewSlack {
		t.Errorf("updated_at = %d is still in the future", got.UpdatedAt)
	}
}

// ...and the churn that clamping was originally dodging is fixed where it
// belongs: an unchanged manifest must not re-publish its rows.
func TestUnchangedManifestDoesNotChurnTheFeed(t *testing.T) {
	mount := t.TempDir()
	const inode = 950002
	dir := filepath.Join(mount, derivatives.DerivDirRel(inode))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "poster.jpg"), []byte("jpg"), 0o644); err != nil {
		t.Fatal(err)
	}
	blob := "poster.jpg"
	sz, mt := int64(1024), nowUnix()-7200
	sc := ManifestSidecar{Inode: inode, Derivatives: []derivatives.DerivRow{{
		Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1,
		BlobRelPath: &blob, SourceSize: &sz, SourceMtime: &mt,
		UpdatedAt: nowUnix() + 86400*365, // implausible → clamped every sweep
	}}}
	writeSidecarFixture(t, mount, inode, sc)

	store, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// ADVANCE THE CLOCK between sweeps. Without this the test passes trivially:
	// both sweeps clamp to the same nowUnix() second, so an identical stamp
	// proves nothing about whether the row was rewritten. (Caught by a neuter
	// run — removing the skip left the test green.)
	realNow := nowUnix
	defer func() { nowUnix = realNow }()
	clock := realNow()
	nowUnix = func() int64 { return clock }

	if _, err := ReconcileOneSidecar(store, mount, inode); err != nil {
		t.Fatal(err)
	}
	first, _ := store.Manifest(inode)

	clock += 3600 // an hour later, next periodic sweep
	// Re-ingest the SAME file, as the periodic sweep does.
	reconcileOneSidecarInto(store, mount, inode)
	second, _ := store.Manifest(inode)

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("row counts %d then %d", len(first), len(second))
	}
	if first[0].UpdatedAt != second[0].UpdatedAt {
		t.Errorf("unchanged manifest re-stamped %d -> %d — every sweep would "+
			"re-emit this derivative on /derivatives/changes forever",
			first[0].UpdatedAt, second[0].UpdatedAt)
	}
}

func writeSidecarFixture(t *testing.T, mount string, inode uint64, sc ManifestSidecar) {
	t.Helper()
	b, err := json.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mount, derivatives.DerivBlobRel(inode, "manifest.json")), b, 0o644); err != nil {
		t.Fatal(err)
	}
}
