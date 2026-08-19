package nfs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// FIXTURES ARE REAL BYTES. Both bodies below were captured verbatim from `._`
// files sitting on the live /Volumes/zpool volume on 2026-08-18, gzipped and
// base64'd only to keep them in one file. Hand-built AppleDouble fixtures were
// deliberately avoided: the first hand-built parser in this work had the ATTR
// header at the wrong offset (+32 instead of +34) and cheerfully reported that
// every sidecar on the volume was attribute-free. Real bytes caught that.

// adEmptyReal is a `._` file for a file with no user metadata: two entries,
// 32 zero bytes of Finder info, the canonical 286-byte blank resource fork,
// and one `com.apple.provenance` xattr — the shape 56 of 60 sampled files had.
const adEmptyReal = "H4sICCCsKWoCAy5fLmJ1bGtyZXByby0yAGNgFWNnYGJg8E1MVvAPVohQgAKQGAMnEBsxMPBtANJAPt8jBgZGOQaCwDEkJAjCAulgmAHE3GhKGBHiosn5uXqJBQU5qXoFRfllqXmJecmpDIxMDOyzZofstL7EMApGwSgYBaNgFIyCUTAKRsEoGAWjYBQQBRihGAzkQjIyixWKUovzS4uSUxXS8ouyFTLzSlLzSjLz8xJzcioVclLTShSSchLzskH94GHnf7iwDIPc//8ArQ+rdQAQAAA="

// adFinderInfoReal is a `._` file whose Finder info block is NOT zero (this one
// belonged to a directory carrying real Finder state). It must never be elided.
const adFinderInfoReal = "H4sICJd6TGoCAy5fVGlrVG9rIFNvdW5kIFZhdWx0AGNgFWNnYGJg8E1MVvAPVohQgAKQGAMnEBsxMPBtANJAPt8jBgZGOQYcoAHOcgwJCYKwQDoYZgAxN5piRoS4aHJ+rl5iQUFOql5BUX5Zal5iXnIqAyMTA/us2SE7rS8xjIJRMApGwSgYBaNgFIyCUTAKRsEoGAVEAUYoBgO5kIzMYoWi1OL80qLkVIW0/KJshcy8ktS8ksz8vMScnEqFnNS0EoWknMS8bFA/eNj5Hy4swyD3/z8A1V7t7wAQAAA="

func mustFixture(t *testing.T, b64 string) []byte {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("gunzip fixture: %v", err)
	}
	body, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return body
}

func TestAppleDoubleEmptyRealBodyIsElidable(t *testing.T) {
	body := mustFixture(t, adEmptyReal)
	if len(body) != 4096 {
		t.Fatalf("fixture size = %d, want 4096", len(body))
	}
	if !appleDoubleIsDefaultEmpty(body) {
		t.Fatal("a real metadata-free `._` body was NOT recognised as elidable — " +
			"the whole optimisation is dead if this fails")
	}
}

// TestAppleDoubleRefusesRealFinderInfo is the primary NEUTER check: a REAL
// sidecar that carries real Finder state must be materialised.
func TestAppleDoubleRefusesRealFinderInfo(t *testing.T) {
	body := mustFixture(t, adFinderInfoReal)
	if appleDoubleIsDefaultEmpty(body) {
		t.Fatal("a `._` body with a non-zero Finder info block was elided — " +
			"this silently loses a user's Finder tags/labels")
	}
}

// TestAppleDoubleRefusesRealXattr takes the REAL empty body and renames its one
// ignorable attribute to a same-length name that is NOT ignorable. Every other
// byte — offsets, lengths, padding, the ATTR header — stays exactly as macOS
// wrote it, so this isolates the attribute-name decision and nothing else.
func TestAppleDoubleRefusesRealXattr(t *testing.T) {
	body := mustFixture(t, adEmptyReal)
	old := []byte("com.apple.provenance")
	nw := []byte("com.adobe.premiere.x") // same length, not ignorable
	i := bytes.Index(body, old)
	if i < 0 {
		t.Fatal("fixture no longer contains com.apple.provenance")
	}
	copy(body[i:], nw)
	if appleDoubleIsDefaultEmpty(body) {
		t.Fatalf("a `._` body carrying the real xattr %q was elided — user metadata would be lost", nw)
	}
}

// TestAppleDoubleFailsClosedOnDamage: truncated or corrupt bodies must be
// materialised, never assumed empty.
func TestAppleDoubleFailsClosedOnDamage(t *testing.T) {
	good := mustFixture(t, adEmptyReal)

	cases := []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"one byte", []byte{0}},
		{"header only, no entries", good[:26]},
		{"truncated mid entry table", good[:30]},
		{"truncated after entry table", good[:64]},
		{"truncated to half", good[:2048]},
		{"random garbage", bytes.Repeat([]byte{0xAB}, 4096)},
		{"all zeros", make([]byte, 4096)},
	}
	for _, tc := range cases {
		if appleDoubleIsDefaultEmpty(tc.body) {
			t.Errorf("%s: damaged body was treated as empty — must fail closed", tc.name)
		}
	}

	// Mutations of the otherwise-valid body.
	mut := func(name string, f func(b []byte)) {
		b := append([]byte(nil), good...)
		f(b)
		if appleDoubleIsDefaultEmpty(b) {
			t.Errorf("%s: was treated as empty — must fail closed", name)
		}
	}
	mut("bad magic", func(b []byte) { b[0] = 0xFF })
	mut("bad version", func(b []byte) { b[4] = 0x09 })
	mut("entry count 3", func(b []byte) { b[25] = 3 })
	mut("entry count 0", func(b []byte) { b[25] = 0 })
	mut("unknown entry id", func(b []byte) { b[29] = 7 })         // Finder info id 9 -> 7
	mut("finder info dirty", func(b []byte) { b[50] = 1 })        // first Finder info byte
	mut("entry offset past EOF", func(b []byte) { b[32] = 0xFF }) // Finder info offset high byte
}

// TestAppleDoubleKillSwitch: the env gate defaults ON and "0" turns it off.
func TestAppleDoubleKillSwitch(t *testing.T) {
	t.Setenv("JM_DRAIN_SKIP_EMPTY_SIDECARS", "")
	if !drainSkipEmptySidecarsEnabled() {
		t.Fatal("elision must default ON when the env var is unset")
	}
	t.Setenv("JM_DRAIN_SKIP_EMPTY_SIDECARS", "0")
	if drainSkipEmptySidecarsEnabled() {
		t.Fatal("JM_DRAIN_SKIP_EMPTY_SIDECARS=0 must disable the elision")
	}
}

// TestSidecarNameClassificationIsShared pins the reuse of the existing
// read-side classifier rather than a second copy of the `._` rule.
func TestSidecarNameClassificationIsShared(t *testing.T) {
	for _, n := range []string{"._Foo", "._a", "._.DS_Store"} {
		if !isSidecarName(n) {
			t.Errorf("isSidecarName(%q) = false, want true", n)
		}
	}
	for _, n := range []string{"Foo", ".DS_Store", "._", "_.x", ".x"} {
		if isSidecarName(n) {
			t.Errorf("isSidecarName(%q) = true, want false", n)
		}
	}
}

// ---- drainer integration ----

// TestDrainerElidesEmptySidecar proves the end-to-end behaviour: a metadata-free
// `._` row is completed WITHOUT a backend file, and its mirror entry is handed
// to the skip hook so the path reads as absent.
func TestDrainerElidesEmptySidecar(t *testing.T) {
	t.Setenv("JM_DRAIN_SKIP_EMPTY_SIDECARS", "")
	spool, d := newTestDrainer(t, DrainerConfig{})
	d.skipEmptySidecars = drainSkipEmptySidecarsEnabled()

	var skipped []string
	d.SetOnSidecarSkip(func(p string) bool { skipped = append(skipped, p); return true })

	body := mustFixture(t, adEmptyReal)
	e := writeSpoolEntry(t, spool, "/Films/._clip.mov", body)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if n := d.DrainOnceForTest(ctx); n != 1 {
		t.Fatalf("rows processed = %d, want 1", n)
	}

	dest := filepath.Join(d.fuseRoot, "Films/._clip.mov")
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("the sidecar was materialised on the backend (stat err = %v) — the create was not elided", err)
	}
	if len(skipped) != 1 || skipped[0] != "/Films/._clip.mov" {
		t.Fatalf("skip hook calls = %v, want exactly [/Films/._clip.mov]", skipped)
	}
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState != metadata.DrainDone {
		t.Fatalf("row not marked done: %+v", row)
	}
	if got := d.metrics.SidecarsSkipped.Load(); got != 1 {
		t.Errorf("SidecarsSkipped = %d, want 1", got)
	}
	// The elision touched no object store, so it must NOT count as proof the
	// backend is reachable (that would suppress a reachability probe).
	if !d.LastDrainSuccess().IsZero() {
		t.Error("an elided sidecar stamped drain liveness — it never reached the backend")
	}
	if got := d.metrics.BytesDrained.Load(); got != 0 {
		t.Errorf("BytesDrained = %d, want 0 (no bytes left this machine)", got)
	}
}

// TestDrainerMaterialisesRealSidecar is the drainer-level companion refusal:
// a sidecar carrying real Finder state still lands on the backend, byte-exact.
func TestDrainerMaterialisesRealSidecar(t *testing.T) {
	t.Setenv("JM_DRAIN_SKIP_EMPTY_SIDECARS", "")
	spool, d := newTestDrainer(t, DrainerConfig{})
	d.skipEmptySidecars = drainSkipEmptySidecarsEnabled()
	d.SetOnSidecarSkip(func(p string) bool {
		t.Errorf("skip hook called for a sidecar with real Finder info: %s", p)
		return true
	})

	body := mustFixture(t, adFinderInfoReal)
	writeSpoolEntry(t, spool, "/Films/._tagged", body)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if n := d.DrainOnceForTest(ctx); n != 1 {
		t.Fatalf("rows processed = %d, want 1", n)
	}
	got, err := os.ReadFile(filepath.Join(d.fuseRoot, "Films/._tagged"))
	if err != nil {
		t.Fatalf("sidecar with real metadata was NOT materialised: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Error("materialised sidecar differs from the spooled bytes")
	}
}

// TestDrainerKillSwitchMaterialisesEverything: with the env kill switch set,
// even a provably-empty sidecar goes to the backend (field revert, no rebuild).
func TestDrainerKillSwitchMaterialisesEverything(t *testing.T) {
	t.Setenv("JM_DRAIN_SKIP_EMPTY_SIDECARS", "0")
	spool, d := newTestDrainer(t, DrainerConfig{})
	d.skipEmptySidecars = drainSkipEmptySidecarsEnabled()
	d.SetOnSidecarSkip(func(p string) bool {
		t.Errorf("skip hook called while the kill switch was set: %s", p)
		return true
	})

	writeSpoolEntry(t, spool, "/Films/._clip.mov", mustFixture(t, adEmptyReal))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d.DrainOnceForTest(ctx)
	if _, err := os.Stat(filepath.Join(d.fuseRoot, "Films/._clip.mov")); err != nil {
		t.Fatalf("kill switch did not restore materialisation: %v", err)
	}
}

// TestDrainerSkipVetoFallsBackToNormalDrain: a hook veto (live NFS handle on
// the path) must produce a normal drain, never a dropped row.
func TestDrainerSkipVetoFallsBackToNormalDrain(t *testing.T) {
	t.Setenv("JM_DRAIN_SKIP_EMPTY_SIDECARS", "")
	spool, d := newTestDrainer(t, DrainerConfig{})
	d.skipEmptySidecars = drainSkipEmptySidecarsEnabled()
	d.SetOnSidecarSkip(func(string) bool { return false })

	body := mustFixture(t, adEmptyReal)
	writeSpoolEntry(t, spool, "/Films/._clip.mov", body)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d.DrainOnceForTest(ctx)

	got, err := os.ReadFile(filepath.Join(d.fuseRoot, "Films/._clip.mov"))
	if err != nil {
		t.Fatalf("vetoed skip did not fall back to a normal drain: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Error("vetoed drain wrote the wrong bytes")
	}
}

// TestDrainerNonSidecarNeverElided: a real user file the size of a sidecar,
// even one whose bytes happen to be an empty AppleDouble, must always drain.
func TestDrainerNonSidecarNeverElided(t *testing.T) {
	t.Setenv("JM_DRAIN_SKIP_EMPTY_SIDECARS", "")
	spool, d := newTestDrainer(t, DrainerConfig{})
	d.skipEmptySidecars = drainSkipEmptySidecarsEnabled()
	d.SetOnSidecarSkip(func(p string) bool {
		t.Errorf("skip hook called for a NON-sidecar path: %s", p)
		return true
	})

	writeSpoolEntry(t, spool, "/Films/clip.mov", mustFixture(t, adEmptyReal))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d.DrainOnceForTest(ctx)
	if _, err := os.Stat(filepath.Join(d.fuseRoot, "Films/clip.mov")); err != nil {
		t.Fatalf("a non-sidecar file was elided: %v", err)
	}
}
