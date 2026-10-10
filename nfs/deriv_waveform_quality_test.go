package nfs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// wf writes a waveform.json with the given shape. data is not parsed by the
// comparator, so a token array keeps the fixtures small.
func wf(t *testing.T, dir, name string, spp, length int) string {
	return wfRate(t, dir, name, 48000, spp, length)
}

// wfRate is the same but names the sample rate, which is load-bearing: the two
// producers do NOT work at the same rate.
func wfRate(t *testing.T, dir, name string, rate, spp, length int) string {
	t.Helper()
	p := filepath.Join(dir, name)
	body := fmt.Sprintf(`{"version":2,"channels":1,"sample_rate":%d,`+
		`"samples_per_pixel":%d,"bits":8,"length":%d,"data":[0,0]}`, rate, spp, length)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

// THE SHORT-CLIP CASE, which is what the live collisions actually are.
//
// Inode 1597077 on 2026-08-19: 12.9 s of audio. The farm produced 607 pixels at
// samples_per_pixel 1024; ClipLogger produced ~2,000 pixels of the same audio.
// Ownership says keep the farm's. Resolution says the client's has 3.3x the
// detail, and downsampling is possible where upsampling is not.
func TestClientPreviewWinsOnAShortClip(t *testing.T) {
	d := t.TempDir()
	// 607 x 1024 = 621,568 samples ~= 12.9 s at 48 kHz. Same audio, 2,000 px.
	existing := wf(t, d, "farm.json", 1024, 607)
	incoming := wf(t, d, "client.json", 311, 2000)

	finer, ok := waveformIsFiner(incoming, existing)
	if !ok {
		t.Fatal("the comparison should be answerable — both are valid v2 over the same audio")
	}
	if !finer {
		t.Error("a 2,000-pixel waveform of 12.9 s lost to a 607-pixel one — this is the " +
			"live case, and refusing it is what stranded every short-clip contribution")
	}
}

// THE LONG-INTERVIEW CASE, which is why the guard was built. It must still win.
func TestFarmFullResolutionWinsOnALongInterview(t *testing.T) {
	d := t.TempDir()
	// 149,166 x 1,024 ~= 53 minutes — the 2026-08-11 artifact.
	existing := wf(t, d, "farm.json", 1024, 149166)
	incoming := wf(t, d, "client.json", 76373, 2000)

	finer, ok := waveformIsFiner(incoming, existing)
	if !ok {
		t.Fatal("the comparison should be answerable")
	}
	if finer {
		t.Error("a 2,000-pixel preview beat a 149,166-pixel full-resolution waveform — " +
			"this is exactly the downgrade the guard was created to prevent")
	}
}

// FAIL CLOSED. Every way the question can go unanswered must report ok=false,
// because the caller keeps the existing blob on ok=false and losing detail is
// the direction that cannot be undone.
func TestUnanswerableComparisonsFailClosed(t *testing.T) {
	d := t.TempDir()
	good := wf(t, d, "good.json", 1024, 600)

	bad := filepath.Join(d, "bad.json")
	if err := os.WriteFile(bad, []byte("this is not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	v1 := filepath.Join(d, "v1.json")
	if err := os.WriteFile(v1, []byte(`{"version":1,"length":9000,"samples_per_pixel":64}`), 0o644); err != nil {
		t.Fatal(err)
	}
	empty := wf(t, d, "empty.json", 1024, 0) // length 0 — nothing to compare

	for _, tc := range []struct{ name, in, ex string }{
		{"incoming unparseable", bad, good},
		{"existing unparseable", good, bad},
		{"incoming missing", filepath.Join(d, "nope.json"), good},
		{"existing missing", good, filepath.Join(d, "nope.json")},
		{"wrong schema version", v1, good},
		{"zero length", empty, good},
	} {
		if _, ok := waveformIsFiner(tc.in, tc.ex); ok {
			t.Errorf("%s: the comparison claimed to be answerable; it must fail closed", tc.name)
		}
	}
}

// DIFFERENT AUDIO IS NOT COMPARABLE. If the two blobs cover materially different
// amounts of audio they are not two renderings of one source, and pixel counts
// then measure different things — a 2,000-pixel waveform of ten seconds is not
// "finer" than a 1,000-pixel waveform of an hour.
func TestDifferentDurationsAreNotComparable(t *testing.T) {
	d := t.TempDir()
	existing := wf(t, d, "hour.json", 1024, 149166) // ~53 min
	incoming := wf(t, d, "clip.json", 311, 2000)    // ~13 s

	if _, ok := waveformIsFiner(incoming, existing); ok {
		t.Error("blobs covering wildly different audio were treated as comparable")
	}

	// Within tolerance the comparison IS allowed: the two tools will not agree
	// to the sample, and demanding exactness would fail closed on every real pair.
	near := wf(t, d, "near.json", 1024, 600)
	almost := wf(t, d, "almost.json", 1024, 610) // 1.6% apart
	if _, ok := waveformIsFiner(almost, near); !ok {
		t.Error("a 1.6% duration difference was rejected — real pairs never agree exactly")
	}
}

// Only waveform blobs take this path; nothing else in the tree is affected.
func TestOnlyWaveformBlobsAreCompared(t *testing.T) {
	for _, p := range []string{
		".juicemount/derivatives/1/waveform.json",
	} {
		if !isWaveformBlob(p) {
			t.Errorf("%s should be treated as a waveform blob", p)
		}
	}
	for _, p := range []string{
		".juicemount/derivatives/1/poster.jpg",
		".juicemount/derivatives/1/manifest.json",
		".juicemount/derivatives/1/strip.jpg",
		".juicemount/derivatives/1/._waveform.json",
	} {
		if isWaveformBlob(p) {
			t.Errorf("%s must NOT be treated as a waveform blob", p)
		}
	}
}

// END TO END through the guard, which is what the drain actually calls.
// The comparator being right is not enough — the guard has to consult it, and
// only for waveforms.
func TestGuardAllowsAFinerWaveformAndStillRefusesACoarserOne(t *testing.T) {
	d := t.TempDir()
	// Stand in for the other producer, exactly as the ownership tests do: the
	// real collision is a root container vs a uid-501 desktop app, which an
	// unprivileged test cannot stage.
	orig := derivClobberEUID
	derivClobberEUID = func() int { return os.Geteuid() + 1 }
	defer func() { derivClobberEUID = orig }()

	rel := ".juicemount/derivatives/1597077/waveform.json"

	// The live short-clip case: existing 607 px, incoming 2,000 px of the same
	// 12.9 s. The guard must step aside.
	existing := wf(t, d, "dest.json", 1024, 607)
	finerIn := wf(t, d, "in-finer.json", 311, 2000)
	if err := checkDerivClobber(rel, existing, finerIn, 14094); err != nil {
		t.Errorf("the guard refused a strictly finer waveform: %v", err)
	}

	// The long-interview case inverted: incoming is the coarse preview. Refuse.
	coarseDest := wf(t, d, "dest2.json", 1024, 149166)
	coarseIn := wf(t, d, "in-coarse.json", 76373, 2000)
	if err := checkDerivClobber(rel, coarseDest, coarseIn, 11541); err == nil {
		t.Error("the guard allowed a 2,000-pixel preview over a full-resolution waveform")
	}

	// Unparseable incoming: keep what is there.
	junk := filepath.Join(d, "junk.json")
	if err := os.WriteFile(junk, []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkDerivClobber(rel, existing, junk, 4); err == nil {
		t.Error("the guard allowed an unparseable contribution to overwrite a real waveform")
	}

	// A NON-waveform blob must be unaffected — poster.jpg still refuses purely
	// on ownership, with no content inspection.
	poster := ".juicemount/derivatives/1597077/poster.jpg"
	if err := checkDerivClobber(poster, existing, finerIn, 999); err == nil {
		t.Error("a poster collision was allowed; only waveforms are resolution-judged")
	}
}

// THE REAL PAIR THAT THE FIRST IMPLEMENTATION GOT WRONG.
//
// Inode 1596857, taken off the live spool on 2026-08-19. The two producers do
// not work at the same sample rate — the farm renders at the source's 48 kHz,
// ClipLogger downsamples to 8 kHz first — so comparing raw SAMPLE COUNTS makes
// 368,640 against 62,000 look like an 83% mismatch when it is 7.68 s against
// 7.75 s, 0.9% apart. That rejected all 89 live contributions as "not the same
// audio" and failed closed on every one.
func TestRealWorldPairAtDifferentSampleRates(t *testing.T) {
	d := t.TempDir()
	farm := wfRate(t, d, "farm.json", 48000, 1024, 360)   // 7.68 s
	client := wfRate(t, d, "client.json", 8000, 31, 2000) // 7.75 s

	finer, ok := waveformIsFiner(client, farm)
	if !ok {
		t.Fatal("the real live pair was judged incomparable — comparing sample counts " +
			"instead of DURATION is what rejected all 89 contributions")
	}
	if !finer {
		t.Error("2,000 pixels lost to 360 for the same 7.7 s of audio")
	}
}

// A missing or zero sample rate makes duration unknowable, so it must fail closed
// rather than silently comparing sample counts again.
func TestMissingSampleRateFailsClosed(t *testing.T) {
	d := t.TempDir()
	good := wf(t, d, "good.json", 1024, 600)
	noRate := filepath.Join(d, "norate.json")
	if err := os.WriteFile(noRate, []byte(
		`{"version":2,"length":2000,"samples_per_pixel":31,"data":[0,0]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := waveformIsFiner(noRate, good); ok {
		t.Error("a document with no sample_rate was treated as comparable")
	}
}

// A PERCENTAGE IS THE WRONG SHAPE AT SHORT DURATIONS.
//
// The two contributions the relative-only tolerance still refused, measured off
// the live spool on 2026-08-19: 2.11 s against 2.00 s (5.3%) and 1.34 s against
// 1.25 s (7.0%). Both are the same clip; the absolute disagreement is 0.11 s and
// 0.09 s. The client just reports a coarser duration, and at one or two seconds
// a tenth of a second is a large percentage of nothing.
func TestShortClipsAreComparableDespiteTheRelativeTolerance(t *testing.T) {
	d := t.TempDir()
	for _, tc := range []struct {
		name                 string
		exRate, exSpp, exLen int
		inRate, inSpp, inLen int
	}{
		// 2.11 s @ 99 px  vs  2.00 s @ 2,000 px  — 5.3% apart, 0.11 s
		{"1596899", 48000, 1024, 99, 8000, 8, 2000},
		// 1.34 s @ 63 px  vs  1.25 s @ 2,000 px  — 7.0% apart, 0.09 s
		{"1596967", 48000, 1024, 63, 8000, 5, 2000},
	} {
		ex := wfRate(t, d, tc.name+"-farm.json", tc.exRate, tc.exSpp, tc.exLen)
		in := wfRate(t, d, tc.name+"-client.json", tc.inRate, tc.inSpp, tc.inLen)
		finer, ok := waveformIsFiner(in, ex)
		if !ok {
			t.Errorf("%s: judged incomparable — a tenth of a second on a ~2 s clip is not "+
				"a different recording", tc.name)
			continue
		}
		if !finer {
			t.Errorf("%s: %d pixels lost to %d", tc.name, tc.inLen, tc.exLen)
		}
	}
}

// The absolute floor must not swallow the relative test on long clips: a quarter
// second is nothing there, so genuinely different audio is still refused.
func TestTheAbsoluteFloorDoesNotWeakenLongClips(t *testing.T) {
	d := t.TempDir()
	hour := wfRate(t, d, "hour.json", 48000, 1024, 149166) // ~53 min
	clip := wfRate(t, d, "clip.json", 8000, 31, 2000)      // ~7.8 s
	if _, ok := waveformIsFiner(clip, hour); ok {
		t.Error("a 7.8 s waveform was compared against a 53 minute one")
	}
}
