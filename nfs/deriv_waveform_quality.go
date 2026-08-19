package nfs

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path"
)

// deriv_waveform_quality.go — decide which of two waveform.json blobs is finer.
//
// WHY OWNERSHIP ALONE IS THE WRONG DISCRIMINATOR (measured 2026-08-19).
//
// The clobber guard refuses any cross-owner overwrite because, on 2026-08-11, a
// client preview would have replaced a full-resolution farm waveform. That was
// true of the asset it was measured on — and it is not a property of the
// producers. The two use different strategies:
//
//	farm         fixed samples_per_pixel (1024)  -> resolution scales WITH duration
//	ClipLogger   fixed ~2,000-pixel preview      -> resolution scales INVERSELY
//
// They are equal at 2000 x 1024 / 48000 = 42.7 SECONDS of audio. The 2026-08-11
// case was ~53 minutes, deep in farm-wins territory. A year of short social
// clips is the other side: of 14 live collisions sampled on 2026-08-19, every
// one was 4-34 s and the CLIENT's waveform was finer — 607 px vs ~2,000 px on
// inode 1597077. The guard was refusing the better artifact every time.
//
// So compare the artifacts instead of their authors. More pixels over the same
// audio is strictly more detail, and downsampling is possible where upsampling
// is not, so the finer one is the one worth keeping.

// waveformDoc is the subset of audiowaveform v2 needed to judge resolution.
type waveformDoc struct {
	Version         int `json:"version"`
	SampleRate      int `json:"sample_rate"`
	SamplesPerPixel int `json:"samples_per_pixel"`
	Length          int `json:"length"`
}

// maxWaveformBytes caps what will be read for a comparison. A full-resolution
// waveform runs to hundreds of KB; anything past this is not a waveform we
// should be parsing on the drain path.
const maxWaveformBytes = 8 << 20

// waveformDurationTolerance is how far two blobs' total sample counts may differ
// and still be treated as describing the same audio. They are derived from the
// same source by different tools AT DIFFERENT SAMPLE RATES, so exact agreement
// is not expected; a large disagreement means they are NOT the same audio and
// pixel counts are then not comparable at all.
const waveformDurationTolerance = 0.05

// isWaveformBlob reports whether a derivative path names a waveform blob.
func isWaveformBlob(nfsPath string) bool { return path.Base(nfsPath) == "waveform.json" }

// readWaveformDoc parses a waveform blob, refusing anything that is not a
// well-formed v2 document with the fields the comparison needs.
func readWaveformDoc(p string) (*waveformDoc, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxWaveformBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxWaveformBytes {
		return nil, fmt.Errorf("waveform %s exceeds %d bytes", p, maxWaveformBytes)
	}
	var d waveformDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, err
	}
	// sample_rate is required, not optional: without it the document's duration
	// is unknowable and two blobs at different rates cannot be compared at all.
	if d.Version != 2 || d.Length <= 0 || d.SamplesPerPixel <= 0 || d.SampleRate <= 0 {
		return nil, fmt.Errorf("waveform %s is not a usable v2 document", p)
	}
	return &d, nil
}

// durationSeconds is how much audio the document covers.
//
// It MUST divide by the sample rate. Comparing raw sample counts looks
// equivalent and is not, because the two producers do not work at the same
// rate: the farm renders at the source's 48 kHz while ClipLogger downsamples to
// 8 kHz first. On inode 1596857 that is 360 px x 1024 spp = 368,640 samples for
// the farm against 2,000 x 31 = 62,000 for the client — an 83% "mismatch" that
// is really 7.68 s versus 7.75 s, 0.9% apart. Comparing sample counts rejected
// every real pair as "not the same audio" and failed closed on all 89 of them.
func (d *waveformDoc) durationSeconds() float64 {
	if d.SampleRate <= 0 {
		return 0
	}
	return float64(d.Length) * float64(d.SamplesPerPixel) / float64(d.SampleRate)
}

// waveformIsFiner reports whether the INCOMING waveform has more detail than the
// EXISTING one, and whether the question could be answered at all.
//
// ok=false means DO NOT DECIDE ON THIS BASIS — either blob unreadable, either
// not a v2 document, or the two cover materially different amounts of audio so
// their pixel counts describe different things. The caller must fail closed on
// ok=false: keeping what is already there is the recoverable mistake, and
// upsampling a preview back to full resolution is not possible.
func waveformIsFiner(incomingPath, existingPath string) (finer, ok bool) {
	in, err := readWaveformDoc(incomingPath)
	if err != nil {
		return false, false
	}
	ex, err := readWaveformDoc(existingPath)
	if err != nil {
		return false, false
	}
	a, b := in.durationSeconds(), ex.durationSeconds()
	if a <= 0 || b <= 0 {
		return false, false
	}
	if math.Abs(a-b)/math.Max(a, b) > waveformDurationTolerance {
		return false, false // not the same audio; pixel counts are not comparable
	}
	return in.Length > ex.Length, true
}
