package farm

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestFoldArgsForCount pins the stream-count → ffmpeg-args mapping (no media file).
func TestFoldArgsForCount(t *testing.T) {
	if _, ok := foldArgsForCount(0, 16000); ok {
		t.Fatal("0 streams must be ok=false (no audio)")
	}
	one, ok := foldArgsForCount(1, 16000)
	if !ok || strings.Join(one, " ") != "-map 0:a:0 -ac 1 -ar 16000" {
		t.Fatalf("1 stream args wrong: %v", one)
	}
	multi, ok := foldArgsForCount(4, 48000)
	if !ok {
		t.Fatal("4 streams must be ok")
	}
	joined := strings.Join(multi, " ")
	// Every stream gets a per-input mono cast (the anti-"No channel layout" fix),
	// all four merge, output forced mono at the requested rate.
	for i := 0; i < 4; i++ {
		if !strings.Contains(joined, "[0:a:"+strconv.Itoa(i)+"]aformat=channel_layouts=mono[a"+strconv.Itoa(i)+"]") {
			t.Fatalf("missing per-input mono cast for stream %d: %s", i, joined)
		}
	}
	if !strings.Contains(joined, "amerge=inputs=4[m]") || !strings.Contains(joined, "-map [m] -ac 1 -ar 48000") {
		t.Fatalf("4-stream merge args wrong: %s", joined)
	}
}

// TestAudioFoldDownMergesAllStreams is the real proof of the silent-data-loss
// fix: a 2-stream file whose FIRST stream is silent and SECOND carries a tone
// (mimicking a dead-first-mic camera clip). The old -map 0:a:0 would extract
// silence; the fold-down amerge must surface the tone. Skips without ffmpeg.
func TestAudioFoldDownMergesAllStreams(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not available")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "two.mkv")
	gen := exec.Command(ffmpeg, "-v", "error", "-y",
		"-f", "lavfi", "-i", "anullsrc=r=48000:cl=mono",
		"-f", "lavfi", "-i", "sine=frequency=440:r=48000",
		"-map", "0:a", "-map", "1:a", "-t", "1", "-c:a", "pcm_s24le", src)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("generate 2-stream fixture: %v\n%s", err, out)
	}

	args, ok, err := audioFoldArgs(ffmpeg, src, 16000)
	if err != nil || !ok {
		t.Fatalf("audioFoldArgs: ok=%v err=%v", ok, err)
	}
	if !strings.Contains(strings.Join(args, " "), "amerge=inputs=2") {
		t.Fatalf("expected 2-stream amerge, got %v", args)
	}

	out := filepath.Join(dir, "fold.wav")
	full := append([]string{"-v", "error", "-y", "-i", src}, args...)
	full = append(full, "-c:a", "pcm_s16le", out)
	if o, err := exec.Command(ffmpeg, full...).CombinedOutput(); err != nil {
		t.Fatalf("fold-down run failed: %v\n%s", err, o)
	}

	mean := meanVolumeDB(t, ffmpeg, out)
	if mean < -60 {
		t.Fatalf("folded output is silent (mean_volume %.1f dB) — the tone stream was dropped (the bug)", mean)
	}
	t.Logf("fold-down mean_volume %.1f dB (tone from stream 1 survived)", mean)
}

var meanVolRe = regexp.MustCompile(`mean_volume:\s*(-?[0-9.]+) dB`)

// TestWaveformFoldsAllStreams proves the farm WAVEFORM (not just the transcript)
// surfaces audio from a non-first stream: a 2-stream file (silent first, tone
// second) must produce non-flatline peaks. The old -map 0:a:0 flatlined.
func TestWaveformFoldsAllStreams(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not available")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "two.mkv")
	gen := exec.Command(ffmpeg, "-v", "error", "-y",
		"-f", "lavfi", "-i", "anullsrc=r=48000:cl=mono",
		"-f", "lavfi", "-i", "sine=frequency=440:r=48000",
		"-map", "0:a", "-map", "1:a", "-t", "1", "-c:a", "pcm_s24le", src)
	if o, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("generate 2-stream fixture: %v\n%s", err, o)
	}
	// Waveform writes through a DIRECTORY DESCRIPTOR now, not a path.
	outDir, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer outDir.Close()
	out := filepath.Join(dir, "wave.json")
	n, err := Waveform(ffmpeg, src, outDir, "wave.json", 1024)
	if err != nil {
		t.Fatalf("Waveform: %v", err)
	}
	if n == 0 {
		t.Fatal("waveform produced 0 pixels")
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var wf WaveformJSON
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse waveform json: %v", err)
	}
	nonzero := 0
	for _, v := range wf.Data {
		if v != 0 {
			nonzero++
		}
	}
	if nonzero == 0 {
		t.Fatalf("waveform is a FLATLINE (all-zero peaks across %d samples) — the tone stream was dropped (the bug)", len(wf.Data))
	}
	t.Logf("waveform non-flat: %d/%d non-zero peak samples (tone from stream 1 survived)", nonzero, len(wf.Data))
}

func meanVolumeDB(t *testing.T, ffmpeg, path string) float64 {
	t.Helper()
	det := exec.Command(ffmpeg, "-v", "info", "-i", path, "-af", "volumedetect", "-f", "null", "-")
	out, _ := det.CombinedOutput()
	m := meanVolRe.FindStringSubmatch(string(out))
	if m == nil {
		t.Fatalf("no mean_volume in volumedetect output:\n%s", out)
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		t.Fatalf("parse mean_volume %q: %v", m[1], err)
	}
	return v
}

// AN UNDECODABLE STREAM MUST NOT COST THE WHOLE WAVEFORM.
//
// An iPhone .MOV shot with spatial audio carries an ordinary AAC mix plus a
// second stream in Apple's APAC, whose decoder arrived in ffmpeg 7.1. Naming
// both in the fold graph makes ffmpeg 7.0.2 abort the entire command — "no
// decoder found for: none" — so an asset with a perfectly good AAC track
// produced no waveform at all. Fold what CAN be decoded.
func TestFoldArgsSkipUndecodableOrdinals(t *testing.T) {
	if _, ok := foldArgsForOrdinals(nil, 16000); ok {
		t.Fatal("no decodable stream must be ok=false, the same as no audio")
	}
	// Stream 1 is undecodable: the survivors are ordinals 0 and 2, and they must
	// be addressed by THEIR OWN ordinals — renumbering them 0 and 1 would fold
	// the undecodable stream straight back in.
	args, ok := foldArgsForOrdinals([]int{0, 2}, 48000)
	if !ok {
		t.Fatal("two decodable streams must be ok")
	}
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "[0:a:0]aformat=channel_layouts=mono[a0]") ||
		!strings.Contains(joined, "[0:a:2]aformat=channel_layouts=mono[a1]") {
		t.Fatalf("survivors not addressed by their own ordinals: %s", joined)
	}
	if strings.Contains(joined, "0:a:1") {
		t.Fatalf("the undecodable stream is still in the graph: %s", joined)
	}
	if !strings.Contains(joined, "amerge=inputs=2[m]") {
		t.Fatalf("merge must count only the survivors: %s", joined)
	}
	// A lone survivor takes the simple -map path, still by its own ordinal.
	solo, ok := foldArgsForOrdinals([]int{1}, 16000)
	if !ok || strings.Join(solo, " ") != "-map 0:a:1 -ac 1 -ar 16000" {
		t.Fatalf("lone survivor args wrong: %v", solo)
	}
}

// The discriminator is ffprobe's own codec_name: a stream it reports as "none"
// is one this build has no decoder for.
//
// Tested on the PARSE, not on a generated file. A fixture built with the same
// ffmpeg that then reads it can never contain a stream that ffmpeg cannot
// decode, so a fixture-based version of this test passes whether or not the
// filter exists — which is exactly what happened to its first draft.
func TestParseDecodableOrdinals(t *testing.T) {
	// The real shape of the iPhone spatial-audio case: AAC first, APAC second,
	// reported by ffprobe 7.0.2 as having no decoder.
	got := parseDecodableOrdinals("aac\nnone\n")
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("the undecodable APAC stream must be dropped and the AAC kept, got %v", got)
	}
	// The ordinal counter must advance across the skipped stream, or the
	// survivors are addressed by numbers that belong to other streams.
	got = parseDecodableOrdinals("none\npcm_s24le\nnone\npcm_s24le\n")
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("survivors must keep their own ordinals, got %v", got)
	}
	if len(parseDecodableOrdinals("none\nnone\n")) != 0 {
		t.Fatal("a file whose every audio stream is undecodable has no fold at all")
	}
	if len(parseDecodableOrdinals("")) != 0 {
		t.Fatal("no audio streams must yield no ordinals")
	}
}
