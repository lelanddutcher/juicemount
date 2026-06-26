package farm

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// ffprobeBinFor derives the ffprobe path beside the given ffmpeg binary (same
// directory, ffmpeg→ffprobe), falling back to a PATH lookup of "ffprobe".
func ffprobeBinFor(ffmpegBin string) string {
	if ffmpegBin == "" || ffmpegBin == "ffmpeg" {
		return "ffprobe"
	}
	base := filepath.Base(ffmpegBin)
	probe := strings.Replace(base, "ffmpeg", "ffprobe", 1)
	if probe == base {
		return "ffprobe"
	}
	return filepath.Join(filepath.Dir(ffmpegBin), probe)
}

// audioStreamCount returns how many audio streams srcPath has (0 = no audio).
func audioStreamCount(ffmpegBin, srcPath string) (int, error) {
	cmd := exec.Command(ffprobeBinFor(ffmpegBin), "-v", "error",
		"-select_streams", "a", "-show_entries", "stream=index",
		"-of", "csv=p=0", srcPath)
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("ffprobe audio streams %q: %w", srcPath, err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n, nil
}

// audioFoldArgs builds the ffmpeg arguments (everything between `-i <src>` and the
// output spec) that fold ALL audio streams + channels of src down to a SINGLE mono
// track at sampleRate Hz.
//
// This fixes a SILENT DATA-LOSS bug: the old `-map 0:a:0` grabbed only the FIRST
// audio stream, and on multi-mono-stream camera clips (e.g. 4 separate pcm_s24le
// lav-mic streams) the first stream is often the dead/unused one — so the farm
// transcribed + waveformed pure SILENCE while the other mics carried the audio.
//
//   - 0 streams → ok=false (no audio).
//   - 1 stream  → `-map 0:a:0 -ac 1`: ffmpeg averages all channels of the single
//     stream (mono/stereo/quad/5.1/discrete), headroom-safe, handles unknown layout.
//   - N>=2      → amerge every stream into one, each via a per-input
//     `aformat=channel_layouts=mono` leg. That per-input cast is REQUIRED: without
//     it amerge aborts with "No channel layout for input N" on the
//     channel_layout=unknown streams these camera files carry.
//
// Uses -ac 1 averaging (not `pan` c0+c1+… which sums and clips). The discrete
// per-channel path (one whisper pass per mic, ~Nx slower, for L/R-separated lavs)
// is a deliberate future — the same per-stream legs already exist here, so it's a
// superset, not a rewrite. Persisting per-stream tech (probe.go tech.Audio[]) lets
// the producer choose fold-vs-discrete per asset later.
func audioFoldArgs(ffmpegBin, srcPath string, sampleRate int) (args []string, ok bool, err error) {
	n, err := audioStreamCount(ffmpegBin, srcPath)
	if err != nil {
		return nil, false, err
	}
	args, ok = foldArgsForCount(n, sampleRate)
	return args, ok, nil
}

// foldArgsForCount is the pure (no-I/O) arg builder, split out so the
// stream-count → ffmpeg-args mapping is unit-testable without a media file.
func foldArgsForCount(n, sampleRate int) (args []string, ok bool) {
	if n <= 0 {
		return nil, false
	}
	ar := strconv.Itoa(sampleRate)
	if n == 1 {
		return []string{"-map", "0:a:0", "-ac", "1", "-ar", ar}, true
	}
	var legs, labels strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&legs, "[0:a:%d]aformat=channel_layouts=mono[a%d];", i, i)
		fmt.Fprintf(&labels, "[a%d]", i)
	}
	filter := legs.String() + labels.String() + fmt.Sprintf("amerge=inputs=%d[m]", n)
	return []string{"-filter_complex", filter, "-map", "[m]", "-ac", "1", "-ar", ar}, true
}
