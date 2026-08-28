package farm

import (
	"context"
	"fmt"
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

// decodableAudioOrdinals returns the `0:a:N` ordinals of the audio streams this
// ffmpeg build can actually DECODE.
//
// Counting streams is not enough, and the difference is a whole class of files
// silently losing their waveform. An iPhone .MOV recorded with spatial audio
// carries two audio streams: an ordinary AAC mix, and a second in Apple's APAC
// (tag `apac`), whose decoder landed in ffmpeg 7.1. On ffmpeg 7.0.2 the fold
// filter graph names BOTH streams, the APAC leg cannot be opened —
//
//	Decoding requested, but no decoder found for: none
//	Error initializing a simple filtergraph
//
// — and the whole command exits non-zero, so an asset with a perfectly good AAC
// track produces no waveform at all. On 2026-08-19 that was the true cause of
// the farm's missing waveforms, NOT the `ipcm` decode the ffmpeg 7.0.2 upgrade
// was meant to fix.
//
// ffprobe reports an undecodable stream's codec_name as "none" — ffmpeg's own
// signal that it has no decoder for it — so that is the discriminator, and it
// keeps working when the build later gains the codec: the stream simply stops
// being reported as "none" and rejoins the fold.
//
// Returning ORDINALS, not a count, is what makes skipping possible: `0:a:N`
// addresses the Nth AUDIO stream, so dropping the second of three shifts
// nothing — the survivors keep the ordinals ffmpeg knows them by.
func decodableAudioOrdinals(ffmpegBin, srcPath string) ([]int, error) {
	return decodableAudioOrdinalsContext(context.Background(), ffmpegBin, srcPath)
}

func decodableAudioOrdinalsContext(ctx context.Context, ffmpegBin, srcPath string) ([]int, error) {
	cmd := commandContext(ctx, ffprobeBinFor(ffmpegBin), "-v", "error",
		"-select_streams", "a", "-show_entries", "stream=codec_name",
		"-of", "csv=p=0", srcPath)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe audio streams %q: %w", srcPath, err)
	}
	return parseDecodableOrdinals(string(out)), nil
}

// parseDecodableOrdinals turns ffprobe's one-codec_name-per-audio-stream CSV
// into the ordinals of the streams that have a decoder.
//
// Split out from the shell-out because the shell-out is not the logic, and the
// logic cannot otherwise be tested: synthesising a file with an audio stream
// that the LOCAL ffmpeg cannot decode is not something a test can do with that
// same ffmpeg, so a test built on a generated fixture silently proves nothing.
// (It did: the first version of this check passed with the filter removed.)
//
// The ordinal counter advances for EVERY audio stream, decodable or not,
// because `0:a:N` numbers all of them — skipping the increment would address
// the wrong streams.
func parseDecodableOrdinals(probeOut string) []int {
	var ords []int
	ordinal := 0
	for _, line := range strings.Split(strings.TrimSpace(probeOut), "\n") {
		name := strings.TrimSpace(line)
		if name == "" {
			continue
		}
		if name != "none" && name != "unknown" {
			ords = append(ords, ordinal)
		}
		ordinal++
	}
	return ords
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
	return audioFoldArgsContext(context.Background(), ffmpegBin, srcPath, sampleRate)
}

func audioFoldArgsContext(ctx context.Context, ffmpegBin, srcPath string, sampleRate int) (args []string, ok bool, err error) {
	ords, err := decodableAudioOrdinalsContext(ctx, ffmpegBin, srcPath)
	if err != nil {
		return nil, false, err
	}
	args, ok = foldArgsForOrdinals(ords, sampleRate)
	return args, ok, nil
}

// foldArgsForCount is the pure (no-I/O) arg builder, split out so the
// stream-count → ffmpeg-args mapping is unit-testable without a media file.
func foldArgsForCount(n, sampleRate int) (args []string, ok bool) {
	ords := make([]int, 0, n)
	for i := 0; i < n; i++ {
		ords = append(ords, i)
	}
	return foldArgsForOrdinals(ords, sampleRate)
}

// foldArgsForOrdinals folds the named audio stream ordinals into one mono track.
//
// It takes ordinals rather than a count so that undecodable streams can be left
// out of the graph entirely (see decodableAudioOrdinals). An empty list means
// there is nothing this build can decode, which is reported the same way as no
// audio at all: ok=false.
func foldArgsForOrdinals(ords []int, sampleRate int) (args []string, ok bool) {
	if len(ords) == 0 {
		return nil, false
	}
	ar := strconv.Itoa(sampleRate)
	if len(ords) == 1 {
		return []string{"-map", fmt.Sprintf("0:a:%d", ords[0]), "-ac", "1", "-ar", ar}, true
	}
	var legs, labels strings.Builder
	for i, o := range ords {
		fmt.Fprintf(&legs, "[0:a:%d]aformat=channel_layouts=mono[a%d];", o, i)
		fmt.Fprintf(&labels, "[a%d]", i)
	}
	filter := legs.String() + labels.String() + fmt.Sprintf("amerge=inputs=%d[m]", len(ords))
	return []string{"-filter_complex", filter, "-map", "[m]", "-ac", "1", "-ar", ar}, true
}
