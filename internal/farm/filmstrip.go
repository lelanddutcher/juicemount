package farm

import (
	"fmt"
	"math"
	"os"
	"os/exec"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// Filmstrip renders a sprite-sheet of evenly-spaced frames across the source and
// returns the geometry a scrubber needs (JM-16). The sheet is a FULL cols×rows
// grid — frame_count == cols*rows, cells filled row-major — so the client maps a
// time to a cell with no ambiguity: i = round(t_ms/interval_ms), row = i/cols,
// col = i%cols. Each cell is cellW × cellH, sized from the source aspect (no
// distortion). Web-native JPEG so the same sheet serves OpenLoupe and the web UI.
// frameTarget picks how many distinct frames to sample for a clip.
//
// Roughly one per second, then floored and capped. The FLOOR is the short-clip
// fix (BACKLOG "Ask (optional)", CONSUMER_STATUS 2026-07-21): a flat floor of 12
// made a short clip repeat each frame 3-6x across a wide strip of narrow
// vertical cells — visible banding. Raise it toward 32, but never past what the
// clip actually CONTAINS: a 1s/24fps clip holds ~24 distinct frames and asking
// for more just duplicates them again, which is the very artefact being fixed.
//
// srcFPS <= 0 means "unknown" and keeps the historical flat floor rather than
// guessing. Exported-for-test via the package-internal call in
// filmstrip_density_test.go — the test must exercise THIS function, not a copy
// of its arithmetic, or it stops being a guard.
func frameTarget(durSec, srcFPS float64) int {
	floor := 12
	if srcFPS > 0 && durSec > 0 {
		if avail := int(math.Ceil(durSec * srcFPS)); avail < 32 {
			floor = avail
		} else {
			floor = 32
		}
	}
	if floor < 12 {
		floor = 12
	}
	target := int(math.Round(durSec))
	if target < floor {
		target = floor
	}
	if target > 144 {
		target = 144
	}
	return target
}

func Filmstrip(ffmpegBin, srcPath, outPath string, durationMS int64, srcW, srcH, cellW int, srcFPS float64) (*derivatives.FilmstripGeo, error) {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if durationMS <= 0 || srcW <= 0 || srcH <= 0 {
		return nil, fmt.Errorf("filmstrip: need positive duration+dims (dur=%d %dx%d)", durationMS, srcW, srcH)
	}
	if cellW <= 0 {
		// D5 density ask (CONSUMER_STATUS 2026-07-21): the consumer raised its
		// LOCAL cells to 320x180, so farm strips at 160 stay visibly low-res
		// beside local ones. cellH still follows the SOURCE aspect below, so a
		// 16:9 source lands exactly on 320x180 without hardcoding the height.
		cellW = 320
	}

	durSec := float64(durationMS) / 1000.0
	// ~1 frame/sec, clamped, then snapped to a full grid (12 columns).
	target := frameTarget(durSec, srcFPS)
	cols := 12
	if target < cols {
		cols = target
	}
	rows := (target + cols - 1) / cols
	if rows < 1 {
		rows = 1
	}
	frameCount := cols * rows

	// Cell height from source aspect (preserve it), both dims even for codec
	// friendliness.
	cellH := int(math.Round(float64(cellW) * float64(srcH) / float64(srcW)))
	if cellH < 2 {
		cellH = 2
	}
	cellW += cellW & 1
	cellH += cellH & 1

	// Sample exactly frameCount frames across the whole duration: fps =
	// frames/seconds. tile collects cols*rows frames into one sheet; -frames:v 1
	// emits that first (full) sheet.
	fps := float64(frameCount) / durSec
	intervalMS := int(math.Round(float64(durationMS) / float64(frameCount)))
	if intervalMS < 1 {
		intervalMS = 1
	}

	// WRITES DIRECTLY TO outPath, which the caller already created inside the
	// derivative directory with O_EXCL|O_NOFOLLOW and will commit onto the real
	// blob name with renameat through a held descriptor.
	//
	// This function used to add its OWN atomic layer — encode to a temp sibling,
	// then rename by path — which was redundant once the caller had one, and was
	// the last unanchored surface here: the sibling was NOT created by us, so it
	// could already BE a planted symlink, and os.Rename/os.Open/syncDir all
	// resolve by name. On the farm host that is a root-privileged write out of
	// the volume. Reader visibility is unaffected: the staged name is dot-
	// prefixed and is never the blob's real name, so a reader only ever sees the
	// final name appear atomically at the caller's renameat.
	vf := fmt.Sprintf("fps=%.6f,scale=%d:%d,tile=%dx%d", fps, cellW, cellH, cols, rows)
	// -skip_frame nokey (THE farm-throughput fix, 2026-07-14): the fps= filter
	// sits DOWNSTREAM of the decoder, so without this ffmpeg fully reconstructs
	// every frame (a 5-min 25fps proxy = ~7,500 frames) just to keep 144 — the
	// filmstrip was ~80-90% of per-file CPU and the reason a 90k backfill ETA'd
	// 190h. -discard nokey drops non-keyframe PACKETS at the demuxer (the decoder
	// never sees P/B packets — less work AND less read than -skip_frame, which
	// still parses every packet: live-measured 15s→8s on a 4K/HEVC original); fps= then resamples that sparse
	// keyframe stream to the target rate, so we STILL emit exactly cols×rows
	// evenly-spaced cells (grid math unchanged) — each cell just snaps to the
	// nearest preceding keyframe, a ≤1-2s error invisible in a scrub strip.
	// Live-benchmarked 9-16× faster with byte-identical sprite dimensions.
	// -an: never demux/decode the audio track for a video-only sprite.
	args := append([]string{"-y", "-loglevel", "error"}, ffmpegThreadArgs()...)
	args = append(args, "-discard", "nokey", "-an", "-i", srcPath,
		"-vf", vf, "-frames:v", "1", "-q:v", "4", "-f", "image2", outPath)
	fastOut, fastErr := runFilmstripPass(ffmpegBin, args, outPath)
	if fastErr != nil {
		// A sparse-GOP clip can hold fewer keyframes than tile needs to fill its
		// grid. FFmpeg then exits 0 but writes zero bytes (live reproduced on an
		// 8 s / 2 s-GOP acceptance clip), leaving a permanently failed filmstrip
		// even though a normal decode succeeds. Keep the fast keyframe-only path
		// for the common case; pay for full decode only after a proven empty/error
		// result. The output path is the caller's already-anchored staged file, and
		// ffmpeg -y safely replaces the failed pass in place.
		fallbackArgs := append([]string{"-y", "-loglevel", "error"}, ffmpegThreadArgs()...)
		fallbackArgs = append(fallbackArgs, "-an", "-i", srcPath,
			"-vf", vf, "-frames:v", "1", "-q:v", "4", "-f", "image2", outPath)
		fallbackOut, fallbackErr := runFilmstripPass(ffmpegBin, fallbackArgs, outPath)
		if fallbackErr != nil {
			return nil, fmt.Errorf("ffmpeg filmstrip %q: keyframe pass: %v: %s; full-decode fallback: %w: %s",
				srcPath, fastErr, fastOut, fallbackErr, fallbackOut)
		}
	}

	return &derivatives.FilmstripGeo{
		FrameCount: frameCount, Cols: cols, Rows: rows,
		CellW: cellW, CellH: cellH, IntervalMS: intervalMS, DurationMS: int(durationMS),
	}, nil
}

func runFilmstripPass(ffmpegBin string, args []string, outPath string) ([]byte, error) {
	out, err := exec.Command(ffmpegBin, args...).CombinedOutput()
	if err != nil {
		return out, err
	}
	st, err := os.Stat(outPath)
	if err != nil {
		return out, fmt.Errorf("output missing after successful ffmpeg exit: %w", err)
	}
	if !st.Mode().IsRegular() {
		return out, fmt.Errorf("output is not a regular file")
	}
	if st.Size() == 0 {
		return out, fmt.Errorf("ffmpeg exited successfully but produced empty output")
	}
	return out, nil
}
