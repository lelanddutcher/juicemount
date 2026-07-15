package farm

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// Filmstrip renders a sprite-sheet of evenly-spaced frames across the source and
// returns the geometry a scrubber needs (JM-16). The sheet is a FULL cols×rows
// grid — frame_count == cols*rows, cells filled row-major — so the client maps a
// time to a cell with no ambiguity: i = round(t_ms/interval_ms), row = i/cols,
// col = i%cols. Each cell is cellW × cellH, sized from the source aspect (no
// distortion). Web-native JPEG so the same sheet serves OpenLoupe and the web UI.
func Filmstrip(ffmpegBin, srcPath, outPath string, durationMS int64, srcW, srcH, cellW int) (*derivatives.FilmstripGeo, error) {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if durationMS <= 0 || srcW <= 0 || srcH <= 0 {
		return nil, fmt.Errorf("filmstrip: need positive duration+dims (dur=%d %dx%d)", durationMS, srcW, srcH)
	}
	if cellW <= 0 {
		cellW = 160
	}

	durSec := float64(durationMS) / 1000.0
	// ~1 frame/sec, clamped, then snapped to a full grid (12 columns).
	target := int(math.Round(durSec))
	if target < 12 {
		target = 12
	}
	if target > 144 {
		target = 144
	}
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

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return nil, err
	}
	// Encode to a temp sibling, then atomically rename onto outPath so a
	// concurrent OpenLoupe reader never sees a half-written sprite sheet. -f
	// image2 forces the muxer because the temp path lacks the .jpg extension
	// ffmpeg would otherwise infer the format from.
	tmpPath := atomicTempPath(outPath)
	defer os.Remove(tmpPath) // no-op once the commit rename consumes it
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
		"-vf", vf, "-frames:v", "1", "-q:v", "4", "-f", "image2", tmpPath)
	cmd := exec.Command(ffmpegBin, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("ffmpeg filmstrip %q: %w: %s", srcPath, err, out)
	}
	if err := atomicCommitFile(tmpPath, outPath); err != nil {
		return nil, fmt.Errorf("commit filmstrip %q: %w", outPath, err)
	}

	return &derivatives.FilmstripGeo{
		FrameCount: frameCount, Cols: cols, Rows: rows,
		CellW: cellW, CellH: cellH, IntervalMS: intervalMS, DurationMS: int(durationMS),
	}, nil
}
