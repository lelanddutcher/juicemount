package farm

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// Thumbnail writes a single poster-frame JPEG to outPath, fit within maxDim×maxDim
// (aspect preserved). Web-native by design (image/jpeg) so the same blob serves
// OpenLoupe over the mount AND the future web UI over HTTP.
//
// Frame selection (2026-07-14, farm-throughput): when the duration is known,
// INPUT-SEEK to ~10% in and grab one frame — the fast-seek lands on the nearest
// keyframe via the index and DECODES ONLY THAT FRAME. The prior `thumbnail`
// filter decoded ~100 frames from the start to pick "the most representative"
// one, which on a 4K/HEVC original cost ~7.5s vs ~1.6s for the seek. Seeking to
// 10% skips typical slates/black leaders, so the single grabbed frame is almost
// always real content — a fine trade for a preview thumbnail at ~5× the speed.
// durationMS<=0 (unknown) falls back to the old thumbnail-filter scan.
func Thumbnail(ffmpegBin, srcPath, outPath string, maxDim int, durationMS int64) error {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if maxDim <= 0 {
		maxDim = 640
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	// Encode to a temp sibling, then atomically rename onto outPath so a
	// concurrent OpenLoupe reader never sees a half-written JPEG. -f image2
	// forces the muxer because the temp path lacks the .jpg extension ffmpeg
	// would otherwise infer the format from.
	tmpPath := atomicTempPath(outPath)
	defer os.Remove(tmpPath) // no-op once the commit rename consumes it
	scale := fmt.Sprintf("scale=w=%d:h=%d:force_original_aspect_ratio=decrease", maxDim, maxDim)
	args := append([]string{"-y", "-loglevel", "error"}, ffmpegThreadArgs()...)
	if durationMS > 0 {
		// Input-seek (before -i): fast index seek, decodes only the target
		// keyframe. Seek to 10% in (skips leaders/slates).
		seekSec := float64(durationMS) / 1000.0 / 10.0
		args = append(args, "-ss", fmt.Sprintf("%.3f", seekSec), "-i", srcPath,
			"-frames:v", "1", "-vf", scale, "-q:v", "3", "-f", "image2", tmpPath)
	} else {
		// Duration unknown: keep the representative-frame scan (decodes ~100
		// frames from the start) rather than blind-seek into an unknown length.
		args = append(args, "-i", srcPath, "-vf", "thumbnail,"+scale,
			"-frames:v", "1", "-q:v", "3", "-f", "image2", tmpPath)
	}
	cmd := exec.Command(ffmpegBin, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg thumbnail %q: %w: %s", srcPath, err, out)
	}
	if err := atomicCommitFile(tmpPath, outPath); err != nil {
		return fmt.Errorf("commit thumbnail %q: %w", outPath, err)
	}
	return nil
}
