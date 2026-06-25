package farm

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// Thumbnail writes a single poster-frame JPEG to outPath, fit within maxDim×maxDim
// (aspect preserved). Uses ffmpeg's `thumbnail` filter, which scans a window of
// frames and picks the most representative one rather than a (often black) first
// frame. Web-native by design (image/jpeg) so the same blob serves OpenLoupe over
// the mount AND the future web UI over HTTP.
func Thumbnail(ffmpegBin, srcPath, outPath string, maxDim int) error {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if maxDim <= 0 {
		maxDim = 640
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	vf := fmt.Sprintf("thumbnail,scale=w=%d:h=%d:force_original_aspect_ratio=decrease", maxDim, maxDim)
	cmd := exec.Command(ffmpegBin, "-y", "-loglevel", "error",
		"-i", srcPath, "-vf", vf, "-frames:v", "1", "-q:v", "3", outPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg thumbnail %q: %w: %s", srcPath, err, out)
	}
	return nil
}
