package farm

import (
	"context"
	"fmt"
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
	return ThumbnailContext(context.Background(), ffmpegBin, srcPath, outPath, maxDim, durationMS)
}

func ThumbnailContext(ctx context.Context, ffmpegBin, srcPath, outPath string, maxDim int, durationMS int64) error {
	return ThumbnailDecodeContext(ctx, ffmpegBin, "", 0, srcPath, outPath, maxDim, durationMS)
}

// ThumbnailDecodeContext uses decoder when the queue admitted a verified
// hardware path. It never retries without those input arguments; the caller
// must publish any CPU fallback as a separate, visible queue transition.
func ThumbnailDecodeContext(ctx context.Context, ffmpegBin, decoder string, bitDepth int, srcPath, outPath string, maxDim int, durationMS int64) error {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if maxDim <= 0 {
		// D2 density ask (CONSUMER_STATUS 2026-07-21): the consumer raised its
		// LOCAL poster max to 720, so a farm poster at 640 reads visibly softer
		// next to a locally-generated one in the same hover preview.
		maxDim = 720
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
	decodeArgs, filterPrefix, err := videoDecodePlan(decoder, bitDepth)
	if err != nil {
		return fmt.Errorf("thumbnail decode admission: %w", err)
	}
	scale := filterPrefix + fmt.Sprintf("scale=w=%d:h=%d:force_original_aspect_ratio=decrease", maxDim, maxDim)
	args := append([]string{"-y", "-loglevel", "error"}, ffmpegThreadArgs()...)
	args = append(args, decodeArgs...)
	if durationMS > 0 {
		// Input-seek (before -i): fast index seek, decodes only the target
		// keyframe. Seek to 10% in (skips leaders/slates).
		seekSec := float64(durationMS) / 1000.0 / 10.0
		args = append(args, "-ss", fmt.Sprintf("%.3f", seekSec), "-i", srcPath,
			"-frames:v", "1", "-vf", scale, "-q:v", "3", "-f", "image2", outPath)
	} else {
		// Duration unknown: keep the representative-frame scan (decodes ~100
		// frames from the start) rather than blind-seek into an unknown length.
		args = append(args, "-i", srcPath, "-vf", "thumbnail,"+scale,
			"-frames:v", "1", "-q:v", "3", "-f", "image2", outPath)
	}
	cmd := commandContext(ctx, ffmpegBin, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg thumbnail %q: %w: %s", srcPath, err, out)
	}
	return nil
}
