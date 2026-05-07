package proxy

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// FFmpegBin is the resolved path to the ffmpeg binary. Set at startup.
// Defaults to /opt/homebrew/bin/ffmpeg (Apple Silicon Homebrew) and falls
// back to PATH lookup if that doesn't exist.
var FFmpegBin = resolveFFmpegBin()

func resolveFFmpegBin() string {
	candidates := []string{
		"/opt/homebrew/bin/ffmpeg",  // Apple Silicon Homebrew
		"/usr/local/bin/ffmpeg",     // Intel Homebrew
		"/usr/bin/ffmpeg",           // Just in case
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if p, err := exec.LookPath("ffmpeg"); err == nil {
		return p
	}
	return "ffmpeg" // exec will fail later with a clear message
}

// ProxySpec describes a single proxy generation job.
type ProxySpec struct {
	SrcPath  string // input file (any codec ffmpeg understands)
	DstPath  string // output file (will be H.264 .mp4)
	Codec    Codec  // source codec (informs encoder flags)
	Width    int    // target width in pixels (height auto from aspect)
	Bitrate  string // e.g. "8M" — target H.264 bitrate
	Hardware bool   // use VideoToolbox encoder (vs libx264 software)
}

// DefaultSpec returns a Quick Look-friendly proxy spec for the given codec.
// 1280-wide H.264 at 8 Mbps using VideoToolbox.
func DefaultSpec(srcPath, dstPath string, codec Codec) ProxySpec {
	return ProxySpec{
		SrcPath:  srcPath,
		DstPath:  dstPath,
		Codec:    codec,
		Width:    1280,
		Bitrate:  "8M",
		Hardware: true,
	}
}

// buildArgs assembles the ffmpeg command-line for this spec.
// Codec-specific tuning lives here so the rest of the system stays simple.
func (s ProxySpec) buildArgs() []string {
	args := []string{
		"-y",                  // overwrite output
		"-hide_banner",
		"-nostdin",            // don't read from stdin (background safe)
		"-loglevel", "error",  // we capture stderr; only show errors
	}

	// Codec-specific input flags (e.g., R3D needs explicit color trc)
	switch s.Codec {
	case CodecR3D:
		// RED RAW: keep colorspace consistent so VideoToolbox doesn't trip.
		args = append(args, "-color_primaries", "bt709",
			"-color_trc", "bt709", "-colorspace", "bt709")
	case CodecARRI:
		// ARRIRAW: explicit pixel format avoids libavcodec autodetect surprises.
		// Handled at encoder side via -pix_fmt below.
	}

	args = append(args, "-i", s.SrcPath)

	// Encoder selection
	encoder := "libx264"
	encoderFlags := []string{"-preset", "veryfast", "-tune", "fastdecode"}
	if s.Hardware {
		encoder = "h264_videotoolbox"
		encoderFlags = []string{
			"-allow_sw", "1", // fall back to software if HW encoder rejects input
			"-realtime", "1", // prioritize speed over quality
		}
	}

	args = append(args, "-c:v", encoder)
	args = append(args, encoderFlags...)
	args = append(args, "-b:v", s.Bitrate)
	args = append(args, "-vf", fmt.Sprintf("scale=%d:-2", s.Width))
	args = append(args, "-pix_fmt", "yuv420p") // Quick Look-friendly
	args = append(args, "-an")                 // no audio in proxies
	args = append(args, "-movflags", "+faststart") // mp4 metadata at front
	args = append(args, "-f", "mp4")           // explicit format — required when DstPath has unusual suffix (.tmp.<pid>)
	args = append(args, s.DstPath)
	return args
}

// Generate runs ffmpeg to produce the proxy at s.DstPath. Blocks until done
// or context expires. Returns generation duration on success.
//
// Writes to a `.tmp.<pid>` sibling file and renames atomically on success.
// This prevents a TOCTOU race where Get() polls and sees a partial mp4
// header that ffmpeg writes early in its run.
//
// Uses GOMAXPROCS-friendly thread pinning: lets ffmpeg use all cores for
// software decode, which is the bottleneck for R3D/BRAW/ARRI. The encoder
// is hardware-accelerated so it doesn't fight for CPU.
func (s ProxySpec) Generate(ctx context.Context) (time.Duration, error) {
	if FFmpegBin == "" {
		return 0, fmt.Errorf("ffmpeg not found on this system")
	}
	if _, err := os.Stat(s.SrcPath); err != nil {
		return 0, fmt.Errorf("source missing: %w", err)
	}

	// Make sure the destination directory exists
	if err := os.MkdirAll(parentDir(s.DstPath), 0o755); err != nil {
		return 0, fmt.Errorf("mkdir dst dir: %w", err)
	}

	// Write to .tmp.<pid> first, rename on success. Prevents readers from
	// seeing a partial mp4 (ffmpeg writes the moov atom early).
	tmpPath := fmt.Sprintf("%s.tmp.%d", s.DstPath, os.Getpid())
	defer os.Remove(tmpPath) // safe even if rename succeeded

	// Build args targeting the tmp path, not the final dest
	specForTmp := s
	specForTmp.DstPath = tmpPath
	args := specForTmp.buildArgs()

	start := time.Now()
	cmd := exec.CommandContext(ctx, FFmpegBin, args...)
	stderr := &strings.Builder{}
	cmd.Stderr = stderr

	if err := cmd.Run(); err != nil {
		os.Remove(tmpPath)
		return time.Since(start), fmt.Errorf("ffmpeg %s: %w (stderr: %s)",
			s.Codec, err, truncate(stderr.String(), 500))
	}

	// Sanity check before rename
	info, err := os.Stat(tmpPath)
	if err != nil {
		return time.Since(start), fmt.Errorf("output missing after ffmpeg: %w", err)
	}
	// A real mp4 with our config is always >>1KB. Anything smaller is a
	// header skeleton from a busted run that exited 0 anyway (rare with
	// VideoToolbox but possible).
	if info.Size() < 1024 {
		os.Remove(tmpPath)
		return time.Since(start), fmt.Errorf("ffmpeg produced suspiciously small output (%d bytes); stderr: %s",
			info.Size(), truncate(stderr.String(), 200))
	}

	// Atomic rename — readers see either no file or the complete proxy.
	if err := os.Rename(tmpPath, s.DstPath); err != nil {
		return time.Since(start), fmt.Errorf("rename tmp to dst: %w", err)
	}

	return time.Since(start), nil
}

func parentDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...[truncated]"
}

// recommendedWorkerCount returns a sensible default for the worker pool size.
// We want ffmpeg jobs to share the box without starving the NFS server.
func recommendedWorkerCount() int {
	n := runtime.NumCPU() / 2
	if n < 1 {
		n = 1
	}
	if n > 4 {
		n = 4 // cap at 4 — diminishing returns past that for hardware encode
	}
	return n
}
