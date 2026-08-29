package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

type renderFallbackTarget struct {
	Path   string
	Reason string
}

type renderTrackProbe func(string) (*farm.VideoTrack, error)

// partitionRenderProxyTargets applies hardware-path admission per file. A
// mixed camera directory is therefore two independent work sets: sources that
// can stay on verified GPU decode+encode, and the exact sources that need an
// explicit CPU fallback. One ProRes clip must never demote thousands of H.264
// or HEVC clips to libx264.
func partitionRenderProxyTargets(worker farmqueue.Worker, encoder string, targets []string, probe renderTrackProbe) (hardware []string, fallback []renderFallbackTarget) {
	if worker.Role != farmqueue.QueueClassRender || encoder == "" {
		return append([]string(nil), targets...), nil
	}
	for _, path := range targets {
		track, err := probe(path)
		if err != nil {
			// No live proof means this file cannot enter a hardware-only job. Keep
			// the uncertainty scoped to this source instead of poisoning the batch.
			fallback = append(fallback, renderFallbackTarget{Path: path, Reason: "live compatibility probe failed: " + err.Error()})
			continue
		}
		if reason := unsupportedHardwareDecodeReason(worker, encoder, track); reason != "" {
			fallback = append(fallback, renderFallbackTarget{Path: path, Reason: reason})
			continue
		}
		hardware = append(hardware, path)
	}
	return hardware, fallback
}

// probeRenderVideoTrack describes the bytes about to be rendered. Stored
// metadata can order work, but it may predate a file replacement and cannot
// admit a source to the hardware-only path by itself.
func probeRenderVideoTrack(path string) (*farm.VideoTrack, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	tech, err := farm.Probe("", path, info.Size())
	if err != nil {
		return nil, err
	}
	return tech.Video, nil
}

func unsupportedHardwareDecodeReason(worker farmqueue.Worker, encoder string, track *farm.VideoTrack) string {
	if track == nil {
		return ""
	}
	family := hardwareFamily(encoder)
	if family == "" {
		return "selected encoder is not a hardware backend"
	}
	codec := normalizedDecodeCodec(track.Codec)
	if codec == "" {
		return fmt.Sprintf("codec %s has no verified hardware decoder", displayValue(track.Codec))
	}
	required := codec + "_" + family
	if !containsString(worker.Decoders, required) {
		return fmt.Sprintf("decoder %s was not verified", required)
	}
	profile := farmqueue.NormalizeVideoProfile(codec, track.Profile)
	if profile != "" && !farmqueue.WorkerSupports(worker, farmqueue.DecoderRequirements(required, codec, profile)) {
		return fmt.Sprintf("decoder %s profile %s was not verified", required, profile)
	}
	if reason := unsupportedHardwareVideoFormatReason(codec, track); reason != "" {
		return reason
	}
	pixelFormat := farmqueue.NormalizePixelFormat(track.PixFmt)
	if pixelFormat == "" {
		return "pixel format is unknown"
	}
	if !farmqueue.WorkerSupports(worker, []string{farmqueue.DecoderPixelFormatRequirement(required, pixelFormat)}) {
		return fmt.Sprintf("decoder %s pixel format %s was not verified", required, pixelFormat)
	}
	if !farmqueue.WorkerSupportsDecodeSize(worker, required, track.Width, track.Height) {
		limit := worker.DecodeLimits[required]
		if limit.MaxWidth > 0 && limit.MaxHeight > 0 {
			return fmt.Sprintf("decoder %s was verified only through %dx%d, source is %dx%d",
				required, limit.MaxWidth, limit.MaxHeight, track.Width, track.Height)
		}
		return fmt.Sprintf("decoder %s has no verified frame-size limit for %dx%d source",
			required, track.Width, track.Height)
	}
	return ""
}

// unsupportedHardwareVideoFormatReason rejects source properties that are
// independent of which live worker is selected. The server planner uses this
// before publishing proxy children, preventing known 4:2:2/4:4:4 or excessive
// bit-depth media from bouncing through the render queue just to be demoted.
func unsupportedHardwareVideoFormatReason(codec string, track *farm.VideoTrack) string {
	if track == nil {
		return ""
	}
	pixFmt := farmqueue.NormalizePixelFormat(track.PixFmt)
	if pixFmt == "" {
		return "pixel format is unknown"
	}
	if !strings.Contains(pixFmt, "420") && !strings.Contains(pixFmt, "422") && !strings.Contains(pixFmt, "444") &&
		pixFmt != "nv12" && !strings.HasPrefix(pixFmt, "p010") {
		return fmt.Sprintf("pixel format %s is outside the supported hardware decode classes", pixFmt)
	}
	if codec == "h264" && track.BitDepth > 8 {
		return fmt.Sprintf("H.264 %d-bit decode was not verified", track.BitDepth)
	}
	if codec == "hevc" && track.BitDepth > 10 {
		return fmt.Sprintf("HEVC %d-bit decode was not verified", track.BitDepth)
	}
	if codec == "av1" && track.BitDepth > 10 {
		return fmt.Sprintf("AV1 %d-bit decode was not verified", track.BitDepth)
	}
	return ""
}

func normalizedDecodeCodec(codec string) string {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "h264", "avc1":
		return "h264"
	case "hevc", "h265", "hev1", "hvc1":
		return "hevc"
	case "av1", "av01":
		return "av1"
	default:
		return ""
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func displayValue(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return "unknown"
}
