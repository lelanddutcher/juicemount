package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// validateRenderProxyTargets admits a claimed proxy batch only when every
// source can stay on the worker's verified hardware decode+encode path. A
// directory may contain a mix of cameras; failing the batch before rendering
// lets the durable queue visibly move it to the server CPU lane instead of
// leaving failed derivative rows or silently decoding on CPU.
func validateRenderProxyTargets(worker farmqueue.Worker, encoder string, targets []string) error {
	if worker.Role != farmqueue.QueueClassRender || encoder == "" {
		return nil
	}
	for _, path := range targets {
		// Admission must describe the bytes about to be rendered. Stored
		// metadata may predate a file replacement or an earlier probe and is
		// useful for ordering, but cannot prove that the current source is safe
		// for the worker's verified hardware-only path.
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("render compatibility stat: %w", err)
		}
		tech, err := farm.Probe("", path, info.Size())
		if err != nil {
			return fmt.Errorf("render compatibility probe: %w", err)
		}
		track := tech.Video
		if track == nil {
			continue
		}
		if reason := unsupportedHardwareDecodeReason(worker, encoder, track); reason != "" {
			return fmt.Errorf("render worker cannot keep source on verified hardware path (%s); queued explicit CPU/H.264 fallback", reason)
		}
	}
	return nil
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

	pixFmt := strings.ToLower(strings.TrimSpace(track.PixFmt))
	if pixFmt == "" {
		return "pixel format is unknown"
	}
	if strings.Contains(pixFmt, "422") || strings.Contains(pixFmt, "444") {
		return fmt.Sprintf("pixel format %s requires a non-4:2:0 decode path", pixFmt)
	}
	if !strings.Contains(pixFmt, "420") && pixFmt != "nv12" && !strings.HasPrefix(pixFmt, "p010") {
		return fmt.Sprintf("pixel format %s is outside the verified 4:2:0 decode path", pixFmt)
	}
	if codec == "h264" && track.BitDepth > 8 {
		return fmt.Sprintf("H.264 %d-bit decode was not verified", track.BitDepth)
	}
	if codec == "hevc" && track.BitDepth > 10 {
		return fmt.Sprintf("HEVC %d-bit decode was not verified", track.BitDepth)
	}
	return ""
}

func normalizedDecodeCodec(codec string) string {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "h264", "avc1":
		return "h264"
	case "hevc", "h265", "hev1", "hvc1":
		return "hevc"
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
