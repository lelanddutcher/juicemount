package farm

import (
	"fmt"
	"strings"
)

// videoDecodePlan returns the ffmpeg input options and CPU-filter handoff for
// one admitted hardware decoder. The decode remains hardware-only; hwdownload
// merely exposes the decoded frame to poster/filmstrip image filters.
func videoDecodePlan(decoder string, bitDepth int) (inputArgs []string, filterPrefix string, err error) {
	decoder = strings.ToLower(strings.TrimSpace(decoder))
	if decoder == "" {
		return nil, "", nil
	}
	swFormat := "nv12"
	if bitDepth > 8 {
		swFormat = "p010le"
	}
	switch {
	case strings.HasSuffix(decoder, "_vaapi"):
		return []string{
			"-hwaccel", "vaapi",
			"-hwaccel_device", "/dev/dri/renderD128",
			"-hwaccel_output_format", "vaapi",
		}, "hwdownload,format=" + swFormat + ",", nil
	case strings.HasSuffix(decoder, "_qsv"):
		return []string{
			"-hwaccel", "qsv",
			"-hwaccel_device", "/dev/dri/renderD128",
			"-hwaccel_output_format", "qsv",
		}, "hwdownload,format=" + swFormat + ",", nil
	case strings.HasSuffix(decoder, "_nvenc") || strings.HasSuffix(decoder, "_cuda"):
		return []string{
			"-hwaccel", "cuda",
			"-hwaccel_output_format", "cuda",
		}, "hwdownload,format=" + swFormat + ",", nil
	default:
		return nil, "", fmt.Errorf("unverified hardware decoder %q", decoder)
	}
}
