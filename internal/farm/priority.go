package farm

import "strings"

// FARM-3 (CONSUMER_STATUS 2026-08-03 B, founder-ruled): weight proxy generation
// toward the media where a farm proxy still earns its keep.
//
// The consumer's measurement behind the ask: ClipLogger now bundles libdav1d, so
// AV1 decodes locally and an AV1 proxy is near-worthless to it; and playback only
// rescues a stream delivering UNDER 15 fps absolute, so a 41 fps software decode
// plays as-is and never wants a proxy. What remains genuinely painful is the
// software-only decode family — HEVC Rext 4:2:2/4:4:4 and XF-AVC 4K60, the
// ~0.65x-realtime classes.
//
// IMPORTANT CONSTRAINT, surfaced to the consumer rather than papered over: the
// codec is NOT knowable during the filesystem walk. collectTargets sees paths and
// sizes only; classifying by codec costs one ffprobe per file, which is the very
// expense a sweep is trying to schedule. So this weighting applies where tech is
// ALREADY known (any inode the farm has probed before). On a first sweep of
// never-probed media the order is unchanged — deliberately, rather than paying a
// full probe pass up front to decide what to probe.

// ProxyPriority orders proxy work. Higher runs first.
type ProxyPriority int

const (
	// ProxyPriorityNormal is everything a client can decode acceptably itself,
	// including AV1 and ordinary H.264.
	ProxyPriorityNormal ProxyPriority = 0
	// ProxyPriorityHigh is the software-only decode family, where a farm proxy
	// is the difference between usable and unusable playback.
	ProxyPriorityHigh ProxyPriority = 1
)

// ClassifyProxyPriority reports how much a farm proxy is worth for one video
// track. A nil track (audio-only, or tech we never captured) is Normal.
func ClassifyProxyPriority(v *VideoTrack) ProxyPriority {
	if v == nil {
		return ProxyPriorityNormal
	}
	codec := strings.ToLower(strings.TrimSpace(v.Codec))
	pix := strings.ToLower(strings.TrimSpace(v.PixFmt))

	// HEVC Rext 4:2:2 / 4:4:4. We have no `profile` field, but the chroma
	// subsampling IS the thing that forces the software path, and pix_fmt
	// encodes it exactly (yuv422p10le, yuv444p12le, ...). Better signal than a
	// profile string would be.
	if codec == "hevc" || codec == "h265" {
		if strings.Contains(pix, "422") || strings.Contains(pix, "444") {
			return ProxyPriorityHigh
		}
	}

	// XF-AVC 4K60: H.264 at 4K and a high frame rate. Canon's XF-AVC is
	// H.264-in-MXF; the punishing part is 4K at >=50fps, not the container, so
	// classify on the decode load rather than sniffing a vendor name.
	if codec == "h264" || codec == "avc1" {
		if v.Width >= 3840 && v.FPS >= 50 {
			return ProxyPriorityHigh
		}
	}

	// Everything else — AV1 (decoded locally via libdav1d), ordinary H.264,
	// ProRes and friends — stays Normal.
	return ProxyPriorityNormal
}

// PrioritizeTargets returns targets reordered so high-priority media comes
// first, preserving the original relative order within each class (a stable
// partition, so a sweep stays predictable and resumable).
//
// techFor resolves a path to its already-known video track; it must return nil
// when tech is unknown, which keeps unprobed media in its original position
// rather than guessing. targets is not mutated.
func PrioritizeTargets(targets []string, techFor func(path string) *VideoTrack) []string {
	if len(targets) < 2 || techFor == nil {
		return targets
	}
	high := make([]string, 0, len(targets))
	rest := make([]string, 0, len(targets))
	for _, t := range targets {
		if ClassifyProxyPriority(techFor(t)) == ProxyPriorityHigh {
			high = append(high, t)
			continue
		}
		rest = append(rest, t)
	}
	if len(high) == 0 {
		return targets
	}
	return append(high, rest...)
}
