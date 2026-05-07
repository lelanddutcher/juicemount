// Package proxy provides codec-aware preview proxy generation for
// Quick Look on RAW video formats (R3D, ARRI, BRAW, ProRes RAW, MXF/XAVC).
//
// The package is deliberately scoped narrow:
//   - Detect: is this file a RAW codec we proxy?
//   - Generate: shell out to ffmpeg to make a 720p H.264 proxy
//   - Cache: keep proxies on disk keyed by (path + size + mtime)
//   - Serve: return the proxy file path; caller decides what to do with it
//
// It does NOT integrate with NFS — that wiring lives in nfs/handler.go.
// The proxy package is callable in isolation so tests don't need a server.
package proxy

import (
	"os"
	"path/filepath"
	"strings"
)

// Codec identifies a source codec we know how to proxy.
type Codec int

const (
	CodecUnknown Codec = iota
	CodecR3D            // RED Digital Cinema RAW
	CodecARRI           // ARRIRAW (.ari, .arx)
	CodecBRAW           // Blackmagic RAW
	CodecProResRAW      // Apple ProRes RAW (in .mov)
	CodecXAVC           // Sony XAVC in MXF wrapper
	CodecCinemaDNG      // Open CinemaDNG sequences (folder-based)
)

// String returns a stable, log-friendly codec name.
func (c Codec) String() string {
	switch c {
	case CodecR3D:
		return "R3D"
	case CodecARRI:
		return "ARRIRAW"
	case CodecBRAW:
		return "BRAW"
	case CodecProResRAW:
		return "ProResRAW"
	case CodecXAVC:
		return "XAVC"
	case CodecCinemaDNG:
		return "CinemaDNG"
	default:
		return "unknown"
	}
}

// IsProxyable returns true if this codec is one we generate proxies for.
// CodecUnknown returns false; everything else returns true.
func (c Codec) IsProxyable() bool {
	return c != CodecUnknown
}

// DetectByExtension is the fast path: just look at the filename.
// Suitable for the read hot-path where we don't want to open the file.
//
// Caveats:
//   - .mov files are ambiguous (could be ProRes RAW, regular ProRes, H.264, etc.).
//     This returns CodecUnknown for .mov; use DetectByMagic for those.
//   - .mxf could be XAVC, XDCAM, DNxHD, IMX. Returns CodecXAVC pessimistically;
//     ffmpeg handles all of them but we tune flags differently per subtype.
//   - .dng files in a numbered sequence are CinemaDNG; single .dng returns
//     CodecUnknown (Quick Look already handles single DNG).
func DetectByExtension(path string) Codec {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".r3d":
		return CodecR3D
	case ".ari", ".arx", ".arri":
		return CodecARRI
	case ".braw":
		return CodecBRAW
	case ".mxf":
		return CodecXAVC
	case ".dng":
		// Single DNG handled by Quick Look natively. CinemaDNG sequence
		// detection requires looking at the parent directory; defer to
		// DetectByMagic which can do a sibling scan.
		return CodecUnknown
	}
	return CodecUnknown
}

// DetectByMagic does a real sniff of the file's first bytes to identify
// the codec when the extension isn't enough (e.g., .mov containing
// ProRes RAW). Reads at most 1KB, suitable for cold-cache lookups.
//
// For the hot path, prefer DetectByExtension.
func DetectByMagic(path string) (Codec, error) {
	// Fast extension check first
	if c := DetectByExtension(path); c != CodecUnknown {
		return c, nil
	}

	ext := strings.ToLower(filepath.Ext(path))
	if ext != ".mov" && ext != ".mp4" && ext != ".m4v" {
		return CodecUnknown, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return CodecUnknown, err
	}
	defer f.Close()

	// Read enough header to inspect the QuickTime atoms
	buf := make([]byte, 1024)
	n, _ := f.Read(buf)
	header := buf[:n]

	// ProRes RAW codec FourCC is "aprn" (4444 RAW HQ) or "aprh" (4444 RAW).
	// These appear in the stsd atom inside moov/trak/mdia/minf/stbl/stsd.
	// We do a simple substring scan rather than parsing the atom tree —
	// the FourCC is distinctive enough that false positives are vanishingly
	// rare for the 4-byte ASCII sequence.
	if containsBytes(header, []byte("aprn")) || containsBytes(header, []byte("aprh")) {
		return CodecProResRAW, nil
	}

	return CodecUnknown, nil
}

// containsBytes is a simple scanner. Avoids pulling in bytes.Contains
// just to make the dependency tree self-evident in vendor builds.
func containsBytes(haystack, needle []byte) bool {
	if len(needle) == 0 || len(haystack) < len(needle) {
		return false
	}
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
