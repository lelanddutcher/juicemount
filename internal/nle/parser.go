// Package nle parses video-editing project files and extracts the list
// of media file paths they reference. It is part of the JuiceMount
// offline-pin feature: when the user asks to pre-cache everything for
// a Premiere/Resolve/FCPX project, this package answers "what files?".
//
// The package supports three NLE formats:
//   - Adobe Premiere Pro .prproj  (gzipped XML)
//   - DaVinci Resolve .drp        (zip-of-sqlite, partial; see resolve.go)
//   - Apple Final Cut Pro X       (.fcpxml plain XML, .fcpbundle directory)
//
// All exported APIs live in this file. Format-specific implementations
// live in premiere.go, resolve.go, and fcpx.go.
package nle

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// MediaRef is a single media file referenced by a project.
type MediaRef struct {
	// Path is the absolute filesystem path of the media on disk.
	// Example: /Volumes/zpool/Footage/A001_C001.r3d
	Path string

	// Format is the lower-case extension without the dot ("r3d", "mov",
	// "wav"). Informational only; the prefetcher uses this to decide
	// whether to prefetch sidecars (e.g. .R3D + .RMD).
	Format string

	// Size is the file size in bytes if the project file recorded it,
	// otherwise 0. Pre-cache schedulers can use this to budget bandwidth.
	Size int64

	// Optional is true for proxies, peak files, audio waveforms, and
	// other low-priority prefetch targets. Required media has Optional
	// set to false.
	Optional bool
}

// ProjectKind identifies the editor that produced the project file.
type ProjectKind int

const (
	// KindPremiere is Adobe Premiere Pro (.prproj, gzipped XML).
	KindPremiere ProjectKind = iota

	// KindResolve is Blackmagic DaVinci Resolve (.drp, zip with SQLite).
	KindResolve

	// KindFCPX is Apple Final Cut Pro X (.fcpxml plain XML, or
	// .fcpbundle directory bundle).
	KindFCPX

	// KindUnknown means the path does not match any supported format.
	KindUnknown
)

// String returns a short human-readable name for the kind.
func (k ProjectKind) String() string {
	switch k {
	case KindPremiere:
		return "premiere"
	case KindResolve:
		return "resolve"
	case KindFCPX:
		return "fcpx"
	default:
		return "unknown"
	}
}

// ErrUnknownKind is returned by Parse when the file extension does not
// match any supported NLE format.
var ErrUnknownKind = errors.New("nle: unknown project kind")

// DetectKind sniffs a path by file extension (and, for ambiguous cases,
// by checking whether it is a directory bundle) and returns the
// ProjectKind. It does not open the file; a non-existent path with a
// recognised extension still returns the matching kind. This lets
// callers decide their own error policy.
func DetectKind(path string) ProjectKind {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".prproj":
		return KindPremiere
	case ".drp":
		return KindResolve
	case ".fcpxml":
		return KindFCPX
	case ".fcpbundle":
		// .fcpbundle is a directory; treat as FCPX regardless.
		return KindFCPX
	}
	// Fallback: a directory ending in .fcpbundle or containing a
	// CurrentVersion.fcpxml — be lenient.
	if fi, err := os.Stat(path); err == nil && fi.IsDir() {
		if strings.HasSuffix(strings.ToLower(path), ".fcpbundle") {
			return KindFCPX
		}
	}
	return KindUnknown
}

// Parse reads a project file at path and returns all media references
// it can extract. The returned error is non-nil only on hard parse
// failures (corrupt file, unreadable archive, unsupported format).
// Missing media that the project COULD have referenced — e.g. an asset
// element without a src attribute — is silently skipped; the caller
// gets the refs that were extractable with no error.
//
// Callers should treat the returned slice as best-effort. Premiere
// projects in particular contain many path variants per clip
// (master path, proxy path, peak file); Parse de-duplicates by Path.
func Parse(path string) ([]MediaRef, error) {
	kind := DetectKind(path)
	switch kind {
	case KindPremiere:
		return parsePremiere(path)
	case KindResolve:
		return parseResolve(path)
	case KindFCPX:
		return parseFCPX(path)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownKind, filepath.Ext(path))
	}
}

// dedupeRefs removes duplicate refs (same Path), preferring the
// non-Optional entry if both exist. Stable wrt input order.
func dedupeRefs(in []MediaRef) []MediaRef {
	if len(in) == 0 {
		return in
	}
	seen := make(map[string]int, len(in))
	out := make([]MediaRef, 0, len(in))
	for _, r := range in {
		if r.Path == "" {
			continue
		}
		if idx, ok := seen[r.Path]; ok {
			// Prefer the required version, prefer the one with size.
			if !r.Optional && out[idx].Optional {
				out[idx].Optional = false
			}
			if r.Size > 0 && out[idx].Size == 0 {
				out[idx].Size = r.Size
			}
			continue
		}
		seen[r.Path] = len(out)
		out = append(out, r)
	}
	return out
}

// extOf returns the lower-case extension of a path without the leading
// dot ("r3d", "mov", "wav"), or "" if there is no extension.
func extOf(path string) string {
	ext := filepath.Ext(path)
	if ext == "" {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(ext, "."))
}
