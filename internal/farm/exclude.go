package farm

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Derivative exclusion (2026-07-14, user-directed): the farm should not spend
// decode budget on media that isn't worth deriving. Three rules, applied at the
// walk (collectTargets) so an excluded file is never queued — no tech, no
// poster, no filmstrip, nothing:
//
//  1. PROXY — anything labeled a proxy (either spelling: "proxy" or "proxies"),
//     in a PARENT FOLDER or the FILENAME, is skipped. Proxies are already the
//     lightweight browse copy; deriving them duplicates the original's work.
//  2. TOO SMALL — media under the size floor (default 20 MiB, JM_FARM_MIN_SIZE_MB)
//     is skipped: a thumbnail/strip for a trivially small clip isn't worth the
//     per-file overhead in a mass sweep.
//  3. SKIP-DIR — a small default set of NLE ephemeral/cache dir names that hold
//     throwaway media (render previews, media caches). Extend with
//     JM_FARM_SKIP_DIRS (comma-separated, case-insensitive substrings).
//
// Everything is case-insensitive and matched against the path components.

// defaultSkipDirSubstrings are lowercased path substrings that mark a directory
// as non-content (ephemeral NLE output the farm should never derive). Kept
// deliberately CONSERVATIVE so real footage folders are never caught; the user
// adds more via JM_FARM_SKIP_DIRS.
var defaultSkipDirSubstrings = []string{
	"proxy", "proxies",
	"adobe premiere pro video previews",
	"adobe premiere pro audio previews",
	// "media cache" already covers "Media Cache Files/" (superstring), so the
	// longer form is intentionally omitted — a shorter substring matches both.
	"media cache",
	".cache", "peak files",
}

// appleDoubleHusk matches the orphaned AppleDouble shells macOS leaves beside
// media when a consumer does an atomic sidecar write over SMB — e.g.
// `._.C0012.MXF.logger.json.9F3A-...tmp.sb-1a2b3c4d-XyZ`. The consumer reported
// these on 2026-08-03 (FARM-4), owns the fix, and asked us not to classify them
// as sidecars or alarm on them meanwhile.
//
// Shape: an AppleDouble prefix `._` AND a `.tmp.sb-` marker somewhere after it.
// Both are required — `._` alone is an ordinary AppleDouble sidecar (which the
// NFS layer handles and which is NOT a husk), and `.tmp.sb-` alone is a live
// atomic-write temp we should also leave alone but which is not this class.
var appleDoubleHusk = regexp.MustCompile(`(^|/)\._.*\.tmp\.sb-`)

// IsAppleDoubleHusk reports whether path is one of those orphaned husks.
//
// In practice these are ~4 KB and were already excluded by the size floor, so
// this rule changes nothing today — it is here so the exclusion is EXPLICIT and
// survives someone lowering JM_FARM_MIN_SIZE_MB, rather than resting on a
// coincidence. Safe to garbage-collect on age > 24h, but the farm never deletes:
// we only decline to derive them.
func IsAppleDoubleHusk(path string) bool { return appleDoubleHusk.MatchString(path) }

// SkipDirSubstrings is the exported resolver (defaults + JM_FARM_SKIP_DIRS)
// for callers that pass the list into ExcludeReason.
func SkipDirSubstrings() []string { return skipDirSubstrings() }

// skipDirSubstrings returns the default skip substrings plus any from
// JM_FARM_SKIP_DIRS (comma-separated). All lowercased.
func skipDirSubstrings() []string {
	subs := make([]string, len(defaultSkipDirSubstrings))
	copy(subs, defaultSkipDirSubstrings)
	if raw := os.Getenv("JM_FARM_SKIP_DIRS"); raw != "" {
		for _, s := range strings.Split(raw, ",") {
			if s = strings.ToLower(strings.TrimSpace(s)); s != "" {
				subs = append(subs, s)
			}
		}
	}
	return subs
}

// pathIsProxy reports whether any path component or the filename marks this as
// a proxy — "proxy" OR "proxies" (the two spellings), case-insensitive. Note
// "proxies" does not contain the substring "proxy", so both are checked.
func pathIsProxy(lowerPath string) bool {
	return strings.Contains(lowerPath, "proxy") || strings.Contains(lowerPath, "proxies")
}

// ExcludeReason reports why a media file should be skipped from derivation, or
// "" to process it. size<0 means "size unknown" (the size rule is not applied).
// skipSubs is the resolved skip-dir substring list (pass skipDirSubstrings());
// minBytes is the size floor (0 = no floor).
// streamPartialPrefix marks an in-flight streamed destination. MUST match
// nfs.streamTempPrefix and metadata.streamPartialPrefix exactly — the farm is a
// separate server-side binary and cannot import either, so the constant is
// duplicated deliberately and pinned by test.
const streamPartialPrefix = ".juicemount-streaming-"

func ExcludeReason(path string, size, minBytes int64, skipSubs []string) string {
	l := strings.ToLower(path)
	if IsAppleDoubleHusk(path) {
		return "appledouble-husk"
	}
	// An in-flight STREAMED destination is incomplete by construction.
	//
	// The streaming spool drain writes a large file to a hidden sibling of its
	// destination and renames it into place only when complete. While that copy
	// is in flight — hours for a multi-hundred-GB file — the partial is a real,
	// walkable file of substantial size sitting next to real footage.
	//
	// collectTargets does NOT otherwise catch it: its dot-prefix rule is inside
	// `if info.IsDir()` and so skips dot-DIRECTORIES only, the only file-name
	// rule is the "._" AppleDouble prefix, and a partial is far too large to be
	// excluded by min-size. So without this the farm enqueues it and generates a
	// proxy or thumbnail from truncated bytes — the black-frame class of failure,
	// arriving through the farm rather than through a read.
	//
	// Checked on the BASE NAME, since the partial is a sibling and can appear at
	// any depth. Kept in lockstep with nfs.streamTempPrefix and
	// metadata.StreamPartialName by TestStreamPartialPrefixIsConsistent.
	if strings.HasPrefix(filepath.Base(path), streamPartialPrefix) {
		return "streaming-partial"
	}
	if pathIsProxy(l) {
		return "proxy"
	}
	for _, sub := range skipSubs {
		// A bare "proxy"/"proxies" is already handled above; skip re-matching
		// them here so the reason string is the specific "proxy".
		if sub == "proxy" || sub == "proxies" {
			continue
		}
		if strings.Contains(l, sub) {
			return "skip-dir:" + sub
		}
	}
	if minBytes > 0 && size >= 0 && size < minBytes {
		return "too-small"
	}
	return ""
}

// DirIsExcluded reports whether a DIRECTORY path should be skipped wholesale
// (proxy or a skip-dir) — used by the watcher to avoid even enqueuing it. Size
// is not a dir concept, so only the name rules apply.
func DirIsExcluded(path string) bool {
	l := strings.ToLower(path)
	if pathIsProxy(l) {
		return true
	}
	for _, sub := range skipDirSubstrings() {
		if sub == "proxy" || sub == "proxies" {
			continue
		}
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}
