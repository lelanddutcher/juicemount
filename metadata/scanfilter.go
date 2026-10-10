package metadata

import (
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// ============================================================================
// Task #78: internal-namespace scan filter (single source of truth).
//
// THE BUG: the authoritative full SCAN (luaScanBatch + syncMetadata's Go path
// reconstruction) never yields entries in backend-internal namespaces, but the
// keyspace-push insert paths (applyEvent, reconcileDir) had no matching
// filter. Pushed events for those namespaces were mirrored into SQLite and
// then — being permanently absent from every SCAN diff — cycled forever in
// the pruneAbsent ladder (~112k pending_prune floor, live-probed 2026-07-02)
// and fueled the pre-G6 Layer-A probe storms.
//
// WHAT THE SCAN ACTUALLY EXCLUDES (read from the code, not guessed):
//
//   - luaScanBatch itself has NO name-level filters. Its MATCH 'd[0-9]*' only
//     excludes non-dentry keys (delfiles/delSlices — QA-34).
//   - The namespace exclusion is STRUCTURAL, in syncMetadata's Go path
//     reconstruction: an entry survives only if its parent-inode chain
//     resolves to root inode "1" within 50 hops. JuiceFS's built-in trash
//     tree hangs off the internal trash inode, which has NO dentry chain from
//     root — so the entire ".trash/..." tree can never appear in a SCAN diff.
//   - ".juicemount/..." (server-side derivative namespace) was live-proven
//     (task #78 probes, 2026-07-02) permanently absent from every SCAN diff
//     while its rows cycled in pruneAbsent.
//   - `._` AppleDouble sidecars are NOT name-filtered by the SCAN: a drained
//     `._` file exists in Redis under a root-resolvable dentry and IS
//     returned. Only un-drained (spool-only) sidecars are absent from Redis,
//     and the pre-existing `._` guards (pruneAbsent ladder, scopedPrune,
//     collectFastPathPrunes) already handle that. `._` therefore does NOT
//     belong in this filter — its push handling is deliberately unchanged.
//
// SINGLE SOURCE OF TRUTH: scanFilteredPath MUST stay in lockstep with the
// SCAN's effective exclusions above. If luaScanBatch / syncMetadata's path
// reconstruction ever changes which namespaces it yields, update this
// predicate in the same commit.
// ============================================================================

// scanInternalNamespaces are the volume-root-level namespaces the full SCAN
// can never return (see the block comment above).
var scanInternalNamespaces = [...]string{
	".trash",      // JuiceFS built-in trash (structurally unreachable from root inode 1)
	".juicemount", // server-side derivative namespace (task #78 live-proven SCAN-absent)
}

// scanFilteredPath reports whether p (a store-internal path, e.g.
// "movies/a.mov") is the root of, or lies under, a backend-internal namespace
// that the authoritative full SCAN never yields. Such paths must never be
// mirrored by the keyspace-push insert paths and must never be tracked by the
// pruneAbsent ladder — "absent from the SCAN" carries zero delete signal for
// them.
//
// Only VOLUME-ROOT-LEVEL namespaces match: "movies/.trash/x" is a user path
// and is NOT filtered.
func scanFilteredPath(p string) bool {
	for _, ns := range scanInternalNamespaces {
		if len(p) >= len(ns) && p[:len(ns)] == ns &&
			(len(p) == len(ns) || p[len(ns)] == '/') {
			return true
		}
	}
	return false
}

// ScanFilteredPath exposes scanFilteredPath to peer packages. The nfs
// FUSE-sourced mirror-insert paths (cold readdir listing, U7 async dir
// refresh, the prefetcher) must apply the SAME exclusions as the push paths:
// without them, any readdir descending into .trash/.juicemount re-mirrored
// the children the open-GC had just removed, every session (batch-3
// adversarial review #2/#4). Single source of truth stays in this file —
// see the lockstep comment above.
func ScanFilteredPath(p string) bool { return scanFilteredPath(p) }

// scanFilteredDescendant reports whether p lies STRICTLY UNDER a scan-
// filtered namespace — the bare ".trash"/".juicemount" dir rows themselves
// do NOT match (they do match scanFilteredPath).
//
// Batch-3 adversarial review #5: the bare dir rows are deliberately kept in
// the mirror (they match real FUSE dirs and are cheap), but excluding them
// from BOTH the pruneAbsent ladder and the scopedPrune candidates made them
// permanently unprunable — a ghost dir in root listings forever if the
// backend namespace is ever genuinely removed (juicefarm uninstalled and
// .juicemount rmdir'd server-side, trash disabled/compacted away). The prune
// TRACKING paths (trackAbsentPaths, scopedPrune candidate skip) therefore
// use THIS predicate: descendants stay untracked ("absent from the SCAN"
// carries zero delete signal for them), while the bare rows re-enter the
// normal FUSE-Lstat-verified prune paths — while the namespace exists on
// FUSE the Layer-A probe spares them (worst case 2 bounded Lstats per
// full-SCAN cycle — the pre-#78 status quo for 2 rows instead of ~112k);
// once FUSE genuinely reports ENOENT, the ladder/scopedPrune remove the
// ghost. Insert-skip paths and the open-GC keep using scanFilteredPath
// (root-or-descendant).
func scanFilteredDescendant(p string) bool {
	for _, ns := range scanInternalNamespaces {
		if len(p) > len(ns) && p[:len(ns)] == ns && p[len(ns)] == '/' {
			return true
		}
	}
	return false
}

// scanFilterSkipLogWindow bounds Debug logging of push-driven skips to one
// line per burst window: a keyspace burst (mass delete-to-trash, farm
// derivative fan-out) can carry thousands of events, and one line per event
// would be its own log storm.
const scanFilterSkipLogWindow = 5 * time.Second

var scanFilterSkip struct {
	mu      sync.Mutex
	last    time.Time
	pending int
}

// noteScanFilteredSkip records n push-driven mirror writes skipped because
// the target lies in a scan-filtered namespace, emitting at most one Debug
// line per scanFilterSkipLogWindow (the first skip of a burst logs
// immediately; the rest accumulate into the next line's count).
func noteScanFilteredSkip(source, sample string, n int) {
	scanFilterSkip.mu.Lock()
	scanFilterSkip.pending += n
	now := time.Now()
	if now.Sub(scanFilterSkip.last) < scanFilterSkipLogWindow {
		scanFilterSkip.mu.Unlock()
		return
	}
	count := scanFilterSkip.pending
	scanFilterSkip.pending = 0
	scanFilterSkip.last = now
	scanFilterSkip.mu.Unlock()
	jmlog.Debug("metadata: skipped push-driven mirror writes in scan-filtered namespace",
		"source", source, "sample", sample, "count", count)
}

// ============================================================================
// Streamed-partial name filter.
//
// The streaming spool drain writes a large file to a HIDDEN SIBLING of its
// destination and renames it into place only when complete (nfs/spool_streampath.go).
// While that copy is in flight — potentially hours for a multi-hundred-GB file —
// the partial is a REAL JuiceFS file: it has a root-resolvable dentry, so unlike
// the .trash and .juicemount namespaces above it IS returned by the authoritative
// SCAN and IS reported by keyspace push.
//
// That makes it structurally different from everything scanFilteredPath handles.
// Those are volume-root-level namespaces the SCAN can never yield; this is an
// ordinary file at an ordinary path whose NAME is the only thing marking it as
// not-yet-content. So the filter is name-level, not path-prefix-level, and
// scanFilteredPath deliberately does NOT cover it (".juicemount-streaming-…"
// does not match the ".juicemount" namespace either — that matcher requires the
// next character to be '/' or end-of-string).
//
// WHAT GOES WRONG WITHOUT THIS: the partial is mirrored, so it appears in Finder
// listings as a real file, its half-written size is published as authoritative,
// and a derivative generator could key a thumbnail off half a clip. Filtering at
// mirror entry is the single choke point that covers SCAN, push and reconcile at
// once — nothing that never enters the mirror can be served by a readdir, which
// is mirror-served.
//
// KEPT IN SYNC BY NAME, NOT BY IMPORT: nfs cannot import metadata's internals
// and metadata must not import nfs, so the prefix is duplicated deliberately.
// The two are pinned together by TestStreamTempPrefixMatchesMetadataFilter in
// package nfs, which fails if either side changes alone.
// ============================================================================

// streamPartialPrefix marks an in-progress streamed destination. MUST match
// nfs.streamTempPrefix exactly.
const streamPartialPrefix = ".juicemount-streaming-"

// StreamPartialName reports whether a directory-entry NAME is an in-flight
// streamed partial that must never be exposed as content.
//
// Takes a base name rather than a path: the partial is a sibling of its
// destination, so it can appear at any depth, and matching on the full path
// would need every caller to split it identically.
func StreamPartialName(name string) bool {
	return len(name) > len(streamPartialPrefix) &&
		name[:len(streamPartialPrefix)] == streamPartialPrefix
}
