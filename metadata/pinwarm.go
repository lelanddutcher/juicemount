package metadata

import (
	"context"
	"errors"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// errPinInternalNamespace is returned when a pin root matches ScanFilteredPath
// (.trash/.juicemount). reconcileDir's #78 filter would silently no-op such a
// path, so warming it is impossible; callers surface this as a clear "cannot
// pin internal namespace" rather than weakening the filter.
var errPinInternalNamespace = errors.New("cannot pin internal namespace (scan-filtered: .trash/.juicemount)")

// ============================================================================
// Item 0 — pinned-subtree metadata warming.
//
// THE BUG this fixes: pinning a directory caches its file BLOCKS (pin store +
// prefetcher) but never writes the directory's METADATA subtree into the
// mirror. pin.CountFilesUnder returns FILES ONLY, so NFSServerPin/PinMany warm
// blocks but never reconcile subtree rows into the Store. Online a not-yet-
// mirrored pinned dir falls through to the FUSE fallback and lists; OFFLINE the
// handler correctly refuses FUSE and returns empty (nfs/handler.go) → a pinned
// dir lists EMPTY offline. A pinned dir MUST list offline (core promise).
//
// THE FIX: at pin time (and at boot, for dirs pinned in a prior session), warm
// the metadata mirror by looping the EXISTING reconcileDir over the pinned
// subtree and its ancestor chain. reconcileDir durably writes SQLite `entries`
// (via Insert -> ftsExternalUpsert's INSERT OR REPLACE INTO entries) AND the
// childrenIdx, so the rows survive a reboot — the load-bearing property that
// makes the pinned dir list offline. We deliberately REUSE reconcileDir rather
// than writing a parallel Redis fetcher.
//
// GATE: skip the whole pass when pin.IsOffline() — an offline reconcileDir
// burns a full 30s Redis timeout per dir (keyspace.go). The pin BLOCKS are
// already cached; metadata warming resumes when online.
//
// KILL SWITCH: JM_PIN_WARM_METADATA (default on; env-off = today's behavior).
//
// GUARD: a pin root under a scan-filtered namespace (.trash/.juicemount) is
// rejected — reconcileDir's #78 filter would silently no-op it, so warming it
// is impossible; surface that plainly rather than weakening the filter.
// ============================================================================

// pinWarmDefaultMaxDirs bounds ReconcileSubtree's directory walk. A photography
// reel or a deep project tree is thousands of dirs; 50k is a generous ceiling
// that still caps the work (and the Redis round-trips) if a user pins the whole
// volume by mistake.
const pinWarmDefaultMaxDirs = 50_000

// PinWarmMetadataEnabled reports whether pinned-subtree metadata warming is
// enabled. Default ON; JM_PIN_WARM_METADATA=0 restores today's behavior (blocks
// cached, metadata not warmed — pinned dirs stay empty offline until a full
// SCAN happens to cover them).
func PinWarmMetadataEnabled() bool {
	return pinWarmKillSwitch() != "0"
}

// pinWarmKillSwitch is split out so tests can assert the default-on semantics
// against the raw env value.
func pinWarmKillSwitch() string {
	return os.Getenv("JM_PIN_WARM_METADATA")
}

// ReconcileAncestors walks internalPath's parent chain to the tree root and
// reconciles each ancestor's PARENT so the full chain of dir rows is present in
// the mirror. Warming a pinned dir's rows is useless if the ancestor rows that
// lead to it are absent — offline ListChildren of the parent would not surface
// the pinned dir itself.
//
// internalPath is a store-internal (volume-relative) path, e.g.
// "movies/2026/reel". reconcileDir(parentInode) writes the children OF that
// parent, so to make `child` appear we must reconcile child's PARENT. We walk
// bottom-up: for each ancestor dir on the chain we resolve its inode from the
// store and reconcileDir it (which writes that dir's children — i.e. the next
// element on the chain). The tree root (inode 1) is always reconciled so
// top-level entries land under childrenIdx["."].
func (rc *RedisClient) ReconcileAncestors(internalPath string) error {
	if !PinWarmMetadataEnabled() {
		return nil
	}
	if pin.IsOffline() {
		jmlog.Info("pin warm: ReconcileAncestors skipped (offline)", "path", internalPath)
		return nil
	}
	// A pin root inside a scan-filtered namespace cannot be warmed (reconcileDir
	// #78-filters it). Reject clearly.
	if scanFilteredPath(internalPath) {
		jmlog.Warn("pin warm: cannot pin internal namespace (scan-filtered) — ReconcileAncestors refused",
			"path", internalPath)
		return errPinInternalNamespace
	}

	// Collect the ancestor dir paths, root-first. path.Dir on a bare top-level
	// name returns ".", which we treat as the tree root (inode 1). Deduplicate
	// via the walk terminating at "" / "." / ".".
	var chain []string // deepest ancestor last
	for p := path.Dir(internalPath); p != "" && p != "."; p = path.Dir(p) {
		chain = append(chain, p)
	}

	// Always reconcile the tree root so top-level entries (incl. the first
	// element of the chain, or internalPath itself when it is top-level) exist.
	// Route through keyspaceReconcileDir so the existing test seam observes it.
	if err := rc.keyspaceReconcileDir(1); err != nil {
		jmlog.Warn("pin warm: ReconcileAncestors root reconcile failed", "error", err.Error())
		// keep going — a per-dir failure must not abort the whole chain
	}

	// Walk root-first (reverse of the bottom-up collection) so each parent's
	// rows exist before we resolve the next child's inode from the store.
	for i := len(chain) - 1; i >= 0; i-- {
		anc := chain[i]
		ent := rc.store.LookupByPath(anc)
		if ent == nil || !ent.IsDir {
			// Ancestor not yet mirrored (or not a dir) — reconcileDir of the
			// PREVIOUS level should have established it; if it didn't, the
			// authoritative SCAN will. Nothing more we can do without its inode.
			continue
		}
		if err := rc.keyspaceReconcileDir(ent.Inode); err != nil {
			jmlog.Warn("pin warm: ReconcileAncestors reconcile failed",
				"ancestor", anc, "inode", ent.Inode, "error", err.Error())
		}
	}
	return nil
}

// ReconcileSubtree BFS-walks the directory tree rooted at rootInode, calling
// reconcileDir on every directory inode it discovers, bounded at maxDirs. This
// durably writes the pinned subtree's entries + childrenIdx into the mirror so
// the dir and all its descendants list OFFLINE.
//
// The walk is driven by the SAME machinery reconcileDir uses to enumerate
// children: HGETALL d{inode} yields the child (inode, filetype) pairs; a child
// with filetype==dir is enqueued for its own reconcile. reconcileDir writes the
// current dir's rows; we separately re-read d{inode} to find the child dirs to
// recurse into (reconcileDir does not return them). Both reads hit the same
// warm Redis dir hash, so the extra HGETALL is cheap relative to the per-child
// attr GETs reconcileDir already does.
//
// maxDirs<=0 uses pinWarmDefaultMaxDirs. On truncation we log clearly and stop
// (the authoritative SCAN eventually covers the remainder).
func (rc *RedisClient) ReconcileSubtree(rootInode uint64, maxDirs int) error {
	if !PinWarmMetadataEnabled() {
		return nil
	}
	if pin.IsOffline() {
		jmlog.Info("pin warm: ReconcileSubtree skipped (offline)", "root_inode", rootInode)
		return nil
	}
	if maxDirs <= 0 {
		maxDirs = pinWarmDefaultMaxDirs
	}

	// Guard the ROOT of the subtree against the scan-filtered namespace: if the
	// pinned dir itself is .trash/.juicemount (or under it), reconcileDir will
	// no-op every level. Resolve its path to check; a root we can't resolve is
	// left to the SCAN.
	if ent := rc.store.LookupByInode(rootInode); ent != nil && scanFilteredPath(ent.Path) {
		jmlog.Warn("pin warm: cannot pin internal namespace (scan-filtered) — ReconcileSubtree refused",
			"path", ent.Path, "inode", rootInode)
		return errPinInternalNamespace
	}

	visited := make(map[uint64]struct{}, 1024)
	queue := []uint64{rootInode}
	dirsDone := 0
	truncated := false

	for len(queue) > 0 {
		if dirsDone >= maxDirs {
			truncated = true
			break
		}
		dirInode := queue[0]
		queue = queue[1:]
		if _, seen := visited[dirInode]; seen {
			continue
		}
		visited[dirInode] = struct{}{}

		// Reconcile this dir's rows (children entries + childrenIdx, durable in
		// SQLite via Insert/BulkInsert). Route through keyspaceReconcileDir so
		// the existing test seam observes it. Errors are logged and skipped so
		// one unreachable dir doesn't abort the subtree.
		if err := rc.keyspaceReconcileDir(dirInode); err != nil {
			jmlog.Warn("pin warm: ReconcileSubtree reconcileDir failed",
				"inode", dirInode, "error", err.Error())
			// Still attempt to discover children below — HGETALL may succeed
			// even if a downstream attr GET in reconcileDir failed. But if the
			// HGETALL itself fails we simply won't enqueue anything.
		}
		dirsDone++

		// Discover child DIRECTORIES to recurse into, using the same d{inode}
		// hash decode reconcileDir uses. This is the walk driver.
		childDirs, derr := rc.childDirInodes(dirInode)
		if derr != nil {
			jmlog.Warn("pin warm: ReconcileSubtree child discovery failed",
				"inode", dirInode, "error", derr.Error())
			continue
		}
		for _, ci := range childDirs {
			if _, seen := visited[ci]; !seen {
				queue = append(queue, ci)
			}
		}
	}

	if truncated {
		jmlog.Warn("pin warm: ReconcileSubtree truncated at cap — remainder left to the full SCAN",
			"root_inode", rootInode, "dirs_reconciled", dirsDone, "max_dirs", maxDirs,
			"queue_remaining", len(queue))
	} else {
		jmlog.Info("pin warm: ReconcileSubtree complete",
			"root_inode", rootInode, "dirs_reconciled", dirsDone)
	}
	return nil
}

// childDirInodes reads d{dirInode} and returns the inodes of child entries that
// are directories (filetype==dir). Mirrors reconcileDir's decode so the subtree
// walk enumerates exactly the dirs reconcileDir would mirror. Non-dir children
// (files, symlinks) are ignored — reconcileDir already wrote their rows.
//
// Dispatches to the testChildDirInodes seam when set, so ReconcileSubtree's
// bounding/dedup/cap logic is testable without a live Redis.
func (rc *RedisClient) childDirInodes(dirInode uint64) ([]uint64, error) {
	if rc.testChildDirInodes != nil {
		return rc.testChildDirInodes(dirInode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rdb := rc.redisDB()
	key := "d" + strconv.FormatUint(dirInode, 10)
	raw, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	out := make([]uint64, 0, len(raw))
	for _, valStr := range raw {
		childInode, ft, ok := decodeDirChild([]byte(valStr))
		if !ok {
			continue
		}
		if ft == jfsTypeDir {
			out = append(out, childInode)
		}
	}
	return out, nil
}
