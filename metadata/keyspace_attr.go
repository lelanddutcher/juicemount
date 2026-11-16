package metadata

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// ============================================================================
// File attribute push (peer edits of existing files).
//
// THE PROBLEM (laptop run 2026-11-13, bugs B1/B2): the keyspace push watches
// only d{inode} keys, which change when a folder's members change. A peer that
// appends to or overwrites an EXISTING file changes only that file's i{inode}
// key (its attr: length, mtime) and its c{inode}_* chunk lists. So the mirror
// kept the old size and mtime until the 24 h backstop SCAN, and the NFS layer
// had no reason to reopen its pooled FUSE descriptor, whose juicefs open state
// kept serving the old slices.
//
// THE FIX: subscribe to __keyspace@<db>__:i* on the same connection. Each
// event names an inode. A short debounce batches them; one MGET per batch (in
// chunks) reads the attrs; a row whose size or mtime differs is upserted
// through the same guarded reconcile path reconcileDir uses. That upsert fires
// the scan content hook, which drops the byte caches AND the pooled read
// descriptor for the path (nfs/handler.go onScanContentInvalidated), so the
// next READ reopens through juicefs and sees the new slices.
//
// COST: no round trip on any read. One MGET per batch, in the background, and
// only for inodes the mirror holds as regular files. Directory i-keys (every
// create in a folder touches its parent's attr) are dropped locally without a
// round trip. Our own writes echo here too: their rows usually already match,
// so they cost the MGET and nothing else; a pending spool entry is skipped.
//
// Like the d-feed, events are fire-and-forget: one lost while disconnected is
// recovered by the reconnect gap-fill SCAN, which upserts through the same
// hook. JM_METADATA_ATTR_PUSH=0 turns this feed off.
// ============================================================================

// AttrPushEnabled reports whether the file attribute push is on (default on).
func AttrPushEnabled() bool {
	return os.Getenv("JM_METADATA_ATTR_PUSH") != "0"
}

// keyspaceAttrNotifySufficient reports whether notify-keyspace-events
// publishes the string-class events (SET of an i key): 'K' plus 'A' or '$'.
func keyspaceAttrNotifySufficient(flags string) bool {
	return strings.Contains(flags, "K") && (strings.Contains(flags, "A") || strings.Contains(flags, "$"))
}

// ensureAttrNotify reports whether the server publishes i-key events. When it
// does not (an older NAS configured with the former "Kghx" recommendation),
// it adds '$' with CONFIG SET, as the d-feed self-heal does, unless
// JM_KEYSPACE_AUTOCONFIG=0. Through the proxy only an admin principal may;
// otherwise the feed stays off and the backstop SCAN remains the bound.
func (rc *RedisClient) ensureAttrNotify(ctx context.Context, flags string) bool {
	if keyspaceAttrNotifySufficient(flags) {
		return true
	}
	if os.Getenv("JM_KEYSPACE_AUTOCONFIG") != "0" && rc.testProbeKeyspaceConfig == nil {
		if err := rc.redisDB().ConfigSet(ctx, "notify-keyspace-events", flags+"$").Err(); err == nil {
			if _, now := rc.probeKeyspaceConfig(ctx); keyspaceAttrNotifySufficient(now) {
				jmlog.Info("metadata attr push: enabled string-class keyspace events", "flags", now)
				return true
			}
		}
	}
	jmlog.Warn("metadata attr push: off; the server's notify-keyspace-events lacks string-class events, so a peer's edit of an existing file reaches the mirror only at the backstop SCAN",
		"flags", flags, "want", "KEA or add $")
	return false
}

// parseFileInodeFromChannel extracts the inode from "__keyspace@1__:i732093".
// Any other key, or 'i' followed by a non-number, is not an inode attr key.
func parseFileInodeFromChannel(channel, prefix string) (uint64, bool) {
	if !strings.HasPrefix(channel, prefix) {
		return 0, false
	}
	key := channel[len(prefix):]
	if len(key) < 2 || key[0] != 'i' {
		return 0, false
	}
	inode, err := strconv.ParseUint(key[1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return inode, true
}

// attrTuningForClass sets the attr batch window. It is shorter than the d-feed
// window: a batch costs one MGET, not one HGETALL per folder, and this window
// is most of the visibility bound for a peer's edit (see the PR's RESULTS).
// maxWait caps the delay while a peer writes continuously (a render): then at
// most one MGET per maxWait.
func attrTuningForClass(c linkClass) coalescerTuning {
	switch c {
	case classLAN:
		return coalescerTuning{debounce: 100 * time.Millisecond, maxWait: time.Second, burstCeiling: attrBurstCeiling}
	case classWiFi:
		return coalescerTuning{debounce: 200 * time.Millisecond, maxWait: time.Second, burstCeiling: attrBurstCeiling}
	default:
		return coalescerTuning{debounce: 500 * time.Millisecond, maxWait: 2 * time.Second, burstCeiling: attrBurstCeiling}
	}
}

const (
	// attrMGetChunk keys per MGET round trip.
	attrMGetChunk = 500
	// attrBurstCeiling is the number of known files in one batch above which
	// the batch is handed to one full SCAN instead (a mass import or restore).
	attrBurstCeiling = 5000
	// attrMaxRetries bounds the re-queues of an inode whose attr read or
	// upsert failed (backend flap, a racing local publication).
	attrMaxRetries = 3
)

var (
	attrPushDelivered atomic.Int64 // i-key events received
	attrPushBatches   atomic.Int64 // batches that read attrs (one MGET or more)
	attrPushRefreshed atomic.Int64 // mirror rows whose size/mtime changed
	// attrPushLateCommits: rows another client changed under the same mtime
	// and size (mtime_span.go).
	attrPushLateCommits atomic.Int64
)

func noteAttrPushDelivered() { attrPushDelivered.Add(1) }

// AttrPushCounters exposes the attr feed's counts (tests, diagnostics).
func AttrPushCounters() (delivered, batches, refreshed int64) {
	return attrPushDelivered.Load(), attrPushBatches.Load(), attrPushRefreshed.Load()
}

// attrRetryBase is the first retry delay of a failed attr refresh; the n-th
// retry waits attrRetryBase << (n-1) (1 s, 2 s, 4 s). A variable for tests.
var attrRetryBase = time.Second

func (rc *RedisClient) newAttrCoalescer(generation uint64) *inodeCoalescer {
	c := newInodeCoalescer(rc)
	c.backendGeneration = generation
	c.tuningFn = func() coalescerTuning { return attrTuningForClass(currentLinkClass()) }
	var retryMu sync.Mutex
	retries := map[uint64]int{} // consecutive failures per inode
	c.apply = func(batch []uint64) {
		failed, deferred, err := rc.reconcileFileAttrsOwned(batch, generation)
		for _, inode := range deferred {
			// Client-lifetime queue: survives a resubscribe (review M1).
			rc.deferOwnedFile(inode)
		}
		failedSet := make(map[uint64]struct{}, len(failed))
		for _, inode := range failed {
			failedSet[inode] = struct{}{}
		}
		retryMu.Lock()
		defer retryMu.Unlock()
		// Only a success clears an inode's count. A failed inode keeps it, so
		// attrMaxRetries holds even when the retry lands in a later batch
		// (review MEDIUM-1: clearing the whole batch retried forever, ~10/s).
		for _, inode := range batch {
			if _, bad := failedSet[inode]; !bad {
				delete(retries, inode)
			}
		}
		var dropped []uint64
		for _, inode := range failed {
			n := retries[inode] + 1
			if n > attrMaxRetries || !rc.backendAdmitted(generation) {
				// Give up until the next event for this inode (or the
				// gap-fill/backstop SCAN) brings it back with a fresh count.
				delete(retries, inode)
				dropped = append(dropped, inode)
				continue
			}
			retries[inode] = n
			inode := inode
			time.AfterFunc(attrRetryBase<<(n-1), func() { c.add(inode) })
		}
		if err != nil {
			jmlog.Warn("metadata attr push: refresh failed", "inodes", len(batch), "retry", len(failed)-len(dropped),
				"gave_up", len(dropped), "error", err.Error())
		}
	}
	return c
}

// reconcileFileAttrs reads the attrs of the batch's known regular files and
// upserts those whose size or mtime changed, or that another client changed
// again under the same mtime (a late commit, mtime_span.go). It returns the
// inodes to retry.
func (rc *RedisClient) reconcileFileAttrs(inodes []uint64, generation uint64) (retry []uint64, err error) {
	retry, _, err = rc.reconcileFileAttrsOwned(inodes, generation)
	return retry, err
}

// reconcileFileAttrsOwned is reconcileFileAttrs that also returns the inodes
// deferred because a foreground mutation owns their path (an in-place WRITE
// still syncing: foreground_owner.go). A deferred inode is not a failure: it
// is read again after attrRetryBase, without using its retry budget.
func (rc *RedisClient) reconcileFileAttrsOwned(inodes []uint64, generation uint64) (retry, deferred []uint64, err error) {
	token := rc.scanToken()
	if token.backend != generation || !rc.scanTokenValid(token) {
		return nil, nil, ErrBackendUnqualified
	}
	guard := rc.loadSpoolGuard()
	known := make([]*Entry, 0, len(inodes))
	for _, inode := range inodes {
		e := rc.store.LookupByInode(inode)
		// A folder, an unknown inode (a new file reaches the mirror through
		// its parent's d-key event), a row this client has not seen in Redis
		// yet, or an internal namespace: nothing to refresh.
		if e == nil || e.IsDir || e.LocalOnly || !e.Mode.IsRegular() || e.Inode != inode || scanFilteredPath(e.Path) {
			continue
		}
		// A file with a pending local write: this client is the authority
		// for it until the drain publishes.
		if guard != nil && guard(e.Path) {
			continue
		}
		// This client is changing the file right now; its own publication
		// lands first (the Store rechecks under its writer lock).
		if rc.store.ForegroundOwned(e.Path) {
			deferred = append(deferred, inode)
			rc.store.renewOwnEcho(inode) // its window runs from the release (own_marks.go)
			continue
		}
		known = append(known, e)
	}
	if len(known) == 0 {
		return nil, deferred, nil
	}
	if len(known) > attrBurstCeiling {
		noteKeyspaceScanPromotion()
		jmlog.Info("metadata attr push: burst over ceiling, promoting to full SCAN",
			"files", len(known), "ceiling", attrBurstCeiling)
		rc.keyspaceTriggerSync()
		return nil, deferred, nil
	}
	attrPushBatches.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var toUpsert []*Entry
	for start := 0; start < len(known); start += attrMGetChunk {
		chunk := known[start:min(start+attrMGetChunk, len(known))]
		keys := make([]string, len(chunk))
		files := make([]sliceQuery, len(chunk))
		for i, e := range chunk {
			keys[i] = "i" + strconv.FormatUint(e.Inode, 10)
			files[i] = sliceQuery{e.Inode, e.Size}
		}
		// One round trip: the attrs and the slice signatures, the latter
		// for the cached size (mtime_span.go; review L1).
		vals, sigs, merr := rc.readAttrsAndSigs(ctx, keys, files)
		if merr != nil || len(vals) != len(chunk) {
			for _, e := range known[start:] {
				retry = append(retry, e.Inode)
			}
			if merr == nil {
				merr = ErrMirrorConflict
			}
			return retry, deferred, merr
		}
		for i, v := range vals {
			str, ok := v.(string)
			if !ok {
				continue // deleted: the parent's d-key event prunes the row
			}
			mtime, size, ok := decodeInodeAttrTime([]byte(str))
			if !ok {
				continue
			}
			e := chunk[i]
			sig := sigs[i]
			if chunksFor(size) != chunksFor(e.Size) {
				sig = unknownSig // read for other chunk lists
			}
			ctime, ctimeOK := decodeInodeAttrCtime([]byte(str))
			if e.Mtime.Equal(mtime) && e.MtimeNsecKnown && e.Size == size {
				if !ctimeOK {
					continue
				}
				prev, have := rc.store.attrObservation(e.Inode)
				if judgeCtime(prev, have, mtime.UnixNano(), ctime.UnixNano()) == verdictNone {
					continue
				}
				obs := attrObs{mtimeNS: mtime.UnixNano(), ctimeNS: ctime.UnixNano(), sig: sig}
				if rc.store.ownBytes(e.Inode, size, mtime) {
					// A later part of the Mac's own bytes: exactly the size
					// and mtime its own change produced (own_marks.go).
					rc.store.attrObs.putIf(e.Inode, obs, prev, have)
					continue
				}
				if sig.sameContent(prev.sig) {
					// The same slices: a chmod, chown, rename or link. The
					// event uses up an echo mark (review N4).
					rc.store.takeOwnChange(e.Inode)
					rc.store.attrObs.putIf(e.Inode, obs, prev, have)
					continue
				}
				if (!sig.known() || !prev.sig.known()) && rc.store.takeOwnChange(e.Inode) {
					// The signature cannot decide, and this process just
					// changed the file itself: that one event is its echo
					// (review H1 of the second review; the mark is used up).
					rc.store.attrObs.putIf(e.Inode, obs, prev, have)
					continue
				}
				// A late commit: it goes to the Store write. An echo mark is
				// used up only once that write commits (settleOwnMarks), so
				// a row skipped as owned keeps it (#100 review M4).
				toUpsert = append(toUpsert, &Entry{
					Path:           e.Path,
					Name:           e.Name,
					ParentPath:     e.ParentPath,
					Size:           size,
					Mtime:          mtime,
					MtimeNsecKnown: true,
					Inode:          e.Inode,
					Mode:           e.Mode,
					// Another client's part committed after this mtime.
					span: &attrSpan{ctime: ctime, sig: sig, lateCommit: true, echoGen: rc.store.ownEchoGen(e.Inode)},
				})
				continue
			}
			up := &Entry{
				Path:           e.Path,
				Name:           e.Name,
				ParentPath:     e.ParentPath,
				Size:           size,
				Mtime:          mtime,
				MtimeNsecKnown: true,
				Inode:          e.Inode,
				Mode:           e.Mode,
				// The Mac's own bytes (an exact content mark) are reported
				// with their true mtime (review N6). Nothing is used up
				// before the write (settleOwnMarks).
				span: &attrSpan{sig: sig, own: rc.store.ownBytes(e.Inode, size, mtime), echoGen: rc.store.ownEchoGen(e.Inode)},
			}
			if ctimeOK {
				up.span.ctime = ctime
			}
			toUpsert = append(toUpsert, up)
		}
	}
	// Another client's later change within its interval waits (finding R1,
	// peer_refresh_gate.go).
	toUpsert = rc.holdPeerChanges(toUpsert)
	if len(toUpsert) == 0 {
		return nil, deferred, nil
	}
	owned, err := rc.applyReconcileUpsertsOwned(toUpsert, &token)
	if err != nil {
		for _, e := range toUpsert {
			retry = append(retry, e.Inode)
		}
		return retry, deferred, err
	}
	ownedSet := make(map[string]struct{}, len(owned))
	for _, p := range owned {
		ownedSet[p] = struct{}{}
	}
	late, refreshed := 0, 0
	for _, e := range toUpsert {
		if _, held := ownedSet[e.Path]; held {
			// An owner took the path between the pre-check and the write:
			// nothing was recorded, and no own mark was used, so the re-read
			// judges it again (#100 review M4).
			deferred = append(deferred, e.Inode)
			rc.store.renewOwnEcho(e.Inode)
			continue
		}
		refreshed++
		if e.span != nil && e.span.lateCommit {
			late++
		}
		rc.store.settleOwnMarks(e) // mtime_span.go
		rc.notePeerPublished(e)    // peer_refresh_gate.go
	}
	if refreshed > 0 {
		attrPushRefreshed.Add(int64(refreshed))
		if late > 0 {
			attrPushLateCommits.Add(int64(late))
		}
		jmlog.Info("metadata attr push: refreshed", "events", len(inodes), "files", len(known), "changed", refreshed,
			"first", toUpsert[0].Path, "late_commits", late)
	}
	return nil, deferred, nil
}
