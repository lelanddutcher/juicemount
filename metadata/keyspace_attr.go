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
		failed, err := rc.reconcileFileAttrs(batch, generation)
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
	token := rc.scanToken()
	if token.backend != generation || !rc.scanTokenValid(token) {
		return nil, ErrBackendUnqualified
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
		known = append(known, e)
	}
	if len(known) == 0 {
		return nil, nil
	}
	if len(known) > attrBurstCeiling {
		noteKeyspaceScanPromotion()
		jmlog.Info("metadata attr push: burst over ceiling, promoting to full SCAN",
			"files", len(known), "ceiling", attrBurstCeiling)
		rc.keyspaceTriggerSync()
		return nil, nil
	}
	attrPushBatches.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var toUpsert []*Entry
	type observed struct {
		inode        uint64
		mtime, ctime time.Time
	}
	var ctimes []observed // to record once the rows reflect them
	for start := 0; start < len(known); start += attrMGetChunk {
		chunk := known[start:min(start+attrMGetChunk, len(known))]
		keys := make([]string, len(chunk))
		for i, e := range chunk {
			keys[i] = "i" + strconv.FormatUint(e.Inode, 10)
		}
		var vals []interface{}
		var merr error
		if rc.testReadAttrs != nil {
			vals, merr = rc.testReadAttrs(keys)
		} else {
			vals, merr = rc.redisDB().MGet(ctx, keys...).Result()
		}
		if merr != nil || len(vals) != len(chunk) {
			for _, e := range known[start:] {
				retry = append(retry, e.Inode)
			}
			if merr == nil {
				merr = ErrMirrorConflict
			}
			return retry, merr
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
			sameMtime := e.Mtime.Equal(mtime) && e.MtimeNsecKnown
			bump := uint32(0)
			if sameMtime {
				bump = e.MtimeBump
			}
			ctime, ctimeOK := decodeInodeAttrCtime([]byte(str))
			if ctimeOK {
				if sameMtime && rc.attrCtimes.lateCommit(e.Inode, mtime, ctime) {
					bump++ // another client's part committed after this mtime
				}
				ctimes = append(ctimes, observed{e.Inode, mtime, ctime})
			}
			if sameMtime && e.Size == size && bump == e.MtimeBump {
				continue
			}
			toUpsert = append(toUpsert, &Entry{
				Path:           e.Path,
				Name:           e.Name,
				ParentPath:     e.ParentPath,
				Size:           size,
				Mtime:          mtime,
				MtimeNsecKnown: true,
				Inode:          e.Inode,
				Mode:           e.Mode,
				MtimeBump:      bump,
			})
		}
	}
	noteCtimes := func(skip map[uint64]bool) {
		for _, o := range ctimes {
			if !skip[o.inode] {
				rc.attrCtimes.note(o.inode, o.mtime, o.ctime)
			}
		}
	}
	if len(toUpsert) == 0 {
		noteCtimes(nil)
		return nil, nil
	}
	if err := rc.applyReconcileUpsertsWithToken(toUpsert, &token); err != nil {
		// The retry must see these ctimes as new again.
		failed := make(map[uint64]bool, len(toUpsert))
		for _, e := range toUpsert {
			retry = append(retry, e.Inode)
			failed[e.Inode] = true
		}
		noteCtimes(failed)
		return retry, err
	}
	noteCtimes(nil)
	attrPushRefreshed.Add(int64(len(toUpsert)))
	jmlog.Info("metadata attr push: refreshed", "events", len(inodes), "files", len(known), "changed", len(toUpsert),
		"first", toUpsert[0].Path, "mtime_bump", toUpsert[0].MtimeBump)
	return nil, nil
}
