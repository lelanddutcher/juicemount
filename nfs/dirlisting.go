package nfs

import (
	"path"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Incomplete directory listings.
//
// A READDIR serves a directory's mirror rows as the WHOLE folder whenever it
// has any. Two producers can leave rows that are only part of the folder:
//
//   - a cold listing pass (coldDirListing) that hit a stat or gate timeout or
//     an I/O error publishes the children it did reach;
//   - a single-row insert (Stat/Lstat/ToHandle fallback, create, mkdir,
//     symlink, rename) into a directory that had no rows at all.
//
// Such a directory is recorded here, in memory, and a READDIR of it re-fires
// the bounded refresh on every link class: it is the user's own folder, not
// speculative prefetch. A complete pass clears the record. Push reconciles
// (HGETALL of the whole directory) and the SCAN never create partial sets, so
// they need no record.
//
// Retries are bounded two ways. A pass that ends incomplete backs off
// exponentially. And a directory is never retried while a FUSE call from an
// earlier pass is still parked holding a gate slot, so one hung child costs
// at most one slot, however often Finder re-reads its folder.

// dirListingRetryBase is the first backoff after an incomplete pass; it
// doubles per consecutive incomplete pass up to dirListingRetryMax.
var (
	dirListingRetryBase = time.Second
	dirListingRetryMax  = 30 * time.Second
)

// dirListingMaxTracked bounds the record. A record dropped at the bound only
// loses the re-fire for that folder (the pre-record behavior).
const dirListingMaxTracked = 8192

type dirListingState struct {
	failures int
	retryAt  time.Time
	parked   []<-chan struct{} // closed when that earlier FUSE call returns
}

// settledLocked drops parked calls that have returned and reports whether
// none is left. Caller holds dirRefreshMu.
func (st *dirListingState) settledLocked() bool {
	live := st.parked[:0]
	for _, ch := range st.parked {
		select {
		case <-ch:
		default:
			live = append(live, ch)
		}
	}
	for i := len(live); i < len(st.parked); i++ {
		st.parked[i] = nil
	}
	st.parked = live
	return len(live) == 0
}

// dirListingRetryAllowedLocked reports whether a refresh of dirname may run
// now. A directory without a record is always allowed. Caller holds
// dirRefreshMu.
func (h *JuiceMountHandler) dirListingRetryAllowedLocked(dirname string, now time.Time) bool {
	st := h.dirIncomplete[dirname]
	if st == nil {
		return true
	}
	return st.settledLocked() && !now.Before(st.retryAt)
}

// dirListingDue is the READDIR check: the directory is recorded incomplete
// and a refresh may run now. One atomic load when nothing is recorded.
func (h *JuiceMountHandler) dirListingDue(dirname string) bool {
	if h.dirIncompleteCount.Load() == 0 {
		return false
	}
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	return h.dirIncomplete[dirname] != nil && h.dirListingRetryAllowedLocked(dirname, time.Now())
}

func (h *JuiceMountHandler) dirListingStateLocked(dirname string) *dirListingState {
	st := h.dirIncomplete[dirname]
	if st != nil {
		return st
	}
	if len(h.dirIncomplete) >= dirListingMaxTracked {
		for k := range h.dirIncomplete {
			delete(h.dirIncomplete, k)
			break
		}
	}
	st = &dirListingState{}
	h.dirIncomplete[dirname] = st
	h.dirIncompleteCount.Store(int64(len(h.dirIncomplete)))
	return st
}

// markDirListingIncomplete records a pass of dirname that did not see every
// child, with the FUSE calls it left parked.
func (h *JuiceMountHandler) markDirListingIncomplete(dirname string, parked []<-chan struct{}) {
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	st := h.dirListingStateLocked(dirname)
	st.failures++
	backoff := dirListingRetryBase
	for i := 1; i < st.failures && backoff < dirListingRetryMax; i++ {
		backoff *= 2
	}
	if backoff > dirListingRetryMax {
		backoff = dirListingRetryMax
	}
	st.retryAt = time.Now().Add(backoff)
	st.parked = append(st.parked, parked...)
}

// markDirListingUnknown records that dirname is about to gain a row that did
// not come from a listing. No backoff: nothing has failed.
func (h *JuiceMountHandler) markDirListingUnknown(dirname string) {
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	h.dirListingStateLocked(dirname)
}

// clearDirListingIncomplete is called after a complete pass. It reports
// whether a record existed. Parked calls from earlier passes keep their
// slots until they return; the record only gated retries, so dropping it is
// safe.
func (h *JuiceMountHandler) clearDirListingIncomplete(dirname string) bool {
	if h.dirIncompleteCount.Load() == 0 {
		return false
	}
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	if _, ok := h.dirIncomplete[dirname]; !ok {
		return false
	}
	delete(h.dirIncomplete, dirname)
	h.dirIncompleteCount.Store(int64(len(h.dirIncomplete)))
	return true
}

// noteSingleChildInsert must be called BEFORE inserting one row for
// childPath that did not come from a listing of its parent. If the parent
// has no rows yet, the insert would make READDIR serve that one row as the
// whole folder, so the parent is recorded as unknown and its next READDIR
// runs a full listing. A parent this mount created itself (LocalOnly, not
// yet confirmed by a reconcile) holds only what we put there, so it is
// skipped. Cost: one RAM check, plus one indexed SQLite EXISTS when RAM has
// no rows for the parent.
func (h *JuiceMountHandler) noteSingleChildInsert(childPath string) {
	parent := path.Dir(childPath)
	if parent == "/" || parent == "" {
		parent = "."
	}
	if metadata.ScanFilteredPath(parent) {
		return
	}
	has, err := h.store.HasKnownChildren(parent)
	if err != nil || has {
		return
	}
	if parent != "." {
		if pe := h.store.LookupByPath(parent); pe != nil && pe.LocalOnly {
			return
		}
	}
	h.markDirListingUnknown(parent)
}

// dirListingRetryAllowed is dirListingRetryAllowedLocked for callers that do
// not hold dirRefreshMu.
func (h *JuiceMountHandler) dirListingRetryAllowed(dirname string) bool {
	if h.dirIncompleteCount.Load() == 0 {
		return true
	}
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	return h.dirListingRetryAllowedLocked(dirname, time.Now())
}
