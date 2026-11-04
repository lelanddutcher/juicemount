package nfs

import (
	"errors"
	"io/fs"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
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
// Such a directory is recorded here, and a READDIR of it re-fires the bounded
// refresh on every link class: it is the user's own folder, not speculative
// prefetch. A complete pass clears the record, as does a pass that finds the
// directory gone. Push reconciles (HGETALL of the whole directory) and the
// SCAN never create partial sets, so they need no record.
//
// The record is kept in the mirror DB as well (metadata/dir_listing.go) and
// loaded at handler start, so a restart does not serve such a folder's rows
// as the whole folder. Memory holds the retry state and, up to
// dirListingMaxTracked, the membership; the DB holds every member. A local
// rename carries the records of the renamed tree and a local delete drops
// them, so neither the new path serves partial rows as complete nor a later
// folder at the old path inherits a stale record.
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

// dirListingMaxTracked bounds the in-memory record. A record dropped from
// memory at the bound stays in the DB (dirIncompleteSpilled), so its folder
// is still re-listed; it only loses its backoff state. A var for tests.
var dirListingMaxTracked = 8192

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

// dirListingNone reports that nothing is recorded, in memory or the DB.
func (h *JuiceMountHandler) dirListingNone() bool {
	return h.dirIncompleteCount.Load() == 0 && !h.dirIncompleteSpilled.Load()
}

// dirListingDue is the READDIR check: the directory is recorded incomplete
// and a refresh may run now. One atomic load when nothing is recorded; one
// indexed SQLite lookup for a folder not in memory once the bound spilled.
// A DB-only hit is brought back into memory, so its re-list is tracked
// (backoff, parked calls) like any other, unless dirListingPersistMu is busy:
// its holders write SQLite, and READDIR must not wait behind a bulk insert.
func (h *JuiceMountHandler) dirListingDue(dirname string) bool {
	if h.dirListingNone() {
		return false
	}
	h.dirRefreshMu.Lock()
	st := h.dirIncomplete[dirname]
	due := st != nil && h.dirListingRetryAllowedLocked(dirname, time.Now())
	h.dirRefreshMu.Unlock()
	if st != nil || !h.dirIncompleteSpilled.Load() {
		return due
	}
	has, err := h.store.HasDirListingIncomplete(dirname)
	if err != nil || !has {
		return false
	}
	if !h.dirListingPersistMu.TryLock() {
		return true
	}
	defer h.dirListingPersistMu.Unlock()
	// A complete pass may have cleared it meanwhile: do not bring it back.
	if has, err = h.store.HasDirListingIncomplete(dirname); err != nil || !has {
		return false
	}
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	h.dirListingStateLocked(dirname)
	return h.dirListingRetryAllowedLocked(dirname, time.Now())
}

// dirListingStateLocked returns dirname's state, creating it (created=true)
// when absent. At the bound a settled record leaves memory but not the DB,
// preferably one not backing off (its state is the least worth keeping). A
// record whose earlier FUSE call is still parked is never the one dropped:
// without its parked calls a re-list could pin another gate slot behind the
// same hung child. If every record is parked, memory exceeds the bound; the
// parked calls each hold a gate slot, so there are few of them. Caller holds
// dirListingPersistMu and dirRefreshMu.
func (h *JuiceMountHandler) dirListingStateLocked(dirname string) (st *dirListingState, created bool) {
	st = h.dirIncomplete[dirname]
	if st != nil {
		return st, false
	}
	if len(h.dirIncomplete) >= dirListingMaxTracked {
		now, victim := time.Now(), ""
		for k, old := range h.dirIncomplete {
			if !old.settledLocked() {
				continue
			}
			if victim == "" {
				victim = k
			}
			if !now.Before(old.retryAt) {
				victim = k
				break
			}
		}
		if victim != "" {
			delete(h.dirIncomplete, victim)
			h.dirIncompleteSpilled.Store(true)
		}
	}
	st = &dirListingState{}
	h.dirIncomplete[dirname] = st
	h.dirIncompleteCount.Store(int64(len(h.dirIncomplete)))
	return st, true
}

// persistDirListingMark writes a new record to the DB. Caller holds
// dirListingPersistMu, which orders DB writes like the memory changes.
func (h *JuiceMountHandler) persistDirListingMark(dirname string) {
	if err := h.store.MarkDirListingIncomplete(dirname); err != nil {
		jmlog.Warn("dir listing record not persisted", "dir", dirname, "error", err.Error())
	}
}

// markDirListingIncomplete records a pass of dirname that did not see every
// child, with the FUSE calls it left parked. Call it BEFORE publishing the
// pass's rows, so a crash between the two never leaves partial rows without
// a durable record.
func (h *JuiceMountHandler) markDirListingIncomplete(dirname string, parked []<-chan struct{}) {
	h.dirListingPersistMu.Lock()
	defer h.dirListingPersistMu.Unlock()
	h.dirRefreshMu.Lock()
	st, created := h.dirListingStateLocked(dirname)
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
	h.dirRefreshMu.Unlock()
	if created {
		h.persistDirListingMark(dirname)
	}
}

// markDirListingUnknown records that dirname is about to gain a row that did
// not come from a listing. No backoff: nothing has failed. It reports
// whether the record is new in memory.
func (h *JuiceMountHandler) markDirListingUnknown(dirname string) bool {
	h.dirListingPersistMu.Lock()
	defer h.dirListingPersistMu.Unlock()
	h.dirRefreshMu.Lock()
	_, created := h.dirListingStateLocked(dirname)
	h.dirRefreshMu.Unlock()
	if created {
		h.persistDirListingMark(dirname)
	}
	return created
}

// clearDirListingIncomplete is called after a complete pass, or one that
// found dirname gone. It reports whether a record existed. Parked calls from
// earlier passes keep their slots until they return; the record only gated
// retries, so dropping it is safe.
func (h *JuiceMountHandler) clearDirListingIncomplete(dirname string) bool {
	if h.dirListingNone() {
		return false
	}
	h.dirListingPersistMu.Lock()
	defer h.dirListingPersistMu.Unlock()
	h.dirRefreshMu.Lock()
	_, ok := h.dirIncomplete[dirname]
	delete(h.dirIncomplete, dirname)
	h.dirIncompleteCount.Store(int64(len(h.dirIncomplete)))
	h.dirRefreshMu.Unlock()
	spilled := h.dirIncompleteSpilled.Load()
	if !ok && !spilled {
		return false
	}
	deleted, err := h.store.ClearDirListingIncomplete(dirname)
	if err != nil {
		jmlog.Warn("dir listing record not cleared", "dir", dirname, "error", err.Error())
	}
	if spilled && deleted {
		h.unspillDirListingLocked()
	}
	return ok || deleted
}

// unspillDirListingLocked brings every durable record back into memory once
// the DB holds fewer than the bound, so READDIR stops consulting SQLite.
// Caller holds dirListingPersistMu.
func (h *JuiceMountHandler) unspillDirListingLocked() {
	n, err := h.store.CountDirListingIncomplete()
	if err != nil || n >= dirListingMaxTracked {
		return
	}
	dirs, err := h.store.ListDirListingIncomplete()
	if err != nil {
		return
	}
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	for _, d := range dirs {
		if h.dirIncomplete[d] == nil {
			h.dirIncomplete[d] = &dirListingState{}
		}
	}
	h.dirIncompleteCount.Store(int64(len(h.dirIncomplete)))
	h.dirIncompleteSpilled.Store(false)
}

// dirListingGone reports a readdir error that means the directory no longer
// exists: removed (ENOENT) or replaced by a non-directory (ENOTDIR).
func dirListingGone(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

// inDirTree reports whether p is dir or below it.
func inDirTree(p, dir string) bool {
	return p == dir || (strings.HasPrefix(p, dir) && p[len(dir)] == '/')
}

// dirListingTreeKeysLocked returns the recorded keys in dir's tree. It scans
// the whole record, so it runs under dirListingPersistMu alone: every writer
// of the map holds that lock too, and READDIR's dirRefreshMu stays free.
func (h *JuiceMountHandler) dirListingTreeKeysLocked(dir string) []string {
	var keys []string
	for k := range h.dirIncomplete {
		if inDirTree(k, dir) {
			keys = append(keys, k)
		}
	}
	return keys
}

// clearDirListingTree drops the records of dir and everything below it
// after directory dir was removed.
func (h *JuiceMountHandler) clearDirListingTree(dir string) {
	if h.dirListingNone() {
		return
	}
	h.dirListingPersistMu.Lock()
	defer h.dirListingPersistMu.Unlock()
	keys := h.dirListingTreeKeysLocked(dir)
	if len(keys) > 0 {
		h.dirRefreshMu.Lock()
		for _, k := range keys {
			delete(h.dirIncomplete, k)
		}
		h.dirIncompleteCount.Store(int64(len(h.dirIncomplete)))
		h.dirRefreshMu.Unlock()
	}
	spilled := h.dirIncompleteSpilled.Load()
	if len(keys) == 0 && !spilled {
		return
	}
	dropped, err := h.store.ClearDirListingIncompleteTree(dir)
	if err != nil {
		jmlog.Warn("dir listing records not cleared", "dir", dir, "error", err.Error())
	}
	if spilled && dropped > 0 {
		h.unspillDirListingLocked()
	}
}

// renameDirListingTree carries the records of oldpath and everything below
// it to newpath, with their retry state, and drops those of the newpath tree
// the rename replaced.
func (h *JuiceMountHandler) renameDirListingTree(oldpath, newpath string) {
	if h.dirListingNone() || oldpath == newpath {
		return
	}
	h.dirListingPersistMu.Lock()
	defer h.dirListingPersistMu.Unlock()
	from := h.dirListingTreeKeysLocked(oldpath)
	replaced := h.dirListingTreeKeysLocked(newpath)
	if len(from)+len(replaced) > 0 {
		h.dirRefreshMu.Lock()
		moved := make(map[string]*dirListingState, len(from))
		for _, k := range from {
			moved[newpath+k[len(oldpath):]] = h.dirIncomplete[k]
			delete(h.dirIncomplete, k)
		}
		for _, k := range replaced {
			delete(h.dirIncomplete, k)
		}
		for k, st := range moved {
			h.dirIncomplete[k] = st
		}
		h.dirIncompleteCount.Store(int64(len(h.dirIncomplete)))
		h.dirRefreshMu.Unlock()
	}
	spilled := h.dirIncompleteSpilled.Load()
	if len(from)+len(replaced) == 0 && !spilled {
		return
	}
	dropped, err := h.store.RenameDirListingIncomplete(oldpath, newpath)
	if err != nil {
		jmlog.Warn("dir listing records not renamed", "old", oldpath, "new", newpath, "error", err.Error())
	}
	if spilled && dropped > 0 {
		h.unspillDirListingLocked()
	}
}

// loadDirListingRecords restores the DB's records at handler start. Their
// retry state starts fresh: whatever FUSE calls an earlier process parked
// ended with it.
func (h *JuiceMountHandler) loadDirListingRecords() {
	dirs, err := h.store.LoadDirListingIncomplete()
	if err != nil {
		jmlog.Warn("dir listing records not loaded", "error", err.Error())
		return
	}
	h.dirListingPersistMu.Lock()
	defer h.dirListingPersistMu.Unlock()
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	for _, d := range dirs {
		if len(h.dirIncomplete) >= dirListingMaxTracked {
			h.dirIncompleteSpilled.Store(true)
			break
		}
		h.dirIncomplete[d] = &dirListingState{}
	}
	h.dirIncompleteCount.Store(int64(len(h.dirIncomplete)))
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
