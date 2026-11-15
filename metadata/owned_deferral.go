package metadata

import (
	"path"
	"sync"
	"time"
)

// OWNED DEFERRALS (review H1/M1/M2 of #100).
//
// A background publication that meets a path a foreground mutation owns
// (foreground_owner.go) is not a failure: the owner publishes its own result
// within seconds. It must not take the failure path (scheduleAttrRetry), which
// records baseline debt, marks push DEGRADED and arms full SCANs. Instead the
// folder or file is read again after attrRetryBase through whichever push
// session is live then. The queue belongs to the client, not to a session, so
// a resubscribe does not lose it: entries with no live consumer wait and are
// re-armed when the next session wires its coalescers.

type ownedDeferralQueue struct {
	mu    sync.Mutex
	dirs  map[uint64]struct{}
	files map[uint64]struct{}
	armed bool
}

var ownedDeferralsArmed func() // test hook: called when the timer is armed

// deferOwnedDir re-reads folder inode after attrRetryBase.
func (rc *RedisClient) deferOwnedDir(inode uint64) {
	rc.ownedDeferrals.add(rc, inode, true)
}

// deferOwnedFile re-reads file inode's attributes after attrRetryBase. Without
// a live attr push it falls back to the parent folder (see fire).
func (rc *RedisClient) deferOwnedFile(inode uint64) {
	rc.ownedDeferrals.add(rc, inode, false)
}

// deferOwnedPath re-reads the folder holding p (a SCAN row or a pub/sub event
// skipped while owned).
func (rc *RedisClient) deferOwnedPath(p string) {
	parent := path.Dir(p)
	if parent == "." || parent == "/" || parent == "" {
		rc.deferOwnedDir(1)
		return
	}
	if e := rc.store.LookupByPath(parent); e != nil && e.IsDir && e.Inode != 0 {
		rc.deferOwnedDir(e.Inode)
	}
}

func (q *ownedDeferralQueue) add(rc *RedisClient, inode uint64, dir bool) {
	if inode == 0 {
		return
	}
	q.mu.Lock()
	if q.dirs == nil {
		q.dirs, q.files = make(map[uint64]struct{}), make(map[uint64]struct{})
	}
	if dir {
		q.dirs[inode] = struct{}{}
	} else {
		q.files[inode] = struct{}{}
	}
	arm := !q.armed
	q.armed = true
	q.mu.Unlock()
	if arm {
		noteOwnedDeferral()
		if ownedDeferralsArmed != nil {
			ownedDeferralsArmed()
		}
		time.AfterFunc(attrRetryBase, func() { q.fire(rc) })
	}
}

// fire hands the queued inodes to the live coalescers. With none live it
// keeps them for resumeOwnedDeferrals.
func (q *ownedDeferralQueue) fire(rc *RedisClient) {
	select {
	case <-rc.stopCh:
		return
	default:
	}
	dirFn := rc.keyspaceRequeue.Load()
	attrFn := rc.attrRequeue.Load()
	q.mu.Lock()
	q.armed = false
	if dirFn == nil {
		q.mu.Unlock()
		return // no session: resumeOwnedDeferrals re-arms
	}
	dirs, files := q.dirs, q.files
	q.dirs, q.files = make(map[uint64]struct{}), make(map[uint64]struct{})
	q.mu.Unlock()
	for inode := range files {
		if attrFn != nil {
			(*attrFn)(inode)
			continue
		}
		// No attr push (JM_METADATA_ATTR_PUSH=0, or notify flags the
		// principal cannot set): the folder's reconcile reads its attrs.
		if e := rc.store.LookupByInode(inode); e != nil {
			parent := path.Dir(e.Path)
			if parent == "." {
				dirs[1] = struct{}{}
			} else if p := rc.store.LookupByPath(parent); p != nil && p.IsDir {
				dirs[p.Inode] = struct{}{}
			}
		}
	}
	for inode := range dirs {
		(*dirFn)(inode)
	}
}

// resumeOwnedDeferrals re-arms entries that waited for a session. Called once
// a new session has wired its coalescers.
func (rc *RedisClient) resumeOwnedDeferrals() {
	q := &rc.ownedDeferrals
	q.mu.Lock()
	pending := len(q.dirs)+len(q.files) > 0 && !q.armed
	if pending {
		q.armed = true
	}
	q.mu.Unlock()
	if pending {
		time.AfterFunc(attrRetryBase, func() { q.fire(rc) })
	}
}
