package metadata

import (
	"errors"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

var publicationRaceRequeues atomic.Int64

func notePublicationRaceRequeue() { publicationRaceRequeues.Add(1) }

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
	races map[uint64]int // consecutive publication-race requeues per folder
}

// publicationRaceBudget bounds the consecutive folder re-reads after a
// concurrent background publication; past it the failure path runs.
const publicationRaceBudget = 3

// requeueAfterPublicationRace: two background publications (the attr push of
// one file and a folder's update) ran at once, and every fenced publication
// moves publicationGeneration, so the slower one fails its token check. When
// nothing else moved (same debt, session and backend: no event was lost), the
// folder is read again after attrRetryBase like an owned wait, instead of
// recording baseline debt and marking push DEGRADED. Each abroad own save
// produces one deferred attr refresh, so this race became the common case
// (review H1 of #100, second lab round). Bounded by publicationRaceBudget.
func (rc *RedisClient) requeueAfterPublicationRace(dirInode uint64, parentPath string, token scanBaselineToken, err error) bool {
	if !errors.Is(err, errScanPublicationInvalidated) {
		return false
	}
	rc.lifecycleMu.Lock()
	onlyPublication := rc.backendAdmittedLocked(token.backend) && token.debt == rc.debtGeneration &&
		token.session == rc.sessionGeneration
	rc.lifecycleMu.Unlock()
	if !onlyPublication || rc.keyspaceRequeue.Load() == nil {
		return false
	}
	q := &rc.ownedDeferrals
	q.mu.Lock()
	if q.races == nil {
		q.races = make(map[uint64]int)
	}
	q.races[dirInode]++
	n := q.races[dirInode]
	if n > publicationRaceBudget {
		delete(q.races, dirInode)
	}
	q.mu.Unlock()
	if n > publicationRaceBudget {
		return false
	}
	notePublicationRaceRequeue()
	jmlog.Info("metadata keyspace push: reconcileDir raced another publication; reading the folder again",
		"parent", parentPath, "try", n)
	rc.deferOwnedDir(dirInode)
	return true
}

func (q *ownedDeferralQueue) clearRace(dirInode uint64) {
	q.mu.Lock()
	delete(q.races, dirInode)
	q.mu.Unlock()
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
		// Its folder is read again anyway, and that read carries its attrs:
		// one publication instead of two that race each other.
		if e := rc.store.LookupByInode(inode); e != nil {
			if p := path.Dir(e.Path); p == "." {
				if _, ok := dirs[1]; ok {
					continue
				}
			} else if pe := rc.store.LookupByPath(p); pe != nil {
				if _, ok := dirs[pe.Inode]; ok {
					continue
				}
			}
		}
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
