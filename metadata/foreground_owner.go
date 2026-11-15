package metadata

import (
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
)

// FOREGROUND OWNERSHIP (laptop bug B1, 2026-11-14).
//
// A foreground NFS mutation (an in-place WRITE, a RENAME, a CREATE) captures a
// mirror token, changes the backing file through FUSE, then publishes the
// result with a compare-and-swap against that token. Over a 200 ms link the
// backing step takes 0.5–6 s. Meanwhile the background feeds see the
// mutation's own effect in Redis and publish it first: the i-key attr push
// (keyspace_attr.go), the d-key reconcileDir and its scoped prune
// (keyspace.go), and the pub/sub echo of this client's previous mutation
// (redis.go applyEvent). Each of those bumps the row's revision, so the
// foreground CAS fails with ErrMirrorConflict after the bytes or the name
// already changed. The NFS layer cannot retry that by name, so it keeps the
// names poisoned (nfs/namespace_operations.go finish) and the save fails.
//
// The fix: while a foreground operation owns a path, background publication
// of that path waits. Writers that hold writeMu check the guard inside that
// lock, and every foreground capture (CaptureMirror, CaptureRename, ...) also
// takes writeMu, so the order is total: a background row either lands before
// the operation captures its token, or it is skipped and retried after.

// ErrForegroundOwned refuses a background publication of a path that a
// foreground mutation owns. It wraps ErrMirrorConflict, so callers that
// already retry a conflict retry this too.
var ErrForegroundOwned = fmt.Errorf("path owned by a foreground mutation: %w", ErrMirrorConflict)

type foregroundOwnerFunc func(path string) bool

// SetForegroundOwner installs the probe the background feeds consult. fn must
// not call into Store (it runs under writeMu) and must be cheap.
func (s *Store) SetForegroundOwner(fn func(path string) bool) {
	if fn == nil {
		s.foregroundOwner.Store(nil)
		return
	}
	f := foregroundOwnerFunc(fn)
	s.foregroundOwner.Store(&f)
}

// ForegroundOwned reports whether a foreground mutation owns path now. Outside
// writeMu it is advisory (skip early work); the publication itself rechecks.
func (s *Store) ForegroundOwned(path string) bool {
	if path == "" {
		return false
	}
	p := s.foregroundOwner.Load()
	return p != nil && (*p)(path)
}

// IsForegroundOwned reports whether err is a foreground-ownership refusal.
func IsForegroundOwned(err error) bool { return errors.Is(err, ErrForegroundOwned) }

// InsertFromFeed is Insert for the unversioned pub/sub feed. The ownership
// check, the RAM publication and the durable write are one writeMu section,
// so no foreground capture can fall between the check and the write. It
// returns false when the path is owned and nothing was written.
func (s *Store) InsertFromFeed(e *Entry) (bool, error) {
	if !e.Version.IsZero() {
		return false, ErrMirrorConflict
	}
	if StreamPartialName(e.Name) {
		return true, nil
	}
	s.writeMu.Lock()
	if s.ForegroundOwned(e.Path) {
		s.writeMu.Unlock()
		return false, nil
	}
	s.InsertToCache(e)
	err := s.insertWriteLocked(e)
	s.writeMu.Unlock()
	if err == nil {
		s.wakeFTSCompactor()
	}
	return true, err
}

var feedEventsDeferred atomic.Int64

func noteFeedEventDeferred() { feedEventsDeferred.Add(1) }

// FeedEventsDeferred counts pub/sub events dropped because a foreground
// mutation owned their path (diagnostics and tests).
func FeedEventsDeferred() int64 { return feedEventsDeferred.Load() }

// validateAbsentOwned checks an absent-path token whose name a foreground
// operation owns: the name is still absent in the same store. It ignores the
// store-wide epoch, which unrelated paths move.
func validateAbsentOwned(tx *sql.Tx, expected MirrorToken) error {
	if expected.StoreID == "" || expected.Incarnation != "" {
		return ErrMirrorConflict
	}
	got, err := captureToken(tx, expected.Path)
	if err != nil {
		return err
	}
	if got.Incarnation != "" || got.StoreID != expected.StoreID {
		return ErrMirrorConflict
	}
	return nil
}
