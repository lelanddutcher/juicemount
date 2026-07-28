package metadata

// R1 — remote mutations must reach the consumer's fd pool, not just the mirror.
//
// applyEvent is how a delete or rename performed by ANOTHER writer (a peer Mac,
// the linux farm, OpenLoupe, ClipLogger) lands here. It updated the metadata
// mirror and stopped. The NFS layer, meanwhile, pools open FUSE fds keyed by
// PATH ALONE, so:
//
//	read X.mov here (fd pooled) → a peer deletes X.mov → the mirror entry drops
//	→ someone recreates X.mov and it re-mirrors → OpenFile takes the `e != nil`
//	branch → fdPool.Get hands back the fd to the DELETED inode
//
// Wrong bytes, no error at any layer — C1/C3 with a remote actor, in an
// explicitly multi-writer product. metadata/ cannot import nfs/, so the
// invalidation is published through Store.SetOnPathInvalidated, exactly like
// SetOnSubtreeRenamed.

import (
	"testing"
	"time"
)

type invalidation struct {
	path  string
	isDir bool
}

// captureInvalidations wires a probe onto the store and returns the accumulator.
// The probe deliberately calls BACK into the store: the hook must be fired
// OUTSIDE s.mu (the real consumer takes the FDPool mutex), and a regression that
// fires it under the lock deadlocks here instead of silently shipping a lock
// inversion between the store and the pool.
func captureInvalidations(s *Store) *[]invalidation {
	var got []invalidation
	s.SetOnPathInvalidated(func(p string, isDir bool) {
		_ = s.LookupByPath(p) // takes s.mu.RLock — deadlocks if fired under the lock
		got = append(got, invalidation{p, isDir})
	})
	return &got
}

func TestApplyEventRemoteDeleteInvalidatesThePath(t *testing.T) {
	s := newTestStore(t)
	rc := &RedisClient{store: s}
	got := captureInvalidations(s)

	s.InsertToCache(MakeEntry("movies/clip.mov", false, 100, time.Now(), 42))
	rc.applyEvent(MetadataEvent{Op: "delete", Path: "movies/clip.mov"})

	if len(*got) != 1 {
		t.Fatalf("remote delete fired %d invalidations, want 1 — a pooled fd survives the delete and "+
			"serves the deleted inode to whoever recreates the path (C3, remote actor)", len(*got))
	}
	if (*got)[0] != (invalidation{"movies/clip.mov", false}) {
		t.Fatalf("invalidation = %+v, want {movies/clip.mov false}", (*got)[0])
	}
}

// TestApplyEventRemoteDeleteOfADirectoryScopesToTheSubtree pins the type
// resolution: the delete publisher does NOT set IsDir, so the event alone cannot
// say whether a pooled SUBTREE hangs off this path. The mirror can, and it must
// be consulted BEFORE the eviction that is about to remove the answer.
func TestApplyEventRemoteDeleteOfADirectoryScopesToTheSubtree(t *testing.T) {
	s := newTestStore(t)
	rc := &RedisClient{store: s}
	got := captureInvalidations(s)

	s.InsertToCache(MakeEntry("shoot", true, 0, time.Now(), 7))
	rc.applyEvent(MetadataEvent{Op: "delete", Path: "shoot"}) // note: IsDir NOT set on the wire

	if len(*got) != 1 || !(*got)[0].isDir {
		t.Fatalf("invalidations = %+v, want one with isDir=true — the consumer would drop only the "+
			"directory's own key and leave every descendant's pooled fd serving its pre-delete inode", *got)
	}
}

func TestApplyEventRemoteRenameInvalidatesBothEnds(t *testing.T) {
	s := newTestStore(t)
	rc := &RedisClient{store: s}
	got := captureInvalidations(s)

	rc.applyEvent(MetadataEvent{
		Op: "rename", OldPath: "movies/a.mov", Path: "movies/a_v1.mov",
		Inode: 42, Mtime: time.Now().Unix(),
	})

	want := []invalidation{{"movies/a.mov", false}, {"movies/a_v1.mov", false}}
	if len(*got) != len(want) {
		t.Fatalf("remote rename fired %+v, want both ends %+v — the SOURCE name may be recreated and "+
			"the DESTINATION just replaced whatever rename unlinked there", *got, want)
	}
	for i := range want {
		if (*got)[i] != want[i] {
			t.Fatalf("invalidation[%d] = %+v, want %+v", i, (*got)[i], want[i])
		}
	}
}

// TestApplyEventRemoteDirectoryRenameScopesToTheSubtree: the rename publisher DOES
// carry IsDir (it rides along for the symlink/type discriminators), so both ends
// must be tree-scoped.
func TestApplyEventRemoteDirectoryRenameScopesToTheSubtree(t *testing.T) {
	s := newTestStore(t)
	rc := &RedisClient{store: s}
	got := captureInvalidations(s)

	rc.applyEvent(MetadataEvent{
		Op: "rename", OldPath: "shoot", Path: "shoot_old",
		Inode: 7, IsDir: true, Mtime: time.Now().Unix(),
	})

	if len(*got) != 2 || !(*got)[0].isDir || !(*got)[1].isDir {
		t.Fatalf("invalidations = %+v, want both ends with isDir=true", *got)
	}
}

// TestApplyEventCreateDoesNotInvalidate keeps the hook off the hot path: a
// create/update event is the overwhelmingly common one (every peer write, every
// self-echo), it does not change which inode lives at a path, and firing there
// would put a pool scan on every mirrored event for no correctness gain.
func TestApplyEventCreateDoesNotInvalidate(t *testing.T) {
	s := newTestStore(t)
	rc := &RedisClient{store: s}
	got := captureInvalidations(s)

	rc.applyEvent(MetadataEvent{Op: "create", Path: "movies/new.mov", Inode: 9, Mtime: time.Now().Unix()})
	rc.applyEvent(MetadataEvent{Op: "update", Path: "movies/new.mov", Inode: 9, Size: 10, Mtime: time.Now().Unix()})

	if len(*got) != 0 {
		t.Fatalf("create/update fired %+v, want none", *got)
	}
}

// TestNotifyPathInvalidatedUnwiredAndEmptyAreSafe guards the degenerate inputs
// the wiring can hand us (no consumer registered; an empty OldPath on a rename
// event, which is legal on the wire).
func TestNotifyPathInvalidatedUnwiredAndEmptyAreSafe(t *testing.T) {
	s := newTestStore(t)
	s.NotifyPathInvalidated("movies/x.mov", false) // no hook registered

	got := captureInvalidations(s)
	s.NotifyPathInvalidated("", false)
	if len(*got) != 0 {
		t.Fatalf("empty path fired %+v, want none", *got)
	}

	rc := &RedisClient{store: s}
	rc.applyEvent(MetadataEvent{Op: "rename", Path: "movies/b.mov", Inode: 3, Mtime: time.Now().Unix()})
	if len(*got) != 1 || (*got)[0].path != "movies/b.mov" {
		t.Fatalf("rename with no OldPath fired %+v, want only the destination", *got)
	}
}
