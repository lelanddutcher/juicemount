package metadata

import (
	"testing"
	"time"
)

func shrinkStore(t *testing.T) (*RedisClient, *Store) {
	t.Helper()
	st := newTestStore(t)
	rc := &RedisClient{store: st}
	// A mirrored parent, so reconcile has somewhere to aim.
	st.InsertToCache(&Entry{Path: "DCIM", Name: "DCIM", ParentPath: "", IsDir: true, Inode: 10})
	st.InsertToCache(&Entry{
		Path: "DCIM/clip.mov", Name: "clip.mov", ParentPath: "DCIM",
		Size: 1000, Inode: 42, Mtime: time.Unix(1000, 0),
	})
	return rc, st
}

// A SHRINKING EVENT MUST NOT BE APPLIED.
//
// Keyspace events carry no sequence or version and delivery is unordered, so a
// reordered event can lower a cached size below the truth — and every read after
// that is SHORT, with no error anywhere. Reachable rather than theoretical: the
// pub/sub replays this client's own writes back to it, so ordinary local
// activity generates the racing events.
func TestShrinkingEventDoesNotShrinkTheMirror(t *testing.T) {
	rc, st := shrinkStore(t)

	rc.applyEvent(MetadataEvent{
		Op: "update", Path: "DCIM/clip.mov", Size: 400, Inode: 42, Mtime: 2000,
	})

	got := st.LookupByPath("DCIM/clip.mov")
	if got == nil {
		t.Fatal("entry vanished")
	}
	if got.Size != 1000 {
		t.Errorf("cached size shrank to %d on an unordered event, want 1000 — every "+
			"read past %d would return short with no error", got.Size, got.Size)
	}
}

// A GROWING event still applies immediately. The append-only write stream is the
// common case and must not pay a reconcile round trip.
func TestGrowingEventAppliesDirectly(t *testing.T) {
	rc, st := shrinkStore(t)

	rc.applyEvent(MetadataEvent{
		Op: "update", Path: "DCIM/clip.mov", Size: 5000, Inode: 42, Mtime: 2000,
	})

	got := st.LookupByPath("DCIM/clip.mov")
	if got == nil || got.Size != 5000 {
		t.Fatalf("growing event did not apply (size=%v) — the common append path "+
			"must not be penalised", got)
	}
}

// THE SHRINK IS RECONCILED, NOT REFUSED.
//
// This is what keeps the fix from being MAX() by another name — the C4 bug,
// where a sticky high-water mark made a legitimate shrink permanently
// invisible. A shrinking event must requeue the PARENT so the authoritative
// size lands, downward or not.
func TestShrinkingEventRequeuesTheParentForReconcile(t *testing.T) {
	rc, _ := shrinkStore(t)

	var requeued []uint64
	fn := requeueFunc(func(ino uint64) { requeued = append(requeued, ino) })
	rc.keyspaceRequeue.Store(&fn)

	rc.applyEvent(MetadataEvent{
		Op: "update", Path: "DCIM/clip.mov", Size: 400, Inode: 42, Mtime: 2000,
	})

	if len(requeued) != 1 || requeued[0] != 10 {
		t.Fatalf("parent requeue = %v, want [10] — without it this is just MAX(), and "+
			"a legitimate remote truncate would never become visible", requeued)
	}
}

// Directories are exempt: a dir's Size is not file content and the shrink
// reasoning does not apply.
func TestShrinkGuardIgnoresDirectories(t *testing.T) {
	rc, st := shrinkStore(t)
	st.InsertToCache(&Entry{Path: "DCIM/sub", Name: "sub", ParentPath: "DCIM",
		IsDir: true, Size: 4096, Inode: 77})

	rc.applyEvent(MetadataEvent{
		Op: "update", Path: "DCIM/sub", Size: 0, IsDir: true, Inode: 77, Mtime: 2000,
	})

	if got := st.LookupByPath("DCIM/sub"); got == nil || got.Size != 0 {
		t.Errorf("directory event was diverted through the shrink guard (got %v)", got)
	}
}

// The guard must be counted. Applying a reordered shrink produces short reads
// with no error anywhere, so a rising count is the only signal that push is
// delivering out of order often enough to matter.
func TestShrinkReconcilesAreCounted(t *testing.T) {
	rc, _ := shrinkStore(t)
	before := KeyspaceShrinkReconciles()

	rc.applyEvent(MetadataEvent{
		Op: "update", Path: "DCIM/clip.mov", Size: 400, Inode: 42, Mtime: 2000,
	})

	if after := KeyspaceShrinkReconciles(); after != before+1 {
		t.Errorf("shrink-reconcile count %d -> %d, want +1 — an uncounted guard "+
			"cannot show whether reordering is happening in the field", before, after)
	}
}
