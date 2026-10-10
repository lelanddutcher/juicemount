package metadata

import (
	"strings"
	"testing"
	"time"
)

// A reconcile hint contains no predecessor payload authority. It only queues
// the current parent for an authoritative read through the real receiver.
func TestReconcileCompletionHintRootNestedAndStalePayload(t *testing.T) {
	s := newTestStore(t)
	rc := &RedisClient{store: s}
	var queued []uint64
	fn := requeueFunc(func(ino uint64) { queued = append(queued, ino) })
	rc.keyspaceRequeue.Store(&fn)
	parent := MakeEntry("folder", true, 0, time.Unix(2000, 0), 77)
	if err := s.Insert(parent); err != nil {
		t.Fatal(err)
	}
	successor := MakeEntry("folder/._clip", false, 8, time.Unix(3000, 0), 88)
	successor.Mode = 0600
	if err := s.Insert(successor); err != nil {
		t.Fatal(err)
	}
	before := s.LookupByPath(successor.Path)
	if before == nil {
		t.Fatal("missing successor")
	}
	stale := MetadataEvent{Op: "reconcile", Path: successor.Path, Size: 9999, Mtime: 1, Inode: 5}
	rc.applyEvent(stale)
	rc.applyEvent(MetadataEvent{Op: "reconcile", Path: "._root", Size: 9999, Mtime: 1, Inode: 5})
	if len(queued) != 2 || queued[0] != 77 || queued[1] != 1 {
		t.Fatalf("queued %v, want nested77/root1", queued)
	}
	after := s.LookupByPath(successor.Path)
	if after == nil || after.Version != before.Version || after.Size != 8 || after.Inode != 88 || after.Mode != 0600 || after.Mtime.Unix() != 3000 {
		t.Fatalf("stale hint mutated successor %+v", after)
	}
	for _, bad := range []string{"../escape", "/absolute", "folder//x", "missing/._x", ".trash/._x", strings.Repeat("x", 4097), string([]byte{'x', 0xff})} {
		rc.applyEvent(MetadataEvent{Op: "reconcile", Path: bad, Size: 9999, Mtime: 1, Inode: 5})
	}
	if len(queued) != 2 {
		t.Fatalf("invalid/unavailable hint queued %v", queued)
	}
	rc.keyspaceRequeue.Store(nil)
	rc.applyEvent(stale) // unavailable consumer: no payload apply, no delivery claim
	after = s.LookupByPath(successor.Path)
	if after == nil || after.Version != before.Version || after.Size != 8 {
		t.Fatalf("consumer absence changed successor %+v", after)
	}
}
