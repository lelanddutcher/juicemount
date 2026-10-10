package metadata

import (
	"testing"
	"time"
)

func TestRenameParentsObservationCacheOnlyAndEvictedDurable(t *testing.T) {
	s := newTestStore(t)
	cacheOnly := MakeEntry("cache-only", true, 0, time.Now(), 41)
	s.InsertToCache(cacheOnly)
	durable := MakeEntry("durable", true, 0, time.Now(), 42)
	if err := s.Insert(durable); err != nil {
		t.Fatal(err)
	}
	// Model complete RAM eviction of an existing durable directory. Its
	// original real-inode handle and observed ID must still be admissible.
	s.mu.Lock()
	delete(s.pathCache, "durable")
	delete(s.inodeCache, 42)
	s.mu.Unlock()
	current, err := s.RenameParentsMatchObservation(
		RenameParentObservation{Path: "cache-only", HandleInode: 41, ObservedFileid: 41},
		RenameParentObservation{Path: "durable", HandleInode: 42, ObservedFileid: 42},
	)
	if err != nil || !current {
		t.Fatalf("live cache-only plus evicted durable parent refused: matched=%v err=%v", current, err)
	}
	for _, parent := range []RenameParentObservation{
		{Path: "durable", HandleInode: 42, ObservedFileid: 43},
		{Path: "durable", HandleInode: 43, ObservedFileid: 42},
		{Path: ".", HandleInode: 1, ObservedFileid: 2},
		{Path: "cache-only", HandleInode: 42, ObservedFileid: 41},
	} {
		current, err := s.RenameParentsMatchObservation(parent)
		if err != nil || current {
			t.Fatalf("mismatched parent accepted: %+v matched=%v err=%v", parent, current, err)
		}
	}
}
