package binder

import (
	"context"
	"errors"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// Hints lists the catalog's path hints (stale after a folder rename);
// Locate resolves the live path by inode and reads the modification time
// from the volume; Applied sees each committed operation.
func TestHintsLocateAndApplied(t *testing.T) {
	var mu sync.Mutex
	var applied []string
	f := newFixtureWith(t, Config{Applied: func(op catalog.Operation) {
		mu.Lock()
		applied = append(applied, op.ID)
		mu.Unlock()
	}}, true)
	ctx := context.Background()
	if _, _, err := (&Binder{}).Locate(ctx, 1, ""); !errors.Is(err, ErrNotReady) {
		t.Fatalf("Locate before connect: %v", err)
	}
	a := f.create("Projects/A/a.mov")
	b := f.create("Projects/A/b.mov")
	f.feed(Event{Op: "create", Path: "Projects/A/a.mov", Inode: a}, Event{Op: "create", Path: "Projects/A/b.mov", Inode: b})
	mu.Lock()
	if len(applied) != 2 {
		t.Fatalf("applied = %v", applied)
	}
	mu.Unlock()
	hints := map[uint64]Hint{}
	if err := f.b.Hints(ctx, func(h Hint) bool { hints[h.Ino] = h; return true }); err != nil {
		t.Fatal(err)
	}
	if len(hints) != 2 || hints[a].Path != "Projects/A/a.mov" || hints[a].AssetID != AssetID(testUUID, a) || hints[a].Sequence <= 0 {
		t.Fatalf("hints = %+v", hints)
	}
	n := 0
	if err := f.b.Hints(ctx, func(Hint) bool { n++; return false }); err != nil || n != 1 {
		t.Fatalf("stopped walk: %d %v", n, err)
	}

	// The folder is renamed: one event for the directory, so the hints
	// stay stale, and Locate finds the live path by inode.
	f.rename("Projects/A", "Projects/B")
	f.feed(Event{Op: "rename", Path: "Projects/B", OldPath: "Projects/A", IsDir: true})
	if err := f.b.Hints(ctx, func(h Hint) bool { hints[h.Ino] = h; return true }); err != nil || hints[a].Path != "Projects/A/a.mov" {
		t.Fatalf("hint after the folder rename = %+v, %v", hints[a], err)
	}
	var attr meta.Attr
	mtime := time.Date(2026, 3, 4, 5, 6, 7, 8, time.UTC)
	attr.Mtime, attr.Mtimensec = mtime.Unix(), uint32(mtime.Nanosecond())
	if st := f.admin.SetAttr(f.ctx, meta.Ino(a), meta.SetAttrMtime, 0, &attr); st != 0 {
		t.Fatal(st)
	}
	p, mt, err := f.b.Locate(ctx, a, hints[a].Path)
	if err != nil || p != "Projects/B/a.mov" || !mt.Equal(mtime) {
		t.Fatalf("Locate = %q %v %v", p, mt, err)
	}
	// Gone: in the trash, or deleted for good.
	f.unlink("Projects/B/a.mov", false)
	f.unlink("Projects/B/b.mov", true)
	for _, ino := range []uint64{a, b} {
		if p, _, err := f.b.Locate(ctx, ino, path.Join("Projects/B", "x.mov")); p != "" || err != nil {
			t.Fatalf("Locate of a deleted file = %q %v", p, err)
		}
	}
	// A directory is not a file.
	if p, _, err := f.b.Locate(ctx, uint64(f.resolve("Projects/B")), ""); p != "" || err != nil {
		t.Fatalf("Locate of a directory = %q %v", p, err)
	}
}

// An update event moves the hint's sequence (the recency marker queries
// pick candidates by), coalesced per inode and rate limited; the value and
// every other record stay as they were.
func TestUpdateTouchesHintSequence(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	f := newFixtureWith(t, Config{Now: clock, TouchRate: 0.001, TouchBurst: 1}, true)
	a := f.create("P/a.mov")
	b := f.create("P/b.mov")
	f.feed(Event{Op: "create", Path: "P/a.mov", Inode: a}, Event{Op: "create", Path: "P/b.mov", Inode: b})
	hint := func(ino uint64) catalog.Location { return f.location(HintID(testUUID, ino)) }
	bound := hint(a)
	// Just bound: an update is coalesced.
	f.feed(Event{Op: "update", Path: "P/a.mov", Inode: a})
	if h := hint(a); h.UpdatedSequence != bound.UpdatedSequence || f.b.Stats().TouchesSkipped != 1 {
		t.Fatalf("touched right after the bind: %+v %+v", h, f.b.Stats())
	}
	advance(11 * time.Minute)
	events := f.events()
	f.feed(Event{Op: "update", Path: "P/a.mov", Inode: a})
	h := hint(a)
	if h.UpdatedSequence <= bound.UpdatedSequence || h.Value != bound.Value || h.Removed || f.events() != events+1 || f.b.Stats().Touched != 1 {
		t.Fatalf("after an update: %+v (was %+v), %+v", h, bound, f.b.Stats())
	}
	f.requireBound(a, "P/a.mov", "a.mov")
	// Again within the interval: coalesced. Another file: over the rate.
	f.feed(Event{Op: "update", Path: "P/a.mov", Inode: a}, Event{Op: "update", Path: "P/b.mov", Inode: b})
	if hint(a).UpdatedSequence != h.UpdatedSequence || hint(b).UpdatedSequence >= h.UpdatedSequence || f.b.Stats().TouchesSkipped != 3 {
		t.Fatalf("coalescing and rate: %+v %+v", hint(b), f.b.Stats())
	}
	// A create event (or rename) never touches.
	advance(time.Hour)
	f.feed(Event{Op: "create", Path: "P/b.mov", Inode: b})
	if f.b.Stats().Touched != 1 {
		t.Fatalf("a create touched: %+v", f.b.Stats())
	}
}
