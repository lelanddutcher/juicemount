//go:build linux || darwin

package binder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// stats reads the counters by their JSON names.
func (f *fixture) stats() map[string]uint64 {
	f.t.Helper()
	data, err := json.Marshal(f.b.Stats())
	if err != nil {
		f.t.Fatal(err)
	}
	var all map[string]any
	if err := json.Unmarshal(data, &all); err != nil {
		f.t.Fatal(err)
	}
	out := map[string]uint64{}
	for k, v := range all {
		if n, ok := v.(float64); ok {
			out[k] = uint64(n)
		}
	}
	return out
}

// requireCarries checks that inode ino carries asset id through the given
// pair of records (LocationID's, or the continuation pair "-c"), hinting p.
func (f *fixture) requireCarries(ino uint64, pair string, id continuity.AssetID, p string) {
	f.t.Helper()
	base := LocationID(testUUID, ino) + pair
	want := catalog.LocationValue{AssetID: id, Source: "jfs:" + testUUID, Locator: fmt.Sprintf("ino:%d", ino)}
	if l := f.location(base); l.Removed || l.Value != want {
		f.t.Fatalf("%s = %+v, want active %+v", base, l, want)
	}
	wantHint := catalog.LocationValue{AssetID: id, Source: "jfs-path:" + testUUID, Locator: p}
	if l := f.location(base + "-hint"); l.Removed || l.Value != wantHint {
		f.t.Fatalf("%s-hint = %+v, want active %+v", base, l, wantHint)
	}
	ctx := context.Background()
	if v, err := f.b.AssetByPath(ctx, p); err != nil || v.AssetID != id || v.Path != p {
		f.t.Fatalf("by path %s = %+v, %v; want %s", p, v, err, id)
	}
	if got, err := f.b.AssetAt(ctx, p); err != nil || got != id {
		f.t.Fatalf("asset at %s = %s, %v; want %s", p, got, err, id)
	}
	if a, err := f.b.CheckAccess(ctx, id, func(string) bool { return true }); err != nil || !a.Allowed || a.Missing || a.Name != p {
		f.t.Fatalf("access to %s = %+v, %v", id, a, err)
	}
}

func (f *fixture) requireNoAsset(id continuity.AssetID) {
	f.t.Helper()
	if _, err := f.store.GetAsset(context.Background(), f.lib, id); !errors.Is(err, catalog.ErrNotFound) {
		f.t.Fatalf("asset %s exists: %v", id, err)
	}
}

func (f *fixture) requireEvent(id string) {
	f.t.Helper()
	if _, err := f.store.GetEvent(context.Background(), f.lib, id); err != nil {
		f.t.Fatalf("no operation %s: %v", id, err)
	}
}

// collect puts the asset in a new collection through the catalog API, as
// the collections routes do.
func (f *fixture) collect(id continuity.AssetID) catalog.CollectionID {
	f.t.Helper()
	col := catalog.CollectionID("col_" + string(id[len("asset_"):]))
	if _, err := f.store.Apply(context.Background(), catalog.Operation{
		Version: catalog.OperationVersion, LibraryID: f.lib, ID: "test-collect-" + string(id), Actor: "test",
		Changes: []catalog.Change{
			{CreateCollection: &catalog.CreateCollection{CollectionID: col, Name: "Cut"}},
			{AddCollectionMember: &catalog.AddCollectionMember{CollectionID: col, AssetID: id}},
		},
	}); err != nil {
		f.t.Fatal(err)
	}
	return col
}

// requireMember: the asset is an active member and resolves to a live file at p.
func (f *fixture) requireMember(col catalog.CollectionID, id continuity.AssetID, p string) {
	f.t.Helper()
	ctx := context.Background()
	m, err := f.store.GetCollectionMember(ctx, f.lib, col, id)
	if err != nil || m.Removed {
		f.t.Fatalf("member %s of %s = %+v, %v", id, col, m, err)
	}
	if v, err := f.b.AssetByID(ctx, id); err != nil || v.Removed || v.Path != p {
		f.t.Fatalf("member %s resolves to %+v, %v; want %s", id, v, err, p)
	}
}

// The usual save: write a temp file, rename it over the document. The new
// inode continues the document's asset, so it stays in its collection.
func TestAtomicSaveContinuesAsset(t *testing.T) {
	f := newFixture(t, 0, true)
	const doc = "Projects/edit.prproj"
	old := f.create(doc)
	f.feed(Event{Op: "create", Path: doc, Inode: old})
	id := f.requireBound(old, doc, "edit.prproj")
	col := f.collect(id)
	oldSeq := f.location(LocationID(testUUID, old)).UpdatedSequence

	tmp := f.create("Projects/.edit.prproj.tmp")
	f.rename("Projects/.edit.prproj.tmp", doc)
	history := []Event{
		{Op: "create", Path: "Projects/.edit.prproj.tmp", Inode: tmp},
		{Op: "rename", Path: doc, OldPath: "Projects/.edit.prproj.tmp", Inode: tmp},
	}
	f.feed(history...)
	f.requireCarries(tmp, "", id, doc)
	f.requireRemoved(old)
	f.requireNoAsset(AssetID(testUUID, tmp))
	f.requireMember(col, id, doc)
	f.requireEvent(fmt.Sprintf("jfs-%s-%d-continue-%d-0", testUUID, tmp, oldSeq))
	if a := f.asset(id); a.Label != "edit.prproj" {
		t.Fatalf("label changed: %+v", a)
	}
	if s := f.stats(); s["continued"] != 1 || s["merged"] != 0 || s["not_continued"] != 0 || s["failed"] != 0 {
		t.Fatalf("stats %v", s)
	}
	n := f.events()
	f.feed(history...)
	if got := f.events(); got != n {
		t.Fatalf("replay added %d operations", got-n)
	}

	// The next save continues it again; the earlier inode's records stay
	// as tombstones of the same asset.
	next := f.create("Projects/.edit.prproj.tmp")
	f.rename("Projects/.edit.prproj.tmp", doc)
	f.feed(Event{Op: "rename", Path: doc, OldPath: "Projects/.edit.prproj.tmp", Inode: next})
	f.requireCarries(next, "", id, doc)
	f.requireRemoved(tmp)
	f.requireMember(col, id, doc)
	if s := f.stats(); s["continued"] != 2 {
		t.Fatalf("stats %v", s)
	}
}

// The replacement is noticed from the volume's state, not the event kind:
// an update of the path handled before the rename (another client's event,
// or a lagging queue) continues the asset, and the rename then changes
// nothing.
func TestAtomicSaveNoticedOutOfOrder(t *testing.T) {
	f := newFixture(t, 0, true)
	old := f.create("A/clip.mov")
	f.feed(Event{Op: "create", Path: "A/clip.mov", Inode: old})
	id := f.requireBound(old, "A/clip.mov", "clip.mov")
	tmp := f.create("A/.clip.tmp")
	f.rename("A/.clip.tmp", "A/clip.mov")
	f.feed(Event{Op: "update", Path: "A/clip.mov", Inode: tmp})
	f.requireCarries(tmp, "", id, "A/clip.mov")
	n := f.events()
	f.feed(Event{Op: "rename", Path: "A/clip.mov", OldPath: "A/.clip.tmp", Inode: tmp})
	if f.events() != n || f.stats()["continued"] != 1 {
		t.Fatalf("the late rename recorded %d operations: %v", f.events()-n, f.stats())
	}
}

// The temp file was bound first (its create event came before the rename,
// so it got a provisional asset). The rename merges: the temp file's
// records are retired, its continuation pair carries the document's asset,
// and a relationship records the merge. Also for a temp file written in
// another folder and moved over.
func TestAtomicSaveMergesProvisionalAsset(t *testing.T) {
	for _, tmpPath := range []string{"Projects/edit.prproj.sb-1a2b", "Scratch/render-0001.tmp"} {
		t.Run(tmpPath, func(t *testing.T) {
			f := newFixture(t, 0, true)
			const doc = "Projects/edit.prproj"
			old := f.create(doc)
			f.feed(Event{Op: "create", Path: doc, Inode: old})
			id := f.requireBound(old, doc, "edit.prproj")
			col := f.collect(id)
			tmp := f.create(tmpPath)
			f.feed(Event{Op: "create", Path: tmpPath, Inode: tmp})
			provisional := f.requireBound(tmp, tmpPath, path.Base(tmpPath))
			f.rename(tmpPath, doc)
			f.feed(Event{Op: "rename", Path: doc, OldPath: tmpPath, Inode: tmp})

			f.requireCarries(tmp, "-c", id, doc)
			f.requireRemoved(old)
			f.requireRemoved(tmp) // the provisional pair
			f.requireMember(col, id, doc)
			rel, err := f.store.GetRelationship(context.Background(), f.lib, LocationID(testUUID, tmp)+"-merge")
			if err != nil || rel.Removed || rel.Value.SourceAssetID != provisional || rel.Value.TargetAssetID != id ||
				rel.Value.Predicate != "continuity:merged-into" || rel.Value.Provenance != catalog.Observed {
				t.Fatalf("merge relationship %+v, %v", rel, err)
			}
			if s := f.stats(); s["continued"] != 1 || s["merged"] != 1 || s["failed"] != 0 {
				t.Fatalf("stats %v", s)
			}

			// The continuation pair is the inode's binding from now on: a
			// rename moves its hint, a trip through .Trashes retires and
			// revives it, and the next save continues the asset again.
			f.rename(doc, "Projects/final.prproj")
			f.feed(Event{Op: "rename", Path: "Projects/final.prproj", OldPath: doc, Inode: tmp})
			f.requireCarries(tmp, "-c", id, "Projects/final.prproj")
			f.resolve(".Trashes/501")
			f.rename("Projects/final.prproj", ".Trashes/501/final.prproj")
			f.feed(Event{Op: "rename", Path: ".Trashes/501/final.prproj", OldPath: "Projects/final.prproj", Inode: tmp})
			if l := f.location(LocationID(testUUID, tmp) + "-c"); !l.Removed {
				t.Fatalf("trashed file still active: %+v", l)
			}
			f.rename(".Trashes/501/final.prproj", doc)
			f.feed(Event{Op: "rename", Path: doc, OldPath: ".Trashes/501/final.prproj", Inode: tmp})
			f.requireCarries(tmp, "-c", id, doc)
			f.requireRemoved(tmp)
			next := f.create("Projects/.edit.tmp")
			f.rename("Projects/.edit.tmp", doc)
			f.feed(Event{Op: "rename", Path: doc, OldPath: "Projects/.edit.tmp", Inode: next})
			f.requireCarries(next, "", id, doc)
			if l := f.location(LocationID(testUUID, tmp) + "-c"); !l.Removed {
				t.Fatalf("replaced continuation pair still active: %+v", l)
			}
			f.requireMember(col, id, doc)
		})
	}
}

// A file bound long ago is someone's document: moved over another file
// (Finder's replace), it keeps its own asset and the replaced file's is
// retired, as before. Counted.
func TestEstablishedFileMovedOverKeepsItsAsset(t *testing.T) {
	var offset atomic.Int64
	f := newFixtureWith(t, Config{Now: func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }}, true)
	old := f.create("A/clip.mov")
	mine := f.create("B/clip.mov")
	f.feed(Event{Op: "create", Path: "A/clip.mov", Inode: old}, Event{Op: "create", Path: "B/clip.mov", Inode: mine})
	f.requireBound(old, "A/clip.mov", "clip.mov")
	offset.Store(int64(2 * time.Hour))
	f.rename("B/clip.mov", "A/clip.mov")
	f.feed(Event{Op: "rename", Path: "A/clip.mov", OldPath: "B/clip.mov", Inode: mine})
	f.requireBound(mine, "A/clip.mov", "clip.mov")
	f.requireRemoved(old)
	if s := f.stats(); s["continued"] != 0 || s["not_continued"] != 1 {
		t.Fatalf("stats %v", s)
	}
}

// Hard links: a replaced file that keeps another name was not replaced (it
// keeps its asset, hinting its other name); a new file with another name
// is not continued (counted). Both are handled as before.
func TestAtomicSaveWithHardLinksFallsBack(t *testing.T) {
	t.Run("replaced file linked elsewhere", func(t *testing.T) {
		f := newFixture(t, 0, true)
		old := f.create("A/doc.txt")
		f.feed(Event{Op: "create", Path: "A/doc.txt", Inode: old})
		f.link(old, "Archive/doc.txt")
		tmp := f.create("A/.doc.tmp")
		f.rename("A/.doc.tmp", "A/doc.txt")
		f.feed(Event{Op: "rename", Path: "A/doc.txt", OldPath: "A/.doc.tmp", Inode: tmp})
		f.requireBound(old, "Archive/doc.txt", "doc.txt")
		f.requireBound(tmp, "A/doc.txt", "doc.txt")
		if s := f.stats(); s["continued"] != 0 || s["not_continued"] != 0 {
			t.Fatalf("stats %v", s)
		}
	})
	t.Run("new file linked elsewhere", func(t *testing.T) {
		f := newFixture(t, 0, true)
		old := f.create("A/doc.txt")
		f.feed(Event{Op: "create", Path: "A/doc.txt", Inode: old})
		tmp := f.create("A/.doc.tmp")
		f.link(tmp, "Other/doc-link.txt")
		f.rename("A/.doc.tmp", "A/doc.txt")
		f.feed(Event{Op: "rename", Path: "A/doc.txt", OldPath: "A/.doc.tmp", Inode: tmp})
		f.requireRemoved(old)
		f.requireBound(tmp, "A/doc.txt", "doc.txt")
		if s := f.stats(); s["continued"] != 0 || s["not_continued"] != 1 {
			t.Fatalf("stats %v", s)
		}
	})
}

// A Manager restart between the temp file's create and the rename empties
// the path cache; the new binder seeds it from the catalog's hints, so the
// save is still continued.
func TestAtomicSaveAcrossRestart(t *testing.T) {
	f := newFixture(t, 0, true)
	const doc = "Projects/edit.prproj"
	old := f.create(doc)
	f.feed(Event{Op: "create", Path: doc, Inode: old})
	id := f.requireBound(old, doc, "edit.prproj")
	col := f.collect(id)
	tmp := f.create("Projects/edit.prproj.sb-77")
	f.feed(Event{Op: "create", Path: "Projects/edit.prproj.sb-77", Inode: tmp})
	f.restart()
	f.rename("Projects/edit.prproj.sb-77", doc)
	f.feed(Event{Op: "rename", Path: doc, OldPath: "Projects/edit.prproj.sb-77", Inode: tmp})
	f.requireCarries(tmp, "-c", id, doc)
	f.requireRemoved(old)
	f.requireMember(col, id, doc)
	if s := f.stats(); s["merged"] != 1 {
		t.Fatalf("stats %v", s)
	}
}

// The safe save some Mac apps use has no rename over: write a temp file,
// rename the original to a backup name, rename the temp file to the
// original name, delete the backup. The new file continues the asset when
// the backup's delete follows within the window.
func TestSafeSaveContinuesAsset(t *testing.T) {
	for _, tc := range []struct {
		name, backup string
		lockstep     bool // each event handled before the next change
	}{
		{"visible backup, lagging events", "Projects/edit (backup).prproj", false},
		{"hidden backup, lagging events", "Projects/.edit.prproj.bak", false},
		{"visible backup, lockstep", "Projects/edit (backup).prproj", true},
		{"hidden backup, lockstep", "Projects/.edit.prproj.bak", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, 0, true)
			const doc, tmpPath = "Projects/edit.prproj", "Projects/edit.prproj.sb-9"
			old := f.create(doc)
			f.feed(Event{Op: "create", Path: doc, Inode: old})
			id := f.requireBound(old, doc, "edit.prproj")
			col := f.collect(id)
			var tmp uint64
			steps := []struct {
				do func()
				ev func() Event
			}{
				{func() { tmp = f.create(tmpPath) }, func() Event { return Event{Op: "create", Path: tmpPath, Inode: tmp} }},
				{func() { f.rename(doc, tc.backup) }, func() Event { return Event{Op: "rename", Path: tc.backup, OldPath: doc, Inode: old} }},
				{func() { f.rename(tmpPath, doc) }, func() Event { return Event{Op: "rename", Path: doc, OldPath: tmpPath, Inode: tmp} }},
				// A client's synthetic inode: the binder finds the backup by path.
				{func() { f.unlink(tc.backup, false) }, func() Event { return Event{Op: "delete", Path: tc.backup, Inode: 1<<63 | 9} }},
			}
			if tc.lockstep {
				for _, s := range steps {
					s.do()
					f.feed(s.ev())
				}
			} else {
				for _, s := range steps {
					s.do()
				}
				for _, s := range steps {
					f.feed(s.ev())
				}
			}
			pair := "-c" // the temp file was bound on its way
			if _, err := f.store.GetLocation(context.Background(), f.lib, LocationID(testUUID, tmp)+"-c"); errors.Is(err, catalog.ErrNotFound) {
				pair = ""
			}
			f.requireCarries(tmp, pair, id, doc)
			f.requireRemoved(old)
			f.requireMember(col, id, doc)
			if s := f.stats(); s["continued"] != 1 || s["failed"] != 0 {
				t.Fatalf("stats %v", s)
			}
		})
	}
}

// Without the backup's delete, or with it after the window, the backup is
// a file of its own: it keeps the asset and the new file gets another.
func TestSafeSaveNeedsTheBackupDeletedInTime(t *testing.T) {
	for _, tc := range []struct {
		name   string
		delete bool
		late   time.Duration
	}{{"backup kept", false, 0}, {"deleted after the window", true, time.Minute}} {
		t.Run(tc.name, func(t *testing.T) {
			var offset atomic.Int64
			f := newFixtureWith(t, Config{Now: func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }}, true)
			old := f.create("P/doc.txt")
			f.feed(Event{Op: "create", Path: "P/doc.txt", Inode: old})
			id := f.requireBound(old, "P/doc.txt", "doc.txt")
			f.rename("P/doc.txt", "P/doc.txt~")
			f.feed(Event{Op: "rename", Path: "P/doc.txt~", OldPath: "P/doc.txt", Inode: old})
			tmp := f.create("P/.doc.tmp")
			f.rename("P/.doc.tmp", "P/doc.txt")
			f.feed(Event{Op: "rename", Path: "P/doc.txt", OldPath: "P/.doc.tmp", Inode: tmp})
			offset.Store(int64(tc.late))
			if tc.delete {
				f.unlink("P/doc.txt~", false)
				f.feed(Event{Op: "delete", Path: "P/doc.txt~", Inode: old})
				f.requireRemoved(old)
			} else {
				f.requireCarries(old, "", id, "P/doc.txt~")
			}
			f.requireBound(tmp, "P/doc.txt", "doc.txt")
			if s := f.stats(); s["continued"] != 0 {
				t.Fatalf("stats %v", s)
			}
		})
	}
}

// The seed is bounded by the cache and only reads active hints.
func TestWarmSeedsRecentPaths(t *testing.T) {
	f := newFixtureWith(t, Config{PathCacheSize: 2}, true)
	a := f.create("W/a.mov")
	b := f.create("W/b.mov")
	c := f.create("W/c.mov")
	f.feed(Event{Op: "create", Path: "W/a.mov", Inode: a}, Event{Op: "create", Path: "W/b.mov", Inode: b}, Event{Op: "create", Path: "W/c.mov", Inode: c})
	f.unlink("W/c.mov", false)
	f.feed(Event{Op: "delete", Path: "W/c.mov", Inode: c})
	f.restart()
	f.waitConnected()
	if n := f.b.paths.len(); n != 2 {
		t.Fatalf("seeded %d paths", n)
	}
	for p, want := range map[string]uint64{"W/a.mov": a, "W/b.mov": b} {
		if got, ok := f.b.paths.get(p); !ok || got != want {
			t.Fatalf("seed %s = %d, %v", p, got, ok)
		}
	}
	if s := f.stats(); s["path_cache_evictions"] != 0 {
		t.Fatalf("seeding evicted: %v", s)
	}
}

func (f *fixture) waitConnected() {
	f.t.Helper()
	// Run seeds the cache before it takes the first event: one handled
	// event (of a path that does not exist) proves the seed finished.
	f.feed(Event{Op: "reconcile", Path: "Sync/missing.mov"})
}
