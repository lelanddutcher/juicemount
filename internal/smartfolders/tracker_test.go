package smartfolders

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) tick()          { c.t = c.t.Add(time.Second) }

func newTestTracker(t *testing.T, max int, maxAge time.Duration) (*Tracker, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
	tr, err := NewTracker(TrackerConfig{Root: "Recents", MaxPerFolder: max, MaxAge: maxAge, Now: clk.now})
	if err != nil {
		t.Fatal(err)
	}
	return tr, clk
}

func paths(es []Entry) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.Path)
	}
	return out
}

func create(tr *Tracker, clk *fakeClock, p string) {
	clk.tick()
	tr.Apply(Event{Op: "create", Path: p})
}

func TestTrackerBoundsEachFolder(t *testing.T) {
	tr, clk := newTestTracker(t, 3, 0)
	for i := 1; i <= 5; i++ {
		create(tr, clk, fmt.Sprintf("A/f%d.mov", i))
	}
	create(tr, clk, "B/x.mov")
	if got := paths(tr.Recent("A")); !reflect.DeepEqual(got, []string{"A/f5.mov", "A/f4.mov", "A/f3.mov"}) {
		t.Fatalf("A = %v", got)
	}
	if got := paths(tr.Recent("B")); !reflect.DeepEqual(got, []string{"B/x.mov"}) {
		t.Fatalf("B = %v", got)
	}
	// Touching an old file again moves it to the front.
	create(tr, clk, "A/f3.mov")
	if got := paths(tr.Recent("A")); !reflect.DeepEqual(got, []string{"A/f3.mov", "A/f5.mov", "A/f4.mov"}) {
		t.Fatalf("A after touch = %v", got)
	}
	if got := tr.TakeDirty(); !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Fatalf("dirty = %v", got)
	}
	if got := tr.TakeDirty(); len(got) != 0 {
		t.Fatalf("dirty not cleared: %v", got)
	}
}

func TestTrackerAgeCutoff(t *testing.T) {
	tr, clk := newTestTracker(t, 10, time.Hour)
	create(tr, clk, "A/old.mov")
	clk.t = clk.t.Add(50 * time.Minute)
	create(tr, clk, "A/new.mov")
	clk.t = clk.t.Add(15 * time.Minute) // old.mov is now 65 minutes old
	if got := paths(tr.Recent("A")); !reflect.DeepEqual(got, []string{"A/new.mov"}) {
		t.Fatalf("Recent = %v", got)
	}
	tr.TakeDirty()
	tr.Expire()
	if got := tr.TakeDirty(); !reflect.DeepEqual(got, []string{"A"}) {
		t.Fatalf("Expire did not mark A dirty: %v", got)
	}
	clk.t = clk.t.Add(time.Hour)
	tr.Expire()
	if got := tr.Folders(); len(got) != 0 {
		t.Fatalf("folders after full expiry = %v", got)
	}
}

func TestTrackerIgnoredPaths(t *testing.T) {
	tr, clk := newTestTracker(t, 10, 0)
	ignored := []Event{
		{Op: "create", Path: "A/dir", IsDir: true},
		{Op: "create", Path: "A/link.mov", IsSymlink: true},
		{Op: "create", Path: "root-file.mov"},
		{Op: "create", Path: "A/.DS_Store"},
		{Op: "create", Path: "A/._clip.mov"},
		{Op: "create", Path: "A/.hidden/clip.mov"},
		{Op: "create", Path: "A/Icon\r"},
		{Op: "create", Path: ".trash/2026-11-02-12/1-A|clip.mov"},
		{Op: "create", Path: ".juicemount/derivatives/clip.jpg"},
		{Op: "create", Path: "Recents/A/clip.mov"},
		{Op: "create", Path: "A/../B/clip.mov"},
		{Op: "create", Path: "../A/clip.mov"},
		{Op: "create", Path: "A//clip.mov"},
		{Op: "create", Path: "A/./clip.mov"},
		{Op: "create", Path: "A/bad\x00.mov"},
		{Op: "create", Path: "A/\xff.mov"},
		{Op: "create", Path: "A/" + strings.Repeat("x", 256)},
		{Op: "create", Path: ""},
		{Op: "open", Path: "A/clip.mov"},
	}
	for _, ev := range ignored {
		clk.tick()
		if tr.Apply(ev) {
			t.Errorf("event %+v changed the tracker", ev)
		}
	}
	if got := tr.Folders(); len(got) != 0 {
		t.Fatalf("tracked %v", got)
	}
	// A leading slash is tolerated; "reconcile" (spool drain hint) counts.
	create(tr, clk, "/A/one.mov")
	clk.tick()
	tr.Apply(Event{Op: "reconcile", Path: "A/two.mov"})
	if got := paths(tr.Recent("A")); !reflect.DeepEqual(got, []string{"A/two.mov", "A/one.mov"}) {
		t.Fatalf("Recent = %v", got)
	}
}

func TestTrackerRenameAndDelete(t *testing.T) {
	tr, clk := newTestTracker(t, 10, 0)
	create(tr, clk, "A/clip.mov")
	clipAt := tr.Recent("A")[0].Changed
	create(tr, clk, "A/sub/one.mov")
	create(tr, clk, "A/sub/two.mov")
	create(tr, clk, "A/keep.mov")

	// A file rename moves the entry and keeps its time.
	clk.tick()
	tr.Apply(Event{Op: "rename", OldPath: "A/clip.mov", Path: "B/clip-final.mov"})
	if got := paths(tr.Recent("B")); !reflect.DeepEqual(got, []string{"B/clip-final.mov"}) {
		t.Fatalf("B = %v", got)
	}
	if !tr.Recent("B")[0].Changed.Equal(clipAt) {
		t.Fatal("rename changed the entry's time")
	}

	// A directory rename moves everything under it.
	clk.tick()
	tr.Apply(Event{Op: "rename", OldPath: "A/sub", Path: "A/renamed", IsDir: true})
	if got := paths(tr.Recent("A")); !reflect.DeepEqual(got, []string{"A/keep.mov", "A/renamed/two.mov", "A/renamed/one.mov"}) {
		t.Fatalf("A after dir rename = %v", got)
	}

	// An app saving through a hidden temp file: the destination is fresh.
	clk.tick()
	tr.Apply(Event{Op: "create", Path: "A/.edit.prproj.tmp"})
	clk.tick()
	tr.Apply(Event{Op: "rename", OldPath: "A/.edit.prproj.tmp", Path: "A/edit.prproj"})
	if got := tr.Recent("A")[0]; got.Path != "A/edit.prproj" || !got.Changed.Equal(clk.t) {
		t.Fatalf("saved file = %+v", got)
	}

	// Renaming into the trash, a hidden name or the smart root drops it.
	tr.Apply(Event{Op: "rename", OldPath: "A/keep.mov", Path: ".trash/x/keep.mov"})
	tr.Apply(Event{Op: "rename", OldPath: "A/edit.prproj", Path: "Recents/A/edit.prproj"})
	if got := paths(tr.Recent("A")); !reflect.DeepEqual(got, []string{"A/renamed/two.mov", "A/renamed/one.mov"}) {
		t.Fatalf("A = %v", got)
	}

	// A symlink renamed over a tracked file replaces it.
	tr.Apply(Event{Op: "rename", OldPath: "A/l", Path: "A/renamed/one.mov", IsSymlink: true})
	if got := paths(tr.Recent("A")); !reflect.DeepEqual(got, []string{"A/renamed/two.mov"}) {
		t.Fatalf("A after symlink rename = %v", got)
	}

	// Deletes drop a file, or a whole directory (deletes carry no is_dir).
	create(tr, clk, "A/renamed/three.mov")
	tr.Apply(Event{Op: "delete", Path: "A/renamed/three.mov"})
	tr.Apply(Event{Op: "delete", Path: "A/renamed"})
	tr.Apply(Event{Op: "delete", Path: "B"})
	if got := tr.Folders(); len(got) != 0 {
		t.Fatalf("folders after deletes = %v", got)
	}

	// Forget drops one path without marking anything dirty.
	create(tr, clk, "C/x.mov")
	tr.TakeDirty()
	tr.Forget("C/x.mov", "C/unknown.mov", "../bad")
	if len(tr.Folders()) != 0 || len(tr.TakeDirty()) != 0 {
		t.Fatal("Forget misbehaved")
	}
}

func TestTrackerPersistenceRoundTrip(t *testing.T) {
	tr, clk := newTestTracker(t, 3, 24*time.Hour)
	for i := 1; i <= 4; i++ {
		create(tr, clk, fmt.Sprintf("A/f%d.mov", i))
	}
	create(tr, clk, "B/sub/x.wav")
	data, gen, err := tr.MarshalState()
	if err != nil || gen == 0 {
		t.Fatalf("MarshalState: gen=%d err=%v", gen, err)
	}

	tr2, clk2 := newTestTracker(t, 3, 24*time.Hour)
	clk2.t = clk.t
	if err := tr2.LoadState(data); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"A", "B"} {
		if !reflect.DeepEqual(tr2.Recent(f), tr.Recent(f)) {
			t.Fatalf("%s: loaded %v, want %v", f, tr2.Recent(f), tr.Recent(f))
		}
	}
	if got := tr2.TakeDirty(); !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Fatalf("loaded folders not dirty: %v", got)
	}
	again, _, _ := tr2.MarshalState()
	if string(again) != string(data) {
		t.Fatalf("state is not stable:\n%s\n%s", again, data)
	}

	// Loading re-validates, bounds, clamps and ages out.
	future := clk.t.Add(time.Hour).Format(time.RFC3339Nano)
	old := clk.t.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	raw := `{"version":1,"entries":[
	 {"path":"A/1.mov","changed":"` + clk.t.Add(-1*time.Minute).Format(time.RFC3339Nano) + `"},
	 {"path":"A/2.mov","changed":"` + clk.t.Add(-2*time.Minute).Format(time.RFC3339Nano) + `"},
	 {"path":"A/3.mov","changed":"` + clk.t.Add(-3*time.Minute).Format(time.RFC3339Nano) + `"},
	 {"path":"A/4.mov","changed":"` + clk.t.Add(-4*time.Minute).Format(time.RFC3339Nano) + `"},
	 {"path":"A/future.mov","changed":"` + future + `"},
	 {"path":"B/old.mov","changed":"` + old + `"},
	 {"path":"../etc/passwd","changed":"` + future + `"},
	 {"path":".trash/x/y.mov","changed":"` + future + `"},
	 {"path":"Recents/A/1.mov","changed":"` + future + `"}
	]}`
	tr3, clk3 := newTestTracker(t, 3, 24*time.Hour)
	clk3.t = clk.t
	if err := tr3.LoadState([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	got := tr3.Recent("A")
	if !reflect.DeepEqual(paths(got), []string{"A/future.mov", "A/1.mov", "A/2.mov"}) || !got[0].Changed.Equal(clk.t) {
		t.Fatalf("loaded A = %+v", got)
	}
	if !reflect.DeepEqual(tr3.Folders(), []string{"A"}) {
		t.Fatalf("folders = %v", tr3.Folders())
	}
	if err := tr3.LoadState([]byte(`{"version":2,"entries":[]}`)); err == nil {
		t.Fatal("unknown version accepted")
	}
	if err := tr3.LoadState([]byte(`not json`)); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestParseEvent(t *testing.T) {
	ev, err := ParseEvent([]byte(`{"op":"rename","path":"A/b.mov","old_path":"A/a.mov","size":12,"mtime":1,"inode":7,"is_dir":false}`))
	if err != nil || ev != (Event{Op: "rename", Path: "A/b.mov", OldPath: "A/a.mov"}) {
		t.Fatalf("ParseEvent = %+v, %v", ev, err)
	}
	if _, err := ParseEvent([]byte(`{`)); err == nil {
		t.Fatal("garbage parsed")
	}
	if _, err := ParseEvent([]byte(`{"op":"create","path":"` + strings.Repeat("x", maxEventBytes) + `"}`)); err == nil {
		t.Fatal("oversized payload parsed")
	}
}

func TestNewTrackerRejectsBadRoot(t *testing.T) {
	for _, root := range []string{".trash", ".juicemount", "a/b", "..", ".hidden"} {
		if _, err := NewTracker(TrackerConfig{Root: root}); err == nil {
			t.Errorf("root %q accepted", root)
		}
	}
}

func TestTrackerFolderLimitAndExistence(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
	real := map[string]bool{"A": true, "B": true, "C": true, "D": true}
	asked := map[string]int{}
	tr, err := NewTracker(TrackerConfig{MaxFolders: 2, Now: clk.now, FolderOK: func(f string) bool {
		asked[f]++
		return real[f]
	}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		create(tr, clk, fmt.Sprintf("Fake%d/x.mov", i))
	}
	create(tr, clk, "A/1.mov")
	create(tr, clk, "A/2.mov") // A is tracked now: not asked again
	create(tr, clk, "B/1.mov")
	create(tr, clk, "C/1.mov") // over the folder limit
	if got := tr.Folders(); !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Fatalf("folders = %v", got)
	}
	if asked["A"] != 1 {
		t.Fatalf("FolderOK asked %d times for a tracked folder", asked["A"])
	}
	// A bare folder rename into a folder that does not exist is refused too.
	tr.Apply(Event{Op: "rename", OldPath: "A", Path: "Nope", IsDir: true})
	if got := tr.Folders(); !reflect.DeepEqual(got, []string{"B"}) {
		t.Fatalf("after rename to a missing folder: %v", got)
	}
	// Overlong paths are not tracked (they bound the state file).
	create(tr, clk, "B/"+strings.Repeat("x", 200)+"/"+strings.Repeat("y", 900))
	if len(tr.Recent("B")) != 1 {
		t.Fatal("overlong path tracked")
	}
	data, _, _ := tr.MarshalState()
	if len(data) > 4096 {
		t.Fatalf("state is %d bytes after a flood of fake events", len(data))
	}
}
