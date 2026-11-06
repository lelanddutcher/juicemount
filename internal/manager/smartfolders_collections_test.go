package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
)

// osVolume is the binder's view of a local directory: inode numbers from
// lstat, names by walking the tree. Test-only.
type osVolume struct{ root string }

const osVolumeUUID = "00000000-0000-4000-8000-0000000000c4"

func (v osVolume) VolumeUUID() string { return osVolumeUUID }
func (v osVolume) Close() error       { return nil }

func osNode(fi fs.FileInfo) jfsmeta.Node {
	st := fi.Sys().(*syscall.Stat_t)
	n := jfsmeta.Node{Ino: uint64(st.Ino), Type: smartfolders.TypeOther, Nlink: uint32(st.Nlink), Parent: 1, Mtime: fi.ModTime()}
	switch m := fi.Mode(); {
	case m&fs.ModeSymlink != 0:
		n.Type = smartfolders.TypeSymlink
	case m.IsDir():
		n.Type = smartfolders.TypeDir
	case m.IsRegular():
		n.Type = smartfolders.TypeRegular
	}
	return n
}

func (v osVolume) LookupIno(p string) (jfsmeta.Node, error) {
	fi, err := os.Lstat(filepath.Join(v.root, filepath.FromSlash(p)))
	if err != nil {
		return jfsmeta.Node{}, syscall.ENOENT
	}
	return osNode(fi), nil
}

func (v osVolume) walk(ino uint64) ([]string, *jfsmeta.Node) {
	var out []string
	var node *jfsmeta.Node
	_ = filepath.WalkDir(v.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		if n := osNode(fi); n.Ino == ino && p != v.root {
			rel, _ := filepath.Rel(v.root, p)
			out = append(out, filepath.ToSlash(rel))
			node = &n
		}
		return nil
	})
	return out, node
}

func (v osVolume) StatIno(ino uint64) (jfsmeta.Node, error) {
	if _, n := v.walk(ino); n != nil {
		return *n, nil
	}
	return jfsmeta.Node{}, syscall.ENOENT
}

func (v osVolume) GetPaths(ino uint64) ([]string, error) {
	paths, n := v.walk(ino)
	if n == nil {
		return nil, syscall.ENOENT
	}
	return paths, nil
}

// collViewsEnv: a local directory as the volume, the binder and the catalog
// over it, and the smart-folder service with collection views attached.
type collViewsEnv struct {
	t       *testing.T
	root    string
	cont    *continuityService
	svc     *smartFoldersService
	clk     *testClock
	leaser  *fakeLeaser
	library continuity.LibraryID
	ops     int
}

func newCollViewsEnv(t *testing.T, files ...string) *collViewsEnv {
	t.Helper()
	root := t.TempDir()
	for _, f := range files {
		writeVolumeFile(t, root, f)
	}
	cfg := continuityTestConfig(t)
	store, err := openContinuityCatalog(filepath.Dir(cfg.StateFile))
	if err != nil {
		t.Fatal(err)
	}
	cont := &continuityService{store: store}
	b, err := binder.New(binder.Config{Store: store, Applied: cont.applied, TouchInterval: time.Nanosecond,
		Connect: func() (binder.Volume, error) { return osVolume{root}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	cont.binder = b
	cont.start()
	t.Cleanup(cont.Stop)
	library, err := store.LibraryID()
	if err != nil {
		t.Fatal(err)
	}
	e := &collViewsEnv{t: t, root: root, cont: cont, library: library, leaser: &fakeLeaser{},
		clk: &testClock{t: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}}
	e.bind(files...)
	e.svc = newTestSmartFolders(t, svcOpts{root: root, leaser: e.leaser, clock: e.clk})
	t.Cleanup(func() { _ = e.svc.cfg.Redis.Close() })
	e.svc.coll.debounce = time.Hour // passes are driven by the test
	return e
}

// attach turns collection views on, as startContinuity does.
func (e *collViewsEnv) attach() {
	e.t.Helper()
	a := &API{jobs: &JobManager{smartFolders: e.svc}}
	a.attachCollectionViews(e.cont)
	if e.cont.views.Load() != e.svc {
		e.t.Fatal("collection views not attached")
	}
}

func (e *collViewsEnv) ino(p string) uint64 {
	e.t.Helper()
	n, err := osVolume{e.root}.LookupIno(p)
	if err != nil {
		e.t.Fatalf("%s: %v", p, err)
	}
	return n.Ino
}

func (e *collViewsEnv) asset(p string) continuity.AssetID {
	return binder.AssetID(osVolumeUUID, e.ino(p))
}

// bind feeds create events to the binder and waits until they are applied.
func (e *collViewsEnv) bind(files ...string) {
	e.t.Helper()
	before := e.cont.binder.Stats().Handled
	for _, f := range files {
		e.cont.binder.Offer(binder.Event{Op: "create", Path: f, Inode: e.ino(f)})
	}
	deadline := time.Now().Add(10 * time.Second)
	for e.cont.binder.Stats().Handled < before+uint64(len(files)) {
		if time.Now().After(deadline) {
			e.t.Fatalf("events not handled: %+v", e.cont.binder.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (e *collViewsEnv) apply(changes ...catalog.Change) {
	e.t.Helper()
	e.ops++
	if _, err := e.cont.store.Apply(context.Background(), catalog.Operation{Version: catalog.OperationVersion,
		LibraryID: e.library, ID: fmt.Sprintf("test-op-%d", e.ops), Actor: "admin", Changes: changes}); err != nil {
		e.t.Fatal(err)
	}
}

func collID(n int) catalog.CollectionID { return catalog.CollectionID(fmt.Sprintf("col_%032x", n)) }

func (e *collViewsEnv) create(id catalog.CollectionID, name string, members ...string) {
	e.t.Helper()
	changes := []catalog.Change{{CreateCollection: &catalog.CreateCollection{CollectionID: id, Name: name}}}
	e.apply(changes...)
	for _, m := range members {
		e.add(id, m) // one at a time: join order is the order given
	}
}

func (e *collViewsEnv) add(id catalog.CollectionID, p string) {
	e.apply(catalog.Change{AddCollectionMember: &catalog.AddCollectionMember{CollectionID: id, AssetID: e.asset(p)}})
}

func (e *collViewsEnv) pass() { e.svc.collectionsOnlyCycle(context.Background()) }

func (e *collViewsEnv) move(from, to string) {
	e.t.Helper()
	dst := filepath.Join(e.root, filepath.FromSlash(to))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		e.t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(e.root, filepath.FromSlash(from)), dst); err != nil {
		e.t.Fatal(err)
	}
}

// tree maps "<view>/<name>" to the volume path a link resolves to, and a
// non-link entry to "<file>", for every view under Collections.
func (e *collViewsEnv) tree() map[string]string { e.t.Helper(); return e.treeOf("Collections") }

// treeOf is tree for the views under root.
func (e *collViewsEnv) treeOf(root string) map[string]string {
	e.t.Helper()
	out := map[string]string{}
	base := filepath.Join(e.root, root)
	views, err := os.ReadDir(base)
	if errors.Is(err, os.ErrNotExist) {
		return out
	}
	if err != nil {
		e.t.Fatal(err)
	}
	for _, v := range views {
		if !v.IsDir() {
			out[v.Name()] = "<file>"
			continue
		}
		ents, err := os.ReadDir(filepath.Join(base, v.Name()))
		if err != nil {
			e.t.Fatal(err)
		}
		if len(ents) == 0 {
			out[v.Name()+"/"] = "<empty>"
		}
		for _, ent := range ents {
			p := filepath.Join(base, v.Name(), ent.Name())
			if ent.Type()&os.ModeSymlink == 0 {
				out[v.Name()+"/"+ent.Name()] = "<file>"
				continue
			}
			text, err := os.Readlink(p)
			if err != nil || filepath.IsAbs(text) {
				e.t.Fatalf("%s -> %q (%v): not a relative link", p, text, err)
			}
			if _, err := os.Stat(p); err != nil {
				out[v.Name()+"/"+ent.Name()] = "<dangling " + text + ">"
				continue
			}
			rel, _ := filepath.Rel(e.root, filepath.Join(filepath.Dir(p), text))
			out[v.Name()+"/"+ent.Name()] = filepath.ToSlash(rel)
		}
	}
	return out
}

func (e *collViewsEnv) want(want map[string]string) {
	e.t.Helper()
	if got := e.tree(); !reflect.DeepEqual(got, want) {
		e.t.Fatalf("collection views =\n%v\nwant\n%v\n(log %v)", got, want, e.svc.lastLog)
	}
}

// The lifecycle: links at the members' live paths, moved with a member
// file or its folder, gone with the file or the membership; a renamed
// collection's view is renamed; a deleted one is removed, except while a
// user's file is in it, which is never touched.
func TestCollectionViewsFollowCollections(t *testing.T) {
	e := newCollViewsEnv(t, "Proj/A/clip.mov", "Proj/B/clip.mov", "Proj/c.mov", "Proj/d.mov")
	e.attach()
	shoot := collID(1)
	e.create(shoot, "Shoot 1/2", "Proj/A/clip.mov", "Proj/B/clip.mov", "Proj/c.mov")
	e.pass()
	e.want(map[string]string{
		"Shoot 1-2/clip.mov":     "Proj/A/clip.mov", // joined first: the plain name
		"Shoot 1-2/clip (B).mov": "Proj/B/clip.mov",
		"Shoot 1-2/c.mov":        "Proj/c.mov",
	})

	// A member file is renamed: nothing happens until its event (or the
	// sweep), then the link follows the live path, never the binder's hint.
	e.move("Proj/c.mov", "Proj/sub/c2.mov")
	e.pass()
	if got := e.tree()["Shoot 1-2/c.mov"]; got != "<dangling ../../Proj/c.mov>" {
		t.Fatalf("rebuilt without a reason: %q", got)
	}
	e.svc.noteCollectionEvent(smartfolders.Event{Op: "rename", OldPath: "Proj/c.mov", Path: "Proj/sub/c2.mov"})
	e.pass()
	e.want(map[string]string{
		"Shoot 1-2/clip.mov":     "Proj/A/clip.mov",
		"Shoot 1-2/clip (B).mov": "Proj/B/clip.mov",
		"Shoot 1-2/c2.mov":       "Proj/sub/c2.mov",
	})

	// A folder above a member is renamed (one event for the folder).
	e.move("Proj/A", "Proj/A2")
	e.svc.noteCollectionEvent(smartfolders.Event{Op: "rename", OldPath: "Proj/A", Path: "Proj/A2", IsDir: true})
	e.pass()
	e.want(map[string]string{
		"Shoot 1-2/clip.mov":     "Proj/A2/clip.mov", // same target, same name
		"Shoot 1-2/clip (B).mov": "Proj/B/clip.mov",
		"Shoot 1-2/c2.mov":       "Proj/sub/c2.mov",
	})

	// A member file is deleted: its link goes; it stays a member.
	if err := os.Remove(filepath.Join(e.root, "Proj/B/clip.mov")); err != nil {
		t.Fatal(err)
	}
	e.svc.noteCollectionEvent(smartfolders.Event{Op: "delete", Path: "Proj/B/clip.mov"})
	e.pass()
	e.want(map[string]string{"Shoot 1-2/clip.mov": "Proj/A2/clip.mov", "Shoot 1-2/c2.mov": "Proj/sub/c2.mov"})
	if st := e.svc.collectionStats(); st.MissingMembers != 1 || st.Links != 2 || st.Views != 1 {
		t.Fatalf("stats = %+v", st)
	}

	// A member leaves: its link goes (a catalog change; no event needed).
	e.apply(catalog.Change{RemoveCollectionMember: &catalog.RemoveCollectionMember{CollectionID: shoot, AssetID: e.asset("Proj/sub/c2.mov")}})
	e.pass()
	e.want(map[string]string{"Shoot 1-2/clip.mov": "Proj/A2/clip.mov"})

	// The collection is renamed: the view is renamed (new view, old gone).
	e.apply(catalog.Change{RenameCollection: &catalog.RenameCollection{CollectionID: shoot, Name: "Final"}})
	e.pass()
	e.want(map[string]string{"Final/clip.mov": "Proj/A2/clip.mov"})

	// A user's file in the view: rebuilt views never touch it, and deleting
	// the collection removes the Manager's links but leaves the view.
	writeVolumeFile(t, e.root, "Collections/Final/notes.txt")
	e.add(shoot, "Proj/d.mov")
	e.pass()
	e.want(map[string]string{"Final/clip.mov": "Proj/A2/clip.mov", "Final/d.mov": "Proj/d.mov", "Final/notes.txt": "<file>"})
	e.apply(catalog.Change{RemoveCollection: &catalog.RemoveCollection{CollectionID: shoot}})
	e.pass()
	e.want(map[string]string{"Final/notes.txt": "<file>"})
	if data, err := os.ReadFile(filepath.Join(e.root, "Collections/Final/notes.txt")); err != nil || string(data) != "Collections/Final/notes.txt" {
		t.Fatalf("user's file changed: %q %v", data, err)
	}
	if st := e.svc.collectionStats(); !reflect.DeepEqual(st.ViewsLeft, []string{"Collections/Final: holds entries the Manager did not create"}) {
		t.Fatalf("stats = %+v", st)
	}
	// Once the user takes the file out, the view goes (after the backoff).
	if err := os.Remove(filepath.Join(e.root, "Collections/Final/notes.txt")); err != nil {
		t.Fatal(err)
	}
	e.clk.add(time.Hour)
	e.pass()
	e.want(map[string]string{})
	if st := e.svc.collectionStats(); st.Views != 0 || len(st.ViewsLeft) != 0 || len(e.svc.coll.own.Views) != 0 {
		t.Fatalf("stats = %+v, records %+v", st, e.svc.coll.own.Views)
	}
	// Every write checked out the view's project.
	asked, _ := e.leaser.snapshot()
	for _, p := range asked {
		if !strings.HasPrefix(p, "path:/Collections/") {
			t.Fatalf("checked out %q", p)
		}
	}
}

// Changes made through the collections API schedule a debounced pass.
func TestCollectionViewsAPIChangeSchedulesPass(t *testing.T) {
	e := newCollViewsEnv(t, "Proj/a.mov")
	e.svc.coll.debounce = time.Millisecond
	e.attach()
	<-e.svc.coll.wake // the pass attach schedules
	e.cont.collectionsChanged()
	select {
	case <-e.svc.coll.wake:
	case <-time.After(5 * time.Second):
		t.Fatal("no pass scheduled after a collection change")
	}
	// An event for a path no view links does not schedule one.
	e.svc.noteCollectionEvent(smartfolders.Event{Op: "create", Path: "Proj/other.mov"})
	select {
	case <-e.svc.coll.wake:
		t.Fatal("pass scheduled for an unrelated event")
	case <-time.After(50 * time.Millisecond):
	}
}

// Name clashes and limits: deterministic folder names, at most maxViews
// views and maxLinks links, overflow counted; a member's file outside the
// linkable part of the volume (here inside /Recents) gets no link.
func TestCollectionViewsNamesAndLimits(t *testing.T) {
	e := newCollViewsEnv(t, "P/a.mov", "P/b.mov", "P/c.mov", "Recents/x/user.mov")
	e.svc.coll.maxViews, e.svc.coll.maxLinks = 2, 2
	e.attach()
	e.create(collID(3), "Shoot", "P/a.mov", "P/b.mov", "P/c.mov")
	e.create(collID(2), "shoot", "P/c.mov", "Recents/x/user.mov")
	e.create(collID(1), ".", "P/a.mov") // created last: over the view limit
	e.pass()
	e.want(map[string]string{
		"Shoot/a.mov":     "P/a.mov",
		"Shoot/b.mov":     "P/b.mov",
		"shoot (2)/c.mov": "P/c.mov",
	})
	st := e.svc.collectionStats()
	if st.OverflowCollections != 1 || st.OverflowMembers != 1 || st.RefusedLinks != 1 || st.Collections != 3 || st.Views != 2 {
		t.Fatalf("stats = %+v", st)
	}
	// The first collection is deleted: the second keeps its folder.
	e.apply(catalog.Change{RemoveCollection: &catalog.RemoveCollection{CollectionID: collID(3)}})
	e.pass()
	e.want(map[string]string{"shoot (2)/c.mov": "P/c.mov", "Untitled/a.mov": "P/a.mov"})
}

// A /Collections someone else made is left alone (and stops only the
// collection views, not /Recents), and checked again later.
func TestCollectionViewsUserMadeRootLeftAlone(t *testing.T) {
	e := newCollViewsEnv(t, "Proj/a.mov", "Collections/mine.txt")
	e.attach()
	e.create(collID(1), "Shoot", "Proj/a.mov")
	e.svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Proj/a.mov"})
	e.svc.cycle(context.Background())
	e.want(map[string]string{"mine.txt": "<file>"})
	if got := viewTree(t, e.root); !reflect.DeepEqual(got, map[string]string{"Proj/a.mov": "Proj/a.mov"}) {
		t.Fatalf("/Recents = %v", got)
	}
	if asked, _ := e.leaser.snapshot(); countPrefix(asked, "path:/Collections/") != 0 {
		t.Fatalf("checked out %v under a user's Collections", asked)
	}
	if st := e.svc.collectionStats(); !st.RootForeign {
		t.Fatalf("stats = %+v", st)
	}
	// The user renames it; after the recheck delay the views appear.
	if err := os.Rename(filepath.Join(e.root, "Collections"), filepath.Join(e.root, "Mine")); err != nil {
		t.Fatal(err)
	}
	e.pass()
	e.want(map[string]string{})
	e.clk.add(smartFoldersRootRecheck + time.Second)
	e.pass()
	e.want(map[string]string{"Shoot/a.mov": "Proj/a.mov"})
}

func countPrefix(xs []string, prefix string) int {
	n := 0
	for _, x := range xs {
		if strings.HasPrefix(x, prefix) {
			n++
		}
	}
	return n
}

// Either switch off creates nothing: without continuity attached (or with
// smart folders off) no collection view is written, and /Recents keeps
// listing a top-level folder named Collections.
func TestCollectionViewsOffCreatesNothing(t *testing.T) {
	e := newCollViewsEnv(t, "Proj/a.mov")
	e.create(collID(1), "Shoot", "Proj/a.mov")
	e.svc.cycle(context.Background())
	e.pass()
	e.want(map[string]string{})
	if e.svc.coll.state() != nil {
		t.Fatal("records written while off")
	}
	// Smart folders off: there is no service to attach to.
	(&API{jobs: &JobManager{}}).attachCollectionViews(e.cont)
	if e.cont.views.Load() != nil {
		t.Fatal("attached without smart folders")
	}
	e.cont.collectionsChanged() // no-op
	// A collections root that clashes with the Recents root keeps them off.
	on, set, err := smartFoldersEnv(envMap(map[string]string{"JM_SMART_FOLDERS": "on", "JM_SMART_FOLDERS_ROOT": "collections"}))
	if err != nil || !on || set.Root != "collections" || set.CollectionsRoot != "" || set.CollectionsOff == "" {
		t.Fatalf("settings = %+v %v", set, err)
	}
	_, set, _ = smartFoldersEnv(envMap(map[string]string{"JM_SMART_FOLDERS": "on", "JM_SMART_FOLDERS_COLLECTIONS_ROOT": ".hidden"}))
	if set.CollectionsRoot != "" || set.Root != "Recents" {
		t.Fatalf("settings = %+v", set)
	}
	_, set, _ = smartFoldersEnv(envMap(map[string]string{"JM_SMART_FOLDERS": "on", "JM_SMART_FOLDERS_COLLECTIONS_ROOT": "Sets"}))
	if set.CollectionsRoot != "Sets" {
		t.Fatalf("settings = %+v", set)
	}
	e.svc.cfg.Settings.CollectionsRoot = ""
	if e.svc.attachCollections(e.cont) || e.svc.coll.source() != nil {
		t.Fatal("attached with no usable root")
	}
	// /Recents still lists a top-level Collections folder while they are off.
	writeVolumeFile(t, e.root, "Collections/x/user.mov")
	e.svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Collections/x/user.mov"})
	if got := e.svc.tracker.Folders(); !reflect.DeepEqual(got, []string{"Collections"}) {
		t.Fatalf("tracked %v", got)
	}
}

// /Recents leaves the collections root out only once the Manager owns it:
// a user-made /Collections stays an ordinary folder for /Recents (listed,
// linkable, its /Recents view kept); the Manager's own is not listed.
func TestCollectionViewsRecentsAndCollectionsRoot(t *testing.T) {
	e := newCollViewsEnv(t, "Collections/x/user.mov", "Proj/a.mov")
	e.svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Collections/x/user.mov"})
	e.svc.cycle(context.Background())
	recents := map[string]string{"Collections/user.mov": "Collections/x/user.mov"}
	if got := viewTree(t, e.root); !reflect.DeepEqual(got, recents) {
		t.Fatalf("/Recents = %v", got)
	}
	e.attach()
	e.create(collID(1), "Shoot", "Proj/a.mov")
	e.svc.cycle(context.Background()) // the collections root is the user's
	writeVolumeFile(t, e.root, "Collections/x/two.mov")
	e.svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Collections/x/two.mov"})
	e.clk.add(time.Second)
	e.svc.cycle(context.Background())
	recents["Collections/two.mov"] = "Collections/x/two.mov"
	if got := viewTree(t, e.root); !reflect.DeepEqual(got, recents) {
		t.Fatalf("/Recents with a user-made /Collections = %v (log %v)", got, e.svc.lastLog)
	}
	// The user's folder goes; the Manager's root comes: no longer listed.
	if err := os.RemoveAll(filepath.Join(e.root, "Collections")); err != nil {
		t.Fatal(err)
	}
	e.clk.add(smartFoldersRootRecheck + time.Second)
	e.pass()
	e.want(map[string]string{"Shoot/a.mov": "Proj/a.mov"})
	writeVolumeFile(t, e.root, "Collections/Shoot/notes.txt")
	e.svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Collections/Shoot/notes.txt"})
	for _, en := range e.svc.tracker.Recent("Collections") {
		if en.Path == "Collections/Shoot/notes.txt" {
			t.Fatal("listed a file under the Manager's collections root")
		}
	}
}

// smart-folders.json: a version 2 file loads unchanged and stays version 2
// until collection views record something; then it is version 3, and a
// restart reloads those records (an unchanged view is not rewritten).
func TestSmartFoldersStateMigratesToVersion3(t *testing.T) {
	e := newCollViewsEnv(t, "Proj/a.mov")
	state := e.svc.cfg.StatePath
	e.svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Proj/a.mov"})
	e.svc.cycle(context.Background())
	readState := func() (map[string]json.RawMessage, int) {
		t.Helper()
		data, err := os.ReadFile(state)
		if err != nil {
			t.Fatal(err)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatal(err)
		}
		var v int
		_ = json.Unmarshal(raw["version"], &v)
		return raw, v
	}
	raw, v := readState()
	if v != 2 || raw["collections"] != nil || raw["ownership"] == nil {
		t.Fatalf("before collection views: version %d, keys %v", v, keys(raw))
	}
	// A version 2 file as the previous Manager wrote it loads.
	svc2 := newTestSmartFolders(t, svcOpts{root: e.root, state: state, leaser: &fakeLeaser{}})
	defer svc2.cfg.Redis.Close()
	if svc2.own.Root == nil || svc2.coll.state() != nil {
		t.Fatal("version 2 not loaded")
	}

	e.attach()
	e.create(collID(1), "Shoot", "Proj/a.mov")
	e.pass()
	raw, v = readState()
	if v != 3 || raw["collections"] == nil || raw["ownership"] == nil || raw["tracker"] == nil {
		t.Fatalf("after collection views: version %d, keys %v", v, keys(raw))
	}
	var coll collectionViewsState
	if err := json.Unmarshal(raw["collections"], &coll); err != nil || coll.Ownership.Root == nil ||
		coll.Ownership.Views["Shoot"].Links["a.mov"].Ino == 0 || coll.Folders[collID(1)] != "Shoot" {
		t.Fatalf("collections = %s (%v)", raw["collections"], err)
	}
	// Restart: the records come back; nothing unchanged is rewritten.
	leaser := &fakeLeaser{}
	svc3 := newTestSmartFolders(t, svcOpts{root: e.root, state: state, leaser: leaser, clock: e.clk})
	defer svc3.cfg.Redis.Close()
	svc3.coll.debounce = time.Hour
	if !reflect.DeepEqual(svc3.coll.own, e.svc.coll.own) || svc3.coll.folders[collID(1)] != "Shoot" {
		t.Fatal("collection records not reloaded")
	}
	svc3.attachCollections(e.cont)
	svc3.collectionsOnlyCycle(context.Background())
	if asked, _ := leaser.snapshot(); len(asked) != 0 {
		t.Fatalf("restart rewrote an unchanged view (checkouts %v)", asked)
	}
	e.want(map[string]string{"Shoot/a.mov": "Proj/a.mov"})

	// Formats this Manager does not know are refused, never rewritten; a
	// version 2 file claiming collection records is refused too.
	for _, bad := range []string{`{"version":5}`, `{"version":2,"collections":{}}`, `{"version":3,"queries":{}}`} {
		if err := os.WriteFile(state, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := newSmartFoldersService(smartFoldersConfig{Settings: smartFoldersSettings{Root: "Recents", MaxViews: 1},
			Backend: svc3.cfg.Backend, Redis: svc3.cfg.Redis, StatePath: state}); err == nil {
			t.Fatalf("%s accepted", bad)
		}
		if data, _ := os.ReadFile(state); string(data) != bad {
			t.Fatalf("refused state rewritten: %s", data)
		}
	}
}

func keys(m map[string]json.RawMessage) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// One member whose path cannot be resolved keeps its link, is counted and
// is not resolved again until its own backoff passes; the rest of the view
// follows. A volume not connected at all leaves the view as it is.
func TestCollectionViewsUnknownIsNotGone(t *testing.T) {
	e := newCollViewsEnv(t, "Proj/a.mov", "Proj/b.mov", "Proj/c.mov")
	e.attach()
	e.create(collID(1), "Shoot", "Proj/a.mov", "Proj/b.mov")
	e.pass()
	e.want(map[string]string{"Shoot/a.mov": "Proj/a.mov", "Shoot/b.mov": "Proj/b.mov"})
	fs := &failingSource{continuityService: e.cont, bad: e.asset("Proj/a.mov")}
	e.svc.coll.src.Store(&collectionSourceRef{fs})
	e.add(collID(1), "Proj/c.mov")
	e.pass()
	e.want(map[string]string{"Shoot/a.mov": "Proj/a.mov", "Shoot/b.mov": "Proj/b.mov", "Shoot/c.mov": "Proj/c.mov"})
	if st := e.svc.collectionStats(); st.UnresolvedMembers != 1 {
		t.Fatalf("stats = %+v", st)
	}
	if b := e.svc.coll.backoff["Shoot"]; b != nil {
		t.Fatalf("the view backed off: %+v", b)
	}
	// Backed off: the next rebuild does not ask about it again.
	asked := fs.badCalls.Load()
	e.svc.coll.markDirty(collID(1))
	e.pass()
	if fs.badCalls.Load() != asked {
		t.Fatal("re-resolved a backed-off member")
	}
	e.want(map[string]string{"Shoot/a.mov": "Proj/a.mov", "Shoot/b.mov": "Proj/b.mov", "Shoot/c.mov": "Proj/c.mov"})
	// After its backoff it is asked again.
	e.clk.add(2 * time.Hour)
	e.svc.coll.markDirty(collID(1))
	e.pass()
	if fs.badCalls.Load() == asked {
		t.Fatal("never retried")
	}
	// Not connected: nothing changes, retried at the next pass, no backoff.
	fs.notReady.Store(true)
	e.svc.coll.markDirty(collID(1))
	e.pass()
	e.want(map[string]string{"Shoot/a.mov": "Proj/a.mov", "Shoot/b.mov": "Proj/b.mov", "Shoot/c.mov": "Proj/c.mov"})
	e.svc.coll.mu.Lock()
	dirty := e.svc.coll.dirty[collID(1)]
	e.svc.coll.mu.Unlock()
	if b := e.svc.coll.backoff["Shoot"]; b != nil || !dirty {
		t.Fatalf("backoff %+v, dirty %v", b, e.svc.coll.dirty)
	}
}

type failingSource struct {
	*continuityService
	bad      continuity.AssetID
	badCalls atomic.Int64
	notReady atomic.Bool
}

func (f *failingSource) resolveAsset(ctx context.Context, id continuity.AssetID) (string, string, error) {
	if f.notReady.Load() {
		return "", "unknown", binder.ErrNotReady
	}
	if id == f.bad {
		f.badCalls.Add(1)
		return "", "unknown", nil
	}
	return f.continuityService.resolveAsset(ctx, id)
}

// A view left behind (a user's file in it) does not count against the view
// limit; a user's folder in the root, in any case, is never a view's name.
func TestCollectionViewsLeftBehindAndUserFolders(t *testing.T) {
	e := newCollViewsEnv(t, "P/a.mov", "P/b.mov")
	e.svc.coll.maxViews = 1
	e.attach()
	e.create(collID(1), "Old", "P/a.mov")
	e.pass()
	writeVolumeFile(t, e.root, "Collections/Old/notes.txt")
	writeVolumeFile(t, e.root, "Collections/SHOOT/mine.txt") // a user's folder
	e.apply(catalog.Change{RemoveCollection: &catalog.RemoveCollection{CollectionID: collID(1)}})
	e.create(collID(2), "Shoot", "P/b.mov")
	e.pass()
	e.want(map[string]string{"Old/notes.txt": "<file>", "SHOOT/mine.txt": "<file>", "Shoot (2)/b.mov": "P/b.mov"})
}

// A pass stops starting views at its budget and schedules the next; every
// view is built in the end, changed collections before the sweep's.
func TestCollectionViewsPassBudget(t *testing.T) {
	e := newCollViewsEnv(t, "P/a.mov", "P/b.mov", "P/c.mov")
	e.svc.coll.passOps = 2
	e.svc.coll.debounce = time.Millisecond
	e.attach()
	<-e.svc.coll.wake // the pass attach schedules
	for i, f := range []string{"P/a.mov", "P/b.mov", "P/c.mov"} {
		e.create(collID(i+1), fmt.Sprintf("C%d", i+1), f)
	}
	counter := &countingSource{continuityService: e.cont}
	e.svc.coll.src.Store(&collectionSourceRef{counter})
	passes := 0
	for len(e.tree()) < 3 {
		if passes++; passes > 5 {
			t.Fatalf("not done after 5 passes: %v", e.tree())
		}
		before := counter.calls.Load()
		e.pass()
		if n := counter.calls.Load() - before; n > 2 {
			t.Fatalf("a pass resolved %d members (budget 2)", n)
		}
		if len(e.tree()) < 3 {
			select { // the rest was scheduled
			case <-e.svc.coll.wake:
			case <-time.After(5 * time.Second):
				t.Fatal("no follow-up pass scheduled")
			}
		}
	}
	if passes != 2 { // two views (one resolution each), then the third
		t.Fatalf("passes = %d", passes)
	}
	e.want(map[string]string{"C1/a.mov": "P/a.mov", "C2/b.mov": "P/b.mov", "C3/c.mov": "P/c.mov"})
}

type countingSource struct {
	*continuityService
	calls atomic.Int64
}

func (c *countingSource) resolveAsset(ctx context.Context, id continuity.AssetID) (string, string, error) {
	c.calls.Add(1)
	return c.continuityService.resolveAsset(ctx, id)
}

// A numbered folder name is recorded as soon as it is assigned, before the
// view is written (here the lease is refused), so it never changes hands.
func TestCollectionViewsNamesRecordedFirst(t *testing.T) {
	e := newCollViewsEnv(t, "P/a.mov")
	e.attach()
	e.create(collID(1), "Shoot")
	e.create(collID(2), "shoot", "P/a.mov")
	e.leaser.mu.Lock()
	e.leaser.refuse = true
	e.leaser.mu.Unlock()
	e.pass()
	e.want(map[string]string{})
	data, err := os.ReadFile(e.svc.cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var st smartFoldersState
	if err := json.Unmarshal(data, &st); err != nil || st.Collections == nil || st.Collections.Folders[collID(2)] != "shoot (2)" {
		t.Fatalf("state = %s (%v)", data, err)
	}
}

// refusingBackend is the OS backend whose FS refuses (EROFS) removing an
// entry of the collections root, as the RESP proxy refuses a view it has
// no record of.
type refusingBackend struct {
	osBackend
	tries atomic.Int64
}

type refusingFS struct {
	smartfolders.FS
	b *refusingBackend
}

func (f refusingFS) Rmdir(p string) error {
	if filepath.Dir(p) == "Collections" {
		f.b.tries.Add(1)
		return syscall.EROFS
	}
	return f.FS.Rmdir(p)
}

func (b *refusingBackend) acquire() (smartfolders.FS, func(), error) {
	fsys, release, err := b.osBackend.acquire()
	if err != nil {
		return nil, nil, err
	}
	return refusingFS{fsys, b}, release, nil
}

func newRefusingEnv(t *testing.T, files ...string) (*collViewsEnv, *refusingBackend) {
	e := newCollViewsEnv(t, files...)
	b := &refusingBackend{osBackend: osBackend{root: e.root, mountCheck: func(string) error { return nil }}}
	e.svc.cfg.Backend = b
	return e, b
}

// A view the volume refuses to remove is parked: not tried again at the
// next backoff, only after the park time.
func TestCollectionViewsRefusedRemovalIsParked(t *testing.T) {
	e, b := newRefusingEnv(t, "P/a.mov")
	e.attach()
	e.create(collID(1), "Shoot", "P/a.mov")
	e.pass()
	e.apply(catalog.Change{RemoveCollection: &catalog.RemoveCollection{CollectionID: collID(1)}})
	e.pass()
	if b.tries.Load() != 1 {
		t.Fatalf("tries = %d", b.tries.Load())
	}
	e.want(map[string]string{"Shoot/": "<empty>"})
	for i := 0; i < 3; i++ {
		e.clk.add(time.Hour) // past every ordinary backoff step so far
		e.pass()
	}
	if st := e.svc.collectionStats(); b.tries.Load() != 1 || st.ViewsParked != 1 {
		t.Fatalf("tries = %d, stats %+v", b.tries.Load(), st)
	}
	e.clk.add(collectionViewsPark)
	e.pass()
	if b.tries.Load() != 2 {
		t.Fatalf("not tried after the park time: %d", b.tries.Load())
	}
}

// Recorded views are capped at twice the view limit: at the cap no new
// view is made (counted) and views left behind are not retried.
func TestCollectionViewsRecordedCap(t *testing.T) {
	e, b := newRefusingEnv(t, "P/a.mov", "P/b.mov", "P/c.mov")
	e.svc.coll.maxViews = 1
	e.attach()
	for i, f := range []string{"P/a.mov", "P/b.mov"} {
		e.create(collID(i+1), fmt.Sprintf("C%d", i+1), f)
		e.pass()
		e.apply(catalog.Change{RemoveCollection: &catalog.RemoveCollection{CollectionID: collID(i + 1)}})
		e.pass()
	}
	e.create(collID(3), "C3", "P/c.mov")
	e.pass()
	e.want(map[string]string{"C1/": "<empty>", "C2/": "<empty>"})
	tries := b.tries.Load()
	e.clk.add(2 * collectionViewsPark)
	e.pass()
	st := e.svc.collectionStats()
	if st.ViewsAtCap != 1 || st.ViewsParked != 2 || b.tries.Load() != tries || len(e.svc.coll.own.Views) != 2 {
		t.Fatalf("tries %d->%d, stats %+v", tries, b.tries.Load(), st)
	}
}

// Under a tight budget, collections that change every pass do not starve
// a later one.
func TestCollectionViewsBudgetRotates(t *testing.T) {
	e := newCollViewsEnv(t, "P/a.mov", "P/b.mov", "P/c.mov")
	e.svc.coll.passOps = 1
	e.attach()
	for i, f := range []string{"P/a.mov", "P/b.mov", "P/c.mov"} {
		e.create(collID(i+1), fmt.Sprintf("C%d", i+1), f)
	}
	for i := 0; i < 4; i++ {
		e.pass()
		e.svc.coll.markDirty(collID(1)) // C1 and C2 keep changing
		e.svc.coll.markDirty(collID(2))
	}
	if got := e.tree()["C3/c.mov"]; got != "P/c.mov" {
		t.Fatalf("C3 starved: %v", e.tree())
	}
}

// Events mark collections dirty while a pass runs (run with -race).
func TestCollectionViewsDirtyMarkedDuringPass(t *testing.T) {
	e := newCollViewsEnv(t, "P/a.mov")
	e.attach()
	e.create(collID(1), "Shoot", "P/a.mov")
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				e.svc.coll.markDirty(collID(1))
			}
		}
	}()
	for i := 0; i < 5; i++ {
		e.pass()
	}
	close(stop)
	<-done
	e.want(map[string]string{"Shoot/a.mov": "P/a.mov"})
}
