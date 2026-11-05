package sftest

// Views named after something other than a volume folder (the collection
// views): a second root beside Recents, views that are removed when their
// collection goes, and the other root excluded as a link target.

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

// newCollectionsRig is a rig at "Collections" that excludes "Recents".
func newCollectionsRig(t *testing.T, fx Fixture) *rig {
	g := newRigAt(t, fx, "Collections")
	g.rec.Exclude = []string{"Recents"}
	return g
}

// shoot reconciles the view "Shoot" with two links.
func shoot(t *testing.T, fx Fixture, g *rig) {
	t.Helper()
	files(t, fx, "Proj/a.mov", "Proj/sub/b.mov")
	g.reconcile(t, "Shoot", link("a.mov", "Proj/a.mov"), link("b.mov", "Proj/sub/b.mov"))
	assertView(t, fx, "Collections/Shoot", map[string]string{"a.mov": "Proj/a.mov", "b.mov": "Proj/sub/b.mov"})
}

// rootEntries lists the root, hidden names included.
func rootEntries(t *testing.T, fx Fixture) []string {
	t.Helper()
	return fx.ReadDir(t, "Collections")
}

func targetsIntact(t *testing.T, fx Fixture) {
	t.Helper()
	for _, p := range []string{"Proj/a.mov", "Proj/sub/b.mov"} {
		if in, err := fx.FS().Lstat(p); err != nil || in.Type != smartfolders.TypeRegular {
			t.Fatalf("target %s touched: %+v %v", p, in, err)
		}
	}
}

func removeCrashing(t *testing.T, g *rig) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(crashed); !ok {
				panic(p)
			}
		}
	}()
	_, _ = g.rec.RemoveView("Shoot")
	t.Fatal("the removal did not reach the crash point")
}

var viewTests = map[string]func(*testing.T, Fixture){
	// A removed view: links first, then the directory, moved aside under a
	// recorded hidden name; nothing left, no record, nothing in the trash.
	"ZV_RemoveViewRemovesLinksAndDirectory": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		g.events = nil
		res, err := g.rec.RemoveView("Shoot")
		if err != nil || !res.RemovedView || len(res.Removed) != 2 || !res.Changed() {
			t.Fatalf("RemoveView = %+v, %v", res, err)
		}
		if exists(fx, "Collections/Shoot") || len(rootEntries(t, fx)) != 0 {
			t.Fatalf("root holds %v", rootEntries(t, fx))
		}
		if _, ok := g.rec.Own.Views["Shoot"]; ok {
			t.Fatal("record kept")
		}
		// Recorded before moved: the hidden name is persisted first.
		var recorded string
		for _, ev := range g.events {
			if strings.HasPrefix(ev, "persist ") && strings.Contains(ev, `"remove_as"`) && recorded == "" {
				recorded = ev
			}
			if strings.HasPrefix(ev, "place Collections/Shoot ") && recorded == "" {
				t.Fatalf("moved before recorded: %v", g.events)
			}
		}
		if recorded == "" {
			t.Fatalf("hidden name never recorded: %v", g.events)
		}
		if n := fx.TrashCount(t); n > 0 {
			t.Fatalf("%d entries went to the trash", n)
		}
		targetsIntact(t, fx)
		// Gone views are a no-op.
		if res, err := g.rec.RemoveView("Shoot"); err != nil || res.Changed() {
			t.Fatalf("again = %+v, %v", res, err)
		}
	},
	// A user's file in a view: the Manager's links go, the file and the view
	// stay (never moved: the emptiness check comes first), until the user
	// takes the file out.
	"ZV_RemoveViewLeavesUserFile": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		fx.WriteFile(t, "Collections/Shoot/notes.txt")
		before := ino(t, fx, "Collections/Shoot")
		g.events = nil
		res, err := g.rec.RemoveView("Shoot")
		if !errors.Is(err, smartfolders.ErrViewInUse) || res.RemovedView || len(res.Removed) != 2 {
			t.Fatalf("RemoveView = %+v, %v", res, err)
		}
		assertView(t, fx, "Collections/Shoot", nil, "notes.txt")
		if ino(t, fx, "Collections/Shoot") != before || g.rec.Own.Views["Shoot"] == nil {
			t.Fatal("view replaced or forgotten")
		}
		for _, ev := range g.events {
			if strings.HasPrefix(ev, "place Collections/Shoot ") || strings.HasPrefix(ev, "rmdir") {
				t.Fatalf("a view holding a user's file was moved or removed: %v", g.events)
			}
		}
		fx.Remove(t, "Collections/Shoot/notes.txt")
		if res, err := g.rec.RemoveView("Shoot"); err != nil || !res.RemovedView {
			t.Fatalf("after the user's file left: %+v, %v", res, err)
		}
		targetsIntact(t, fx)
	},
	// A user's file arriving between the check and the move: the view
	// (with it) goes back to its name.
	"ZV_RemoveViewRaceUserFileGoesBack": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		g.log.beforePlace = func(from, to string) {
			if from == "Collections/Shoot" {
				fx.WriteFile(t, "Collections/Shoot/notes.txt")
			}
		}
		_, err := g.rec.RemoveView("Shoot")
		if !errors.Is(err, smartfolders.ErrViewInUse) {
			t.Fatalf("err = %v", err)
		}
		assertView(t, fx, "Collections/Shoot", nil, "notes.txt")
		if v := g.rec.Own.Views["Shoot"]; v == nil || v.RemoveAs != "" {
			t.Fatalf("record = %+v", v)
		}
		if names := rootEntries(t, fx); !reflect.DeepEqual(names, []string{"Shoot"}) {
			t.Fatalf("root holds %v", names)
		}
	},
	// The view swapped for a user's own (empty) folder between the check
	// and the move: that folder is moved back, never removed.
	"ZV_RemoveViewRaceUserDirNeverRemoved": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		var userIno uint64
		g.log.beforePlace = func(from, to string) {
			if from == "Collections/Shoot" && userIno == 0 {
				fx.MkdirAll(t, "Collections/mine")
				userIno = ino(t, fx, "Collections/mine")
				if err := fx.FS().Swap("Collections/mine", "Collections/Shoot"); err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, err := g.rec.RemoveView("Shoot"); err == nil {
			t.Fatal("removal reported done")
		}
		if !exists(fx, "Collections/Shoot") || ino(t, fx, "Collections/Shoot") != userIno {
			t.Fatalf("the user's folder was not put back (root %v)", rootEntries(t, fx))
		}
		if names := rootEntries(t, fx); !reflect.DeepEqual(names, []string{"Shoot", "mine"}) {
			t.Fatalf("root holds %v", names)
		}
		// Next time the folder is recognised as someone else's.
		if _, err := g.rec.RemoveView("Shoot"); !errors.Is(err, smartfolders.ErrNotManaged) {
			t.Fatalf("err = %v", err)
		}
		if g.rec.Own.Views["Shoot"] != nil || ino(t, fx, "Collections/Shoot") != userIno {
			t.Fatal("the user's folder was adopted or touched")
		}
	},
	// The process dies right after the view was moved aside: the next run
	// finds it under the recorded hidden name and removes it there.
	"ZV_RemoveViewCrashAfterMove": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		g.log.afterPlace = func(from, to string) error {
			if from == "Collections/Shoot" {
				panic(crashed{})
			}
			return nil
		}
		removeCrashing(t, g)
		if exists(fx, "Collections/Shoot") {
			t.Fatal("view not moved")
		}
		g2 := g.restart(t)
		if res, err := g2.rec.RemoveView("Shoot"); err != nil || !res.RemovedView {
			t.Fatalf("restart = %+v, %v", res, err)
		}
		if names := rootEntries(t, fx); len(names) != 0 {
			t.Fatalf("root holds %v", names)
		}
	},
	// Same crash, but the collection wants its view back: Reconcile settles
	// the interrupted removal first, then builds the view again.
	"ZV_RemoveViewCrashThenWantedAgain": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		g.log.afterPlace = func(from, to string) error {
			if from == "Collections/Shoot" {
				panic(crashed{})
			}
			return nil
		}
		removeCrashing(t, g)
		g2 := g.restart(t)
		g2.reconcile(t, "Shoot", link("a.mov", "Proj/a.mov"))
		assertView(t, fx, "Collections/Shoot", map[string]string{"a.mov": "Proj/a.mov"})
		if names := rootEntries(t, fx); !reflect.DeepEqual(names, []string{"Shoot"}) {
			t.Fatalf("root holds %v", names)
		}
	},
	// A crash after the move, then a user's file put into the hidden
	// directory: the next run puts the view back with it and keeps it.
	"ZV_RemoveViewCrashUserFileInHiddenDir": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		var hidden string
		g.log.afterPlace = func(from, to string) error {
			if from == "Collections/Shoot" {
				hidden = to
				panic(crashed{})
			}
			return nil
		}
		removeCrashing(t, g)
		fx.WriteFile(t, hidden+"/notes.txt")
		g2 := g.restart(t)
		if _, err := g2.rec.RemoveView("Shoot"); !errors.Is(err, smartfolders.ErrViewInUse) {
			t.Fatalf("err = %v", err)
		}
		assertView(t, fx, "Collections/Shoot", nil, "notes.txt")
		if names := rootEntries(t, fx); !reflect.DeepEqual(names, []string{"Shoot"}) {
			t.Fatalf("root holds %v", names)
		}
	},
	// The volume refuses to move the view aside (the RESP proxy lets only
	// an admin rename or remove a folder at project depth): the view
	// stays, emptied, with its record, and the refusal is reported.
	"ZV_RemoveViewMoveRefusedKeepsEmptyView": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		before := ino(t, fx, "Collections/Shoot")
		g.log.refusePlace = func(from, to string) error {
			if from == "Collections/Shoot" {
				return syscall.EROFS
			}
			return nil
		}
		_, err := g.rec.RemoveView("Shoot")
		if !errors.Is(err, syscall.EROFS) || errors.Is(err, smartfolders.ErrViewInUse) {
			t.Fatalf("err = %v", err)
		}
		assertView(t, fx, "Collections/Shoot", nil)
		if v := g.rec.Own.Views["Shoot"]; v == nil || v.RemoveAs != "" || ino(t, fx, "Collections/Shoot") != before {
			t.Fatalf("record = %+v", v)
		}
		var stuck *smartfolders.StuckError
		if errors.As(err, &stuck) {
			t.Fatalf("a refused move reported as stuck: %v", err)
		}
		// Wanted again: the same (owned) folder is used.
		g.log.refusePlace = nil
		g.reconcile(t, "Shoot", link("a.mov", "Proj/a.mov"))
		assertView(t, fx, "Collections/Shoot", map[string]string{"a.mov": "Proj/a.mov"})
		if ino(t, fx, "Collections/Shoot") != before {
			t.Fatal("view replaced")
		}
	},
	// The Finder's .DS_Store (a regular file, exactly that name) goes with
	// the view; it does not keep it in use.
	"ZV_RemoveViewTakesFinderFile": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		fx.WriteFile(t, "Collections/Shoot/.DS_Store")
		if res, err := g.rec.RemoveView("Shoot"); err != nil || !res.RemovedView {
			t.Fatalf("RemoveView = %+v, %v", res, err)
		}
		if names := rootEntries(t, fx); len(names) != 0 {
			t.Fatalf("root holds %v", names)
		}
		targetsIntact(t, fx)
	},
	// Nothing else counts as the Finder's: AppleDouble files, other
	// dot-names, a .DS_Store that is not a regular file, or a .DS_Store
	// next to anything else keep the view, untouched.
	"ZV_RemoveViewKeepsOtherDotFiles": func(t *testing.T, fx Fixture) {
		for i, c := range []struct{ file, dir, link string }{
			{file: "._a.mov"}, {file: ".notes"}, {dir: ".DS_Store"}, {link: ".DS_Store"},
			{file: ".DS_Store", dir: "sub"},
		} {
			if i > 0 {
				removeTree(t, fx, "Collections")
			}
			g := newCollectionsRig(t, fx)
			if i == 0 {
				shoot(t, fx, g)
			} else {
				g.reconcile(t, "Shoot", link("a.mov", "Proj/a.mov"), link("b.mov", "Proj/sub/b.mov"))
			}
			var others []string
			if c.file != "" {
				fx.WriteFile(t, "Collections/Shoot/"+c.file)
				others = append(others, c.file)
			}
			if c.dir != "" {
				fx.MkdirAll(t, "Collections/Shoot/"+c.dir)
				others = append(others, c.dir)
			}
			if c.link != "" {
				fx.Symlink(t, "x", "Collections/Shoot/"+c.link)
				others = append(others, c.link)
			}
			g.events = nil
			if _, err := g.rec.RemoveView("Shoot"); !errors.Is(err, smartfolders.ErrViewInUse) {
				t.Fatalf("%+v: err = %v", c, err)
			}
			sort.Strings(others)
			if names := fx.ReadDir(t, "Collections/Shoot"); !reflect.DeepEqual(names, others) {
				t.Fatalf("%+v: view holds %v", c, names)
			}
			for _, ev := range g.events {
				for _, o := range others {
					if strings.HasPrefix(ev, "remove Collections/Shoot/"+o) || strings.HasPrefix(ev, "place Collections/Shoot ") {
						t.Fatalf("%+v: touched: %v", c, g.events)
					}
				}
			}
		}
	},
	// A refused removal writes the state file once (the intent), not
	// again when nothing changed.
	"ZV_RemoveViewRefusedWritesStateOnce": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		g.rec.Reconcile("Shoot", nil) // links gone first
		g.log.refusePlace = func(from, to string) error {
			if from == "Collections/Shoot" {
				return syscall.EROFS
			}
			return nil
		}
		before := len(g.persists)
		if _, err := g.rec.RemoveView("Shoot"); !errors.Is(err, syscall.EROFS) {
			t.Fatalf("err = %v", err)
		}
		if n := len(g.persists) - before; n != 1 {
			t.Fatalf("persisted %d times", n)
		}
		// The restart from that state settles the stale intent.
		g2 := g.restart(t)
		if v := g2.rec.Own.Views["Shoot"]; v == nil || v.RemoveAs == "" {
			t.Fatalf("persisted record = %+v", v)
		}
		g2.reconcile(t, "Shoot", link("a.mov", "Proj/a.mov"))
		if v := g2.rec.Own.Views["Shoot"]; v == nil || v.RemoveAs != "" {
			t.Fatalf("record = %+v", v)
		}
		assertView(t, fx, "Collections/Shoot", map[string]string{"a.mov": "Proj/a.mov"})
	},
	// The root renamed away while a view was moved aside: the hidden path
	// is kept in Lost, never silently forgotten.
	"ZV_RemoveViewRootGoneKeepsLost": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		var hidden string
		g.log.afterPlace = func(from, to string) error {
			if from == "Collections/Shoot" {
				hidden = to
				panic(crashed{})
			}
			return nil
		}
		removeCrashing(t, g)
		if err := fx.FS().Place("Collections", "Old"); err != nil {
			t.Fatal(err)
		}
		g2 := g.restart(t)
		_, _ = g2.rec.RemoveView("Shoot")
		if !reflect.DeepEqual(g2.rec.Own.Lost, []string{hidden}) {
			t.Fatalf("lost = %v, want %v", g2.rec.Own.Lost, hidden)
		}
		if !strings.Contains(g2.persists[len(g2.persists)-1], hidden[len("Collections/"):]) {
			t.Fatal("lost path not persisted")
		}
	},
	// The rmdir is refused after the move: the view goes back to its name.
	"ZV_RemoveViewRmdirRefusedGoesBack": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		g.log.refuseRmdir = func(string) error { return syscall.EPERM }
		if _, err := g.rec.RemoveView("Shoot"); !errors.Is(err, syscall.EPERM) {
			t.Fatalf("err = %v", err)
		}
		if names := rootEntries(t, fx); !reflect.DeepEqual(names, []string{"Shoot"}) {
			t.Fatalf("root holds %v", names)
		}
		if v := g.rec.Own.Views["Shoot"]; v == nil || v.RemoveAs != "" {
			t.Fatalf("record = %+v", v)
		}
		g.log.refuseRmdir = nil
		if res, err := g.rec.RemoveView("Shoot"); err != nil || !res.RemovedView {
			t.Fatalf("retry = %+v, %v", res, err)
		}
	},
	// A refused gate removes nothing.
	"ZV_RemoveViewGateRefused": func(t *testing.T, fx Fixture) {
		g := newCollectionsRig(t, fx)
		shoot(t, fx, g)
		g.failGate = errors.New("lease held by john@mac")
		g.events = nil
		if _, err := g.rec.RemoveView("Shoot"); err == nil {
			t.Fatal("removal ignored the gate")
		}
		for _, ev := range g.events {
			if !strings.HasPrefix(ev, "persist ") {
				t.Fatalf("wrote despite the refused gate: %v", g.events)
			}
		}
		assertView(t, fx, "Collections/Shoot", map[string]string{"a.mov": "Proj/a.mov", "b.mov": "Proj/sub/b.mov"})
	},
	// A Collections folder someone else made is never written to, and the
	// other smart root is never a link target.
	"ZV_ForeignRootAndExcludedRoot": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Recents/Proj/user.mov")
		g := newCollectionsRig(t, fx)
		res := g.reconcile(t, "Shoot", link("a.mov", "Proj/a.mov"), link("user.mov", "Recents/Proj/user.mov"))
		if len(res.Refused) != 1 || !errors.Is(res.Refused[0].Err, smartfolders.ErrTargetForbidden) {
			t.Fatalf("refused = %+v", res.Refused)
		}
		assertView(t, fx, "Collections/Shoot", map[string]string{"a.mov": "Proj/a.mov"})

		removeTree(t, fx, "Collections")
		fx.MkdirAll(t, "Collections")
		fx.WriteFile(t, "Collections/mine.txt")
		g2 := newCollectionsRig(t, fx)
		if _, err := g2.rec.Reconcile("Shoot", []smartfolders.Link{link("a.mov", "Proj/a.mov")}); !errors.Is(err, smartfolders.ErrNotManaged) {
			t.Fatalf("err = %v", err)
		}
		if _, err := g2.rec.RemoveView("Shoot"); !errors.Is(err, smartfolders.ErrNotManaged) {
			t.Fatalf("err = %v", err)
		}
		if len(g2.events) != 0 || !reflect.DeepEqual(fx.ReadDir(t, "Collections"), []string{"mine.txt"}) {
			t.Fatalf("wrote into a foreign root: %v", g2.events)
		}
	},
}
