package sftest

// Regression cases from the final crash/lost-reply review (round 7).

import (
	"errors"
	"io/fs"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

var lost = errors.New("connection reset (reply lost)")

func isTmp(p string) bool { return strings.Contains(p, "/.") }

// clean: the view holds exactly want, no temp records remain, and removing
// everything afterwards leaves the view empty (no orphaned own link).
func clean(t *testing.T, fx Fixture, g *rig, want map[string]string, links ...smartfolders.Link) {
	t.Helper()
	if _, err := g.rec.Reconcile("Proj", links); err != nil {
		t.Fatalf("settling run: %v", err)
	}
	if _, err := g.rec.Reconcile("Proj", links); err != nil {
		t.Fatalf("second run: %v", err)
	}
	assertView(t, fx, "Recents/Proj", want)
	if v := g.rec.Own.Views["Proj"]; v != nil && len(v.Temps) != 0 {
		t.Fatalf("temp records left: %+v", v.Temps)
	}
	if _, err := g.rec.Reconcile("Proj", nil); err != nil {
		t.Fatalf("remove-all: %v", err)
	}
	if names := fx.ReadDir(t, "Recents/Proj"); len(names) != 0 {
		t.Fatalf("orphaned entries after remove-all: %v (records %+v)", names, g.rec.Own.Views["Proj"])
	}
}

func userFileOnly(t *testing.T, fx Fixture, g *rig) {
	t.Helper()
	assertUserFileAtName(t, fx, g)
	// And a later remove-all must not touch it.
	_, _ = g.rec.Reconcile("Proj", nil)
	assertUserFileAtName(t, fx, g)
}

// reviewTests are crash and lost-reply scenarios from the final review: each
// stops or confuses one step of a create, retarget or remove (a crash, a
// lost reply, a retried request), sometimes with a user acting in the
// window, and requires the next runs to converge with no user data lost
// and nothing of the Manager's orphaned.
var reviewTests = map[string]func(*testing.T, Fixture){
	"ZR_CreateCrashAfterSymlink": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.log.afterSymlink = func(string) error { panic(crashed{}) }
		g.runCrashing(t, link("x.mov", "Proj/a.mov"))
		clean(t, fx, g.restart(t), map[string]string{"x.mov": "Proj/a.mov"}, link("x.mov", "Proj/a.mov"))
	},
	"ZR_CreateLostSymlinkReply": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("y.mov", "Proj/a.mov")) // view exists
		g.log.afterSymlink = func(string) error { return lost }
		_, _ = g.rec.Reconcile("Proj", []smartfolders.Link{link("y.mov", "Proj/a.mov"), link("x.mov", "Proj/a.mov")})
		g.log.afterSymlink = nil
		clean(t, fx, g, map[string]string{"x.mov": "Proj/a.mov", "y.mov": "Proj/a.mov"}, link("y.mov", "Proj/a.mov"), link("x.mov", "Proj/a.mov"))
	},
	"ZR_CreatePlaceLostReply": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("y.mov", "Proj/a.mov"))
		g.log.afterPlace = func(string, string) error { return lost }
		_, _ = g.rec.Reconcile("Proj", []smartfolders.Link{link("y.mov", "Proj/a.mov"), link("x.mov", "Proj/a.mov")})
		g.log.afterPlace = nil
		clean(t, fx, g, map[string]string{"x.mov": "Proj/a.mov", "y.mov": "Proj/a.mov"}, link("y.mov", "Proj/a.mov"), link("x.mov", "Proj/a.mov"))
	},
	"ZR_CreateRaceCrashInTempRemove": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("y.mov", "Proj/a.mov"))
		g.log.beforePlace = func(from, to string) {
			if isX(to) {
				fx.WriteFile(t, "Recents/Proj/x.mov")
			}
		}
		g.log.afterRemove = func(string) error { panic(crashed{}) }
		g.runCrashing(t, link("y.mov", "Proj/a.mov"), link("x.mov", "Proj/a.mov"))
		g2 := g.restart(t)
		_, _ = g2.rec.Reconcile("Proj", []smartfolders.Link{link("y.mov", "Proj/a.mov"), link("x.mov", "Proj/a.mov")})
		assertView(t, fx, "Recents/Proj", map[string]string{"y.mov": "Proj/a.mov"}, "x.mov")
		if v := g2.rec.Own.Views["Proj"]; len(v.Temps) != 0 {
			t.Fatalf("temps left %+v", v.Temps)
		}
	},
	"ZR_RetargetCrashAfterSymlink": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Proj/b.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		g.log.afterSymlink = func(string) error { panic(crashed{}) }
		g.runCrashing(t, link("x.mov", "Proj/b.mov"))
		clean(t, fx, g.restart(t), map[string]string{"x.mov": "Proj/b.mov"}, link("x.mov", "Proj/b.mov"))
	},
	"ZR_RetargetCrashAfterSwap": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Proj/b.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		g.log.afterSwap = func(string, string) error { panic(crashed{}) }
		g.runCrashing(t, link("x.mov", "Proj/b.mov"))
		g2 := g.restart(t)
		clean(t, fx, g2, map[string]string{"x.mov": "Proj/b.mov"}, link("x.mov", "Proj/b.mov"))
	},
	"ZR_RetargetCrashAfterOldRemoved": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Proj/b.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		g.log.afterRemove = func(string) error { panic(crashed{}) }
		g.runCrashing(t, link("x.mov", "Proj/b.mov"))
		clean(t, fx, g.restart(t), map[string]string{"x.mov": "Proj/b.mov"}, link("x.mov", "Proj/b.mov"))
	},
	"ZR_RetargetRaceCrashAfterSwapBack": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Proj/b.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		n := 0
		g.log.beforeSwap = func(string, string) {
			n++
			if n == 1 {
				userReplace(t, fx)
			}
		}
		g.log.afterSwap = func(string, string) error {
			if n == 2 {
				panic(crashed{})
			}
			return nil
		}
		g.runCrashing(t, link("x.mov", "Proj/b.mov"))
		g2 := g.restart(t)
		_, _ = g2.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
		userFileOnly(t, fx, g2)
	},
	"ZR_RetargetRaceCrashBetweenSwaps": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Proj/b.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		n := 0
		g.log.beforeSwap = func(string, string) {
			n++
			if n == 1 {
				userReplace(t, fx)
			}
			if n == 2 {
				panic(crashed{})
			}
		}
		g.runCrashing(t, link("x.mov", "Proj/b.mov"))
		g2 := g.restart(t)
		_, _ = g2.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
		userFileOnly(t, fx, g2)
	},
	"ZR_RetargetRaceSwapBackLostReply": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Proj/b.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		n := 0
		g.log.beforeSwap = func(string, string) {
			n++
			if n == 1 {
				userReplace(t, fx)
			}
		}
		g.log.afterSwap = func(string, string) error {
			if n == 2 {
				return lost
			}
			return nil
		}
		_, _ = g.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
		g.log.beforeSwap, g.log.afterSwap = nil, nil
		_, _ = g.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
		userFileOnly(t, fx, g)
	},
	"ZR_RetargetRaceCrashInTempRemove": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Proj/b.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		n := 0
		g.log.beforeSwap = func(string, string) {
			n++
			if n == 1 {
				userReplace(t, fx)
			}
		}
		g.log.beforeRemove = func(string) { panic(crashed{}) }
		g.runCrashing(t, link("x.mov", "Proj/b.mov"))
		g2 := g.restart(t)
		_, _ = g2.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
		userFileOnly(t, fx, g2)
	},
	"ZR_RemoveCrashAfterPlace": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		g.log.afterPlace = func(string, string) error { panic(crashed{}) }
		g.runCrashing(t)
		clean(t, fx, g.restart(t), map[string]string{})
	},
	"ZR_RemoveCrashAfterRemove": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		g.log.afterRemove = func(string) error { panic(crashed{}) }
		g.runCrashing(t)
		clean(t, fx, g.restart(t), map[string]string{})
	},
	"ZR_RemoveRaceCrashAfterPlaceBack": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		g.log.beforePlace = func(from, to string) {
			if isX(from) {
				userReplace(t, fx)
			}
		}
		g.log.afterPlace = func(from, to string) error {
			if isX(to) {
				panic(crashed{})
			}
			return nil
		}
		g.runCrashing(t)
		g2 := g.restart(t)
		_, _ = g2.rec.Reconcile("Proj", nil)
		userFileOnly(t, fx, g2)
	},
	"ZR_RemoveRacePlaceBackLostReply": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		g.log.beforePlace = func(from, to string) {
			if isX(from) {
				userReplace(t, fx)
			}
		}
		g.log.afterPlace = func(from, to string) error {
			if isX(to) {
				return lost
			}
			return nil
		}
		_, _ = g.rec.Reconcile("Proj", nil)
		g.log.beforePlace, g.log.afterPlace = nil, nil
		_, _ = g.rec.Reconcile("Proj", nil)
		userFileOnly(t, fx, g)
	},
	// Crash during the restart's own settle (two crashes in a row).
	"ZR_RetargetRaceDoubleCrash": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Proj/b.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		g.log.beforeSwap = func(string, string) { userReplace(t, fx) }
		g.log.afterSwap = func(string, string) error { panic(crashed{}) }
		g.runCrashing(t, link("x.mov", "Proj/b.mov"))
		g2 := g.restart(t)
		g2.persists = append([]string(nil), g.persists...) // the state file carries over
		g2.log.afterSwap = func(string, string) error { panic(crashed{}) }
		g2.runCrashing(t, link("x.mov", "Proj/b.mov"))
		g3 := g2.restart(t)
		_, _ = g3.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
		userFileOnly(t, fx, g3)
	},
	// A committed symlink whose retried request reports EEXIST.
	"ZR_SymlinkRetryEEXIST": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("y.mov", "Proj/a.mov"))
		g.log.afterSymlink = func(string) error { return fs.ErrExist }
		_, _ = g.rec.Reconcile("Proj", []smartfolders.Link{link("y.mov", "Proj/a.mov"), link("x.mov", "Proj/a.mov")})
		g.log.afterSymlink = nil
		clean(t, fx, g, map[string]string{"x.mov": "Proj/a.mov", "y.mov": "Proj/a.mov"}, link("y.mov", "Proj/a.mov"), link("x.mov", "Proj/a.mov"))
	},
	// A committed exchange applied twice by a retry (net: nothing moved,
	// reported success).
	"ZR_SwapAppliedTwice": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov", "Proj/b.mov")
		g := newRig(t, fx)
		g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
		g.log.afterSwap = func(tmp, p string) error { return fx.FS().Swap(tmp, p) }
		_, _ = g.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
		g.log.afterSwap = nil
		clean(t, fx, g, map[string]string{"x.mov": "Proj/b.mov"}, link("x.mov", "Proj/b.mov"))
	},
	"ZR_ViewMkdirCrash": func(t *testing.T, fx Fixture) {
		files(t, fx, "Proj/a.mov")
		g := newRig(t, fx)
		g.log.afterMkdir = func(p string) error {
			if strings.HasSuffix(p, "/Proj") {
				panic(crashed{})
			}
			return nil
		}
		g.runCrashing(t, link("x.mov", "Proj/a.mov"))
		clean(t, fx, g.restart(t), map[string]string{"x.mov": "Proj/a.mov"}, link("x.mov", "Proj/a.mov"))
	},
}
