// Package sftest is the reconciler behaviour suite, run against every
// smartfolders.FS implementation (the os.Root backend and the JuiceFS
// metadata backend) so both are held to the same contract. Test-only.
package sftest

import (
	"encoding/json"
	"errors"
	"io/fs"
	"path"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

// Fixture is a fresh, empty volume plus the setup and inspection tools a
// test needs that the reconciler itself must not have.
type Fixture interface {
	FS() smartfolders.FS
	MkdirAll(t *testing.T, p string)
	WriteFile(t *testing.T, p string)
	Symlink(t *testing.T, text, p string)
	Remove(t *testing.T, p string) // a file or symlink
	ReadDir(t *testing.T, p string) []string
	// TrashCount reports entries in the JuiceFS trash, or -1 when the
	// backend has none to inspect.
	TrashCount(t *testing.T) int
}

// opLog wraps an FS and records every mutating call, interleaved with the
// ownership snapshots Persist saved, so tests can prove "recorded before
// created".
type opLog struct {
	smartfolders.FS
	events *[]string
	// beforeSwap runs just before each Swap reaches the volume (a user
	// acting in the window after the reconciler's last check).
	beforeSwap func(tmp, p string)
	// beforePlace / beforeRemove run just before each Place / Remove
	// reaches the volume.
	beforePlace  func(from, to string)
	beforeRemove func(p string)
	// afterPlace / afterSwap run once the operation has reached the volume;
	// a non-nil error replaces its result (a lost reply), and a panic
	// stands for the process dying right there.
	afterPlace   func(from, to string) error
	afterSwap    func(tmp, p string) error
	beforeChmod  func(p string)
	afterSymlink func(p string) error
	afterRemove  func(p string) error
	afterMkdir   func(p string) error
	// lostMkdirReply makes Mkdir succeed on the volume but report an error,
	// as when the reply is lost.
	lostMkdirReply bool
}

func (o *opLog) Mkdir(p string, perm fs.FileMode) (smartfolders.Info, error) {
	*o.events = append(*o.events, "mkdir "+p)
	info, err := o.FS.Mkdir(p, perm)
	if err == nil && o.afterMkdir != nil {
		if e := o.afterMkdir(p); e != nil {
			return smartfolders.Info{}, e
		}
	}
	if err == nil && o.lostMkdirReply {
		return smartfolders.Info{}, errors.New("connection reset (reply lost)")
	}
	return info, err
}

func (o *opLog) Symlink(text, p string) (smartfolders.Info, error) {
	*o.events = append(*o.events, "symlink "+p)
	in, err := o.FS.Symlink(text, p)
	if err == nil && o.afterSymlink != nil {
		if e := o.afterSymlink(p); e != nil {
			return smartfolders.Info{}, e
		}
	}
	return in, err
}

func (o *opLog) Place(tmp, p string) error {
	*o.events = append(*o.events, "place "+tmp+" "+p)
	if o.beforePlace != nil {
		o.beforePlace(tmp, p)
	}
	err := o.FS.Place(tmp, p)
	if err == nil && o.afterPlace != nil {
		if e := o.afterPlace(tmp, p); e != nil {
			return e
		}
	}
	return err
}

func (o *opLog) Swap(tmp, p string) error {
	*o.events = append(*o.events, "swap "+tmp+" "+p)
	if o.beforeSwap != nil {
		o.beforeSwap(tmp, p)
	}
	err := o.FS.Swap(tmp, p)
	if err == nil && o.afterSwap != nil {
		if e := o.afterSwap(tmp, p); e != nil {
			return e
		}
	}
	return err
}

func (o *opLog) Chmod(p string, ino uint64, perm fs.FileMode) error {
	*o.events = append(*o.events, "chmod "+p)
	if o.beforeChmod != nil {
		o.beforeChmod(p)
	}
	return o.FS.Chmod(p, ino, perm)
}

func (o *opLog) Remove(p string) error {
	*o.events = append(*o.events, "remove "+p)
	if o.beforeRemove != nil {
		o.beforeRemove(p)
	}
	err := o.FS.Remove(p)
	if err == nil && o.afterRemove != nil {
		if e := o.afterRemove(p); e != nil {
			return e
		}
	}
	return err
}

// rig is one reconciler over a fixture, with counters.
type rig struct {
	fx       Fixture
	log      *opLog
	rec      *smartfolders.Reconciler
	events   []string
	persists []string // JSON of Own at each Persist
	gates    []string
	failGate error
}

func newRig(t *testing.T, fx Fixture) *rig {
	g := &rig{fx: fx}
	g.log = &opLog{FS: fx.FS(), events: &g.events}
	g.rec = &smartfolders.Reconciler{
		FS:   g.log,
		Root: "Recents",
		Own:  &smartfolders.Ownership{},
		Persist: func() error {
			b, err := json.Marshal(g.rec.Own)
			if err != nil {
				return err
			}
			g.persists = append(g.persists, string(b))
			g.events = append(g.events, "persist "+string(b))
			return nil
		},
		Gate: func(view string) (func(), error) {
			g.gates = append(g.gates, view)
			if g.failGate != nil {
				return nil, g.failGate
			}
			return func() {}, nil
		},
	}
	return g
}

func (g *rig) reconcile(t *testing.T, folder string, want ...smartfolders.Link) smartfolders.Result {
	t.Helper()
	res, err := g.rec.Reconcile(folder, want)
	if err != nil {
		t.Fatalf("Reconcile(%s): %v", folder, err)
	}
	return res
}

func link(name, target string) smartfolders.Link {
	return smartfolders.Link{Name: name, Target: target}
}

func ino(t *testing.T, fx Fixture, p string) uint64 {
	t.Helper()
	in, err := fx.FS().Lstat(p)
	if err != nil {
		t.Fatalf("lstat %s: %v", p, err)
	}
	return in.Ino
}

// assertView checks the view holds exactly the given links (name -> target
// volume path), each relative and resolving to its target, plus `others`.
func assertView(t *testing.T, fx Fixture, view string, want map[string]string, others ...string) {
	t.Helper()
	fsys := fx.FS()
	got := map[string]string{}
	var extra []string
	for _, name := range fx.ReadDir(t, view) {
		p := view + "/" + name
		in, err := fsys.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		if in.Type != smartfolders.TypeSymlink {
			extra = append(extra, name)
			continue
		}
		text, err := fsys.Readlink(p)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(text, "/") || !strings.HasPrefix(text, "../") {
			t.Errorf("%s -> %q is not relative", p, text)
		}
		resolved := path.Clean(path.Join(view, text))
		if want[name] == "" {
			extra = append(extra, name)
			continue
		}
		rin, err := fsys.Lstat(resolved)
		if err != nil || rin.Type != smartfolders.TypeRegular {
			t.Errorf("%s -> %q does not resolve to a file (%v)", p, text, err)
			continue
		}
		if rin.Ino != ino(t, fx, want[name]) {
			t.Errorf("%s resolves to %s, want %s", p, resolved, want[name])
		}
		got[name] = resolved
	}
	if !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
		t.Errorf("view %s = %v, want %v", view, got, want)
	}
	sort.Strings(extra)
	sort.Strings(others)
	if !reflect.DeepEqual(extra, others) && !(len(extra) == 0 && len(others) == 0) {
		t.Errorf("view %s other entries = %v, want %v", view, extra, others)
	}
}

func exists(fx Fixture, p string) bool {
	_, err := fx.FS().Lstat(p)
	return !errors.Is(err, fs.ErrNotExist)
}

// Run runs the suite; newFixture must return a fresh empty volume.
func Run(t *testing.T, newFixture func(t *testing.T) Fixture) {
	tests := map[string]func(*testing.T, Fixture){
		"CreateRetargetRemove":                testCreateRetargetRemove,
		"Idempotent":                          testIdempotent,
		"RecordedBeforeCreated":               testRecordedBeforeCreated,
		"PersistFailureWritesNothing":         testPersistFailureWritesNothing,
		"NoViewWithoutValidLinks":             testNoViewWithoutValidLinks,
		"EmptiedViewStays":                    testEmptiedViewStays,
		"NeverTouchesForeignEntries":          testNeverTouchesForeignEntries,
		"NeverAdoptsExistingRoot":             testNeverAdoptsExistingRoot,
		"NeverAdoptsExistingView":             testNeverAdoptsExistingView,
		"RefusesForbiddenTargets":             testRefusesForbiddenTargets,
		"TempCleanupOnlyRecorded":             testTempCleanupOnlyRecorded,
		"PendingRecordsSettle":                testPendingRecordsSettle,
		"PendingDirsNeverAdoptUserDirs":       testPendingDirsNeverAdoptUserDirs,
		"LostMkdirReplyRecovers":              testLostMkdirReplyRecovers,
		"RetargetNeverDestroysUserFile":       testRetargetNeverDestroysAUserFile,
		"RemoveNeverDestroysUserFile":         testRemoveNeverDestroysUserFile,
		"CrashAfterFirstPlaceRestarts":        testCrashAfterFirstPlaceRestarts,
		"RemoveCrashAfterMoveKeepsUserFile":   testRemoveCrashAfterMoveKeepsUserFile,
		"RemoveLostReplyKeepsUserFile":        testRemoveLostReplyKeepsUserFile,
		"RemoveLostReplyCleansOwnLink":        testRemoveLostReplyCleansOwnLink,
		"RetargetLostReplyKeepsUserFile":      testRetargetLostReplyKeepsUserFile,
		"RetargetCrashAfterSwapKeepsUserFile": testRetargetCrashAfterSwapKeepsUserFile,
		"RetargetLostReplyKeepsOwnLink":       testRetargetLostReplyKeepsOwnLink,
		"TempEntryNeverForgotten":             testTempEntryNeverForgotten,
		"DroppedRecordsNameTheirTemps":        testDroppedRecordsNameTheirTemps,
		"MaxViews":                            testMaxViews,
		"RootRemovedIsRecreated":              testRootRemovedIsRecreated,
		"GateRefusedWritesNothing":            testGateRefusedWritesNothing,
		"SymlinkedParentsNeverWritten":        testSymlinkedParents,
	}
	for k, v := range reviewTests {
		tests[k] = v
	}
	names := make([]string, 0, len(tests))
	for n := range tests {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fn := tests[n]
		t.Run(n, func(t *testing.T) { fn(t, newFixture(t)) })
	}
}

// caseInsensitive reports whether the fixture's filesystem folds case.
func caseInsensitive(t *testing.T, fx Fixture) bool {
	t.Helper()
	files(t, fx, "CaseProbe/probe")
	defer fx.Remove(t, "CaseProbe/probe")
	return exists(fx, "CaseProbe/PROBE")
}

func files(t *testing.T, fx Fixture, ps ...string) {
	for _, p := range ps {
		fx.WriteFile(t, p)
	}
}

func testCreateRetargetRemove(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/sub/b.mov", "Proj/c.wav", "Proj/other/a.mov")
	g := newRig(t, fx)
	res := g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"), link("b.mov", "Proj/sub/b.mov"))
	if !res.CreatedRoot || !res.CreatedView || !reflect.DeepEqual(res.Created, []string{"a.mov", "b.mov"}) {
		t.Fatalf("first run = %+v", res)
	}
	assertView(t, fx, "Recents/Proj", map[string]string{"a.mov": "Proj/a.mov", "b.mov": "Proj/sub/b.mov"})
	if text, _ := fx.FS().Readlink("Recents/Proj/b.mov"); text != "../../Proj/sub/b.mov" {
		t.Fatalf("b.mov -> %q", text)
	}

	bIno := ino(t, fx, "Recents/Proj/b.mov")
	res = g.reconcile(t, "Proj", link("a.mov", "Proj/other/a.mov"), link("b.mov", "Proj/sub/b.mov"), link("c.wav", "Proj/c.wav"))
	if !reflect.DeepEqual(res.Retargeted, []string{"a.mov"}) || !reflect.DeepEqual(res.Created, []string{"c.wav"}) ||
		!reflect.DeepEqual(res.Unchanged, []string{"b.mov"}) || len(res.Removed) != 0 || res.CreatedView {
		t.Fatalf("second run = %+v", res)
	}
	if ino(t, fx, "Recents/Proj/b.mov") != bIno {
		t.Fatal("unchanged link was rewritten")
	}
	assertView(t, fx, "Recents/Proj", map[string]string{"a.mov": "Proj/other/a.mov", "b.mov": "Proj/sub/b.mov", "c.wav": "Proj/c.wav"})

	res = g.reconcile(t, "Proj", link("c.wav", "Proj/c.wav"))
	if !reflect.DeepEqual(res.Removed, []string{"a.mov", "b.mov"}) {
		t.Fatalf("third run = %+v", res)
	}
	assertView(t, fx, "Recents/Proj", map[string]string{"c.wav": "Proj/c.wav"})
	if v := g.rec.Own.Views["Proj"]; len(v.Temps) != 0 || len(v.Links) != 1 {
		t.Fatalf("records = %+v", v)
	}
	for _, f := range []string{"Proj/a.mov", "Proj/sub/b.mov", "Proj/c.wav", "Proj/other/a.mov"} {
		if in, err := fx.FS().Lstat(f); err != nil || in.Type != smartfolders.TypeRegular {
			t.Fatalf("target %s touched: %+v %v", f, in, err)
		}
	}
	if n := fx.TrashCount(t); n > 0 {
		t.Fatalf("removing and retargeting links put %d entries in the trash", n)
	}
	if len(g.gates) != 3 {
		t.Fatalf("gate ran %d times, want once per changing run (3)", len(g.gates))
	}
}

func testIdempotent(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/b.mov")
	g := newRig(t, fx)
	want := []smartfolders.Link{link("a.mov", "Proj/a.mov"), link("b.mov", "Proj/b.mov")}
	g.reconcile(t, "Proj", want...)
	inodes := map[string]uint64{}
	for _, p := range []string{"Recents", "Recents/Proj", "Recents/Proj/a.mov", "Recents/Proj/b.mov"} {
		inodes[p] = ino(t, fx, p)
	}
	persists, gates, events := len(g.persists), len(g.gates), len(g.events)
	res := g.reconcile(t, "Proj", want...)
	if res.Changed() || !reflect.DeepEqual(res.Unchanged, []string{"a.mov", "b.mov"}) || len(res.Refused)+len(res.Foreign) != 0 {
		t.Fatalf("second run = %+v", res)
	}
	if len(g.persists) != persists || len(g.gates) != gates || len(g.events) != events {
		t.Fatalf("an idempotent run persisted, gated or wrote: %v", g.events[events:])
	}
	for p, i := range inodes {
		if ino(t, fx, p) != i {
			t.Fatalf("%s was rewritten", p)
		}
	}
}

func testRecordedBeforeCreated(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/b.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	g.reconcile(t, "Proj", link("a.mov", "Proj/b.mov")) // retarget through a temp name
	// Every created entry must appear in an earlier persisted snapshot.
	var last string
	for _, ev := range g.events {
		switch {
		case strings.HasPrefix(ev, "persist "):
			last = ev
		case strings.HasPrefix(ev, "mkdir ") || strings.HasPrefix(ev, "symlink "):
			p := ev[strings.Index(ev, " ")+1:]
			parts := strings.Split(p, "/")
			key := `"` + parts[len(parts)-1] + `"`
			if len(parts) == 1 {
				key = `"root":`
			}
			if !strings.Contains(last, key) {
				t.Fatalf("%s happened before it was recorded (last persisted: %s)", ev, last)
			}
		}
	}
}

func testPersistFailureWritesNothing(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.rec.Persist = func() error { return errors.New("disk full") }
	if _, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("a.mov", "Proj/a.mov")}); err == nil {
		t.Fatal("reconcile succeeded without persisting")
	}
	if exists(fx, "Recents") {
		t.Fatal("root created although its record was not saved")
	}
}

func testNoViewWithoutValidLinks(t *testing.T, fx Fixture) {
	g := newRig(t, fx)
	res := g.reconcile(t, "Ghost", link("x.mov", "Ghost/x.mov"))
	if len(res.Refused) != 1 || !res.Refused[0].TargetGone() || res.Changed() {
		t.Fatalf("res = %+v", res)
	}
	if exists(fx, "Recents") || len(g.gates)+len(g.persists) != 0 {
		t.Fatalf("a view with no valid link was started (gates %v, persists %d)", g.gates, len(g.persists))
	}
}

func testEmptiedViewStays(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	viewIno := ino(t, fx, "Recents/Proj")
	res := g.reconcile(t, "Proj")
	if !reflect.DeepEqual(res.Removed, []string{"a.mov"}) {
		t.Fatalf("res = %+v", res)
	}
	if ino(t, fx, "Recents/Proj") != viewIno || len(fx.ReadDir(t, "Recents/Proj")) != 0 {
		t.Fatal("the emptied view did not stay as an empty directory")
	}
	// It is still the Manager's, and fills again.
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	assertView(t, fx, "Recents/Proj", map[string]string{"a.mov": "Proj/a.mov"})
	if n := fx.TrashCount(t); n > 0 {
		t.Fatalf("%d trash entries", n)
	}
}

func testNeverTouchesForeignEntries(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/b.mov", "Proj/c.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"), link("c.mov", "Proj/c.mov"))
	// A user drops a file, a directory and a symlink of their own, one of
	// them on a name the view wants next, and replaces an owned link.
	fx.WriteFile(t, "Recents/Proj/notes.txt")
	fx.WriteFile(t, "Recents/Proj/b.mov")
	fx.MkdirAll(t, "Recents/Proj/folder")
	fx.Symlink(t, "../../Proj/a.mov", "Recents/Proj/mylink.mov")
	fx.Remove(t, "Recents/Proj/c.mov")
	fx.WriteFile(t, "Recents/Proj/c.mov")

	res := g.reconcile(t, "Proj", link("b.mov", "Proj/b.mov"), link("c.mov", "Proj/c.mov"))
	if !reflect.DeepEqual(res.Removed, []string{"a.mov"}) || !reflect.DeepEqual(res.Foreign, []string{"c.mov"}) {
		t.Fatalf("res = %+v", res)
	}
	taken := map[string]bool{}
	for _, r := range res.Refused {
		if errors.Is(r.Err, smartfolders.ErrNameTaken) {
			taken[r.Name] = true
		}
	}
	if !taken["b.mov"] || !taken["c.mov"] {
		t.Fatalf("refused = %+v", res.Refused)
	}
	assertView(t, fx, "Recents/Proj", map[string]string{}, "b.mov", "c.mov", "folder", "mylink.mov", "notes.txt")
	if in, _ := fx.FS().Lstat("Recents/Proj/b.mov"); in.Type != smartfolders.TypeRegular {
		t.Fatal("user's b.mov replaced")
	}
	// The user's own symlink with a managed-looking target is never removed.
	g.reconcile(t, "Proj")
	if !exists(fx, "Recents/Proj/mylink.mov") {
		t.Fatal("user's symlink removed")
	}
	if _, ok := g.rec.Own.Views["Proj"].Links["c.mov"]; ok {
		t.Fatal("replaced link still recorded as owned")
	}
}

func testNeverAdoptsExistingRoot(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	fx.MkdirAll(t, "Recents") // a user's own (empty) Recents
	g := newRig(t, fx)
	if st, err := g.rec.CheckRoot(); st != smartfolders.RootForeign || err != nil {
		t.Fatalf("CheckRoot = %v, %v", st, err)
	}
	_, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("a.mov", "Proj/a.mov")})
	if !errors.Is(err, smartfolders.ErrNotManaged) {
		t.Fatalf("err = %v", err)
	}
	if len(fx.ReadDir(t, "Recents")) != 0 || len(g.gates) != 0 {
		t.Fatal("wrote into, or checked out a lease for, a folder the Manager did not create")
	}
}

func testNeverAdoptsExistingView(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Other/b.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Other", link("b.mov", "Other/b.mov")) // the Manager's root
	fx.MkdirAll(t, "Recents/Proj")                        // a user's empty folder in it
	_, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("a.mov", "Proj/a.mov")})
	if !errors.Is(err, smartfolders.ErrNotManaged) {
		t.Fatalf("err = %v", err)
	}
	if len(fx.ReadDir(t, "Recents/Proj")) != 0 {
		t.Fatal("wrote into a view the Manager did not create")
	}
	// A view the Manager created but someone replaced is dropped, not used.
	fx.Remove(t, "Recents/Other/b.mov")
	if err := removeDir(fx, "Recents/Other"); err != nil {
		t.Fatalf("rmdir: %v", err)
	}
	fx.MkdirAll(t, "Recents/Other")
	if ino(t, fx, "Recents/Other") == g.rec.Own.Views["Other"].Ino {
		// Identity is the inode number. JuiceFS never reuses one; a local
		// test filesystem may hand the freed number straight back.
		t.Log("the replacement reused the inode number; identity cannot tell them apart on this filesystem")
		return
	}
	if _, err := g.rec.Reconcile("Other", []smartfolders.Link{link("b.mov", "Other/b.mov")}); !errors.Is(err, smartfolders.ErrNotManaged) {
		t.Fatalf("replaced view: err = %v", err)
	}
	if _, ok := g.rec.Own.Views["Other"]; ok {
		t.Fatal("replaced view still recorded")
	}
}

// removeDir is optional fixture support.
func removeDir(fx Fixture, p string) error {
	if r, ok := fx.(interface{ Rmdir(string) error }); ok {
		return r.Rmdir(p)
	}
	return errors.ErrUnsupported
}

func testRefusesForbiddenTargets(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/.hidden/x.mov", "Elsewhere/ok.mov")
	fx.MkdirAll(t, "Proj/dir")
	fx.Symlink(t, "a.mov", "Proj/link.mov")
	fx.Symlink(t, "../Elsewhere", "Proj/escape")
	g := newRig(t, fx)
	g.reconcile(t, "Elsewhere", link("ok.mov", "Elsewhere/ok.mov"))
	cases := map[string]error{
		"/Proj/a.mov":                       smartfolders.ErrTargetForbidden,
		"../Proj/a.mov":                     smartfolders.ErrTargetForbidden,
		"Proj/../Proj/a.mov":                smartfolders.ErrTargetForbidden,
		"Proj/./a.mov":                      smartfolders.ErrTargetForbidden,
		"Proj//a.mov":                       smartfolders.ErrTargetForbidden,
		"":                                  smartfolders.ErrTargetForbidden,
		"a.mov":                             smartfolders.ErrTargetForbidden,
		".trash/2026-11-01-10/1-Proj|a.mov": smartfolders.ErrTargetForbidden,
		".juicemount/derivatives/a.jpg":     smartfolders.ErrTargetForbidden,
		"Proj/.hidden/x.mov":                smartfolders.ErrTargetForbidden,
		"Recents/Elsewhere/ok.mov":          smartfolders.ErrTargetForbidden,
		"Proj/missing.mov":                  smartfolders.ErrTargetMissing,
		"Proj/a.mov/child":                  smartfolders.ErrTargetMissing,
		"Proj/dir":                          smartfolders.ErrTargetNotRegular,
		"Proj/link.mov":                     smartfolders.ErrTargetNotRegular,
		"Proj/escape/ok.mov":                smartfolders.ErrTargetNotRegular,
	}
	var want []smartfolders.Link
	i := 0
	for target := range cases {
		i++
		want = append(want, link("n"+string(rune('a'+i))+".mov", target))
	}
	want = append(want, link("good.mov", "Proj/a.mov"))
	res := g.reconcile(t, "Proj", want...)
	if !reflect.DeepEqual(res.Created, []string{"good.mov"}) || len(res.Refused) != len(cases) {
		t.Fatalf("res = %+v", res)
	}
	for _, r := range res.Refused {
		if !errors.Is(r.Err, cases[r.Target]) {
			t.Errorf("target %q: err = %v, want %v", r.Target, r.Err, cases[r.Target])
		}
	}
	assertView(t, fx, "Recents/Proj", map[string]string{"good.mov": "Proj/a.mov"})

	// Bad names are refused before anything else.
	res = g.reconcile(t, "Proj", link(".hidden.mov", "Proj/a.mov"), link("a/b.mov", "Proj/a.mov"),
		link("..", "Proj/a.mov"), link("", "Proj/a.mov"), link("GOOD.MOV", "Proj/a.mov"), link("good.mov", "Proj/a.mov"))
	if len(res.Refused) != 5 || !reflect.DeepEqual(res.Removed, []string{"good.mov"}) {
		t.Fatalf("names: %+v", res)
	}
	// GOOD.MOV replaces good.mov. A case-sensitive volume (JuiceFS, every
	// production backend) does that in one pass. On a case-insensitive test
	// filesystem (APFS) the old link still holds the name while GOOD.MOV is
	// placed, so it is refused as taken, nothing is overwritten, and the next
	// pass places it.
	if caseInsensitive(t, fx) {
		if len(res.Created) != 0 {
			t.Fatalf("case-insensitive pass created %v", res.Created)
		}
		res = g.reconcile(t, "Proj", link("GOOD.MOV", "Proj/a.mov"))
	}
	if !reflect.DeepEqual(res.Created, []string{"GOOD.MOV"}) {
		t.Fatalf("GOOD.MOV not placed: %+v", res)
	}
	assertView(t, fx, "Recents/Proj", map[string]string{"GOOD.MOV": "Proj/a.mov"})
	for _, folder := range []string{"", ".x", "a/b", "Recents"} {
		if _, err := g.rec.Reconcile(folder, nil); !errors.Is(err, smartfolders.ErrBadView) {
			t.Errorf("folder %q: err = %v", folder, err)
		}
	}
}

func testTempCleanupOnlyRecorded(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	// A temp link from an interrupted retarget, recorded; and a look-alike
	// name nobody recorded.
	recorded := ".juicemount-smart-folder.tmp-0001"
	unrecorded := ".juicemount-smart-folder.tmp-0002"
	fx.Symlink(t, "../../Proj/a.mov", "Recents/Proj/"+recorded)
	fx.Symlink(t, "../../Proj/a.mov", "Recents/Proj/"+unrecorded)
	g.rec.Own.Views["Proj"].Temps = map[string]*smartfolders.OwnedTemp{recorded: {For: "b.mov", Target: "Proj/a.mov", Text: "../../Proj/a.mov"}}
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	if exists(fx, "Recents/Proj/"+recorded) {
		t.Fatal("recorded temp not cleaned")
	}
	if !exists(fx, "Recents/Proj/"+unrecorded) {
		t.Fatal("unrecorded temp-looking entry removed")
	}
	if len(g.rec.Own.Views["Proj"].Temps) != 0 {
		t.Fatal("temp record kept")
	}
}

func testPendingRecordsSettle(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/b.mov", "Proj/c.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	v := g.rec.Own.Views["Proj"]
	// Crash after a temp link was created, its inode recorded and renamed
	// into place, before the final save: the link is the Manager's.
	fx.Symlink(t, "../../Proj/b.mov", "Recents/Proj/b.mov")
	bIno := ino(t, fx, "Recents/Proj/b.mov")
	v.Temps = map[string]*smartfolders.OwnedTemp{
		".juicemount-smart-folder.tmp-placed": {For: "b.mov", Target: "Proj/b.mov", Text: "../../Proj/b.mov", Ino: bIno},
		// Crash after the intent was saved, before the temp link existed;
		// meanwhile a user made their own c.mov link with the same text.
		".juicemount-smart-folder.tmp-never": {For: "c.mov", Target: "Proj/c.mov", Text: "../../Proj/c.mov"},
	}
	fx.Symlink(t, "../../Proj/c.mov", "Recents/Proj/c.mov")

	res := g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"), link("b.mov", "Proj/b.mov"), link("c.mov", "Proj/c.mov"))
	if !reflect.DeepEqual(res.Unchanged, []string{"a.mov", "b.mov"}) || res.Changed() {
		t.Fatalf("res = %+v", res)
	}
	if l := v.Links["b.mov"]; l == nil || l.Ino != bIno {
		t.Fatalf("placed temp not settled: %+v", l)
	}
	if _, ok := v.Links["c.mov"]; ok || len(v.Temps) != 0 {
		t.Fatalf("a user's same-name same-target link was adopted: %+v %+v", v.Links, v.Temps)
	}
	if len(res.Refused) != 1 || !errors.Is(res.Refused[0].Err, smartfolders.ErrNameTaken) {
		t.Fatalf("refused = %+v", res.Refused)
	}
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	if !exists(fx, "Recents/Proj/c.mov") {
		t.Fatal("the user's link was removed")
	}
}

// A crash between recording a directory as pending and creating it never
// adopts a directory a user made under that name meanwhile.
func testPendingDirsNeverAdoptUserDirs(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Other/b.mov")
	g := newRig(t, fx)
	g.rec.Own.Root = &smartfolders.OwnedDir{Pending: true}
	fx.MkdirAll(t, "Recents") // the user's own, made after the crash
	if _, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("a.mov", "Proj/a.mov")}); !errors.Is(err, smartfolders.ErrNotManaged) {
		t.Fatalf("pending root: err = %v", err)
	}
	if g.rec.Own.Root != nil || len(fx.ReadDir(t, "Recents")) != 0 {
		t.Fatal("adopted or wrote into the user's Recents")
	}

	// Same for a view, under a root the Manager did create.
	fx2 := g.fx
	if err := removeDir(fx2, "Recents"); err != nil {
		t.Fatal(err)
	}
	g.reconcile(t, "Other", link("b.mov", "Other/b.mov"))
	g.rec.Own.Views["Proj"] = &smartfolders.OwnedView{OwnedDir: smartfolders.OwnedDir{Pending: true}}
	fx.MkdirAll(t, "Recents/Proj")
	if _, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("a.mov", "Proj/a.mov")}); !errors.Is(err, smartfolders.ErrNotManaged) {
		t.Fatalf("pending view: err = %v", err)
	}
	if _, ok := g.rec.Own.Views["Proj"]; ok || len(fx.ReadDir(t, "Recents/Proj")) != 0 {
		t.Fatal("adopted or wrote into the user's view folder")
	}
}

// A lost mkdir reply leaves the record pending; the next run recognises the
// directory by its mode and carries on.
func testLostMkdirReplyRecovers(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.log.lostMkdirReply = true
	if _, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("a.mov", "Proj/a.mov")}); err == nil {
		t.Fatal("the lost reply was not reported")
	}
	if r := g.rec.Own.Root; r == nil || !r.Pending {
		t.Fatalf("root record = %+v, want pending", r)
	}
	g.log.lostMkdirReply = false
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	assertView(t, fx, "Recents/Proj", map[string]string{"a.mov": "Proj/a.mov"})
	if in, _ := fx.FS().Lstat("Recents"); in.Perm != smartfolders.OwnedDirPerm || g.rec.Own.Root.Ino != in.Ino || g.rec.Own.Root.Pending {
		t.Fatalf("root = %+v, record %+v", in, g.rec.Own.Root)
	}
}

// A user replacing the managed link with a real file while a retarget is in
// flight never loses the file, whichever side of the last check it lands.
func testRetargetNeverDestroysAUserFile(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/b.mov")
	for _, when := range []string{"before the check", "after the check"} {
		t.Run(when, func(t *testing.T) {
			g := newRig(t, fx)
			if exists(fx, "Recents") {
				removeTree(t, fx, "Recents")
			}
			g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
			replace := func() {
				fx.Remove(t, "Recents/Proj/x.mov")
				fx.WriteFile(t, "Recents/Proj/x.mov") // the user's file
			}
			if when == "before the check" {
				g.rec.Gate = func(string) (func(), error) { replace(); return nil, nil }
			} else {
				g.log.beforeSwap = func(string, string) {
					g.log.beforeSwap = nil // once: the user acts once
					replace()
				}
			}
			res, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if in, err := fx.FS().Lstat("Recents/Proj/x.mov"); err != nil || in.Type != smartfolders.TypeRegular {
				t.Fatalf("user's file lost: %+v %v", in, err)
			}
			if names := fx.ReadDir(t, "Recents/Proj"); !reflect.DeepEqual(names, []string{"x.mov"}) {
				t.Fatalf("view holds %v (temp left behind or file moved)", names)
			}
			if _, ok := g.rec.Own.Views["Proj"].Links["x.mov"]; ok || len(res.Retargeted) != 0 || !reflect.DeepEqual(res.Foreign, []string{"x.mov"}) {
				t.Fatalf("res = %+v, links %+v", res, g.rec.Own.Views["Proj"].Links)
			}
			swaps := 0
			for _, ev := range g.events {
				if strings.HasPrefix(ev, "swap ") {
					swaps++
				}
			}
			// Caught by the re-check: no exchange at all. Caught after the
			// exchange: exchanged back.
			if want := map[string]int{"before the check": 0, "after the check": 2}[when]; swaps != want {
				t.Fatalf("%d exchanges, want %d: %v", swaps, want, g.events)
			}
		})
	}
}

// A user replacing the managed link with a real file while it is being
// removed never loses the file, whichever side of the last check it lands.
func testRemoveNeverDestroysUserFile(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/b.mov")
	for _, when := range []string{"before the check", "after the check"} {
		t.Run(when, func(t *testing.T) {
			g := newRig(t, fx)
			if exists(fx, "Recents") {
				removeTree(t, fx, "Recents")
			}
			g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"), link("keep.mov", "Proj/b.mov"))
			replace := func() {
				fx.Remove(t, "Recents/Proj/x.mov")
				fx.WriteFile(t, "Recents/Proj/x.mov") // the user's file
			}
			if when == "before the check" {
				g.rec.Gate = func(string) (func(), error) { replace(); return nil, nil }
			} else {
				// The user acts once, just after the last check, whatever the
				// reconciler does next to the link's name.
				once := func(p string) {
					if p == "Recents/Proj/x.mov" {
						g.log.beforePlace, g.log.beforeRemove = nil, nil
						replace()
					}
				}
				g.log.beforePlace = func(from, to string) { once(from) }
				g.log.beforeRemove = once
			}
			mark := len(g.events)
			res, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("keep.mov", "Proj/b.mov")})
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if in, err := fx.FS().Lstat("Recents/Proj/x.mov"); err != nil || in.Type != smartfolders.TypeRegular {
				t.Fatalf("user's file lost: %+v %v", in, err)
			}
			if names := fx.ReadDir(t, "Recents/Proj"); !reflect.DeepEqual(names, []string{"keep.mov", "x.mov"}) {
				t.Fatalf("view holds %v (temp left behind or file moved)", names)
			}
			if _, ok := g.rec.Own.Views["Proj"].Links["x.mov"]; ok || len(res.Removed) != 0 || !reflect.DeepEqual(res.Foreign, []string{"x.mov"}) {
				t.Fatalf("res = %+v, links %+v", res, g.rec.Own.Views["Proj"].Links)
			}
			if len(g.rec.Own.Views["Proj"].Temps) != 0 {
				t.Fatalf("temp records left: %+v", g.rec.Own.Views["Proj"].Temps)
			}
			moves, removes := 0, 0
			for _, ev := range g.events[mark:] {
				switch {
				case strings.HasPrefix(ev, "place "):
					moves++
				case strings.HasPrefix(ev, "remove ") && strings.HasSuffix(ev, "/x.mov"):
					removes++
				}
			}
			// Caught by the re-check: nothing moved. Caught after moving the
			// entry aside: moved back. Never an unlink.
			if want := map[string]int{"before the check": 0, "after the check": 2}[when]; moves != want || removes != 0 {
				t.Fatalf("%d moves (want %d), %d removes: %v", moves, want, removes, g.events[mark:])
			}
		})
	}
	// The normal path still removes, through a recorded temp name.
	g := newRig(t, fx)
	removeTree(t, fx, "Recents")
	g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
	res := g.reconcile(t, "Proj")
	if !reflect.DeepEqual(res.Removed, []string{"x.mov"}) || len(fx.ReadDir(t, "Recents/Proj")) != 0 || len(g.rec.Own.Views["Proj"].Temps) != 0 {
		t.Fatalf("plain remove: %+v, view %v", res, fx.ReadDir(t, "Recents/Proj"))
	}
}

// ---- crash and lost-reply recovery (review round 5) ----------------------

// crashed stands for the process dying inside an operation.
type crashed struct{}

// runCrashing runs one reconcile that is expected to die in a hook.
func (g *rig) runCrashing(t *testing.T, want ...smartfolders.Link) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(crashed); !ok {
				panic(p)
			}
		}
	}()
	_, _ = g.rec.Reconcile("Proj", want)
	t.Fatal("the run did not reach the crash point")
}

// restart is a new process: a reconciler over the same volume whose records
// are the last ones persisted (decoded from JSON, as the Manager loads them).
func (g *rig) restart(t *testing.T) *rig {
	t.Helper()
	n := newRig(t, g.fx)
	if len(g.persists) > 0 {
		own := &smartfolders.Ownership{}
		if err := json.Unmarshal([]byte(g.persists[len(g.persists)-1]), own); err != nil {
			t.Fatal(err)
		}
		n.rec.Own = own
	}
	return n
}

func userReplace(t *testing.T, fx Fixture) {
	t.Helper()
	fx.Remove(t, "Recents/Proj/x.mov")
	fx.WriteFile(t, "Recents/Proj/x.mov")
}

// assertUserFileAtName checks the user's file is back under its own name,
// alone in the view apart from `others`, with no temp record left.
func assertUserFileAtName(t *testing.T, fx Fixture, g *rig, others ...string) {
	t.Helper()
	if in, err := fx.FS().Lstat("Recents/Proj/x.mov"); err != nil || in.Type != smartfolders.TypeRegular {
		t.Fatalf("the user's file is not at x.mov: %+v %v; view %v", in, err, fx.ReadDir(t, "Recents/Proj"))
	}
	want := append([]string{"x.mov"}, others...)
	sort.Strings(want)
	if names := fx.ReadDir(t, "Recents/Proj"); !reflect.DeepEqual(names, want) {
		t.Fatalf("view holds %v, want %v", names, want)
	}
	if v := g.rec.Own.Views["Proj"]; v != nil && len(v.Temps) != 0 {
		t.Fatalf("temp records left: %+v", v.Temps)
	}
}

func isX(p string) bool { return strings.HasSuffix(p, "/x.mov") }

// A crash right after a new view's first link was placed persisted temps
// but no links map (omitempty); the restart must not panic.
func testCrashAfterFirstPlaceRestarts(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.log.afterPlace = func(string, string) error { panic(crashed{}) }
	g.runCrashing(t, link("x.mov", "Proj/a.mov"))
	g2 := g.restart(t)
	res := g2.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
	if !reflect.DeepEqual(res.Unchanged, []string{"x.mov"}) || res.Changed() {
		t.Fatalf("restart = %+v", res)
	}
	assertView(t, fx, "Recents/Proj", map[string]string{"x.mov": "Proj/a.mov"})
}

// Crash right after the move-aside of a removal, when a user's file had
// replaced the link just before it: the restart moves the file back.
func testRemoveCrashAfterMoveKeepsUserFile(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
	g.log.beforePlace = func(from, to string) {
		if isX(from) {
			userReplace(t, fx)
		}
	}
	g.log.afterPlace = func(from, to string) error {
		if isX(from) {
			panic(crashed{})
		}
		return nil
	}
	g.runCrashing(t)
	g2 := g.restart(t)
	res := g2.reconcile(t, "Proj")
	assertUserFileAtName(t, fx, g2)
	if !reflect.DeepEqual(res.Foreign, []string{"x.mov"}) {
		t.Fatalf("res = %+v", res)
	}
}

// Same, with the move-aside's reply lost instead of a crash.
func testRemoveLostReplyKeepsUserFile(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
	g.log.beforePlace = func(from, to string) {
		if isX(from) {
			userReplace(t, fx)
		}
	}
	g.log.afterPlace = func(from, to string) error {
		if isX(from) {
			return errors.New("connection reset (reply lost)")
		}
		return nil
	}
	if _, err := g.rec.Reconcile("Proj", nil); err == nil {
		t.Fatal("the lost reply was not reported")
	}
	g.log.beforePlace, g.log.afterPlace = nil, nil
	g.reconcile(t, "Proj")
	assertUserFileAtName(t, fx, g)
}

// A move-aside that happened but reported ENOENT (a client retry after a
// committed rename): the Manager's own link must not leak under a temp name.
func testRemoveLostReplyCleansOwnLink(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
	g.log.afterPlace = func(from, to string) error {
		if isX(from) {
			return syscall.ENOENT
		}
		return nil
	}
	_, _ = g.rec.Reconcile("Proj", nil)
	g.log.afterPlace = nil
	g.reconcile(t, "Proj")
	if names := fx.ReadDir(t, "Recents/Proj"); len(names) != 0 || len(g.rec.Own.Views["Proj"].Temps) != 0 {
		t.Fatalf("view holds %v, temps %+v", names, g.rec.Own.Views["Proj"].Temps)
	}
}

// Retarget: a user replaces the link just before the exchange and the
// exchange's reply is lost.
func testRetargetLostReplyKeepsUserFile(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/b.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
	g.log.beforeSwap = func(string, string) { userReplace(t, fx) }
	g.log.afterSwap = func(string, string) error { return errors.New("connection reset (reply lost)") }
	if _, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")}); err == nil {
		t.Fatal("the lost reply was not reported")
	}
	g.log.beforeSwap, g.log.afterSwap = nil, nil
	g.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
	assertUserFileAtName(t, fx, g)
}

// Same, with the process dying right after the exchange.
func testRetargetCrashAfterSwapKeepsUserFile(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/b.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
	g.log.beforeSwap = func(string, string) { userReplace(t, fx) }
	g.log.afterSwap = func(string, string) error { panic(crashed{}) }
	g.runCrashing(t, link("x.mov", "Proj/b.mov"))
	g2 := g.restart(t)
	g2.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
	assertUserFileAtName(t, fx, g2)
}

// Retarget, lost reply, no user: the Manager's new link is at the name and
// must stay the Manager's (not foreign forever).
func testRetargetLostReplyKeepsOwnLink(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Proj/b.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
	g.log.afterSwap = func(string, string) error { return errors.New("connection reset (reply lost)") }
	_, _ = g.rec.Reconcile("Proj", []smartfolders.Link{link("x.mov", "Proj/b.mov")})
	g.log.afterSwap = nil
	res := g.reconcile(t, "Proj", link("x.mov", "Proj/b.mov"))
	if !reflect.DeepEqual(res.Unchanged, []string{"x.mov"}) || len(res.Foreign)+len(res.Refused) != 0 {
		t.Fatalf("after the lost reply = %+v", res)
	}
	assertView(t, fx, "Recents/Proj", map[string]string{"x.mov": "Proj/b.mov"})
	g.reconcile(t, "Proj")
	if names := fx.ReadDir(t, "Recents/Proj"); len(names) != 0 {
		t.Fatalf("the Manager's own link was orphaned: %v", names)
	}
}

// A user's entry under a temp name whose original name has since been
// taken again is never forgotten: the record stays and the error names it.
func testTempEntryNeverForgotten(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("x.mov", "Proj/a.mov"))
	g.log.beforePlace = func(from, to string) {
		if isX(from) {
			userReplace(t, fx)
		}
	}
	g.log.afterPlace = func(from, to string) error {
		if isX(from) {
			return errors.New("connection reset (reply lost)")
		}
		return nil
	}
	_, _ = g.rec.Reconcile("Proj", nil)
	g.log.beforePlace, g.log.afterPlace = nil, nil
	fx.WriteFile(t, "Recents/Proj/x.mov") // the user made a new x.mov meanwhile
	var hidden string
	for name := range g.rec.Own.Views["Proj"].Temps {
		hidden = name
	}
	for i := 0; i < 2; i++ {
		_, err := g.rec.Reconcile("Proj", nil)
		if err == nil || hidden == "" || !strings.Contains(err.Error(), hidden) {
			t.Fatalf("run %d: err = %v, want it to name %q", i, err, hidden)
		}
		if _, ok := g.rec.Own.Views["Proj"].Temps[hidden]; !ok {
			t.Fatal("the temp record was dropped while a user's entry sits there")
		}
		if in, err := fx.FS().Lstat("Recents/Proj/" + hidden); err != nil || in.Type != smartfolders.TypeRegular {
			t.Fatalf("the user's entry under %s: %+v %v", hidden, in, err)
		}
	}
	// Once the name is free again, it goes back.
	fx.Remove(t, "Recents/Proj/x.mov")
	g.reconcile(t, "Proj")
	assertUserFileAtName(t, fx, g)
}

// When a user removes or replaces the root or a view, the records under it
// are dropped, but every hidden temp name they held is reported (it may
// hold a user's entry), never silently forgotten.
func testDroppedRecordsNameTheirTemps(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov", "Other/b.mov")
	stuck := func(g *rig, folder string) string {
		name := ".juicemount-smart-folder.tmp-stuck" + folder
		fx.WriteFile(t, "Recents/"+folder+"/"+name) // a user's entry, stuck
		g.rec.Own.Views[folder].Temps[name] = &smartfolders.OwnedTemp{For: "x.mov", Target: folder + "/a.mov", Text: "../../x", Ino: 1}
		return "Recents/" + folder + "/" + name
	}
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	g.reconcile(t, "Other", link("b.mov", "Other/b.mov"))

	// A view replaced by the user's own folder.
	hidden := stuck(g, "Proj")
	removeTree(t, fx, "Recents/Proj")
	fx.MkdirAll(t, "Spare1") // take the freed inode number (local filesystems reuse it)
	fx.MkdirAll(t, "Recents/Proj")
	if ino(t, fx, "Recents/Proj") == g.rec.Own.Views["Proj"].Ino {
		t.Skip("the replacement reused the inode number on this filesystem")
	}
	_, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("a.mov", "Proj/a.mov")})
	var d *smartfolders.DroppedTempsError
	if !errors.Is(err, smartfolders.ErrNotManaged) || !errors.As(err, &d) || !reflect.DeepEqual(d.Names, []string{hidden}) {
		t.Fatalf("replaced view: err = %v", err)
	}

	// The whole root renamed away (here: removed, then the user's own made).
	hidden = stuck(g, "Other")
	rootIno := g.rec.Own.Root.Ino
	removeTree(t, fx, "Recents")
	fx.MkdirAll(t, "Spare2")
	fx.MkdirAll(t, "Recents")
	if ino(t, fx, "Recents") == rootIno {
		t.Skip("the replacement reused the inode number on this filesystem")
	}
	st, err := g.rec.CheckRoot()
	if st != smartfolders.RootForeign || !errors.As(err, &d) || !reflect.DeepEqual(d.Names, []string{hidden}) {
		t.Fatalf("replaced root: %v, err = %v", st, err)
	}
	if g.rec.Own.Root != nil || len(g.rec.Own.Views) != 0 {
		t.Fatalf("records kept for a root that is not the Manager's: %+v", g.rec.Own)
	}
}

func removeTree(t *testing.T, fx Fixture, p string) {
	t.Helper()
	for _, n := range fx.ReadDir(t, p) {
		c := p + "/" + n
		if in, _ := fx.FS().Lstat(c); in.Type == smartfolders.TypeDir {
			removeTree(t, fx, c)
			continue
		}
		fx.Remove(t, c)
	}
	if err := removeDir(fx, p); err != nil {
		t.Fatal(err)
	}
}

func testMaxViews(t *testing.T, fx Fixture) {
	files(t, fx, "A/a.mov", "B/b.mov", "C/c.mov")
	g := newRig(t, fx)
	g.rec.MaxViews = 2
	g.reconcile(t, "A", link("a.mov", "A/a.mov"))
	g.reconcile(t, "B", link("b.mov", "B/b.mov"))
	if _, err := g.rec.Reconcile("C", []smartfolders.Link{link("c.mov", "C/c.mov")}); !errors.Is(err, smartfolders.ErrTooManyViews) {
		t.Fatalf("err = %v", err)
	}
	if exists(fx, "Recents/C") {
		t.Fatal("view beyond the limit created")
	}
	// Existing views keep working at the limit.
	g.reconcile(t, "A")
}

func testRootRemovedIsRecreated(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	fx.Remove(t, "Recents/Proj/a.mov")
	if err := errors.Join(removeDir(fx, "Recents/Proj"), removeDir(fx, "Recents")); err != nil {
		t.Fatalf("rmdir: %v", err)
	}
	if st, err := g.rec.CheckRoot(); st != smartfolders.RootAbsent || err != nil || g.rec.Own.Root != nil || len(g.rec.Own.Views) != 0 {
		t.Fatalf("CheckRoot = %v %v, own %+v", st, err, g.rec.Own)
	}
	res := g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	if !res.CreatedRoot || !res.CreatedView {
		t.Fatalf("res = %+v", res)
	}
}

func testGateRefusedWritesNothing(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	g := newRig(t, fx)
	g.failGate = errors.New("lease held by john@mac")
	if _, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("a.mov", "Proj/a.mov")}); err == nil {
		t.Fatal("reconcile ignored the gate")
	}
	if exists(fx, "Recents") || len(g.events) != 0 {
		t.Fatalf("wrote despite the refused gate: %v", g.events)
	}
}

func testSymlinkedParents(t *testing.T, fx Fixture) {
	files(t, fx, "Proj/a.mov")
	fx.MkdirAll(t, "Elsewhere")
	g := newRig(t, fx)
	g.reconcile(t, "Proj", link("a.mov", "Proj/a.mov"))
	// Replace the Manager's root with a symlink to another folder.
	fx.Remove(t, "Recents/Proj/a.mov")
	if err := errors.Join(removeDir(fx, "Recents/Proj"), removeDir(fx, "Recents")); err != nil {
		t.Fatalf("rmdir: %v", err)
	}
	fx.Symlink(t, "Elsewhere", "Recents")
	if _, err := g.rec.Reconcile("Proj", []smartfolders.Link{link("a.mov", "Proj/a.mov")}); !errors.Is(err, smartfolders.ErrNotManaged) {
		t.Fatalf("err = %v", err)
	}
	if len(fx.ReadDir(t, "Elsewhere")) != 0 {
		t.Fatal("wrote through a symlinked root")
	}
	// The FS itself refuses symlinked components.
	if _, err := fx.FS().Symlink("x", "Recents/Proj"); !errors.Is(err, smartfolders.ErrSymlinkInPath) {
		t.Fatalf("symlink through a symlinked parent: %v", err)
	}
	if _, err := fx.FS().Mkdir("Recents/X", 0o755); !errors.Is(err, smartfolders.ErrSymlinkInPath) {
		t.Fatalf("mkdir through symlink: %v", err)
	}
}
