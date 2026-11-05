package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

func qryID(n int) catalog.QueryID { return catalog.QueryID(fmt.Sprintf("qry_%032x", n)) }

func (e *collViewsEnv) createQuery(id catalog.QueryID, name string, def catalog.QueryDefinition) {
	e.t.Helper()
	if def.Limit == 0 {
		def.Limit = queryDefaultLimit
	}
	e.apply(catalog.Change{CreateQuery: &catalog.CreateQuery{QueryID: id, Name: name, Definition: def}})
}

func (e *collViewsEnv) touch(p string, at time.Time) {
	e.t.Helper()
	if err := os.Chtimes(filepath.Join(e.root, filepath.FromSlash(p)), at, at); err != nil {
		e.t.Fatal(err)
	}
}

func (e *collViewsEnv) qpass() { e.svc.queriesOnlyCycle(context.Background()) }

func (e *collViewsEnv) wantQueries(want map[string]string) {
	e.t.Helper()
	if got := e.treeOf("Queries"); !reflect.DeepEqual(got, want) {
		e.t.Fatalf("query views =\n%v\nwant\n%v\n(log %v)", got, want, e.svc.lastLog)
	}
}

// run evaluates a query directly, as the results API does.
func (e *collViewsEnv) run(def catalog.QueryDefinition, now time.Time) queryResult {
	e.t.Helper()
	if def.Limit == 0 {
		def.Limit = queryDefaultLimit
	}
	res, err := e.cont.runQuery(context.Background(), qryID(99), def, []string{"Recents", "Collections", "Queries"}, now)
	if err != nil {
		e.t.Fatal(err)
	}
	return res
}

func hitPaths(r queryResult) []string {
	out := []string{}
	for _, h := range r.Hits {
		out = append(out, h.Path)
	}
	return out
}

// Evaluation: folders, extensions, name, modified-within, limit, and a
// stable order (newest first, then by path), over live paths only.
func TestQueryEvaluation(t *testing.T) {
	files := []string{"Proj/A/x1.mov", "Proj/A/Take2.MOV", "Proj/A/notes.txt", "Proj/A/same1.mov", "Proj/A/same2.mov",
		"Proj/A/sub/w.braw", "Proj/B/y.mov", "Other/z.mov", "Projects/p.mov"}
	e := newCollViewsEnv(t, files...)
	now := time.Now().Truncate(time.Second)
	for p, age := range map[string]time.Duration{"Proj/A/x1.mov": time.Hour, "Proj/A/Take2.MOV": 2 * time.Hour, "Proj/A/notes.txt": 3 * time.Hour,
		"Proj/A/same1.mov": 5 * time.Hour, "Proj/A/same2.mov": 5 * time.Hour, "Proj/A/sub/w.braw": 48 * time.Hour, "Proj/B/y.mov": 10 * time.Minute,
		"Other/z.mov": 0, "Projects/p.mov": 0} {
		e.touch(p, now.Add(-age))
	}
	for _, c := range []struct {
		name string
		def  catalog.QueryDefinition
		want []string
		over int
	}{
		{"folder", catalog.QueryDefinition{Prefixes: []string{"Proj/A"}},
			[]string{"Proj/A/x1.mov", "Proj/A/Take2.MOV", "Proj/A/notes.txt", "Proj/A/same1.mov", "Proj/A/same2.mov", "Proj/A/sub/w.braw"}, 0},
		{"a folder is not a name prefix", catalog.QueryDefinition{Prefixes: []string{"Proj"}, Extensions: []string{"mov"}},
			[]string{"Proj/B/y.mov", "Proj/A/x1.mov", "Proj/A/Take2.MOV", "Proj/A/same1.mov", "Proj/A/same2.mov"}, 0},
		{"extensions", catalog.QueryDefinition{Prefixes: []string{"Proj/A"}, Extensions: []string{"braw", "txt"}},
			[]string{"Proj/A/notes.txt", "Proj/A/sub/w.braw"}, 0},
		{"name, ignoring case", catalog.QueryDefinition{Prefixes: []string{"Proj/A"}, NameContains: "TAKE"}, []string{"Proj/A/Take2.MOV"}, 0},
		{"modified within", catalog.QueryDefinition{Prefixes: []string{"Proj/A"}, ModifiedWithinSeconds: 4 * 3600},
			[]string{"Proj/A/x1.mov", "Proj/A/Take2.MOV", "Proj/A/notes.txt"}, 0},
		{"limit", catalog.QueryDefinition{Prefixes: []string{"Proj/A"}, Limit: 2}, []string{"Proj/A/x1.mov", "Proj/A/Take2.MOV"}, 4},
		{"several folders", catalog.QueryDefinition{Prefixes: []string{"Proj/A/sub", "Proj/B"}}, []string{"Proj/B/y.mov", "Proj/A/sub/w.braw"}, 0},
	} {
		res := e.run(c.def, now)
		if got := hitPaths(res); !reflect.DeepEqual(got, c.want) || res.Over != c.over {
			t.Errorf("%s: %v (over %d), want %v (over %d)", c.name, got, res.Over, c.want, c.over)
		}
	}
	// The order is stable: ties by path, every run.
	for i := 0; i < 3; i++ {
		if got := hitPaths(e.run(catalog.QueryDefinition{Prefixes: []string{"Proj/A"}, NameContains: "same"}, now)); !reflect.DeepEqual(got, []string{"Proj/A/same1.mov", "Proj/A/same2.mov"}) {
			t.Fatalf("tie order: %v", got)
		}
	}

	// The folder is renamed with one event for the folder: the hints stay
	// stale and every result is found at its live path.
	e.move("Proj/A", "Proj/C")
	e.cont.binder.Offer(binder.Event{Op: "rename", Path: "Proj/C", OldPath: "Proj/A", IsDir: true})
	if got := hitPaths(e.run(catalog.QueryDefinition{Prefixes: []string{"Proj"}, ModifiedWithinSeconds: 3 * 3600 / 2}, now)); !reflect.DeepEqual(got, []string{"Proj/B/y.mov", "Proj/C/x1.mov"}) {
		t.Fatalf("after the folder rename: %v", got)
	}
	// A file moved out of the folder or deleted drops out, stale hint or not.
	if got := hitPaths(e.run(catalog.QueryDefinition{Prefixes: []string{"Proj/A"}}, now)); len(got) != 0 {
		t.Fatalf("the old folder still answers: %v", got)
	}
	// Residual (documented): a hint still names the old folder, so the new
	// folder alone finds nothing until the files' own events arrive.
	if got := hitPaths(e.run(catalog.QueryDefinition{Prefixes: []string{"Proj/C"}}, now)); len(got) != 0 {
		t.Fatalf("stale hints under the new folder: %v", got)
	}
	e.bind("Proj/C/x1.mov")
	if got := hitPaths(e.run(catalog.QueryDefinition{Prefixes: []string{"Proj/C"}}, now)); !reflect.DeepEqual(got, []string{"Proj/C/x1.mov"}) {
		t.Fatalf("after the file's own event: %v", got)
	}
	if err := os.Remove(filepath.Join(e.root, "Proj/B/y.mov")); err != nil {
		t.Fatal(err)
	}
	if got := hitPaths(e.run(catalog.QueryDefinition{Prefixes: []string{"Proj/B"}}, now)); len(got) != 0 {
		t.Fatalf("a deleted file: %v", got)
	}
}

// Smart roots are never in a query's results; candidates beyond the cap
// keep the most recently recorded hints.
func TestQueryCandidatesBounds(t *testing.T) {
	def := catalog.QueryDefinition{Prefixes: []string{"Queries", "P"}, Limit: 1}
	for p, want := range map[string]bool{"P/a.mov": true, "P": false, "PX/a.mov": false, "Queries/v/a.mov": false, "queries/a.mov": false} {
		if got := inQueryFolders(def, p, []string{"Recents", "Queries"}); got != want {
			t.Errorf("%s: %v", p, got)
		}
	}
	var c queryCandidates
	for i, seq := range []int64{5, 1, 9, 3, 7} {
		c.add(binder.Hint{AssetID: binder.AssetID(osVolumeUUID, uint64(i+1)), Ino: uint64(i + 1), Sequence: seq}, 3)
	}
	var seqs []int64
	for _, h := range c.ordered() {
		seqs = append(seqs, h.Sequence)
	}
	if !reflect.DeepEqual(seqs, []int64{9, 7, 5}) || c.capped != 2 {
		t.Fatalf("kept %v, capped %d", seqs, c.capped)
	}
}

// The lifecycle of a query view: created with the results, newest first;
// a new file (bound by the binder) and a folder rename follow; a renamed
// query's view is renamed, a deleted one's removed.
func TestQueryViewsFollowQueries(t *testing.T) {
	e := newCollViewsEnv(t, "Proj/A/a.mov", "Proj/A/b.mov", "Proj/A/c.txt")
	now := e.clk.now()
	e.touch("Proj/A/a.mov", now.Add(-2*time.Hour))
	e.touch("Proj/A/b.mov", now.Add(-time.Hour))
	e.svc.qry.debounce = time.Hour // passes are driven by the test
	e.attach()
	e.createQuery(qryID(1), "Movs", catalog.QueryDefinition{Prefixes: []string{"Proj"}, Extensions: []string{"mov"}})
	e.qpass()
	e.wantQueries(map[string]string{"Movs/a.mov": "Proj/A/a.mov", "Movs/b.mov": "Proj/A/b.mov"})
	if st := e.svc.queryStats(); st == nil || st.Queries != 1 || st.Views != 1 || st.Links != 2 {
		t.Fatalf("stats = %+v", st)
	}

	// A new file: the binder's catalog change marks the view dirty.
	writeVolumeFile(t, e.root, "Proj/A/d.mov")
	e.bind("Proj/A/d.mov")
	e.svc.qry.mu.Lock()
	dirty := e.svc.qry.dirty[qryID(1)]
	e.svc.qry.mu.Unlock()
	if !dirty {
		t.Fatal("a bound file did not mark the query dirty")
	}
	// Changes under its folders rebuild a view at most once a minute: the
	// view stays dirty until then.
	e.qpass()
	e.wantQueries(map[string]string{"Movs/a.mov": "Proj/A/a.mov", "Movs/b.mov": "Proj/A/b.mov"})
	e.svc.qry.mu.Lock()
	dirty = e.svc.qry.dirty[qryID(1)]
	e.svc.qry.mu.Unlock()
	if !dirty {
		t.Fatal("a deferred view is no longer dirty")
	}
	e.clk.add(queryViewsMinInterval)
	e.qpass()
	e.wantQueries(map[string]string{"Movs/a.mov": "Proj/A/a.mov", "Movs/b.mov": "Proj/A/b.mov", "Movs/d.mov": "Proj/A/d.mov"})

	// The folder is renamed (one event, for the folder): the links follow
	// the files' live paths.
	e.move("Proj/A", "Proj/Z")
	ev := smartfolders.Event{Op: "rename", Path: "Proj/Z", OldPath: "Proj/A", IsDir: true}
	e.cont.binder.Offer(binder.Event{Op: ev.Op, Path: ev.Path, OldPath: ev.OldPath, IsDir: true})
	e.svc.noteQueryEvent(ev)
	e.clk.add(queryViewsMinInterval)
	e.qpass()
	e.wantQueries(map[string]string{"Movs/a.mov": "Proj/Z/a.mov", "Movs/b.mov": "Proj/Z/b.mov", "Movs/d.mov": "Proj/Z/d.mov"})

	// An event outside the query's folders marks nothing.
	e.svc.noteQueryEvent(smartfolders.Event{Op: "create", Path: "Other/x.mov"})
	e.svc.qry.mu.Lock()
	n := len(e.svc.qry.dirty)
	e.svc.qry.mu.Unlock()
	if n != 0 {
		t.Fatalf("dirty after an unrelated event: %v", e.svc.qry.dirty)
	}

	// Renamed, then deleted.
	e.apply(catalog.Change{RenameQuery: &catalog.RenameQuery{QueryID: qryID(1), Name: "Clips"}})
	e.qpass()
	e.wantQueries(map[string]string{"Clips/a.mov": "Proj/Z/a.mov", "Clips/b.mov": "Proj/Z/b.mov", "Clips/d.mov": "Proj/Z/d.mov"})
	e.apply(catalog.Change{RemoveQuery: &catalog.RemoveQuery{QueryID: qryID(1)}})
	e.qpass()
	e.wantQueries(map[string]string{})
	if len(e.svc.qry.own.Views) != 0 {
		t.Fatalf("records left: %+v", e.svc.qry.own.Views)
	}
}

// Query views and the other roots: /Recents leaves the Manager's query
// root out, a query never links into the collections root while its views
// run, and smart-folders.json goes to version 4 and reloads.
func TestQueryViewsRootsAndState(t *testing.T) {
	e := newCollViewsEnv(t, "Proj/a.mov")
	e.svc.qry.debounce = time.Hour
	e.svc.coll.debounce = time.Hour
	e.attach()
	e.create(collID(1), "Shoot", "Proj/a.mov")
	e.createQuery(qryID(1), "All", catalog.QueryDefinition{Prefixes: []string{"Proj"}})
	e.pass()
	e.qpass()
	e.wantQueries(map[string]string{"All/a.mov": "Proj/a.mov"})
	e.want(map[string]string{"Shoot/a.mov": "Proj/a.mov"})
	// A query over the collections root (made directly in the catalog: the
	// API refuses one) finds nothing there while collection views run.
	writeVolumeFile(t, e.root, "Collections/Shoot/user.mov")
	e.bind("Collections/Shoot/user.mov")
	e.createQuery(qryID(2), "Coll", catalog.QueryDefinition{Prefixes: []string{"Collections"}})
	e.qpass()
	e.wantQueries(map[string]string{"All/a.mov": "Proj/a.mov"})
	e.apply(catalog.Change{RemoveQuery: &catalog.RemoveQuery{QueryID: qryID(2)}})
	e.qpass()
	writeVolumeFile(t, e.root, "Queries/All/notes.txt")
	e.svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Queries/All/notes.txt"})
	if got := e.svc.tracker.Folders(); len(got) != 0 {
		t.Fatalf("/Recents tracks %v", got)
	}

	data, err := os.ReadFile(e.svc.cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	var v int
	_ = json.Unmarshal(raw["version"], &v)
	var qs queryViewsState
	if err := json.Unmarshal(raw["queries"], &qs); v != 4 || err != nil || raw["collections"] == nil || qs.Folders[qryID(1)] != "All" || qs.Ownership.Views["All"] == nil {
		t.Fatalf("state version %d, queries %s (%v)", v, raw["queries"], err)
	}
	svc2 := newTestSmartFolders(t, svcOpts{root: e.root, state: e.svc.cfg.StatePath, leaser: &fakeLeaser{}, clock: e.clk})
	defer svc2.cfg.Redis.Close()
	if !reflect.DeepEqual(svc2.qry.own, e.svc.qry.own) || svc2.qry.folders[qryID(1)] != "All" || !reflect.DeepEqual(svc2.coll.own, e.svc.coll.own) {
		t.Fatal("query records not reloaded")
	}
}

// Either switch off creates nothing: no continuity (nothing attached), or
// an unusable root. The root clashes are the proxy's (case folding).
func TestQueryViewsOffCreatesNothing(t *testing.T) {
	e := newCollViewsEnv(t, "Proj/a.mov")
	e.createQuery(qryID(1), "All", catalog.QueryDefinition{Prefixes: []string{"Proj"}})
	e.svc.cycle(context.Background())
	e.qpass()
	e.wantQueries(map[string]string{})
	if e.svc.qry.state() != nil {
		t.Fatal("records written while off")
	}
	e.cont.queriesChanged() // no-op
	for _, c := range []struct {
		env  map[string]string
		root string
	}{
		{map[string]string{}, "Queries"},
		{map[string]string{"JM_SMART_FOLDERS_QUERIES_ROOT": " Finds "}, "Finds"},
		{map[string]string{"JM_SMART_FOLDERS_QUERIES_ROOT": "recents"}, ""},
		{map[string]string{"JM_SMART_FOLDERS_COLLECTIONS_ROOT": "Sets", "JM_SMART_FOLDERS_QUERIES_ROOT": "SETS"}, ""},
		{map[string]string{"JM_SMART_FOLDERS_QUERIES_ROOT": ".q"}, ""},
	} {
		c.env["JM_SMART_FOLDERS"] = "on"
		on, set, err := smartFoldersEnv(envMap(c.env))
		if err != nil || !on || set.QueriesRoot != c.root || (c.root == "") != (set.QueriesOff != "") || set.Root != "Recents" {
			t.Errorf("%v: %+v %v", c.env, set, err)
		}
	}
	e.svc.cfg.Settings.QueriesRoot = ""
	if e.svc.attachQueries(e.cont) || e.svc.qry.source() != nil {
		t.Fatal("attached with no usable root")
	}
}

// countingQuerySource records the catalog scans, the single-file re-checks
// and can act once a run finishes (before its view is linked).
type countingQuerySource struct {
	*continuityService
	scans    [][]catalog.QueryID
	locates  int
	ops      []int
	finished func()
}

func (c *countingQuerySource) scanQueries(ctx context.Context, defs map[catalog.QueryID]catalog.QueryDefinition, exclude []string) (map[catalog.QueryID]*queryCandidates, error) {
	var ids []catalog.QueryID
	for id := range defs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	c.scans = append(c.scans, ids)
	return c.continuityService.scanQueries(ctx, defs, exclude)
}

func (c *countingQuerySource) advanceQuery(ctx context.Context, r *queryRun, exclude []string, maxOps int, until time.Time) (int, error) {
	n, err := c.continuityService.advanceQuery(ctx, r, exclude, maxOps, until)
	c.ops = append(c.ops, n)
	if err == nil && r.done() && c.finished != nil {
		c.finished()
		c.finished = nil
	}
	return n, err
}

func (c *countingQuerySource) locateHit(ctx context.Context, def catalog.QueryDefinition, h binder.Hint, exclude []string, now time.Time) (queryHit, bool, error) {
	c.locates++
	return c.continuityService.locateHit(ctx, def, h, exclude, now)
}

// A rebuild stops at the pass budget and resumes where it stopped; one
// catalog scan serves the pass's jobs and is reused until a change under a
// query's folders (that query only) or the next sweep; a file gone between
// the run and its link is looked up again alone; capped queries are named.
func TestQueryViewsBudgetScansAndCaps(t *testing.T) {
	files := []string{"P/1.mov", "P/2.mov", "P/3.mov", "P/4.mov", "P/5.mov", "P/6.mov", "O/x.mov"}
	e := newCollViewsEnv(t, files...)
	e.svc.qry.debounce = time.Hour
	e.svc.qry.passOps = 4
	e.attach()
	cs := &countingQuerySource{continuityService: e.cont}
	e.svc.qry.src.Store(&sourceRef[querySource]{cs})
	e.createQuery(qryID(1), "P", catalog.QueryDefinition{Prefixes: []string{"P"}})
	e.createQuery(qryID(2), "O", catalog.QueryDefinition{Prefixes: []string{"O"}})
	both := []catalog.QueryID{qryID(1), qryID(2)}

	e.qpass()
	e.wantQueries(map[string]string{}) // P stopped at the budget; O waits
	if !reflect.DeepEqual(cs.scans, [][]catalog.QueryID{both}) || !reflect.DeepEqual(cs.ops, []int{4}) {
		t.Fatalf("first pass: scans %v, ops %v", cs.scans, cs.ops)
	}
	e.qpass()
	all := map[string]string{"O/x.mov": "O/x.mov"}
	for _, f := range files[:6] {
		all["P/"+f[2:]] = f
	}
	e.wantQueries(all)
	// O first (the pass resumes after the job it stopped at), then P's rest.
	if len(cs.scans) != 1 || !reflect.DeepEqual(cs.ops, []int{4, 1, 2}) {
		t.Fatalf("second pass: scans %v, ops %v", cs.scans, cs.ops)
	}

	// A change under P: P alone is scanned again (after the minimum
	// interval). A file found gone when linking is looked up alone.
	e.svc.noteQueryEvent(smartfolders.Event{Op: "create", Path: "P/7.mov"})
	cs.finished = func() { e.move("P/1.mov", "P/sub/1.mov") }
	e.clk.add(queryViewsMinInterval)
	e.qpass()
	e.qpass()
	all["P/1.mov"] = "P/sub/1.mov"
	e.wantQueries(all)
	if !reflect.DeepEqual(cs.scans[1:], [][]catalog.QueryID{{qryID(1)}}) || cs.locates != 1 {
		t.Fatalf("after a change: scans %v, single re-checks %d", cs.scans, cs.locates)
	}

	// A sweep scans every query again; with a cap of 3 candidates, P is
	// named as capped (3 of 6 examined) and keeps the 3 most recent.
	saved := queryCandidateCap
	queryCandidateCap = 3
	defer func() { queryCandidateCap = saved }()
	e.clk.add(collectionViewsSweep)
	for i := 0; i < 3; i++ {
		e.qpass()
	}
	if !reflect.DeepEqual(cs.scans[2], both) {
		t.Fatalf("sweep scans: %v", cs.scans)
	}
	st := e.svc.queryStats()
	if len(st.CappedQueries) != 1 || st.CappedQueries[0] != (cappedView{ID: string(qryID(1)), Examined: 3, Candidates: 6}) {
		t.Fatalf("capped = %+v", st.CappedQueries)
	}
}

// The candidates kept under the cap are the most recently changed: a file
// saved in place (an update event) moves ahead of newer, untouched ones.
func TestQueryCapKeepsRecentlySaved(t *testing.T) {
	e := newCollViewsEnv(t, "P/old.mov", "P/a.mov", "P/b.mov")
	saved := queryCandidateCap
	queryCandidateCap = 2
	defer func() { queryCandidateCap = saved }()
	def := catalog.QueryDefinition{Prefixes: []string{"P"}}
	if got := hitPaths(e.run(def, time.Now())); len(got) != 2 || slices.Contains(got, "P/old.mov") {
		t.Fatalf("before the save: %v", got)
	}
	e.touch("P/old.mov", time.Now())
	before := e.cont.binder.Stats().Handled
	e.cont.binder.Offer(binder.Event{Op: "update", Path: "P/old.mov", Inode: e.ino("P/old.mov")})
	for deadline := time.Now().Add(10 * time.Second); e.cont.binder.Stats().Handled <= before; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("update not handled")
		}
	}
	res := e.run(def, time.Now())
	if got := hitPaths(res); len(got) != 2 || got[0] != "P/old.mov" || res.Capped != 1 || res.Examined != 2 {
		t.Fatalf("after the save: %v (capped %d, examined %d)", got, res.Capped, res.Examined)
	}
}

// A name stored decomposed (as macOS writes it) matches a composed
// fragment, and the reverse.
func TestQueryNameMatchesUnicodeForms(t *testing.T) {
	nfd, nfc := "Café take.mov", "café"
	if !queryNameMatches(catalog.QueryDefinition{NameContains: nfc}, "P/"+nfd) ||
		!queryNameMatches(catalog.QueryDefinition{NameContains: "Café"}, "P/café.mov") {
		t.Fatal("normalization forms do not match")
	}
}
