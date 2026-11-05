package manager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
)

var discoveryBase = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

// newDiscoveryEnv adds 20 clips and a note in A, 10 sounds in B and a file
// under the Queries root to the collections volume; clip i is modified i
// hours after discoveryBase.
func newDiscoveryEnv(t *testing.T) *collectionsEnv {
	t.Helper()
	e := newAgentEnv(t)
	var evs []binder.Event
	add := func(p string, ino uint64, mtime time.Time) {
		e.vol.set(func(f map[string]uint64) { f[p] = ino })
		e.vol.mu.Lock()
		if e.vol.mtime == nil {
			e.vol.mtime = map[uint64]time.Time{}
		}
		e.vol.mtime[ino] = mtime
		e.vol.mu.Unlock()
		evs = append(evs, binder.Event{Op: "create", Path: p, Inode: ino})
	}
	for i := 0; i < 20; i++ {
		add(fmt.Sprintf("Clients/A/clip%02d.mov", i), uint64(100+i), discoveryBase.Add(time.Duration(i)*time.Hour))
	}
	add("Clients/A/notes.txt", 200, discoveryBase)
	for i := 0; i < 10; i++ {
		add(fmt.Sprintf("Clients/B/sound%02d.wav", i), uint64(300+i), discoveryBase)
	}
	add("Queries/Q/clip.mov", 400, discoveryBase)
	e.offer(evs...)
	return e
}

type findReply struct {
	Files      []FileView `json:"files"`
	Next       string     `json:"next"`
	Scanned    int        `json:"scanned"`
	Resolved   int        `json:"resolved"`
	Unresolved int        `json:"unresolved"`
	Budget     bool       `json:"budget_exhausted"`
}

func (e *collectionsEnv) find(who string, q url.Values) findReply {
	e.t.Helper()
	rec := e.do(who, http.MethodGet, "/api/continuity/find?"+q.Encode(), nil)
	var out findReply
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
		e.t.Fatalf("%.8s find %v: %d %s", who, q, rec.Code, rec.Body)
	}
	return out
}

// findAll pages to the end and returns the paths found, and the pages.
func (e *collectionsEnv) findAll(who string, q url.Values) ([]string, int) {
	e.t.Helper()
	var paths []string
	seen := map[continuity.AssetID]bool{}
	pages := 0
	for cursor := ""; ; {
		qq := url.Values{}
		for k, v := range q {
			qq[k] = v
		}
		if cursor != "" {
			qq.Set("cursor", cursor)
		}
		r := e.find(who, qq)
		pages++
		for _, f := range r.Files {
			if seen[f.AssetID] {
				e.t.Fatalf("%s found twice", f.Path)
			}
			seen[f.AssetID] = true
			paths = append(paths, f.Path)
		}
		if r.Next == "" {
			break
		}
		if r.Next == cursor {
			e.t.Fatalf("cursor did not move: %s", cursor)
		}
		cursor = r.Next
		if pages > 200 {
			e.t.Fatal("no end")
		}
	}
	sort.Strings(paths)
	return paths, pages
}

func allUnder(paths []string, prefix string) bool {
	for _, p := range paths {
		if !strings.HasPrefix(p, prefix) {
			return false
		}
	}
	return true
}

func TestFindPagingIsStable(t *testing.T) {
	e := newDiscoveryEnv(t)
	all, pages := e.findAll("admin", url.Values{"limit": {"7"}})
	// 4 + 20 + 1 + 10, and not the file under the Queries root (a last
	// page may be empty: the page before it ended at the limit).
	if len(all) != 35 || pages < 5 || pages > 6 {
		t.Fatalf("%d files in %d pages: %v", len(all), pages, all)
	}
	whole, _ := e.findAll("admin", url.Values{"limit": {"200"}})
	if strings.Join(whole, ",") != strings.Join(all, ",") {
		t.Fatal("one page and seven-file pages differ")
	}
	// The same cursor gives the same page.
	first := e.find("admin", url.Values{"limit": {"7"}})
	a := e.find("admin", url.Values{"limit": {"7"}, "cursor": {first.Next}})
	b := e.find("admin", url.Values{"limit": {"7"}, "cursor": {first.Next}})
	if len(a.Files) != 7 || a.Next != b.Next || a.Files[0].AssetID != b.Files[0].AssetID {
		t.Fatalf("repeat page: %+v vs %+v", a, b)
	}
	// A file moved while paging keeps its place in the order (its hint).
	e.vol.set(func(f map[string]uint64) {
		delete(f, "Clients/A/clip05.mov")
		f["Clients/A/sub/clip05.mov"] = 105
	})
	e.offer(binder.Event{Op: "rename", Path: "Clients/A/sub/clip05.mov", OldPath: "Clients/A/clip05.mov", Inode: 105})
	moved, _ := e.findAll("admin", url.Values{"limit": {"7"}})
	if len(moved) != 35 || !strings.Contains(strings.Join(moved, ","), "/Clients/A/sub/clip05.mov") {
		t.Fatalf("after a move: %v", moved)
	}
	// One result in full.
	r := e.find("admin", url.Values{"name": {"clip07"}})
	f := r.Files[0]
	if len(r.Files) != 1 || f.Name != "clip07.mov" || f.Label != "clip07.mov" || f.Path != "/Clients/A/clip07.mov" || f.PathHint != "Clients/A/clip07.mov" ||
		f.Size != 1070 || !f.Modified.Equal(discoveryBase.Add(7*time.Hour)) || f.AssetID != binder.AssetID(e.vol.VolumeUUID(), 107) {
		t.Fatalf("clip07: %+v", r)
	}
	for _, bad := range []url.Values{{"limit": {"0"}}, {"limit": {"201"}}, {"cursor": {"x"}}, {"name": {"a/b"}},
		{"ext": {"m.ov"}}, {"kind": {"spreadsheet"}}, {"modified_after": {"yesterday"}}, {"seen_after": {"-1"}}, {"folder": {"../x"}}} {
		if rec := e.do("admin", http.MethodGet, "/api/continuity/find?"+bad.Encode(), nil); rec.Code != 400 {
			t.Errorf("%v: %d %s", bad, rec.Code, rec.Body)
		}
	}
	if rec := e.do("admin", http.MethodGet, "/api/continuity/find?collection=col_00000000000000000000000000000009", nil); rec.Code != 404 {
		t.Errorf("unknown collection: %d", rec.Code)
	}
}

// The budgets cut a page short, with a cursor that continues it; paging to
// the end still finds every file once.
func TestFindBudget(t *testing.T) {
	e := newDiscoveryEnv(t)
	savedR, savedS := findResolveBudget, findScanBudget
	defer func() { findResolveBudget, findScanBudget = savedR, savedS }()
	findResolveBudget = 3
	r := e.find("admin", url.Values{"limit": {"50"}})
	if len(r.Files) != 3 || r.Resolved != 3 || !r.Budget || r.Next == "" {
		t.Fatalf("resolve budget: %+v", r)
	}
	all, pages := e.findAll("admin", url.Values{"limit": {"50"}})
	if len(all) != 35 || pages != 12 {
		t.Fatalf("with the resolve budget: %d files in %d pages", len(all), pages)
	}
	// A filter that matches nothing reads the catalog only, up to the scan
	// budget per request.
	findResolveBudget, findScanBudget = savedR, 4
	r = e.find("admin", url.Values{"name": {"nothing-like-this"}})
	if len(r.Files) != 0 || r.Scanned != 4 || r.Resolved != 0 || !r.Budget || r.Next == "" {
		t.Fatalf("scan budget: %+v", r)
	}
	// 36 hints, 4 a page.
	if none, pages := e.findAll("admin", url.Values{"name": {"nothing-like-this"}}); len(none) != 0 || pages != 9 {
		t.Fatalf("empty scan: %v in %d pages", none, pages)
	}
}

// A person (and their agent) finds only what their grants cover; by ID
// another project's file does not exist for them.
func TestFindFollowsGrants(t *testing.T) {
	e := newDiscoveryEnv(t)
	tok := e.mint("Ann", map[string]any{"name": "agent"}).Token
	ann, _ := e.findAll("Ann", url.Values{})
	agent, _ := e.findAll(tok, url.Values{})
	bob, _ := e.findAll("Bob", url.Values{})
	al, _ := e.findAll("Al", url.Values{})
	nia, _ := e.findAll("Nia", url.Values{})
	if len(ann) != 24 || !allUnder(ann, "/Clients/A/") || strings.Join(agent, ",") != strings.Join(ann, ",") {
		t.Fatalf("Ann %d %v, agent %d", len(ann), ann, len(agent))
	}
	if len(bob) != 11 || !allUnder(bob, "/Clients/B/") || len(al) != 35 || len(nia) != 0 {
		t.Fatalf("Bob %d, Al %d, Nia %d", len(bob), len(al), len(nia))
	}
	// A file Ann's grant covers once moved into A is found at its new path.
	e.vol.set(func(f map[string]uint64) {
		delete(f, "Clients/B/sound03.wav")
		f["Clients/A/sound03.wav"] = 303
	})
	e.offer(binder.Event{Op: "rename", Path: "Clients/A/sound03.wav", OldPath: "Clients/B/sound03.wav", Inode: 303})
	if got := e.find(tok, url.Values{"name": {"sound"}}); len(got.Files) != 1 || got.Files[0].Path != "/Clients/A/sound03.wav" {
		t.Fatalf("moved into A: %+v", got)
	}
	one := string(binder.AssetID(e.vol.VolumeUUID(), 42))
	three := string(binder.AssetID(e.vol.VolumeUUID(), 44))
	col := e.write("admin", "/api/collections", map[string]any{"name": "Selects", "members": pathRefs("Clients/A/one.mov")}, 200).Collection
	for _, c := range []struct {
		who, id string
		code    int
	}{
		{tok, one, 200}, {"Ann", one, 200}, {tok, three, 404}, {"Bob", three, 200}, {"Bob", one, 404}, {"admin", three, 200},
		{"Nia", one, 404}, {tok, "asset_00000000000000000000000000000001", 404}, {tok, "nope", 404},
	} {
		rec := e.do(c.who, http.MethodGet, "/api/continuity/asset/"+c.id, nil)
		if rec.Code != c.code {
			t.Errorf("%.8s asset %s: %d %s, want %d", c.who, c.id, rec.Code, rec.Body, c.code)
		}
		if c.code == 200 && c.id == one {
			var v FileView
			if json.Unmarshal(rec.Body.Bytes(), &v) != nil || v.Path != "/Clients/A/one.mov" || len(v.Collections) != 1 || v.Collections[0].ID != col.ID || v.Collections[0].Name != "Selects" {
				t.Errorf("asset view: %s", rec.Body)
			}
		}
	}
	// A file gone from the volume is not found.
	e.vol.set(func(f map[string]uint64) { delete(f, "Clients/A/one.mov") })
	if rec := e.do(tok, http.MethodGet, "/api/continuity/asset/"+one, nil); rec.Code != 404 {
		t.Fatalf("gone file: %d %s", rec.Code, rec.Body)
	}
}

func TestFindFilters(t *testing.T) {
	e := newDiscoveryEnv(t)
	count := func(q url.Values) int {
		t.Helper()
		got, _ := e.findAll("admin", q)
		return len(got)
	}
	col := string(e.write("admin", "/api/collections", map[string]any{"name": "Two", "members": pathRefs("Clients/A/clip01.mov", "Clients/B/sound01.wav")}, 200).Collection.ID)
	cases := []struct {
		q    url.Values
		want int
	}{
		{url.Values{"name": {"CLIP1"}}, 10},
		{url.Values{"name": {"clip"}, "folder": {"/Clients/A/sub"}}, 0},
		{url.Values{"folder": {"Clients/A/sub/"}}, 1},
		{url.Values{"folder": {"Clients/B", "Clients/A/sub"}}, 12},
		{url.Values{"ext": {".WAV"}}, 10},
		{url.Values{"kind": {"audio"}}, 10},
		{url.Values{"kind": {"video"}, "ext": {"txt"}}, 25},
		{url.Values{"kind": {"document"}}, 1},
		{url.Values{"collection": {col}}, 2},
		{url.Values{"collection": {col}, "kind": {"audio"}}, 1},
		{url.Values{"modified_after": {discoveryBase.Add(15 * time.Hour).Format(time.RFC3339)}}, 4},
		{url.Values{"modified_after": {fmt.Sprint(discoveryBase.Add(18 * time.Hour).UnixMilli())}}, 1},
	}
	for _, c := range cases {
		if got := count(c.q); got != c.want {
			t.Errorf("%v: %d, want %d", c.q, got, c.want)
		}
	}
	// seen_after: only files whose hint moved since a catalog sequence.
	var last int64
	evs, err := e.svc.store.Events(t.Context(), mustLibrary(t, e), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs {
		last = max(last, ev.Receipt.Sequence)
	}
	e.vol.set(func(f map[string]uint64) {
		delete(f, "Clients/A/clip02.mov")
		f["Clients/A/clip02-final.mov"] = 102
	})
	e.offer(binder.Event{Op: "rename", Path: "Clients/A/clip02-final.mov", OldPath: "Clients/A/clip02.mov", Inode: 102})
	got, _ := e.findAll("admin", url.Values{"seen_after": {fmt.Sprint(last)}})
	if len(got) != 1 || got[0] != "/Clients/A/clip02-final.mov" {
		t.Fatalf("seen_after %d: %v", last, got)
	}
}

func mustLibrary(t *testing.T, e *collectionsEnv) continuity.LibraryID {
	t.Helper()
	l, err := e.svc.store.LibraryID()
	if err != nil {
		t.Fatal(err)
	}
	return l
}
