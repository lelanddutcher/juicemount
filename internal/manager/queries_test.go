package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// newQueriesEnv is the collections environment with the query routes too.
func newQueriesEnv(t *testing.T) *collectionsEnv {
	t.Helper()
	e := newCollectionsEnv(t)
	e.mux.HandleFunc("/manager/api/queries", e.h.serveQueries)
	e.mux.HandleFunc("/manager/api/queries/", e.h.serveQueries)
	e.h.roots = func() []string { return []string{"Recents", "Collections", "Queries"} }
	return e
}

type queryReply struct {
	Query     QueryView `json:"query"`
	Operation struct {
		ID       string `json:"id"`
		Sequence int64  `json:"sequence"`
		Actor    string `json:"actor"`
	} `json:"operation"`
}

func (e *collectionsEnv) queryWrite(who, path string, body any, want int) queryReply {
	e.t.Helper()
	rec := e.do(who, http.MethodPost, path, body)
	if rec.Code != want {
		e.t.Fatalf("%s POST %s %v: %d %s, want %d", who, path, body, rec.Code, rec.Body, want)
	}
	var out queryReply
	if want == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			e.t.Fatal(err)
		}
	}
	return out
}

type resultsReply struct {
	Query       QueryView         `json:"query"`
	Results     []QueryResultView `json:"results"`
	EvaluatedAt time.Time         `json:"evaluated_at"`
	AgeSeconds  int64             `json:"age_seconds"`
	Unresolved  int               `json:"unresolved"`
	OverLimit   int               `json:"results_over_limit"`
	Capped      bool              `json:"capped"`
	Examined    int               `json:"examined"`
	Candidates  int               `json:"candidates"`
}

// expireResults ages the remembered evaluation past the one-minute reuse.
func (e *collectionsEnv) expireResults(id catalog.QueryID) {
	if r, ok := e.svc.results.get(id); ok {
		r.at = r.at.Add(-2 * queryResultsFresh)
		e.svc.results.put(id, r)
	}
}

func (e *collectionsEnv) results(who string, id catalog.QueryID) resultsReply {
	e.t.Helper()
	rec := e.do(who, http.MethodGet, "/api/queries/"+string(id)+"/results", nil)
	var out resultsReply
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
		e.t.Fatalf("results as %s: %d %s", who, rec.Code, rec.Body)
	}
	return out
}

func resultPaths(r resultsReply) []string {
	out := []string{}
	for _, x := range r.Results {
		out = append(out, x.Path)
	}
	return out
}

func TestQueriesOffIs404(t *testing.T) {
	a := &API{adminKey: collectionsAdminKey}
	mux := http.NewServeMux()
	a.registerCollections(mux, "/manager", nil)
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/manager/api/queries"},
		{http.MethodPost, "/manager/api/queries"},
		{http.MethodGet, "/manager/api/queries/qry_303132333435363738393a3b3c3d3e3f/results"},
	} {
		req := httptest.NewRequest(r.method, r.path, strings.NewReader(`{"name":"x","folders":["A"]}`))
		req.Header.Set("X-JuiceMount-Admin-Key", collectionsAdminKey)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s with continuity off: %d %s", r.method, r.path, rec.Code, rec.Body)
		}
	}
}

// Writes need grants covering every folder of the query (a parent of a
// granted project is not covered); reads, results included, are shared.
func TestQueriesWritesNeedGrantsOnEveryFolder(t *testing.T) {
	e := newQueriesEnv(t)
	for _, c := range []struct {
		who  string
		code int
	}{{"", 401}, {"admin", 200}, {"Bob", 200}, {"Pending", 403}, {"Nobody", 403}} {
		if rec := e.do(c.who, http.MethodGet, "/api/queries", nil); rec.Code != c.code {
			t.Errorf("list as %q: %d %s, want %d", c.who, rec.Code, rec.Body, c.code)
		}
	}
	created := e.queryWrite("Ann", "/api/queries", map[string]any{"name": "A movs", "folders": []string{"/Clients/A"}, "extensions": []string{".MOV"}}, 200)
	q := created.Query
	if q.Folders[0] != "Clients/A" || q.Extensions[0] != "mov" || q.Limit != queryDefaultLimit || created.Operation.Actor != e.ids["Ann"] {
		t.Fatalf("created: %+v", created)
	}
	library, _ := e.svc.store.LibraryID()
	if ev, err := e.svc.store.GetEvent(context.Background(), library, created.Operation.ID); err != nil || ev.Operation.Actor != e.ids["Ann"] {
		t.Fatalf("catalog actor: %+v %v", ev.Operation, err)
	}
	// Outside her grant, partly covered, or a parent of her project.
	for _, folders := range [][]string{{"Clients/B"}, {"Clients/A/sub", "Clients/B"}, {"Clients"}} {
		rec := e.do("Ann", http.MethodPost, "/api/queries", map[string]any{"name": "x", "folders": folders})
		if rec.Code != 403 || !strings.Contains(rec.Body.String(), "no project grant covers it") {
			t.Fatalf("Ann over %v: %d %s", folders, rec.Code, rec.Body)
		}
	}
	e.queryWrite("Ann", "/api/queries", map[string]any{"name": "sub", "folders": []string{"Clients/A/sub"}}, 200)
	// Bob's grant does not cover Ann's query: no rename, no delete. Reads
	// are shared.
	path := "/api/queries/" + string(q.ID)
	e.queryWrite("Bob", path+"/rename", map[string]any{"name": "Mine"}, 403)
	e.queryWrite("Bob", path+"/delete", map[string]any{}, 403)
	if rec := e.do("Bob", http.MethodGet, path, nil); rec.Code != 200 {
		t.Fatalf("Bob reads: %d %s", rec.Code, rec.Body)
	}
	if got := resultPaths(e.results("Bob", q.ID)); !reflect.DeepEqual(got, []string{"Clients/A/one.mov", "Clients/A/sub/four.mov", "Clients/A/two.mov"}) {
		t.Fatalf("Bob's results: %v", got)
	}
	// No write access writes nothing; access "all" and the admin write anywhere.
	e.queryWrite("Nia", "/api/queries", map[string]any{"name": "x", "folders": []string{"Clients/N"}}, 403)
	e.queryWrite("Al", "/api/queries", map[string]any{"name": "all", "folders": []string{"Clients"}}, 200)
	if r := e.queryWrite("admin", path+"/rename", map[string]any{"name": "By admin"}, 200); r.Query.Name != "By admin" || r.Operation.Actor != "admin" {
		t.Fatalf("admin rename: %+v", r)
	}
	if r := e.queryWrite("Ann", path+"/rename", map[string]any{"name": "Ann's"}, 200); r.Query.Name != "Ann's" {
		t.Fatalf("rename: %+v", r)
	}
	e.queryWrite("Ann", path+"/delete", map[string]any{}, 200)
	if rec := e.do("Ann", http.MethodGet, path, nil); rec.Code != 404 {
		t.Fatalf("deleted query: %d", rec.Code)
	}
	e.queryWrite("Ann", path+"/rename", map[string]any{"name": "again"}, 404)
	// No admin key while grants are in force: the open path reads only.
	e.a.adminKey = ""
	if rec := e.do("", http.MethodGet, "/api/queries", nil); rec.Code != 200 {
		t.Fatalf("open read: %d", rec.Code)
	}
	if rec := e.do("", http.MethodPost, "/api/queries", map[string]any{"name": "x", "folders": []string{"A"}}); rec.Code != 403 || !strings.Contains(rec.Body.String(), "admin key") {
		t.Fatalf("open write with grants in force: %d %s", rec.Code, rec.Body)
	}
}

func TestQueriesInputAndRetries(t *testing.T) {
	e := newQueriesEnv(t)
	for name, body := range map[string]map[string]any{
		"no name":          {"folders": []string{"A"}},
		"no folders":       {"name": "x"},
		"root":             {"name": "x", "folders": []string{"/"}},
		"dot dot":          {"name": "x", "folders": []string{"A/../B"}},
		"hidden":           {"name": "x", "folders": []string{"A/.trash"}},
		"smart root":       {"name": "x", "folders": []string{"recents/A"}},
		"query root":       {"name": "x", "folders": []string{"Queries"}},
		"bad extension":    {"name": "x", "folders": []string{"A"}, "extensions": []string{"m.ov"}},
		"slash in name":    {"name": "x", "folders": []string{"A"}, "name_contains": "a/b"},
		"limit over":       {"name": "x", "folders": []string{"A"}, "limit": 501},
		"negative within":  {"name": "x", "folders": []string{"A"}, "modified_within_seconds": -1},
		"unknown field":    {"name": "x", "folders": []string{"A"}, "rank": true},
		"bad operation id": {"name": "x", "folders": []string{"A"}, "operation_id": "a b"},
	} {
		if rec := e.do("admin", http.MethodPost, "/api/queries", body); rec.Code != 400 {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body)
		}
	}
	// Folders and extensions are normalized: each once, sorted.
	body := map[string]any{"name": "Takes", "folders": []string{"B/", "/A", "A"}, "extensions": []string{"MOV", "mov", ".braw"},
		"name_contains": "Take", "modified_within_seconds": 3600, "limit": 5, "operation_id": "take-1"}
	first := e.queryWrite("Ann", "/api/queries", map[string]any{"name": "x", "folders": []string{"Clients/A"}, "operation_id": "same"}, 200)
	if !strings.HasPrefix(first.Operation.ID, "qry-") {
		t.Fatalf("operation id: %s", first.Operation.ID)
	}
	q := e.queryWrite("admin", "/api/queries", body, 200)
	want := QueryView{ID: q.Query.ID, Name: "Takes", Folders: []string{"A", "B"}, NameContains: "Take", Extensions: []string{"braw", "mov"},
		ModifiedWithinSeconds: 3600, Limit: 5, CreatedSequence: q.Operation.Sequence, UpdatedSequence: q.Operation.Sequence}
	if !reflect.DeepEqual(q.Query, want) {
		t.Fatalf("created %+v\nwant %+v", q.Query, want)
	}
	// The same request under the same operation_id is the same query; a
	// different one is 409.
	if again := e.queryWrite("admin", "/api/queries", body, 200); again.Query.ID != q.Query.ID || again.Operation.Sequence != q.Operation.Sequence {
		t.Fatalf("retry: %+v", again)
	}
	body["name"] = "Other"
	e.queryWrite("admin", "/api/queries", body, 409)
	// The same key from another actor is another operation.
	if other := e.queryWrite("Ann", "/api/queries", map[string]any{"name": "x", "folders": []string{"Clients/A"}, "operation_id": "same"}, 200); other.Query.ID != first.Query.ID {
		t.Fatalf("Ann's retry named another query")
	}
	if byAdmin := e.queryWrite("admin", "/api/queries", map[string]any{"name": "x", "folders": []string{"Clients/A"}, "operation_id": "same"}, 200); byAdmin.Query.ID == first.Query.ID {
		t.Fatal("the admin's operation_id named Ann's query")
	}
	// A definition never changes.
	e.queryWrite("admin", "/api/queries/"+string(q.Query.ID)+"/rename", map[string]any{"name": "y", "folders": []string{"C"}}, 400)
	if rec := e.do("admin", http.MethodDelete, "/api/queries/"+string(q.Query.ID), nil); rec.Code != 200 {
		t.Fatalf("DELETE: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do("admin", http.MethodGet, "/api/queries?after=qry_x", nil); rec.Code != 400 {
		t.Fatalf("bad cursor: %d", rec.Code)
	}
}

// Results come from the volume: a stale hint (a folder renamed with no
// event per file) resolves to the live path; a file gone drops out; an
// unreadable volume makes candidates unresolved, not gone.
func TestQueriesResultsResolveLive(t *testing.T) {
	e := newQueriesEnv(t)
	q := e.queryWrite("admin", "/api/queries", map[string]any{"name": "A", "folders": []string{"Clients"}}, 200).Query
	if got := resultPaths(e.results("Ann", q.ID)); !reflect.DeepEqual(got, []string{"Clients/A/one.mov", "Clients/A/sub/four.mov", "Clients/A/two.mov", "Clients/B/three.mov"}) {
		t.Fatalf("results: %v", got)
	}
	// Within a minute the results API answers from the last evaluation.
	e.vol.set(func(f map[string]uint64) { delete(f, "Clients/B/three.mov") })
	cached := e.results("Ann", q.ID)
	if got := resultPaths(cached); len(got) != 4 || cached.AgeSeconds > 5 || cached.EvaluatedAt.IsZero() {
		t.Fatalf("cached results: %+v", cached)
	}
	e.vol.set(func(f map[string]uint64) { f["Clients/B/three.mov"] = 44 })
	e.vol.set(func(f map[string]uint64) {
		f["Clients/C/sub/four.mov"] = f["Clients/A/sub/four.mov"]
		delete(f, "Clients/A/sub/four.mov")
		delete(f, "Clients/A/two.mov")
	})
	e.expireResults(q.ID)
	r := e.results("Ann", q.ID)
	if got := resultPaths(r); !reflect.DeepEqual(got, []string{"Clients/A/one.mov", "Clients/B/three.mov", "Clients/C/sub/four.mov"}) {
		t.Fatalf("results after the move: %v", got)
	}
	e.vol.setFail(true)
	e.expireResults(q.ID)
	if r := e.results("Ann", q.ID); len(r.Results) != 0 || r.Unresolved != 4 {
		t.Fatalf("results while the volume fails: %+v", r)
	}
	e.vol.setFail(false)
	if rec := e.do("Ann", http.MethodGet, "/api/queries/qry_303132333435363738393a3b3c3d3e3f/results", nil); rec.Code != 404 {
		t.Fatalf("results of a missing query: %d", rec.Code)
	}
}

// At most queryMaxPerPerson queries per person (the admin is exempt) and
// catalog.MaxQueries per library: 409 beyond. A folder in a smart root is
// refused only while the Manager owns that root. Capped results say so.
func TestQueriesLimitsRootsAndCaps(t *testing.T) {
	e := newQueriesEnv(t)
	for i := 0; i < queryMaxPerPerson; i++ {
		e.queryWrite("Ann", "/api/queries", map[string]any{"name": fmt.Sprintf("q%d", i), "folders": []string{"Clients/A"}}, 200)
	}
	rec := e.do("Ann", http.MethodPost, "/api/queries", map[string]any{"name": "one more", "folders": []string{"Clients/A"}})
	if rec.Code != 409 || !strings.Contains(rec.Body.String(), "32 saved queries") {
		t.Fatalf("over the per-person limit: %d %s", rec.Code, rec.Body)
	}
	e.queryWrite("Al", "/api/queries", map[string]any{"name": "Al's", "folders": []string{"Clients"}}, 200)
	first := e.queryWrite("admin", "/api/queries", map[string]any{"name": "admin", "folders": []string{"Clients"}}, 200).Query
	// Fill the library to its limit, then even the admin gets 409.
	library, _ := e.svc.store.LibraryID()
	var changes []catalog.Change
	for i := queryMaxPerPerson + 2; i < catalog.MaxQueries; i++ {
		changes = append(changes, catalog.Change{CreateQuery: &catalog.CreateQuery{QueryID: qryID(1000 + i), Name: "x",
			Definition: catalog.QueryDefinition{Prefixes: []string{"X"}, Limit: 1}}})
	}
	if _, err := e.svc.store.Apply(context.Background(), catalog.Operation{Version: catalog.OperationVersion, LibraryID: library,
		ID: "fill", Actor: "admin", Changes: changes}); err != nil {
		t.Fatal(err)
	}
	if rec := e.do("admin", http.MethodPost, "/api/queries", map[string]any{"name": "x", "folders": []string{"A"}}); rec.Code != 409 || !strings.Contains(rec.Body.String(), "at most 256") {
		t.Fatalf("over the library limit: %d %s", rec.Code, rec.Body)
	}
	e.queryWrite("admin", "/api/queries/"+string(first.ID)+"/delete", map[string]any{}, 200)

	// The refusal names the folder quoted; control characters are refused.
	rec = e.do("Bob", http.MethodPost, "/api/queries", map[string]any{"name": "x", "folders": []string{"Clients/A"}})
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), `\"/Clients/A\"`) {
		t.Fatalf("refusal text: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do("admin", http.MethodPost, "/api/queries", map[string]any{"name": "x", "folders": []string{"A\tB"}}); rec.Code != 400 {
		t.Fatalf("control character: %d %s", rec.Code, rec.Body)
	}
	// A user-made Recents (not the Manager's) is ordinary data.
	e.h.roots = func() []string { return []string{"Collections"} }
	e.queryWrite("admin", "/api/queries/"+string(first.ID)+"/rename", map[string]any{"name": "y"}, 404)
	recents := e.queryWrite("admin", "/api/queries", map[string]any{"name": "user recents", "folders": []string{"Recents/A"}}, 200).Query
	e.queryWrite("admin", "/api/queries/"+string(recents.ID)+"/delete", map[string]any{}, 200)
	if rec := e.do("admin", http.MethodPost, "/api/queries", map[string]any{"name": "x", "folders": []string{"collections/A"}}); rec.Code != 400 {
		t.Fatalf("the Manager's collections root: %d %s", rec.Code, rec.Body)
	}

	// Capped: more candidates than examined.
	saved := queryCandidateCap
	queryCandidateCap = 2
	defer func() { queryCandidateCap = saved }()
	capped := e.queryWrite("admin", "/api/queries", map[string]any{"name": "capped", "folders": []string{"Clients"}}, 200).Query
	if r := e.results("Bob", capped.ID); !r.Capped || r.Examined != 2 || r.Candidates != 4 || len(r.Results) != 2 {
		t.Fatalf("capped results: %+v", r)
	}
}
