package manager

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// An encoded slash makes an empty path segment. Such a path is refused
// (404) before any slot decision, so it can neither skip the volume-read
// slot (a results or members page while every slot is busy) nor the
// per-person count (concurrent creates past the limit).
func TestCatalogAPINonCanonicalPathsRefused(t *testing.T) {
	saved := collectionBusyWait
	collectionBusyWait = 50 * time.Millisecond
	defer func() { collectionBusyWait = saved }()
	e := newQueriesEnv(t)
	q := e.queryWrite("admin", "/api/queries", map[string]any{"name": "A", "folders": []string{"Clients"}}, 200).Query
	col := e.write("admin", "/api/collections", map[string]any{"name": "C", "members": pathRefs("Clients/A/one.mov")}, 200).Collection
	for i := 0; i < cap(e.h.slots); i++ {
		e.h.slots <- struct{}{}
	}
	for _, p := range []string{
		"/api/queries/" + string(q.ID) + "/results%2F%2F",
		"/api/queries/" + string(q.ID) + "/%2Fresults",
		"/api/collections/" + string(col.ID) + "/members%2F%2F",
	} {
		if rec := e.do("Ann", http.MethodGet, p, nil); rec.Code != http.StatusNotFound {
			t.Errorf("GET %s: %d %s", p, rec.Code, rec.Body)
		}
	}
	// The canonical forms still wait for a slot (503 here: all are busy).
	for _, p := range []string{"/api/queries/" + string(q.ID) + "/results", "/api/queries/" + string(q.ID) + "/results/",
		"/api/collections/" + string(col.ID) + "/members"} {
		if rec := e.do("Ann", http.MethodGet, p, nil); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("GET %s with every slot busy: %d", p, rec.Code)
		}
	}
	for i := 0; i < cap(e.h.slots); i++ {
		<-e.h.slots
	}

	for i := 0; i < queryMaxPerPerson-1; i++ {
		e.queryWrite("Ann", "/api/queries", map[string]any{"name": fmt.Sprintf("q%d", i), "folders": []string{"Clients/A"}}, 200)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e.do("Ann", http.MethodPost, "/api/queries/%2F", map[string]any{"name": fmt.Sprintf("race%d", i), "folders": []string{"Clients/A"}})
		}(i)
	}
	wg.Wait()
	if n := e.queriesBy("Ann"); n != queryMaxPerPerson-1 {
		t.Fatalf("Ann has %d queries after creates on a non-canonical path", n)
	}
}

func (e *collectionsEnv) queriesBy(name string) int {
	e.t.Helper()
	library, _ := e.svc.store.LibraryID()
	qs, err := e.svc.activeQueries(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	n := 0
	for _, q := range qs {
		evs, err := e.svc.store.Events(context.Background(), library, q.CreatedSequence-1, 1)
		if err == nil && len(evs) == 1 && evs[0].Operation.Actor == e.ids[name] {
			n++
		}
	}
	return n
}

// The per-person count holds by itself: concurrent creates routed without
// the slot (straight to the router) still stop at the limit.
func TestQueriesPerPersonLimitHoldsWithoutTheSlot(t *testing.T) {
	e := newQueriesEnv(t)
	for i := 0; i < queryMaxPerPerson-1; i++ {
		e.queryWrite("Ann", "/api/queries", map[string]any{"name": fmt.Sprintf("q%d", i), "folders": []string{"Clients/A"}}, 200)
	}
	mac := e.macs["Ann"]
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/manager/api/queries", strings.NewReader(fmt.Sprintf(`{"name":"race%d","folders":["Clients/A"]}`, i)))
			req.Header.Set("X-JuiceMount-Mac-Instance", mac[0])
			req.Header.Set("X-JuiceMount-Mac-Token", mac[1])
			req.Header.Set("X-JuiceMount-Mac-Device", mac[2])
			actor, err := e.h.authenticate(req)
			if err != nil {
				t.Error(err)
				return
			}
			_ = e.h.routeQueries(httptest.NewRecorder(), req, e.svc, actor, []string{})
		}(i)
	}
	wg.Wait()
	if n := e.queriesBy("Ann"); n != queryMaxPerPerson {
		t.Fatalf("Ann has %d queries, limit %d", n, queryMaxPerPerson)
	}
}

func TestRouteParts(t *testing.T) {
	for p, want := range map[string][]string{
		"/m/api/queries": {}, "/m/api/queries/": {}, "/m/api/queries/a": {"a"}, "/m/api/queries/a/results/": {"a", "results"},
		"/m/api/queries//": nil, "/m/api/queries/a//": nil, "/m/api/queries/a/./b": nil, "/m/api/queries/../x": nil, "/m/api/queriesx": nil,
	} {
		got, ok := routeParts(p, "/m/api/queries")
		if ok != (want != nil) || (ok && !reflect.DeepEqual(got, want)) {
			t.Errorf("%s: %v %v", p, got, ok)
		}
	}
}

// A new catalog is format 3 (the Manager before saved queries opens it);
// the first query created through the API makes it format 4.
func TestQueriesNewCatalogFormat3UntilFirstQuery(t *testing.T) {
	e := newQueriesEnv(t)
	if f := e.svc.store.Format(); f != 3 {
		t.Fatalf("new catalog format %d", f)
	}
	// A refused create leaves it at format 3.
	e.queryWrite("Bob", "/api/queries", map[string]any{"name": "x", "folders": []string{"Clients/A"}}, 403)
	if f := e.svc.store.Format(); f != 3 {
		t.Fatalf("after a refused create: format %d", f)
	}
	e.queryWrite("Ann", "/api/queries", map[string]any{"name": "x", "folders": []string{"Clients/A"}}, 200)
	if f := e.svc.store.Format(); f != 4 {
		t.Fatalf("after the first query: format %d", f)
	}
}

// Recency marks go only to files under some query's folder (NFC), and the
// folders follow query changes.
func TestQueriesTouchOnlyUnderQueryFolders(t *testing.T) {
	e := newQueriesEnv(t)
	if e.svc.wantsTouch("Clients/A/one.mov") {
		t.Fatal("touch wanted with no query")
	}
	nfd := "Clients/Café"
	q := e.queryWrite("admin", "/api/queries", map[string]any{"name": "x", "folders": []string{nfd}}, 200).Query
	if q.Folders[0] != "Clients/Café" {
		t.Fatalf("folder stored as %q", q.Folders[0])
	}
	for p, want := range map[string]bool{nfd + "/a.mov": true, "Clients/Café/b.mov": true, "Clients/A/one.mov": false} {
		if got := e.svc.wantsTouch(p); got != want {
			t.Errorf("%s: %v", p, got)
		}
	}
	if !inQueryFolders(catalog.QueryDefinition{Prefixes: []string{"Clients/Café"}}, nfd+"/a.mov", nil) {
		t.Fatal("a decomposed path is not in its composed folder")
	}
	e.queryWrite("admin", "/api/queries/"+string(q.ID)+"/delete", map[string]any{}, 200)
	if e.svc.wantsTouch(nfd + "/a.mov") {
		t.Fatal("touch still wanted after the query was deleted")
	}
}

// A grant and a path name the same folder in either Unicode form (a grant
// copied from a macOS listing is often decomposed): queries, collections
// and find compare them composed. The grant is kept as it was given.
func TestGrantsCoverEitherUnicodeForm(t *testing.T) {
	e := newAgentEnv(t)
	cafeNFC, cafeNFD := "Clients/Caf\u00e9", "Clients/Cafe\u0301"
	bebeNFC, bebeNFD := "Clients/B\u00e9b\u00e9", "Clients/Be\u0301be\u0301"
	e.vol.set(func(f map[string]uint64) {
		f[cafeNFC+"/cut.mov"] = 60
		f[bebeNFD+"/take.mov"] = 61
	})
	e.offer(binder.Event{Op: "create", Path: cafeNFC + "/cut.mov", Inode: 60}, binder.Event{Op: "create", Path: bebeNFD + "/take.mov", Inode: 61})
	for _, g := range []string{"/" + cafeNFD, "/" + bebeNFC} {
		if err := e.people.AddGrant(e.ids["Bob"], g); err != nil {
			t.Fatal(err)
		}
	}
	for _, folder := range []string{cafeNFC, cafeNFD, bebeNFD} {
		e.queryWrite("Bob", "/api/queries", map[string]any{"name": "q " + folder, "folders": []string{folder}}, 200)
	}
	e.write("Bob", "/api/collections", map[string]any{"name": "C", "members": pathRefs(cafeNFC+"/cut.mov", bebeNFD+"/take.mov")}, 200)
	found := e.find("Bob", url.Values{"name": {".mov"}})
	var paths []string
	for _, f := range found.Files {
		paths = append(paths, f.Path)
	}
	if len(paths) != 3 { // and Clients/B/three.mov
		t.Fatalf("Bob finds %q", paths)
	}
	// Still not a grant over another folder.
	e.queryWrite("Bob", "/api/queries", map[string]any{"name": "a", "folders": []string{"Clients/A"}}, 403)
	for _, p := range e.people.PeopleSnapshot().People {
		if p.ID == e.ids["Bob"] && !reflect.DeepEqual(p.Grants, []string{"/Clients/B", "/" + bebeNFC, "/" + cafeNFD}) {
			t.Fatalf("grants stored as %q", p.Grants)
		}
	}
}
