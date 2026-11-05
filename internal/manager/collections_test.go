package manager

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
)

// lockedVolume is a fake volume (path -> inode) the test changes while the
// binder reads it. fail makes every read an I/O error.
type lockedVolume struct {
	mu    sync.Mutex
	files map[string]uint64
	fail  bool
}

func (v *lockedVolume) VolumeUUID() string { return "00000000-0000-4000-8000-0000000000d3" }
func (v *lockedVolume) node(ino uint64) jfsmeta.Node {
	return jfsmeta.Node{Ino: ino, Type: smartfolders.TypeRegular, Nlink: 1, Parent: 1}
}
func (v *lockedVolume) LookupIno(p string) (jfsmeta.Node, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.fail {
		return jfsmeta.Node{}, syscall.EIO
	}
	if ino, ok := v.files[p]; ok {
		return v.node(ino), nil
	}
	return jfsmeta.Node{}, syscall.ENOENT
}
func (v *lockedVolume) StatIno(ino uint64) (jfsmeta.Node, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.fail {
		return jfsmeta.Node{}, syscall.EIO
	}
	for _, i := range v.files {
		if i == ino {
			return v.node(ino), nil
		}
	}
	return jfsmeta.Node{}, syscall.ENOENT
}
func (v *lockedVolume) GetPaths(ino uint64) ([]string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.fail {
		return nil, syscall.EIO
	}
	var out []string
	for p, i := range v.files {
		if i == ino {
			out = append(out, p)
		}
	}
	return out, nil
}
func (v *lockedVolume) Close() error { return nil }
func (v *lockedVolume) set(fn func(map[string]uint64)) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fn(v.files)
}
func (v *lockedVolume) setFail(on bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.fail = on
}

// collectionsEnv: a binder over a fake volume, people with Macs, and the
// collections routes.
type collectionsEnv struct {
	t      *testing.T
	a      *API
	mux    *http.ServeMux
	svc    *continuityService
	vol    *lockedVolume
	people *PrincipalStore
	macs   map[string][2]string // person name -> instance ID, poll token
	ids    map[string]string    // person name -> person ID
}

const collectionsAdminKey = "test-admin-key"

func newCollectionsEnv(t *testing.T) *collectionsEnv {
	t.Helper()
	cfg := continuityTestConfig(t)
	store, err := openContinuityCatalog(filepath.Dir(cfg.StateFile))
	if err != nil {
		t.Fatal(err)
	}
	vol := &lockedVolume{files: map[string]uint64{
		"Clients/A/one.mov": 42, "Clients/A/two.mov": 43, "Clients/B/three.mov": 44, "Clients/A/sub/four.mov": 45,
	}}
	b, err := binder.New(binder.Config{Store: store, Connect: func() (binder.Volume, error) { return vol, nil }})
	if err != nil {
		t.Fatal(err)
	}
	svc := &continuityService{binder: b, store: store}
	svc.start()
	t.Cleanup(svc.Stop)
	for p, ino := range vol.files {
		b.Offer(binder.Event{Op: "create", Path: p, Inode: ino})
	}
	deadline := time.Now().Add(10 * time.Second)
	for b.Stats().Applied < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("events not applied: %+v", b.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	proxyDir := t.TempDir()
	people, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(t.TempDir(), "principals.json"), ProxyDir: proxyDir})
	if err != nil {
		t.Fatal(err)
	}
	startRealProxyReloader(t, proxyDir)
	env := &collectionsEnv{t: t, svc: svc, vol: vol, people: people, macs: map[string][2]string{}, ids: map[string]string{}}
	for i, spec := range []struct{ name, grant string }{
		{"Ann", "/Clients/A"}, {"Bob", "/Clients/B"}, {"Al", ""}, {"Nia", "/Clients/N"}, {"Pending", ""}, {"Nobody", ""},
	} {
		instance := fmt.Sprintf("%08X-7B3D-4C1A-9E2B-0A1B2C3D4E5F", 0x5A000000+i)
		resp, err := people.Enroll(EnrollRequest{InstanceID: instance, Host: "bay", Secret: testMacSecret}, "192.0.2.7")
		if err != nil {
			t.Fatal(err)
		}
		env.macs[spec.name] = [2]string{instance, resp.PollToken}
		if spec.name == "Pending" {
			continue
		}
		if err := people.Approve(macPrincipalName(instance)); err != nil {
			t.Fatal(err)
		}
		if spec.name == "Nobody" {
			continue
		}
		p, err := people.CreatePerson(spec.name)
		if err != nil {
			t.Fatal(err)
		}
		env.ids[spec.name] = p.ID
		if err := people.AssignDevice(macPrincipalName(instance), p.ID); err != nil {
			t.Fatal(err)
		}
		if spec.grant != "" {
			if err := people.AddGrant(p.ID, spec.grant); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Nia keeps access "granted" with no grant: no write access at all.
	if err := people.RemoveGrant(env.ids["Nia"], "/Clients/N"); err != nil {
		t.Fatal(err)
	}
	env.a = &API{adminKey: collectionsAdminKey, continuity: svc}
	env.mux = http.NewServeMux()
	env.a.registerCollections(env.mux, "/manager", people)
	return env
}

// do sends a request as who: "admin", "" (no credentials) or a person's Mac.
func (e *collectionsEnv) do(who, method, path string, body any) *httptest.ResponseRecorder {
	e.t.Helper()
	var data []byte
	if body != nil {
		var err error
		if data, err = json.Marshal(body); err != nil {
			e.t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, "/manager"+path, bytes.NewReader(data))
	switch who {
	case "":
	case "admin":
		req.Header.Set("X-JuiceMount-Admin-Key", collectionsAdminKey)
	default:
		mac, ok := e.macs[who]
		if !ok {
			e.t.Fatalf("no Mac for %s", who)
		}
		req.Header.Set("X-JuiceMount-Mac-Instance", mac[0])
		req.Header.Set("X-JuiceMount-Mac-Token", mac[1])
	}
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec
}

type writeReply struct {
	Collection CollectionView `json:"collection"`
	Operation  struct {
		ID       string `json:"id"`
		Sequence int64  `json:"sequence"`
		Actor    string `json:"actor"`
	} `json:"operation"`
}

func (e *collectionsEnv) write(who, path string, body any, want int) writeReply {
	e.t.Helper()
	rec := e.do(who, http.MethodPost, path, body)
	if rec.Code != want {
		e.t.Fatalf("%s POST %s %v: %d %s, want %d", who, path, body, rec.Code, rec.Body, want)
	}
	var out writeReply
	if want == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			e.t.Fatal(err)
		}
	}
	return out
}

type membersReply struct {
	Collection CollectionView `json:"collection"`
	Members    []MemberView   `json:"members"`
	Next       string         `json:"next"`
}

func (e *collectionsEnv) members(id catalog.CollectionID, query string) membersReply {
	e.t.Helper()
	rec := e.do("admin", http.MethodGet, "/api/collections/"+string(id)+"/members"+query, nil)
	var out membersReply
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
		e.t.Fatalf("members: %d %s", rec.Code, rec.Body)
	}
	return out
}

func memberPaths(r membersReply) map[string]string {
	out := map[string]string{}
	for _, m := range r.Members {
		out[m.Label] = m.State + ":" + m.Path
	}
	return out
}

func pathRefs(paths ...string) []map[string]string {
	var out []map[string]string
	for _, p := range paths {
		out = append(out, map[string]string{"path": p})
	}
	return out
}

func TestCollectionsOffIs404(t *testing.T) {
	a := &API{adminKey: collectionsAdminKey}
	mux := http.NewServeMux()
	a.registerCollections(mux, "/manager", nil)
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/manager/api/collections"},
		{http.MethodPost, "/manager/api/collections"},
		{http.MethodGet, "/manager/api/collections/col_303132333435363738393a3b3c3d3e3f/members"},
	} {
		req := httptest.NewRequest(r.method, r.path, strings.NewReader(`{"name":"x"}`))
		req.Header.Set("X-JuiceMount-Admin-Key", collectionsAdminKey)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s with continuity off: %d %s", r.method, r.path, rec.Code, rec.Body)
		}
	}
}

func TestCollectionsAuthentication(t *testing.T) {
	e := newCollectionsEnv(t)
	for _, c := range []struct {
		who  string
		code int
	}{{"", 401}, {"admin", 200}, {"Ann", 200}, {"Nia", 200}, {"Pending", 403}, {"Nobody", 403}} {
		if rec := e.do(c.who, http.MethodGet, "/api/collections", nil); rec.Code != c.code {
			t.Errorf("list as %q: %d %s, want %d", c.who, rec.Code, rec.Body, c.code)
		}
	}
	bad := e.macs["Ann"]
	bad[1] = strings.Repeat("0", 64)
	e.macs["Forged"] = bad
	if rec := e.do("Forged", http.MethodGet, "/api/collections", nil); rec.Code != 401 {
		t.Errorf("wrong token: %d", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/manager/api/collections", nil)
	req.Header.Set("X-JuiceMount-Admin-Key", "wrong")
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Errorf("wrong admin key: %d", rec.Code)
	}
	// A revoked Mac is refused even with its old token.
	if err := e.people.Revoke(macPrincipalName(e.macs["Bob"][0])); err != nil {
		t.Fatal(err)
	}
	if rec := e.do("Bob", http.MethodGet, "/api/collections", nil); rec.Code != 403 {
		t.Errorf("revoked Mac: %d %s", rec.Code, rec.Body)
	}
}

func TestCollectionsWritesNeedGrantsOnEveryMember(t *testing.T) {
	e := newCollectionsEnv(t)
	ctx := context.Background()
	library, _ := e.svc.store.LibraryID()
	created := e.write("Ann", "/api/collections", map[string]any{"name": "Selects", "members": pathRefs("Clients/A/one.mov", "Clients/A/sub/four.mov")}, 200)
	id := created.Collection.ID
	if created.Collection.Members != 2 || created.Operation.Actor != e.ids["Ann"] {
		t.Fatalf("created: %+v", created)
	}
	event, err := e.svc.store.GetEvent(ctx, library, created.Operation.ID)
	if err != nil || event.Operation.Actor != e.ids["Ann"] {
		t.Fatalf("catalog actor: %+v %v", event.Operation, err)
	}
	add := "/api/collections/" + string(id) + "/members/add"
	// Outside her grant, alone or in a partly covered set: refused, unchanged.
	e.write("Ann", add, map[string]any{"members": pathRefs("Clients/B/three.mov")}, 403)
	rec := e.do("Ann", http.MethodPost, add, map[string]any{"members": pathRefs("Clients/A/two.mov", "Clients/B/three.mov")})
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "/Clients/B/three.mov") {
		t.Fatalf("partly covered add: %d %s", rec.Code, rec.Body)
	}
	if got := e.members(id, "").Collection.Members; got != 2 {
		t.Fatalf("a refused add changed the collection: %d members", got)
	}
	// Bob's grant covers none of Ann's members.
	e.write("Bob", "/api/collections/"+string(id)+"/rename", map[string]any{"name": "Mine"}, 403)
	e.write("Bob", "/api/collections/"+string(id)+"/delete", map[string]any{}, 403)
	// The admin may add anything; then Ann no longer covers every member.
	byAdmin := e.write("admin", add, map[string]any{"members": pathRefs("Clients/B/three.mov")}, 200)
	if byAdmin.Operation.Actor != "admin" || byAdmin.Collection.Members != 3 {
		t.Fatalf("admin add: %+v", byAdmin)
	}
	e.write("Ann", "/api/collections/"+string(id)+"/rename", map[string]any{"name": "Ann's"}, 403)
	e.write("Ann", "/api/collections/"+string(id)+"/delete", map[string]any{}, 403)
	// Bob may remove the member his grant covers; then Ann covers all again.
	e.write("Bob", "/api/collections/"+string(id)+"/members/remove", map[string]any{"members": pathRefs("Clients/B/three.mov")}, 200)
	if r := e.write("Ann", "/api/collections/"+string(id)+"/rename", map[string]any{"name": "Ann's"}, 200); r.Collection.Name != "Ann's" {
		t.Fatalf("rename: %+v", r)
	}
	// Access "all" covers everything; no write access covers nothing.
	e.write("Al", "/api/collections/"+string(id)+"/rename", map[string]any{"name": "Al's"}, 200)
	e.write("Nia", "/api/collections", map[string]any{"name": "Empty"}, 403)
	empty := e.write("Bob", "/api/collections", map[string]any{"name": "Empty"}, 200)
	e.write("Bob", "/api/collections/"+string(empty.Collection.ID)+"/delete", map[string]any{}, 200)
	// A gone member needs no grant: Bob may delete a collection whose only
	// member (in /Clients/A) no longer exists.
	gone := e.write("admin", "/api/collections", map[string]any{"name": "Gone", "members": pathRefs("Clients/A/two.mov")}, 200)
	e.write("Bob", "/api/collections/"+string(gone.Collection.ID)+"/delete", map[string]any{}, 403)
	e.vol.set(func(f map[string]uint64) { delete(f, "Clients/A/two.mov") })
	e.write("Bob", "/api/collections/"+string(gone.Collection.ID)+"/delete", map[string]any{}, 200)
	// Unknown is not gone: an unreadable volume refuses a person's change.
	e.vol.setFail(true)
	e.write("Ann", "/api/collections/"+string(id)+"/rename", map[string]any{"name": "x"}, 503)
	if m := e.members(id, ""); m.Members[0].State != "unknown" {
		t.Fatalf("member during a volume failure: %+v", m.Members[0])
	}
	e.vol.setFail(false)
	if rec := e.do("Ann", http.MethodGet, "/api/collections/"+string(id), nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "Al's") {
		t.Fatalf("get: %d %s", rec.Code, rec.Body)
	}
}

func TestCollectionsMembersResolveLive(t *testing.T) {
	e := newCollectionsEnv(t)
	id := e.write("admin", "/api/collections", map[string]any{"name": "Live", "members": pathRefs("Clients/A/one.mov", "Clients/A/two.mov", "Clients/B/three.mov")}, 200).Collection.ID
	want := map[string]string{"one.mov": "present:Clients/A/one.mov", "two.mov": "present:Clients/A/two.mov", "three.mov": "present:Clients/B/three.mov"}
	if got := memberPaths(e.members(id, "")); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("members: %v", got)
	}
	// A rename and a folder move the binder has not seen (its hints are
	// stale), and a delete: paths resolve from the volume; the deleted
	// member stays, as missing.
	e.vol.set(func(f map[string]uint64) {
		delete(f, "Clients/A/one.mov")
		f["Clients/A/renamed.mov"] = 42
		delete(f, "Clients/B/three.mov")
		f["Archive/B/three.mov"] = 44
		delete(f, "Clients/A/two.mov")
	})
	got := e.members(id, "")
	want = map[string]string{"one.mov": "present:Clients/A/renamed.mov", "two.mov": "missing:", "three.mov": "present:Archive/B/three.mov"}
	if fmt.Sprint(memberPaths(got)) != fmt.Sprint(want) || got.Collection.Members != 3 || len(got.Members) != 3 {
		t.Fatalf("after rename/delete: %+v", got)
	}
	// Adding by the new path finds the same asset (through the volume, not
	// the stale hint): nothing is added.
	if r := e.write("admin", "/api/collections/"+string(id)+"/members/add", map[string]any{"members": pathRefs("Clients/A/renamed.mov")}, 200); r.Collection.Members != 3 {
		t.Fatalf("re-add by new path: %+v", r)
	}
	e.write("admin", "/api/collections/"+string(id)+"/members/add", map[string]any{"members": pathRefs("Clients/A/one.mov")}, 404)
	// Paging.
	first := e.members(id, "?limit=2")
	if len(first.Members) != 2 || first.Next == "" {
		t.Fatalf("first page: %+v", first)
	}
	rest := e.members(id, "?limit=2&after="+first.Next)
	if len(rest.Members) != 1 || rest.Next != "" || rest.Members[0].AssetID <= first.Members[1].AssetID {
		t.Fatalf("second page: %+v", rest)
	}
	// Members by asset ID; an unknown asset is 404.
	asset := binder.AssetID(e.vol.VolumeUUID(), 45)
	if r := e.write("admin", "/api/collections/"+string(id)+"/members/add", map[string]any{"members": []map[string]string{{"asset_id": string(asset)}}}, 200); r.Collection.Members != 4 {
		t.Fatalf("add by id: %+v", r)
	}
	e.write("admin", "/api/collections/"+string(id)+"/members/add", map[string]any{"members": []map[string]string{{"asset_id": "asset_00000000000000000000000000000077"}}}, 404)
	e.write("admin", "/api/collections/"+string(id)+"/members/add", map[string]any{"members": []map[string]string{{"asset_id": string(asset), "path": "Clients/A/sub/four.mov"}}}, 400)
}

func TestCollectionsRetriesAreIdempotent(t *testing.T) {
	e := newCollectionsEnv(t)
	ctx := context.Background()
	library, _ := e.svc.store.LibraryID()
	body := map[string]any{"name": "Selects", "members": pathRefs("Clients/A/one.mov"), "operation_id": "create-1"}
	first := e.write("Ann", "/api/collections", body, 200)
	again := e.write("Ann", "/api/collections", body, 200)
	if again.Collection.ID != first.Collection.ID || again.Operation != first.Operation {
		t.Fatalf("retry: %+v vs %+v", again, first)
	}
	list, err := e.svc.store.ListCollections(ctx, library, "", 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("collections after a retried create: %d %v", len(list), err)
	}
	// The same key with another request is a conflict; another person's
	// identical key is their own operation.
	body["name"] = "Other"
	e.write("Ann", "/api/collections", body, 409)
	body["name"] = "Selects"
	if other := e.write("Al", "/api/collections", body, 200); other.Collection.ID == first.Collection.ID {
		t.Fatal("two actors' keys collided")
	}
	id := string(first.Collection.ID)
	add := map[string]any{"members": pathRefs("Clients/A/two.mov"), "operation_id": "add-1"}
	a1 := e.write("Ann", "/api/collections/"+id+"/members/add", add, 200)
	a2 := e.write("Ann", "/api/collections/"+id+"/members/add", add, 200)
	if a1.Operation != a2.Operation || a2.Collection.Members != 2 {
		t.Fatalf("add retry: %+v vs %+v", a1, a2)
	}
	del := map[string]any{"operation_id": "delete-1"}
	d1 := e.write("Ann", "/api/collections/"+id+"/delete", del, 200)
	// The retry reaches the removed collection and gets the original result.
	d2 := e.write("Ann", "/api/collections/"+id+"/delete", del, 200)
	if d1.Operation != d2.Operation || !d2.Collection.Removed {
		t.Fatalf("delete retry: %+v vs %+v", d1, d2)
	}
	// A new request on the removed collection is 404.
	e.write("Ann", "/api/collections/"+id+"/delete", map[string]any{"operation_id": "delete-2"}, 404)
	e.write("Ann", "/api/collections/"+id+"/members/add", map[string]any{"members": pathRefs("Clients/A/one.mov")}, 404)
	if rec := e.do("Ann", http.MethodGet, "/api/collections/"+id+"/members", nil); rec.Code != 404 {
		t.Fatalf("members of a removed collection: %d", rec.Code)
	}
	events, err := e.svc.store.Events(ctx, library, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var mine int
	for _, ev := range events {
		if ev.Operation.Actor == e.ids["Ann"] {
			mine++
		}
	}
	if mine != 3 {
		t.Fatalf("Ann's events: %d, want 3 (create, add, delete)", mine)
	}
	for name, b := range map[string]any{
		"bad key":       map[string]any{"name": "x", "operation_id": "no spaces"},
		"unknown field": map[string]any{"name": "x", "extra": 1},
		"no name":       map[string]any{},
		"long name":     map[string]any{"name": strings.Repeat("n", 257)},
	} {
		e.write("Ann", "/api/collections", b, 400)
		_ = name
	}
}

// A format 2 catalog left by slice 2 is upgraded when the Manager opens it.
func TestContinuityUpgradesSliceTwoCatalog(t *testing.T) {
	cfg := continuityTestConfig(t)
	stateDir := filepath.Dir(cfg.StateFile)
	if err := os.Mkdir(filepath.Join(stateDir, continuityDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stateDir, continuityDirName, continuityCatalogName)
	fixture, err := os.ReadFile("../continuity/catalog/testdata/continuity-s3-v2-fixture.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(fixture)); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, version, err := catalog.Inspect(context.Background(), path); err != nil || version != 2 {
		t.Fatalf("fixture: format %d, %v", version, err)
	}
	store, err := openContinuityCatalog(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	library, _ := store.LibraryID()
	if _, version, err := catalog.Inspect(context.Background(), path); err != nil || version != 3 {
		t.Fatalf("after open: format %d, %v", version, err)
	}
	if events, err := store.Events(context.Background(), library, 0, 10); err != nil || len(events) != 3 {
		t.Fatalf("history: %d %v", len(events), err)
	}
	if _, err := store.GetAsset(context.Background(), library, continuity.AssetID("asset_202122232425262728292a2b2c2d2e2f")); err != nil {
		t.Fatal(err)
	}
}
