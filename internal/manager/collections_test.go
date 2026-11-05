package manager

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
// binder reads it. fail makes every read an I/O error. An inode in trash
// with no name left is in the JuiceFS trash (its attributes remain).
type lockedVolume struct {
	mu    sync.Mutex
	files map[string]uint64
	trash map[uint64]time.Time // inode -> when it went to the trash
	fail  bool
}

func (v *lockedVolume) VolumeUUID() string { return "00000000-0000-4000-8000-0000000000d3" }

// node: caller holds mu. Nlink counts the inode's names.
func (v *lockedVolume) node(ino uint64) jfsmeta.Node {
	var links uint32
	for _, i := range v.files {
		if i == ino {
			links++
		}
	}
	return jfsmeta.Node{Ino: ino, Type: smartfolders.TypeRegular, Nlink: links, Parent: 1}
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
	if at, ok := v.trash[ino]; ok {
		return jfsmeta.Node{Ino: ino, Type: smartfolders.TypeRegular, Nlink: 1, InTrash: true, Ctime: at}, nil
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
	macs   map[string][3]string // person name -> instance ID, poll token, device digest
	ids    map[string]string    // person name -> person ID
	h      *collectionsAPI
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
	env := &collectionsEnv{t: t, svc: svc, vol: vol, people: people, macs: map[string][3]string{}, ids: map[string]string{}}
	for i, spec := range []struct{ name, grant string }{
		{"Ann", "/Clients/A"}, {"Bob", "/Clients/B"}, {"Al", ""}, {"Nia", "/Clients/N"}, {"Pending", ""}, {"Nobody", ""},
	} {
		instance := fmt.Sprintf("%08X-7B3D-4C1A-9E2B-0A1B2C3D4E5F", 0x5A000000+i)
		device := fmt.Sprintf("%032x", i+1)
		resp, err := people.Enroll(EnrollRequest{InstanceID: instance, Host: "bay", Secret: testMacSecret, DeviceDigest: device}, "192.0.2.7")
		if err != nil {
			t.Fatal(err)
		}
		env.macs[spec.name] = [3]string{instance, resp.PollToken, device}
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
	env.h = newCollectionsAPI(env.a, "/manager", people)
	// The budgets have their own test.
	env.h.ipLimit, env.h.macLimit = newIPLimiter(1_000_000, time.Now), newIPLimiter(1_000_000, time.Now)
	env.mux.HandleFunc("/manager/api/collections", env.h.serve)
	env.mux.HandleFunc("/manager/api/collections/", env.h.serve)
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
		if mac[2] != "" {
			req.Header.Set("X-JuiceMount-Mac-Device", mac[2])
		}
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

// A save that writes a temp file and renames it over the document gives
// the file a new inode; the member is the same asset, still present, and
// the grant check follows the new inode.
func TestCollectionsMemberSurvivesAtomicSave(t *testing.T) {
	e := newCollectionsEnv(t)
	one := binder.AssetID(e.vol.VolumeUUID(), 42)
	id := e.write("admin", "/api/collections", map[string]any{"name": "Cut", "members": pathRefs("Clients/A/one.mov")}, 200).Collection.ID
	e.vol.set(func(f map[string]uint64) { f["Clients/A/one.mov"] = 50 })
	e.vol.mu.Lock()
	e.vol.trash = map[uint64]time.Time{42: time.Now()} // the rename over sent it to the trash
	e.vol.mu.Unlock()
	e.offer(binder.Event{Op: "rename", Path: "Clients/A/one.mov", OldPath: "Clients/A/.one.mov.tmp", Inode: 50})
	got := e.members(id, "")
	if len(got.Members) != 1 || got.Members[0].AssetID != one || got.Members[0].State != "present" || got.Members[0].Path != "Clients/A/one.mov" {
		t.Fatalf("after an atomic save: %+v", got)
	}
	// Adding by path finds the same asset: nothing is added.
	if r := e.write("admin", "/api/collections/"+string(id)+"/members/add", map[string]any{"members": pathRefs("Clients/A/one.mov")}, 200); r.Collection.Members != 1 {
		t.Fatalf("re-add by path: %+v", r)
	}
	e.write("Bob", "/api/collections/"+string(id)+"/members/remove", map[string]any{"members": assetRefs(one)}, 403)
	e.write("Ann", "/api/collections/"+string(id)+"/members/remove", map[string]any{"members": assetRefs(one)}, 200)
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

func (e *collectionsEnv) offer(evs ...binder.Event) {
	e.t.Helper()
	want := e.svc.binder.Stats().Handled + uint64(len(evs))
	for _, ev := range evs {
		e.svc.binder.Offer(ev)
	}
	deadline := time.Now().Add(10 * time.Second)
	for e.svc.binder.Stats().Handled < want {
		if time.Now().After(deadline) {
			e.t.Fatalf("events not handled: %+v", e.svc.binder.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assetRefs(ids ...continuity.AssetID) []map[string]string {
	var out []map[string]string
	for _, id := range ids {
		out = append(out, map[string]string{"asset_id": string(id)})
	}
	return out
}

// A file moved under a hidden name is still in its folder: it is not
// missing, so A's grant does not let Ann act on a file now in B.
func TestCollectionsHiddenNamesAndLinksNeedGrants(t *testing.T) {
	e := newCollectionsEnv(t)
	three := binder.AssetID(e.vol.VolumeUUID(), 44)
	two := binder.AssetID(e.vol.VolumeUUID(), 43)
	sel := string(e.write("admin", "/api/collections", map[string]any{"name": "Mixed", "members": pathRefs("Clients/A/one.mov", "Clients/B/three.mov")}, 200).Collection.ID)
	only := string(e.write("admin", "/api/collections", map[string]any{"name": "B only", "members": pathRefs("Clients/B/three.mov")}, 200).Collection.ID)
	e.vol.set(func(f map[string]uint64) {
		delete(f, "Clients/B/three.mov")
		f["Clients/B/.Proj-old/three.mov"] = 44
	})
	e.offer(binder.Event{Op: "rename", Path: "Clients/B/.Proj-old/three.mov", OldPath: "Clients/B/three.mov", Inode: 44})
	if m := memberPaths(e.members(catalog.CollectionID(only), "")); m["three.mov"] != "missing:" {
		t.Fatalf("listed as: %v", m)
	}
	rec := e.do("Ann", http.MethodPost, "/api/collections/"+sel+"/members/remove", map[string]any{"members": assetRefs(three)})
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "/Clients/B/.Proj-old/three.mov") {
		t.Fatalf("Ann removing a hidden B file: %d %s", rec.Code, rec.Body)
	}
	e.write("Ann", "/api/collections/"+only+"/delete", map[string]any{}, 403)
	e.write("Ann", "/api/collections/"+only+"/rename", map[string]any{"name": "mine"}, 403)
	e.write("Bob", "/api/collections/"+sel+"/members/remove", map[string]any{"members": assetRefs(three)}, 200)
	// A hard link into A and B needs both grants, as a write through the
	// proxy does: neither Ann nor Bob alone, but access "all".
	e.vol.set(func(f map[string]uint64) { f["Clients/B/two-link.mov"] = 43 })
	linked := string(e.write("admin", "/api/collections", map[string]any{"name": "Linked", "members": assetRefs(two)}, 200).Collection.ID)
	rec = e.do("Ann", http.MethodPost, "/api/collections/"+linked+"/rename", map[string]any{"name": "Ann"})
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "/Clients/B/two-link.mov") {
		t.Fatalf("Ann renaming a collection of a file linked into B: %d %s", rec.Code, rec.Body)
	}
	e.write("Bob", "/api/collections/"+linked+"/rename", map[string]any{"name": "Bob"}, 403)
	e.write("Al", "/api/collections/"+linked+"/rename", map[string]any{"name": "Al"}, 200)
	// A grant covers its folder, not a sibling sharing its prefix.
	e.vol.set(func(f map[string]uint64) { f["Clients/AB/x.mov"] = 46 })
	e.offer(binder.Event{Op: "create", Path: "Clients/AB/x.mov", Inode: 46})
	rec = e.do("Ann", http.MethodPost, "/api/collections/"+sel+"/members/add", map[string]any{"members": pathRefs("Clients/AB/x.mov")})
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "/Clients/AB/x.mov") {
		t.Fatalf("Ann adding a /Clients/AB file: %d %s", rec.Code, rec.Body)
	}
}

func TestCollectionsMacDeviceAndBudget(t *testing.T) {
	e := newCollectionsEnv(t)
	mac := e.macs["Ann"]
	e.macs["Ann-nodevice"] = [3]string{mac[0], mac[1], ""}
	if rec := e.do("Ann-nodevice", http.MethodGet, "/api/collections", nil); rec.Code != 401 || !strings.Contains(rec.Body.String(), "device digest") {
		t.Fatalf("no device digest: %d %s", rec.Code, rec.Body)
	}
	// Another Mac's digest with Ann's identity: flagged, then refused for good.
	e.macs["Ann-clone"] = [3]string{mac[0], mac[1], strings.Repeat("e", 32)}
	if rec := e.do("Ann-clone", http.MethodGet, "/api/collections", nil); rec.Code != 403 || !strings.Contains(rec.Body.String(), "two Macs") {
		t.Fatalf("cloned Mac: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do("Ann", http.MethodGet, "/api/collections", nil); rec.Code != 403 {
		t.Fatalf("flagged Mac with its own digest: %d %s", rec.Code, rec.Body)
	}
	// Budgets: fake tokens do not drain a Mac's budget; a Mac's budget does
	// not starve another Mac at the same address; an address has a looser
	// one before the token is checked.
	h := newCollectionsAPI(e.a, "/manager", e.people)
	// A stopped clock: no tokens refill while the test runs (slow under -race).
	frozen := time.Now()
	h.macLimit.now = func() time.Time { return frozen }
	h.ipLimit.now = h.macLimit.now
	send := func(who, token string) int {
		m := e.macs[who]
		if token == "" {
			token = m[1]
		}
		req := httptest.NewRequest(http.MethodGet, "/manager/api/collections", nil)
		req.Header.Set("X-JuiceMount-Mac-Instance", m[0])
		req.Header.Set("X-JuiceMount-Mac-Token", token)
		req.Header.Set("X-JuiceMount-Mac-Device", m[2])
		rec := httptest.NewRecorder()
		h.serve(rec, req)
		return rec.Code
	}
	codes := map[int]int{}
	for i := 0; i < 100; i++ {
		codes[send("Bob", strings.Repeat("0", 64))]++
	}
	for i := 0; i < collectionMacBurst+1; i++ {
		codes[send("Bob", "")]++
	}
	if codes[401] != 100 || codes[200] != collectionMacBurst || codes[429] != 1 {
		t.Fatalf("Mac budget: %v", codes)
	}
	if code := send("Al", ""); code != 200 {
		t.Fatalf("another Mac at the same address: %d", code)
	}
	h.ipLimit = newIPLimiter(2, h.macLimit.now)
	if a, b, c := send("Al", ""), send("Al", ""), send("Al", strings.Repeat("0", 64)); a != 200 || b != 200 || c != 429 {
		t.Fatalf("address budget: %d %d %d", a, b, c)
	}
}

// A Mac enrolled before device digests: the first digest seen is recorded,
// a different one later flags it.
func TestCollectionsMacWithoutDigestTrustsFirstUse(t *testing.T) {
	e := newCollectionsEnv(t)
	instance := "5B000000-7B3D-4C1A-9E2B-0A1B2C3D4E5F"
	resp, err := e.people.Enroll(EnrollRequest{InstanceID: instance, Host: "old", Secret: testMacSecret}, "192.0.2.8")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.people.Approve(macPrincipalName(instance)); err != nil {
		t.Fatal(err)
	}
	if err := e.people.AssignDevice(macPrincipalName(instance), e.ids["Al"]); err != nil {
		t.Fatal(err)
	}
	e.macs["Old"] = [3]string{instance, resp.PollToken, strings.Repeat("a", 32)}
	for i := 0; i < 2; i++ {
		if rec := e.do("Old", http.MethodGet, "/api/collections", nil); rec.Code != 200 {
			t.Fatalf("first use %d: %d %s", i, rec.Code, rec.Body)
		}
	}
	e.macs["Old"] = [3]string{instance, resp.PollToken, strings.Repeat("b", 32)}
	if rec := e.do("Old", http.MethodGet, "/api/collections", nil); rec.Code != 403 {
		t.Fatalf("a second device: %d %s", rec.Code, rec.Body)
	}
}

// Adding members depends only on the added members: a concurrent change
// to the collection does not make it a conflict.
func TestCollectionsConcurrentAddersDoNotConflict(t *testing.T) {
	e := newCollectionsEnv(t)
	ctx := context.Background()
	library, _ := e.svc.store.LibraryID()
	id := e.write("Ann", "/api/collections", map[string]any{"name": "Ann's", "members": pathRefs("Clients/A/one.mov")}, 200).Collection.ID
	collectionBeforeApply = func() {
		collectionBeforeApply = nil
		if _, err := e.svc.store.Apply(ctx, catalog.Operation{Version: 1, LibraryID: library, ID: "other-adder", Actor: "admin",
			Changes: []catalog.Change{{AddCollectionMember: &catalog.AddCollectionMember{CollectionID: id, AssetID: binder.AssetID(e.vol.VolumeUUID(), 44)}}}}); err != nil {
			t.Error(err)
		}
	}
	defer func() { collectionBeforeApply = nil }()
	if r := e.write("Ann", "/api/collections/"+string(id)+"/members/add", map[string]any{"members": pathRefs("Clients/A/two.mov")}, 200); r.Collection.Members != 3 {
		t.Fatalf("add beside a concurrent add: %+v", r.Collection)
	}
}

// The write carries the sequence read for its check: a membership change in
// between makes it 409, and a fresh attempt is checked against the new
// members.
func TestCollectionsCheckAndWriteAreTied(t *testing.T) {
	e := newCollectionsEnv(t)
	ctx := context.Background()
	library, _ := e.svc.store.LibraryID()
	id := e.write("Ann", "/api/collections", map[string]any{"name": "Ann's", "members": pathRefs("Clients/A/one.mov")}, 200).Collection.ID
	three := binder.AssetID(e.vol.VolumeUUID(), 44)
	collectionBeforeApply = func() {
		collectionBeforeApply = nil
		if _, err := e.svc.store.Apply(ctx, catalog.Operation{Version: 1, LibraryID: library, ID: "sneak", Actor: "admin",
			Changes: []catalog.Change{{AddCollectionMember: &catalog.AddCollectionMember{CollectionID: id, AssetID: three}}}}); err != nil {
			t.Error(err)
		}
	}
	defer func() { collectionBeforeApply = nil }()
	rec := e.do("Ann", http.MethodPost, "/api/collections/"+string(id)+"/delete", map[string]any{})
	if rec.Code != 409 || !strings.Contains(rec.Body.String(), "changed while") {
		t.Fatalf("delete raced by an add: %d %s", rec.Code, rec.Body)
	}
	e.write("Ann", "/api/collections/"+string(id)+"/delete", map[string]any{}, 403) // now holds a B file
	if c := e.members(id, ""); c.Collection.Removed || c.Collection.Members != 2 {
		t.Fatalf("collection: %+v", c.Collection)
	}
}

func TestCollectionsRetryIsNotReauthorized(t *testing.T) {
	e := newCollectionsEnv(t)
	id := string(e.write("Ann", "/api/collections", map[string]any{"name": "S", "members": pathRefs("Clients/A/one.mov")}, 200).Collection.ID)
	add := map[string]any{"members": pathRefs("Clients/A/two.mov"), "operation_id": "add-1"}
	first := e.write("Ann", "/api/collections/"+id+"/members/add", add, 200)
	// The collection moves on (the retry's precondition is the original's)
	// and Ann loses her grant: the committed change still answers.
	e.write("admin", "/api/collections/"+id+"/members/add", map[string]any{"members": pathRefs("Clients/B/three.mov")}, 200)
	if err := e.people.RemoveGrant(e.ids["Ann"], "/Clients/A"); err != nil {
		t.Fatal(err)
	}
	again := e.write("Ann", "/api/collections/"+id+"/members/add", add, 200)
	if again.Operation != first.Operation {
		t.Fatalf("retry: %+v vs %+v", again.Operation, first.Operation)
	}
	e.write("Ann", "/api/collections/"+id+"/members/add", map[string]any{"members": pathRefs("Clients/A/two.mov"), "operation_id": "add-2"}, 403)
}

func TestCollectionsDeleteFormsAndOpenAdmin(t *testing.T) {
	e := newCollectionsEnv(t)
	a := string(e.write("admin", "/api/collections", map[string]any{"name": "a"}, 200).Collection.ID)
	b := string(e.write("admin", "/api/collections", map[string]any{"name": "b"}, 200).Collection.ID)
	if rec := e.do("admin", http.MethodDelete, "/api/collections/"+a, nil); rec.Code != 200 {
		t.Fatalf("DELETE: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do("admin", http.MethodPost, "/api/collections/"+b+"/delete", nil); rec.Code != 200 {
		t.Fatalf("POST delete with no body: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do("admin", http.MethodGet, "/api/collections/"+b+"/members?limit=101", nil); rec.Code != 404 {
		t.Fatalf("members of a deleted collection: %d", rec.Code)
	}
	c := string(e.write("admin", "/api/collections", map[string]any{"name": "c"}, 200).Collection.ID)
	if rec := e.do("admin", http.MethodGet, "/api/collections/"+c+"/members?limit=101", nil); rec.Code != 400 {
		t.Fatalf("member page over 100: %d", rec.Code)
	}
	// No admin key while grants are in force: the open admin path reads but
	// does not write.
	e.a.adminKey = ""
	if rec := e.do("", http.MethodGet, "/api/collections", nil); rec.Code != 200 {
		t.Fatalf("open read: %d", rec.Code)
	}
	if rec := e.do("", http.MethodPost, "/api/collections", map[string]any{"name": "x"}); rec.Code != 403 || !strings.Contains(rec.Body.String(), "admin key") {
		t.Fatalf("open write with grants in force: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do("", http.MethodDelete, "/api/collections/"+c, nil); rec.Code != 403 {
		t.Fatalf("open delete: %d", rec.Code)
	}
	// Persons still write with their Macs.
	e.write("Al", "/api/collections/"+c+"/rename", map[string]any{"name": "Al"}, 200)
}

func TestCollectionsBusyAndRefusalLog(t *testing.T) {
	e := newCollectionsEnv(t)
	saved := collectionBusyWait
	collectionBusyWait = 50 * time.Millisecond
	defer func() { collectionBusyWait = saved }()
	for i := 0; i < collectionVolumeSlots; i++ {
		e.h.slots <- struct{}{}
	}
	if rec := e.do("Bob", http.MethodPost, "/api/collections", map[string]any{"name": "x"}); rec.Code != 503 || !strings.Contains(rec.Body.String(), "busy") {
		t.Fatalf("all people's slots taken: %d %s", rec.Code, rec.Body)
	}
	if rec := e.do("Bob", http.MethodGet, "/api/collections", nil); rec.Code != 200 {
		t.Fatalf("a list needs no slot: %d", rec.Code)
	}
	if rec := e.do("admin", http.MethodPost, "/api/collections", map[string]any{"name": "x"}); rec.Code != 200 {
		t.Fatalf("the admin's own slot: %d %s", rec.Code, rec.Body)
	}
	for i := 0; i < collectionVolumeSlots; i++ {
		<-e.h.slots
	}
	// One volume-reading request per actor at a time.
	e.h.inflight[e.ids["Ann"]] = true
	if rec := e.do("Ann", http.MethodPost, "/api/collections", map[string]any{"name": "x"}); rec.Code != 503 || !strings.Contains(rec.Body.String(), "still being checked") {
		t.Fatalf("Ann's second request: %d %s", rec.Code, rec.Body)
	}
	e.write("Bob", "/api/collections", map[string]any{"name": "y"}, 200)
	delete(e.h.inflight, e.ids["Ann"])
	e.write("Ann", "/api/collections", map[string]any{"name": "x"}, 200)
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	for i := 0; i < 3; i++ {
		e.write("Nia", "/api/collections", map[string]any{"name": "x"}, 403)
	}
	if n := strings.Count(buf.String(), "collections: refused"); n != 1 {
		t.Fatalf("refusal lines: %d\n%s", n, buf.String())
	}
}

func TestContinuityRefusesNewerCatalogAsNewer(t *testing.T) {
	cfg := continuityTestConfig(t)
	stateDir := filepath.Dir(cfg.StateFile)
	store, err := openContinuityCatalog(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stateDir, continuityDirName, continuityCatalogName)
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := db.Exec(`PRAGMA user_version=4`)
	if err := errors.Join(execErr, db.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err := openContinuityCatalog(stateDir); err == nil || !strings.Contains(err.Error(), "newer JuiceMount") {
		t.Fatalf("newer catalog: %v", err)
	}
}
