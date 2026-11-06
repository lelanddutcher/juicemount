package manager

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
)

// TestCollectionViewsThroughRESPProxyEnforce runs collection views end to
// end in the default deployment's shape: the binder reads the volume and
// the views are written as the `manager` principal through a real
// jm-respproxy in enforce mode with the lease engine and the smart roots
// configured. Creating a view, following a member move and removing the
// old view of a renamed or deleted collection (a direct rmdir of a view
// the proxy saw the Manager create) are admitted; a view the proxy has no
// record of is refused and parked. While a Mac holds the view's project
// the Manager writes nothing.
func TestCollectionViewsThroughRESPProxyEnforce(t *testing.T) {
	direct := standaloneTestURL(t)
	m := formatTestVolume(t, direct)
	for _, f := range []string{"Projects/a.mov", "Projects/b.mov"} {
		metaWriteFile(t, m, f)
	}
	opts, err := redisurl.Parse(direct)
	if err != nil {
		t.Fatal(err)
	}
	db := opts.DB
	hash, err := respproxy.HashPassword("synthetic-secret", 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	principals, err := respproxy.ParsePrincipals([]byte(fmt.Sprintf(
		`{"principals":[{"name":"manager","kind":"manager","host":"nas","argon2id":%q},{"name":"mac1","kind":"mac","host":"mac1-host","argon2id":%q}]}`,
		hash, hash)))
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := respproxy.New(respproxy.Config{Upstream: opts.Addr, Principals: principals,
		Mode: respproxy.ModeEnforce, VolumeDB: db, Logf: t.Logf, ModePoll: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	eng := respproxy.NewLeaseEngine(respproxy.LeaseConfig{Side: proxy.Side(), VolumeDB: db, Logf: t.Logf,
		SweepEvery: time.Hour, FlushEvery: 50 * time.Millisecond, SmartRoots: []string{"Recents", "Collections"}})
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	proxy.SetPolicy(eng)
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go proxy.Serve(ln)
	defer proxy.Close()
	viaProxy := func(user string) string {
		return fmt.Sprintf("redis://%s:synthetic-secret@%s/%d", user, ln.Addr(), db)
	}

	// The continuity binder and catalog, reading through the proxy.
	store, err := openContinuityCatalog(filepath.Dir(continuityTestConfig(t).StateFile))
	if err != nil {
		t.Fatal(err)
	}
	b, err := binder.New(binder.Config{Store: store, Connect: func() (binder.Volume, error) {
		fsys, err := jfsmeta.OpenReadOnly(viaProxy("manager"))
		if err != nil {
			return nil, err
		}
		return fsys, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	cont := &continuityService{binder: b, store: store}
	cont.start()
	defer cont.Stop()
	lookup := func(p string) uint64 {
		t.Helper()
		ino := meta.RootInode
		var attr meta.Attr
		for _, c := range strings.Split(p, "/") {
			if st := m.Lookup(meta.Background(), ino, c, &ino, &attr, false); st != 0 {
				t.Fatalf("lookup %s: %v", p, st)
			}
		}
		return uint64(ino)
	}
	for _, f := range []string{"Projects/a.mov", "Projects/b.mov"} {
		b.Offer(binder.Event{Op: "create", Path: f, Inode: lookup(f)})
	}
	for deadline := time.Now().Add(10 * time.Second); b.Stats().Handled < 2; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("not bound: %+v", b.Stats())
		}
	}
	library, _ := store.LibraryID()
	ops := 0
	apply := func(changes ...catalog.Change) {
		t.Helper()
		ops++
		if _, err := store.Apply(context.Background(), catalog.Operation{Version: catalog.OperationVersion,
			LibraryID: library, ID: fmt.Sprintf("op-%d", ops), Actor: "admin", Changes: changes}); err != nil {
			t.Fatal(err)
		}
	}
	uuid := m.GetFormat().UUID
	asset := func(p string) catalog.AddCollectionMember {
		return catalog.AddCollectionMember{CollectionID: collID(1), AssetID: binder.AssetID(uuid, lookup(p))}
	}

	managerLeases, err := NewRedisLeaseClient(viaProxy("manager"), "")
	if err != nil {
		t.Fatal(err)
	}
	backend := &metaBackend{url: viaProxy("manager"), open: jfsmeta.Open}
	clk := &testClock{t: time.Now()}
	svc := newTestSmartFolders(t, svcOpts{backend: backend, leaser: NewProjectLeaser(managerLeases), clock: clk})
	defer svc.Stop()
	svc.coll.debounce = time.Hour
	if !svc.attachCollections(cont) {
		t.Fatal("not attached")
	}
	ctx := context.Background()
	assertTree := func(want map[string]string) {
		t.Helper()
		if got := metaTreeAt(t, m, "Collections"); !reflect.DeepEqual(got, want) {
			t.Fatalf("views = %v, want %v (log: %v)", got, want, svc.lastLog)
		}
	}

	// Create.
	a1, a2 := asset("Projects/a.mov"), asset("Projects/b.mov")
	apply(catalog.Change{CreateCollection: &catalog.CreateCollection{CollectionID: collID(1), Name: "Shoot"}},
		catalog.Change{AddCollectionMember: &a1}, catalog.Change{AddCollectionMember: &a2})
	svc.cycle(ctx)
	assertTree(map[string]string{"Shoot/a.mov": "Projects/a.mov", "Shoot/b.mov": "Projects/b.mov"})

	// A member moves.
	proj := metaMkdirAll(t, m, "Projects")
	sub := metaMkdirAll(t, m, "Projects/sub")
	var ino meta.Ino
	var attr meta.Attr
	if st := m.Rename(meta.Background(), proj, "a.mov", sub, "a.mov", 0, &ino, &attr); st != 0 {
		t.Fatal(st)
	}
	svc.noteCollectionEvent(smartfolders.Event{Op: "rename", Path: "Projects/sub/a.mov", OldPath: "Projects/a.mov"})
	svc.collectionsOnlyCycle(ctx)
	assertTree(map[string]string{"Shoot/a.mov": "Projects/sub/a.mov", "Shoot/b.mov": "Projects/b.mov"})

	// Renamed: the new view is made, and the old one, emptied, is removed
	// through the proxy (a direct rmdir of a view it saw the Manager make).
	apply(catalog.Change{RenameCollection: &catalog.RenameCollection{CollectionID: collID(1), Name: "Final"}})
	svc.collectionsOnlyCycle(ctx)
	assertTree(map[string]string{"Final/a.mov": "Projects/sub/a.mov", "Final/b.mov": "Projects/b.mov"})
	coll := metaMkdirAll(t, m, "Collections")
	gone := func(name string) {
		t.Helper()
		var n meta.Ino
		if st := m.Lookup(meta.Background(), coll, name, &n, &attr, false); st != syscall.ENOENT {
			t.Fatalf("view %s still there: %v (log %v)", name, st, svc.lastLog)
		}
	}
	gone("Shoot")
	if st := svc.collectionStats(); len(st.ViewsLeft) != 0 || svc.coll.own.Views["Shoot"] != nil {
		t.Fatalf("stats = %+v", st)
	}

	// A view the proxy has no record of (made here straight through Redis,
	// as by a Manager before the record existed) is refused and parked.
	proxied := svc.cfg.Backend
	directBackend := &metaBackend{url: direct, open: jfsmeta.Open}
	defer directBackend.close()
	b2 := catalog.AddCollectionMember{CollectionID: collID(2), AssetID: a2.AssetID}
	apply(catalog.Change{CreateCollection: &catalog.CreateCollection{CollectionID: collID(2), Name: "Old"}},
		catalog.Change{AddCollectionMember: &b2})
	svc.cfg.Backend = directBackend
	svc.collectionsOnlyCycle(ctx)
	svc.cfg.Backend = proxied
	assertTree(map[string]string{"Final/a.mov": "Projects/sub/a.mov", "Final/b.mov": "Projects/b.mov", "Old/b.mov": "Projects/b.mov"})
	apply(catalog.Change{RemoveCollection: &catalog.RemoveCollection{CollectionID: collID(2)}})
	svc.collectionsOnlyCycle(ctx)
	assertTree(map[string]string{"Final/a.mov": "Projects/sub/a.mov", "Final/b.mov": "Projects/b.mov"})
	if st := svc.collectionStats(); len(st.ViewsLeft) != 1 || !strings.HasPrefix(st.ViewsLeft[0], "Collections/Old: removal refused") ||
		!strings.Contains(st.ViewsLeft[0], "parked") || svc.coll.parked["Old"].IsZero() {
		t.Fatalf("stats = %+v", st)
	}

	// A member leaves (a write into the view under its checkout).
	apply(catalog.Change{RemoveCollectionMember: &catalog.RemoveCollectionMember{CollectionID: collID(1), AssetID: a1.AssetID}})
	svc.collectionsOnlyCycle(ctx)
	assertTree(map[string]string{"Final/b.mov": "Projects/b.mov"})

	// A Mac holds the view's project: nothing is written.
	macLeases, err := NewRedisLeaseClient(viaProxy("mac1"), "")
	if err != nil {
		t.Fatal(err)
	}
	grant, err := macLeases.Acquire(ctx, "path:/Collections/Final", "checkout", time.Hour)
	if err != nil || (grant.Status != "GRANTED" && grant.Status != "RENEWED") {
		t.Fatalf("mac checkout = %+v, %v", grant, err)
	}
	apply(catalog.Change{RemoveCollection: &catalog.RemoveCollection{CollectionID: collID(1)}})
	svc.collectionsOnlyCycle(ctx)
	assertTree(map[string]string{"Final/b.mov": "Projects/b.mov"})
	if !strings.Contains(svc.lastLog["collections:Final"], "mac1") {
		t.Fatalf("refusal not reported: %q", svc.lastLog["collections:Final"])
	}

	// Released: after the backoff the deleted collection's view goes.
	if st, err := macLeases.Release(ctx, "path:/Collections/Final", grant.Epoch); err != nil || st != "RELEASED" {
		t.Fatalf("release = %q, %v", st, err)
	}
	clk.add(time.Hour)
	svc.collectionsOnlyCycle(ctx)
	assertTree(map[string]string{})
	gone("Final")
	if sessions, err := m.ListSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("the backends hold %d sessions (%v)", len(sessions), err)
	}
}
