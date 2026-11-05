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
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
)

// TestQueryViewsThroughRESPProxyEnforce runs query views end to end in the
// default deployment's shape: the binder reads the volume and the views
// are written as the `manager` principal through a real jm-respproxy in
// enforce mode with the lease engine. With the query root among the
// proxy's smart roots (as the entrypoint renders it with JM_SMART_FOLDERS
// and JM_CONTINUITY on), the view of a renamed or deleted query is removed
// (a direct rmdir of a view the proxy saw the Manager create); without it
// (an entrypoint from before query views) the removal is refused and the
// emptied view is parked.
func TestQueryViewsThroughRESPProxyEnforce(t *testing.T) {
	for _, c := range []struct {
		name   string
		roots  []string
		admits bool
	}{
		{"query root configured", []string{"Recents", "Collections", "Queries"}, true},
		{"query root not configured", []string{"Recents", "Collections"}, false},
	} {
		t.Run(c.name, func(t *testing.T) { queryViewsThroughProxy(t, c.roots, c.admits) })
	}
}

func queryViewsThroughProxy(t *testing.T, roots []string, admits bool) {
	direct := standaloneTestURL(t)
	m := formatTestVolume(t, direct)
	files := []string{"Projects/a.mov", "Projects/b.mov", "Projects/c.txt"}
	for _, f := range files {
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
		`{"principals":[{"name":"manager","kind":"manager","host":"nas","argon2id":%q}]}`, hash)))
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := respproxy.New(respproxy.Config{Upstream: opts.Addr, Principals: principals,
		Mode: respproxy.ModeEnforce, VolumeDB: db, Logf: t.Logf, ModePoll: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	eng := respproxy.NewLeaseEngine(respproxy.LeaseConfig{Side: proxy.Side(), VolumeDB: db, Logf: t.Logf,
		SweepEvery: time.Hour, FlushEvery: 50 * time.Millisecond, SmartRoots: roots})
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
	viaProxy := fmt.Sprintf("redis://manager:synthetic-secret@%s/%d", ln.Addr(), db)

	store, err := openContinuityCatalog(filepath.Dir(continuityTestConfig(t).StateFile))
	if err != nil {
		t.Fatal(err)
	}
	cont := &continuityService{store: store}
	b, err := binder.New(binder.Config{Store: store, Applied: cont.applied, Connect: func() (binder.Volume, error) {
		fsys, err := jfsmeta.OpenReadOnly(viaProxy)
		if err != nil {
			return nil, err
		}
		return fsys, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	cont.binder = b
	cont.start()
	defer cont.Stop()
	lookup := func(p string) uint64 {
		t.Helper()
		ino := meta.RootInode
		var attr meta.Attr
		for _, part := range strings.Split(p, "/") {
			if st := m.Lookup(meta.Background(), ino, part, &ino, &attr, false); st != 0 {
				t.Fatalf("lookup %s: %v", p, st)
			}
		}
		return uint64(ino)
	}
	for _, f := range files {
		b.Offer(binder.Event{Op: "create", Path: f, Inode: lookup(f)})
	}
	for deadline := time.Now().Add(10 * time.Second); b.Stats().Handled < uint64(len(files)); time.Sleep(5 * time.Millisecond) {
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

	managerLeases, err := NewRedisLeaseClient(viaProxy, "")
	if err != nil {
		t.Fatal(err)
	}
	backend := &metaBackend{url: viaProxy, open: jfsmeta.Open}
	svc := newTestSmartFolders(t, svcOpts{backend: backend, leaser: NewProjectLeaser(managerLeases), clock: &testClock{t: time.Now()}})
	defer svc.Stop()
	svc.qry.debounce = time.Hour
	if !svc.attachQueries(cont) {
		t.Fatal("not attached")
	}
	ctx := context.Background()
	assertTree := func(want map[string]string) {
		t.Helper()
		if got := metaTreeAt(t, m, "Queries"); !reflect.DeepEqual(got, want) {
			t.Fatalf("views = %v, want %v (log: %v)", got, want, svc.lastLog)
		}
	}
	queries := func() meta.Ino { return metaMkdirAll(t, m, "Queries") }
	gone := func(name string) bool {
		t.Helper()
		var n meta.Ino
		var attr meta.Attr
		return m.Lookup(meta.Background(), queries(), name, &n, &attr, false) == syscall.ENOENT
	}

	// Created: one link per result, through the proxy.
	apply(catalog.Change{CreateQuery: &catalog.CreateQuery{QueryID: qryID(1), Name: "Movs",
		Definition: catalog.QueryDefinition{Prefixes: []string{"Projects"}, Extensions: []string{"mov"}, Limit: 200}}})
	svc.queriesOnlyCycle(ctx)
	assertTree(map[string]string{"Movs/a.mov": "Projects/a.mov", "Movs/b.mov": "Projects/b.mov"})

	// Renamed: the new view is made; the old one, emptied, is removed only
	// when the proxy knows the query root.
	apply(catalog.Change{RenameQuery: &catalog.RenameQuery{QueryID: qryID(1), Name: "Clips"}})
	svc.queriesOnlyCycle(ctx)
	assertTree(map[string]string{"Clips/a.mov": "Projects/a.mov", "Clips/b.mov": "Projects/b.mov"})
	st := svc.queryStats()
	if gone("Movs") != admits {
		t.Fatalf("old view removed: %v, want %v (stats %+v)", gone("Movs"), admits, st)
	}
	if admits {
		if len(st.ViewsLeft) != 0 || svc.qry.own.Views["Movs"] != nil {
			t.Fatalf("old view: stats %+v", st)
		}
	} else if len(st.ViewsLeft) != 1 || !strings.HasPrefix(st.ViewsLeft[0], "Queries/Movs: removal refused") ||
		!strings.Contains(st.ViewsLeft[0], "parked") || svc.qry.parked["Movs"].IsZero() {
		t.Fatalf("stats = %+v", st)
	}

	// Deleted: the view goes (or is emptied and parked).
	apply(catalog.Change{RemoveQuery: &catalog.RemoveQuery{QueryID: qryID(1)}})
	svc.queriesOnlyCycle(ctx)
	assertTree(map[string]string{})
	if gone("Clips") != admits {
		t.Fatalf("the deleted query's view removed: %v, want %v", gone("Clips"), admits)
	}
	if sessions, err := m.ListSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("the backends hold %d sessions (%v)", len(sessions), err)
	}
}
