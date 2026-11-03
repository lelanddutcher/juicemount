package manager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/redis/go-redis/v9"
)

// Grants are not put in force while a quota exists above the project
// folders (or while the volume cannot be checked), and the page names the
// quota and how to remove it if one exists anyway (review round 4: the
// usage of such a quota could not be flushed by a limited person without
// letting them move it for every project below it).
func TestGrantsNotInForceWithQuotaAboveProjects(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	startRealProxyReloader(t, dir)
	var paths []string
	var checkErr error
	calls := 0
	s.SetQuotaScope(func(context.Context) ([]string, error) { calls++; return paths, checkErr })
	jane, _ := s.CreatePerson("Jane")
	if calls != 0 {
		t.Fatalf("creating a person with access all checked quotas")
	}
	paths = []string{"/Clients"}
	for what, err := range map[string]error{
		"grant":    s.AddGrant(jane.ID, "/Clients/A"),
		"access":   s.UpdatePerson(jane.ID, nil, ptr(personAccessGranted)),
		"restrict": s.SetRestrictUnassigned(true),
	} {
		if !errors.Is(err, ErrPersonInvalid) || !strings.Contains(err.Error(), `juicefs quota delete <META-URL> --path "/Clients"`) {
			t.Errorf("%s with a quota on /Clients: %v", what, err)
		}
	}
	if snap := s.PeopleSnapshot(); snap.GrantsInForce || len(snap.People[0].Grants) != 0 || snap.People[0].Access != personAccessAll {
		t.Fatalf("a refused change was kept: %+v", snap)
	}
	paths, checkErr = nil, errors.New("connection refused")
	if err := s.AddGrant(jane.ID, "/Clients/A"); err == nil || !strings.Contains(err.Error(), "cannot check") {
		t.Errorf("grant while the volume cannot be checked: %v", err)
	}
	checkErr = nil
	if err := s.AddGrant(jane.ID, "/Clients/A"); err != nil {
		t.Fatal(err)
	}
	// In force: no check per change; a quota that appears anyway is a
	// problem on the page.
	paths, n := []string{"/Clients"}, calls
	if err := s.AddGrant(jane.ID, "/Clients/B"); err != nil || calls != n {
		t.Fatalf("second grant: %v (checks %d -> %d)", err, n, calls)
	}
	pa := &peopleAPI{store: s, quotas: &quotaScopeCache{fn: s.quotaScope}}
	snap := pa.snapshot()
	if len(snap.QuotaConflicts) != 1 || !strings.Contains(snap.Problem, "quota usage of limited people's writes does not update") ||
		!strings.Contains(snap.Problem, "--path \"/Clients\"") {
		t.Errorf("snapshot with a quota above the projects: %+v", snap)
	}
}

func ptr[T any](v T) *T { return &v }

// quotasAboveProjects reads the volume: quotas on the root and on client
// folders, not on projects or below.
func TestQuotasAboveProjectsReadsVolume(t *testing.T) {
	addr := os.Getenv("JM_RESPPROXY_TEST_REDIS")
	if addr == "" {
		t.Skip("JM_RESPPROXY_TEST_REDIS not set")
	}
	const db = 13
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	defer rdb.Close()
	ctx := context.Background()
	rdb.FlushDB(ctx)
	defer rdb.FlushDB(ctx)
	conf := meta.DefaultConf()
	conf.NoBGJob = true
	m := meta.NewClient(fmt.Sprintf("redis://%s/%d", addr, db), conf)
	if err := m.Init(&meta.Format{Name: "qs", UUID: "00000000-0000-4000-8000-0000000000ef", Storage: "file",
		Bucket: t.TempDir() + "/", BlockSize: 4096}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Load(true); err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()
	ino := map[string]meta.Ino{"/": meta.RootInode}
	for _, d := range []string{"/Clients", "/Clients/A", "/Clients/A/sub", "/Other"} {
		var i meta.Ino
		var attr meta.Attr
		if st := m.Mkdir(meta.Background(), ino[filepath.Dir(d)], filepath.Base(d), 0o755, 0, 0, &i, &attr); st != 0 {
			t.Fatal(d, st)
		}
		ino[d] = i
	}
	got := func() []string {
		p, err := quotasAboveProjects(ctx, rdb)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	if p := got(); len(p) != 0 {
		t.Fatalf("no quota: %v", p)
	}
	for _, d := range []string{"/Clients/A", "/Clients/A/sub", "/Clients", "/"} {
		if err := m.HandleQuota(meta.Background(), meta.QuotaSet, d, map[string]*meta.Quota{d: {MaxSpace: 1 << 30, MaxInodes: 100}}, false, false, false); err != nil {
			t.Fatal(d, err)
		}
	}
	if p := got(); fmt.Sprint(p) != "[/ /Clients]" {
		t.Errorf("quotas above the projects: %v", p)
	}
}
