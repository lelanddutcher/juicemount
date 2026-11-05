package manager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/redis/go-redis/v9"
)

// dirExists: the fake volume holds files; a directory is any folder above
// one of them.
func (v *lockedVolume) dirExists(_ context.Context, p string) (bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.fail {
		return false, syscall.EIO
	}
	for f := range v.files {
		if strings.HasPrefix(f, p+"/") {
			return true, nil
		}
	}
	return false, nil
}

// useVolumeForms makes grants and query folders take the fake volume's
// forms, as the Manager does over the volume's metadata.
func (e *collectionsEnv) useVolumeForms() {
	form := func(ctx context.Context, p string) (string, bool, error) { return volumeForm(ctx, e.vol.dirExists, p) }
	e.people.SetGrantForm(form)
	e.svc.folderForm = form
}

func (e *collectionsEnv) addFiles(files map[string]uint64) {
	e.t.Helper()
	var evs []binder.Event
	e.vol.set(func(f map[string]uint64) {
		for p, ino := range files {
			f[p] = ino
			evs = append(evs, binder.Event{Op: "create", Path: p, Inode: ino})
		}
	})
	e.offer(evs...)
}

func (e *collectionsEnv) grantsOf(name string) (stored, proxy []string) {
	e.t.Helper()
	for _, p := range e.people.PeopleSnapshot().People {
		if p.ID == e.ids[name] {
			stored = p.Grants
		}
	}
	e.people.mu.Lock()
	entries, v2 := e.people.proxyPeopleLocked()
	e.people.mu.Unlock()
	if !v2 {
		e.t.Fatal("the proxy file carries no people")
	}
	for _, p := range entries {
		if p.ID == e.ids[name] {
			proxy = p.Grants
		}
	}
	return stored, proxy
}

const (
	cafeNFC  = "Clients/Caf\u00e9"
	cafeNFD  = "Clients/Cafe\u0301"
	bebeNFC  = "Clients/B\u00e9b\u00e9"
	bebeNFD  = "Clients/Be\u0301be\u0301"
	bebeMix  = "Clients/B\u00e9be\u0301" // neither form
	novelNFD = "Clients/Nove\u0301"      // not on the volume
)

// A grant given decomposed for a folder the volume holds composed is stored
// composed: the Manager's check and the proxy's file name the folder's
// bytes. A folder not on the volume is stored as given.
func TestGrantStoredInTheVolumesForm(t *testing.T) {
	e := newAgentEnv(t)
	e.useVolumeForms()
	e.addFiles(map[string]uint64{cafeNFC + "/cut.mov": 60})
	if err := e.people.AddGrant(e.ids["Bob"], "/"+cafeNFD); err != nil {
		t.Fatal(err)
	}
	if err := e.people.AddGrant(e.ids["Bob"], "/"+novelNFD); err != nil {
		t.Fatal(err)
	}
	stored, proxy := e.grantsOf("Bob")
	want := []string{"/Clients/B", "/" + cafeNFC, "/" + novelNFD}
	if !slices.Equal(stored, want) || !slices.Equal(proxy, want) {
		t.Fatalf("grants stored %q, in the proxy file %q; want %q", stored, proxy, want)
	}
	// The Manager's check, byte for byte, matches the volume's names.
	e.write("Bob", "/api/collections", map[string]any{"name": "C", "members": pathRefs(cafeNFC + "/cut.mov")}, 200)
	q := e.queryWrite("Bob", "/api/queries", map[string]any{"name": "q", "folders": []string{cafeNFD}}, 200).Query
	if !slices.Equal(q.Folders, []string{cafeNFC}) {
		t.Fatalf("query folder stored as %q", q.Folders)
	}
	if found := e.find("Bob", url.Values{"name": {"cut"}}); len(found.Files) != 1 {
		t.Fatalf("Bob finds %+v", found.Files)
	}
	if !e.svc.wantsTouch(cafeNFD + "/x.mov") {
		t.Fatal("no recency marks under the query's folder")
	}
	// A query folder not on the volume is stored composed, as before.
	if q := e.queryWrite("admin", "/api/queries", map[string]any{"name": "n", "folders": []string{novelNFD}}, 200).Query; q.Folders[0] != "Clients/Nov\u00e9" {
		t.Fatalf("absent folder stored as %q", q.Folders)
	}
	// The grant is removed by either form.
	if err := e.people.RemoveGrant(e.ids["Bob"], "/"+cafeNFD); err != nil {
		t.Fatal(err)
	}
	if stored, _ := e.grantsOf("Bob"); slices.Contains(stored, "/"+cafeNFC) {
		t.Fatalf("grants after removal: %q", stored)
	}
	// A volume that cannot be read refuses the grant (503 from the API).
	e.vol.setFail(true)
	if err := e.people.AddGrant(e.ids["Bob"], "/"+cafeNFD); !errors.Is(err, errVolumeUnread) {
		t.Fatalf("grant with the volume unreadable: %v", err)
	}
}

// Two folders whose names differ only in form are two folders: a grant on
// one does not cover the other, in the catalog as at the proxy.
func TestGrantOnTwinFolderCoversOnlyIt(t *testing.T) {
	e := newAgentEnv(t)
	e.useVolumeForms()
	e.addFiles(map[string]uint64{cafeNFC + "/ours.mov": 70, cafeNFD + "/theirs.mov": 71})
	if err := e.people.AddGrant(e.ids["Bob"], "/"+cafeNFC); err != nil {
		t.Fatal(err)
	}
	if stored, _ := e.grantsOf("Bob"); !slices.Contains(stored, "/"+cafeNFC) || slices.Contains(stored, "/"+cafeNFD) {
		t.Fatalf("grants: %q", stored)
	}
	e.write("Bob", "/api/collections", map[string]any{"name": "T", "members": pathRefs(cafeNFD + "/theirs.mov")}, 403)
	e.write("Bob", "/api/collections", map[string]any{"name": "O", "members": pathRefs(cafeNFC + "/ours.mov")}, 200)
	e.queryWrite("Bob", "/api/queries", map[string]any{"name": "t", "folders": []string{cafeNFD}}, 403)
	if q := e.queryWrite("Bob", "/api/queries", map[string]any{"name": "o", "folders": []string{cafeNFC}}, 200).Query; q.Folders[0] != cafeNFC {
		t.Fatalf("query folder stored as %q", q.Folders)
	}
	found := e.find("Bob", url.Values{"name": {".mov"}})
	for _, f := range found.Files {
		if f.Name == "theirs.mov" {
			t.Fatalf("Bob finds the other folder's file: %+v", found.Files)
		}
	}
}

// A name the volume holds in both forms, given in neither, is refused: the
// grant or folder could mean either folder.
func TestGrantBothFormsWithoutExactRefused(t *testing.T) {
	e := newAgentEnv(t)
	e.useVolumeForms()
	e.addFiles(map[string]uint64{bebeNFC + "/a.mov": 80, bebeNFD + "/b.mov": 81})
	err := e.people.AddGrant(e.ids["Bob"], "/"+bebeMix)
	if !errors.Is(err, ErrPersonInvalid) || !errors.Is(err, errFolderFormAmbiguous) {
		t.Fatalf("ambiguous grant: %v", err)
	}
	if stored, _ := e.grantsOf("Bob"); !slices.Equal(stored, []string{"/Clients/B"}) {
		t.Fatalf("grants after a refusal: %q", stored)
	}
	if rec := e.do("admin", http.MethodPost, "/api/queries", map[string]any{"name": "m", "folders": []string{bebeMix}}); rec.Code != 400 || !strings.Contains(rec.Body.String(), "both composed and decomposed") {
		t.Fatalf("ambiguous query folder: %d %s", rec.Code, rec.Body)
	}
	// Each exact form is accepted as itself.
	for _, g := range []string{bebeNFC, bebeNFD} {
		if err := e.people.AddGrant(e.ids["Bob"], "/"+g); err != nil {
			t.Fatal(err)
		}
	}
	if stored, _ := e.grantsOf("Bob"); len(stored) != 3 {
		t.Fatalf("grants: %q", stored)
	}
}

// The lookup over the volume's metadata finds names byte for byte, as the
// proxy resolves grants, and only directories.
func TestRedisFolderFormReadsTheVolume(t *testing.T) {
	addr := os.Getenv("JM_RESPPROXY_TEST_REDIS")
	if addr == "" {
		t.Skip("JM_RESPPROXY_TEST_REDIS not set")
	}
	const db = 12
	url := fmt.Sprintf("redis://%s/%d", addr, db)
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	defer rdb.Close()
	ctx := context.Background()
	rdb.FlushDB(ctx)
	defer rdb.FlushDB(ctx)
	conf := meta.DefaultConf()
	conf.NoBGJob = true
	m := meta.NewClient(url, conf)
	if err := m.Init(&meta.Format{Name: "vf", UUID: "00000000-0000-4000-8000-0000000000f1", Storage: "file",
		Bucket: t.TempDir() + "/", BlockSize: 4096}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Load(true); err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()
	ino := map[string]meta.Ino{".": meta.RootInode}
	for _, d := range []string{"Clients", cafeNFC, bebeNFC, bebeNFD} {
		var i meta.Ino
		var attr meta.Attr
		if st := m.Mkdir(meta.Background(), ino[filepath.Dir(d)], filepath.Base(d), 0o755, 0, 0, &i, &attr); st != 0 {
			t.Fatal(d, st)
		}
		ino[d] = i
	}
	var fi meta.Ino
	var attr meta.Attr
	if st := m.Create(meta.Background(), ino["Clients"], "file.mov", 0o644, 0, 0, &fi, &attr); st != 0 {
		t.Fatal(st)
	}
	form := redisFolderForm(url)
	for given, want := range map[string]string{cafeNFD: cafeNFC, cafeNFC: cafeNFC, bebeNFD: bebeNFD, bebeNFC: bebeNFC} {
		if got, found, err := form(ctx, given); err != nil || !found || got != want {
			t.Errorf("%q: %q %v %v, want %q", given, got, found, err, want)
		}
	}
	for _, absent := range []string{novelNFD, "Clients/file.mov", "Other"} {
		if got, found, err := form(ctx, absent); err != nil || found || got != absent {
			t.Errorf("%q: %q %v %v", absent, got, found, err)
		}
	}
	if _, _, err := form(ctx, bebeMix); !errors.Is(err, errFolderFormAmbiguous) {
		t.Errorf("both forms, neither given: %v", err)
	}
}
