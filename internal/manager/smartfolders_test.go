package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

func envMap(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }

func TestSmartFoldersEnv(t *testing.T) {
	if on, _, err := smartFoldersEnv(envMap(nil)); on || err != nil {
		t.Fatalf("default = %v, %v; want off", on, err)
	}
	for _, v := range []string{"off", "0", "false", "no"} {
		if on, _, err := smartFoldersEnv(envMap(map[string]string{"JM_SMART_FOLDERS": v})); on || err != nil {
			t.Fatalf("%q = %v, %v", v, on, err)
		}
	}
	on, s, err := smartFoldersEnv(envMap(map[string]string{"JM_SMART_FOLDERS": "on"}))
	if !on || s.Root != "Recents" || s.MaxViews != 256 || err != nil {
		t.Fatalf("on = %v %+v %v", on, s, err)
	}
	on, s, err = smartFoldersEnv(envMap(map[string]string{"JM_SMART_FOLDERS": "ON", "JM_SMART_FOLDERS_ROOT": "Latest", "JM_SMART_FOLDERS_MAX_VIEWS": "10"}))
	if !on || s.Root != "Latest" || s.MaxViews != 10 || err != nil {
		t.Fatalf("custom = %v %+v %v", on, s, err)
	}
	for _, bad := range []map[string]string{
		{"JM_SMART_FOLDERS": "maybe"},
		{"JM_SMART_FOLDERS": "on", "JM_SMART_FOLDERS_ROOT": ".trash"},
		{"JM_SMART_FOLDERS": "on", "JM_SMART_FOLDERS_ROOT": "a/b"},
		{"JM_SMART_FOLDERS": "on", "JM_SMART_FOLDERS_MAX_VIEWS": "0"},
		{"JM_SMART_FOLDERS": "on", "JM_SMART_FOLDERS_MAX_VIEWS": "100000"},
	} {
		if on, _, err := smartFoldersEnv(envMap(bad)); on || err == nil {
			t.Fatalf("%v accepted", bad)
		}
	}
}

var smartOn = envMap(map[string]string{"JM_SMART_FOLDERS": "on"})

func TestStartSmartFoldersDisabledAndBackendChoice(t *testing.T) {
	a := &API{jobs: NewJobManager("juicefs", RunSyncSpec{})}
	state := filepath.Join(t.TempDir(), "state.json")
	cases := []struct {
		name string
		cfg  Config
		env  func(string) string
	}{
		{"off by default", Config{FUSEMount: t.TempDir(), OverviewMetaURL: "redis://127.0.0.1:1/0", StateFile: state}, envMap(nil)},
		{"no state file", Config{MetaURL: "redis://127.0.0.1:1/1", VolName: "v"}, smartOn},
		{"no fuse and no meta", Config{StateFile: state}, smartOn},
		{"embedded mode", Config{FUSEMount: t.TempDir(), OverviewMetaURL: "redis://127.0.0.1:1/0", StateFile: state}, smartOn},
	}
	for _, c := range cases {
		if svc := a.buildSmartFolders(c.cfg, c.env); svc != nil {
			t.Fatalf("%s: enabled", c.name)
		}
	}
	// Standalone (the default deployment): the JuiceFS metadata backend.
	svc := a.buildSmartFolders(Config{MetaURL: "redis://manager@127.0.0.1:1/1", VolName: "v", StateFile: state}, smartOn)
	if svc == nil {
		t.Fatal("standalone mode did not start")
	}
	if _, ok := svc.cfg.Backend.(*metaBackend); !ok {
		t.Fatalf("standalone backend = %T", svc.cfg.Backend)
	}
	if d := svc.cfg.Backend.describe(); strings.Contains(d, "pass") || !strings.Contains(d, "manager@") {
		t.Fatalf("describe = %q", d)
	}
	svc.Stop()
	// Embedded mode is refused, even with every URL configured: a directory
	// created through a mount cannot be identified atomically.
	if svc := a.buildSmartFolders(Config{FUSEMount: t.TempDir(), MetaURL: "redis://manager@127.0.0.1:1/1",
		OverviewMetaURL: "redis://127.0.0.1:1/0", StateFile: state}, smartOn); svc != nil {
		svc.Stop()
		t.Fatal("embedded mode started smart folders")
	}
}

type fakeLeaser struct {
	mu      sync.Mutex
	refuse  bool
	block   chan struct{}
	entered chan struct{}
	asked   []string
	release int
}

func (f *fakeLeaser) Checkout(ctx context.Context, projects []string) (func(), <-chan struct{}, error) {
	f.mu.Lock()
	f.asked = append(f.asked, projects...)
	refuse, block, entered := f.refuse, f.block, f.entered
	f.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if block != nil {
		<-block
	}
	if refuse {
		return nil, nil, &ProjectLeasedError{Project: projects[0], Holder: "john@mac"}
	}
	return func() { f.mu.Lock(); f.release++; f.mu.Unlock() }, nil, nil
}

func (f *fakeLeaser) snapshot() ([]string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.asked...), f.release
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func writeVolumeFile(t *testing.T, root, rel string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(rel), 0o644); err != nil {
		t.Fatal(err)
	}
}

type svcOpts struct {
	root     string
	state    string
	leaser   ProjectLeaser
	clock    *testClock
	rdb      *redis.Client
	backend  smartFoldersBackend
	maxViews int
}

func newTestSmartFolders(t *testing.T, o svcOpts) *smartFoldersService {
	t.Helper()
	if o.rdb == nil {
		o.rdb = redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	}
	if o.backend == nil {
		o.backend = &osBackend{root: o.root, mountCheck: func(string) error { return nil }}
	}
	if o.state == "" {
		o.state = filepath.Join(t.TempDir(), smartFoldersStateName)
	}
	if o.maxViews == 0 {
		o.maxViews = 256
	}
	cfg := smartFoldersConfig{
		Settings:  smartFoldersSettings{Root: "Recents", MaxViews: o.maxViews, CollectionsRoot: "Collections"},
		Backend:   o.backend,
		Redis:     o.rdb,
		StatePath: o.state,
		Leaser:    func() ProjectLeaser { return o.leaser },
		Interval:  time.Hour, // cycles are driven by kick() or called directly
		StopWait:  2 * time.Second,
	}
	if o.clock != nil {
		cfg.Now = o.clock.now
	}
	svc, err := newSmartFoldersService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// viewTree maps "<folder>/<name>" to the volume path each link resolves to,
// failing on any link that is absolute or does not resolve.
func viewTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	base := filepath.Join(root, "Recents")
	folders, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return out
		}
		t.Fatal(err)
	}
	for _, f := range folders {
		if !f.IsDir() {
			continue
		}
		ents, err := os.ReadDir(filepath.Join(base, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			if e.Type()&os.ModeSymlink == 0 {
				continue
			}
			p := filepath.Join(base, f.Name(), e.Name())
			text, err := os.Readlink(p)
			if errors.Is(err, os.ErrNotExist) {
				// The reconciler renamed or removed the entry after the
				// ReadDir (its hidden temp links are renamed into place).
				// Record it so waitForTree polls again; an exact comparison
				// fails if the tree never settles.
				out[f.Name()+"/"+e.Name()] = "<vanished>"
				continue
			}
			if err != nil || filepath.IsAbs(text) {
				t.Fatalf("%s -> %q (%v): not a relative link", p, text, err)
			}
			if _, err := os.Stat(p); err != nil {
				// A link dangles between a user's rename and the next
				// reconcile. Record it so waitForTree keeps polling and an
				// exact comparison fails if it never resolves.
				out[f.Name()+"/"+e.Name()] = "<dangling " + text + ">"
				continue
			}
			rel, _ := filepath.Rel(root, filepath.Join(filepath.Dir(p), text))
			out[f.Name()+"/"+e.Name()] = filepath.ToSlash(rel)
		}
	}
	return out
}

// A user's own Recents stops everything before any lease is checked out,
// and is looked at again only after ten minutes.
func TestSmartFoldersForeignRootTakesNoLease(t *testing.T) {
	root := t.TempDir()
	writeVolumeFile(t, root, "Proj/a.mov")
	writeVolumeFile(t, root, "Recents/mine.txt")
	leaser := &fakeLeaser{}
	clk := &testClock{t: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
	svc := newTestSmartFolders(t, svcOpts{root: root, leaser: leaser, clock: clk})
	defer svc.cfg.Redis.Close()
	svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Proj/a.mov"})
	for i := 0; i < 3; i++ {
		svc.cycle(context.Background())
	}
	if asked, _ := leaser.snapshot(); len(asked) != 0 {
		t.Fatalf("checked out %v under a user's Recents", asked)
	}
	if !svc.rootRetryAt.Equal(clk.now().Add(smartFoldersRootRecheck)) {
		t.Fatalf("rootRetryAt = %v", svc.rootRetryAt)
	}
	if ents, _ := os.ReadDir(filepath.Join(root, "Recents")); len(ents) != 1 {
		t.Fatal("wrote into a user's Recents")
	}
	// The user renames their folder; after the recheck delay the view appears.
	if err := os.Rename(filepath.Join(root, "Recents"), filepath.Join(root, "Mine")); err != nil {
		t.Fatal(err)
	}
	svc.cycle(context.Background())
	if got := viewTree(t, root); len(got) != 0 {
		t.Fatal("rechecked before the delay")
	}
	clk.add(smartFoldersRootRecheck + time.Second)
	svc.cycle(context.Background())
	if got := viewTree(t, root); !reflect.DeepEqual(got, map[string]string{"Proj/a.mov": "Proj/a.mov"}) {
		t.Fatalf("after recheck: %v", got)
	}
}

// A refused lease backs off exponentially instead of retrying every cycle.
func TestSmartFoldersLeaseRefusedBacksOff(t *testing.T) {
	root := t.TempDir()
	writeVolumeFile(t, root, "Proj/a.mov")
	leaser := &fakeLeaser{refuse: true}
	clk := &testClock{t: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
	svc := newTestSmartFolders(t, svcOpts{root: root, leaser: leaser, clock: clk})
	svc.cfg.Interval = 30 * time.Second
	defer svc.cfg.Redis.Close()
	svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Proj/a.mov"})

	attempts := func() int { a, _ := leaser.snapshot(); return len(a) }
	svc.cycle(context.Background())
	svc.cycle(context.Background())
	if attempts() != 1 {
		t.Fatalf("retried within the backoff: %d checkouts", attempts())
	}
	clk.add(31 * time.Second)
	svc.cycle(context.Background())
	clk.add(31 * time.Second) // second backoff is 60s
	svc.cycle(context.Background())
	if attempts() != 2 {
		t.Fatalf("checkouts = %d, want 2", attempts())
	}
	if asked, _ := leaser.snapshot(); asked[0] != "path:/Recents/Proj" {
		t.Fatalf("checked out %v", asked)
	}
	if got := viewTree(t, root); len(got) != 0 {
		t.Fatalf("view written despite the refused lease: %v", got)
	}
	leaser.mu.Lock()
	leaser.refuse = false
	leaser.mu.Unlock()
	clk.add(time.Minute)
	svc.cycle(context.Background())
	if got := viewTree(t, root); !reflect.DeepEqual(got, map[string]string{"Proj/a.mov": "Proj/a.mov"}) {
		t.Fatalf("after the lease is granted: %v", got)
	}
	if _, released := leaser.snapshot(); released != 1 {
		t.Fatalf("lease released %d times", released)
	}
}

// Events from any client cannot create views for folders that do not exist,
// nor grow the state without bound.
func TestSmartFoldersFloodOfFakeEvents(t *testing.T) {
	root := t.TempDir()
	writeVolumeFile(t, root, "Real/a.mov")
	svc := newTestSmartFolders(t, svcOpts{root: root, leaser: &fakeLeaser{}, maxViews: 8})
	defer svc.cfg.Redis.Close()
	for i := 0; i < 200000; i++ {
		svc.tracker.Apply(smartfolders.Event{Op: "create", Path: fmt.Sprintf("Fake%d/x.mov", i)})
	}
	for i := 0; i < 5000; i++ {
		svc.tracker.Apply(smartfolders.Event{Op: "create", Path: fmt.Sprintf("Real/missing%d.mov", i)})
	}
	svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Real/a.mov"})
	svc.cycle(context.Background())
	if got := viewTree(t, root); !reflect.DeepEqual(got, map[string]string{"Real/a.mov": "Real/a.mov"}) {
		t.Fatalf("views = %v", got)
	}
	ents, _ := os.ReadDir(filepath.Join(root, "Recents"))
	if len(ents) != 1 {
		t.Fatalf("%d views", len(ents))
	}
	fi, err := os.Stat(svc.cfg.StatePath)
	if err != nil || fi.Size() > 16<<10 {
		t.Fatalf("state file: %v, %d bytes", err, fi.Size())
	}
}

// Ownership survives a restart through the state file; a corrupt state file
// disables the feature instead of guessing.
func TestSmartFoldersOwnershipPersists(t *testing.T) {
	root := t.TempDir()
	writeVolumeFile(t, root, "Proj/a.mov")
	state := filepath.Join(t.TempDir(), smartFoldersStateName)
	svc := newTestSmartFolders(t, svcOpts{root: root, leaser: &fakeLeaser{}, state: state})
	svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Proj/a.mov"})
	svc.cycle(context.Background())
	svc.cfg.Redis.Close()
	var st smartFoldersState
	data, _ := os.ReadFile(state)
	if err := json.Unmarshal(data, &st); err != nil || st.Ownership == nil || st.Ownership.Root == nil ||
		st.Ownership.Views["Proj"].Links["a.mov"].Ino == 0 {
		t.Fatalf("state = %s (%v)", data, err)
	}
	leaser := &fakeLeaser{}
	svc2 := newTestSmartFolders(t, svcOpts{root: root, leaser: leaser, state: state})
	defer svc2.cfg.Redis.Close()
	svc2.cycle(context.Background())
	if asked, _ := leaser.snapshot(); len(asked) != 0 {
		t.Fatalf("restart rewrote an unchanged view (checkouts %v)", asked)
	}
	if got := viewTree(t, root); len(got) != 1 {
		t.Fatalf("views after restart = %v", got)
	}
	if err := os.WriteFile(state, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newSmartFoldersService(smartFoldersConfig{Settings: smartFoldersSettings{Root: "Recents", MaxViews: 1},
		Backend: svc2.cfg.Backend, Redis: svc2.cfg.Redis, StatePath: state}); err == nil {
		t.Fatal("corrupt state accepted")
	}
}

// Stop returns within its bound even when a cycle is stuck in a lease call.
func TestSmartFoldersStopIsBounded(t *testing.T) {
	root := t.TempDir()
	writeVolumeFile(t, root, "Proj/a.mov")
	leaser := &fakeLeaser{block: make(chan struct{}), entered: make(chan struct{}, 1)}
	svc := newTestSmartFolders(t, svcOpts{root: root, leaser: leaser})
	svc.cfg.StopWait = 200 * time.Millisecond
	svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Proj/a.mov"})
	svc.Start()
	svc.kick()
	select {
	case <-leaser.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cycle never reached the lease")
	}
	start := time.Now()
	svc.Stop()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Stop took %s", d)
	}
	close(leaser.block) // let the stuck cycle finish before the temp dirs go
	svc.done.Wait()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func publisher(t *testing.T, ctx context.Context, rdb *redis.Client) func(...metadata.MetadataEvent) {
	return func(evs ...metadata.MetadataEvent) {
		t.Helper()
		for _, ev := range evs {
			payload, err := json.Marshal(ev)
			if err != nil {
				t.Fatal(err)
			}
			if err := rdb.Publish(ctx, metadata.SubscribeChannel, string(payload)).Err(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(5 * time.Millisecond) // distinct receipt times
		}
	}
}

func openTestRedis(t *testing.T, raw string) *redis.Client {
	t.Helper()
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return redis.NewClient(opts)
}

// TestSmartFoldersRedisIntegration (embedded mode, FUSE-style backend): events
// published on juicemount:metadata in the clients' payload format become
// /Recents/<folder>/ views in a temp "volume"; names stay put; a restart
// rebuilds nothing that is unchanged. Set JM_TEST_MANAGER_REDIS (pub/sub
// only; no keys are written).
func TestSmartFoldersRedisIntegration(t *testing.T) {
	metaURL := os.Getenv("JM_TEST_MANAGER_REDIS")
	if metaURL == "" {
		t.Skip("set JM_TEST_MANAGER_REDIS to run the smart-folders Redis integration test")
	}
	root := t.TempDir()
	for _, f := range []string{
		"Projects/Interviews/clip.mov", "Projects/B-Roll/clip.mov", "Projects/edit.prproj",
		"Projects/old.mov", "Music/song.wav", "Music/gone.wav", "Music/sentinel.wav",
	} {
		writeVolumeFile(t, root, f)
	}
	state := filepath.Join(t.TempDir(), smartFoldersStateName)
	leaser := &fakeLeaser{}
	svc := newTestSmartFolders(t, svcOpts{root: root, leaser: leaser, state: state, rdb: openTestRedis(t, metaURL)})
	svc.Start()
	stopped := false
	defer func() {
		if !stopped {
			svc.Stop()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	select {
	case <-svc.ready:
	case <-ctx.Done():
		t.Fatal("subscription never became ready")
	}
	pub := openTestRedis(t, metaURL)
	defer pub.Close()
	publish := publisher(t, ctx, pub)
	publish(
		metadata.MetadataEvent{Op: "create", Path: "Projects", IsDir: true},
		metadata.MetadataEvent{Op: "create", Path: "Projects/old.mov", Size: 1, Mtime: 1, Inode: 10},
		metadata.MetadataEvent{Op: "create", Path: "Projects/Interviews/clip.mov", Size: 1, Mtime: 1, Inode: 11},
		metadata.MetadataEvent{Op: "create", Path: "Projects/B-Roll/clip.mov", Size: 1, Mtime: 1, Inode: 12},
		metadata.MetadataEvent{Op: "create", Path: "Projects/.DS_Store", Inode: 13},
		metadata.MetadataEvent{Op: "create", Path: "Projects/._clip.mov", Inode: 14},
		metadata.MetadataEvent{Op: "create", Path: ".trash/2026-11-02-12/15-Projects|x.mov", Inode: 15},
		metadata.MetadataEvent{Op: "create", Path: ".juicemount/derivatives/x.jpg", Inode: 16},
		metadata.MetadataEvent{Op: "create", Path: "Recents/Projects/loop.mov", Inode: 17},
		metadata.MetadataEvent{Op: "create", Path: "NoSuchFolder/x.mov", Inode: 21},
		metadata.MetadataEvent{Op: "create", Path: "Projects/.edit.prproj.tmp", Inode: 18},
		metadata.MetadataEvent{Op: "rename", Path: "Projects/edit.prproj", OldPath: "Projects/.edit.prproj.tmp", Inode: 18},
		metadata.MetadataEvent{Op: "create", Path: "Music/gone.wav", Inode: 19},
		metadata.MetadataEvent{Op: "reconcile", Path: "Music/song.wav"},
		metadata.MetadataEvent{Op: "create", Path: "Music/vanished.wav", Inode: 20}, // never on disk
		metadata.MetadataEvent{Op: "delete", Path: "Projects/old.mov", Inode: 10},
		metadata.MetadataEvent{Op: "delete", Path: "Music/gone.wav", Inode: 19},
		metadata.MetadataEvent{Op: "create", Path: "Music/sentinel.wav", Inode: 22},
	)
	waitFor(t, "the last event", func() bool {
		for _, e := range svc.tracker.Recent("Music") {
			if e.Path == "Music/sentinel.wav" {
				return true
			}
		}
		return false
	})
	want := map[string]string{
		"Projects/edit.prproj":           "Projects/edit.prproj",
		"Projects/clip.mov":              "Projects/B-Roll/clip.mov",
		"Projects/clip (Interviews).mov": "Projects/Interviews/clip.mov",
		"Music/song.wav":                 "Music/song.wav",
		"Music/sentinel.wav":             "Music/sentinel.wav",
	}
	waitForTree(t, svc, root, want)
	if svc.tracker.Folders()[0] != "Music" || len(svc.tracker.Folders()) != 2 {
		t.Fatalf("tracked folders = %v", svc.tracker.Folders())
	}
	asked, _ := leaser.snapshot()
	sort.Strings(asked)
	if i := sort.SearchStrings(asked, "path:/Recents/Projects"); i == len(asked) || asked[i] != "path:/Recents/Projects" {
		t.Fatalf("no checkout of path:/Recents/Projects (asked %v)", asked)
	}

	// A newer file named clip.mov does not take the name from the one that
	// has it: the newcomer gets the suffix.
	writeVolumeFile(t, root, "Projects/Selects/clip.mov")
	publish(metadata.MetadataEvent{Op: "create", Path: "Projects/Selects/clip.mov", Inode: 23})
	want["Projects/clip (Selects).mov"] = "Projects/Selects/clip.mov"
	waitForTree(t, svc, root, want)

	// A top-level folder rename moves its entries; the old view stays, empty.
	if err := os.Rename(filepath.Join(root, "Music"), filepath.Join(root, "Audio")); err != nil {
		t.Fatal(err)
	}
	publish(metadata.MetadataEvent{Op: "rename", Path: "Audio", OldPath: "Music", IsDir: true})
	delete(want, "Music/song.wav")
	delete(want, "Music/sentinel.wav")
	want["Audio/song.wav"] = "Audio/song.wav"
	want["Audio/sentinel.wav"] = "Audio/sentinel.wav"
	waitForTree(t, svc, root, want)
	if ents, err := os.ReadDir(filepath.Join(root, "Recents", "Music")); err != nil || len(ents) != 0 {
		t.Fatalf("emptied view Recents/Music: %v entries, %v", len(ents), err)
	}

	svc.Stop()
	stopped = true
	// Restart: unchanged views are not touched (no checkout at all).
	leaser2 := &fakeLeaser{}
	svc2 := newTestSmartFolders(t, svcOpts{root: root, leaser: leaser2, state: state, rdb: openTestRedis(t, metaURL)})
	svc2.Start()
	defer svc2.Stop()
	svc2.kick()
	time.Sleep(300 * time.Millisecond)
	if got := viewTree(t, root); !reflect.DeepEqual(got, want) {
		t.Fatalf("after restart: %v", got)
	}
	if asked, _ := leaser2.snapshot(); len(asked) != 0 {
		t.Fatalf("restart checked out %v for unchanged views", asked)
	}
}

func waitForTree(t *testing.T, svc *smartFoldersService, root string, want map[string]string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var got map[string]string
	for time.Now().Before(deadline) {
		svc.kick()
		time.Sleep(50 * time.Millisecond)
		if got = viewTree(t, root); reflect.DeepEqual(got, want) {
			return
		}
	}
	t.Fatalf("views = %v\nwant    %v", got, want)
}

// ---- standalone mode: the JuiceFS metadata backend --------------------------

// standaloneTestURL is JM_TEST_SMARTFOLDERS_META_REDIS: a caller-owned,
// disposable, unauthenticated Redis database that these tests flush and
// format throwaway JuiceFS volumes in. It must not be shared with any other
// test (the jfsmeta package has its own, JM_TEST_SMARTFOLDERS_JFS_REDIS):
// go test runs packages in parallel.
func standaloneTestURL(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("JM_TEST_SMARTFOLDERS_META_REDIS")
	if raw == "" {
		t.Skip("set JM_TEST_SMARTFOLDERS_META_REDIS to a disposable Redis database of its own (it is flushed)")
	}
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Username != "" || opts.Password != "" {
		t.Skip("the standalone test needs an unauthenticated disposable Redis")
	}
	return fmt.Sprintf("redis://%s/%d", opts.Addr, opts.DB)
}

func formatTestVolume(t *testing.T, url string) meta.Meta {
	t.Helper()
	rdb := openTestRedis(t, url)
	defer rdb.Close()
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := openTestRedis(t, url)
		defer c.Close()
		_ = c.FlushDB(context.Background()).Err()
	})
	conf := meta.DefaultConf()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	m := meta.NewClient(url, conf)
	if err := m.Init(&meta.Format{Name: "mgrsftest", UUID: "00000000-0000-4000-8000-00000000abcd", Storage: "file",
		Bucket: t.TempDir() + "/", BlockSize: 4096, TrashDays: 1}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Load(true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	return m
}

func metaMkdirAll(t *testing.T, m meta.Meta, p string) meta.Ino {
	t.Helper()
	ctx := meta.Background()
	ino := meta.RootInode
	for _, c := range strings.Split(p, "/") {
		var next meta.Ino
		var attr meta.Attr
		st := m.Lookup(ctx, ino, c, &next, &attr, false)
		if st == syscall.ENOENT {
			st = m.Mkdir(ctx, ino, c, 0o755, 0o022, 0, &next, &attr)
		}
		if st != 0 {
			t.Fatalf("mkdir %s: %v", p, st)
		}
		ino = next
	}
	return ino
}

func metaWriteFile(t *testing.T, m meta.Meta, p string) {
	t.Helper()
	dir := metaMkdirAll(t, m, path.Dir(p))
	var ino meta.Ino
	var attr meta.Attr
	if st := m.Create(meta.Background(), dir, path.Base(p), 0o644, 0o022, syscall.O_EXCL, &ino, &attr); st != 0 {
		t.Fatalf("create %s: %v", p, st)
	}
	_ = m.Close(meta.Background(), ino)
}

// metaTree is viewTree for the metadata backend, resolving links by hand.
func metaTree(t *testing.T, m meta.Meta) map[string]string {
	t.Helper()
	return metaTreeAt(t, m, "Recents")
}

// metaTreeAt is metaTree for the smart-folder root `root`.
func metaTreeAt(t *testing.T, m meta.Meta, root string) map[string]string {
	t.Helper()
	ctx := meta.Background()
	out := map[string]string{}
	var recents meta.Ino
	var attr meta.Attr
	if st := m.Lookup(ctx, meta.RootInode, root, &recents, &attr, false); st == syscall.ENOENT {
		return out
	}
	var views []*meta.Entry
	if st := m.Readdir(ctx, recents, 1, &views); st != 0 {
		t.Fatal(st)
	}
	for _, v := range views {
		if n := string(v.Name); n == "." || n == ".." || v.Attr.Typ != meta.TypeDirectory {
			continue
		}
		var links []*meta.Entry
		if st := m.Readdir(ctx, v.Inode, 1, &links); st != 0 {
			t.Fatal(st)
		}
		for _, l := range links {
			if l.Attr.Typ != meta.TypeSymlink {
				continue
			}
			var buf []byte
			if st := m.ReadLink(ctx, l.Inode, &buf); st != 0 {
				t.Fatal(st)
			}
			text := string(buf)
			if strings.HasPrefix(text, "/") {
				t.Fatalf("absolute link %q", text)
			}
			target := path.Clean(path.Join(root, string(v.Name), text))
			cur := meta.RootInode
			for _, c := range strings.Split(target, "/") {
				var next meta.Ino
				if st := m.Lookup(ctx, cur, c, &next, &attr, false); st != 0 {
					t.Fatalf("%s -> %s does not resolve: %v", l.Name, text, st)
				}
				cur = next
			}
			if attr.Typ != meta.TypeFile {
				t.Fatalf("%s -> %s is not a file", l.Name, text)
			}
			out[string(v.Name)+"/"+string(l.Name)] = target
		}
	}
	return out
}

// TestSmartFoldersStandaloneMetadataBackend runs the Manager component in
// standalone mode: no mount, views written through a JuiceFS metadata client
// that holds no session.
func TestSmartFoldersStandaloneMetadataBackend(t *testing.T) {
	url := standaloneTestURL(t)
	m := formatTestVolume(t, url)
	for _, f := range []string{"Projects/a.mov", "Projects/sub/b.mov", "Music/x.wav"} {
		metaWriteFile(t, m, f)
	}
	leaser := &fakeLeaser{}
	backend := &metaBackend{url: url, open: jfsmeta.Open}
	svc := newTestSmartFolders(t, svcOpts{leaser: leaser, rdb: openTestRedis(t, url), backend: backend})
	svc.Start()
	stopped := false
	defer func() {
		if !stopped {
			svc.Stop()
		}
	}()
	select {
	case <-svc.ready:
	case <-time.After(20 * time.Second):
		t.Fatal("subscription never became ready")
	}
	pub := openTestRedis(t, url)
	defer pub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	publisher(t, ctx, pub)(
		metadata.MetadataEvent{Op: "create", Path: "Projects/a.mov", Inode: 1},
		metadata.MetadataEvent{Op: "create", Path: "Projects/sub/b.mov", Inode: 2},
		metadata.MetadataEvent{Op: "create", Path: "Music/x.wav", Inode: 3},
		metadata.MetadataEvent{Op: "create", Path: "Ghost/y.wav", Inode: 4},
	)
	want := map[string]string{
		"Projects/a.mov": "Projects/a.mov",
		"Projects/b.mov": "Projects/sub/b.mov",
		"Music/x.wav":    "Music/x.wav",
	}
	deadline := time.Now().Add(15 * time.Second)
	var got map[string]string
	for time.Now().Before(deadline) {
		svc.kick()
		time.Sleep(50 * time.Millisecond)
		if got = metaTree(t, m); reflect.DeepEqual(got, want) {
			break
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("views = %v, want %v", got, want)
	}
	// No JuiceFS session (its refresh can exit the process); see jfsmeta.
	sessions, err := m.ListSessions()
	if err != nil || len(sessions) != 0 {
		t.Fatalf("sessions while running = %d, %v", len(sessions), err)
	}
	svc.Stop()
	stopped = true
}

// The event path never connects the metadata backend (connecting can take
// as long as jfsmeta.OpenTimeout); only the reconcile loop does.
func TestSmartFoldersEventPathNeverConnects(t *testing.T) {
	opened := make(chan struct{}, 1)
	backend := &metaBackend{url: "redis://127.0.0.1:1/1", open: func(string) (*jfsmeta.FS, error) {
		opened <- struct{}{}
		time.Sleep(time.Second)
		return nil, errors.New("unreachable")
	}}
	svc := newTestSmartFolders(t, svcOpts{backend: backend, leaser: &fakeLeaser{}})
	defer svc.cfg.Redis.Close()
	start := time.Now()
	for i := 0; i < 100; i++ {
		svc.tracker.Apply(smartfolders.Event{Op: "create", Path: fmt.Sprintf("F%d/x.mov", i)})
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("event intake took %s", d)
	}
	select {
	case <-opened:
		t.Fatal("the event path connected the backend")
	default:
	}
	svc.cycle(context.Background())
	select {
	case <-opened:
	default:
		t.Fatal("the reconcile loop did not connect")
	}
}

// TestSmartFoldersThroughRESPProxyEnforce runs the standalone backend as
// the `manager` principal through a real jm-respproxy in enforce mode with
// the L2 lease engine: Recents, the view and the links are admitted
// (create, retarget, remove) without a JuiceFS session; while a Mac holds
// the view's project lease the Manager refuses cleanly and writes nothing;
// after release the next run completes.
func TestSmartFoldersThroughRESPProxyEnforce(t *testing.T) {
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
		SweepEvery: time.Hour, FlushEvery: 50 * time.Millisecond})
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

	managerLeases, err := NewRedisLeaseClient(viaProxy("manager"), "")
	if err != nil {
		t.Fatal(err)
	}
	backend := &metaBackend{url: viaProxy("manager"), open: jfsmeta.Open}
	clk := &testClock{t: time.Now()}
	svc := newTestSmartFolders(t, svcOpts{backend: backend, leaser: NewProjectLeaser(managerLeases), clock: clk})
	defer svc.Stop()
	ctx := context.Background()
	run := func() {
		t.Helper()
		svc.cycle(ctx)
	}
	assertTree := func(want map[string]string) {
		t.Helper()
		if got := metaTree(t, m); !reflect.DeepEqual(got, want) {
			t.Fatalf("views = %v, want %v (log: %v)", got, want, svc.lastLog)
		}
	}

	// Create: Recents (volume root), the view and a link.
	svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Projects/a.mov"})
	run()
	assertTree(map[string]string{"Projects/a.mov": "Projects/a.mov"})
	if sessions, err := m.ListSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("the backend holds %d sessions (%v)", len(sessions), err)
	}

	// Retarget: the file moves, its link keeps the name and is exchanged.
	var ino meta.Ino
	var attr meta.Attr
	proj := metaMkdirAll(t, m, "Projects")
	sub := metaMkdirAll(t, m, "Projects/sub")
	if st := m.Rename(meta.Background(), proj, "a.mov", sub, "a.mov", 0, &ino, &attr); st != 0 {
		t.Fatal(st)
	}
	svc.tracker.Apply(smartfolders.Event{Op: "rename", OldPath: "Projects/a.mov", Path: "Projects/sub/a.mov"})
	run()
	assertTree(map[string]string{"Projects/a.mov": "Projects/sub/a.mov"})

	// Remove: the file is deleted, its link goes.
	if st := m.Unlink(meta.Background(), sub, "a.mov", true); st != 0 {
		t.Fatal(st)
	}
	svc.tracker.Apply(smartfolders.Event{Op: "delete", Path: "Projects/sub/a.mov"})
	run()
	assertTree(map[string]string{})

	// A Mac holds the view's project: the Manager refuses and writes nothing.
	macLeases, err := NewRedisLeaseClient(viaProxy("mac1"), "")
	if err != nil {
		t.Fatal(err)
	}
	grant, err := macLeases.Acquire(ctx, "path:/Recents/Projects", "checkout", time.Hour)
	if err != nil || (grant.Status != "GRANTED" && grant.Status != "RENEWED") {
		t.Fatalf("mac checkout = %+v, %v", grant, err)
	}
	svc.tracker.Apply(smartfolders.Event{Op: "create", Path: "Projects/b.mov"})
	run()
	assertTree(map[string]string{})
	if !strings.Contains(svc.lastLog["Projects"], "mac1") {
		t.Fatalf("refusal not reported: %q", svc.lastLog["Projects"])
	}
	if v := svc.own.Views["Projects"]; len(v.Temps) != 0 || len(v.Links) != 0 {
		t.Fatalf("records changed while refused: %+v", v)
	}

	// Released: after the backoff the next run completes.
	if st, err := macLeases.Release(ctx, "path:/Recents/Projects", grant.Epoch); err != nil || st != "RELEASED" {
		t.Fatalf("release = %q, %v", st, err)
	}
	clk.add(time.Hour)
	run()
	assertTree(map[string]string{"Projects/b.mov": "Projects/b.mov"})
}

type panickingBackend struct {
	smartFoldersBackend
	calls int
}

func (p *panickingBackend) acquire() (smartfolders.FS, func(), error) {
	p.calls++
	panic("internal error")
}

// A panic in a cycle turns smart folders off for the process instead of
// taking the Manager down, and later cycles do nothing.
func TestSmartFoldersPanicDisablesFeatureNotManager(t *testing.T) {
	root := t.TempDir()
	pb := &panickingBackend{smartFoldersBackend: &osBackend{root: root, mountCheck: func(string) error { return nil }}}
	svc := newTestSmartFolders(t, svcOpts{root: root, backend: pb, leaser: &fakeLeaser{}})
	defer svc.cfg.Redis.Close()
	svc.cycle(context.Background()) // must not panic
	svc.cycle(context.Background())
	if !svc.disabled.Load() || pb.calls != 1 {
		t.Fatalf("disabled=%v calls=%d", svc.disabled.Load(), pb.calls)
	}
}

// A state file saved mid-run (a view with temps but no links map) loads
// without a nil map.
func TestSmartFoldersLoadNormalisesRecords(t *testing.T) {
	state := filepath.Join(t.TempDir(), smartFoldersStateName)
	data := `{"version":2,"ownership":{"root":{"ino":5},"views":{"Proj":{"ino":6,"temps":{".juicemount-smart-folder.tmp-1":{"for":"x.mov","target":"Proj/a.mov","text":"../../Proj/a.mov","ino":7}}}}}}`
	if err := os.WriteFile(state, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := newTestSmartFolders(t, svcOpts{root: t.TempDir(), state: state, leaser: &fakeLeaser{}})
	defer svc.cfg.Redis.Close()
	if v := svc.own.Views["Proj"]; v == nil || v.Links == nil || v.Temps == nil {
		t.Fatalf("records not normalised: %+v", v)
	}
}

type panickingLookup struct {
	smartFoldersBackend
}

func (panickingLookup) lookup() (smartfolders.FS, func(), error) { panic("internal error") }

// A panic on the event path (tracker -> folderOK -> backend lookup) turns
// smart folders off for the process instead of taking the Manager down.
func TestSmartFoldersEventPathPanicDisablesFeature(t *testing.T) {
	metaURL := os.Getenv("JM_TEST_MANAGER_REDIS")
	if metaURL == "" {
		t.Skip("set JM_TEST_MANAGER_REDIS to run the smart-folders event-path test")
	}
	root := t.TempDir()
	backend := panickingLookup{&osBackend{root: root, mountCheck: func(string) error { return nil }}}
	svc := newTestSmartFolders(t, svcOpts{root: root, backend: backend, leaser: &fakeLeaser{}, rdb: openTestRedis(t, metaURL)})
	svc.Start()
	defer svc.Stop()
	select {
	case <-svc.ready:
	case <-time.After(20 * time.Second):
		t.Fatal("subscription never became ready")
	}
	pub := openTestRedis(t, metaURL)
	defer pub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	publisher(t, ctx, pub)(metadata.MetadataEvent{Op: "create", Path: "NewFolder/x.mov"})
	waitFor(t, "smart folders to disable themselves", svc.disabled.Load)
}
