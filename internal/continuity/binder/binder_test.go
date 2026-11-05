//go:build linux || darwin

package binder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

// Test Redis: JM_TEST_CONTINUITY_REDIS names a disposable database of this
// package's own (it is flushed); otherwise each test starts a private
// redis-server (JM_TEST_REDIS_SERVER, or the Redis 7.4.11 the CI image and
// the dev tools ship) on a free loopback port. Pub/sub uses a channel per
// test, so a shared server carries no other test's events.
var redisServers = []string{"/opt/redis-7.4.11/bin/redis-server", "/opt/jm-ci-tools/redis/bin/redis-server"}

func testRedisURL(t *testing.T) string {
	t.Helper()
	if u := os.Getenv("JM_TEST_CONTINUITY_REDIS"); u != "" {
		flush := func() {
			c := openRedis(t, u)
			defer c.Close()
			if err := c.FlushDB(context.Background()).Err(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
		flush()
		t.Cleanup(flush)
		return u
	}
	bin := os.Getenv("JM_TEST_REDIS_SERVER")
	for _, p := range redisServers {
		if bin != "" {
			break
		}
		if _, err := os.Stat(p); err == nil {
			bin = p
		}
	}
	if bin == "" {
		t.Skip("set JM_TEST_CONTINUITY_REDIS (a disposable database) or JM_TEST_REDIS_SERVER (a redis-server binary)")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	cmd := exec.Command(bin, "--bind", "127.0.0.1", "--port", fmt.Sprint(port), "--save", "", "--appendonly", "no", "--dir", t.TempDir())
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	u := fmt.Sprintf("redis://127.0.0.1:%d/0", port)
	c := openRedis(t, u)
	defer c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for c.Ping(context.Background()).Err() != nil {
		if time.Now().After(deadline) {
			t.Fatal("private redis-server did not answer")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return u
}

func openRedis(t *testing.T, raw string) *redis.Client {
	t.Helper()
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return redis.NewClient(opts)
}

const testUUID = "00000000-0000-4000-8000-0000000000c1"

type fixture struct {
	t     *testing.T
	url   string
	admin meta.Meta
	ctx   meta.Context
	store *catalog.Store
	lib   continuity.LibraryID
	b     *Binder
	stop  func()
	sent  int
}

// newFixture formats a throwaway volume (trash on, as deployed) and starts
// a binder over a fresh catalog. run=false leaves the worker stopped.
func newFixture(t *testing.T, queue int, run bool) *fixture {
	t.Helper()
	url := testRedisURL(t)
	conf := meta.DefaultConf()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	admin := meta.NewClient(url, conf)
	if err := admin.Init(&meta.Format{Name: "binder", UUID: testUUID, Storage: "file", Bucket: t.TempDir() + "/", BlockSize: 4096, TrashDays: 1}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Load(true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Shutdown() })
	f := &fixture{t: t, url: url, admin: admin, ctx: meta.Background()}
	f.store, f.lib = newStore(t)
	b, err := New(Config{Store: f.store, QueueSize: queue, Connect: func() (Volume, error) { return jfsmeta.Open(url) }, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	f.b = b
	if run {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); b.Run(ctx) }()
		f.stop = func() { cancel(); <-done; _ = b.Close() }
		t.Cleanup(f.stop)
	}
	return f
}

func newStore(t *testing.T) (*catalog.Store, continuity.LibraryID) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "continuity")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	lib, err := continuity.NewLibraryID()
	if err != nil {
		t.Fatal(err)
	}
	store, err := catalog.Create(filepath.Join(dir, "catalog.sqlite"), lib)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, lib
}

func (f *fixture) resolve(p string) meta.Ino {
	f.t.Helper()
	ino := meta.RootInode
	if p == "." {
		return ino
	}
	for _, c := range strings.Split(p, "/") {
		var next meta.Ino
		var attr meta.Attr
		st := f.admin.Lookup(f.ctx, ino, c, &next, &attr, false)
		if st == syscall.ENOENT {
			st = f.admin.Mkdir(f.ctx, ino, c, 0o755, 0o022, 0, &next, &attr)
		}
		if st != 0 {
			f.t.Fatalf("resolve %s: %v", p, st)
		}
		ino = next
	}
	return ino
}

func (f *fixture) create(p string) uint64 {
	f.t.Helper()
	var ino meta.Ino
	var attr meta.Attr
	if st := f.admin.Create(f.ctx, f.resolve(path.Dir(p)), path.Base(p), 0o644, 0o022, syscall.O_EXCL, &ino, &attr); st != 0 {
		f.t.Fatalf("create %s: %v", p, st)
	}
	_ = f.admin.Close(f.ctx, ino)
	return uint64(ino)
}

func (f *fixture) rename(from, to string) {
	f.t.Helper()
	var ino meta.Ino
	var attr meta.Attr
	if st := f.admin.Rename(f.ctx, f.resolve(path.Dir(from)), path.Base(from), f.resolve(path.Dir(to)), path.Base(to), 0, &ino, &attr); st != 0 {
		f.t.Fatalf("rename %s %s: %v", from, to, st)
	}
}

func (f *fixture) link(ino uint64, p string) {
	f.t.Helper()
	var attr meta.Attr
	if st := f.admin.Link(f.ctx, meta.Ino(ino), f.resolve(path.Dir(p)), path.Base(p), &attr); st != 0 {
		f.t.Fatalf("link %s: %v", p, st)
	}
}

// unlink removes p: into the trash, or for good with skipTrash.
func (f *fixture) unlink(p string, skipTrash bool) {
	f.t.Helper()
	if st := f.admin.Unlink(f.ctx, f.resolve(path.Dir(p)), path.Base(p), skipTrash); st != 0 {
		f.t.Fatalf("unlink %s: %v", p, st)
	}
}

// feed offers events and waits until the worker has handled all of them.
func (f *fixture) feed(evs ...Event) {
	f.t.Helper()
	for _, ev := range evs {
		if !f.b.Offer(ev) {
			f.t.Fatalf("event %+v dropped", ev)
		}
		f.sent++
	}
	f.waitHandled(uint64(f.sent))
}

func (f *fixture) waitHandled(n uint64) {
	f.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for f.b.Stats().Handled < n {
		if time.Now().After(deadline) {
			f.t.Fatalf("handled %d of %d events: %+v", f.b.Stats().Handled, n, f.b.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *fixture) location(ino uint64) catalog.Location {
	f.t.Helper()
	loc, err := f.store.GetLocation(context.Background(), f.lib, LocationID(testUUID, ino))
	if err != nil {
		f.t.Fatalf("location of %d: %v", ino, err)
	}
	return loc
}

func (f *fixture) asset(id continuity.AssetID) catalog.Asset {
	f.t.Helper()
	a, err := f.store.GetAsset(context.Background(), f.lib, id)
	if err != nil {
		f.t.Fatalf("asset %s: %v", id, err)
	}
	return a
}

// events counts the catalog's operations (pages may be short; only an empty
// page ends the history).
func (f *fixture) events() int {
	f.t.Helper()
	var n int
	var after int64
	for {
		page, err := f.store.Events(context.Background(), f.lib, after, catalog.MaxPageSize)
		if err != nil {
			f.t.Fatal(err)
		}
		if len(page) == 0 {
			return n
		}
		n += len(page)
		after = page[len(page)-1].Receipt.Sequence
	}
}

// requireBound checks the inode's asset and active location, and its hint.
func (f *fixture) requireBound(ino uint64, hint string) continuity.AssetID {
	f.t.Helper()
	loc := f.location(ino)
	want := catalog.LocationValue{AssetID: AssetID(testUUID, ino), Source: "jfs:" + testUUID, Locator: fmt.Sprintf("ino:%d", ino)}
	if loc.Removed || loc.Value != want {
		f.t.Fatalf("location of %d = %+v, want active %+v", ino, loc, want)
	}
	if a := f.asset(want.AssetID); a.Label != hint {
		f.t.Fatalf("hint of %d = %q, want %q", ino, a.Label, hint)
	}
	return want.AssetID
}

func TestCreateBindsAssetAndLocation(t *testing.T) {
	f := newFixture(t, 0, true)
	ino := f.create("Projects/A/clip.mov")
	f.feed(Event{Op: "create", Path: "Projects/A/clip.mov", Inode: ino})
	f.requireBound(ino, "Projects/A/clip.mov")
	if f.events() != 1 {
		t.Fatalf("a bind is one operation, got %d", f.events())
	}
	// One leading slash is tolerated; the published inode is not trusted
	// (the path is resolved), and an update changes nothing.
	f.feed(Event{Op: "update", Path: "/Projects/A/clip.mov", Inode: 999})
	if f.events() != 1 {
		t.Fatalf("an unchanged file recorded %d operations", f.events())
	}
	if _, err := f.store.GetLocation(context.Background(), f.lib, LocationID(testUUID, 999)); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("event inode was trusted: %v", err)
	}
}

func TestIgnoresWhatIsNotAVisibleRegularFile(t *testing.T) {
	f := newFixture(t, 0, true)
	f.resolve("Projects/Dir")
	hiddenIno := f.create("Projects/.edit.tmp")
	var ino meta.Ino
	var attr meta.Attr
	if st := f.admin.Symlink(f.ctx, f.resolve("Projects"), "l", "A/clip.mov", &ino, &attr); st != 0 {
		t.Fatal(st)
	}
	f.feed(
		Event{Op: "create", Path: "Projects/Dir", IsDir: true},
		Event{Op: "create", Path: "Projects/Dir"}, // no is_dir: resolved
		Event{Op: "create", Path: "Projects/l"},
		Event{Op: "create", Path: "Projects/.edit.tmp", Inode: hiddenIno},
		Event{Op: "create", Path: "Projects/missing.mov", Inode: 77},
		Event{Op: "create", Path: "Projects/../etc"},
		Event{Op: "chmod", Path: "Projects/Dir"},
	)
	if n := f.events(); n != 0 {
		t.Fatalf("recorded %d operations", n)
	}
	if s := f.b.Stats(); s.Ignored != 7 || s.Failed != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestRenameKeepsAssetAndMovesHint(t *testing.T) {
	f := newFixture(t, 0, true)
	ino := f.create("Projects/A/.clip.mov.tmp")
	f.rename("Projects/A/.clip.mov.tmp", "Projects/A/clip.mov")
	f.feed(
		Event{Op: "create", Path: "Projects/A/.clip.mov.tmp", Inode: ino},
		Event{Op: "rename", Path: "Projects/A/clip.mov", OldPath: "Projects/A/.clip.mov.tmp", Inode: ino},
	)
	id := f.requireBound(ino, "Projects/A/clip.mov")
	created := f.location(ino).CreatedSequence
	f.rename("Projects/A/clip.mov", "Projects/B/final.mov")
	f.feed(Event{Op: "rename", Path: "Projects/B/final.mov", OldPath: "Projects/A/clip.mov", Inode: ino})
	if got := f.requireBound(ino, "Projects/B/final.mov"); got != id {
		t.Fatalf("rename changed the asset: %s -> %s", id, got)
	}
	if loc := f.location(ino); loc.CreatedSequence != created {
		t.Fatalf("rename recreated the location: %+v", loc)
	}
	if n := f.events(); n != 2 {
		t.Fatalf("bind + one hint change, got %d operations", n)
	}
}

func TestUnlinkTombstonesLocation(t *testing.T) {
	for _, skipTrash := range []bool{false, true} {
		t.Run(fmt.Sprintf("skipTrash=%v", skipTrash), func(t *testing.T) {
			f := newFixture(t, 0, true)
			ino := f.create("Projects/clip.mov")
			f.feed(Event{Op: "create", Path: "Projects/clip.mov", Inode: ino})
			id := f.requireBound(ino, "Projects/clip.mov")
			f.unlink("Projects/clip.mov", skipTrash)
			f.feed(Event{Op: "delete", Path: "Projects/clip.mov", Inode: ino})
			loc := f.location(ino)
			if !loc.Removed || loc.Value.AssetID != id {
				t.Fatalf("location after unlink = %+v", loc)
			}
			if a := f.asset(id); a.Label != "Projects/clip.mov" {
				t.Fatalf("asset after unlink = %+v", a)
			}
			// A delete naming no inode, for a path the binder never saw, is
			// counted as unresolved rather than guessed.
			f.feed(Event{Op: "delete", Path: "Projects/other.mov"})
			if s := f.b.Stats(); s.Unresolved != 1 || s.Failed != 0 {
				t.Fatalf("stats %+v", s)
			}
		})
	}
}

func TestDeleteOfLiveInodeKeepsLocation(t *testing.T) {
	f := newFixture(t, 0, true)
	ino := f.create("Projects/clip.mov")
	f.feed(Event{Op: "create", Path: "Projects/clip.mov", Inode: ino})
	// A stale or forged delete for a file that is still there.
	f.feed(Event{Op: "delete", Path: "Projects/clip.mov", Inode: ino})
	f.requireBound(ino, "Projects/clip.mov")
}

func TestReplayIsIdempotent(t *testing.T) {
	f := newFixture(t, 0, true)
	a := f.create("Projects/a.mov")
	b := f.create("Projects/b.mov")
	f.rename("Projects/a.mov", "Projects/a2.mov")
	f.unlink("Projects/b.mov", false)
	history := []Event{
		{Op: "create", Path: "Projects/a.mov", Inode: a},
		{Op: "create", Path: "Projects/b.mov", Inode: b},
		{Op: "rename", Path: "Projects/a2.mov", OldPath: "Projects/a.mov", Inode: a},
		{Op: "delete", Path: "Projects/b.mov", Inode: b},
	}
	// The creates come after the change (the queue lags): b is already
	// gone and a is no longer at its first name, so only a is bound.
	f.feed(history...)
	f.requireBound(a, "Projects/a2.mov")
	if _, err := f.store.GetLocation(context.Background(), f.lib, LocationID(testUUID, b)); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("b bound after it was gone: %v", err)
	}
	n := f.events()
	f.feed(history...)
	f.feed(history...)
	if got := f.events(); got != n {
		t.Fatalf("replay added %d operations", got-n)
	}

	// An uncertain commit retried from the same state is the identical
	// operation: the catalog returns the original receipt.
	c := f.create("Projects/c.mov")
	v := f.b.vol.Load()
	op, err := f.b.planBind(context.Background(), v, c, "Projects/c.mov")
	if err != nil || op == nil {
		t.Fatalf("plan: %v %v", op, err)
	}
	first, err := f.store.Apply(context.Background(), *op)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.store.Apply(context.Background(), *op)
	if err != nil || again != first {
		t.Fatalf("retry = %+v, %v; want %+v", again, err, first)
	}
	if op.ID != fmt.Sprintf("jfs-%s-%d-bind-0-0", testUUID, c) {
		t.Fatalf("operation ID %q", op.ID)
	}
	f.feed(Event{Op: "create", Path: "Projects/c.mov", Inode: c})
	if got := f.events(); got != n+1 {
		t.Fatalf("the event after the retried bind added %d operations", got-n-1)
	}
}

func TestRevivesTombstonedLocation(t *testing.T) {
	f := newFixture(t, 0, true)
	ino := f.create("Projects/clip.mov")
	f.feed(Event{Op: "create", Path: "Projects/clip.mov", Inode: ino})
	created := f.location(ino).CreatedSequence
	// Deleted into the trash (tombstoned), then restored from it: the same
	// inode comes back, so its location is reactivated, not recreated.
	f.unlink("Projects/clip.mov", false)
	f.feed(Event{Op: "delete", Path: "Projects/clip.mov", Inode: ino})
	paths, err := f.b.vol.Load().GetPaths(ino)
	if err != nil || len(paths) != 0 {
		t.Fatalf("trashed file paths = %v, %v", paths, err)
	}
	trashed := f.trashName(ino)
	f.rename(trashed, "Projects/restored.mov")
	f.feed(Event{Op: "rename", Path: "Projects/restored.mov", OldPath: trashed, Inode: ino})
	f.requireBound(ino, "Projects/restored.mov")
	if loc := f.location(ino); loc.CreatedSequence != created {
		t.Fatalf("location recreated: %+v", loc)
	}
}

// trashName finds the inode's entry in the volume's trash.
func (f *fixture) trashName(ino uint64) string {
	f.t.Helper()
	var subs []*meta.Entry
	if st := f.admin.Readdir(f.ctx, meta.TrashInode, 0, &subs); st != 0 {
		f.t.Fatal(st)
	}
	for _, s := range subs {
		var ents []*meta.Entry
		if st := f.admin.Readdir(f.ctx, s.Inode, 0, &ents); st != 0 {
			continue
		}
		for _, e := range ents {
			if uint64(e.Inode) == ino {
				return meta.TrashName + "/" + string(s.Name) + "/" + string(e.Name)
			}
		}
	}
	f.t.Fatalf("inode %d not in the trash", ino)
	return ""
}

func TestHardLinksUseSmallestPath(t *testing.T) {
	f := newFixture(t, 0, true)
	ino := f.create("Projects/z.mov")
	f.link(ino, "Projects/b.mov")
	f.link(ino, "Projects/.hidden.mov")
	f.feed(Event{Op: "create", Path: "Projects/z.mov", Inode: ino})
	f.requireBound(ino, "Projects/b.mov")
	// Losing the hinted name moves the hint to the next canonical one.
	f.unlink("Projects/b.mov", false)
	f.feed(Event{Op: "delete", Path: "Projects/b.mov", Inode: ino})
	f.requireBound(ino, "Projects/z.mov")
	// Losing a name that is not the hint changes nothing.
	n := f.events()
	f.unlink("Projects/.hidden.mov", false)
	f.feed(Event{Op: "delete", Path: "Projects/.hidden.mov", Inode: ino})
	if f.events() != n {
		t.Fatal("losing an unhinted name recorded an operation")
	}
	// The last name goes: the location is tombstoned.
	f.unlink("Projects/z.mov", false)
	f.feed(Event{Op: "delete", Path: "Projects/z.mov", Inode: ino})
	if !f.location(ino).Removed {
		t.Fatal("location of an unlinked hard-linked file is active")
	}
}

func TestRenameOverRetiresReplacedFile(t *testing.T) {
	f := newFixture(t, 0, true)
	old := f.create("Projects/edit.prproj")
	f.feed(Event{Op: "create", Path: "Projects/edit.prproj", Inode: old})
	// The usual save: write a temp file, rename it over the original. No
	// delete event is published for the replaced file.
	tmp := f.create("Projects/.edit.prproj.tmp")
	f.rename("Projects/.edit.prproj.tmp", "Projects/edit.prproj")
	f.feed(
		Event{Op: "create", Path: "Projects/.edit.prproj.tmp", Inode: tmp},
		Event{Op: "rename", Path: "Projects/edit.prproj", OldPath: "Projects/.edit.prproj.tmp", Inode: tmp},
	)
	f.requireBound(tmp, "Projects/edit.prproj")
	if !f.location(old).Removed {
		t.Fatal("the replaced file's location is still active")
	}
}

func TestQueueOverflowDropsAndCounts(t *testing.T) {
	f := newFixture(t, 2, false) // worker not running: nothing drains
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5; i++ {
			f.b.Offer(Event{Op: "create", Path: fmt.Sprintf("A/%d.mov", i)})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Offer blocked on a full queue")
	}
	if s := f.b.Stats(); s.Received != 5 || s.Dropped != 3 || s.Queued != 2 || s.Complete {
		t.Fatalf("stats %+v", s)
	}
	// The queued events are still applied once the worker runs.
	ino := f.create("A/0.mov")
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() { defer close(done); f.b.Run(ctx) }()
	defer func() { cancel(); <-done; _ = f.b.Close() }()
	f.waitHandled(2)
	f.requireBound(ino, "A/0.mov")
}

func TestSubscribeAppliesPublishedEvents(t *testing.T) {
	f := newFixture(t, 0, true)
	channel := fmt.Sprintf("juicemount:metadata:test-%d", time.Now().UnixNano())
	rdb := openRedis(t, f.url)
	defer rdb.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.b.Subscribe(ctx, rdb, channel) }()
	defer func() { cancel(); <-done }()
	select {
	case <-f.b.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("subscription not acknowledged")
	}
	if f.b.Stats().ObservedSince.IsZero() {
		t.Fatal("no observation start recorded")
	}
	ino := f.create("Projects/clip.mov")
	pub := openRedis(t, f.url)
	defer pub.Close()
	payload, _ := json.Marshal(metadata.MetadataEvent{Op: "create", Path: "Projects/clip.mov", Size: 1, Mtime: 1, Inode: ino})
	for _, p := range []string{"{garbage", string(payload)} {
		if err := pub.Publish(ctx, channel, p).Err(); err != nil {
			t.Fatal(err)
		}
	}
	f.sent = 1 // the garbage payload is counted, never queued
	f.waitHandled(1)
	f.requireBound(ino, "Projects/clip.mov")
	if s := f.b.Stats(); s.Received != 2 || s.Ignored != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestQueries(t *testing.T) {
	f := newFixture(t, 0, true)
	ino := f.create("Projects/clip.mov")
	f.link(ino, "Archive/clip.mov")
	f.feed(Event{Op: "create", Path: "Projects/clip.mov", Inode: ino})
	ctx := context.Background()
	byPath, err := f.b.AssetByPath(ctx, "/Projects/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	want := AssetView{
		AssetID:   AssetID(testUUID, ino),
		PathHint:  "Archive/clip.mov",
		Path:      "Archive/clip.mov",
		Paths:     []string{"Archive/clip.mov", "Projects/clip.mov"},
		Locations: []LocationView{{ID: LocationID(testUUID, ino), Source: "jfs:" + testUUID, Locator: fmt.Sprintf("ino:%d", ino), UpdatedSequence: 1}},
	}
	if !reflect.DeepEqual(byPath, want) {
		t.Fatalf("by path = %+v\nwant %+v", byPath, want)
	}
	byID, err := f.b.AssetByID(ctx, want.AssetID)
	if err != nil || !reflect.DeepEqual(byID, want) {
		t.Fatalf("by id = %+v, %v", byID, err)
	}
	f.create("Projects/uncatalogued.mov")
	for _, p := range []string{"Projects/uncatalogued.mov", "Projects/nope.mov"} {
		if _, err := f.b.AssetByPath(ctx, p); !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("%s: %v", p, err)
		}
	}
	if _, err := f.b.AssetByPath(ctx, "../x"); !errors.Is(err, catalog.ErrInvalid) {
		t.Fatalf("bad path: %v", err)
	}
	if _, err := f.b.AssetByID(ctx, "asset_nope"); !errors.Is(err, catalog.ErrInvalid) {
		t.Fatalf("bad id: %v", err)
	}
}

func TestNotConnectedYet(t *testing.T) {
	store, _ := newStore(t)
	var calls atomic.Int32
	b, err := New(Config{Store: store, RetryMax: 10 * time.Millisecond, Logf: t.Logf, Connect: func() (Volume, error) {
		calls.Add(1)
		return nil, errors.New("unreachable")
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.AssetByPath(context.Background(), "A/x.mov"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("by path before connect: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	b.Run(ctx) // returns when ctx ends, after retrying
	if calls.Load() < 2 || b.Stats().Connected {
		t.Fatalf("connect calls %d, stats %+v", calls.Load(), b.Stats())
	}
}

// TestEventMatchesPublishedPayload pins Event to the payload the clients
// publish (metadata.MetadataEvent); the binder does not import metadata.
func TestEventMatchesPublishedPayload(t *testing.T) {
	in := metadata.MetadataEvent{Op: "rename", Path: "B/n.mov", OldPath: "A/o.mov", Size: 3, Mtime: 4, Inode: 9, IsDir: true, IsSymlink: true}
	payload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseEvent(payload)
	want := Event{Op: "rename", Path: "B/n.mov", OldPath: "A/o.mov", Inode: 9, IsDir: true, IsSymlink: true}
	if err != nil || got != want {
		t.Fatalf("ParseEvent = %+v, %v; want %+v", got, err, want)
	}
	if metadata.SubscribeChannel != "juicemount:metadata" {
		t.Fatalf("channel renamed to %q", metadata.SubscribeChannel)
	}
	if _, err := ParseEvent([]byte(`{"op":"create","path":"` + strings.Repeat("x", MaxEventBytes) + `"}`)); err == nil {
		t.Fatal("oversized payload parsed")
	}
}
