//go:build !serviceclient

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

func pinJobFixture(t *testing.T) (*pin.Store, *pin.Prefetcher) {
	t.Helper()
	ps, err := pin.Open(filepath.Join(t.TempDir(), "pin.db"))
	if err != nil {
		t.Fatal(err)
	}
	pf := pin.NewPrefetcher(ps, t.TempDir(), "/Volumes/zpool", 1)
	t.Cleanup(func() {
		if !pf.StopWithin(time.Second) {
			t.Error("fixture prefetcher did not join")
		}
		if err := ps.Close(); err != nil {
			t.Error(err)
		}
	})
	return ps, pf
}

func TestPinWalkJoinRetainsCapturedStoreThroughFinalInsert(t *testing.T) {
	oldStore, oldPF := pinJobFixture(t)
	newStore, newPF := pinJobFixture(t)
	root := "/Volumes/zpool/reel"
	walkPath := filepath.Join(t.TempDir(), "reel")
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	inserted := make(chan error, 1)
	if !schedulePinWalkWith(oldPF, root, walkPath, "/Volumes/zpool", filepath.Dir(walkPath),
		func(string) ([]pin.Entry, error) {
			return []pin.Entry{{Path: filepath.Join(walkPath, "clip.mov"), Size: 5}}, nil
		},
		func(entries []pin.Entry) error {
			close(entered)
			<-release
			err := oldStore.PinMany(entries)
			inserted <- err
			return err
		},
		func(context.Context) { t.Error("metadata warm started after cancellation") }) {
		t.Fatal("live prefetcher refused pin walk")
	}
	<-entered
	globalMu.Lock()
	priorPF, priorStore := globalPrefetcher, globalPinStore
	globalPrefetcher, globalPinStore = newPF, newStore
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalPrefetcher, globalPinStore = priorPF, priorStore
		globalMu.Unlock()
	})
	if oldPF.StopWithin(0) {
		t.Fatal("stop joined before held SQLite insert returned")
	}
	if _, ok := oldStore.Get(root + "/clip.mov"); ok {
		t.Fatal("final insert happened before release")
	}
	releaseOnce.Do(func() { close(release) })
	if !oldPF.StopWithin(time.Second) {
		t.Fatal("stop retry missed final pin insert")
	}
	if err := <-inserted; err != nil {
		t.Fatal(err)
	}
	if e, ok := oldStore.Get(root + "/clip.mov"); !ok || e.Size != 5 {
		t.Fatalf("captured store missing final pin: entry=%+v ok=%t", e, ok)
	}
	if _, ok := newStore.Get(root + "/clip.mov"); ok {
		t.Fatal("replacement store inherited old generation pin")
	}
	for _, scanning := range pin.ScanningRoots() {
		if scanning.Root == root {
			t.Fatal("scanning marker survived admitted job completion")
		}
	}
	if schedulePinWalkWith(oldPF, root, walkPath, "/Volumes/zpool", filepath.Dir(walkPath),
		func(string) ([]pin.Entry, error) { t.Error("walk started after Stop"); return nil, nil },
		oldStore.PinMany, func(context.Context) { t.Error("warm started after Stop") }) {
		t.Fatal("stopped prefetcher admitted a new pin walk")
	}
	for _, scanning := range pin.ScanningRoots() {
		if scanning.Root == root {
			t.Fatal("rejected pin left a scanning marker")
		}
	}
}

func TestPinControlRefusesStoppedPrefetcherWithoutScanning(t *testing.T) {
	ps, pf := pinJobFixture(t)
	if !pf.StopWithin(time.Second) {
		t.Fatal("fixture stop failed")
	}
	root := "/Volumes/zpool/stopped"
	result := pinRootResult(root, ps, pf, nil, "/Volumes/zpool", t.TempDir())
	if result.OK || result.Scanning || result.Error == "" {
		t.Fatalf("stopped Pin claimed admission: %+v", result)
	}
	for _, scanning := range pin.ScanningRoots() {
		if scanning.Root == root {
			t.Fatal("stopped Pin published a scanning marker")
		}
	}
}

func TestPinWalkCancelledAfterHeldEnumerationSkipsInsert(t *testing.T) {
	store, pf := pinJobFixture(t)
	root := "/Volumes/zpool/cancelled"
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	if !schedulePinWalkWith(pf, root, t.TempDir(), "/Volumes/zpool", t.TempDir(),
		func(string) ([]pin.Entry, error) {
			close(entered)
			<-release
			return []pin.Entry{{Path: "/Volumes/zpool/cancelled/late.mov", Size: 1}}, nil
		},
		store.PinMany, func(context.Context) { t.Error("cancelled walk warmed metadata") }) {
		t.Fatal("live prefetcher refused enumeration")
	}
	<-entered
	if pf.StopWithin(0) {
		t.Fatal("stop joined while enumeration was held")
	}
	once.Do(func() { close(release) })
	if !pf.StopWithin(time.Second) {
		t.Fatal("held enumeration did not join")
	}
	if _, ok := store.Get("/Volumes/zpool/cancelled/late.mov"); ok {
		t.Fatal("cancelled walk inserted a new root")
	}
	for _, scanning := range pin.ScanningRoots() {
		if scanning.Root == root {
			t.Fatal("cancelled walk stranded a scanning marker")
		}
	}
}

func TestBootWarmJoinsHeldFinalEffectAndSkipsNextRoot(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "1")
	// Keep any offline-intent writes inside this test's private fixture.
	pin.SetOfflinePersistPath(filepath.Join(t.TempDir(), "offline-intent"))
	pin.SetOffline(false)
	pin.SetAutoOffline(false, "")
	t.Cleanup(func() {
		pin.SetOffline(false)
		pin.SetAutoOffline(false, "")
		pin.SetOfflinePersistPath("")
	})
	ps, pf := pinJobFixture(t)
	for _, root := range []string{"/Volumes/zpool/a", "/Volumes/zpool/b"} {
		if err := ps.Pin(root+"/clip.mov", 1, root); err != nil {
			t.Fatal(err)
		}
	}
	rc, err := metadata.NewRedisClientDeferred("redis://127.0.0.1:6379/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rc.Stop)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	finished := make(chan error, 1)
	if !scheduleBootPinWarmWith(pf, ps, rc, "/Volumes/zpool", func(_ context.Context, _ *metadata.RedisClient, root, _ string) {
		if root != "/Volumes/zpool/a" {
			t.Errorf("new root warm started after stop: %s", root)
			return
		}
		close(entered)
		<-release
		finished <- ps.Pin(root+"/final.mov", 2, root)
	}) {
		t.Fatal("boot warm was not admitted")
	}
	<-entered
	if pf.StopWithin(0) {
		t.Fatal("boot warm joined during held metadata effect")
	}
	once.Do(func() { close(release) })
	if !pf.StopWithin(time.Second) {
		t.Fatal("boot warm did not join after final effect")
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if _, ok := ps.Get("/Volumes/zpool/a/final.mov"); !ok {
		t.Fatal("joined final boot effect missing")
	}
	if scheduleBootPinWarmWith(pf, ps, rc, "/Volumes/zpool", func(context.Context, *metadata.RedisClient, string, string) {
		t.Error("warm started after Stop")
	}) {
		t.Fatal("stopped prefetcher admitted boot warm")
	}
}

func TestCacheClearReverifyReportsAdmissionTruthfully(t *testing.T) {
	privateHome := t.TempDir()
	t.Setenv("HOME", privateHome)
	if err := os.MkdirAll(filepath.Join(privateHome, ".juicefs", "cache"), 0700); err != nil {
		t.Fatal(err)
	}
	ps, pf := pinJobFixture(t)
	path := "/Volumes/zpool/ready.mov"
	if err := ps.Pin(path, 4, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	seedStatusReady(t, ps, path)
	globalMu.Lock()
	previous := globalPrefetcher
	globalPrefetcher = pf
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalPrefetcher = previous
		globalMu.Unlock()
	})
	request := func() bool {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/cache-clear?keep-pinned=true", nil)
		req.Header.Set(localControlHeader, "1")
		rec := httptest.NewRecorder()
		handleCacheClearHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("cache-clear status=%d body=%s", rec.Code, rec.Body.String())
		}
		var body struct {
			OK        bool `json:"ok"`
			Triggered bool `json:"pin_reverify_triggered"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if !body.OK {
			t.Fatalf("cache clear failed: %s", rec.Body.String())
		}
		return body.Triggered
	}
	if !request() {
		t.Fatal("live prefetcher admission was reported false")
	}
	if !pf.StopWithin(time.Second) {
		t.Fatal("admitted reverify did not join")
	}
	if request() {
		t.Fatal("cache-clear claimed reverify was triggered after Stop")
	}
}

func TestScheduledReverifyAppliesRealPendingStatus(t *testing.T) {
	ps, pf := pinJobFixture(t)
	path := "/Volumes/zpool/ready-for-repair.mov"
	if err := ps.Pin(path, 9, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	seedStatusReady(t, ps, path)
	finished := make(chan error, 1)
	if !schedulePinReverifyWith(pf, func(ctx context.Context) error {
		_, err := pf.VerifyAndRepair(ctx)
		finished <- err
		return err
	}) {
		t.Fatal("live reverify was refused")
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if !pf.StopWithin(time.Second) {
		t.Fatal("reverify did not join")
	}
	if entry, ok := ps.Get(path); !ok || entry.Status != pin.StatusPending {
		t.Fatalf("reverify did not apply final Pending status: entry=%+v ok=%t", entry, ok)
	}
}

func TestReverifyRetainsCapturedOwnerThroughFinalStoreEffect(t *testing.T) {
	oldStore, oldPF := pinJobFixture(t)
	newStore, newPF := pinJobFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	finished := make(chan error, 1)
	if !schedulePinReverifyWith(oldPF, func(context.Context) error {
		close(entered)
		<-release
		err := oldStore.Pin("/Volumes/zpool/repair.mov", 3, "/Volumes/zpool")
		finished <- err
		return err
	}) {
		t.Fatal("live owner refused reverify")
	}
	<-entered
	globalMu.Lock()
	previous := globalPrefetcher
	globalPrefetcher = newPF
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalPrefetcher = previous
		globalMu.Unlock()
	})
	if oldPF.StopWithin(0) {
		t.Fatal("reverify joined while final store effect was held")
	}
	once.Do(func() { close(release) })
	if !oldPF.StopWithin(time.Second) {
		t.Fatal("reverify did not join after final effect")
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if _, ok := oldStore.Get("/Volumes/zpool/repair.mov"); !ok {
		t.Fatal("captured owner store missing final effect")
	}
	if _, ok := newStore.Get("/Volumes/zpool/repair.mov"); ok {
		t.Fatal("replacement owner inherited old final effect")
	}
	if schedulePinReverifyWith(oldPF, func(context.Context) error {
		t.Error("verification started after Stop")
		return nil
	}) {
		t.Fatal("stopped owner admitted reverify")
	}
}
