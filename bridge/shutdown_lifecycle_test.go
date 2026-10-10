package main

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func shutdownTestDrainer(t *testing.T) (*jmnfs.SpoolStore, *jmnfs.Drainer) {
	t.Helper()
	// Startup fault tests can leave the process offline. This fixture drains
	// only into a private plain directory, so establish its unit-test gates.
	identityOK, identityReason := pin.FUSEIdentityState()
	t.Logf("inherited drain gates: user_offline=%t auto_offline=%t identity_ok=%t reason=%s",
		pin.IsUserOffline(), pin.IsAutoOffline(), identityOK, identityReason)
	resetGates := func() {
		pin.SetOffline(false)
		pin.SetAutoOffline(false, "")
		pin.ResetFUSEIdentityForTest()
	}
	resetGates()
	t.Cleanup(resetGates)
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := jmnfs.NewSpoolStore(t.TempDir(), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(spool.Stop)
	drainer, err := jmnfs.NewDrainer(spool, jmnfs.DrainerConfig{
		FuseRoot: t.TempDir(), PollFallback: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.SetSpoolStore(spool.Meta())
	drainer.SetOnDrainCommit(store.DrainComplete)
	t.Cleanup(func() {
		if !drainer.Stop(time.Second) {
			t.Error("fixture drainer did not stop before spool/store cleanup")
		}
	})
	return spool, drainer
}

func publishShutdownTestDrainer(t *testing.T, spool *jmnfs.SpoolStore, drainer *jmnfs.Drainer, sweepDone chan struct{}) {
	t.Helper()
	globalMu.Lock()
	if globalServer != nil || globalSpool != nil || globalDrainer != nil || globalStopPending {
		globalMu.Unlock()
		t.Fatal("test requires an idle bridge lifecycle")
	}
	globalSpool = spool
	globalDrainer = drainer
	globalDrainerAtomic.Store(drainer)
	globalSpoolSweepDone = sweepDone
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalSpool = nil
		globalDrainer = nil
		globalDrainerAtomic.Store(nil)
		globalSpoolSweepDone = nil
		globalStopPending = false
		globalMu.Unlock()
	})
}

func TestStopRetainsSpoolUntilBlockedWorkerFinishes(t *testing.T) {
	spool, drainer := shutdownTestDrainer(t)
	entry, err := spool.OpenWrite("/blocked.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.WriteAt([]byte("bytes"), 0); err != nil {
		t.Fatal(err)
	}
	if err := entry.Close(); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	drainer.SetOnDrainCommit(func(item metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
		close(entered)
		<-release
		return metadata.DrainCommitResult{SpoolID: item.SpoolID, Done: false}, nil
	})
	rows, err := spool.Meta().ListReady(2)
	if err != nil || len(rows) != 1 || rows[0].NFSPath != "/blocked.bin" || rows[0].Size != 5 {
		t.Fatalf("blocked worker fixture requires one ready five-byte row: rows=%+v err=%v", rows, err)
	}
	publishShutdownTestDrainer(t, spool, drainer, nil)
	drainer.Start()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach blocked callback")
	}
	if stopServerSerializedWithDeadline(20 * time.Millisecond) {
		t.Fatal("stop claimed completion while worker was blocked")
	}
	globalMu.Lock()
	retained := globalSpool == spool && globalDrainer == drainer && globalStopPending
	globalMu.Unlock()
	if !retained {
		t.Fatal("incomplete stop detached worker-owned resources")
	}
	if e, err := spool.OpenWrite("/still-open.bin"); err != nil {
		t.Fatalf("spool was closed before worker finished: %v", err)
	} else {
		_ = e.Close()
	}
	releaseOnce.Do(func() { close(release) })
	if !stopServerSerializedWithDeadline(time.Second) {
		t.Fatal("retry did not finish after worker release")
	}
	if _, err := spool.OpenWrite("/after-stop.bin"); err == nil {
		t.Fatal("spool remained open after complete stop")
	}
}

func TestStopRetainsPinStoreAndPrefetcherUntilTrackedWorkFinishes(t *testing.T) {
	store, err := pin.Open(filepath.Join(t.TempDir(), "pin.db"))
	if err != nil {
		t.Fatal(err)
	}
	prefetcher := pin.NewPrefetcher(store, t.TempDir(), "/Volumes/zpool", 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		prefetcher.Stop()
		_ = store.Close()
	})
	globalMu.Lock()
	if globalServer != nil || globalSpool != nil || globalDrainer != nil || globalPinStore != nil || globalPrefetcher != nil {
		globalMu.Unlock()
		t.Fatal("test requires an idle bridge lifecycle")
	}
	globalPinStore = store
	globalPrefetcher = prefetcher
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalPinStore = nil
		globalPrefetcher = nil
		globalStopPending = false
		globalMu.Unlock()
	})
	entered := make(chan struct{})
	finished := make(chan error, 1)
	prefetcher.Go(func() {
		close(entered)
		<-release
		finished <- store.Pin("/Volumes/zpool/completion.mov", 1, "/Volumes/zpool")
	})
	<-entered
	if quiesceServerSerialized(10 * time.Millisecond) {
		t.Fatal("bridge stop completed while prefetch work was held")
	}
	globalMu.Lock()
	retained := globalPinStore == store && globalPrefetcher == prefetcher && globalStopPending
	globalMu.Unlock()
	if !retained {
		t.Fatal("incomplete stop detached pin dependencies")
	}
	if err := store.Pin("/Volumes/zpool/still-open.mov", 1, "/Volumes/zpool"); err != nil {
		t.Fatalf("pin store closed before prefetch work returned: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	if !stopServerSerializedWithDeadline(time.Second) {
		t.Fatal("bridge retry did not complete after prefetch work returned")
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("tracked completion lost pin store: %v", err)
		}
	default:
		t.Fatal("stop completed before tracked final store effect")
	}
	globalMu.Lock()
	cleared := globalPinStore == nil && globalPrefetcher == nil
	globalMu.Unlock()
	if !cleared {
		t.Fatal("completed stop retained published pin dependencies")
	}
}

func TestStopRetainsSpoolDuringBootSweep(t *testing.T) {
	spool, drainer := shutdownTestDrainer(t)
	sweepDone := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(sweepDone) }) })
	publishShutdownTestDrainer(t, spool, drainer, sweepDone)
	if stopServerSerializedWithDeadline(20 * time.Millisecond) {
		t.Fatal("stop claimed completion while boot sweep still owned spool")
	}
	if e, err := spool.OpenWrite("/still-open.bin"); err != nil {
		t.Fatalf("spool was closed during boot sweep: %v", err)
	} else {
		_ = e.Close()
	}
	releaseOnce.Do(func() { close(sweepDone) })
	if !stopServerSerializedWithDeadline(time.Second) {
		t.Fatal("retry did not finish after boot sweep")
	}
}

func TestUnmountFailurePreservesMountForRetry(t *testing.T) {
	globalMu.Lock()
	previous := globalMountPath
	globalMountPath = "/Volumes/ShutdownRetryTest"
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalMountPath = previous
		globalMu.Unlock()
	})
	if unmountAndClearNFS("/Volumes/ShutdownRetryTest", func(string) bool { return false }) {
		t.Fatal("failed unmount reported success")
	}
	globalMu.Lock()
	retained := globalMountPath
	globalMu.Unlock()
	if retained != "/Volumes/ShutdownRetryTest" {
		t.Fatalf("failed unmount lost retry identity: %q", retained)
	}
	if !unmountAndClearNFS(retained, func(string) bool { return true }) {
		t.Fatal("successful retry reported failure")
	}
	globalMu.Lock()
	cleared := globalMountPath
	globalMu.Unlock()
	if cleared != "" {
		t.Fatalf("successful retry left mount identity: %q", cleared)
	}
}

func TestFinalSpoolCheckSeesLateWrite(t *testing.T) {
	spool, _ := shutdownTestDrainer(t)
	if !spoolClearAfterUnmount(spool) {
		t.Fatal("empty spool was not clear")
	}
	entry, err := spool.OpenWrite("/late-write.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.WriteAt([]byte("late"), 0); err != nil {
		t.Fatal(err)
	}
	if spoolClearAfterUnmount(spool) {
		t.Fatal("late NFS write escaped the final spool check")
	}
	_ = entry.Close()
}

func TestShutdownDoesNotCheckSpoolAheadOfBootSweep(t *testing.T) {
	spool, drainer := shutdownTestDrainer(t)
	sweepDone := make(chan struct{})
	publishShutdownTestDrainer(t, spool, drainer, sweepDone)
	if shutdownServer(false) != 0 {
		t.Fatal("shutdown succeeded while boot sweep could still publish rows")
	}
	globalMu.Lock()
	retained := globalSpool == spool && globalDrainer == drainer
	globalMu.Unlock()
	if !retained {
		t.Fatal("shutdown detached spool before boot sweep completed")
	}
	close(sweepDone)
}

func TestSoftStopRefusalDoesNotRetireMountRecovery(t *testing.T) {
	globalMu.Lock()
	previousPath, previousFlights := globalMountPath, globalMountFlights
	flights := newNFSMountFlights()
	globalMountPath = "/Volumes/still-mounted"
	globalMountFlights = flights
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalMountPath, globalMountFlights = previousPath, previousFlights
		globalMu.Unlock()
	})
	if stopServerSerializedWithDeadline(20 * time.Millisecond) {
		t.Fatal("soft stop accepted a live NFS mount")
	}
	if !flights.begin() {
		t.Fatal("refused soft stop retired mount recovery")
	}
	flights.end()
}
