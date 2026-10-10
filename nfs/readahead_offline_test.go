package nfs

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// These tests change process-wide offline state; none run in parallel.
func resetPrefetchOfflineState(t *testing.T) {
	t.Helper()
	prior := pin.State()
	pin.SetOffline(false)
	pin.SetAutoOffline(false, "")
	t.Cleanup(func() {
		pin.SetOffline(prior.UserOffline)
		pin.SetAutoOffline(prior.AutoOffline, prior.Reason)
	})
}

func awaitPrefetchSignal[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for readahead worker boundary")
		var zero T
		return zero
	}
}

func prefetchTestFile(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	const name = "reel.mov"
	if err := os.WriteFile(filepath.Join(dir, name), make([]byte, 6*readaheadBlockSize), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, name
}

func triggerRealPrefetch(rm *ReadaheadManager, name string) {
	for i := 0; i < 4; i++ {
		rm.OnRead(81, int64(i)*readaheadBlockSize, readaheadBlockSize, name)
	}
}

func TestReadaheadOfflineSourcesAndOnlineResume(t *testing.T) {
	resetPrefetchOfflineState(t)
	t.Setenv("JM_SERVER_READAHEAD", "")
	dir, name := prefetchTestFile(t)
	rm := NewReadaheadManager(dir, nil, nil)
	defer rm.Stop()
	var acquisitions atomic.Int32
	rm.afterPrefetchAcquire = func(*os.File) { acquisitions.Add(1) }

	for _, state := range []struct {
		user, auto bool
	}{
		{user: true},
		{user: true, auto: true},
		{auto: true},
	} {
		pin.SetOffline(state.user)
		pin.SetAutoOffline(state.auto, "test")
		triggerRealPrefetch(rm, name)
		rm.prefetch(name, 0, readaheadBlockSize, 4) // direct entry also refuses
		if triggered, prefetched := rm.Stats(); triggered != 0 || prefetched != 0 {
			t.Fatalf("offline user=%v auto=%v: scheduled=%d read=%d", state.user, state.auto, triggered, prefetched)
		}
		rm.mu.Lock()
		trackers := len(rm.trackers)
		rm.mu.Unlock()
		if trackers != 0 || acquisitions.Load() != 0 {
			t.Fatalf("offline mutated %d trackers or acquired %d descriptors", trackers, acquisitions.Load())
		}
	}

	pin.SetOffline(false)
	pin.SetAutoOffline(false, "")
	completed := make(chan struct{})
	rm.prefetchWorkerExit = func() { close(completed) }
	triggerRealPrefetch(rm, name)
	awaitPrefetchSignal(t, completed)
	if triggered, prefetched := rm.Stats(); triggered == 0 || prefetched == 0 {
		t.Fatalf("online sequential run did not resume: scheduled=%d read=%d", triggered, prefetched)
	}
}

func TestQueuedReadaheadUsesCurrentOfflineState(t *testing.T) {
	resetPrefetchOfflineState(t)
	t.Setenv("JM_SERVER_READAHEAD", "")
	dir, name := prefetchTestFile(t)
	for _, outcome := range []string{"offline", "back-online"} {
		t.Run(outcome, func(t *testing.T) {
			pin.SetOffline(false)
			rm := NewReadaheadManager(dir, nil, nil)
			entered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer func() { unblock(); rm.Stop() }()
			rm.prefetchWorkerEntry = func() {
				close(entered)
				<-release
			}
			exited := make(chan struct{})
			rm.prefetchWorkerExit = func() { close(exited) }
			var acquisitions atomic.Int32
			acquired := make(chan struct{}, 1)
			readDone := make(chan struct{}, 1)
			rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
				n, err := fd.ReadAt(buf, off)
				select {
				case readDone <- struct{}{}:
				default:
				}
				return n, err
			}
			rm.afterPrefetchAcquire = func(*os.File) {
				acquisitions.Add(1)
				acquired <- struct{}{}
			}
			triggerRealPrefetch(rm, name)
			awaitPrefetchSignal(t, entered)
			pin.SetOffline(true)
			if outcome == "back-online" {
				pin.SetOffline(false)
			}
			unblock()
			if outcome == "back-online" {
				awaitPrefetchSignal(t, acquired)
				awaitPrefetchSignal(t, readDone)
			} else {
				awaitPrefetchSignal(t, exited)
			}
			rm.Stop()
			if outcome == "offline" && acquisitions.Load() != 0 {
				t.Fatalf("queued worker acquired %d descriptors offline", acquisitions.Load())
			}
			if outcome == "back-online" && acquisitions.Load() == 0 {
				t.Fatal("worker did not resume after the offline interval")
			}
		})
	}
}

func TestReadaheadAfterAcquireOfflineReleasesAndStopJoins(t *testing.T) {
	resetPrefetchOfflineState(t)
	t.Setenv("JM_SERVER_READAHEAD", "")
	for _, pooled := range []bool{false, true} {
		for _, stopDuringAcquire := range []bool{false, true} {
			name := "direct"
			if pooled {
				name = "pooled"
			}
			if stopDuringAcquire {
				name += "/stop"
			} else {
				name += "/offline"
			}
			t.Run(name, func(t *testing.T) {
				pin.SetOffline(false)
				dir, file := prefetchTestFile(t)
				var pool *FDPool
				if pooled {
					pool = NewFDPool()
					defer pool.Stop()
				}
				rm := NewReadaheadManager(dir, pool, nil)
				acquired := make(chan *os.File, 1)
				exited := make(chan struct{})
				rm.prefetchWorkerExit = func() { close(exited) }
				release := make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer func() { unblock(); rm.Stop() }()
				rm.afterPrefetchAcquire = func(fd *os.File) {
					acquired <- fd
					<-release
				}
				var reads atomic.Int32
				rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
					reads.Add(1)
					return fd.ReadAt(buf, off)
				}
				triggerRealPrefetch(rm, file)
				fd := awaitPrefetchSignal(t, acquired)
				if pool != nil {
					_, active := pool.Stats()
					if active != 1 {
						t.Fatalf("pooled descriptor has %d active refs, want 1", active)
					}
				}
				pin.SetOffline(true)
				var stopped chan struct{}
				if stopDuringAcquire {
					stopped = make(chan struct{})
					go func() { rm.Stop(); close(stopped) }()
					awaitPrefetchSignal(t, rm.stopCh)
					select {
					case <-stopped:
						t.Fatal("Stop returned while acquired descriptor was still held")
					case <-time.After(50 * time.Millisecond):
						// stopCh closed and the worker still holds the descriptor.
					}
				}
				unblock()
				awaitPrefetchSignal(t, exited)
				if stopped != nil {
					awaitPrefetchSignal(t, stopped)
				}
				if reads.Load() != 0 {
					t.Fatalf("read %d blocks after acquisition was refused", reads.Load())
				}
				if pool != nil {
					open, active := pool.Stats()
					if open != 1 || active != 0 {
						t.Fatalf("pooled descriptor cleanup = open %d active %d", open, active)
					}
				} else if _, err := fd.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("direct descriptor was not closed: %v", err)
				}
			})
		}
	}
}

func TestReadaheadAfterGateOfflineReleasesToken(t *testing.T) {
	if !fuseDataGateEnabled || backgroundFUSEDataWidth() == 0 {
		t.Skip("background data gate disabled")
	}
	resetPrefetchOfflineState(t)
	t.Setenv("JM_SERVER_READAHEAD", "")
	dir, name := prefetchTestFile(t)
	rm := NewReadaheadManager(dir, nil, nil)
	exited := make(chan struct{})
	rm.prefetchWorkerExit = func() { close(exited) }
	gateHeld := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer func() { unblock(); rm.Stop() }()
	rm.afterPrefetchGate = func() { close(gateHeld); <-release }
	var reads atomic.Int32
	rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
		reads.Add(1)
		return fd.ReadAt(buf, off)
	}
	before, _ := fuseDataGateDepth()
	triggerRealPrefetch(rm, name)
	awaitPrefetchSignal(t, gateHeld)
	if inUse, _ := fuseDataGateDepth(); inUse != before+1 {
		t.Fatalf("admitted gate depth = %d, want %d", inUse, before+1)
	}
	pin.SetAutoOffline(true, "test")
	unblock()
	awaitPrefetchSignal(t, exited)
	if reads.Load() != 0 {
		t.Fatalf("read %d blocks after offline gate acquisition", reads.Load())
	}
	if inUse, _ := fuseDataGateDepth(); inUse != before {
		t.Fatalf("gate token leaked: before=%d after=%d", before, inUse)
	}
}

func TestReadaheadDuringReadKeepsOwnershipUntilCompletion(t *testing.T) {
	if !fuseDataGateEnabled || backgroundFUSEDataWidth() == 0 {
		t.Skip("background data gate disabled")
	}
	resetPrefetchOfflineState(t)
	t.Setenv("JM_SERVER_READAHEAD", "")
	for _, stopDuringRead := range []bool{false, true} {
		name := "offline-without-stop"
		if stopDuringRead {
			name = "stop-joins-admitted-read"
		}
		t.Run(name, func(t *testing.T) {
			pin.SetOffline(false)
			dir, file := prefetchTestFile(t)
			rm := NewReadaheadManager(dir, nil, nil)
			readStarted := make(chan struct{})
			readResult := make(chan struct {
				n   int
				err error
			}, 1)
			exited := make(chan struct{})
			rm.prefetchWorkerExit = func() { close(exited) }
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer func() { unblock(); rm.Stop() }()
			var reads atomic.Int32
			rm.prefetchReadAt = func(fd *os.File, buf []byte, off int64) (int, error) {
				first := reads.Add(1) == 1
				if first {
					close(readStarted)
					<-release
				}
				n, err := fd.ReadAt(buf, off)
				if first {
					readResult <- struct {
						n   int
						err error
					}{n, err}
				}
				return n, err
			}
			before, _ := fuseDataGateDepth()
			triggerRealPrefetch(rm, file)
			awaitPrefetchSignal(t, readStarted)
			if inUse, _ := fuseDataGateDepth(); inUse != before+1 {
				t.Fatalf("in-flight read lost gate token: depth=%d", inUse)
			}
			toggled := make(chan struct{})
			go func() { pin.SetOffline(true); close(toggled) }()
			awaitPrefetchSignal(t, toggled)
			// This plain local-file read shows independent local I/O stays
			// available. The tagged bound-cache test checks actual cachedFile bytes.
			local, err := os.Open(filepath.Join(dir, file))
			if err != nil {
				t.Fatal(err)
			}
			var sample [16]byte
			_, err = local.ReadAt(sample[:], 0)
			_ = local.Close()
			if err != nil {
				t.Fatalf("independent local-file read while offline: %v", err)
			}
			var stopped chan struct{}
			if stopDuringRead {
				stopped = make(chan struct{})
				go func() { rm.Stop(); close(stopped) }()
				awaitPrefetchSignal(t, rm.stopCh)
				select {
				case <-stopped:
					t.Fatal("Stop returned while the actual ReadAt remained held")
				case <-time.After(50 * time.Millisecond):
					// stopCh closed and the worker is still inside the held ReadAt.
				}
			}
			if inUse, _ := fuseDataGateDepth(); inUse != before+1 {
				t.Fatalf("in-flight read token released early: depth=%d", inUse)
			}
			unblock()
			result := awaitPrefetchSignal(t, readResult)
			if result.n != readaheadBlockSize || result.err != nil {
				t.Fatalf("admitted real ReadAt = %d %v, want full block", result.n, result.err)
			}
			// In the offline-only case the worker must exit before Stop, so the
			// no-second-block assertion cannot pass through Stop cancellation.
			awaitPrefetchSignal(t, exited)
			if stopped != nil {
				awaitPrefetchSignal(t, stopped)
			}
			if reads.Load() != 1 {
				t.Fatalf("read %d blocks, want only the admitted first block", reads.Load())
			}
			if _, prefetched := rm.Stats(); prefetched != 1 {
				t.Fatalf("prefetched %d blocks, want the completed admitted block", prefetched)
			}
			if inUse, _ := fuseDataGateDepth(); inUse != before {
				t.Fatalf("gate depth after worker completion=%d, want %d", inUse, before)
			}
		})
	}
}
