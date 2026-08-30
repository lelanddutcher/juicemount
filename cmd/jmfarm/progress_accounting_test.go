package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
)

func TestRunPassesCountsPartialBlobFailure(t *testing.T) {
	dir := t.TempDir()
	store, err := derivatives.Open(filepath.Join(dir, "derivatives.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	good := filepath.Join(dir, "good.mov")
	bad := filepath.Join(dir, "bad.mov")
	fresh := filepath.Join(dir, "fresh.mov")
	statusPath := filepath.Join(dir, "farm-status.json")
	processed, failed, failedTargets, details, safetyErr := runPasses(passOpts{
		mode: "derivatives", effConc: 2, status: statusPath,
		producer: "test", target: dir, store: store,
		process: func(_ *derivatives.Store, path string, _ farm.Options) farm.Result {
			if path == bad {
				return farm.Result{Path: path, Inode: 2, BlobErr: errors.New("waveform decode unavailable")}
			}
			if path == fresh {
				return farm.Result{Path: path, Inode: 3, SkippedFresh: true}
			}
			return farm.Result{Path: path, Inode: 1, ThumbWrote: true}
		},
	}, []string{good, bad, fresh})
	if safetyErr != nil {
		t.Fatalf("unexpected safety error: %v", safetyErr)
	}

	if processed != 2 || failed != 1 {
		t.Fatalf("counts = processed %d failed %d, want 2/1", processed, failed)
	}
	if len(failedTargets) != 1 || failedTargets[0] != bad {
		t.Fatalf("failed targets = %v, want %q", failedTargets, bad)
	}
	if len(details) != 1 || !strings.Contains(details[0], "waveform decode unavailable") {
		t.Fatalf("failure details = %v", details)
	}

	raw, err := os.ReadFile(statusPath)
	if err != nil {
		t.Fatal(err)
	}
	var status farm.FarmStatus
	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatal(err)
	}
	if status.LastSweep.Processed != 2 || status.LastSweep.Failed != 1 {
		t.Fatalf("status sweep = %+v, want processed=2 failed=1", status.LastSweep)
	}
	if status.InProgress != nil {
		t.Fatalf("terminal status retained in-progress block: %+v", status.InProgress)
	}
}

func TestRunPassesPreCanceledDoesNotLaunchTargets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls int64
	processed, failed, failedTargets, details, safetyErr := runPasses(passOpts{
		ctx: ctx, mode: "derivatives", effConc: 4,
		process: func(_ *derivatives.Store, path string, opt farm.Options) farm.Result {
			atomic.AddInt64(&calls, 1)
			return farm.Result{Path: path}
		},
	}, []string{"one.mov", "two.mov", "three.mov"})
	if safetyErr != nil {
		t.Fatalf("unexpected safety error: %v", safetyErr)
	}

	if calls != 0 {
		t.Fatalf("process calls = %d, want zero after cancellation", calls)
	}
	if processed != 0 || failed != 0 || len(failedTargets) != 0 || len(details) != 0 {
		t.Fatalf("cancelled counts = %d/%d targets=%v details=%v", processed, failed, failedTargets, details)
	}
}

func TestRunPassesPublishesBoundedActivityProgress(t *testing.T) {
	var mu sync.Mutex
	var updates []int
	processed, failed, _, _, safetyErr := runPasses(passOpts{
		mode: "proxy", effConc: 1,
		process: func(_ *derivatives.Store, path string, _ farm.Options) farm.Result {
			return farm.Result{Path: path}
		},
		progress: func(stage string, pct int) {
			if stage != "proxy" {
				t.Errorf("stage = %q", stage)
			}
			if pct < 0 || pct > 100 {
				t.Errorf("pct = %d", pct)
			}
			mu.Lock()
			updates = append(updates, pct)
			mu.Unlock()
		},
	}, []string{"one.mov", "two.mov"})
	if safetyErr != nil || processed != 2 || failed != 0 {
		t.Fatalf("result processed=%d failed=%d safety=%v", processed, failed, safetyErr)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(updates) < 2 || updates[0] != 0 || updates[len(updates)-1] != 100 {
		t.Fatalf("progress updates = %v, want 0 ... 100", updates)
	}
}

func TestRunPassesSurfacesStorageSafetySeparatelyFromMediaFailure(t *testing.T) {
	broken := filepath.Join(t.TempDir(), "storage.mov")
	processed, failed, failedTargets, _, safetyErr := runPasses(passOpts{
		mode: "derivatives", effConc: 1,
		process: func(_ *derivatives.Store, path string, _ farm.Options) farm.Result {
			return farm.Result{Path: path, BlobErr: errors.New("ffmpeg: Error writing trailer: Input/output error")}
		},
	}, []string{broken})
	if processed != 0 || failed != 1 || len(failedTargets) != 1 || failedTargets[0] != broken {
		t.Fatalf("counts=%d/%d targets=%v", processed, failed, failedTargets)
	}
	if safetyErr == nil || !isFarmStoragePressure(safetyErr) {
		t.Fatalf("safety error = %v", safetyErr)
	}
}

func TestRunPassesStorageSafetyRetriesEveryUnfinishedTarget(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.mov")
	broken := filepath.Join(dir, "broken.mov")
	pending := filepath.Join(dir, "pending.mov")
	var calls int64
	processed, failed, failedTargets, _, safetyErr := runPasses(passOpts{
		mode: "derivatives", effConc: 1,
		process: func(_ *derivatives.Store, path string, _ farm.Options) farm.Result {
			atomic.AddInt64(&calls, 1)
			if path == broken {
				return farm.Result{Path: path, BlobErr: errors.New("ffmpeg: Error writing trailer: Input/output error")}
			}
			return farm.Result{Path: path}
		},
	}, []string{good, broken, pending})
	if processed != 1 || failed != 1 {
		t.Fatalf("counts=%d/%d, want 1/1", processed, failed)
	}
	if calls != 2 {
		t.Fatalf("process calls=%d, want completed target plus triggering failure only", calls)
	}
	wantTargets := []string{broken, pending}
	if len(failedTargets) != len(wantTargets) || failedTargets[0] != wantTargets[0] || failedTargets[1] != wantTargets[1] {
		t.Fatalf("retry targets=%v, want unfinished %v", failedTargets, wantTargets)
	}
	if safetyErr == nil || !isFarmStoragePressure(safetyErr) {
		t.Fatalf("safety error = %v", safetyErr)
	}
}

func TestRunPassesCancellationStopsSchedulingAndDoesNotCountFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	result := make(chan [2]int, 1)
	var calls int64
	go func() {
		processed, failed, _, _, _ := runPasses(passOpts{
			ctx: ctx, mode: "derivatives", effConc: 1,
			process: func(_ *derivatives.Store, path string, opt farm.Options) farm.Result {
				if atomic.AddInt64(&calls, 1) == 1 {
					close(started)
				}
				<-opt.Context.Done()
				return farm.Result{Path: path, Err: opt.Context.Err()}
			},
		}, []string{"one.mov", "two.mov", "three.mov"})
		result <- [2]int{processed, failed}
	}()
	<-started
	cancel()

	select {
	case counts := <-result:
		if counts != [2]int{} {
			t.Fatalf("cancelled counts = %v, want zero", counts)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runPasses did not return after cancellation")
	}
	if calls != 1 {
		t.Fatalf("process calls = %d, want only the active target", calls)
	}
}
