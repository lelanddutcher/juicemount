package pin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type capacityReadyProvider struct{}

func (capacityReadyProvider) Capture(context.Context, Entry) (OwnedAuthorityJob, error) {
	return nil, ErrAuthorityUnavailable
}
func (capacityReadyProvider) ValidateReady(context.Context, Entry) bool { return true }
func (capacityReadyProvider) Current(Entry) bool                        { return true }
func (capacityReadyProvider) DiscardReady(Entry)                        {}

func TestZeroCacheWindowKeepsSmallPinPendingBeforeFUSE(t *testing.T) {
	t.Cleanup(func() { SetCachePolicy(0, 0, "", 0, false) })
	store := newTestStore(t)
	const path = "/Volumes/zpool/clip.mov"
	const size = int64(59_939_761)
	if err := store.Pin(path, size, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	cacheRoot := t.TempDir()
	free, dev, ok := volumeFreeBytesAtKnown(cacheRoot)
	if !ok {
		t.Fatal("test cache volume unavailable")
	}
	SetCachePolicy(1<<30, free+(1<<30), cacheRoot, dev, true)
	v := ComputeCapacity(store, cacheRoot)
	if !v.PolicyKnown || v.CacheCapacityBytes != 0 || v.ShortfallBytes != size || !v.OverCapacity {
		t.Fatalf("zero window hidden by hysteresis: %+v", v)
	}
	p := NewPrefetcher(store, "/definitely/not/a/mount", "/Volumes/zpool", 1)
	defer p.Stop()
	e, ok := store.Get(path)
	if !ok {
		t.Fatal("pin lost")
	}
	if err := p.prefetch(*e); !errors.Is(err, ErrNoUsableCacheCapacity) {
		t.Fatalf("prefetch error=%v, want explicit capacity refusal before FUSE", err)
	}
	pending, err := store.Pending(10)
	if err != nil || len(pending) != 1 || pending[0].Status != StatusPending || pending[0].BytesCached != 0 || pending[0].LastError != ErrNoUsableCacheCapacity.Error() {
		t.Fatalf("pin intent/progress changed: pending=%+v err=%v", pending, err)
	}
}

func TestUnknownCachePolicyRefusesDownload(t *testing.T) {
	t.Cleanup(func() { SetCachePolicy(0, 0, "", 0, false) })
	SetCachePolicy(0, 0, "", 0, false)
	if err := DownloadAdmission(); !errors.Is(err, ErrCacheCapacityUnknown) {
		t.Fatalf("unknown policy admitted: %v", err)
	}
}

func TestKnownBudgetRefusesImpossibleFileAndPinnedSet(t *testing.T) {
	t.Cleanup(func() { SetCachePolicy(0, 0, "", 0, false) })
	root := t.TempDir()
	free, dev, ok := volumeFreeBytesAtKnown(root)
	if !ok || free < 16 {
		t.Fatal("test cache volume lacks measurable free space")
	}
	budget := free / 8
	SetCachePolicy(budget, free/2, root, dev, true)
	ComputeCapacity(nil, root)
	if err := DownloadAdmissionFor(budget/4, budget/4); err != nil {
		t.Fatalf("healthy known file refused: %v", err)
	}
	if err := DownloadAdmissionFor(budget+1, 0); !errors.Is(err, ErrInsufficientCacheCapacity) {
		t.Fatalf("file larger than hard budget admitted: %v", err)
	}
	store := newTestStore(t)
	fileBytes := budget/2 + 1
	if err := store.Pin("/v/a", fileBytes, "/v"); err != nil {
		t.Fatal(err)
	}
	if err := store.Pin("/v/b", fileBytes, "/v"); err != nil {
		t.Fatal(err)
	}
	ComputeCapacity(store, root)
	if err := DownloadAdmissionFor(fileBytes, 2*fileBytes); !errors.Is(err, ErrInsufficientCacheCapacity) {
		t.Fatalf("pinned set larger than hard budget admitted: %v", err)
	}
}

func TestCacheVolumeMeasurementAndDeviceMismatchAreUnknown(t *testing.T) {
	t.Cleanup(func() { SetCachePolicy(0, 0, "", 0, false) })
	root := t.TempDir()
	file := filepath.Join(root, "regular-file")
	if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := volumeFreeBytesAtKnown(filepath.Join(file, "cache")); ok {
		t.Fatal("ENOTDIR climbed to a healthy ancestor")
	}
	free, dev, ok := volumeFreeBytesAtKnown(root)
	if !ok || free < 16 {
		t.Fatal("test cache volume unavailable")
	}
	SetCachePolicy(free/8, free/2, root, dev+1, true)
	if v := ComputeCapacity(nil, root); v.PolicyKnown {
		t.Fatalf("different volume accepted: %+v", v)
	}
	if err := DownloadAdmissionFor(1, 1); !errors.Is(err, ErrCacheCapacityUnknown) {
		t.Fatalf("different volume admitted: %v", err)
	}
}

func TestNewPinsAfterCapacitySnapshotRefuseImpossibleSetBeforeFUSE(t *testing.T) {
	t.Cleanup(func() { SetCachePolicy(0, 0, "", 0, false) })
	root := t.TempDir()
	free, dev, ok := volumeFreeBytesAtKnown(root)
	if !ok || free < 16 {
		t.Fatal("test cache volume lacks measurable free space")
	}
	budget := free / 8
	SetCachePolicy(budget, free/2, root, dev, true)
	store := newTestStore(t)
	ComputeCapacity(store, root) // snapshot predates both new pins
	fileBytes := budget/2 + 1
	if err := store.Pin("/v/a", fileBytes, "/v"); err != nil {
		t.Fatal(err)
	}
	if err := store.Pin("/v/b", fileBytes, "/v"); err != nil {
		t.Fatal(err)
	}
	p := NewPrefetcher(store, "/definitely/not/a/mount", "/v", 1)
	defer p.Stop()
	e, ok := store.Get("/v/a")
	if !ok {
		t.Fatal("new pin lost")
	}
	if err := p.prefetch(*e); !errors.Is(err, ErrInsufficientCacheCapacity) {
		t.Fatalf("stale capacity snapshot admitted impossible new set: %v", err)
	}
}

func TestVerifyAndRepairPreservesValidReadyWithNoDownloadCapacity(t *testing.T) {
	for _, mode := range []string{"unknown", "zero"} {
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(func() { SetCachePolicy(0, 0, "", 0, false) })
			store := newTestStore(t)
			const path = "/v/ready.mov"
			if err := store.Pin(path, 4, "/v"); err != nil {
				t.Fatal(err)
			}
			ready := markTestPinReady(t, store, path)
			if mode == "unknown" {
				SetCachePolicy(0, 0, "", 0, false)
			} else {
				root := t.TempDir()
				free, dev, ok := volumeFreeBytesAtKnown(root)
				if !ok {
					t.Fatal("test cache volume unavailable")
				}
				SetCachePolicy(1<<30, free+(1<<30), root, dev, true)
				ComputeCapacity(store, root)
			}
			p := NewPrefetcher(store, "/definitely/not/a/mount", "/v", 1, capacityReadyProvider{})
			defer p.Stop()
			report, err := p.VerifyAndRepair(context.Background())
			if err != nil || report.AlreadyComplete != 1 || report.Reenqueued != 0 {
				t.Fatalf("valid Ready was requeued under %s capacity: report=%+v err=%v", mode, report, err)
			}
			after, ok := store.Get(path)
			if !ok || after.Status != StatusReady || after.Generation != ready.Generation || string(after.Authority) != string(ready.Authority) {
				t.Fatalf("Ready authority changed: %+v", after)
			}
		})
	}
}

func TestOldCapacityWalkCannotPublishAfterPolicyReplacement(t *testing.T) {
	t.Cleanup(func() { SetCachePolicy(0, 0, "", 0, false) })
	started, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = w.Write([]byte("juicefs_blockcache_bytes 0\n"))
	}))
	defer srv.Close()
	blockCache.mu.Lock()
	oldAddr, oldVal, oldOK, oldTry := blockCache.addr, blockCache.lastVal, blockCache.lastOK, blockCache.lastTry
	blockCache.mu.Unlock()
	t.Cleanup(func() {
		blockCache.mu.Lock()
		blockCache.addr, blockCache.lastVal, blockCache.lastOK, blockCache.lastTry = oldAddr, oldVal, oldOK, oldTry
		blockCache.mu.Unlock()
	})
	SetBlockCacheMetricsAddr(hostPort(srv.URL))
	root := t.TempDir()
	free, dev, ok := volumeFreeBytesAtKnown(root)
	if !ok || free < 16 {
		t.Fatal("test cache volume unavailable")
	}
	SetCachePolicy(free/8, free/2, root, dev, true)
	done := make(chan CapacityVerdict, 1)
	go func() { done <- ComputeCapacity(nil, root) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("capacity walk did not reach metrics rendezvous")
	}
	SetCachePolicy(free/8, free/3, root, dev, true)
	close(release)
	v := <-done
	if v.PolicyKnown || !Capacity().Computed.IsZero() {
		t.Fatalf("old walk published after policy replacement: result=%+v published=%+v", v, Capacity())
	}
}
