//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// The existing prepared-read test waits for the preview worker to finish.
// This test instead observes a foreground read while that worker still owns
// the same path's remote proof. Both calls must settle before fixture teardown.
func TestSlowPreviewForegroundOverlapsHeldPathProof(t *testing.T) {
	x, previewDone := slowMetadataFixture(t, true)
	h := x.jfs.handler
	b := h.cacheBinding.Load()
	originalProof := b.readProof
	entered := make(chan int64, 2)
	joined := make(chan struct{}, 1)
	b.testProofFlightJoin = func() { joined <- struct{}{} }
	release := make(chan struct{})
	var once sync.Once
	var calls atomic.Int64
	var active atomic.Int64
	b.readProof = func(ctx context.Context, uuid, path string, ids []uint64) (metadata.RedisPathProof, error) {
		if path != "media/clip.mov" {
			return originalProof(ctx, uuid, path, ids)
		}
		active.Add(1)
		defer active.Add(-1)
		sequence := calls.Add(1)
		entered <- sequence
		select {
		case <-release:
		case <-ctx.Done():
			return metadata.RedisPathProof{}, ctx.Err()
		}
		return originalProof(ctx, uuid, path, ids)
	}
	var foregroundDone chan struct{}
	previewEntered := false
	defer func() {
		once.Do(func() { close(release) })
		if foregroundDone != nil {
			<-foregroundDone
		}
		if previewEntered {
			<-previewDone
		}
	}()
	cf := openBoundCacheFile(t, x)
	_ = p86ReadDirPrompt(t, x)
	select {
	case n := <-entered:
		previewEntered = true
		if n != 1 {
			t.Fatalf("first proof call=%d, want preview call one", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("preview did not enter target path proof")
	}
	type readResult struct {
		n   int
		err error
		buf []byte
	}
	result := make(chan readResult, 1)
	foregroundDone = make(chan struct{})
	go func() {
		defer close(foregroundDone)
		buf := make([]byte, 4096)
		n, err := cf.ReadAt(buf, 0)
		result <- readResult{n: n, err: err, buf: buf}
	}()
	select {
	case <-joined:
		if active.Load() != 1 || calls.Load() != 1 {
			t.Fatalf("joined call started duplicate proof: active=%d calls=%d", active.Load(), calls.Load())
		}
	case n := <-entered:
		t.Fatalf("foreground started duplicate remote proof call %d", n)
	case <-time.After(3 * time.Second):
		t.Fatal("foreground neither joined nor started proof while preview proof held")
	}
	once.Do(func() { close(release) })
	select {
	case got := <-result:
		if got.err != nil || got.n != len(got.buf) || !bytes.Equal(got.buf, x.want[:len(got.buf)]) {
			t.Fatalf("foreground overlap read n=%d err=%v bytes=%q", got.n, got.err, got.buf[:8])
		}
	case <-time.After(6 * time.Second):
		t.Fatal("foreground read did not settle after proof release")
	}
	<-foregroundDone
	foregroundDone = nil
	if cf.fuseFD != nil {
		t.Fatal("foreground overlap read opened FUSE")
	}
	select {
	case <-previewDone:
		previewEntered = false
	case <-time.After(5 * time.Second):
		t.Fatal("preview worker did not join after proof release")
	}
	if calls.Load() != 1 {
		t.Fatalf("same-path remote proof calls=%d, want one shared call", calls.Load())
	}
	control := openBoundCacheFile(t, x)
	assertPhaseRead(t, control, 'B', false)
}

func TestCacheProofFlightExpiredRefusesFallback(t *testing.T) {
	for _, joinBeforeExpiry := range []bool{false, true} {
		name := "late-caller"
		if joinBeforeExpiry {
			name = "joined-caller"
		}
		t.Run(name, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			b := x.jfs.handler.cacheBinding.Load()
			id, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
			if !ok {
				t.Fatal("missing mirrored file")
			}
			binding, bound := x.session.Reader().VolumeBinding()
			if !bound {
				t.Fatal("missing binding")
			}
			selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
			epoch, ready := b.session.RemoteObservation()
			if !ready {
				t.Fatal("missing remote observation")
			}
			entered := make(chan struct{}, 1)
			joined := make(chan struct{}, 1)
			b.testProofFlightJoin = func() { joined <- struct{}{} }
			release := make(chan struct{})
			var releaseOnce sync.Once
			var calls atomic.Int64
			b.readProof = func(context.Context, string, string, []uint64) (metadata.RedisPathProof, error) {
				calls.Add(1)
				entered <- struct{}{}
				<-release // The actual backend ignores canceled work until released.
				return metadata.RedisPathProof{}, nil
			}
			ownerDone := make(chan error, 1)
			go func() { _, err := b.qualifyPath(context.Background(), selected, id, epoch); ownerDone <- err }()
			var foregroundDone chan struct{}
			defer func() {
				releaseOnce.Do(func() { close(release) })
				if ownerDone != nil {
					<-ownerDone
				}
				if foregroundDone != nil {
					<-foregroundDone
				}
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("physical proof did not enter")
			}
			cf := openBoundCacheFile(t, x)
			type readResult struct {
				n   int
				err error
				buf []byte
			}
			result := make(chan readResult, 1)
			read := func() {
				foregroundDone = make(chan struct{})
				go func() {
					defer close(foregroundDone)
					buf := make([]byte, 4096)
					n, err := cf.ReadAt(buf, 0)
					result <- readResult{n, err, buf}
				}()
			}
			if joinBeforeExpiry {
				read()
				select {
				case <-joined:
				case <-time.After(3 * time.Second):
					t.Fatal("foreground did not join physical proof")
				}
			}
			b.proofMu.Lock()
			flightCount := len(b.proofFlights)
			if flightCount != 1 {
				b.proofMu.Unlock()
				t.Fatalf("physical flight count=%d", flightCount)
			}
			flight := b.proofFlights[0]
			b.proofMu.Unlock()
			flight.cancel() // Deterministic expiry without a fixed sleep.
			if !joinBeforeExpiry {
				read()
			}
			if joinBeforeExpiry {
				releaseOnce.Do(func() { close(release) })
			}
			select {
			case got := <-result:
				if got.n != 0 || !errors.Is(got.err, syscall.EAGAIN) || cf.fuseFD != nil || !bytes.Equal(got.buf, make([]byte, len(got.buf))) {
					t.Fatalf("expired shared proof allowed fallback: n=%d err=%v FUSE=%v", got.n, got.err, cf.fuseFD != nil)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("expired proof did not refuse foreground")
			}
			if calls.Load() != 1 {
				t.Fatalf("expired physical proof duplicated: %d", calls.Load())
			}
			releaseOnce.Do(func() { close(release) })
			ownerErr := <-ownerDone
			ownerDone = nil
			if !errors.Is(ownerErr, syscall.EAGAIN) {
				t.Fatalf("expired owner returned %v", ownerErr)
			}
			<-foregroundDone
			foregroundDone = nil
		})
	}
}

// The preview owns the physical request, not the foreground caller's deadline.
// Its canceled caller cannot cancel a still-live foreground joiner or release
// the handler's physical-work slot before the backend call actually settles.
func TestSlowPreviewProofOwnerCancellationRetainsLiveJoiner(t *testing.T) {
	x := newBoundCacheFixture(t)
	b := x.jfs.handler.cacheBinding.Load()
	id, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok {
		t.Fatal("missing mirrored file")
	}
	epoch, ready := b.session.RemoteObservation()
	if !ready {
		t.Fatal("missing remote observation")
	}
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("missing volume binding")
	}
	selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
	originalProof := b.readProof
	entered := make(chan struct{}, 1)
	joined := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var calls atomic.Int64
	b.testProofFlightJoin = func() { joined <- struct{}{} }
	b.readProof = func(ctx context.Context, uuid, path string, ids []uint64) (metadata.RedisPathProof, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release // Deliberately model a remote call that ignores cancellation.
		return originalProof(ctx, uuid, path, ids)
	}
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerDone := make(chan error, 1)
	ownerJoined := make(chan struct{})
	go func() {
		defer close(ownerJoined)
		_, err := b.qualifyPath(ownerCtx, selected, id, epoch)
		ownerDone <- err
	}()
	var foregroundJoined chan struct{}
	defer func() {
		cancelOwner()
		releaseOnce.Do(func() { close(release) })
		<-ownerJoined
		if foregroundJoined != nil {
			<-foregroundJoined
		}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("owner did not enter physical proof")
	}
	cf := openBoundCacheFile(t, x)
	foregroundJoined = make(chan struct{})
	type readResult struct {
		n   int
		err error
		buf []byte
	}
	readDone := make(chan readResult, 1)
	go func() {
		defer close(foregroundJoined)
		buf := make([]byte, 4096)
		n, err := cf.ReadAt(buf, 0)
		readDone <- readResult{n, err, buf}
	}()
	select {
	case <-joined:
	case <-time.After(3 * time.Second):
		t.Fatal("foreground did not join the held proof")
	}
	cancelOwner()
	select {
	case err := <-ownerDone:
		if err != syscall.EAGAIN {
			t.Fatalf("canceled owner returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled owner caller did not return")
	}
	b.workMu.Lock()
	active := b.workActive
	b.workMu.Unlock()
	if active < 2 || calls.Load() != 1 {
		t.Fatalf("physical owner or joiner lost: active=%d proofs=%d", active, calls.Load())
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case got := <-readDone:
		if got.err != nil || got.n != len(got.buf) || !bytes.Equal(got.buf, x.want[:len(got.buf)]) || cf.fuseFD != nil {
			t.Fatalf("joined foreground read n=%d err=%v FUSE=%v", got.n, got.err, cf.fuseFD != nil)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("joined foreground read did not settle")
	}
	<-foregroundJoined
	foregroundJoined = nil
	if calls.Load() != 1 {
		t.Fatalf("remote proofs after owner cancellation=%d, want one", calls.Load())
	}
}

func TestCacheProofFlightKeyRetainsFullObservation(t *testing.T) {
	x := newBoundCacheFixture(t)
	observed, err := x.paths.CaptureCachePathContext(context.Background(), "media/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("missing binding")
	}
	selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
	flight := &cacheProofFlight{selected: selected, observed: observed, epoch: 7}
	if !sameProofFlight(flight, selected, observed, 7) {
		t.Fatal("identical complete proof observation did not match")
	}
	otherBinding := selected
	otherBinding.Digest += "changed"
	if sameProofFlight(flight, otherBinding, observed, 7) || sameProofFlight(flight, selected, observed, 8) {
		t.Fatal("changed binding or remote epoch shared the flight")
	}
	other := observed
	other.Epoch.Epoch++
	if sameProofFlight(flight, selected, other, 7) {
		t.Fatal("changed store epoch shared the flight")
	}
	other = observed
	other.Chain = append([]metadata.CachePathNode(nil), observed.Chain...)
	other.Chain[0].Token.Revision++
	if sameProofFlight(flight, selected, other, 7) {
		t.Fatal("changed ancestor token shared the flight")
	}
}

func TestCacheProofFlightChangedAncestorStartsIndependentProof(t *testing.T) {
	x := newBoundCacheFixture(t)
	b := x.jfs.handler.cacheBinding.Load()
	id, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok {
		t.Fatal("missing mirrored file")
	}
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("missing binding")
	}
	selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
	epoch, ready := b.session.RemoteObservation()
	if !ready {
		t.Fatal("missing remote observation")
	}
	originalProof := b.readProof
	entered := make(chan int64, 2)
	release := make(chan struct{})
	var once sync.Once
	var calls atomic.Int64
	b.readProof = func(ctx context.Context, uuid, path string, ids []uint64) (metadata.RedisPathProof, error) {
		n := calls.Add(1)
		entered <- n
		if n == 1 {
			<-release // Keep the old observation physically in flight.
		}
		return originalProof(ctx, uuid, path, ids)
	}
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	var startedFirst, startedSecond bool
	defer func() {
		once.Do(func() { close(release) })
		if startedFirst {
			<-firstDone
		}
		if startedSecond {
			<-secondDone
		}
	}()
	startedFirst = true
	go func() { _, err := b.qualifyPath(context.Background(), selected, id, epoch); firstDone <- err }()
	select {
	case n := <-entered:
		if n != 1 {
			t.Fatalf("first remote proof call=%d", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first proof did not enter")
	}
	before, err := x.paths.CaptureCachePathContext(context.Background(), id.Path)
	if err != nil {
		t.Fatal(err)
	}
	replaceBoundCacheEntry(t, x, "media")
	after, err := x.paths.CaptureCachePathContext(context.Background(), id.Path)
	if err != nil {
		t.Fatal(err)
	}
	if sameProofFlight(&cacheProofFlight{selected: selected, observed: before, epoch: epoch}, selected, after, epoch) || before.Chain[len(before.Chain)-1] != after.Chain[len(after.Chain)-1] {
		t.Fatal("ancestor mutation did not preserve the leaf while changing the full observation")
	}
	startedSecond = true
	go func() { _, err := b.qualifyPath(context.Background(), selected, id, epoch); secondDone <- err }()
	select {
	case n := <-entered:
		if n != 2 {
			t.Fatalf("changed ancestor reused physical proof call %d", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("changed ancestor joined old proof instead of starting its own")
	}
	once.Do(func() { close(release) })
	firstErr := <-firstDone
	startedFirst = false
	if firstErr != syscall.EAGAIN {
		t.Fatalf("stale ancestor first qualifier returned %v", firstErr)
	}
	secondErr := <-secondDone
	startedSecond = false
	if secondErr != nil {
		t.Fatalf("current ancestor qualifier returned %v", secondErr)
	}
}
