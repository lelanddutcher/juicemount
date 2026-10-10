//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

// This uses the real Redis directory reconcile, not a direct call to the
// preparation worker. The candidate is queued before worker activation, as
// happens when boot SyncOnce commits before handler/spool attachment.
func TestMetadataProducerReconcileToResidentWire(t *testing.T) {
	if memBufServeEnabled {
		t.Fatal("requires disabled memory-buffer serving")
	}
	x := newBoundCacheFixture(t)
	body := seedBoundAppleDouble(t, x) // valid CRC block, but no map/path proof
	h := x.jfs.handler
	if err := x.store.Delete(localSidecarPath); err != nil {
		t.Fatal(err)
	}
	hints := make(chan metadata.CacheCandidate, 16)
	var offered atomic.Int64
	x.rc.SetCacheCandidateSink(func(c metadata.CacheCandidate) {
		offered.Add(1)
		select {
		case hints <- c:
		default:
		}
	})
	if err := x.rc.ReconcileAncestors(localSidecarPath); err != nil {
		t.Fatal(err)
	}
	if offered.Load() != 1 || len(hints) != 1 {
		t.Fatalf("real reconcile candidate count=%d pending=%d", offered.Load(), len(hints))
	}
	id, ok := x.store.LookupCachedFileIdentity(localSidecarPath)
	if !ok {
		t.Fatal("real reconciliation did not publish sidecar identity")
	}
	volume, _ := x.session.Reader().VolumeBinding()
	selected := metadata.CachePathBinding{Digest: volume.Backend, FormatUUID: volume.UUID}
	if x.paths.CachePathEligible(selected, id) {
		t.Fatal("hint alone granted path authority")
	}
	// A missing paired map must not be mistaken for the preseeded CRC body.
	attr := cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}
	if _, err := x.session.Reader().ReadResidentRange(id.Inode, 0, make([]byte, int(id.Size)), attr); !errors.Is(err, cache.ErrCacheMiss) {
		t.Fatalf("hint alone granted resident bytes: %v", err)
	}
	// Session construction in production first sets auto-offline and recovery
	// clears it only after spool wiring. The buffered boot hint must survive.
	pin.SetAutoOffline(true, "boot qualification pending")
	t.Cleanup(func() { pin.SetAutoOffline(false, "") })
	h.StartMetadataPreparation(hints)
	defer func() {
		select {
		case <-h.beginMetadataPreparationStop():
		case <-time.After(6 * time.Second):
			t.Error("producer did not physically join during cleanup")
		}
	}()
	// Cross multiple worker offline-poll intervals. This is a controlled
	// recovery surrogate, not a bridge safe-recovery integration test.
	select {
	case <-time.After(250 * time.Millisecond):
	case <-h.metadataPrepareDone:
		t.Fatal("metadata worker exited before offline retention was checked")
	}
	if len(hints) != 1 || x.paths.CachePathEligible(selected, id) {
		t.Fatal("offline boot consumed a buffered hint or granted authority")
	}
	pin.SetAutoOffline(false, "")
	deadline := time.After(6 * time.Second)
	for {
		if x.paths.CachePathEligible(selected, id) {
			if token, err := x.session.Reader().ReadResidentRange(id.Inode, 0, make([]byte, int(id.Size)), attr); err == nil && token.Valid() {
				break
			}
		}
		select {
		case <-deadline:
			t.Fatal("production hint did not produce paired local path/map/CRC authority")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Join the original producer before arming refusals or closing the fixture.
	select {
	case <-h.beginMetadataPreparationStop():
	case <-time.After(6 * time.Second):
		t.Fatal("producer did not physically join")
	}
	var proofs atomic.Int64
	b := h.cacheBinding.Load()
	b.readProof = func(context.Context, string, string, []uint64) (metadata.RedisPathProof, error) {
		proofs.Add(1)
		return metadata.RedisPathProof{}, context.DeadlineExceeded
	}
	x.session.Reader().SetRemoteAvailable(false)
	h.testRefuseFUSEContentReads = true
	if err := os.Remove(filepath.Join(x.fuseRoot, localSidecarPath)); err != nil {
		t.Fatal(err)
	}
	client := localSidecarLoopback(t, h)
	beforeFUSE, before := fuseCellCounts(t), metrics.Default().Snapshot()
	localSidecarWireRead(t, client, h, 1<<20, body, 1)
	afterFUSE, after := fuseCellCounts(t), metrics.Default().Snapshot()
	for _, op := range []string{"foreground/open", "foreground/fstat", "foreground/pread"} {
		calls, _, _, _ := cellDelta(beforeFUSE, afterFUSE, op)
		if calls != 0 {
			t.Fatalf("prepared wire read used %s: %d", op, calls)
		}
	}
	if after.DirectSSDCacheHit-before.DirectSSDCacheHit != 1 ||
		after.DirectSSDCacheHitBytes-before.DirectSSDCacheHitBytes != 4096 || proofs.Load() != 0 ||
		h.strongSidecarRead(localSidecarPath) != nil {
		t.Fatalf("wrong resident route: SSD hits=%d bytes=%d proof calls=%d strong=%v",
			after.DirectSSDCacheHit-before.DirectSSDCacheHit,
			after.DirectSSDCacheHitBytes-before.DirectSSDCacheHitBytes, proofs.Load(), h.strongSidecarRead(localSidecarPath) != nil)
	}
	if !bytes.Equal(body, bytes.Repeat([]byte{'S'}, 4096)) {
		t.Fatal("fixture source changed")
	}
}

func TestMetadataProducerStaleHintRefused(t *testing.T) {
	x := newBoundCacheFixture(t)
	seedBoundAppleDouble(t, x)
	h := x.jfs.handler
	if err := x.store.Delete(localSidecarPath); err != nil {
		t.Fatal(err)
	}
	hints := make(chan metadata.CacheCandidate, 16)
	x.rc.SetCacheCandidateSink(func(c metadata.CacheCandidate) { hints <- c })
	if err := x.rc.ReconcileAncestors(localSidecarPath); err != nil {
		t.Fatal(err)
	}
	if len(hints) != 1 {
		t.Fatalf("expected one committed candidate, got %d", len(hints))
	}
	old := <-hints
	// Replacing the leaf before activation leaves a buffered, stale hint.
	replaceBoundCacheEntry(t, x, localSidecarPath)
	var proofCalls atomic.Int64
	h.cacheBinding.Load().readProof = func(context.Context, string, string, []uint64) (metadata.RedisPathProof, error) {
		proofCalls.Add(1)
		return metadata.RedisPathProof{}, context.DeadlineExceeded
	}
	h.prepareMetadataHint(old) // refusal control only; positive uses real event/worker
	volume, _ := x.session.Reader().VolumeBinding()
	selected := metadata.CachePathBinding{Digest: volume.Backend, FormatUUID: volume.UUID}
	newID, found := x.store.LookupCachedFileIdentity(localSidecarPath)
	if !found || newID == old.Identity || x.paths.CachePathEligible(selected, old.Identity) ||
		x.paths.CachePathEligible(selected, newID) || proofCalls.Load() != 0 {
		t.Fatal("stale hint qualified or probed a successor")
	}
}

func TestMetadataProducerHeldProofRetainsStopAndBinding(t *testing.T) {
	x := newBoundCacheFixture(t)
	seedBoundAppleDouble(t, x)
	h := x.jfs.handler
	if err := x.store.Delete(localSidecarPath); err != nil {
		t.Fatal(err)
	}
	hints := make(chan metadata.CacheCandidate, 16)
	x.rc.SetCacheCandidateSink(func(c metadata.CacheCandidate) { hints <- c })
	entered, release := make(chan struct{}), make(chan struct{})
	b := h.cacheBinding.Load()
	b.readProof = func(context.Context, string, string, []uint64) (metadata.RedisPathProof, error) {
		close(entered)
		<-release // deliberately ignores cancellation: original work is retained
		return metadata.RedisPathProof{}, context.Canceled
	}
	if err := x.rc.ReconcileAncestors(localSidecarPath); err != nil {
		t.Fatal(err)
	}
	if len(hints) != 1 {
		t.Fatalf("expected committed candidate, got %d", len(hints))
	}
	h.StartMetadataPreparation(hints)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		select {
		case <-h.beginMetadataPreparationStop():
		case <-time.After(3 * time.Second):
			t.Error("held original proof did not join during cleanup")
		}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("original proof worker did not enter")
	}
	// Binding replacement must not transfer ownership of the old call.
	h.SetBoundCacheSession(x.session, x.rc, x.paths)
	if h.stopHandlerWithDeadline(20 * time.Millisecond) {
		t.Fatal("Stop joined while original proof call was still held")
	}
	close(release)
	select {
	case <-h.beginMetadataPreparationStop():
	case <-time.After(3 * time.Second):
		t.Fatal("held original proof did not settle after release")
	}
	if !h.stopHandlerWithDeadline(3 * time.Second) {
		t.Fatal("Stop did not join after original proof settled")
	}
}

// Construct source-branch controls from a real publication hint and full
// authority. The backing file is ordinary owned test storage, not JuiceFS;
// these tests cannot prove that a JuiceFS read populates its disk cache.
func metadataSourceFixture(t *testing.T, blockSnapshot bool) (*boundCacheFixture, *metadataPrepareJob) {
	t.Helper()
	x := newBoundCacheFixture(t)
	seedBoundAppleDouble(t, x)
	h := x.jfs.handler
	if blockSnapshot {
		// No mappings have been fetched yet. A directory at the snapshot's
		// destination makes its atomic file rename fail deterministically.
		if err := os.Mkdir(x.session.Reader().MappingSnapshotPath(x.state), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := x.store.Delete(localSidecarPath); err != nil {
		t.Fatal(err)
	}
	hints := make(chan metadata.CacheCandidate, 16)
	x.rc.SetCacheCandidateSink(func(c metadata.CacheCandidate) {
		select {
		case hints <- c:
		default:
		}
	})
	if err := x.rc.ReconcileAncestors(localSidecarPath); err != nil {
		t.Fatal(err)
	}
	if len(hints) != 1 {
		t.Fatalf("source fixture committed hints=%d", len(hints))
	}
	hint := <-hints
	b, r := h.cacheBinding.Load(), x.session.Reader()
	volume, bound := r.VolumeBinding()
	epoch, online := x.session.RemoteObservation()
	if !bound || !online {
		t.Fatal("source fixture is not qualified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	selected := metadata.CachePathBinding{Digest: volume.Backend, FormatUUID: volume.UUID}
	observed, err := b.qualifyPathOwned(ctx, selected, hint.Identity, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.PrepareChunkMapping(ctx, hint.Identity.Inode, 0); err != nil {
		t.Fatal(err)
	}
	ticket, err := h.namespace.admitRead(localSidecarPath)
	if err != nil {
		t.Fatal(err)
	}
	h.activeWritersMu.Lock()
	writer := h.activeWriterGen[localSidecarPath]
	h.activeWritersMu.Unlock()
	b.workMu.Lock()
	b.workActive++
	b.workMu.Unlock()
	j := &metadataPrepareJob{h: h, b: b, r: r, hint: hint, volume: volume, epoch: epoch,
		observed: observed, writer: writer, ticket: ticket}
	t.Cleanup(j.release)
	if !j.current() {
		t.Fatal("source fixture lacks current full-path authority")
	}
	return x, j
}

func TestMetadataProducerResidentDoesNotDependOnSnapshotWrite(t *testing.T) {
	_, j := metadataSourceFixture(t, true)
	if err := j.r.FlushMappingSnapshot(); err == nil {
		t.Fatal("snapshot failure control did not fail")
	}
	if !j.resident() {
		t.Fatal("snapshot persistence failure misclassified verified local content as missing")
	}
}

func TestMetadataProducerSourceReadDoesNotMintResidentBytes(t *testing.T) {
	_, j := metadataSourceFixture(t, false)
	if !j.resident() {
		t.Fatal("positive control lacks verified resident content before block removal")
	}
	chunks, err := cache.VolumeCacheDir("selected-volume")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(chunks, "0/0/78_0_4096")); err != nil {
		t.Fatal(err)
	}
	if j.resident() {
		t.Fatal("missing CRC block was accepted before source read")
	}
	if !j.readSource() {
		t.Fatal("matching original file did not pass real size+1 read and EOF verification")
	}
	if j.resident() {
		t.Fatal("ordinary source read falsely minted SSD residency")
	}
	id := j.hint.Identity
	_, err = j.r.ReadResidentRange(id.Inode, 0, make([]byte, int(id.Size)),
		cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true})
	if !errors.Is(err, cache.ErrCacheMiss) {
		t.Fatalf("absent physical cache must remain a cache miss after source read: %v", err)
	}
}

func TestMetadataProducerSourceRefusesChangedIdentity(t *testing.T) {
	for _, scenario := range []string{"replacement-inode", "truncated", "extended", "mtime", "ancestor", "writer-aba", "namespace"} {
		t.Run(scenario, func(t *testing.T) {
			x, j := metadataSourceFixture(t, false)
			source := filepath.Join(x.fuseRoot, localSidecarPath)
			id := j.hint.Identity
			switch scenario {
			case "replacement-inode":
				replacement := source + ".replacement"
				if err := os.WriteFile(replacement, bytes.Repeat([]byte{'S'}, int(id.Size)), 0600); err != nil {
					t.Fatal(err)
				}
				stamp := time.Unix(0, id.MtimeNS)
				if err := os.Chtimes(replacement, stamp, stamp); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(replacement, source); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				if err := os.Truncate(source, id.Size-1); err != nil {
					t.Fatal(err)
				}
			case "extended":
				if err := os.Truncate(source, id.Size+1); err != nil {
					t.Fatal(err)
				}
			case "mtime":
				stamp := time.Unix(0, id.MtimeNS).Add(time.Second)
				if err := os.Chtimes(source, stamp, stamp); err != nil {
					t.Fatal(err)
				}
			case "ancestor":
				replaceBoundCacheEntry(t, x, "media")
			case "writer-aba":
				j.h.incActiveWriter(localSidecarPath)
				j.h.decActiveWriter(localSidecarPath)
			case "namespace":
				op := j.h.namespace.begin([]string{localSidecarPath})
				if op == nil {
					t.Fatal("namespace mutation not admitted")
				}
				op.blockReads()
				op.finish(nil)
			}
			if j.readSource() {
				t.Fatal("changed source or admission accepted")
			}
		})
	}
}
