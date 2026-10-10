//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Give the ordinary Reader real Redis ancestry and slice evidence without
// reading the sidecar through NFS (which could mint a strong source proof).
func prepareResidentAppleDouble(t *testing.T, x *boundCacheFixture) (*cachedFile, *cache.Reader) {
	t.Helper()
	seedBoundAppleDouble(t, x)
	f := openBoundAppleDouble(t, x)
	reader := x.session.Reader()
	volume, bound := reader.VolumeBinding()
	if !bound {
		t.Fatal("fixture Reader has no selected binding")
	}
	identity, ok := x.store.LookupCachedFileIdentity(localSidecarPath)
	if !ok || identity != f.cacheIdentity {
		t.Fatal("sidecar lacks exact mirror identity")
	}
	epoch, ready := x.session.RemoteObservation()
	if !ready {
		t.Fatal("fixture has no remote observation for qualification")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	b := x.jfs.handler.cacheBinding.Load()
	selected := metadata.CachePathBinding{Digest: volume.Backend, FormatUUID: volume.UUID}
	if _, err := b.qualifyPathOwned(ctx, selected, identity, epoch); err != nil {
		t.Fatalf("qualify real sidecar ancestry: %v", err)
	}
	if err := reader.PrepareChunkMapping(ctx, identity.Inode, 0); err != nil {
		t.Fatalf("prepare real sidecar slice mapping: %v", err)
	}
	if !x.paths.CachePathEligible(selected, identity) ||
		x.jfs.handler.strongSidecarRead(localSidecarPath) != nil ||
		x.jfs.handler.beginSidecarSnapshotRead(localSidecarPath) != nil {
		t.Fatal("fixture unexpectedly lacks path proof or gained dedicated sidecar proof")
	}
	return f, reader
}

func TestResidentMetadataWideWireUsesVerifiedLocalEOF(t *testing.T) {
	if memBufServeEnabled {
		t.Fatal("requires disabled memory-buffer serve")
	}
	x := newBoundCacheFixture(t)
	f, reader := prepareResidentAppleDouble(t, x)
	var remoteProofs atomic.Int64
	b := x.jfs.handler.cacheBinding.Load()
	b.readProof = func(context.Context, string, string, []uint64) (metadata.RedisPathProof, error) {
		remoteProofs.Add(1)
		return metadata.RedisPathProof{}, context.DeadlineExceeded
	}
	reader.SetRemoteAvailable(false)
	// The complete 4 KiB body and paired attributes are on SSD. A backing
	// descriptor cannot answer this READ and no strong sidecar proof exists.
	if err := os.Remove(filepath.Join(x.fuseRoot, localSidecarPath)); err != nil {
		t.Fatal(err)
	}
	client := localSidecarLoopback(t, x.jfs.handler)
	beforeFUSE, beforeMetrics := fuseCellCounts(t), metrics.Default().Snapshot()
	localSidecarWireRead(t, client, x.jfs.handler, 1<<20, bytes.Repeat([]byte{'S'}, 4096), 1)
	afterFUSE, afterMetrics := fuseCellCounts(t), metrics.Default().Snapshot()
	for _, op := range []string{"foreground/open", "foreground/fstat", "foreground/pread"} {
		calls, _, _, _ := cellDelta(beforeFUSE, afterFUSE, op)
		if calls != 0 {
			t.Fatalf("resident metadata READ used %s: %d calls", op, calls)
		}
	}
	if afterMetrics.DirectSSDCacheHit-beforeMetrics.DirectSSDCacheHit != 1 ||
		afterMetrics.DirectSSDCacheHitBytes-beforeMetrics.DirectSSDCacheHitBytes != 4096 || remoteProofs.Load() != 0 {
		t.Fatalf("resident witness: SSD hits=%d bytes=%d remote proofs=%d",
			afterMetrics.DirectSSDCacheHit-beforeMetrics.DirectSSDCacheHit,
			afterMetrics.DirectSSDCacheHitBytes-beforeMetrics.DirectSSDCacheHitBytes, remoteProofs.Load())
	}
	if f.sidecarProof != nil || f.snapshotProof != nil || x.jfs.handler.strongSidecarRead(localSidecarPath) != nil {
		t.Fatal("resident EOF was incorrectly promoted to a dedicated sidecar source proof")
	}
}

func TestResidentMetadataRefusesMissingAndChangedAuthority(t *testing.T) {
	for _, scenario := range []string{"missing-block", "missing-mapping", "weak-body-only", "writer-generation", "namespace-generation", "ancestor-replacement", "binding-refusal"} {
		t.Run(scenario, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			f, reader := prepareResidentAppleDouble(t, x)
			if scenario == "missing-block" || scenario == "missing-mapping" || scenario == "weak-body-only" {
				chunks, err := cache.VolumeCacheDir("selected-volume")
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "missing-mapping" {
					reader.InvalidateSliceCache(f.inode)
				} else {
					if err := os.Remove(filepath.Join(chunks, "0/0/78_0_4096")); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "weak-body-only" {
					id := f.cacheIdentity
					x.jfs.handler.sidecar.put(localSidecarPath, bytes.Repeat([]byte{'S'}, 4096), id.MtimeNS/1e9, id.Size)
				}
				reader.SetRemoteAvailable(false)
				prepared, err := f.PrepareResidentRead(0, 1<<20)
				if err != nil || prepared != nil {
					t.Fatalf("missing CRC block or weak body minted terminal EOF: proof=%v err=%v", prepared != nil, err)
				}
				return
			}
			admissionCheck, release, err := f.BeginReadAdmission()
			if err != nil {
				t.Fatal(err)
			}
			if release != nil {
				defer release()
			}
			prepared, err := f.PrepareResidentRead(0, 1<<20)
			if err != nil || prepared == nil || prepared.Check == nil || len(prepared.Data) != 4096 {
				t.Fatalf("missing positive resident control: proof=%v err=%v", prepared != nil, err)
			}
			if err := prepared.Check(); err != nil {
				t.Fatalf("unchanged resident control refused: %v", err)
			}
			switch scenario {
			case "writer-generation":
				x.jfs.handler.incActiveWriter(localSidecarPath)
				x.jfs.handler.decActiveWriter(localSidecarPath)
			case "namespace-generation":
				op := x.jfs.handler.namespace.begin([]string{localSidecarPath})
				if op == nil {
					t.Fatal("namespace operation not admitted")
				}
				op.blockReads()
				op.finish(nil)
			case "ancestor-replacement":
				replaceBoundCacheEntry(t, x, "media")
			case "binding-refusal":
				x.session.RefuseIdentity(cache.ErrVolumeBinding)
			}
			preparedErr := prepared.Check()
			admissionErr := admissionCheck()
			if preparedErr == nil && admissionErr == nil {
				t.Fatal("changed authority retained both resident and READ-admission proof")
			}
			if f.fuseFD != nil {
				t.Fatal("authority check opened backing descriptor")
			}
		})
	}
}
