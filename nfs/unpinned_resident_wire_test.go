//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

// A qualified ordinary file must use its retained mapping and verified SSD
// bytes even when both metadata entry points would refuse new work. The
// fixture deliberately stores B on SSD and F in the backing file.
func TestUnpinnedResidentWireReadNeedsNoRemoteMetadata(t *testing.T) {
	if memBufServeEnabled {
		t.Fatal("requires disabled memory buffer")
	}
	x := newBoundCacheFixture(t)
	reader, hook := phaseReader(t, x)
	defer hook.releaseOnce.Do(func() { close(hook.release) })
	pins, err := pin.Open(filepath.Join(t.TempDir(), "empty-pins.db"))
	if err != nil {
		t.Fatal(err)
	}
	x.jfs.handler.SetPinStore(pins, "/private-volume")
	t.Cleanup(func() {
		x.jfs.handler.SetPinStore(nil, "/private-volume")
		if err := pins.Close(); err != nil {
			t.Error(err)
		}
	})
	canonical := "/private-volume/media/clip.mov"
	if entry, exists := pins.Get(canonical); exists || entry != nil || pins.IsPinnedReady(canonical) {
		t.Fatal("test file unexpectedly has pin authority")
	}

	// Ordinary warm read establishes the selected-volume ancestry proof and
	// one RAM slice mapping. A fresh NFS READ below is the measured operation.
	control := openBoundCacheFile(t, x)
	assertPhaseRead(t, control, 'B', false)
	assertPhasePathQualified(t, x, reader)
	if hook.calls.Load() != 1 {
		t.Fatalf("warm mapping calls=%d, want one", hook.calls.Load())
	}
	client := localSidecarLoopback(t, x.jfs.handler)
	b := x.jfs.handler.cacheBinding.Load()
	var remoteProofs, cacheEntries atomic.Int64
	b.readProof = func(context.Context, string, string, []uint64) (metadata.RedisPathProof, error) {
		remoteProofs.Add(1)
		return metadata.RedisPathProof{}, errors.New("test-owned path proof refusal")
	}
	b.beforeCacheRead = func() { cacheEntries.Add(1) }
	hook.armed.Store(true)

	for _, offline := range []bool{false, true} {
		name := "online"
		if offline {
			name = "offline"
			x.session.SetOnline(false)
			reader.SetRemoteAvailable(false) // phaseReader replaced the session's Reader
			pin.SetOffline(true)
		}
		t.Run(name, func(t *testing.T) {
			beforeFUSE := fuseCellCounts(t)
			beforeMetrics := metrics.Default().Snapshot()
			beforeMappings, beforeProofs := hook.calls.Load(), remoteProofs.Load()
			beforeCache := cacheEntries.Load()
			residentWireRead(t, client, x.jfs.handler, 0, 4096, x.want[:4096], 0)
			afterFUSE := fuseCellCounts(t)
			for key, cell := range afterFUSE {
				if len(key) >= len("foreground/") && key[:len("foreground/")] == "foreground/" && cell.Calls != beforeFUSE[key].Calls {
					t.Fatalf("resident wire READ used %s: before=%d after=%d", key, beforeFUSE[key].Calls, cell.Calls)
				}
			}
			if delta := metrics.Default().Snapshot().DirectSSDCacheHit - beforeMetrics.DirectSSDCacheHit; delta != 1 {
				t.Fatalf("verified SSD hits=%d, want one body hit", delta)
			}
			if hook.calls.Load() != beforeMappings || remoteProofs.Load() != beforeProofs {
				t.Fatalf("resident wire READ attempted remote mapping/proof: mapping=%d proof=%d", hook.calls.Load()-beforeMappings, remoteProofs.Load()-beforeProofs)
			}
			wantCache := int64(1)
			if offline {
				wantCache++ // actual empty-pin-store OpenFile probe, then body
			}
			if got := cacheEntries.Load() - beforeCache; got != wantCache {
				t.Fatalf("cache entries=%d, want %d (offline probe included)", got, wantCache)
			}
		})
	}

	// With only the mapping removed, the CRC-backed B block is still on disk.
	// The armed hook must now be observed. F from the backing file proves a
	// missing mapping was not misclassified as a resident SSD hit.
	pin.SetOffline(false)
	reader.SetRemoteAvailable(true) // negative control must reach the armed mapping hook
	reader.InvalidateSliceCache(3)
	assertPhasePathQualified(t, x, reader)
	beforeMappings, beforeProofs := hook.calls.Load(), remoteProofs.Load()
	beforeFUSE := fuseCellCounts(t)
	beforeMetrics := metrics.Default().Snapshot()
	residentWireRead(t, client, x.jfs.handler, 0, 4096, bytes.Repeat([]byte{'F'}, 4096), 0)
	if hook.calls.Load() != beforeMappings+1 || hook.finished.Load() < 1 || remoteProofs.Load() != beforeProofs {
		t.Fatalf("negative control mapping/proof calls=%d/%d finished=%d", hook.calls.Load()-beforeMappings, remoteProofs.Load()-beforeProofs, hook.finished.Load())
	}
	if delta := metrics.Default().Snapshot().DirectSSDCacheHit - beforeMetrics.DirectSSDCacheHit; delta != 0 {
		t.Fatalf("mapping miss reported %d verified SSD hits", delta)
	}
	afterFUSE := fuseCellCounts(t)
	open, _, _, _ := cellDelta(beforeFUSE, afterFUSE, "foreground/open")
	if open != 1 {
		t.Fatalf("mapping miss backing opens=%d, want one", open)
	}
}
