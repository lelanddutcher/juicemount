//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/redis/go-redis/v9"
)

// The first selected-volume mapping lookup fails while the ordinary FUSE
// demand succeeds. The second is the bounded, owned metadata handoff.
type demandFirstMappingMiss struct {
	calls         atomic.Int64
	refuse        atomic.Bool
	holdSecond    chan struct{}
	secondEntered chan struct{}
}

func (*demandFirstMappingMiss) DialHook(next redis.DialHook) redis.DialHook { return next }
func (*demandFirstMappingMiss) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *demandFirstMappingMiss) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" {
			for _, arg := range cmd.Args() {
				if fmt.Sprint(arg) == "c3_0" {
					call := h.calls.Add(1)
					if call == 1 {
						return fmt.Errorf("first mapping deadline: %w", context.DeadlineExceeded)
					}
					if h.refuse.Load() {
						return errors.New("test-owned mapping refusal")
					}
					if call == 2 && h.holdSecond != nil {
						close(h.secondEntered)
						<-h.holdSecond // intentionally outlives ctx cancellation until test releases it
					}
				}
			}
		}
		return next(ctx, cmd)
	}
}

func TestDemandMappingSuccessfulFallbackPreparesFreshRead(t *testing.T) {
	x := newBoundCacheFixture(t)
	h := x.jfs.handler
	defer func() { <-h.beginDemandStop() }() // settle the original worker before reader cleanup, including on Fatal
	h.sidecar.enabled = false
	// Backing and CRC bytes match: route/counters, not body alone, establish
	// that the second handle was served by the verified local block.
	if err := os.WriteFile(filepath.Join(x.fuseRoot, "media/clip.mov"), x.want, 0600); err != nil {
		t.Fatal(err)
	}
	const off int64 = 8192 // short, nonsequential demand; no readahead worker
	control := openBoundCacheFile(t, x)
	buf := make([]byte, 4096)
	if n, err := control.ReadAt(buf, off); n != len(buf) || err != nil || !bytes.Equal(buf, x.want[off:off+4096]) {
		t.Fatalf("qualification control n=%d err=%v", n, err)
	}
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("fixture lacks volume binding")
	}
	hook := &demandFirstMappingMiss{}
	rdb := redis.NewClient(x.rdb.Options())
	rdb.AddHook(hook)
	reader, err := cache.NewVolumeReader(binding, rdb)
	if err != nil {
		_ = rdb.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Stop(); _ = rdb.Close() })
	h.SetCacheReader(reader)
	assertPhasePathQualified(t, x, reader)
	first := openBoundCacheFile(t, x)
	before := metrics.Default().Snapshot()
	var firstTrace cachedReadTrace
	n, err := first.readAtTraced(buf, off, &firstTrace)
	if n != len(buf) || err != nil || !bytes.Equal(buf, x.want[off:off+4096]) || first.fuseFD == nil ||
		firstTrace.Reader.Outcome != "mapping_deadline" || firstTrace.FinalRoute != "fuse_online" {
		t.Fatalf("first demand n=%d err=%v route=%+v sourceOpen=%v", n, err, firstTrace, first.fuseFD != nil)
	}
	// The admission barrier joins the original physical metadata worker. A
	// fast return here without preparing a map makes the fresh control fail.
	select {
	case <-h.beginDemandStop():
	case <-time.After(5 * time.Second):
		t.Fatal("owned demand preparation did not settle")
	}
	if hook.calls.Load() != 2 {
		t.Fatalf("mapping lookups=%d, want demand miss + one preparation", hook.calls.Load())
	}
	hook.refuse.Store(true)
	h.testRefuseFUSEContentReads = true
	second := openBoundCacheFile(t, x)
	clear(buf)
	n, err = second.ReadAt(buf, off)
	after := metrics.Default().Snapshot()
	if n != len(buf) || err != nil || !bytes.Equal(buf, x.want[off:off+4096]) || second.fuseFD != nil ||
		hook.calls.Load() != 2 || after.DirectSSDCacheHit-before.DirectSSDCacheHit != 1 ||
		phaseForegroundOpenCount(after)-phaseForegroundOpenCount(before) != 1 {
		t.Fatalf("fresh read n=%d err=%v sourceOpen=%v mapping=%d SSDhit=%d FUSEopen=%d", n, err,
			second.fuseFD != nil, hook.calls.Load(), after.DirectSSDCacheHit-before.DirectSSDCacheHit,
			phaseForegroundOpenCount(after)-phaseForegroundOpenCount(before))
	}
	if errors.Is(err, cache.ErrVolumeBinding) {
		t.Fatal("unexpected volume refusal")
	}
}

func TestDemandMappingHeldPhysicalWorkSurvivesBindingReplacement(t *testing.T) {
	x := newBoundCacheFixture(t)
	h := x.jfs.handler
	h.sidecar.enabled = false
	if err := os.WriteFile(filepath.Join(x.fuseRoot, "media/clip.mov"), x.want, 0600); err != nil {
		t.Fatal(err)
	}
	const off int64 = 8192
	control := openBoundCacheFile(t, x)
	buf := make([]byte, 4096)
	if n, err := control.ReadAt(buf, off); n != len(buf) || err != nil {
		t.Fatalf("qualification n=%d err=%v", n, err)
	}
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("missing bound volume")
	}
	hook := &demandFirstMappingMiss{holdSecond: make(chan struct{}), secondEntered: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(hook.holdSecond)
		}
		<-h.beginDemandStop() // join the original work on every failure path
	}()
	rdb := redis.NewClient(x.rdb.Options())
	rdb.AddHook(hook)
	reader, err := cache.NewVolumeReader(binding, rdb)
	if err != nil {
		_ = rdb.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Stop(); _ = rdb.Close() })
	h.SetCacheReader(reader)
	cf := openBoundCacheFile(t, x)
	var trace cachedReadTrace
	n, err := cf.readAtTraced(buf, off, &trace)
	if n != len(buf) || err != nil || cf.fuseFD == nil || trace.Reader.Outcome != "mapping_deadline" {
		t.Fatalf("source demand n=%d err=%v trace=%+v", n, err, trace)
	}
	select {
	case <-hook.secondEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("physical preparation did not enter")
	}
	// A replacement binding cannot admit another job while the old client is
	// still in its Redis hook. Stop must retain the original physical worker.
	h.SetBoundCacheSession(x.session, x.rc, x.paths)
	if h.reserveDemand() {
		h.releaseDemand()
		t.Fatal("replacement binding admitted a second demand")
	}
	if h.stopHandlerWithDeadline(20 * time.Millisecond) {
		t.Fatal("StopHandler claimed success with a physical mapping still held")
	}
	close(hook.holdSecond)
	released = true
	if !h.StopHandler() {
		t.Fatal("StopHandler did not join original mapping after release")
	}
}

func TestDemandMappingCapturedAuthorityRejectsMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *boundCacheFixture)
	}{
		{name: "unchanged"},
		{name: "ancestor", mutate: func(t *testing.T, x *boundCacheFixture) { replaceBoundCacheEntry(t, x, "media") }},
		{name: "writer ABA", mutate: func(_ *testing.T, x *boundCacheFixture) {
			x.jfs.handler.incActiveWriter("media/clip.mov")
			x.jfs.handler.decActiveWriter("media/clip.mov")
		}},
		{name: "namespace ABA", mutate: func(t *testing.T, x *boundCacheFixture) {
			op := x.jfs.handler.namespace.begin([]string{"media/clip.mov"})
			if op == nil {
				t.Fatal("namespace operation not admitted")
			}
			op.blockReads()
			op.finish(nil)
		}},
		{name: "reader", mutate: func(_ *testing.T, x *boundCacheFixture) { x.jfs.handler.SetCacheReader(nil) }},
		{name: "remote epoch", mutate: func(_ *testing.T, x *boundCacheFixture) { x.session.SetOnline(false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			h := x.jfs.handler
			f := openBoundCacheFile(t, x)
			var buf [4096]byte
			if n, err := f.ReadAt(buf[:], 0); n != len(buf) || err != nil {
				t.Fatalf("ordinary qualification n=%d err=%v", n, err)
			}
			x.session.Reader().InvalidateSliceCache(3)
			j := h.captureDemandMapping(f)
			if j == nil {
				t.Fatal("unchanged qualified path failed demand capture")
			}
			defer j.release()
			j.record(0, len(buf))
			if tc.mutate != nil {
				tc.mutate(t, x)
			}
			if got := j.current(); got != (tc.mutate == nil) {
				t.Fatalf("captured demand current=%v after %s", got, tc.name)
			}
			j.prepare()
			witness := cache.ResidentAttr{Size: j.identity.Size, MtimeNS: j.identity.MtimeNS, Valid: true}
			_, readErr := j.reader.ReadResidentRange(j.identity.Inode, 0, buf[:], witness)
			if tc.mutate == nil {
				if readErr != nil || !bytes.Equal(buf[:], bytes.Repeat([]byte{'B'}, len(buf))) || !j.current() {
					t.Fatalf("unchanged job did not prepare verified mapping: err=%v", readErr)
				}
			} else if !errors.Is(readErr, cache.ErrCacheMiss) {
				t.Fatalf("mutated %s published mapping: err=%v", tc.name, readErr)
			}
		})
	}
}

func TestDemandMappingBusyLeavesOriginalFUSEResponse(t *testing.T) {
	x := newBoundCacheFixture(t)
	h := x.jfs.handler
	h.sidecar.enabled = false
	if err := os.WriteFile(filepath.Join(x.fuseRoot, "media/clip.mov"), x.want, 0600); err != nil {
		t.Fatal(err)
	}
	const off int64 = 8192
	buf := make([]byte, 4096)
	control := openBoundCacheFile(t, x)
	if n, err := control.ReadAt(buf, off); n != len(buf) || err != nil {
		t.Fatalf("qualification n=%d err=%v", n, err)
	}
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("missing bound volume")
	}
	hook := &demandFirstMappingMiss{}
	rdb := redis.NewClient(x.rdb.Options())
	rdb.AddHook(hook)
	reader, err := cache.NewVolumeReader(binding, rdb)
	if err != nil {
		_ = rdb.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Stop(); _ = rdb.Close() })
	h.SetCacheReader(reader)
	if !h.reserveDemand() {
		t.Fatal("test could not occupy handler's sole demand slot")
	}
	defer h.releaseDemand()
	f := openBoundCacheFile(t, x)
	var tr cachedReadTrace
	n, err := f.readAtTraced(buf, off, &tr)
	if n != len(buf) || err != nil || !bytes.Equal(buf, x.want[off:off+4096]) || f.fuseFD == nil ||
		tr.Reader.Outcome != "mapping_deadline" || tr.FinalRoute != "fuse_online" || hook.calls.Load() != 1 {
		t.Fatalf("busy demand changed original response n=%d err=%v open=%v trace=%+v maps=%d", n, err, f.fuseFD != nil, tr, hook.calls.Load())
	}
}
