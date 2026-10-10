//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

// phaseMappingHook touches only the bound c3_0 EVAL. It never delays path
// qualification, Redis setup, or the ordinary warm-mapping control.
type phaseMappingHook struct {
	armed       atomic.Bool
	calls       atomic.Int64
	finished    atomic.Int64
	entered     chan struct{}
	completed   chan error
	release     chan struct{}
	releaseOnce sync.Once
}

func (h *phaseMappingHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *phaseMappingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *phaseMappingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "eval" {
			return next(ctx, cmd)
		}
		mapping := false
		for _, arg := range cmd.Args() {
			if fmt.Sprint(arg) == "c3_0" {
				mapping = true
				break
			}
		}
		if !mapping {
			return next(ctx, cmd)
		}
		h.calls.Add(1)
		if !h.armed.Load() {
			return next(ctx, cmd)
		}
		select {
		case h.entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
		case <-h.release:
		}
		err := fmt.Errorf("held c3_0 mapping: %w", ctx.Err())
		if ctx.Err() == nil {
			err = fmt.Errorf("test-owned mapping release: %w", context.Canceled)
		}
		h.finished.Add(1)
		select {
		case h.completed <- err:
		default:
		}
		return err
	}
}

func phaseReader(t *testing.T, x *boundCacheFixture) (*cache.Reader, *phaseMappingHook) {
	t.Helper()
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("fixture session lacks selected-volume binding")
	}
	hook := &phaseMappingHook{entered: make(chan struct{}, 2), completed: make(chan error, 2), release: make(chan struct{})}
	rdb := redis.NewClient(x.rdb.Options())
	rdb.AddHook(hook)
	reader, err := cache.NewVolumeReader(binding, rdb)
	if err != nil {
		_ = rdb.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Stop(); _ = rdb.Close() })
	x.jfs.handler.SetCacheReader(reader)
	return reader, hook
}

func phaseForegroundOpenCount(s metrics.Snapshot) uint64 {
	for _, cell := range s.FUSEAttrib.Cells {
		if cell.Source == "foreground" && cell.Op == "open" {
			return cell.Calls
		}
	}
	return 0
}

func assertPhasePathQualified(t *testing.T, x *boundCacheFixture, reader *cache.Reader) metadata.CachedFileIdentity {
	t.Helper()
	binding, bound := reader.VolumeBinding()
	if !bound {
		t.Fatal("reader lost volume binding")
	}
	id, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok || !x.paths.CachePathEligible(metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}, id) {
		t.Fatal("selected path has no retained local ancestry proof")
	}
	return id
}

func assertPhaseRead(t *testing.T, cf *cachedFile, marker byte, backing bool) {
	t.Helper()
	got := make([]byte, 4096)
	n, err := cf.ReadAt(got, 0)
	if n != len(got) || err != nil || !bytes.Equal(got, bytes.Repeat([]byte{marker}, len(got))) || (cf.fuseFD != nil) != backing {
		t.Fatalf("read = n=%d err=%v first=%q backingOpen=%v; want %q backing=%v", n, err, got[:min(n, 8)], cf.fuseFD != nil, marker, backing)
	}
}

func assertPhaseTracedRead(t *testing.T, cf *cachedFile, marker byte, backing bool) cachedReadTrace {
	t.Helper()
	trace := cachedReadTrace{QualificationNS: 1, DescriptorAcquireNS: 1, FinalRoute: "stale", Reader: cache.ReadTrace{Outcome: "stale"}}
	got := make([]byte, 4096)
	n, err := cf.readAtTraced(got, 0, &trace)
	if n != len(got) || err != nil || !bytes.Equal(got, bytes.Repeat([]byte{marker}, len(got))) || (cf.fuseFD != nil) != backing {
		t.Fatalf("traced read = n=%d err=%v backingOpen=%v trace=%+v", n, err, cf.fuseFD != nil, trace)
	}
	if trace.ReturnedBytes != n || trace.FinalResultClass != "ok" || trace.TotalNS <= 0 {
		t.Fatalf("traced reply does not describe returned bytes: %+v", trace)
	}
	return trace
}

func TestBoundCacheReadPhaseHeldMappingFallsBackWithResidentBytes(t *testing.T) {
	x := newBoundCacheFixture(t)
	reader, hook := phaseReader(t, x)
	defer hook.releaseOnce.Do(func() { close(hook.release) })
	control := openBoundCacheFile(t, x)
	beforeWarm := metrics.Default().Snapshot()
	assertPhaseRead(t, control, 'B', false)
	afterWarm := metrics.Default().Snapshot()
	if afterWarm.DirectSSDCacheHit <= beforeWarm.DirectSSDCacheHit || hook.calls.Load() != 1 ||
		phaseForegroundOpenCount(afterWarm) != phaseForegroundOpenCount(beforeWarm) {
		t.Fatal("warm mapping did not produce verified SSD control")
	}
	id := assertPhasePathQualified(t, x, reader)
	b := x.jfs.handler.cacheBinding.Load()
	var proofs, cacheEntries atomic.Int64
	actualProof := b.readProof
	b.readProof = func(ctx context.Context, uuid, name string, ids []uint64) (metadata.RedisPathProof, error) {
		proofs.Add(1)
		return actualProof(ctx, uuid, name, ids)
	}
	b.beforeCacheRead = func() { cacheEntries.Add(1) }
	control2 := openBoundCacheFile(t, x)
	hook.armed.Store(true)
	beforeControl := metrics.Default().Snapshot()
	warmTrace := assertPhaseTracedRead(t, control2, 'B', false)
	afterControl := metrics.Default().Snapshot()
	if hook.calls.Load() != 1 || proofs.Load() != 0 || cacheEntries.Load() != 1 ||
		phaseForegroundOpenCount(afterControl) != phaseForegroundOpenCount(beforeControl) {
		t.Fatalf("warm control used remote phase: mapping=%d proof=%d cache=%d", hook.calls.Load(), proofs.Load(), cacheEntries.Load())
	}
	if !warmTrace.CachePathInitiallyEligible || warmTrace.Reader.Outcome != "verified_hit" ||
		warmTrace.Reader.MappingRemoteSharedCount != 0 || warmTrace.Reader.MappingLookupCount != 1 ||
		warmTrace.FinalRoute != "verified_ssd" || warmTrace.DescriptorAcquireNS != 0 || warmTrace.FUSEReadNS != 0 {
		t.Fatalf("warm same-call attribution = %+v", warmTrace)
	}
	reader.InvalidateSliceCache(3)
	assertPhasePathQualified(t, x, reader)
	blockDir, err := cache.VolumeCacheDir("selected-volume")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(blockDir, "0/0/77_0_65536")); err != nil {
		t.Fatalf("resident CRC block missing before mapping hold: %v", err)
	}
	var missJoined, readJoined chan struct{}
	var cancelMiss context.CancelFunc
	defer func() {
		hook.releaseOnce.Do(func() { close(hook.release) })
		if cancelMiss != nil {
			cancelMiss()
		}
		if missJoined != nil {
			<-missJoined
		}
		if readJoined != nil {
			<-readJoined
		}
	}()
	// Direct lookup establishes the error class; unlike a backing fallback it
	// cannot conceal the mapping timeout behind coherent FUSE bytes.
	miss := make(chan error, 1)
	missJoined = make(chan struct{})
	missCtx, cancel := context.WithCancel(context.Background())
	cancelMiss = cancel
	go func() {
		defer close(missJoined)
		_, err := x.jfs.handler.readCacheBlock(missCtx, reader, id, id.Inode, 0, make([]byte, 4096), 50*time.Millisecond)
		miss <- err
	}()
	select {
	case <-hook.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("c3_0 EVAL did not enter")
	}
	select {
	case err := <-miss:
		if !errors.Is(err, cache.ErrCacheMiss) {
			t.Fatalf("held mapping class = %v, want cache miss", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bounded lookup did not return")
	}
	select {
	case err := <-hook.completed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("mapping hook ended with %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("mapping hook did not join")
	}
	cf := openBoundCacheFile(t, x)
	before := metrics.Default().Snapshot()
	type result struct {
		n               int
		err             error
		data            []byte
		mappingFinished int64
		trace           cachedReadTrace
	}
	done := make(chan result, 1)
	readJoined = make(chan struct{})
	go func() {
		defer close(readJoined)
		buf := make([]byte, 4096)
		var trace cachedReadTrace
		n, err := cf.readAtTraced(buf, 0, &trace)
		done <- result{n, err, buf, hook.finished.Load(), trace}
	}()
	select {
	case <-hook.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("file read did not request cold mapping")
	}
	select {
	case r := <-done:
		if r.err != nil || r.n != 4096 || !bytes.Equal(r.data, bytes.Repeat([]byte{'F'}, 4096)) ||
			cf.fuseFD == nil || r.mappingFinished != 2 {
			t.Fatalf("mapping miss fallback = n=%d err=%v backingOpen=%v finishedMappings=%d", r.n, r.err, cf.fuseFD != nil, r.mappingFinished)
		}
		if r.trace.Reader.Outcome != "mapping_deadline" || r.trace.Reader.MappingRemoteSharedCount != 1 ||
			r.trace.Reader.MappingLookupCount != 1 || r.trace.FinalRoute != "fuse_online" ||
			r.trace.DescriptorAcquireNS <= 0 || r.trace.FUSEReadNS <= 0 ||
			r.trace.ReturnedBytes != 4096 || r.trace.FinalResultClass != "ok" {
			t.Fatalf("held mapping same-call attribution = %+v", r.trace)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("file read did not finish after mapping deadline")
	}
	select {
	case err := <-hook.completed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("mapping hook ended with %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second mapping hook did not join")
	}
	// The successful source demand may start one owned, metadata-only map
	// preparation. Join it before checking aggregate hook counts; the trace
	// above still proves this READ itself made exactly one mapping attempt.
	<-x.jfs.handler.beginDemandStop()
	after := metrics.Default().Snapshot()
	if after.DirectSSDCacheMiss-before.DirectSSDCacheMiss != 1 ||
		phaseForegroundOpenCount(after)-phaseForegroundOpenCount(before) != 1 || proofs.Load() != 0 ||
		cacheEntries.Load() != 3 || hook.calls.Load() != 4 {
		t.Fatalf("phase counts miss=%d proof=%d cache=%d mapping=%d", after.DirectSSDCacheMiss-before.DirectSSDCacheMiss, proofs.Load(), cacheEntries.Load(), hook.calls.Load())
	}
}

func TestBoundCacheReadPhaseMissingBlockFallsBackWithoutMapping(t *testing.T) {
	x := newBoundCacheFixture(t)
	reader, hook := phaseReader(t, x)
	defer hook.releaseOnce.Do(func() { close(hook.release) })
	control := openBoundCacheFile(t, x)
	beforeWarm := metrics.Default().Snapshot()
	assertPhaseRead(t, control, 'B', false)
	if phaseForegroundOpenCount(metrics.Default().Snapshot()) != phaseForegroundOpenCount(beforeWarm) {
		t.Fatal("missing-block control opened FUSE despite resident B bytes")
	}
	assertPhasePathQualified(t, x, reader)
	if hook.calls.Load() != 1 {
		t.Fatalf("warm mapping calls=%d", hook.calls.Load())
	}
	b := x.jfs.handler.cacheBinding.Load()
	var proofs, cacheEntries atomic.Int64
	actualProof := b.readProof
	b.readProof = func(ctx context.Context, uuid, name string, ids []uint64) (metadata.RedisPathProof, error) {
		proofs.Add(1)
		return actualProof(ctx, uuid, name, ids)
	}
	b.beforeCacheRead = func() { cacheEntries.Add(1) }
	blockDir, err := cache.VolumeCacheDir("selected-volume")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(blockDir, "0/0/77_0_65536")); err != nil {
		t.Fatal(err)
	}
	hook.armed.Store(true)
	cf := openBoundCacheFile(t, x)
	before := metrics.Default().Snapshot()
	missingTrace := assertPhaseTracedRead(t, cf, 'F', true)
	after := metrics.Default().Snapshot()
	if after.DirectSSDCacheMiss-before.DirectSSDCacheMiss != 1 ||
		phaseForegroundOpenCount(after)-phaseForegroundOpenCount(before) != 1 || hook.calls.Load() != 1 ||
		proofs.Load() != 0 || cacheEntries.Load() != 1 {
		t.Fatalf("missing-block phase miss=%d mapping=%d proof=%d cache=%d", after.DirectSSDCacheMiss-before.DirectSSDCacheMiss, hook.calls.Load(), proofs.Load(), cacheEntries.Load())
	}
	if missingTrace.Reader.Outcome != "block_unavailable" || missingTrace.Reader.MappingRemoteSharedCount != 0 ||
		missingTrace.Reader.MappingLookupCount != 1 || missingTrace.FinalRoute != "fuse_online" ||
		missingTrace.DescriptorAcquireNS <= 0 || missingTrace.FUSEReadNS <= 0 {
		t.Fatalf("missing block same-call attribution = %+v", missingTrace)
	}
}

func TestBoundCacheReadPhaseCorruptBlockIsUnverified(t *testing.T) {
	x := newBoundCacheFixture(t)
	reader, hook := phaseReader(t, x)
	defer hook.releaseOnce.Do(func() { close(hook.release) })
	assertPhaseRead(t, openBoundCacheFile(t, x), 'B', false)
	assertPhasePathQualified(t, x, reader)
	blockDir, err := cache.VolumeCacheDir("selected-volume")
	if err != nil {
		t.Fatal(err)
	}
	block := filepath.Join(blockDir, "0/0/77_0_65536")
	fd, err := os.OpenFile(block, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := fd.WriteAt([]byte{'X'}, 0)
	closeErr := fd.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("corrupt owned block: write=%v close=%v", writeErr, closeErr)
	}
	hook.armed.Store(true)
	stale := cache.ReadTrace{Outcome: "block_unavailable", MappingLookupCount: 99, MappingRemoteSharedCount: 99}
	if n, err := reader.ReadBlockWithMappingTimeoutTrace(context.Background(), 3, 0, make([]byte, 4096), 500*time.Millisecond, &stale); n != 0 || !errors.Is(err, cache.ErrCacheMiss) ||
		stale.Outcome != "block_unverified_or_read_error" || stale.MappingLookupCount != 1 || stale.MappingRemoteSharedCount != 0 {
		t.Fatalf("reused reader trace retained stale outcome: n=%d err=%v trace=%+v", n, err, stale)
	}
	before := metrics.Default().Snapshot()
	trace := assertPhaseTracedRead(t, openBoundCacheFile(t, x), 'F', true)
	after := metrics.Default().Snapshot()
	if trace.Reader.Outcome != "block_unverified_or_read_error" || trace.Reader.MappingLookupCount != 1 ||
		trace.Reader.MappingRemoteSharedCount != 0 || trace.FinalRoute != "fuse_online" ||
		hook.calls.Load() != 1 || after.DirectSSDCacheMiss-before.DirectSSDCacheMiss != 1 ||
		phaseForegroundOpenCount(after)-phaseForegroundOpenCount(before) != 1 {
		t.Fatalf("corrupt block attribution = %+v mappingCalls=%d", trace, hook.calls.Load())
	}
}

func TestBoundCacheReadPhaseQualificationDeadlineStopsBeforeMapping(t *testing.T) {
	x := newBoundCacheFixture(t)
	cf := openBoundCacheFile(t, x)
	b := x.jfs.handler.cacheBinding.Load()
	actual := b.readProof
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var joined chan struct{}
	defer func() {
		releaseOnce.Do(func() { close(release) })
		if joined != nil {
			<-joined
		}
	}()
	b.readProof = func(ctx context.Context, uuid, name string, ids []uint64) (metadata.RedisPathProof, error) {
		proof, err := actual(ctx, uuid, name, ids)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return proof, err
	}
	type result struct {
		n     int
		err   error
		trace cachedReadTrace
	}
	done := make(chan result, 1)
	joined = make(chan struct{})
	go func() {
		defer close(joined)
		var trace cachedReadTrace
		n, err := cf.readAtTraced(make([]byte, 4096), 0, &trace)
		done <- result{n, err, trace}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("qualification did not enter")
	}
	select {
	case r := <-done:
		if r.n != 0 || !errors.Is(r.err, syscall.EAGAIN) || r.trace.CachePathInitiallyEligible ||
			r.trace.QualificationNS <= 0 || r.trace.CacheReadNS != 0 || r.trace.Reader.MappingLookupCount != 0 ||
			r.trace.Reader.Outcome != "not_attempted" || r.trace.DescriptorAcquireNS != 0 || r.trace.FUSEReadNS != 0 ||
			r.trace.FinalRoute != "not_attempted" || r.trace.FinalResultClass != "retryable" {
			t.Fatalf("qualification refusal attribution = n=%d err=%v trace=%+v", r.n, r.err, r.trace)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("qualification deadline did not return")
	}
}
