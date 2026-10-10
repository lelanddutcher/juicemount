//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

type readaheadTargetMappingRefusal struct {
	key   string
	armed atomic.Bool
	calls atomic.Int64
}

func (*readaheadTargetMappingRefusal) DialHook(next redis.DialHook) redis.DialHook { return next }
func (*readaheadTargetMappingRefusal) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *readaheadTargetMappingRefusal) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" {
			for _, arg := range cmd.Args() {
				if fmt.Sprint(arg) == h.key && h.armed.Load() {
					h.calls.Add(1)
					return errors.New("test-owned terminal-chunk mapping refusal")
				}
			}
		}
		return next(ctx, cmd)
	}
}

// A sparse ordinary source plus two genuine CRC-encoded 4 MiB blocks keeps
// physical fixture size small. The disk blocks are explicitly preseeded: this
// proves the application's metadata handoff boundary, not that a local file
// read itself populated JuiceFS's persistent block cache.
func TestOrdinaryReadaheadPreparesSuccessfulTargetMapping(t *testing.T) {
	t.Setenv("JM_SERVER_READAHEAD", "")
	x := newBoundCacheFixture(t)
	h := x.jfs.handler
	h.sidecar.enabled = false
	rm := h.readahead
	rm.profile = nil // Medium's deterministic three-hit, 32 MiB policy.
	const sourceSize = cache.ChunkSize + readaheadBlockSize
	sourcePath := filepath.Join(x.fuseRoot, "media/clip.mov")
	source, err := os.OpenFile(sourcePath, os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Truncate(sourceSize); err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1700000000, 123)
	if err := os.Chtimes(sourcePath, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(sourcePath)
	if err != nil || !stat.Mode().IsRegular() || stat.Size() != sourceSize {
		t.Fatalf("sparse source stat=%v err=%v", stat, err)
	}
	inode := stat.Sys().(*syscall.Stat_t).Ino
	e := metadata.MakeEntry("media/clip.mov", false, sourceSize, mtime, inode)
	e.MtimeNsecKnown = true
	if err := x.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	var edge [9]byte
	edge[0] = meta.TypeFile
	binary.BigEndian.PutUint64(edge[1:], inode)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	pipe := x.rdb.Pipeline()
	pipe.HSet(ctx, "d2", "clip.mov", edge[:])
	pipe.Set(ctx, "i"+strconv.FormatUint(inode, 10), (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: uint64(sourceSize), Mtime: 1700000000, Mtimensec: 123}).Marshal(), 0)
	pipe.Del(ctx, "c3_0")
	for chunk := 0; chunk < 2; chunk++ {
		key := "c" + strconv.FormatUint(inode, 10) + "_" + strconv.Itoa(chunk)
		for part := 0; part < 16; part++ {
			if chunk == 1 && part > 0 {
				break
			}
			var record [24]byte
			binary.BigEndian.PutUint32(record[0:4], uint32(part*readaheadBlockSize))
			binary.BigEndian.PutUint64(record[4:12], uint64(88+chunk))
			binary.BigEndian.PutUint32(record[12:16], readaheadBlockSize)
			binary.BigEndian.PutUint32(record[20:24], readaheadBlockSize)
			pipe.RPush(ctx, key, string(record[:]))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	cacheDir, err := cache.VolumeCacheDir("selected-volume")
	if err != nil {
		t.Fatal(err)
	}
	zeroBlock := make([]byte, readaheadBlockSize)
	encoded := append([]byte(nil), zeroBlock...)
	table := crc32.MakeTable(crc32.Castagnoli)
	for off := 0; off < len(zeroBlock); off += 32 << 10 {
		var sum [4]byte
		binary.BigEndian.PutUint32(sum[:], crc32.Checksum(zeroBlock[off:off+(32<<10)], table))
		encoded = append(encoded, sum[:]...)
	}
	for _, sliceID := range []int{88, 89} {
		block := filepath.Join(cacheDir, "0/0/"+strconv.Itoa(sliceID)+"_0_4194304")
		if err := os.MkdirAll(filepath.Dir(block), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(block, encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
	id, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok || id.Inode != inode || id.Size != sourceSize {
		t.Fatal("large file missing selected mirror identity")
	}
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("selected reader has no volume binding")
	}
	hook := &readaheadTargetMappingRefusal{key: "c" + strconv.FormatUint(inode, 10) + "_1"}
	serviceClient := redis.NewClient(x.rdb.Options())
	serviceClient.AddHook(hook)
	reader, err := cache.NewVolumeReader(binding, serviceClient)
	if err != nil {
		_ = serviceClient.Close()
		t.Fatal(err)
	}
	h.SetCacheReader(reader)
	t.Cleanup(func() { h.StopHandler(); reader.Stop(); _ = serviceClient.Close() })
	cf := openBoundCacheFile(t, x)
	first := make([]byte, 4096)
	if n, err := cf.ReadAt(first, 0); n != len(first) || err != nil || !bytes.Equal(first, make([]byte, len(first))) || cf.fuseFD != nil {
		t.Fatalf("ordinary first-chunk qualification n=%d err=%v FUSE=%v", n, err, cf.fuseFD != nil)
	}
	selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
	if !x.paths.CachePathEligible(selected, id) {
		t.Fatal("ordinary read did not qualify path")
	}
	witness := cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}
	if _, err := reader.ReadResidentRange(inode, cache.ChunkSize, make([]byte, 4096), witness); !errors.Is(err, cache.ErrCacheMiss) {
		t.Fatalf("target mapping unexpectedly resident before producer: %v", err)
	}
	// The wrapper calls the actual descriptor ReadAt. A successful target
	// completion, not a synthetic callback, is the producer-side witness.
	targetRead := make(chan struct{}, 1)
	workerExited := make(chan struct{}, 1)
	rm.prefetchWorkerExit = func() { workerExited <- struct{}{} }
	var targetSuccess atomic.Int64
	rm.prefetchReadAt = func(fd *os.File, p []byte, off int64) (int, error) {
		n, err := fd.ReadAt(p, off)
		if off == cache.ChunkSize && n == readaheadBlockSize && err == nil && fd.Name() == sourcePath && bytes.Equal(p[:n], zeroBlock) {
			targetSuccess.Add(1)
			select {
			case targetRead <- struct{}{}:
			default:
			}
		}
		return n, err
	}
	cf.readahead = rm
	for _, off := range []int64{48 << 20, 52 << 20, 56 << 20, 60 << 20} {
		got := make([]byte, readaheadBlockSize)
		if n, err := cf.ReadAt(got, off); n != len(got) || err != nil || !bytes.Equal(got, zeroBlock) || cf.fuseFD != nil {
			t.Fatalf("ordinary chunk-zero demand off=%d n=%d err=%v FUSE=%v", off, n, err, cf.fuseFD != nil)
		}
	}
	select {
	case <-targetRead:
	case <-time.After(5 * time.Second):
		t.Fatal("OnRead worker did not complete real target fd.ReadAt")
	}
	// Observe natural completion first: Stop must not cancel the handoff.
	select {
	case <-workerExited:
	case <-time.After(5 * time.Second):
		t.Fatal("prefetch worker did not complete naturally")
	}
	rm.Stop() // Join every actual worker before mapping/baseline assertions.
	cf.readahead = nil
	if triggered, prefetched := rm.Stats(); triggered == 0 || prefetched == 0 || targetSuccess.Load() == 0 {
		t.Fatalf("real producer work missing: triggered=%d prefetched=%d target=%d", triggered, prefetched, targetSuccess.Load())
	}
	residentTarget := make([]byte, 4096)
	if _, err := reader.ReadResidentRange(inode, cache.ChunkSize, residentTarget, witness); err != nil || !bytes.Equal(residentTarget, make([]byte, len(residentTarget))) {
		t.Fatalf("successful readahead did not prepare the service target mapping: err=%v", err)
	}
	// A separate Reader may fetch the target map and CRC-check the preseeded
	// block; it cannot publish that map into the foreground service Reader.
	oracleClient := redis.NewClient(x.rdb.Options())
	oracle, err := cache.NewVolumeReader(binding, oracleClient)
	if err != nil {
		_ = oracleClient.Close()
		t.Fatal(err)
	}
	verify := make([]byte, 4096)
	if n, err := oracle.ReadBlock(context.Background(), inode, cache.ChunkSize, verify); n != len(verify) || err != nil || !bytes.Equal(verify, make([]byte, len(verify))) {
		oracle.Stop()
		_ = oracleClient.Close()
		t.Fatalf("independent target CRC oracle n=%d err=%v", n, err)
	}
	oracle.Stop()
	_ = oracleClient.Close()
	if !x.paths.CachePathEligible(selected, id) {
		t.Fatal("qualified local path changed while worker read ahead")
	}
	b := h.cacheBinding.Load()
	originalProof := b.readProof
	var unexpectedProofs atomic.Int64
	b.readProof = func(ctx context.Context, uuid, path string, ids []uint64) (metadata.RedisPathProof, error) {
		unexpectedProofs.Add(1)
		return originalProof(ctx, uuid, path, ids)
	}
	hook.armed.Store(true)
	beforeControl := metrics.Default().Snapshot()
	fuseBefore := fuseCellCounts(t)
	control := openBoundCacheFile(t, x)
	controlBuf := make([]byte, 4096)
	if n, err := control.ReadAt(controlBuf, 0); n != len(controlBuf) || err != nil || !bytes.Equal(controlBuf, make([]byte, len(controlBuf))) || control.fuseFD != nil {
		t.Fatalf("mapped control n=%d err=%v FUSE=%v", n, err, control.fuseFD != nil)
	}
	if hook.calls.Load() != 0 || unexpectedProofs.Load() != 0 || metrics.Default().Snapshot().DirectSSDCacheHit-beforeControl.DirectSSDCacheHit != 1 {
		t.Fatalf("mapped control consulted metadata: mapping=%d proof=%d SSDdelta=%d", hook.calls.Load(), unexpectedProofs.Load(), metrics.Default().Snapshot().DirectSSDCacheHit-beforeControl.DirectSSDCacheHit)
	}
	if open, _, _, _ := cellDelta(fuseBefore, fuseCellCounts(t), "foreground/open"); open != 0 {
		t.Fatalf("mapped control opened FUSE %d times", open)
	}
	beforeTarget := metrics.Default().Snapshot()
	targetFUSE := fuseCellCounts(t)
	target := openBoundCacheFile(t, x)
	targetBuf := make([]byte, 4096)
	n, readErr := target.ReadAt(targetBuf, cache.ChunkSize)
	if n != len(targetBuf) || readErr != nil || !bytes.Equal(targetBuf, make([]byte, len(targetBuf))) || target.fuseFD != nil {
		t.Fatalf("first target demand n=%d err=%v FUSE=%v", n, readErr, target.fuseFD != nil)
	}
	if hook.calls.Load() != 0 || unexpectedProofs.Load() != 0 || metrics.Default().Snapshot().DirectSSDCacheHit-beforeTarget.DirectSSDCacheHit != 1 {
		t.Fatalf("first target demand consulted remote metadata: mapping=%d proof=%d SSDdelta=%d", hook.calls.Load(), unexpectedProofs.Load(), metrics.Default().Snapshot().DirectSSDCacheHit-beforeTarget.DirectSSDCacheHit)
	}
	if open, _, _, _ := cellDelta(targetFUSE, fuseCellCounts(t), "foreground/open"); open != 0 {
		t.Fatalf("first target demand opened FUSE %d times", open)
	}
}
