//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type ownedPinDenyRedis struct{ calls atomic.Int64 }

func (*ownedPinDenyRedis) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *ownedPinDenyRedis) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(context.Context, redis.Cmder) error {
		h.calls.Add(1)
		return errors.New("test-owned metadata Redis refusal")
	}
}
func (h *ownedPinDenyRedis) ProcessPipelineHook(redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(context.Context, []redis.Cmder) error {
		h.calls.Add(1)
		return errors.New("test-owned metadata Redis pipeline refusal")
	}
}

// A sparse regular source and one reusable CRC-backed 4 MiB zero block cover
// two complete 64 MiB chunks plus a terminal tail without a 128 MiB fixture.
func ownedPinThreeChunkFixture(t *testing.T) (*boundCacheFixture, *pin.Store, *pin.Prefetcher, *OwnedPinProvider, uint64, int64, string, string) {
	t.Helper()
	x := newBoundCacheFixture(t)
	h := x.jfs.handler
	h.sidecar.enabled = false
	const size = 2*cache.ChunkSize + 4096
	path := filepath.Join(x.fuseRoot, "media/clip.mov")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1700000000, 123)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	inode := info.Sys().(*syscall.Stat_t).Ino
	e := metadata.MakeEntry("media/clip.mov", false, size, mtime, inode)
	e.MtimeNsecKnown = true
	if err := x.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	attr := (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: uint64(size), Mtime: 1700000000, Mtimensec: 123}).Marshal()
	edge := make([]byte, 9)
	edge[0] = byte(meta.TypeFile)
	binary.BigEndian.PutUint64(edge[1:], inode)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	pipe := x.rdb.Pipeline()
	pipe.HSet(ctx, "d2", "clip.mov", edge)
	pipe.Set(ctx, "i"+strconv.FormatUint(inode, 10), attr, 0)
	for chunk := 0; chunk < 3; chunk++ {
		key := "c" + strconv.FormatUint(inode, 10) + "_" + strconv.Itoa(chunk)
		pipe.Del(ctx, key)
		pieces := 16
		if chunk == 2 {
			pieces = 1
		}
		for part := 0; part < pieces; part++ {
			var record [24]byte
			binary.BigEndian.PutUint32(record[0:4], uint32(part*(4<<20)))
			sliceID := uint64(88)
			if chunk == 1 && part == 8 {
				sliceID = 89
			}
			binary.BigEndian.PutUint64(record[4:12], sliceID)
			binary.BigEndian.PutUint32(record[12:16], 4<<20)
			length := uint32(4 << 20)
			if chunk == 2 {
				length = 4096
			}
			binary.BigEndian.PutUint32(record[20:24], length)
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
	block := filepath.Join(cacheDir, "0/0/88_0_4194304")
	middleBlock := filepath.Join(cacheDir, "0/0/89_0_4194304")
	if err := os.MkdirAll(filepath.Dir(block), 0700); err != nil {
		t.Fatal(err)
	}
	zeroBlock := make([]byte, 4<<20)
	encoded := append([]byte(nil), zeroBlock...)
	table := crc32.MakeTable(crc32.Castagnoli)
	for at := 0; at < len(zeroBlock); at += 32 << 10 {
		var sum [4]byte
		binary.BigEndian.PutUint32(sum[:], crc32.Checksum(zeroBlock[at:at+(32<<10)], table))
		encoded = append(encoded, sum[:]...)
	}
	if err := os.WriteFile(block, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(middleBlock, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	pinDBPath := filepath.Join(t.TempDir(), "pins.db")
	ps, err := pin.Open(pinDBPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ps.Close() })
	h.SetPinStore(ps, "/volume")
	provider := NewOwnedPinProvider(h, ps)
	if err := ps.SetReadyValidator(provider.Current); err != nil {
		t.Fatal(err)
	}
	if err := ps.SetReadyRevoker(provider.Revoke); err != nil {
		t.Fatal(err)
	}
	pf := pin.NewPrefetcher(ps, x.fuseRoot, "/volume", 1, provider)
	t.Cleanup(func() {
		if !pf.StopWithin(5 * time.Second) {
			t.Error("prefetch worker did not join")
		}
	})
	canonical := "/volume/media/clip.mov"
	if err := ps.Pin(canonical, size, "/volume"); err != nil {
		t.Fatal(err)
	}
	// The fixture's FUSE source is a regular test file, not a FUSEManager.
	// Supply the selected cache volume policy that a real admitted mount would
	// publish before its pin worker starts.
	cacheRoot := filepath.Dir(filepath.Dir(filepath.Dir(cacheDir)))
	var cacheStat syscall.Stat_t
	var volumeStat syscall.Statfs_t
	if err := syscall.Stat(cacheRoot, &cacheStat); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Statfs(cacheRoot, &volumeStat); err != nil {
		t.Fatal(err)
	}
	free := int64(volumeStat.Bsize) * int64(volumeStat.Bavail)
	if free < 4*size {
		t.Fatal("test cache volume lacks a full pinned working set")
	}
	pin.SetCachePolicy(free/2, free/4, cacheRoot, uint64(cacheStat.Dev), true)
	t.Cleanup(func() { pin.SetCachePolicy(0, 0, "", 0, false) })
	if v := pin.ComputeCapacity(ps, cacheRoot); !v.PolicyKnown || v.CacheCapacityBytes < size {
		t.Fatalf("test cache policy did not admit source: %+v", v)
	}
	return x, ps, pf, provider, inode, size, middleBlock, pinDBPath
}

// Every attempted Redis connection in the reopened Session reaches this
// test-owned endpoint. A control PING proves the endpoint counts real traffic;
// the post-control baseline must stay unchanged through restore and reads.
func ownedPinRemoteRefusal(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int64
	var workers sync.WaitGroup
	acceptDone := make(chan struct{})
	observed := make(chan struct{}, 1)
	settled := make(chan struct{}, 1)
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			requests.Add(1)
			workers.Add(1)
			go func(c net.Conn) {
				defer workers.Done()
				defer func() {
					_ = c.Close()
					select {
					case settled <- struct{}{}:
					default:
					}
				}()
				_ = c.SetDeadline(time.Now().Add(time.Second))
				var request [4096]byte
				if n, _ := c.Read(request[:]); n > 0 {
					select {
					case observed <- struct{}{}:
					default:
					}
					_, _ = c.Write([]byte("-ERR test-owned remote refusal\r\n"))
				}
			}(conn)
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-acceptDone; workers.Wait() })
	control := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := control.Ping(ctx).Err(); err == nil {
		t.Fatal("refusal endpoint accepted control PING")
	}
	_ = control.Close()
	select {
	case <-observed:
	case <-ctx.Done():
		t.Fatal("refusal endpoint did not observe control request")
	}
	select {
	case <-settled:
	case <-ctx.Done():
		t.Fatal("refusal control connection did not settle")
	}
	return listener.Addr().String(), &requests
}

func ownedPinWireFullDigest(t *testing.T, client *latencyClient, h *JuiceMountHandler, size int64) [sha256.Size]byte {
	t.Helper()
	hash := sha256.New()
	for off := int64(0); off < size; off += 512 << 10 {
		count := uint32(512 << 10)
		pending, err := client.send(100003, 6, latencyReadArgs{Handle: h.ToHandle(nil, []string{"media", "clip.mov"}), Offset: uint64(off), Count: count})
		if err != nil {
			t.Fatal(err)
		}
		reply, err := pending.await(time.Now().Add(10 * time.Second))
		if err != nil {
			t.Fatal(err)
		}
		reader, err := latencyOK(reply)
		if err != nil {
			t.Fatal(err)
		}
		if err := latencyPostAttr(reader); err != nil {
			t.Fatal(err)
		}
		gotCount, err := xdr.ReadUint32(reader)
		if err != nil {
			t.Fatal(err)
		}
		eof, err := xdr.ReadUint32(reader)
		if err != nil {
			t.Fatal(err)
		}
		body, err := latencyOpaque(reader)
		wantCount := int64(count)
		if remain := size - off; remain < wantCount {
			wantCount = remain
		}
		wantEOF := uint32(0)
		if off+wantCount == size {
			wantEOF = 1
		}
		if err != nil || int64(gotCount) != wantCount || int64(len(body)) != wantCount || eof != wantEOF || reader.Len() != 0 {
			t.Fatalf("fullbody off=%d count=%d body=%d eof=%d err=%v", off, gotCount, len(body), eof, err)
		}
		_, _ = hash.Write(body)
	}
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(nil))
	return sum
}

func TestOwnedPinProducerThreeChunksTailAndReopen(t *testing.T) {
	x, ps, pf, _, _, size, block, pinDBPath := ownedPinThreeChunkFixture(t)
	const canonical = "/volume/media/clip.mov"
	e, ok := ps.Get(canonical)
	if !ok {
		t.Fatal("missing queued pin")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := pf.EnqueueWait(ctx, *e); err != nil {
		t.Fatal(err)
	}
	if !ps.IsPinnedReady(canonical) {
		t.Fatal("producer completion did not admit witness")
	}
	ready, _ := ps.Get(canonical)
	if len(ready.Authority) == 0 || ready.Status != pin.StatusReady {
		t.Fatalf("no durable witness: %+v", ready)
	}
	source, err := os.Open(filepath.Join(x.fuseRoot, "media/clip.mov"))
	if err != nil {
		t.Fatal(err)
	}
	sourceHash := sha256.New()
	if n, err := io.Copy(sourceHash, source); err != nil || n != size {
		_ = source.Close()
		t.Fatalf("source digest bytes=%d err=%v", n, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	var wantDigest [sha256.Size]byte
	copy(wantDigest[:], sourceHash.Sum(nil))
	// The ordinary fresh handle must use local CRC bytes before owner retirement.
	x.session.SetOnline(false)
	pin.SetOffline(true)
	for _, offset := range []int64{0, cache.ChunkSize - 2048, cache.ChunkSize + 4096, 2*cache.ChunkSize + 2048} {
		f := openBoundCacheFile(t, x)
		if f.pinGeneration != ready.Generation {
			t.Fatal("fresh handle did not retain owned generation")
		}
		got := make([]byte, 1024)
		if n, err := f.ReadAt(got, offset); n != len(got) || err != nil || !bytes.Equal(got, make([]byte, len(got))) || f.fuseFD != nil {
			t.Fatalf("local read offset=%d n=%d err=%v FUSE=%v", offset, n, err, f.fuseFD != nil)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Every original worker/handle is joined before reopening local authority.
	if !pf.StopWithin(5 * time.Second) {
		t.Fatal("prefetcher did not join before local reopen")
	}
	if !x.jfs.handler.StopHandler() {
		t.Fatal("old handler did not join")
	}
	x.session.Stop()
	x.rc.Stop()
	if err := x.rdb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := x.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}
	fuseBefore := fuseCellCounts(t)
	denyAddr, remoteAttempts := ownedPinRemoteRefusal(t)
	remoteBaseline := remoteAttempts.Load()
	var newStore *metadata.Store
	var newHandler *JuiceMountHandler
	var newRC *metadata.RedisClient
	var newSession *cache.Session
	var reopened *pin.Store
	var restore *pin.Prefetcher
	t.Cleanup(func() {
		if restore != nil && !restore.StopWithin(5*time.Second) {
			t.Error("restore worker did not join")
		}
		if newHandler != nil && !newHandler.StopHandler() {
			t.Error("reopened handler did not join")
		}
		if newSession != nil {
			newSession.Stop()
		}
		if newRC != nil {
			newRC.Stop()
		}
		if reopened != nil {
			_ = reopened.Close()
		}
		if newStore != nil {
			_ = newStore.Close()
		}
	})
	newStore, err = metadata.Open(x.db)
	if err != nil {
		t.Fatal(err)
	}
	newPaths, err := metadata.OpenCachePathEligibility(newStore)
	if err != nil {
		t.Fatal(err)
	}
	newHandler = NewHandler(newStore, x.fuseRoot)
	newHandler.sidecar.enabled = false
	newRC, err = metadata.NewRedisClientDeferred(x.raw, newStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataDeny := &ownedPinDenyRedis{}
	newRC.RawDB().AddHook(metadataDeny)
	controlCtx, controlCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer controlCancel()
	if err := newRC.RawDB().Ping(controlCtx).Err(); err == nil || metadataDeny.calls.Load() == 0 {
		t.Fatalf("metadata remote refusal control failed: err=%v calls=%d", err, metadataDeny.calls.Load())
	}
	commandCalls := metadataDeny.calls.Load()
	if _, err := newRC.RawDB().Pipelined(controlCtx, func(pipe redis.Pipeliner) error { pipe.Ping(controlCtx); return nil }); err == nil || metadataDeny.calls.Load() <= commandCalls {
		t.Fatalf("metadata pipeline refusal control failed: err=%v calls=%d", err, metadataDeny.calls.Load())
	}
	metadataBaseline := metadataDeny.calls.Load()
	opts, err := redisurl.Parse(x.raw)
	if err != nil {
		t.Fatal(err)
	}
	opts.Addr = denyAddr
	newSession, err = cache.NewSession(cache.SessionConfig{Store: newStore, LogicalURL: x.raw, Options: opts, StateDir: x.state, VerifyReader: func(*cache.Reader) error { return nil }, OnReader: newHandler.SetCacheReader, OnQualified: newRC.SetBackendQualified, OnStatus: newHandler.SetCacheBackendStatus})
	if err != nil {
		t.Fatal(err)
	}
	newHandler.SetBoundCacheSession(newSession, newRC, newPaths)
	newHandler.SetCacheReader(newSession.Reader())
	if newSession.Reader() == nil || newSession.Reader() == x.session.Reader() {
		t.Fatal("reopened session reused or omitted Reader")
	}
	if remoteAttempts.Load() != remoteBaseline || metadataDeny.calls.Load() != metadataBaseline {
		t.Fatal("local reopen attempted remote metadata")
	}
	y := &boundCacheFixture{jfs: &juiceFS{handler: newHandler}, store: newStore, session: newSession, paths: newPaths, fuseRoot: x.fuseRoot}
	reopened, err = pin.Open(pinDBPath)
	if err != nil {
		t.Fatal(err)
	}
	provider := NewOwnedPinProvider(newHandler, reopened)
	if err := reopened.SetReadyValidator(provider.Current); err != nil {
		t.Fatal(err)
	}
	if err := reopened.SetReadyRevoker(provider.Revoke); err != nil {
		t.Fatal(err)
	}
	newHandler.SetPinStore(reopened, "/volume")
	if reopened.IsPinnedReady(canonical) {
		t.Fatal("raw SQLite Ready admitted after reopen")
	}
	restore = pin.NewPrefetcher(reopened, x.fuseRoot, "/volume", 1, provider)
	restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer restoreCancel()
	if count, err := restore.RestoreReady(restoreCtx); err != nil || count != 1 {
		t.Fatalf("restore count=%d err=%v", count, err)
	}
	if remoteAttempts.Load() != remoteBaseline || metadataDeny.calls.Load() != metadataBaseline {
		t.Fatal("restore consulted remote")
	}
	for key, after := range fuseCellCounts(t) {
		if strings.HasPrefix(key, "foreground/") && after.Calls != fuseBefore[key].Calls {
			t.Fatalf("restore used foreground FUSE %s: %d calls", key, after.Calls-fuseBefore[key].Calls)
		}
	}
	client := localSidecarLoopback(t, newHandler)
	metricsBefore := metrics.Default().Snapshot()
	if digest := ownedPinWireFullDigest(t, client, newHandler, size); digest != wantDigest {
		t.Fatalf("fresh full-body NFS digest=%x want=%x", digest, wantDigest)
	}
	residentWireRead(t, client, newHandler, uint64(size-2048), 4096, make([]byte, 2048), 1)
	residentWireRead(t, client, newHandler, uint64(size), 4096, nil, 1)
	metricsAfter := metrics.Default().Snapshot()
	if metricsAfter.DirectSSDCacheHitBytes-metricsBefore.DirectSSDCacheHitBytes != uint64(size+2048) ||
		metricsAfter.BytesRead-metricsBefore.BytesRead != uint64(size+2048) {
		t.Fatalf("wire local-byte accounting SSD=%d read=%d want=%d", metricsAfter.DirectSSDCacheHitBytes-metricsBefore.DirectSSDCacheHitBytes, metricsAfter.BytesRead-metricsBefore.BytesRead, size+2048)
	}
	for key, after := range fuseCellCounts(t) {
		if strings.HasPrefix(key, "foreground/") && after.Calls != fuseBefore[key].Calls {
			t.Fatalf("fresh NFS read used foreground FUSE %s: %d calls", key, after.Calls-fuseBefore[key].Calls)
		}
	}
	if remoteAttempts.Load() != remoteBaseline || metadataDeny.calls.Load() != metadataBaseline {
		t.Fatal("fresh NFS read attempted remote command/connection")
	}
	f := openBoundCacheFile(t, y)
	got := make([]byte, 4096)
	if n, err := f.ReadAt(got, 0); n != len(got) || err != nil || f.fuseFD != nil {
		t.Fatalf("restored local read %d %v FUSE=%v", n, err, f.fuseFD != nil)
	}
	// A transient namespace owner must refuse, then release without converting
	// the immutable witness into an unrelated generation.
	op := newHandler.namespace.begin([]string{"media/clip.mov"})
	if op == nil {
		t.Fatal("namespace operation was not admitted")
	}
	op.blockReads()
	clear(got)
	if n, err := f.ReadAt(got, 0); n != 0 || err == nil {
		op.finish(nil)
		t.Fatalf("namespace owner leaked bytes: n=%d err=%v", n, err)
	}
	op.finish(nil)
	if n, err := f.ReadAt(got, 0); n != len(got) || err != nil {
		t.Fatalf("released namespace did not recover: n=%d err=%v", n, err)
	}
	// The single distinct middle block is required even though the two other
	// 64 MiB chunks remain resident.
	middleOffset := int64(cache.ChunkSize + 8*(4<<20))
	if err := os.Remove(block); err != nil {
		t.Fatal(err)
	}
	clear(got)
	if n, err := f.ReadAt(got, middleOffset); n != 0 || err == nil || f.fuseFD != nil || !bytes.Equal(got, make([]byte, len(got))) {
		t.Fatalf("evicted block fell through to FUSE: n=%d err=%v FUSE=%v", n, err, f.fuseFD != nil)
	}
	if err := os.WriteFile(block, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if n, err := f.ReadAt(got, middleOffset); n != 0 || err == nil || f.fuseFD != nil {
		t.Fatalf("corrupt middle block fell through: n=%d err=%v FUSE=%v", n, err, f.fuseFD != nil)
	}
	newHandler.incActiveWriter("media/clip.mov")
	var releaseWriter sync.Once
	finishWriter := func() { releaseWriter.Do(func() { newHandler.decActiveWriter("media/clip.mov") }) }
	defer finishWriter()
	if n, err := f.ReadAt(got, 0); n != 0 || err == nil {
		t.Fatalf("active writer retained Ready: n=%d err=%v", n, err)
	}
	finishWriter()
	if err := reopened.Pin(canonical, size, "/volume"); err != nil {
		t.Fatal(err)
	}
	if reopened.IsPinnedReady(canonical) {
		t.Fatal("repin retained old admission")
	}
	if n, err := f.ReadAt(got, 0); n != 0 || err == nil || f.fuseFD != nil {
		t.Fatalf("retained handle served successor: n=%d err=%v FUSE=%v", n, err, f.fuseFD != nil)
	}
	if remoteAttempts.Load() != remoteBaseline || metadataDeny.calls.Load() != metadataBaseline {
		t.Fatal("negative safety checks attempted remote command/connection")
	}
	for key, after := range fuseCellCounts(t) {
		if strings.HasPrefix(key, "foreground/") && after.Calls != fuseBefore[key].Calls {
			t.Fatalf("negative safety check used foreground FUSE %s: %d calls", key, after.Calls-fuseBefore[key].Calls)
		}
	}
}
