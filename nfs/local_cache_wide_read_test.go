//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

const localSidecarPath = "media/._clip.mov"

func seedBoundAppleDouble(t *testing.T, x *boundCacheFixture) []byte {
	t.Helper()
	body := bytes.Repeat([]byte{'S'}, 4096)
	backing := filepath.Join(x.fuseRoot, localSidecarPath)
	if err := os.WriteFile(backing, body, 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(backing)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Ino == 0 || info.Size() != int64(len(body)) {
		t.Fatal("backing source identity unavailable")
	}
	inode := uint64(st.Ino)
	name := strconv.FormatUint(inode, 10)
	entry := metadata.MakeEntry(localSidecarPath, false, info.Size(), info.ModTime(), inode)
	entry.MtimeNsecKnown = true
	entry.Mode = info.Mode()
	if err := x.store.Insert(entry); err != nil {
		t.Fatal(err)
	}
	var edge [9]byte
	edge[0] = meta.TypeFile
	binary.BigEndian.PutUint64(edge[1:], inode)
	var record [24]byte
	binary.BigEndian.PutUint64(record[4:12], 78)
	binary.BigEndian.PutUint32(record[12:16], uint32(len(body)))
	binary.BigEndian.PutUint32(record[20:24], uint32(len(body)))
	attr := (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: uint64(len(body)),
		Mtime: info.ModTime().Unix(), Mtimensec: uint32(info.ModTime().Nanosecond())}).Marshal()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pipe := x.rdb.Pipeline()
	pipe.HSet(ctx, "d2", "._clip.mov", edge[:])
	pipe.Set(ctx, "i"+name, attr, 0)
	pipe.RPush(ctx, "c"+name+"_0", string(record[:]))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	chunks, err := cache.VolumeCacheDir("selected-volume")
	if err != nil {
		t.Fatal(err)
	}
	block := filepath.Join(chunks, "0/0/78_0_4096")
	if err := os.MkdirAll(filepath.Dir(block), 0700); err != nil {
		t.Fatal(err)
	}
	encoded := append([]byte(nil), body...)
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	encoded = append(encoded, sum[:]...)
	if err := os.WriteFile(block, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	row := x.store.LookupByPath(localSidecarPath)
	if row == nil || row.Inode != inode || row.Size != info.Size() ||
		row.Mtime.UnixNano() != info.ModTime().UnixNano() || row.Mode != info.Mode() {
		t.Fatal("mirror and retained source identity differ")
	}
	if x.jfs.handler.strongSidecarRead(localSidecarPath) != nil ||
		x.jfs.handler.beginSidecarSnapshotRead(localSidecarPath) != nil {
		t.Fatal("sidecar was prewarmed before wire READ")
	}
	return body
}

func openBoundAppleDouble(t *testing.T, x *boundCacheFixture) *cachedFile {
	t.Helper()
	opened, err := x.jfs.OpenFile(localSidecarPath, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := opened.(*cachedFile)
	if !ok {
		_ = opened.Close()
		t.Fatalf("unexpected sidecar handle %T", opened)
	}
	t.Cleanup(func() { _ = f.Close() })
	f.memBuf = nil
	f.readahead = nil
	return f
}

// The existing latencyClient framing/parser is used with an owned readLoop
// join, which newLatencyClient does not expose.
func localSidecarLoopback(t *testing.T, h *JuiceMountHandler) *latencyClient {
	t.Helper()
	server := nfslib.NewManagedServer(h)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		_ = listener.Close()
		<-serveDone
		t.Fatal(err)
	}
	client := &latencyClient{conn: conn, pending: make(map[uint32]chan latencyReply)}
	readDone := make(chan struct{})
	go func() { client.readLoop(); close(readDone) }()
	var once sync.Once
	t.Cleanup(func() {
		once.Do(func() {
			_ = conn.Close()
			_ = listener.Close()
			if !server.Quiesce(3 * time.Second) {
				t.Error("server did not quiesce")
			}
			select {
			case <-readDone:
			case <-time.After(3 * time.Second):
				t.Error("client readLoop did not join")
			}
			select {
			case <-serveDone:
			case <-time.After(3 * time.Second):
				t.Error("Serve did not join")
			}
		})
	})
	return client
}

func localSidecarWireRead(t *testing.T, client *latencyClient, h *JuiceMountHandler, count uint32, want []byte, wantEOF uint32) {
	t.Helper()
	pending, err := client.send(100003, 6, latencyReadArgs{
		Handle: h.ToHandle(nil, []string{"media", "._clip.mov"}), Offset: 0, Count: count,
	})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := pending.await(time.Now().Add(3 * time.Second))
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
	data, err := latencyOpaque(reader)
	if err != nil || gotCount != uint32(len(want)) || eof != wantEOF || !bytes.Equal(data, want) || reader.Len() != 0 {
		t.Fatalf("READ count=%d want=%d EOF=%d want=%d err=%v trailing=%d", gotCount, len(want), eof, wantEOF, err, reader.Len())
	}
}

func TestBoundAppleDoubleWideCachedWireAvoidsRepeatedFUSEMetadata(t *testing.T) {
	if memBufServeEnabled {
		t.Fatal("this wire oracle requires the default disabled memBuf serve path")
	}
	// A small request uses an independent unprimed fixture. It need not
	// claim terminal EOF when the requested count equals the file size.
	t.Run("exact-4096-control", func(t *testing.T) {
		x := newBoundCacheFixture(t)
		body := seedBoundAppleDouble(t, x)
		client := localSidecarLoopback(t, x.jfs.handler)
		before := fuseCellCounts(t)
		localSidecarWireRead(t, client, x.jfs.handler, 4096, body, 0)
		after := fuseCellCounts(t)
		open, _, _, _ := cellDelta(before, after, "foreground/open")
		fstat, _, _, _ := cellDelta(before, after, "foreground/fstat")
		if open != 0 || fstat != 0 {
			t.Fatalf("small verified local read entered FUSE metadata: open=%d fstat=%d", open, fstat)
		}
		if x.jfs.handler.strongSidecarRead(localSidecarPath) != nil {
			t.Fatal("unsized small SSD read minted a terminal source proof")
		}
	})

	t.Run("wide-repeat", func(t *testing.T) {
		x := newBoundCacheFixture(t)
		body := seedBoundAppleDouble(t, x)
		client := localSidecarLoopback(t, x.jfs.handler)
		beforeCells := fuseCellCounts(t)
		beforeMetrics := metrics.Default().Snapshot()
		localSidecarWireRead(t, client, x.jfs.handler, 65536, body, 1)
		firstCells := fuseCellCounts(t)
		firstMetrics := metrics.Default().Snapshot()
		firstOpen, _, _, _ := cellDelta(beforeCells, firstCells, "foreground/open")
		firstFstat, _, _, _ := cellDelta(beforeCells, firstCells, "foreground/fstat")
		if firstOpen == 0 || firstFstat == 0 ||
			firstMetrics.DirectSSDCacheHit <= beforeMetrics.DirectSSDCacheHit ||
			firstMetrics.SidecarCachePut <= beforeMetrics.SidecarCachePut {
			t.Fatalf("first wide READ did not prove expected local+metadata route: open=%d fstat=%d SSD=%d->%d sidecarPut=%d->%d",
				firstOpen, firstFstat, beforeMetrics.DirectSSDCacheHit, firstMetrics.DirectSSDCacheHit,
				beforeMetrics.SidecarCachePut, firstMetrics.SidecarCachePut)
		}
		if x.jfs.handler.strongSidecarRead(localSidecarPath) == nil {
			t.Fatal("complete bound SSD read did not finish the sized source proof")
		}
		localSidecarWireRead(t, client, x.jfs.handler, 65536, body, 1)
		secondCells := fuseCellCounts(t)
		secondMetrics := metrics.Default().Snapshot()
		secondOpen, _, _, _ := cellDelta(firstCells, secondCells, "foreground/open")
		secondFstat, _, _, _ := cellDelta(firstCells, secondCells, "foreground/fstat")
		t.Logf("secret-free repeated READ: first open/fstat=%d/%d repeat open/fstat=%d/%d SSD hits=%d->%d sidecar hits=%d->%d",
			firstOpen, firstFstat, secondOpen, secondFstat, firstMetrics.DirectSSDCacheHit,
			secondMetrics.DirectSSDCacheHit, firstMetrics.SidecarCacheHit, secondMetrics.SidecarCacheHit)
		if secondOpen != 0 || secondFstat != 0 {
			t.Errorf("verified repeated wide sidecar READ still paid foreground FUSE metadata: open=%d fstat=%d", secondOpen, secondFstat)
		}
	})
}

func TestBoundAppleDoubleCachePromotionGuards(t *testing.T) {
	for _, scenario := range []string{"partial-and-offset", "unbound-reader", "writer-generation", "stale-low"} {
		t.Run(scenario, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			body := seedBoundAppleDouble(t, x)
			f := openBoundAppleDouble(t, x)
			if scenario == "stale-low" {
				backing := filepath.Join(x.fuseRoot, localSidecarPath)
				if err := os.WriteFile(backing, append(append([]byte(nil), body...), body...), 0600); err != nil {
					t.Fatal(err)
				}
				if size, err := f.AuthoritativeSize(); size != 2*int64(len(body)) || err != nil || f.sourceProof != nil {
					t.Fatalf("stale-low authoritative boundary=%d,%v proof=%v", size, err, f.sourceProof != nil)
				}
			} else if size, err := f.AuthoritativeSize(); size != int64(len(body)) || err != nil || f.sourceProof == nil {
				t.Fatalf("expected original source boundary=%d,%v proof=%v", size, err, f.sourceProof != nil)
			}
			if scenario == "unbound-reader" {
				chunks, err := cache.VolumeCacheDir("selected-volume")
				if err != nil {
					t.Fatal(err)
				}
				f.cacheReader = cache.NewReader(chunks, 4096, x.rdb)
				if _, bound := f.cacheReader.VolumeBinding(); bound {
					t.Fatal("negative reader unexpectedly selected a volume")
				}
			}
			if scenario == "writer-generation" {
				b := x.jfs.handler.cacheBinding.Load()
				b.beforeCacheRead = func() {
					x.jfs.handler.incActiveWriter(localSidecarPath)
					x.jfs.handler.decActiveWriter(localSidecarPath)
				}
				t.Cleanup(func() { b.beforeCacheRead = nil })
			}
			count, off := len(body), int64(0)
			if scenario == "partial-and-offset" {
				count, off = len(body)/2, int64(len(body)/2)
			}
			before := metrics.Default().Snapshot()
			got := make([]byte, count)
			n, err := f.ReadAt(got, off)
			if err != nil || n != count || !bytes.Equal(got, body[off:off+int64(count)]) {
				t.Fatalf("verified cache read=%d,%v", n, err)
			}
			if metrics.Default().Snapshot().DirectSSDCacheHit <= before.DirectSSDCacheHit {
				t.Fatal("negative did not traverse the direct SSD path")
			}
			if x.jfs.handler.strongSidecarRead(localSidecarPath) != nil || f.completedSidecarSourceProof() != nil {
				t.Fatal("incomplete, unbound, changed-writer, or stale-low SSD bytes gained terminal source proof")
			}
		})
	}
}
