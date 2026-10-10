//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

func configureReplacementResidentCache(t *testing.T, c *chunk.Config) {
	configureReplacementResidentCacheVolume(t, c, "12345678-1234-abcd-5678-123456789abc")
}

func configureReplacementResidentCacheVolume(t *testing.T, c *chunk.Config, uuid string) {
	t.Helper()
	chunks, err := cache.VolumeCacheDir(uuid)
	if err != nil {
		t.Fatal(err)
	}
	c.CacheDir = filepath.Dir(filepath.Dir(chunks))
	var disk syscall.Statfs_t
	if err := syscall.Statfs(c.CacheDir, &disk); err != nil {
		t.Fatal(err)
	}
	if uint64(disk.Bavail)*uint64(disk.Bsize) < 128<<20 {
		t.Fatal("owned fixture needs 128 MiB")
	}
	c.FreeSpace, c.CacheSize, c.CacheMode = 0.01, 64<<20, 0600
	c.AutoCreate, c.CacheLargeWrite, c.CacheFullBlock = true, true, true
	c.CacheChecksum, c.CacheEviction = chunk.CsExtend, "none"
}

func publishedSettlementSDKFixture(t *testing.T, body []byte) (*boundCacheFixture, *SpoolStore, *metadata.SpoolRow) {
	t.Helper()
	x, spool, d, row, witness, _ := replacementNestedFixtureBody(t, body)
	fs, _ := replacementProxyAndFSConfig(t, x, spool, d, func(c *chunk.Config) { configureReplacementResidentCache(t, c) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := d.publishReplacement(ctx, row.ID, func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
		return &replacementSDKIO{fs: fs, m: fs.Meta(), w: w, ctx: jfsmeta.Background(), parent: 4}, func() {}, nil
	}, func(b metadata.CachePathBinding) error {
		if b != witness.Binding {
			return metadata.ErrMirrorConflict
		}
		return nil
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := x.session.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	journal, err := spool.Meta().ReplacementPublication(row)
	if err != nil || journal == nil {
		t.Fatalf("journal: %v", err)
	}
	receipt, err := replaceguard.Decode([]byte(journal.Record))
	if err != nil {
		t.Fatal(err)
	}
	r := x.session.Reader()
	r.InvalidateSliceCache(receipt.Stage.Inode)
	last := int64(0)
	if len(body) > 0 {
		last = (int64(len(body)) - 1) / cache.ChunkSize
	}
	for i := int64(0); i <= last; i++ {
		if err := r.PrepareChunkMapping(ctx, receipt.Stage.Inode, i); err != nil {
			t.Fatal(err)
		}
	}
	attr := cache.ResidentAttr{Size: int64(len(body)), MtimeNS: receipt.Stage.MtimeNS, Valid: true}
	got := make([]byte, len(body))
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		token, err := r.ReadResidentRange(receipt.Stage.Inode, 0, got, attr)
		if len(body) == 0 {
			token, err = r.ResidentEOF(receipt.Stage.Inode, attr)
		}
		if err == nil && token.Valid() && bytes.Equal(got, body) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatalf("cache publication: %v", err)
		case <-ticker.C:
		}
	}
	return x, spool, row
}

func TestReplacementSettlementSDKOfflineReopen(t *testing.T) { testReplacementOfflineReopen(t, false) }

func TestReplacementCleanupSDKOfflineReopen(t *testing.T) { testReplacementOfflineReopen(t, true) }

func testReplacementOfflineReopen(t *testing.T, cleanup bool) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"three_blocks", bytes.Repeat([]byte("sdk-settlement-"), 700003)},
		{"empty", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, spool, row := publishedSettlementSDKFixture(t, tc.body)
			used, _ := spool.Capacity()
			if err := x.jfs.handler.settleReplacement(context.Background(), row.ID); err != nil {
				t.Fatal(err)
			}
			current, err := spool.Meta().Get(row.ID)
			if err != nil || current.DrainState != metadata.DrainDone {
				t.Fatalf("settled row: %+v %v", current, err)
			}
			journal, err := spool.Meta().ReplacementPublication(current)
			if err != nil || journal == nil || journal.Phase != metadata.ReplacementPublicationSettled {
				t.Fatalf("settled journal: %+v %v", journal, err)
			}
			record, err := journal.SettlementRecord()
			if err != nil {
				t.Fatal(err)
			}
			mirror := x.store.LookupByPath(row.NFSPath)
			if mirror == nil || mirror.LocalOnly || mirror.Version != record.Published {
				t.Fatalf("published mirror: %+v", mirror)
			}
			if _, ok := spool.index.Lookup(row.NFSPath); ok {
				t.Fatal("old shadow remains authoritative")
			}
			if after, _ := spool.Capacity(); after != used {
				t.Fatal("settlement released private capacity")
			}
			if got, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(got, tc.body) {
				t.Fatalf("private image: %v", err)
			}
			expectedUsed := used
			if cleanup {
				if err := x.jfs.handler.cleanupReplacement(context.Background(), row.ID); err != nil {
					t.Fatal(err)
				}
				expectedUsed -= row.Size
				if after, _ := spool.Capacity(); after != expectedUsed {
					t.Fatalf("cleanup capacity %d != %d", after, expectedUsed)
				}
				if _, err := os.Lstat(row.SpoolFile); !os.IsNotExist(err) {
					t.Fatalf("cleanup source absence: %v", err)
				}
			}
			assertReplacementOfflineNFS(t, x, spool, row, tc.body, expectedUsed, !cleanup)

		})
	}
}

func assertReplacementOfflineNFS(t *testing.T, x *boundCacheFixture, spool *SpoolStore, row *metadata.SpoolRow, body []byte, expectedUsed int64, private bool) {
	t.Helper()
	assertReplacementOfflineNFSRoute(t, x, spool, row, body, expectedUsed, private, false)
}

func assertReplacementOfflineNFSRoute(t *testing.T, x *boundCacheFixture, spool *SpoolStore, row *metadata.SpoolRow, body []byte, expectedUsed int64, private, activeSpool bool) {
	t.Helper()
	opts := *x.rdb.Options()
	store, recovered := reopenReplacementPublicationFixture(t, x, spool.root)
	if _, err := recovered.RecoverOnBoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after, _ := recovered.Capacity(); after != expectedUsed {
		t.Fatalf("recovered capacity: %d != %d", after, expectedUsed)
	}
	if !private {
		if _, err := os.Lstat(row.SpoolFile); !os.IsNotExist(err) {
			t.Fatalf("recovery resurrected source: %v", err)
		}
	} else if got, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("recovery deleted private source: %v", err)
	}
	paths, err := metadata.OpenCachePathEligibility(store)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, x.fuseRoot)
	// A fresh offline Session loads saved binding and mappings. Refuse any
	// attempt to connect so success cannot rely on an available server.
	opts.Addr = "127.0.0.1:1"
	session, err := cache.NewSession(cache.SessionConfig{Store: store, LogicalURL: x.raw, Options: &opts, StateDir: x.state, VerifyReader: func(*cache.Reader) error { return nil }, OnReader: h.SetCacheReader, OnStatus: h.SetCacheBackendStatus})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.StopHandler(); session.Stop() })
	h.SetBoundCacheSession(session, nil, paths)
	h.SetCacheReader(session.Reader())
	h.SetSpool(recovered, nil)
	pin.SetOffline(true)
	// Exercise the actual NFS READ path, including its resident EOF
	// preparer; calling ReadAt past EOF directly bypasses that layer.
	if err := os.Remove(filepath.Join(x.fuseRoot, row.NFSPath)); err != nil {
		t.Fatal(err)
	}
	client := localSidecarLoopback(t, h)
	beforeFUSE := fuseCellCounts(t)
	if activeSpool {
		file, err := (&juiceFS{handler: h}).OpenFile(row.NFSPath, os.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, isSpool := file.(*spoolReadFile)
		closeErr := file.Close()
		if !private || !isSpool || closeErr != nil {
			t.Fatalf("private spool route=%T close=%v", file, closeErr)
		}
	}
	const readSize = 128 << 10
	compared := 0
	for off := 0; ; off += readSize {
		end := min(off+readSize, len(body))
		eof := uint32(0)
		if end == len(body) && !activeSpool {
			eof = 1
		}
		settlementWireRead(t, client, h, row.NFSPath, uint64(off), readSize, body[off:end], eof)
		compared += end - off
		if end == len(body) {
			break
		}
	}
	if compared != len(body) {
		t.Fatalf("compared %d of %d bytes", compared, len(body))
	}
	// A private spool may return its final bytes without EOF because it
	// clamps to its contiguous prefix. The next exact-end READ must prove EOF;
	// resident-cache cases keep their stricter EOF-on-tail contract above.
	settlementWireRead(t, client, h, row.NFSPath, uint64(len(body)), 128<<10, nil, 1)
	settlementWireRead(t, client, h, row.NFSPath, uint64(len(body)+4096), 128<<10, nil, 1)
	afterFUSE := fuseCellCounts(t)
	for _, op := range []string{"foreground/open", "foreground/fstat", "foreground/pread"} {
		calls, _, _, _ := cellDelta(beforeFUSE, afterFUSE, op)
		if calls != 0 {
			t.Fatalf("offline resident NFS used %s: %d", op, calls)
		}
	}
	if len(body) == 0 {
		f, err := (&juiceFS{handler: h}).OpenFile(row.NFSPath, os.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		cached, ok := f.(*cachedFile)
		if !ok {
			t.Fatalf("read route %T", f)
		}
		prepared, err := cached.PrepareResidentRead(0, 128<<10)
		if err != nil || prepared == nil || prepared.Size != 0 {
			t.Fatalf("empty resident proof: %+v %v", prepared, err)
		}
		if err := prepared.Check(); err != nil {
			t.Fatal(err)
		}
		session.Reader().InvalidateSliceCache(cached.inode)
		if err := prepared.Check(); err == nil {
			t.Fatal("invalidated empty proof remained valid")
		}
		if missing, err := cached.PrepareResidentRead(0, 128<<10); err != nil || missing != nil {
			t.Fatalf("missing empty witness minted EOF: %+v %v", missing, err)
		}
	}
}

func TestReplacementSettlementRevocationRetainsPrivateCustody(t *testing.T) {
	x, spool, row := publishedSettlementSDKFixture(t, []byte("retained saved image"))
	before, _ := spool.Capacity()
	b := x.jfs.handler.cacheBinding.Load()
	proof := b.readProof
	calls := 0
	b.readProof = func(ctx context.Context, uuid, name string, ids []uint64) (metadata.RedisPathProof, error) {
		p, err := proof(ctx, uuid, name, ids)
		calls++
		if calls == 2 {
			x.session.SetOnline(false)
		}
		return p, err
	}
	if err := x.jfs.handler.settleReplacement(context.Background(), row.ID); err == nil {
		t.Fatal("revoked session settled")
	}
	if calls != 2 {
		t.Fatalf("revocation boundary not reached: proof calls=%d", calls)
	}
	current, err := spool.Meta().Get(row.ID)
	if err != nil || current.DrainState != metadata.DrainReady {
		t.Fatalf("custody row: %+v %v", current, err)
	}
	p, err := spool.Meta().ReplacementPublication(current)
	if err != nil || p == nil || p.Phase != metadata.ReplacementPublicationCommitted {
		t.Fatalf("custody journal: %+v %v", p, err)
	}
	if after, _ := spool.Capacity(); after != before {
		t.Fatal("refusal released capacity")
	}
	if _, err := os.Stat(row.SpoolFile); err != nil {
		t.Fatal(err)
	}
}

func settlementWireRead(t *testing.T, client *latencyClient, h *JuiceMountHandler, name string, off uint64, count uint32, want []byte, wantEOF uint32) {
	t.Helper()
	pending, err := client.send(100003, 6, latencyReadArgs{Handle: h.ToHandle(nil, strings.Split(name, "/")), Offset: off, Count: count})
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
	body, err := latencyOpaque(reader)
	if err != nil || gotCount != uint32(len(want)) || eof != wantEOF || !bytes.Equal(body, want) || reader.Len() != 0 {
		t.Fatalf("offline NFS READ offset=%d count=%d want=%d EOF=%d want=%d err=%v trailing=%d", off, gotCount, len(want), eof, wantEOF, err, reader.Len())
	}
}
