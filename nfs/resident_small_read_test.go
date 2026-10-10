//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

func assertSmallResidentNoForegroundFUSE(t *testing.T, before map[string]metrics.FUSECellSnapshot) {
	t.Helper()
	for key, row := range fuseCellCounts(t) {
		if strings.HasPrefix(key, "foreground/") && row.Calls != before[key].Calls {
			t.Fatalf("small resident READ entered %s: before=%d after=%d", key, before[key].Calls, row.Calls)
		}
	}
}

func TestResidentSmallCrossEOFAndAtEOFWire(t *testing.T) {
	if memBufServeEnabled {
		t.Fatal("requires disabled memory buffer")
	}
	x := newBoundCacheFixture(t)
	client := localSidecarLoopback(t, x.jfs.handler)
	residentWireRead(t, client, x.jfs.handler, 0, 4096, x.want[:4096], 0)
	for _, tc := range []struct {
		name   string
		offset uint64
		want   []byte
		eof    uint32
		hits   uint64
	}{
		{"in-range-control", 61440, x.want[61440:], 0, 1},
		{"cross-EOF", 63488, x.want[63488:], 1, 1},
		{"at-EOF", 65536, nil, 1, 0},
		{"past-EOF", 69632, nil, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := fuseCellCounts(t)
			hits := metrics.Default().Snapshot().DirectSSDCacheHit
			residentWireRead(t, client, x.jfs.handler, tc.offset, 4096, tc.want, tc.eof)
			assertSmallResidentNoForegroundFUSE(t, before)
			if got := metrics.Default().Snapshot().DirectSSDCacheHit - hits; got != tc.hits {
				t.Fatalf("direct SSD hits=%d want=%d", got, tc.hits)
			}
		})
	}
}

func TestResidentSmallCrossEOFRejectsInvalidationBeforeReply(t *testing.T) {
	x := newBoundCacheFixture(t)
	warmResidentOriginal(t, x)
	var hooks atomic.Int32
	x.jfs.handler.testReadCachedInfo = func(name string) {
		if name == "media/clip.mov" {
			hooks.Add(1)
			x.session.Reader().InvalidateSliceCache(3)
		}
	}
	client := localSidecarLoopback(t, x.jfs.handler)
	before := fuseCellCounts(t)
	pending, err := client.send(100003, 6, latencyReadArgs{Handle: x.jfs.handler.ToHandle(nil, []string{"media", "clip.mov"}), Offset: 63488, Count: 4096})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := pending.await(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if hooks.Load() != 1 || len(reply.body) < 4 || binary.BigEndian.Uint32(reply.body[:4]) != uint32(nfslib.NFSStatusJukebox) {
		t.Fatalf("small prepared proof did not refuse before reply: hooks=%d body=%x", hooks.Load(), reply.body)
	}
	assertSmallResidentNoForegroundFUSE(t, before)
}

func TestResidentSmallCrossEOFMissPreservesOriginalCount(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt", "unqualified"} {
		t.Run(kind, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			if kind != "unqualified" {
				warmResidentOriginal(t, x)
				chunks, err := cache.VolumeCacheDir("selected-volume")
				if err != nil {
					t.Fatal(err)
				}
				block := filepath.Join(chunks, "0/0/77_0_65536")
				if kind == "missing" {
					if err := os.Remove(block); err != nil {
						t.Fatal(err)
					}
				} else {
					body, err := os.ReadFile(block)
					if err != nil {
						t.Fatal(err)
					}
					body[63488] ^= 1
					if err := os.WriteFile(block, body, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Backend is deliberately larger and has F bytes. If a failed local
			// proof clamps Count to old EOF, this request would silently truncate.
			if err := os.WriteFile(filepath.Join(x.fuseRoot, "media/clip.mov"), bytes.Repeat([]byte{'F'}, 131072), 0600); err != nil {
				t.Fatal(err)
			}
			client := localSidecarLoopback(t, x.jfs.handler)
			residentWireRead(t, client, x.jfs.handler, 63488, 4096, bytes.Repeat([]byte{'F'}, 4096), 0)
		})
	}
}
