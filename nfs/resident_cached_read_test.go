//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

func residentWireRead(t *testing.T, client *latencyClient, h *JuiceMountHandler, off uint64, count uint32, want []byte, wantEOF uint32) {
	t.Helper()
	pending, err := client.send(100003, 6, latencyReadArgs{Handle: h.ToHandle(nil, []string{"media", "clip.mov"}), Offset: off, Count: count})
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
		t.Fatalf("READ count=%d want=%d EOF=%d want=%d err=%v trailing=%d", gotCount, len(want), eof, wantEOF, err, reader.Len())
	}
}

// This is distinct from the frozen red diagnostic so the exact original test
// can be rerun unchanged against this candidate. FUSE counters are local
// backing syscall counts, not a claim about remote packets or Finder paint.
func TestResidentTailWireUsesNoBackingMetadataOnlineOffline(t *testing.T) {
	if memBufServeEnabled {
		t.Fatal("requires disabled memory buffer")
	}
	x := newBoundCacheFixture(t)
	client := localSidecarLoopback(t, x.jfs.handler)
	// Fetch the joint mapping+inode attr once through the ordinary SSD lane.
	residentWireRead(t, client, x.jfs.handler, 0, 4096, x.want[:4096], 0)
	for _, offline := range []bool{false, true} {
		pin.SetOffline(offline)
		x.session.SetOnline(!offline)
		for _, tc := range []struct {
			name  string
			off   uint64
			count uint32
			want  []byte
			eof   uint32
		}{
			{"tail", 61440, 65536, x.want[61440:], 1},
			{"pastEOF", uint64(len(x.want)) + 4096, 65536, nil, 1},
		} {
			t.Run(tc.name+map[bool]string{false: "-online", true: "-offline"}[offline], func(t *testing.T) {
				before := fuseCellCounts(t)
				bm := metrics.Default().Snapshot()
				residentWireRead(t, client, x.jfs.handler, tc.off, tc.count, tc.want, tc.eof)
				after := fuseCellCounts(t)
				open, _, _, _ := cellDelta(before, after, "foreground/open")
				stat, _, _, _ := cellDelta(before, after, "foreground/fstat")
				if open != 0 || stat != 0 {
					t.Fatalf("resident READ consulted backing open=%d fstat=%d", open, stat)
				}
				if tc.name == "tail" && metrics.Default().Snapshot().DirectSSDCacheHit-bm.DirectSSDCacheHit != 1 {
					t.Fatal("expected exactly one verified SSD hit for prepared tail")
				}
			})
		}
	}
}

func TestResidentTailDeclinesStaleLowAndMissingRange(t *testing.T) {
	x := newBoundCacheFixture(t)
	f := openBoundCacheFile(t, x)
	buf := make([]byte, 4096)
	if _, err := f.ReadAt(buf, 61440); err != nil {
		t.Fatal(err)
	}
	// A new server attr can describe more bytes while the local mirror still
	// says 65536. Revalidation revokes the old tail witness.
	backing := filepath.Join(x.fuseRoot, "media/clip.mov")
	if err := os.WriteFile(backing, bytes.Repeat([]byte{'F'}, 131072), 0600); err != nil {
		t.Fatal(err)
	}
	attr := (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: 131072, Mtime: 1700000000, Mtimensec: 123}).Marshal()
	if err := x.rdb.Set(t.Context(), "i3", attr, 0).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := x.session.Reader().RevalidateMappings(ctx); err != nil {
		t.Fatal(err)
	}
	prepared, err := f.PrepareResidentRead(61440, 65536)
	if err != nil || prepared != nil {
		t.Fatalf("stale-low mirror minted EOF: proof=%v err=%v", prepared != nil, err)
	}
	// A missing resident mapping cannot mint terminal EOF either.
	x.session.Reader().InvalidateSliceCache(3)
	prepared, err = f.PrepareResidentRead(61440, 65536)
	if err != nil || prepared != nil {
		t.Fatalf("missing mapping minted EOF: proof=%v err=%v", prepared != nil, err)
	}
}
