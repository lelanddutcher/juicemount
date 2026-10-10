//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

func warmResidentOriginal(t *testing.T, x *boundCacheFixture) *cachedFile {
	t.Helper()
	f := openBoundCacheFile(t, x)
	buf := make([]byte, 4096)
	if n, err := f.ReadAt(buf, 61440); err != nil || n != len(buf) || !bytes.Equal(buf, x.want[61440:]) {
		t.Fatalf("warm local tail n=%d err=%v", n, err)
	}
	return f
}

func TestResidentPreparedProofRejectsKnownChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *boundCacheFixture)
	}{
		{"completed-writer", func(t *testing.T, x *boundCacheFixture) {
			x.jfs.handler.incActiveWriter("media/clip.mov")
			x.jfs.handler.decActiveWriter("media/clip.mov")
		}},
		{"leaf-replacement", func(t *testing.T, x *boundCacheFixture) { replaceBoundCacheEntry(t, x, "media/clip.mov") }},
		{"ancestor-replacement", func(t *testing.T, x *boundCacheFixture) { replaceBoundCacheEntry(t, x, "media") }},
		{"mapping-invalidation", func(t *testing.T, x *boundCacheFixture) { x.session.Reader().InvalidateSliceCache(3) }},
		{"volume-refusal", func(t *testing.T, x *boundCacheFixture) { x.session.RefuseIdentity(cache.ErrVolumeBinding) }},
		{"session-stop", func(t *testing.T, x *boundCacheFixture) { x.session.Stop() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			f := warmResidentOriginal(t, x)
			prepared, err := f.PrepareResidentRead(61440, 65536)
			if err != nil || prepared == nil || prepared.Check == nil || prepared.Size != 65536 || !bytes.Equal(prepared.Data, x.want[61440:]) {
				t.Fatalf("missing positive prepared control: proof=%v err=%v", prepared != nil, err)
			}
			if err := prepared.Check(); err != nil {
				t.Fatalf("unchanged proof refused: %v", err)
			}
			tc.mutate(t, x)
			if err := prepared.Check(); err == nil {
				t.Fatal("known change retained terminal proof")
			}
			if f.fuseFD != nil {
				t.Fatal("proof validation opened backing descriptor")
			}
		})
	}
}

func TestResidentWireRefusesInvalidationBeforeReply(t *testing.T) {
	x := newBoundCacheFixture(t)
	warmResidentOriginal(t, x)
	var calls atomic.Int32
	x.jfs.handler.testReadCachedInfo = func(name string) {
		if name == "media/clip.mov" {
			calls.Add(1)
			x.session.Reader().InvalidateSliceCache(3)
		}
	}
	client := localSidecarLoopback(t, x.jfs.handler)
	before := fuseCellCounts(t)
	pending, err := client.send(100003, 6, latencyReadArgs{Handle: x.jfs.handler.ToHandle(nil, []string{"media", "clip.mov"}), Offset: 61440, Count: 65536})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := pending.await(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || len(reply.body) < 4 || binary.BigEndian.Uint32(reply.body[:4]) != uint32(nfslib.NFSStatusJukebox) {
		t.Fatalf("expected one after-staging hook and JUKEBOX, hooks=%d reply=%x", calls.Load(), reply.body)
	}
	after := fuseCellCounts(t)
	open, _, _, _ := cellDelta(before, after, "foreground/open")
	stat, _, _, _ := cellDelta(before, after, "foreground/fstat")
	if open != 0 || stat != 0 {
		t.Fatalf("rejected snapshot fell back: open=%d fstat=%d", open, stat)
	}
}

func TestResidentMissingOrCorruptTailDoesNotClampFallback(t *testing.T) {
	for _, name := range []string{"missing", "corrupt", "unqualified"} {
		t.Run(name, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			if name != "unqualified" {
				warmResidentOriginal(t, x)
				chunks, err := cache.VolumeCacheDir("selected-volume")
				if err != nil {
					t.Fatal(err)
				}
				block := filepath.Join(chunks, "0/0/77_0_65536")
				if name == "missing" {
					if err := os.Remove(block); err != nil {
						t.Fatal(err)
					}
				} else {
					body, err := os.ReadFile(block)
					if err != nil {
						t.Fatal(err)
					}
					body[61440] ^= 1
					if err := os.WriteFile(block, body, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			// The old mirror says 64 KiB; a declined preparation must leave the
			// original 64 KiB request intact so authoritative fallback sees growth.
			if err := os.WriteFile(filepath.Join(x.fuseRoot, "media/clip.mov"), bytes.Repeat([]byte{'F'}, 131072), 0600); err != nil {
				t.Fatal(err)
			}
			client := localSidecarLoopback(t, x.jfs.handler)
			residentWireRead(t, client, x.jfs.handler, 61440, 65536, bytes.Repeat([]byte{'F'}, 65536), 0)
		})
	}
}
