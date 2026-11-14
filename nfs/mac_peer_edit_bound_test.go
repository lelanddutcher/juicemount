//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// P4 (laptop run 2026-11-14): a teammate's save landed while a READ was
// served from the verified SSD copy. The identity check refused the bytes
// (EAGAIN), the NFS layer answered JUKEBOX, and the Mac retried 4 s later:
// playback froze for 4 s per save. An in-place update of the same mirror row
// now falls through to the live FUSE read. A replacement keeps the refusal
// (TestBoundCacheRetainedHandleRefusesReplacement and the proof-window tests).
func TestBoundCachePeerSaveDuringReadFallsThroughToLiveRead(t *testing.T) {
	for _, grow := range []int64{0, 1 << 20} {
		x := newBoundCacheFixture(t)
		cf := openBoundCacheFile(t, x)
		b := x.jfs.handler.cacheBinding.Load()
		edited := false
		b.beforeCacheRead = func() {
			if edited {
				return
			}
			edited = true
			old := x.store.LookupByPath("media/clip.mov")
			epoch, err := x.store.CaptureStoreEpoch()
			if err != nil {
				t.Fatal(err)
			}
			e := metadata.MakeEntry(old.Path, false, old.Size+grow, old.Mtime.Add(time.Second), old.Inode)
			e.Mode, e.MtimeNsecKnown = old.Mode, true
			if _, err := x.store.ReconcileUpsertBatch([]*metadata.Entry{e}, epoch); err != nil {
				t.Fatal(err)
			}
		}
		got := make([]byte, 4096)
		n, err := cf.ReadAt(got, 0)
		if err != nil || n != len(got) || !bytes.Equal(got, bytes.Repeat([]byte{'F'}, len(got))) || cf.fuseFD == nil {
			t.Fatalf("grow %d: read during a teammate's save = %d %v, FUSE=%v (want the live bytes, no JUKEBOX)", grow, n, err, cf.fuseFD != nil)
		}
	}
}

// The P4 fallthrough is for an in-place save only (review M2). A teammate's
// save that makes the file shorter, and a replacement of the row (delete and
// create, even with the same inode number: a new incarnation), keep the
// refusal: no byte, EAGAIN (JUKEBOX), and no FUSE read.
func TestBoundCachePeerChangeDuringReadKeepsRefusal(t *testing.T) {
	for _, change := range []string{"shorter", "new incarnation"} {
		t.Run(change, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			cf := openBoundCacheFile(t, x)
			b := x.jfs.handler.cacheBinding.Load()
			changed := false
			b.beforeCacheRead = func() {
				if changed {
					return
				}
				changed = true
				old := x.store.LookupByPath("media/clip.mov")
				if change == "new incarnation" {
					replaceBoundCacheEntry(t, x, "media/clip.mov")
					if now := x.store.LookupByPath("media/clip.mov"); now == nil || now.Inode != old.Inode {
						t.Fatal("the replacement must keep the inode number")
					}
					return
				}
				epoch, err := x.store.CaptureStoreEpoch()
				if err != nil {
					t.Fatal(err)
				}
				e := metadata.MakeEntry(old.Path, false, old.Size-4096, old.Mtime.Add(time.Second), old.Inode)
				e.Mode, e.MtimeNsecKnown = old.Mode, true
				if _, err := x.store.ReconcileUpsertBatch([]*metadata.Entry{e}, epoch); err != nil {
					t.Fatal(err)
				}
			}
			assertBoundCacheRefusal(t, cf, syscall.EAGAIN)
			if !changed {
				t.Fatal("the change never ran under the read")
			}
		})
	}
}
