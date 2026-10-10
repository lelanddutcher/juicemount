//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"os"
	"sync/atomic"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func TestBoundDirectSSDOfflineSequentialHitsDoNotPrefetch(t *testing.T) {
	resetPrefetchOfflineState(t)
	t.Setenv("JM_SERVER_READAHEAD", "")
	x := newBoundCacheFixture(t)
	cf := openBoundCacheFile(t, x)
	// Populate and verify the mapping before disconnecting Redis. The fixture
	// deliberately detaches readahead; attach the real handler manager below.
	warm := make([]byte, len(x.want))
	if n, err := cf.ReadAt(warm, 0); err != nil || n != len(warm) || !bytes.Equal(warm, x.want) {
		t.Fatalf("warm verified SSD read = %d %v", n, err)
	}
	if cf.fuseFD != nil {
		t.Fatal("warm read acquired the backing FUSE descriptor")
	}
	rm := x.jfs.handler.readahead
	if rm == nil {
		t.Fatal("handler has no readahead manager")
	}
	var speculativeReads atomic.Int32
	rm.prefetchReadAt = func(fd *os.File, p []byte, off int64) (int, error) {
		speculativeReads.Add(1)
		return fd.ReadAt(p, off)
	}
	cf.readahead = rm
	x.session.SetOnline(false)
	pin.SetOffline(true)
	if err := x.rdb.Del(context.Background(), "setting", "d1", "d2", "i1", "i2", "i3", "c3_0").Err(); err != nil {
		t.Fatal(err)
	}
	const chunk = 4096
	for off := 0; off < len(x.want); off += chunk {
		got := make([]byte, chunk)
		n, err := cf.ReadAt(got, int64(off))
		if err != nil || n != chunk || !bytes.Equal(got, x.want[off:off+chunk]) {
			t.Fatalf("offline verified SSD read at %d = %d %v", off, n, err)
		}
		if cf.fuseFD != nil {
			t.Fatalf("offline direct SSD hit at %d opened FUSE", off)
		}
	}
	rm.Stop()
	if scheduled, prefetched := rm.Stats(); scheduled != 0 || prefetched != 0 || speculativeReads.Load() != 0 {
		t.Fatalf("offline direct SSD hits scheduled=%d prefetched=%d ReadAt=%d", scheduled, prefetched, speculativeReads.Load())
	}
}
