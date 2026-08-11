package nfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const sentinel = 0xA7

func marked(b []byte) bool {
	return len(b) > 0 && b[0] == sentinel && b[len(b)-1] == sentinel
}

func mark(b []byte) {
	b[0] = sentinel
	b[len(b)-1] = sentinel
}

// POSITIVE CONTROL. Without this, the timed-out test below would pass even if
// releaseOfflineReadBuf recycled nothing at all — proving only that a pool
// which never pools also never mis-pools.
func TestCompletedReadBufferIsRecycled(t *testing.T) {
	seen := false
	for i := 0; i < 200 && !seen; i++ {
		b := offlineReadBuf(offlineReadBufCap)
		mark(b)
		releaseOfflineReadBuf(b, true)
		got := offlineReadBuf(offlineReadBufCap)
		seen = marked(got)
		releaseOfflineReadBuf(got, true)
	}
	if !seen {
		t.Fatal("no buffer was ever recycled after a COMPLETED read — the pool is " +
			"not pooling, which makes TestTimedOutReadBufferIsNeverRecycled vacuous")
	}
}

// THE RELEASE RULE. A bound-exceeded read leaves readAtBounded's goroutine
// still holding and still writing into the buffer. Recycling it hands a
// live-written buffer to the next reader — the exact cross-request corruption
// the private buffer exists to prevent, only with a longer fuse.
func TestTimedOutReadBufferIsNeverRecycled(t *testing.T) {
	for i := 0; i < 200; i++ {
		b := offlineReadBuf(offlineReadBufCap)
		mark(b)
		releaseOfflineReadBuf(b, false) // bound exceeded: goroutine still owns it

		got := offlineReadBuf(offlineReadBufCap)
		if marked(got) {
			t.Fatalf("iteration %d: a buffer from a TIMED-OUT read came back out of "+
				"the pool. Its orphan goroutine is still writing into it, so the "+
				"next offline read would serve bytes another read is mutating", i)
		}
		releaseOfflineReadBuf(got, true)
	}
}

// An oversized request bypasses the pool entirely; releasing it must not push a
// wrong-capacity buffer in, or later reads would get a short buffer.
func TestOversizedBufferIsNotPooled(t *testing.T) {
	big := offlineReadBuf(offlineReadBufCap * 2)
	if len(big) != offlineReadBufCap*2 {
		t.Fatalf("offlineReadBuf(%d) returned %d bytes", offlineReadBufCap*2, len(big))
	}
	releaseOfflineReadBuf(big, true)
	for i := 0; i < 50; i++ {
		if got := offlineReadBuf(offlineReadBufCap); cap(got) != offlineReadBufCap {
			t.Fatalf("pool handed out cap=%d, want %d — an oversized buffer was pooled",
				cap(got), offlineReadBufCap)
		}
	}
}

func TestBufferIsExactlyTheRequestedLength(t *testing.T) {
	for _, n := range []int{1, 4096, 64 << 10, offlineReadBufCap} {
		if got := offlineReadBuf(n); len(got) != n {
			t.Errorf("offlineReadBuf(%d) len = %d", n, len(got))
		}
	}
}

// End-to-end through readAtBounded: the real caller's buffer must never be the
// one the bounded read wrote into. Run under -race, an orphan goroutine writing
// into a buffer another reader holds is a reported data race.
func TestBoundedReadNeverWritesTheCallersBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.bin")
	payload := bytes.Repeat([]byte{0x5A}, 64<<10)
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	callers := make([]byte, len(payload)) // stands in for the pooled NFS buffer
	tmp := offlineReadBuf(len(callers))
	n, rerr, done := readAtBounded(f, tmp, 0, 1500*time.Millisecond)
	if !done || rerr != nil {
		t.Fatalf("done=%v err=%v", done, rerr)
	}
	if !bytes.Equal(callers, make([]byte, len(payload))) {
		t.Fatal("the bounded read wrote into the caller's buffer directly")
	}
	copy(callers, tmp[:n])
	releaseOfflineReadBuf(tmp, done)
	if !bytes.Equal(callers, payload) {
		t.Fatal("copied-out bytes do not match the file")
	}
}
