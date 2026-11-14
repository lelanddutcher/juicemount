package nfs

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Bug F1 (laptop run 2026-11-14): a teammate's long save on a far link
// commits parts early, so the attr push reports the played clip every ~2 s
// for the whole save, and a refresh is always pending. #96's READ waited for
// "no refresh pending", which never held: every READ waited the whole bound
// and answered JUKEBOX, and the Mac backed off for up to 30 s.

// The model: hooks every 15 ms for 1.2 s (a continuous save), a refresh takes
// 60 ms, a READ every 5 ms with a 400 ms bound. Every READ must read on,
// none may wait much longer than two refreshes, and the runs must be
// coalesced, not one per hook.
func TestContentRefreshContinuousSaveNeverStarvesReads(t *testing.T) {
	var runs atomic.Int32
	c := newContentRefresher(func(uint64, string) error {
		runs.Add(1)
		time.Sleep(60 * time.Millisecond)
		return nil
	})
	stop := make(chan struct{})
	var hooks atomic.Int32
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.begin("clip", 9, "/fuse/clip")
			hooks.Add(1)
			time.Sleep(15 * time.Millisecond)
		}
	}()
	var reads, starved int
	var longest time.Duration
	for end := time.Now().Add(1200 * time.Millisecond); time.Now().Before(end); {
		start := time.Now()
		if err := c.wait("clip", 400*time.Millisecond); err != nil {
			starved++
		}
		longest = max(longest, time.Since(start))
		reads++
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	t.Logf("%d hooks, %d refreshes, %d READs, %d starved, longest wait %s", hooks.Load(), runs.Load(), reads, starved, longest)
	if starved != 0 {
		t.Fatalf("%d of %d READs waited past the bound during a continuous save", starved, reads)
	}
	// A READ waits for the run going when it arrives, a pause no longer than
	// that run, and one full run: about 3 x 60 ms.
	if longest > 300*time.Millisecond {
		t.Fatalf("longest READ wait %s, want about three refresh times", longest)
	}
	if h, r := hooks.Load(), runs.Load(); r*3 > h {
		t.Fatalf("%d refreshes for %d hooks: not coalesced", r, h)
	}
	if err := c.wait("clip", 2*time.Second); err != nil {
		t.Fatalf("after the save: %v", err)
	}
	for deadline := time.Now().Add(2 * time.Second); c.waiting.Load() != 0; {
		if time.Now().After(deadline) {
			t.Fatal("the refresh loop did not end after the save")
		}
		time.Sleep(time.Millisecond)
	}
	c.mu.Lock()
	left := len(c.paths)
	c.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d covered paths kept", left)
	}
}

// A READ waits only for the hooks before it: one that arrives while a run
// covers it reads on even though a later hook started another run.
func TestContentRefreshReadWaitsOnlyForEarlierHooks(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 8)
	c := newContentRefresher(func(uint64, string) error { started <- struct{}{}; <-gate; return nil })
	c.maxGap = 0
	c.begin("f", 3, "/x/f") // hook 1; run 1 covers it
	<-started
	c.begin("f", 3, "/x/f") // hook 2, during run 1
	res := make(chan error, 1)
	go func() { res <- c.wait("f", 5*time.Second) }() // needs hook 2: run 2
	time.Sleep(20 * time.Millisecond)
	gate <- struct{}{} // run 1 ends; run 2 starts
	<-started
	select {
	case err := <-res:
		t.Fatalf("READ read on before the run that covers its hook (%v)", err)
	case <-time.After(30 * time.Millisecond):
	}
	c.begin("f", 3, "/x/f") // hook 3, after the READ: must not hold it back
	gate <- struct{}{}      // run 2 ends
	select {
	case err := <-res:
		if err != nil {
			t.Fatalf("READ after its covering run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a later hook held the READ back")
	}
	<-started
	close(gate)
}

// P2 under coalescing: the peer saves version after version of the held
// file while readers play it. No reply may pair a version's attributes with
// older bytes, no READ may answer JUKEBOX, and the last version is what a
// READ after the save returns.
func TestPeerEditContinuousSaveNeverPairsOldBytesWithNewAttributes(t *testing.T) {
	jfs, store, s, file := peerEditHarness(t, true)
	s.delay = 40 * time.Millisecond
	const size = 64 << 10
	base := store.LookupByPath(file).Mtime
	version := func(k int) []byte { return bytes.Repeat([]byte{byte('B' + k)}, size) } // 'B'..'Z'
	mtimeOf := func(k int) time.Time { return base.Add(time.Duration(k+1) * time.Millisecond) }

	var mu sync.Mutex
	mtimeVersion := map[int64]int{base.UnixNano(): -1}
	type reply struct {
		mtime int64
		data  byte
		err   error
	}
	var replies []reply
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				p, err := nfsReadErr(t, jfs, file, 8<<10, 4096)
				var b byte
				if err == nil && len(p.data) > 0 {
					b = p.data[0]
				}
				mu.Lock()
				replies = append(replies, reply{mtime: p.mtime.UnixNano(), data: b, err: err})
				mu.Unlock()
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}
	const saves = 25
	for k := 0; k < saves; k++ {
		mu.Lock()
		mtimeVersion[mtimeOf(k).UnixNano()] = k
		mu.Unlock()
		s.stage(version(k))
		peerEdit(t, store, file, size, mtimeOf(k))
		time.Sleep(12 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	byteVersion := func(b byte) int {
		if b == 'A' {
			return -1
		}
		if k := int(b) - 'B'; k >= 0 && k < saves {
			return k
		}
		return -2
	}
	var bad, jukebox, served int
	for _, r := range replies {
		if r.err != nil {
			if errors.Is(r.err, syscall.EAGAIN) {
				jukebox++
				continue
			}
			t.Fatalf("READ: %v", r.err)
		}
		served++
		want, ok := mtimeVersion[r.mtime]
		if !ok {
			t.Fatalf("reply mtime %d is no version's", r.mtime)
		}
		if got := byteVersion(r.data); got < want {
			bad++
		}
	}
	t.Logf("%d replies, %d served, %d JUKEBOX, %d old bytes under newer attributes, %d refreshes for %d saves",
		len(replies), served, jukebox, bad, s.calls.Load(), saves)
	if bad != 0 {
		t.Fatalf("%d READ replies paired a version's attributes with older bytes", bad)
	}
	if jukebox != 0 {
		t.Fatalf("%d READs answered JUKEBOX during a continuous save", jukebox)
	}
	if n := s.calls.Load(); n == 0 || n >= saves {
		t.Fatalf("%d refreshes for %d saves: want coalesced runs", n, saves)
	}
	p := nfsRead(t, jfs, file, 8<<10, 4096)
	if !p.mtime.Equal(mtimeOf(saves-1)) || !bytes.Equal(p.data, version(saves - 1)[:4096]) {
		t.Fatalf("after the save: mtime %v data %q, want the last version", p.mtime, p.data[:1])
	}
}
