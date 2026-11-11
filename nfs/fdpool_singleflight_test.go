package nfs

// Single-flight read opens (2026-11-11). macOS sends READs at 0 and 1 MiB for
// the first open of a file. Each built its own cachedFile and called
// FDPool.Get; both missed and both ran an open. macFUSE serves one request per
// file at a time, so on a slow link the second open queued behind the first
// read, passed the read-open timeout, and the client retried ~4 s later.
// Get now shares one open per path. These tests pin the sharing and the
// protections it must keep: failure, invalidation and remount during a shared
// open, a wedged open, Stop, and the bounded-open custody.

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// waitJoiners blocks until n callers wait on the shared open for path.
func waitJoiners(t *testing.T, p *FDPool, path string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		got := 0
		if rec := p.readFlights[path]; rec != nil {
			got = rec.joiners
		}
		p.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d joiner(s), have %d", n, got)
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// readEntry returns a copy of the read-slot entry for path, or nil, so the
// caller never reads pool state outside p.mu.
func readEntry(p *FDPool, path string) *poolEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.entries[fdKey{path: path}]
	if e == nil {
		return nil
	}
	c := *e
	return &c
}

// scriptedOpen blocks every open until release is closed, then runs os.Open,
// or returns failWith when it is set. It counts the opens that ran.
type scriptedOpen struct {
	mu       sync.Mutex
	calls    int
	release  chan struct{}
	failWith error
	first    *os.File // the descriptor the first open returned
}

func (s *scriptedOpen) open(path string) (*os.File, error) {
	s.mu.Lock()
	s.calls++
	isFirst := s.calls == 1
	s.mu.Unlock()
	<-s.release
	if s.failWith != nil {
		return nil, s.failWith
	}
	fd, err := os.Open(path)
	if isFirst {
		s.mu.Lock()
		s.first = fd
		s.mu.Unlock()
	}
	return fd, err
}

func (s *scriptedOpen) firstFD() *os.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.first
}

func (s *scriptedOpen) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func seedPoolFile(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte("clip"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFDPoolConcurrentReadOpensShareOneOpen(t *testing.T) {
	path := seedPoolFile(t, "open-a1-1.mov")
	p := NewFDPool()
	defer p.Stop()
	so := &scriptedOpen{release: make(chan struct{})}
	p.openRead = so.open

	const callers = 8
	res := make(chan getResult, callers)
	for i := 0; i < callers; i++ {
		go func() {
			fd, err := p.Get(path)
			res <- getResult{fd, err}
		}()
	}
	waitOpensInFlight(t, p, 1)
	waitJoiners(t, p, path, callers-1)
	close(so.release)

	var first *os.File
	for i := 0; i < callers; i++ {
		r := <-res
		if r.err != nil {
			t.Fatalf("Get: %v", r.err)
		}
		if first == nil {
			first = r.fd
		} else if r.fd != first {
			t.Fatal("a concurrent Get of the same path got a second descriptor")
		}
	}
	if n := so.count(); n != 1 {
		t.Fatalf("opens = %d, want 1 shared open", n)
	}
	e := readEntry(p, path)
	if e == nil || e.stale || e.refCount != callers {
		t.Fatalf("entry = %+v, want servable with refCount %d (one ref per caller)", e, callers)
	}
	for i := 0; i < callers; i++ {
		p.Release(path)
	}
	if e := readEntry(p, path); e.refCount != 0 {
		t.Fatalf("refCount after every Release = %d, want 0", e.refCount)
	}
	p.mu.Lock()
	flights, inflight := len(p.readFlights), len(p.openInFlight)
	p.mu.Unlock()
	if flights != 0 || inflight != 0 {
		t.Fatalf("flight records left: readFlights=%d openInFlight=%d", flights, inflight)
	}
}

// Writes never share: GetWrite callers may pass different flags.
func TestFDPoolWriteOpensDoNotJoinReadFlight(t *testing.T) {
	path := seedPoolFile(t, "w.mov")
	p := NewFDPool()
	defer p.Stop()
	so := &scriptedOpen{release: make(chan struct{})}
	p.openRead = so.open
	res := make(chan getResult, 1)
	go func() {
		fd, err := p.Get(path)
		res <- getResult{fd, err}
	}()
	waitOpensInFlight(t, p, 1)
	wfd, err := p.GetWrite(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("GetWrite while a read open is in flight: %v", err)
	}
	p.ReleaseWrite(path)
	close(so.release)
	r := <-res
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.fd == wfd {
		t.Fatal("read and write slots share a descriptor")
	}
	p.Release(path)
}

func TestFDPoolFailedSharedOpenFailsJoinersAndDoesNotWedge(t *testing.T) {
	path := seedPoolFile(t, "fail.mov")
	p := NewFDPool()
	defer p.Stop()
	boom := errors.New("scripted open failure")
	so := &scriptedOpen{release: make(chan struct{}), failWith: boom}
	p.openRead = so.open

	res := make(chan getResult, 3)
	for i := 0; i < 3; i++ {
		go func() {
			fd, err := p.Get(path)
			res <- getResult{fd, err}
		}()
	}
	waitOpensInFlight(t, p, 1)
	waitJoiners(t, p, path, 2)
	close(so.release)
	for i := 0; i < 3; i++ {
		if r := <-res; !errors.Is(r.err, boom) || r.fd != nil {
			t.Fatalf("Get = (%v, %v), want the leader's error", r.fd, r.err)
		}
	}
	if n := so.count(); n != 1 {
		t.Fatalf("a failing open ran %d times, want once for all waiters", n)
	}
	if e := readEntry(p, path); e != nil {
		t.Fatalf("a failed open pooled an entry: %+v", e)
	}

	// The next Get opens afresh and succeeds.
	so.failWith = nil
	fd, err := p.Get(path)
	if err != nil || fd == nil {
		t.Fatalf("Get after a failed shared open = (%v, %v); the failure wedged later opens", fd, err)
	}
	p.Release(path)
	if n := so.count(); n != 2 {
		t.Fatalf("opens = %d, want 2", n)
	}
}

// Invalidate (rename or delete) during a shared open: the leader keeps its fd,
// pooled stale; every joiner refuses it and gets a fresh open.
func TestFDPoolInvalidateDuringSharedOpenRefusesJoiners(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invalidate func(p *FDPool, path string)
	}{
		{"Invalidate", func(p *FDPool, path string) { p.Invalidate(path) }},
		{"InvalidateReads", func(p *FDPool, path string) { p.InvalidateReads(path) }},
		{"InvalidateTree", func(p *FDPool, path string) { p.InvalidateTree(filepath.Dir(path)) }},
		{"FlushStale-remount", func(p *FDPool, _ string) { p.FlushStale() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := seedPoolFile(t, "renamed.mov")
			p := NewFDPool()
			defer p.Stop()
			so := &scriptedOpen{release: make(chan struct{})}
			p.openRead = so.open

			leader := make(chan getResult, 1)
			go func() {
				fd, err := p.Get(path)
				leader <- getResult{fd, err}
			}()
			waitOpensInFlight(t, p, 1)
			joiners := make(chan getResult, 2)
			for i := 0; i < 2; i++ {
				go func() {
					fd, err := p.Get(path)
					joiners <- getResult{fd, err}
				}()
			}
			waitJoiners(t, p, path, 2)
			tc.invalidate(p, path)

			// A Get that arrives after the invalidation must not join the
			// poisoned flight either.
			late := make(chan getResult, 1)
			go func() {
				fd, err := p.Get(path)
				late <- getResult{fd, err}
			}()
			waitOpensInFlight(t, p, 2)
			close(so.release)

			l := <-leader
			if l.err != nil {
				t.Fatalf("leader Get: %v", l.err)
			}
			poisoned := so.firstFD()
			others := []getResult{<-joiners, <-joiners, <-late}
			for _, r := range others {
				if r.err != nil {
					t.Fatalf("joiner Get: %v", r.err)
				}
				if r.fd == poisoned {
					t.Fatal("a joiner was served the descriptor opened across the invalidation")
				}
			}
			if n := so.count(); n < 2 {
				t.Fatalf("opens = %d, want the poisoned open plus at least one fresh open", n)
			}
			e := readEntry(p, path)
			if e == nil || e.stale || e.fd == poisoned {
				t.Fatalf("pooled entry after the race = %+v, want a fresh servable fd", e)
			}
			p.Release(path) // leader: if its stale entry was displaced, this pays the parked debt
			for range others {
				p.Release(path)
			}
			if e := readEntry(p, path); e.refCount != 0 {
				t.Fatalf("refCount after all Releases = %d, want 0", e.refCount)
			}
		})
	}
}

// A remount hands out new descriptors: after FlushStale no Get may be served
// an fd from before it, shared or not.
func TestFDPoolRemountAfterSharedOpenHandsOutNewFD(t *testing.T) {
	path := seedPoolFile(t, "remount.mov")
	p := NewFDPool()
	defer p.Stop()
	old, err := p.Get(path)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := p.Get(path); again != old {
		t.Fatal("precondition: a second Get should reuse the pooled fd")
	}
	p.FlushStale()
	var wg sync.WaitGroup
	got := make(chan *os.File, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fd, err := p.Get(path)
			if err != nil {
				t.Error(err)
				return
			}
			got <- fd
		}()
	}
	wg.Wait()
	close(got)
	var fresh *os.File
	for fd := range got {
		if fd == old {
			t.Fatal("a Get after the remount was served the pre-remount fd")
		}
		if fresh == nil {
			fresh = fd
		} else if fd != fresh {
			t.Fatal("post-remount Gets did not converge on one pooled fd")
		}
	}
	p.Release(path)
	p.Release(path)
	for i := 0; i < 4; i++ {
		p.Release(path)
	}
	if e := readEntry(p, path); e.refCount != 0 {
		t.Fatalf("refCount = %d, want 0", e.refCount)
	}
}

// An open older than fdOpenJoinMax is treated as wedged: a new caller opens
// on its own instead of waiting on it.
func TestFDPoolWedgedSharedOpenIsNotJoined(t *testing.T) {
	path := seedPoolFile(t, "wedged.mov")
	p := NewFDPool()
	defer p.Stop()
	wedged := make(chan struct{})
	var calls int
	var mu sync.Mutex
	p.openRead = func(name string) (*os.File, error) {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			<-wedged
		}
		return os.Open(name)
	}
	leader := make(chan getResult, 1)
	go func() {
		fd, err := p.Get(path)
		leader <- getResult{fd, err}
	}()
	waitOpensInFlight(t, p, 1)
	p.mu.Lock()
	p.readFlights[path].started = time.Now().Add(-fdOpenJoinMax - time.Second)
	p.mu.Unlock()

	fd, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get behind a wedged open: %v", err)
	}
	close(wedged)
	l := <-leader
	if l.err != nil {
		t.Fatal(l.err)
	}
	// The wedged leader found the fresh entry and closed its own fd.
	if l.fd != fd {
		t.Fatal("the late leader did not converge on the pooled entry")
	}
	if e := readEntry(p, path); e.refCount != 2 {
		t.Fatalf("refCount = %d, want 2", e.refCount)
	}
	p.Release(path)
	p.Release(path)
	p.mu.Lock()
	flights := len(p.readFlights)
	p.mu.Unlock()
	if flights != 0 {
		t.Fatalf("readFlights left: %d", flights)
	}
}

func TestFDPoolStopDuringSharedOpenFailsJoiners(t *testing.T) {
	path := seedPoolFile(t, "stop.mov")
	p := NewFDPool()
	so := &scriptedOpen{release: make(chan struct{})}
	p.openRead = so.open
	res := make(chan getResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			fd, err := p.Get(path)
			res <- getResult{fd, err}
		}()
	}
	waitOpensInFlight(t, p, 1)
	waitJoiners(t, p, path, 1)
	p.Stop()
	close(so.release)
	for i := 0; i < 2; i++ {
		if r := <-res; !errors.Is(r.err, os.ErrClosed) {
			t.Fatalf("Get across Stop = (%v, %v), want ErrClosed", r.fd, r.err)
		}
	}
}

// A joiner that times out in pooledReadOpenWithTimeout still owes its ref
// once the shared open lands; the late-release watcher pays it, and the
// leader's ref is untouched.
func TestPooledReadOpenJoinerTimeoutKeepsRefCustody(t *testing.T) {
	path := seedPoolFile(t, "timeout.mov")
	p := NewFDPool()
	defer p.Stop()
	so := &scriptedOpen{release: make(chan struct{})}
	p.openRead = so.open
	leader := make(chan getResult, 1)
	go func() {
		fd, err := p.Get(path)
		leader <- getResult{fd, err}
	}()
	waitOpensInFlight(t, p, 1)

	fd, err, ok := pooledReadOpenWithTimeout(metrics.FUSESrcForeground, p, path, 50*time.Millisecond)
	if ok || fd != nil || err != nil {
		t.Fatalf("bounded joiner = (%v, %v, %v), want a timeout", fd, err, ok)
	}
	close(so.release)
	if l := <-leader; l.err != nil {
		t.Fatal(l.err)
	}
	waitReadOpenCondition(t, "joiner's late Release", func() bool {
		e := readEntry(p, path)
		return e != nil && e.refCount == 1 && len(nfsLstatGate) == 0
	})
	p.Release(path)
	if n := so.count(); n != 1 {
		t.Fatalf("opens = %d, want 1", n)
	}
}

// The first-file shape end to end through the bounded open: two RPCs open the
// same file at once, both get the same descriptor from one open.
func TestPooledReadOpenConcurrentFirstReadsShareDescriptor(t *testing.T) {
	path := seedPoolFile(t, "first.mov")
	p := NewFDPool()
	defer p.Stop()
	so := &scriptedOpen{release: make(chan struct{})}
	p.openRead = so.open
	type res struct {
		fd *os.File
		ok bool
	}
	out := make(chan res, 2)
	for i := 0; i < 2; i++ {
		go func() {
			fd, err, ok := pooledReadOpenWithTimeout(metrics.FUSESrcForeground, p, path, 3*time.Second)
			if err != nil {
				ok = false
			}
			out <- res{fd, ok}
		}()
	}
	waitOpensInFlight(t, p, 1)
	waitJoiners(t, p, path, 1)
	close(so.release)
	a, b := <-out, <-out
	if !a.ok || !b.ok || a.fd != b.fd {
		t.Fatalf("first reads = %+v / %+v, want one shared descriptor", a, b)
	}
	p.Release(path)
	p.Release(path)
}
