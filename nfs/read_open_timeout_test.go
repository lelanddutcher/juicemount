package nfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Exercise all foreground read-open callers with a real blocked kernel open.
// The release is deliberately beyond the old 800ms limit, but within 2s.
// FIFO contents are never read: it supplies only the descriptor-open barrier.
func TestReadOpenSlowLinkSurvivesLegacyDeadline(t *testing.T) {
	t.Setenv("JM_FUSE_OP_TIMEOUT_MS", "")
	p := netprofile.Default()
	class := netprofile.ClassSlow
	p.ForceClass(&class)
	t.Cleanup(func() { p.ForceClass(nil) })
	for _, lane := range []string{"lazy-pooled", "lazy-unpooled", "mirror-without-reader", "unmirrored"} {
		t.Run(lane, func(t *testing.T) { testBlockedReadOpen(t, lane, 1100*time.Millisecond, false) })
	}
}

type blockedReadOpenResult struct {
	f       io.Closer
	err     error
	elapsed time.Duration
}

func testBlockedReadOpen(t *testing.T, lane string, delay time.Duration, wantTimeout bool) {
	t.Helper()
	jfs, store, root := newIntegrityHarness(t)
	const rel = "cold.mov"
	fifo := mkfifoT(t, root, rel)
	var open func() (io.Closer, error)
	switch lane {
	case "lazy-pooled", "lazy-unpooled":
		f := &cachedFile{fusePath: fifo}
		if lane == "lazy-pooled" {
			f.fdPool = jfs.handler.fdPool
		}
		open = func() (io.Closer, error) { return f, f.ensureFuseFD() }
	case "mirror-without-reader":
		if err := store.Insert(metadata.MakeEntry(rel, false, 4096, time.Now(), 100)); err != nil {
			t.Fatal(err)
		}
		open = func() (io.Closer, error) { return jfs.OpenFile(rel, os.O_RDONLY, 0) }
	case "unmirrored":
		open = func() (io.Closer, error) { return jfs.OpenFile(rel, os.O_RDONLY, 0) }
	default:
		t.Fatal(lane)
	}
	waitReadOpenCondition(t, "preceding foreground syscall retirement", func() bool { return len(nfsLstatGate) == 0 })
	result := make(chan blockedReadOpenResult, 1)
	go func() {
		start := time.Now()
		f, err := open()
		result <- blockedReadOpenResult{f, err, time.Since(start)}
	}()
	// Opening both ends without blocking also lets cleanup release a syscall
	// after an assertion fails, without leaving a writer goroutine behind.
	var partner *os.File
	received := false
	unblock := func() {
		if partner == nil {
			var err error
			partner, err = os.OpenFile(fifo, os.O_RDWR|syscall.O_NONBLOCK, 0)
			if err != nil {
				t.Fatalf("release FIFO open: %v", err)
			}
		}
	}
	defer func() {
		unblock()
		if !received {
			select {
			case r := <-result:
				if r.f != nil {
					r.f.Close()
				}
			case <-time.After(3 * time.Second):
				t.Error("read-open worker did not retire during cleanup")
			}
		}
		waitReadOpenCondition(t, "foreground syscall retirement", func() bool { return len(nfsLstatGate) == 0 })
		partner.Close()
	}()
	// Admission proves the old 800ms timer has started before the 1100ms
	// hold begins; goroutine scheduling cannot turn the regression green.
	waitReadOpenCondition(t, "foreground open admission", func() bool { return len(nfsLstatGate) == 1 })
	time.Sleep(delay)
	unblock()
	select {
	case r := <-result:
		received = true
		if r.f != nil {
			defer r.f.Close()
		}
		if wantTimeout {
			if !errors.Is(r.err, errFUSETimeout) {
				t.Fatalf("%s returned %v, want bounded timeout", lane, r.err)
			}
			if r.elapsed >= delay {
				t.Errorf("%s did not return before syscall release: %v >= %v", lane, r.elapsed, delay)
			}
		} else if r.err != nil {
			t.Fatalf("healthy slow-link %s failed after %v: %v", lane, delay, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read open did not terminate after FIFO release")
	}
}

func TestReadOpenExplicitLimitBoundsAllLanes(t *testing.T) {
	t.Setenv("JM_FUSE_OP_TIMEOUT_MS", "40")
	p := netprofile.Default()
	class := netprofile.ClassSlow
	p.ForceClass(&class)
	t.Cleanup(func() { p.ForceClass(nil) })
	for _, lane := range []string{"lazy-pooled", "lazy-unpooled", "mirror-without-reader", "unmirrored"} {
		t.Run(lane, func(t *testing.T) { testBlockedReadOpen(t, lane, 200*time.Millisecond, true) })
	}
}

func TestReadOpenLANStillBoundsBlockedOpen(t *testing.T) {
	t.Setenv("JM_FUSE_OP_TIMEOUT_MS", "")
	p := netprofile.Default()
	p.ResetForTransportChange()
	class := netprofile.ClassFast
	p.ForceClass(&class)
	t.Cleanup(func() { p.ForceClass(nil); p.ResetForTransportChange() })
	testBlockedReadOpen(t, "lazy-pooled", 1100*time.Millisecond, true)
}

func TestReadOpenTimeoutFollowsTransportChanges(t *testing.T) {
	t.Setenv("JM_FUSE_OP_TIMEOUT_MS", "")
	p := netprofile.Default()
	p.ResetForTransportChange()
	t.Cleanup(func() { p.ForceClass(nil); p.ResetForTransportChange() })
	base := fuseStatTimeout
	for _, c := range []netprofile.LinkClass{netprofile.ClassFast, netprofile.ClassSlow, netprofile.ClassMetered, netprofile.ClassMedium} {
		c := c
		p.ForceClass(&c)
		want := base
		if c == netprofile.ClassSlow || c == netprofile.ClassMetered {
			want = max(want, 2*time.Second)
		}
		if got := readOpenTimeout(); got != want {
			t.Errorf("class %v bound=%v, want %v", c, got, want)
		}
		if fuseStatTimeout != base {
			t.Fatal("read profile changed global metadata bound")
		}
	}
	for i := 0; i < 6; i++ {
		p.ObserveRTT(300 * time.Millisecond)
	}
	if !p.HighLatency() || readOpenTimeout() != max(base, 2*time.Second) {
		t.Fatal("high-latency link did not extend READ-open bound")
	}
	if got := readOpenTimeoutFor(netprofile.ClassSlow, true, 5*time.Second); got != 5*time.Second {
		t.Fatalf("larger base was shortened: %v", got)
	}
	for _, bad := range []string{"garbage", "0", "-1"} {
		t.Setenv("JM_FUSE_OP_TIMEOUT_MS", bad)
		if got := readOpenTimeout(); got != max(base, 2*time.Second) {
			t.Errorf("invalid override %q changed bound to %v", bad, got)
		}
	}
}

func waitReadOpenCondition(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for " + what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReadOpenLatePooledReferenceCustody(t *testing.T) {
	for _, invalidate := range []bool{false, true} {
		name := "reusable"
		if invalidate {
			name = "invalidated"
		}
		t.Run(name, func(t *testing.T) {
			p := NewFDPool()
			defer p.Stop()
			fifo := mkfifoT(t, t.TempDir(), "late-open")
			result := make(chan bool, 1)
			received := false
			var partner *os.File
			unblock := func() {
				if partner != nil {
					return
				}
				var err error
				partner, err = os.OpenFile(fifo, os.O_RDWR|syscall.O_NONBLOCK, 0)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer func() {
				unblock()
				if !received {
					select {
					case <-result:
					case <-time.After(3 * time.Second):
						t.Error("pooled open worker did not retire during cleanup")
					}
				}
				waitReadOpenCondition(t, "pooled syscall retirement", func() bool { return len(nfsLstatGate) == 0 })
				partner.Close()
			}()
			go func() {
				fd, err, ok := pooledReadOpenWithTimeout(metrics.FUSESrcForeground, p, fifo, 100*time.Millisecond)
				if fd != nil && err == nil {
					p.Release(fifo)
				}
				result <- fd == nil && err == nil && !ok
			}()
			waitOpensInFlight(t, p, 1)
			select {
			case timedOut := <-result:
				received = true
				if !timedOut {
					t.Fatal("blocked open did not time out")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("bounded pooled open did not return")
			}
			if len(nfsLstatGate) == 0 {
				t.Fatal("timeout prematurely released syscall gate")
			}
			if invalidate {
				p.Invalidate(fifo)
			}
			unblock()
			waitReadOpenCondition(t, "late pool reference release", func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()
				entry := p.entries[fdKey{path: fifo}]
				return len(p.openInFlight) == 0 && len(nfsLstatGate) == 0 &&
					entry != nil && entry.refCount == 0 && entry.stale == invalidate
			})
			p.mu.Lock()
			oldFD := p.entries[fdKey{path: fifo}].fd
			p.mu.Unlock()
			fd, err := p.Get(fifo)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Release(fifo)
			if _, err := fd.Stat(); err != nil {
				t.Fatalf("late cleanup closed shared descriptor: %v", err)
			}
			if invalidate {
				if fd == oldFD {
					t.Fatal("invalidated late descriptor was reused")
				}
				if _, err := oldFD.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("displaced idle descriptor not closed: %v", err)
				}
			} else if fd != oldFD {
				t.Fatal("late cleanup discarded a reusable pooled descriptor")
			}
		})
	}
}

func TestReadOpenLateUnpooledDescriptorCloses(t *testing.T) {
	name := filepath.Join(t.TempDir(), "local")
	if err := os.WriteFile(name, []byte("local"), 0600); err != nil {
		t.Fatal(err)
	}
	fd, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	unblock := make(chan struct{})
	defer func() {
		select {
		case <-unblock:
		default:
			close(unblock)
		}
	}()
	_, _, ok := openWithTimeout(metrics.FUSESrcForeground, 40*time.Millisecond, func() (*os.File, error) {
		<-unblock
		return fd, nil
	})
	if ok {
		t.Fatal("blocked open did not time out")
	}
	if len(nfsLstatGate) == 0 {
		t.Fatal("timeout prematurely released syscall gate")
	}
	close(unblock)
	waitReadOpenCondition(t, "late descriptor close", func() bool {
		_, err := fd.Stat()
		return errors.Is(err, os.ErrClosed) && len(nfsLstatGate) == 0
	})
}
