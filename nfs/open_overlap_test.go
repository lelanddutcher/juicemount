package nfs

import (
	"context"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

func overlapClass(t *testing.T, c netprofile.LinkClass) {
	t.Helper()
	netprofile.Default().ForceClass(&c)
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
}

func overlapFile(t *testing.T, p *FDPool, path string) *cachedFile {
	t.Helper()
	return &cachedFile{
		name: "clip.mov", fusePath: path, fdPool: p, handler: &JuiceMountHandler{},
		cacheReader: cache.NewReader(t.TempDir(), 4<<20, nil), inode: 7, fileSize: 4,
	}
}

func waitOpens(t *testing.T, so *scriptedOpen, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for so.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d open(s), have %d", n, so.count())
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// On a far link, a cold file's FUSE open starts beside the SSD reader's
// proof; the read's own descriptor acquisition joins it: one open in all.
func TestOverlappedOpenIsJoinedByTheRead(t *testing.T) {
	overlapClass(t, netprofile.ClassSlow)
	path := seedPoolFile(t, "clip.mov")
	p := NewFDPool()
	defer p.Stop()
	so := &scriptedOpen{release: make(chan struct{})}
	p.openRead = so.open
	f := overlapFile(t, p, path)

	f.startOverlappedOpen(0)
	waitOpens(t, so, 1)
	f.startOverlappedOpen(1 << 20) // once per file
	done := make(chan error, 1)
	go func() { done <- f.ensureFuseFD() }()
	waitJoiners(t, p, path, 1)
	close(so.release)
	if err := <-done; err != nil {
		t.Fatalf("ensureFuseFD: %v", err)
	}
	if so.count() != 1 {
		t.Fatalf("%d opens, want 1 shared open", so.count())
	}
	if f.fuseFD != so.firstFD() {
		t.Fatal("the read did not get the overlapped open's descriptor")
	}
	// The overlapped open's reference is released; the read holds one.
	deadline := time.Now().Add(5 * time.Second)
	for {
		e := readEntry(p, path)
		if e != nil && e.refCount == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool entry = %+v, want one reference (the read's)", e)
		}
		time.Sleep(200 * time.Microsecond)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if e := readEntry(p, path); e == nil || e.refCount != 0 {
		t.Fatalf("after close the entry = %+v, want an idle pooled descriptor", e)
	}
}

// No overlapped open where it cannot help or is not allowed.
func TestOverlappedOpenSkips(t *testing.T) {
	path := seedPoolFile(t, "clip.mov")
	for _, tc := range []struct {
		name  string
		class netprofile.LinkClass
		setup func(t *testing.T, f *cachedFile)
	}{
		{name: "near link", class: netprofile.ClassFast},
		{name: "medium link", class: netprofile.ClassMedium},
		{name: "offline", class: netprofile.ClassSlow, setup: func(t *testing.T, f *cachedFile) {
			was := pin.IsOffline()
			pin.SetOffline(true)
			t.Cleanup(func() { pin.SetOffline(was) })
		}},
		{name: "FUSE content reads refused", class: netprofile.ClassSlow, setup: func(t *testing.T, f *cachedFile) {
			f.handler.testRefuseFUSEContentReads = true
		}},
		{name: "no SSD reader", class: netprofile.ClassSlow, setup: func(t *testing.T, f *cachedFile) {
			f.cacheReader = nil
		}},
		{name: "no FD pool", class: netprofile.ClassSlow, setup: func(t *testing.T, f *cachedFile) {
			f.fdPool = nil
		}},
		{name: "switched off", class: netprofile.ClassSlow, setup: func(t *testing.T, f *cachedFile) {
			openOverlapOff = true
			t.Cleanup(func() { openOverlapOff = false })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			overlapClass(t, tc.class)
			p := NewFDPool()
			defer p.Stop()
			so := &scriptedOpen{release: make(chan struct{})}
			close(so.release)
			p.openRead = so.open
			f := overlapFile(t, p, path)
			if tc.setup != nil {
				tc.setup(t, f)
			}
			before := overlapOpenStarted.Load()
			f.startOverlappedOpen(0)
			time.Sleep(20 * time.Millisecond)
			if so.count() != 0 || overlapOpenStarted.Load() != before {
				t.Fatalf("an overlapped open ran (%d opens)", so.count())
			}
		})
	}
}

// The mapping job beside the proof is binding work: a closed binding starts
// none, and an open binding's stop waits for it.
func TestPrepareMappingBesideProofIsBindingWork(t *testing.T) {
	overlapClass(t, netprofile.ClassSlow)
	r := cache.NewReader(t.TempDir(), 4<<20, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &cacheBinding{workCtx: ctx, workCancel: cancel, workDone: make(chan struct{})}

	b.prepareMappingBesideProof(r, 7, 0, time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.workMu.Lock()
		active := b.workActive
		b.workMu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mapping job still active: %d", active)
		}
		time.Sleep(time.Millisecond)
	}

	b.workMu.Lock()
	b.workClosed = true
	b.workMu.Unlock()
	b.prepareMappingBesideProof(r, 7, 0, time.Second)
	b.workMu.Lock()
	active := b.workActive
	b.workMu.Unlock()
	if active != 0 {
		t.Fatalf("a closed binding started a mapping job (%d active)", active)
	}
}
