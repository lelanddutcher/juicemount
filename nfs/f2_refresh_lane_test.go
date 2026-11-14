package nfs

import (
	"bytes"
	"encoding/binary"
	"io/fs"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Bug F2 (laptop run 2026-11-14): abroad, while a teammate saved the clip
// being played and the Mac saved its own files, every content refresh waited
// 1.5-6.7 s for juicefs's one control channel behind the drain's
// create-files messages, and each READ of the clip waited for a refresh.

func refreshReply(flags byte) []byte {
	r := []byte{0, flags}
	r = binary.BigEndian.AppendUint64(r, 64<<10)
	return binary.BigEndian.AppendUint64(r, 42)
}

func laneTestControls(t *testing.T) (lane, shared *jfsControl) {
	t.Helper()
	root := t.TempDir()
	t.Cleanup(func() {
		jfsControlsMu.Lock()
		delete(jfsControls, root)
		delete(jfsLanes, root)
		jfsControlsMu.Unlock()
	})
	lane, shared = refreshJFSControl(root), sharedJFSControl(root)
	if lane == shared || lane.controlFile() != jfsControlLaneFile || shared.controlFile() != ".control" || lane.fallback != shared {
		t.Fatalf("lane %q (fallback %p), shared %q (%p)", lane.controlFile(), lane.fallback, shared.controlFile(), shared)
	}
	if refreshJFSControl(root) != lane {
		t.Fatal("a second lookup made another lane")
	}
	return lane, shared
}

// The refresh runs on the lane while a create-files message holds the shared
// channel for its uploads. The drain's messages keep the shared channel and
// its order: a pin sent behind the create still waits for it.
func TestContentRefreshRunsBesideTheDrainOnTheControlLane(t *testing.T) {
	lane, shared := laneTestControls(t)
	var mu sync.Mutex
	var sharedCmds, laneCmds []uint32
	inCreate, release := make(chan struct{}), make(chan struct{})
	shared.testLocked = func(msg []byte, _ int) ([]byte, error) {
		cmd := binary.BigEndian.Uint32(msg)
		mu.Lock()
		sharedCmds = append(sharedCmds, cmd)
		mu.Unlock()
		switch cmd {
		case jfsMsgCreateFiles:
			close(inCreate)
			<-release // the uploads of a far link
			r := []byte{jfsCreateFilesReplyMarker, 0, 0, 1, 0}
			return binary.BigEndian.AppendUint64(r, 900), nil
		case jfsMsgRefreshInode:
			t.Error("the refresh used the shared channel")
		}
		return []byte{0}, nil
	}
	lane.testLocked = func(msg []byte, _ int) ([]byte, error) {
		mu.Lock()
		laneCmds = append(laneCmds, binary.BigEndian.Uint32(msg))
		mu.Unlock()
		return refreshReply(jfsRefreshKernelAccepted), nil
	}

	created := make(chan error, 1)
	go func() {
		out, err := shared.createFiles([]jfsNewFile{{parent: 1, name: "w.bin", mode: 0o644, mtime: time.Now(), data: []byte("x")}})
		if err == nil && (len(out) != 1 || out[0].ino != 900 || out[0].err != nil) {
			err = syscall.EIO
		}
		created <- err
	}()
	<-inCreate
	pinned := make(chan struct{})
	go func() {
		_, _ = shared.exchange(controlHeader(1105, 0), 1)
		close(pinned)
	}()

	start := time.Now()
	flags, _, _, tm, err := lane.refreshInodeTimed(5, time.Second)
	took := time.Since(start)
	if err != nil || flags != jfsRefreshKernelAccepted {
		t.Fatalf("refresh during the create: flags %d err %v", flags, err)
	}
	if took > 500*time.Millisecond || tm.wait > 100*time.Millisecond {
		t.Fatalf("refresh took %s (waited %s for its channel) behind the drain", took, tm.wait)
	}
	select {
	case <-pinned:
		t.Fatal("the pin overtook the create-files message on the shared channel")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-created; err != nil {
		t.Fatalf("create-files: %v", err)
	}
	<-pinned
	mu.Lock()
	defer mu.Unlock()
	if len(sharedCmds) != 2 || sharedCmds[0] != jfsMsgCreateFiles || sharedCmds[1] != 1105 {
		t.Fatalf("shared channel carried %v, want [create-files pin]", sharedCmds)
	}
	if len(laneCmds) != 1 || laneCmds[0] != jfsMsgRefreshInode {
		t.Fatalf("lane carried %v, want [refresh]", laneCmds)
	}
}

// A juicefs without patch 0033 has no lane: the open fails with ENOENT and
// nothing is sent. The refresh then uses the shared channel, from then on
// without trying the lane again.
func TestContentRefreshLaneFallsBackToTheSharedChannel(t *testing.T) {
	lane, shared := laneTestControls(t)
	var laneTries, sharedRefreshes atomic.Int32
	lane.testLocked = func([]byte, int) ([]byte, error) {
		laneTries.Add(1)
		return nil, &errControlNotSent{&fs.PathError{Op: "open", Path: jfsControlLaneFile, Err: syscall.ENOENT}}
	}
	shared.testLocked = func(msg []byte, _ int) ([]byte, error) {
		if binary.BigEndian.Uint32(msg) == jfsMsgRefreshInode {
			sharedRefreshes.Add(1)
		}
		return refreshReply(jfsRefreshKernelAccepted), nil
	}
	for i := 0; i < 2; i++ {
		if flags, _, _, _, err := lane.refreshInodeTimed(5, time.Second); err != nil || flags != jfsRefreshKernelAccepted {
			t.Fatalf("refresh %d: flags %d err %v", i, flags, err)
		}
	}
	if laneTries.Load() != 1 || sharedRefreshes.Load() != 2 {
		t.Fatalf("lane tried %d times, shared carried %d refreshes; want 1 and 2", laneTries.Load(), sharedRefreshes.Load())
	}
	// Any other failure on the lane is the refresh's own error, not a reason
	// to move the refresh to the drain's channel.
	lane2, shared2 := laneTestControls(t)
	lane2.testLocked = func([]byte, int) ([]byte, error) { return nil, syscall.EIO }
	shared2.testLocked = func([]byte, int) ([]byte, error) {
		t.Error("an EIO on the lane fell back to the shared channel")
		return nil, nil
	}
	if _, _, _, _, err := lane2.refreshInodeTimed(5, time.Second); err == nil {
		t.Fatal("EIO on the lane: no error")
	}
	if lane2.laneMissing.Load() {
		t.Fatal("EIO marked the lane missing")
	}
}

// A mount that is briefly gone (a juicefs restart) has neither control file.
// The refresh fails over to the shared channel for that message only; the
// lane stays on, and the next refresh uses it again (review M1 of #98).
func TestContentRefreshLaneSurvivesAMissingMount(t *testing.T) {
	lane, shared := laneTestControls(t)
	mountUp := false
	var laneTries atomic.Int32
	lane.testLocked = func([]byte, int) ([]byte, error) {
		laneTries.Add(1)
		if !mountUp {
			return nil, &errControlNotSent{&fs.PathError{Op: "open", Path: jfsControlLaneFile, Err: syscall.ENOENT}}
		}
		return refreshReply(jfsRefreshKernelAccepted), nil
	}
	shared.testReachable = func() bool { return mountUp }
	shared.testLocked = func([]byte, int) ([]byte, error) {
		return nil, &errControlNotSent{&fs.PathError{Op: "open", Path: ".control", Err: syscall.ENOENT}}
	}
	if _, _, _, _, err := lane.refreshInodeTimed(5, time.Second); err == nil {
		t.Fatal("refresh with the mount gone: no error")
	}
	if lane.laneMissing.Load() {
		t.Fatal("a missing mount turned the lane off")
	}
	mountUp = true
	if flags, _, _, _, err := lane.refreshInodeTimed(5, time.Second); err != nil || flags != jfsRefreshKernelAccepted {
		t.Fatalf("refresh after the mount returned: flags %d err %v", flags, err)
	}
	if laneTries.Load() != 2 {
		t.Fatalf("lane tried %d times; want 2", laneTries.Load())
	}
}

// A READ the verified SSD route serves does not wait for a pending refresh:
// that route reads juicefs's blocks by slice ID from a mapping the content
// hook already dropped, never juicefs's read buffers or the kernel's pages,
// which the refresh (and its kernel notify, behind the file's reads in
// flight) drops. A READ the SSD route misses still waits and reads the
// refreshed FUSE file: no reply pairs the new attributes with old bytes.
func TestPeerEditSSDReadDoesNotWaitForTheRefresh(t *testing.T) {
	jfs, store, s, file := peerEditHarness(t, true)
	const size = 64 << 10
	s.delay = 1500 * time.Millisecond // the exchange plus a notify behind reads in flight
	var mapped atomic.Value           // what Redis's chunk lists describe
	mapped.Store(byte('A'))
	var hit atomic.Bool
	var ssdReads atomic.Int32
	jfs.handler.testVerifiedSSD = func(_ string, p []byte, _ int64) (int, error, bool) {
		if !hit.Load() {
			return 0, nil, false
		}
		ssdReads.Add(1)
		for i := range p {
			p[i] = mapped.Load().(byte)
		}
		return len(p), nil, true
	}
	base := store.LookupByPath(file).Mtime
	edited := base.Add(time.Second)
	mapped.Store(byte('B')) // the peer's save is in Redis before its attr push
	s.stage(bytes.Repeat([]byte{'B'}, size))
	peerEdit(t, store, file, size, edited)

	hit.Store(true)
	start := time.Now()
	p, err := nfsReadErr(t, jfs, file, 8<<10, 4096)
	if err != nil || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("SSD READ during the refresh: %v after %s", err, time.Since(start))
	}
	if !p.mtime.Equal(edited) || p.data[0] != 'B' || ssdReads.Load() != 1 {
		t.Fatalf("SSD READ: mtime %v data %q (SSD reads %d)", p.mtime, p.data[:1], ssdReads.Load())
	}

	hit.Store(false)
	start = time.Now()
	p, err = nfsReadErr(t, jfs, file, 8<<10, 4096)
	if err != nil {
		t.Fatalf("FUSE READ: %v", err)
	}
	if time.Since(start) < 500*time.Millisecond {
		t.Fatalf("an SSD miss read FUSE after %s, before the refresh ended", time.Since(start))
	}
	if !p.mtime.Equal(edited) || p.data[0] != 'B' {
		t.Fatalf("FUSE READ after the refresh: mtime %v data %q", p.mtime, p.data[:1])
	}
	if n := s.calls.Load(); n != 1 {
		t.Fatalf("%d refreshes, want 1", n)
	}

	// The row moves during the SSD read (a late commit republished it): the
	// early attempt answers EAGAIN. The READ must not answer JUKEBOX: it
	// waits for the refresh and tries the routes again.
	edited2 := edited.Add(time.Second)
	mapped.Store(byte('C'))
	s.stage(bytes.Repeat([]byte{'C'}, size))
	peerEdit(t, store, file, size, edited2)
	var tries atomic.Int32
	jfs.handler.testVerifiedSSD = func(_ string, p []byte, _ int64) (int, error, bool) {
		if tries.Add(1) == 1 {
			return 0, syscall.EAGAIN, true
		}
		for i := range p {
			p[i] = mapped.Load().(byte)
		}
		return len(p), nil, true
	}
	p, err = nfsReadErr(t, jfs, file, 8<<10, 4096)
	if err != nil || tries.Load() != 2 || !p.mtime.Equal(edited2) || p.data[0] != 'C' {
		t.Fatalf("SSD EAGAIN during the refresh: err %v, tries %d, mtime %v data %q", err, tries.Load(), p.mtime, p.data[:1])
	}
}
