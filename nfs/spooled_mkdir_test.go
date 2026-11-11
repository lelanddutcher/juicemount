package nfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// fakeJuiceFSControl answers the patch 0022 control messages over a temp
// folder that stands in for the JuiceFS mount. It cannot give a folder a
// chosen inode on disk, so it keeps its own inode -> path map and records the
// order of creations.
type fakeJuiceFSControl struct {
	t    *testing.T
	root string

	mu          sync.Mutex
	next        uint64
	reserved    map[uint64]bool
	paths       map[uint64]string // volume inode -> path ("" = root)
	created     []string
	reserveErr  syscall.Errno
	mkdirErr    syscall.Errno // forced reply, once
	reserveSeen int
	releaseSeen int
	// mkdirEntered/mkdirGate, when set, hold a folder creation mid-flight.
	mkdirEntered chan struct{}
	mkdirGate    chan struct{}
}

func newFakeJuiceFSControl(t *testing.T, root string) *fakeJuiceFSControl {
	return &fakeJuiceFSControl{t: t, root: root, next: 5000, reserved: map[uint64]bool{}, paths: map[uint64]string{1: ""}}
}

// known registers a folder that exists on the "volume" under its disk inode.
func (f *fakeJuiceFSControl) known(rel string) uint64 {
	fi, err := os.Lstat(filepath.Join(f.root, rel))
	if err != nil {
		f.t.Fatal(err)
	}
	ino := fi.Sys().(*syscall.Stat_t).Ino
	f.mu.Lock()
	f.paths[ino] = rel
	f.mu.Unlock()
	return ino
}

func (f *fakeJuiceFSControl) exchange(msg []byte, replyLen int) ([]byte, error) {
	if binary.BigEndian.Uint32(msg[0:4]) == jfsMsgMkdirInode && f.mkdirGate != nil {
		f.mkdirEntered <- struct{}{}
		<-f.mkdirGate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := binary.BigEndian.Uint32(msg[0:4])
	body := msg[8:]
	if int(binary.BigEndian.Uint32(msg[4:8])) != len(body) {
		f.t.Fatalf("control message length %d, body %d", binary.BigEndian.Uint32(msg[4:8]), len(body))
	}
	switch cmd {
	case jfsMsgReserveInodes:
		f.reserveSeen++
		n := binary.BigEndian.Uint32(body)
		reply := make([]byte, 13)
		if f.reserveErr != 0 {
			reply[0] = byte(f.reserveErr)
			return reply, nil
		}
		first := f.next
		f.next += uint64(n)
		for i := uint64(0); i < uint64(n); i++ {
			f.reserved[first+i] = true
		}
		binary.BigEndian.PutUint64(reply[1:9], first)
		binary.BigEndian.PutUint32(reply[9:13], n)
		return reply, nil
	case jfsMsgMkdirInode:
		parent := binary.BigEndian.Uint64(body[0:8])
		ino := binary.BigEndian.Uint64(body[8:16])
		mode := os.FileMode(binary.BigEndian.Uint16(body[16:18]))
		nameLen := int(body[20])
		name := string(body[21 : 21+nameLen])
		if f.mkdirErr != 0 {
			e := f.mkdirErr
			f.mkdirErr = 0
			return []byte{byte(e)}, nil
		}
		if !f.reserved[ino] {
			return []byte{byte(syscall.ESRCH)}, nil
		}
		pp, ok := f.paths[parent]
		if !ok {
			return []byte{byte(syscall.ENOENT)}, nil
		}
		rel := filepath.Join(pp, name)
		if err := os.Mkdir(filepath.Join(f.root, rel), mode); err != nil {
			var errno syscall.Errno
			if errors.As(err, &errno) {
				return []byte{byte(errno)}, nil
			}
			return []byte{byte(syscall.EIO)}, nil
		}
		delete(f.reserved, ino)
		f.paths[ino] = rel
		f.created = append(f.created, rel)
		return []byte{0}, nil
	case jfsMsgReleaseInodes:
		first := binary.BigEndian.Uint64(body[0:8])
		n := binary.BigEndian.Uint32(body[8:12])
		var released uint32
		for i := uint64(0); i < uint64(n); i++ {
			if f.reserved[first+i] {
				delete(f.reserved, first+i)
				released++
			}
		}
		f.releaseSeen++
		reply := make([]byte, 5)
		binary.BigEndian.PutUint32(reply[1:5], released)
		return reply, nil
	}
	f.t.Fatalf("unexpected control message %d", cmd)
	return nil, nil
}

func (f *fakeJuiceFSControl) createdOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.created...)
}

func newSpooledMkdirFixture(t *testing.T) (*juiceFS, string, *SpoolStore, *Drainer, *fakeJuiceFSControl) {
	t.Helper()
	jfs, fuseRoot, spool, drainer := newSymlinkSpoolTestFS(t)
	fake := newFakeJuiceFSControl(t, fuseRoot)
	if drainer.dirInodes == nil {
		t.Fatal("spooled mkdir is not wired")
	}
	drainer.dirInodes.ctl.testExchange = fake.exchange
	// A folder that exists on the volume and in the mirror.
	if err := os.Mkdir(filepath.Join(fuseRoot, "w"), 0o755); err != nil {
		t.Fatal(err)
	}
	ino := fake.known("w")
	e := metadata.MakeEntry("w", true, 0, time.Now(), ino)
	if err := jfs.handler.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	return jfs, fuseRoot, spool, drainer, fake
}

func TestSpooledMkdirAnswersLocallyWithReservedInode(t *testing.T) {
	jfs, fuseRoot, spool, drainer, fake := newSpooledMkdirFixture(t)

	if err := jfs.MkdirAll("w/new", 0o750); err != nil {
		t.Fatal(err)
	}
	if err := jfs.MkdirAll("w/new/sub", 0o755); err != nil {
		t.Fatal(err)
	}
	// Nothing on the volume yet: MKDIR did not wait for it.
	if _, err := os.Lstat(filepath.Join(fuseRoot, "w/new")); !os.IsNotExist(err) {
		t.Fatalf("MKDIR created the folder on the volume: %v", err)
	}
	e := jfs.handler.store.LookupByPath("w/new")
	if e == nil || !e.IsDir || !e.LocalOnly || syntheticInode(e.Inode) || e.Inode < 5000 {
		t.Fatalf("mirror entry %+v: want a LocalOnly folder with a reserved inode", e)
	}
	fi, err := jfs.Stat("w/new")
	if err != nil || !fi.IsDir() {
		t.Fatalf("Stat pending folder: %v %v", fi, err)
	}
	if got := fi.Sys().(*syscall.Stat_t).Ino; got != e.Inode {
		t.Fatalf("file id %d, want the reserved inode %d", got, e.Inode)
	}
	sub := jfs.handler.store.LookupByPath("w/new/sub")
	if sub == nil || sub.Inode == e.Inode {
		t.Fatalf("nested folder entry %+v", sub)
	}
	if !spool.pendingAncestor("w/new/sub/clip.mov") {
		t.Fatal("a file in the pending folder must wait for it")
	}

	if remaining := drainer.materializePendingDirs(); remaining {
		t.Fatal("folders left after materialization")
	}
	if got := fake.createdOrder(); len(got) != 2 || got[0] != "w/new" || got[1] != "w/new/sub" {
		t.Fatalf("created %v, want parent before child", got)
	}
	for _, p := range []string{"w/new", "w/new/sub"} {
		if _, err := os.Stat(filepath.Join(fuseRoot, p)); err != nil {
			t.Fatalf("%s not on the volume: %v", p, err)
		}
		if _, ok := spool.pendingDir(p); ok {
			t.Fatalf("%s still pending", p)
		}
		if got := jfs.handler.store.LookupByPath(p); got == nil || got.LocalOnly {
			t.Fatalf("%s: LocalOnly not cleared: %+v", p, got)
		}
	}
	if fi, _ := os.Stat(filepath.Join(fuseRoot, "w/new")); fi.Mode().Perm() != 0o750 {
		t.Fatalf("mode %o, want 0750", fi.Mode().Perm())
	}
	if after := jfs.handler.store.LookupByPath("w/new"); after.Inode != e.Inode {
		t.Fatalf("inode changed %d -> %d", e.Inode, after.Inode)
	}
	if rows, _ := spool.Meta().ListPendingDirs(); len(rows) != 0 {
		t.Fatalf("durable rows left: %+v", rows)
	}
	if spool.pendingAncestor("w/new/sub/clip.mov") {
		t.Fatal("file still waits after its folder exists")
	}
}

// While the drainer creates a folder on the volume (one slow round trip on a
// remote link), the client keeps working inside it: MKDIR and CREATE there
// are local and must not be refused with JUKEBOX. Renaming the folder itself
// still waits for the creation.
func TestSpooledMkdirClientWorksInsideFolderBeingCreated(t *testing.T) {
	jfs, fuseRoot, _, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/new", 0o755); err != nil {
		t.Fatal(err)
	}
	fake.mkdirEntered = make(chan struct{})
	fake.mkdirGate = make(chan struct{})
	done := make(chan bool)
	go func() { done <- drainer.materializePendingDirs() }()
	<-fake.mkdirEntered

	start := time.Now()
	if err := jfs.MkdirAll("w/new/inner", 0o755); err != nil {
		t.Fatalf("MKDIR inside the folder being created: %v", err)
	}
	f, err := jfs.Create("w/new/clip.xmp")
	if err != nil {
		t.Fatalf("CREATE inside the folder being created: %v", err)
	}
	_ = f.Close()
	if took := time.Since(start); took > time.Second {
		t.Fatalf("work inside the folder waited %v for the server", took)
	}
	if err := jfs.Rename("w/new", "w/renamed"); err == nil {
		t.Fatal("rename of the folder being created was not refused")
	}

	fake.mkdirGate <- struct{}{} // w/new
	<-done
	// w/new/inner arrived after this pass listed the rows: the next pass
	// creates it.
	fake.mkdirGate = nil
	if remaining := drainer.materializePendingDirs(); remaining {
		t.Fatal("folders left after materialization")
	}
	if got := fake.createdOrder(); len(got) != 2 || got[0] != "w/new" || got[1] != "w/new/inner" {
		t.Fatalf("created %v, want [w/new w/new/inner]", got)
	}
	if _, err := os.Stat(filepath.Join(fuseRoot, "w/new/inner")); err != nil {
		t.Fatal(err)
	}
}

func TestSpooledMkdirRenameAndRemoveBeforeDrain(t *testing.T) {
	jfs, fuseRoot, spool, drainer, fake := newSpooledMkdirFixture(t)
	for _, p := range []string{"w/a", "w/a/b", "w/a/c"} {
		if err := jfs.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := jfs.Rename("w/a", "w/z"); err != nil {
		t.Fatalf("rename pending folder: %v", err)
	}
	if err := jfs.RemoveDirectory("w/z/b"); err != nil {
		t.Fatalf("remove pending folder: %v", err)
	}
	if err := jfs.RemoveDirectory("w/z"); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("remove non-empty pending folder: %v, want ENOTEMPTY", err)
	}
	drainer.materializePendingDirs()
	if got := fake.createdOrder(); len(got) != 2 || got[0] != "w/z" || got[1] != "w/z/c" {
		t.Fatalf("created %v, want [w/z w/z/c]", got)
	}
	for _, gone := range []string{"w/a", "w/z/b"} {
		if _, err := os.Lstat(filepath.Join(fuseRoot, gone)); !os.IsNotExist(err) {
			t.Fatalf("%s exists on the volume", gone)
		}
	}
	if spool.pendingDirCount() != 0 {
		t.Fatalf("pending rows left: %d", spool.pendingDirCount())
	}
}

func TestSpooledMkdirAdoptsFolderCreatedElsewhere(t *testing.T) {
	jfs, fuseRoot, spool, drainer, _ := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/shared", 0o755); err != nil {
		t.Fatal(err)
	}
	// Another Mac creates the same folder before the drain.
	if err := os.Mkdir(filepath.Join(fuseRoot, "w/shared"), 0o755); err != nil {
		t.Fatal(err)
	}
	if drainer.materializePendingDirs() {
		t.Fatal("existing folder not adopted")
	}
	if spool.pendingDirCount() != 0 {
		t.Fatal("row kept for an adopted folder")
	}
	if e := jfs.handler.store.LookupByPath("w/shared"); e == nil || e.LocalOnly {
		t.Fatalf("adopted folder entry %+v", e)
	}
}

func TestSpooledMkdirRecreatesParentRemovedRemotely(t *testing.T) {
	jfs, fuseRoot, spool, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/late", 0o755); err != nil {
		t.Fatal(err)
	}
	// The parent disappears on the volume (another Mac removed it).
	if err := os.Remove(filepath.Join(fuseRoot, "w")); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	for ino, p := range fake.paths {
		if p == "w" {
			delete(fake.paths, ino)
		}
	}
	fake.mu.Unlock()
	// The drainer finds the parent gone (ENOENT), recreates it on the mount
	// and retries; the fake learns the new parent by its disk inode.
	drainer.dirInodes.ctl.testExchange = func(msg []byte, n int) ([]byte, error) {
		if binary.BigEndian.Uint32(msg[0:4]) == jfsMsgMkdirInode {
			if fi, err := os.Lstat(filepath.Join(fuseRoot, "w")); err == nil {
				fake.mu.Lock()
				fake.paths[fi.Sys().(*syscall.Stat_t).Ino] = "w"
				fake.mu.Unlock()
			}
		}
		return fake.exchange(msg, n)
	}
	if drainer.materializePendingDirs() {
		t.Fatal("folder not created after its parent was removed")
	}
	if _, err := os.Stat(filepath.Join(fuseRoot, "w/late")); err != nil {
		t.Fatalf("folder missing: %v", err)
	}
	if spool.pendingDirCount() != 0 {
		t.Fatal("row left")
	}
}

func TestSpooledMkdirFallsBackToPlainMkdirWhenNumberUnknown(t *testing.T) {
	jfs, fuseRoot, _, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/restart", 0o755); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.reserved = map[uint64]bool{} // juicefs restarted and forgot its numbers
	fake.mu.Unlock()
	if drainer.materializePendingDirs() {
		t.Fatal("folder not created")
	}
	if _, err := os.Stat(filepath.Join(fuseRoot, "w/restart")); err != nil {
		t.Fatalf("plain mkdir fallback: %v", err)
	}
	drainer.dirInodes.mu.Lock()
	n := drainer.dirInodes.availableLocked()
	drainer.dirInodes.mu.Unlock()
	if n != 0 {
		t.Fatalf("pool kept %d numbers of a restarted juicefs", n)
	}
	// L1: the unused numbers went back to juicefs, so a juicefs that did not
	// in fact restart does not keep them reserved.
	fake.mu.Lock()
	left, releases := len(fake.reserved), fake.releaseSeen
	fake.mu.Unlock()
	// The fake forgot every number itself, so only the messages can be
	// checked: the pool tried to give its numbers back.
	_ = left
	if releases == 0 {
		t.Fatal("after a restart: no release message")
	}
}

// L1: stopping the drainer gives unused reserved numbers back.
func TestSpooledMkdirStopReleasesUnusedNumbers(t *testing.T) {
	jfs, _, _, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/one", 0o755); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	before := len(fake.reserved)
	fake.mu.Unlock()
	if before == 0 {
		t.Fatal("nothing reserved")
	}
	drainer.Stop(5 * time.Second)
	fake.mu.Lock()
	after := len(fake.reserved)
	fake.mu.Unlock()
	// The number given to w/one stays reserved: its pending row still needs it.
	if after != 1 {
		t.Fatalf("%d numbers still reserved after Stop, want 1 (the pending folder's)", after)
	}
}

// L2: a rename into a pending folder while offline fails at once; it must
// not wait for the control-file reply timeout.
func TestSpooledMkdirChainFailsFastOffline(t *testing.T) {
	jfs, _, _, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/new", 0o755); err != nil {
		t.Fatal(err)
	}
	wasOffline := pin.IsOffline()
	t.Cleanup(func() { pin.SetOffline(wasOffline) })
	pin.SetOffline(true)
	var mkdirs atomic.Int32
	drainer.dirInodes.ctl.testExchange = func(msg []byte, n int) ([]byte, error) {
		if binary.BigEndian.Uint32(msg[0:4]) == jfsMsgMkdirInode {
			mkdirs.Add(1)
		}
		return fake.exchange(msg, n)
	}
	start := time.Now()
	if err := drainer.materializeChainOwned("w/new/f.txt", false); !errors.Is(err, errPendingChainOffline) {
		t.Fatalf("chain offline: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("offline refusal took %v", took)
	}
	if mkdirs.Load() != 0 {
		t.Fatal("folder creation sent while offline")
	}
}

func TestSpooledMkdirKeepsSynchronousPathWithoutReservation(t *testing.T) {
	jfs, fuseRoot, spool, _, fake := newSpooledMkdirFixture(t)
	fake.reserveErr = syscall.ENOTSUP // a juicefs without patch 0022
	if err := jfs.MkdirAll("w/sync", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(fuseRoot, "w/sync")); err != nil {
		t.Fatalf("synchronous mkdir did not create the folder: %v", err)
	}
	if spool.pendingDirCount() != 0 {
		t.Fatal("pending row without a reserved inode")
	}

	jfs2, fuseRoot2, spool2, _, _ := newSpooledMkdirFixture(t)
	wasOffline := pin.IsOffline()
	t.Cleanup(func() { pin.SetOffline(wasOffline) })
	pin.SetOffline(true)
	if err := jfs2.MkdirAll("w/off", 0o755); err != nil {
		t.Fatal(err)
	}
	if spool2.pendingDirCount() != 0 {
		t.Fatal("offline MKDIR took the online spooled path")
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot2, "w/off")); !os.IsNotExist(err) {
		t.Fatal("offline MKDIR touched the volume")
	}
}

func TestSpooledMkdirRenameIntoPendingFolderCreatesItFirst(t *testing.T) {
	jfs, fuseRoot, spool, _, fake := newSpooledMkdirFixture(t)
	if err := os.WriteFile(filepath.Join(fuseRoot, "w/old.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Lstat(filepath.Join(fuseRoot, "w/old.txt"))
	e := metadata.MakeEntry("w/old.txt", false, 1, fi.ModTime(), fi.Sys().(*syscall.Stat_t).Ino)
	if err := jfs.handler.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	if err := jfs.MkdirAll("w/dest", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := jfs.Rename("w/old.txt", "w/dest/old.txt"); err != nil {
		t.Fatalf("rename into pending folder: %v", err)
	}
	if got := fake.createdOrder(); len(got) != 1 || got[0] != "w/dest" {
		t.Fatalf("created %v, want the destination folder first", got)
	}
	if _, err := os.Stat(filepath.Join(fuseRoot, "w/dest/old.txt")); err != nil {
		t.Fatalf("file not moved on the volume: %v", err)
	}
	if spool.pendingDirCount() != 0 {
		t.Fatal("destination still pending")
	}
}

func TestJFSControlMessages(t *testing.T) {
	c := &jfsControl{}
	var sent []byte
	c.testExchange = func(msg []byte, n int) ([]byte, error) {
		sent = append([]byte(nil), msg...)
		if binary.BigEndian.Uint32(msg) == jfsMsgReserveInodes {
			r := make([]byte, 13)
			binary.BigEndian.PutUint64(r[1:], 77)
			binary.BigEndian.PutUint32(r[9:], 3)
			return r, nil
		}
		return []byte{byte(syscall.EEXIST)}, nil
	}
	first, n, err := c.reserveInodes(3)
	if err != nil || first != 77 || n != 3 {
		t.Fatalf("reserve: %d %d %v", first, n, err)
	}
	if binary.BigEndian.Uint32(sent[4:]) != 4 || binary.BigEndian.Uint32(sent[8:]) != 3 {
		t.Fatalf("reserve message %x", sent)
	}
	err = c.mkdirInode(9, "Résumé", 0o750, 77)
	if !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("mkdir reply: %v", err)
	}
	if binary.BigEndian.Uint64(sent[8:]) != 9 || binary.BigEndian.Uint64(sent[16:]) != 77 ||
		binary.BigEndian.Uint16(sent[24:]) != 0o750 || int(sent[28]) != len("Résumé") || string(sent[29:]) != "Résumé" {
		t.Fatalf("mkdir message %x", sent)
	}
	c.testExchange = func([]byte, int) ([]byte, error) { return make([]byte, 13), nil }
	c.testExchange = func([]byte, int) ([]byte, error) {
		r := make([]byte, 13)
		r[0] = byte(syscall.ENOTSUP)
		return r, nil
	}
	if _, _, err := c.reserveInodes(1); !errors.Is(err, errControlUnsupported) {
		t.Fatalf("unsupported juicefs: %v", err)
	}
}

// emptyDirInodePool leaves the pool with no number and no way to refill, so
// the next MKDIR takes the synchronous path.
func emptyDirInodePool(d *Drainer, fake *fakeJuiceFSControl) {
	fake.mu.Lock()
	fake.reserveErr = syscall.EAGAIN
	fake.mu.Unlock()
	p := d.dirInodes
	p.mu.Lock()
	p.ranges = nil
	p.retryAt = time.Now().Add(time.Hour)
	p.mu.Unlock()
}

// assertCreatedWithReservedInode checks that dir reached the volume through
// the reserved-inode message, not a plain mkdir, and that the mirror kept its
// number.
func assertCreatedWithReservedInode(t *testing.T, jfs *juiceFS, fake *fakeJuiceFSControl, dir string, ino uint64) {
	t.Helper()
	fake.mu.Lock()
	got := fake.paths[ino]
	fake.mu.Unlock()
	if got != dir {
		t.Fatalf("%s not created with its reserved inode %d (control map: %q)", dir, ino, got)
	}
	if e := jfs.handler.store.LookupByPath(dir); e == nil || e.Inode != ino {
		t.Fatalf("%s mirror entry %+v, want inode %d", dir, e, ino)
	}
}

// H1: with the pool empty, MKDIR inside a pending folder is synchronous. It
// must create the pending parent with its reserved inode first, never by a
// plain MkdirAll.
func TestSpooledMkdirSyncChildCreatesPendingParentWithItsInode(t *testing.T) {
	jfs, fuseRoot, spool, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/new", 0o750); err != nil {
		t.Fatal(err)
	}
	ino := jfs.handler.store.LookupByPath("w/new").Inode
	emptyDirInodePool(drainer, fake)
	if err := jfs.MkdirAll("w/new/sub", 0o755); err != nil {
		t.Fatalf("synchronous MKDIR under a pending folder: %v", err)
	}
	assertCreatedWithReservedInode(t, jfs, fake, "w/new", ino)
	if fi, err := os.Stat(filepath.Join(fuseRoot, "w/new")); err != nil || fi.Mode().Perm() != 0o750 {
		t.Fatalf("parent on the volume: %v %v, want mode 0750", fi, err)
	}
	if _, err := os.Stat(filepath.Join(fuseRoot, "w/new/sub")); err != nil {
		t.Fatal(err)
	}
	if spool.pendingDirCount() != 0 {
		t.Fatal("parent still pending")
	}
}

// H1: the streaming drain must not create a pending parent folder.
func TestSpooledMkdirStreamerWaitsForPendingFolder(t *testing.T) {
	jfs, fuseRoot, spool, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/new", 0o755); err != nil {
		t.Fatal(err)
	}
	ino := jfs.handler.store.LookupByPath("w/new").Inode
	f, err := jfs.Create("w/new/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("bytes"))
	_ = f.Close()
	e, ok := spool.LookupActive("w/new/clip.mov")
	if !ok {
		t.Fatal("file not spooled")
	}
	drainer.streamer = &streamer{sessions: map[int64]*streamSession{}}
	if ss := drainer.sessionFor(e); ss != nil {
		t.Fatal("stream session opened under a pending folder")
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "w/new")); !os.IsNotExist(err) {
		t.Fatalf("streamer created the pending folder: %v", err)
	}
	drainer.materializePendingDirs()
	assertCreatedWithReservedInode(t, jfs, fake, "w/new", ino)
}

// H1: a symlink persisted under a pending folder waits for the folder drain.
func TestSpooledMkdirSymlinkWaitsForPendingFolder(t *testing.T) {
	jfs, fuseRoot, spool, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/new", 0o755); err != nil {
		t.Fatal(err)
	}
	ino := jfs.handler.store.LookupByPath("w/new").Inode
	if err := jfs.Symlink("target.mov", "w/new/link"); err != nil {
		t.Fatal(err)
	}
	if !drainer.materializePendingSymlinks() {
		t.Fatal("symlink under a pending folder reported done")
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "w/new")); !os.IsNotExist(err) {
		t.Fatalf("symlink materialization created the pending folder: %v", err)
	}
	drainer.materializePendingDirs()
	assertCreatedWithReservedInode(t, jfs, fake, "w/new", ino)
	if drainer.materializePendingSymlinks() {
		t.Fatal("symlink left after its folder exists")
	}
	if got, err := os.Readlink(filepath.Join(fuseRoot, "w/new/link")); err != nil || got != "target.mov" {
		t.Fatalf("link %q %v", got, err)
	}
	_ = spool
}

// H2: the volume has a FILE where a pending folder should go. The folder and
// everything in it (nested folders and spooled files) move to a conflict
// name, the conflict is recorded, every file drains there, and the file on
// the volume is untouched.
func TestSpooledMkdirFileOnVolumeTakesFolderName(t *testing.T) {
	jfs, fuseRoot, spool, drainer, fake := newSpooledMkdirFixture(t)
	drainConflictHost = func() string { return "mac1" }
	drainConflictNow = func() time.Time { return time.Date(2026, 11, 11, 9, 30, 0, 0, time.UTC) }
	t.Cleanup(func() { drainConflictHost, drainConflictNow = defaultConflictHost, time.Now })

	for _, p := range []string{"w/Exports", "w/Exports/Sub"} {
		if err := jfs.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{"w/Exports/a.wav", "w/Exports/Sub/b.wav"} {
		f, err := jfs.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.Write([]byte("audio " + p))
		_ = f.Close()
	}
	for _, p := range []string{"w/Exports/a.wav", "w/Exports/Sub/b.wav"} {
		e, ok := spool.LookupActive(p)
		if !ok {
			t.Fatalf("%s not spooled", p)
		}
		if err := e.Close(); err != nil { // the client's COMMIT
			t.Fatal(err)
		}
	}
	// Another Mac saved a file named Exports before our folder drained.
	if err := os.WriteFile(filepath.Join(fuseRoot, "w/Exports"), []byte("theirs"), 0o644); err != nil {
		t.Fatal(err)
	}

	drainer.materializePendingDirs() // finds the file, renames the folder
	drainer.materializePendingDirs() // creates the renamed folders
	conflict := "w/Exports (conflict mac1 2026-11-11 093000)"
	if got := fake.createdOrder(); len(got) != 2 || got[0] != conflict || got[1] != conflict+"/Sub" {
		t.Fatalf("created %v, want the conflict folder then its subfolder", got)
	}
	if spool.pendingDirCount() != 0 {
		t.Fatalf("pending folders left: %d", spool.pendingDirCount())
	}
	cs, err := spool.Meta().ListFolderConflicts(10)
	if err != nil || len(cs) != 1 || cs[0].OriginalPath != "w/Exports" || cs[0].ConflictPath != conflict {
		t.Fatalf("recorded conflicts %+v %v", cs, err)
	}
	if e := jfs.handler.store.LookupByPath(conflict); e == nil || !e.IsDir {
		t.Fatalf("conflict folder not in the mirror: %+v", e)
	}

	ctx, cancel := context.WithTimeout(context.Background(), drainPassDeadline)
	defer cancel()
	drainer.DrainOnceForTest(ctx)
	for _, p := range []string{conflict + "/a.wav", conflict + "/Sub/b.wav"} {
		if _, err := os.Stat(filepath.Join(fuseRoot, p)); err != nil {
			t.Fatalf("%s did not drain: %v", p, err)
		}
	}
	if b, err := os.ReadFile(filepath.Join(fuseRoot, "w/Exports")); err != nil || string(b) != "theirs" {
		t.Fatalf("the other Mac's file changed: %q %v", b, err)
	}
	if rows, _ := spool.Meta().ListReadyIncludingWaiting(10); len(rows) != 0 {
		t.Fatalf("rows left: %d", len(rows))
	}
}

// M1: a pending folder and the spooled files in it move in one SQLite
// transaction. A refused folder move leaves the files where they were; a
// successful one moves both, durably and in memory, in one call.
func TestSpooledMkdirRenameMovesFolderAndFilesTogether(t *testing.T) {
	jfs, _, spool, _, _ := newSpooledMkdirFixture(t)
	for _, p := range []string{"w/a", "w/z"} {
		if err := jfs.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	f, err := jfs.Create("w/a/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// w/z is pending: moving w/a onto it must refuse, and move nothing.
	if _, _, err := spool.MigrateForRename("w/a", "w/z"); !errors.Is(err, metadata.ErrPendingDirConflict) {
		t.Fatalf("migrate onto a pending folder: %v, want ErrPendingDirConflict", err)
	}
	if r, _ := spool.Meta().LookupByPath("w/a/f.txt"); r == nil {
		t.Fatal("file row moved although the folder move was refused")
	}
	if _, ok := spool.pendingDir("w/a"); !ok {
		t.Fatal("pending folder lost")
	}

	if _, _, err := spool.MigrateForRename("w/a", "w/b"); err != nil {
		t.Fatal(err)
	}
	if r, _ := spool.Meta().LookupByPath("w/b/f.txt"); r == nil {
		t.Fatal("file row not moved")
	}
	if _, ok := spool.pendingDir("w/b"); !ok {
		t.Fatal("pending folder not moved in memory")
	}
	rows, _ := spool.Meta().ListPendingDirs()
	paths := map[string]bool{}
	for _, r := range rows {
		paths[r.Path] = true
	}
	if !paths["w/b"] || paths["w/a"] {
		t.Fatalf("durable pending rows %v", paths)
	}
}

// M2: files waiting for a stuck pending folder must not fill the page the
// dispatcher lists, or an unrelated save written after them never drains.
func TestSpooledMkdirWaitingFilesDoNotBlockOtherSaves(t *testing.T) {
	jfs, _, spool, drainer, _ := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/stuck", 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(p string) {
		f, err := jfs.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.Write([]byte(p))
		_ = f.Close()
		e, ok := spool.LookupActive(p)
		if !ok {
			t.Fatalf("%s not spooled", p)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		write(fmt.Sprintf("w/stuck/f%02d.txt", i))
	}
	write("w/other.txt")
	rows, err := drainer.listDispatchable(8)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].NFSPath != "w/other.txt" {
		var got []string
		for _, r := range rows {
			got = append(got, r.NFSPath)
		}
		t.Fatalf("listed %v, want only w/other.txt", got)
	}
	// The waiting rows were marked; once their folder is on the volume they
	// are listed again.
	drainer.materializePendingDirs()
	rows, err = drainer.listDispatchable(8)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 8 {
		t.Fatalf("after the folder landed: %d rows listed, want 8", len(rows))
	}
}

// L3: a folder that keeps failing is not retried on every tick.
func TestSpooledMkdirBacksOffAfterRepeatedFailures(t *testing.T) {
	jfs, _, _, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/stuck", 0o755); err != nil {
		t.Fatal(err)
	}
	var tries atomic.Int32
	drainer.dirInodes.ctl.testExchange = func(msg []byte, n int) ([]byte, error) {
		if binary.BigEndian.Uint32(msg[0:4]) == jfsMsgMkdirInode {
			tries.Add(1)
			return []byte{byte(syscall.EIO)}, nil
		}
		return fake.exchange(msg, n)
	}
	for i := 0; i < 10; i++ {
		drainer.materializePendingDirs()
	}
	if got := tries.Load(); got != 2 {
		t.Fatalf("%d attempts in 10 quick passes, want 2 (then back off)", got)
	}
}

// L4: a folder moved into a newer pending folder has the older row id; the
// drain still creates the parent first.
func TestSpooledMkdirParentFirstAfterMoveIntoNewerFolder(t *testing.T) {
	jfs, _, _, drainer, fake := newSpooledMkdirFixture(t)
	for _, p := range []string{"w/p", "w/q"} {
		if err := jfs.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := jfs.Rename("w/p", "w/q/p"); err != nil {
		t.Fatal(err)
	}
	drainer.materializePendingDirs()
	if got := fake.createdOrder(); len(got) != 2 || got[0] != "w/q" || got[1] != "w/q/p" {
		t.Fatalf("created %v, want [w/q w/q/p]", got)
	}
}

// reloadPendingDirs drops the in-memory pending set and reads it back from
// SQLite, as a restart does, and returns the durable paths.
func reloadPendingDirs(t *testing.T, spool *SpoolStore) map[string]bool {
	t.Helper()
	spool.dirs.mu.Lock()
	spool.dirs.byPath = nil
	spool.dirs.mu.Unlock()
	if err := spool.loadPendingDirs(); err != nil {
		t.Fatal(err)
	}
	rows, err := spool.Meta().ListPendingDirs()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r.Path] = true
	}
	return got
}

// C1: Finder's New Folder then rename. An EMPTY pending folder (no spool
// rows) and one holding only pending folders must move in SQLite, not only
// in memory, and survive a restart under the new name.
func TestSpooledMkdirRenameEmptyPendingFolderIsDurable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		make  []string
		from  string
		to    string
		after []string
	}{
		{"empty", []string{"w/untitled folder"}, "w/untitled folder", "w/Project X", []string{"w/Project X"}},
		{"only subfolders", []string{"w/untitled folder", "w/untitled folder/A", "w/untitled folder/A/B"},
			"w/untitled folder", "w/Project X", []string{"w/Project X", "w/Project X/A", "w/Project X/A/B"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jfs, fuseRoot, spool, drainer, fake := newSpooledMkdirFixture(t)
			for _, p := range tc.make {
				if err := jfs.MkdirAll(p, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			ino := jfs.handler.store.LookupByPath(tc.from).Inode
			if err := jfs.Rename(tc.from, tc.to); err != nil {
				t.Fatal(err)
			}
			got := reloadPendingDirs(t, spool)
			if len(got) != len(tc.after) {
				t.Fatalf("durable rows after restart %v, want %v", got, tc.after)
			}
			for _, p := range tc.after {
				if !got[p] {
					t.Fatalf("durable rows after restart %v, want %v", got, tc.after)
				}
				if _, ok := spool.pendingDir(p); !ok {
					t.Fatalf("%s not pending after reload", p)
				}
			}
			// The old name is free again: a second New Folder works.
			if err := jfs.MkdirAll(tc.from, 0o755); err != nil {
				t.Fatalf("MKDIR of the old name after the rename: %v", err)
			}
			drainer.materializePendingDirs()
			assertCreatedWithReservedInode(t, jfs, fake, tc.to, ino)
			if rows, _ := spool.Meta().ListPendingDirs(); len(rows) != 0 {
				t.Fatalf("rows left after the drain: %+v", rows)
			}
			if _, err := os.Stat(filepath.Join(fuseRoot, tc.from)); err != nil {
				t.Fatalf("second folder at the old name not created: %v", err)
			}
		})
	}
}

// M-A: a conflict name always fits in 255 bytes, keeps its tag and
// extension, and never splits a UTF-8 character.
func TestConflictNamesFitTheNameLimit(t *testing.T) {
	long := strings.Repeat("é", 120) // 240 bytes
	now := time.Date(2026, 11, 11, 9, 30, 0, 0, time.UTC)
	for _, got := range []string{
		path.Base(conflictFolderPath("w/"+long, "a-rather-long-mac-name", now, 2)),
		filepath.Base(conflictCopyPath("w/"+long+".prproj", "a-rather-long-mac-name", now, 2)),
	} {
		if len(got) > maxNameBytes || !utf8.ValidString(got) {
			t.Fatalf("name of %d bytes (valid UTF-8 %v): %q", len(got), utf8.ValidString(got), got)
		}
		if !strings.Contains(got, "(conflict a-rather-long-mac-name 2026-11-11 093000 2)") {
			t.Fatalf("tag lost: %q", got)
		}
	}
	if got := filepath.Base(conflictCopyPath("w/"+long+".prproj", "m", now, 1)); !strings.HasSuffix(got, ".prproj") {
		t.Fatalf("extension lost: %q", got)
	}
	if got := conflictFolderPath("w/short", "m", now, 1); got != "w/short (conflict m 2026-11-11 093000)" {
		t.Fatalf("short name changed: %q", got)
	}
}

// M-B: a juicefs that never answers the release does not hold Stop. L-b:
// after Stop, MKDIR reserves no new numbers.
func TestSpooledMkdirStopIsBoundedAndClosesThePool(t *testing.T) {
	jfs, fuseRoot, _, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/one", 0o755); err != nil {
		t.Fatal(err)
	}
	hang := make(chan struct{})
	defer close(hang)
	var reserves atomic.Int32
	drainer.dirInodes.ctl.testExchange = func(msg []byte, n int) ([]byte, error) {
		switch binary.BigEndian.Uint32(msg[0:4]) {
		case jfsMsgReleaseInodes:
			<-hang // a wedged mount
		case jfsMsgReserveInodes:
			reserves.Add(1)
		}
		return fake.exchange(msg, n)
	}
	start := time.Now()
	drainer.Stop(5 * time.Second)
	if took := time.Since(start); took > 4*time.Second {
		t.Fatalf("Stop took %v with a juicefs that does not answer", took)
	}
	if _, ok := drainer.dirInodes.take(time.Second); ok {
		t.Fatal("the pool gave a number after Stop")
	}
	if err := jfs.MkdirAll("w/after", 0o755); err != nil {
		t.Fatal(err)
	}
	if reserves.Load() != 0 {
		t.Fatalf("%d reservations after Stop", reserves.Load())
	}
	if _, err := os.Stat(filepath.Join(fuseRoot, "w/after")); err != nil {
		t.Fatalf("MKDIR after Stop is not synchronous: %v", err)
	}
}

// L-c: an ambiguous create (juicefs may have committed, the number is gone)
// followed by ESRCH must not drop the whole pool or claim a restart.
func TestSpooledMkdirAmbiguousFailureKeepsThePool(t *testing.T) {
	jfs, fuseRoot, _, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/flaky", 0o755); err != nil {
		t.Fatal(err)
	}
	var first atomic.Bool
	first.Store(true)
	drainer.dirInodes.ctl.testExchange = func(msg []byte, n int) ([]byte, error) {
		if binary.BigEndian.Uint32(msg[0:4]) == jfsMsgMkdirInode && first.Swap(false) {
			ino := binary.BigEndian.Uint64(msg[16:24])
			fake.mu.Lock()
			delete(fake.reserved, ino) // juicefs never offers it again
			fake.mu.Unlock()
			return []byte{byte(syscall.EIO)}, nil
		}
		return fake.exchange(msg, n)
	}
	drainer.dirInodes.mu.Lock()
	before := drainer.dirInodes.availableLocked()
	drainer.dirInodes.mu.Unlock()
	drainer.materializePendingDirs() // EIO: ambiguous
	drainer.materializePendingDirs() // ESRCH: explained by the EIO
	if _, err := os.Stat(filepath.Join(fuseRoot, "w/flaky")); err != nil {
		t.Fatalf("folder not created plainly after the ambiguous failure: %v", err)
	}
	drainer.dirInodes.mu.Lock()
	after := drainer.dirInodes.availableLocked()
	drainer.dirInodes.mu.Unlock()
	if after == 0 || after+1 < before {
		t.Fatalf("pool went from %d to %d numbers: dropped after one ambiguous failure", before, after)
	}
}

// L2: juicefs refuses one folder's number with ESRCH but still holds the
// others (not a restart). The pool keeps its numbers; exactly one number is
// given back, as the probe.
func TestSpooledMkdirESRCHForOneNumberKeepsThePool(t *testing.T) {
	jfs, fuseRoot, _, drainer, fake := newSpooledMkdirFixture(t)
	if err := jfs.MkdirAll("w/odd", 0o755); err != nil {
		t.Fatal(err)
	}
	drainer.dirInodes.mu.Lock()
	before := drainer.dirInodes.availableLocked()
	drainer.dirInodes.mu.Unlock()
	fake.mu.Lock()
	fake.mkdirErr = syscall.ESRCH // this number only
	releasesBefore := fake.releaseSeen
	fake.mu.Unlock()
	drainer.materializePendingDirs()
	if _, err := os.Stat(filepath.Join(fuseRoot, "w/odd")); err != nil {
		t.Fatalf("folder not created plainly: %v", err)
	}
	drainer.dirInodes.mu.Lock()
	after := drainer.dirInodes.availableLocked()
	drainer.dirInodes.mu.Unlock()
	fake.mu.Lock()
	releases := fake.releaseSeen - releasesBefore
	fake.mu.Unlock()
	if after != before-1 || releases != 1 {
		t.Fatalf("pool %d -> %d numbers with %d release messages; want one number given back as the probe", before, after, releases)
	}
}
