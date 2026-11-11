package nfs

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

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
	fake.mkdirErr = syscall.ESRCH // juicefs restarted and forgot its numbers
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
