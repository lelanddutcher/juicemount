package vfs

// JuiceMount: the JMPinCache control message (patch 0026, jm_pin.go).

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

// pinVFS has a 64 MiB disk cache with 1 MiB blocks. The cache folder is not
// removed: the disk cache's goroutines outlive the test.
func pinVFS(t *testing.T) *VFS {
	t.Helper()
	dir, err := os.MkdirTemp("", "jmpinvfs")
	if err != nil {
		t.Fatal(err)
	}
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs"
	m := meta.NewClient("memkv://", metaConf)
	format := &meta.Format{Name: "pin", UUID: uuid.New().String(), Storage: "mem", BlockSize: 1024, Compression: "none"}
	if err := m.Init(format, true); err != nil {
		t.Fatal(err)
	}
	conf := &Config{
		Meta: metaConf, Format: *format, Version: "Juicefs", FuseOpts: &FuseOptions{},
		Chunk: &chunk.Config{BlockSize: 1 << 20, Compress: "none", MaxUpload: 2, BufferSize: 64 << 20,
			CacheSize: 64 << 20, CacheDir: dir, FreeSpace: 0.0001, CacheMode: 0600, AutoCreate: true,
			PutTimeout: time.Minute, GetTimeout: time.Minute, MaxRetries: 1},
	}
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, *conf.Chunk, registry)
	return NewVFS(conf, m, store, prometheus.WrapRegistererWithPrefix("juicefs_", registry), registry)
}

func pinMsg(v *VFS, op byte, args ...uint64) []byte {
	body := []byte{op}
	for _, a := range args {
		body = binary.BigEndian.AppendUint64(body, a)
	}
	var out bytes.Buffer
	v.jmPinMsg(utils.ReadBuffer(body), &out)
	return out.Bytes()
}

func sweepMsg(v *VFS, phase byte, inos ...uint64) []byte {
	body := []byte{meta.JMPinOpSweep, phase}
	body = binary.BigEndian.AppendUint32(body, uint32(len(inos)))
	for _, a := range inos {
		body = binary.BigEndian.AppendUint64(body, a)
	}
	var out bytes.Buffer
	v.jmPinMsg(utils.ReadBuffer(body), &out)
	return out.Bytes()
}

func TestJMPinMessageFillCheckSweep(t *testing.T) {
	v := pinVFS(t)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	write := func(name string, size int) Ino {
		fe, fh, e := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
		if e != 0 {
			t.Fatalf("create: %s", e)
		}
		if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'x'}, size), 0, fh); e != 0 {
			t.Fatalf("write: %s", e)
		}
		if e := v.Flush(ctx, fe.Inode, fh, 0); e != 0 {
			t.Fatalf("flush: %s", e)
		}
		v.Release(ctx, fe.Inode, fh)
		return fe.Inode
	}
	size := 3<<20 + 5000
	a := write("a", size)
	b := write("b", 1<<20)
	// Start cold: the writer cached its blocks.
	v.cacheFiller.Cache(meta.Background(), EvictCache, []string{"/a", "/b"}, 2, nil)

	if r := pinMsg(v, meta.JMPinOpStatus, uint64(a)); len(r) != 1 || r[0] != uint8(syscall.ENOENT) {
		t.Fatalf("status before start: %v", r)
	}
	if r := pinMsg(v, meta.JMPinOpStart, uint64(a)); !bytes.Equal(r, []byte{0}) {
		t.Fatalf("start: %v", r)
	}
	var st []byte
	deadline := time.Now().Add(10 * time.Second)
	for {
		st = pinMsg(v, meta.JMPinOpStatus, uint64(a))
		if len(st) != 27 || st[0] != 0 {
			t.Fatalf("status reply %v", st)
		}
		if st[1] != jmPinRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fill never finished")
		}
		time.Sleep(10 * time.Millisecond)
	}
	total, done, fetched := binary.BigEndian.Uint64(st[3:]), binary.BigEndian.Uint64(st[11:]), binary.BigEndian.Uint64(st[19:])
	if st[1] != jmPinDone || st[2] != 0 || total != uint64(size) || done != total || fetched != total {
		t.Fatalf("status state %d errno %d total %d done %d fetched %d; want done, %d each", st[1], st[2], total, done, fetched, size)
	}
	ck := pinMsg(v, meta.JMPinOpCheck, uint64(a))
	if len(ck) != 34 || ck[0] != 0 || ck[1] != 1 || binary.BigEndian.Uint64(ck[2:]) != uint64(size) || binary.BigEndian.Uint64(ck[10:]) != 0 ||
		binary.BigEndian.Uint64(ck[18:]) != uint64(size) || binary.BigEndian.Uint64(ck[26:]) == 0 {
		t.Fatalf("check: %v", ck)
	}
	if ck := pinMsg(v, meta.JMPinOpCheck, uint64(b)); ck[1] != 0 {
		t.Fatal("an inode never pinned is known")
	}
	// A sweep that keeps nothing unpins a; its blocks stay cached.
	sw := sweepMsg(v, 3)
	if len(sw) != 21 || sw[0] != 0 || binary.BigEndian.Uint32(sw[1:]) != 1 || binary.BigEndian.Uint64(sw[5:]) != 0 {
		t.Fatalf("sweep: %v", sw)
	}
	if ck := pinMsg(v, meta.JMPinOpCheck, uint64(a)); ck[1] != 0 {
		t.Fatal("swept inode is still known")
	}
	// A sweep in parts keeps the listed inodes.
	pinMsg(v, meta.JMPinOpStart, uint64(a))
	waitUntil(t, "refill", 10*time.Second, func() bool { return pinMsg(v, meta.JMPinOpStatus, uint64(a))[1] == jmPinDone })
	if st := pinMsg(v, meta.JMPinOpStatus, uint64(a)); binary.BigEndian.Uint64(st[19:]) != 0 {
		t.Fatal("refill of cached blocks downloaded")
	}
	sweepMsg(v, 1, uint64(b))
	sw = sweepMsg(v, 2, uint64(a))
	if binary.BigEndian.Uint32(sw[1:]) != 0 || binary.BigEndian.Uint64(sw[5:]) == 0 {
		t.Fatalf("kept sweep: %v", sw)
	}
	if r := sweepMsg(v, 2); r[0] != uint8(syscall.EINVAL) {
		t.Fatal("sweep end without a start was accepted")
	}
	if r := pinMsg(v, 99); !bytes.Equal(r, []byte{uint8(syscall.EINVAL)}) {
		t.Fatalf("unknown op: %v", r)
	}
	// A missing inode fails the fill with its errno.
	pinMsg(v, meta.JMPinOpStart, 999999)
	waitUntil(t, "failed fill", 5*time.Second, func() bool { return pinMsg(v, meta.JMPinOpStatus, 999999)[1] == jmPinFailed })
	if st := pinMsg(v, meta.JMPinOpStatus, 999999); st[2] != uint8(syscall.ENOENT) {
		t.Fatalf("fill of a missing inode: errno %d", st[2])
	}
}

func TestJMPinMessageCapIsENOSPC(t *testing.T) {
	v := pinVFS(t) // 64 MiB cache: cap 57.6 MiB
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	fe, fh, _ := v.Create(ctx, 1, "big", 0644, 0, syscall.O_RDWR)
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'y'}, 60<<20), 0, fh); e != 0 {
		t.Fatal(e)
	}
	_ = v.Flush(ctx, fe.Inode, fh, 0)
	v.Release(ctx, fe.Inode, fh)
	pinMsg(v, meta.JMPinOpStart, uint64(fe.Inode))
	waitUntil(t, "refused fill", 5*time.Second, func() bool { return pinMsg(v, meta.JMPinOpStatus, uint64(fe.Inode))[1] != jmPinRunning })
	if st := pinMsg(v, meta.JMPinOpStatus, uint64(fe.Inode)); st[1] != jmPinFailed || st[2] != uint8(syscall.ENOSPC) {
		t.Fatalf("fill past the cap: state %d errno %d, want failed ENOSPC", st[1], st[2])
	}
}

func TestJMPinMessageShortBodiesAreEINVAL(t *testing.T) {
	v := pinVFS(t)
	for _, body := range [][]byte{{}, {meta.JMPinOpStart, 1, 2}, {meta.JMPinOpCheck}, {meta.JMPinOpSweep, 3, 0, 0, 0, 2, 1}, {meta.JMPinOpVerify, 0, 0, 0, 1}, {meta.JMPinOpSetCap}} {
		var out bytes.Buffer
		v.jmPinMsg(utils.ReadBuffer(body), &out)
		if !bytes.Equal(out.Bytes(), []byte{uint8(syscall.EINVAL)}) {
			t.Fatalf("body %v: reply %v", body, out.Bytes())
		}
	}
}

func TestJMPinMessageVerifyCancelSetCapAndChange(t *testing.T) {
	v := pinVFS(t)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	fe, fh, _ := v.Create(ctx, 1, "f", 0644, 0, syscall.O_RDWR)
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'z'}, 3<<20), 0, fh); e != 0 {
		t.Fatal(e)
	}
	_ = v.Flush(ctx, fe.Inode, fh, 0)
	ino := uint64(fe.Inode)
	pinMsg(v, meta.JMPinOpStart, ino)
	waitUntil(t, "fill", 10*time.Second, func() bool { return pinMsg(v, meta.JMPinOpStatus, ino)[1] == jmPinDone })
	ck := pinMsg(v, meta.JMPinOpCheck, ino)
	length, mtime := binary.BigEndian.Uint64(ck[18:]), binary.BigEndian.Uint64(ck[26:])
	verify := func(items ...uint64) []byte {
		body := binary.BigEndian.AppendUint32([]byte{meta.JMPinOpVerify}, uint32(len(items)/3))
		for _, x := range items {
			body = binary.BigEndian.AppendUint64(body, x)
		}
		var out bytes.Buffer
		v.jmPinMsg(utils.ReadBuffer(body), &out)
		return out.Bytes()
	}
	if r := verify(ino, length, mtime); !bytes.Equal(r, []byte{0, 0, 0, 0, 0}) {
		t.Fatalf("verify of a current pin: %v", r)
	}
	// The file grows: the recorded length no longer matches the mirror's.
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'z'}, 1<<20), 3<<20, fh); e != 0 {
		t.Fatal(e)
	}
	_ = v.Flush(ctx, fe.Inode, fh, 0)
	v.Release(ctx, fe.Inode, fh)
	r := verify(ino, length+1<<20, mtime, 424242, 1, 1)
	if len(r) != 5+2*17 || binary.BigEndian.Uint32(r[1:]) != 2 || r[13] != 2 || r[30] != 1 {
		t.Fatalf("verify of a changed and an unknown file: %v", r)
	}
	// START re-reads the slices: the new block is pinned.
	pinMsg(v, meta.JMPinOpStart, ino)
	waitUntil(t, "refill", 10*time.Second, func() bool { return pinMsg(v, meta.JMPinOpStatus, ino)[1] == jmPinDone })
	if ck := pinMsg(v, meta.JMPinOpCheck, ino); binary.BigEndian.Uint64(ck[2:]) != 4<<20 || binary.BigEndian.Uint64(ck[18:]) != 4<<20 {
		t.Fatalf("after the change: %v", ck)
	}
	// CANCEL of an unknown inode; SETCAP grows the cache.
	if r := pinMsg(v, meta.JMPinOpCancel, 777); r[0] != uint8(syscall.ENOENT) {
		t.Fatalf("cancel unknown: %v", r)
	}
	if r := pinMsg(v, meta.JMPinOpSetCap, 128<<20); len(r) != 25 || r[0] != 0 || binary.BigEndian.Uint64(r[1:]) != 128<<20 {
		t.Fatalf("setcap: %v", r)
	}
}

func TestJMPinMessageCancelStopsFill(t *testing.T) {
	v := pinVFS(t)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	fe, fh, _ := v.Create(ctx, 1, "c", 0644, 0, syscall.O_RDWR)
	_ = v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'c'}, 8<<20), 0, fh)
	_ = v.Flush(ctx, fe.Inode, fh, 0)
	v.Release(ctx, fe.Inode, fh)
	v.cacheFiller.Cache(meta.Background(), EvictCache, []string{"/c"}, 1, nil)
	ino := uint64(fe.Inode)
	pinMsg(v, meta.JMPinOpStart, ino)
	pinMsg(v, meta.JMPinOpCancel, ino)
	waitUntil(t, "cancel", 5*time.Second, func() bool { return pinMsg(v, meta.JMPinOpStatus, ino)[1] != jmPinRunning })
	st := pinMsg(v, meta.JMPinOpStatus, ino)
	if st[1] == jmPinFailed && st[2] != uint8(syscall.ECANCELED) {
		t.Fatalf("canceled fill: state %d errno %d", st[1], st[2])
	}
}

// slowAttrMeta blocks the first GetAttr of ino until release is closed.
type slowAttrMeta struct {
	meta.Meta
	ino     Ino
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (m *slowAttrMeta) GetAttr(ctx meta.Context, ino Ino, attr *Attr) syscall.Errno {
	if ino == m.ino {
		m.once.Do(func() {
			close(m.entered)
			<-m.release
		})
	}
	return m.Meta.GetAttr(ctx, ino, attr)
}

// Re-review LOW: START with a fill running answers without reading metadata
// on the control path; the change check runs in the background.
func TestJMPinMessageStartDoesNotWaitForMetadata(t *testing.T) {
	v := pinVFS(t)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	fe, fh, _ := v.Create(ctx, 1, "s", 0644, 0, syscall.O_RDWR)
	_ = v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'s'}, 2<<20), 0, fh)
	_ = v.Flush(ctx, fe.Inode, fh, 0)
	v.Release(ctx, fe.Inode, fh)
	// A running fill that saw other attributes.
	st := v.jmPin()
	canceled := make(chan struct{})
	old := &jmPinJob{cancel: func() { close(canceled) }, attr: &chunk.JMPinAttr{Length: 1}}
	old.state.Store(jmPinRunning)
	st.mu.Lock()
	st.jobs[fe.Inode] = old
	st.mu.Unlock()
	slow := &slowAttrMeta{Meta: v.Meta, ino: fe.Inode, entered: make(chan struct{}), release: make(chan struct{})}
	v.Meta = slow
	ino := uint64(fe.Inode)
	replied := make(chan []byte, 1)
	go func() { replied <- pinMsg(v, meta.JMPinOpStart, ino) }()
	<-slow.entered
	select {
	case r := <-replied:
		if r[0] != 0 {
			t.Fatalf("start: %v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("START waited for GetAttr")
	}
	// A second START while the check runs starts no second check.
	if r := pinMsg(v, meta.JMPinOpStart, ino); r[0] != 0 {
		t.Fatalf("second start: %v", r)
	}
	close(slow.release)
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("the stale fill was not canceled")
	}
	waitUntil(t, "refill", 10*time.Second, func() bool { return pinMsg(v, meta.JMPinOpStatus, ino)[1] == jmPinDone })
	if ck := pinMsg(v, meta.JMPinOpCheck, ino); binary.BigEndian.Uint64(ck[2:]) != 2<<20 {
		t.Fatalf("after the restart: %v", ck)
	}
}

// Re-review MEDIUM-5: VERIFY with flags bit 1 finds a file whose slices
// changed while its length and mtime did not (as compaction does).
func TestJMPinMessageVerifyComparesSlices(t *testing.T) {
	v := pinVFS(t)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	fe, fh, _ := v.Create(ctx, 1, "k", 0644, 0, syscall.O_RDWR)
	_ = v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'k'}, 2<<20), 0, fh)
	_ = v.Flush(ctx, fe.Inode, fh, 0)
	ino := uint64(fe.Inode)
	pinMsg(v, meta.JMPinOpStart, ino)
	waitUntil(t, "fill", 10*time.Second, func() bool { return pinMsg(v, meta.JMPinOpStatus, ino)[1] == jmPinDone })
	ck := pinMsg(v, meta.JMPinOpCheck, ino)
	length, mtime := binary.BigEndian.Uint64(ck[18:]), binary.BigEndian.Uint64(ck[26:])
	verify := func(flags ...byte) []byte {
		body := binary.BigEndian.AppendUint32([]byte{meta.JMPinOpVerify}, 1)
		for _, x := range []uint64{ino, length, mtime} {
			body = binary.BigEndian.AppendUint64(body, x)
		}
		body = append(body, flags...)
		var out bytes.Buffer
		v.jmPinMsg(utils.ReadBuffer(body), &out)
		return out.Bytes()
	}
	if r := verify(1); !bytes.Equal(r, []byte{0, 0, 0, 0, 0}) {
		t.Fatalf("deep verify of a current pin: %v", r)
	}
	// New slices, same length; then the old mtime back.
	_ = v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'K'}, 2<<20), 0, fh)
	_ = v.Flush(ctx, fe.Inode, fh, 0)
	v.Release(ctx, fe.Inode, fh)
	attr := Attr{Mtime: int64(mtime) / 1e9, Mtimensec: uint32(int64(mtime) % 1e9)}
	if e := v.Meta.SetAttr(meta.Background(), fe.Inode, meta.SetAttrMtime, 0, &attr); e != 0 {
		t.Fatal(e)
	}
	if r := verify(); !bytes.Equal(r, []byte{0, 0, 0, 0, 0}) {
		t.Fatalf("local verify cannot see the new slices, got %v", r)
	}
	if r := verify(1); len(r) != 5+17 || r[13] != 2 {
		t.Fatalf("deep verify of new slices: %v", r)
	}
}
