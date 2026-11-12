package vfs

// JuiceMount: the JMPinCache control message (patch 0025, jm_pin.go).

import (
	"bytes"
	"encoding/binary"
	"os"
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
	body := []byte{jmPinOpSweep, phase}
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

	if r := pinMsg(v, jmPinOpStatus, uint64(a)); len(r) != 1 || r[0] != uint8(syscall.ENOENT) {
		t.Fatalf("status before start: %v", r)
	}
	if r := pinMsg(v, jmPinOpStart, uint64(a)); !bytes.Equal(r, []byte{0}) {
		t.Fatalf("start: %v", r)
	}
	var st []byte
	deadline := time.Now().Add(10 * time.Second)
	for {
		st = pinMsg(v, jmPinOpStatus, uint64(a))
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
	ck := pinMsg(v, jmPinOpCheck, uint64(a))
	if len(ck) != 18 || ck[0] != 0 || ck[1] != 1 || binary.BigEndian.Uint64(ck[2:]) != uint64(size) || binary.BigEndian.Uint64(ck[10:]) != 0 {
		t.Fatalf("check: %v", ck)
	}
	if ck := pinMsg(v, jmPinOpCheck, uint64(b)); ck[1] != 0 {
		t.Fatal("an inode never pinned is known")
	}
	// A sweep that keeps nothing unpins a; its blocks stay cached.
	sw := sweepMsg(v, 3)
	if len(sw) != 21 || sw[0] != 0 || binary.BigEndian.Uint32(sw[1:]) != 1 || binary.BigEndian.Uint64(sw[5:]) != 0 {
		t.Fatalf("sweep: %v", sw)
	}
	if ck := pinMsg(v, jmPinOpCheck, uint64(a)); ck[1] != 0 {
		t.Fatal("swept inode is still known")
	}
	// A sweep in parts keeps the listed inodes.
	pinMsg(v, jmPinOpStart, uint64(a))
	waitUntil(t, "refill", 10*time.Second, func() bool { return pinMsg(v, jmPinOpStatus, uint64(a))[1] == jmPinDone })
	if st := pinMsg(v, jmPinOpStatus, uint64(a)); binary.BigEndian.Uint64(st[19:]) != 0 {
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
	pinMsg(v, jmPinOpStart, 999999)
	waitUntil(t, "failed fill", 5*time.Second, func() bool { return pinMsg(v, jmPinOpStatus, 999999)[1] == jmPinFailed })
	if st := pinMsg(v, jmPinOpStatus, 999999); st[2] != uint8(syscall.ENOENT) {
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
	pinMsg(v, jmPinOpStart, uint64(fe.Inode))
	waitUntil(t, "refused fill", 5*time.Second, func() bool { return pinMsg(v, jmPinOpStatus, uint64(fe.Inode))[1] != jmPinRunning })
	if st := pinMsg(v, jmPinOpStatus, uint64(fe.Inode)); st[1] != jmPinFailed || st[2] != uint8(syscall.ENOSPC) {
		t.Fatalf("fill past the cap: state %d errno %d, want failed ENOSPC", st[1], st[2])
	}
}
