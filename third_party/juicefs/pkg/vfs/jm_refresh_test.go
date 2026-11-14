package vfs

// JuiceMount: the JMRefreshInode control message (patch 0031, jm_refresh.go).

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

func refreshMsg(v *VFS, body []byte) []byte {
	var out bytes.Buffer
	v.handleInternalMsg(meta.NewContext(1, 0, []uint32{0}), meta.JMRefreshInode, utils.ReadBuffer(body), &out)
	return out.Bytes()
}

func refreshBody(ino Ino) []byte {
	return binary.BigEndian.AppendUint64([]byte{jmRefreshVersion}, uint64(ino))
}

// A descriptor this mount holds open keeps serving its read buffers after
// another client's write (on macOS no second open reaches juicefs: P1). The
// message makes the same open descriptor serve the new bytes and the
// appended length, and it drops the kernel's pages synchronously.
func TestJMRefreshHeldReaderSeesPeerWrite(t *testing.T) {
	v := pinVFS(t)
	// The peer: another vfs on the same metadata and objects. It has its own
	// readers and writers, as another client has.
	reg := prometheus.NewRegistry()
	peer := NewVFS(v.Conf, v.Meta, v.Store, prometheus.WrapRegistererWithPrefix("juicefs_", reg), reg)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))

	const size = 2 << 20
	fe, fh, e := v.Create(ctx, 1, "clip", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatal(e)
	}
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'a'}, size), 0, fh); e != 0 {
		t.Fatal(e)
	}
	if e := v.Flush(ctx, fe.Inode, fh, 0); e != 0 {
		t.Fatal(e)
	}
	v.Release(ctx, fe.Inode, fh)
	ino := fe.Inode

	_, rfh, e := v.Open(ctx, ino, syscall.O_RDONLY)
	if e != 0 {
		t.Fatal(e)
	}
	defer v.Release(ctx, ino, rfh)
	read := func(off uint64, n int) []byte {
		buf := make([]byte, n)
		got, e := v.Read(ctx, ino, buf, off, rfh)
		if e != 0 {
			t.Fatalf("read at %d: %s", off, e)
		}
		return buf[:got]
	}
	if got := read(0, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'a'}, 4096)) {
		t.Fatal("first read")
	}

	// The teammate's save: 4 KiB in place at 0, and 1 MiB appended.
	_, pfh, e := peer.Open(ctx, ino, syscall.O_RDWR)
	if e != 0 {
		t.Fatal(e)
	}
	if e := peer.Write(ctx, ino, bytes.Repeat([]byte{'b'}, 4096), 0, pfh); e != 0 {
		t.Fatal(e)
	}
	if e := peer.Write(ctx, ino, bytes.Repeat([]byte{'c'}, 1<<20), size, pfh); e != 0 {
		t.Fatal(e)
	}
	if e := peer.Flush(ctx, ino, pfh, 0); e != 0 {
		t.Fatal(e)
	}
	peer.Release(ctx, ino, pfh)

	// Without the message the held reader serves its old buffer and length.
	if got := read(0, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'a'}, 4096)) {
		t.Fatalf("the model is off: the held reader already sees the peer's bytes (%q...)", got[:4])
	}
	if got := read(size, 4096); len(got) != 0 {
		t.Fatalf("the model is off: the held reader already reads %d appended bytes", len(got))
	}

	var notified atomic.Uint64
	JMSetInodeInvalidator(func(i Ino) syscall.Errno { notified.Store(uint64(i)); return 0 })
	defer JMSetInodeInvalidator(nil)

	reply := refreshMsg(v, refreshBody(ino))
	if len(reply) != jmRefreshReplyOK || reply[0] != 0 {
		t.Fatalf("reply % x", reply)
	}
	if reply[1] != JMRefreshKernelAccepted || notified.Load() != uint64(ino) {
		t.Fatalf("kernel flags %d, notified inode %d; want the kernel notify for %d accepted", reply[1], notified.Load(), ino)
	}
	if got := binary.BigEndian.Uint64(reply[2:]); got != size+1<<20 {
		t.Fatalf("reply length %d, want %d", got, size+1<<20)
	}
	if got := read(0, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'b'}, 4096)) {
		t.Fatalf("after the refresh the held reader still serves old bytes (%q...)", got[:4])
	}
	if got := read(4096, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'a'}, 4096)) {
		t.Fatal("untouched bytes changed")
	}
	if got := read(size, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'c'}, 4096)) {
		t.Fatalf("after the refresh the appended bytes are not readable (%d bytes)", len(got))
	}
}

func TestJMRefreshKernelFlags(t *testing.T) {
	v := pinVFS(t)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	fe, fh, _ := v.Create(ctx, 1, "f", 0644, 0, syscall.O_RDWR)
	v.Release(ctx, fe.Inode, fh)

	JMSetInodeInvalidator(nil)
	if r := refreshMsg(v, refreshBody(fe.Inode)); len(r) != jmRefreshReplyOK || r[0] != 0 || r[1] != JMRefreshKernelNone {
		t.Fatalf("no FUSE server: % x", r)
	}
	JMSetInodeInvalidator(func(Ino) syscall.Errno { return syscall.ENOSYS })
	if r := refreshMsg(v, refreshBody(fe.Inode)); r[1] != JMRefreshKernelNone {
		t.Fatalf("kernel without the notify: % x", r)
	}
	JMSetInodeInvalidator(func(Ino) syscall.Errno { return syscall.EIO })
	if r := refreshMsg(v, refreshBody(fe.Inode)); r[0] != 0 || r[1] != JMRefreshKernelFailed {
		t.Fatalf("failed notify: % x", r)
	}
	release := make(chan struct{})
	JMSetInodeInvalidator(func(Ino) syscall.Errno { <-release; return 0 })
	old := jmRefreshNotifyWait
	jmRefreshNotifyWait = 50 * time.Millisecond
	start := time.Now()
	r := refreshMsg(v, refreshBody(fe.Inode))
	jmRefreshNotifyWait = old
	close(release)
	if r[0] != 0 || r[1] != JMRefreshKernelTimeout || time.Since(start) > 2*time.Second {
		t.Fatalf("stuck notify: % x after %s", r, time.Since(start))
	}
	JMSetInodeInvalidator(nil)
}

func TestJMRefreshRefusals(t *testing.T) {
	v := pinVFS(t)
	for name, body := range map[string][]byte{
		"empty":     {},
		"version 2": binary.BigEndian.AppendUint64([]byte{2}, 1),
		"short":     {jmRefreshVersion, 0, 0, 0},
		"long":      append(refreshBody(1), 0),
	} {
		if got := refreshMsg(v, body); !bytes.Equal(got, []byte{uint8(syscall.EINVAL)}) {
			t.Fatalf("%s: reply % x, want EINVAL", name, got)
		}
	}
	if got := refreshMsg(v, refreshBody(987654)); !bytes.Equal(got, []byte{uint8(syscall.ENOENT)}) {
		t.Fatalf("unknown inode: % x, want ENOENT", got)
	}
}
