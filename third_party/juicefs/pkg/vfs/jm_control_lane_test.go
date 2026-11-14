package vfs

// JuiceMount: the control lane, a second control file (patch 0033,
// jm_control_lane.go).

import (
	"bytes"
	"encoding/binary"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// The lane is found by name in the root only, is never listed, and its name
// cannot be created there.
func TestJMControlLaneNamespace(t *testing.T) {
	v := pinVFS(t)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	e, err := v.Lookup(ctx, 1, ".jmcontrol")
	if err != 0 || e.Inode != jmControlLaneInode || !e.Attr.Full || e.Attr.Typ != meta.TypeFile {
		t.Fatalf("lookup in the root: %v %+v", err, e)
	}
	if a, err := v.GetAttr(ctx, jmControlLaneInode, 0); err != 0 || a.Inode != jmControlLaneInode {
		t.Fatalf("getattr: %v", err)
	}
	d, err := v.Mkdir(ctx, 1, "d", 0755, 0)
	if err != 0 {
		t.Fatal(err)
	}
	if _, err := v.Lookup(ctx, d.Inode, ".jmcontrol"); err != syscall.ENOENT {
		t.Fatalf("lookup in a folder: %v, want ENOENT", err)
	}
	if _, _, err := v.Create(ctx, 1, ".jmcontrol", 0644, 0, syscall.O_RDWR); err != syscall.EEXIST {
		t.Fatalf("create in the root: %v, want EEXIST", err)
	}
	fh, err := v.Opendir(ctx, 1, 0)
	if err != 0 {
		t.Fatal(err)
	}
	entries, _, err := v.Readdir(ctx, 1, 1024, 0, fh, true)
	if err != 0 {
		t.Fatal(err)
	}
	for _, e := range entries {
		if string(e.Name) == ".jmcontrol" || e.Inode == jmControlLaneInode {
			t.Fatalf("the root lists the lane")
		}
	}
	v.Releasedir(ctx, 1, fh)
	// Read-write like .control; the other internal files stay read-only.
	if _, lfh, err := v.Open(ctx, jmControlLaneInode, syscall.O_RDWR); err != 0 {
		t.Fatalf("open the lane read-write: %v", err)
	} else {
		v.Release(ctx, jmControlLaneInode, lfh)
	}
	if _, _, err := v.Open(ctx, StatsInode, syscall.O_RDWR); err != syscall.EACCES {
		t.Fatalf("open .stats read-write: %v, want EACCES", err)
	}
}

// On macOS a process's control handle is chosen by pid. The lane gets its
// own: releasing one (a close of that file) leaves the other.
func TestJMControlLaneHandlePerFile(t *testing.T) {
	v := pinVFS(t)
	const pid = 4242
	c := v.getControlHandle(controlInode, pid)
	l := v.getControlHandle(jmControlLaneInode, pid)
	if c == 0 || l == 0 || c == l {
		t.Fatalf("handles: .control %d, lane %d", c, l)
	}
	if again := v.getControlHandle(jmControlLaneInode, pid); again != l {
		t.Fatalf("second lookup of the lane handle: %d, want %d", again, l)
	}
	if v.findHandle(jmControlLaneInode, l) == nil || v.findHandle(controlInode, c) == nil {
		t.Fatal("handles not registered under their files")
	}
	v.releaseControlHandle(jmControlLaneInode, pid)
	if v.findHandle(jmControlLaneInode, l) != nil {
		t.Fatal("lane handle kept after its release")
	}
	if v.findHandle(controlInode, c) == nil || v.getControlHandle(controlInode, pid) != c {
		t.Fatal("releasing the lane released .control")
	}
	v.releaseControlHandle(controlInode, pid)
}

// A message on the lane is answered while one on .control still runs: a
// content refresh no longer waits for the drain's create-files message.
func TestJMControlLaneRunsBesideControl(t *testing.T) {
	v := pinVFS(t)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	fe, fh, _ := v.Create(ctx, 1, "f", 0644, 0, syscall.O_RDWR)
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'a'}, 4096), 0, fh); e != 0 {
		t.Fatal(e)
	}
	v.Release(ctx, fe.Inode, fh)

	// The message on .control: a synchronous refresh whose kernel notify is
	// stuck, as one behind a long read in flight.
	release := make(chan struct{})
	JMSetInodeInvalidator(func(Ino, int64, int64) syscall.Errno { <-release; return 0 })
	defer JMSetInodeInvalidator(nil)
	// The reply follows the message in the file, as the app reads it at its
	// offset after the write.
	msgLen := uint64(8 + len(refreshBody(1)))
	send := func(ino Ino, body []byte) uint64 {
		t.Helper()
		_, h, err := v.Open(ctx, ino, syscall.O_RDWR)
		if err != 0 {
			t.Fatalf("open %d: %v", ino, err)
		}
		msg := binary.BigEndian.AppendUint32(nil, meta.JMRefreshInode)
		msg = binary.BigEndian.AppendUint32(msg, uint32(len(body)))
		if err := v.Write(ctx, ino, append(msg, body...), 0, h); err != 0 {
			t.Fatalf("write %d: %v", ino, err)
		}
		return h
	}
	read := func(ino Ino, h uint64, want int, within time.Duration) []byte {
		t.Helper()
		buf := make([]byte, 64)
		var got []byte
		for deadline := time.Now().Add(within); len(got) < want && time.Now().Before(deadline); {
			n, err := v.Read(ctx, ino, buf, msgLen+uint64(len(got)), h)
			if err != 0 {
				t.Fatalf("read %d: %v", ino, err)
			}
			got = append(got, buf[:n]...)
			if n == 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}
		return got
	}
	ch := send(controlInode, refreshBody(fe.Inode))
	time.Sleep(50 * time.Millisecond)
	if r := read(controlInode, ch, 1, 100*time.Millisecond); len(r) != 0 {
		t.Fatalf(".control answered while its notify is stuck: % x", r)
	}
	// The lane: a refresh of an unknown inode answers ENOENT at once.
	lh := send(jmControlLaneInode, refreshBody(987654))
	if r := read(jmControlLaneInode, lh, 1, 2*time.Second); !bytes.Equal(r, []byte{uint8(syscall.ENOENT)}) {
		t.Fatalf("lane reply % x, want ENOENT while .control is busy", r)
	}
	close(release)
	if r := read(controlInode, ch, jmRefreshReplyOK, 10*time.Second); len(r) != jmRefreshReplyOK || r[0] != 0 || r[1]&JMRefreshKernelAccepted == 0 {
		t.Fatalf(".control reply after the notify: % x", r)
	}
	v.Release(ctx, controlInode, ch)
	v.Release(ctx, jmControlLaneInode, lh)
}
