package vfs

// JuiceMount: the JMRefreshInode control message (patch 0031, jm_refresh.go).

import (
	"bytes"
	"encoding/binary"
	"sync"
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
	JMSetInodeInvalidator(func(i Ino, _, _ int64) syscall.Errno { notified.Store(uint64(i)); return 0 })
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
	JMSetInodeInvalidator(func(Ino, int64, int64) syscall.Errno { return syscall.ENOSYS })
	if r := refreshMsg(v, refreshBody(fe.Inode)); r[1] != JMRefreshKernelNone {
		t.Fatalf("kernel without the notify: % x", r)
	}
	JMSetInodeInvalidator(func(Ino, int64, int64) syscall.Errno { return syscall.EIO })
	if r := refreshMsg(v, refreshBody(fe.Inode)); r[0] != 0 || r[1] != JMRefreshKernelFailed {
		t.Fatalf("failed notify: % x", r)
	}
	release := make(chan struct{})
	JMSetInodeInvalidator(func(Ino, int64, int64) syscall.Errno { <-release; return 0 })
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

// jmBufferStates lists the states of the inode's read buffers by chunk.
func jmBufferStates(v *VFS, ino Ino) map[uint32][]sstate {
	out := map[uint32][]sstate{}
	v.reader.(*dataReader).visit(ino, func(f *fileReader) {
		f.visit(func(s *sliceReader) bool {
			out[s.indx] = append(out[s.indx], s.state)
			return true
		})
	})
	return out
}

func jmWaitBuffersSettled(t *testing.T, v *VFS, ino Ino) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; {
		busy := false
		for _, ss := range jmBufferStates(v, ino) {
			for _, s := range ss {
				if s == NEW || s == BUSY || s == REFRESH {
					busy = true
				}
			}
		}
		if !busy {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("buffers still filling: %v", jmBufferStates(v, ino))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Patch 0032 (bug F1): a refresh keeps the read buffers of chunks whose
// slice list did not change, and drops those of a chunk the peer wrote. A
// continuous save on a far link refreshed a played clip every 2 s, and
// dropping the player's whole read-ahead each time stalled playback.
func TestJMRefreshKeepsBuffersOfUnchangedChunks(t *testing.T) {
	v := pinVFS(t)
	reg := prometheus.NewRegistry()
	peer := NewVFS(v.Conf, v.Meta, v.Store, prometheus.WrapRegistererWithPrefix("juicefs_", reg), reg)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	const chunk1 = meta.ChunkSize
	fe, fh, e := v.Create(ctx, 1, "clip", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatal(e)
	}
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'a'}, 1<<20), 0, fh); e != 0 {
		t.Fatal(e)
	}
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'x'}, 4096), chunk1, fh); e != 0 {
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
	read(0, 4096)
	read(chunk1, 4096)
	jmWaitBuffersSettled(t, v, ino)
	before := jmBufferStates(v, ino)
	if len(before[0]) == 0 || len(before[1]) == 0 {
		t.Fatalf("no buffers to keep: %v", before)
	}

	JMSetInodeInvalidator(func(Ino, int64, int64) syscall.Errno { return 0 })
	defer JMSetInodeInvalidator(nil)

	// A refresh with no change keeps every buffer.
	if r := refreshMsg(v, refreshBody(ino)); r[0] != 0 {
		t.Fatalf("reply % x", r)
	}
	if got := jmBufferStates(v, ino); len(got[0]) != len(before[0]) || len(got[1]) != len(before[1]) {
		t.Fatalf("unchanged file: buffers %v, want %v kept", got, before)
	}
	for _, ss := range jmBufferStates(v, ino) {
		for _, s := range ss {
			if s != READY {
				t.Fatalf("unchanged file: a buffer went %s", s)
			}
		}
	}

	// The peer saves chunk 1 in place: its buffers go, chunk 0's stay.
	_, pfh, e := peer.Open(ctx, ino, syscall.O_RDWR)
	if e != 0 {
		t.Fatal(e)
	}
	if e := peer.Write(ctx, ino, bytes.Repeat([]byte{'b'}, 4096), chunk1, pfh); e != 0 {
		t.Fatal(e)
	}
	if e := peer.Flush(ctx, ino, pfh, 0); e != 0 {
		t.Fatal(e)
	}
	peer.Release(ctx, ino, pfh)
	if got := read(chunk1, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'x'}, 4096)) {
		t.Fatal("the model is off: the held reader already sees the peer's bytes")
	}
	jmWaitBuffersSettled(t, v, ino)
	before = jmBufferStates(v, ino)
	if r := refreshMsg(v, refreshBody(ino)); r[0] != 0 {
		t.Fatalf("reply % x", r)
	}
	after := jmBufferStates(v, ino)
	if len(after[0]) != len(before[0]) {
		t.Fatalf("chunk 0 did not change, but its buffers went: %v -> %v", before, after)
	}
	for _, s := range after[0] {
		if s != READY {
			t.Fatalf("chunk 0 buffer went %s", s)
		}
	}
	for _, s := range after[1] {
		if s == READY {
			t.Fatalf("a buffer of the changed chunk 1 stayed: %v", after)
		}
	}
	if got := read(chunk1, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'b'}, 4096)) {
		t.Fatalf("after the refresh chunk 1 still serves old bytes (%q...)", got[:4])
	}
	if got := read(0, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'a'}, 4096)) {
		t.Fatal("chunk 0 bytes changed")
	}
}

// Patch 0032: two lists of one chunk give the same bytes for a range when
// their pieces over that range are the same.
func TestJMRefreshSameRange(t *testing.T) {
	const M = 1 << 20
	old := []meta.Slice{{Id: 1, Size: 64 * M, Off: 0, Len: 64 * M}}
	// A 4 KiB write at 8 MiB splits the one slice in three.
	split := []meta.Slice{{Id: 1, Size: 64 * M, Off: 0, Len: 8 * M}, {Id: 2, Size: 4096, Off: 0, Len: 4096},
		{Id: 1, Size: 64 * M, Off: 8*M + 4096, Len: 56*M - 4096}}
	holes := []meta.Slice{{Size: 2 * M, Len: 2 * M}, {Id: 3, Size: M, Len: M}}
	holes2 := []meta.Slice{{Size: 3 * M, Len: 3 * M}}
	for _, c := range []struct {
		name     string
		a, b     []meta.Slice
		from, to uint32
		same     bool
	}{
		{"a block before the write", old, split, 0, 4 * M, true},
		{"a block after the write", old, split, 12 * M, 16 * M, true},
		{"the block with the write", old, split, 8 * M, 12 * M, false},
		{"a block ending at the write", old, split, 4 * M, 8 * M, true},
		{"one byte over the write", old, split, 4 * M, 8*M + 1, false},
		{"identical lists", split, split, 0, 64 * M, true},
		{"a hole is a hole whatever its size", holes, holes2, 0, 2 * M, true},
		{"a hole filled", holes, holes2, 2 * M, 3 * M, false},
		{"a list that ends before the range", old[:0], old, 0, M, false},
	} {
		if got := jmSameRange(c.a, c.b, c.from, c.to); got != c.same {
			t.Errorf("%s: same %v, want %v", c.name, got, c.same)
		}
	}
}

// Patch 0032 (bug F1): the peer's save writes a 4 KiB block every 8 MiB, so
// every early commit changes the slice list of the chunk the player reads.
// The player's buffers between those blocks still hold current bytes and
// stay; a buffer over a written block goes, and reads the new bytes.
func TestJMRefreshKeepsBuffersOutsideTheChangedRange(t *testing.T) {
	v := pinVFS(t)
	reg := prometheus.NewRegistry()
	peer := NewVFS(v.Conf, v.Meta, v.Store, prometheus.WrapRegistererWithPrefix("juicefs_", reg), reg)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	const far = 32 << 20 // in chunk 0, far from the player's buffers
	fe, fh, e := v.Create(ctx, 1, "clip", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatal(e)
	}
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'a'}, 2<<20), 0, fh); e != 0 {
		t.Fatal(e)
	}
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'x'}, 4096), far, fh); e != 0 {
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
	peerWrite := func(b byte, off uint64) {
		_, pfh, e := peer.Open(ctx, ino, syscall.O_RDWR)
		if e != 0 {
			t.Fatal(e)
		}
		if e := peer.Write(ctx, ino, bytes.Repeat([]byte{b}, 4096), off, pfh); e != 0 {
			t.Fatal(e)
		}
		if e := peer.Flush(ctx, ino, pfh, 0); e != 0 {
			t.Fatal(e)
		}
		peer.Release(ctx, ino, pfh)
	}
	JMSetInodeInvalidator(func(Ino, int64, int64) syscall.Errno { return 0 })
	defer JMSetInodeInvalidator(nil)

	read(0, 4096)
	jmWaitBuffersSettled(t, v, ino)
	before := jmBufferStates(v, ino)[0]
	if len(before) == 0 {
		t.Fatal("no buffers")
	}
	peerWrite('b', far)
	if r := refreshMsg(v, refreshBody(ino)); r[0] != 0 {
		t.Fatalf("reply % x", r)
	}
	after := jmBufferStates(v, ino)[0]
	if len(after) != len(before) {
		t.Fatalf("a write at 32 MiB dropped the buffers at 0: %v -> %v", before, after)
	}
	for _, s := range after {
		if s != READY {
			t.Fatalf("a buffer at 0 went %s", s)
		}
	}
	if got := read(far, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'b'}, 4096)) {
		t.Fatalf("the written block reads old bytes (%q...)", got[:4])
	}

	// A write inside the buffered range drops that buffer.
	jmWaitBuffersSettled(t, v, ino)
	peerWrite('c', 0)
	if r := refreshMsg(v, refreshBody(ino)); r[0] != 0 {
		t.Fatalf("reply % x", r)
	}
	if got := read(0, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'c'}, 4096)) {
		t.Fatalf("after a write at 0 the buffer at 0 still serves old bytes (%q...)", got[:4])
	}
	if got := read(4096, 4096); !bytes.Equal(got, bytes.Repeat([]byte{'a'}, 4096)) {
		t.Fatal("untouched bytes changed")
	}
}

// Patch 0032: the chunk positions where two lists give different bytes.
func TestJMRefreshChangedRanges(t *testing.T) {
	const M = 1 << 20
	one := []meta.Slice{{Id: 1, Size: 64 * M, Len: 64 * M}}
	split := []meta.Slice{{Id: 1, Size: 64 * M, Len: 8 * M}, {Id: 2, Size: 4096, Len: 4096},
		{Id: 1, Size: 64 * M, Off: 8*M + 4096, Len: 56*M - 4096}}
	twice := []meta.Slice{{Id: 1, Size: 64 * M, Len: 8 * M}, {Id: 3, Size: 4096, Len: 4096},
		{Id: 1, Size: 64 * M, Off: 8*M + 4096, Len: 8 * M}, {Id: 4, Size: 4096, Len: 4096},
		{Id: 1, Size: 64 * M, Off: 16*M + 8192, Len: 48*M - 8192}}
	short := []meta.Slice{{Id: 1, Size: 64 * M, Len: 2 * M}}
	for _, c := range []struct {
		name       string
		old, fresh []meta.Slice
		want       [][2]uint32
	}{
		{"no change", one, one, nil},
		{"a 4 KiB write at 8 MiB", one, split, [][2]uint32{{8 * M, 8*M + 4096}}},
		{"the next save rewrites it and one more", split, twice, [][2]uint32{{8 * M, 8*M + 4096}, {16*M + 4096, 16*M + 8192}}},
		{"unknown old list", nil, one, [][2]uint32{{0, meta.ChunkSize}}},
		{"fresh covers more", short, one, [][2]uint32{{2 * M, 64 * M}}},
		{"old covers more", one, short, [][2]uint32{{2 * M, 64 * M}}},
		{"holes of other sizes", []meta.Slice{{Size: 9, Len: M}}, []meta.Slice{{Size: 3, Len: M}}, nil},
	} {
		got := jmChangedRanges(c.old, c.fresh)
		if len(got) != len(c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: %v, want %v", c.name, got, c.want)
			}
		}
	}
}

// Patch 0032 (bug F1): the kernel notify of a refresh covers only the bytes
// the peer changed in the parts this mount has read, so it does not wait for
// the player's reads of other parts. No change: the attributes only. A notify
// the kernel refuses keeps those ranges for the next refresh. A local write
// makes the next refresh notify the whole file.
func TestJMRefreshNotifiesOnlyChangedRanges(t *testing.T) {
	v := pinVFS(t)
	reg := prometheus.NewRegistry()
	peer := NewVFS(v.Conf, v.Meta, v.Store, prometheus.WrapRegistererWithPrefix("juicefs_", reg), reg)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	const tagAt = 8 << 20
	fe, fh, e := v.Create(ctx, 1, "clip", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatal(e)
	}
	if e := v.Write(ctx, fe.Inode, bytes.Repeat([]byte{'a'}, 12<<20), 0, fh); e != 0 {
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
	buf := make([]byte, 4096)
	if _, e := v.Read(ctx, ino, buf, 0, rfh); e != 0 {
		t.Fatal(e)
	}
	jmWaitBuffersSettled(t, v, ino)

	var mu sync.Mutex
	var calls [][2]int64
	answer := syscall.Errno(0)
	JMSetInodeInvalidator(func(i Ino, off, n int64) syscall.Errno {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, [2]int64{off, n})
		return answer
	})
	defer JMSetInodeInvalidator(nil)
	refresh := func(eno syscall.Errno) [][2]int64 {
		t.Helper()
		mu.Lock()
		calls, answer = nil, eno
		mu.Unlock()
		if r := refreshMsg(v, refreshBody(ino)); r[0] != 0 {
			t.Fatalf("reply % x", r)
		}
		mu.Lock()
		defer mu.Unlock()
		return append([][2]int64(nil), calls...)
	}
	peerTag := func(b byte) {
		_, pfh, e := peer.Open(ctx, ino, syscall.O_RDWR)
		if e != 0 {
			t.Fatal(e)
		}
		if e := peer.Write(ctx, ino, bytes.Repeat([]byte{b}, 4096), tagAt, pfh); e != 0 {
			t.Fatal(e)
		}
		if e := peer.Flush(ctx, ino, pfh, 0); e != 0 {
			t.Fatal(e)
		}
		peer.Release(ctx, ino, pfh)
	}
	same := func(got, want [][2]int64) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}

	if got := refresh(0); !same(got, [][2]int64{{-1, 0}}) {
		t.Fatalf("no change: notifies %v, want the attributes only", got)
	}
	// An attribute reply with the same length (VFS.UpdateLength) keeps the
	// record.
	v.UpdateLength(ino, &meta.Attr{Full: true, Typ: meta.TypeFile, Length: 12 << 20})
	peerTag('b')
	if got := refresh(0); !same(got, [][2]int64{{tagAt, 4096}}) {
		t.Fatalf("a 4 KiB peer write at 8 MiB: notifies %v, want only that range", got)
	}
	if got := refresh(0); !same(got, [][2]int64{{-1, 0}}) {
		t.Fatalf("after a confirmed refresh: notifies %v, want the attributes only", got)
	}
	peerTag('c')
	if got := refresh(syscall.EIO); !same(got, [][2]int64{{tagAt, 4096}}) {
		t.Fatalf("refused notify: %v", got)
	}
	if got := refresh(0); !same(got, [][2]int64{{tagAt, 4096}}) {
		t.Fatalf("after a refused notify the next refresh notifies %v, want the range again", got)
	}
	// A local write leaves pages no list describes.
	_, wfh, e := v.Open(ctx, ino, syscall.O_RDWR)
	if e != 0 {
		t.Fatal(e)
	}
	if e := v.Write(ctx, ino, []byte("local"), 100, wfh); e != 0 {
		t.Fatal(e)
	}
	if e := v.Flush(ctx, ino, wfh, 0); e != 0 {
		t.Fatal(e)
	}
	v.Release(ctx, ino, wfh)
	if got := refresh(0); !same(got, [][2]int64{{0, 0}}) {
		t.Fatalf("after a local write: notifies %v, want the whole file", got)
	}
}
