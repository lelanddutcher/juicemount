package jmengine

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine/wire"
)

// fakeReadEngine serves one file (inode 2) of known bytes.
type fakeReadEngine struct {
	fakeEngine
	data     []byte
	mu       sync.Mutex
	next     uint64
	open     map[uint64]bool
	released []uint64
	block    chan struct{} // Read waits on it (or ctx)
	canceled chan struct{} // closed when a blocked Read saw its ctx end
}

func newFakeRead(data []byte) *fakeReadEngine {
	return &fakeReadEngine{data: data, next: 100, open: map[uint64]bool{}}
}

func (f *fakeReadEngine) Open(ctx context.Context, ino uint64) (uint64, wire.Attr, syscall.Errno) {
	if ino != 2 {
		return 0, wire.Attr{}, syscall.ENOENT
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	f.open[f.next] = true
	return f.next, wire.Attr{Inode: 2, Size: uint64(len(f.data)), Type: 1}, 0
}

func (f *fakeReadEngine) Read(ctx context.Context, ino, fh uint64, buf []byte, off uint64) (int, syscall.Errno) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			close(f.canceled)
			return 0, syscall.ETIMEDOUT
		}
	}
	if off >= uint64(len(f.data)) {
		return 0, 0
	}
	return copy(buf, f.data[off:]), 0
}

func (f *fakeReadEngine) Release(ino, fh uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.open, fh)
	f.released = append(f.released, fh)
}

func readCall(t *testing.T, c interface {
	Write([]byte) (int, error)
	Read([]byte) (int, error)
	SetReadDeadline(time.Time) error
}, a wire.ReadArgs) (uint32, []byte) {
	t.Helper()
	if err := wire.WriteRequest(c, wire.Request{Op: wire.OpRead, Class: wire.ClassRead, Body: a.Encode()}); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	dst := make([]byte, a.Length)
	st, n, err := wire.ReadReplyInto(c, dst)
	if err != nil {
		t.Fatal(err)
	}
	return st, dst[:n]
}

func TestOpenReadRelease(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 200<<10/16) // 200 KiB: the vectored reply
	eng := newFakeRead(data)
	s, path := serve(t, Config{Engine: eng})
	c, r := dialHello(t, path, testToken, wire.ProtoMajor)
	var h wire.HelloReply
	if json.Unmarshal(r.Body, &h) != nil || !strings.Contains(strings.Join(h.Ops, ","), "read") || h.ProtoMinor != wire.ProtoMinor {
		t.Fatalf("hello does not list read: %+v", h)
	}
	var o wire.OpenReply
	if r := call(t, c, wire.OpOpen, wire.OpenRequest{Inode: 2}); r.Status != 0 || json.Unmarshal(r.Body, &o) != nil || o.Attr.Size != uint64(len(data)) {
		t.Fatalf("open: %+v", r)
	}
	st, got := readCall(t, c, wire.ReadArgs{Inode: 2, Handle: o.Handle, Offset: 0, Length: 128 << 10})
	if st != 0 || !bytes.Equal(got, data[:128<<10]) {
		t.Fatalf("read: status %d, %d bytes", st, len(got))
	}
	// A read across the end of the file returns the bytes up to it.
	st, got = readCall(t, c, wire.ReadArgs{Inode: 2, Handle: o.Handle, Offset: uint64(len(data) - 10), Length: 4096})
	if st != 0 || !bytes.Equal(got, data[len(data)-10:]) {
		t.Fatalf("read at the end: status %d, %d bytes", st, len(got))
	}
	// A read past the end returns no bytes and no error.
	if st, got = readCall(t, c, wire.ReadArgs{Inode: 2, Handle: o.Handle, Offset: uint64(len(data) + 5), Length: 10}); st != 0 || len(got) != 0 {
		t.Fatalf("read past the end: status %d, %d bytes", st, len(got))
	}
	// A handle the server did not open, or of another inode, is refused.
	if st, _ = readCall(t, c, wire.ReadArgs{Inode: 2, Handle: 9999, Length: 10}); st != uint32(syscall.EBADF) {
		t.Fatalf("unknown handle: status %d", st)
	}
	if st, _ = readCall(t, c, wire.ReadArgs{Inode: 3, Handle: o.Handle, Length: 10}); st != uint32(syscall.EBADF) {
		t.Fatalf("other inode: status %d", st)
	}
	if st, _ = readCall(t, c, wire.ReadArgs{Inode: 2, Handle: o.Handle, Length: wire.MaxReadLen + 1}); st != uint32(syscall.EINVAL) {
		t.Fatalf("too long: status %d", st)
	}
	if r := call(t, c, wire.OpOpen, wire.OpenRequest{Inode: 7}); r.Status != uint32(syscall.ENOENT) {
		t.Fatalf("open missing: %d", r.Status)
	}
	if s.OpenHandles() != 1 {
		t.Fatalf("handles %d, want 1", s.OpenHandles())
	}
	if r := call(t, c, wire.OpRelease, wire.ReleaseRequest{Inode: 2, Handle: o.Handle}); r.Status != 0 {
		t.Fatalf("release: %d", r.Status)
	}
	if r := call(t, c, wire.OpRelease, wire.ReleaseRequest{Inode: 2, Handle: o.Handle}); r.Status != uint32(syscall.EBADF) {
		t.Fatalf("second release: %d", r.Status)
	}
	if st, _ = readCall(t, c, wire.ReadArgs{Inode: 2, Handle: o.Handle, Length: 10}); st != uint32(syscall.EBADF) {
		t.Fatalf("read after release: status %d", st)
	}
	// The connection still serves after all that.
	if r := call(t, c, wire.OpPing, nil); r.Status != 0 {
		t.Fatalf("ping: %d", r.Status)
	}
}

func TestCloseReleasesHandles(t *testing.T) {
	eng := newFakeRead([]byte("x"))
	s, path := serve(t, Config{Engine: eng})
	c, _ := dialHello(t, path, testToken, wire.ProtoMajor)
	for i := 0; i < 3; i++ {
		if r := call(t, c, wire.OpOpen, wire.OpenRequest{Inode: 2}); r.Status != 0 {
			t.Fatal(r.Status)
		}
	}
	s.Close(time.Second)
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.open) != 0 || len(eng.released) != 3 {
		t.Fatalf("after close: open %v released %v", eng.open, eng.released)
	}
}

func TestNoReadEngineAnswersENOSYS(t *testing.T) {
	_, path := serve(t, Config{})
	c, r := dialHello(t, path, testToken, wire.ProtoMajor)
	var h wire.HelloReply
	if json.Unmarshal(r.Body, &h) != nil || strings.Contains(strings.Join(h.Ops, ","), "read") {
		t.Fatalf("a helper with no read path lists read: %+v", h.Ops)
	}
	if r := call(t, c, wire.OpOpen, wire.OpenRequest{Inode: 2}); r.Status != uint32(syscall.ENOSYS) {
		t.Fatalf("open: %d", r.Status)
	}
}

// Closing the connection is the client's cancel: the engine call ends.
func TestClientCloseCancelsRead(t *testing.T) {
	eng := newFakeRead([]byte("abc"))
	eng.block = make(chan struct{})
	eng.canceled = make(chan struct{})
	_, path := serve(t, Config{Engine: eng})
	c, _ := dialHello(t, path, testToken, wire.ProtoMajor)
	var o wire.OpenReply
	if r := call(t, c, wire.OpOpen, wire.OpenRequest{Inode: 2}); json.Unmarshal(r.Body, &o) != nil {
		t.Fatal(r.Status)
	}
	if err := wire.WriteRequest(c, wire.Request{Op: wire.OpRead, Body: wire.ReadArgs{Inode: 2, Handle: o.Handle, Length: 3}.Encode()}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	c.Close()
	select {
	case <-eng.canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("closing the connection did not cancel the read")
	}
}

// A finished watched call leaves the connection usable for the next one.
func TestWatcherLeavesConnectionUsable(t *testing.T) {
	eng := newFakeRead([]byte("hello world"))
	_, path := serve(t, Config{Engine: eng})
	c, _ := dialHello(t, path, testToken, wire.ProtoMajor)
	var o wire.OpenReply
	if r := call(t, c, wire.OpOpen, wire.OpenRequest{Inode: 2}); json.Unmarshal(r.Body, &o) != nil {
		t.Fatal(r.Status)
	}
	for i := 0; i < 200; i++ {
		st, got := readCall(t, c, wire.ReadArgs{Inode: 2, Handle: o.Handle, Offset: 6, Length: 5})
		if st != 0 || string(got) != "world" {
			t.Fatalf("read %d: status %d %q", i, st, got)
		}
	}
}
