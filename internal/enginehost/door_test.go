package enginehost

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine"
	"github.com/juicedata/juicefs/pkg/jmengine/wire"
)

type doorEngine struct{ data []byte }

func (doorEngine) VolumeID() string   { return "vol" }
func (doorEngine) VolumeName() string { return "vol" }
func (doorEngine) SessionID() uint64  { return 7 }
func (doorEngine) ReadOnly() bool     { return false }
func (doorEngine) Metrics() string    { return "" }
func (doorEngine) GetAttr(ctx context.Context, ino uint64) (wire.Attr, syscall.Errno) {
	return wire.Attr{Inode: ino}, 0
}
func (e doorEngine) Open(ctx context.Context, ino uint64) (uint64, wire.Attr, syscall.Errno) {
	if ino != 2 {
		return 0, wire.Attr{}, syscall.ENOENT
	}
	return 99, wire.Attr{Inode: 2, Size: uint64(len(e.data))}, 0
}
func (e doorEngine) Read(ctx context.Context, ino, fh uint64, buf []byte, off uint64) (int, syscall.Errno) {
	if off >= uint64(len(e.data)) {
		return 0, 0
	}
	return copy(buf, e.data[off:]), 0
}
func (doorEngine) Release(ino, fh uint64) {}

// fakeMount stands in for `juicefs mount` with the door flags: it takes
// duplicates of the descriptors the child would inherit, then serves.
func fakeMount(t *testing.T, files []*os.File, eng jmengine.Engine, version string) func() {
	t.Helper()
	dup := func(f *os.File) *os.File {
		fd, err := syscall.Dup(int(f.Fd()))
		if err != nil {
			t.Fatal(err)
		}
		return os.NewFile(uintptr(fd), f.Name())
	}
	start, lf := dup(files[0]), dup(files[1])
	done := make(chan struct{})
	go func() {
		defer close(done)
		msg, err := wire.ReadStartMessage(start)
		if err != nil {
			return
		}
		ln, err := net.FileListener(lf)
		lf.Close()
		if err != nil {
			return
		}
		tok := make([]byte, 32)
		for i := range tok {
			tok[i] = hexNibble(msg.Token[2*i])<<4 | hexNibble(msg.Token[2*i+1])
		}
		srv := jmengine.New(jmengine.Config{Listener: ln.(*net.UnixListener), Token: tok, Engine: eng,
			Version: version, Epoch: uint64(time.Now().UnixNano()), AllowPeer: func(int, int) bool { return true }})
		go srv.Serve()
		buf := make([]byte, 1)
		_, _ = start.Read(buf) // until the app closes the pipe
		srv.Close(time.Second)
	}()
	// The fake mount stops when the door closes its pipe (Prepare of the
	// next launch, or Close): wait for that.
	return func() { <-done; start.Close() }
}

func hexNibble(c byte) byte {
	if c >= 'a' {
		return c - 'a' + 10
	}
	return c - '0'
}

func TestDoorReadsThroughTheMountSocket(t *testing.T) {
	d := NewDoor(DoorConfig{SocketBase: shortTempDir(t), ExpectVersion: "v-jm"})
	defer d.Close()
	args, files, err := d.Prepare(3)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(args, " ") != "--jm-door-start-fd 3 --jm-door-listen-fd 4" || len(files) != 2 {
		t.Fatalf("args %v files %d", args, len(files))
	}
	if _, _, ok := d.Current(); ok {
		t.Fatal("ready before the mount started")
	}
	data := bytes.Repeat([]byte("frame"), 1000)
	stop := fakeMount(t, files, doorEngine{data: data}, "v-jm")
	d.Started(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, err := d.WaitReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cl, epoch, ok := d.Current()
	if !ok || epoch != h.Epoch {
		t.Fatalf("current: ok %v epoch %d hello %d", ok, epoch, h.Epoch)
	}
	o, err := cl.Open(2, time.Second)
	if err != nil || o.Attr.Size != uint64(len(data)) {
		t.Fatalf("open: %+v %v", o, err)
	}
	p := make([]byte, 4096)
	if n, err := cl.Read(2, o.Handle, p, 10, time.Second); err != nil || n != 4096 || !bytes.Equal(p, data[10:4106]) {
		t.Fatalf("read: %d %v", n, err)
	}
	if n, err := cl.Read(2, o.Handle, p, int64(len(data)-3), time.Second); err != nil || n != 3 {
		t.Fatalf("read at the end: %d %v", n, err)
	}
	_, err = cl.Read(2, 12345, p, 0, time.Second)
	var se *StatusError
	if !errors.As(err, &se) || se.EngineStatus() != syscall.EBADF {
		t.Fatalf("unknown handle: %v", err)
	}
	if err := cl.Release(2, o.Handle, time.Second); err != nil {
		t.Fatal(err)
	}

	// A remount is a new launch: the old client is closed, a new epoch.
	_, files2, err := d.Prepare(3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Open(2, time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("old client after a new launch: %v", err)
	}
	stop() // the first launch's pipe closed with the new Prepare
	stop2 := fakeMount(t, files2, doorEngine{data: data}, "v-jm")
	d.Started(nil)
	h2, err := d.WaitReady(ctx)
	if err != nil || h2.Epoch == h.Epoch {
		t.Fatalf("second launch: %+v %v", h2, err)
	}
	d.Close()
	stop2()
}

func TestDoorRefusesAnotherVersionOrNoReads(t *testing.T) {
	for _, tc := range []struct {
		name    string
		eng     jmengine.Engine
		version string
	}{
		{"version", doorEngine{}, "v-other"},
		{"no read ops", noReadEngine{}, "v-jm"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDoor(DoorConfig{SocketBase: shortTempDir(t), ExpectVersion: "v-jm"})
			_, files, err := d.Prepare(3)
			if err != nil {
				t.Fatal(err)
			}
			stop := fakeMount(t, files, tc.eng, tc.version)
			defer stop()
			defer d.Close() // runs first: the fake mount sees the pipe close
			d.Started(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := d.WaitReady(ctx); !errors.Is(err, ErrRefused) {
				t.Fatalf("want refused, got %v", err)
			}
			if _, _, ok := d.Current(); ok {
				t.Fatal("a refused door is current")
			}
		})
	}
}

type noReadEngine struct{}

func (noReadEngine) VolumeID() string   { return "vol" }
func (noReadEngine) VolumeName() string { return "vol" }
func (noReadEngine) SessionID() uint64  { return 0 }
func (noReadEngine) ReadOnly() bool     { return true }
func (noReadEngine) Metrics() string    { return "" }
func (noReadEngine) GetAttr(ctx context.Context, ino uint64) (wire.Attr, syscall.Errno) {
	return wire.Attr{}, 0
}

func TestDoorStartFailureClosesFiles(t *testing.T) {
	d := NewDoor(DoorConfig{SocketBase: shortTempDir(t)})
	defer d.Close()
	_, files, err := d.Prepare(3)
	if err != nil {
		t.Fatal(err)
	}
	d.Started(errors.New("exec failed"))
	if _, err := files[0].Stat(); err == nil {
		t.Fatal("the child's ends stay open after a failed start")
	}
	if _, err := d.WaitReady(context.Background()); err == nil {
		t.Fatal("a failed start reported ready")
	}
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "jd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
