package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type heldReadOpenFS struct {
	*eofTestFS
	entered chan string
	release chan struct{}
	err     error
}

func (f *heldReadOpenFS) Open(name string) (billy.File, error) {
	f.entered <- name
	<-f.release
	if f.err != nil {
		return nil, f.err
	}
	return f.eofTestFS.Open(name)
}

func readOpenTraceResponse(t *testing.T, off uint64) *response {
	t.Helper()
	var args bytes.Buffer
	if err := xdr.Write(&args, nfsReadArgs{Handle: []byte{1}, Offset: off, Count: 4}); err != nil {
		t.Fatal(err)
	}
	w := newTestResponse(args.Bytes())
	w.req.xid = 0x12345678
	w.req.Header.Prog, w.req.Header.Proc = nfsServiceID, uint32(NFSProcedureRead)
	return w
}

func TestReadOpenTraceHeldFailurePreservesWireStatus(t *testing.T) {
	fs := &heldReadOpenFS{
		eofTestFS: &eofTestFS{}, entered: make(chan string, 1), release: make(chan struct{}),
		err: fmt.Errorf("held open: %w", syscall.EAGAIN),
	}
	w := readOpenTraceResponse(t, 8)
	logs, done := make(chan string, 1), make(chan error, 1)
	var releaseOnce sync.Once
	finished := false
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(fs.release) })
		if !finished {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("original held Open did not join")
			}
		}
	})
	go func() {
		done <- onReadWithOpenTrace(context.Background(), w, &mockHandler{fs: fs}, true,
			func(format string, args ...interface{}) { logs <- fmt.Sprintf(format, args...) })
	}()
	select {
	case name := <-fs.entered:
		if name != "f.bin" {
			t.Fatalf("Open name=%q", name)
		}
	case <-time.After(time.Second):
		t.Fatal("Open was not entered")
	}
	select {
	case line := <-logs:
		t.Fatalf("trace emitted before Open settled: %q", line)
	default:
	}
	releaseOnce.Do(func() { close(fs.release) })
	var err error
	select {
	case err = <-done:
		finished = true
	case <-time.After(time.Second):
		t.Fatal("Open did not return after release")
	}
	var status *NFSStatusError
	if !errors.As(err, &status) || status.NFSStatus != NFSStatusJukebox ||
		!errors.Is(err, syscall.EAGAIN) || w.writer.Len() != 0 {
		t.Fatalf("READ before formatter err=%v body=%d", err, w.writer.Len())
	}
	var line string
	select {
	case line = <-logs:
	default:
		t.Fatal("settled Open produced no opt-in trace")
	}
	for _, field := range []string{"TRACE read open", "xid=305419896", `path="f.bin"`,
		"offset=8", "count=4", "elapsed_ms=", "result=error", "held open: resource temporarily unavailable"} {
		if !strings.Contains(line, field) {
			t.Fatalf("trace %q missing %q", line, field)
		}
	}
	if err := w.conn.err(context.Background(), w, err); err != nil {
		t.Fatal(err)
	}
	body := replyBodyAfterRPCHeader(t, w.writer.Bytes())
	if len(body) != 8 || binary.BigEndian.Uint32(body[:4]) != uint32(NFSStatusJukebox) ||
		binary.BigEndian.Uint32(body[4:]) != 0 {
		t.Fatalf("READ3resfail=%x", body)
	}
}

func TestReadOpenTraceSuccessAndDisabled(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		f := &eofTestFile{body: []byte("body"), cached: 4}
		fs := &eofTestFS{file: f, size: 4}
		w := readOpenTraceResponse(t, 0)
		logs := make([]string, 0, 1)
		logf := func(format string, args ...interface{}) {
			if !enabled {
				t.Fatal("disabled trace invoked logger")
			}
			logs = append(logs, fmt.Sprintf(format, args...))
		}
		if err := onReadWithOpenTrace(context.Background(), w, &mockHandler{fs: fs}, enabled, logf); err != nil {
			t.Fatal(err)
		}
		if f.closeCalls != 1 || len(logs) != map[bool]int{true: 1, false: 0}[enabled] {
			t.Fatalf("enabled=%v close=%d logs=%v", enabled, f.closeCalls, logs)
		}
		if enabled && (!strings.Contains(logs[0], `path="f.bin" offset=0 count=4`) ||
			!strings.Contains(logs[0], "result=ok err=<nil>")) {
			t.Fatalf("success trace=%q", logs[0])
		}
		r := bytes.NewReader(replyBodyAfterRPCHeader(t, w.writer.Bytes()))
		if status := readU32(t, r); status != 0 {
			t.Fatalf("status=%d", status)
		}
		if readU32(t, r) == 1 {
			skipN(t, r, 84)
		}
		var got nfsReadResponse
		if err := xdr.Read(r, &got); err != nil {
			t.Fatal(err)
		}
		if got.Count != 4 || !bytes.Equal(got.Data, []byte("body")) || r.Len() != 0 {
			t.Fatalf("enabled=%v READ count=%d body=%q trailing=%d", enabled, got.Count, got.Data, r.Len())
		}
	}
}

func TestReadOpenTraceDisabledFailure(t *testing.T) {
	w := readOpenTraceResponse(t, 4)
	fs := &retryOpenFS{eofTestFS: &eofTestFS{}, err: syscall.EAGAIN}
	err := onReadWithOpenTrace(context.Background(), w, &mockHandler{fs: fs}, false,
		func(string, ...interface{}) { t.Fatal("disabled failure invoked logger") })
	var status *NFSStatusError
	if !errors.As(err, &status) || status.NFSStatus != NFSStatusJukebox || w.writer.Len() != 0 {
		t.Fatalf("disabled READ before formatter err=%v body=%d", err, w.writer.Len())
	}
	if err := w.conn.err(context.Background(), w, err); err != nil {
		t.Fatal(err)
	}
	body := replyBodyAfterRPCHeader(t, w.writer.Bytes())
	if len(body) != 8 || binary.BigEndian.Uint32(body[:4]) != uint32(NFSStatusJukebox) ||
		binary.BigEndian.Uint32(body[4:]) != 0 {
		t.Fatalf("disabled READ3resfail=%x", body)
	}
}

var _ billy.Filesystem = (*heldReadOpenFS)(nil)
