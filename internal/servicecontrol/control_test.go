package servicecontrol

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testPath(t *testing.T) string {
	t.Helper()
	// A Unix socket path has a small kernel limit. The build train supplies a
	// private TMPDIR; use a short child rather than t.TempDir's test-name path.
	directory, err := os.MkdirTemp(os.TempDir(), "sc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "s")
	if err := validatePath(path); err != nil {
		t.Fatal(err)
	}
	return path
}

func startServer(t *testing.T, handler func(Request) Response) (*Server, string) {
	t.Helper()
	path := testPath(t)
	s, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- s.Serve(handler) }()
	t.Cleanup(func() {
		_ = s.Close()
		select {
		case err := <-finished:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("Serve did not stop")
		}
		deadline := time.Now().Add(time.Second)
		for {
			s.mu.Lock()
			retired := s.lock == nil
			s.mu.Unlock()
			if retired {
				break
			}
			if time.Now().After(deadline) {
				t.Error("accepted handler still owns lifetime lock at cleanup")
				break
			}
			time.Sleep(time.Millisecond)
		}
	})
	return s, path
}

func callWithTimeout(path, method string) (Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return Call(ctx, path, Request{Version: ProtocolVersion, Method: method, Build: "fixture"})
}

func TestServiceControlCallAndApplicationError(t *testing.T) {
	_, path := startServer(t, func(req Request) Response {
		if req.Build != "fixture" {
			return Response{Error: "wrong build"}
		}
		return Response{Text: req.Method, Int: len(req.Strings), Error: "application failure"}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := Call(ctx, path, Request{Version: ProtocolVersion, Method: "status", Build: "fixture", Strings: []string{"x"}})
	if err != nil || resp.Version != ProtocolVersion || resp.Text != "status" || resp.Int != 1 || resp.Error != "application failure" {
		t.Fatalf("response=%+v transport=%v", resp, err)
	}
	if _, err := Call(context.Background(), path, Request{Version: ProtocolVersion, Method: "status"}); err == nil {
		t.Fatal("unbounded call was admitted")
	}
}

func TestServiceControlRejectsMalformedFramesBeforeHandler(t *testing.T) {
	var calls atomic.Int32
	_, path := startServer(t, func(Request) Response { calls.Add(1); return Response{} })
	cases := [][]byte{
		[]byte(`{"version":1,"method":"x","build":"","strings":[],"ints":[],"extra":1}`),
		[]byte(`{"version":1,"version":1,"method":"x"}`),
		[]byte(`{"version":2,"method":"x"}`),
		[]byte(`{"version":1,"method":"x"} {}`),
	}
	for _, body := range cases {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(body)))
		if _, err := conn.Write(append(header[:], body...)); err != nil {
			t.Fatal(err)
		}
		if _, err := readFrame(conn); err == nil {
			t.Fatalf("malformed request got a reply: %s", body)
		}
		_ = conn.Close()
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], maxPayload+1)
	if _, err := conn.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrame(conn); err == nil {
		t.Fatal("oversized request got a reply")
	}
	_ = conn.Close()
	if calls.Load() != 0 {
		t.Fatalf("invalid requests reached handler %d times", calls.Load())
	}
}

func TestServiceControlCallRejectsMalformedResponse(t *testing.T) {
	for name, payload := range map[string][]byte{
		"missing":    []byte(`{"version":1}`),
		"null-int":   []byte(`{"version":1,"text":"","int":null,"error":""}`),
		"wrong-case": []byte(`{"Version":1,"text":"","int":0,"error":""}`),
		"unknown":    []byte(`{"version":1,"text":"x","extra":1}`),
		"duplicate":  []byte(`{"version":1,"version":1}`),
		"version":    []byte(`{"version":2}`),
		"trailing":   []byte(`{"version":1} {}`),
	} {
		t.Run(name, func(t *testing.T) {
			path := testPath(t)
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_, _ = readFrame(conn)
				var header [4]byte
				binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
				_, _ = conn.Write(append(header[:], payload...))
			}()
			if _, err := callWithTimeout(path, "status"); err == nil {
				t.Fatal("malformed response accepted")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("fake peer did not finish")
			}
		})
	}
}

func TestServiceControlListenRefusesCollisionAndUnsafeFiles(t *testing.T) {
	path := testPath(t)
	s, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("second service acquired lifetime lock")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Close left socket: %v", err)
	}
	if st, err := os.Lstat(path + ".lock"); err != nil || !st.Mode().IsRegular() {
		t.Fatalf("permanent lifetime lock was removed: %v", err)
	}
	// A previous same-user service may have died after binding. Its stale
	// socket may be removed only once the persistent lock is acquired.
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	_ = stale.Close()
	reopened, err := Listen(path)
	if err != nil {
		t.Fatalf("stale same-user socket was not retired: %v", err)
	}
	_ = reopened.Close()
	occupied, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	occupied.SetUnlinkOnClose(false)
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("responsive same-user socket was replaced")
	}
	_ = occupied.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte("not a socket"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("regular socket-path occupant was removed")
	}
	_ = os.Remove(path)
	if err := os.Symlink("missing", path); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("socket symlink was removed")
	}
}

func TestServiceControlRefusesInsecureDirectoryAndHardlinkedLock(t *testing.T) {
	path := testPath(t)
	dir := filepath.Dir(path)
	alias := filepath.Join(dir, "d")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(filepath.Join(alias, "s")); err == nil {
		t.Fatal("service directory symlink admitted")
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("insecure service directory admitted")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	lock := path + ".lock"
	if err := os.WriteFile(lock, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(lock, filepath.Join(dir, "alias")); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("hardlinked lifetime lock admitted")
	}
	if err := os.Remove(filepath.Join(dir, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", lock); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("symlink lifetime lock admitted")
	}
	if _, err := os.Lstat(filepath.Join(dir, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target was created before refusal: %v", err)
	}
}

func TestServiceControlBusyAdmissionIsBounded(t *testing.T) {
	s, path := startServer(t, func(Request) Response { return Response{} })
	connections := make([]net.Conn, 0, maxConnections)
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	for i := 0; i < maxConnections; i++ {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
	}
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		pending := len(s.pending)
		s.mu.Unlock()
		if pending == maxConnections {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d idle clients admitted", pending)
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	if _, err := callWithTimeout(path, "status"); err == nil {
		t.Fatal("overloaded service admitted another operation")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("overload refusal took %v", elapsed)
	}
}

func TestServiceControlCanceledClientAndCloseRetainAcceptedOperation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	s, path := startServer(t, func(req Request) Response {
		if req.Method == "status" {
			return Response{Text: "ready"}
		}
		close(entered)
		<-release
		close(finished)
		return Response{Text: "committed"}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	result := make(chan error, 1)
	go func() {
		_, err := Call(ctx, path, Request{Version: ProtocolVersion, Method: "mutation"})
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("mutation never entered handler")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled client got success")
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("canceled client did not return")
	}
	resp, err := callWithTimeout(path, "status")
	if err != nil || resp.Text != "ready" {
		close(release)
		t.Fatalf("status blocked behind mutation: %+v %v", resp, err)
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Close waited for accepted engine operation")
	}
	if _, err := Listen(path); err == nil {
		close(release)
		t.Fatal("successor acquired lock during accepted operation")
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("client cancellation ended accepted operation")
	}
	deadline := time.Now().Add(time.Second)
	for {
		next, err := Listen(path)
		if err == nil {
			_ = next.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("completed operation did not release lifetime lock: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestServiceControlFrameBoundsAndExactNoRetry(t *testing.T) {
	if _, err := readFrame(strings.NewReader("\x00\x10\x00\x01")); err == nil {
		t.Fatal("oversized frame accepted")
	}
	if _, err := readFrame(strings.NewReader("\x00\x00\x00\x05hi")); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated frame: %v", err)
	}
	var calls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	_, path := startServer(t, func(Request) Response {
		calls.Add(1)
		close(entered)
		<-release
		return Response{Text: "done"}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	result := make(chan error, 1)
	go func() {
		_, err := Call(ctx, path, Request{Version: ProtocolVersion, Method: "once"})
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("request never entered handler")
	}
	cancel()
	if err := <-result; err == nil {
		close(release)
		t.Fatal("lost reply was treated as success")
	}
	close(release)
	if calls.Load() != 1 {
		t.Fatalf("ambiguous operation executed %d times", calls.Load())
	}
}

// ErrNotSent marks exactly the failures before the request was written: a
// caller may try another route only then.
func TestCallErrNotSentOnlyBeforeTheWrite(t *testing.T) {
	path := testPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := Call(ctx, path, Request{Version: ProtocolVersion, Method: "ready"})
	if !errors.Is(err, ErrNotSent) {
		t.Fatalf("no socket: %v, want ErrNotSent", err)
	}
	srv, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	go srv.Serve(func(Request) Response { time.Sleep(time.Second); return Response{Version: ProtocolVersion} })
	_, err = Call(ctx, path, Request{Version: ProtocolVersion, Method: "ready"})
	if err == nil || errors.Is(err, ErrNotSent) {
		t.Fatalf("lost reply after the write: %v, must not be ErrNotSent", err)
	}
}
