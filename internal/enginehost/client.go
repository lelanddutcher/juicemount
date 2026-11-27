package enginehost

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine/wire"
)

// ErrClosed is returned by a client after Close.
var ErrClosed = errors.New("engine client closed")

// StatusError is a non-zero reply status (an errno of this host).
type StatusError struct {
	Op     wire.Op
	Status syscall.Errno
}

func (e *StatusError) Error() string { return fmt.Sprintf("engine %s: %v", e.Op, e.Status) }

// poolSize is the number of idle connections kept per class (design 2.9).
var poolSize = map[wire.Class]int{
	wire.ClassCtl: 4, wire.ClassRead: 32, wire.ClassMeta: 8, wire.ClassWrite: 8, wire.ClassBulk: 2,
}

// Client talks to one helper (one epoch). Each connection carries one request
// at a time; a request takes an idle connection of its class or dials one.
type Client struct {
	path  string
	token []byte
	mu    sync.Mutex
	idle  map[wire.Class][]net.Conn
	shut  bool
}

func newClient(path string, token []byte) *Client {
	return &Client{path: path, token: token, idle: map[wire.Class][]net.Conn{}}
}

// dial opens a connection and completes the hello.
func dialHello(path string, token []byte, major int, deadline time.Time) (net.Conn, wire.HelloReply, error) {
	var h wire.HelloReply
	d := net.Dialer{Deadline: deadline}
	c, err := d.Dial("unix", path)
	if err != nil {
		return nil, h, err
	}
	_ = c.SetDeadline(deadline)
	body, _ := json.Marshal(wire.HelloRequest{ProtoMajor: major, ProtoMinor: wire.ProtoMinor, Token: hex.EncodeToString(token)})
	werr := wire.WriteRequest(c, wire.Request{Op: wire.OpHello, Class: wire.ClassCtl, Body: body})
	// A refused peer gets its answer before it writes; read it either way.
	r, err := wire.ReadReply(c)
	if err != nil && werr != nil {
		err = werr
	}
	if err != nil {
		c.Close()
		return nil, h, err
	}
	if len(r.Body) > 0 {
		_ = json.Unmarshal(r.Body, &h)
	}
	if r.Status != 0 {
		c.Close()
		return nil, h, &StatusError{Op: wire.OpHello, Status: syscall.Errno(r.Status)}
	}
	_ = c.SetDeadline(time.Time{})
	return c, h, nil
}

func (cl *Client) get(class wire.Class, deadline time.Time) (net.Conn, error) {
	cl.mu.Lock()
	if cl.shut {
		cl.mu.Unlock()
		return nil, ErrClosed
	}
	if n := len(cl.idle[class]); n > 0 {
		c := cl.idle[class][n-1]
		cl.idle[class] = cl.idle[class][:n-1]
		cl.mu.Unlock()
		return c, nil
	}
	cl.mu.Unlock()
	c, _, err := dialHello(cl.path, cl.token, wire.ProtoMajor, deadline)
	return c, err
}

func (cl *Client) put(class wire.Class, c net.Conn) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.shut || len(cl.idle[class]) >= poolSize[class] {
		c.Close()
		return
	}
	cl.idle[class] = append(cl.idle[class], c)
}

// Call sends one request and returns the reply body. timeout bounds the
// whole call here and is sent to the helper as the request deadline.
func (cl *Client) Call(class wire.Class, op wire.Op, req interface{}, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	c, err := cl.get(class, deadline)
	if err != nil {
		return nil, err
	}
	var body []byte
	if req != nil {
		if body, err = json.Marshal(req); err != nil {
			cl.put(class, c)
			return nil, err
		}
	}
	_ = c.SetDeadline(deadline)
	if err := wire.WriteRequest(c, wire.Request{Op: op, Class: class, DeadlineMs: uint32(timeout / time.Millisecond), Body: body}); err != nil {
		c.Close()
		return nil, err
	}
	r, err := wire.ReadReply(c)
	if err != nil {
		// Closing the connection is the cancel (design 2.9).
		c.Close()
		return nil, err
	}
	_ = c.SetDeadline(time.Time{})
	if op == wire.OpShutdown {
		c.Close()
	} else {
		cl.put(class, c)
	}
	if r.Status != 0 {
		return nil, &StatusError{Op: op, Status: syscall.Errno(r.Status)}
	}
	return r.Body, nil
}

// Ping answers without the NAS or an engine lock.
func (cl *Client) Ping(timeout time.Duration) (wire.PingReply, error) {
	var p wire.PingReply
	b, err := cl.Call(wire.ClassCtl, wire.OpPing, nil, timeout)
	if err == nil {
		err = json.Unmarshal(b, &p)
	}
	return p, err
}

// Stats returns the helper's counters and engine metrics.
func (cl *Client) Stats(timeout time.Duration) (wire.StatsReply, error) {
	var s wire.StatsReply
	b, err := cl.Call(wire.ClassCtl, wire.OpStats, nil, timeout)
	if err == nil {
		err = json.Unmarshal(b, &s)
	}
	return s, err
}

// GetAttr reads one inode's attributes through the engine.
func (cl *Client) GetAttr(ino uint64, timeout time.Duration) (wire.Attr, error) {
	var a wire.Attr
	b, err := cl.Call(wire.ClassMeta, wire.OpGetAttr, wire.GetAttrRequest{Inode: ino}, timeout)
	if err == nil {
		err = json.Unmarshal(b, &a)
	}
	return a, err
}

// Shutdown asks the helper to stop accepting, flush, close its session and
// exit. It returns once the helper acknowledged.
func (cl *Client) Shutdown(timeout time.Duration) error {
	_, err := cl.Call(wire.ClassCtl, wire.OpShutdown, nil, timeout)
	return err
}

// Close drops every idle connection; later calls fail with ErrClosed.
func (cl *Client) Close() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.shut = true
	for k, cs := range cl.idle {
		for _, c := range cs {
			c.Close()
		}
		delete(cl.idle, k)
	}
}

// Open opens one file read-only by inode (step 2). The handle is valid on
// this helper only, until Release.
func (cl *Client) Open(ino uint64, timeout time.Duration) (wire.OpenReply, error) {
	var o wire.OpenReply
	b, err := cl.Call(wire.ClassRead, wire.OpOpen, wire.OpenRequest{Inode: ino}, timeout)
	if err == nil {
		err = json.Unmarshal(b, &o)
	}
	return o, err
}

// Release closes a handle from Open.
func (cl *Client) Release(ino, handle uint64, timeout time.Duration) error {
	_, err := cl.Call(wire.ClassRead, wire.OpRelease, wire.ReleaseRequest{Inode: ino, Handle: handle}, timeout)
	return err
}

// Read reads into p from off (step 2). The bytes go straight from the
// socket into p. n < len(p) with a nil error means the file ends at off+n.
// timeout bounds the call here and is the helper's deadline; when it
// passes, the connection is closed, which cancels the engine call.
func (cl *Client) Read(ino, handle uint64, p []byte, off int64, timeout time.Duration) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(p) > wire.MaxReadLen || off < 0 {
		return 0, syscall.EINVAL
	}
	deadline := time.Now().Add(timeout)
	c, err := cl.get(wire.ClassRead, deadline)
	if err != nil {
		return 0, err
	}
	_ = c.SetDeadline(deadline)
	body := wire.ReadArgs{Inode: ino, Handle: handle, Offset: uint64(off), Length: uint32(len(p))}.Encode()
	if err := wire.WriteRequest(c, wire.Request{Op: wire.OpRead, Class: wire.ClassRead,
		DeadlineMs: uint32(timeout / time.Millisecond), Body: body}); err != nil {
		c.Close()
		return 0, err
	}
	status, n, err := wire.ReadReplyInto(c, p)
	if err != nil {
		c.Close() // the cancel; and a torn reply never returns to the pool
		return 0, err
	}
	_ = c.SetDeadline(time.Time{})
	cl.put(wire.ClassRead, c)
	if status != 0 {
		return 0, &StatusError{Op: wire.OpRead, Status: syscall.Errno(status)}
	}
	return n, nil
}

// Unwrap lets errors.Is match the errno.
func (e *StatusError) Unwrap() error { return e.Status }

// EngineStatus is the errno the engine answered (as opposed to an errno of
// the socket itself, which a net error also carries).
func (e *StatusError) EngineStatus() syscall.Errno { return e.Status }
