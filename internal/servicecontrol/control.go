// Package servicecontrol carries one bounded control operation per Unix socket
// connection. It is a same-user process boundary, not mount ownership proof.
package servicecontrol

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"golang.org/x/sys/unix"
)

const (
	ProtocolVersion = 1
	maxPayload      = 1 << 20
	maxSocketPath   = 103 // below Darwin's sockaddr_un.sun_path capacity
	maxConnections  = 32
	requestTimeout  = 5 * time.Second
	replyTimeout    = 5 * time.Second
)

var ErrClosed = errors.New("service control listener closed")

type Request struct {
	Version int      `json:"version"`
	Method  string   `json:"method"`
	Build   string   `json:"build"`
	Strings []string `json:"strings"`
	Ints    []int    `json:"ints"`
}

type Response struct {
	Version int    `json:"version"`
	Text    string `json:"text"`
	Int     int    `json:"int"`
	Error   string `json:"error"`
}

// SocketPath derives the per-user service address without creating files.
func SocketPath() (string, error) {
	scope, isolated, err := appisolation.Current()
	if err != nil {
		return "", err
	}
	if isolated {
		path := filepath.Join(scope.Root, "s.sock")
		if err := validatePath(path); err != nil {
			return "", err
		}
		return path, nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	path := filepath.Join(base, "JuiceMount", "service", "control.sock")
	if err := validatePath(path); err != nil {
		return "", err
	}
	return path, nil
}

func validatePath(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.IndexByte(path, 0) >= 0 || len(path) > maxSocketPath || filepath.Base(path) == "." {
		return errors.New("service control requires a canonical absolute short socket path")
	}
	return nil
}

// dialChecked is Call up to the first write: argument checks, the dial and
// the peer check. It returns the connection with ctx's deadline set.
func dialChecked(ctx context.Context, path string, req Request) (net.Conn, error) {
	if !platformSupported() {
		return nil, errors.New("service control peer authentication unsupported on this platform")
	}
	if ctx == nil {
		return nil, errors.New("service control call requires context")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("service control call requires deadline")
	}
	if err := validatePath(path); err != nil {
		return nil, err
	}
	if req.Version != ProtocolVersion || req.Method == "" {
		return nil, errors.New("invalid service control request")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		return nil, errors.New("service control requires Unix connection")
	}
	if err := requireSameUID(unixConn); err != nil {
		conn.Close()
		return nil, err
	}
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// ErrNotSent marks a Call error returned before any byte of the request was
// written (bad arguments, no socket to dial, a peer of another user). Only
// then may a caller try another route; after it, the outcome is unknown.
var ErrNotSent = errors.New("service control request not sent")

// notSent wraps a pre-send error; its text is unchanged.
type notSent struct{ err error }

func (e notSent) Error() string        { return e.err.Error() }
func (e notSent) Unwrap() error        { return e.err }
func (e notSent) Is(target error) bool { return target == ErrNotSent }

// Call sends exactly once. A canceled or lost reply is ambiguous to the
// caller; this function never retries an operation, including a read method.
// ctx must carry a deadline so a dead service cannot park its controller.
// Response.Error is the service's application error, not a transport retry cue.
// An error that matches ErrNotSent (errors.Is) means nothing was sent.
func Call(ctx context.Context, path string, req Request) (Response, error) {
	return callWithPeerPID(ctx, path, req, 0)
}

// CallWithPeerPID admits an explicitly identified service before writing. Used
// by upgrade clients; the ordinary transport retains its same-UID API.
func CallWithPeerPID(ctx context.Context, path string, req Request, pid int) (Response, error) {
	if pid <= 0 {
		return Response{}, notSent{errors.New("invalid expected service PID")}
	}
	return callWithPeerPID(ctx, path, req, pid)
}

func callWithPeerPID(ctx context.Context, path string, req Request, pid int) (Response, error) {
	conn, err := dialChecked(ctx, path, req)
	if err != nil {
		return Response{}, notSent{err}
	}
	defer conn.Close()
	if pid != 0 {
		uc, ok := conn.(*net.UnixConn)
		if !ok {
			return Response{}, notSent{errors.New("service peer is not Unix")}
		}
		got, err := peerPID(uc)
		if err != nil || got != pid {
			return Response{}, notSent{errors.New("service peer PID changed")}
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()
	if err := writeFrame(conn, req); err != nil {
		return Response{}, err
	}
	data, err := readFrame(conn)
	if err != nil {
		return Response{}, err
	}
	var resp Response
	if err := decodeStrict(data, &resp); err != nil {
		return Response{}, err
	}
	if resp.Version != ProtocolVersion {
		return Response{}, errors.New("service control response version mismatch")
	}
	return resp, nil
}

func writeFrame(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > maxPayload {
		return errors.New("service control payload exceeds bound")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, data)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func readFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > maxPayload {
		return nil, errors.New("service control frame size invalid")
	}
	data := make([]byte, size)
	_, err := io.ReadFull(r, data)
	return data, err
}

func decodeStrict(data []byte, out any) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var required []string
	switch out.(type) {
	case *Request:
		required = []string{"version", "method", "build", "strings", "ints"}
	case *Response:
		required = []string{"version", "text", "int", "error"}
	default:
		return errors.New("unsupported service control message")
	}
	if len(fields) != len(required) {
		return errors.New("incomplete service control message")
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || (bytes.Equal(bytes.TrimSpace(value), []byte("null")) && name != "strings" && name != "ints") {
			return fmt.Errorf("missing/null service control field %s", name)
		}
	}
	if err := rejectDuplicateKeys(data); err != nil {
		return fmt.Errorf("service control JSON: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("service control JSON: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("service control JSON has trailing data")
	}
	return nil
}

func rejectDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := consumeUnique(dec, 0); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}

func consumeUnique(dec *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("JSON nesting exceeds bound")
	}
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object key is not string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeUnique(dec, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := consumeUnique(dec, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = dec.Token()
	return err
}

type Server struct {
	mu       sync.Mutex
	listener *net.UnixListener
	root     *os.Root
	dir      *os.File
	lock     *os.File
	path     string
	name     string
	sockInfo os.FileInfo
	closed   bool
	started  bool
	serveEnd bool
	pending  map[*net.UnixConn]struct{}
	active   int
	handlers int
	slots    chan struct{}
	drained  chan struct{}
}

// Listen owns a permanent-name flock file for the entire lifetime of every
// admitted handler. A stale socket can be removed only while that lock is held.
func Listen(path string) (*Server, error) {
	if !platformSupported() {
		return nil, errors.New("service control peer authentication unsupported on this platform")
	}
	if err := validatePath(path); err != nil {
		return nil, err
	}
	directory := filepath.Dir(path)
	if err := ensureDirectory(directory); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	s := &Server{root: root, path: path, name: filepath.Base(path), pending: make(map[*net.UnixConn]struct{}), slots: make(chan struct{}, maxConnections), drained: make(chan struct{}), serveEnd: true}
	fail := func(err error) (*Server, error) {
		if s.listener != nil {
			_ = s.listener.Close()
		}
		if s.sockInfo != nil {
			if current, statErr := root.Lstat(s.name); statErr == nil && os.SameFile(s.sockInfo, current) {
				_ = root.Remove(s.name)
			}
		}
		if s.lock != nil {
			_ = s.lock.Close()
		}
		if s.dir != nil {
			_ = s.dir.Close()
		}
		_ = root.Close()
		return nil, err
	}
	s.dir, err = root.Open(".")
	if err != nil {
		return fail(err)
	}
	if err := privateFile(s.dir, true); err != nil {
		return fail(err)
	}
	s.lock, err = privatefile.OpenAt(s.dir, s.name+".lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fail(err)
	}
	if err := privateFile(s.lock, false); err != nil {
		return fail(err)
	}
	if err := unix.Flock(int(s.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fail(fmt.Errorf("service control already owned: %w", err))
	}
	if err := s.removeStaleSocket(); err != nil {
		return fail(err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return fail(err)
	}
	s.listener = listener
	// net.UnixListener otherwise unlinks the pathname on Close without
	// checking whether another inode replaced it in the meantime.
	listener.SetUnlinkOnClose(false)
	s.sockInfo, err = root.Lstat(s.name)
	if err != nil {
		return fail(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		return fail(err)
	}
	current, err := root.Lstat(s.name)
	if err != nil {
		return fail(err)
	}
	if !os.SameFile(s.sockInfo, current) || current.Mode()&os.ModeSocket == 0 {
		return fail(errors.New("service control socket did not bind safely"))
	}
	if err := privateSocket(current); err != nil {
		return fail(err)
	}
	return s, nil
}

func privateFile(f *os.File, directory bool) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || st.Mode().Perm()&0077 != 0 || st.IsDir() != directory || (!directory && (!st.Mode().IsRegular() || stat.Nlink != 1)) {
		return errors.New("service control requires private singly linked files owned by this user")
	}
	return nil
}

func privateSocket(st os.FileInfo) error {
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || st.Mode()&os.ModeSocket == 0 || int(stat.Uid) != os.Geteuid() || st.Mode().Perm()&0077 != 0 || stat.Nlink != 1 {
		return errors.New("service control socket is not private and owned by this user")
	}
	return nil
}

func ensureDirectory(path string) error {
	err := os.Mkdir(path, 0700)
	if errors.Is(err, os.ErrNotExist) {
		if err := ensureDirectory(filepath.Dir(path)); err != nil {
			return err
		}
		err = os.Mkdir(path, 0700)
	}
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	st, err := os.Lstat(path)
	if err != nil || !st.IsDir() {
		return errors.New("service control directory is absent or a symlink")
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}

func (s *Server) removeStaleSocket() error {
	st, err := s.root.Lstat(s.name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := privateSocket(st); err != nil {
		return err
	}
	// The lock is the cooperating-service exclusion, but do not replace an
	// unrelated responsive same-user listener if its lockfile was lost.
	probe, dialErr := net.DialTimeout("unix", s.path, 100*time.Millisecond)
	if dialErr == nil {
		_ = probe.Close()
		return errors.New("service control socket is still responsive")
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("service control socket occupancy unknown: %w", dialErr)
	}
	current, err := s.root.Lstat(s.name)
	if err != nil || !os.SameFile(st, current) {
		return errors.New("service control socket changed during stale check")
	}
	return s.root.Remove(s.name)
}

// Serve admits independent handlers. A client disconnect after a complete
// request does not cancel an accepted operation. Serve may be called once.
func (s *Server) Serve(handler func(Request) Response) error {
	return s.ServeWithReplyHook(handler, nil)
}

// ServeWithReplyHook observes a completed response write. A failed write is
// ambiguous to the client and must not silently retire an original operation.
func (s *Server) ServeWithReplyHook(handler func(Request) Response, afterReply func(Request, Response, error)) error {
	if handler == nil {
		return errors.New("service control requires handler")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.started {
		s.mu.Unlock()
		return errors.New("service control Serve already called")
	}
	s.started, s.serveEnd = true, false
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.serveEnd = true
		s.maybeReleaseLocked()
		s.mu.Unlock()
	}()
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if err := requireSameUID(conn); err != nil {
			_ = conn.Close()
			continue
		}
		select {
		case s.slots <- struct{}{}:
		default:
			// Closing is a bounded transport rejection; no handler ran and
			// the caller must still decide whether its attempt is ambiguous.
			_ = conn.Close()
			continue
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			<-s.slots
			_ = conn.Close()
			continue
		}
		s.pending[conn] = struct{}{}
		s.handlers++
		s.mu.Unlock()
		go s.serveConn(conn, handler, afterReply)
	}
}

func (s *Server) serveConn(conn *net.UnixConn, handler func(Request) Response, afterReply func(Request, Response, error)) {
	active := false
	defer func() {
		_ = conn.Close()
		<-s.slots
		s.mu.Lock()
		delete(s.pending, conn)
		if active {
			s.active--
		}
		s.handlers--
		s.maybeReleaseLocked()
		s.mu.Unlock()
	}()
	_ = conn.SetReadDeadline(time.Now().Add(requestTimeout))
	data, err := readFrame(conn)
	if err != nil {
		s.dropPending(conn)
		return
	}
	var req Request
	if err := decodeStrict(data, &req); err != nil || req.Version != ProtocolVersion || req.Method == "" {
		s.dropPending(conn)
		return
	}
	s.mu.Lock()
	delete(s.pending, conn)
	if s.closed {
		s.maybeReleaseLocked()
		s.mu.Unlock()
		return
	}
	s.active++
	active = true
	s.mu.Unlock()
	// No context, socket read, or client liveness is consulted past this
	// point. The engine operation must finish on its own durable terms.
	resp := handler(req)
	resp.Version = ProtocolVersion
	_ = conn.SetDeadline(time.Now().Add(replyTimeout))
	writeErr := writeFrame(conn, resp)
	if afterReply != nil {
		afterReply(req, resp, writeErr)
	}
}

func (s *Server) dropPending(conn *net.UnixConn) {
	s.mu.Lock()
	delete(s.pending, conn)
	s.mu.Unlock()
}

// Close stops new admission immediately. It neither invokes engine teardown
// nor waits for handlers. The lock stays held until all admitted work finishes.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	_ = s.listener.Close()
	for conn := range s.pending {
		_ = conn.Close()
	}
	err := s.removeOwnSocket()
	s.maybeReleaseLocked()
	s.mu.Unlock()
	return err
}

// WaitClosed joins the original listener and every admitted handler, including
// the final reply connection, after Close has stopped admission.
func (s *Server) WaitClosed(ctx context.Context) error {
	if s == nil || ctx == nil {
		return errors.New("service control close join requires server and context")
	}
	select {
	case <-s.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) removeOwnSocket() error {
	st, err := s.root.Lstat(s.name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !os.SameFile(s.sockInfo, st) {
		return errors.New("service control socket was replaced")
	}
	return s.root.Remove(s.name)
}

func (s *Server) maybeReleaseLocked() {
	if !s.closed || !s.serveEnd || len(s.pending) != 0 || s.active != 0 || s.handlers != 0 || s.lock == nil {
		return
	}
	_ = s.lock.Close()
	s.lock = nil
	_ = s.dir.Close()
	s.dir = nil
	_ = s.root.Close()
	s.root = nil
	close(s.drained)
}
