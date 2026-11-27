package enginehost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine/wire"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// Door is the engine socket inside one `juicefs mount` process (engine-
// direct step 2, level "reads"; design 7.2 "both doors"). The mount and the
// socket are two doors into one engine: one block cache, one set of open
// files, one pin table. No second engine runs.
//
// The mount's owner calls Prepare before it starts each `juicefs mount`
// child and Started right after. Prepare makes a fresh private socket and
// start pipe; the child inherits both. Started sends the launch token and
// connects in the background. A remount is a new launch with a new token
// and a new epoch: handles of the old launch are then refused, and the NFS
// front reopens by inode.
//
// Who may connect is the same as for the helper (design 2.9): the folder is
// 0700, the mount accepts only its own parent process and user, and every
// connection starts with the token.
type Door struct {
	cfg DoorConfig

	mu   sync.Mutex
	cur  *doorLaunch
	shut bool
}

// DoorConfig configures a Door.
type DoorConfig struct {
	SocketBase string // parent of the private socket folder; "" = os.TempDir()
	// ExpectVersion is the juicefs version this app build expects in the
	// hello. "" accepts any version with the same protocol major.
	ExpectVersion string
	// ConnectTimeout bounds Started → first good hello (120 s: a mount
	// abroad loads its settings over several round trips first).
	ConnectTimeout time.Duration
	// OnEvent gets "ready" (Epoch, Elapsed), "start-failed" (Err) and
	// "refused" (Err). Keep it short.
	OnEvent func(Event)
}

type doorLaunch struct {
	seq      uint64
	dir      string
	sock     string
	token    []byte
	pipeW    *os.File
	child    []*os.File // the child's ends: closed once it started
	prepared time.Time
	client   *Client
	hello    wire.HelloReply
	ready    chan struct{} // closed on ready or on failure
	err      error
	stop     chan struct{}
	fmu      sync.Mutex // finish and stop
}

// NewDoor returns a door with no launch yet.
func NewDoor(cfg DoorConfig) *Door {
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 120 * time.Second
	}
	return &Door{cfg: cfg}
}

// DoorArgs are the mount flags for a socket on descriptors start and
// start+1 (the first two of cmd.ExtraFiles when firstFD is 3).
func DoorArgs(firstFD int) []string {
	return []string{
		"--jm-door-start-fd", strconv.Itoa(firstFD),
		"--jm-door-listen-fd", strconv.Itoa(firstFD + 1),
	}
}

// Prepare retires the previous launch and makes the socket and the pipe for
// the next one. files go to cmd.ExtraFiles in this order, starting at
// descriptor firstFD; args go after the mount's other flags.
func (d *Door) Prepare(firstFD int) (args []string, files []*os.File, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.shut {
		return nil, nil, ErrClosed
	}
	var seq uint64
	if d.cur != nil {
		seq = d.cur.seq + 1
		d.cur.retire()
		d.cur = nil
	}
	dir, err := privateDir(d.cfg.SocketBase)
	if err != nil {
		return nil, nil, err
	}
	l := &doorLaunch{seq: seq, dir: dir, sock: filepath.Join(dir, "s"), ready: make(chan struct{}),
		stop: make(chan struct{}), prepared: time.Now()}
	fail := func(err error) ([]string, []*os.File, error) {
		for _, f := range l.child {
			f.Close()
		}
		if l.pipeW != nil {
			l.pipeW.Close()
		}
		os.RemoveAll(dir)
		return nil, nil, err
	}
	if len(l.sock) > maxSocketPath {
		return fail(fmt.Errorf("engine socket path is %d bytes, over %d", len(l.sock), maxSocketPath))
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: l.sock, Net: "unix"})
	if err != nil {
		return fail(err)
	}
	ln.SetUnlinkOnClose(false)
	if err := os.Chmod(l.sock, 0o600); err != nil {
		ln.Close()
		return fail(err)
	}
	lf, err := ln.File()
	ln.Close() // the mount holds the only listening descriptor from here on
	if err != nil {
		return fail(err)
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		lf.Close()
		return fail(err)
	}
	l.child = []*os.File{pr, lf}
	l.pipeW = pw
	l.token = make([]byte, 32)
	if _, err := rand.Read(l.token); err != nil {
		return fail(err)
	}
	d.cur = l
	return DoorArgs(firstFD), l.child, nil
}

// Started is called once the mount child started (startErr nil) or failed
// to start. It closes the child's ends, sends the start message and
// connects in the background.
func (d *Door) Started(startErr error) {
	d.mu.Lock()
	l := d.cur
	d.mu.Unlock()
	if l == nil {
		return
	}
	for _, f := range l.child {
		f.Close()
	}
	l.child = nil
	if startErr == nil {
		startErr = wire.WriteStartMessage(l.pipeW, wire.StartMessage{Token: hex.EncodeToString(l.token)})
	}
	if startErr != nil {
		l.finish(nil, wire.HelloReply{}, fmt.Errorf("engine door: %w", startErr))
		d.event(Event{Kind: EventStartFailed, Err: l.err})
		return
	}
	go d.connect(l)
}

func (d *Door) event(e Event) {
	if d.cfg.OnEvent != nil {
		d.cfg.OnEvent(e)
	}
}

func (d *Door) connect(l *doorLaunch) {
	deadline := l.prepared.Add(d.cfg.ConnectTimeout)
	wait := 50 * time.Millisecond
	for {
		attempt := time.Now().Add(5 * time.Second)
		if attempt.After(deadline) {
			attempt = deadline
		}
		conn, h, err := dialHello(l.sock, l.token, wire.ProtoMajor, attempt)
		if err == nil {
			if verr := d.verify(h); verr != nil {
				conn.Close()
				l.finish(nil, h, verr)
				d.event(Event{Kind: EventRefused, Err: verr})
				return
			}
			cl := newClient(l.sock, l.token)
			cl.put(wire.ClassCtl, conn)
			if !l.finish(cl, h, nil) {
				cl.Close()
				return
			}
			d.event(Event{Kind: EventReady, PID: h.PID, Epoch: h.Epoch, Elapsed: time.Since(l.prepared)})
			return
		}
		var se *StatusError
		if errors.As(err, &se) && se.Status == syscall.EACCES {
			// The mount refused this process: wrong token, or the mount is
			// not this process's direct child (a juicefs supervisor stage in
			// between; the app sets JFS_SUPERVISOR). Retrying cannot help.
			rerr := fmt.Errorf("%w: the mount refused this process (EACCES); is juicefs its direct child?", ErrRefused)
			l.finish(nil, h, rerr)
			d.event(Event{Kind: EventRefused, Err: rerr})
			return
		}
		if errors.As(err, &se) && se.Status == syscall.EPROTONOSUPPORT {
			rerr := fmt.Errorf("%w: protocol %d.%d, app speaks %d.%d (juicefs %s)", ErrRefused,
				h.ProtoMajor, h.ProtoMinor, wire.ProtoMajor, wire.ProtoMinor, h.JuiceFSVersion)
			l.finish(nil, h, rerr)
			d.event(Event{Kind: EventRefused, Err: rerr})
			return
		}
		if time.Now().After(deadline) {
			ferr := fmt.Errorf("engine door: no hello within %s: %w", d.cfg.ConnectTimeout, err)
			l.finish(nil, h, ferr)
			d.event(Event{Kind: EventStartFailed, Err: ferr})
			return
		}
		select {
		case <-l.stop:
			l.finish(nil, h, ErrClosed)
			return
		case <-time.After(wait):
		}
		if wait < time.Second {
			wait *= 2
		}
	}
}

func (d *Door) verify(h wire.HelloReply) error {
	if h.ProtoMajor != wire.ProtoMajor {
		return fmt.Errorf("%w: protocol major %d, app speaks %d", ErrRefused, h.ProtoMajor, wire.ProtoMajor)
	}
	if d.cfg.ExpectVersion != "" && h.JuiceFSVersion != d.cfg.ExpectVersion {
		return fmt.Errorf("%w: juicefs %q, this app expects %q", ErrRefused, h.JuiceFSVersion, d.cfg.ExpectVersion)
	}
	for _, op := range []wire.Op{wire.OpOpen, wire.OpRead, wire.OpRelease} {
		if !slices.Contains(h.Ops, op.String()) {
			return fmt.Errorf("%w: the engine socket does not serve %s", ErrRefused, op)
		}
	}
	return nil
}

// finish records the outcome once. It reports false when the launch was
// already finished or retired.
func (l *doorLaunch) finish(cl *Client, h wire.HelloReply, err error) bool {
	l.fmu.Lock()
	defer l.fmu.Unlock()
	select {
	case <-l.ready:
		return false
	default:
	}
	l.client, l.hello, l.err = cl, h, err
	close(l.ready)
	return true
}

func (l *doorLaunch) retire() {
	l.fmu.Lock()
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
	l.fmu.Unlock()
	l.finish(nil, wire.HelloReply{}, ErrClosed)
	if l.client != nil {
		l.client.Close()
	}
	for _, f := range l.child {
		f.Close()
	}
	if l.pipeW != nil {
		l.pipeW.Close() // the mount's door stops serving; the mount carries on
	}
	os.RemoveAll(l.dir)
}

// Current returns the client of the ready launch and its epoch. ok is false
// while no socket is ready (before the first hello, after a failure, after
// Close): the caller then reads through the mount door, as today.
func (d *Door) Current() (cl *Client, epoch uint64, ok bool) {
	d.mu.Lock()
	l := d.cur
	d.mu.Unlock()
	if l == nil {
		return nil, 0, false
	}
	select {
	case <-l.ready:
	default:
		return nil, 0, false
	}
	if l.client == nil {
		return nil, 0, false
	}
	return l.client, l.hello.Epoch, true
}

// WaitReady waits for the current launch's hello (tests and the lab).
func (d *Door) WaitReady(ctx context.Context) (wire.HelloReply, error) {
	d.mu.Lock()
	l := d.cur
	d.mu.Unlock()
	if l == nil {
		return wire.HelloReply{}, errors.New("engine door: no launch")
	}
	select {
	case <-l.ready:
		return l.hello, l.err
	case <-ctx.Done():
		return wire.HelloReply{}, ctx.Err()
	}
}

// Close retires the current launch. Later Prepare calls fail.
func (d *Door) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.shut = true
	if d.cur != nil {
		d.cur.retire()
		d.cur = nil
	}
}

// LogDoorEvents writes the door's events to the JuiceMount log ("engine
// socket ready" carries the epoch and ms since the mount was prepared).
func LogDoorEvents(e Event) {
	switch e.Kind {
	case EventReady:
		jmlog.Info("engine socket ready", "epoch", e.Epoch, "pid", e.PID, "ms", e.Elapsed.Milliseconds())
	default:
		msg := ""
		if e.Err != nil {
			msg = e.Err.Error()
		}
		jmlog.Warn("engine socket "+e.Kind+"; reads stay on the mount", "error", msg)
	}
}
