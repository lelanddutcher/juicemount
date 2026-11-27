package enginehost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine/wire"
)

// ObjectKey is this Mac's object-store key. It goes to the helper through
// the private pipe only.
type ObjectKey struct {
	AccessKey string
	SecretKey string
}

// Config configures a Supervisor. Zero durations take the design's values
// (engine-direct design 2.4–2.7).
type Config struct {
	Bin  string   // the bundled juicefs
	Args []string // after the binary: "jm-engine", META-URL (no password), flags
	Env  []string // the child's whole environment (no secrets)

	MetaPassword string    // Redis password: start message only
	ObjectKey    ObjectKey // object key: start message only

	SocketBase string // parent of the private socket folder; "" = os.TempDir()
	LogPath    string // helper stdout and stderr, appended; "" = discarded

	// ExpectVersion is the juicefs version string this app build expects in
	// the hello. "" accepts any version with the same protocol major.
	ExpectVersion string

	StartTimeout time.Duration   // launch → hello answered (60 s)
	PingInterval time.Duration   // between pings while ready (2 s)
	PingTimeout  time.Duration   // one ping (2 s)
	MissLimit    int             // missed pings in a row before a kill (3)
	Backoff      []time.Duration // restart delays (0.5, 1, 2, 4, 8, 16, 30 s)
	ResetAfter   time.Duration   // ready this long resets the back-off (60 s)
	StopBound    time.Duration   // shutdown → exit before SIGTERM (30 s)
	// FailureReport is the start-failure count in a row that is reported
	// once as persistent (5).
	FailureReport int

	// OnEvent receives every lifecycle event, synchronously. Keep it short.
	OnEvent func(Event)
}

func (c *Config) defaults() {
	if c.StartTimeout <= 0 {
		c.StartTimeout = 60 * time.Second
	}
	if c.PingInterval <= 0 {
		// The design says 5 s. 2 s keeps the worst case of a frozen helper
		// (interval + 3 misses × 2 s + dump + back-off + start) under the
		// 10 s of check E6. A ping costs well under a millisecond.
		c.PingInterval = 2 * time.Second
	}
	if c.PingTimeout <= 0 {
		c.PingTimeout = 2 * time.Second
	}
	if c.MissLimit <= 0 {
		c.MissLimit = 3
	}
	if len(c.Backoff) == 0 {
		c.Backoff = []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second,
			8 * time.Second, 16 * time.Second, 30 * time.Second}
	}
	if c.ResetAfter <= 0 {
		c.ResetAfter = 60 * time.Second
	}
	if c.StopBound <= 0 {
		c.StopBound = 30 * time.Second
	}
	if c.FailureReport <= 0 {
		c.FailureReport = 5
	}
}

// Event kinds.
const (
	EventLaunch       = "launch"       // child started (PID set)
	EventReady        = "ready"        // hello accepted (Elapsed = launch → ready)
	EventStartFailed  = "start-failed" // no good hello (Err)
	EventRefused      = "refused"      // version or protocol mismatch: no restart (Err)
	EventExit         = "exit"         // child exited while ready (Err = wait status)
	EventPingMiss     = "ping-miss"    // one ping failed (Attempt = misses in a row)
	EventUnresponsive = "unresponsive" // MissLimit misses: dump, kill, restart
	EventBackoff      = "backoff"      // waiting Elapsed before the next launch
	EventPersistent   = "persistent"   // FailureReport start failures in a row
	EventStopped      = "stopped"      // Stop finished (Err = how, if not clean)
)

// Event is one lifecycle event.
type Event struct {
	Kind    string
	PID     int
	Epoch   uint64
	Elapsed time.Duration
	Attempt int
	Err     error
}

// State of the supervisor.
type State string

const (
	StateIdle     State = "idle"
	StateStarting State = "starting"
	StateReady    State = "ready"
	StateBackoff  State = "backoff"
	StateRefused  State = "refused" // version mismatch: stays down
	StateStopped  State = "stopped"
)

// Status is a snapshot for health and logs.
type Status struct {
	State               State
	PID                 int
	Epoch               uint64
	Version             string
	VolumeID            string
	Restarts            int
	ConsecutiveFailures int
	LastError           string
	ReadySince          time.Time
	LastStart           time.Duration // last launch → ready
}

// ErrRefused marks a helper the app will not use (version or protocol).
var ErrRefused = errors.New("engine helper refused")

// ErrStopped is returned by WaitReady after Stop.
var ErrStopped = errors.New("engine supervisor stopped")

type child struct {
	cmd     *exec.Cmd
	pid     int
	dir     string
	sock    string
	pipeW   *os.File
	exited  chan struct{}
	waitErr error
	client  *Client
	hello   wire.HelloReply
}

// Supervisor owns at most one helper at a time.
type Supervisor struct {
	cfg     Config
	mu      sync.Mutex
	status  Status
	cur     *child
	changed chan struct{} // closed and replaced on every status change
	started bool
	stopCh  chan struct{}
	done    chan struct{}
	stopOne sync.Once
	how     string // how the last child was stopped
}

// New returns a supervisor; call Start.
func New(cfg Config) *Supervisor {
	cfg.defaults()
	return &Supervisor{
		cfg: cfg, status: Status{State: StateIdle},
		changed: make(chan struct{}), stopCh: make(chan struct{}), done: make(chan struct{}),
	}
}

func (s *Supervisor) event(e Event) {
	if s.cfg.OnEvent != nil {
		s.cfg.OnEvent(e)
	}
}

func (s *Supervisor) update(f func(*Status)) {
	s.mu.Lock()
	f(&s.status)
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}

// Status returns a snapshot.
func (s *Supervisor) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Client returns the client of the ready helper, or nil.
func (s *Supervisor) Client() *Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status.State != StateReady || s.cur == nil {
		return nil
	}
	return s.cur.client
}

// Start launches the run loop. It returns at once; see WaitReady.
func (s *Supervisor) Start() {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	go s.run()
}

// WaitReady blocks until the helper is ready, refused, or stopped, or ctx ends.
func (s *Supervisor) WaitReady(ctx context.Context) (Status, error) {
	for {
		s.mu.Lock()
		st, ch := s.status, s.changed
		s.mu.Unlock()
		switch st.State {
		case StateReady:
			return st, nil
		case StateRefused:
			return st, fmt.Errorf("%w: %s", ErrRefused, st.LastError)
		case StateStopped:
			return st, ErrStopped
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return st, ctx.Err()
		}
	}
}

// Stop stops the helper (shutdown message, then the pipe, then SIGTERM after
// StopBound, then SIGKILL) and the run loop. It returns how the helper went:
// "shutdown", "term", "kill" or "none", and the time it took.
func (s *Supervisor) Stop() (string, time.Duration) {
	start := time.Now()
	s.stopOne.Do(func() { close(s.stopCh) })
	s.mu.Lock()
	started := s.started
	s.started = true // a later Start is a no-op
	s.mu.Unlock()
	if !started {
		close(s.done)
		s.update(func(st *Status) { st.State = StateStopped })
	}
	<-s.done
	s.mu.Lock()
	how := s.how
	s.mu.Unlock()
	if how == "" {
		how = "none"
	}
	return how, time.Since(start)
}

func (s *Supervisor) stopping() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

func (s *Supervisor) run() {
	defer close(s.done)
	backoffIdx := 0
	failures := 0
	for !s.stopping() {
		s.update(func(st *Status) { st.State = StateStarting })
		c, err := s.launch()
		if err != nil {
			if errors.Is(err, ErrRefused) {
				s.event(Event{Kind: EventRefused, Err: err})
				s.update(func(st *Status) { st.State = StateRefused; st.LastError = err.Error(); st.PID = 0 })
				<-s.stopCh
				break
			}
			if s.stopping() {
				break
			}
			failures++
			s.event(Event{Kind: EventStartFailed, Attempt: failures, Err: err})
			if failures == s.cfg.FailureReport {
				s.event(Event{Kind: EventPersistent, Attempt: failures, Err: err})
			}
			s.update(func(st *Status) {
				st.ConsecutiveFailures = failures
				st.LastError = err.Error()
				st.PID = 0
			})
		} else {
			failures = 0
			readyAt := time.Now()
			reason := s.watch(c)
			if reason == "stop" {
				s.shutdownChild(c)
				break
			}
			s.retire(c)
			if time.Since(readyAt) >= s.cfg.ResetAfter {
				backoffIdx = 0
			}
			s.update(func(st *Status) {
				st.Restarts++
				st.LastError = reason
				st.PID = 0
				st.ConsecutiveFailures = 0
			})
		}
		d := s.cfg.Backoff[min(backoffIdx, len(s.cfg.Backoff)-1)]
		backoffIdx++
		s.update(func(st *Status) { st.State = StateBackoff })
		s.event(Event{Kind: EventBackoff, Elapsed: d})
		select {
		case <-time.After(d):
		case <-s.stopCh:
		}
	}
	s.update(func(st *Status) { st.State = StateStopped; st.PID = 0 })
	how := s.how
	var err error
	if how != "shutdown" && how != "" {
		err = fmt.Errorf("helper stopped by %s", how)
	}
	s.event(Event{Kind: EventStopped, Err: err})
}

// privateDir makes the socket folder: mode 0700, owned by this user.
func privateDir(base string) (string, error) {
	if base == "" {
		base = os.TempDir()
	}
	dir, err := os.MkdirTemp(base, "jme-")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	fi, err := os.Lstat(dir)
	if err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		os.RemoveAll(dir)
		return "", fmt.Errorf("engine socket folder %s is not private", dir)
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Getuid() {
		os.RemoveAll(dir)
		return "", fmt.Errorf("engine socket folder %s is not owned by this user", dir)
	}
	return dir, nil
}

// failedShutdownGrace is how long a helper that did not answer the
// shutdown request has to exit on its own before SIGKILL.
const failedShutdownGrace = 2 * time.Second

// maxSocketPath is below macOS's 104-byte sun_path.
const maxSocketPath = 100

func (s *Supervisor) launch() (*child, error) {
	dir, err := privateDir(s.cfg.SocketBase)
	if err != nil {
		return nil, err
	}
	c := &child{dir: dir, sock: filepath.Join(dir, "s"), exited: make(chan struct{})}
	fail := func(err error) (*child, error) {
		if c.pipeW != nil {
			c.pipeW.Close()
		}
		if c.cmd != nil && c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
			<-c.exited
		}
		os.RemoveAll(dir)
		return nil, err
	}
	if len(c.sock) > maxSocketPath {
		return fail(fmt.Errorf("engine socket path is %d bytes, over %d", len(c.sock), maxSocketPath))
	}
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: c.sock, Net: "unix"})
	if err != nil {
		return fail(err)
	}
	l.SetUnlinkOnClose(false)
	if err := os.Chmod(c.sock, 0o600); err != nil {
		l.Close()
		return fail(err)
	}
	lf, err := l.File()
	l.Close() // the helper holds the only listening descriptor from here on
	if err != nil {
		return fail(err)
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		lf.Close()
		return fail(err)
	}
	c.pipeW = pw
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		pr.Close()
		lf.Close()
		return fail(err)
	}
	cmd := exec.Command(s.cfg.Bin, s.cfg.Args...)
	cmd.Env = s.cfg.Env
	if cmd.Env == nil {
		cmd.Env = []string{}
	}
	cmd.ExtraFiles = []*os.File{pr, lf} // fd 3: start pipe, fd 4: socket
	// Its own process group: a terminal's Ctrl-C reaches the app, which then
	// stops the helper in order.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var logf *os.File
	if s.cfg.LogPath != "" {
		if f, err := os.OpenFile(s.cfg.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err == nil {
			logf = f
			cmd.Stdout, cmd.Stderr = f, f
		}
	}
	launched := time.Now()
	err = cmd.Start()
	pr.Close()
	lf.Close()
	if logf != nil {
		logf.Close()
	}
	if err != nil {
		return fail(err)
	}
	c.cmd, c.pid = cmd, cmd.Process.Pid
	go func() { c.waitErr = cmd.Wait(); close(c.exited) }()
	s.update(func(st *Status) { st.PID = c.pid })
	s.event(Event{Kind: EventLaunch, PID: c.pid})
	if err := wire.WriteStartMessage(pw, wire.StartMessage{
		Token: hex.EncodeToString(token), MetaPassword: s.cfg.MetaPassword,
		AccessKey: s.cfg.ObjectKey.AccessKey, SecretKey: s.cfg.ObjectKey.SecretKey,
	}); err != nil {
		return fail(fmt.Errorf("write start message: %w", err))
	}

	type res struct {
		conn  net.Conn
		hello wire.HelloReply
		err   error
	}
	got := make(chan res, 1)
	go func() {
		conn, h, err := dialHello(c.sock, token, wire.ProtoMajor, launched.Add(s.cfg.StartTimeout))
		got <- res{conn, h, err}
	}()
	var r res
	select {
	case r = <-got:
	case <-c.exited:
		return fail(fmt.Errorf("helper exited during start: %v", c.waitErr))
	case <-s.stopCh:
		return fail(errors.New("stopped during start"))
	}
	if r.err != nil {
		var se *StatusError
		if errors.As(r.err, &se) && se.Status == syscall.EPROTONOSUPPORT {
			return fail(fmt.Errorf("%w: protocol %d.%d, app speaks %d.%d (juicefs %s)", ErrRefused,
				r.hello.ProtoMajor, r.hello.ProtoMinor, wire.ProtoMajor, wire.ProtoMinor, r.hello.JuiceFSVersion))
		}
		select {
		case <-c.exited:
			return fail(fmt.Errorf("helper exited during start: %v", c.waitErr))
		case <-time.After(100 * time.Millisecond):
		}
		return fail(fmt.Errorf("hello: %w", r.err))
	}
	if r.hello.ProtoMajor != wire.ProtoMajor {
		r.conn.Close()
		return fail(fmt.Errorf("%w: protocol major %d, app speaks %d", ErrRefused, r.hello.ProtoMajor, wire.ProtoMajor))
	}
	if s.cfg.ExpectVersion != "" && r.hello.JuiceFSVersion != s.cfg.ExpectVersion {
		r.conn.Close()
		return fail(fmt.Errorf("%w: juicefs %q, this app expects %q", ErrRefused, r.hello.JuiceFSVersion, s.cfg.ExpectVersion))
	}
	c.hello = r.hello
	c.client = newClient(c.sock, token)
	c.client.put(wire.ClassCtl, r.conn)
	elapsed := time.Since(launched)
	s.mu.Lock()
	s.cur = c
	s.mu.Unlock()
	s.update(func(st *Status) {
		st.State = StateReady
		st.Epoch = r.hello.Epoch
		st.Version = r.hello.JuiceFSVersion
		st.VolumeID = r.hello.VolumeID
		st.ReadySince = time.Now()
		st.LastStart = elapsed
		st.LastError = ""
		st.ConsecutiveFailures = 0
	})
	s.event(Event{Kind: EventReady, PID: c.pid, Epoch: r.hello.Epoch, Elapsed: elapsed})
	return c, nil
}

// watch returns "exit", "unresponsive" or "stop".
func (s *Supervisor) watch(c *child) string {
	t := time.NewTimer(s.cfg.PingInterval)
	defer t.Stop()
	misses := 0
	for {
		select {
		case <-c.exited:
			s.event(Event{Kind: EventExit, PID: c.pid, Epoch: c.hello.Epoch, Err: c.waitErr})
			return "exit"
		case <-s.stopCh:
			return "stop"
		case <-t.C:
			if _, err := c.client.Ping(s.cfg.PingTimeout); err == nil {
				misses = 0
				t.Reset(s.cfg.PingInterval)
				continue
			} else {
				select {
				case <-c.exited:
					s.event(Event{Kind: EventExit, PID: c.pid, Epoch: c.hello.Epoch, Err: c.waitErr})
					return "exit"
				default:
				}
				misses++
				s.event(Event{Kind: EventPingMiss, PID: c.pid, Epoch: c.hello.Epoch, Attempt: misses, Err: err})
			}
			if misses >= s.cfg.MissLimit {
				s.event(Event{Kind: EventUnresponsive, PID: c.pid, Epoch: c.hello.Epoch, Attempt: misses})
				s.dumpAndKill(c)
				return "unresponsive"
			}
			t.Reset(0) // the next try at once: detection stays within interval + MissLimit × timeout
		}
	}
}

// dumpAndKill asks the Go runtime for a goroutine dump (SIGQUIT writes it to
// the helper's log and exits), then kills. SIGCONT lets a stopped helper
// take the SIGQUIT.
func (s *Supervisor) dumpAndKill(c *child) {
	_ = c.cmd.Process.Signal(syscall.SIGQUIT)
	_ = c.cmd.Process.Signal(syscall.SIGCONT)
	select {
	case <-c.exited:
		return
	case <-time.After(500 * time.Millisecond):
	}
	_ = c.cmd.Process.Kill()
	select {
	case <-c.exited:
	case <-time.After(5 * time.Second):
	}
}

// retire releases a child that already exited or was killed.
func (s *Supervisor) retire(c *child) {
	s.mu.Lock()
	if s.cur == c {
		s.cur = nil
	}
	s.mu.Unlock()
	c.client.Close()
	c.pipeW.Close()
	select {
	case <-c.exited:
	case <-time.After(5 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.exited
	}
	os.RemoveAll(c.dir)
}

// shutdownChild stops a ready child in order (design 2.7).
//
// A helper that did not answer the shutdown request (frozen, or stuck in
// an engine call) gets SIGCONT and failedShutdownGrace to exit on the
// pipe's end of file, then SIGKILL. A step-1 helper is read-only and holds
// nothing to flush, so waiting out StopBound for it would only delay quit.
// A step that holds writes must revisit this.
func (s *Supervisor) shutdownChild(c *child) {
	how := "shutdown"
	deadline := time.Now().Add(s.cfg.StopBound)
	answered := c.client.Shutdown(min(5*time.Second, s.cfg.StopBound)) == nil
	c.client.Close()
	c.pipeW.Close() // end of file: the helper stops even if it missed the message
	if !answered {
		how = "pipe"
		_ = c.cmd.Process.Signal(syscall.SIGCONT)
		if until := time.Until(deadline); until > failedShutdownGrace {
			deadline = time.Now().Add(failedShutdownGrace)
		}
	}
	select {
	case <-c.exited:
	case <-time.After(time.Until(deadline)):
		if !answered {
			how = "kill"
			_ = c.cmd.Process.Kill()
			<-c.exited
			break
		}
		how = "term"
		_ = c.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-c.exited:
		case <-time.After(2 * time.Second):
			how = "kill"
			_ = c.cmd.Process.Kill()
			<-c.exited
		}
	}
	s.mu.Lock()
	if s.cur == c {
		s.cur = nil
	}
	s.how = how
	s.mu.Unlock()
	os.RemoveAll(c.dir)
}
