package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Transport errors. Daemon-reported errors are *DaemonError and match the
// code sentinels below with errors.Is.
var (
	ErrUnsupported  = errors.New("unmountd client: XPC is only available on macOS")
	ErrTimeout      = errors.New("unmountd client: call timed out")
	ErrInterrupted  = errors.New("unmountd client: daemon connection interrupted (daemon exited; retry)")
	ErrUnavailable  = errors.New("unmountd client: daemon unavailable (not registered, not approved, or connection invalid)")
	ErrPeerRejected = errors.New("unmountd client: daemon failed the client's code-signing requirement")
	ErrClosed       = errors.New("unmountd client: closed")
	// ErrPending: the retirement is still running. The Outcome returned with
	// it is progress (State, Holders, Descendants). Call Retire again to wait
	// longer or to escalate; a second Retire joins the same job.
	ErrPending = errors.New("unmountd client: retirement still in progress")
)

// Daemon error sentinels, one per response code.
var (
	ErrBadRequest      = errors.New("unmountd: bad request")
	ErrNotAuthorized   = errors.New("unmountd: caller role may not perform this operation")
	ErrReceiptLimit    = errors.New("unmountd: per-uid receipt limit reached")
	ErrReceiptExists   = errors.New("unmountd: receipt already exists for this op")
	ErrNoReceipt       = errors.New("unmountd: no receipt for this op")
	ErrInvalidReceipt  = errors.New("unmountd: invalid receipt")
	ErrIdentityRefused = errors.New("unmountd: identity predicate refused")
	ErrOpIDReused      = errors.New("unmountd: op id already used in this daemon's lifetime")
	ErrPostState       = errors.New("unmountd: unmount reported success but the fsid stayed present")
	ErrAlreadyBound    = errors.New("unmountd: connection is already bound to an op")
	ErrBusy            = errors.New("unmountd: too many requests in flight on this connection")
	ErrInternal        = errors.New("unmountd: internal daemon error")
	ErrNotCancellable  = errors.New("unmountd: retirement cannot be cancelled (a force was requested or issued)")
)

var codeSentinel = map[string]error{
	CodeBadRequest:      ErrBadRequest,
	CodeNotAuthorized:   ErrNotAuthorized,
	CodeReceiptLimit:    ErrReceiptLimit,
	CodeReceiptExists:   ErrReceiptExists,
	CodeNoReceipt:       ErrNoReceipt,
	CodeInvalidReceipt:  ErrInvalidReceipt,
	CodeIdentityRefused: ErrIdentityRefused,
	CodeOpIDReused:      ErrOpIDReused,
	CodePostState:       ErrPostState,
	CodeAlreadyBound:    ErrAlreadyBound,
	CodeBusy:            ErrBusy,
	CodeInternal:        ErrInternal,
	CodeNotCancellable:  ErrNotCancellable,
}

// DaemonError is an error the daemon reported.
type DaemonError struct {
	Code    string
	Message string
}

func (e *DaemonError) Error() string {
	if e.Message == "" {
		return "unmountd: " + e.Code
	}
	return "unmountd: " + e.Code + ": " + e.Message
}

// Unwrap returns the code's sentinel (nil for an unknown code).
func (e *DaemonError) Unwrap() error { return codeSentinel[e.Code] }

// Event is an asynchronous connection event.
type Event int

const (
	EventInterrupted  Event = iota + 1 // daemon exited; the next send relaunches it
	EventInvalid                       // the connection can never be used again
	EventPeerRejected                  // the daemon failed DaemonRequirement
)

// Conn is one XPC connection to the daemon. The darwin implementation wraps
// libxpc; tests substitute a fake.
type Conn interface {
	// RoundTrip sends one message and waits for its reply or ctx.
	RoundTrip(ctx context.Context, version int64, body []byte) (int64, []byte, error)
	// Events delivers Interrupted/Invalid/PeerRejected. It is never closed.
	Events() <-chan Event
	// Close cancels the connection. Idempotent.
	Close()
}

// Dialer opens a connection to service, requiring requirement of the peer.
type Dialer func(service, requirement string) (Conn, error)

// Options configures a Client. Zero values select the defaults.
type Options struct {
	Service       string        // default MachService (RoleService)
	Requirement   string        // default DaemonRequirement()
	CallTimeout   time.Duration // per call, on top of any retire wait; default 5s
	ReclaimWindow time.Duration // how long a lease retries "reclaim" after the daemon restarts; default 25s
	Logf          func(format string, args ...any)
	Dial          Dialer // default: libxpc (darwin) or ErrUnsupported
}

// Default timings.
const (
	DefaultCallTimeout   = 5 * time.Second
	DefaultReclaimWindow = 25 * time.Second // the daemon's orphan window is 30 s
	// QuitForceAfter is the founder's busy-Quit policy: an ordinary
	// retirement first, then Retire(EjectNow) after 30 s.
	QuitForceAfter = 30 * time.Second
)

// Client is safe for concurrent use.
type Client struct {
	opts Options

	mu     sync.Mutex
	shared Conn
	closed bool
}

// New builds a Client. It does not connect until the first call.
func New(opts Options) *Client {
	if opts.Service == "" {
		opts.Service = MachService
	}
	if opts.Requirement == "" {
		opts.Requirement = DaemonRequirement()
	}
	if opts.CallTimeout <= 0 {
		opts.CallTimeout = DefaultCallTimeout
	}
	if opts.ReclaimWindow <= 0 {
		opts.ReclaimWindow = DefaultReclaimWindow
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	if opts.Dial == nil {
		opts.Dial = dialXPC
	}
	return &Client{opts: opts}
}

// Close closes the shared connection. Leases are independent; close them
// separately.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.shared != nil {
		c.shared.Close()
		c.shared = nil
	}
}

func (c *Client) sharedConn() (Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if c.shared != nil {
		return c.shared, nil
	}
	conn, err := c.opts.Dial(c.opts.Service, c.opts.Requirement)
	if err != nil {
		return nil, err
	}
	c.shared = conn
	return conn, nil
}

// dropShared forgets a connection that can never work again, so the next
// call dials afresh (e.g. after the user approves the helper).
func (c *Client) dropShared(conn Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shared == conn {
		c.shared.Close()
		c.shared = nil
	}
}

// call performs one round trip with a bounded deadline: CallTimeout plus
// extra (the retire wait). A deadline that this bound set is ErrTimeout; a
// caller's own ctx ending returns ctx's error.
func (c *Client) call(ctx context.Context, conn Conn, req Request, extra time.Duration) (Response, error) {
	body, err := EncodeRequest(req)
	if err != nil {
		return Response{}, err
	}
	cctx, cancel := context.WithTimeout(ctx, c.opts.CallTimeout+extra)
	defer cancel()
	v, rbody, err := conn.RoundTrip(cctx, ProtocolVersion, body)
	if err != nil {
		if ctx.Err() != nil {
			return Response{}, ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return Response{}, fmt.Errorf("%w: %s after %s", ErrTimeout, req.Op, c.opts.CallTimeout+extra)
		}
		return Response{}, err
	}
	resp, err := DecodeResponse(v, rbody)
	if err != nil {
		return Response{}, err
	}
	switch resp.Code {
	case CodeOK:
		return resp, nil
	case CodePending:
		return resp, ErrPending
	default:
		return resp, &DaemonError{Code: resp.Code, Message: resp.Message}
	}
}

// callShared runs an idempotent call on the shared connection, retrying
// once if the daemon was restarting.
func (c *Client) callShared(ctx context.Context, req Request, extra time.Duration) (Response, error) {
	for attempt := 0; ; attempt++ {
		conn, err := c.sharedConn()
		if err != nil {
			return Response{}, err
		}
		resp, err := c.call(ctx, conn, req, extra)
		if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrPeerRejected) {
			c.dropShared(conn)
		}
		if errors.Is(err, ErrInterrupted) && attempt == 0 {
			continue
		}
		return resp, err
	}
}

// Ping reports the daemon's protocol, channel and the role and uid it
// admitted this connection as.
func (c *Client) Ping(ctx context.Context) (PingInfo, error) {
	resp, err := c.callShared(ctx, Request{Op: OpPing}, 0)
	if err != nil {
		return PingInfo{}, err
	}
	if resp.Ping == nil {
		return PingInfo{}, framing("ping reply without info")
	}
	return *resp.Ping, nil
}

// Status reports progress for one of this uid's ops.
func (c *Client) Status(ctx context.Context, opID string) (Outcome, error) {
	resp, err := c.callShared(ctx, Request{Op: OpStatus, OpID: opID}, 0)
	if err != nil {
		return outcomeOf(resp), err
	}
	return outcomeOf(resp), nil
}

// RetireRequest mirrors the core's RetireRequest.
type RetireRequest struct {
	Liveness string // LivenessUnknown/Alive/Dead/Wedged; "" means unknown
	EjectNow bool
}

// Retire asks the daemon to retire opID and waits up to wait (capped at
// MaxRetireWait) for a final state. A still-running job returns its
// progress with ErrPending. Retire is idempotent: a repeat joins the job
// and may escalate it (EjectNow is sticky).
func (c *Client) Retire(ctx context.Context, opID string, rr RetireRequest, wait time.Duration) (Outcome, error) {
	if rr.Liveness == "" {
		rr.Liveness = LivenessUnknown
	}
	ms := wait.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	if ms > MaxRetireWait {
		ms = MaxRetireWait
	}
	req := Request{Op: OpRetire, OpID: opID, Retire: &RetireArgs{Liveness: rr.Liveness, EjectNow: rr.EjectNow}, WaitMillis: ms}
	resp, err := c.callShared(ctx, req, time.Duration(ms)*time.Millisecond)
	return outcomeOf(resp), err
}

// Cancel withdraws this uid's ordinary retirement of opID, so a busy volume
// is not ejected later by an orphaned EBUSY-backoff job. It is accepted in
// any non-final state, also with an ordinary call in the kernel (nothing more
// is issued; that call finishes on its own), and refused (ErrNotCancellable,
// with progress) once a force was requested or issued. The receipt is kept;
// the op is REGISTERED, and the helper's orphan timer will not restart it.
func (c *Client) Cancel(ctx context.Context, opID string) (Outcome, error) {
	resp, err := c.callShared(ctx, Request{Op: OpCancel, OpID: opID}, 0)
	return outcomeOf(resp), err
}

// RetireForQuit implements the busy-Quit policy: an ordinary retirement
// with the proven liveness for grace (QuitForceAfter), then Retire(EjectNow).
// The force is still not immediate: an ordinary call already in the kernel
// must return first, and a foreign mount nested inside ours blocks it
// (State WAIT_NESTED, Outcome.Descendants). The second call can therefore
// also return ErrPending; surface Holders/Descendants to the user.
func (c *Client) RetireForQuit(ctx context.Context, opID, liveness string, grace time.Duration) (Outcome, error) {
	out, err := c.Retire(ctx, opID, RetireRequest{Liveness: liveness}, grace)
	if !errors.Is(err, ErrPending) {
		return out, err
	}
	return c.Retire(ctx, opID, RetireRequest{Liveness: liveness, EjectNow: true}, time.Duration(MaxRetireWait)*time.Millisecond)
}

func outcomeOf(r Response) Outcome {
	if r.Outcome == nil {
		return Outcome{}
	}
	return *r.Outcome
}

// RegisterRequest is the owner's claim about a mount it just observed.
type RegisterRequest struct {
	OpID string
	RegisterArgs
}

// Register records a receipt for a mount this service just created and
// returns a Lease bound to it. Keep the Lease open for as long as the mount
// is yours: when it closes (or this process dies) the daemon starts the
// orphan window, and retires the mount if nobody reclaims it.
func (c *Client) Register(ctx context.Context, rr RegisterRequest) (*Lease, Receipt, error) {
	args := rr.RegisterArgs
	req := Request{Op: OpRegister, OpID: rr.OpID, Register: &args}
	l, resp, err := c.bind(ctx, rr.OpID, req)
	if err != nil {
		return nil, Receipt{}, err
	}
	if resp.Receipt == nil {
		l.Close()
		return nil, Receipt{}, framing("register reply without receipt")
	}
	return l, *resp.Receipt, nil
}

// ReclaimResult says whether a pending orphan retirement was cancelled.
type ReclaimResult struct {
	Reclaimed bool    // an orphan timer was pending and is now cancelled
	Outcome   Outcome // current status of the op
}

// Reclaim re-binds a restarted service to one of its ops. Call it within
// the orphan window (30 s) of the previous owner's exit. If the op already
// finished, it returns ErrNoReceipt with the final Outcome and no Lease.
func (c *Client) Reclaim(ctx context.Context, opID string) (*Lease, ReclaimResult, error) {
	l, resp, err := c.bind(ctx, opID, Request{Op: OpReclaim, OpID: opID})
	res := ReclaimResult{Reclaimed: resp.Reclaimed, Outcome: outcomeOf(resp)}
	if err != nil {
		return nil, res, err
	}
	return l, res, nil
}

func (c *Client) bind(ctx context.Context, opID string, req Request) (*Lease, Response, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, Response{}, ErrClosed
	}
	if err := req.Validate(); err != nil {
		return nil, Response{}, err
	}
	conn, err := c.opts.Dial(c.opts.Service, c.opts.Requirement)
	if err != nil {
		return nil, Response{}, err
	}
	resp, err := c.call(ctx, conn, req, 0)
	if err != nil {
		conn.Close()
		return nil, resp, err
	}
	l := &Lease{c: c, opID: opID, conn: conn, lost: make(chan struct{}), stop: make(chan struct{})}
	go l.watch()
	return l, resp, nil
}

// Lease is a dedicated connection bound to one op.
type Lease struct {
	c    *Client
	opID string
	conn Conn

	lost     chan struct{}
	stop     chan struct{}
	lostOnce sync.Once
	stopOnce sync.Once
	mu       sync.Mutex
	err      error
}

// OpID is the op this lease holds.
func (l *Lease) OpID() string { return l.opID }

// Lost is closed when the lease can no longer protect the op: the receipt
// is gone (retired), the daemon became unavailable, or re-binding after a
// daemon restart failed within the reclaim window. Err says why.
func (l *Lease) Lost() <-chan struct{} { return l.lost }

// Err is the reason the lease was lost (nil while it holds).
func (l *Lease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Close releases the lease. If the op still has a receipt, the daemon starts
// its orphan window: release only after a successful Retire, or when the
// mount genuinely has no owner any more.
func (l *Lease) Close() {
	l.stopOnce.Do(func() {
		close(l.stop)
		l.conn.Close()
	})
}

func (l *Lease) markLost(err error) {
	l.lostOnce.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		close(l.lost)
		l.c.opts.Logf("unmountd lease %s lost: %v", l.opID, err)
	})
}

func (l *Lease) watch() {
	for {
		select {
		case <-l.stop:
			return
		case ev := <-l.conn.Events():
			switch ev {
			case EventInterrupted:
				if err := l.rebind(); err != nil {
					l.markLost(err)
					return
				}
			case EventPeerRejected:
				l.markLost(ErrPeerRejected)
				return
			default:
				select {
				case <-l.stop:
				default:
					l.markLost(ErrUnavailable)
				}
				return
			}
		}
	}
}

// rebind re-sends "reclaim" on the same connection after the daemon
// restarted (libxpc relaunches it on the next send). The new daemon's launch
// sweep has armed the orphan timer for our receipt; this cancels it.
func (l *Lease) rebind() error {
	deadline := time.Now().Add(l.c.opts.ReclaimWindow)
	backoff := 250 * time.Millisecond
	for {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		resp, err := l.c.call(ctx, l.conn, Request{Op: OpReclaim, OpID: l.opID}, 0)
		cancel()
		if err == nil {
			l.c.opts.Logf("unmountd lease %s re-bound after daemon restart (reclaimed=%v)", l.opID, resp.Reclaimed)
			return nil
		}
		if !errors.Is(err, ErrInterrupted) && !errors.Is(err, ErrTimeout) && !errors.Is(err, ErrBusy) {
			return err
		}
		select {
		case <-l.stop:
			return ErrClosed
		case <-time.After(backoff):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("re-bind after daemon restart: %w", err)
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}
