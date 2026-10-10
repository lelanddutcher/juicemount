package unmountd

// The XPC transport's platform-neutral half: which requirement each listener
// applies, how a peer becomes a Caller, request dispatch onto the core, and
// per-op connection binding that feeds Disconnected/Reclaim. The libxpc glue
// is xpc_darwin.{go,c}; it only moves bytes and connection events.
//
// Authentication is entirely libxpc's: each listener sets its requirement on
// every peer connection with xpc_connection_set_peer_code_signing_requirement
// before activating it, and libxpc delivers only messages whose sender (by
// audit token, not pid) satisfies it. The Caller's UID is the connection's
// euid (xpc_connection_get_euid). Role is the listener's: the service
// listener's requirement admits only the service binary, so a message that
// arrives there came from it. No payload field is ever read as identity.

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// PeerChannel is the requirement channel compiled into this binary:
// client.ChannelRelease unless built with -tags jmdevunmountd.
func PeerChannel() string { return peerChannel }

// PeerRequirements returns the requirement each listener applies.
func PeerRequirements() (service, status string) {
	return client.ServiceRequirement(peerChannel), client.StatusRequirement(peerChannel)
}

// xpcService is the slice of *Service the transport drives.
type xpcService interface {
	Register(Caller, RegisterRequest) (Receipt, error)
	Retire(context.Context, Caller, string, RetireRequest) (Outcome, error)
	Status(Caller, string) (Outcome, error)
	Cancel(Caller, string) (Outcome, error)
	Reclaim(Caller, string) (bool, error)
	Disconnected(uid uint32, opID string, liveness Liveness) error
}

type xpcListener struct {
	name        string
	role        Role
	requirement string
	signingID   string // diagnostic only (Caller.SigningID)
}

func xpcListeners(channel string) []xpcListener {
	return []xpcListener{
		{client.MachService, RoleService, client.ServiceRequirement(channel), client.ServiceSigningID},
		{client.StatusMachService, RoleGUI, client.StatusRequirement(channel),
			client.AppSigningID + "|" + client.ServiceSigningID},
	}
}

// maxPeerInflight bounds concurrent requests on one connection. A retire can
// hold a request open for up to client.MaxRetireWait.
const maxPeerInflight = 4

type xpcPeer struct {
	id       uint64
	caller   Caller
	key      *jobKey // the op this connection is bound to
	binding  bool
	closed   bool
	inflight int
}

type xpcTransport struct {
	svc       xpcService
	channel   string
	listeners []xpcListener
	logf      func(string, ...any)
	base      context.Context

	// mu orders binding against the orphan timer. The bound count and the
	// core's timer state change together under mu: Disconnected (arm) and the
	// post-bind Reclaim (cancel) both run while mu is held, so "last
	// connection closed" and "a connection bound" can never interleave into
	// an armed timer while a connection is bound.
	//
	// Lock order: mu, then the core's locks (Service.mu, Store.mu). The core
	// never calls into the transport, and dispatch calls the core's
	// Register/Retire/Status/Reclaim WITHOUT mu, so no path takes them in the
	// reverse order.
	mu       sync.Mutex
	peers    map[uint64]*xpcPeer
	bound    map[jobKey]int // live connections bound to each op
	active   int            // requests being dispatched, all peers
	draining bool           // set once by quiesce: refuse every new request

	// Test seams, nil in production.
	beforeBind       func(peer uint64) // dispatch: after the core accepted a bind, before bindFinish
	beforeDisconnect func(k jobKey)    // after the last connection was judged gone, before arming
}

func newXPCTransport(svc xpcService, channel string, logf func(string, ...any)) *xpcTransport {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &xpcTransport{
		svc: svc, channel: channel, listeners: xpcListeners(channel), logf: logf,
		base: context.Background(), peers: map[uint64]*xpcPeer{}, bound: map[jobKey]int{},
	}
}

// peerOpen admits a new connection on listener. setRequirement applies the
// listener's requirement to the connection (libxpc in production). If it
// fails the connection is refused: nothing it sends ever reaches the core.
func (t *xpcTransport) peerOpen(listener int, id uint64, euid, asid uint32, setRequirement func(string) error) bool {
	if listener < 0 || listener >= len(t.listeners) {
		return false
	}
	l := t.listeners[listener]
	if err := setRequirement(l.requirement); err != nil {
		t.logf("peer %d on %s refused: code-signing requirement not applied: %v", id, l.name, err)
		return false
	}
	p := &xpcPeer{id: id, caller: Caller{UID: euid, Role: l.role, AuditSession: asid, SigningID: l.signingID}}
	t.mu.Lock()
	t.peers[id] = p
	t.mu.Unlock()
	return true
}

// peerRejected: libxpc found a message whose sender failed the requirement.
func (t *xpcTransport) peerRejected(id uint64) {
	t.mu.Lock()
	p := t.peers[id]
	t.mu.Unlock()
	if p != nil {
		t.logf("peer %d (uid=%d asid=%d) failed the %s code-signing requirement; connection cancelled",
			id, p.caller.UID, p.caller.AuditSession, roleName(p.caller.Role))
	}
}

// peerClosed: the connection is gone. If it was the last one bound to an op,
// the owner has disconnected: start the core's orphan window.
func (t *xpcTransport) peerClosed(id uint64) {
	t.mu.Lock()
	p := t.peers[id]
	if p == nil {
		t.mu.Unlock()
		return
	}
	delete(t.peers, id)
	p.closed = true
	if key, last := t.unbindLocked(p); last {
		t.disconnectLocked(key)
	}
	t.mu.Unlock()
}

func (t *xpcTransport) unbindLocked(p *xpcPeer) (jobKey, bool) {
	if p.key == nil {
		return jobKey{}, false
	}
	k := *p.key
	p.key = nil
	t.bound[k]--
	if t.bound[k] > 0 {
		return k, false
	}
	delete(t.bound, k)
	return k, true
}

// disconnectLocked arms the orphan window. Caller holds t.mu (see mu).
// Liveness is Unknown: a dropped connection proves only that the service
// went away, not that the server behind the mount (the NFS engine or
// juicefs) is dead, so the orphan retirement may only try an ordinary
// unmount. An op whose owner last sent Cancel is never restarted (see
// Service.Disconnected).
//
// Known remaining risk (accepted, not widened to a force): for an NFS
// receipt the NFS engine usually died WITH the service that dropped the
// connection. The bridge proves that and retires with Liveness Dead (a force)
// only if the service restarts and reclaims within OrphanReclaim. If it does
// not, this Unknown-liveness ordinary unmount goes to a dead NFS server; per
// converge's analysis it can block in VFS_SYNC with MNT_LUNMOUNT set and
// leave the mount wedged until reboot. The helper cannot tell a dead engine
// from a slow one, and forcing on a guess could drop data a live engine was
// still writing, so the trade-off stays on the side of never forcing.
func (t *xpcTransport) disconnectLocked(k jobKey) {
	if t.beforeDisconnect != nil {
		t.beforeDisconnect(k)
	}
	err := t.svc.Disconnected(k.uid, k.op, LivenessUnknown)
	switch {
	case err == nil:
		t.logf("owner of uid=%d op=%s disconnected; orphan window started", k.uid, k.op)
	case errors.Is(err, ErrNoReceipt):
		// Already retired: nothing to protect.
	default:
		t.logf("disconnect uid=%d op=%s: %v", k.uid, k.op, err)
	}
}

// handle decodes one request, runs it, and returns the encoded reply. nil
// means "send no reply" (unknown or closed peer).
func (t *xpcTransport) handle(id uint64, version int64, body []byte) []byte {
	t.mu.Lock()
	p := t.peers[id]
	if p == nil || p.closed {
		t.mu.Unlock()
		return nil
	}
	if t.draining {
		t.mu.Unlock()
		return t.encode(client.Response{Code: client.CodeBusy, Message: "daemon restarting for an update; retry"})
	}
	if p.inflight >= maxPeerInflight {
		t.mu.Unlock()
		return t.encode(client.Response{Code: client.CodeBusy})
	}
	p.inflight++
	t.active++
	caller := p.caller
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		p.inflight--
		t.active--
		t.mu.Unlock()
	}()

	req, err := client.DecodeRequest(version, body)
	if err != nil {
		return t.encode(client.Response{Code: client.CodeBadRequest, Message: err.Error()})
	}
	return t.encode(t.dispatch(p, caller, req))
}

func (t *xpcTransport) encode(r client.Response) []byte {
	b, err := client.EncodeResponse(r)
	if err != nil {
		t.logf("encode response: %v", err)
		b, _ = client.EncodeResponse(client.Response{Code: client.CodeInternal})
	}
	return b
}

func (t *xpcTransport) dispatch(p *xpcPeer, caller Caller, req client.Request) client.Response {
	switch req.Op {
	case client.OpPing:
		return client.Response{Code: client.CodeOK, Ping: &client.PingInfo{
			Protocol: client.ProtocolVersion, Channel: t.channel, Role: roleName(caller.Role), UID: caller.UID}}

	case client.OpStatus:
		out, err := t.svc.Status(caller, req.OpID)
		if err != nil {
			return errResponse(err)
		}
		return client.Response{Code: client.CodeOK, Outcome: wireOutcome(out)}

	case client.OpRetire:
		ctx, cancel := context.WithTimeout(t.base, time.Duration(req.WaitMillis)*time.Millisecond)
		defer cancel()
		out, err := t.svc.Retire(ctx, caller, req.OpID, RetireRequest{
			Liveness: livenessFromWire(req.Retire.Liveness), EjectNow: req.Retire.EjectNow})
		if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
			return client.Response{Code: client.CodePending, Outcome: wireOutcome(out)}
		}
		if err != nil {
			return errResponse(err)
		}
		return client.Response{Code: client.CodeOK, Outcome: wireOutcome(out)}

	case client.OpCancel:
		out, err := t.svc.Cancel(caller, req.OpID)
		if err != nil {
			resp := errResponse(err)
			resp.Outcome = wireOutcome(out)
			return resp
		}
		return client.Response{Code: client.CodeOK, Outcome: wireOutcome(out)}

	case client.OpRegister:
		if !t.bindStart(p) {
			return client.Response{Code: client.CodeAlreadyBound}
		}
		a := req.Register
		r, err := t.svc.Register(caller, RegisterRequest{
			OpID: req.OpID, Type: MountType(a.Type), FSID: FSID(a.FSID), Token: a.Token,
			MntOnName: a.MntOnName, MntFromName: a.MntFromName, Generation: a.Generation})
		if err != nil {
			t.bindAbort(p)
			return errResponse(err)
		}
		if t.beforeBind != nil {
			t.beforeBind(p.id)
		}
		t.bindFinish(p, caller, jobKey{caller.UID, req.OpID})
		return client.Response{Code: client.CodeOK, Receipt: wireReceipt(r)}

	case client.OpReclaim:
		if !t.bindStart(p) {
			return client.Response{Code: client.CodeAlreadyBound}
		}
		reclaimed, err := t.svc.Reclaim(caller, req.OpID)
		if err != nil {
			t.bindAbort(p)
			return errResponse(err)
		}
		out, err := t.svc.Status(caller, req.OpID)
		if err != nil {
			t.bindAbort(p)
			resp := errResponse(err)
			resp.Reclaimed = reclaimed
			return resp
		}
		if out.State.terminal() {
			// Finished: there is no mount left to hold.
			t.bindAbort(p)
			return client.Response{Code: client.CodeNoReceipt, Message: "op already finished: " + string(out.State),
				Outcome: wireOutcome(out), Reclaimed: reclaimed}
		}
		if t.beforeBind != nil {
			t.beforeBind(p.id)
		}
		if t.bindFinish(p, caller, jobKey{caller.UID, req.OpID}) {
			reclaimed = true
		}
		return client.Response{Code: client.CodeOK, Outcome: wireOutcome(out), Reclaimed: reclaimed}
	}
	return client.Response{Code: client.CodeBadRequest, Message: "unknown op"}
}

// A connection binds to at most one op, once.
func (t *xpcTransport) bindStart(p *xpcPeer) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p.key != nil || p.binding {
		return false
	}
	p.binding = true
	return true
}

func (t *xpcTransport) bindAbort(p *xpcPeer) {
	t.mu.Lock()
	p.binding = false
	t.mu.Unlock()
}

// bindFinish records the binding and, under the same lock, cancels any
// orphan timer armed since the core accepted the bind (e.g. the previous
// owner's connection closed in between and was judged the last one). It
// reports whether it cancelled one.
func (t *xpcTransport) bindFinish(p *xpcPeer, caller Caller, k jobKey) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	p.binding = false
	if p.closed {
		// The owner vanished while its register/reclaim was in flight.
		if t.bound[k] == 0 {
			t.disconnectLocked(k)
		}
		return false
	}
	p.key = &k
	t.bound[k]++
	cancelled, err := t.svc.Reclaim(caller, k.op)
	if err != nil {
		t.logf("post-bind reclaim uid=%d op=%s: %v", k.uid, k.op, err)
	}
	return cancelled
}

// quiesce stops the transport for good if, atomically with respect to
// dispatch, no request is in flight and idle() holds: from then on every
// request is refused (CodeBusy, retryable), so nothing can start retirement
// work between the check and the process exit. It returns false, changing
// nothing, if either condition fails.
func (t *xpcTransport) quiesce(idle func() bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.draining {
		return true
	}
	if t.active > 0 || !idle() {
		return false
	}
	t.draining = true
	return true
}

func roleName(r Role) string {
	switch r {
	case RoleService:
		return "service"
	case RoleGUI:
		return "gui"
	}
	return "none"
}

// errCodes maps core sentinels to wire codes. Anything else is internal and
// its text stays in the daemon log.
var errCodes = []struct {
	err  error
	code string
}{
	{ErrNotAuthorized, client.CodeNotAuthorized},
	{ErrReceiptLimit, client.CodeReceiptLimit},
	{ErrReceiptExists, client.CodeReceiptExists},
	{ErrNoReceipt, client.CodeNoReceipt},
	{ErrInvalidReceipt, client.CodeInvalidReceipt},
	{ErrIdentityRefused, client.CodeIdentityRefused},
	{ErrOpIDReused, client.CodeOpIDReused},
	{ErrPostState, client.CodePostState},
	{ErrNotCancellable, client.CodeNotCancellable},
}

func errResponse(err error) client.Response {
	for _, e := range errCodes {
		if errors.Is(err, e.err) {
			return client.Response{Code: e.code, Message: err.Error()}
		}
	}
	return client.Response{Code: client.CodeInternal}
}

func livenessFromWire(s string) Liveness {
	switch s {
	case client.LivenessAlive:
		return LivenessAlive
	case client.LivenessDead:
		return LivenessDead
	case client.LivenessWedged:
		return LivenessWedged
	}
	return LivenessUnknown
}

func wireEntries(es []Entry) []client.Entry {
	if len(es) == 0 {
		return nil
	}
	out := make([]client.Entry, len(es))
	for i, e := range es {
		out[i] = client.Entry{FSID: [2]int32(e.FSID), Owner: e.Owner, Type: e.Type, MntOnName: e.MntOnName, MntFromName: e.MntFromName}
	}
	return out
}

func wireOutcome(o Outcome) *client.Outcome {
	w := &client.Outcome{
		OpID: o.OpID, State: string(o.State), Reason: o.Reason, Syscalls: o.Syscalls, Forced: o.Forced,
		StuckObserved: o.StuckObserved, HoldersStale: o.HoldersStale,
		Descendants: wireEntries(o.Descendants), Anomaly: wireEntries(o.Anomaly),
	}
	for _, h := range o.Holders {
		w.Holders = append(w.Holders, client.Holder{PID: h.PID, UID: h.UID, Name: h.Name, Files: h.Files})
	}
	if o.Err != nil {
		w.Err = o.Err.Error()
	}
	return w
}

func wireReceipt(r Receipt) *client.Receipt {
	return &client.Receipt{UID: r.UID, Boot: r.Boot, OpID: r.OpID, Type: string(r.Type), FSID: [2]int32(r.FSID),
		MntOnName: r.MntOnName, MntFromName: r.MntFromName, Generation: r.Generation}
}

// idle reports that no retirement work is in progress: no running job, no
// pending orphan timer, no unmount syscall in the kernel. Bound connections
// do not count; their owners re-bind after a restart (client.Lease).
func (s *Service) idle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.jobs) == 0 && len(s.orphans) == 0 && len(s.inflight) == 0
}

// ArmLaunchOrphans starts the orphan window for every receipt the launch
// sweep found still mounted. Their owners (if alive) re-bind with "reclaim"
// within Policy.OrphanReclaim; the rest are retired with LivenessUnknown,
// i.e. by ordinary unmount only. Call it after SweepAtLaunch and before the
// transport starts listening, so a queued reclaim always finds its timer.
func ArmLaunchOrphans(svc *Service, rep SweepReport, logf func(string, ...any)) int {
	armed := 0
	for _, r := range rep.Pending {
		if err := svc.Disconnected(r.UID, r.OpID, LivenessUnknown); err != nil {
			if logf != nil {
				logf("arm orphan uid=%d op=%s: %v", r.UID, r.OpID, err)
			}
			continue
		}
		armed++
	}
	return armed
}
