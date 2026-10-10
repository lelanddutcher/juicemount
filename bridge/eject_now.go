package main

// H-3 L4/L5 service-side wiring of the privileged unmount helper
// (juicemount-unmountd). This file owns:
//
//   - the process's one helper client (role "service"), shared by the NFS
//     controller, the FUSE generation manager and recovery;
//   - the control routes the GUI's busy-Quit panel uses (/eject-now);
//   - the health monitor's StaleMountRetirer implementation;
//   - the /health label for the helper receipt.
//
// The helper retires a kernel mount by fsid plus the receipt this service
// registered, never by path. Nothing here takes a path to unmount.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/nfscustody"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// serviceUnmountHelper is everything this process asks of the helper. It
// satisfies both nfscustody.UnmountHelper and health.UnmountHelper (their
// lease types are the same unnamed interface).
type serviceUnmountHelper interface {
	Ping(ctx context.Context) (client.PingInfo, error)
	Register(ctx context.Context, rr client.RegisterRequest) (health.UnmountLease, error)
	Reclaim(ctx context.Context, opID string) (health.UnmountLease, client.ReclaimResult, error)
	Retire(ctx context.Context, opID string, rr client.RetireRequest, wait time.Duration) (client.Outcome, error)
	Cancel(ctx context.Context, opID string) (client.Outcome, error)
}

var (
	_ nfscustody.UnmountHelper = serviceUnmountHelper(nil)
	_ health.UnmountHelper     = serviceUnmountHelper(nil)
)

// unmountdAdapter wraps the real XPC client.
type unmountdAdapter struct{ c *client.Client }

func (a unmountdAdapter) Ping(ctx context.Context) (client.PingInfo, error) { return a.c.Ping(ctx) }

func (a unmountdAdapter) Register(ctx context.Context, rr client.RegisterRequest) (health.UnmountLease, error) {
	l, _, err := a.c.Register(ctx, rr)
	if err != nil || l == nil {
		if err == nil {
			err = errors.New("unmount helper returned no lease")
		}
		return nil, err // never a typed-nil interface
	}
	return l, nil
}

func (a unmountdAdapter) Reclaim(ctx context.Context, opID string) (health.UnmountLease, client.ReclaimResult, error) {
	l, res, err := a.c.Reclaim(ctx, opID)
	if err != nil || l == nil {
		if err == nil {
			err = errors.New("unmount helper returned no lease")
		}
		return nil, res, err
	}
	return l, res, nil
}

func (a unmountdAdapter) Cancel(ctx context.Context, opID string) (client.Outcome, error) {
	return a.c.Cancel(ctx, opID)
}

func (a unmountdAdapter) Retire(ctx context.Context, opID string, rr client.RetireRequest, wait time.Duration) (client.Outcome, error) {
	return a.c.Retire(ctx, opID, rr, wait)
}

var (
	processHelperOnce sync.Once
	processHelper     serviceUnmountHelper
)

// processUnmountHelper returns this process's helper client, or nil in a Go
// test binary (tests inject a fake; they must never dial the real daemon,
// which may be installed and approved on a developer Mac). The client does
// not connect until its first call; an unregistered or unapproved helper
// then fails fast with client.ErrUnavailable and every caller keeps the
// descriptor-only behaviour.
var processUnmountHelper = func() serviceUnmountHelper {
	processHelperOnce.Do(func() {
		if strings.HasSuffix(os.Args[0], ".test") {
			return
		}
		processHelper = unmountdAdapter{client.New(client.Options{Logf: func(format string, args ...any) {
			jmlog.Info("unmountd client", "detail", fmt.Sprintf(format, args...))
		}})}
	})
	return processHelper
}

func init() {
	// Every generation FUSE manager in this binary (the volume owner and the
	// in-process bridge) registers its receipt with the same client.
	if h := processUnmountHelper(); h != nil {
		health.SetDefaultUnmountHelper(h)
	}
}

// helperRetiringNFS is the controller surface the helper routes need. The
// production *nfscustody.Controller implements it.
type helperRetiringNFS interface {
	RetireWithHelper(ctx context.Context, rr client.RetireRequest, wait time.Duration) (client.Outcome, error)
	// HelperWithdrawGeneration / RetireWithHelperAt order a POST against a
	// DELETE that lands while it is in flight (nfscustody.HelperRetireGate).
	HelperWithdrawGeneration() uint64
	RetireWithHelperAt(ctx context.Context, gen uint64, rr client.RetireRequest, wait time.Duration) (client.Outcome, error)
	// StartRetireWithHelperAt is the busy-Quit START request: ordinary
	// only, and it restarts the busy window at its arrival.
	StartRetireWithHelperAt(ctx context.Context, gen uint64, rr client.RetireRequest, wait time.Duration) (client.Outcome, error)
	HelperStatus() nfscustody.HelperStatus
	CancelHelperRetire()
	WithdrawHelperRetire(ctx context.Context) (client.Outcome, error)
	OrdinaryBusy() (since, last time.Time)
}

var _ helperRetiringNFS = (*nfscustody.Controller)(nil)

func currentHelperNFS() (nfsMountController, helperRetiringNFS) {
	globalMu.Lock()
	c := globalNFSController
	globalMu.Unlock()
	if c == nil {
		return nil, nil
	}
	rc, _ := c.(helperRetiringNFS)
	return c, rc
}

// ejectNowWait is the helper retire wait for one eject-now (the daemon caps
// a single retire request at client.MaxRetireWait).
var ejectNowWait = time.Duration(client.MaxRetireWait) * time.Millisecond

// Eject-now force gate (founder floor): a force is sent only for an ESCALATE
// request, after an ordinary retire of the same op has been coming back BUSY
// for at least ejectNowFloor since the most recent START request, and is
// confirmed busy again just now. A start request restarts the window at its
// arrival; the controller also restarts it after a gap
// (nfscustody.OrdinaryBusyMaxGap), so an old observation never counts.
var (
	ejectNowFloor        = client.QuitForceAfter
	ejectNowOrdinaryWait = 3 * time.Second
	ejectNowNow          = time.Now
)

type ejectNowReply struct {
	OK         bool   `json:"ok"`
	State      string `json:"state,omitempty"`
	Error      string `json:"error,omitempty"`
	Available  *bool  `json:"available,omitempty"`
	OpID       string `json:"op_id,omitempty"`
	RetryAfter int    `json:"retry_after,omitempty"` // seconds
}

// Eject-now request phases (POST body {"phase":…}).
const (
	// ejectNowPhaseStart: the busy-Quit countdown just began. Never forces.
	ejectNowPhaseStart = "start"
	// ejectNowPhaseEscalate: the countdown ended or the user pressed
	// "Eject now". May force once the floor has passed.
	ejectNowPhaseEscalate = "escalate"
)

// ejectNowPhase reads the POST's phase. No body, or a body without a phase
// (a GUI from before round 7), is a start: it never forces. Any other value
// is refused. (A server request's Body is never nil.)
func ejectNowPhase(r *http.Request) (string, error) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		return "", fmt.Errorf("reading the request body: %w", err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return ejectNowPhaseStart, nil
	}
	var req struct {
		Phase *string `json:"phase"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		return "", fmt.Errorf("unreadable request body: %w", err)
	}
	if req.Phase == nil {
		return ejectNowPhaseStart, nil
	}
	switch *req.Phase {
	case ejectNowPhaseStart, ejectNowPhaseEscalate:
		return *req.Phase, nil
	}
	return "", fmt.Errorf("unknown eject phase %q", *req.Phase)
}

func writeEjectNow(w http.ResponseWriter, status int, reply ejectNowReply) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(reply)
}

// handleEjectNowHTTP is the busy-Quit force-eject (founder decision: after
// the GUI's 30 s countdown).
//
//	GET    /eject-now → 200 {"available":true} only when this service's own
//	                    helper client pings as role "service"; otherwise
//	                    200 {"available":false,"error":…}.
//	POST   /eject-now  (X-JM-Control: 1) for the op this service owns (its NFS
//	                    mount; the FUSE generation belongs to the volume
//	                    owner and is retired by its own Stop once NFS is gone).
//	                    Body {"phase":"start"} or {"phase":"escalate"}; no
//	                    body or no phase means "start". Another phase is
//	                    400 {"state":"bad_request"}.
//	                    - an ordinary retire runs first (and may just finish:
//	                      200 {"ok":true,"state":"RETIRED"});
//	                    - "start" (the countdown began) NEVER forces. It ends
//	                      any earlier busy window and starts a new one at its
//	                      own arrival, so the force floor always counts from
//	                      the countdown the user was shown. Still busy:
//	                      202 {"ok":false,"state":"ordinary_in_progress",
//	                      "retry_after":N};
//	                    - "escalate" (the countdown ended, or "Eject now"):
//	                      until ordinary retires of the op have been BUSY for
//	                      30 s, measured from the ARRIVAL of the most recent
//	                      start (or, if none reached this service, of the
//	                      first busy request), 202 with retry_after N and
//	                      nothing is forced;
//	                    - "escalate" after the floor, confirmed busy again
//	                      now: Retire(op, {Alive, EjectNow}, 60 s) →
//	                      200 {"ok":true,"state":"RETIRED"} or
//	                      {"ok":false,"error":…,"state":…}.
//	                    A DELETE that lands while a POST is in flight wins
//	                    unless that POST's force was already sent: the POST
//	                    then forces nothing, withdraws any ordinary job it
//	                    started (unless a newer POST shares it), and answers
//	                    409 {"ok":false,"state":"withdrawn"}. JuiceMount's own
//	                    withdrawals (a Stop or the stale retirer giving up)
//	                    never refuse a POST: one whose job they ended answers
//	                    202 "ordinary_in_progress" with retry_after 1.
//	DELETE /eject-now  (X-JM-Control: 1) withdraws the ordinary retirement a
//	                    POST started: 200 {"ok":true,"state":…}. The helper
//	                    issues nothing further for it, and its orphan timer
//	                    will not restart it. "state":"RETIRED" means there was
//	                    nothing left to withdraw: the volume is already
//	                    ejected. A force already requested or issued is never
//	                    withdrawn: 409 {"ok":false,"state":"force_in_progress",…}.
//
// GUI contract (the busy-Quit panel):
//   - The countdown starts: POST {"phase":"start"}. A 202 means "keep
//     counting"; it never forces, whatever happened before.
//   - "Eject now" (or the countdown ending): POST {"phase":"escalate"}. While
//     the answer is 202, wait retry_after seconds and POST escalate again;
//     stop on 200 (ejected) or any 4xx/5xx (show the error). A single POST
//     is not enough: a 202 forces nothing, and the force happens only on a
//     later escalate POST, 30 s after the start at the earliest.
//   - Send "escalate" only after a start POST was answered 202. A start with
//     no answer (or an error answer) may never have reached this service,
//     which can still hold a busy window an earlier panel left; an escalate
//     would force on it at once. Send "start" again instead, and escalate
//     after that 202's retry_after (review #7, finding 1).
//   - "Cancel Quit", or the panel closing without an eject: stop the POST
//     loop and send DELETE once. Without it, the ordinary job the first POST
//     started keeps retrying in the helper and ejects the volume as soon as
//     the apps let go, while JuiceMount keeps running.
//   - Do not DELETE after a POST has crossed the floor: that is the user's
//     force, and DELETE then answers 409.
func handleEjectNowHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		available := false
		reply := ejectNowReply{Available: &available}
		h := processUnmountHelper()
		if h == nil {
			reply.Error = "unmount helper client unavailable in this process"
			writeEjectNow(w, http.StatusOK, reply)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		info, err := h.Ping(ctx)
		cancel()
		switch {
		case err != nil:
			reply.Error = err.Error()
		case info.Role != "service":
			reply.Error = "unmount helper admitted this service as role " + info.Role
		default:
			available = true
			reply.OK = true
		}
		writeEjectNow(w, http.StatusOK, reply)
		return
	}
	if r.Method == http.MethodDelete {
		handleEjectNowWithdraw(w, r)
		return
	}
	if !requireLocalControlMutation(w, r) {
		return
	}
	if processUnmountHelper() == nil {
		writeEjectNow(w, http.StatusServiceUnavailable, ejectNowReply{Error: "unmount helper unavailable", State: "UNAVAILABLE"})
		return
	}
	phase, perr := ejectNowPhase(r)
	if perr != nil {
		writeEjectNow(w, http.StatusBadRequest, ejectNowReply{Error: perr.Error(), State: "bad_request"})
		return
	}
	_, rc := currentHelperNFS()
	if rc == nil {
		writeEjectNow(w, http.StatusConflict, ejectNowReply{Error: "this service owns no mount registered with the unmount helper", State: "NO_RECEIPT"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), ejectNowWait+ejectNowOrdinaryWait+10*time.Second)
	defer cancel()
	op := rc.HelperStatus().OpID
	// Read on ARRIVAL: a DELETE after this point wins over this request.
	gen := rc.HelperWithdrawGeneration()
	// The NFS engine is this live process: liveness Alive is proven. The
	// ordinary pass both starts the floor and confirms "still busy" now. A
	// start request restarts the floor at its arrival and never forces.
	ordinary := rc.RetireWithHelperAt
	if phase == ejectNowPhaseStart {
		ordinary = rc.StartRetireWithHelperAt
	}
	out, err := ordinary(ctx, gen, client.RetireRequest{Liveness: client.LivenessAlive}, ejectNowOrdinaryWait)
	if errors.Is(err, nfscustody.ErrHelperRetireWithdrawn) {
		writeEjectNowWithdrawn(w, op)
		return
	}
	if errors.Is(err, nfscustody.ErrHelperRetireInterrupted) {
		// JuiceMount's own withdrawal (a Stop or the stale retirer giving
		// up) ended the job this request joined. The user did not cancel:
		// say so with a 202, which the GUI treats like any other "still in
		// progress" reply. Before escalation it ignores retry_after and keeps
		// counting: its next POST comes when the countdown ends (or on "Eject
		// now"), and until then no ordinary job runs. After escalation it
		// POSTs again after retry_after (1 s). Either way the next POST starts
		// a fresh job and a fresh 30 s floor.
		jmlog.Info("eject-now: an internal withdrawal ended the ordinary job; asking again", "op", op)
		writeEjectNow(w, http.StatusAccepted, ejectNowReply{State: "ordinary_in_progress", OpID: op, RetryAfter: 1,
			Error: "JuiceMount stopped its own eject attempt while this request was in flight; nothing was forced. Ejecting normally again"})
		return
	}
	if err == nil {
		jmlog.Info("eject-now: ordinary helper retire finished", "op", op)
		writeEjectNow(w, http.StatusOK, ejectNowReply{OK: true, State: out.State, OpID: op})
		return
	}
	if !errors.Is(err, client.ErrPending) {
		writeEjectNow(w, http.StatusBadGateway, ejectNowReply{Error: err.Error(), State: out.State, OpID: op})
		return
	}
	since, _ := rc.OrdinaryBusy()
	now := ejectNowNow()
	if phase != ejectNowPhaseEscalate || since.IsZero() || now.Sub(since) < ejectNowFloor {
		retry := ejectNowFloor
		if !since.IsZero() {
			retry = ejectNowFloor - now.Sub(since)
		}
		if retry < time.Second {
			retry = time.Second
		}
		jmlog.Info("eject-now: ordinary retire still busy; force waits for the floor", "op", op, "phase", phase, "retry_after_s", int(retry.Seconds()))
		writeEjectNow(w, http.StatusAccepted, ejectNowReply{State: "ordinary_in_progress", OpID: op,
			Error: "ejecting normally; apps still have files open", RetryAfter: int((retry + time.Second - 1) / time.Second)})
		return
	}
	out, err = rc.RetireWithHelperAt(ctx, gen, client.RetireRequest{Liveness: client.LivenessAlive, EjectNow: true}, ejectNowWait)
	if errors.Is(err, nfscustody.ErrHelperRetireWithdrawn) {
		writeEjectNowWithdrawn(w, op)
		return
	}
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, client.ErrPending) {
			status = http.StatusGatewayTimeout
		}
		msg := err.Error()
		if len(out.Descendants) > 0 {
			msg += fmt.Sprintf(" (%d volume(s) mounted inside; eject them first)", len(out.Descendants))
		}
		jmlog.Warn("eject-now: helper did not retire the NFS mount", "op", op, "state", out.State, "error", msg)
		writeEjectNow(w, status, ejectNowReply{Error: msg, State: out.State, OpID: op})
		return
	}
	jmlog.Info("eject-now: helper retired the NFS mount", "op", op, "forced", out.Forced)
	writeEjectNow(w, http.StatusOK, ejectNowReply{OK: true, State: out.State, OpID: op})
}

// writeEjectNowWithdrawn answers a POST that the user's DELETE overtook:
// nothing was forced, and any ordinary job it started was withdrawn again.
// Only a user withdrawal reaches here (nfscustody.ErrHelperRetireWithdrawn);
// JuiceMount's own withdrawals never refuse a POST.
func writeEjectNowWithdrawn(w http.ResponseWriter, op string) {
	jmlog.Info("eject-now: request withdrawn while in flight; nothing forced", "op", op)
	writeEjectNow(w, http.StatusConflict, ejectNowReply{State: "withdrawn", OpID: op,
		Error: "the eject was withdrawn (Quit was cancelled) while this request was in flight; nothing was forced"})
}

// handleEjectNowWithdraw is DELETE /eject-now: withdraw this service's
// ordinary helper retirement (the busy-Quit panel's "Cancel Quit"). It never
// withdraws a force; the helper refuses that and the refusal is reported.
func handleEjectNowWithdraw(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(localControlHeader) != "1" {
		http.Error(w, "missing local control header", http.StatusForbidden)
		return
	}
	_, rc := currentHelperNFS()
	if rc == nil {
		writeEjectNow(w, http.StatusConflict, ejectNowReply{Error: "this service owns no mount registered with the unmount helper", State: "NO_RECEIPT"})
		return
	}
	op := rc.HelperStatus().OpID
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	out, err := rc.WithdrawHelperRetire(ctx)
	switch {
	case errors.Is(err, client.ErrNotCancellable):
		jmlog.Info("eject-now: withdraw refused; a force eject is under way", "op", op, "state", out.State)
		writeEjectNow(w, http.StatusConflict, ejectNowReply{State: "force_in_progress", OpID: op,
			Error: "the forced eject is already under way and cannot be withdrawn"})
	case err != nil:
		writeEjectNow(w, http.StatusBadGateway, ejectNowReply{Error: err.Error(), State: out.State, OpID: op})
	case out.State == "RETIRED":
		// Nothing left to withdraw: the job had already ejected the volume.
		jmlog.Info("eject-now: withdraw found the volume already ejected", "op", op)
		writeEjectNow(w, http.StatusOK, ejectNowReply{OK: true, State: out.State, OpID: op})
	default:
		jmlog.Info("eject-now: ordinary helper retirement withdrawn", "op", op, "state", out.State)
		writeEjectNow(w, http.StatusOK, ejectNowReply{OK: true, State: out.State, OpID: op})
	}
}

// nfsStaleRetirer implements health.StaleMountRetirer: the monitor's stale
// NFS recovery retires the owned mount by its helper receipt, then completes
// the controller's durable retirement so a new birth is admitted. It takes
// no path, accepts only PROVEN liveness (Dead/Wedged; the monitor never
// calls it otherwise), and withdraws its job if it gives up.
type nfsStaleRetirer struct{}

var _ health.StaleMountRetirer = nfsStaleRetirer{}

func (nfsStaleRetirer) RetireStaleNFSMount(ctx context.Context, liveness string) error {
	if liveness != client.LivenessDead && liveness != client.LivenessWedged {
		return fmt.Errorf("stale NFS retirement refused: liveness %q is not proven", liveness)
	}
	c, rc := currentHelperNFS()
	if c == nil || rc == nil {
		return errors.New("no owned NFS controller with a helper receipt")
	}
	wait := 15 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		wait = time.Until(deadline) - 2*time.Second
	}
	if wait <= 0 {
		return errors.New("stale NFS retirement budget exhausted")
	}
	if _, err := rc.RetireWithHelper(ctx, client.RetireRequest{Liveness: liveness}, wait); err != nil {
		// Timed out or refused: withdraw the job (the helper refuses only a
		// force already under way; that is logged).
		rc.CancelHelperRetire()
		return err
	}
	if !stopOwnedNFSMount(c.Path()) {
		return errors.New("helper retired the NFS mount but the controller did not settle")
	}
	return nil
}

// wireStaleNFSRecovery gives the health monitor its stale-NFS recovery
// seams: the helper-backed retirer and the exact JuiceFS child's liveness.
// Without the liveness source the monitor never proves death and never calls
// the retirer (the helper-required state holds instead).
func wireStaleNFSRecovery(m *health.HealthMonitor) {
	m.SetStaleMountRetirer(nfsStaleRetirer{})
	m.SetJuiceFSLiveness(func() (bool, bool) { return juiceFSLivenessSource() })
}

// juiceFSLivenessSource is exactJuiceFSLiveness (a test seam).
var juiceFSLivenessSource = exactJuiceFSLiveness

// exactJuiceFSLiveness is the stale-NFS path's liveness source: the original
// child's Wait, either locally or through the selected owner's bounded
// authenticated control read. A held lifecycle lock is unproven.
func exactJuiceFSLiveness() (alive, proven bool) {
	if !globalMu.TryLock() {
		return false, false
	}
	fm := globalFUSE
	owner := globalOwner
	globalMu.Unlock()
	if fm != nil {
		return fm.ExactChildLiveness()
	}
	if owner != nil {
		evidence := owner.fuseEvidence()
		if !globalMu.TryLock() {
			return false, false
		}
		current := globalOwner == owner && globalFUSE == nil
		globalMu.Unlock()
		if !current {
			return false, false
		}
		return evidence.ChildAlive, evidence.ChildProven
	}
	return false, false
}

// unmountHelperHealthLabel is the /health "unmount_helper" component: "ok"
// while this service's NFS mount holds a helper receipt (or the helper
// retired it), otherwise the typed state and its reason.
func unmountHelperHealthLabel() string {
	_, rc := currentHelperNFS()
	if rc == nil {
		if processUnmountHelper() == nil {
			return "unavailable"
		}
		return "no-mount"
	}
	st := rc.HelperStatus()
	switch st.State {
	case nfscustody.HelperStateRegistered, nfscustody.HelperStateRetired, nfscustody.HelperStateRetiring:
		return "ok"
	}
	if st.Error != "" {
		return st.State + ": " + st.Error
	}
	return st.State
}
