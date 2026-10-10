package unmountd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"
)

// Policy holds the convergence timings (design §3).
type Policy struct {
	// Watch bounds one unmount syscall. When it expires the call is IN_KERNEL.
	Watch time.Duration
	// InKernelPoll is the snapshot cadence while a call is IN_KERNEL.
	InKernelPoll time.Duration
	// SnapshotWatch bounds one getfsstat; on expiry the snapshot is an error.
	SnapshotWatch time.Duration
	// HolderWatch bounds one holder listing; on expiry the listing is skipped.
	HolderWatch time.Duration
	// MaxZeroPresent is how many consecutive "unmount returned 0 but the fsid
	// is still present" results end the job as ANOMALY instead of retrying.
	MaxZeroPresent int
	// BackoffMin/Max bound the ordinary-EBUSY retry backoff.
	BackoffMin, BackoffMax time.Duration
	// NestedRetry is the cadence while a mount is nested inside ours.
	NestedRetry time.Duration
	// ForceConfirmGap separates the two consecutive descendant-free
	// snapshots required before MNT_FORCE.
	ForceConfirmGap time.Duration
	// ErrorRetry is the wait after a failed snapshot.
	ErrorRetry time.Duration
	// OrphanReclaim is how long a dropped owner has to reconnect.
	OrphanReclaim time.Duration
}

// DefaultPolicy is the design's timing.
func DefaultPolicy() Policy {
	return Policy{
		Watch:           20 * time.Second,
		InKernelPoll:    5 * time.Second,
		SnapshotWatch:   10 * time.Second,
		HolderWatch:     10 * time.Second,
		MaxZeroPresent:  3,
		BackoffMin:      2 * time.Second,
		BackoffMax:      30 * time.Second,
		NestedRetry:     30 * time.Second,
		ForceConfirmGap: 500 * time.Millisecond,
		ErrorRetry:      5 * time.Second,
		OrphanReclaim:   30 * time.Second,
	}
}

// State is a receipt's convergence state.
type State string

const (
	StateRegistered      State = "REGISTERED"
	StateVerify          State = "VERIFY"
	StateOrdinary        State = "ORDINARY"
	StateBackoff         State = "BACKOFF"
	StateInKernel        State = "IN_KERNEL"
	StateWaitNested      State = "WAIT_NESTED"
	StateForceConfirm    State = "FORCE_CONFIRM"
	StateForce           State = "FORCE"
	StateVerifyAbsent    State = "VERIFY_ABSENT"
	StateRetired         State = "RETIRED"
	StateRefusedIdentity State = "REFUSED_IDENTITY"
	StateVoid            State = "VOID"
	StateFailed          State = "FAILED"
	// StateAnomaly: the kernel reported success but the fsid stayed present
	// repeatedly (e.g. a filesystem's own vfs_sysctl intercepted
	// VFS_CTL_UMOUNT, vfs_subr.c:4801). Reported, never spun on.
	StateAnomaly State = "ANOMALY"
	// StateCancelled: the owner withdrew an ordinary retirement before any
	// force was requested or issued. No further call is issued after the
	// withdrawal; an ordinary call already in the kernel finishes on its own
	// (EBUSY: the job ends; success: the volume was idle and is gone). The
	// receipt stays; the op is REGISTERED again. Not terminal: the mount is
	// still there and still the owner's.
	StateCancelled State = "CANCELLED"
)

func (s State) terminal() bool {
	switch s {
	case StateRetired, StateRefusedIdentity, StateVoid, StateFailed, StateAnomaly:
		return true
	}
	return false
}

// RetireRequest is what the owner proved when asking for retirement.
//
// Flavor rules (review-mandated, see converge):
//   - Liveness Dead/Wedged → straight to MNT_FORCE after the identity check;
//     never an ordinary unmount first.
//   - otherwise (server alive or unproven) → ordinary MNT_NOBLOCK only, with
//     backoff on EBUSY, forever; a healthy busy volume is never forced...
//   - ...unless EjectNow (the user's explicit "Eject now"), which forces.
//   - No flavor ever forces while a mount is nested inside ours.
type RetireRequest struct {
	Liveness Liveness
	EjectNow bool
}

func (r RetireRequest) wantsForce() bool { return r.EjectNow || r.Liveness.provenGone() }

// Outcome is the result (or current progress) of a retirement.
type Outcome struct {
	OpID     string
	State    State
	Reason   string
	Syscalls int  // unmount syscalls issued by this job
	Forced   bool // a MNT_FORCE call was issued
	// StuckObserved: an ordinary call outlived the watch (IN_KERNEL). It is
	// reported, not acted on: by itself it does not prove the server dead.
	StuckObserved bool
	Holders       []Holder // filtered to the caller's uid
	// HoldersStale: a previous holder listing for this fsid is still stuck in
	// the kernel, so no new one was started and Holders may be out of date.
	HoldersStale bool
	Descendants  []Entry
	// Anomaly lists entries, other than ours, present before our successful
	// unmount and absent after it (post_state_anomaly incident).
	Anomaly []Entry
	Err     error
}

// Config wires a Service.
type Config struct {
	Store   *Store
	Kernel  Kernel
	Holders HolderLister // optional
	Policy  Policy
	Clock   Clock // optional
	Logf    func(format string, args ...any)
}

// Service is the helper's API surface for the transport lane.
type Service struct {
	store   *Store
	kernel  Kernel
	holders HolderLister
	pol     Policy
	clock   Clock
	logf    func(string, ...any)

	base   context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	snapCur  *snapCall       // the one getfsstat in flight, if any
	holderIn map[FSID]bool   // a holder listing for this fsid is in flight
	inflight map[FSID]*kcall // at most one unmount syscall per fsid, ever
	jobs     map[jobKey]*job
	results  map[jobKey]Outcome
	orphans  map[jobKey]chan struct{}
	// withdrawn: ops whose last owner request was an accepted Cancel. The
	// orphan timer never restarts them (see Disconnected). Mirrored durably
	// in the store (Store.MarkWithdrawn) so a daemon restart keeps it.
	withdrawn map[jobKey]bool
	// parked: withdrawn ops whose orphan timer fired and was skipped. The
	// owner is gone, so the op stays reclaimable: a successor's Reclaim
	// returns true for it, exactly as for a pending timer, and its own
	// retirement runs. Without this the successor saw "still held by a live
	// owner" and the mount was stranded until reboot. Not counted by idle():
	// after a daemon restart the launch sweep re-arms the timer, which parks
	// the op again.
	parked map[jobKey]bool

	// orphanFired is a test seam run after an orphan timer fires and before
	// it takes the lock.
	orphanFired func(uid uint32, op string)
	// beforeIssue is a test seam run after a pass has chosen its unmount
	// flavor and before it issues the call.
	beforeIssue func(op string, flags int)
	// afterConverge is a test seam run after converge returned and before
	// run decides whether the job ends; beforeRemove runs after the job is
	// marked ended and before it leaves s.jobs.
	afterConverge func(op string, st State)
	beforeRemove  func(op string)
	// beforeStart is a test seam run after a starter read the receipt and
	// before it re-takes the lock to publish its new job.
	beforeStart func(op string)
}

type jobKey struct {
	uid uint32
	op  string
}

type kcall struct {
	flags int
	done  chan struct{}
	err   error
}

type job struct {
	key     jobKey
	receipt Receipt
	caller  Caller
	done    chan struct{}
	wake    chan struct{}

	mu        sync.Mutex
	req       RetireRequest
	out       Outcome
	cancelled bool // owner withdrew the ordinary retirement (Cancel)
	// ended: the job goroutine has stopped converging and is about to leave
	// s.jobs. A request that finds an ended job must not join it (its waiter
	// would get the old result, e.g. CANCELLED with no error); it waits for
	// the removal and starts a fresh job. Set under mu.
	ended bool
}

func (j *job) request() RetireRequest {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.req
}

func (j *job) update(f func(*Outcome)) {
	j.mu.Lock()
	f(&j.out)
	j.mu.Unlock()
}

func (j *job) snapshot() Outcome {
	j.mu.Lock()
	defer j.mu.Unlock()
	o := j.out
	o.Holders = append([]Holder(nil), o.Holders...)
	o.Descendants = append([]Entry(nil), o.Descendants...)
	o.Anomaly = append([]Entry(nil), o.Anomaly...)
	return o
}

// NewService builds a Service. Close it to stop background jobs.
func NewService(cfg Config) (*Service, error) {
	if cfg.Store == nil || cfg.Kernel == nil {
		return nil, errors.New("unmountd: store and kernel are required")
	}
	if cfg.Policy == (Policy{}) {
		cfg.Policy = DefaultPolicy()
	}
	if cfg.Policy.HolderWatch <= 0 {
		cfg.Policy.HolderWatch = cfg.Policy.SnapshotWatch
	}
	if cfg.Policy.MaxZeroPresent <= 0 {
		cfg.Policy.MaxZeroPresent = 3
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		store: cfg.Store, kernel: cfg.Kernel, holders: cfg.Holders, pol: cfg.Policy,
		clock: cfg.Clock, logf: cfg.Logf, base: ctx, cancel: cancel,
		holderIn: map[FSID]bool{},
		inflight: map[FSID]*kcall{}, jobs: map[jobKey]*job{},
		results: map[jobKey]Outcome{}, orphans: map[jobKey]chan struct{}{},
		withdrawn: map[jobKey]bool{}, parked: map[jobKey]bool{},
	}, nil
}

// Close stops all jobs and orphan timers. In-flight kernel calls are not
// interruptible and finish on their own.
func (s *Service) Close() { s.cancel() }

// RegisterRequest is the owner's claim about a mount it just observed.
type RegisterRequest struct {
	OpID  string
	Type  MountType
	FSID  FSID
	Token string
	// MntOnName/MntFromName are the kernel's f_mntonname/f_mntfromname as
	// observed, never a path rebuilt from configuration: through a symlinked
	// mount point the kernel records the resolved target (H3 L0 probe M3),
	// and Register refuses strings that do not match the live entry.
	MntOnName   string
	MntFromName string
	Generation  uint64
}

// Register records a receipt after proving it against a fresh snapshot: the
// live entry must already satisfy the full predicate for this caller.
func (s *Service) Register(caller Caller, req RegisterRequest) (Receipt, error) {
	if caller.Role != RoleService {
		return Receipt{}, ErrNotAuthorized
	}
	if s.opIDUsed(jobKey{caller.UID, req.OpID}) {
		return Receipt{}, ErrOpIDReused
	}
	// Registration may share an in-flight snapshot: it only proves the mount
	// existed after Observed, and joining avoids a spurious "busy" refusal.
	snap, err := s.snapshotJoin()
	if err != nil {
		return Receipt{}, err
	}
	r := Receipt{
		Version: receiptVersion, UID: caller.UID, Boot: snap.Boot, OpID: req.OpID, Type: req.Type,
		FSID: req.FSID, Token: req.Token, MntOnName: req.MntOnName, MntFromName: req.MntFromName,
		Generation: req.Generation, CreatedAt: s.clock.Now().UTC(),
	}
	if err := r.validate(); err != nil {
		return Receipt{}, err
	}
	if v := Verify(r, caller, snap); v.Kind != VerdictAct {
		return Receipt{}, fmt.Errorf("%w: %s: %s", ErrIdentityRefused, v.Kind, v.Reason)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Re-check under the lock that also publishes job results, so a job that
	// deleted this op's receipt cannot publish a stale result over a new one.
	if s.opIDUsedLocked(jobKey{caller.UID, req.OpID}) {
		return Receipt{}, ErrOpIDReused
	}
	if err := s.store.Put(r); err != nil {
		return Receipt{}, err
	}
	return r, nil
}

// Retire drives the caller's receipt toward RETIRED. A second Retire for the
// same op joins the running job and may escalate it (EjectNow is sticky;
// Liveness is replaced). The job keeps running if ctx ends; Retire then
// returns the current progress with ctx's error.
func (s *Service) Retire(ctx context.Context, caller Caller, opID string, req RetireRequest) (Outcome, error) {
	if caller.Role != RoleService {
		return Outcome{}, ErrNotAuthorized
	}
	// A new owner request supersedes an earlier Cancel.
	s.setWithdrawn(jobKey{caller.UID, opID}, false)
	j, err := s.startOrJoin(caller, opID, req)
	if err != nil {
		return Outcome{}, err
	}
	select {
	case <-j.done:
		return j.snapshot(), nil
	case <-ctx.Done():
		return j.snapshot(), ctx.Err()
	}
}

// Status reports progress for one of the caller's own ops. GUI and service
// roles may call it.
func (s *Service) Status(caller Caller, opID string) (Outcome, error) {
	if caller.Role != RoleService && caller.Role != RoleGUI {
		return Outcome{}, ErrNotAuthorized
	}
	key := jobKey{caller.UID, opID}
	s.mu.Lock()
	j, running := s.jobs[key]
	res, finished := s.results[key]
	s.mu.Unlock()
	if running {
		return j.snapshot(), nil
	}
	if finished {
		return res, nil
	}
	if _, err := s.store.Get(caller.UID, opID); err != nil {
		return Outcome{}, err
	}
	return Outcome{OpID: opID, State: StateRegistered}, nil
}

// opID reuse is forbidden while this daemon remembers the op: a running job
// or a finished result would otherwise be confused with the new receipt.
func (s *Service) opIDUsed(k jobKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opIDUsedLocked(k)
}

func (s *Service) opIDUsedLocked(k jobKey) bool {
	_, running := s.jobs[k]
	_, finished := s.results[k]
	return running || finished
}

func (s *Service) startOrJoin(caller Caller, opID string, req RetireRequest) (*job, error) {
	key := jobKey{caller.UID, opID}
	for {
		s.mu.Lock()
		if j, ok := s.jobs[key]; ok {
			s.mu.Unlock()
			if s.join(j, req) {
				return j, nil
			}
			// The job already stopped converging (e.g. it just returned
			// CANCELLED). Its removal is immediate; then start afresh.
			<-j.done
			continue
		}
		if res, ok := s.results[key]; ok && res.State == StateRetired {
			s.mu.Unlock()
			j := &job{key: key, done: make(chan struct{}), out: res, ended: true}
			close(j.done)
			return j, nil
		}
		s.mu.Unlock()
		// Only the caller's own uid directory is ever consulted.
		r, err := s.store.Get(caller.UID, opID)
		if err != nil {
			return nil, err
		}
		if s.beforeStart != nil {
			s.beforeStart(opID)
		}
		s.mu.Lock()
		if _, ok := s.jobs[key]; ok { // raced with another starter: join it
			s.mu.Unlock()
			continue
		}
		j := &job{key: key, receipt: r, caller: caller, req: req,
			done: make(chan struct{}), wake: make(chan struct{}, 1),
			out: Outcome{OpID: opID, State: StateVerify}}
		s.jobs[key] = j
		s.mu.Unlock()
		go s.run(j)
		return j, nil
	}
}

// join merges req into a running job. It returns false, changing nothing,
// when the job has already stopped converging.
func (s *Service) join(j *job, req RetireRequest) bool {
	j.mu.Lock()
	if j.ended {
		j.mu.Unlock()
		return false
	}
	// Proof is sticky. A joining request that knows less (an orphan sweep
	// or reconnect with LivenessUnknown) must not downgrade a proven
	// Dead/Wedged job: the next pass would issue an ordinary unmount
	// against a dead server and wedge convergence (see converge).
	if req.Liveness.provenGone() || !j.req.Liveness.provenGone() {
		j.req.Liveness = req.Liveness
	}
	// "Eject now" is sticky too: the Quit policy escalates once.
	j.req.EjectNow = j.req.EjectNow || req.EjectNow
	// A new request revives a withdrawn job that has not ended yet. If
	// converge already returned CANCELLED, run sees the flag cleared and
	// converges again, so this request is never answered by the old Cancel.
	j.cancelled = false
	j.mu.Unlock()
	select {
	case j.wake <- struct{}{}:
	default:
	}
	return true
}

// run drives one job to its end. A CANCELLED result is final only if the
// withdrawal still stands when the job ends: a request that joined after
// converge returned (and so cleared the flag) gets a fresh convergence on
// the same job instead of the stale CANCELLED.
func (s *Service) run(j *job) {
	var out Outcome
	for {
		out = s.converge(j)
		if s.afterConverge != nil {
			s.afterConverge(j.key.op, out.State)
		}
		j.mu.Lock()
		revived := out.State == StateCancelled && !j.cancelled && s.base.Err() == nil
		if !revived {
			j.ended = true
		}
		j.mu.Unlock()
		if !revived {
			break
		}
		s.logf("unmountd: op=%s revived by a request that arrived as it was withdrawn", j.key.op)
	}
	j.update(func(o *Outcome) { *o = out })
	if s.beforeRemove != nil {
		s.beforeRemove(j.key.op)
	}
	s.mu.Lock()
	delete(s.jobs, j.key)
	if out.State.terminal() {
		s.results[j.key] = out
		delete(s.parked, j.key)
	}
	s.mu.Unlock()
	close(j.done)
}

type snapCall struct {
	done chan struct{}
	snap Snapshot
	err  error
}

// snapshot returns a snapshot STARTED by this call, for the predicate that
// precedes an unmount syscall. getfsstat can park in the kernel even with
// MNT_NOWAIT (internal/mounttable/mounttable_darwin.go), so at most one runs
// at a time; if one is in flight this fails fast instead of reusing an older
// observation or piling on a second kernel observer.
func (s *Service) snapshot() (Snapshot, error) {
	s.mu.Lock()
	if s.snapCur != nil {
		s.mu.Unlock()
		return Snapshot{}, errors.New("unmountd: previous snapshot still in the kernel")
	}
	c := s.startSnapLocked()
	s.mu.Unlock()
	return s.awaitSnap(c)
}

// snapshotJoin shares an in-flight snapshot if there is one. Only callers
// that do not issue an unmount on the result (Register, the launch sweep)
// may use it.
func (s *Service) snapshotJoin() (Snapshot, error) {
	s.mu.Lock()
	c := s.snapCur
	if c == nil {
		c = s.startSnapLocked()
	}
	s.mu.Unlock()
	return s.awaitSnap(c)
}

func (s *Service) startSnapLocked() *snapCall {
	c := &snapCall{done: make(chan struct{})}
	s.snapCur = c
	go func() {
		c.snap, c.err = s.kernel.Snapshot()
		if c.err == nil && c.snap.Boot == "" {
			c.err = errors.New("unmountd: snapshot without boot id")
		}
		s.mu.Lock()
		s.snapCur = nil
		s.mu.Unlock()
		close(c.done)
	}()
	return c
}

func (s *Service) awaitSnap(c *snapCall) (Snapshot, error) {
	select {
	case <-c.done:
		if c.err != nil {
			return Snapshot{}, c.err
		}
		snap := c.snap
		snap.Entries = append([]Entry(nil), c.snap.Entries...)
		return snap, nil
	case <-s.clock.After(s.pol.SnapshotWatch):
		return Snapshot{}, errors.New("unmountd: snapshot watch expired")
	}
}

// issue starts exactly one unmount syscall for fsid on its own goroutine.
// The caller must hold no in-flight call for fsid (checked under s.mu).
func (s *Service) issue(fsid FSID, flags int) (*kcall, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.inflight[fsid]; c != nil {
		return c, false
	}
	c := &kcall{flags: flags, done: make(chan struct{})}
	s.inflight[fsid] = c
	go func() {
		c.err = s.kernel.UnmountByFSID(fsid, flags)
		s.mu.Lock()
		delete(s.inflight, fsid)
		s.mu.Unlock()
		close(c.done)
	}()
	return c, true
}

// issueFor issues j's unmount with flags, unless j's request escalated to a
// force after the pass chose an ordinary call. The check and the issue happen
// under j.mu, the lock startOrJoin merges under, so a join that proved death
// either lands before the check (no ordinary call) or after the call is
// already registered in the kernel gate (the proof arrived too late for that
// call, and the next pass forces). An ordinary call is never issued after
// the proof is recorded. Lock order: j.mu, then s.mu (issue); nothing takes
// them in the reverse order.
func (s *Service) issueFor(j *job, flags int) (c *kcall, started, escalated bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cancelled || flags&FlagForce == 0 && j.req.wantsForce() {
		return nil, false, true
	}
	c, started = s.issue(j.receipt.FSID, flags)
	return c, started, false
}

func (s *Service) inflightFor(fsid FSID) *kcall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight[fsid]
}

// listHolders names the caller's processes holding fsid. PROC_PIDFDVNODEINFO
// stats each vnode (vn_stat → VNOP_GETATTR) and can block forever on a
// wedged volume, pinning an OS thread per blocked cgo call. So listings are
// single-flight per fsid: while one is stuck, no new one starts and the
// result is marked stale. One stuck thread per fsid is the bound.
func (s *Service) listHolders(caller Caller, fsid FSID) (hs []Holder, stale bool) {
	if s.holders == nil {
		return nil, false
	}
	s.mu.Lock()
	if s.holderIn[fsid] {
		s.mu.Unlock()
		return nil, true
	}
	s.holderIn[fsid] = true
	s.mu.Unlock()
	ch := make(chan []Holder, 1)
	go func() {
		hs, err := s.holders.Holders(caller.UID, fsid)
		if err != nil {
			hs = nil
		}
		s.mu.Lock()
		delete(s.holderIn, fsid)
		s.mu.Unlock()
		ch <- hs
	}()
	select {
	case hs := <-ch:
		return filterHolders(caller.UID, hs), false
	case <-s.clock.After(s.pol.HolderWatch):
		return nil, true
	}
}

func isErrno(err error, e syscall.Errno) bool {
	var got syscall.Errno
	return errors.As(err, &got) && got == e
}

// Cancel withdraws the caller's ordinary retirement of opID. It is accepted
// whenever no force was requested (EjectNow or proven Dead/Wedged) and none
// was issued, in any non-terminal state including ORDINARY and IN_KERNEL:
// the job issues nothing after the withdrawal. issueFor re-checks the flag
// under the same lock a pass issues under, so a pass that already chose an
// ordinary call does not issue it, and the loop top ends the job once an
// in-kernel call returns. That call itself is not interruptible: EBUSY ends
// the job, success means the volume was idle and is gone. A force path is
// left as is and ErrNotCancellable is returned with its progress. The receipt
// is kept, and the op is marked withdrawn so the orphan timer never restarts
// it (Disconnected). With no running job there is nothing to stop; the op is
// still marked withdrawn (unless it already finished) and its status returns.
//
// A job that has already ENDED (it stopped converging and is about to leave
// s.jobs) is not refused: there is nothing left to withdraw, and refusing
// would report "a force was requested or issued" for a job that never had
// one. Cancel waits for its removal and returns its real final outcome, e.g.
// RETIRED (the volume is already gone) with no error.
func (s *Service) Cancel(caller Caller, opID string) (Outcome, error) {
	if caller.Role != RoleService {
		return Outcome{}, ErrNotAuthorized
	}
	key := jobKey{caller.UID, opID}
	s.mu.Lock()
	j, running := s.jobs[key]
	s.mu.Unlock()
	if !running {
		out, err := s.Status(caller, opID)
		if err == nil && !out.State.terminal() {
			s.setWithdrawn(key, true)
		}
		return out, err
	}
	j.mu.Lock()
	if j.ended {
		j.mu.Unlock()
		// Removal follows the end at once (run); bounded all the same, like
		// the running case below.
		select {
		case <-j.done:
		case <-s.base.Done():
		case <-s.clock.After(2 * s.pol.SnapshotWatch):
		}
		out := j.snapshot()
		if !out.State.terminal() {
			s.setWithdrawn(key, true)
		}
		return out, nil
	}
	st := j.out.State
	ok := !j.req.wantsForce() && !j.out.Forced && !st.terminal()
	if ok {
		j.cancelled = true
	}
	j.mu.Unlock()
	if !ok {
		return j.snapshot(), ErrNotCancellable
	}
	s.setWithdrawn(key, true)
	select {
	case j.wake <- struct{}{}:
	default:
	}
	// Between kernel calls the job ends at its next loop top: at most one
	// snapshot (bounded by SnapshotWatch) away. With a call in the kernel it
	// ends when that call returns. Never block the caller longer than this.
	select {
	case <-j.done:
	case <-s.base.Done():
	case <-s.clock.After(2 * s.pol.SnapshotWatch):
	}
	return j.snapshot(), nil
}

// setWithdrawn records (or clears) that the op's last owner request was a
// Cancel, in memory and durably. A store failure is logged: the in-memory
// mark still protects this daemon's lifetime.
func (s *Service) setWithdrawn(k jobKey, on bool) {
	s.mu.Lock()
	if on {
		s.withdrawn[k] = true
	} else {
		delete(s.withdrawn, k)
	}
	s.mu.Unlock()
	var err error
	if on {
		err = s.store.MarkWithdrawn(k.uid, k.op)
	} else {
		// Also clears a mark that survived a daemon restart.
		err = s.store.ClearWithdrawn(k.uid, k.op)
	}
	if err != nil {
		s.logf("unmountd: withdrawn mark uid=%d op=%s on=%v: %v", k.uid, k.op, on, err)
	}
}

// isWithdrawn reports whether the op's last owner request was a Cancel. An
// unreadable durable mark counts as withdrawn: the safe answer for the
// orphan timer is to do nothing.
func (s *Service) isWithdrawn(k jobKey) bool {
	s.mu.Lock()
	w := s.withdrawn[k]
	s.mu.Unlock()
	if w {
		return true
	}
	on, err := s.store.Withdrawn(k.uid, k.op)
	return on || err != nil
}

func (j *job) isCancelled() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.cancelled
}
