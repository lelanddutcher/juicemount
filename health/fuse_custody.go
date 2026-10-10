package health

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/fusecustody"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

type fuseCustodian interface {
	WaitReady(time.Duration) error
	Identity() (string, bool, [2]int32, bool)
	Detach(time.Duration) error
	Release(time.Duration) error
	Detached() bool
	PendingDetach() bool
	ReconcileDetach(time.Duration) error
	NeverAdmittedSettled() bool
	Settled() bool
	PID() int
}

var _ fuseCustodian = (*fusecustody.Session)(nil)

// Caller holds fm.mu. A late admission is bound to the same still-live child
// and frozen exactly once before it can authorize descriptor detach.
func (fm *FUSEManager) finishCustodyAdmission(r *MountReceipt, timeout time.Duration) error {
	if r == nil || r.custody == nil || r.waitDone == nil {
		return errors.New("FUSE custody receipt incomplete")
	}
	if r.custodyUUID == "" && waitFUSEChannel(r.waitDone, 0) {
		return errors.New("original FUSE child exited before custody admission")
	}
	if err := r.custody.WaitReady(timeout); err != nil {
		return fmt.Errorf("FUSE custody admission incomplete: %w", err)
	}
	uuid, readonly, fsid, ready := r.custody.Identity()
	if !ready || readonly || fsid != r.custodyFSID || !fusecustody.CanonicalUUID(uuid) {
		return errors.New("FUSE custody identity invalid")
	}
	if r.custodyUUID == "" {
		if waitFUSEChannel(r.waitDone, 0) {
			return errors.New("original FUSE child exited during custody admission")
		}
		r.custodyUUID = uuid
	} else if r.custodyUUID != uuid {
		return errors.New("FUSE custody UUID changed")
	}
	if !r.custodyRecorded && fm.generation != nil {
		if err := fm.generation.markCustody(uuid, r.custody.PID()); err != nil {
			return fmt.Errorf("persist FUSE custody: %w", err)
		}
	}
	r.custodyRecorded = true
	return nil
}

// FUSEStopState describes what the manager can prove, not whether a pathname
// unmount command happened to return. Only FUSEStopped permits dependency close.
type FUSEStopState string

const (
	FUSEStopped  FUSEStopState = "stopped"
	FUSEInFlight FUSEStopState = "in_flight"
	FUSEUnknown  FUSEStopState = "unknown"
	FUSEPresent  FUSEStopState = "present"
	// FUSEAdmissionPending: the mount is present and the custodian's root
	// open has not returned within this Stop's bound. Deliberately neutral:
	// an unanswered Network Volumes prompt and an unresponsive JuiceFS daemon
	// both park that open in the kernel, and the custodian has no signal that
	// tells them apart. A late admission is picked up by the next Stop.
	FUSEAdmissionPending FUSEStopState = "admission_pending"
	// FUSEPermissionDenied: the custodian's root open was refused with
	// EPERM/EACCES (macOS Network Volumes access not granted). Without that
	// descriptor the mount cannot be detached by identity, so it is retained.
	FUSEPermissionDenied FUSEStopState = "permission_denied"
)

// startFUSECustodian launches one fresh custodian for an exact live child.
// Package seam so tests never spawn the real helper.
var startFUSECustodian = func(root, source string, child, parent int) (fuseCustodian, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	keeper := filepath.Join(filepath.Dir(executable), "juicemount-mount-probe")
	session, err := fusecustody.Start(keeper, root, source, child, parent)
	if err != nil {
		return nil, err // never a typed-nil interface
	}
	return session, nil
}

// renewDeniedCustodyLocked replaces a custodian that macOS denied with one
// fresh attempt, so a permission granted later in System Settings lets Stop
// finish. Caller holds fm.mu. It acts only when every identity is exact:
// the old custodian definitively refused, exited and was reaped through its
// own exec handle (NeverAdmittedSettled), and the original JuiceFS child is still the live process we started. It never
// signals anything and never touches the mount by path; the new custodian
// re-verifies FSID, source and .config Pid/PPid itself before admitting.
func (fm *FUSEManager) renewDeniedCustodyLocked(r *MountReceipt) error {
	if r == nil || r.custody == nil || r.process == nil || r.waitDone == nil {
		return errors.New("FUSE custody renewal lacks exact handles")
	}
	if !r.custody.NeverAdmittedSettled() {
		return errors.New("denied FUSE custodian has not been joined")
	}
	if waitFUSEChannel(r.waitDone, 0) {
		return errors.New("original FUSE child exited; renewal refused")
	}
	fresh, err := startFUSECustodian(fm.cfg.MountPoint, r.custodySource, r.process.Pid, os.Getpid())
	if err != nil {
		return fmt.Errorf("fresh FUSE custodian start: %w", err)
	}
	r.custody = fresh
	r.custodyStartedAt = fuseNow()
	return nil
}

// custodyAdmissionStopResult maps an admission failure to its typed state.
func custodyAdmissionStopResult(err error) FUSEStopResult {
	switch {
	case errors.Is(err, fusecustody.ErrAdmissionPermissionDenied):
		return FUSEStopResult{FUSEPermissionDenied, "macOS Network Volumes access was not granted to the FUSE custodian: " + err.Error()}
	case errors.Is(err, fusecustody.ErrAdmissionPending):
		return FUSEStopResult{FUSEAdmissionPending, "waiting for macOS to open the volume; this is either a pending Network Volumes prompt or an unresponsive filesystem: " + err.Error()}
	default:
		return FUSEStopResult{FUSEInFlight, err.Error()}
	}
}

type FUSEStopResult struct {
	State  FUSEStopState
	Detail string
}

func (r FUSEStopResult) Complete() bool { return r.State == FUSEStopped }
func (r FUSEStopResult) String() string {
	if r.Detail == "" {
		return string(r.State)
	}
	return string(r.State) + ": " + r.Detail
}

// rollbackReceipt first proves that this exact receipt still owns the active
// child. Otherwise a delayed rollback from a prior mount could stop its
// successor. Admission closes while fm.mu excludes replacement.
func (fm *FUSEManager) rollbackReceipt(r *MountReceipt) error {
	if !fm.tryLockWithin(2 * time.Second) {
		return errors.New("FUSE receipt rollback incomplete: lifecycle mutex held")
	}
	if fm.activeReceipt != r {
		fm.mu.Unlock()
		if r.custody != nil && !r.custody.Settled() && !r.custody.NeverAdmittedSettled() {
			return errors.New("stale FUSE receipt custodian is not settled")
		}
		if r.process == nil && r.waitDone == nil {
			return nil
		}
		if !waitFUSEChannel(r.waitDone, 0) {
			return errors.New("stale FUSE receipt child is not settled")
		}
		r.stopTail()
		if r.tailDone != nil && !waitFUSEChannel(r.tailDone, 2*time.Second) {
			return errors.New("stale FUSE receipt log helper is not settled")
		}
		if r.observerDone != nil && !waitFUSEChannel(r.observerDone, 2*time.Second) {
			return errors.New("stale FUSE receipt observer is not settled")
		}
		return nil
	}
	fm.stopping.Store(true)
	fm.stopOnce.Do(func() { close(fm.stopCh) })
	fm.mu.Unlock()
	result := fm.Stop()
	if !result.Complete() {
		return fmt.Errorf("FUSE rollback incomplete: %s", result)
	}
	return nil
}

// CanReuse is deliberately a nonblocking, local check. A matching path and
// endpoint alone cannot adopt a FUSE mount; the exact live child receipt must
// still belong to this manager.
func (fm *FUSEManager) CanReuse(mountPoint, redisURL string) bool {
	if fm == nil || fm.stopping.Load() || fm.cfg.MountPoint != mountPoint || fm.cfg.RedisURL != redisURL || !fm.mu.TryLock() {
		return false
	}
	defer fm.mu.Unlock()
	r := fm.activeReceipt
	if r == nil || r.process == nil || r.waitDone == nil {
		return false
	}
	select {
	case <-r.waitDone:
		return false
	default:
		return true
	}
}

// QuiesceRemountCallbacks closes callback admission independently of fm.mu, so
// a mount operation holding that mutex cannot block the caller. An already
// running callback must finish before its captured stores may be closed.
func (fm *FUSEManager) QuiesceRemountCallbacks(timeout time.Duration) bool {
	if fm == nil {
		return true
	}
	fm.callbackMu.Lock()
	fm.callbacksClosed = true
	fm.onRemount = nil
	active, idle := fm.callbacksActive, fm.callbacksIdle
	fm.callbackMu.Unlock()
	if active == 0 {
		return true
	}
	return waitFUSEChannel(idle, timeout)
}

func waitFUSEChannel(done <-chan struct{}, timeout time.Duration) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
	}
	if timeout <= 0 {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (fm *FUSEManager) observeFUSE(ctx context.Context) (mounttable.Snapshot, error) {
	if fm.observeMount != nil {
		return fm.observeMount(ctx)
	}
	executable, err := os.Executable()
	if err != nil {
		return mounttable.Snapshot{}, err
	}
	helper := filepath.Join(filepath.Dir(executable), "juicemount-mount-probe")
	return mounttable.SnapshotFromHelper(ctx, helper)
}

func (fm *FUSEManager) snapshotFUSE() (mounttable.Snapshot, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := fm.observeFUSE(ctx)
	if err != nil {
		return s, false, err
	}
	if s.Version != 1 || strings.TrimSpace(s.BootID) == "" || s.Entries == nil {
		return s, false, errors.New("incomplete FUSE mount snapshot")
	}
	path := fm.cfg.MountPoint
	if path == "" || path == "/" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return s, false, errors.New("invalid FUSE mount path")
	}
	folded := strings.ToLower(path)
	present := false
	for _, e := range s.Entries {
		if e.Path == "" || !filepath.IsAbs(e.Path) || filepath.Clean(e.Path) != e.Path || e.Source == "" || e.Type == "" {
			return s, false, errors.New("incomplete FUSE mount entry")
		}
		entry := strings.ToLower(e.Path)
		if entry == folded || strings.HasPrefix(entry, folded+string(os.PathSeparator)) {
			present = true
		}
	}
	return s, present, nil
}

// prepareMountLocked refuses new creation while an old child or any kernel
// entry can still exist. Caller holds fm.mu throughout creation publication.
func (fm *FUSEManager) prepareMountLocked() error {
	if fm.stopping.Load() {
		return errors.New("FUSE manager is stopping")
	}
	fm.helperMu.Lock()
	helperActive := fm.helperActive
	fm.helperMu.Unlock()
	if helperActive != 0 {
		return errors.New("prior FUSE path helper has not settled")
	}
	_, present, err := fm.snapshotFUSE()
	if err != nil {
		return fmt.Errorf("FUSE creation observation unknown: %w", err)
	}
	if present {
		return errors.New("FUSE path or descendant already occupied; creation refused")
	}
	if r := fm.activeReceipt; r != nil {
		if r.custody != nil && !r.custody.Settled() && !r.custody.NeverAdmittedSettled() {
			return errors.New("prior FUSE custodian has not settled")
		}
		if !waitFUSEChannel(r.waitDone, 0) {
			return errors.New("prior exact JuiceFS child has not exited")
		}
		r.stopTail()
		if r.tailDone != nil && !waitFUSEChannel(r.tailDone, 2*time.Second) {
			return errors.New("prior JuiceFS log helper has not settled")
		}
		if r.observerDone != nil && !waitFUSEChannel(r.observerDone, 2*time.Second) {
			return errors.New("prior JuiceFS exit observer has not settled")
		}
		fm.activeReceipt = nil
	}
	if fm.capabilityProbe != nil && !fm.capabilityProbe.Settled() {
		return errors.New("prior FUSE capability probe still active")
	}
	if fm.stopping.Load() {
		return errors.New("FUSE manager is stopping")
	}
	return nil
}

func (r *MountReceipt) stopTail() {
	if r != nil && r.tailStop != nil {
		r.tailStopOnce.Do(func() { close(r.tailStop) })
	}
}

func (fm *FUSEManager) scheduleSpotlightIndexing() {
	fm.helperMu.Lock()
	if fm.helperClosed || fm.stopping.Load() {
		fm.helperMu.Unlock()
		return
	}
	if fm.helperActive == 0 {
		fm.helperIdle = make(chan struct{})
	}
	fm.helperActive++
	fm.helperMu.Unlock()
	// Scheduling is asynchronous and follows stable data-plane verification.
	write := fm.spotlightWrite
	if write == nil {
		write = os.WriteFile
	}
	done := suppressSpotlightIndexing(fm.cfg.MountPoint, write)
	go func() {
		<-done
		fm.helperMu.Lock()
		fm.helperActive--
		if fm.helperActive == 0 {
			close(fm.helperIdle)
		}
		fm.helperMu.Unlock()
	}()
}

func (fm *FUSEManager) quiesceSpotlight(timeout time.Duration) bool {
	fm.helperMu.Lock()
	fm.helperClosed = true
	active, idle := fm.helperActive, fm.helperIdle
	fm.helperMu.Unlock()
	if active == 0 {
		return true
	}
	return waitFUSEChannel(idle, timeout)
}

// earlyHelperStopReady is the exceptional failed-start route when the original
// custodian could not be launched. A helper receipt is useful only while this
// manager still holds its exact child, generation and live registration lease.
// The caller also verifies the fresh kernel boot/source/FSID before Retire.
func (fm *FUSEManager) earlyHelperStopReady(r *MountReceipt) bool {
	if r == nil || r.owner != fm || r != fm.activeReceipt || r.generation == nil ||
		r.generation != fm.generation || r.process == nil || r.process.Pid <= 1 || r.waitDone == nil ||
		r.custodyBoot == "" || r.custodySource == "" || r.custodyFSID == [2]int32{} ||
		r.custodyBoot != r.readinessBoot || r.custodySource != r.readinessSource ||
		r.custodyFSID != r.readinessFSID || r.helper == nil {
		return false
	}
	h := r.helper
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state != FUSEHelperRegistered || h.retired || h.lease == nil ||
		h.op == "" || h.op != FUSEHelperOpID(fm.cfg.MountPoint) || h.lease.OpID() != h.op || h.lease.Lost() == nil {
		return false
	}
	select {
	case <-h.lease.Lost():
		return false
	default:
		return true
	}
}

// stopWithBounds is the deterministic test seam. The production Stop budgets
// monitor/callback/helper settlement and lifecycle lock acquisition; no timeout
// invokes a destructive fallback.
func (fm *FUSEManager) stopWithBounds(joinWait, lockWait time.Duration) FUSEStopResult {
	if fm == nil {
		return FUSEStopResult{State: FUSEStopped}
	}
	fm.stopping.Store(true)
	fm.stopOnce.Do(func() { close(fm.stopCh) })
	fm.monitorState.CompareAndSwap(0, 2)
	if !fm.QuiesceRemountCallbacks(joinWait) {
		return FUSEStopResult{FUSEInFlight, "remount callback still active"}
	}
	if fm.monitorState.Load() == 1 && !waitFUSEChannel(fm.done, joinWait) {
		return FUSEStopResult{FUSEInFlight, "monitor or mount creation still active"}
	}
	if !fm.quiesceSpotlight(joinWait) {
		return FUSEStopResult{FUSEInFlight, "FUSE path helper still active"}
	}
	if !fm.tryLockWithin(lockWait) {
		return FUSEStopResult{FUSEInFlight, "FUSE lifecycle mutex held"}
	}
	defer fm.mu.Unlock()
	if fm.capabilityProbe != nil && !fm.capabilityProbe.Settled() {
		return FUSEStopResult{FUSEInFlight, "FUSE capability probe still active"}
	}
	before, present, err := fm.snapshotFUSE()
	if err != nil {
		return FUSEStopResult{FUSEUnknown, err.Error()}
	}
	r := fm.activeReceipt
	if r != nil && r.custodyBoot != "" && (before.BootID != r.custodyBoot || (fm.generation != nil && before.BootID != fm.generation.bootID)) {
		return FUSEStopResult{FUSEUnknown, "FUSE custody boot generation changed"}
	}
	if present {
		if r == nil || (r.custody == nil && !fm.earlyHelperStopReady(r)) {
			return FUSEStopResult{FUSEPresent, "FUSE path occupied without exact custodian"}
		}
		if before.BootID != r.custodyBoot || (fm.generation != nil && before.BootID != fm.generation.bootID) {
			return FUSEStopResult{FUSEUnknown, "FUSE custody boot generation changed"}
		}
		matches := 0
		for _, entry := range before.Entries {
			if strings.EqualFold(entry.Path, fm.cfg.MountPoint) {
				if entry.Path != fm.cfg.MountPoint || entry.Type != "macfuse" || entry.Source != r.custodySource || entry.FSID != r.custodyFSID {
					return FUSEStopResult{FUSEPresent, "FUSE root differs from retained custodian"}
				}
				matches++
			} else if strings.HasPrefix(strings.ToLower(entry.Path), strings.ToLower(fm.cfg.MountPoint)+string(os.PathSeparator)) {
				return FUSEStopResult{FUSEPresent, "FUSE mounted descendant prevents retirement"}
			}
		}
		if matches != 1 {
			return FUSEStopResult{FUSEPresent, "FUSE root observation ambiguous"}
		}
		if r.helper.proven() {
			// The helper retired our fsid; a present root is not ours.
			return FUSEStopResult{FUSEPresent, "FUSE path occupied after helper retirement"}
		}
		if r.custody == nil {
			if err := fm.retireViaHelper(r, client.RetireRequest{Liveness: childLiveness(r)}, joinWait); err != nil {
				return FUSEStopResult{FUSEInFlight, "early FUSE helper retirement incomplete: " + err.Error()}
			}
		} else if err := fm.finishCustodyAdmission(r, joinWait); err != nil {
			if errors.Is(err, fusecustody.ErrAdmissionPermissionDenied) {
				// One fresh exact-handle attempt per Stop: if the user has
				// since granted access, this Stop can finish now. After a
				// renewal admission can take ~2x joinWait (~20 s), still
				// inside the 90 s owner stop RPC.
				if renewErr := fm.renewDeniedCustodyLocked(r); renewErr == nil {
					err = fm.finishCustodyAdmission(r, joinWait)
				}
			}
			if err != nil {
				result := custodyAdmissionStopResult(err)
				// H-10 handoff (design §4): a denied open, or a lost/invalid
				// custodian, goes to the helper at once; a pending open only
				// after FUSEPendingHelperHandoff, so a user answering the
				// prompt can still finish through the descriptor. Without a
				// receipt or a RETIRED proof the typed state stays.
				if result.State == FUSEAdmissionPending && !pendingBeyondHandoff(r) {
					return result
				}
				if herr := fm.retireViaHelper(r, client.RetireRequest{Liveness: childLiveness(r)}, joinWait); herr != nil {
					result.Detail += "; unmount helper: " + herr.Error()
					return result
				}
			}
		}
		if r.custody != nil && !r.helper.proven() {
			if err := r.custody.Detach(joinWait); err != nil {
				// Descriptor detach retained (busy or lost): retire by fsid.
				if herr := fm.retireViaHelper(r, client.RetireRequest{Liveness: childLiveness(r)}, joinWait); herr != nil {
					return FUSEStopResult{FUSEInFlight, "retained FUSE detach pending: " + err.Error() + "; unmount helper: " + herr.Error()}
				}
			}
		}
		var stillPresent bool
		beforeDetach := before
		before, stillPresent, err = fm.snapshotFUSE()
		if err != nil || before.BootID != beforeDetach.BootID {
			return FUSEStopResult{FUSEUnknown, "post-detach mount observation unavailable or boot changed"}
		}
		if stillPresent {
			return FUSEStopResult{FUSEPresent, "FUSE path or descendant remains occupied after detach"}
		}
		r.helper.markAbsent()
	} else if r != nil && r.helper.proven() {
		r.helper.markAbsent()
	} else if r != nil && r.custody != nil && !r.custody.Detached() {
		if r.custody.PendingDetach() {
			if err := r.custody.ReconcileDetach(joinWait); err != nil {
				return FUSEStopResult{FUSEInFlight, "pending FD detach result: " + err.Error()}
			}
		} else {
			_ = r.custody.WaitReady(joinWait)
			if !r.custody.NeverAdmittedSettled() {
				return FUSEStopResult{FUSEUnknown, "mount absent without proven retained-FD detach"}
			}
		}
	}
	if r != nil {
		if r.waitDone == nil {
			return FUSEStopResult{FUSEUnknown, "exact child waiter unavailable"}
		}
		if r.helper.proven() {
			if err := awaitChildAfterRetire(r, joinWait); err != nil {
				return FUSEStopResult{FUSEInFlight, err.Error()}
			}
		} else if !waitFUSEChannel(r.waitDone, joinWait) {
			// JuiceFS handles TERM by force-unmounting its configured pathname.
			// Even an exact child PID cannot authorize that indirect effect after
			// the observed mount has been replaced. Retain it until independently
			// exited and reaped; Stop itself has no child signal authority.
			return FUSEStopResult{FUSEInFlight, "exact JuiceFS child remains live; identity-safe stop unavailable"}
		}
		r.stopTail()
		if r.tailDone != nil && !waitFUSEChannel(r.tailDone, joinWait) {
			return FUSEStopResult{FUSEInFlight, "JuiceFS log helper still active"}
		}
		if r.observerDone != nil && !waitFUSEChannel(r.observerDone, joinWait) {
			return FUSEStopResult{FUSEInFlight, "JuiceFS exit observer still active"}
		}
	}
	after, present, err := fm.snapshotFUSE()
	if err != nil || after.BootID != before.BootID {
		return FUSEStopResult{FUSEUnknown, "post-retirement mount observation unavailable or boot changed"}
	}
	if present {
		return FUSEStopResult{FUSEPresent, "FUSE path or descendant reappeared"}
	}
	if fm.generation != nil {
		if err := fm.generation.retire(); err != nil {
			return FUSEStopResult{FUSEUnknown, "generation retirement not durable: " + err.Error()}
		}
	}
	if r != nil && r.custody != nil && !r.custody.NeverAdmittedSettled() {
		if r.helper.proven() && !r.custody.Detached() {
			if err := settleCustodianAfterRetire(r.custody, joinWait); err != nil {
				return FUSEStopResult{FUSEInFlight, err.Error()}
			}
		} else if err := r.custody.Release(joinWait); err != nil {
			return FUSEStopResult{FUSEInFlight, "FUSE custodian release or join pending: " + err.Error()}
		}
	}
	fm.finishHelperReceipt(r)
	fm.activeReceipt = nil
	return FUSEStopResult{State: FUSEStopped}
}

// UnmountLease is a held helper receipt binding. It aliases an unnamed
// interface so one adapter satisfies the identical NFS and FUSE seams.
type UnmountLease = interface {
	OpID() string
	Lost() <-chan struct{}
	Err() error
	Close()
}

// UnmountHelper is the volume owner's seam to the privileged unmount helper
// (juicemount-unmountd, H-3 L5). The helper targets a mount by fsid plus the
// receipt registered here, never by path. Production wraps *client.Client.
type UnmountHelper interface {
	Register(ctx context.Context, rr client.RegisterRequest) (UnmountLease, error)
	Reclaim(ctx context.Context, opID string) (UnmountLease, client.ReclaimResult, error)
	Retire(ctx context.Context, opID string, rr client.RetireRequest, wait time.Duration) (client.Outcome, error)
	Cancel(ctx context.Context, opID string) (client.Outcome, error)
}

var defaultHelper atomic.Pointer[unmountHelperBox]

type unmountHelperBox struct{ h UnmountHelper }

// SetDefaultUnmountHelper installs the process helper picked up by every
// later generation manager and by prior-generation recovery. nil removes it.
func SetDefaultUnmountHelper(h UnmountHelper) {
	if h == nil {
		defaultHelper.Store(nil)
		return
	}
	defaultHelper.Store(&unmountHelperBox{h})
}

func defaultUnmountHelper() UnmountHelper {
	if b := defaultHelper.Load(); b != nil {
		return b.h
	}
	return nil
}

// SetUnmountHelper wires the helper for this manager before Mount.
func (fm *FUSEManager) SetUnmountHelper(h UnmountHelper) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.unmountd = h
}

var (
	// FUSEPendingHelperHandoff is how long a custodian root open may stay
	// pending (an unanswered Network Volumes prompt or a wedged daemon)
	// before Stop hands the mount to the helper (founder Q5: 60 s).
	FUSEPendingHelperHandoff = 60 * time.Second
	// FUSEChildKillAfterRetire is how long the exact JuiceFS child may stay
	// alive after the helper proved its mount gone. Then, and only then, the
	// parent sends SIGKILL to that unreaped child. JuiceFS never gets
	// SIGTERM from us: it answers TERM with a pathname force-unmount.
	FUSEChildKillAfterRetire  = 30 * time.Second
	fuseHelperRegisterTimeout = 5 * time.Second
	fuseNow                   = time.Now
)

// Helper receipt states (UnmountHelperStatus).
const (
	FUSEHelperUnavailable    = "unavailable"
	FUSEHelperRegistered     = "registered"
	FUSEHelperRegisterFailed = "register_failed"
	FUSEHelperLost           = "lease_lost"
	FUSEHelperRetiring       = "retiring"
	FUSEHelperRetired        = "retired"
)

type fuseHelperReceipt struct {
	mu       sync.Mutex
	op       string
	lease    UnmountLease
	state    string
	err      string
	retired  bool      // the helper reported RETIRED for op
	absentAt time.Time // first fresh absence observed after that proof
}

func (h *fuseHelperReceipt) set(state, detail string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state, h.err = state, detail
}

func (h *fuseHelperReceipt) proven() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.retired
}

// UnmountHelperStatus reports the newest child's receipt for /health. It
// never takes fm.mu.
func (fm *FUSEManager) UnmountHelperStatus() (op, state, detail string) {
	if fm == nil {
		return "", "", ""
	}
	h := fm.helperView.Load()
	if h == nil {
		return "", "", ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	state, detail = h.state, h.err
	if state == FUSEHelperRegistered && h.lease != nil {
		select {
		case <-h.lease.Lost():
			state = FUSEHelperLost
			if err := h.lease.Err(); err != nil {
				detail = err.Error()
			}
		default:
		}
	}
	return h.op, state, detail
}

// registerWithHelperLocked records the helper receipt for a just-observed
// generation mount and holds the lease. Caller holds fm.mu. A helper
// failure never fails the mount (today's behaviour; the GUI gates Start).
func (fm *FUSEManager) registerWithHelperLocked(r *MountReceipt, onName, source string, fsid [2]int32) {
	nonce, gen, ok := parseGenerationMountPath(fm.cfg.MountPoint)
	h := &fuseHelperReceipt{op: FUSEHelperOpID(fm.cfg.MountPoint)}
	r.helper = h
	fm.helperView.Store(h)
	switch {
	case fm.unmountd == nil:
		h.set(FUSEHelperUnavailable, "")
		return
	case !ok || fm.generation == nil:
		h.set(FUSEHelperRegisterFailed, "mount path is not a generation path")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), fuseHelperRegisterTimeout)
	lease, err := fm.unmountd.Register(ctx, client.RegisterRequest{OpID: h.op, RegisterArgs: client.RegisterArgs{
		Type: "macfuse", FSID: fsid, Token: nonce, MntOnName: onName, MntFromName: source, Generation: gen}})
	cancel()
	if err == nil && lease == nil {
		err = errors.New("helper returned no lease")
	}
	if err != nil {
		log.Printf("[fuse] unmount helper register %s failed; descriptor custody only: %v", h.op, err)
		h.set(FUSEHelperRegisterFailed, err.Error())
		return
	}
	h.mu.Lock()
	h.lease, h.state = lease, FUSEHelperRegistered
	h.mu.Unlock()
	log.Printf("[fuse] unmount helper receipt %s registered (fsid %v)", h.op, fsid)
}

// childLiveness is what the parent has PROVEN about the exact JuiceFS
// child: Dead once its own Wait returned, otherwise Alive.
func childLiveness(r *MountReceipt) string {
	if r != nil && waitFUSEChannel(r.waitDone, 0) {
		return client.LivenessDead
	}
	return client.LivenessAlive
}

// retireViaHelper asks the helper to retire this receipt's fsid. It never
// takes a path. Only a RETIRED outcome for exactly this op becomes proof.
func (fm *FUSEManager) retireViaHelper(r *MountReceipt, rr client.RetireRequest, wait time.Duration) error {
	if r == nil || r.helper == nil {
		return errors.New("FUSE mount has no helper receipt")
	}
	h := r.helper
	h.mu.Lock()
	op, lease, retired := h.op, h.lease, h.retired
	h.mu.Unlock()
	if retired {
		return nil
	}
	helper := fm.unmountd
	if helper == nil {
		return errors.New("unmount helper unavailable")
	}
	if lease == nil || op == "" {
		return errors.New("FUSE mount has no registered helper receipt")
	}
	h.set(FUSEHelperRetiring, "")
	ctx, cancel := context.WithTimeout(context.Background(), wait+5*time.Second)
	out, err := helper.Retire(ctx, op, rr, wait)
	cancel()
	if err == nil && (out.OpID != op || out.State != "RETIRED") {
		err = fmt.Errorf("helper ended %s in state %s", op, out.State)
	}
	if err != nil {
		h.set(FUSEHelperRegistered, err.Error())
		if !rr.EjectNow && rr.Liveness != client.LivenessDead && rr.Liveness != client.LivenessWedged {
			// Stop gives up on this ordinary retirement; withdraw it so an
			// orphaned EBUSY-backoff job cannot eject a busy, healthy volume
			// later. Refused only once a force was requested/issued; that is
			// only logged.
			cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
			if cout, cerr := helper.Cancel(cctx, op); cerr != nil {
				log.Printf("[fuse] unmount helper cancel %s refused or failed (state %s): %v", op, cout.State, cerr)
			}
			ccancel()
		}
		return err
	}
	h.mu.Lock()
	h.retired, h.state, h.err = true, FUSEHelperRetired, ""
	h.mu.Unlock()
	log.Printf("[fuse] unmount helper retired %s (liveness=%s eject_now=%v forced=%v)", op, rr.Liveness, rr.EjectNow, out.Forced)
	return nil
}

// RetireDeadChildMount is the watchdog's route when the exact JuiceFS child
// is proven dead (its Wait returned) while its mount is still present: the
// helper forces the stale fsid. It holds fm.mu only to read the receipt.
func (fm *FUSEManager) RetireDeadChildMount(wait time.Duration) error {
	if !fm.tryLockWithin(2 * time.Second) {
		return errors.New("FUSE lifecycle mutex held")
	}
	r := fm.activeReceipt
	fm.mu.Unlock()
	if r == nil || r.waitDone == nil || !waitFUSEChannel(r.waitDone, 0) {
		return errors.New("exact JuiceFS child not proven dead")
	}
	if r.helper.proven() {
		return nil
	}
	_, present, err := fm.snapshotFUSE()
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	return fm.retireViaHelper(r, client.RetireRequest{Liveness: client.LivenessDead}, wait)
}

// pendingBeyondHandoff reports a custodian root open pending past Q5's bound.
func pendingBeyondHandoff(r *MountReceipt) bool {
	return r != nil && !r.custodyStartedAt.IsZero() && fuseNow().Sub(r.custodyStartedAt) >= FUSEPendingHelperHandoff
}

// awaitChildAfterRetire joins the exact JuiceFS child after the helper
// proved its mount gone. The kernel closed the macFUSE channel, so JuiceFS
// normally exits by itself. Only if it is still alive FUSEChildKillAfterRetire
// after the proven absence does the parent SIGKILL its own unreaped child.
func awaitChildAfterRetire(r *MountReceipt, joinWait time.Duration) error {
	if waitFUSEChannel(r.waitDone, joinWait) {
		return nil
	}
	r.helper.mu.Lock()
	absentAt := r.helper.absentAt
	r.helper.mu.Unlock()
	if absentAt.IsZero() || fuseNow().Sub(absentAt) < FUSEChildKillAfterRetire {
		return errors.New("waiting for the exact JuiceFS child to exit after helper retirement")
	}
	if r.process == nil {
		return errors.New("exact JuiceFS child handle unavailable")
	}
	// SIGKILL runs no handler, so JuiceFS cannot turn it into a pathname
	// unmount. Kill goes through the exact os.Process from cmd.Start: once
	// its Wait has returned, Kill reports ErrProcessDone and signals nothing.
	// A narrow window remains on darwin (no pidfd): the kernel can reap the
	// child inside Wait just before os.Process records it as done, and a
	// PID reused inside that window would receive the signal. That needs a
	// full PID-space wrap within microseconds; improbable, not impossible.
	if err := r.process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("SIGKILL exact JuiceFS child: %w", err)
	}
	log.Printf("[fuse] exact JuiceFS child %d still alive %v after helper retirement; sent SIGKILL", r.process.Pid, FUSEChildKillAfterRetire)
	if !waitFUSEChannel(r.waitDone, joinWait) {
		return errors.New("exact JuiceFS child not joined after SIGKILL")
	}
	return nil
}

// custodianAbandoner is implemented by a custodian that can be ended once
// the helper retired its mount (a descriptor on a detached mount can never
// report a descriptor detach).
type custodianAbandoner interface{ Abandon(time.Duration) error }

// settleCustodianAfterRetire joins the custodian after a helper retirement.
// A denied or pending custodian exits once its open fails; an admitted one
// holds a dead descriptor and must be abandoned through its exec handle.
func settleCustodianAfterRetire(c fuseCustodian, joinWait time.Duration) error {
	if c == nil {
		return nil
	}
	deadline := time.Now().Add(joinWait)
	for {
		if c.Settled() || c.NeverAdmittedSettled() {
			return nil
		}
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if a, ok := c.(custodianAbandoner); ok {
		return a.Abandon(joinWait)
	}
	return errors.New("FUSE custodian retained after helper retirement (fusecustody has no Abandon)")
}

// finishHelperReceipt releases the lease after a completed Stop. When a
// descriptor detach (not the helper) removed the mount, the helper is asked
// to retire the now-absent receipt so no orphan sweep is left pending.
func (fm *FUSEManager) finishHelperReceipt(r *MountReceipt) {
	if r == nil || r.helper == nil {
		return
	}
	h := r.helper
	h.mu.Lock()
	op, lease, retired := h.op, h.lease, h.retired
	h.lease = nil
	h.mu.Unlock()
	if lease == nil {
		return
	}
	if !retired && fm.unmountd != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		out, err := fm.unmountd.Retire(ctx, op, client.RetireRequest{Liveness: childLiveness(r)}, 2*time.Second)
		cancel()
		if err != nil || out.State != "RETIRED" {
			log.Printf("[fuse] unmount helper receipt %s left for its orphan sweep: state=%s err=%v", op, out.State, err)
		} else {
			h.set(FUSEHelperRetired, "")
		}
	}
	lease.Close()
}

// markAbsent records the first fresh absence after a RETIRED proof; the
// SIGKILL grace for the exact child is measured from here.
func (h *fuseHelperReceipt) markAbsent() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.retired && h.absentAt.IsZero() {
		h.absentAt = fuseNow()
	}
}

// ExactChildLiveness is the exact JuiceFS child's liveness from its own
// Wait: proven=false when this manager holds no child receipt (or its lock
// is busy), in which case nothing may be inferred.
func (fm *FUSEManager) ExactChildLiveness() (alive, proven bool) {
	if fm == nil || !fm.mu.TryLock() {
		return false, false
	}
	r := fm.activeReceipt
	fm.mu.Unlock()
	if r == nil || r.waitDone == nil {
		return false, false
	}
	return !waitFUSEChannel(r.waitDone, 0), true
}
