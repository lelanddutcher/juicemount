package nfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

var ErrBoundFDPhase = errors.New("bound predecessor FD phase conflict")

type PendingBoundFDPhase struct{ Cause error }

func (e *PendingBoundFDPhase) Error() string {
	return fmt.Sprintf("bound predecessor FD remains owned: %v", e.Cause)
}
func (e *PendingBoundFDPhase) Unwrap() error { return e.Cause }

// A registry excludes every birth operation at the same path. It owns no
// legacy/live drain until a later activation explicitly registers one.
type boundFDPhaseRegistry struct {
	mu       sync.Mutex
	byPath   map[string]*boundFDPhase
	stopping bool
	empty    chan struct{}
}

func newBoundFDPhaseRegistry() *boundFDPhaseRegistry {
	c := make(chan struct{})
	close(c)
	return &boundFDPhaseRegistry{byPath: make(map[string]*boundFDPhase), empty: c}
}

func (r *boundFDPhaseRegistry) stopAccepting() {
	r.mu.Lock()
	r.stopping = true
	r.mu.Unlock()
}
func (r *boundFDPhaseRegistry) waitAll() { r.mu.Lock(); c := r.empty; r.mu.Unlock(); <-c }
func (r *boundFDPhaseRegistry) isEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byPath) == 0
}

type boundFDWorker struct {
	done chan struct{}
	err  error
	kind string
}

// boundFDPhase owns the pinned private source and original destination FD.
// Its lock precedes Store admission locks; no Store lock spans FD I/O/wait.
type boundFDPhase struct {
	mu                       sync.Mutex
	registry                 *boundFDPhaseRegistry
	store                    *metadata.Store
	obs                      metadata.BoundModeObservation
	src, dst                 *os.File
	destPath, destIdentity   string
	mountDev                 uint64
	worker                   *boundFDWorker
	settled, batch, released bool
	validated                *metadata.BoundDrainCommitResult
	effectsClaimed           bool
	mtime                    time.Time
}

func fdPhaseIdentity(f *os.File) (string, uint64, os.FileInfo, error) {
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		return "", 0, nil, ErrBoundFDPhase
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", 0, nil, ErrBoundFDPhase
	}
	return fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino), uint64(st.Dev), fi, nil
}
func pathPhaseIdentity(name string) (string, error) {
	fi, err := os.Lstat(name)
	if err != nil || !fi.Mode().IsRegular() {
		return "", ErrBoundFDPhase
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", ErrBoundFDPhase
	}
	return fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino), nil
}

// begin pins a birth-identity-qualified source and predecessor destination
// before any attribute phase. P40 later proves the full source image and path;
// this lease alone does not. mountDev is the caller's verified mount device.
func (r *boundFDPhaseRegistry) begin(store *metadata.Store, obs metadata.BoundModeObservation, src, dst *os.File, destPath string, mountDev uint64) (*boundFDPhase, error) {
	if store == nil || src == nil || dst == nil || destPath == "" || obs.SpoolID <= 0 {
		return nil, ErrBoundFDPhase
	}
	srcID, _, _, err := fdPhaseIdentity(src)
	if err != nil || srcID != obs.FileIdentity {
		return nil, ErrBoundFDPhase
	}
	dstID, dev, _, err := fdPhaseIdentity(dst)
	if err != nil || dev != mountDev {
		return nil, ErrBoundFDPhase
	}
	pathID, err := pathPhaseIdentity(destPath)
	if err != nil || pathID != dstID {
		return nil, ErrBoundFDPhase
	}
	current, err := store.CaptureBoundMode(obs.SpoolID, obs.BirthID, obs.Path, obs.SpoolFile, obs.FileIdentity)
	if err != nil || current != obs {
		return nil, ErrBoundFDPhase
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping || r.byPath[obs.Path] != nil {
		return nil, ErrBoundFDPhase
	}
	if len(r.byPath) == 0 {
		r.empty = make(chan struct{})
	}
	l := &boundFDPhase{registry: r, store: store, obs: obs, src: src, dst: dst, destPath: destPath, destIdentity: dstID, mountDev: mountDev}
	r.byPath[obs.Path] = l
	return l, nil
}

func (l *boundFDPhase) verifyFD(withPath bool) (os.FileInfo, error) {
	id, dev, fi, err := fdPhaseIdentity(l.dst)
	if err != nil || id != l.destIdentity || dev != l.mountDev {
		return nil, ErrBoundFDPhase
	}
	if withPath {
		pathID, err := pathPhaseIdentity(l.destPath)
		if err != nil || pathID != id {
			return nil, ErrBoundFDPhase
		}
	}
	return fi, nil
}

type boundFDOperation func(*os.File, fs.FileMode, time.Time, bool) error

func defaultBoundFDOperation(f *os.File, mode fs.FileMode, mtime time.Time, withTime bool) error {
	return boundFDOperationWithChmod(f, mode, mtime, withTime, (*os.File).Chmod)
}

func boundFDOperationWithChmod(f *os.File, mode fs.FileMode, mtime time.Time, withTime bool, chmod func(*os.File, fs.FileMode) error) error {
	if err := chmod(f, mode.Perm()); err != nil {
		if !errors.Is(err, syscall.EPERM) {
			return err
		}
		fi, statErr := f.Stat()
		if statErr != nil || fi.Mode().Perm() != mode.Perm() {
			return err
		}
	}
	if withTime {
		tv := unix.NsecToTimeval(mtime.UnixNano())
		if err := unix.Futimes(int(f.Fd()), []unix.Timeval{tv, tv}); err != nil {
			return err
		}
	}
	return nil
}

// launch never closes an FD on timeout. The goroutine owns the syscall until
// join observes completion; Stop cannot finish while the phase remains held.
func (l *boundFDPhase) launch(kind string, mode fs.FileMode, mtime time.Time, withTime bool, timeout time.Duration, op boundFDOperation) error {
	if op == nil {
		op = defaultBoundFDOperation
	}
	if l.worker != nil || l.released || l.batch {
		return &PendingBoundFDPhase{Cause: ErrBoundFDPhase}
	}
	w := &boundFDWorker{done: make(chan struct{}), kind: kind}
	l.worker = w
	go func() {
		_, err := l.verifyFD(true)
		if err == nil {
			err = op(l.dst, mode, mtime, withTime)
		}
		if err == nil {
			var fi os.FileInfo
			fi, err = l.verifyFD(true)
			if err == nil && fi.Mode().Perm() != mode.Perm() {
				err = ErrBoundFDPhase
			}
			// Local Unix-microsecond equality is this helper's precision policy,
			// not a promise that every FUSE backend stores finer timestamps.
			if err == nil && withTime && fi.ModTime().UnixMicro() != mtime.UnixMicro() {
				err = ErrBoundFDPhase
			}
		}
		l.mu.Lock()
		w.err = err
		close(w.done)
		l.mu.Unlock()
	}()
	l.mu.Unlock()
	if timeout <= 0 {
		l.mu.Lock()
		return &PendingBoundFDPhase{Cause: context.DeadlineExceeded}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-w.done:
		l.mu.Lock()
		return l.joinLocked(w)
	case <-timer.C:
		l.mu.Lock()
		return &PendingBoundFDPhase{Cause: context.DeadlineExceeded}
	}
}

func (l *boundFDPhase) joinLocked(w *boundFDWorker) error {
	if l.worker != w {
		return ErrBoundFDPhase
	}
	select {
	case <-w.done:
	default:
		return &PendingBoundFDPhase{Cause: context.DeadlineExceeded}
	}
	l.worker = nil
	if w.err != nil {
		return w.err
	}
	if w.kind == "settle" {
		l.settled = true
	}
	return nil
}

// settle captures the mode/token atomically, applies mode+mtime to the pinned
// predecessor FD, and returns the token P40 may use only after joined success.
func (l *boundFDPhase) settle(mtime time.Time, timeout time.Duration, op boundFDOperation) (metadata.BoundModeObservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.worker != nil || l.released || l.batch {
		return metadata.BoundModeObservation{}, &PendingBoundFDPhase{Cause: ErrBoundFDPhase}
	}
	current, err := l.store.CaptureBoundMode(l.obs.SpoolID, l.obs.BirthID, l.obs.Path, l.obs.SpoolFile, l.obs.FileIdentity)
	if err != nil || current != l.obs {
		return metadata.BoundModeObservation{}, ErrBoundFDPhase
	}
	l.mtime = mtime
	err = l.launch("settle", current.Mode, mtime, true, timeout, op)
	if err != nil {
		return metadata.BoundModeObservation{}, err
	}
	return l.obs, nil
}

// setattr cannot acknowledge a mode until the same predecessor FD reflects
// the committed metadata CAS. Any slow/error path retains lease and source.
func (l *boundFDPhase) setattr(mode fs.FileMode, timeout time.Duration, op boundFDOperation) (metadata.BoundModeObservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.worker != nil || l.released || l.batch {
		return metadata.BoundModeObservation{}, &PendingBoundFDPhase{Cause: ErrBoundFDPhase}
	}
	next, err := l.store.ApplyBoundMode(l.obs, mode)
	if err != nil {
		return metadata.BoundModeObservation{}, err
	}
	l.obs = next
	l.settled = false
	err = l.launch("setattr", next.Mode, l.mtime, false, timeout, op)
	if err != nil {
		return metadata.BoundModeObservation{}, err
	}
	return next, nil
}

func (l *boundFDPhase) join(ctx context.Context) error {
	l.mu.Lock()
	w := l.worker
	l.mu.Unlock()
	if w == nil {
		return nil
	}
	select {
	case <-w.done:
	case <-ctx.Done():
		return &PendingBoundFDPhase{Cause: ctx.Err()}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.joinLocked(w)
}

// transferBatch closes only the settled destination FD. The private source
// stays pinned and this path lease remains registered until exact result handoff.
func (l *boundFDPhase) transferBatch() (metadata.BoundModeObservation, string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.worker != nil || !l.settled || l.batch || l.released {
		return metadata.BoundModeObservation{}, "", ErrBoundFDPhase
	}
	if _, err := l.verifyFD(true); err != nil {
		return metadata.BoundModeObservation{}, "", err
	}
	if err := l.dst.Close(); err != nil {
		return metadata.BoundModeObservation{}, "", err
	}
	l.dst = nil
	l.batch = true
	return l.obs, l.destIdentity, nil
}

// acceptBatchResults requires every lease's exact typed P40 result, even when
// replies arrive reordered. Missing/uncertain results transfer no ownership.
func (r *boundFDPhaseRegistry) acceptBatchResults(leases []*boundFDPhase, results []metadata.BoundDrainCommitResult) error {
	if len(leases) == 0 || len(leases) != len(results) {
		return &PendingBoundFDPhase{Cause: ErrBoundFDPhase}
	}
	byID := make(map[int64]metadata.BoundDrainCommitResult, len(results))
	for _, res := range results {
		if _, ok := byID[res.SpoolID]; ok {
			return ErrBoundFDPhase
		}
		byID[res.SpoolID] = res
	}
	seenLeases := make(map[*boundFDPhase]struct{}, len(leases))
	seenIDs := make(map[int64]struct{}, len(leases))
	for _, l := range leases {
		if l == nil {
			return &PendingBoundFDPhase{Cause: ErrBoundFDPhase}
		}
		l.mu.Lock()
		_, duplicateLease := seenLeases[l]
		_, duplicateID := seenIDs[l.obs.SpoolID]
		res, ok := byID[l.obs.SpoolID]
		valid := !duplicateLease && !duplicateID && ok && l.registry == r && l.batch && !l.released && l.src != nil && res.BirthID == l.obs.BirthID && res.Path == l.obs.Path &&
			res.SpoolFile == l.obs.SpoolFile && res.FileIdentity == l.obs.FileIdentity && res.BackingIdentity == l.destIdentity &&
			res.Published.Path == l.obs.Path && res.Published.Incarnation == l.obs.Token.Incarnation &&
			res.Published.Revision == l.obs.Token.Revision+1 && res.FullSize == l.obs.FullSize &&
			res.OriginalInode == l.obs.Inode && res.OriginalMode == l.obs.Mode
		l.mu.Unlock()
		if !valid {
			return &PendingBoundFDPhase{Cause: ErrBoundFDPhase}
		}
		seenLeases[l] = struct{}{}
		seenIDs[l.obs.SpoolID] = struct{}{}
		if err := l.store.ValidateBoundDrainCompletion(res); err != nil {
			return &PendingBoundFDPhase{Cause: err}
		}
	}
	if len(seenIDs) != len(byID) {
		return &PendingBoundFDPhase{Cause: ErrBoundFDPhase}
	}
	for _, l := range leases {
		l.mu.Lock()
		res := byID[l.obs.SpoolID]
		l.validated = &res
		l.mu.Unlock()
	}
	return nil
}

// takeForEffects explicitly transfers the still-open private source and
// validated completion receipt to an effects owner. This does not authorize
// effects: the next owner must revalidate generation for each effect. Until
// this call, Stop continues to wait for the registry-owned source.
func (l *boundFDPhase) takeForEffects() (*os.File, metadata.BoundDrainCommitResult, error) {
	l.registry.mu.Lock()
	defer l.registry.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.effectsClaimed || !l.batch || l.validated == nil || l.src == nil {
		return nil, metadata.BoundDrainCommitResult{}, ErrBoundFDPhase
	}
	src, res := l.src, *l.validated
	l.src = nil
	l.released = true
	delete(l.registry.byPath, l.obs.Path)
	if len(l.registry.byPath) == 0 {
		close(l.registry.empty)
	}
	return src, res, nil
}

// takeForReconciliation explicitly moves a failed/joined phase to a caller
// that will retain both descriptors. It cannot run while a syscall is live.
func (l *boundFDPhase) takeForReconciliation() (*os.File, *os.File, error) {
	l.registry.mu.Lock()
	defer l.registry.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.worker != nil || l.released || l.effectsClaimed || l.src == nil {
		return nil, nil, ErrBoundFDPhase
	}
	src, dst := l.src, l.dst
	l.src, l.dst = nil, nil
	l.released = true
	delete(l.registry.byPath, l.obs.Path)
	if len(l.registry.byPath) == 0 {
		close(l.registry.empty)
	}
	return src, dst, nil
}
