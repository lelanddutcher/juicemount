package nfs

import (
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// namespaceCoordinator owns names until the backing mutation and mirror
// publication have both finished. RPC timeout does not cancel a filesystem
// syscall, so ownership belongs to the worker, never the waiting RPC.
// Its mutex is only used for admission/retirement, before all store/spool locks.
type namespaceCoordinator struct {
	mu       sync.Mutex
	active   map[*namespaceOperation][]string
	reads    map[string]*readEpoch
	stopping bool
	changed  chan struct{}
	// Tests install immutable phase barriers before starting any operations.
	phase func(string, string)
}

type readEpoch struct {
	generation uint64
	refs       int
}

type readAdmission struct {
	owner      *namespaceCoordinator
	name       string
	epoch      *readEpoch
	generation uint64
}

func cleanNamespaceName(name string) string {
	name = path.Clean("/" + name)[1:]
	if name == "" {
		return "."
	}
	return name
}

func (c *namespaceCoordinator) readErrorLocked(name string, allActive bool) error {
	active := false
	for op, names := range c.active {
		if !allActive && !op.failed && !op.blocksReads {
			continue
		}
		for _, held := range names {
			if namespaceOverlap(name, held) {
				if op.failed {
					return syscall.EIO
				}
				active = true
			}
		}
	}
	if active {
		if !allActive {
			metrics.Default().IncAdmissionRefusal(metrics.AdmissionNamespaceServingBlocked)
		}
		return errFUSETimeout
	}
	return nil
}

// admitRead records only live readers. Namespace admission invalidates every
// overlapping ticket, including a read waiting on QoS or a later subread.
func (c *namespaceCoordinator) admitRead(name string) (*readAdmission, error) {
	name = cleanNamespaceName(name)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.readErrorLocked(name, true); err != nil {
		return nil, err
	}
	if c.reads == nil {
		c.reads = make(map[string]*readEpoch)
	}
	e := c.reads[name]
	if e == nil {
		e = &readEpoch{}
		c.reads[name] = e
	}
	e.refs++
	return &readAdmission{owner: c, name: name, epoch: e, generation: e.generation}, nil
}

func (r *readAdmission) checkLocked() error {
	if err := r.owner.readErrorLocked(r.name, true); err != nil {
		return err
	}
	if r.epoch.generation != r.generation {
		return errFUSETimeout
	}
	return nil
}

func (r *readAdmission) check() error {
	r.owner.mu.Lock()
	defer r.owner.mu.Unlock()
	return r.checkLocked()
}

func (r *readAdmission) release() {
	r.owner.mu.Lock()
	defer r.owner.mu.Unlock()
	r.epoch.refs--
	if r.epoch.refs == 0 {
		delete(r.owner.reads, r.name)
	}
}

func (r *readAdmission) finishBytes(p []byte, n *int, err *error) {
	if refusal := r.check(); refusal != nil {
		clear(p)
		*n, *err = 0, refusal
	}
	r.release()
}

type namespaceOperation struct {
	owner       *namespaceCoordinator
	mutated     bool
	poison      bool
	failed      bool // guarded by owner.mu
	blocksReads bool // guarded by owner.mu
}

func (op *namespaceOperation) blockReads() {
	op.owner.mu.Lock()
	op.blocksReads = true
	op.owner.mu.Unlock()
}

func (c *namespaceCoordinator) servingError(name string) error {
	name = cleanNamespaceName(name)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readErrorLocked(name, false)
}

func namespaceOverlap(a, b string) bool {
	return a == "." || b == "." || a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func (c *namespaceCoordinator) begin(names []string) *namespaceOperation {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping || len(c.active) >= 24 {
		// Preserve original priority when both refusal conditions hold.
		if c.stopping {
			metrics.Default().IncAdmissionRefusal(metrics.AdmissionNamespaceStopping)
		} else {
			metrics.Default().IncAdmissionRefusal(metrics.AdmissionNamespaceActiveCap)
		}
		return nil
	}
	clean := make([]string, len(names))
	for i, name := range names {
		clean[i] = cleanNamespaceName(name)
		for _, held := range c.active {
			for _, other := range held {
				if namespaceOverlap(clean[i], other) {
					metrics.Default().IncAdmissionRefusal(metrics.AdmissionNamespaceOverlap)
					return nil
				}
			}
		}
	}
	if c.active == nil {
		c.active = make(map[*namespaceOperation][]string)
		c.changed = make(chan struct{})
	}
	op := &namespaceOperation{owner: c}
	c.active[op] = clean
	for name, epoch := range c.reads {
		for _, held := range clean {
			if namespaceOverlap(name, held) {
				epoch.generation++
				break
			}
		}
	}
	return op
}

func (op *namespaceOperation) checkpoint(phase, name string) {
	if op.owner.phase != nil {
		op.owner.phase(phase, name)
	}
}

func (op *namespaceOperation) finish(err error) {
	c := op.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	// A failed publication after a successful mutation has no safe retry by
	// name. Keep the scope blocked until recovery is implemented (H8).
	if op.poison || (op.mutated && err != nil) {
		op.failed = true
	} else {
		delete(c.active, op)
	}
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *namespaceCoordinator) stop(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		c.mu.Lock()
		c.stopping = true
		if len(c.active) == 0 {
			c.mu.Unlock()
			return true
		}
		for op := range c.active {
			if op.failed {
				c.mu.Unlock()
				return false
			}
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			return false
		}
	}
}

// The result channel transfers ownership of a returned file only if the RPC
// is still waiting. A late file is closed while the namespace is still owned.
func runNamespaceOperation[T any](h *JuiceMountHandler, names []string, work func(*namespaceOperation) (T, error), abandon func(T) error) (T, error) {
	return runNamespaceOperationUntil(h, names, time.Now().Add(mutationOpTimeout()), work, abandon)
}

func runNamespaceOperationUntil[T any](h *JuiceMountHandler, names []string, deadline time.Time, work func(*namespaceOperation) (T, error), abandon func(T) error) (T, error) {
	var zero T
	if time.Until(deadline) <= 0 {
		metrics.Default().IncAdmissionRefusal(metrics.AdmissionNamespacePreDeadline)
		return zero, errFUSETimeout
	}
	op := h.namespace.begin(names)
	if op == nil {
		return zero, errFUSETimeout
	}
	if time.Until(deadline) <= 0 {
		op.finish(nil)
		metrics.Default().IncAdmissionRefusal(metrics.AdmissionNamespacePostDeadline)
		return zero, errFUSETimeout
	}
	type result struct {
		value T
		err   error
	}
	results := make(chan result)
	abandoned := make(chan struct{})
	done := make(chan struct{})
	go func() {
		value, err := work(op)
		select {
		case results <- result{value, err}:
		case <-abandoned:
			if abandon != nil {
				if closeErr := abandon(value); closeErr != nil {
					op.poison = true
				}
			}
		}
		op.finish(err)
		close(done)
	}()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case result := <-results:
		<-done
		return result.value, result.err
	case <-timer.C:
		close(abandoned)
		metrics.Default().IncAdmissionRefusal(metrics.AdmissionNamespaceWorkerDeadline)
		return zero, errFUSETimeout
	}
}

func closeNamespaceFile(f billy.File) error {
	if wf, ok := f.(*writeFile); ok {
		// The abandoned result was never exposed to a caller. Its creating
		// worker still owns the namespace, so release without nested admission.
		wf.release()
		return nil
	}
	if f != nil {
		return f.Close()
	}
	return nil
}

func (jfs *juiceFS) namespaceMutation(names []string, work func(*namespaceOperation) error) error {
	_, err := runNamespaceOperation(jfs.handler, names, func(op *namespaceOperation) (struct{}, error) {
		return struct{}{}, work(op)
	}, nil)
	return err
}
