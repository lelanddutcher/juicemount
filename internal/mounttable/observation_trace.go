package mounttable

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// observation is diagnostic only. The one shared queryGate, not this record,
// is the authority to launch another kernel observer.
type observation struct {
	id         uint64
	kind       string
	executable string
	pid        int
	acquired   time.Time
	launched   time.Time
	deadline   time.Time
	timedOut   bool
}

var observationState struct {
	sync.Mutex
	next   uint64
	active *observation
}

func beginObservation(ctx context.Context, gate chan struct{}, kind, executable string) *observation {
	if gate != queryGate {
		return nil // private test gates do not own the process-global diagnostic slot
	}
	observationState.Lock()
	observationState.next++
	deadline, _ := ctx.Deadline()
	op := &observation{id: observationState.next, kind: kind, executable: executable,
		acquired: time.Now(), deadline: deadline}
	observationState.active = op
	observationState.Unlock()
	jmlog.Info("mount observer acquired", "operation_id", op.id, "kind", kind,
		"executable", executable, "deadline", deadline)
	return op
}

func launchedObservation(op *observation, cmd *exec.Cmd) {
	if op == nil {
		return
	}
	observationState.Lock()
	if observationState.active == op {
		op.pid, op.launched = cmd.Process.Pid, time.Now()
	}
	observationState.Unlock()
	jmlog.Info("mount observer child started", "operation_id", op.id, "kind", op.kind,
		"executable", op.executable, "pid", op.pid, "launched", op.launched)
}

func timeoutObservation(op *observation, reason error) {
	if op == nil {
		return
	}
	observationState.Lock()
	active := observationState.active == op
	if active {
		op.timedOut = true
	}
	observationState.Unlock()
	if active {
		jmlog.Warn("mount observer deadline while original child retained", "operation_id", op.id,
			"kind", op.kind, "executable", op.executable, "pid", op.pid,
			"launched", op.launched, "deadline", op.deadline, "error", reason)
	}
}

func activeObservation() string {
	observationState.Lock()
	defer observationState.Unlock()
	op := observationState.active
	if op == nil {
		return "owner=unknown"
	}
	return fmt.Sprintf("operation=%d kind=%s executable=%q pid=%d launched=%s elapsed=%s timed_out=%t",
		op.id, op.kind, op.executable, op.pid, op.launched.Format(time.RFC3339Nano),
		time.Since(op.acquired).Round(time.Millisecond), op.timedOut)
}

func blockedObservation() string {
	owner := activeObservation()
	jmlog.Warn("mount observer gate blocked by original operation", "active", owner)
	return owner
}

// Only the original cmd.Wait path may call this with waited=true. The gate
// release and active-record clear occur under one lock and match this token;
// a late timeout from a retired operation cannot change a new owner.
func finishObservation(op *observation, gate chan struct{}, err error, waited bool) {
	if op != nil {
		observationState.Lock()
		if observationState.active == op {
			<-gate
			observationState.active = nil
		} else {
			// A mismatched token must never release another observer's gate.
			observationState.Unlock()
			return
		}
		observationState.Unlock()
		jmlog.Info("mount observer operation settled", "operation_id", op.id,
			"kind", op.kind, "executable", op.executable, "pid", op.pid,
			"launched", op.launched, "joined", waited, "wait_error", err)
		return
	}
	<-gate
}
