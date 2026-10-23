package privatefile

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

// These fixed modes exist only in the privatefile test build. The production
// API exposes neither a syscall callback nor a mutable process-wide hook.
type PublicationTestPhase string

const (
	PublicationTestRename      PublicationTestPhase = "rename"
	PublicationTestReadySync   PublicationTestPhase = "ready-sync"
	PublicationTestAttemptSync PublicationTestPhase = "attempt-sync"
)

type PublicationTestTrace struct {
	rename  atomic.Int32
	ready   atomic.Int32
	attempt atomic.Int32
}

func (tr *PublicationTestTrace) Calls() (rename, readySync, attemptSync int32) {
	return tr.rename.Load(), tr.ready.Load(), tr.attempt.Load()
}

type PublicationTestGate struct {
	PublicationTestTrace
	entered  chan struct{}
	released chan struct{}
	once     sync.Once
}

func (g *PublicationTestGate) Entered() <-chan struct{} { return g.entered }
func (g *PublicationTestGate) Release()                 { g.once.Do(func() { close(g.released) }) }
func (g *PublicationTestGate) hold()                    { close(g.entered); <-g.released }

type PublicationTestSyncFault struct{ PublicationTestTrace }

func validPublicationTestPhase(phase PublicationTestPhase) bool {
	return phase == PublicationTestRename || phase == PublicationTestReadySync || phase == PublicationTestAttemptSync
}

func publicationTestDispatch(trace *PublicationTestTrace, gate *PublicationTestGate, gatePhase, faultPhase PublicationTestPhase) *publicationSyscalls {
	return &publicationSyscalls{
		rename: func(oldDir int, oldName string, newDir int, newName string) error {
			if err := renameNoReplace(oldDir, oldName, newDir, newName); err != nil {
				return err
			}
			trace.rename.Add(1)
			if gatePhase == PublicationTestRename {
				gate.hold()
			}
			return nil
		},
		readySync: func(dir *os.File) error {
			if err := dir.Sync(); err != nil {
				return err
			}
			trace.ready.Add(1)
			if gatePhase == PublicationTestReadySync {
				gate.hold()
			}
			if faultPhase == PublicationTestReadySync {
				return errors.New("injected reported error after underlying ready directory sync")
			}
			return nil
		},
		attemptSync: func(dir *os.File) error {
			if err := dir.Sync(); err != nil {
				return err
			}
			trace.attempt.Add(1)
			if gatePhase == PublicationTestAttemptSync {
				gate.hold()
			}
			if faultPhase == PublicationTestAttemptSync {
				return errors.New("injected reported error after underlying attempt directory sync")
			}
			return nil
		},
	}
}

func InstallPublicationGateForTest(p *PreparedFile, phase PublicationTestPhase) (*PublicationTestGate, error) {
	if p == nil || p.publication != nil || !validPublicationTestPhase(phase) {
		return nil, errors.New("publication test gate requires one prepared descriptor and fixed phase")
	}
	gate := &PublicationTestGate{entered: make(chan struct{}), released: make(chan struct{})}
	p.publication = publicationTestDispatch(&gate.PublicationTestTrace, gate, phase, "")
	return gate, nil
}

func InstallPublicationSyncFaultForTest(p *PreparedFile, phase PublicationTestPhase) (*PublicationTestSyncFault, error) {
	if p == nil || p.publication != nil || phase != PublicationTestReadySync && phase != PublicationTestAttemptSync {
		return nil, errors.New("publication test sync fault requires one prepared descriptor and fixed sync phase")
	}
	fault := &PublicationTestSyncFault{}
	p.publication = publicationTestDispatch(&fault.PublicationTestTrace, nil, "", phase)
	return fault, nil
}

func InstallPublicationTraceForTest(p *PreparedFile) (*PublicationTestTrace, error) {
	if p == nil || p.publication != nil {
		return nil, errors.New("publication test trace requires one prepared descriptor")
	}
	trace := &PublicationTestTrace{}
	p.publication = publicationTestDispatch(trace, nil, "", "")
	return trace, nil
}

// Recovery test dispatch is fixed to the original two directory Sync calls.
// A gate holds an operation boundary after its underlying syscall returns.
type RecoveryTestPhase string

const (
	RecoveryTestReadySync   RecoveryTestPhase = "ready-sync"
	RecoveryTestAttemptSync RecoveryTestPhase = "attempt-sync"
)

type RecoveryTestTrace struct{ ready, attempt atomic.Int32 }

func (tr *RecoveryTestTrace) Calls() (ready, attempt int32) {
	return tr.ready.Load(), tr.attempt.Load()
}

type RecoveryTestGate struct {
	RecoveryTestTrace
	entered  chan struct{}
	released chan struct{}
	once     sync.Once
}

func (g *RecoveryTestGate) Entered() <-chan struct{} { return g.entered }
func (g *RecoveryTestGate) Release()                 { g.once.Do(func() { close(g.released) }) }

type RecoveryTestSyncFault struct{ RecoveryTestTrace }

func recoveryTestDispatch(trace *RecoveryTestTrace, gate *RecoveryTestGate, gatePhase, faultPhase RecoveryTestPhase) *recoverySyncOps {
	return &recoverySyncOps{
		ready: func(dir *os.File) error {
			if err := dir.Sync(); err != nil {
				return err
			}
			trace.ready.Add(1)
			if gatePhase == RecoveryTestReadySync {
				close(gate.entered)
				<-gate.released
			}
			if faultPhase == RecoveryTestReadySync {
				return errors.New("injected reported error after existing-final ready sync")
			}
			return nil
		},
		attempt: func(dir *os.File) error {
			if err := dir.Sync(); err != nil {
				return err
			}
			trace.attempt.Add(1)
			if gatePhase == RecoveryTestAttemptSync {
				close(gate.entered)
				<-gate.released
			}
			if faultPhase == RecoveryTestAttemptSync {
				return errors.New("injected reported error after existing-final attempt sync")
			}
			return nil
		},
	}
}
func InstallRecoveryTraceForTest(o *OwnedReady) (*RecoveryTestTrace, error) {
	if o == nil || o.syncOps != nil {
		return nil, errors.New("recovery trace requires one owned final")
	}
	trace := &RecoveryTestTrace{}
	o.syncOps = recoveryTestDispatch(trace, nil, "", "")
	return trace, nil
}
func InstallRecoverySyncFaultForTest(o *OwnedReady, phase RecoveryTestPhase) (*RecoveryTestSyncFault, error) {
	if o == nil || o.syncOps != nil || phase != RecoveryTestReadySync && phase != RecoveryTestAttemptSync {
		return nil, errors.New("recovery sync fault requires one owned final and fixed phase")
	}
	fault := &RecoveryTestSyncFault{}
	o.syncOps = recoveryTestDispatch(&fault.RecoveryTestTrace, nil, "", phase)
	return fault, nil
}
func InstallRecoveryGateForTest(o *OwnedReady, phase RecoveryTestPhase) (*RecoveryTestGate, error) {
	if o == nil || o.syncOps != nil || phase != RecoveryTestReadySync && phase != RecoveryTestAttemptSync {
		return nil, errors.New("recovery gate requires one owned final and fixed phase")
	}
	gate := &RecoveryTestGate{entered: make(chan struct{}), released: make(chan struct{})}
	o.syncOps = recoveryTestDispatch(&gate.RecoveryTestTrace, gate, phase, "")
	return gate, nil
}

// Close-fault modes are fixed and per attempt. Each wrapper calls the actual
// os.File.Close once, then reports an injected error; it never preserves a
// descriptor or pretends to know whether a failed close retained its lock.
type AttemptCloseTestPhase string

const (
	AttemptCloseTestData      AttemptCloseTestPhase = "data"
	AttemptCloseTestRead      AttemptCloseTestPhase = "read"
	AttemptCloseTestDirectory AttemptCloseTestPhase = "directory"
	AttemptCloseTestLock      AttemptCloseTestPhase = "lock"
)

type AttemptCloseTestFault struct{ calls atomic.Int32 }

func (f *AttemptCloseTestFault) Calls() int32 { return f.calls.Load() }

func installAttemptCloseFault(a *Attempt, phase AttemptCloseTestPhase) (*AttemptCloseTestFault, error) {
	if a == nil || a.closeOps != nil {
		return nil, errors.New("attempt close fault requires one owned attempt")
	}
	if phase != AttemptCloseTestData && phase != AttemptCloseTestRead && phase != AttemptCloseTestDirectory && phase != AttemptCloseTestLock {
		return nil, errors.New("unknown attempt close phase")
	}
	fault := &AttemptCloseTestFault{}
	wrap := func(file *os.File) error {
		original := file.Close()
		fault.calls.Add(1)
		return errors.Join(original, errors.New("injected original attempt close uncertainty"))
	}
	ops := &attemptCloseOps{}
	switch phase {
	case AttemptCloseTestData:
		ops.data = wrap
	case AttemptCloseTestRead:
		ops.read = wrap
	case AttemptCloseTestDirectory:
		ops.directory = wrap
	case AttemptCloseTestLock:
		ops.lock = wrap
	}
	a.closeOps = ops
	return fault, nil
}

func InstallAttemptCloseFaultForTest(a *Attempt, phase AttemptCloseTestPhase) (*AttemptCloseTestFault, error) {
	return installAttemptCloseFault(a, phase)
}
func InstallPreparedCloseFaultForTest(p *PreparedFile, phase AttemptCloseTestPhase) (*AttemptCloseTestFault, error) {
	if p == nil || p.attempt == nil {
		return nil, errors.New("prepared close fault needs owned attempt")
	}
	if phase == AttemptCloseTestData {
		return nil, errors.New("prepared data writer already closed")
	}
	return installAttemptCloseFault(p.attempt, phase)
}
