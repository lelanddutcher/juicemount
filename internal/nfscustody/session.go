package nfscustody

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
)

type received struct {
	event event
	err   error
}
type Session struct {
	cmd                                       *exec.Cmd
	stdin                                     io.WriteCloser
	events                                    chan received
	waitDone                                  chan struct{}
	gate                                      chan struct{}
	mu                                        sync.Mutex
	session, boot, operation, path, source    string
	waitErr, readerErr                        error
	ready, refused, armed, detached, released bool
	fsid                                      [2]int32
	nextID, pendingID                         uint64
	pending                                   string
	pendingRecord                             *mountbroker.Record
	armedRecord                               *mountbroker.Record
	writeDone                                 chan error
	written                                   bool
}

func startSession(helper, boot, operation, path, source string) (*Session, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	session := hex.EncodeToString(nonce)
	cmd := exec.Command(helper, "--nfs-custodian", session, boot, operation, path, source)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	s := &Session{cmd: cmd, stdin: stdin, events: make(chan received, 4), waitDone: make(chan struct{}), gate: make(chan struct{}, 1), session: session, boot: boot, operation: operation, path: path, source: source}
	s.gate <- struct{}{}
	go func() {
		r := bufio.NewReaderSize(stdout, maxFrame+1)
		for {
			var e event
			err := readFrame(r, &e)
			if err != nil {
				s.mu.Lock()
				s.readerErr = err
				s.mu.Unlock()
			}
			select {
			case s.events <- received{e, err}:
			default:
				s.mu.Lock()
				s.readerErr = errors.New("NFS custody response overflow")
				s.mu.Unlock()
			}
			if err != nil {
				_, _ = io.Copy(io.Discard, r)
				break
			}
		}
		waitErr := cmd.Wait()
		s.mu.Lock()
		s.waitErr = waitErr
		s.mu.Unlock()
		close(s.waitDone)
	}()
	return s, nil
}
func (s *Session) lock(timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("NFS custody operation pending")
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-s.gate:
		return nil
	case <-t.C:
		return errors.New("NFS custody operation busy")
	}
}
func (s *Session) unlock() { s.gate <- struct{}{} }
func (s *Session) waitEvent(timeout time.Duration) (event, error) {
	if timeout <= 0 {
		return event{}, errors.New("NFS custody response pending")
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case r := <-s.events:
		if r.err != nil {
			return event{}, r.err
		}
		return r.event, nil
	case <-t.C:
		return event{}, errors.New("NFS custody response pending")
	}
}
func (s *Session) correlated(e event, id uint64) bool {
	return e.Version == protocolVersion && e.Session == s.session && e.ID == id && e.Boot == s.boot && e.Operation == s.operation && e.Path == s.path && e.Source == s.source
}
func (s *Session) WaitReady(timeout time.Duration) error {
	if s == nil {
		return errors.New("missing NFS custody session")
	}
	deadline := time.Now().Add(timeout)
	if err := s.lock(timeout); err != nil {
		return err
	}
	defer s.unlock()
	s.mu.Lock()
	ready, refused := s.ready, s.refused
	s.mu.Unlock()
	if ready {
		return nil
	}
	if refused {
		return errors.New("NFS custody admission refused")
	}
	e, err := s.waitEvent(time.Until(deadline))
	if err != nil {
		return err
	}
	if !s.correlated(e, 0) {
		return errors.New("NFS custody admission correlation invalid")
	}
	if e.State == "admission_refused" || e.State == "unsupported_os" {
		s.mu.Lock()
		s.refused = true
		s.mu.Unlock()
		return errors.New("NFS custody admission refused")
	}
	if e.State != "root_admitted" || e.FSID == [2]int32{} {
		return errors.New("NFS custody admission invalid")
	}
	s.mu.Lock()
	s.ready = true
	s.fsid = e.FSID
	s.mu.Unlock()
	return nil
}
func (s *Session) Identity() ([2]int32, bool) {
	if s == nil {
		return [2]int32{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fsid, s.ready
}
func (s *Session) LiveAdmitted() bool {
	if s == nil {
		return false
	}
	select {
	case <-s.waitDone:
		return false
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready && !s.detached && !s.released && s.readerErr == nil && s.fsid != [2]int32{}
}
func (s *Session) Detached() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.detached
}
func (s *Session) Armed() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.armed
}
func (s *Session) ArmPending() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending == "arm"
}
func sameArmRecord(a, b *mountbroker.Record) bool {
	return a != nil && b != nil && a.Version == b.Version &&
		a.OperationID == b.OperationID && a.BootID == b.BootID &&
		a.Spec == b.Spec && a.State == b.State && a.Mount != nil &&
		b.Mount != nil && *a.Mount == *b.Mount
}
func (s *Session) Settled() bool {
	if s == nil {
		return false
	}
	select {
	case <-s.waitDone:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.released && s.waitErr == nil && errors.Is(s.readerErr, io.EOF)
	default:
		return false
	}
}
func (s *Session) operationCommand(name string, timeout time.Duration, existingOnly bool) (event, error) {
	return s.operationCommandWithRecord(name, nil, timeout, existingOnly)
}
func (s *Session) operationCommandWithRecord(name string, record *mountbroker.Record, timeout time.Duration, existingOnly bool) (event, error) {
	deadline := time.Now().Add(timeout)
	if err := s.lock(timeout); err != nil {
		return event{}, err
	}
	defer s.unlock()
	s.mu.Lock()
	if !s.ready || s.released || s.pending != "" && s.pending != name ||
		existingOnly && s.pending != name || name == "arm" &&
		(s.detached || s.armed || record == nil || record.State != mountbroker.Observed ||
			record.Mount == nil || record.Mount.FSID != s.fsid) ||
		s.pending == "arm" && !sameArmRecord(s.pendingRecord, record) {
		s.mu.Unlock()
		return event{}, errors.New("NFS custody command state invalid")
	}
	if s.pending == "" && s.readerErr != nil {
		s.mu.Unlock()
		return event{}, errors.New("NFS custody output fault; no new command permitted")
	}
	if s.pending == "" {
		s.nextID++
		s.pendingID = s.nextID
		s.pending = name
		if record != nil {
			copy := *record
			mount := *record.Mount
			copy.Mount = &mount
			s.pendingRecord = &copy
		}
		s.written = false
		s.writeDone = make(chan error, 1)
		req := request{Version: protocolVersion, Session: s.session, ID: s.pendingID, Op: name, Record: s.pendingRecord}
		writeDone, stdin := s.writeDone, s.stdin
		go func() { writeDone <- writeRequestFrame(stdin, req) }()
	}
	written, writeDone, id := s.written, s.writeDone, s.pendingID
	s.mu.Unlock()
	if !written {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return event{}, errors.New("NFS custody write pending")
		}
		t := time.NewTimer(remaining)
		defer t.Stop()
		select {
		case err := <-writeDone:
			if err != nil {
				return event{}, fmt.Errorf("NFS custody write uncertain: %w", err)
			}
			s.mu.Lock()
			s.written = true
			s.mu.Unlock()
		case <-t.C:
			return event{}, errors.New("NFS custody write pending")
		}
	}
	e, err := s.waitEvent(time.Until(deadline))
	if err != nil {
		return event{}, err
	}
	if !s.correlated(e, id) {
		return event{}, errors.New("NFS custody response correlation invalid")
	}
	valid := name == "detach" && (e.State == "detached" || e.State == "already_detached" || e.State == "retained_retry") ||
		name == "release" && (e.State == "released" || e.State == "close_failed") ||
		name == "arm" && e.State == "armed" && e.FSID == s.fsid
	if !valid {
		return event{}, errors.New("NFS custody response invalid")
	}
	s.mu.Lock()
	s.pending = ""
	if e.State == "armed" {
		s.armed = true
		s.armedRecord = s.pendingRecord
	}
	s.pendingRecord = nil
	if e.State == "detached" || e.State == "already_detached" {
		s.detached = true
	}
	if e.State == "released" {
		s.released = true
	}
	s.mu.Unlock()
	return e, nil
}
func (s *Session) Arm(record mountbroker.Record, timeout time.Duration) error {
	if s == nil {
		return errors.New("missing NFS custody session")
	}
	s.mu.Lock()
	armed, accepted := s.armed, s.armedRecord
	s.mu.Unlock()
	if armed {
		if sameArmRecord(accepted, &record) {
			return nil
		}
		return errors.New("NFS custodian arm identity changed after acknowledgment")
	}
	e, err := s.operationCommandWithRecord("arm", &record, timeout, false)
	if err != nil {
		return err
	}
	if e.State != "armed" {
		return errors.New("NFS custodian did not arm original descriptor")
	}
	return nil
}
func (s *Session) Detach(timeout time.Duration) error {
	if s.Detached() {
		return nil
	}
	e, err := s.operationCommand("detach", timeout, false)
	if err != nil {
		return err
	}
	if e.State != "detached" && e.State != "already_detached" {
		return errors.New("NFS custody detach retained for retry")
	}
	return nil
}
func (s *Session) ReconcileDetach(timeout time.Duration) error {
	if s.Detached() {
		return nil
	}
	e, err := s.operationCommand("detach", timeout, true)
	if err != nil {
		return err
	}
	if e.State != "detached" && e.State != "already_detached" {
		return errors.New("NFS custody detach retained for retry")
	}
	return nil
}
func (s *Session) Release(timeout time.Duration) error {
	if s == nil || !s.Detached() {
		return errors.New("NFS custody cannot release before detach")
	}
	deadline := time.Now().Add(timeout)
	s.mu.Lock()
	released := s.released
	s.mu.Unlock()
	if !released {
		e, err := s.operationCommand("release", timeout, false)
		if err != nil {
			return err
		}
		if e.State != "released" {
			return errors.New("NFS custody close retained")
		}
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return errors.New("NFS custody helper join pending")
	}
	t := time.NewTimer(remaining)
	defer t.Stop()
	select {
	case <-s.waitDone:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.waitErr != nil {
			return s.waitErr
		}
		if !errors.Is(s.readerErr, io.EOF) {
			return errors.New("NFS custody output did not end cleanly")
		}
		return nil
	case <-t.C:
		return errors.New("NFS custody helper join pending")
	}
}

// CapabilityProbe retains the exact original helper child through EOF and
// Wait. A timeout never authorizes a mount birth or a replacement probe.
type CapabilityProbe struct {
	done chan struct{}
	err  error
}

func StartCapability(helper string) (*CapabilityProbe, error) {
	cmd := exec.Command(helper, "--nfs-custodian-capability")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		return nil, err
	}
	p := &CapabilityProbe{done: make(chan struct{})}
	go func() {
		r := bufio.NewReaderSize(stdout, maxFrame+1)
		var e event
		readErr := readFrame(r, &e)
		var extra [1]byte
		n, tailErr := r.Read(extra[:])
		waitErr := cmd.Wait()
		if readErr != nil || n != 0 || tailErr != io.EOF || waitErr != nil || e.Version != protocolVersion || e.State != "available" || e.ID != 0 || e.Session != "" || e.Boot != "" || e.Operation != "" || e.Path != "" || e.Source != "" || e.FSID != [2]int32{} {
			p.err = errors.New("NFS helper capability unsupported or malformed")
		}
		close(p.done)
	}()
	return p, nil
}
func (p *CapabilityProbe) Settled() bool {
	if p == nil {
		return false
	}
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}
func (p *CapabilityProbe) Wait(timeout time.Duration) error {
	if p == nil {
		return errors.New("missing NFS capability probe")
	}
	if timeout <= 0 {
		return errors.New("NFS capability probe pending")
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-p.done:
		return p.err
	case <-t.C:
		return errors.New("NFS capability probe pending; child retained")
	}
}
