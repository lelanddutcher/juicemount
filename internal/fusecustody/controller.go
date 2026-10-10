package fusecustody

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

// Typed admission outcomes. A caller can tell "the helper's root open has not
// returned" from "macOS refused the open" without parsing message text.
//
// ErrAdmissionPending means no admission frame arrived within the caller's
// bound. On macOS the most common cause is the Network Volumes (TCC) prompt:
// the helper's open() parks in the kernel until the user answers it.
// ErrAdmissionPermissionDenied means the helper's root open failed with
// EPERM/EACCES, which is how a denied Network Volumes approval surfaces.
var (
	ErrAdmissionPending          = errors.New("custody response pending")
	ErrAdmissionRefused          = errors.New("custody admission refused")
	ErrAdmissionPermissionDenied = errors.New("custody admission permission denied")
)

type received struct {
	event Event
	err   error
}

// Session retains one helper process, its exact Wait result and one operation
// at a time. An uncertain write or result is never resent under a new ID.
type Session struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	events    chan received
	waitDone  chan struct{}
	session   string
	opMu      sync.Mutex
	mu        sync.Mutex
	waitErr   error
	readerErr error
	ready     bool
	refused   bool
	denied    bool
	uuid      string
	readonly  bool
	fsid      [2]int32
	nextID    uint64
	pending   string
	pendingID uint64
	writeDone chan error
	written   bool
	detached  bool
	released  bool
}

func newSession(cmd *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser, session string) *Session {
	return newSessionWithWait(cmd, stdin, stdout, session, cmd.Wait)
}

func newSessionWithWait(cmd *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser, session string, wait func() error) *Session {
	s := &Session{cmd: cmd, stdin: stdin, events: make(chan received, 4), waitDone: make(chan struct{}), session: session}
	go func() {
		defer func() { waitErr := wait(); s.mu.Lock(); s.waitErr = waitErr; s.mu.Unlock(); close(s.waitDone) }()
		reader := bufio.NewReaderSize(stdout, maxFrame+1)
		for {
			var event Event
			err := readFrame(reader, &event)
			if err != nil {
				s.mu.Lock()
				s.readerErr = err
				s.mu.Unlock()
			}
			select {
			case s.events <- received{event, err}:
			default:
				s.mu.Lock()
				s.readerErr = errors.New("custody response overflow")
				s.mu.Unlock()
			}
			if err != nil {
				_, _ = io.Copy(io.Discard, reader)
				return
			}
		}
	}()
	return s
}
func Start(executable, root, source string, child, parent int) (*Session, error) {
	if executable == "" || root == "" || source == "" || child <= 1 || parent <= 1 {
		return nil, errors.New("incomplete FUSE custody claim")
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	session := hex.EncodeToString(nonce)
	cmd := exec.Command(executable, "--fuse-custodian", root, strconv.Itoa(child), strconv.Itoa(parent), source, session)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return newSession(cmd, stdin, stdout, session), nil
}
func (s *Session) PID() int {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return 0
	}
	return s.cmd.Process.Pid
}
func (s *Session) waitEvent(timeout time.Duration) (Event, error) {
	if timeout <= 0 {
		return Event{}, ErrAdmissionPending
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-s.events:
		if r.err != nil {
			return Event{}, fmt.Errorf("custody output lost: %w", r.err)
		}
		return r.event, nil
	case <-timer.C:
		return Event{}, ErrAdmissionPending
	}
}
func (s *Session) WaitReady(timeout time.Duration) error {
	if s == nil {
		return errors.New("custody helper missing")
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	if s.ready {
		s.mu.Unlock()
		return nil
	}
	if s.refused {
		denied := s.denied
		s.mu.Unlock()
		if denied {
			return ErrAdmissionPermissionDenied
		}
		return ErrAdmissionRefused
	}
	s.mu.Unlock()
	event, err := s.waitEvent(timeout)
	if err != nil {
		return err
	}
	if event.Version != protocolVersion || event.Session != s.session || event.ID != 0 {
		return errors.New("custody admission correlation invalid")
	}
	if event.State == "admission_refused" || event.State == "unsupported_os" || event.State == "admission_permission_denied" {
		denied := event.State == "admission_permission_denied"
		s.mu.Lock()
		s.refused, s.denied = true, denied
		s.mu.Unlock()
		if denied {
			return ErrAdmissionPermissionDenied
		}
		return ErrAdmissionRefused
	}
	if event.State != "root_admitted" || !CanonicalUUID(event.UUID) || event.ReadOnly == nil || event.FSID == [2]int32{} {
		return errors.New("custody admission invalid")
	}
	s.mu.Lock()
	s.ready, s.uuid, s.readonly, s.fsid = true, event.UUID, *event.ReadOnly, event.FSID
	s.mu.Unlock()
	return nil
}
func (s *Session) Identity() (string, bool, [2]int32, bool) {
	if s == nil {
		return "", false, [2]int32{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uuid, s.readonly, s.fsid, s.ready
}
func (s *Session) Detached() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.detached
}
func (s *Session) Settled() bool {
	if s == nil {
		return true
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
func (s *Session) NeverAdmittedSettled() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	refused := s.refused
	s.mu.Unlock()
	if !refused {
		return false
	}
	select {
	case <-s.waitDone:
		s.mu.Lock()
		err := s.waitErr
		readerErr := s.readerErr
		s.mu.Unlock()
		var exit *exec.ExitError
		return errors.As(err, &exit) && exit.ExitCode() == 78 && errors.Is(readerErr, io.EOF)
	default:
		return false
	}
}
func (s *Session) PendingDetach() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending == "detach"
}
func (s *Session) operation(name string, timeout time.Duration, existingOnly bool) (Event, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	if !s.ready || s.released || (s.pending != "" && s.pending != name) || (existingOnly && s.pending != name) {
		s.mu.Unlock()
		return Event{}, errors.New("custody operation state invalid")
	}
	if s.pending == "" {
		s.nextID++
		s.pendingID = s.nextID
		s.pending = name
		s.written = false
		s.writeDone = make(chan error, 1)
		req := request{Version: protocolVersion, Session: s.session, ID: s.pendingID, Op: name}
		raw, _ := json.Marshal(req)
		raw = append(raw, '\n')
		writeDone := s.writeDone
		stdin := s.stdin
		go func() {
			n, err := stdin.Write(raw)
			if err == nil && n != len(raw) {
				err = io.ErrShortWrite
			}
			writeDone <- err
		}()
	}
	written, writeDone, id := s.written, s.writeDone, s.pendingID
	s.mu.Unlock()
	if !written {
		if timeout <= 0 {
			return Event{}, errors.New("custody write pending")
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case err := <-writeDone:
			if err != nil {
				return Event{}, fmt.Errorf("custody write uncertain: %w", err)
			}
			s.mu.Lock()
			s.written = true
			s.mu.Unlock()
		case <-timer.C:
			return Event{}, errors.New("custody write pending")
		}
	}
	event, err := s.waitEvent(timeout)
	if err != nil {
		return Event{}, err
	}
	if event.Version != protocolVersion || event.Session != s.session || event.ID != id {
		return Event{}, errors.New("custody response correlation invalid")
	}
	valid := false
	switch name {
	case "detach":
		valid = event.State == "detached_child_join_required" || event.State == "already_detached" || event.State == "retained_retry"
	case "release":
		valid = event.State == "released" || event.State == "close_failed"
	}
	if !valid {
		return Event{}, errors.New("custody response state invalid")
	}
	s.mu.Lock()
	s.pending = ""
	if event.State == "detached_child_join_required" || event.State == "already_detached" {
		s.detached = true
	}
	if event.State == "released" {
		s.released = true
	}
	s.mu.Unlock()
	return event, nil
}
func (s *Session) Detach(timeout time.Duration) error {
	if s.Detached() {
		return nil
	}
	event, err := s.operation("detach", timeout, false)
	if err != nil {
		return err
	}
	if event.State != "detached_child_join_required" && event.State != "already_detached" {
		return errors.New("custody retained for retry")
	}
	return nil
}

// ReconcileDetach consumes only an already-issued response; it never sends a
// second command after a possibly successful kernel detach.
func (s *Session) ReconcileDetach(timeout time.Duration) error {
	if s.Detached() {
		return nil
	}
	event, err := s.operation("detach", timeout, true)
	if err != nil {
		return err
	}
	if event.State != "detached_child_join_required" && event.State != "already_detached" {
		return errors.New("custody retained for retry")
	}
	return nil
}
func (s *Session) Release(timeout time.Duration) error {
	if s == nil || !s.Detached() {
		return errors.New("custody cannot release before detach")
	}
	s.mu.Lock()
	released := s.released
	s.mu.Unlock()
	if !released {
		event, err := s.operation("release", timeout, false)
		if err != nil {
			return err
		}
		if event.State != "released" {
			return errors.New("custody close retained")
		}
	}
	if timeout <= 0 {
		return errors.New("custody helper join pending")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-s.waitDone:
		s.mu.Lock()
		err := s.waitErr
		readerErr := s.readerErr
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("custody helper exit: %w", err)
		}
		if !errors.Is(readerErr, io.EOF) {
			return errors.New("custody helper output did not end cleanly")
		}
		return nil
	case <-timer.C:
		return errors.New("custody helper join pending")
	}
}
