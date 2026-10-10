package fusecustody

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

const testSession = "0123456789abcdef0123456789abcdef"

type sigpipeWitnessWriter struct{}

func (sigpipeWitnessWriter) Write(p []byte) (int, error) {
	n, err := os.Stdout.Write(p)
	if errors.Is(err, syscall.EPIPE) {
		_, _ = io.WriteString(os.Stderr, "EPIPE_WITNESSED\n")
	} else if err != nil {
		_, _ = io.WriteString(os.Stderr, "OTHER_WRITE_ERROR\n")
	}
	return n, err
}

func startTestSession(t *testing.T, mode string) *Session {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestCustodyTransportProcess")
	cmd.Env = append(os.Environ(), "CUSTODY_TRANSPORT_MODE="+mode)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := newSession(cmd, stdin, stdout, testSession)
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-s.waitDone:
		case <-time.After(3 * time.Second):
			t.Error("test helper did not join")
		}
	})
	return s
}
func TestCustodyTransportProcess(t *testing.T) {
	mode := os.Getenv("CUSTODY_TRANSPORT_MODE")
	if mode == "" {
		return
	}
	switch mode {
	case "refused":
		_ = writeEvent(os.Stdout, Event{Version: 1, Session: testSession, State: "admission_refused"})
		os.Exit(78)
	case "eof":
		os.Exit(7)
	case "permission_denied":
		// The real custodian path, with the root open failing exactly as a
		// denied macOS Network Volumes approval reports it.
		os.Exit(runCustodianWithOpen("/owned", 11, 22, "source", testSession, os.Stdin, os.Stdout, func(string, int, int, string) (rootCustody, error) {
			return nil, &os.PathError{Op: "open", Path: "/owned", Err: syscall.EPERM}
		}))
	case "permission_hold":
		// The root open never returns, as while the TCC prompt is unanswered.
		os.Exit(runCustodianWithOpen("/owned", 11, 22, "source", testSession, os.Stdin, os.Stdout, func(string, int, int, string) (rootCustody, error) {
			time.Sleep(time.Hour)
			return nil, errors.New("unreachable")
		}))
	case "bad":
		_, _ = os.Stdout.Write([]byte("{bad}\n"))
		time.Sleep(time.Second)
		os.Exit(0)
	case "sigpipe":
		_ = runCustodianWithOpen("/owned", 11, 22, "source", testSession, os.Stdin, sigpipeWitnessWriter{}, func(string, int, int, string) (rootCustody, error) { return &fakeRoot{}, nil })
		os.Exit(0)
	}
	if mode == "delay_admit" || mode == "delay_both" {
		time.Sleep(80 * time.Millisecond)
	}
	readonly := false
	_ = writeEvent(os.Stdout, Event{Version: 1, Session: testSession, State: "root_admitted", UUID: "12345678-1234-1234-1234-123456789012", ReadOnly: &readonly, FSID: [2]int32{7, 9}})
	if mode == "stdout_close_hold" {
		_ = os.Stdout.Close()
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil || line != "release\n" {
			os.Exit(8)
		}
		os.Exit(0)
	}
	if mode == "stdout_close" {
		_ = os.Stdout.Close()
		time.Sleep(time.Second)
		os.Exit(0)
	}
	if mode == "closed_stdin" {
		_ = os.Stdin.Close()
		time.Sleep(time.Second)
		os.Exit(0)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			os.Exit(9)
		}
		var req request
		if json.Unmarshal([]byte(line), &req) != nil {
			os.Exit(8)
		}
		if req.Version != 1 || req.Session != testSession {
			os.Exit(8)
		}
		if req.Op == "detach" {
			if mode == "wrong_id" {
				_ = writeEvent(os.Stdout, Event{Version: 1, Session: testSession, ID: req.ID + 1, State: "detached_child_join_required"})
				time.Sleep(time.Second)
				os.Exit(0)
			}
			if mode == "delay_detach" || mode == "delay_both" {
				time.Sleep(80 * time.Millisecond)
			}
			_ = writeEvent(os.Stdout, Event{Version: 1, Session: testSession, ID: req.ID, State: "detached_child_join_required"})
			continue
		}
		if req.Op == "release" {
			_ = writeEvent(os.Stdout, Event{Version: 1, Session: testSession, ID: req.ID, State: "released"})
			os.Exit(0)
		}
		os.Exit(8)
	}
}
func TestSessionDelayedAdmissionAndSinglePendingDetach(t *testing.T) {
	s := startTestSession(t, "delay_both")
	if err := s.WaitReady(time.Millisecond); err == nil {
		t.Fatal("delayed admission looked ready")
	}
	if err := s.WaitReady(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.Detach(time.Millisecond); err == nil || !s.PendingDetach() {
		t.Fatalf("detach should remain pending: %v", err)
	}
	if err := s.ReconcileDetach(time.Second); err != nil || !s.Detached() {
		t.Fatalf("late response not reconciled: %v", err)
	}
	if err := s.Release(3 * time.Second); err != nil || !s.Settled() {
		t.Fatalf("release/join incomplete: %v", err)
	}
}
func TestSessionNeverAdmittedVersusUnknownOutput(t *testing.T) {
	refused := startTestSession(t, "refused")
	if err := refused.WaitReady(time.Second); err == nil {
		t.Fatal("refusal admitted")
	}
	deadline := time.After(3 * time.Second)
	for !refused.NeverAdmittedSettled() {
		select {
		case <-deadline:
			t.Fatal("definite refusal not joined")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	eof := startTestSession(t, "eof")
	if err := eof.WaitReady(time.Second); err == nil || eof.NeverAdmittedSettled() {
		t.Fatalf("EOF misclassified: %v", err)
	}
	malformed := startTestSession(t, "bad")
	if err := malformed.WaitReady(time.Second); err == nil || malformed.NeverAdmittedSettled() {
		t.Fatalf("malformed output misclassified: %v", err)
	}
}
func TestCustodianKeepsDescriptorWhenStdoutCloses(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestCustodyTransportProcess")
	cmd.Env = append(os.Environ(), "CUSTODY_TRANSPORT_MODE=sigpipe")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	first, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || !strings.Contains(first, "root_admitted") {
		t.Fatalf("no admission: %q %v", first, err)
	}
	_ = stdout.Close()
	raw, _ := json.Marshal(request{Version: 1, Session: testSession, ID: 1, Op: "detach"})
	_, _ = stdin.Write(append(raw, '\n'))
	witness := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(stderr).ReadString('\n'); witness <- line }()
	select {
	case line := <-witness:
		if line != "EPIPE_WITNESSED\n" {
			t.Fatalf("no EPIPE witness: %q", line)
		}
	case <-time.After(time.Second):
		t.Fatal("helper did not attempt broken stdout write")
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("SIGPIPE dropped retained helper: %v", err)
	}
}
func TestSessionClosedInputRetainsUncertainOperation(t *testing.T) {
	s := startTestSession(t, "closed_stdin")
	if err := s.WaitReady(time.Second); err != nil {
		t.Fatal(err)
	}
	err := s.Detach(50 * time.Millisecond)
	if err == nil || !s.PendingDetach() || s.Detached() {
		t.Fatalf("closed input lost uncertainty: %v", err)
	}
}
func TestSessionClosedOutputLiveHelperDoesNotLockState(t *testing.T) {
	s := startTestSession(t, "stdout_close")
	if err := s.WaitReady(time.Second); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _, _, _, _ = s.Identity(); _ = s.Settled(); _ = s.PendingDetach(); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("state access blocked on live helper Wait")
	}
	if err := s.Detach(100 * time.Millisecond); err == nil || !s.PendingDetach() {
		t.Fatalf("lost output settled detach: %v", err)
	}
}

// The waiter hook proves cmd.Wait has been entered while the exact child is
// still held alive. Both the corrected and deliberately regressed controller
// run this identical bounded assertion and explicit cleanup.
func TestSessionStateAccessAfterOriginalWaitEntered(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestCustodyTransportProcess")
	cmd.Env = append(os.Environ(), "CUSTODY_TRANSPORT_MODE=stdout_close_hold")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitEntered := make(chan struct{})
	s := newSessionWithWait(cmd, stdin, stdout, testSession, func() error { close(waitEntered); return cmd.Wait() })
	released := false
	defer func() {
		if !released {
			_, _ = io.WriteString(stdin, "release\n")
		}
		select {
		case <-s.waitDone:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-s.waitDone
		}
	}()
	select {
	case <-waitEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("fixture never entered original Wait")
	}
	caseDone := make(chan error, 1)
	go func() {
		if err := s.WaitReady(time.Second); err != nil {
			caseDone <- err
			return
		}
		_, _, _, ready := s.Identity()
		if !ready {
			caseDone <- errors.New("admission not ready")
			return
		}
		_ = s.PendingDetach()
		_ = s.Settled()
		caseDone <- nil
	}()
	var caseErr error
	callerDone := false
	select {
	case caseErr = <-caseDone:
		callerDone = true
	case <-time.After(2 * time.Second):
		caseErr = errors.New("bounded session access blocked while original Wait held")
	}
	select {
	case <-s.waitDone:
		caseErr = errors.New("fixture child exited before release")
	default:
	}
	if _, err := io.WriteString(stdin, "release\n"); err != nil && caseErr == nil {
		caseErr = err
	}
	released = true
	select {
	case <-s.waitDone:
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		<-s.waitDone
		if caseErr == nil {
			caseErr = errors.New("original helper Wait did not join")
		}
	}
	s.mu.Lock()
	waitErr := s.waitErr
	s.mu.Unlock()
	if waitErr != nil && caseErr == nil {
		caseErr = waitErr
	}
	if caseErr != nil {
		// A regressed mutex ordering may unblock only after release; drain the
		// exact blocked caller before asserting failure.
		if !callerDone {
			select {
			case <-caseDone:
			case <-time.After(3 * time.Second):
				t.Fatal("fixture caller failed to join after child release")
			}
		}
		t.Fatal(caseErr)
	}
}

func TestSessionWrongResponseIDKeepsPendingDetach(t *testing.T) {
	s := startTestSession(t, "wrong_id")
	if err := s.WaitReady(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.Detach(time.Second); err == nil || !s.PendingDetach() || s.Detached() {
		t.Fatalf("mismatched response cleared custody: %v", err)
	}
}

func TestSessionTypesPermissionDeniedAndPendingAdmission(t *testing.T) {
	denied := startTestSession(t, "permission_denied")
	if err := denied.WaitReady(time.Second); !errors.Is(err, ErrAdmissionPermissionDenied) {
		t.Fatalf("denied root open not typed: %v", err)
	}
	// The cached refusal keeps its type on every later call.
	if err := denied.WaitReady(time.Millisecond); !errors.Is(err, ErrAdmissionPermissionDenied) {
		t.Fatalf("repeated denial lost its type: %v", err)
	}
	deadline := time.After(3 * time.Second)
	for !denied.NeverAdmittedSettled() {
		select {
		case <-deadline:
			t.Fatal("denied custodian was not joined through its exact process handle")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	held := startTestSession(t, "permission_hold")
	start := time.Now()
	err := held.WaitReady(100 * time.Millisecond)
	if !errors.Is(err, ErrAdmissionPending) || errors.Is(err, ErrAdmissionPermissionDenied) {
		t.Fatalf("held root open not typed pending: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("pending admission exceeded its bound: %v", elapsed)
	}
	if held.NeverAdmittedSettled() {
		t.Fatal("a still-running custodian was reported settled")
	}
	refused := startTestSession(t, "refused")
	if err := refused.WaitReady(time.Second); !errors.Is(err, ErrAdmissionRefused) || errors.Is(err, ErrAdmissionPermissionDenied) {
		t.Fatalf("ordinary refusal mistyped: %v", err)
	}
}
