package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
	"github.com/lelanddutcher/juicemount/internal/version"
)

type statusCommandClient struct {
	outcome  client.Outcome
	err      error
	gotID    string
	deadline time.Time
	calls    int
	closed   bool
}

func (c *statusCommandClient) Status(ctx context.Context, opID string) (client.Outcome, error) {
	c.calls++
	c.gotID = opID
	c.deadline, _ = ctx.Deadline()
	return c.outcome, c.err
}

func (c *statusCommandClient) Close() { c.closed = true }

func statusCommandJSON(t *testing.T, b *bytes.Buffer) unmountStatusResult {
	t.Helper()
	var got unmountStatusResult
	if err := json.Unmarshal(bytes.TrimSpace(b.Bytes()), &got); err != nil {
		t.Fatalf("decode command result: %v; output=%q", err, b.String())
	}
	if got.SchemaVersion != 1 || got.SourceCommit != version.Commit {
		t.Fatalf("diagnostic identity schema=%d commit=%q, want schema 1 commit %q", got.SchemaVersion, got.SourceCommit, version.Commit)
	}
	return got
}

func TestUnmountOperationStatusRejectsBadArgsBeforeDial(t *testing.T) {
	for _, args := range [][]string{
		{unmountOperationStatusFlag},
		{unmountOperationStatusFlag, "bad/id"},
		{unmountOperationStatusFlag, "valid-op", "extra"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var out, stderr bytes.Buffer
			dialed := false
			code := runUnmountOperationStatus(args, &out, &stderr, false, false, func(string) unmountStatusClient {
				dialed = true
				return &statusCommandClient{}
			}, 5*time.Second)
			if code == 0 || dialed {
				t.Fatalf("code=%d dialed=%v", code, dialed)
			}
			if got := statusCommandJSON(t, &out); got.ErrorCode != "invalid_arguments" || got.StatusRead {
				t.Fatalf("unexpected result: %+v", got)
			}
		})
	}
}

func TestUnmountOperationStatusRejectsRootAndServiceBuildBeforeDial(t *testing.T) {
	for _, tc := range []struct {
		name        string
		root, build bool
		want        string
	}{
		{"root", true, false, "root_not_allowed"},
		{"service", false, true, "service_build_not_allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, stderr bytes.Buffer
			dialed := false
			code := runUnmountOperationStatus([]string{unmountOperationStatusFlag, "op-1"}, &out, &stderr,
				tc.root, tc.build, func(string) unmountStatusClient {
					dialed = true
					return &statusCommandClient{}
				}, 5*time.Second)
			if code == 0 || dialed {
				t.Fatalf("code=%d dialed=%v", code, dialed)
			}
			if got := statusCommandJSON(t, &out); got.ErrorCode != tc.want {
				t.Fatalf("error_code=%q, want %q", got.ErrorCode, tc.want)
			}
		})
	}
}

func TestUnmountOperationStatusUsesStatusServiceAndReportsProgressOnly(t *testing.T) {
	for _, state := range []string{"IN_KERNEL", "RETIRED"} {
		t.Run(state, func(t *testing.T) {
			var out, stderr bytes.Buffer
			c := &statusCommandClient{outcome: client.Outcome{
				OpID: "op-1", State: state, Syscalls: 2, Forced: true, StuckObserved: true,
				Holders: []client.Holder{{Name: "holder-secret", Files: 1}}, HoldersStale: true,
				Descendants: []client.Entry{{MntOnName: "/private/path"}}, Anomaly: []client.Entry{{MntFromName: "private-source"}},
			}}
			var service string
			code := runUnmountOperationStatus([]string{unmountOperationStatusFlag, "op-1"}, &out, &stderr,
				false, false, func(got string) unmountStatusClient { service = got; return c }, 5*time.Second)
			if code != 0 || service != client.StatusMachService || c.calls != 1 || c.gotID != "op-1" || !c.closed {
				t.Fatalf("code=%d service=%q calls=%d id=%q closed=%v", code, service, c.calls, c.gotID, c.closed)
			}
			if left := time.Until(c.deadline); left <= 0 || left > 5*time.Second {
				t.Fatalf("status context deadline remaining %s, want at most 5s", left)
			}
			got := statusCommandJSON(t, &out)
			if !got.StatusRead || got.OperationRetired != (state == "RETIRED") || got.Holders != 1 ||
				got.Descendants != 1 || got.Anomalies != 1 || got.Syscalls != 2 || !got.Forced || !got.StuckObserved {
				t.Fatalf("unexpected status: %+v", got)
			}
			for _, private := range []string{"holder-secret", "/private/path", "private-source"} {
				if strings.Contains(out.String(), private) {
					t.Fatalf("status leaked %q: %s", private, out.String())
				}
			}
		})
	}
}

func TestUnmountOperationStatusPreservesDaemonCodeWithoutMessage(t *testing.T) {
	var out, stderr bytes.Buffer
	c := &statusCommandClient{err: &client.DaemonError{Code: client.CodeNoReceipt, Message: "private receipt path"}}
	code := runUnmountOperationStatus([]string{unmountOperationStatusFlag, "op-1"}, &out, &stderr,
		false, false, func(string) unmountStatusClient { return c }, 5*time.Second)
	got := statusCommandJSON(t, &out)
	if code == 0 || got.ErrorCode != client.CodeNoReceipt || got.StatusRead || !c.closed {
		t.Fatalf("code=%d result=%+v closed=%v", code, got, c.closed)
	}
	if strings.Contains(out.String(), "private receipt path") {
		t.Fatalf("daemon message leaked: %s", out.String())
	}
}

func TestUnmountOperationStatusRejectsMismatchedOrEmptyResponse(t *testing.T) {
	for _, outcome := range []client.Outcome{{OpID: "other", State: "RETIRED"}, {OpID: "op-1"}} {
		var out, stderr bytes.Buffer
		c := &statusCommandClient{outcome: outcome}
		code := runUnmountOperationStatus([]string{unmountOperationStatusFlag, "op-1"}, &out, &stderr,
			false, false, func(string) unmountStatusClient { return c }, 5*time.Second)
		if code == 0 || statusCommandJSON(t, &out).ErrorCode != "invalid_status_response" || !c.closed {
			t.Fatalf("code=%d result=%s closed=%v", code, out.String(), c.closed)
		}
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("writer failed") }

func TestUnmountOperationStatusOutputFailureIsBounded(t *testing.T) {
	var stderr bytes.Buffer
	code := writeUnmountStatusResult(failedWriter{}, &stderr, unmountStatusResult{StatusRead: true}, 0)
	if code == 0 || !strings.Contains(stderr.String(), `"error_code":"output_failed"`) {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	var got unmountStatusResult
	if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &got); err != nil || got.SchemaVersion != 1 || got.SourceCommit != version.Commit {
		t.Fatalf("fallback identity=%+v err=%v", got, err)
	}
}

func TestUnmountOperationStatusCommandRecognitionIsNarrow(t *testing.T) {
	if !isUnmountOperationStatusCommand([]string{unmountOperationStatusFlag}) {
		t.Fatal("status flag was not recognized")
	}
	for _, args := range [][]string{nil, {"--volume-owner"}, {"--other"}} {
		if isUnmountOperationStatusCommand(args) {
			t.Fatalf("unexpected status command match: %v", args)
		}
	}
}

func TestUnmountOperationStatusMainEntryRejectsMalformedBeforeStartup(t *testing.T) {
	if os.Getenv("JM_TEST_UNMOUNT_STATUS_CHILD") == "1" {
		os.Args = []string{os.Args[0], unmountOperationStatusFlag}
		main()
		t.Fatal("main returned without handling the status command")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestUnmountOperationStatusMainEntryRejectsMalformedBeforeStartup$")
	cmd.Env = append(os.Environ(), "JM_TEST_UNMOUNT_STATUS_CHILD=1")
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 2 {
		t.Fatalf("child exit=%v output=%q, want exit 2", err, output)
	}
	var got unmountStatusResult
	if err := json.Unmarshal(bytes.TrimSpace(output), &got); err != nil {
		t.Fatalf("decode child result: %v; output=%q", err, output)
	}
	if got.SchemaVersion != 1 || got.SourceCommit != version.Commit || got.StatusRead || got.ErrorCode != "invalid_arguments" {
		t.Fatalf("unexpected child result: %+v", got)
	}
}
