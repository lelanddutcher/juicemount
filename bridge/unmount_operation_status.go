package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
	"github.com/lelanddutcher/juicemount/internal/version"
)

const (
	unmountOperationStatusFlag    = "--unmount-operation-status"
	unmountOperationStatusTimeout = 5 * time.Second
)

type unmountStatusClient interface {
	Status(context.Context, string) (client.Outcome, error)
	Close()
}

type unmountStatusResult struct {
	SchemaVersion int    `json:"schema_version"`
	SourceCommit  string `json:"source_commit"`
	StatusRead    bool   `json:"status_read"`
	OpID          string `json:"op_id,omitempty"`
	State         string `json:"state,omitempty"`
	// OperationRetired reflects only the helper's reported state; it does not
	// prove owner/generation/lifetime retirement or process joins.
	OperationRetired bool   `json:"operation_retired"`
	Syscalls         int    `json:"syscalls"`
	Forced           bool   `json:"forced"`
	StuckObserved    bool   `json:"stuck_observed"`
	Holders          int    `json:"holders"`
	HoldersStale     bool   `json:"holders_stale"`
	Descendants      int    `json:"descendants"`
	Anomalies        int    `json:"anomalies"`
	ErrorCode        string `json:"error_code,omitempty"`
}

func isUnmountOperationStatusCommand(args []string) bool {
	return len(args) > 0 && args[0] == unmountOperationStatusFlag
}

func runUnmountOperationStatusCommand(args []string, stdout, stderr io.Writer) int {
	return runUnmountOperationStatus(args, stdout, stderr, os.Geteuid() == 0, serviceClientBuild,
		newUnmountStatusClient, unmountOperationStatusTimeout)
}

func newUnmountStatusClient(service string) unmountStatusClient {
	return client.New(client.Options{Service: service})
}

// runUnmountOperationStatus intentionally exposes only Status and Close to its
// client seam. A status query cannot register, reclaim, cancel, ping, or retire.
func runUnmountOperationStatus(args []string, stdout, stderr io.Writer, root, serviceBuild bool,
	newClient func(string) unmountStatusClient, timeout time.Duration) int {
	result := unmountStatusResult{SchemaVersion: 1, SourceCommit: version.Commit}
	if len(args) != 2 || args[0] != unmountOperationStatusFlag || !client.ValidOpID(args[1]) {
		result.ErrorCode = "invalid_arguments"
		return writeUnmountStatusResult(stdout, stderr, result, 2)
	}
	if root {
		result.OpID = args[1]
		result.ErrorCode = "root_not_allowed"
		return writeUnmountStatusResult(stdout, stderr, result, 2)
	}
	if serviceBuild {
		result.OpID = args[1]
		result.ErrorCode = "service_build_not_allowed"
		return writeUnmountStatusResult(stdout, stderr, result, 2)
	}
	if newClient == nil || timeout <= 0 {
		result.OpID = args[1]
		result.ErrorCode = "status_client_unavailable"
		return writeUnmountStatusResult(stdout, stderr, result, 1)
	}
	c := newClient(client.StatusMachService)
	if c == nil {
		result.OpID = args[1]
		result.ErrorCode = "status_client_unavailable"
		return writeUnmountStatusResult(stdout, stderr, result, 1)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	outcome, err := c.Status(ctx, args[1])
	if err != nil {
		result.OpID = args[1]
		result.ErrorCode = unmountStatusErrorCode(err)
		return writeUnmountStatusResult(stdout, stderr, result, 1)
	}
	if outcome.OpID != args[1] || outcome.State == "" {
		result.OpID = args[1]
		result.ErrorCode = "invalid_status_response"
		return writeUnmountStatusResult(stdout, stderr, result, 1)
	}
	result = unmountStatusResult{
		SchemaVersion:    1,
		SourceCommit:     version.Commit,
		StatusRead:       true,
		OpID:             outcome.OpID,
		State:            outcome.State,
		OperationRetired: outcome.State == "RETIRED",
		Syscalls:         outcome.Syscalls,
		Forced:           outcome.Forced,
		StuckObserved:    outcome.StuckObserved,
		Holders:          len(outcome.Holders),
		HoldersStale:     outcome.HoldersStale,
		Descendants:      len(outcome.Descendants),
		Anomalies:        len(outcome.Anomaly),
	}
	return writeUnmountStatusResult(stdout, stderr, result, 0)
}

func unmountStatusErrorCode(err error) string {
	var daemonErr *client.DaemonError
	if errors.As(err, &daemonErr) && daemonErr.Code != "" {
		return daemonErr.Code
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, client.ErrTimeout):
		return "transport_timeout"
	case errors.Is(err, client.ErrUnavailable):
		return "transport_unavailable"
	case errors.Is(err, client.ErrPeerRejected):
		return "transport_peer_rejected"
	default:
		return "status_read_failed"
	}
}

func writeUnmountStatusResult(stdout, stderr io.Writer, result unmountStatusResult, exitCode int) int {
	data, err := json.Marshal(result)
	if err == nil {
		var n int
		n, err = stdout.Write(append(data, '\n'))
		if err == nil && n != len(data)+1 {
			err = io.ErrShortWrite
		}
	}
	if err != nil {
		fallback := unmountStatusResult{SchemaVersion: result.SchemaVersion, SourceCommit: result.SourceCommit, ErrorCode: "output_failed"}
		if fallback.SchemaVersion == 0 {
			fallback.SchemaVersion = 1
		}
		if fallback.SourceCommit == "" {
			fallback.SourceCommit = version.Commit
		}
		data, _ := json.Marshal(fallback)
		_, _ = stderr.Write(append(data, '\n'))
		return 1
	}
	return exitCode
}
