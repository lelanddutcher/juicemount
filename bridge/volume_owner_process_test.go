//go:build !serviceclient

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// This child runs the actual service entry point. The parent sends only inspect
// and shutdown: it cannot create a FUSE generation, Link node, or mount.
func TestVolumeOwnerProcessFixture(t *testing.T) {
	if os.Getenv("JUICEMOUNT_OWNER_PROCESS_FIXTURE") != "1" {
		return
	}
	os.Args = []string{os.Args[0], "--volume-owner"}
	main()
}

func TestVolumeOwnerBootstrapProcessExitsThroughAuthenticatedShutdown(t *testing.T) {
	root := privateIsolationTestRoot(t, "vop-")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	childCtx, cancelChild := context.WithCancel(context.Background())
	defer cancelChild()
	cmd := exec.CommandContext(childCtx, executable, "-test.run=^TestVolumeOwnerProcessFixture$")
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "TMPDIR", "JM_APP_ISOLATION", "JM_APP_ISOLATION_ROOT", "JUICEMOUNT_OWNER_PROCESS_FIXTURE":
			continue
		}
		cmd.Env = append(cmd.Env, value)
	}
	// Set the real isolation inputs only in this empty child's environment.
	cmd.Env = append(cmd.Env, "TMPDIR="+filepath.Join(root, "tmp"),
		"JM_APP_ISOLATION=1", "JM_APP_ISOLATION_ROOT="+root, "JUICEMOUNT_OWNER_PROCESS_FIXTURE=1")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }() // the only Wait for this exact child
	joined := false
	var selected volumeOwnerReceipt
	const shutdownID = "fedcba9876543210fedcba9876543210"
	path := filepath.Join(root, "o.sock")
	call := func(method string, values []string) (volumeOwnerReceipt, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := servicecontrol.Call(ctx, path, servicecontrol.Request{
			Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: method, Strings: values})
		if err != nil {
			return volumeOwnerReceipt{}, err
		}
		var receipt volumeOwnerReceipt
		if err := json.Unmarshal([]byte(response.Text), &receipt); err != nil {
			return receipt, err
		}
		if response.Error != "" {
			return receipt, errors.New(response.Error)
		}
		return receipt, nil
	}
	defer func() {
		if joined {
			return
		}
		if selected.Instance != "" {
			_, _ = call("shutdown", []string{selected.Instance, shutdownID, selected.ConfigDigest})
		}
		select {
		case <-waited:
			joined = true
			return
		case <-time.After(3 * time.Second):
		}
		// Failure cleanup is restricted to this known empty test child, which
		// never received claim/mount. It is a test failure, not normal shutdown.
		t.Error("authenticated shutdown did not join empty owner; canceling original test child")
		cancelChild()
		select {
		case <-waited:
			joined = true
		case <-time.After(3 * time.Second):
			t.Error("original empty owner child still not joined")
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-waited:
			joined = true
			t.Fatalf("owner exited before readiness: %v %s", err, output.String())
		default:
		}
		if receipt, err := call("inspect", []string{"", ""}); err == nil {
			selected = receipt
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if selected.Instance == "" || selected.Build != version.Commit || selected.IsolationRoot != root || selected.ConfigDigest != "" {
		t.Fatalf("private empty owner identity unavailable: %+v", selected)
	}
	if _, err := call("shutdown", []string{"wrong-instance", shutdownID, ""}); err == nil {
		t.Fatal("foreign owner identity authorized shutdown")
	}
	if receipt, err := call("inspect", []string{selected.Instance, ""}); err != nil || receipt.Instance != selected.Instance {
		t.Fatalf("rejected shutdown affected original owner: %+v %v", receipt, err)
	}
	if receipt, err := call("shutdown", []string{selected.Instance, shutdownID, selected.ConfigDigest}); err != nil || receipt.Operation != shutdownID {
		t.Fatalf("authenticated owner shutdown not acknowledged: %+v %v", receipt, err)
	}
	select {
	case err := <-waited:
		joined = true
		if err != nil {
			t.Fatalf("original owner exit failed: %v %s", err, output.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acknowledged shutdown did not exit original owner")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("original owner socket remains after joined exit: %v", err)
	}
}
