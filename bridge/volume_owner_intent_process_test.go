//go:build !serviceclient

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// Exercises the production command configuration and actual owner main, with
// no claim, Link, JuiceFS child, or mount. It does not simulate engine death.
func TestVolumeOwnerSelectedProcessReceivesOriginalBootstrap(t *testing.T) {
	t.Run("acknowledged", func(t *testing.T) { testSelectedOwnerTerminalProcess(t, false) })
	t.Run("lost-reply", func(t *testing.T) { testSelectedOwnerTerminalProcess(t, true) })
}

func testSelectedOwnerTerminalProcess(t *testing.T, dropReply bool) {
	root, err := os.MkdirTemp(os.TempDir(), "voi-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	if err := os.Mkdir(filepath.Join(root, "tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, appisolation.Marker), []byte("JuiceMount isolated state v1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	scope := appisolation.Scope{Root: root}
	intent, created, err := createOwnerIntent(scope, ServerConfig{RedisURL: "redis://never-contacted.invalid:6379"})
	if err != nil || !created {
		t.Fatalf("select intent: %v", err)
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd, log, err := selectedOwnerCommand(scope, executable, read)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	// Only the test entry selection changes. The production descriptor,
	// process group, and independent log configuration stay intact.
	cmd.Args = []string{executable, "-test.run=^TestVolumeOwnerProcessFixture$"}
	env := make([]string, 0, len(cmd.Env)+5)
	for _, value := range cmd.Env {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "TMPDIR", "JM_APP_ISOLATION", "JM_APP_ISOLATION_ROOT", "JUICEMOUNT_OWNER_PROCESS_FIXTURE":
			continue
		}
		env = append(env, value)
	}
	cmd.Env = append(env, "TMPDIR="+filepath.Join(root, "tmp"), "JM_APP_ISOLATION=1", "JM_APP_ISOLATION_ROOT="+root, "JUICEMOUNT_OWNER_PROCESS_FIXTURE=1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = log.Close() // the child's output must not depend on this descriptor
	_ = read.Close()
	result := &ownerChildResult{done: make(chan struct{})}
	go func() { result.err = cmd.Wait(); close(result.done) }()
	path := filepath.Join(root, "o.sock")
	shutdownID := ownerShutdownID(intent)
	call := func(method string, values []string) (volumeOwnerReceipt, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := servicecontrol.Call(ctx, path, servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: method, Strings: values})
		if err != nil {
			return volumeOwnerReceipt{}, err
		}
		if response.Error != "" {
			return volumeOwnerReceipt{}, fmt.Errorf("owner rejected request: %s", response.Error)
		}
		var receipt volumeOwnerReceipt
		err = json.Unmarshal([]byte(response.Text), &receipt)
		return receipt, err
	}
	joined := false
	defer func() {
		_ = write.Close() // release bootstrap on every early assertion path
		if joined {
			return
		}
		_, _ = call("shutdown", []string{intent.Instance, shutdownID, intent.ConfigDigest})
		select {
		case <-result.done:
			return
		case <-time.After(3 * time.Second):
		}
		// This exact child has received no resource-creating request. Forced
		// cleanup is a test failure, never evidence of supported shutdown.
		t.Error("original empty selected owner did not settle; terminating only that test child")
		_ = cmd.Process.Kill()
		select {
		case <-result.done:
		case <-time.After(3 * time.Second):
			t.Error("original selected owner Wait not joined")
		}
	}()
	if _, err := write.WriteString(intent.Instance); err != nil {
		t.Fatal(err)
	}
	if err := write.Close(); err != nil {
		t.Fatal(err)
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil || pgid != cmd.Process.Pid || pgid == syscall.Getpgrp() {
		t.Fatalf("owner shares serving process group: pgid=%d pid=%d err=%v", pgid, cmd.Process.Pid, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var got volumeOwnerReceipt
	for time.Now().Before(deadline) {
		select {
		case <-result.done:
			joined = true
			body, _ := os.ReadFile(filepath.Join(root, "volume-owner.log"))
			t.Fatalf("original selected child exited before ready: %v %s", result.err, body)
		default:
		}
		got, err = call("inspect", []string{intent.Instance, intent.ConfigDigest})
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || got.Instance != intent.Instance || got.ConfigDigest != intent.ConfigDigest || got.Build != intent.Build || got.IsolationRoot != root {
		t.Fatalf("original inherited selection not available: %+v %v", got, err)
	}
	if dropReply {
		conn, err := net.DialTimeout("unix", path, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		payload, err := json.Marshal(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
			Build: version.Commit, Method: "shutdown", Strings: []string{intent.Instance, shutdownID, intent.ConfigDigest}})
		if err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		frame := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
		copy(frame[4:], payload)
		n, err := conn.Write(frame)
		_ = conn.Close() // deliberately discard the admitted operation's response
		if err != nil || n != len(frame) {
			t.Fatalf("terminal request not fully sent: %d/%d %v", n, len(frame), err)
		}
	} else {
		if _, err := call("shutdown", []string{intent.Instance, shutdownID, intent.ConfigDigest}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-result.done:
		joined = true
		if result.err != nil {
			t.Fatalf("supported selected owner exit: %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original selected child did not exit")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("selected owner socket retained: %v", err)
	}
	retained, err := readOwnerIntent(scope)
	if err != nil || retained != intent {
		t.Fatalf("shutdown changed selected intent: %v", err)
	}
	if err := confirmOwnerRetired(intent); err != nil {
		t.Fatalf("joined original did not publish terminal authority: %v", err)
	}
	next, created, err := createOwnerIntent(scope, ServerConfig{RedisURL: "redis://never-contacted.invalid:6379"})
	if err != nil || !created || next.Instance == intent.Instance {
		t.Fatalf("terminal process did not admit successor: created=%v err=%v", created, err)
	}
	if _, err := readOwnerRetirement(scope, intent); err != nil {
		t.Fatalf("successor lost predecessor proof: %v", err)
	}
}
