//go:build !serviceclient

package main

import (
	"context"
	"errors"
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

type abandonedOwnerChild struct {
	pid    int
	result *ownerChildResult
}

// launchOwnerFixture starts the real owner main through the production command
// (process group, bootstrap descriptor, log file) for one selected intent, and
// waits until it answers inspect on its control socket.
func launchOwnerFixture(t *testing.T, scope appisolation.Scope, intent volumeOwnerIntent) abandonedOwnerChild {
	t.Helper()
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
	cmd.Args = []string{executable, "-test.run=^TestVolumeOwnerProcessFixture$"}
	env := make([]string, 0, len(cmd.Env)+4)
	for _, value := range cmd.Env {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "TMPDIR", "JM_APP_ISOLATION", "JM_APP_ISOLATION_ROOT", "JUICEMOUNT_OWNER_PROCESS_FIXTURE":
			continue
		}
		env = append(env, value)
	}
	cmd.Env = append(env, "TMPDIR="+filepath.Join(scope.Root, "tmp"), "JM_APP_ISOLATION=1",
		"JM_APP_ISOLATION_ROOT="+scope.Root, "JUICEMOUNT_OWNER_PROCESS_FIXTURE=1")
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	_ = log.Close()
	_ = read.Close()
	result := &ownerChildResult{done: make(chan struct{})}
	go func() { result.err = cmd.Wait(); close(result.done) }()
	child := abandonedOwnerChild{pid: cmd.Process.Pid, result: result}
	t.Cleanup(func() {
		select {
		case <-result.done:
		default:
			_ = syscall.Kill(-child.pid, syscall.SIGKILL)
			<-result.done
		}
	})
	if _, err := write.WriteString(intent.Instance); err != nil {
		t.Fatal(err)
	}
	_ = write.Close()
	socket := filepath.Join(scope.Root, "o.sock")
	deadline := time.Now().Add(5 * time.Second)
	for !ownerSocketAnswers(socket, intent) {
		select {
		case <-result.done:
			body, _ := os.ReadFile(filepath.Join(scope.Root, "volume-owner.log"))
			t.Fatalf("owner exited before ready: %v %s", result.err, body)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("owner never answered its control socket")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return child
}

func shutdownOwnerFixture(t *testing.T, scope appisolation.Scope, intent volumeOwnerIntent, child abandonedOwnerChild) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := servicecontrol.Call(ctx, filepath.Join(scope.Root, "o.sock"), servicecontrol.Request{
		Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "shutdown",
		Strings: []string{intent.Instance, ownerShutdownID(intent), intent.ConfigDigest}})
	if err != nil || resp.Error != "" {
		t.Fatalf("successor shutdown: %v %s", err, resp.Error)
	}
	select {
	case <-child.result.done:
	case <-time.After(5 * time.Second):
		t.Fatal("successor owner did not exit after shutdown")
	}
}

// Laptop run 2026-11-12, scenario 5-unclean: the link was cut, the graceful
// stop could not finish, and the processes were killed. Every later Start then
// failed with "selected owner unavailable; original intent retained, no
// replacement launch", online or offline, because the intent file outlived its
// owner with no retirement record. A user reaches the same state through power
// loss, a crash, or a reboot after a Stop that could not finish.
//
// This kills a real owner process with SIGKILL and checks the next launch.
func TestKilledOwnerIsReplacedOnNextLaunch(t *testing.T) {
	root := privateIsolationTestRoot(t, "vok-")
	scope := appisolation.Scope{Root: root}
	socket := filepath.Join(root, "o.sock")
	cfg := ServerConfig{RedisURL: "redis://never-contacted.invalid:6379"}

	original, created, err := createOwnerIntent(scope, cfg)
	if err != nil || !created {
		t.Fatalf("select original: %v", err)
	}
	child := launchOwnerFixture(t, scope, original)

	// While the original runs, it is never replaced.
	if ownerAbandonedAt(scope, socket, original) {
		t.Fatal("a live owner was reported abandoned")
	}
	servingOwnerSelection.Lock()
	_, err = replaceAbandonedOwner(scope, cfg, original, socket)
	servingOwnerSelection.Unlock()
	if !errors.Is(err, errOwnerStillRunning) {
		t.Fatalf("live owner replaced or wrong refusal: %v", err)
	}
	if got, err := readOwnerIntent(scope); err != nil || got != original {
		t.Fatalf("refusal changed the selected intent: %v", err)
	}

	// Kill it the way the lab driver (or a crash, or power loss) does.
	if err := syscall.Kill(-child.pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.result.done:
	case <-time.After(5 * time.Second):
		t.Fatal("killed owner did not exit")
	}

	// This is the state that held every later Start before the fix: the
	// intent survives, without retirement, and nothing answers its socket.
	if got, again, err := createOwnerIntent(scope, cfg); err != nil || again || got != original {
		t.Fatalf("precondition: retained intent not reused: %v", err)
	}
	if _, err := readOwnerRetirement(scope, original); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("precondition: killed owner published retirement: %v", err)
	}
	if ownerSocketAnswers(socket, original) {
		t.Fatal("precondition: killed owner still answers")
	}

	// Next launch: the dead original is proven gone and replaced.
	if !ownerAbandonedAt(scope, socket, original) {
		t.Fatal("killed owner not recognised as abandoned")
	}
	servingOwnerSelection.Lock()
	next, err := replaceAbandonedOwner(scope, cfg, original, socket)
	servingOwnerSelection.Unlock()
	if err != nil {
		t.Fatalf("killed owner held the next launch: %v", err)
	}
	if next.Instance == original.Instance || next.ClaimID == original.ClaimID {
		t.Fatal("successor reused the original identity")
	}
	if got, err := readOwnerIntent(scope); err != nil || got != next {
		t.Fatalf("successor intent not published: %v", err)
	}

	// The successor owner really starts over the killed one's leftovers
	// (stale socket file, lifetime lock file) and answers.
	successor := launchOwnerFixture(t, scope, next)
	if ownerAbandonedAt(scope, socket, next) {
		t.Fatal("live successor reported abandoned")
	}
	shutdownOwnerFixture(t, scope, next, successor)
	if err := confirmOwnerRetired(next); err != nil {
		t.Fatalf("successor did not retire normally: %v", err)
	}
}

// A dead original also used to block a Start with changed settings
// ("selected owner config changed; retained intent cannot be rebound").
func TestAbandonedOwnerSuccessorTakesNewConfig(t *testing.T) {
	root := privateIsolationTestRoot(t, "voc-")
	scope := appisolation.Scope{Root: root}
	socket := filepath.Join(root, "o.sock")
	original, _, err := createOwnerIntent(scope, ServerConfig{RedisURL: "redis://old.invalid:6379"})
	if err != nil {
		t.Fatal(err)
	}
	changed := ServerConfig{RedisURL: "redis://new.invalid:6379"}
	if _, _, err := createOwnerIntent(scope, changed); err == nil {
		t.Fatal("precondition: changed config rebound a retained intent")
	}
	servingOwnerSelection.Lock()
	next, err := replaceAbandonedOwner(scope, changed, original, socket)
	servingOwnerSelection.Unlock()
	if err != nil {
		t.Fatalf("never-started original held a changed Start: %v", err)
	}
	want, err := servingConfigDigest(changed)
	if err != nil || next.ConfigDigest != want {
		t.Fatalf("successor not bound to the new config: %v", err)
	}
}

// The lifetime lock is the proof of life. A holder (a live owner past
// admission) keeps the original selected and names the reason.
func TestHeldLifetimeKeepsOriginalAndSaysWhy(t *testing.T) {
	root := privateIsolationTestRoot(t, "voh-")
	scope := appisolation.Scope{Root: root}
	socket := filepath.Join(root, "o.sock")
	cfg := ServerConfig{RedisURL: "redis://never-contacted.invalid:6379"}
	original, _, err := createOwnerIntent(scope, cfg)
	if err != nil {
		t.Fatal(err)
	}
	l, err := acquireOwnerLifetime(scope)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	if ownerAbandonedAt(scope, socket, original) {
		t.Fatal("held lifetime reported abandoned")
	}
	servingOwnerSelection.Lock()
	_, err = replaceAbandonedOwner(scope, cfg, original, socket)
	servingOwnerSelection.Unlock()
	if !errors.Is(err, errOwnerStillRunning) || !strings.Contains(err.Error(), "still running") {
		t.Fatalf("held owner replaced or unclear: %v", err)
	}
	if got, err := readOwnerIntent(scope); err != nil || got != original {
		t.Fatal("held owner's intent changed")
	}
}
