package mounttable

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestSharedObserverGateNamesOriginalChildAndJoinsBeforeReuse(t *testing.T) {
	helper, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_MOUNT_SNAPSHOT_TEST_CHILD", "valid")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := output(ctx, queryGate, func() *exec.Cmd {
			cmd := exec.Command(helper)
			cmd.Env = append(os.Environ(), "JM_MOUNT_SNAPSHOT_TEST_CHILD=hang")
			return cmd
		})
		first <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		owner := activeObservation()
		if strings.Contains(owner, "kind=legacy mount") && !strings.Contains(owner, "pid=0 ") {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("original child did not launch: %s", owner)
		}
		time.Sleep(time.Millisecond)
	}
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = SnapshotFromHelper(blockedCtx, helper)
	blockedCancel()
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "kind=legacy mount") ||
		!strings.Contains(err.Error(), "executable=\""+helper+"\"") {
		cancel()
		t.Fatalf("blocked snapshot lost original identity: %v", err)
	}
	cancel() // output kills only its own child; its Wait remains the gate owner.
	select {
	case err := <-first:
		if err == nil {
			t.Fatalf("original result = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("original observer did not return")
	}
	freshCtx, freshCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer freshCancel()
	snapshot, err := SnapshotFromHelper(freshCtx, helper)
	if err != nil || snapshot.BootID != "boot-session" {
		t.Fatalf("original Wait did not release gate: %+v %v", snapshot, err)
	}
}

func TestLateTimeoutCannotRelabelNewObservation(t *testing.T) {
	queryGate <- struct{}{}
	old := beginObservation(context.Background(), queryGate, "legacy mount", "/old")
	timeoutObservation(old, context.DeadlineExceeded)
	select {
	case queryGate <- struct{}{}:
		<-queryGate
		t.Fatal("timeout released original observer gate before Wait")
	default:
	}
	finishObservation(old, queryGate, nil, true)
	queryGate <- struct{}{}
	newOp := beginObservation(context.Background(), queryGate, "native snapshot", "/new")
	timeoutObservation(old, context.DeadlineExceeded)
	finishObservation(old, queryGate, nil, true) // stale Wait cannot release new owner
	observationState.Lock()
	active, timedOut := observationState.active, newOp.timedOut
	observationState.Unlock()
	finishObservation(newOp, queryGate, nil, true)
	if active != newOp || timedOut {
		t.Fatal("retired observer changed new owner")
	}
}
