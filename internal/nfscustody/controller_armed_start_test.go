package nfscustody

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// The executable is an owned local protocol peer. The other child only creates
// a marker after its gate opens; neither child invokes mount_nfs or mounts.
func armedStartFixture(t *testing.T, delayAck bool) (*Controller, string, string, string) {
	t.Helper()
	root := t.TempDir()
	marker, entered := filepath.Join(root, "mounted"), filepath.Join(root, "entered")
	release, armSeen := filepath.Join(root, "release"), filepath.Join(root, "arm-seen")
	c, _, spec := harmlessMountChild(t, fmt.Sprintf("touch %q; while [ ! -e %q ]; do sleep .01; done; touch %q", entered, release, marker))
	c.creator.Observe = func(context.Context) (mounttable.Snapshot, error) {
		rows := []mounttable.MountEntry{}
		if _, err := os.Stat(marker); err == nil {
			rows = append(rows, mounttable.MountEntry{Path: spec.MountPath, Source: spec.Source, Type: "nfs", FSID: [2]int32{31, 41}})
		}
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: rows}, nil
	}
	ackGate := filepath.Join(root, "ack-gate")
	ackWait := ""
	if delayAck {
		ackWait = fmt.Sprintf("while [ ! -e %q ]; do sleep .01; done", ackGate)
	}
	helper := filepath.Join(root, "owned-custodian.sh")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--nfs-custodian-capability" ]; then
  printf '%%s\n' '{"version":1,"id":0,"state":"available"}'
  exit 0
fi
[ "$1" = "--nfs-custodian" ] || exit 64
session="$2"; boot="$3"; operation="$4"; path="$5"; source="$6"
printf '{"version":1,"session":"%%s","id":0,"state":"root_admitted","boot":"%%s","operation":"%%s","path":"%%s","source":"%%s","fsid":[31,41]}\n' "$session" "$boot" "$operation" "$path" "$source"
IFS= read -r arm || exit 65
case "$arm" in
  *'"op":"arm"'*)
    printf '%%s\n' "$arm" > %q
    %s
    printf '{"version":1,"session":"%%s","id":1,"state":"armed","boot":"%%s","operation":"%%s","path":"%%s","source":"%%s","fsid":[31,41]}\n' "$session" "$boot" "$operation" "$path" "$source"
    IFS= read -r detach || exit 66
    detach_id=2; release_id=3
    ;;
  *'"op":"detach"'*)
    detach="$arm"; detach_id=1; release_id=2
    ;;
  *) exit 68 ;;
esac
rm -f %q
printf '{"version":1,"session":"%%s","id":%%s,"state":"detached","boot":"%%s","operation":"%%s","path":"%%s","source":"%%s"}\n' "$session" "$detach_id" "$boot" "$operation" "$path" "$source"
IFS= read -r releaseRequest || exit 67
printf '{"version":1,"session":"%%s","id":%%s,"state":"released","boot":"%%s","operation":"%%s","path":"%%s","source":"%%s"}\n' "$session" "$release_id" "$boot" "$operation" "$path" "$source"
`, armSeen, ackWait, marker)
	if err := os.WriteFile(helper, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	c.helper = helper
	return c, entered, release, armSeen
}

func waitOwnedFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("owned child did not reach %s", filepath.Base(path))
}

func TestControllerStartArmsOnlyAfterOriginalMountChildJoins(t *testing.T) {
	c, entered, release, armSeen := armedStartFixture(t, false)
	result := make(chan error, 1)
	go func() { result <- c.Start(context.Background()) }()
	joined := false
	defer func() {
		_ = os.WriteFile(release, nil, 0600)
		if !joined {
			select {
			case <-result:
			case <-time.After(10 * time.Second):
				t.Error("original Start did not join")
			}
		}
		if err := c.Stop(context.Background(), 2*time.Second); err != nil {
			t.Errorf("owned helper/operation not retired: %v", err)
		}
	}()
	waitOwnedFile(t, entered)
	if _, err := os.Stat(armSeen); err == nil || c.Active() {
		t.Fatal("arm or active state preceded original mount child join")
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		joined = true
		if err != nil {
			t.Fatalf("actual Start did not arm: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("actual Start did not join")
	}
	waitOwnedFile(t, armSeen)
	if !c.Active() || !exactArmReceipt(t, c, armSeen) {
		t.Fatal("actual Start did not send original arm and await acknowledgment")
	}
}

func exactArmReceipt(t *testing.T, c *Controller, path string) bool {
	t.Helper()
	var q request
	if err := json.Unmarshal(mustReadOwned(t, path), &q); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	finalized := c.finalized
	s := c.session
	c.mu.Unlock()
	return finalized != nil && s != nil && q.Version == protocolVersion && q.Session == s.session && q.ID == 1 && q.Op == "arm" && sameArmRecord(q.Record, finalized)
}

func mustReadOwned(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestControllerStartPendingArmStopReconcilesOriginalID(t *testing.T) {
	c, entered, release, armSeen := armedStartFixture(t, true)
	ackGate := filepath.Join(filepath.Dir(entered), "ack-gate")
	result := make(chan error, 1)
	go func() { result <- c.Start(context.Background()) }()
	joined := false
	defer func() {
		_ = os.WriteFile(release, nil, 0600)
		_ = os.WriteFile(ackGate, nil, 0600)
		if !joined {
			select {
			case <-result:
			case <-time.After(10 * time.Second):
				t.Error("pending original Start not joined")
			}
		}
		if err := c.Stop(context.Background(), 2*time.Second); err != nil {
			t.Errorf("pending arm original not retired: %v", err)
		}
	}()
	waitOwnedFile(t, entered)
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	waitOwnedFile(t, armSeen)
	if c.Active() {
		t.Fatal("unacknowledged arm reported active")
	}
	select {
	case err := <-result:
		joined = true
		if err == nil {
			t.Fatal("Start accepted an unacknowledged arm")
		}
	case <-time.After(7 * time.Second):
		t.Fatal("original Start did not report unsettled arm")
	}
	if err := c.Stop(context.Background(), 50*time.Millisecond); err == nil || c.Retired() {
		t.Fatal("Stop detached or retired while the original arm acknowledgment was withheld")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(entered), "mounted")); err != nil {
		t.Fatal("pending arm stop lost the original mounted row")
	}
	if err := os.WriteFile(ackGate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(context.Background(), 2*time.Second); err != nil {
		t.Fatalf("Stop did not reconcile original arm: %v", err)
	}
	if c.Active() || !c.Retired() {
		t.Fatal("settled original was not retired")
	}
	b := mustReadOwned(t, armSeen)
	if strings.Count(string(b), `"op":"arm"`) != 1 || !exactArmReceipt(t, c, armSeen) {
		t.Fatal("pending arm used a replacement command or ID")
	}
}

func TestControllerStartFailedOriginalCommandNeverArms(t *testing.T) {
	c, _, _, armSeen := armedStartFixture(t, false)
	defer func() {
		if err := c.Stop(context.Background(), 2*time.Second); err != nil {
			t.Errorf("failed original not retired: %v", err)
		}
	}()
	failedChild := filepath.Join(filepath.Dir(armSeen), "failed-mount.sh")
	if err := os.WriteFile(failedChild, []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c.mountExecutable = failedChild
	if err := c.Start(context.Background()); err == nil {
		t.Fatal("failed original command accepted")
	}
	if _, err := os.Stat(armSeen); err == nil || c.Active() {
		t.Fatal("failed original command armed")
	}
}
