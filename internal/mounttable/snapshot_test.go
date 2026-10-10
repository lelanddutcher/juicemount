package mounttable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validSnapshotJSON = `{"version":1,"boot_id":"boot-session","entries":[{"path":"/Volumes/JuiceMount","source":"127.0.0.1:/jm-0123456789abcdef0123456789abcdef","type":"nfs","fsid":[12,-34]}]}`

// The test binary itself is the private owned child. SnapshotFromHelper must
// invoke it without arguments; this TestMain mode avoids host observation.
func TestMain(m *testing.M) {
	switch os.Getenv("JM_MOUNT_SNAPSHOT_TEST_CHILD") {
	case "valid":
		fmt.Fprint(os.Stdout, validSnapshotJSON)
		os.Exit(0)
	case "malformed":
		fmt.Fprint(os.Stdout, `{"version":1`)
		os.Exit(0)
	case "trailing":
		fmt.Fprint(os.Stdout, validSnapshotJSON+` {}`)
		os.Exit(0)
	case "oversized":
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), maxSnapshotBytes+1))
		os.Exit(0)
	case "hang":
		time.Sleep(10 * time.Second)
		os.Exit(0)
	case "failure":
		fmt.Fprintln(os.Stderr, "child observation failed")
		os.Exit(7)
	}
	os.Exit(m.Run())
}

func TestParseSnapshotStrictAndComplete(t *testing.T) {
	snapshot, err := parseSnapshot([]byte(validSnapshotJSON + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 1 || snapshot.BootID != "boot-session" || len(snapshot.Entries) != 1 ||
		snapshot.Entries[0].FSID != [2]int32{12, -34} {
		t.Fatalf("decoded snapshot = %+v", snapshot)
	}
	for name, raw := range map[string]string{
		"empty": "", "truncated": `{"version":1`, "trailing": validSnapshotJSON + ` {}`,
		"unknown-field":    `{"version":1,"boot_id":"b","entries":[],"extra":1}`,
		"duplicate-header": `{"version":1,"version":2,"boot_id":"b","entries":[]}`,
		"duplicate-entry":  `{"version":1,"boot_id":"b","entries":[{"path":"/p","path":"/other","source":"s","type":"nfs","fsid":[1,2]}]}`,
		"wrong-version":    `{"version":2,"boot_id":"b","entries":[]}`,
		"missing-version":  `{"boot_id":"b","entries":[]}`,
		"empty-boot":       `{"version":1,"boot_id":" ","entries":[]}`,
		"missing-entries":  `{"version":1,"boot_id":"b"}`,
		"null-entries":     `{"version":1,"boot_id":"b","entries":null}`,
		"missing-path":     `{"version":1,"boot_id":"b","entries":[{"source":"s","type":"nfs","fsid":[1,2]}]}`,
		"missing-source":   `{"version":1,"boot_id":"b","entries":[{"path":"/p","type":"nfs","fsid":[1,2]}]}`,
		"missing-type":     `{"version":1,"boot_id":"b","entries":[{"path":"/p","source":"s","fsid":[1,2]}]}`,
		"short-fsid":       `{"version":1,"boot_id":"b","entries":[{"path":"/p","source":"s","type":"nfs","fsid":[1]}]}`,
		"long-fsid":        `{"version":1,"boot_id":"b","entries":[{"path":"/p","source":"s","type":"nfs","fsid":[1,2,3]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseSnapshot([]byte(raw)); err == nil {
				t.Fatal("accepted incomplete or malformed observation")
			}
		})
	}
	if _, err := parseSnapshot(bytes.Repeat([]byte("x"), maxSnapshotBytes+1)); err == nil {
		t.Fatal("accepted oversized observation")
	}
}

func TestSnapshotFromHelperValidatesPrivateChild(t *testing.T) {
	helperPath, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"valid", "malformed", "trailing", "oversized", "failure"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("JM_MOUNT_SNAPSHOT_TEST_CHILD", mode)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			snapshot, err := SnapshotFromHelper(ctx, helperPath)
			if mode == "valid" {
				if err != nil || len(snapshot.Entries) != 1 || snapshot.Entries[0].FSID != [2]int32{12, -34} {
					t.Fatalf("valid helper: snapshot=%+v err=%v", snapshot, err)
				}
			} else if err == nil {
				t.Fatalf("%s helper produced accepted observation", mode)
			}
		})
	}
	t.Setenv("JM_MOUNT_SNAPSHOT_TEST_CHILD", "valid")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := SnapshotFromHelper(ctx, filepath.Join(t.TempDir(), "missing-helper")); err == nil {
		t.Fatal("missing helper produced absence")
	}
	if _, err := SnapshotFromHelper(ctx, "relative-helper"); err == nil {
		t.Fatal("relative helper was accepted")
	}
}

func TestSnapshotFromHelperTimeoutAndGateRetirement(t *testing.T) {
	helperPath, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// An occupied gate must time out without starting another observer.
	snapshotHelperGate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = SnapshotFromHelper(ctx, helperPath)
	cancel()
	<-snapshotHelperGate
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "prior helper") {
		t.Fatalf("occupied gate error = %v", err)
	}

	t.Setenv("JM_MOUNT_SNAPSHOT_TEST_CHILD", "hang")
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err = SnapshotFromHelper(ctx, helperPath)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hung helper error = %v", err)
	}
	// Once the killed child has actually been reaped, a fresh complete helper
	// may acquire the gate. Until then the second call waits on that same gate.
	t.Setenv("JM_MOUNT_SNAPSHOT_TEST_CHILD", "valid")
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	snapshot, err := SnapshotFromHelper(ctx, helperPath)
	if err != nil || snapshot.BootID != "boot-session" {
		t.Fatalf("gate did not retire after child reap: snapshot=%+v err=%v", snapshot, err)
	}
}
