package nfscustody

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// These tests enter the native target admission path, whose ACL and descriptor
// custody implementations exist only on macOS. Pure gate/protocol tests remain
// cross-platform in the same files.
func requireDarwinNativeTarget(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("native NFS target admission requires macOS")
	}
}

func noMountController(t *testing.T) (*Controller, mountbroker.Creator, mountbroker.Spec) {
	t.Helper()
	root := t.TempDir()
	journal := filepath.Join(root, "journal")
	if err := os.Mkdir(journal, 0700); err != nil {
		t.Fatal(err)
	}
	spec := mountbroker.Spec{MountPath: filepath.Join(root, "volume"), Source: "127.0.0.1:/jm-0123456789abcdef0123456789abcdef", Endpoint: "127.0.0.1:12049", ExportPath: "/jm-0123456789abcdef0123456789abcdef", MetadataPath: filepath.Join(root, "meta.db")}
	creator := mountbroker.Creator{Directory: journal, Observe: func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{}}, nil
	}}
	c, err := New(creator, spec, "hard,vers=3,tcp")
	if err != nil {
		t.Fatal(err)
	}
	return c, creator, spec
}

// The substituted executable is an owned shell script. Production still
// constructs the same argv and retains the original exec.Cmd/Wait; no kernel
// mount, NFS endpoint, or administrator prompt is involved.
func harmlessMountChild(t *testing.T, body string) (*Controller, mountbroker.Creator, mountbroker.Spec) {
	t.Helper()
	c, creator, spec := noMountController(t)
	root, err := filepath.EvalSymlinks(filepath.Dir(spec.MountPath))
	if err != nil {
		t.Fatal(err)
	}
	spec.MountPath = filepath.Join(root, "volume")
	c.spec = spec
	c.options = "bg,retrycnt=10000,hard,vers=3,tcp"
	if err := os.Mkdir(spec.MountPath, 0700); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(root, "owned-mount-child.sh")
	if err := os.WriteFile(child, []byte("#!/bin/sh\n"+body+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c.mountExecutable = child
	return c, creator, spec
}

func TestNFSForegroundMountArgsOverrideInheritedBackground(t *testing.T) {
	c, _, spec := harmlessMountChild(t, "exit 7")
	want := []string{"-o", "bg,retrycnt=10000,hard,vers=3,tcp", "-o", "nobg,retrycnt=1", spec.Source, spec.MountPath}
	if !reflect.DeepEqual(c.mountArgs(), want) {
		t.Fatalf("unsafe mount argv ordering: %q", c.mountArgs())
	}
	if !foregroundMountArgs(c.mountArgs()) || foregroundMountArgs(append(c.mountArgs(), "-b")) || foregroundMountArgs([]string{"-o", c.options, "-o", "bg", spec.Source, spec.MountPath}) {
		t.Fatal("foreground provenance accepted a late background override")
	}
	if c.mountExecutable != "/sbin/mount_nfs" && filepath.Base(c.mountExecutable) != "owned-mount-child.sh" {
		t.Fatal("unexpected executable")
	}
}

func TestNFSNormalFailedForegroundChildRetiresOwnOperation(t *testing.T) {
	requireDarwinNativeTarget(t)
	c, creator, spec := harmlessMountChild(t, "exit 7")
	done := c.createOperation(context.Background(), c.startCommand)
	<-done
	if c.command == nil || c.command.ProcessState == nil || !c.command.ProcessState.Exited() || c.command.ProcessState.ExitCode() != 7 || !c.commandForeground {
		t.Fatal("original foreground child did not provide normal failure proof")
	}
	if !reflect.DeepEqual(c.command.Args[1:], c.mountArgs()) {
		t.Fatal("actual original child received a different argv")
	}
	if err := c.Stop(context.Background(), 2*time.Second); err != nil {
		t.Fatalf("failed child did not retire: %v", err)
	}
	if !c.Retired() {
		t.Fatal("failed original operation remained active")
	}
	called := false
	next := creator.CreateBound(context.Background(), spec, func(mountbroker.Record) error {
		called = true
		return errors.New("next child deliberately not started")
	})
	if !called || !next.Attempted {
		t.Fatalf("durable successor still blocked: %+v", next)
	}
}

func TestNFSForegroundFailureProofRefusesZeroSignalAndPending(t *testing.T) {
	requireDarwinNativeTarget(t)
	for _, tc := range []struct {
		name, body string
		pending    bool
	}{
		{"zero-exit", "exit 0", false},
		{"signaled", "kill -TERM $$", false},
		{"pending", "read line; exit 7", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, _ := harmlessMountChild(t, tc.body)
			var inputRead, inputWrite *os.File
			if tc.pending {
				var err error
				inputRead, inputWrite, err = os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				c.mountInput = inputRead
			}
			done := c.createOperation(context.Background(), c.startCommand)
			t.Cleanup(func() {
				if inputWrite != nil {
					_ = inputWrite.Close()
				}
				<-done // original Wait and creation job join before TempDir cleanup
				if inputRead != nil {
					_ = inputRead.Close()
				}
			})
			if tc.pending {
				startedDeadline := time.After(time.Second)
				for {
					c.mu.Lock()
					started := c.commandStarted
					c.mu.Unlock()
					if started {
						break
					}
					select {
					case <-done:
						t.Fatal("test child never started")
					case <-startedDeadline:
						t.Fatal("test child startup deadline")
					case <-time.After(time.Millisecond):
					}
				}
				if err := c.Stop(context.Background(), 20*time.Millisecond); err == nil || c.Retired() {
					t.Fatalf("live original child retired: %v", err)
				}
				if err := inputWrite.Close(); err != nil {
					t.Fatal(err)
				}
				<-done
				if err := c.Stop(context.Background(), 2*time.Second); err != nil {
					t.Fatalf("joined failed child did not settle: %v", err)
				}
				return
			}
			<-done
			if err := c.Stop(context.Background(), time.Second); err == nil || c.Retired() {
				t.Fatalf("unqualified original exit retired: %v", err)
			}
		})
	}
}

func TestNFSFailedForegroundChildRefusesUnreliableAbsence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		snapshot func(mountbroker.Spec) mounttable.Snapshot
	}{
		{"boot-changed", func(mountbroker.Spec) mounttable.Snapshot {
			return mounttable.Snapshot{Version: 1, BootID: "other", Entries: []mounttable.MountEntry{}}
		}},
		{"incomplete", func(mountbroker.Spec) mounttable.Snapshot {
			return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: nil}
		}},
		{"source-moved", func(s mountbroker.Spec) mounttable.Snapshot {
			return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{{Path: "/foreign/volume", Source: s.Source, Type: "nfs", FSID: [2]int32{8, 9}}}}
		}},
		{"mounted-descendant", func(s mountbroker.Spec) mounttable.Snapshot {
			return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{{Path: s.MountPath + "/other", Source: "other", Type: "apfs", FSID: [2]int32{8, 9}}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, spec := harmlessMountChild(t, "exit 7")
			done := c.createOperation(context.Background(), c.startCommand)
			<-done
			original := c.creator.Observe
			c.creator.Observe = func(context.Context) (mounttable.Snapshot, error) { return tc.snapshot(spec), nil }
			if err := c.Stop(context.Background(), time.Second); err == nil || c.Retired() {
				t.Fatalf("unreliable absence retired original operation: %v", err)
			}
			c.creator.Observe = original
			if err := c.Stop(context.Background(), time.Second); err != nil {
				t.Fatalf("same operation did not reconcile after valid absence: %v", err)
			}
		})
	}
}

func TestNFSControllerNeverAdoptsPriorJournalOperation(t *testing.T) {
	c, creator, spec := noMountController(t)
	prior := creator.CreateBound(context.Background(), spec, func(mountbroker.Record) error { return errors.New("no mount child born") })
	if prior.Record == nil {
		t.Fatalf("prior record missing: %+v", prior)
	}
	done := c.createOperation(context.Background(), func(mountbroker.Record) error { t.Fatal("prior operation replayed"); return nil })
	<-done
	if c.Record() != nil {
		t.Fatal("prior record adopted as live controller authority")
	}
	if err := c.Stop(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
	if !c.Retired() {
		t.Fatal("no-birth controller not settled")
	}
	still := creator.CreateBound(context.Background(), spec, func(mountbroker.Record) error { t.Fatal("prior operation unexpectedly retired"); return nil })
	if still.Record == nil || still.Record.OperationID != prior.Record.OperationID || still.Attempted {
		t.Fatalf("prior journal changed: %+v", still)
	}
}

func TestNFSControllerRetiresOwnPrecommandIntent(t *testing.T) {
	c, creator, spec := noMountController(t)
	done := c.createOperation(context.Background(), func(rec mountbroker.Record) error {
		c.mu.Lock()
		c.record = &rec
		c.mu.Unlock()
		return errors.New("precommand refusal")
	})
	<-done
	if c.Record() == nil {
		t.Fatal("own operation not published")
	}
	if err := c.Stop(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
	if !c.Retired() {
		t.Fatal("own precommand operation not retired")
	}
	called := false
	next := creator.CreateBound(context.Background(), spec, func(mountbroker.Record) error {
		called = true
		return errors.New("next operation stopped before mount")
	})
	if !called || !next.Attempted {
		t.Fatalf("durable retirement did not admit successor: %+v", next)
	}
}

func TestNFSControllerStopRereadsAfterOriginalJobJoins(t *testing.T) {
	c, creator, spec := noMountController(t)
	entered, release, observed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.stopObservedJob = func() { close(observed) }
	job := c.createOperation(context.Background(), func(rec mountbroker.Record) error {
		close(entered)
		<-release
		c.mu.Lock()
		c.record = &rec
		c.mu.Unlock()
		return errors.New("precommand refusal")
	})
	<-entered
	stopDone := make(chan error, 1)
	go func() { stopDone <- c.Stop(context.Background(), 2*time.Second) }()
	<-observed
	close(release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not join original creation job")
	}
	<-job
	if !c.Retired() {
		t.Fatal("Stop lost post-join own record")
	}
	called := false
	next := creator.CreateBound(context.Background(), spec, func(mountbroker.Record) error { called = true; return errors.New("next no-birth operation") })
	if !called || !next.Attempted {
		t.Fatalf("Stop did not durably retire late-published record: %+v", next)
	}
}

func TestNFSControllerPendingJobAndCapabilityRetained(t *testing.T) {
	c, _, _ := noMountController(t)
	c.mu.Lock()
	c.jobDone = make(chan struct{})
	c.mu.Unlock()
	if err := c.Stop(context.Background(), time.Millisecond); err == nil || c.Retired() {
		t.Fatal("live creation job discarded")
	}
	c.mu.Lock()
	close(c.jobDone)
	c.capability = &CapabilityProbe{done: make(chan struct{})}
	c.mu.Unlock()
	if err := c.Stop(context.Background(), time.Second); err == nil || c.Retired() {
		t.Fatal("live capability process discarded")
	}
	close(c.capability.done)
	if err := c.Stop(context.Background(), time.Second); err != nil || !c.Retired() {
		t.Fatalf("terminal no-birth cleanup: %v", err)
	}
}

func TestNFSControllerReconcilesLateReadyOnSameHelper(t *testing.T) {
	c, _, spec := noMountController(t)
	rec := mountbroker.Record{Version: 1, OperationID: "abcdef0123456789abcdef0123456789", BootID: "boot", Spec: spec, State: mountbroker.InFlight}
	c.creator.Observe = func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{{Path: spec.MountPath, Source: spec.Source, Type: "nfs", FSID: [2]int32{31, 41}}}}, nil
	}
	s := &Session{events: make(chan received, 1), waitDone: make(chan struct{}), gate: make(chan struct{}, 1), session: "0123456789abcdef0123456789abcdef", boot: "boot", operation: rec.OperationID, path: spec.MountPath, source: spec.Source}
	s.gate <- struct{}{}
	c.mu.Lock()
	c.record = &rec
	c.session = s
	c.started = true
	c.mu.Unlock()
	s.events <- received{event: event{Version: protocolVersion, Session: s.session, ID: 0, State: "root_admitted", Boot: "boot", Operation: rec.OperationID, Path: spec.MountPath, Source: spec.Source, FSID: [2]int32{31, 41}}}
	if err := c.admit(time.Second); err != nil {
		t.Fatal(err)
	}
	if c.session != s || c.Active() || c.Record().Mount == nil {
		t.Fatal("late admission failed to bind original helper and row")
	}
	s.mu.Lock()
	s.armed = true // Active becomes true only after the original arm acknowledgment.
	s.mu.Unlock()
	if !c.Active() {
		t.Fatal("acknowledged original helper did not become active")
	}
	s.mu.Lock()
	s.readerErr = io.ErrUnexpectedEOF
	s.mu.Unlock()
	if c.Active() {
		t.Fatal("helper protocol failure still reported active")
	}
	close(s.waitDone)
	if c.Active() {
		t.Fatal("exited helper still reported active")
	}
}

func TestNFSControllerRefusesForeignSnapshotBeforeTargetTraversal(t *testing.T) {
	c, _, spec := noMountController(t)
	// No directory exists at spec.MountPath. A target traversal would produce
	// a provisioning error; the complete foreign snapshot must win first.
	c.creator.Observe = func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{{Path: spec.MountPath, Source: "foreign", Type: "nfs", FSID: [2]int32{8, 9}}}}, nil
	}
	helper := filepath.Join(t.TempDir(), "capability")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\nprintf '%s\\n' '{\"version\":1,\"id\":0,\"state\":\"available\"}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c.helper = helper
	err := c.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "occupied") || c.Record() != nil {
		t.Fatalf("foreign snapshot not refused before target traversal: %v", err)
	}
	if err := c.Stop(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestNFSAdmissionFramePreflightCoversWorstEvent(t *testing.T) {
	const operation = "0123456789abcdef0123456789abcdef"
	base := "/private/"
	maxAllowed := -1
	for n := 0; n < maxFrame; n++ {
		if validateEventCapacity("boot", operation, base+strings.Repeat("x", n), "127.0.0.1:/jm-"+operation) == nil {
			maxAllowed = n
		} else {
			break
		}
	}
	// Independently encode the largest actual helper response at every
	// admitted boundary; an undersized preflight must fail this oracle.
	for _, state := range []string{"root_admitted", "admission_refused", "unsupported_os", "already_detached", "retained_retry", "command_refused", "close_failed", "released"} {
		e := event{Version: 1, Session: strings.Repeat("0", 32), ID: ^uint64(0), State: state, Boot: "boot", Operation: operation, Path: base + strings.Repeat("x", maxAllowed), Source: "127.0.0.1:/jm-" + operation, FSID: [2]int32{-2147483648, -2147483648}}
		if err := writeFrame(io.Discard, e); err != nil {
			t.Fatalf("preflight admitted an oversized actual %s event", state)
		}
	}
	if maxAllowed < 0 {
		t.Fatal("no admitted frame size")
	}
	if err := validateEventCapacity("boot", operation, base+strings.Repeat("x", maxAllowed+1), "127.0.0.1:/jm-"+operation); err == nil {
		t.Fatal("post-birth oversized ready event possible")
	}
}

func TestNFSControllerRefusesRemoteAncestorBeforeTraversal(t *testing.T) {
	c, _, spec := noMountController(t)
	c.creator.Observe = func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{{Path: filepath.Dir(spec.MountPath), Type: "smbfs", Source: "remote", FSID: [2]int32{8, 9}}}}, nil
	}
	err := c.startCommand(mountbroker.Record{Version: 1, BootID: "boot", OperationID: "0123456789abcdef0123456789abcdef", Spec: spec})
	if err == nil || !strings.Contains(err.Error(), "nonlocal containing mount") {
		t.Fatalf("remote ancestor traversed: %v", err)
	}
	if c.command != nil || c.session != nil {
		t.Fatal("remote ancestor launched effects")
	}
}

func TestNFSTargetRevalidationReadsNewDirectoryStream(t *testing.T) {
	requireDarwinNativeTarget(t)
	root := t.TempDir()
	path := filepath.Join(root, "leaf")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	parent, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		unix.Close(parent)
		t.Fatal(err)
	}
	g := &targetGuard{parent: parent, leaf: leaf, name: "leaf", parentPath: root}
	defer g.Close()
	if err := unix.Fstat(parent, &g.parentStat); err != nil {
		t.Fatal(err)
	}
	if err := unix.Fstat(leaf, &g.stat); err != nil {
		t.Fatal(err)
	}
	if err := g.Revalidate(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "new-file"), []byte("owned fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := g.Revalidate(); err == nil {
		t.Fatal("new directory entry hidden by stale enumeration offset")
	}
}
