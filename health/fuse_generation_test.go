package health

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func generationTestPath(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "fuse")
}

func emptyGenerationSnapshot(context.Context) (mounttable.Snapshot, error) {
	return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{}}, nil
}

func TestGenerationReservationNeverReusesRetiredPath(t *testing.T) {
	logical := generationTestPath(t)
	a, err := reserveFUSEGeneration(logical, "digest-a", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	first := a.Path
	if first == logical || !strings.HasPrefix(first, logical+".generations"+string(os.PathSeparator)) {
		t.Fatalf("physical path %q does not isolate logical %q", first, logical)
	}
	if filepath.Base(filepath.Dir(filepath.Dir(first))) != strings.Split(a.ID, "-")[0] {
		t.Fatalf("installation nonce missing from physical path: %q", first)
	}
	if err := a.markLaunching(); err != nil {
		t.Fatal(err)
	}
	if err := a.markChild(12345); err != nil {
		t.Fatal(err)
	}
	events, err := generationEvents(a.ledger)
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.State != "born" || last.ChildPID != 12345 || last.ParentPID != os.Getpid() {
		t.Fatalf("child identity not retained: %+v", last)
	}
	if err := a.retire(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(first); err != nil {
		t.Fatalf("retired mount tombstone removed: %v", err)
	}
	b, err := reserveFUSEGeneration(logical, "digest-a", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer b.close()
	if b.Path == first || b.ID == a.ID {
		t.Fatalf("generation reused: A=%s B=%s", first, b.Path)
	}
	if err := b.retire(); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationReopenRefusesUnresolvedIntentAndTornLedger(t *testing.T) {
	logical := generationTestPath(t)
	a, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	a.close() // model process death after durable reservation
	if _, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot); err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatalf("unresolved allocation admitted another child: %v", err)
	}
	ledger := filepath.Join(logical+".generations", "ledger.jsonl")
	f, err := os.OpenFile(ledger, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("{\"version\":1"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot); err == nil || !strings.Contains(err.Error(), "torn") {
		t.Fatalf("torn ledger admitted another child: %v", err)
	}
}

func TestGenerationReservationRefusesConcurrentOwnerAndSymlink(t *testing.T) {
	logical := generationTestPath(t)
	a, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot); err == nil {
		t.Fatal("second manager acquired live installation")
	}
	if err := a.retire(); err != nil {
		t.Fatal(err)
	}
	other := generationTestPath(t)
	if err := os.Symlink(logical+".generations", other+".generations"); err != nil {
		t.Fatal(err)
	}
	if _, err := reserveFUSEGeneration(other, "digest", emptyGenerationSnapshot); err == nil {
		t.Fatal("symlink generation root accepted")
	}
}

func TestGenerationReservationRefusesUncertainOrOccupiedNamespace(t *testing.T) {
	logical := generationTestPath(t)
	unknown := func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{}, errors.New("observer failed")
	}
	if _, err := reserveFUSEGeneration(logical, "digest", unknown); err == nil {
		t.Fatal("observer failure allocated")
	}
	if _, err := os.Lstat(logical + ".generations"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("observer failure touched namespace: %v", err)
	}
	occupied := func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{{Path: logical, Source: "foreign", Type: "macfuse"}}}, nil
	}
	if _, err := reserveFUSEGeneration(logical, "digest", occupied); err == nil {
		t.Fatal("occupied requested path allocated")
	}
	if err := os.Mkdir(logical+".generations", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(logical+".generations", "unknown"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot); err == nil {
		t.Fatal("pre-existing untracked entry accepted")
	}
}

func TestGenerationAnchorRejectsMovedRootAndLaunchDrift(t *testing.T) {
	logical := generationTestPath(t)
	a, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	oldRoot := logical + ".generations"
	if err := os.Rename(oldRoot, oldRoot+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := a.markLaunching(); err == nil {
		t.Fatal("launch accepted moved generation ancestry")
	}
	a.close()
	if _, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot); err == nil || !strings.Contains(err.Error(), "anchor") {
		t.Fatalf("moved root did not block reinstall allocation: %v", err)
	}
}

func TestGenerationFailedBirthCannotRetrySamePath(t *testing.T) {
	logical := generationTestPath(t)
	a, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.markLaunching(); err != nil {
		t.Fatal(err)
	}
	if !a.spent {
		t.Fatal("birth intent did not consume generation")
	}
	a.close() // crash or Start failure before the exact child is recorded
	if _, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot); err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatalf("failed birth reused generation: %v", err)
	}
}

func TestGenerationStopRetainsPresentMountAndLedger(t *testing.T) {
	logical := generationTestPath(t)
	reservation, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.close()
	fm := NewFUSEManager(FUSEConfig{MountPoint: reservation.Path})
	fm.generation = reservation
	fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{{Path: reservation.Path, Source: "JuiceFS:test", Type: "macfuse"}}}, nil
	}
	got := fm.Stop()
	if got.State != FUSEPresent || reservation.retired || reservation.ledger == nil {
		t.Fatalf("present mount released generation custody: state=%s retired=%t ledger=%v", got.State, reservation.retired, reservation.ledger)
	}
}

func TestGenerationInventoryAllowsRootAndRejectsOccupiedDescendant(t *testing.T) {
	logical := generationTestPath(t)
	observe := func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{
			{Path: "/", Source: "/dev/disk-root", Type: "apfs"},
			{Path: "/System/Volumes/Data", Source: "/dev/disk-data", Type: "apfs"},
		}}, nil
	}
	r, err := reserveFUSEGeneration(logical, "digest", observe)
	if err != nil {
		t.Fatalf("normal root inventory refused: %v", err)
	}
	if err := r.retire(); err != nil {
		t.Fatal(err)
	}
	occupied := func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{
			{Path: "/", Source: "/dev/disk-root", Type: "apfs"},
			{Path: filepath.Join(logical+".generations", "foreign"), Source: "foreign", Type: "macfuse"},
		}}, nil
	}
	if _, err := reserveFUSEGeneration(logical, "digest", occupied); err == nil || !strings.Contains(err.Error(), "occupied") {
		t.Fatalf("mounted generation descendant admitted: %v", err)
	}
}

func TestGenerationBornSkipsMountedLeafButChecksAncestors(t *testing.T) {
	logical := generationTestPath(t)
	r, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if err := r.markLaunching(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(r.Path, r.Path+".reserved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(r.Path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := r.markChild(12345); err != nil {
		t.Fatalf("post-spawn leaf replacement blocked birth: %v", err)
	}
	events, err := generationEvents(r.ledger)
	if err != nil {
		t.Fatal(err)
	}
	if got := events[len(events)-1]; got.State != "born" || got.ChildPID != 12345 {
		t.Fatalf("born record lost exact PID: %+v", got)
	}
}

func generationFailedChild(t *testing.T) *exec.Cmd {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-juicefs")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(path)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func TestGenerationChangedAncestorRetainsExactWaiter(t *testing.T) {
	logical := generationTestPath(t)
	r, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if err := r.markLaunching(); err != nil {
		t.Fatal(err)
	}
	cmd := generationFailedChild(t)
	fm := NewFUSEManager(FUSEConfig{MountPoint: r.Path})
	receipt := &MountReceipt{owner: fm, generation: r, process: cmd.Process}
	processExit := startFUSEChildWaiter(cmd, nil, receipt)
	fm.activeReceipt = receipt
	ancestor := filepath.Dir(r.Path)
	if err := os.Rename(ancestor, ancestor+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(ancestor, 0700); err != nil {
		t.Fatal(err)
	}
	if err := r.markChild(cmd.Process.Pid); err == nil {
		t.Fatal("changed generation ancestor admitted child")
	}
	if fm.activeReceipt != receipt || receipt.waitDone == nil {
		t.Fatal("failed birth lost exact child waiter")
	}
	select {
	case <-receipt.waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("exact child waiter did not settle")
	}
	if receipt.exitErr == nil || <-processExit == nil {
		t.Fatal("exact child exit error was lost")
	}
}

func TestGenerationJournalFailureAfterStartRetainsExitError(t *testing.T) {
	logical := generationTestPath(t)
	r, err := reserveFUSEGeneration(logical, "digest", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if err := r.markLaunching(); err != nil {
		t.Fatal(err)
	}
	cmd := generationFailedChild(t)
	fm := NewFUSEManager(FUSEConfig{MountPoint: r.Path})
	receipt := &MountReceipt{owner: fm, generation: r, process: cmd.Process}
	processExit := startFUSEChildWaiter(cmd, nil, receipt)
	fm.activeReceipt = receipt
	r.journalFailed = true
	if err := r.markChild(cmd.Process.Pid); err == nil {
		t.Fatal("failed born journal write accepted")
	}
	select {
	case <-receipt.waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("exact child waiter did not settle")
	}
	if fm.activeReceipt != receipt || receipt.exitErr == nil || <-processExit == nil {
		t.Fatal("journal failure discarded exact receipt or exit error")
	}
}
