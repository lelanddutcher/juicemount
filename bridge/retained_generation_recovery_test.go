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
	"testing"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/version"
	"golang.org/x/sys/unix"
)

func TestRetainedRecoveryRejectsArgumentsRootAndProductionBeforeHelper(t *testing.T) {
	old := newRetainedGenerationClient
	called := 0
	newRetainedGenerationClient = func() (health.RetainedGenerationHelper, func()) {
		called++
		return nil, func() {}
	}
	t.Cleanup(func() { newRetainedGenerationClient = old })
	valid := []string{retainedRecoveryFlag, "/unselected", "owner", "oldbuild",
		"fuse-0123456789abcdef0123456789abcdef-0000000000000001"}
	for _, tc := range []struct {
		name string
		args []string
		root bool
		code string
	}{
		{"arguments", []string{retainedRecoveryFlag}, false, "invalid_arguments"},
		{"root", valid, true, "root_not_allowed"},
		{"production", valid, false, "isolation_mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("JM_APP_ISOLATION", "")
			t.Setenv("JM_APP_ISOLATION_ROOT", "")
			var out bytes.Buffer
			exit := runRetainedGenerationRecovery(tc.args, &out, tc.root, false)
			var report retainedRecoveryReport
			if err := json.Unmarshal(out.Bytes(), &report); err != nil {
				t.Fatal(err)
			}
			if exit == 0 || report.ErrorCode != tc.code || report.Retired || called != 0 {
				t.Fatalf("early rejection failed: code=%d report=%+v helper calls=%d", exit, report, called)
			}
		})
	}
}

func TestRetainedRecoveryMissingExistingFencesNeverCreatesOrCallsHelper(t *testing.T) {
	root := controlIsolationFixture(t)
	op := "fuse-0123456789abcdef0123456789abcdef-0000000000000001"
	old := newRetainedGenerationClient
	called := 0
	newRetainedGenerationClient = func() (health.RetainedGenerationHelper, func()) {
		called++
		return nil, func() {}
	}
	t.Cleanup(func() { newRetainedGenerationClient = old })
	var out bytes.Buffer
	args := []string{retainedRecoveryFlag, root, "0123456789abcdef0123456789abcdef", "oldbuild", op}
	if code := runRetainedGenerationRecovery(args, &out, false, false); code == 0 {
		t.Fatal("missing fence admitted recovery")
	}
	for _, leaf := range []string{"s.sock.lock", ownerLifetimeLeaf} {
		if _, err := os.Lstat(filepath.Join(root, leaf)); !os.IsNotExist(err) {
			t.Fatalf("recovery created absent %s: %v", leaf, err)
		}
	}
	if called != 0 {
		t.Fatal("missing service fence reached helper")
	}
}

func TestRetainedRecoveryStoppedOwnerStillRequiresNFSProofBeforeHelper(t *testing.T) {
	root := controlIsolationFixture(t)
	for _, leaf := range []string{"s.sock.lock", ownerLifetimeLeaf} {
		if err := os.WriteFile(filepath.Join(root, leaf), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := ServerConfig{DBPath: filepath.Join(root, "metadata.db"), FUSEPath: filepath.Join(root, "fuse")}
	revision, err := beginServingDesired(cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	intent, created, err := createOwnerIntentUnfenced(scope, cfg)
	if err != nil || !created {
		t.Fatalf("owner selection: %+v %v", intent, err)
	}
	if err := updateServingDesired(revision, func(r *servingDesiredRecord) error { r.Owner = intent; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := confirmServingDesired(revision); err != nil {
		t.Fatal(err)
	}
	if _, err := stopServingDesired(); err != nil {
		t.Fatal(err)
	}
	desired, err := readServingDesiredRecord(scope, servingDesiredLeaf)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := recoveryOwnerAbsent(scope, desired, intent.Instance, intent.Build); err != nil || got != intent {
		t.Fatalf("exact stopped owner rejected: %+v %v", got, err)
	}
	if _, err := recoveryOwnerAbsent(scope, desired, intent.Instance, "wrong-build"); err == nil {
		t.Fatal("cross-build owner accepted")
	}
	old := newRetainedGenerationClient
	called := 0
	newRetainedGenerationClient = func() (health.RetainedGenerationHelper, func()) {
		called++
		return nil, func() {}
	}
	t.Cleanup(func() { newRetainedGenerationClient = old })
	var out bytes.Buffer
	args := []string{retainedRecoveryFlag, root, intent.Instance, intent.Build,
		"fuse-0123456789abcdef0123456789abcdef-0000000000000001"}
	if !isRetainedGenerationRecoveryCommand(args) {
		t.Fatal("pre-serving command not recognized")
	}
	exit := runRetainedGenerationRecovery(args, &out, false, false)
	var report retainedRecoveryReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if exit == 0 || report.ErrorCode != "nfs_custody_unavailable" || called != 0 {
		t.Fatalf("NFS custody gate failed before helper: exit=%d report=%+v calls=%d", exit, report, called)
	}
}

func TestRetainedRecoveryAdmitsExactHistoricalStoppedResourceOnly(t *testing.T) {
	root := controlIsolationFixture(t)
	for _, leaf := range []string{"s.sock.lock", ownerLifetimeLeaf} {
		if err := os.WriteFile(filepath.Join(root, leaf), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := ServerConfig{DBPath: filepath.Join(root, "metadata.db"),
		FUSEPath: filepath.Join(root, "fuse-internal"), MountPoint: filepath.Join(root, "volume")}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	intent, err := intendedOwner(cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	intent.Build = "historical-build"
	desired := servingDesiredRecord{Version: 1, Build: intent.Build, Root: root, Revision: 2,
		Desired: "stopped", Config: cfg, Owner: intent, NFSPhase: "bound",
		NFSRecord: &mountbroker.Record{OperationID: "nfs-inert"}}
	for path, value := range map[string]any{
		ownerIntentPath(root):                   intent,
		filepath.Join(root, servingDesiredLeaf): desired,
	} {
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	oldNFS, oldRecover, oldClient := observeRetainedPriorNFS, recoverRetainedGeneration, newRetainedGenerationClient
	nfsCalls, recoverCalls := 0, 0
	observeRetainedPriorNFS = func(context.Context, servingDesiredRecord) (bool, error) {
		nfsCalls++
		return true, nil // inert read-only NFS proof seam
	}
	recoverRetainedGeneration = func(_ context.Context, spec health.RetainedGenerationRecovery,
		_ health.RetainedGenerationHelper, _ func(context.Context) (mounttable.Snapshot, error)) health.RetainedGenerationOutcome {
		recoverCalls++
		if spec.Root != root || spec.OwnerInstance != intent.Instance || spec.Build != intent.Build ||
			spec.ConfigDigest != intent.ConfigDigest || spec.LogicalPath != cfg.FUSEPath {
			t.Fatalf("wrong historical handoff: %+v", spec)
		}
		return health.RetainedGenerationOutcome{State: "RECOVERED_RESOURCE_RETIRED", Retired: true, Syscalls: 1}
	}
	newRetainedGenerationClient = func() (health.RetainedGenerationHelper, func()) {
		return retainedGenerationClient{}, func() {}
	}
	t.Cleanup(func() {
		observeRetainedPriorNFS, recoverRetainedGeneration, newRetainedGenerationClient = oldNFS, oldRecover, oldClient
	})
	op := "fuse-0123456789abcdef0123456789abcdef-0000000000000001"
	var out bytes.Buffer
	if exit := runRetainedGenerationRecovery([]string{retainedRecoveryFlag, root, intent.Instance, intent.Build, op}, &out, false, false); exit != 0 {
		t.Fatalf("exact historical resource held: exit=%d report=%s", exit, out.String())
	}
	var report retainedRecoveryReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if nfsCalls != 1 || recoverCalls != 1 || !report.Retired || report.WholeServiceQuiescent ||
		report.OriginalOwnerWaitProven || report.OldBuild != intent.Build || report.OpID != op {
		t.Fatalf("overclaimed or missed exact handoff: %+v calls=%d,%d", report, nfsCalls, recoverCalls)
	}
	if _, err := os.Lstat(filepath.Join(root, ownerRetirementLeaf(intent))); !os.IsNotExist(err) {
		t.Fatalf("recovery manufactured owner retirement: %v", err)
	}
}

// This exercises the real main dispatch in an inert child test process.
// Removing only the new pre-serving dispatch causes the child to emit the
// ordinary --serve usage error instead of this structured argument refusal.
func TestRetainedRecoveryMainEntryRejectsMalformedBeforeStartup(t *testing.T) {
	if os.Getenv("JM_TEST_RETAINED_RECOVERY_CHILD") == "1" {
		os.Args = []string{os.Args[0], retainedRecoveryFlag}
		main()
		t.Fatal("main returned without handling retained recovery")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRetainedRecoveryMainEntryRejectsMalformedBeforeStartup$")
	cmd.Env = append(os.Environ(), "JM_TEST_RETAINED_RECOVERY_CHILD=1")
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 2 {
		t.Fatalf("child exit=%v output=%q, want exit 2", err, output)
	}
	var got retainedRecoveryReport
	if err := json.Unmarshal(bytes.TrimSpace(output), &got); err != nil {
		t.Fatalf("decode child result: %v; output=%q", err, output)
	}
	if got.SchemaVersion != 1 || got.SourceCommit != version.Commit || got.ErrorCode != "invalid_arguments" || got.Retired {
		t.Fatalf("unexpected child result: %+v", got)
	}
}

func TestRetainedRecoveryHeldOwnerLifetimeRefusesBeforeHelper(t *testing.T) {
	root := controlIsolationFixture(t)
	for _, leaf := range []string{"s.sock.lock", ownerLifetimeLeaf} {
		if err := os.WriteFile(filepath.Join(root, leaf), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	held, err := os.OpenFile(filepath.Join(root, ownerLifetimeLeaf), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	old := newRetainedGenerationClient
	called := 0
	newRetainedGenerationClient = func() (health.RetainedGenerationHelper, func()) {
		called++
		return nil, func() {}
	}
	t.Cleanup(func() { newRetainedGenerationClient = old })
	var out bytes.Buffer
	args := []string{retainedRecoveryFlag, root, "0123456789abcdef0123456789abcdef", "oldbuild",
		"fuse-0123456789abcdef0123456789abcdef-0000000000000001"}
	if exit := runRetainedGenerationRecovery(args, &out, false, false); exit == 0 {
		t.Fatal("held owner lifetime admitted")
	}
	var report retainedRecoveryReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.ErrorCode != "owner_lifetime_unavailable" || called != 0 {
		t.Fatalf("held owner reached helper: %+v calls=%d", report, called)
	}
}
