package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
	"github.com/lelanddutcher/juicemount/internal/version"
	"golang.org/x/sys/unix"
)

const retainedRecoveryFlag = "--recover-retained-generation"
const retainedRecoveryEjectFlag = "--eject-now"
const retainedRecoveryWithdrawFlag = "--withdraw"

type retainedRecoveryReport struct {
	SchemaVersion           int    `json:"schema_version"`
	SourceCommit            string `json:"source_commit"`
	OldBuild                string `json:"old_build,omitempty"`
	OpID                    string `json:"op_id,omitempty"`
	State                   string `json:"state"`
	Retired                 bool   `json:"retired"`
	Withdrawn               bool   `json:"withdrawn"`
	Forced                  bool   `json:"forced"`
	Syscalls                int    `json:"syscalls"`
	ErrorCode               string `json:"error_code,omitempty"`
	WholeServiceQuiescent   bool   `json:"whole_service_quiescent"`
	OriginalOwnerWaitProven bool   `json:"original_owner_wait_proven"`
	HelperMayContinue       bool   `json:"helper_may_continue"`
}

type retainedGenerationClient struct{ unmountdAdapter }

func (c retainedGenerationClient) Status(ctx context.Context, op string) (client.Outcome, error) {
	return c.c.Status(ctx, op)
}

var newRetainedGenerationClient = func() (health.RetainedGenerationHelper, func()) {
	c := client.New(client.Options{})
	return retainedGenerationClient{unmountdAdapter{c}}, c.Close
}
var observeRetainedGeneration = observeOwnedNFS
var observeRetainedPriorNFS = priorServingShutdownNFSRetired
var recoverRetainedGeneration = health.RecoverRetainedGeneration

func isRetainedGenerationRecoveryCommand(args []string) bool {
	return len(args) > 0 && args[0] == retainedRecoveryFlag
}

func writeRetainedRecoveryReport(stdout io.Writer, r retainedRecoveryReport, code int) int {
	b, err := json.Marshal(r)
	if err != nil {
		return 1
	}
	if n, err := stdout.Write(append(b, '\n')); err != nil || n != len(b)+1 {
		return 1
	}
	return code
}

// openExistingRecoveryFence never creates or replaces a missing lifetime lock.
func openExistingRecoveryFence(scope appisolation.Scope, leaf string) (*os.File, error) {
	path := filepath.Join(scope.Root, leaf)
	if err := scope.RequireChild(path); err != nil {
		return nil, err
	}
	dir, err := os.Open(scope.Root)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	f, err := privatefile.OpenAt(dir, leaf, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err := privateOwnerRecord(f, 0); err != nil {
		f.Close()
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("recovery lifetime already held")
	}
	return f, nil
}

func recoveryOwnerAbsent(scope appisolation.Scope, desired servingDesiredRecord, instance, build string) (volumeOwnerIntent, error) {
	intent, err := readOwnerIntentRecord(scope)
	if err != nil {
		return volumeOwnerIntent{}, err
	}
	if err := validateOwnerIntentForBuild(intent, scope.Root, build); err != nil {
		return volumeOwnerIntent{}, err
	}
	if intent.Instance != instance || desired.Owner != intent || desired.Build != build || desired.Root != scope.Root ||
		desired.Desired != "stopped" || desired.Config.FUSEPath == "" {
		return volumeOwnerIntent{}, errors.New("historical stopped owner selection mismatch")
	}
	digest, err := servingConfigDigest(desired.Config)
	if err != nil || digest != intent.ConfigDigest {
		return volumeOwnerIntent{}, errors.New("historical config digest mismatch")
	}
	if _, err := readOwnerRetirementForBuild(scope, intent, build); !errors.Is(err, os.ErrNotExist) {
		return volumeOwnerIntent{}, errors.New("owner retirement exists or is unreadable")
	}
	return intent, nil
}

func runRetainedGenerationRecovery(args []string, stdout io.Writer, root bool, serviceBuild bool) int {
	r := retainedRecoveryReport{SchemaVersion: 1, SourceCommit: version.Commit, State: "HOLD"}
	if (len(args) != 5 && len(args) != 6) || args[0] != retainedRecoveryFlag ||
		(len(args) == 6 && args[5] != retainedRecoveryEjectFlag && args[5] != retainedRecoveryWithdrawFlag) || args[1] == "" ||
		args[2] == "" || args[3] == "" || args[4] == "" || !client.ValidOpID(args[4]) {
		r.ErrorCode = "invalid_arguments"
		return writeRetainedRecoveryReport(stdout, r, 2)
	}
	r.OldBuild, r.OpID = args[3], args[4]
	if root {
		r.ErrorCode = "root_not_allowed"
		return writeRetainedRecoveryReport(stdout, r, 2)
	}
	if serviceBuild {
		r.ErrorCode = "service_build_not_allowed"
		return writeRetainedRecoveryReport(stdout, r, 2)
	}
	scope, isolated, err := appisolation.CurrentControl()
	if err != nil || !isolated || scope.Root != args[1] {
		r.ErrorCode = "isolation_mismatch"
		return writeRetainedRecoveryReport(stdout, r, 2)
	}
	serviceFence, err := openExistingRecoveryFence(scope, "s.sock.lock")
	if err != nil {
		r.ErrorCode = "service_lifetime_unavailable"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	defer serviceFence.Close()
	ownerFence, err := openExistingRecoveryFence(scope, ownerLifetimeLeaf)
	if err != nil {
		r.ErrorCode = "owner_lifetime_unavailable"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	defer ownerFence.Close()
	desired, err := readServingDesiredRecord(scope, servingDesiredLeaf)
	if err != nil {
		r.ErrorCode = "desired_record_unavailable"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	intent, err := recoveryOwnerAbsent(scope, desired, args[2], args[3])
	if err != nil {
		r.ErrorCode = "historical_owner_mismatch"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	if err := scope.RequireChild(desired.Config.FUSEPath + ".generations"); err != nil {
		r.ErrorCode = "generation_outside_isolation"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	if desired.Config.MountPoint == "" || desired.NFSPhase != "bound" || desired.NFSRecord == nil {
		r.ErrorCode = "nfs_custody_unavailable"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	nfsCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	retired, nfsErr := observeRetainedPriorNFS(nfsCtx, desired)
	cancel()
	if nfsErr != nil || !retired {
		r.ErrorCode = "nfs_not_proven_retired"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	// Re-read all durable identity after the NFS inventory. The two lifetime
	// fences exclude cooperating owners/services; changed files still HOLD.
	current, err := readServingDesiredRecord(scope, servingDesiredLeaf)
	if err != nil || !reflect.DeepEqual(current, desired) {
		r.ErrorCode = "desired_changed"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	selected, err := readOwnerIntentRecord(scope)
	if err != nil || selected != intent {
		r.ErrorCode = "owner_changed"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	helper, closeHelper := newRetainedGenerationClient()
	if helper == nil {
		r.ErrorCode = "helper_unavailable"
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	defer closeHelper()
	result := recoverRetainedGeneration(context.Background(), health.RetainedGenerationRecovery{
		Root: scope.Root, OwnerInstance: intent.Instance, Build: intent.Build, ConfigDigest: intent.ConfigDigest,
		LogicalPath: desired.Config.FUSEPath, OpID: args[4],
		Withdraw: len(args) == 6 && args[5] == retainedRecoveryWithdrawFlag,
		EjectNow: len(args) == 6 && args[5] == retainedRecoveryEjectFlag,
	}, helper, observeRetainedGeneration)
	r.State, r.Retired, r.Withdrawn, r.Forced, r.Syscalls, r.ErrorCode =
		result.State, result.Retired, result.Withdrawn, result.Forced, result.Syscalls, result.ErrorCode
	r.HelperMayContinue = result.HelperMayContinue
	if !result.Retired && !result.Withdrawn {
		return writeRetainedRecoveryReport(stdout, r, 1)
	}
	return writeRetainedRecoveryReport(stdout, r, 0)
}
