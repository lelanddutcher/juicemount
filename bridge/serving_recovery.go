package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
)

var servingRecoveryStatus atomic.Value // sanitized, never stores config or URLs
var observePriorServingNFS = priorServingNFSRetired
var inspectRecoveryOwner = inspectSelectedOwner
var startRecoveryEngine = startServingRecoverySerialized

func recoveryStatus() string {
	if value := servingRecoveryStatus.Load(); value != nil {
		return value.(string)
	}
	return "not-observed"
}

func setRecoveryStatus(value string) { servingRecoveryStatus.Store(value) }

// A record may describe a successful server-only/offline Start which never
// admitted mount_nfs. A pending admission is deliberately not inferred absent.
func priorServingNFSRetired(ctx context.Context, desired servingDesiredRecord) (bool, error) {
	if desired.NFSPhase == "pending" {
		return false, errors.New("original NFS admission lacks an arm receipt")
	}
	directory := ""
	if desired.Config.MountPoint != "" {
		var err error
		directory, err = mountbroker.LocalJournalDirectory()
		if err != nil {
			return false, err
		}
	}
	return priorServingNFSRetiredInDirectory(ctx, desired, directory)
}

// Replacement Shutdown may inspect its journal while a mounted data-plane
// child is unreadable. Its control directory is validated separately; normal
// startup and recovery retain LocalJournalDirectory's full admission gate.
func priorServingShutdownNFSRetired(ctx context.Context, desired servingDesiredRecord) (bool, error) {
	if desired.NFSPhase == "pending" {
		return false, errors.New("original NFS admission lacks an arm receipt")
	}
	scope, isolated, err := selectedOwnerControlScope()
	if err != nil {
		return false, err
	}
	directory := ""
	if desired.Config.MountPoint != "" {
		if isolated {
			directory = filepath.Join(scope.Root, "mount-transactions")
			if err := scope.RequireChild(directory); err != nil {
				return false, err
			}
		} else {
			directory, err = mountbroker.LocalJournalDirectory()
			if err != nil {
				return false, err
			}
		}
	}
	return priorServingNFSRetiredInDirectory(ctx, desired, directory)
}

func priorServingNFSRetiredInDirectory(ctx context.Context, desired servingDesiredRecord, directory string) (bool, error) {
	if desired.NFSPhase == "pending" {
		return false, errors.New("original NFS admission lacks an arm receipt")
	}
	var journal *mountbroker.Record
	if desired.Config.MountPoint != "" {
		var err error
		journal, err = mountbroker.ReadRecordReadonly(directory, desired.Config.MountPoint)
		if errors.Is(err, mountbroker.ErrJournalBusy) {
			return false, nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	if desired.NFSPhase == "bound" {
		want := desired.NFSRecord
		if want == nil || journal == nil || journal.State != mountbroker.Retired ||
			journal.Version != want.Version || journal.OperationID != want.OperationID || journal.BootID != want.BootID ||
			journal.Spec != want.Spec || !reflect.DeepEqual(journal.Mount, want.Mount) {
			return false, nil
		}
	} else if journal != nil && journal.State != mountbroker.Retired {
		return false, nil
	}
	observeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	snapshot, err := observeOwnedNFS(observeCtx)
	if err != nil || snapshot.Version != 1 || snapshot.BootID == "" || snapshot.Entries == nil {
		return false, errors.New("complete current NFS inventory unavailable")
	}
	if journal != nil && snapshot.BootID != journal.BootID {
		return false, errors.New("NFS operation belongs to another boot")
	}
	var source string
	var fsid [2]int32
	if journal != nil {
		source = journal.Spec.Source
		if journal.Mount != nil {
			fsid = journal.Mount.FSID
		}
	}
	for _, entry := range snapshot.Entries {
		if entry.Path == "" || entry.Source == "" || entry.Type == "" {
			return false, errors.New("incomplete NFS inventory entry")
		}
		if desired.Config.MountPoint != "" && (strings.EqualFold(entry.Path, desired.Config.MountPoint) || strings.HasPrefix(strings.ToLower(entry.Path), strings.ToLower(desired.Config.MountPoint)+"/")) ||
			source != "" && entry.Source == source || fsid != [2]int32{} && entry.FSID == fsid {
			return false, nil
		}
	}
	return true, nil
}

// Exactly one worker belongs to the listener lifetime. It observes without
// holding lifecycle control, then rechecks revision and original owner under
// the same lock that orders Start and every intentional Stop.
func resumeConfirmedServingOnce(ctx context.Context) {
	setRecoveryStatus("checking")
	desired, err := readServingDesired()
	if err == nil && desired.Config.MountPoint != "" && desired.NFSPhase != "pending" {
		// Service-start orphan sweep (H-3 L4): reclaim a dead previous
		// owner's helper receipt inside the helper's 30 s window and drive
		// its retirement, so the wait below can observe a retired journal.
		// Unknown or unproven outcomes change nothing.
		if retired, rerr := retireOrphanNFS(ctx, desired.Config.MountPoint); rerr != nil {
			setRecoveryStatus("orphan-nfs-helper-unresolved")
		} else if retired {
			setRecoveryStatus("orphan-nfs-retired-by-helper")
		}
	}
	if err != nil || desired.Desired != "running" {
		setRecoveryStatus("held-no-confirmed-intent")
		return
	}
	if desired.NFSPhase == "pending" {
		setRecoveryStatus("held-nfs-admission-uncertain")
		return
	}
	setRecoveryStatus("waiting-nfs-retirement")
	window, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		ready, err := observePriorServingNFS(window, desired)
		if err != nil {
			setRecoveryStatus("held-nfs-observation-unknown")
			return
		} // unknown observation is HOLD, not permission
		if ready {
			break
		}
		select {
		case <-window.Done():
			setRecoveryStatus("held-nfs-retirement-timeout")
			return
		case <-ticker.C:
		}
	}
	globalLifecycleMu.Lock()
	defer globalLifecycleMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	current, err := readServingDesired()
	if err != nil || current.Revision != desired.Revision || current.Desired != "running" ||
		current.Config != desired.Config || current.Owner != desired.Owner || current.NFSPhase != desired.NFSPhase ||
		!reflect.DeepEqual(current.NFSRecord, desired.NFSRecord) {
		return
	}
	selected, err := inspectRecoveryOwner(current.Config)
	if err != nil || selected != current.Owner {
		setRecoveryStatus("held-original-owner-unavailable")
		return
	}
	// The inventory may change while owner inspection crosses a socket. The
	// second observation fences admission against a newly occupied NFS path.
	ready, err := observePriorServingNFS(window, current)
	if err != nil || !ready {
		return
	}
	admitted, err := admitServingRecovery(current)
	if err != nil {
		setRecoveryStatus("held-recovery-admission")
		return
	}
	setRecoveryStatus("resuming")
	if err := startRecoveryEngine(admitted); err != nil {
		// The durable recovering state intentionally prevents another replay.
		setRecoveryStatus("held-replacement-start")
		return
	}
	setRecoveryStatus("resumed")
}
