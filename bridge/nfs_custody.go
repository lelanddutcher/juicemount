package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/nfscustody"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

type nfsMountController interface {
	Start(context.Context) error
	Stop(context.Context, time.Duration) error
	Record() *mountbroker.Record
	Path() string
	Active() bool
}

// Protected by globalMu. A timeout never removes this owner.
var globalNFSController nfsMountController
var newNFSMountCreator = mountbroker.LocalCreator
var newNFSController = func(c mountbroker.Creator, s mountbroker.Spec, opts string, interactive bool) (nfsMountController, error) {
	controller, err := nfscustody.NewWithProvisioning(c, s, opts, interactive)
	if err != nil {
		return nil, err
	}
	// Register-on-mount (H-3 L4): the controller records a helper receipt
	// for the Observed fsid and holds the lease while the mount is ours.
	if h := processUnmountHelper(); h != nil {
		controller.SetUnmountHelper(h)
	}
	return controller, nil
}
var observeOwnedNFS = func(ctx context.Context) (mounttable.Snapshot, error) {
	exe, err := os.Executable()
	if err != nil {
		return mounttable.Snapshot{}, err
	}
	return mounttable.SnapshotFromHelper(ctx, filepath.Join(filepath.Dir(exe), "juicemount-mount-probe"))
}

// Caller holds globalMu. Custody remains meaningful even before Start publishes
// a kernel mount, and must prevent shutdown from closing its dependencies.
func retainedNFSPathLocked() string {
	if globalNFSController != nil {
		return globalNFSController.Path()
	}
	return globalMountPath
}

func startOwnedNFSMount(addr, export, path string, interactive bool) error {
	source, opts, err := nfsMountArgs(addr, export)
	if err != nil {
		return err
	}
	globalMu.Lock()
	existing, metadata := globalNFSController, globalDBPath
	globalMu.Unlock()
	if existing != nil {
		return errors.New("previous NFS operation retained; complete Stop Mount before starting again")
	}
	creator, err := newNFSMountCreator()
	if err != nil {
		return err
	}
	// New is inert. No capability or mount child may run until publication.
	controller, err := newNFSController(creator, mountbroker.Spec{MountPath: path, Source: source, Endpoint: addr, ExportPath: export, MetadataPath: metadata}, opts, interactive)
	if err != nil {
		return err
	}
	globalMu.Lock()
	if globalNFSController != nil {
		globalMu.Unlock()
		return errors.New("another NFS operation already owns creation")
	}
	globalNFSController = controller
	globalMountPath = path
	globalMu.Unlock()
	revision := boundServingNFSRevision()
	if err := markServingNFSPending(revision); err != nil {
		return err
	}
	if err := controller.Start(context.Background()); err != nil {
		return err
	}
	return markServingNFSBound(revision, controller.Record())
}

// A table row alone cannot adopt a mount. Only the live controller's admitted
// descriptor plus a fresh complete matching observation can report ready.
func currentOwnedNFSMount(path string) bool {
	globalMu.Lock()
	c := globalNFSController
	globalMu.Unlock()
	if c == nil || c.Path() != path || !c.Active() {
		return false
	}
	r := c.Record()
	if r == nil || r.Mount == nil || r.Spec.MountPath != path || r.Mount.FSID == [2]int32{} {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	snapshot, err := observeOwnedNFS(ctx)
	if err != nil || snapshot.Version != 1 || snapshot.BootID != r.BootID {
		return false
	}
	found := false
	for _, e := range snapshot.Entries {
		if strings.HasPrefix(e.Path, path+"/") {
			return false
		}
		if e.Path == path || e.Source == r.Spec.Source || e.FSID == r.Mount.FSID {
			if found || e != *r.Mount || e.Path != path || e.Source != r.Spec.Source || e.Type != "nfs" {
				return false
			}
			found = true
		}
	}
	globalMu.Lock()
	same := globalNFSController == c
	globalMu.Unlock()
	return found && same && c.Active()
}

func stopOwnedNFSMount(path string) bool {
	globalMu.Lock()
	c := globalNFSController
	globalMu.Unlock()
	if c == nil {
		// No live controller (e.g. after a service restart): a same-boot
		// Observed journal row is retired through the dead owner's helper
		// receipt, never by path. Without that proof nothing changes.
		if path != "" {
			if retired, err := retireOrphanNFS(context.Background(), path); err != nil {
				jmlog.Warn("orphan NFS operation not retired by the unmount helper", "mount_point", path, "error", err.Error())
			} else if retired {
				jmlog.Info("orphan NFS operation retired by the unmount helper", "mount_point", path)
			}
		}
		return unmountNFSWith(path, inspectConfiguredNFSMount, runProcessBounded)
	}
	if path == "" || c.Path() != path {
		return false
	}
	if err := c.Stop(context.Background(), 30*time.Second); err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	snapshot, err := observeOwnedNFS(ctx)
	if err != nil || snapshot.Version != 1 || snapshot.BootID == "" || snapshot.Entries == nil {
		return false
	}
	r := c.Record()
	if r != nil && snapshot.BootID != r.BootID {
		return false
	}
	for _, e := range snapshot.Entries {
		if e.Path == path || strings.HasPrefix(e.Path, path+"/") {
			return false
		}
		if r != nil && (e.Source == r.Spec.Source || r.Mount != nil && e.FSID == r.Mount.FSID) {
			return false
		}
	}
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalNFSController != c {
		return false
	}
	globalNFSController = nil
	return true
}

// orphanNFSRetireWait bounds one orphan retirement. It is below the
// daemon's per-request cap; a busy volume returns pending and is retried.
var orphanNFSRetireWait = 45 * time.Second

// retireOrphanNFS is the seam the Stop and service-start paths call.
var retireOrphanNFS = retireOrphanNFSWithHelper

// retireOrphanNFSWithHelper finishes an NFS operation whose owning process
// died (H-3 L4, design §3 orphan sweep). The journal row is the persisted op
// id (mountbroker.HelperOpID). It reclaims the dead owner's receipt within
// the helper's 30 s orphan window, asks for retirement, and settles the
// journal only on a RETIRED proof for exactly that op plus Retire's own fresh
// absence check. It refuses when this process has a live controller, when
// the row is from another boot, and when another live connection still holds
// the receipt (Reclaimed=false). It reports whether it retired.
func retireOrphanNFSWithHelper(ctx context.Context, mountPoint string) (bool, error) {
	h := processUnmountHelper()
	if h == nil {
		return false, nil
	}
	globalMu.Lock()
	live := globalNFSController != nil
	globalMu.Unlock()
	if live {
		return false, errors.New("a live NFS controller owns this path")
	}
	directory, err := mountbroker.LocalJournalDirectory()
	if err != nil {
		return false, err
	}
	rec, err := mountbroker.ReadRecordReadonly(directory, mountPoint)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, mountbroker.ErrJournalBusy) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return retireOrphanNFSRecord(ctx, h, rec)
}

func retireOrphanNFSRecord(ctx context.Context, h serviceUnmountHelper, rec *mountbroker.Record) (bool, error) {
	if rec == nil || rec.State != mountbroker.Observed || rec.Mount == nil {
		return false, nil // no receipt was ever registered for it
	}
	op := mountbroker.HelperOpID(*rec)
	if op == "" {
		return false, errors.New("journal operation has no helper op id")
	}
	octx, cancel := context.WithTimeout(ctx, 2*time.Second)
	snapshot, err := observeOwnedNFS(octx)
	cancel()
	if err != nil || snapshot.Version != 1 || snapshot.BootID == "" {
		return false, errors.New("complete NFS inventory unavailable")
	}
	if snapshot.BootID != rec.BootID {
		return false, nil // kernel mounts do not survive a reboot; not a helper case
	}
	rctx, rcancel := context.WithTimeout(ctx, orphanNFSRetireWait+10*time.Second)
	defer rcancel()
	lease, res, err := h.Reclaim(rctx, op)
	var out client.Outcome
	switch {
	case err == nil:
		defer lease.Close()
		if !res.Reclaimed {
			return false, errors.New("helper receipt is still held by a live owner")
		}
		// The NFS engine lived in the owner process. Reclaimed=true means the
		// helper's orphan timer was pending, i.e. that owner's connection is
		// gone, and this restarted service serves a new export generation, so
		// nothing answers for this mount any more: Liveness Dead. An ordinary
		// unmount of a dead server would block in VFS_SYNC and set
		// MNT_LUNMOUNT, wedging even a later force.
		out, err = h.Retire(rctx, op, client.RetireRequest{Liveness: client.LivenessDead}, orphanNFSRetireWait)
		if err != nil {
			return false, fmt.Errorf("helper retirement %s: %w (state %s)", op, err, out.State)
		}
	case errors.Is(err, client.ErrNoReceipt):
		out = res.Outcome // the helper's own orphan sweep finished first
	default:
		return false, fmt.Errorf("reclaim %s: %w", op, err)
	}
	if out.OpID != op || out.State != mountbroker.HelperRetiredState {
		return false, fmt.Errorf("no RETIRED proof for %s (state %q)", op, out.State)
	}
	creator, err := newNFSMountCreator()
	if err != nil {
		return false, err
	}
	// No custody of the dead owner's children exists in this process; the
	// helper's proof plus Retire's fresh absence check are the settlement.
	if err := creator.RetireWithHelperProof(ctx, *rec, mountbroker.HelperRetirement{OpID: op, State: out.State},
		func() error { return nil }); err != nil {
		return false, err
	}
	return true, nil
}
