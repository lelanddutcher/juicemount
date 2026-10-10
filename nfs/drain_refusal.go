package nfs

// drain_refusal.go: the drainer's side of the H-8B project lease.
//
// Before a drain writes anything it asks the lease client for the row's
// project (ProjectLeaseClient.EnsureForDrain). If another Mac holds the
// project the row is parked locally in "waiting_project": it stays a READY
// spool row (reads, deletes and renames keep treating it as live) with
// wait_project set, so the dispatcher skips it. The same happens when the
// proxy refuses a write the lease check could not foresee: the bundled
// juicefs maps the proxy's -JMDENY to EROFS (lane L3), and every drain step
// routes its error through failTransient, which holds the row instead of
// spending its attempt budget. The lease client's tick re-acquires waiting
// projects and releases their rows when the project is free.
//
// Nothing here can lose data: a held row keeps its spool file and its
// capacity reservation, and the only backend artefact of a refused drain is
// a private stage, which the failure path already removes.

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
)

// SetProjectLeases wires the project-lease client. Set before Start.
func (d *Drainer) SetProjectLeases(c *ProjectLeaseClient) { d.leasesPtr.Store(c) }

// ProjectLeases returns the wired lease client (nil when none). A nil
// *ProjectLeaseClient is a valid no-op client.
func (d *Drainer) ProjectLeases() *ProjectLeaseClient { return d.leasesPtr.Load() }

// listDispatchable lists rows the dispatcher may claim. Rows waiting for a
// project lease are skipped only while a lease client exists to release
// them; without one a wait must never strand a row.
func (d *Drainer) listDispatchable(limit int) ([]*metadata.SpoolRow, error) {
	if d.ProjectLeases() == nil {
		return d.spool.Meta().ListReadyIncludingWaiting(limit)
	}
	return d.spool.Meta().ListReady(limit)
}

// releaseHoldsWithoutLeases clears every project wait when no lease client is
// wired. Called from Start.
func (d *Drainer) releaseHoldsWithoutLeases() {
	if d.ProjectLeases() != nil {
		return
	}
	if n, err := d.spool.Meta().ReleaseAllProjectHolds(); err != nil {
		log.Printf("drainer: release project waits without a lease client: %v", err)
	} else if n > 0 {
		jmlog.Info("drain: no project-lease client; resuming changes that were waiting for a project",
			"rows", n)
	}
}

// rowDrainBase returns the base identity and accepted lease epoch recorded
// for row. A read error returns UNKNOWN, which keeps pre-H-8B behaviour for
// that attempt only.
func (d *Drainer) rowDrainBase(row *metadata.SpoolRow) (metadata.SpoolBase, uint64) {
	base, epoch, err := d.spool.Meta().DrainBase(row.ID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.Printf("drainer: row %d base identity unreadable: %v", row.ID, err)
		}
		return metadata.SpoolBase{}, 0
	}
	return base, epoch
}

// leaseGate checks the row's project lease before any backend write. It
// returns ok=false after parking the row (another Mac holds the project).
// Without a lease client, outside a project, or when the lease server cannot
// answer, the drain proceeds; the proxy's refusal (EROFS) still stops a
// foreign write and is handled by holdIfRefused.
func (d *Drainer) leaseGate(row *metadata.SpoolRow) (leaseDecision, bool) {
	leases := d.ProjectLeases()
	if leases == nil {
		return leaseDecision{}, true
	}
	project := projectOf(row.NFSPath)
	if project == "" {
		return leaseDecision{}, true
	}
	dec, err := leases.EnsureForDrain(project)
	if err != nil {
		return leaseDecision{project: project}, true
	}
	if !dec.held && dec.heldBy != "" {
		d.holdForProject(row, project, dec.heldBy,
			fmt.Sprintf("waiting_project: %s is being edited by %s", projectDisplayName(project), holderHost(dec.heldBy)))
		return dec, false
	}
	return dec, true
}

// isLeaseRefusal reports whether a drain error is the proxy refusing the
// write (JMDENY, surfaced by the bundled juicefs as EROFS).
func isLeaseRefusal(err error) bool {
	return errors.Is(err, syscall.EROFS)
}

// holdIfRefused parks row when err is a lease refusal in a project. It
// returns true when it disposed of the row.
func (d *Drainer) holdIfRefused(row *metadata.SpoolRow, err error) bool {
	leases := d.ProjectLeases()
	if leases == nil || !isLeaseRefusal(err) {
		// Without a lease client nothing would ever release a held row, so
		// EROFS stays an ordinary transient failure.
		return false
	}
	project := projectOf(row.NFSPath)
	if project == "" {
		// Outside any project there is no lease to wait for; treat it as an
		// ordinary transient failure.
		return false
	}
	leases.NoteRefused(project, "")
	d.holdForProject(row, project, "", fmt.Sprintf("waiting_project: write refused (%v)", err))
	return true
}

// holdForProject moves a draining row to waiting_project.
func (d *Drainer) holdForProject(row *metadata.SpoolRow, project, holder, reason string) {
	held, err := d.spool.Meta().HoldForProject(row.ID, project, reason)
	if err != nil {
		// Could not park it: fall back to a plain requeue. The attempt budget
		// is still untouched, and the next attempt asks the lease again.
		log.Printf("drainer: hold row %d for project %q: %v", row.ID, project, err)
		_ = d.spool.Meta().ResetToReady(row.ID)
		d.scheduleDelayedWake(infraUnavailableBackoff)
		return
	}
	if !held {
		return // canceled or renamed meanwhile; the canceling path owns it
	}
	d.metrics.DrainsRetried.Add(1)
	jmlog.Info("drain: project is held by another Mac; keeping the change on this Mac until it is free",
		"path", row.NFSPath, "project", project, "holder", holder)
}
