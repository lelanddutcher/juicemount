package mountbroker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// DescriptorRetirementCustody is deliberately a live, nonserializable original
// descriptor. A journal row alone can never authorize a detach.
type DescriptorRetirementCustody interface {
	FSID() [2]int32
	Detach() error
	Close() error
}

type ArmedDescriptorRetirement struct {
	mu                                                 sync.Mutex
	creator                                            Creator
	expected                                           Record
	custody                                            DescriptorRetirementCustody
	detachAttempted, detached, retired, closeAttempted bool
}

func exactMounted(snapshot mounttable.Snapshot, expected Record, fsid [2]int32) bool {
	if !validSnapshot(snapshot) || snapshot.BootID != expected.BootID ||
		expected.Mount == nil || expected.Mount.FSID != fsid {
		return false
	}
	var matches int
	for _, row := range snapshot.Entries {
		if row.Path == expected.Spec.MountPath && row.Source == expected.Spec.Source &&
			row.Type == "nfs" && row.FSID == fsid && row == *expected.Mount {
			matches++
			continue
		}
		if entriesAt(mounttable.Snapshot{Version: 1, BootID: snapshot.BootID,
			Entries: []mounttable.MountEntry{row}}, expected.Spec.MountPath) != nil ||
			row.Source == expected.Spec.Source || row.FSID == fsid {
			return false
		}
	}
	return matches == 1
}

func exactAbsent(snapshot mounttable.Snapshot, expected Record, fsid [2]int32) bool {
	return validSnapshot(snapshot) && snapshot.BootID == expected.BootID &&
		entriesAt(snapshot, expected.Spec.MountPath) == nil &&
		!sourceOrFSIDPresent(snapshot, expected.Spec.Source, fsid)
}

func exactObservedRecord(current *Record, expected Record) bool {
	return current != nil && current.Version == expected.Version &&
		current.OperationID == expected.OperationID && current.BootID == expected.BootID &&
		current.Spec == expected.Spec && current.State == Observed &&
		current.Mount != nil && expected.Mount != nil && *current.Mount == *expected.Mount
}

// ArmDescriptorRetirement binds the original descriptor to an already
// durably Observed operation. It cannot be reconstructed from disk after loss.
func (c Creator) ArmDescriptorRetirement(ctx context.Context, expected Record,
	custody DescriptorRetirementCustody) (*ArmedDescriptorRetirement, error) {
	if ctx == nil || c.Observe == nil || custody == nil || expected.State != Observed ||
		!validExpectedRetirement(expected) || expected.Mount == nil ||
		custody.FSID() == [2]int32{} || custody.FSID() != expected.Mount.FSID {
		return nil, errors.New("invalid original NFS descriptor retirement authority")
	}
	mount := *expected.Mount
	expected.Mount = &mount
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	journal, err := openJournal(c.Directory, expected.Spec.MountPath)
	if err != nil {
		return nil, err
	}
	defer journal.close()
	current, err := journal.read()
	if err != nil || !exactObservedRecord(current, expected) {
		return nil, errors.New("armed retirement journal identity differs")
	}
	snapshot, err := c.Observe(ctx)
	if err != nil || !exactMounted(snapshot, expected, custody.FSID()) {
		return nil, errors.New("armed retirement mount observation incomplete or different")
	}
	return &ArmedDescriptorRetirement{creator: c, expected: expected, custody: custody}, nil
}

// Cleanup never retries Detach after a successful original invocation. On
// post-detach uncertainty the same retained object alone may reconcile.
func (a *ArmedDescriptorRetirement) Cleanup(ctx context.Context) error {
	if a == nil || ctx == nil {
		return errors.New("missing armed descriptor cleanup")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closeAttempted {
		return errors.New("original descriptor close already attempted")
	}
	if a.detachAttempted && !a.detached {
		return errors.New("original descriptor detach outcome unknown; no retry")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	journal, err := openJournal(a.creator.Directory, a.expected.Spec.MountPath)
	if err != nil {
		return err
	}
	if a.creator.syncFile != nil {
		journal.syncFile = a.creator.syncFile
	}
	if a.creator.syncDir != nil {
		journal.syncDir = a.creator.syncDir
	}
	// Keep the lock through original physical detach, fresh absence and the
	// durable Retired CAS. Close only after releasing the journal lock.
	err = func() error {
		defer journal.close()
		current, err := journal.read()
		if err != nil {
			return err
		}
		if a.detached && current != nil && current.State == Retired &&
			current.Version == a.expected.Version &&
			current.OperationID == a.expected.OperationID &&
			current.BootID == a.expected.BootID &&
			current.Spec == a.expected.Spec && current.Mount != nil &&
			*current.Mount == *a.expected.Mount {
			snapshot, err := a.creator.Observe(ctx)
			if err != nil || !exactAbsent(snapshot, a.expected, a.custody.FSID()) {
				return errors.New("retired reconciliation absence unavailable")
			}
			// A prior rename may have succeeded before its directory sync failed.
			// Rewriting the exact record reestablishes durability with current hooks.
			if err := journal.write(*current); err != nil {
				return fmt.Errorf("reestablish original NFS retirement durability: %w", err)
			}
			a.retired = true
			return nil
		}
		if !exactObservedRecord(current, a.expected) {
			return errors.New("armed retirement journal successor or divergence")
		}
		if !a.detached {
			snapshot, err := a.creator.Observe(ctx)
			if err != nil || !exactMounted(snapshot, a.expected, a.custody.FSID()) {
				return errors.New("armed retirement pre-detach mount identity changed")
			}
			a.detachAttempted = true
			if err := a.custody.Detach(); err != nil {
				return err
			}
			a.detached = true
		}
		snapshot, err := a.creator.Observe(ctx)
		if err != nil || !exactAbsent(snapshot, a.expected, a.custody.FSID()) {
			return errors.New("armed retirement post-detach absence unavailable")
		}
		retired := *current
		retired.State = Retired
		if err := journal.write(retired); err != nil {
			return fmt.Errorf("persist armed NFS retirement: %w", err)
		}
		a.retired = true
		return nil
	}()
	if err != nil {
		return err
	}
	a.closeAttempted = true
	if err := a.custody.Close(); err != nil {
		return fmt.Errorf("NFS operation retired but original descriptor close failed: %w", err)
	}
	return nil
}

// validRecordMount checks a reported mount's shape. It never grants detach
// authority: the live controller's retained descriptor must establish that.
func validRecordMount(spec Spec, mount *mounttable.MountEntry) bool {
	return mount != nil && mount.Path == spec.MountPath && mount.Source == spec.Source &&
		mount.Type == "nfs" && mount.FSID != [2]int32{}
}

func recordFSID(record Record) [2]int32 {
	if record.Mount == nil {
		return [2]int32{}
	}
	return record.Mount.FSID
}

func sourceOrFSIDPresent(snapshot mounttable.Snapshot, source string, fsid [2]int32) bool {
	for _, entry := range snapshot.Entries {
		if entry.Source == source || fsid != [2]int32{} && entry.FSID == fsid {
			return true
		}
	}
	return false
}

func canonicalOperationID(id string) bool {
	operation, err := hex.DecodeString(id)
	return err == nil && len(operation) == 16 && hex.EncodeToString(operation) == id
}

func validExpectedRetirement(expected Record) bool {
	return expected.Version == 1 && canonicalOperationID(expected.OperationID) && expected.BootID != "" &&
		expected.Spec.validate() == nil &&
		(expected.State == InFlight || expected.State == Observed || expected.State == Retired) &&
		(expected.Mount == nil || validRecordMount(expected.Spec, expected.Mount))
}

// Retire conditionally settles only the exact recorded operation. settle is
// the trusted live controller's bounded proof that its original mount command
// and descriptor helper have joined, or that admission definitely never
// happened. Neither a journal row nor mount-table absence supplies that proof.
// The callback runs while the per-path journal flock is held, before the
// broker's independent fresh same-boot absence observation.
func (c Creator) Retire(ctx context.Context, expected Record, settle func() error) error {
	// A Record value still aliases its Mount pointer. Freeze the caller's
	// claimed identity before settle can mutate its own copy.
	if expected.Mount != nil {
		mount := *expected.Mount
		expected.Mount = &mount
	}
	if ctx == nil || c.Observe == nil || settle == nil || !validExpectedRetirement(expected) {
		return errors.New("missing or invalid retirement authority")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	journal, err := openJournal(c.Directory, expected.Spec.MountPath)
	if err != nil {
		return err
	}
	defer journal.close()
	if c.syncFile != nil {
		journal.syncFile = c.syncFile
	}
	if c.syncDir != nil {
		journal.syncDir = c.syncDir
	}
	current, err := journal.read()
	if err != nil {
		return fmt.Errorf("read mount operation for retirement: %w", err)
	}
	if current == nil || current.OperationID != expected.OperationID ||
		current.BootID != expected.BootID || current.Spec != expected.Spec ||
		current.Version != expected.Version ||
		(current.Mount != nil && (expected.Mount == nil || *current.Mount != *expected.Mount)) {
		return errors.New("retirement operation or mounted identity does not match current journal")
	}
	// A persisted InFlight row may have no Mount even if the original live
	// controller admitted the descriptor and enriched expected.Mount. The
	// callback, not the caller's serialized field, binds that identity.
	if err := settle(); err != nil {
		return fmt.Errorf("live NFS custody has not settled: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshot, err := c.Observe(ctx)
	if err != nil || !validSnapshot(snapshot) || snapshot.BootID != current.BootID {
		return fmt.Errorf("retirement snapshot unavailable or boot changed: %w", observationError(err))
	}
	if entriesAt(snapshot, current.Spec.MountPath) != nil ||
		sourceOrFSIDPresent(snapshot, current.Spec.Source, recordFSID(expected)) {
		return errors.New("retired NFS path, descendant, source or FSID remains mounted")
	}
	retired := *current
	retired.State = Retired
	if expected.Mount != nil {
		mount := *expected.Mount
		retired.Mount = &mount
	}
	if err := journal.write(retired); err != nil {
		return fmt.Errorf("persist NFS retirement: %w", err)
	}
	return nil
}

// HelperOpID is the privileged unmount helper's op id for one NFS journal
// operation (H-3 L4). It is derived, never stored separately: the journal
// row is the persistence a restarted service uses to reclaim its receipt.
func HelperOpID(r Record) string {
	if !canonicalOperationID(r.OperationID) {
		return ""
	}
	return "nfs-" + r.OperationID
}

// HelperRetiredState is the helper's terminal success state.
const HelperRetiredState = "RETIRED"

// HelperRetirement is the privileged unmount helper's report that it retired
// one receipt. The helper registered that receipt with the operation's own
// fsid, export token and kernel strings, and re-proved them against a fresh
// mount table before each unmount syscall. So a RETIRED outcome for the op
// derived from this exact record stands in for the descriptor detach, the
// one proof a lost or TCC-denied custodian cannot give.
type HelperRetirement struct {
	OpID  string
	State string
}

func (p HelperRetirement) proves(expected Record) error {
	op := HelperOpID(expected)
	if op == "" || p.OpID != op {
		return errors.New("helper retirement is for a different operation")
	}
	if p.State != HelperRetiredState {
		return fmt.Errorf("helper retirement not final: %q", p.State)
	}
	if expected.Mount == nil || expected.Mount.FSID == [2]int32{} {
		return errors.New("helper retirement needs the observed mount identity")
	}
	return nil
}

// RetireWithHelperProof settles an operation whose kernel mount the helper
// retired. It accepts only a RETIRED outcome for this exact record's op and
// still runs Retire's own checks: journal identity under the flock, settle,
// then a fresh same-boot snapshot with the path, source and fsid all absent.
// settle is the live controller's join proof; a restarted service that holds
// no custody of the dead owner's children passes a no-op.
func (c Creator) RetireWithHelperProof(ctx context.Context, expected Record, proof HelperRetirement, settle func() error) error {
	if err := proof.proves(expected); err != nil {
		return err
	}
	return c.Retire(ctx, expected, settle)
}
