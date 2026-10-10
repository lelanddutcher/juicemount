package unmountd

import (
	"fmt"
	"syscall"
	"time"
)

// converge is the per-receipt state machine (design §3, with the review's
// corrections):
//
//	VERIFY ─┬─ absent ─────────────────────────────→ RETIRED (zero syscalls)
//	        ├─ boot changed ───────────────────────→ VOID
//	        ├─ predicate mismatch ─────────────────→ REFUSED_IDENTITY (zero syscalls)
//	        ├─ server alive/unproven, no EjectNow ─→ ORDINARY(MNT_NOBLOCK)
//	        │      EBUSY → BACKOFF 2s→30s (or WAIT_NESTED 30s) → VERIFY, forever
//	        └─ server proven dead/wedged, or EjectNow
//	               descendant present → WAIT_NESTED (issue nothing) → VERIFY
//	               two consecutive descendant-free snapshots → FORCE(MNT_FORCE)
//	any call outliving Watch → IN_KERNEL: issue NOTHING for that fsid until the
//	call returns; poll snapshots; absent → RETIRED.
//	success → VERIFY_ABSENT (re-snapshot, anomaly check) → RETIRED.
//
// Why a proven-dead server goes straight to MNT_FORCE: an ordinary unmount
// sets MNT_LUNMOUNT and then blocks in VFS_SYNC(MNT_WAIT) against the dead
// server (vfs_syscalls.c:2737,2783). While that flag is set every later
// unmount, forced or not, returns EBUSY (vfs_syscalls.c:2722). One ordinary
// attempt on a dead server therefore wedges convergence permanently. A
// forced unmount skips the sync (forcedunmount != 0).
//
// IN_KERNEL resolution. The design said both "issue nothing more while
// IN_KERNEL" and "on IN_KERNEL use MNT_FORCE". These conflict, and the kernel
// decides: during the in-flight call MNT_LUNMOUNT is set, so a MNT_FORCE can
// only return EBUSY. The rule is therefore:
//   - while a call is IN_KERNEL, issue nothing for that fsid (enforced
//     service-wide by Service.inflight, across jobs);
//   - when it returns EBUSY/error, re-verify and choose the flavor from the
//     CURRENT request: MNT_FORCE only if the owner has since proven the server
//     dead/wedged or the user said "Eject now". IN_KERNEL alone is reported
//     (Outcome.StuckObserved) but is not proof: a large healthy flush can
//     outlive the watch, and forcing it would drop data that was still
//     reaching the server.
func (s *Service) converge(j *job) Outcome {
	r, caller := j.receipt, j.caller
	set := func(st State, reason string) {
		j.update(func(o *Outcome) { o.State, o.Reason = st, reason })
	}
	finish := func(st State, reason string, err error) Outcome {
		j.update(func(o *Outcome) { o.State, o.Reason, o.Err = st, reason, err })
		return j.snapshot()
	}
	wait := func(d time.Duration) bool {
		// A withdrawn job does not sit out a backoff (up to BackoffMax) or a
		// nested-mount retry: the loop top ends it now.
		if j.isCancelled() {
			return true
		}
		select {
		case <-s.clock.After(d):
			return true
		case <-j.wake:
			return true
		case <-s.base.Done():
			return false
		}
	}
	retire := func(reason string) Outcome {
		err := s.store.Delete(r.UID, r.OpID)
		return finish(StateRetired, reason, err)
	}
	void := func() Outcome {
		err := s.store.Delete(r.UID, r.OpID)
		return finish(StateVoid, "boot changed; kernel mounts do not survive reboot", err)
	}

	cleanStreak := 0
	zeroPresent := 0
	backoff := s.pol.BackoffMin
	var pending *kcall
	var pre Snapshot

	// handle consumes the result of one of OUR unmount calls.
	handle := func(c *kcall) (Outcome, bool) {
		switch {
		case c.err == nil:
			set(StateVerifyAbsent, "unmount returned success")
			post, err := s.snapshot()
			if err != nil {
				set(StateVerifyAbsent, "post-unmount snapshot failed: "+err.Error())
				return Outcome{}, false
			}
			v := Verify(r, caller, post)
			switch v.Kind {
			case VerdictVoid:
				return void(), true
			case VerdictAbsent:
				if a := vanished(pre, post, r.FSID); len(a) > 0 {
					j.update(func(o *Outcome) { o.Anomaly = a })
					s.logf("unmountd: post_state_anomaly op=%s fsid=%s vanished=%d", r.OpID, r.FSID, len(a))
				}
				return retire("fsid absent after unmount"), true
			}
			// Success with the fsid still present: the kernel did not do what
			// it said (an fs-specific vfs_sysctl may have intercepted
			// VFS_CTL_UMOUNT, vfs_subr.c:4801). Back off; after MaxZeroPresent
			// in a row stop and surface it rather than spin.
			zeroPresent++
			s.logf("unmountd: post_state_anomaly op=%s fsid=%s: success but present (%d)", r.OpID, r.FSID, zeroPresent)
			if zeroPresent >= s.pol.MaxZeroPresent {
				return finish(StateAnomaly, fmt.Sprintf("unmount returned success %d times but the fsid is still present", zeroPresent), ErrPostState), true
			}
			set(StateBackoff, "unmount returned success but the fsid is still present")
			d := backoff
			backoff *= 2
			if backoff > s.pol.BackoffMax {
				backoff = s.pol.BackoffMax
			}
			if !wait(d) {
				return finish(StateBackoff, "service closed", s.base.Err()), true
			}
			return Outcome{}, false
		case isErrno(c.err, syscall.ENOENT):
			zeroPresent = 0
			// The kernel found no such fsid; the next VERIFY confirms absence.
			return Outcome{}, false
		case isErrno(c.err, syscall.EBUSY):
			zeroPresent = 0
			s.refreshHolders(j, caller, r.FSID)
			set(StateBackoff, fmt.Sprintf("EBUSY (flags %#x)", c.flags))
			d := backoff
			backoff *= 2
			if backoff > s.pol.BackoffMax {
				backoff = s.pol.BackoffMax
			}
			if !wait(d) {
				return finish(StateBackoff, "service closed", s.base.Err()), true
			}
			return Outcome{}, false
		default:
			return finish(StateFailed, "unmount syscall failed", c.err), true
		}
	}

	for {
		if s.base.Err() != nil {
			return finish(j.snapshot().State, "service closed", s.base.Err())
		}

		// Never issue while any unmount for this fsid is in the kernel.
		if c := s.inflightFor(r.FSID); c != nil || pending != nil {
			if c == nil {
				c = pending
			}
			set(StateInKernel, "an unmount call for this fsid has not returned")
			select {
			case <-c.done:
				if c == pending {
					pending = nil
					if out, done := handle(c); done {
						return out
					}
				}
				continue
			case <-s.clock.After(s.pol.InKernelPoll):
				snap, err := s.snapshot()
				if err != nil {
					continue
				}
				switch Verify(r, caller, snap).Kind {
				case VerdictAbsent:
					return retire("fsid absent while the call was in the kernel")
				case VerdictVoid:
					return void()
				}
				continue
			case <-j.wake:
				continue // escalation cannot act until the call returns
			case <-s.base.Done():
				continue
			}
		}

		if j.isCancelled() {
			return finish(StateCancelled, "ordinary retirement withdrawn by the owner", nil)
		}
		set(StateVerify, "")
		snap, err := s.snapshot()
		if err != nil {
			set(StateVerify, "snapshot failed: "+err.Error())
			if !wait(s.pol.ErrorRetry) {
				return finish(StateVerify, "service closed", s.base.Err())
			}
			continue
		}
		v := Verify(r, caller, snap)
		switch v.Kind {
		case VerdictVoid:
			return void()
		case VerdictAbsent:
			return retire("fsid absent")
		case VerdictRefused:
			// No syscall. The receipt stays; a later request re-verifies on a
			// new snapshot.
			return finish(StateRefusedIdentity, v.Reason, ErrIdentityRefused)
		}
		j.update(func(o *Outcome) { o.Descendants = v.Descendants })

		req := j.request()
		var flags int
		if req.wantsForce() {
			if len(v.Descendants) > 0 {
				// MNT_FORCE would recursively remove the nested mount. Our
				// server keeps serving (or is dead and its data already lost);
				// wait for the nested mount to go.
				cleanStreak = 0
				set(StateWaitNested, fmt.Sprintf("%d mount(s) nested inside ours", len(v.Descendants)))
				if !wait(s.pol.NestedRetry) {
					return finish(StateWaitNested, "service closed", s.base.Err())
				}
				continue
			}
			cleanStreak++
			if cleanStreak < 2 {
				set(StateForceConfirm, "first descendant-free snapshot")
				if !wait(s.pol.ForceConfirmGap) {
					return finish(StateForceConfirm, "service closed", s.base.Err())
				}
				continue
			}
			cleanStreak = 0
			flags = FlagForce
			set(StateForce, "")
		} else {
			cleanStreak = 0
			flags = FlagNoBlock
			set(StateOrdinary, "")
		}

		if s.beforeIssue != nil {
			s.beforeIssue(r.OpID, flags)
		}
		c, started, escalated := s.issueFor(j, flags)
		if escalated {
			// A join proved death (or said "Eject now") after this pass chose
			// an ordinary call. Re-verify and take the force path instead.
			cleanStreak = 0
			continue
		}
		if !started {
			continue // another job's call is in the kernel; loop top waits
		}
		pre = snap
		j.update(func(o *Outcome) {
			o.Syscalls++
			if flags&FlagForce != 0 {
				o.Forced = true
			}
		})
		select {
		case <-c.done:
			if flags == FlagNoBlock && len(v.Descendants) > 0 && isErrno(c.err, syscall.EBUSY) {
				// Nested mount while serving: retry ordinary at the slow cadence.
				s.refreshHolders(j, caller, r.FSID)
				set(StateWaitNested, fmt.Sprintf("%d mount(s) nested inside ours", len(v.Descendants)))
				if !wait(s.pol.NestedRetry) {
					return finish(StateWaitNested, "service closed", s.base.Err())
				}
				continue
			}
			if out, done := handle(c); done {
				return out
			}
		case <-s.clock.After(s.pol.Watch):
			pending = c
			j.update(func(o *Outcome) { o.StuckObserved = true })
		case <-s.base.Done():
			pending = c
		}
	}
}

// vanished returns entries in pre (other than ours) whose fsid is absent from
// post. MNT_FORCE is only issued with no descendants, so any such entry is an
// anomaly, not collateral we chose.
func vanished(pre, post Snapshot, ours FSID) []Entry {
	have := map[FSID]bool{}
	for _, e := range post.Entries {
		have[e.FSID] = true
	}
	var out []Entry
	for _, e := range pre.Entries {
		if e.FSID != ours && !have[e.FSID] {
			out = append(out, e)
		}
	}
	return out
}

func (s *Service) refreshHolders(j *job, caller Caller, fsid FSID) {
	hs, stale := s.listHolders(caller, fsid)
	j.update(func(o *Outcome) {
		o.HoldersStale = stale
		if !stale {
			o.Holders = hs
		}
	})
}
