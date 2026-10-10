package unmountd

import "fmt"

// SweepReport is the launch sweep's result.
type SweepReport struct {
	Voided  []Receipt // other-boot receipts, deleted
	Retired []Receipt // same-boot receipts whose fsid is already absent, deleted
	Pending []Receipt // same-boot receipts still mounted: arm Disconnected for each
	Errors  []error
}

// SweepAtLaunch runs when the daemon starts (RunAtLoad). It deletes receipts
// from other boots, retires same-boot receipts whose fsid is gone, and
// returns the still-mounted same-boot ones. It issues no unmount syscall:
// the transport arms Disconnected for each pending receipt so its owner has
// the reclaim window to reconnect first.
func (s *Service) SweepAtLaunch() (SweepReport, error) {
	var rep SweepReport
	snap, err := s.snapshotJoin()
	if err != nil {
		return rep, err
	}
	uids, err := s.store.UIDs()
	if err != nil {
		return rep, err
	}
	for _, uid := range uids {
		rs, err := s.store.List(uid)
		if err != nil {
			rep.Errors = append(rep.Errors, err)
			continue
		}
		for _, r := range rs {
			v := Verify(r, Caller{UID: r.UID, Role: RoleService}, snap)
			switch v.Kind {
			case VerdictVoid:
				if err := s.store.Delete(r.UID, r.OpID); err != nil {
					rep.Errors = append(rep.Errors, err)
				}
				rep.Voided = append(rep.Voided, r)
			case VerdictAbsent:
				if err := s.store.Delete(r.UID, r.OpID); err != nil {
					rep.Errors = append(rep.Errors, err)
				}
				rep.Retired = append(rep.Retired, r)
			default:
				rep.Pending = append(rep.Pending, r)
			}
		}
	}
	return rep, nil
}

// Disconnected is called by the transport when the owner's per-receipt
// connection drops. If no Reclaim arrives within Policy.OrphanReclaim the
// helper retires the receipt on its own, with the liveness the transport
// could prove (LivenessUnknown → ordinary unmount only, never a force),
// unless the owner's last request for the op was an accepted Cancel
// (isWithdrawn): a withdrawn op is never restarted by the timer.
func (s *Service) Disconnected(uid uint32, opID string, liveness Liveness) error {
	if _, err := s.store.Get(uid, opID); err != nil {
		return err
	}
	key := jobKey{uid, opID}
	cancel := make(chan struct{})
	s.mu.Lock()
	if old, ok := s.orphans[key]; ok {
		close(old)
	}
	s.orphans[key] = cancel
	s.mu.Unlock()
	go func() {
		select {
		case <-cancel:
			return
		case <-s.base.Done():
			return
		case <-s.clock.After(s.pol.OrphanReclaim):
		}
		if s.orphanFired != nil {
			s.orphanFired(key.uid, key.op)
		}
		// The owner's last request was a Cancel (e.g. "Cancel Quit", or a
		// Stop that gave up): it chose to keep the mount. A dropped
		// connection is not a new request, so nothing is restarted. Read
		// before the lock (it may touch the store).
		withdrawn := s.isWithdrawn(key)
		// The timer fired, but a Reclaim or a re-arm may have won the race
		// for the lock. Only the still-current timer may act.
		s.mu.Lock()
		if s.orphans[key] != cancel {
			s.mu.Unlock()
			return
		}
		delete(s.orphans, key)
		if withdrawn {
			// Not restarted, but not forgotten either: the owner is gone, so
			// the op stays reclaimable (see parked). Swapped under the same
			// lock as the timer entry, so a Reclaim never sees neither.
			s.parked[key] = true
		}
		s.mu.Unlock()
		if withdrawn {
			s.logf("unmountd: orphan uid=%d op=%s: owner withdrew its retirement; not restarted, parked for a successor", uid, opID)
			return
		}
		orphanCaller := Caller{UID: uid, Role: RoleService, SigningID: "unmountd-orphan-sweep"}
		if _, err := s.startOrJoin(orphanCaller, opID, RetireRequest{Liveness: liveness}); err != nil {
			s.logf("unmountd: orphan retire uid=%d op=%s: %v", uid, opID, err)
		}
	}()
	return nil
}

// Reclaim cancels a pending orphan retirement, or takes over a parked one
// (a withdrawn op whose timer already fired with no owner connected). It
// reports true in both cases: the op had no owner and the caller now holds
// it. The transport calls it when the same signed service reconnects with
// the same uid and op id.
func (s *Service) Reclaim(caller Caller, opID string) (bool, error) {
	if caller.Role != RoleService {
		return false, ErrNotAuthorized
	}
	key := jobKey{caller.UID, opID}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.orphans[key]; ok {
		close(c)
		delete(s.orphans, key)
		return true, nil
	}
	if s.parked[key] {
		delete(s.parked, key)
		return true, nil
	}
	return false, nil
}

// String renders a short report line.
func (r SweepReport) String() string {
	return fmt.Sprintf("voided=%d retired=%d pending=%d errors=%d", len(r.Voided), len(r.Retired), len(r.Pending), len(r.Errors))
}
