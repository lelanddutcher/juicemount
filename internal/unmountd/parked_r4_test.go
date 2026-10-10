package unmountd

import (
	"testing"
	"time"
)

func parkOne(t *testing.T, op string) (*fakeKernel, *Service) {
	t.Helper()
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	svc := slowBackoffService(t, k)
	mustRegister(t, svc, nfsRegister(op))
	_, _ = retire(t, svc, op, RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond)
	if !waitFor(t, 2*time.Second, stateIs(svc, op, StateBackoff)) {
		t.Fatal("no BACKOFF")
	}
	if _, err := svc.Cancel(service(), op); err != nil {
		t.Fatal(err)
	}
	k.mu.Lock()
	k.busy[ourFSID] = false
	k.mu.Unlock()
	if err := svc.Disconnected(testUID, op, LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * svc.pol.OrphanReclaim)
	return k, svc
}

// A parked op is keyed by uid: another uid's Reclaim of the same op id gets
// false and leaves it parked for the real owner's successor.
func TestProbeR4ParkedReclaimOtherUID(t *testing.T) {
	k, svc := parkOne(t, "op-p")
	other := service()
	other.UID = testUID + 1
	if got, err := svc.Reclaim(other, "op-p"); got || err != nil {
		t.Fatalf("other uid reclaimed a parked op: %v %v", got, err)
	}
	if got, _ := svc.Reclaim(service(), "op-p"); !got {
		t.Fatal("owner uid could not reclaim after another uid tried")
	}
	if c := flagsOf(k.recorded()); c != "N" {
		t.Fatalf("unexpected syscalls: %q", c)
	}
}

// A parked op re-armed by a second Disconnected (as a launch sweep would arm
// it) and left to fire again never unmounts, stays reclaimable, and does
// not keep the daemon from going idle.
func TestProbeR4ParkedRearmNeverUnmounts(t *testing.T) {
	k, svc := parkOne(t, "op-q")
	if err := svc.Disconnected(testUID, "op-q", LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	if svc.idle() {
		t.Fatal("pending timer must keep the daemon up")
	}
	time.Sleep(5 * svc.pol.OrphanReclaim)
	if c := flagsOf(k.recorded()); c != "N" || !entryPresent(k) {
		t.Fatalf("re-armed parked op unmounted without a request: %q", c)
	}
	if !svc.idle() {
		t.Fatal("parked op blocks idle")
	}
	if got, _ := svc.Reclaim(service(), "op-q"); !got {
		t.Fatal("not reclaimable after re-arm")
	}
	// Reclaim itself issues nothing.
	time.Sleep(3 * svc.pol.OrphanReclaim)
	if c := flagsOf(k.recorded()); c != "N" || !entryPresent(k) {
		t.Fatalf("reclaim started an unmount: %q", c)
	}
}

// A parked op whose mount vanished out-of-band: the next Retire (the
// successor) sees absence and retires without a syscall; the parked entry is
// cleared by the terminal result.
func TestProbeR4ParkedThenVanished(t *testing.T) {
	k, svc := parkOne(t, "op-v")
	k.mu.Lock()
	k.snap.Entries = k.snap.Entries[:1]
	k.mu.Unlock()
	if got, _ := svc.Reclaim(service(), "op-v"); !got {
		t.Fatal("not reclaimable")
	}
	out, err := retire(t, svc, "op-v", RetireRequest{Liveness: LivenessUnknown}, 2*time.Second)
	if err != nil || out.State != StateRetired {
		t.Fatalf("%+v %v", out, err)
	}
	svc.mu.Lock()
	n := len(svc.parked)
	svc.mu.Unlock()
	if n != 0 {
		t.Fatalf("parked leaked: %d", n)
	}
}
