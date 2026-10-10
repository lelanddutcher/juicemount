package main

// Review #3 finding 2: DELETE /eject-now against a POST already in flight.
// The interleavings are forced with hooks on the e2e model (real handler,
// real nfscustody.HelperRetireGate, real helper core).

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// racingNFS runs a hook when the handler calls the controller, before the
// gate admits the request.
type racingNFS struct {
	*coreBackedNFS
	before func(rr client.RetireRequest)
}

func (r *racingNFS) RetireWithHelperAt(ctx context.Context, gen uint64, rr client.RetireRequest, wait time.Duration) (client.Outcome, error) {
	if r.before != nil {
		r.before(rr)
	}
	return r.coreBackedNFS.RetireWithHelperAt(ctx, gen, rr, wait)
}

func raceCore(t *testing.T) (*e2eKernel, *coreBackedNFS) {
	t.Helper()
	useFakeServiceHelper(t, &fakeServiceHelper{})
	oldWait := ejectNowOrdinaryWait
	ejectNowOrdinaryWait = 100 * time.Millisecond
	t.Cleanup(func() { ejectNowOrdinaryWait = oldWait })
	k := &e2eKernel{busy: true, there: true}
	st, err := unmountd.OpenStore(filepath.Join(t.TempDir(), "unmountd"), 0)
	if err != nil {
		t.Fatal(err)
	}
	pol := unmountd.DefaultPolicy()
	pol.BackoffMin, pol.BackoffMax = 5*time.Millisecond, 20*time.Millisecond
	pol.SnapshotWatch, pol.Watch, pol.InKernelPoll = time.Second, time.Second, 20*time.Millisecond
	pol.ForceConfirmGap = 5 * time.Millisecond
	svc, err := unmountd.NewService(unmountd.Config{Store: st, Kernel: k, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	ctl := &coreBackedNFS{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, svc: svc, op: "nfs-" + e2eToken}
	if _, err := svc.Register(ctl.caller(), unmountd.RegisterRequest{OpID: ctl.op, Type: unmountd.TypeNFS, FSID: e2eFSID,
		Token: e2eToken, MntOnName: "/Volumes/zpool", MntFromName: e2eSource}); err != nil {
		t.Fatal(err)
	}
	return k, ctl
}

// busyHookNFS runs a hook when the handler reads the busy window, i.e.
// between the POST's ordinary pass and its force step.
type busyHookNFS struct {
	*coreBackedNFS
	onBusy func()
}

func (b *busyHookNFS) OrdinaryBusy() (time.Time, time.Time) {
	if b.onBusy != nil {
		b.onBusy()
	}
	return b.coreBackedNFS.OrdinaryBusy()
}

// PB1 at the floor check: a DELETE that lands after the POST's ordinary pass
// clears the busy window, so the floor check itself refuses the force (202);
// the generation read on arrival covers the remaining gap after the check.
// Either way nothing is forced and the volume stays.
func TestEjectNowDeleteAtFloorCheckWins(t *testing.T) {
	now := fakeEjectClock(t)
	k, core := raceCore(t)
	b := &busyHookNFS{coreBackedNFS: core}
	publishNFSController(t, b)
	if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d %v", code, body)
	}
	*now = now.Add(31 * time.Second)
	var delCode int
	b.onBusy = func() {
		if delCode == 0 {
			delCode, _ = doEjectNow(t, http.MethodDelete, true)
		}
	}
	code, body := doEjectNow(t, http.MethodPost, true)
	_, present := k.state()
	if delCode != http.StatusOK || code == http.StatusOK || !present {
		t.Fatalf("DELETE -> %d; POST -> %d %v; present=%v (want DELETE 200, no force, volume kept)", delCode, code, body, present)
	}
}

// PB1: the DELETE lands between the POST's floor check and its EjectNow
// retire. The DELETE wins: 200 withdrawn, the POST forces nothing (409
// withdrawn), the volume stays.
func TestEjectNowDeleteBeforeForceAdmissionWins(t *testing.T) {
	now := fakeEjectClock(t)
	k, core := raceCore(t)
	r := &racingNFS{coreBackedNFS: core}
	publishNFSController(t, r)
	if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d %v", code, body)
	}
	*now = now.Add(31 * time.Second)
	var delCode int
	var delBody map[string]any
	r.before = func(rr client.RetireRequest) {
		if rr.EjectNow && delCode == 0 {
			delCode, delBody = doEjectNow(t, http.MethodDelete, true)
		}
	}
	code, body := doEjectNow(t, http.MethodPost, true)
	_, present := k.state()
	if delCode != http.StatusOK || code != http.StatusConflict || body["state"] != "withdrawn" || !present {
		t.Fatalf("DELETE -> %d %v; in-flight POST -> %d %v; present=%v (want DELETE 200, POST 409 withdrawn, volume kept)",
			delCode, delBody, code, body, present)
	}
}

// PB1, the other order: the force was already sent when the DELETE lands.
// The DELETE must say so (409 force_in_progress), never 200 withdrawn.
func TestEjectNowDeleteAfterForceSentIsRefused(t *testing.T) {
	now := fakeEjectClock(t)
	k, core := raceCore(t)
	publishNFSController(t, core)
	if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d %v", code, body)
	}
	*now = now.Add(31 * time.Second)
	var delCode int
	var delBody map[string]any
	core.beforeHelper = func(rr client.RetireRequest) {
		if rr.EjectNow && delCode == 0 {
			delCode, delBody = doEjectNow(t, http.MethodDelete, true)
		}
	}
	code, body := doEjectNow(t, http.MethodPost, true)
	_, present := k.state()
	if delCode != http.StatusConflict || delBody["state"] != "force_in_progress" || code != http.StatusOK || present {
		t.Fatalf("DELETE -> %d %v; POST -> %d %v; present=%v (want DELETE 409 force_in_progress, POST 200, volume gone)",
			delCode, delBody, code, body, present)
	}
}

// PB2: the DELETE lands before an in-flight ordinary POST is admitted. The
// POST must start no job: once the apps let go the volume is still there.
func TestEjectNowDeleteBeforeOrdinaryAdmissionStartsNoJob(t *testing.T) {
	k, core := raceCore(t)
	r := &racingNFS{coreBackedNFS: core}
	publishNFSController(t, r)
	if code, _ := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d", code)
	}
	var delCode int
	r.before = func(rr client.RetireRequest) {
		if !rr.EjectNow && delCode == 0 {
			delCode, _ = doEjectNow(t, http.MethodDelete, true)
		}
	}
	code, body := doEjectNow(t, http.MethodPost, true)
	r.before = nil
	k.mu.Lock()
	k.busy = false // apps close their files; JuiceMount is still running
	k.mu.Unlock()
	time.Sleep(400 * time.Millisecond)
	_, present := k.state()
	if delCode != http.StatusOK || code != http.StatusConflict || body["state"] != "withdrawn" || !present {
		t.Fatalf("DELETE -> %d; racing POST -> %d %v; present after apps let go=%v", delCode, code, body, present)
	}
}

// PB2, the other order: the ordinary POST was admitted and reaches the
// helper AFTER the DELETE's Cancel, starting a fresh job. On return it must
// withdraw that job again and not re-arm the busy window.
func TestEjectNowOrdinaryCrossingDeleteIsWithdrawnAgain(t *testing.T) {
	k, core := raceCore(t)
	publishNFSController(t, core)
	if code, _ := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d", code)
	}
	var delCode int
	core.beforeHelper = func(rr client.RetireRequest) {
		if !rr.EjectNow && delCode == 0 {
			delCode, _ = doEjectNow(t, http.MethodDelete, true)
		}
	}
	code, body := doEjectNow(t, http.MethodPost, true)
	core.beforeHelper = nil
	if since, last := core.OrdinaryBusy(); !since.IsZero() || !last.IsZero() {
		t.Fatalf("busy window re-armed by a POST the DELETE overtook: %v..%v", since, last)
	}
	k.mu.Lock()
	k.busy = false
	k.mu.Unlock()
	time.Sleep(400 * time.Millisecond)
	_, present := k.state()
	if delCode != http.StatusOK || code != http.StatusConflict || body["state"] != "withdrawn" || !present {
		t.Fatalf("DELETE -> %d; crossing POST -> %d %v; present after apps let go=%v", delCode, code, body, present)
	}
}

// Finding 3 at the route: DELETE after the ordinary job already ejected the
// volume answers 200 RETIRED (already ejected), never 409 force_in_progress.
func TestEjectNowDeleteAfterOrdinaryRetireSaysRetired(t *testing.T) {
	k, core := raceCore(t)
	publishNFSController(t, core)
	if code, _ := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d", code)
	}
	k.mu.Lock()
	k.busy = false // the ordinary job's next pass unmounts
	k.mu.Unlock()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, present := k.state(); !present || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	code, body := doEjectNow(t, http.MethodDelete, true)
	if code != http.StatusOK || body["state"] != "RETIRED" {
		t.Fatalf("DELETE after the job retired: %d %v (want 200 RETIRED)", code, body)
	}
}
