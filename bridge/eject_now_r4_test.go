package main

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/nfscustody"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// R4-B1 (kills N31): the DELETE lands AFTER the floor check read the busy
// window (so the check passes) and BEFORE the force call's arguments are
// evaluated. Only the generation read on ARRIVAL refuses the force here.
func TestProbeR4DeleteBetweenFloorCheckAndForceCall(t *testing.T) {
	now := fakeEjectClock(t)
	k, core := raceCore(t)
	b := &busyHookNFS{coreBackedNFS: core}
	publishNFSController(t, b)
	if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d %v", code, body)
	}
	*now = now.Add(31 * time.Second)
	armed, fired := false, false
	var delCode int
	b.onBusy = func() { armed = true }
	base := ejectNowNow
	ejectNowNow = func() time.Time {
		if armed && !fired {
			fired = true
			delCode, _ = doEjectNow(t, http.MethodDelete, true)
		}
		return base()
	}
	code, body := doEjectNow(t, http.MethodPost, true)
	ejectNowNow = base
	_, present := k.state()
	if !fired || delCode != http.StatusOK || code == http.StatusOK || !present {
		t.Fatalf("fired=%v DELETE -> %d; POST -> %d %v; present=%v (want DELETE 200, no force, volume kept)", fired, delCode, code, body, present)
	}
}

// R4-B2 (review #4, I4, e2e): POST A crosses DELETE D; POST B (a new Quit)
// arrives after D and is answered 202 (its ordinary job runs). A's second
// Cancel must not kill B's job: once the apps let go, B's volume ejects.
func TestProbeR4CrossedPostSwallowsNewerPostE2E(t *testing.T) {
	k, core := raceCore(t)
	publishNFSController(t, core)
	if code, _ := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d", code)
	}
	var delCode, bCode int
	var bBody map[string]any
	n := 0
	core.beforeHelper = func(rr client.RetireRequest) {
		n++
		if n == 1 { // A, admitted at the old generation, about to reach the helper
			delCode, _ = doEjectNow(t, http.MethodDelete, true)
			bCode, bBody = doEjectNow(t, http.MethodPost, true) // B: new request
		}
	}
	aCode, aBody := doEjectNow(t, http.MethodPost, true)
	core.beforeHelper = nil
	k.mu.Lock()
	k.busy = false
	k.mu.Unlock()
	time.Sleep(400 * time.Millisecond)
	_, present := k.state()
	t.Logf("DELETE -> %d; B -> %d %v; A -> %d %v; present after apps let go=%v", delCode, bCode, bBody, aCode, aBody, present)
	if bCode == http.StatusAccepted && present {
		t.Fatalf("SWALLOWED: B was answered 202 (ordinary eject running) but A's re-withdraw cancelled B's job; volume still mounted after the apps let go")
	}
}

// R4-B3 (review #4, I7): an internal withdrawal (Stop giving up ->
// CancelHelperRetire, or the stale retirer giving up) lands just before the
// user's Eject-now force. It is JuiceMount's own withdrawal, not the user's:
// the user's force still runs, and no reply claims Quit was cancelled.
func TestProbeR4InternalWithdrawClaimsQuitCancelled(t *testing.T) {
	now := fakeEjectClock(t)
	k, core := raceCore(t)
	r := &racingNFS{coreBackedNFS: core}
	publishNFSController(t, r)
	if code, _ := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d", code)
	}
	*now = now.Add(31 * time.Second)
	fired := false
	r.before = func(rr client.RetireRequest) {
		if rr.EjectNow && !fired {
			fired = true
			core.CancelHelperRetire() // e.g. a concurrent Stop that gave up
		}
	}
	code, body := doEjectNow(t, http.MethodPost, true)
	msg, _ := body["error"].(string)
	t.Logf("user POST -> %d %v", code, body)
	if strings.Contains(msg, "Quit was cancelled") {
		t.Fatalf("I7: the user's Eject-now POST answered %q although the user never cancelled", msg)
	}
	_, present := k.state()
	if !fired || code != http.StatusOK || body["state"] != "RETIRED" || present {
		t.Fatalf("fired=%v POST -> %d %v present=%v: the user's force must still run after an internal withdrawal", fired, code, body, present)
	}
}

// R5: an internal withdrawal ends the ordinary job a user POST is waiting
// on. The POST is not refused and does not blame the user: it answers 202
// with retry_after 1 and says JuiceMount stopped its own attempt. The next
// POST starts a fresh job, which ejects once the apps let go.
func TestEjectNowInternalWithdrawInterruptsTruthfully(t *testing.T) {
	k, core := raceCore(t)
	old := ejectNowOrdinaryWait
	ejectNowOrdinaryWait = 2 * time.Second
	t.Cleanup(func() { ejectNowOrdinaryWait = old })
	publishNFSController(t, core)
	done := make(chan struct{})
	core.beforeHelper = func(rr client.RetireRequest) {
		go func() {
			defer close(done)
			time.Sleep(100 * time.Millisecond) // the POST is waiting in the helper
			core.CancelHelperRetire()
		}()
	}
	code, body := doEjectNow(t, http.MethodPost, true)
	core.beforeHelper = nil
	<-done
	msg, _ := body["error"].(string)
	t.Logf("POST -> %d %v", code, body)
	if code != http.StatusAccepted || body["state"] != "ordinary_in_progress" || body["retry_after"] != float64(1) ||
		strings.Contains(msg, "Quit was cancelled") || !strings.Contains(msg, "JuiceMount stopped its own eject attempt") {
		t.Fatalf("POST whose job an internal withdrawal ended: %d %v", code, body)
	}
	if since, _ := core.OrdinaryBusy(); !since.IsZero() {
		t.Fatalf("interrupted POST armed the busy window at %v", since)
	}
	ejectNowOrdinaryWait = 100 * time.Millisecond
	if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("re-POST: %d %v", code, body)
	}
	k.mu.Lock()
	k.busy = false
	k.mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, present := k.state(); !present {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the re-POST's job never ejected the volume after the apps let go")
}

// R5: the handler's reply for an interrupted ordinary step (fake controller).
func TestEjectNowInterruptedReplyText(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, ordinaryErr: nfscustody.ErrHelperRetireInterrupted}
	publishNFSController(t, ctl)
	code, body := doEjectNow(t, http.MethodPost, true)
	msg, _ := body["error"].(string)
	if code != http.StatusAccepted || body["state"] != "ordinary_in_progress" || body["retry_after"] != float64(1) ||
		strings.Contains(msg, "Quit was cancelled") || forced(ctl.calls) != 0 {
		t.Fatalf("interrupted POST: %d %v calls=%+v", code, body, ctl.calls)
	}
}
