package main

// Round 7: POST /eject-now carries a phase. A START request (the busy-Quit
// countdown began) never forces and restarts the force floor at its arrival;
// only an ESCALATE request (the countdown ended, or "Eject now") may force,
// and only once the floor has passed. A POST without a phase is a start.

import (
	"net/http"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

func (k *e2eKernel) forcedCalls() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	n := 0
	for _, f := range k.calls {
		if f&unmountd.FlagForce != 0 {
			n++
		}
	}
	return n
}

// The measured round-6 carry-over, end to end (real handler, real gate, real
// helper core): an earlier panel's escalated POSTs left a busy window that
// nobody withdrew (the panel died, or its DELETE got no answer). A new
// panel's START POST 40 s later must not force: the user is shown a fresh
// 30 s countdown with a Cancel button.
func TestEjectNowR7StartAfterUnwithdrawnWindowNeverForcesE2E(t *testing.T) {
	now := fakeEjectClock(t)
	k, core := raceCore(t)
	publishNFSController(t, core)
	t0 := *now
	for _, at := range []time.Duration{0, 20 * time.Second} {
		*now = t0.Add(at)
		if code, body := doEjectNowPhase(t, "escalate"); code != http.StatusAccepted {
			t.Fatalf("old panel POST at +%v: %d %v", at, code, body)
		}
	}
	*now = t0.Add(40 * time.Second)
	code, body := doEjectNowPhase(t, "start")
	if code != http.StatusAccepted || body["state"] != "ordinary_in_progress" || k.forcedCalls() != 0 {
		t.Fatalf("CARRY-OVER: new panel's start POST at +40 s -> %d %v, forced unmounts %d (want 202, none)", code, body, k.forcedCalls())
	}
	if _, present := k.state(); !present {
		t.Fatal("volume gone after a start POST")
	}
	// The user presses "Eject now" at once: still inside the new floor.
	*now = t0.Add(41 * time.Second)
	if code, body := doEjectNowPhase(t, "escalate"); code != http.StatusAccepted || k.forcedCalls() != 0 {
		t.Fatalf("escalate 1 s after the start -> %d %v, forced %d (want 202, none)", code, body, k.forcedCalls())
	}
	// The countdown ends 30 s after the start: now the force is allowed.
	*now = t0.Add(70 * time.Second)
	code, body = doEjectNowPhase(t, "escalate")
	if code != http.StatusOK || body["state"] != "RETIRED" || k.forcedCalls() == 0 {
		t.Fatalf("escalate at the end of the new countdown -> %d %v, forced %d (want 200 RETIRED, forced)", code, body, k.forcedCalls())
	}
}

// Start, then escalate before the floor (202, nothing forced), then escalate
// after it (forced).
func TestEjectNowR7EscalateWaitsForFloorFromStart(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	now := fakeEjectClock(t)
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, ordinaryErr: client.ErrPending, out: client.Outcome{State: "BACKOFF"}}
	// A met window from before this panel: the start must discard it.
	ctl.since, ctl.last = now.Add(-time.Minute), now.Add(-time.Second)
	publishNFSController(t, ctl)
	t0 := *now
	if code, body := doEjectNowPhase(t, "start"); code != http.StatusAccepted || body["retry_after"] != float64(30) || forced(ctl.calls) != 0 {
		t.Fatalf("start: %d %v forced=%d", code, body, forced(ctl.calls))
	}
	*now = t0.Add(10 * time.Second)
	if code, body := doEjectNowPhase(t, "escalate"); code != http.StatusAccepted || body["retry_after"] != float64(20) || forced(ctl.calls) != 0 {
		t.Fatalf("escalate before the floor: %d %v forced=%d (want 202 retry_after 20)", code, body, forced(ctl.calls))
	}
	*now = t0.Add(30 * time.Second)
	code, body := doEjectNowPhase(t, "escalate")
	if code != http.StatusOK || body["state"] != "RETIRED" || forced(ctl.calls) != 1 {
		t.Fatalf("escalate after the floor: %d %v forced=%d (want 200, one force)", code, body, forced(ctl.calls))
	}
	if ctl.starts != 1 {
		t.Fatalf("start requests seen by the controller: %d, want 1", ctl.starts)
	}
}

// A POST without a phase (a GUI from before round 7) is a start: it never
// forces, however long and recently the volume has been busy. An unknown
// phase is refused before anything reaches the helper.
func TestEjectNowR7NoPhaseNeverForces(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	now := fakeEjectClock(t)
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, ordinaryErr: client.ErrPending}
	ctl.since, ctl.last = now.Add(-time.Minute), now.Add(-time.Second)
	publishNFSController(t, ctl)
	for i, body := range []string{"", "{}", `{"other":1}`, "", "{}"} {
		*now = now.Add(40 * time.Second)
		code, reply := doEjectNowBody(t, http.MethodPost, true, body)
		if code != http.StatusAccepted || forced(ctl.calls) != 0 {
			t.Fatalf("POST %d with body %q: %d %v, forced=%d (want 202, never forced)", i, body, code, reply, forced(ctl.calls))
		}
	}
	if ctl.starts != 5 {
		t.Fatalf("phase-less POSTs handled as start: %d of 5", ctl.starts)
	}
	n := len(ctl.calls)
	for _, body := range []string{`{"phase":"force"}`, `{"phase":""}`, "not json"} {
		code, reply := doEjectNowBody(t, http.MethodPost, true, body)
		if code != http.StatusBadRequest || reply["state"] != "bad_request" {
			t.Fatalf("POST with body %q: %d %v, want 400 bad_request", body, code, reply)
		}
	}
	if len(ctl.calls) != n {
		t.Fatalf("a refused phase reached the helper: %+v", ctl.calls[n:])
	}
}

// The same with the real gate and helper core: phase-less POSTs spanning
// well over the floor never force.
func TestEjectNowR7NoPhaseNeverForcesE2E(t *testing.T) {
	now := fakeEjectClock(t)
	k, core := raceCore(t)
	publishNFSController(t, core)
	for i := 0; i < 5; i++ {
		code, body := doEjectNowPhase(t, "")
		if code != http.StatusAccepted || k.forcedCalls() != 0 {
			t.Fatalf("phase-less POST %d at +%ds: %d %v, forced %d", i, 20*i, code, body, k.forcedCalls())
		}
		*now = now.Add(20 * time.Second)
	}
	if _, present := k.state(); !present {
		t.Fatal("volume gone after phase-less POSTs only")
	}
}

// A start POST never forces even when its own ordinary pass outlasts the
// floor (a slow helper): the floor is met, but only an escalate may act on
// it.
func TestEjectNowR7SlowStartNeverForces(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	now := fakeEjectClock(t)
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, ordinaryErr: client.ErrPending}
	ctl.onOrdinary = func() { *now = now.Add(ejectNowFloor + 5*time.Second) }
	publishNFSController(t, ctl)
	for _, phase := range []string{"start", ""} {
		code, body := doEjectNowPhase(t, phase)
		if code != http.StatusAccepted || forced(ctl.calls) != 0 {
			t.Fatalf("slow start POST (phase %q): %d %v, forced=%d (want 202, never forced)", phase, code, body, forced(ctl.calls))
		}
	}
	// The same slow pass on an escalate does force: the floor is met.
	code, body := doEjectNowPhase(t, "escalate")
	if code != http.StatusOK || forced(ctl.calls) != 1 {
		t.Fatalf("slow escalate after a start: %d %v, forced=%d (want the force)", code, body, forced(ctl.calls))
	}
}
