package main

// Round 8 (review #7 follow-ups): the 30 s force floor at its exact
// boundary, the service side of the panel's lost-start rule, and the
// reviewer's probes adopted as regression tests.

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// Review #7, finding 2: the floor is exactly 30 s. An escalate 29 s after
// the start is still inside it (202, nothing forced); one at 30 s forces.
// The same holds when no start reached the service and the first busy
// escalate opened the window. Real handler, real gate, real helper core.
func TestEjectNowR8FloorBoundaryE2E(t *testing.T) {
	for _, opener := range []string{"start", "escalate"} {
		t.Run(opener, func(t *testing.T) {
			now := fakeEjectClock(t)
			k, core := raceCore(t)
			publishNFSController(t, core)
			t0 := *now
			if code, body := doEjectNowPhase(t, opener); code != http.StatusAccepted || k.forcedCalls() != 0 {
				t.Fatalf("%s at +0: %d %v forced %d (want 202, none)", opener, code, body, k.forcedCalls())
			}
			*now = t0.Add(ejectNowFloor - time.Second)
			if code, body := doEjectNowPhase(t, "escalate"); code != http.StatusAccepted || body["retry_after"] != float64(1) || k.forcedCalls() != 0 {
				t.Fatalf("escalate at +29 s: %d %v forced %d (want 202 retry_after 1, none)", code, body, k.forcedCalls())
			}
			*now = t0.Add(ejectNowFloor)
			code, body := doEjectNowPhase(t, "escalate")
			if code != http.StatusOK || body["state"] != "RETIRED" || k.forcedCalls() == 0 {
				t.Fatalf("escalate at +30 s: %d %v forced %d (want 200 RETIRED, forced)", code, body, k.forcedCalls())
			}
		})
	}
}

// Review #7, finding 1, inverted: an earlier panel left a busy window nobody
// withdrew, and the new panel's start POST never reached the service. The
// panel (EjectNowRequester) escalates only after a start was answered 202,
// so "Eject now" 1 s into the new countdown sends a START, not an escalate:
// nothing is forced, and the force comes only 30 s after that start.
func TestEjectNowR8LostStartPanelResendsStartE2E(t *testing.T) {
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
	// New panel starts at +40 s; its start POST is lost. "Eject now" at +41 s
	// re-sends the start.
	s := t0.Add(41 * time.Second)
	*now = s
	if code, body := doEjectNowPhase(t, "start"); code != http.StatusAccepted || k.forcedCalls() != 0 {
		t.Fatalf("re-sent start 1 s into the new countdown: %d %v forced %d (want 202, none)", code, body, k.forcedCalls())
	}
	*now = s.Add(ejectNowFloor - time.Second)
	if code, body := doEjectNowPhase(t, "escalate"); code != http.StatusAccepted || k.forcedCalls() != 0 {
		t.Fatalf("escalate 29 s after the re-sent start: %d %v forced %d (want 202, none)", code, body, k.forcedCalls())
	}
	*now = s.Add(ejectNowFloor)
	if code, body := doEjectNowPhase(t, "escalate"); code != http.StatusOK || k.forcedCalls() == 0 {
		t.Fatalf("escalate 30 s after the re-sent start: %d %v forced %d (want 200, forced)", code, body, k.forcedCalls())
	}
}

// Review #7 (RV7-B2): the earlier panel's DELETE landed, so nothing carries
// over even if the new panel's start is lost.
func TestRV7DeleteBetweenPanelsNoCarryOverE2E(t *testing.T) {
	now := fakeEjectClock(t)
	k, core := raceCore(t)
	publishNFSController(t, core)
	t0 := *now
	for _, at := range []time.Duration{0, 20 * time.Second} {
		*now = t0.Add(at)
		_, _ = doEjectNowPhase(t, "escalate")
	}
	*now = t0.Add(25 * time.Second)
	if code, body := doEjectNowBody(t, http.MethodDelete, true, ""); code >= 300 {
		t.Fatalf("DELETE: %d %v", code, body)
	}
	*now = t0.Add(41 * time.Second)
	code, body := doEjectNowPhase(t, "escalate")
	if code != http.StatusAccepted || k.forcedCalls() != 0 {
		t.Fatalf("lost start after a DELETE: %d %v forced %d (want 202, none)", code, body, k.forcedCalls())
	}
}

// Review #7 (RV7-B3): repeated starts end to end: the floor counts from the
// last start.
func TestRV7RepeatedStartE2E(t *testing.T) {
	now := fakeEjectClock(t)
	k, core := raceCore(t)
	publishNFSController(t, core)
	t0 := *now
	_, _ = doEjectNowPhase(t, "start")
	*now = t0.Add(25 * time.Second)
	_, _ = doEjectNowPhase(t, "start")
	*now = t0.Add(31 * time.Second)
	if code, body := doEjectNowPhase(t, "escalate"); code != http.StatusAccepted || k.forcedCalls() != 0 {
		t.Fatalf("escalate 6 s after the second start: %d %v forced %d", code, body, k.forcedCalls())
	}
	*now = t0.Add(56 * time.Second)
	if code, body := doEjectNowPhase(t, "escalate"); code != http.StatusOK || k.forcedCalls() == 0 {
		t.Fatalf("escalate 31 s after the second start: %d %v forced %d (want forced)", code, body, k.forcedCalls())
	}
}

// Review #7 (RV7-B4): malformed bodies are refused before anything reaches
// the helper; the accepted oddities are pinned (all harmless, local-only).
func TestRV7BodyParsing(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	now := fakeEjectClock(t)
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, ordinaryErr: client.ErrPending, out: client.Outcome{State: "BACKOFF"}}
	publishNFSController(t, ctl)
	refused := []string{
		`{"phase":1}`, `[]`, `"escalate"`, `{"phase":"ESCALATE"}`, `{"phase":"escalate"} {"phase":"start"}`,
		`{"phase":"escalate"` + strings.Repeat(" ", 5000) + `}`, // truncated at 4096 -> unreadable
		"\xef\xbb\xbf{\"phase\":\"escalate\"}",
	}
	for _, b := range refused {
		n := len(ctl.calls)
		code, reply := doEjectNowBody(t, http.MethodPost, true, b)
		if code != http.StatusBadRequest || len(ctl.calls) != n {
			t.Errorf("body %.40q: %d %v, helper calls +%d (want 400, none)", b, code, reply, len(ctl.calls)-n)
		}
	}
	// Accepted oddities: classify by whether the controller saw a start.
	cases := []struct{ body, want string }{
		{`{"phase":null}`, "start"},
		{`{"PHASE":"escalate"}`, "escalate"},                             // encoding/json keys are case-insensitive
		{`{"phase":"start","phase":"escalate"}`, "escalate"},             // last key wins
		{`{"phase":"escalate"}` + strings.Repeat(" ", 5000), "escalate"}, // trailing whitespace truncated at 4 KiB
		{`{"phase":"start","force":true,"eject_now":true}`, "start"},     // extra fields ignored
	}
	for _, c := range cases {
		ctl.since, ctl.last = time.Time{}, time.Time{}
		before := ctl.starts
		code, reply := doEjectNowBody(t, http.MethodPost, true, c.body)
		got := "escalate"
		if ctl.starts > before {
			got = "start"
		}
		t.Logf("body %.40q -> %d %v as %s", c.body, code, reply, got)
		if got != c.want {
			t.Errorf("body %.40q classified %s, want %s", c.body, got, c.want)
		}
		*now = now.Add(time.Second)
	}
	if f := forced(ctl.calls); f != 0 {
		t.Fatalf("forced %d with no met window", f)
	}
}
