package main

// H-8 per-Mac object keys, bridge side (design §5 "Mac side", test C2): the
// object-key status the app asks for, the immediate poll a refusal
// triggers, and what an enrollment step reports about the key in use.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/enrollclient"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// installKeyStatus substitutes the object-key status source and starts the
// poll gate afresh.
func installKeyStatus(t *testing.T, src func() (health.ObjectKeyStatus, bool)) {
	t.Helper()
	old := currentObjectKeyStatus
	currentObjectKeyStatus = src
	objectKeyPollGate = refusalPollGate{}
	t.Cleanup(func() {
		currentObjectKeyStatus = old
		objectKeyPollGate = refusalPollGate{}
	})
}

func keyStatusAt(t *testing.T, now time.Time) objectKeyStatusOutput {
	t.Helper()
	var out objectKeyStatusOutput
	if err := json.Unmarshal([]byte(objectKeyStatusJSON(now)), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// C2: a refusal the juicefs log tail counted (health.ObjectKeyRefusalCode)
// makes the next status call ask for an immediate enrollment poll: once per
// new refusal, at most once per RefusalPollMin.
func TestObjectKeyRefusalTriggersImmediatePoll(t *testing.T) {
	var mu sync.Mutex
	cur, known := health.ObjectKeyStatus{InUse: "mac-4f1c2a9e7b3d-1"}, true
	set := func(refusals uint64) {
		mu.Lock()
		cur.Refusals, cur.RefusedCode, cur.RefusedAtMS = refusals, "InvalidAccessKeyId", time.Now().UnixMilli()
		mu.Unlock()
	}
	installKeyStatus(t, func() (health.ObjectKeyStatus, bool) {
		mu.Lock()
		defer mu.Unlock()
		return cur, known
	})
	t0 := time.Unix(1_800_000_000, 0)

	if out := keyStatusAt(t, t0); !out.Known || out.Refused || out.PollNow || out.InUse != "mac-4f1c2a9e7b3d-1" {
		t.Fatalf("no refusal: %+v", out)
	}
	set(1)
	// Through the export's JSON input, as the app calls it.
	var first objectKeyStatusOutput
	if err := json.Unmarshal([]byte(enrollStep(`{"op":"object-key-status"}`)), &first); err != nil {
		t.Fatal(err)
	}
	if !first.Refused || !first.PollNow || first.RefusedCode != "InvalidAccessKeyId" {
		t.Fatalf("first refusal did not trigger a poll: %+v", first)
	}
	if out := keyStatusAt(t, time.Now()); !out.Refused || out.PollNow {
		t.Fatalf("the same refusal polled twice: %+v", out)
	}
	// juicefs retries the upload: more refusals within the floor wait.
	set(3)
	now := time.Now()
	if out := keyStatusAt(t, now.Add(10*time.Second)); out.PollNow {
		t.Fatalf("polled again inside RefusalPollMin: %+v", out)
	}
	if out := keyStatusAt(t, now.Add(RefusalPollMin+time.Second)); !out.PollNow {
		t.Fatalf("a refusal that continues past RefusalPollMin did not poll again: %+v", out)
	}
	// No serving process: nothing is known and nothing is triggered.
	mu.Lock()
	known = false
	mu.Unlock()
	if out := keyStatusAt(t, now.Add(time.Hour)); out.Known || out.PollNow || out.Refused {
		t.Fatalf("unknown status: %+v", out)
	}
}

// Review keys-l3 F3: a refusal expires RefusalExpiry after the last one, so
// the red "refused" state clears once access is restored, without a
// restart; an expired refusal triggers no poll, in the status call or in an
// enrollment step's output.
func TestObjectKeyRefusalExpires(t *testing.T) {
	t0 := time.Now()
	st := health.ObjectKeyStatus{InUse: "mac-4f1c2a9e7b3d-1", Refusals: 3, RefusedCode: "InvalidAccessKeyId", RefusedAtMS: t0.UnixMilli()}
	installKeyStatus(t, func() (health.ObjectKeyStatus, bool) { return st, true })
	if RefusalExpiry != 5*time.Minute {
		t.Fatalf("RefusalExpiry = %v, want 5m", RefusalExpiry)
	}
	if out := keyStatusAt(t, t0.Add(4*time.Minute)); !out.Refused || out.RefusedCode != "InvalidAccessKeyId" {
		t.Fatalf("a 4-minute-old refusal is not shown: %+v", out)
	}
	objectKeyPollGate = refusalPollGate{}
	if out := keyStatusAt(t, t0.Add(RefusalExpiry+time.Second)); out.Refused || out.PollNow || out.RefusedCode != "" || !out.Known {
		t.Fatalf("an expired refusal is still shown or polls: %+v", out)
	}

	// The enrollment step reports an expired refusal as none.
	st.RefusedAtMS = time.Now().Add(-RefusalExpiry - time.Minute).UnixMilli()
	srv := httptest.NewServer(&stubManager{approved: true})
	defer srv.Close()
	old := enrollHTTPClient
	enrollHTTPClient = srv.Client()
	t.Cleanup(func() { enrollHTTPClient = old })
	db := filepath.Join(t.TempDir(), "metadata.db")
	out := runEnrollStep(t, srv.URL, db, enrollclient.State{})
	out = runEnrollStep(t, srv.URL, db, out.State)
	if out.ObjectKeyRefused || out.RefusedCode != "" {
		t.Fatalf("step output shows an expired refusal: refused=%v code=%q", out.ObjectKeyRefused, out.RefusedCode)
	}
}

// Review keys-l3 F3: a refusal that never stops (juicefs retries every
// upload; the app checks every 15 s) used to poll the Manager 60 times an
// hour for ever. The wait now doubles to RefusalPollMax; polls stop when
// refusals stop; after refusals expire, the next run starts at the short
// wait again.
func TestPersistentRefusalPollRateIsBounded(t *testing.T) {
	var g refusalPollGate
	t0 := time.Unix(1_800_000_000, 0)
	refusals, lastRefusal := uint64(0), time.Time{}
	// One 15-second status check at at; grow: juicefs logged more refusals.
	check := func(at time.Time, grow bool) bool {
		if grow {
			refusals, lastRefusal = refusals+3, at
		}
		return g.pollNow(health.ObjectKeyStatus{InUse: "mac-4f1c2a9e7b3d-1", Refusals: refusals,
			RefusedCode: "InvalidAccessKeyId", RefusedAtMS: lastRefusal.UnixMilli()}, at)
	}
	count := func(from time.Time, d time.Duration, grow bool) int {
		n := 0
		for at := from; at.Before(from.Add(d)); at = at.Add(15 * time.Second) {
			if check(at, grow) {
				n++
			}
		}
		return n
	}
	first := count(t0, time.Hour, true)
	if first > 8 || first < 5 {
		t.Fatalf("polls in the first hour of continuous refusal = %d, want 5..8 (backoff to %v)", first, RefusalPollMax)
	}
	if second := count(t0.Add(time.Hour), time.Hour, true); second > 4 || second < 3 {
		t.Fatalf("polls in the second hour = %d, want 3..4 (one per %v)", second, RefusalPollMax)
	}
	// Refusals stop. Refusals logged since the last poll may still get one
	// poll while they are active; after they expire, none for the rest of
	// the hour.
	t2 := t0.Add(2 * time.Hour)
	if tail := count(t2, RefusalExpiry, false); tail > 1 {
		t.Fatalf("polls in the %v after refusals stopped = %d, want at most 1", RefusalExpiry, tail)
	}
	if quiet := count(t2.Add(RefusalExpiry), time.Hour-RefusalExpiry, false); quiet != 0 {
		t.Fatalf("polls after refusals stopped and expired = %d, want 0", quiet)
	}
	// A new run of refusals after the old one expired starts at the short
	// wait: two polls RefusalPollMin apart.
	t3 := t0.Add(3 * time.Hour)
	if !check(t3, true) {
		t.Fatal("a new refusal after a quiet hour did not poll")
	}
	if !check(t3.Add(RefusalPollMin+time.Second), true) {
		t.Fatalf("the backoff from the earlier run carried over: no poll %v after the first", RefusalPollMin+time.Second)
	}
}

// A refusal an enrollment step already carried needs no extra poll.
func TestEnrollStepMarksRefusalsSeen(t *testing.T) {
	st := health.ObjectKeyStatus{InUse: "mac-4f1c2a9e7b3d-1", Refusals: 2, RefusedCode: "AccessDenied", RefusedAtMS: time.Now().UnixMilli()}
	installKeyStatus(t, func() (health.ObjectKeyStatus, bool) { return st, true })
	srv := httptest.NewServer(&stubManager{approved: true})
	defer srv.Close()
	old := enrollHTTPClient
	enrollHTTPClient = srv.Client()
	t.Cleanup(func() { enrollHTTPClient = old })
	db := filepath.Join(t.TempDir(), "metadata.db")
	out := runEnrollStep(t, srv.URL, db, enrollclient.State{})
	out = runEnrollStep(t, srv.URL, db, out.State)
	if !out.ObjectKeyRefused || out.RefusedCode != "AccessDenied" {
		t.Fatalf("step output hides the refusal: %+v", out)
	}
	if s := keyStatusAt(t, time.Now()); s.PollNow {
		t.Fatal("a refusal the step already saw triggered another poll")
	}
}

// plusSlashFixture has the characters of a MinIO-generated secret (+ and /).
const plusSlashFixture = "fixture+not/a+key/fixture+not/a+key/fixture"

// perMacStub answers approved with this Mac's per-Mac key of generation
// gen (or the shared key when gen is 0) and records the requests.
type perMacStub struct {
	mu     sync.Mutex
	gen    string
	bodies []map[string]any
}

func (s *perMacStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m map[string]any
	_ = json.NewDecoder(r.Body).Decode(&m)
	s.bodies = append(s.bodies, m)
	inst, _ := m["instance_id"].(string)
	name := "mac-" + strings.ToLower(strings.ReplaceAll(inst, "-", ""))[:12]
	ak := enrollTestAK
	if s.gen != "" {
		ak = name + "-" + s.gen
	}
	_ = json.NewEncoder(w).Encode(map[string]string{
		"principal": name, "fingerprint": "ABCD-1234", "status": "approved", "poll_token": enrollToken,
		"object_access_key": ak, "object_secret_key": plusSlashFixture,
	})
}

// The step reports the key the running juicefs uses (access-key ID only,
// and only this Mac's own per-Mac key) and says when a delivered key needs
// a restart. The shared-key path sends no new field.
func TestEnrollStepReportsKeyInUseAndRestartNeeded(t *testing.T) {
	inUse := ""
	installKeyStatus(t, func() (health.ObjectKeyStatus, bool) { return health.ObjectKeyStatus{InUse: inUse}, true })
	stub := &perMacStub{gen: "2"}
	srv := httptest.NewServer(stub)
	defer srv.Close()
	old := enrollHTTPClient
	enrollHTTPClient = srv.Client()
	t.Cleanup(func() { enrollHTTPClient = old })
	db := filepath.Join(t.TempDir(), "metadata.db")
	out := runEnrollStep(t, srv.URL, db, enrollclient.State{})
	principal := enrollclient.PrincipalFor(out.InstanceID)
	st := out.State

	inUse = principal + "-1"
	out = runEnrollStep(t, srv.URL, db, st)
	if out.Phase != enrollclient.Approved || out.ObjectAccessKey != principal+"-2" {
		t.Fatalf("gen 2 not delivered: %s %q %q", out.Phase, out.ObjectAccessKey, out.Message)
	}
	if got := stub.bodies[len(stub.bodies)-1]["object_key_in_use"]; got != principal+"-1" {
		t.Fatalf("object_key_in_use sent = %v, want %s", got, principal+"-1")
	}
	if out.ObjectKeyInUse != principal+"-1" || !out.ObjectKeyRestartNeeded {
		t.Fatalf("a replaced key does not ask for a restart: in use %q restart %v", out.ObjectKeyInUse, out.ObjectKeyRestartNeeded)
	}

	inUse = principal + "-2"
	if out = runEnrollStep(t, srv.URL, db, out.State); out.ObjectKeyRestartNeeded {
		t.Fatal("restart asked for although the delivered key is in use")
	}
	// Not running: nothing to restart.
	inUse = ""
	if out = runEnrollStep(t, srv.URL, db, out.State); out.ObjectKeyRestartNeeded {
		t.Fatal("restart asked for with no volume running")
	}

	// The shared key: no object_key_in_use field (an older Manager would
	// answer 400 to it), no restart for the same key.
	stub.gen, inUse = "", enrollTestAK
	out = runEnrollStep(t, srv.URL, db, out.State)
	if _, sent := stub.bodies[len(stub.bodies)-1]["object_key_in_use"]; sent || out.ObjectKeyRestartNeeded || out.Phase != enrollclient.Approved {
		t.Fatalf("shared key path changed: sent=%v restart=%v phase=%s", sent, out.ObjectKeyRestartNeeded, out.Phase)
	}
}

// The serving process answers the object-key status call ("enroll-verify"
// {enrollVerifyOpObjectKeyStatus, "", ""}) with its object-key status and
// dials nothing.
func TestEnrollVerifyStatusOnlyCall(t *testing.T) {
	link := &fakeLink{routes: map[string]string{}}
	installFakeLink(t, link)
	setServingRedisURL(t, routeNASRedis)
	res := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "enroll-verify",
		Strings: []string{enrollVerifyOpObjectKeyStatus, "", ""}})
	var out enrollVerifyResult
	if err := json.Unmarshal([]byte(res.Text), &out); err != nil {
		t.Fatalf("%v %q", err, res.Error)
	}
	if out.Live || out.Route != "status" || out.ObjectKey == nil {
		t.Fatalf("status-only answer = %+v", out)
	}
	if want := health.CurrentObjectKeyStatus(); *out.ObjectKey != want {
		t.Fatalf("status %+v, want %+v", *out.ObjectKey, want)
	}
	if len(link.dialed) != 0 {
		t.Fatalf("a status-only call dialled %v", link.dialed)
	}
	// One empty string is not a status call: still a refused login check.
	res = dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "enroll-verify",
		Strings: []string{routeNASRedis, "", ""}})
	_ = json.Unmarshal([]byte(res.Text), &out)
	if out.Route != "refused" {
		t.Fatalf("partial-empty call = %+v", out)
	}
}

// Only the exact status call answers with a status; every other shape is a
// login check under the full restrictions (review keys-l3 F7, neuter N12).
// In particular the old status shape, three empty strings, is now a refused
// login check, and the op string with a principal or a secret is too.
func TestEnrollVerifyStatusCallShapeIsExact(t *testing.T) {
	link := &fakeLink{routes: map[string]string{}}
	installFakeLink(t, link)
	setServingRedisURL(t, routeNASRedis)
	for _, in := range [][3]string{
		{"", "", ""},
		{enrollVerifyOpObjectKeyStatus, "", "x"},
		{enrollVerifyOpObjectKeyStatus, "mac-4f1c2a9e7b3d", ""},
		{enrollVerifyOpObjectKeyStatus, "mac-4f1c2a9e7b3d", "x"},
		{" " + enrollVerifyOpObjectKeyStatus, "", ""},
		{"object-key-status", "", ""},
		{routeNASRedis, "", ""},
	} {
		out := enrollVerifyInServingProcess(in[0], in[1], in[2])
		var res enrollVerifyResult
		if err := json.Unmarshal([]byte(out), &res); err != nil {
			t.Fatal(err)
		}
		if res.Route != "refused" || res.Live || res.ObjectKey != nil {
			t.Errorf("%q -> %s, want a refused login check with no status", in, out)
		}
	}
	if len(link.dialed) != 0 {
		t.Fatalf("a refused shape dialled %v", link.dialed)
	}
	var res enrollVerifyResult
	_ = json.Unmarshal([]byte(enrollVerifyInServingProcess(enrollVerifyOpObjectKeyStatus, "", "")), &res)
	if res.Route != "status" || res.Live || res.ObjectKey == nil {
		t.Fatalf("the status call = %+v", res)
	}
}

// An unknown op is refused before anything is contacted: with an input that
// would otherwise run a full enrollment step (a database path and a
// reachable Manager), the Manager gets no request (review keys-l3 F7,
// neuter N15).
func TestEnrollStepUnknownOp(t *testing.T) {
	installKeyStatus(t, func() (health.ObjectKeyStatus, bool) { return health.ObjectKeyStatus{}, true })
	stub := &stubManager{approved: true}
	srv := httptest.NewServer(stub)
	defer srv.Close()
	old := enrollHTTPClient
	enrollHTTPClient = srv.Client()
	t.Cleanup(func() { enrollHTTPClient = old })
	db := filepath.Join(t.TempDir(), "metadata.db")
	// The same input without the op is a working step.
	if out := runEnrollStep(t, srv.URL, db, enrollclient.State{}); out.Phase == enrollclient.Failed {
		t.Fatalf("control step failed: %q", out.Message)
	}
	stub.mu.Lock()
	before := len(stub.bodies)
	stub.mu.Unlock()
	in, _ := json.Marshal(enrollStepInput{ManagerURL: srv.URL, DBPath: db, Host: "Edit-Bay-2", Label: "Edit Bay 2", Op: "nope"})
	var out enrollStepOutput
	if err := json.Unmarshal([]byte(enrollStep(string(in))), &out); err != nil {
		t.Fatal(err)
	}
	stub.mu.Lock()
	after := len(stub.bodies)
	stub.mu.Unlock()
	if out.Phase != enrollclient.Failed || out.Message != "unknown enrollment operation" || after != before {
		t.Fatalf("unknown op: phase=%s message=%q manager requests %d -> %d", out.Phase, out.Message, before, after)
	}
}
