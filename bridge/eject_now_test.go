package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/nfscustody"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

type fakeBridgeLease struct {
	op     string
	mu     sync.Mutex
	closed bool
}

func (l *fakeBridgeLease) OpID() string          { return l.op }
func (l *fakeBridgeLease) Lost() <-chan struct{} { return make(chan struct{}) }
func (l *fakeBridgeLease) Err() error            { return nil }
func (l *fakeBridgeLease) Close()                { l.mu.Lock(); l.closed = true; l.mu.Unlock() }

// fakeServiceHelper is the bridge's helper seam. It never dials XPC.
type fakeServiceHelper struct {
	mu         sync.Mutex
	ping       client.PingInfo
	pingErr    error
	pings      int
	reclaims   []string
	reclaimed  bool
	reclaimErr error
	reclaimOut client.Outcome
	retires    []client.RetireRequest
	retireOps  []string
	retireErr  error
	onRetire   func()
	leases     []*fakeBridgeLease
}

func (f *fakeServiceHelper) Ping(context.Context) (client.PingInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pings++
	return f.ping, f.pingErr
}
func (f *fakeServiceHelper) Register(_ context.Context, rr client.RegisterRequest) (health.UnmountLease, error) {
	return &fakeBridgeLease{op: rr.OpID}, nil
}
func (f *fakeServiceHelper) Reclaim(_ context.Context, op string) (health.UnmountLease, client.ReclaimResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reclaims = append(f.reclaims, op)
	if f.reclaimErr != nil {
		return nil, client.ReclaimResult{Outcome: f.reclaimOut}, f.reclaimErr
	}
	l := &fakeBridgeLease{op: op}
	f.leases = append(f.leases, l)
	return l, client.ReclaimResult{Reclaimed: f.reclaimed}, nil
}
func (f *fakeServiceHelper) Cancel(_ context.Context, op string) (client.Outcome, error) {
	return client.Outcome{OpID: op, State: "CANCELLED"}, nil
}
func (f *fakeServiceHelper) Retire(_ context.Context, op string, rr client.RetireRequest, _ time.Duration) (client.Outcome, error) {
	f.mu.Lock()
	f.retires = append(f.retires, rr)
	f.retireOps = append(f.retireOps, op)
	err, hook := f.retireErr, f.onRetire
	f.mu.Unlock()
	if err != nil {
		return client.Outcome{OpID: op, State: "ORDINARY"}, err
	}
	if hook != nil {
		hook()
	}
	return client.Outcome{OpID: op, State: "RETIRED"}, nil
}

func useFakeServiceHelper(t *testing.T, h serviceUnmountHelper) {
	t.Helper()
	old := processUnmountHelper
	processUnmountHelper = func() serviceUnmountHelper { return h }
	t.Cleanup(func() { processUnmountHelper = old })
}

// helperNFSCustody is a fake controller that also speaks the helper surface.
// An ordinary request answers ordinaryErr (ErrPending = still busy, which
// also records the busy window like the real controller); a forcing request
// answers forceErr.
type helperNFSCustody struct {
	fakeNFSCustody
	mu          sync.Mutex
	calls       []client.RetireRequest
	out         client.Outcome
	ordinaryErr error
	forceErr    error
	retired     bool
	cancels     int
	withdraws   int
	withdrawErr error
	onOrdinary  func() // runs inside each ordinary call (e.g. advance the clock)
	starts      int    // start-phase requests received
	since, last time.Time
}

func (c *helperNFSCustody) RetireWithHelper(_ context.Context, rr client.RetireRequest, _ time.Duration) (client.Outcome, error) {
	return c.retire(rr, false)
}

// retire models the controller: a start request is ordinary only, and it
// restarts the busy window at its arrival (nfscustody.HelperRetireGate).
func (c *helperNFSCustody) retire(rr client.RetireRequest, start bool) (client.Outcome, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	arrived := ejectNowNow()
	if start {
		if rr.EjectNow || rr.Liveness == client.LivenessDead || rr.Liveness == client.LivenessWedged {
			return client.Outcome{}, nfscustody.ErrHelperRetireStartForce
		}
		c.starts++
		c.since, c.last = time.Time{}, time.Time{}
	}
	c.calls = append(c.calls, rr)
	force := rr.EjectNow || rr.Liveness == client.LivenessDead || rr.Liveness == client.LivenessWedged
	err := c.ordinaryErr
	if force {
		err = c.forceErr
	} else if c.onOrdinary != nil {
		c.onOrdinary()
	}
	if err == nil {
		c.retired = true
		return client.Outcome{State: "RETIRED"}, nil
	}
	if !force && errors.Is(err, client.ErrPending) {
		now := ejectNowNow()
		if c.since.IsZero() || arrived.Sub(c.last) > nfscustody.OrdinaryBusyMaxGap {
			c.since = arrived // like the controller: the window starts at arrival
		}
		c.last = now
	}
	return c.out, err
}

// It has no withdraw ordering of its own (coreBackedNFS runs the real gate).
func (c *helperNFSCustody) HelperWithdrawGeneration() uint64 { return 0 }
func (c *helperNFSCustody) RetireWithHelperAt(ctx context.Context, _ uint64, rr client.RetireRequest, wait time.Duration) (client.Outcome, error) {
	return c.RetireWithHelper(ctx, rr, wait)
}
func (c *helperNFSCustody) StartRetireWithHelperAt(_ context.Context, _ uint64, rr client.RetireRequest, _ time.Duration) (client.Outcome, error) {
	return c.retire(rr, true)
}

// Both fakes must stay helper-capable controllers; the handler finds the
// surface by a runtime type assertion, so a missing method would silently
// turn every route test into NO_RECEIPT.
var (
	_ helperRetiringNFS = (*helperNFSCustody)(nil)
	_ helperRetiringNFS = (*coreBackedNFS)(nil)
)

func (c *helperNFSCustody) HelperStatus() nfscustody.HelperStatus {
	return nfscustody.HelperStatus{OpID: "nfs-0123456789abcdef0123456789abcdef", State: nfscustody.HelperStateRegistered}
}
func (c *helperNFSCustody) CancelHelperRetire() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancels++
	c.since, c.last = time.Time{}, time.Time{}
}
func (c *helperNFSCustody) WithdrawHelperRetire(context.Context) (client.Outcome, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.withdraws++
	c.since, c.last = time.Time{}, time.Time{}
	if c.withdrawErr != nil {
		return client.Outcome{State: "FORCE"}, c.withdrawErr
	}
	return client.Outcome{State: "CANCELLED"}, nil
}
func (c *helperNFSCustody) OrdinaryBusy() (time.Time, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.since, c.last
}

func publishNFSController(t *testing.T, c nfsMountController) {
	t.Helper()
	globalMu.Lock()
	old := globalNFSController
	globalNFSController = c
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalNFSController = old
		globalMu.Unlock()
	})
}

// doEjectNow sends one request. A POST is an ESCALATE request (the
// countdown ended, or "Eject now"); doEjectNowPhase sends the others.
func doEjectNow(t *testing.T, method string, header bool) (int, map[string]any) {
	t.Helper()
	body := ""
	if method == http.MethodPost {
		body = `{"phase":"escalate"}`
	}
	return doEjectNowBody(t, method, header, body)
}

// doEjectNowPhase POSTs with the given phase; "" sends no body at all (a GUI
// from before phases).
func doEjectNowPhase(t *testing.T, phase string) (int, map[string]any) {
	t.Helper()
	body := ""
	if phase != "" {
		body = `{"phase":"` + phase + `"}`
	}
	return doEjectNowBody(t, http.MethodPost, true, body)
}

func doEjectNowBody(t *testing.T, method string, header bool, body string) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "/eject-now", rd)
	if header {
		req.Header.Set(localControlHeader, "1")
	}
	rec := httptest.NewRecorder()
	handleEjectNowHTTP(rec, req)
	var reply map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &reply)
	return rec.Code, reply
}

func TestEjectNowAvailabilityRequiresServiceRolePing(t *testing.T) {
	cases := []struct {
		name string
		h    serviceUnmountHelper
		want bool
	}{
		{"no helper client", nil, false},
		{"ping fails", &fakeServiceHelper{pingErr: client.ErrUnavailable}, false},
		{"admitted as gui", &fakeServiceHelper{ping: client.PingInfo{Protocol: 1, Role: "gui"}}, false},
		{"service role", &fakeServiceHelper{ping: client.PingInfo{Protocol: 1, Role: "service"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			useFakeServiceHelper(t, tc.h)
			code, body := doEjectNow(t, http.MethodGet, false)
			if code != http.StatusOK || body["available"] != tc.want {
				t.Fatalf("GET /eject-now = %d %v, want available=%v", code, body, tc.want)
			}
		})
	}
}

func TestEjectNowPostRequiresControlHeader(t *testing.T) {
	fake := &fakeServiceHelper{ping: client.PingInfo{Role: "service"}}
	useFakeServiceHelper(t, fake)
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}}
	publishNFSController(t, ctl)
	if code, _ := doEjectNow(t, http.MethodPost, false); code != http.StatusForbidden {
		t.Fatalf("headerless POST: %d", code)
	}
	if code, _ := doEjectNow(t, http.MethodPut, true); code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT: %d", code)
	}
	if len(ctl.calls) != 0 {
		t.Fatalf("a request without the control header reached the helper: %+v", ctl.calls)
	}
}

func fakeEjectClock(t *testing.T) *time.Time {
	t.Helper()
	now := time.Unix(2_000_000, 0)
	old := ejectNowNow
	ejectNowNow = func() time.Time { return now }
	t.Cleanup(func() { ejectNowNow = old })
	return &now
}

func forced(calls []client.RetireRequest) int {
	n := 0
	for _, c := range calls {
		if c.EjectNow {
			n++
		}
	}
	return n
}

// The founder floor: no force until an ordinary retire of the op has been
// BUSY for 30 s and is confirmed busy again now.
func TestEjectNowForcesOnlyAfterOrdinaryBusyFloor(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	now := fakeEjectClock(t)
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, ordinaryErr: client.ErrPending, out: client.Outcome{State: "BACKOFF"}}
	publishNFSController(t, ctl)
	code, body := doEjectNow(t, http.MethodPost, true)
	if code != http.StatusAccepted || body["ok"] != false || body["state"] != "ordinary_in_progress" || body["retry_after"] != float64(30) {
		t.Fatalf("first eject-now: %d %v", code, body)
	}
	*now = now.Add(20 * time.Second)
	if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted || body["retry_after"] != float64(10) {
		t.Fatalf("eject-now inside the floor: %d %v", code, body)
	}
	if n := forced(ctl.calls); n != 0 {
		t.Fatalf("forced inside the floor: %+v", ctl.calls)
	}
	*now = now.Add(10 * time.Second)
	code, body = doEjectNow(t, http.MethodPost, true)
	if code != http.StatusOK || body["ok"] != true || body["state"] != "RETIRED" {
		t.Fatalf("eject-now after the floor: %d %v", code, body)
	}
	last := ctl.calls[len(ctl.calls)-1]
	prev := ctl.calls[len(ctl.calls)-2]
	if !last.EjectNow || last.Liveness != client.LivenessAlive || prev.EjectNow || forced(ctl.calls) != 1 {
		t.Fatalf("force was not one EjectNow right after a busy ordinary confirmation: %+v", ctl.calls)
	}
}

func TestEjectNowNeverForcesStaleOrFinishedOrdinary(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	t.Run("ordinary finishes", func(t *testing.T) {
		fakeEjectClock(t)
		ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}}
		publishNFSController(t, ctl)
		if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusOK || body["state"] != "RETIRED" || forced(ctl.calls) != 0 {
			t.Fatalf("idle volume: %d %v %+v", code, body, ctl.calls)
		}
	})
	t.Run("busy window went stale", func(t *testing.T) {
		now := fakeEjectClock(t)
		ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, ordinaryErr: client.ErrPending}
		// An old busy observation (e.g. an hour-old Stop) must not license a force.
		ctl.since, ctl.last = now.Add(-time.Hour), now.Add(-time.Hour)
		publishNFSController(t, ctl)
		if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted || forced(ctl.calls) != 0 {
			t.Fatalf("stale busy window forced: %d %v", code, body)
		}
	})
}

func TestEjectNowReportsUnfinishedRetirement(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	now := fakeEjectClock(t)
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, ordinaryErr: client.ErrPending, forceErr: client.ErrPending,
		out: client.Outcome{State: "WAIT_NESTED", Descendants: []client.Entry{{MntOnName: "/Volumes/zpool/x"}}}}
	ctl.since, ctl.last = now.Add(-time.Minute), now.Add(-time.Second)
	publishNFSController(t, ctl)
	code, body := doEjectNow(t, http.MethodPost, true)
	if code == http.StatusOK || body["ok"] != false || body["state"] != "WAIT_NESTED" || !strings.Contains(body["error"].(string), "inside") {
		t.Fatalf("pending eject-now reported as success: %d %v", code, body)
	}
	publishNFSController(t, nil)
	if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusConflict || body["ok"] != false {
		t.Fatalf("eject-now with no owned mount: %d %v", code, body)
	}
}

func TestStaleNFSRetirerUsesHelperThenSettlesController(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	stopped := 0
	ctl := &helperNFSCustody{}
	ctl.fakeNFSCustody = fakeNFSCustody{path: "/Volumes/zpool", stop: func() error {
		if !ctl.retired {
			return errors.New("stop before helper proof")
		}
		stopped++
		return nil
	}}
	publishNFSController(t, ctl)
	oldObserve := observeOwnedNFS
	observeOwnedNFS = func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{}}, nil
	}
	t.Cleanup(func() { observeOwnedNFS = oldObserve })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, unproven := range []string{client.LivenessUnknown, client.LivenessAlive, ""} {
		if err := (nfsStaleRetirer{}).RetireStaleNFSMount(ctx, unproven); err == nil || len(ctl.calls) != 0 {
			t.Fatalf("liveness %q reached the helper: %v %+v", unproven, err, ctl.calls)
		}
	}
	if err := (nfsStaleRetirer{}).RetireStaleNFSMount(ctx, client.LivenessDead); err != nil {
		t.Fatalf("stale retirer: %v", err)
	}
	if len(ctl.calls) != 1 || ctl.calls[0].EjectNow || ctl.calls[0].Liveness != client.LivenessDead || stopped != 1 {
		t.Fatalf("stale retirement: calls=%+v stopped=%d", ctl.calls, stopped)
	}
	globalMu.Lock()
	cleared := globalNFSController == nil
	globalMu.Unlock()
	if !cleared {
		t.Fatal("controller not released for a new birth")
	}
	if err := (nfsStaleRetirer{}).RetireStaleNFSMount(ctx, client.LivenessDead); err == nil {
		t.Fatal("retirer succeeded with nothing owned")
	}
}

func TestStaleNFSRetirerWithdrawsJobWhenItGivesUp(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, forceErr: client.ErrPending}
	publishNFSController(t, ctl)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := (nfsStaleRetirer{}).RetireStaleNFSMount(ctx, client.LivenessDead); err == nil {
		t.Fatal("unfinished retirement reported success")
	}
	if ctl.cancels != 1 {
		t.Fatalf("timed-out stale retire not withdrawn: cancels=%d", ctl.cancels)
	}
}

// orphanJournal creates a real Observed journal row in a private directory.
func orphanJournal(t *testing.T) (mountbroker.Creator, *mountbroker.Record, *bool) {
	t.Helper()
	dir := t.TempDir()
	const gen = "0123456789abcdef0123456789abcdef"
	spec := mountbroker.Spec{MountPath: filepath.Join(dir, "vol"), Source: "127.0.0.1:/jm-" + gen,
		Endpoint: "127.0.0.1:2049", ExportPath: "/jm-" + gen, MetadataPath: filepath.Join(dir, "meta.db")}
	if err := os.Mkdir(filepath.Join(dir, "journal"), 0o700); err != nil {
		t.Fatal(err)
	}
	present := false
	row := mounttable.MountEntry{Path: spec.MountPath, Source: spec.Source, Type: "nfs", FSID: [2]int32{3, 4}}
	creator := mountbroker.Creator{Directory: filepath.Join(dir, "journal"), Observe: func(context.Context) (mounttable.Snapshot, error) {
		rows := []mounttable.MountEntry{}
		if present {
			rows = append(rows, row)
		}
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: rows}, nil
	}}
	res := creator.CreateBound(context.Background(), spec, func(mountbroker.Record) error { present = true; return nil })
	if res.State != mountbroker.Observed || res.Record == nil {
		t.Fatalf("fixture journal: %+v", res)
	}
	return creator, res.Record, &present
}

func TestOrphanNFSReclaimedAndRetiredOnlyWithProof(t *testing.T) {
	cases := []struct {
		name    string
		fake    *fakeServiceHelper
		retired bool
	}{
		{"reclaimed then retired", &fakeServiceHelper{reclaimed: true}, true},
		{"helper sweep already retired", &fakeServiceHelper{reclaimErr: client.ErrNoReceipt}, true},
		{"live owner still bound", &fakeServiceHelper{reclaimed: false}, false},
		{"retire pending", &fakeServiceHelper{reclaimed: true, retireErr: client.ErrPending}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			creator, rec, present := orphanJournal(t)
			op := mountbroker.HelperOpID(*rec)
			tc.fake.onRetire = func() { *present = false }
			if tc.fake.reclaimErr != nil {
				tc.fake.reclaimOut = client.Outcome{OpID: op, State: "RETIRED"}
				*present = false // the helper's own sweep already detached it
			}
			oldCreator, oldObserve := newNFSMountCreator, observeOwnedNFS
			newNFSMountCreator = func() (mountbroker.Creator, error) { return creator, nil }
			observeOwnedNFS = creator.Observe
			t.Cleanup(func() { newNFSMountCreator, observeOwnedNFS = oldCreator, oldObserve })
			got, err := retireOrphanNFSRecord(context.Background(), tc.fake, rec)
			if got != tc.retired {
				t.Fatalf("retired=%v err=%v, want %v", got, err, tc.retired)
			}
			if len(tc.fake.reclaims) != 1 || tc.fake.reclaims[0] != op {
				t.Fatalf("restart did not reclaim the persisted op %s: %v", op, tc.fake.reclaims)
			}
			after, err := mountbroker.ReadRecordReadonly(creator.Directory, rec.Spec.MountPath)
			if err != nil {
				t.Fatal(err)
			}
			if want := map[bool]mountbroker.State{true: mountbroker.Retired, false: mountbroker.Observed}[tc.retired]; after.State != want {
				t.Fatalf("journal %s, want %s", after.State, want)
			}
			for _, l := range tc.fake.leases {
				if !l.closed {
					t.Fatal("reclaimed lease not released")
				}
			}
			if !tc.retired && len(tc.fake.retires) > 0 && !tc.fake.reclaimed {
				t.Fatal("retired a receipt still held by a live owner")
			}
			for _, rr := range tc.fake.retires {
				if rr.Liveness != client.LivenessDead {
					t.Fatalf("dead owner's mount retired with liveness %q, want dead", rr.Liveness)
				}
			}
		})
	}
}

func TestUnmountHelperHealthLabel(t *testing.T) {
	useFakeServiceHelper(t, nil)
	publishNFSController(t, nil)
	if got := unmountHelperHealthLabel(); got != "unavailable" {
		t.Fatalf("label %q", got)
	}
	useFakeServiceHelper(t, &fakeServiceHelper{})
	publishNFSController(t, &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/v"}})
	if got := unmountHelperHealthLabel(); got != "ok" {
		t.Fatalf("label %q", got)
	}
}
