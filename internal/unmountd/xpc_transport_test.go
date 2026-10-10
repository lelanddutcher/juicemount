package unmountd

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// countingService records every call that reaches the core.
type countingService struct {
	*Service
	calls atomic.Int64
}

func (c *countingService) Register(k Caller, r RegisterRequest) (Receipt, error) {
	c.calls.Add(1)
	return c.Service.Register(k, r)
}
func (c *countingService) Retire(ctx context.Context, k Caller, op string, r RetireRequest) (Outcome, error) {
	c.calls.Add(1)
	return c.Service.Retire(ctx, k, op, r)
}
func (c *countingService) Status(k Caller, op string) (Outcome, error) {
	c.calls.Add(1)
	return c.Service.Status(k, op)
}
func (c *countingService) Reclaim(k Caller, op string) (bool, error) {
	c.calls.Add(1)
	return c.Service.Reclaim(k, op)
}

const (
	lService = 0
	lStatus  = 1
)

func newTestTransport(t *testing.T, k *fakeKernel) (*xpcTransport, *countingService) {
	t.Helper()
	svc, _ := newTestService(t, k, nil)
	cs := &countingService{Service: svc}
	return newXPCTransport(cs, client.ChannelRelease, t.Logf), cs
}

func acceptAll(string) error { return nil }

func send(t *testing.T, tr *xpcTransport, peer uint64, req client.Request) client.Response {
	t.Helper()
	body, err := client.EncodeRequest(req)
	if err != nil {
		t.Fatalf("encode %s: %v", req.Op, err)
	}
	out := tr.handle(peer, client.ProtocolVersion, body)
	if out == nil {
		t.Fatalf("%s: no reply", req.Op)
	}
	resp, err := client.DecodeResponse(client.ProtocolVersion, out)
	if err != nil {
		t.Fatalf("%s: decode reply: %v", req.Op, err)
	}
	return resp
}

// trySend is send for goroutines: it reports with t.Error, never FailNow.
func trySend(t *testing.T, tr *xpcTransport, peer uint64, req client.Request) client.Response {
	body, err := client.EncodeRequest(req)
	if err != nil {
		t.Errorf("encode %s: %v", req.Op, err)
		return client.Response{}
	}
	resp, err := client.DecodeResponse(client.ProtocolVersion, tr.handle(peer, client.ProtocolVersion, body))
	if err != nil {
		t.Errorf("%s: decode reply: %v", req.Op, err)
	}
	return resp
}

func wireRegister(op string) client.Request {
	r := nfsRegister(op)
	return client.Request{Op: client.OpRegister, OpID: op, Register: &client.RegisterArgs{
		Type: string(r.Type), FSID: [2]int32(r.FSID), Token: r.Token, MntOnName: r.MntOnName, MntFromName: r.MntFromName}}
}

func TestTransportAppliesTheListenerRequirementAndRole(t *testing.T) {
	tr, _ := newTestTransport(t, newFake(rootEntry(), ourEntry()))
	var got []string
	record := func(req string) error { got = append(got, req); return nil }
	if !tr.peerOpen(lService, 1, testUID, 100007, record) || !tr.peerOpen(lStatus, 2, testUID, 100007, record) {
		t.Fatal("peer refused")
	}
	svcReq, statusReq := PeerRequirements()
	if len(got) != 2 || got[0] != svcReq || got[1] != statusReq {
		t.Fatalf("requirements applied = %q", got)
	}
	if !strings.Contains(got[0], "notarized") || strings.Contains(got[0], client.AppSigningID) {
		t.Fatalf("service listener requirement is not the release service requirement: %s", got[0])
	}
	p1 := send(t, tr, 1, client.Request{Op: client.OpPing})
	p2 := send(t, tr, 2, client.Request{Op: client.OpPing})
	if p1.Ping.Role != "service" || p2.Ping.Role != "gui" || p1.Ping.UID != testUID || p1.Ping.Channel != client.ChannelRelease {
		t.Fatalf("ping: %+v %+v", p1.Ping, p2.Ping)
	}
	if tr.peerOpen(7, 3, testUID, 0, acceptAll) {
		t.Fatal("unknown listener index admitted")
	}
}

// fakeIdentity models a peer's code signature for the validation seam. The
// real evaluation is libxpc's; xpc_darwin_test.go grounds these strings
// against real signed binaries with the Security framework.
type fakeIdentity struct {
	team, id  string
	adhoc     bool
	notarized bool
	debug     bool // get-task-allow
}

func (f fakeIdentity) satisfy(req string) error {
	switch {
	case f.adhoc:
		return errors.New("ad-hoc signature: not anchored at Apple")
	case !strings.Contains(req, `certificate leaf[subject.OU] = "`+f.team+`"`):
		return errors.New("wrong team")
	case !strings.Contains(req, `identifier "`+f.id+`"`):
		return errors.New("wrong identifier")
	case strings.Contains(req, "notarized") && !f.notarized:
		return errors.New("not notarized")
	case f.debug && strings.Contains(req, "get-task-allow"):
		return errors.New("get-task-allow")
	}
	return nil
}

func TestFakeValidationSeamRejectsWrongTeamAndAdHocPeers(t *testing.T) {
	tr, cs := newTestTransport(t, newFake(rootEntry(), ourEntry()))
	rejected := map[string]fakeIdentity{
		"ad-hoc service":                {team: client.TeamID, id: client.ServiceSigningID, adhoc: true},
		"wrong-team service":            {team: "EQHXZ8M8AV", id: client.ServiceSigningID, notarized: true},
		"unnotarized dev service":       {team: client.TeamID, id: client.ServiceSigningID},
		"debuggable notarized service":  {team: client.TeamID, id: client.ServiceSigningID, notarized: true, debug: true},
		"GUI app on the service socket": {team: client.TeamID, id: client.AppSigningID, notarized: true},
	}
	id := uint64(10)
	for name, ident := range rejected {
		id++
		if tr.peerOpen(lService, id, testUID, 1, ident.satisfy) {
			t.Errorf("%s admitted on the service listener", name)
		}
		body, _ := client.EncodeRequest(wireRegister("op1"))
		if out := tr.handle(id, client.ProtocolVersion, body); out != nil {
			t.Errorf("%s: a refused peer got a reply", name)
		}
	}
	if n := cs.calls.Load(); n != 0 {
		t.Fatalf("refused peers reached the core %d times", n)
	}
	good := fakeIdentity{team: client.TeamID, id: client.ServiceSigningID, notarized: true}
	if !tr.peerOpen(lService, 99, testUID, 1, good.satisfy) {
		t.Fatal("the notarized service was refused")
	}
	if r := send(t, tr, 99, wireRegister("op1")); r.Code != client.CodeOK {
		t.Fatalf("register from the service: %+v", r)
	}
}

func TestGUIRoleMayOnlyReadStatus(t *testing.T) {
	tr, _ := newTestTransport(t, newFake(rootEntry(), ourEntry()))
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	tr.peerOpen(lStatus, 2, testUID, 1, acceptAll)
	if r := send(t, tr, 1, wireRegister("op1")); r.Code != client.CodeOK {
		t.Fatalf("service register: %+v", r)
	}
	for _, req := range []client.Request{
		wireRegister("op2"),
		{Op: client.OpReclaim, OpID: "op1"},
		{Op: client.OpRetire, OpID: "op1", Retire: &client.RetireArgs{Liveness: client.LivenessDead, EjectNow: true}},
		{Op: client.OpCancel, OpID: "op1"},
	} {
		if r := send(t, tr, 2, req); r.Code != client.CodeNotAuthorized {
			t.Errorf("GUI %s: code %s, want not_authorized", req.Op, r.Code)
		}
	}
	if r := send(t, tr, 2, client.Request{Op: client.OpStatus, OpID: "op1"}); r.Code != client.CodeOK || r.Outcome.State != "REGISTERED" {
		t.Fatalf("GUI status: %+v", r)
	}
}

func TestCallerUIDComesFromTheConnection(t *testing.T) {
	tr, _ := newTestTransport(t, newFake(rootEntry(), ourEntry()))
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	tr.peerOpen(lService, 2, testUID+1, 1, acceptAll) // another user's service
	if r := send(t, tr, 1, wireRegister("op1")); r.Code != client.CodeOK {
		t.Fatalf("owner register: %+v", r)
	}
	// The other uid cannot see or retire the owner's op: the store is keyed
	// by the connection's euid, and no payload field names a uid.
	if r := send(t, tr, 2, client.Request{Op: client.OpStatus, OpID: "op1"}); r.Code != client.CodeNoReceipt {
		t.Fatalf("other uid status: %+v", r)
	}
	if r := send(t, tr, 2, client.Request{Op: client.OpRetire, OpID: "op1",
		Retire: &client.RetireArgs{Liveness: client.LivenessDead, EjectNow: true}}); r.Code != client.CodeNoReceipt {
		t.Fatalf("other uid retire: %+v", r)
	}
	// Registering a mount owned by testUID from the other uid fails the predicate.
	if r := send(t, tr, 2, wireRegister("op9")); r.Code != client.CodeIdentityRefused {
		t.Fatalf("other uid register: %+v", r)
	}
}

func TestMalformedRequestNeverReachesTheCore(t *testing.T) {
	tr, cs := newTestTransport(t, newFake(rootEntry(), ourEntry()))
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	for name, b := range map[string][]byte{
		"uid in payload": []byte(`{"op":"status","op_id":"op1","uid":0}`),
		"path op":        []byte(`{"op":"unmount","op_id":"op1"}`),
		"garbage":        []byte("\x00\x01"),
	} {
		resp, err := client.DecodeResponse(client.ProtocolVersion, tr.handle(1, client.ProtocolVersion, b))
		if err != nil || resp.Code != client.CodeBadRequest {
			t.Errorf("%s: %+v %v", name, resp, err)
		}
	}
	resp, _ := client.DecodeResponse(client.ProtocolVersion, tr.handle(1, 9, []byte(`{"op":"ping"}`)))
	if resp.Code != client.CodeBadRequest {
		t.Errorf("wrong version: %+v", resp)
	}
	if n := cs.calls.Load(); n != 0 {
		t.Fatalf("malformed requests reached the core %d times", n)
	}
}

func TestDroppedOwnerArmsOrphanRetirement(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	tr, _ := newTestTransport(t, k)
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	if r := send(t, tr, 1, wireRegister("op1")); r.Code != client.CodeOK {
		t.Fatalf("register: %+v", r)
	}
	tr.peerClosed(1) // the service crashed
	if !waitFor(t, 2*time.Second, func() bool { return len(k.recorded()) > 0 }) {
		t.Fatal("dropped owner did not lead to an orphan retirement")
	}
	if got := flagsOf(k.recorded()); got != "N" {
		t.Fatalf("orphan retirement flags = %s, want one ordinary MNT_NOBLOCK (never forced on unknown liveness)", got)
	}
}

func TestReclaimWithinTheWindowCancelsOrphanRetirement(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	tr, _ := newTestTransport(t, k)
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	send(t, tr, 1, wireRegister("op1"))
	tr.peerClosed(1)
	tr.peerOpen(lService, 2, testUID, 1, acceptAll) // the restarted service
	r := send(t, tr, 2, client.Request{Op: client.OpReclaim, OpID: "op1"})
	if r.Code != client.CodeOK || !r.Reclaimed || r.Outcome.State != "REGISTERED" {
		t.Fatalf("reclaim: %+v", r)
	}
	time.Sleep(4 * fastPolicy().OrphanReclaim)
	if calls := k.recorded(); len(calls) != 0 {
		t.Fatalf("reclaimed op was retired anyway: %v", calls)
	}
}

func TestOnlyTheLastBoundConnectionArmsTheOrphanWindow(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	tr, _ := newTestTransport(t, k)
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	tr.peerOpen(lService, 2, testUID, 1, acceptAll)
	send(t, tr, 1, wireRegister("op1"))
	if r := send(t, tr, 2, client.Request{Op: client.OpReclaim, OpID: "op1"}); r.Code != client.CodeOK || r.Reclaimed {
		t.Fatalf("second bind: %+v", r)
	}
	tr.peerClosed(1)
	time.Sleep(4 * fastPolicy().OrphanReclaim)
	if calls := k.recorded(); len(calls) != 0 {
		t.Fatalf("op retired while a bound connection was still open: %v", calls)
	}
	tr.peerClosed(2)
	if !waitFor(t, 2*time.Second, func() bool { return len(k.recorded()) > 0 }) {
		t.Fatal("closing the last bound connection did not arm the orphan window")
	}
}

func TestConnectionBindsOnce(t *testing.T) {
	tr, _ := newTestTransport(t, newFake(rootEntry(), ourEntry()))
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	send(t, tr, 1, wireRegister("op1"))
	if r := send(t, tr, 1, client.Request{Op: client.OpReclaim, OpID: "op1"}); r.Code != client.CodeAlreadyBound {
		t.Fatalf("rebind on the same connection: %+v", r)
	}
	// A failed bind leaves the connection free to bind.
	tr.peerOpen(lService, 2, testUID, 1, acceptAll)
	if r := send(t, tr, 2, client.Request{Op: client.OpReclaim, OpID: "nope"}); r.Code != client.CodeNoReceipt {
		t.Fatalf("reclaim of unknown op: %+v", r)
	}
	if r := send(t, tr, 2, client.Request{Op: client.OpReclaim, OpID: "op1"}); r.Code != client.CodeOK {
		t.Fatalf("bind after a failed bind: %+v", r)
	}
}

func TestRetirePendingThenEjectNow(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true // a healthy, busy volume
	tr, _ := newTestTransport(t, k)
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	send(t, tr, 1, wireRegister("op1"))
	r := send(t, tr, 1, client.Request{Op: client.OpRetire, OpID: "op1",
		Retire: &client.RetireArgs{Liveness: client.LivenessAlive}, WaitMillis: 50})
	if r.Code != client.CodePending || r.Outcome == nil || r.Outcome.Terminal() || r.Outcome.Forced {
		t.Fatalf("busy retire: %+v", r)
	}
	r = send(t, tr, 1, client.Request{Op: client.OpRetire, OpID: "op1",
		Retire: &client.RetireArgs{Liveness: client.LivenessAlive, EjectNow: true}, WaitMillis: 2000})
	if r.Code != client.CodeOK || r.Outcome.State != "RETIRED" || !r.Outcome.Forced {
		t.Fatalf("eject now: %+v", r)
	}
	// The finished op holds nothing: reclaim reports it gone with the outcome.
	tr.peerOpen(lService, 2, testUID, 1, acceptAll)
	if r := send(t, tr, 2, client.Request{Op: client.OpReclaim, OpID: "op1"}); r.Code != client.CodeNoReceipt || r.Outcome.State != "RETIRED" {
		t.Fatalf("reclaim of retired op: %+v", r)
	}
}

func TestPerConnectionInflightIsBounded(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	tr, _ := newTestTransport(t, k)
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	send(t, tr, 1, wireRegister("op1"))
	var wg sync.WaitGroup
	for i := 0; i < maxPeerInflight; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			trySend(t, tr, 1, client.Request{Op: client.OpRetire, OpID: "op1",
				Retire: &client.RetireArgs{Liveness: client.LivenessAlive}, WaitMillis: 400})
		}()
	}
	if !waitFor(t, time.Second, func() bool {
		tr.mu.Lock()
		defer tr.mu.Unlock()
		return tr.peers[1].inflight == maxPeerInflight
	}) {
		t.Fatal("retires did not all start")
	}
	if r := send(t, tr, 1, client.Request{Op: client.OpPing}); r.Code != client.CodeBusy {
		t.Fatalf("request over the in-flight bound: %+v", r)
	}
	wg.Wait()
}

func TestLaunchSweepArmsOrphansAndAReclaimCancels(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, st := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	svc.Close()

	// The daemon restarts (crash or app update) with the mount still present.
	restart := func() *Service {
		s, err := NewService(Config{Store: st, Kernel: k, Policy: fastPolicy()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s
	}
	s2 := restart()
	rep, err := s2.SweepAtLaunch()
	if err != nil || len(rep.Pending) != 1 {
		t.Fatalf("sweep: %+v %v", rep, err)
	}
	if n := ArmLaunchOrphans(s2, rep, t.Logf); n != 1 {
		t.Fatalf("armed %d", n)
	}
	tr := newXPCTransport(s2, client.ChannelRelease, t.Logf)
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	if r := send(t, tr, 1, client.Request{Op: client.OpReclaim, OpID: "op1"}); !r.Reclaimed {
		t.Fatalf("reclaim after restart: %+v", r)
	}
	time.Sleep(4 * fastPolicy().OrphanReclaim)
	if calls := k.recorded(); len(calls) != 0 {
		t.Fatalf("reclaimed receipt retired: %v", calls)
	}

	// With nobody reclaiming, the orphan window ends in an ordinary unmount.
	s2.Close()
	s3 := restart()
	rep, _ = s3.SweepAtLaunch()
	ArmLaunchOrphans(s3, rep, t.Logf)
	if !waitFor(t, 2*time.Second, func() bool { return len(k.recorded()) > 0 }) {
		t.Fatal("unclaimed launch receipt was never retired")
	}
	if got := flagsOf(k.recorded()); got != "N" {
		t.Fatalf("flags %s", got)
	}
}

// loopConn routes a client.Conn straight into the transport's handler, so
// the client and daemon halves are tested against each other.
type loopConn struct {
	tr     *xpcTransport
	id     uint64
	events chan client.Event
}

func (l *loopConn) RoundTrip(ctx context.Context, v int64, body []byte) (int64, []byte, error) {
	out := l.tr.handle(l.id, v, body)
	if out == nil {
		return 0, nil, client.ErrUnavailable
	}
	return client.ProtocolVersion, out, nil
}
func (l *loopConn) Events() <-chan client.Event { return l.events }
func (l *loopConn) Close()                      { l.tr.peerClosed(l.id) }

func TestClientAgainstTransport(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	tr, _ := newTestTransport(t, k)
	var next atomic.Uint64
	c := client.New(client.Options{CallTimeout: time.Second, Dial: func(service, req string) (client.Conn, error) {
		id := next.Add(1)
		tr.peerOpen(lService, id, testUID, 1, acceptAll)
		return &loopConn{tr: tr, id: id, events: make(chan client.Event)}, nil
	}})
	defer c.Close()
	ctx := context.Background()
	reg := wireRegister("op1")
	lease, rec, err := c.Register(ctx, client.RegisterRequest{OpID: "op1", RegisterArgs: *reg.Register})
	if err != nil || rec.OpID != "op1" || rec.UID != testUID || rec.FSID != [2]int32(ourFSID) {
		t.Fatalf("register: %+v %v", rec, err)
	}
	if _, _, err := c.Register(ctx, client.RegisterRequest{OpID: "op1", RegisterArgs: *reg.Register}); !errors.Is(err, client.ErrReceiptExists) && !errors.Is(err, client.ErrOpIDReused) {
		t.Fatalf("duplicate register: %v", err)
	}
	if _, err := c.Status(ctx, "nope"); !errors.Is(err, client.ErrNoReceipt) {
		t.Fatalf("status of unknown op: %v", err)
	}
	out, err := c.Retire(ctx, "op1", client.RetireRequest{Liveness: client.LivenessAlive}, 2*time.Second)
	if err != nil || out.State != "RETIRED" || out.Forced {
		t.Fatalf("retire: %+v %v", out, err)
	}
	lease.Close()
	if got := flagsOf(k.recorded()); got != "N" {
		t.Fatalf("flags %s", got)
	}
}

func orphanPending(svc *Service, uid uint32, op string) bool {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	_, ok := svc.orphans[jobKey{uid, op}]
	return ok
}

// Review race, ordering 1: the restarted service's reclaim reaches the core
// (nothing to cancel yet), then the old connection closes and is judged the
// last one (timer armed), then the reclaim binds. The bind must cancel the
// timer it raced with. Deterministic: the close runs inside the hook.
func TestBindCancelsATimerArmedWhileItWasInFlight(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	tr, cs := newTestTransport(t, k)
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	send(t, tr, 1, wireRegister("op1"))
	tr.peerOpen(lService, 2, testUID, 1, acceptAll)
	tr.beforeBind = func(peer uint64) {
		if peer == 2 {
			tr.peerClosed(1)
			if !orphanPending(cs.Service, testUID, "op1") {
				t.Error("setup: closing the last bound connection did not arm the timer")
			}
		}
	}
	r := send(t, tr, 2, client.Request{Op: client.OpReclaim, OpID: "op1"})
	if r.Code != client.CodeOK || !r.Reclaimed {
		t.Fatalf("reclaim: %+v", r)
	}
	if orphanPending(cs.Service, testUID, "op1") {
		t.Fatal("orphan timer still armed while connection 2 is bound to the op")
	}
}

// Review race, ordering 2 (the reviewer's): the old connection is judged the
// last one, and before its orphan timer is armed the restarted service's
// reclaim runs and binds. The arm and the bind must serialize: the bind may
// not complete while the arm is pending, and must cancel it afterwards.
func TestStaleDisconnectCannotArmAfterABind(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	tr, cs := newTestTransport(t, k)
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	send(t, tr, 1, wireRegister("op1"))
	tr.peerOpen(lService, 2, testUID, 1, acceptAll)
	done := make(chan client.Response, 1)
	tr.beforeDisconnect = func(jobKey) {
		go func() { done <- trySend(t, tr, 2, client.Request{Op: client.OpReclaim, OpID: "op1"}) }()
		// With the arm under t.mu the reclaim cannot bind now. Give it every
		// chance to (it has no other work): if it completes, the window the
		// reviewer reproduced is open.
		select {
		case r := <-done:
			done <- r
			t.Error("a reclaim bound while the last connection's disconnect was pending")
		case <-time.After(200 * time.Millisecond):
		}
	}
	tr.peerClosed(1)
	r := <-done
	if r.Code != client.CodeOK {
		t.Fatalf("reclaim: %+v", r)
	}
	if orphanPending(cs.Service, testUID, "op1") {
		t.Fatal("op retired while connection 2 is bound to it: the stale disconnect armed the timer after the bind")
	}
	time.Sleep(4 * fastPolicy().OrphanReclaim)
	if calls := k.recorded(); len(calls) != 0 {
		t.Fatalf("op retired while connection 2 is bound to it: %v", calls)
	}
}

// The owner's connection closes while its register is in flight: the
// receipt now exists with nobody holding it, so the orphan window must start.
func TestCloseDuringBindArmsTheOrphanWindow(t *testing.T) {
	for _, op := range []string{client.OpRegister, client.OpReclaim} {
		t.Run(op, func(t *testing.T) {
			k := newFake(rootEntry(), ourEntry())
			tr, cs := newTestTransport(t, k)
			tr.peerOpen(lService, 1, testUID, 1, acceptAll)
			req := wireRegister("op1")
			if op == client.OpReclaim {
				// A receipt from a previous owner, nobody bound.
				mustRegister(t, cs.Service, nfsRegister("op1"))
				req = client.Request{Op: client.OpReclaim, OpID: "op1"}
			}
			tr.beforeBind = func(peer uint64) { tr.peerClosed(peer) }
			send(t, tr, 1, req)
			if !orphanPending(cs.Service, testUID, "op1") {
				t.Fatal("connection closed during its bind, but no orphan window was armed")
			}
			if !waitFor(t, 2*time.Second, func() bool { return len(k.recorded()) > 0 }) {
				t.Fatal("orphan window armed but never retired the unheld receipt")
			}
		})
	}
}

func TestQuiesceIsAtomicWithDispatch(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	tr, cs := newTestTransport(t, k)
	tr.peerOpen(lService, 1, testUID, 1, acceptAll)
	send(t, tr, 1, wireRegister("op1"))
	alwaysIdle := func() bool { return true }

	// A request in flight blocks the exit even if the core looks idle.
	started := make(chan struct{})
	release := make(chan struct{})
	tr.beforeBind = func(uint64) { close(started); <-release }
	tr.peerOpen(lService, 2, testUID, 1, acceptAll)
	go trySend(t, tr, 2, client.Request{Op: client.OpReclaim, OpID: "op1"})
	<-started
	if tr.quiesce(alwaysIdle) {
		t.Fatal("quiesced while a request was being dispatched")
	}
	close(release)
	if !waitFor(t, time.Second, func() bool { tr.mu.Lock(); defer tr.mu.Unlock(); return tr.active == 0 }) {
		t.Fatal("request never finished")
	}
	tr.beforeBind = nil
	if tr.quiesce(func() bool { return false }) {
		t.Fatal("quiesced while the core had work")
	}
	if r := send(t, tr, 1, client.Request{Op: client.OpPing}); r.Code != client.CodeOK {
		t.Fatalf("a failed quiesce left the transport refusing requests: %+v", r)
	}
	if !tr.quiesce(cs.Service.idle) {
		t.Fatal("did not quiesce when idle")
	}
	// After the decision, nothing new may reach the core.
	before := cs.calls.Load()
	r := send(t, tr, 1, client.Request{Op: client.OpRetire, OpID: "op1",
		Retire: &client.RetireArgs{Liveness: client.LivenessDead, EjectNow: true}})
	if r.Code != client.CodeBusy || cs.calls.Load() != before {
		t.Fatalf("request after quiesce: %+v (core calls %d -> %d)", r, before, cs.calls.Load())
	}
}

// The client's Cancel crosses the transport: an ordinary busy job is
// withdrawn (receipt kept), a force-path job is refused with a typed error.
func TestClientCancelAgainstTransport(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	st, err := OpenStore(testRoot(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	pol := fastPolicy()
	pol.BackoffMin, pol.BackoffMax, pol.ForceConfirmGap = time.Hour, time.Hour, time.Hour
	svc, err := NewService(Config{Store: st, Kernel: k, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	tr := newXPCTransport(svc, client.ChannelRelease, t.Logf)
	var next atomic.Uint64
	c := client.New(client.Options{CallTimeout: time.Second, Dial: func(service, req string) (client.Conn, error) {
		id := next.Add(1)
		tr.peerOpen(lService, id, testUID, 1, acceptAll)
		return &loopConn{tr: tr, id: id, events: make(chan client.Event)}, nil
	}})
	defer c.Close()
	ctx := context.Background()
	reg := wireRegister("op1")
	lease, _, err := c.Register(ctx, client.RegisterRequest{OpID: "op1", RegisterArgs: *reg.Register})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if _, err := c.Retire(ctx, "op1", client.RetireRequest{Liveness: client.LivenessAlive}, 50*time.Millisecond); !errors.Is(err, client.ErrPending) {
		t.Fatalf("busy retire: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { o, _ := c.Status(ctx, "op1"); return o.State == "BACKOFF" }) {
		t.Fatal("no BACKOFF")
	}
	out, err := c.Cancel(ctx, "op1")
	if err != nil || out.State != "CANCELLED" {
		t.Fatalf("cancel: %+v %v", out, err)
	}
	if o, err := c.Status(ctx, "op1"); err != nil || o.State != "REGISTERED" {
		t.Fatalf("status after cancel: %+v %v", o, err)
	}
	if _, err := c.Retire(ctx, "op1", client.RetireRequest{Liveness: client.LivenessAlive, EjectNow: true}, 50*time.Millisecond); !errors.Is(err, client.ErrPending) {
		t.Fatalf("eject-now: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { o, _ := c.Status(ctx, "op1"); return o.State == "FORCE_CONFIRM" }) {
		t.Fatal("no FORCE_CONFIRM")
	}
	if _, err := c.Cancel(ctx, "op1"); !errors.Is(err, client.ErrNotCancellable) {
		t.Fatalf("force path cancel: %v", err)
	}
}
