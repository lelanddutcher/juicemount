package main

// Review 2 finding F3: the approved-step login check, and the enrollment
// request to the Manager, take the route the mount takes. With JuiceMount
// Link that is the Link node (in this process) or the volume owner's
// loopback Redis proxy, never plain macOS networking to the NAS address.

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/enrollclient"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// The NAS as configured: TEST-NET-1, unreachable from macOS networking, so a
// check that dials it directly cannot pass.
const routeNASRedis = "redis://192.0.2.10:6379/1"
const routeNASManager = "http://192.0.2.10:30190"

// fakeLink stands in for the Link node's adaptive dialer: it maps the NAS
// address:port to a local listener and records what it was asked to dial.
type fakeLink struct {
	mu     sync.Mutex
	routes map[string]string
	dialed []string
}

func (f *fakeLink) dial(ctx context.Context, network, address string) (net.Conn, error) {
	f.mu.Lock()
	f.dialed = append(f.dialed, address)
	to, ok := f.routes[address]
	f.mu.Unlock()
	if !ok {
		return nil, errors.New("fake link: no route to " + address)
	}
	var d net.Dialer
	return d.DialContext(ctx, network, to)
}

func (f *fakeLink) sawDial(address string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.dialed {
		if a == address {
			return true
		}
	}
	return false
}

func installFakeLink(t *testing.T, link *fakeLink) {
	t.Helper()
	old := inProcessLinkDial
	if link == nil {
		inProcessLinkDial = func() contextDial { return nil }
	} else {
		inProcessLinkDial = func() contextDial { return link.dial }
	}
	t.Cleanup(func() { inProcessLinkDial = old })
}

// refusingDoer is the operating-system Manager client; it must not be used
// when Link is the route.
type refusingDoer struct{ calls int }

func (r *refusingDoer) Do(*http.Request) (*http.Response, error) {
	r.calls++
	return nil, errors.New("macOS networking cannot reach the NAS")
}

func TestH8BEnrollThroughInProcessLink(t *testing.T) {
	stub := &stubManager{}
	mgr := httptest.NewServer(stub)
	defer mgr.Close()
	osClient := &refusingDoer{}
	old := enrollHTTPClient
	enrollHTTPClient = osClient
	t.Cleanup(func() { enrollHTTPClient = old })
	db := filepath.Join(t.TempDir(), "metadata.db")

	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	installFakeLink(t, link)

	// Enrollment request and poll go to the Manager through Link.
	out := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, enrollclient.State{})
	out = runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, out.State)
	if out.Phase != enrollclient.Waiting {
		t.Fatalf("enroll through Link: phase %s %q", out.Phase, out.Message)
	}
	if osClient.calls != 0 || !link.sawDial("192.0.2.10:30190") {
		t.Fatalf("Manager request did not use Link (OS client calls %d, Link dials %v)", osClient.calls, link.dialed)
	}

	// Approved: the login check reaches the proxy through Link too.
	stub.mu.Lock()
	stub.approved = true
	stub.mu.Unlock()
	proxy := newFakeRESP(t, out.State.Principal, out.State.Secret)
	link.mu.Lock()
	link.routes["192.0.2.10:6379"] = proxy.ln.Addr().String()
	link.mu.Unlock()
	got := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, out.State)
	if got.Phase != enrollclient.Approved || !got.CredentialsLive {
		t.Fatalf("approved through Link: phase %s live=%v check=%q", got.Phase, got.CredentialsLive, got.CredentialCheck)
	}
	if !link.sawDial("192.0.2.10:6379") {
		t.Fatalf("the login check did not dial the NAS Redis through Link: %v", link.dialed)
	}
	proxy.mu.Lock()
	users := append([]string(nil), proxy.authUsers...)
	proxy.mu.Unlock()
	if len(users) == 0 || users[len(users)-1] != out.State.Principal {
		t.Fatalf("the check did not log in as the principal: %v", users)
	}
}

func TestH8BPrincipalLoginCheckThroughVolumeOwner(t *testing.T) {
	installFakeLink(t, nil)
	const principal, secret = "mac-4f1c2a9e7b3d", "principal-check-fixture-aaaaaaaaaaaaaaaaaaaaaa"
	ownerProxy := newFakeRESP(t, principal, secret) // the owner's loopback Redis proxy
	owner := &ownerFacade{
		claimed:         volumeOwnerReceipt{LinkEnabled: true, RedisURL: "redis://jmclient@" + ownerProxy.ln.Addr().String() + "/1"},
		logicalRedisURL: "redis://jmclient@192.0.2.10:6379/1",
	}
	globalMu.Lock()
	oldOwner := globalOwner
	globalOwner = owner
	globalMu.Unlock()
	t.Cleanup(func() { globalMu.Lock(); globalOwner = oldOwner; globalMu.Unlock() })

	if r := currentPrincipalLoginRoute(routeNASRedis); r.Name != "owner-link" || r.Addr != ownerProxy.ln.Addr().String() {
		t.Fatalf("route = %+v, want the owner's loopback proxy", r)
	}
	if ok, why := verifyPrincipalLogin(routeNASRedis, principal, secret); !ok {
		t.Fatalf("check through the owner's loopback proxy: %s", why)
	}
	// The check's endpoint is another server than the one the owner was
	// claimed for: the owner's proxy is not its route.
	if r := currentPrincipalLoginRoute("redis://192.0.2.99:6379/1"); r.Name != "system" {
		t.Fatalf("other endpoint route = %+v, want system", r)
	}
	// An owner without Link: the mount uses macOS networking, so does the check.
	owner.mu.Lock()
	owner.claimed.LinkEnabled = false
	owner.mu.Unlock()
	if r := currentPrincipalLoginRoute(routeNASRedis); r.Name != "system" {
		t.Fatalf("owner without Link route = %+v, want system", r)
	}
	// A receipt whose proxy is not on loopback is never used.
	owner.mu.Lock()
	owner.claimed.LinkEnabled = true
	owner.claimed.RedisURL = "redis://192.0.2.50:6379/1"
	owner.mu.Unlock()
	if r := currentPrincipalLoginRoute(routeNASRedis); r.Name != "system" {
		t.Fatalf("non-loopback receipt route = %+v, want system", r)
	}
}

// A TLS Redis address is never checked over the Link route in plaintext.
func TestH8BPrincipalLoginCheckRefusesTLSOverLink(t *testing.T) {
	link := &fakeLink{routes: map[string]string{}}
	ok, why := verifyPrincipalLoginVia(principalLoginRoute{Name: "link", Dial: link.dial}, "rediss://192.0.2.10:6379/1", "mac-4f1c2a9e7b3d", "x")
	if ok || !strings.Contains(why, "TLS") || len(link.dialed) != 0 {
		t.Fatalf("TLS over Link: ok=%v %q dials=%v", ok, why, link.dialed)
	}
}

// The serving process answers "enroll-verify" with the check over its own
// route (desktop archive: the app forwards the check here).
func TestH8BServingProcessAnswersEnrollVerify(t *testing.T) {
	const principal, secret = "mac-4f1c2a9e7b3d", "principal-check-fixture-aaaaaaaaaaaaaaaaaaaaaa"
	proxy := newFakeRESP(t, principal, secret)
	link := &fakeLink{routes: map[string]string{"192.0.2.10:6379": proxy.ln.Addr().String()}}
	installFakeLink(t, link)
	setServingRedisURL(t, "redis://legacyuser@192.0.2.10:6379/1")
	res := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "enroll-verify",
		Strings: []string{routeNASRedis, principal, secret}})
	if res.Error != "" {
		t.Fatal(res.Error)
	}
	var out enrollVerifyResult
	if err := json.Unmarshal([]byte(res.Text), &out); err != nil {
		t.Fatal(err)
	}
	if !out.Live || out.Route != "link" || !link.sawDial("192.0.2.10:6379") {
		t.Fatalf("enroll-verify = %+v (dials %v)", out, link.dialed)
	}
	if strings.Contains(res.Text, secret) {
		t.Fatal("the enroll-verify answer carries the secret")
	}
}

func setServingRedisURL(t *testing.T, url string) {
	t.Helper()
	old := servingRedisURL.Load()
	if url == "" {
		servingRedisURL.Store(nil)
	} else {
		noteServingRedisURL(url)
	}
	t.Cleanup(func() { servingRedisURL.Store(old) })
}

// Review 3 finding 4: "enroll-verify" must not let the caller choose the
// target. The serving process checks only the endpoint it was started with,
// and only as a Mac principal; anything else is refused before any dial, so
// it cannot be used as a login or port oracle through its Link node.
func TestH8BEnrollVerifyRefusesCallerChosenTarget(t *testing.T) {
	victim := newFakeRESP(t, "someuser", "guess-1")
	mine := newFakeRESP(t, "mac-4f1c2a9e7b3d", "principal-check-fixture-aaaaaaaaaaaaaaaaaaaaaa")
	link := &fakeLink{routes: map[string]string{
		"10.66.77.88:7000": victim.ln.Addr().String(),
		"192.0.2.10:6379":  mine.ln.Addr().String(),
	}}
	installFakeLink(t, link)
	call := func(url, user, pass string) enrollVerifyResult {
		res := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "enroll-verify",
			Strings: []string{url, user, pass}})
		var out enrollVerifyResult
		if err := json.Unmarshal([]byte(res.Text), &out); err != nil {
			t.Fatalf("%s %s: %v %q", url, user, err, res.Error)
		}
		return out
	}
	// Never started: nothing is checked.
	setServingRedisURL(t, "")
	if out := call(routeNASRedis, "mac-4f1c2a9e7b3d", "principal-check-fixture-aaaaaaaaaaaaaaaaaaaaaa"); out.Live || out.Route != "refused" {
		t.Fatalf("no configured endpoint: %+v", out)
	}
	setServingRedisURL(t, routeNASRedis)
	for _, tc := range []struct{ url, user, pass string }{
		{"redis://10.66.77.88:7000/0", "someuser", "guess-1"},         // another target, not a Mac
		{"redis://10.66.77.88:7000/0", "mac-4f1c2a9e7b3d", "guess-1"}, // another target
		{"redis://10.66.77.99:22/0", "mac-4f1c2a9e7b3d", "x"},          // a port probe
		{routeNASRedis, "someuser", "guess-1"},                         // the right endpoint, not a Mac principal
		{routeNASRedis, "admin", "guess-1"},
	} {
		if out := call(tc.url, tc.user, tc.pass); out.Live || out.Route != "refused" || strings.Contains(out.Reason, tc.pass) {
			t.Fatalf("%s as %s: %+v", tc.url, tc.user, out)
		}
	}
	if len(link.dialed) != 0 {
		t.Fatalf("refused checks dialled %v", link.dialed)
	}
	// The configured endpoint as a Mac principal is checked.
	if out := call("redis://192.0.2.10:6379/1", "mac-4f1c2a9e7b3d", "principal-check-fixture-aaaaaaaaaaaaaaaaaaaaaa"); !out.Live || out.Route != "link" {
		t.Fatalf("own endpoint: %+v", out)
	}
}
