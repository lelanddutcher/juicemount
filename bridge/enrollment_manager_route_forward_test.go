//go:build serviceclient

package main

// Review 2 finding F3 (the remaining blocker), desktop archive: the app
// process holds no Link node, so its enrollment request to the Manager goes
// to the serving process ("enroll-http"), which holds the Link route.

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/enrollclient"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
)

// inServingProcess is true while the test's serving process handles a
// request. The app and the serving process share this test process, so
// installServingLink gives the Link node to the serving side only: the app
// side must not find one, as in the desktop archive.
var inServingProcess atomic.Bool

func installServingLink(t *testing.T, link *fakeLink) {
	t.Helper()
	old := inProcessLinkDial
	inProcessLinkDial = func() contextDial {
		if inServingProcess.Load() {
			return link.dial
		}
		return nil
	}
	t.Cleanup(func() { inProcessLinkDial = old })
}

// installServingProcess runs dispatchServingRequest behind a real control
// socket in the test's scratch directory (never the user's live service).
func installServingProcess(t *testing.T) *atomic.Int32 {
	t.Helper()
	loginChecks := new(atomic.Int32)
	dir, err := os.MkdirTemp(os.Getenv("TMPDIR"), "lm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	_ = os.Chmod(dir, 0o700)
	sock := filepath.Join(dir, "c.sock")
	srv, err := servicecontrol.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(func(req servicecontrol.Request) servicecontrol.Response {
		inServingProcess.Store(true)
		defer inServingProcess.Store(false)
		if req.Method == "enroll-verify" && len(req.Strings) == 3 && macPrincipalRe.MatchString(req.Strings[1]) {
			loginChecks.Add(1)
		}
		return dispatchServingRequest(req)
	})
	t.Cleanup(func() { srv.Close() })
	oldCall, oldPath := callServingProcess, servingSocketPath
	callServingProcess, servingSocketPath = servicecontrol.Call, func() (string, error) { return sock, nil }
	t.Cleanup(func() { callServingProcess, servingSocketPath = oldCall, oldPath })
	return loginChecks
}

// A Mac away from the LAN: the NAS (TEST-NET-1) is unreachable over macOS
// networking and reachable only through the serving process's Link node.
// The app enrolls, is approved and passes the login check, all over Link,
// through the real control socket.
func TestLinkManagerDesktopEnrollsThroughServingProcessLink(t *testing.T) {
	stub := &stubManager{}
	mgr := httptest.NewServer(stub)
	defer mgr.Close()
	osClient := &refusingDoer{}
	old := enrollHTTPClient
	enrollHTTPClient = osClient
	t.Cleanup(func() { enrollHTTPClient = old })
	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	installServingLink(t, link) // the serving process's Link node; the app has none
	setServingRedisURL(t, "redis://legacyuser@192.0.2.10:6379/1")
	installServingProcess(t)
	db := filepath.Join(t.TempDir(), "metadata.db")

	out := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, enrollclient.State{})
	out = runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, out.State)
	if out.Phase != enrollclient.Waiting || out.State.PollToken != enrollToken {
		t.Fatalf("enroll through the serving process's Link: phase %s %q", out.Phase, out.Message)
	}
	if osClient.calls != 0 || !link.sawDial("192.0.2.10:30190") {
		t.Fatalf("the Manager request did not go through the serving process's Link (macOS calls %d, Link dials %v)", osClient.calls, link.dialed)
	}

	stub.mu.Lock()
	stub.approved = true
	stub.mu.Unlock()
	proxy := newFakeRESP(t, out.State.Principal, out.State.Secret)
	link.mu.Lock()
	link.routes["192.0.2.10:6379"] = proxy.ln.Addr().String()
	link.mu.Unlock()
	got := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, out.State)
	if got.Phase != enrollclient.Approved || !got.CredentialsLive || got.ObjectAccessKey != enrollTestAK {
		t.Fatalf("approved through Link: phase %s live=%v check=%q key=%q", got.Phase, got.CredentialsLive, got.CredentialCheck, got.ObjectAccessKey)
	}
	if osClient.calls != 0 {
		t.Fatalf("macOS networking was used: %d calls", osClient.calls)
	}
}

// countingDoer reaches a local Manager over macOS networking and counts.
type countingDoer struct {
	calls int
	c     *http.Client
}

func (d *countingDoer) Do(r *http.Request) (*http.Response, error) {
	d.calls++
	return d.c.Do(r)
}

func installOSClient(t *testing.T, d enrollclient.Doer) {
	t.Helper()
	old := enrollHTTPClient
	enrollHTTPClient = d
	t.Cleanup(func() { enrollHTTPClient = old })
}

// Nothing reached the serving process (no control socket, an older build
// that refuses the method or the build, or a refusal of the target): the app
// sends over macOS networking itself, as before, and says so when that
// fails.
func TestLinkManagerDesktopFallsBackOnlyWhenNothingWasSent(t *testing.T) {
	installFakeLink(t, nil)
	mgr := httptest.NewServer(&stubManager{})
	defer mgr.Close()
	db := filepath.Join(t.TempDir(), "metadata.db")
	first := runEnrollStepWithRedis(t, mgr.URL, db, "", enrollclient.State{}) // mints the secret, sends nothing

	for _, tc := range []struct {
		name string
		call func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error)
		says string
	}{
		{"no service", func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
			return servicecontrol.Response{}, &net.OpError{Op: "dial", Net: "unix", Err: syscall.ECONNREFUSED}
		}, "no JuiceMount service is running"},
		{"older build", func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
			return servicecontrol.Response{Version: 1, Error: "serving service build/protocol differs; existing runtime retained"}, nil
		}, "older build"},
		{"unknown method", func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
			return servicecontrol.Response{Version: 1, Error: "invalid serving service operation or arguments"}, nil
		}, "older build"},
		{"refused target", func(_ context.Context, _ string, req servicecontrol.Request) (servicecontrol.Response, error) {
			return servicecontrol.Response{Version: 1, Text: `{"route":"refused","refused":"the Manager address is not the Redis host on port 30190, so the JuiceMount service does not carry it"}`}, nil
		}, "not the Redis host on port 30190"},
	} {
		installControlFake(t, tc.call)
		direct := &countingDoer{c: newEnrollHTTPClient()}
		installOSClient(t, direct)
		if out := runEnrollStepWithRedis(t, mgr.URL, db, "", first.State); out.Phase != enrollclient.Waiting || direct.calls != 1 {
			t.Fatalf("%s: phase %s %q, macOS calls %d", tc.name, out.Phase, out.Message, direct.calls)
		}
		// The same with the Manager unreachable over macOS networking: the
		// failure names the route.
		installOSClient(t, &refusingDoer{})
		out := runEnrollStepWithRedis(t, mgr.URL, db, "", first.State)
		if out.Phase != enrollclient.Failed || !strings.Contains(out.Message, tc.says) || !strings.Contains(out.Message, "macOS networking") {
			t.Fatalf("%s, unreachable: phase %s %q", tc.name, out.Phase, out.Message)
		}
	}
}

// Once the request may have reached the serving process (it may have sent
// the secret to the Manager), a lost or late answer is a failure that
// enrollment retries; the app never sends it a second time by another route.
func TestLinkManagerDesktopNoSecondSendAfterServingProcessGotIt(t *testing.T) {
	installFakeLink(t, nil)
	mgr := httptest.NewServer(&stubManager{})
	defer mgr.Close()
	db := filepath.Join(t.TempDir(), "metadata.db")
	first := runEnrollStepWithRedis(t, mgr.URL, db, "", enrollclient.State{})
	for _, tc := range []struct {
		name string
		call func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error)
	}{
		{"deadline", func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
			return servicecontrol.Response{}, context.DeadlineExceeded
		}},
		{"lost reply", func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
			return servicecontrol.Response{}, &net.OpError{Op: "read", Net: "unix", Err: errors.New("connection reset")}
		}},
		{"unreadable answer", func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
			return servicecontrol.Response{Version: 1, Text: "not json"}, nil
		}},
		{"Link could not reach the Manager", func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
			return servicecontrol.Response{Version: 1, Text: `{"route":"link","error":"the Manager could not be reached over JuiceMount Link"}`}, nil
		}},
	} {
		installControlFake(t, tc.call)
		direct := &countingDoer{c: newEnrollHTTPClient()}
		installOSClient(t, direct)
		out := runEnrollStepWithRedis(t, mgr.URL, db, "", first.State)
		if out.Phase != enrollclient.Failed || direct.calls != 0 {
			t.Fatalf("%s: phase %s %q, macOS calls %d (want a failure and no second send)", tc.name, out.Phase, out.Message, direct.calls)
		}
		if strings.Contains(out.Message, "macOS networking") {
			t.Fatalf("%s: message claims a macOS send: %q", tc.name, out.Message)
		}
	}
}

// A selected owner exists and has Link, but its Manager route currently
// fails. The desktop must not retry over macOS networking after owner dispatch.
func TestLinkManagerDesktopSelectedOwnerFailureHasNoSystemFallback(t *testing.T) {
	link := &fakeLink{routes: map[string]string{}}
	selectedEnrollOwner(t, link)
	installServingProcess(t)
	osClient := &refusingDoer{}
	installOSClient(t, osClient)
	db := filepath.Join(t.TempDir(), "metadata.db")
	first := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, enrollclient.State{})
	out := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, first.State)
	if out.Phase != enrollclient.Failed || osClient.calls != 0 || !link.sawDial("192.0.2.10:30190") {
		t.Fatalf("owner failure fell through: phase %s, macOS calls %d, Link dials %v", out.Phase, osClient.calls, link.dialed)
	}
}

// This is the desktop archive's complete selected-owner route: app socket,
// serving dispatcher, owner socket, Manager over owner Link, then the owner's
// loopback Redis proxy for the approved credential check.
func TestLinkManagerDesktopEnrollsAndVerifiesThroughSelectedOwner(t *testing.T) {
	stub := &stubManager{}
	mgr := httptest.NewServer(stub)
	defer mgr.Close()
	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	o, f := selectedEnrollOwner(t, link)
	loginChecks := installServingProcess(t)
	osClient := &refusingDoer{}
	installOSClient(t, osClient)
	db := filepath.Join(t.TempDir(), "metadata.db")
	out := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, enrollclient.State{})
	out = runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, out.State)
	if out.Phase != enrollclient.Waiting || out.State.PollToken != enrollToken || osClient.calls != 0 || !link.sawDial("192.0.2.10:30190") {
		t.Fatalf("selected owner did not enroll: phase %s, token %q, OS calls %d, dials %v", out.Phase, out.State.PollToken, osClient.calls, link.dialed)
	}
	stub.mu.Lock()
	stub.approved = true
	stub.mu.Unlock()
	proxy := newFakeRESP(t, out.State.Principal, out.State.Secret)
	loopback := "redis://jmclient@" + proxy.ln.Addr().String() + "/1"
	o.mu.Lock()
	o.endpoint = loopback
	o.mu.Unlock()
	f.mu.Lock()
	f.claimed.RedisURL = loopback
	f.mu.Unlock()
	got := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, out.State)
	if got.Phase != enrollclient.Approved || !got.CredentialsLive || got.ObjectAccessKey != enrollTestAK || osClient.calls != 0 {
		t.Fatalf("selected owner approval/check: phase %s, live %v, key %q, OS calls %d, detail %q", got.Phase, got.CredentialsLive, got.ObjectAccessKey, osClient.calls, got.CredentialCheck)
	}
	proxy.mu.Lock()
	users := append([]string(nil), proxy.authUsers...)
	proxy.mu.Unlock()
	if len(users) == 0 || users[len(users)-1] != out.State.Principal {
		t.Fatalf("approved principal did not authenticate through owner's Redis proxy: %v", users)
	}
	if loginChecks.Load() != 1 {
		t.Fatalf("credential verification bypassed the serving control socket: dispatched %d login checks", loginChecks.Load())
	}
}

// The owner completed the POST and the Manager accepted its secret before
// the engine loses the owner reply. Desktop failure must not send it again.
func TestLinkManagerDesktopLostOwnerReplyAfterManagerAcceptsDoesNotRetry(t *testing.T) {
	stub := &stubManager{}
	var posts atomic.Int32
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		stub.ServeHTTP(w, r)
	}))
	defer mgr.Close()
	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	_, f := selectedEnrollOwner(t, link)
	var dropped atomic.Bool
	var accepted atomic.Bool
	f.policyCall = func(ctx context.Context, path string, req servicecontrol.Request) (servicecontrol.Response, error) {
		resp, err := servicecontrol.Call(ctx, path, req)
		if err == nil && req.Method == enrollHTTPMethod && !dropped.Swap(true) {
			var receipt volumeOwnerReceipt
			if resp.Error != "" || json.Unmarshal([]byte(resp.Text), &receipt) != nil || receipt.EnrollHTTP == nil ||
				receipt.EnrollHTTP.Status != http.StatusAccepted || receipt.EnrollHTTP.Error != "" || receipt.EnrollHTTP.Refused != "" {
				t.Errorf("lost-reply fixture did not observe Manager acceptance: response error=%q", resp.Error)
			} else {
				accepted.Store(true)
			}
			return servicecontrol.Response{}, context.DeadlineExceeded
		}
		return resp, err
	}
	installServingProcess(t)
	osClient := &refusingDoer{}
	installOSClient(t, osClient)
	db := filepath.Join(t.TempDir(), "metadata.db")
	first := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, enrollclient.State{})
	out := runEnrollStepWithRedis(t, routeNASManager, db, routeNASRedis, first.State)
	if out.Phase != enrollclient.Failed || !dropped.Load() || !accepted.Load() || posts.Load() != 1 || osClient.calls != 0 || !link.sawDial("192.0.2.10:30190") {
		t.Fatalf("lost accepted owner reply replayed: phase %s, dropped %v, posts %d, OS calls %d, dials %v", out.Phase, dropped.Load(), posts.Load(), osClient.calls, link.dialed)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.bodies) != 1 || first.State.Secret == "" || stub.bodies[0]["secret"] != first.State.Secret {
		t.Fatal("Manager did not accept exactly the enrollment secret that the Mac sent")
	}
}

// The serving process's request over a slow or cold Link can take seconds
// (it is bounded at enrollHTTPTimeout). The app waits for the answer instead
// of dropping it at the 2 s default (the shape of review 3 finding 1).
func TestLinkManagerDesktopWaitsForSlowServingProcess(t *testing.T) {
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2500 * time.Millisecond)
		(&stubManager{}).ServeHTTP(w, r)
	}))
	defer mgr.Close()
	host := strings.TrimPrefix(mgr.URL, "http://")
	installServingLink(t, &fakeLink{routes: map[string]string{"192.0.2.10:30190": host}})
	setServingRedisURL(t, routeNASRedis)
	installServingProcess(t)
	installOSClient(t, &refusingDoer{})
	db := filepath.Join(t.TempDir(), "metadata.db")
	first := runEnrollStepWithRedis(t, routeNASManager, db, "", enrollclient.State{})
	if out := runEnrollStepWithRedis(t, routeNASManager, db, "", first.State); out.Phase != enrollclient.Waiting {
		t.Fatalf("the Manager answered through Link after 2.5 s; the app reported phase %s %q", out.Phase, out.Message)
	}
}
