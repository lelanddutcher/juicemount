package main

// Review 2 finding F3 (the remaining blocker): the serving process's side of
// "enroll-http". It sends the app's Manager request through its Link node
// and only to the Manager of the Redis endpoint it was started with.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/enrollclient"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

const routeNASEnroll = routeNASManager + "/api/principals/enroll"

// A POST body as the app sends it; the secret must not reach a log or a
// refusal.
const linkMgrSecret = "link-manager-fixture-secret-zzzzzzzzzzzzzzzzzzzz"

var linkMgrBody = `{"instance_id":"4F1C2A9E-7B3D-4C1A-9E2B-0A1B2C3D4E5F","host":"Edit-Bay-2","secret":"` + linkMgrSecret + `"}`

func callEnrollHTTP(t *testing.T, method, url, body string) enrollHTTPAnswer {
	t.Helper()
	res := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: enrollHTTPMethod,
		Strings: []string{method, url, body}})
	if res.Error != "" {
		t.Fatalf("%s %s: %s", method, url, res.Error)
	}
	var a enrollHTTPAnswer
	if err := json.Unmarshal([]byte(res.Text), &a); err != nil {
		t.Fatalf("%s %s: %v %q", method, url, err, res.Text)
	}
	return a
}

// The serving process sends the request through its Link node: the NAS
// address (TEST-NET-1) is unreachable over the operating system's network,
// so only the Link route can answer. Status, body and the two headers
// enrollclient reads come back.
func TestLinkManagerServingProcessSendsThroughLink(t *testing.T) {
	buf := captureLogs(t)
	stub := &stubManager{}
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(enrollclient.ManagerIDHeader, "0123456789abcdef0123456789abcdef")
		w.Header().Set("Retry-After", "7")
		w.Header().Set("X-Other", "dropped")
		stub.ServeHTTP(w, r)
	}))
	defer mgr.Close()
	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	installFakeLink(t, link)
	setServingRedisURL(t, "redis://legacyuser@192.0.2.10:6379/1")

	a := callEnrollHTTP(t, http.MethodPost, routeNASEnroll, linkMgrBody)
	if a.Route != "link" || a.Status != http.StatusAccepted || a.Refused != "" || a.Error != "" {
		t.Fatalf("enroll-http through Link = %+v", a)
	}
	if !link.sawDial("192.0.2.10:30190") {
		t.Fatalf("the Manager request did not go through Link: %v", link.dialed)
	}
	if a.ManagerID != "0123456789abcdef0123456789abcdef" || a.RetryAfter != "7" || !strings.Contains(string(a.Body), enrollToken) {
		t.Fatalf("answer lost the Manager's ID, Retry-After or body: %+v", a)
	}
	stub.mu.Lock()
	got := stub.bodies[len(stub.bodies)-1]["secret"]
	stub.mu.Unlock()
	if got != linkMgrSecret {
		t.Fatalf("the Manager received secret %q", got)
	}
	if strings.Contains(buf.String(), linkMgrSecret) {
		t.Fatal("the serving process logged the enrollment secret")
	}
}

// The caller cannot choose the target: anything but the Manager of the
// serving process's Redis endpoint is refused before any dial, so
// "enroll-http" is no HTTP proxy onto the tailnet through the Link node.
func TestLinkManagerServingProcessRefusesCallerChosenTarget(t *testing.T) {
	var hits atomic.Int32
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer sink.Close()
	routes := map[string]string{}
	for _, a := range []string{"192.0.2.10:30190", "192.0.2.10:22", "192.0.2.10:80", "192.0.2.10:443", "10.66.77.88:30190", "192.0.2.11:30190"} {
		routes[a] = sink.Listener.Addr().String()
	}
	link := &fakeLink{routes: routes}
	installFakeLink(t, link)

	// Never started: nothing is sent.
	setServingRedisURL(t, "")
	if a := callEnrollHTTP(t, http.MethodPost, routeNASEnroll, linkMgrBody); a.Route != "refused" {
		t.Fatalf("not started: %+v", a)
	}
	setServingRedisURL(t, routeNASRedis)
	for _, tc := range []struct{ method, url, body string }{
		{http.MethodPost, "http://10.66.77.88:30190/api/principals/enroll", linkMgrBody}, // another host
		{http.MethodPost, "http://192.0.2.11:30190/api/principals/enroll", linkMgrBody},  // another host
		{http.MethodPost, "http://192.0.2.10:22/api/principals/enroll", linkMgrBody},     // another port
		{http.MethodPost, "http://192.0.2.10/api/principals/enroll", linkMgrBody},        // default port
		{http.MethodPost, "https://192.0.2.10/api/principals/enroll", linkMgrBody},
		{http.MethodPost, "http://192.0.2.10:30190/api/admin/principals", linkMgrBody}, // another route
		{http.MethodPost, "http://192.0.2.10:30190/api/principals/enroll/../../x", linkMgrBody},
		{http.MethodPost, "http://192.0.2.10:30190/api/principals/enroll?x=1", linkMgrBody},
		{http.MethodPost, "http://u:p@192.0.2.10:30190/api/principals/enroll", linkMgrBody},
		{http.MethodPost, "ftp://192.0.2.10:30190/api/principals/enroll", linkMgrBody},
		{http.MethodDelete, routeNASEnroll, ""},
		{http.MethodGet, routeNASEnroll, linkMgrBody}, // a GET with a body
		{http.MethodPost, routeNASEnroll, strings.Repeat("x", enrollHTTPMaxRequest+1)},
	} {
		a := callEnrollHTTP(t, tc.method, tc.url, tc.body)
		if a.Route != "refused" || a.Status != 0 || strings.Contains(a.Refused, linkMgrSecret) {
			t.Fatalf("%s %s (%d-byte body) = %+v", tc.method, tc.url, len(tc.body), a)
		}
	}
	if len(link.dialed) != 0 || hits.Load() != 0 {
		t.Fatalf("refused requests were sent: dials %v, hits %d", link.dialed, hits.Load())
	}
	// The Manager of the configured endpoint is sent to (spellings of the
	// same host count: case, a GET probe).
	if a := callEnrollHTTP(t, http.MethodGet, routeNASEnroll, ""); a.Route != "link" || a.Status != http.StatusOK {
		t.Fatalf("own Manager GET = %+v", a)
	}
}

// No redirect is followed (a 307 replays the POST body, the secret, to the
// Location), and the answer is bounded at what enrollclient reads.
func TestLinkManagerServingProcessNoRedirectBoundedAnswer(t *testing.T) {
	var elsewhere atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		elsewhere.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer other.Close()
	big := strings.Repeat("y", 3*enrollHTTPMaxResponse)
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Location", other.URL+"/steal")
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		_, _ = io.WriteString(w, big)
	}))
	defer mgr.Close()
	installFakeLink(t, &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}})
	setServingRedisURL(t, routeNASRedis)

	a := callEnrollHTTP(t, http.MethodPost, routeNASEnroll, linkMgrBody)
	if a.Status != http.StatusTemporaryRedirect || elsewhere.Load() != 0 {
		t.Fatalf("redirect: status %d, requests to the Location %d", a.Status, elsewhere.Load())
	}
	a = callEnrollHTTP(t, http.MethodGet, routeNASEnroll, "")
	if a.Status != http.StatusOK || len(a.Body) != enrollHTTPMaxResponse {
		t.Fatalf("a %d-byte answer came back as %d bytes (bound %d)", len(big), len(a.Body), enrollHTTPMaxResponse)
	}
}

// Without a Link node the serving process sends over its own network (the
// route the app would take, so a LAN Mac enrolls as before), and spellings
// of the configured host count as that host.
func TestLinkManagerServingProcessWithoutLinkUsesSystem(t *testing.T) {
	installFakeLink(t, nil)
	setServingRedisURL(t, "redis://127.0.0.1:6379/1")
	// Nothing on this Mac serves a Manager on 30190: the request is sent
	// over the system route and fails to connect.
	if a := callEnrollHTTP(t, http.MethodGet, "http://127.0.0.1:30190/api/principals/enroll", ""); a.Route != "system" || a.Refused != "" {
		t.Fatalf("without Link = %+v", a)
	}
	for _, tc := range []struct{ url, redis string }{
		{"http://[::1]:30190/api/principals/enroll", "redis://[0:0::1]:6379/1"},
		{"http://NAS.Local.:30190/api/principals/enroll", "redis://nas.local:6379/1"},
		{"https://192.0.2.10:30190/api/principals/enroll", routeNASRedis},
	} {
		if why := enrollHTTPTargetAllowed(tc.url, tc.redis); why != "" {
			t.Fatalf("%s for %s refused: %s", tc.url, tc.redis, why)
		}
	}
}
