package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

type refusingDefaultHTTPTransport struct{ calls atomic.Int32 }

func (r *refusingDefaultHTTPTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls.Add(1)
	return nil, errors.New("unexpected operating-system HTTP route in selected-owner fixture")
}

func selectedEnrollOwner(t *testing.T, link *fakeLink) (*volumeOwner, *ownerFacade) {
	t.Helper()
	// A route regression fails immediately instead of attempting to connect
	// to the TEST-NET NAS address. The owner Link client has its own transport.
	guard, oldTransport := &refusingDefaultHTTPTransport{}, http.DefaultTransport
	http.DefaultTransport = guard
	t.Cleanup(func() {
		http.DefaultTransport = oldTransport
		if n := guard.calls.Load(); n != 0 {
			t.Errorf("selected-owner fixture used operating-system HTTP %d times", n)
		}
	})
	o, f := policyPair()
	o.link = &jmnfs.LinkNode{}
	o.logical = routeNASRedis
	o.enrollDial = link.dial // fake transport only; admission uses the real owner protocol
	f.client.path = operationReplyServer(t, o.dispatch)
	f.claimed = o.receiptLocked()
	f.logicalRedisURL = routeNASRedis
	setServingRedisURL(t, routeNASRedis)
	installFakeLink(t, nil) // owner mode has no local Link in the serving process
	globalMu.Lock()
	previous := globalOwner
	globalOwner = f
	globalMu.Unlock()
	t.Cleanup(func() { globalMu.Lock(); globalOwner = previous; globalMu.Unlock() })
	return o, f
}

// The serving process has no Link node. The selected owner carries both
// enrollment verbs through its authenticated socket and retained Link.
func TestLinkManagerSelectedOwnerCarriesEnrollment(t *testing.T) {
	stub := &stubManager{}
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			return
		}
		stub.ServeHTTP(w, r)
	}))
	defer mgr.Close()
	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	o, _ := selectedEnrollOwner(t, link)
	post := callEnrollHTTP(t, http.MethodPost, routeNASEnroll, linkMgrBody)
	get := callEnrollHTTP(t, http.MethodGet, routeNASEnroll, "")
	setServingRedisURL(t, "") // owner claim precedes the engine's Redis publication
	early := callEnrollHTTP(t, http.MethodGet, routeNASEnroll, "")
	if post.Route != "link" || post.Status != http.StatusAccepted || get.Route != "link" || get.Status != http.StatusOK ||
		early.Route != "link" || early.Status != http.StatusOK ||
		!link.sawDial("192.0.2.10:30190") || o.enrollActive != 0 || len(o.ops) != 0 {
		t.Fatalf("selected owner route: POST %+v, GET %+v, early %+v, dials %v", post, get, early, link.dialed)
	}
	stub.mu.Lock()
	secret := stub.bodies[len(stub.bodies)-1]["secret"]
	stub.mu.Unlock()
	if secret != linkMgrSecret {
		t.Fatalf("selected owner sent a different secret: %q", secret)
	}
}

func TestLinkManagerSelectedOwnerRefusesStaleAttachmentAndTarget(t *testing.T) {
	var hits atomic.Int32
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer mgr.Close()
	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	o, f := selectedEnrollOwner(t, link)
	request := func(strings []string, epoch int) servicecontrol.Response {
		return o.dispatch(servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
			Method: enrollHTTPMethod, Strings: strings, Ints: []int{epoch}})
	}
	stringsFor := func() []string {
		return []string{f.intent.Instance, f.intent.ConfigDigest, f.intent.Root, f.token,
			http.MethodPost, routeNASEnroll, linkMgrBody}
	}
	for _, change := range []func([]string){
		func(s []string) { s[0] = "other-instance" },
		func(s []string) { s[1] = "other-config" },
		func(s []string) { s[2] = "other-root" },
		func(s []string) { s[3] = "old-token" },
		func(s []string) { s[4] = http.MethodDelete },
		func(s []string) { s[5] = "http://192.0.2.11:30190/api/principals/enroll" },
		func(s []string) { s[5] = "http://192.0.2.10:30190/other" },
		func(s []string) { s[6] = strings.Repeat("x", enrollHTTPMaxRequest+1) },
	} {
		s := stringsFor()
		change(s)
		if r := request(s, int(f.epoch)); r.Error == "" {
			t.Fatalf("owner accepted unauthorized Manager request: %+v", s[:6])
		}
	}
	if r := request(stringsFor(), int(f.epoch)-1); r.Error == "" {
		t.Fatal("owner accepted stale epoch")
	}
	if len(link.dialed) != 0 || hits.Load() != 0 {
		t.Fatalf("refusal sent Manager traffic: dials %v, hits %d", link.dialed, hits.Load())
	}
	// Reattachment supersedes the facade's saved token and epoch. It must
	// fail closed without sending through the service's network.
	o.mu.Lock()
	o.attachmentEpoch++
	o.attachmentToken = "33333333333333333333333333333333"
	o.mu.Unlock()
	a := callEnrollHTTP(t, http.MethodPost, routeNASEnroll, linkMgrBody)
	if a.Route != "link" || a.Error == "" || hits.Load() != 0 {
		t.Fatalf("stale facade fell through or sent: %+v, hits %d", a, hits.Load())
	}
}

func TestLinkManagerSelectedOwnerBoundsAnswerAndDoesNotFollowRedirect(t *testing.T) {
	var redirected atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
	}))
	defer other.Close()
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Location", other.URL+"/secret")
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		_, _ = io.WriteString(w, strings.Repeat("x", 3*enrollHTTPMaxResponse))
	}))
	defer mgr.Close()
	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	selectedEnrollOwner(t, link)
	if a := callEnrollHTTP(t, http.MethodPost, routeNASEnroll, linkMgrBody); a.Status != http.StatusTemporaryRedirect || redirected.Load() != 0 {
		t.Fatalf("owner followed Manager redirect: %+v, redirected %d", a, redirected.Load())
	}
	if a := callEnrollHTTP(t, http.MethodGet, routeNASEnroll, ""); a.Status != http.StatusOK || len(a.Body) != enrollHTTPMaxResponse {
		t.Fatalf("owner did not bound Manager answer: status %d, body %d", a.Status, len(a.Body))
	}
}

func TestLinkManagerSelectedOwnerRetainsLinkForAcceptedRequest(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusAccepted)
	}))
	defer mgr.Close()
	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	o, _ := selectedEnrollOwner(t, link)
	o.manager = &heldOwnerManager{stopState: health.FUSEStopped}
	var released atomic.Int32
	o.releaseLink = func() { released.Add(1) }
	var once sync.Once
	defer once.Do(func() { close(release) })
	done := make(chan string, 1)
	go func() { done <- enrollHTTPInServingProcess(http.MethodPost, routeNASEnroll, linkMgrBody) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("owner did not start accepted Manager request")
	}
	if _, err := o.stop(); err == nil || released.Load() != 0 || o.link == nil {
		t.Fatalf("Stop released Link during accepted request: %v", err)
	}
	once.Do(func() { close(release) })
	select {
	case raw := <-done:
		var a enrollHTTPAnswer
		if err := json.Unmarshal([]byte(raw), &a); err != nil {
			t.Fatal(err)
		}
		if a.Status != http.StatusAccepted || a.Route != "link" {
			t.Fatalf("accepted request answer: %+v", a)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted Manager request did not settle")
	}
	if _, err := o.stop(); err != nil || released.Load() != 1 {
		t.Fatalf("settled request prevented Link release: %v", err)
	}
}

// An accepted enrollment request blocks Link release, but its first Stop ID
// still settles to a definitive retained error. That ID stays cached; after
// HTTP joins, a fresh Stop ID can retire the original owner.
func TestLinkManagerSelectedOwnerStopRetryAfterAcceptedHTTP(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusAccepted)
	}))
	defer mgr.Close()
	link := &fakeLink{routes: map[string]string{"192.0.2.10:30190": mgr.Listener.Addr().String()}}
	o, f := selectedEnrollOwner(t, link)
	physical := &heldOwnerManager{stopState: health.FUSEStopped}
	o.manager = physical
	var releases atomic.Int32
	o.releaseLink = func() { releases.Add(1) }
	var once sync.Once
	defer once.Do(func() { close(release) })
	httpDone := make(chan enrollHTTPAnswer, 1)
	go func() { httpDone <- f.enrollHTTP(http.MethodPost, routeNASEnroll, linkMgrBody) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("selected owner did not admit Manager HTTP")
	}
	intent := f.intent
	const firstID = "11111111111111111111111111111111"
	const nextID = "22222222222222222222222222222222"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := selectedOwnerOperation(ctx, f.client.path, intent, "stop", firstID, nil, nil)
	if err == nil || first.Operation != firstID || first.OperationState != "error" || first.StopResult == "" ||
		physical.stops.Load() != 1 || releases.Load() != 0 || o.link == nil {
		t.Fatalf("accepted HTTP did not retain original Stop result: %+v, err %v, stops %d, releases %d", first, err, physical.stops.Load(), releases.Load())
	}
	tooSoon, err := selectedOwnerOperation(ctx, f.client.path, intent, "stop", firstID, nil, nil)
	if err == nil || tooSoon.OperationState != "error" || tooSoon.StopResult != first.StopResult ||
		physical.stops.Load() != 1 || releases.Load() != 0 {
		t.Fatalf("same Stop ID ran again or released Link: %+v, err %v, stops %d, releases %d", tooSoon, err, physical.stops.Load(), releases.Load())
	}
	once.Do(func() { close(release) })
	select {
	case answer := <-httpDone:
		if answer.Route != "link" || answer.Status != http.StatusAccepted {
			t.Fatalf("accepted HTTP did not settle: %+v", answer)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted HTTP did not join after Manager reply")
	}
	next, err := selectedOwnerOperation(ctx, f.client.path, intent, "stop", nextID, nil, nil)
	if err != nil || next.Operation != nextID || next.OperationState != "complete" || physical.stops.Load() != 2 || releases.Load() != 1 || o.link != nil {
		t.Fatalf("fresh Stop ID did not retire after HTTP settled: %+v, err %v, stops %d, releases %d", next, err, physical.stops.Load(), releases.Load())
	}
}

func TestLinkManagerSelectedOwnerReplyIdentityIsChecked(t *testing.T) {
	link := &fakeLink{routes: map[string]string{}}
	_, f := selectedEnrollOwner(t, link)
	f.policyCall = func(_ context.Context, _ string, _ servicecontrol.Request) (servicecontrol.Response, error) {
		r := f.claimed
		r.AttachmentEpoch++
		r.EnrollHTTP = &enrollHTTPAnswer{Route: "link", Status: http.StatusAccepted}
		b, _ := json.Marshal(r)
		return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Text: string(b)}, nil
	}
	if a := f.enrollHTTP(http.MethodPost, routeNASEnroll, linkMgrBody); a.Error == "" || len(link.dialed) != 0 {
		t.Fatalf("foreign owner answer accepted: %+v", a)
	}
}
