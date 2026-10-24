package delivery

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationclient"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

// The fixture uses a real catalog and descriptor-backed managed root, with an
// authenticated TLS endpoint that deliberately never returns a status. It proves
// service ownership of the original source-status request, not Gateway policy,
// source verification success, or native delivery acceptance.
type lifecycleRoot struct {
	*privatefile.ManagedRoot
	calls    atomic.Int32
	closeErr error
}

func (r *lifecycleRoot) Close() error {
	r.calls.Add(1)
	return errors.Join(r.ManagedRoot.Close(), r.closeErr)
}

type lifecycleTransport struct {
	original    http.RoundTripper
	canceled    chan struct{}
	release     chan struct{}
	returned    chan struct{}
	established <-chan struct{}
}

func (g *lifecycleTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := g.original.RoundTrip(r)
	// Hold the ORIGINAL call after its actual HTTP transport observes service
	// cancellation. Close must wait even though the socket has stopped.
	// A dial/TLS failure (or any return before validated handler entry) must
	// reach the caller immediately so its failure cleanup can run.
	if err == nil || r.Context().Err() == nil {
		return response, err
	}
	select {
	case <-g.established:
	default:
		return response, err
	}
	close(g.canceled)
	<-g.release
	close(g.returned)
	return response, err
}
func (g *lifecycleTransport) CloseIdleConnections() {
	if c, ok := g.original.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

func TestServiceCloseJoinsAuthenticatedBindAndSharesResult(t *testing.T) {
	library := continuity.LibraryID("lib_" + strings.Repeat("1", 32))
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := catalog.CreateDelivery(context.Background(), filepath.Join(dir, "catalog.sqlite"), library)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	rootPath := filepath.Join(dir, "managed")
	if err := os.Mkdir(rootPath, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"attempts", "ready"} {
		if err := os.Mkdir(filepath.Join(rootPath, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	managed, err := privatefile.OpenManagedRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	closeFailure := errors.New("owned lifecycle close failure")
	root := &lifecycleRoot{ManagedRoot: managed, closeErr: closeFailure}
	t.Cleanup(func() { _ = managed.Close() })
	entered := make(chan struct{})
	handlerDone := make(chan struct{})
	handlerRelease := make(chan struct{})
	handlerCanceled := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		requests.Add(1)
		refuse := func(message string) {
			t.Error(message)
			http.Error(w, "invalid fixture request", http.StatusBadRequest)
		}
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			refuse("request lacked client certificate")
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/images/status" || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/json" {
			refuse("unexpected status request")
			return
		}
		// Read the bounded POST body through EOF before announcing entry, so
		// net/http can start its disconnect reader while this handler waits.
		body, err := io.ReadAll(io.LimitReader(r.Body, 32769))
		if err != nil || len(body) > 32768 {
			refuse("status request body did not reach bounded EOF")
			return
		}
		var ref mutationauthority.ImageRef
		if mutationauthority.DecodeImageCanonical(body, &ref) != nil || ref.Validate() != nil || ref.Version != 1 || ref.LibraryID != string(library) || ref.StoreID != "ims_"+strings.Repeat("4", 32) || ref.ImageID != "img_"+strings.Repeat("8", 32) {
			refuse("invalid status request body")
			return
		}
		close(entered)
		select {
		case <-r.Context().Done():
			close(handlerCanceled)
		case <-handlerRelease:
			// Failure cleanup is only a release path, never cancellation proof.
		}
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)
	// The synthetic endpoint's own generated certificate is also the fixture's
	// client certificate. RequireAnyClientCert exercises client authentication;
	// this is deliberately not production certificate authorization policy.
	certificate := server.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	client, err := mutationclient.NewClient(server.URL, certificate, roots)
	if err != nil {
		t.Fatal(err)
	}
	_, httpClient := client.Transport()
	gate := &lifecycleTransport{original: httpClient.Transport, canceled: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}), established: entered}
	httpClient.Transport = gate
	s := &Service{store: store, library: library, root: root, routes: map[string]route{"reader": {client: client, digest: strings.Repeat("2", 64), leaf: strings.Repeat("3", 64), store: "ims_" + strings.Repeat("4", 32)}}, active: map[uint64]context.CancelFunc{}}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	bindDone := make(chan error, 1)
	closeOne := make(chan error, 1)
	closeTwo := make(chan error, 1)
	// Cleanup cancels/releases/joins all original goroutines before the server,
	// descriptors or borrowed catalog can be torn down, including Fatal paths.
	parent, cancel := context.WithCancel(context.Background())
	var startedBind, startedOne, startedTwo bool
	t.Cleanup(func() {
		cancel()
		close(handlerRelease)
		release()
		if startedBind {
			<-bindDone
		}
		if startedOne {
			<-closeOne
		}
		if startedTwo {
			<-closeTwo
		}
		_ = s.Close()
	})
	input := catalog.SourceBindRequest{Version: 1, LibraryID: library, ID: "lifecycle-bind", ExpectedIncarnation: "auth_" + strings.Repeat("5", 32), AssetID: continuity.AssetID("asset_" + strings.Repeat("6", 32)), RevisionID: continuity.RevisionID("rev1_" + strings.Repeat("7", 64)), Image: mutationauthority.ImageRef{Version: 1, StoreID: "ims_" + strings.Repeat("4", 32), LibraryID: string(library), ImageID: "img_" + strings.Repeat("8", 32)}}
	// An uninitialized owner must be refused before network I/O. Establish the
	// real local authority before exercising ownership of the authenticated call.
	startedBind = true
	go func() { _, err := s.BindSource(parent, input, "reader"); bindDone <- err }()
	select {
	case err := <-bindDone:
		startedBind = false
		if !errors.Is(err, catalog.ErrConflict) || requests.Load() != 0 {
			t.Fatalf("uninitialized bind lost owner refusal: %v requests=%d", err, requests.Load())
		}
	case <-entered:
		t.Fatal("uninitialized bind reached authenticated network I/O")
	}
	owner, err := store.LocalOwner(library)
	if err != nil {
		t.Fatal(err)
	}
	label := "Lifecycle owner"
	value, err := json.Marshal(catalog.BootstrapRequest{OwnerSubjectID: catalog.SubjectID("sub_" + strings.Repeat("a", 32)), CreateLabel: &label})
	if err != nil {
		t.Fatal(err)
	}
	boot, err := owner.Bootstrap(parent, catalog.AdminRequest{Version: 1, LibraryID: library, ID: "lifecycle-bootstrap", Kind: "bootstrap", Value: value})
	if err != nil {
		t.Fatal(err)
	}
	input.ExpectedIncarnation = boot.Result.Incarnation
	startedBind = true
	go func() { _, err := s.BindSource(parent, input, "reader"); bindDone <- err }()
	select {
	case <-entered:
	case err := <-bindDone:
		startedBind = false
		t.Fatalf("BindSource did not reach authenticated fetch: %v", err)
	}
	startedOne = true
	go func() { closeOne <- s.Close() }()
	select {
	case <-gate.canceled:
	case err := <-closeOne:
		startedOne = false
		t.Fatalf("Close failed to cancel/join original fetch: %v", err)
	}
	<-handlerDone
	select {
	case <-handlerCanceled:
	default:
		t.Fatal("handler finished without observing actual request cancellation")
	}
	if root.calls.Load() != 0 {
		t.Error("resource closed while original BindSource was held")
	}
	secondStarted := make(chan struct{})
	startedTwo = true
	go func() { close(secondStarted); closeTwo <- s.Close() }()
	<-secondStarted
	// Observation, not a timeout-based completion claim. The authoritative join
	// and shared result assertions below discriminate an early nil Close.
	select {
	case err := <-closeOne:
		startedOne = false
		t.Errorf("first Close returned before original call: %v", err)
	default:
	}
	select {
	case err := <-closeTwo:
		startedTwo = false
		t.Errorf("second Close returned before original call: %v", err)
	default:
	}
	assertLifecycleRefusal(t, s, context.Background(), errServiceClosed)
	release()
	<-gate.returned
	if err := <-bindDone; !errors.Is(err, context.Canceled) {
		t.Errorf("BindSource cancellation: %v", err)
	}
	startedBind = false
	if startedOne {
		if err := <-closeOne; err != s.closeErr || !errors.Is(err, closeFailure) {
			t.Errorf("first close result: %v", err)
		}
		startedOne = false
	}
	if startedTwo {
		if err := <-closeTwo; err != s.closeErr || !errors.Is(err, closeFailure) {
			t.Errorf("second close result: %v", err)
		}
		startedTwo = false
	}
	if root.calls.Load() != 1 {
		t.Errorf("root closed %d times", root.calls.Load())
	}
	if err := s.Close(); err != s.closeErr || !errors.Is(err, closeFailure) {
		t.Errorf("repeated close result: %v", err)
	}
	assertLifecycleRefusal(t, s, context.Background(), errServiceClosed)
	if requests.Load() != 1 {
		t.Errorf("unexpected post-close network calls: %d", requests.Load())
	}
	for _, name := range []string{"attempts", "ready"} {
		entries, err := os.ReadDir(filepath.Join(rootPath, name))
		if err != nil || len(entries) != 0 {
			t.Errorf("post-close %s mutated: %v %v", name, entries, err)
		}
	}
}

func assertLifecycleRefusal(t *testing.T, s *Service, ctx context.Context, want error) {
	t.Helper()
	if _, err := s.BindSource(ctx, catalog.SourceBindRequest{}, "reader"); !errors.Is(err, want) {
		t.Errorf("BindSource refused: %v", err)
	}
	if _, err := s.Select(ctx, nil, catalog.DeliverySelectRequest{}); !errors.Is(err, want) {
		t.Errorf("Select refused: %v", err)
	}
	if _, err := s.Status(ctx, nil, ""); !errors.Is(err, want) {
		t.Errorf("Status refused: %v", err)
	}
	if _, _, err := s.Materialize(ctx, nil, "", ""); !errors.Is(err, want) {
		t.Errorf("Materialize refused: %v", err)
	}
}

func TestServiceCanceledParentDoesNotTouchResources(t *testing.T) {
	// Nil dependencies make any attempted catalog/route/root use observable as
	// a panic; this invokes the real public methods, not a model of own().
	s := &Service{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assertLifecycleRefusal(t, s, ctx, context.Canceled)
}

// A deterministic pre-entry failure must not become an owned cancellation hold.
// No sockets are used; cleanup still releases and joins the original wrapper
// invocation if the old unconditional-gate behavior is restored as a control.
type lifecycleFailureTransport struct{ err error }

func (f lifecycleFailureTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, f.err
}
func TestLifecycleTransportPreEntryFailureReturns(t *testing.T) {
	failure := errors.New("synthetic pre-entry transport failure")
	gate := &lifecycleTransport{original: lifecycleFailureTransport{failure}, canceled: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}), established: make(chan struct{})}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://fixture.invalid/v1/images/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		close(gate.release)
		if !joined {
			<-result
		}
	})
	go func() { _, err := gate.RoundTrip(request); result <- err }()
	select {
	case err := <-result:
		joined = true
		if err != failure {
			t.Errorf("lost original transport error: %v", err)
		}
	case <-gate.canceled:
		t.Fatal("pre-entry transport failure incorrectly entered cancellation hold")
	}
}
