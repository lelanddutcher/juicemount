package manager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

const deliveryTestLibrary continuity.LibraryID = "lib_000102030405060708090a0b0c0d0e0f"
const deliveryTestAsset continuity.AssetID = "asset_101112131415161718191a1b1c1d1e1f"
const deliveryTestOwner catalog.SubjectID = "sub_11111111111111111111111111111111"
const deliveryTestReader catalog.SubjectID = "sub_22222222222222222222222222222222"

func deliveryTestJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func deliveryTestAdmin(t *testing.T, id, incarnation, kind string, value any) catalog.AdminRequest {
	t.Helper()
	return catalog.AdminRequest{Version: 1, LibraryID: deliveryTestLibrary, ID: id, ExpectedIncarnation: incarnation, Kind: kind, Value: deliveryTestJSON(t, value)}
}
func deliveryTestPrivate(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

type deliveryHTTPFixture struct {
	c                  *Continuity
	listener           net.Listener
	server             *http.Server
	done               chan error
	joined             bool
	client             *http.Client
	prefix             string
	bearer             []byte
	selection          catalog.DeliverySelection
	selectOp           catalog.DeliverySelectRequest
	payload            []byte
	backend            string
	source             *httptest.Server
	statusCalls        atomic.Int32
	gateAt             atomic.Int32
	failStatus         atomic.Bool
	gateEntered        chan struct{}
	gateRelease        chan struct{}
	expectedCloseError error
}

func newDeliveryHTTPFixture(t *testing.T, prefix, mode string, payload []byte) *deliveryHTTPFixture {
	t.Helper()
	isolateContinuityManager(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f := &deliveryHTTPFixture{prefix: prefix, payload: append([]byte(nil), payload...)}
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	ref := mutationauthority.ImageRef{Version: 1, StoreID: "ims_11111111111111111111111111111111", LibraryID: string(deliveryTestLibrary), ImageID: "img_22222222222222222222222222222222"}
	parts := []mutationauthority.ImagePartView{}
	chunks := [][]byte{}
	if len(payload) > 0 {
		split := len(payload) / 2
		if split == 0 {
			split = len(payload)
		}
		chunks = append(chunks, payload[:split])
		if split < len(payload) {
			chunks = append(chunks, payload[split:])
		}
		offset := int64(0)
		for index, chunk := range chunks {
			partSHA := sha256.Sum256(chunk)
			parts = append(parts, mutationauthority.ImagePartView{Index: int64(index), Offset: offset, Size: int64(len(chunk)), SHA256: hex.EncodeToString(partSHA[:])})
			offset += int64(len(chunk))
		}
	}
	view := mutationauthority.ImageView{Version: 1, StoreID: ref.StoreID, Volume: "delivery_http_volume", LibraryID: ref.LibraryID, ImageID: ref.ImageID, Incarnation: "delivery_http_cohort", ImageDigest: strings.Repeat("a", 64), Content: mutationauthority.ImageContent{Size: int64(len(payload)), SHA256: digest}, Activation: 1, Generation: 1, Chunks: parts}
	if err := view.Validate(); err != nil {
		t.Fatal(err)
	}
	encodedView := deliveryTestJSON(t, view)
	source := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 || r.Method != http.MethodPost || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "invalid image request", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, mutationauthority.MaxImageEnvelope+1))
		if err != nil || len(body) > mutationauthority.MaxImageEnvelope {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/v1/images/status":
			var got mutationauthority.ImageRef
			if mutationauthority.DecodeImageCanonical(body, &got) != nil || got != ref {
				http.Error(w, "wrong ref", http.StatusBadRequest)
				return
			}
			ordinal := f.statusCalls.Add(1)
			if ordinal == f.gateAt.Load() && f.gateEntered != nil {
				close(f.gateEntered)
				<-f.gateRelease
			}
			if f.failStatus.Load() {
				http.Error(w, "status refused", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", strconv.Itoa(len(encodedView)))
			_, _ = w.Write(encodedView)
		case "/v1/images/read":
			var got mutationauthority.ImageRead
			if mutationauthority.DecodeImageCanonical(body, &got) != nil || got.Index < 0 || got.Index >= int64(len(chunks)) {
				http.Error(w, "wrong read", http.StatusBadRequest)
				return
			}
			want := mutationauthority.ImageRead{Version: 1, StoreID: ref.StoreID, LibraryID: ref.LibraryID, ImageID: ref.ImageID, ImageDigest: view.ImageDigest, Activation: 1, Generation: 1, Index: got.Index}
			if got != want {
				http.Error(w, "wrong pinned read", http.StatusBadRequest)
				return
			}
			chunk := chunks[got.Index]
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.Header().Set("X-JM-Image-Digest", view.ImageDigest)
			w.Header().Set("X-JM-Image-Activation", "1")
			w.Header().Set("X-JM-Image-Generation", "1")
			w.Header().Set("X-JM-Image-Index", strconv.FormatInt(got.Index, 10))
			w.Header().Set("X-JM-Chunk-SHA256", parts[got.Index].SHA256)
			_, _ = w.Write(chunk)
		default:
			http.NotFound(w, r)
		}
	}))
	source.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	source.StartTLS()
	f.source = source
	t.Cleanup(source.Close)
	certificate := source.TLS.Certificates[0]
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	keyDER, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certPath, keyPath, rootsPath := filepath.Join(dir, "reader.crt"), filepath.Join(dir, "reader.key"), filepath.Join(dir, "roots.crt")
	deliveryTestPrivate(t, certPath, certPEM)
	deliveryTestPrivate(t, keyPath, keyPEM)
	deliveryTestPrivate(t, rootsPath, certPEM)
	root := filepath.Join(dir, "managed")
	for _, path := range []string{root, filepath.Join(root, "attempts"), filepath.Join(root, "ready")} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	registry := filepath.Join(dir, "registry.json")
	registryValue := struct {
		Version             int                  `json:"version"`
		LibraryID           continuity.LibraryID `json:"library_id"`
		MaterializationRoot string               `json:"materialization_root"`
		Routes              []struct {
			ID              string `json:"id"`
			Endpoint        string `json:"endpoint"`
			CertificateFile string `json:"certificate_file"`
			KeyFile         string `json:"key_file"`
			RootsFile       string `json:"roots_file"`
			StoreID         string `json:"store_id"`
		} `json:"routes"`
	}{Version: 1, LibraryID: deliveryTestLibrary, MaterializationRoot: root}
	registryValue.Routes = append(registryValue.Routes, struct {
		ID              string `json:"id"`
		Endpoint        string `json:"endpoint"`
		CertificateFile string `json:"certificate_file"`
		KeyFile         string `json:"key_file"`
		RootsFile       string `json:"roots_file"`
		StoreID         string `json:"store_id"`
	}{"reader", source.URL, certPath, keyPath, rootsPath, ref.StoreID})
	deliveryTestPrivate(t, registry, deliveryTestJSON(t, registryValue))
	catalogPath := filepath.Join(dir, "catalog.sqlite")
	store, err := catalog.CreateDeliveryLifecycle(context.Background(), catalogPath, deliveryTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	owner, err := store.LocalOwner(deliveryTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "delivery owner"
	boot, err := owner.Bootstrap(context.Background(), deliveryTestAdmin(t, "delivery-bootstrap", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: deliveryTestOwner, CreateLabel: &label}))
	if err != nil {
		t.Fatal(err)
	}
	manifest := continuity.Manifest{Version: 1, LibraryID: deliveryTestLibrary, AssetID: deliveryTestAsset, Content: continuity.Content{Size: int64(len(payload)), SHA256: digest}}
	revision, err := manifest.RevisionID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Apply(context.Background(), catalog.Operation{Version: 1, LibraryID: deliveryTestLibrary, ID: "delivery-metadata", Actor: "fixture", Changes: []catalog.Change{{CreateAsset: &catalog.CreateAsset{AssetID: deliveryTestAsset, Label: "media"}}, {RecordRevision: &manifest}, {AdvanceHead: &catalog.AdvanceHead{AssetID: deliveryTestAsset, Revision: revision}}}})
	if err != nil {
		t.Fatal(err)
	}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: deliveryTestLibrary, ID: "delivery-policy", Administration: "local_owner", OperatorSubjectID: deliveryTestOwner, Changes: []catalog.PolicyChange{
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: deliveryTestReader, Value: catalog.SubjectValue{Kind: "person", Label: "reader", Enabled: true}}),
		catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "delivery-discovery", Value: catalog.GrantValue{SubjectID: deliveryTestReader, Scope: catalog.Scope{Kind: "asset", AssetID: deliveryTestAsset}, Actions: []string{"assets.discover", "revisions.read"}}}),
	}}
	_, err = owner.ApplyPolicy(context.Background(), deliveryTestAdmin(t, policy.ID, boot.Result.Incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy}))
	if err != nil {
		t.Fatal(err)
	}
	byteGrant := catalog.ByteGrantValue{SubjectID: deliveryTestReader, Scope: catalog.Scope{Kind: "asset", AssetID: deliveryTestAsset}, Action: "bytes.read"}
	_, err = owner.ApplyBytePolicy(context.Background(), catalog.BytePolicyRequest{Version: 1, LibraryID: deliveryTestLibrary, ID: "delivery-byte-policy", ExpectedIncarnation: boot.Result.Incarnation, Changes: []catalog.ByteGrantChange{{Kind: "put", ID: "bgr_11111111111111111111111111111111", Value: &byteGrant}}})
	if err != nil {
		t.Fatal(err)
	}
	issued, err := owner.IssueCredentialV3(context.Background(), catalog.AdminRequestV3{Version: 3, LibraryID: deliveryTestLibrary, ID: "delivery-credential", ExpectedIncarnation: boot.Result.Incarnation, Kind: "issue_credential", Value: deliveryTestJSON(t, catalog.IssueCredentialRequestV3{SubjectID: deliveryTestReader, Scope: catalog.Scope{Kind: "asset", AssetID: deliveryTestAsset}, Actions: []string{"assets.discover", "bytes.read", "revisions.read"}, ExpiresUnix: time.Now().Add(time.Hour).Unix()})})
	if err != nil {
		t.Fatal(err)
	}
	f.bearer = issued.Bearer
	t.Cleanup(func() { clear(f.bearer) })
	setupService, err := delivery.NewService(context.Background(), store, deliveryTestLibrary, registry)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := setupService.BindSource(context.Background(), catalog.SourceBindRequest{Version: 1, LibraryID: deliveryTestLibrary, ID: "delivery-bind", ExpectedIncarnation: boot.Result.Incarnation, AssetID: deliveryTestAsset, RevisionID: revision, Image: ref}, "reader")
	if err != nil || !bound.CurrentStatus.Enabled {
		t.Fatalf("bind delivery source: %+v %v", bound, err)
	}
	selectOp := catalog.DeliverySelectRequest{Version: 1, LibraryID: deliveryTestLibrary, ID: "delivery-select", AssetID: deliveryTestAsset, Mode: "latest", ExpiresUnix: time.Now().Add(10 * time.Minute).Unix(), MaxBytes: int64(len(payload))}
	f.selectOp = selectOp
	f.selection, err = setupService.Select(context.Background(), f.bearer, selectOp)
	if err != nil {
		t.Fatal(err)
	}
	if err := setupService.Close(); err != nil {
		t.Fatal(err)
	}
	config := ContinuityConfig{CatalogPath: catalogPath, LibraryID: string(deliveryTestLibrary), ManagerAdminKey: continuityTestAdmin, Delivery: ContinuityDeliveryConfig{RegistryPath: registry, Mode: mode}}
	if mode == "https_edge" {
		config.Delivery.EdgeOrigin = "https://delivery.example.test" + prefix
	}
	f.c, err = PrepareContinuity(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	if err := f.c.RegisterRoutes(mux, prefix); err != nil {
		t.Fatal(err)
	}
	f.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.BindDeliveryListeners(f.listener); err != nil {
		t.Fatal(err)
	}
	f.backend = "http://" + f.listener.Addr().String()
	f.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	f.done = make(chan error, 1)
	go func() { f.done <- f.server.Serve(f.listener) }()
	f.client = &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{Proxy: nil}}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.server.Shutdown(ctx); err != nil {
			t.Errorf("delivery HTTP shutdown incomplete: %v", err)
			_ = f.server.Close()
		}
		if !f.joined {
			if err := <-f.done; err != nil && err != http.ErrServerClosed {
				t.Errorf("delivery Serve failed: %v", err)
			}
		}
		if err := f.c.Close(ctx); err != nil && !errors.Is(err, f.expectedCloseError) {
			t.Errorf("delivery service close incomplete: %v", err)
		}
		f.client.CloseIdleConnections()
	})
	return f
}

func (f *deliveryHTTPFixture) post(t *testing.T, client *http.Client, target string, value any) *http.Response {
	t.Helper()
	body := deliveryTestJSON(t, value)
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+string(f.bearer))
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestContinuityDeliveryConfigAndSchemaGate(t *testing.T) {
	for _, cfg := range []ContinuityDeliveryConfig{{Mode: "loopback_http"}, {RegistryPath: "reader.json"}, {RegistryPath: "reader.json", Mode: "https_edge"}, {RegistryPath: "reader.json", Mode: "loopback_http", EdgeOrigin: "https://edge.example"}} {
		if c, err := PrepareContinuity(context.Background(), ContinuityConfig{Delivery: cfg}); c != nil || err == nil {
			t.Fatalf("partial delivery accepted: %+v %v", cfg, err)
		}
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "old.sqlite")
	old, err := catalog.CreateAuthority(context.Background(), path, deliveryTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := old.LocalOwner(deliveryTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "old metadata owner"
	if _, err := owner.Bootstrap(context.Background(), deliveryTestAdmin(t, "old-bootstrap", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: deliveryTestOwner, CreateLabel: &label})); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	c, err := PrepareContinuity(context.Background(), ContinuityConfig{CatalogPath: path, LibraryID: string(deliveryTestLibrary), ManagerAdminKey: continuityTestAdmin, Delivery: ContinuityDeliveryConfig{RegistryPath: "unopened.json", Mode: "loopback_http"}})
	if c != nil || !errors.Is(err, catalog.ErrUpgradeRequired) {
		t.Fatalf("old schema did not hit delivery schema gate: %v", err)
	}
	c, err = PrepareContinuity(context.Background(), ContinuityConfig{CatalogPath: path, LibraryID: string(deliveryTestLibrary), ManagerAdminKey: continuityTestAdmin})
	if err != nil || c == nil {
		t.Fatalf("metadata-only profile changed: %v", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestContinuityDeliveryHTTPFramingAndRoutes(t *testing.T) {
	for _, prefix := range []string{"", "/manager"} {
		t.Run(strings.TrimPrefix(prefix, "/")+"prefix", func(t *testing.T) {
			for _, payload := range [][]byte{[]byte("actual delivery bytes across HTTP"), nil} {
				name := "full"
				if len(payload) == 0 {
					name = "empty"
				}
				t.Run(name, func(t *testing.T) {
					f := newDeliveryHTTPFixture(t, prefix, "loopback_http", payload)
					base := f.backend + prefix + "/api/continuity/v1/delivery/"
					response := f.post(t, f.client, base+"select", f.selectOp)
					body, err := io.ReadAll(response.Body)
					closeErr := response.Body.Close()
					var retried catalog.DeliverySelection
					if err != nil || closeErr != nil || response.StatusCode != 200 || json.Unmarshal(body, &retried) != nil || retried != f.selection {
						t.Fatalf("select retry: %d %s %v %v", response.StatusCode, body, err, closeErr)
					}
					inspection := catalog.DeliveryInspectRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionID: f.selection.SelectionID}
					response = f.post(t, f.client, base+"inspect", inspection)
					body, err = io.ReadAll(response.Body)
					closeErr = response.Body.Close()
					if err != nil || closeErr != nil || response.StatusCode != 200 {
						t.Fatalf("inspect: %d %s %v %v", response.StatusCode, body, err, closeErr)
					}
					lookup := catalog.DeliveryLookupRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionOperationID: "delivery-select"}
					response = f.post(t, f.client, base+"lookup", lookup)
					body, err = io.ReadAll(response.Body)
					closeErr = response.Body.Close()
					if err != nil || closeErr != nil || response.StatusCode != 200 {
						t.Fatalf("lookup: %d %s %v %v", response.StatusCode, body, err, closeErr)
					}
					emptySHA := sha256.Sum256(nil)
					request := delivery.TransferRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionID: f.selection.SelectionID, OperationID: "delivery-materialize", Offset: 0, PrefixSHA256: hex.EncodeToString(emptySHA[:])}
					response = f.post(t, f.client, base+"transfer", request)
					if response.StatusCode != 200 {
						b, _ := io.ReadAll(response.Body)
						response.Body.Close()
						t.Fatalf("transfer: %d %s", response.StatusCode, b)
					}
					rawHeader, err := base64.RawURLEncoding.DecodeString(response.Header.Get("X-JM-Delivery"))
					if err != nil {
						t.Fatal(err)
					}
					header, err := delivery.DecodeTransferHeader(rawHeader)
					if err != nil {
						t.Fatal(err)
					}
					got, readErr := io.ReadAll(response.Body)
					rawEnd, endErr := base64.RawURLEncoding.DecodeString(response.Trailer.Get("X-JM-Transfer-End"))
					end, decodeErr := delivery.DecodeTransferEnd(rawEnd)
					closeErr = response.Body.Close()
					if readErr != nil || endErr != nil || decodeErr != nil || closeErr != nil || response.ContentLength != -1 || response.Header.Get("Content-Encoding") != "" || !bytes.Equal(got, payload) || delivery.ValidateTransferResponse(request, f.selection, header, &end) != nil {
						t.Fatalf("incomplete transfer: body=%q read=%v end=%v decode=%v close=%v header=%+v trailer=%+v", got, readErr, endErr, decodeErr, closeErr, header, end)
					}
					response = f.post(t, f.client, base+"reconcile", deliveryReconcileRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionID: f.selection.SelectionID, OperationID: request.OperationID})
					body, err = io.ReadAll(response.Body)
					closeErr = response.Body.Close()
					var reconciled deliveryReconcileResult
					if err != nil || closeErr != nil || response.StatusCode != 200 || json.Unmarshal(body, &reconciled) != nil || reconciled.Attempt.ID != header.Attempt.ID || reconciled.Attempt.State != "COMPLETED" {
						t.Fatalf("reconcile: %d %s %v %v", response.StatusCode, body, err, closeErr)
					}
					response = f.post(t, f.client, base+"cancel", catalog.DeliveryCancelRequest{Version: 1, LibraryID: deliveryTestLibrary, ID: "delivery-cancel", SelectionID: f.selection.SelectionID})
					body, err = io.ReadAll(response.Body)
					closeErr = response.Body.Close()
					if err != nil || closeErr != nil || response.StatusCode != 200 {
						t.Fatalf("cancel: %d %s %v %v", response.StatusCode, body, err, closeErr)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := f.server.Shutdown(ctx); err != nil {
						t.Fatalf("HTTP graceful shutdown did not complete: %v", err)
					}
					serveErr := <-f.done
					f.joined = true
					if serveErr != nil && serveErr != http.ErrServerClosed {
						t.Fatal(serveErr)
					}
					if err := f.c.Close(ctx); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestContinuityDeliveryHTTPSRequestBoundary(t *testing.T) {
	f := newDeliveryHTTPFixture(t, "/manager", "https_edge", []byte("edge bytes"))
	backendURL, err := url.Parse(f.backend)
	if err != nil {
		t.Fatal(err)
	}
	edge := &httputil.ReverseProxy{Transport: f.client.Transport, Rewrite: func(pr *httputil.ProxyRequest) {
		pr.SetURL(backendURL)
		pr.Out.Host = backendURL.Host
		// Rewrite receives a clean forwarded-header set. The backend makes
		// its decision from the actual local socket and exact Host only.
	}}
	proxy := httptest.NewTLSServer(edge)
	defer proxy.Close()
	inspect := catalog.DeliveryInspectRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionID: f.selection.SelectionID}
	response := f.post(t, proxy.Client(), proxy.URL+"/manager/api/continuity/v1/delivery/inspect", inspect)
	body, err := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil || response.StatusCode != 200 {
		t.Fatalf("HTTPS edge request: %d %s %v %v", response.StatusCode, body, err, closeErr)
	}
	emptySHA := sha256.Sum256(nil)
	transfer := delivery.TransferRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionID: f.selection.SelectionID, OperationID: "edge-materialize", Offset: 0, PrefixSHA256: hex.EncodeToString(emptySHA[:])}
	response = f.post(t, proxy.Client(), proxy.URL+"/manager/api/continuity/v1/delivery/transfer", transfer)
	rawHeader, headerErr := base64.RawURLEncoding.DecodeString(response.Header.Get("X-JM-Delivery"))
	header, decodeHeaderErr := delivery.DecodeTransferHeader(rawHeader)
	body, err = io.ReadAll(response.Body)
	rawEnd, endErr := base64.RawURLEncoding.DecodeString(response.Trailer.Get("X-JM-Transfer-End"))
	end, decodeEndErr := delivery.DecodeTransferEnd(rawEnd)
	closeErr = response.Body.Close()
	if response.StatusCode != 200 || headerErr != nil || decodeHeaderErr != nil || err != nil || endErr != nil || decodeEndErr != nil || closeErr != nil || !bytes.Equal(body, f.payload) || delivery.ValidateTransferResponse(transfer, f.selection, header, &end) != nil {
		t.Fatalf("TLS edge transfer lacked End/EOF/full bytes: status=%d header=%v decodeHeader=%v read=%v end=%v decodeEnd=%v close=%v", response.StatusCode, headerErr, decodeHeaderErr, err, endErr, decodeEndErr, closeErr)
	}
	request := deliveryTestJSON(t, inspect)
	for _, change := range []func(*http.Request){
		func(r *http.Request) { r.Host = "localhost" },
		func(r *http.Request) { r.Header.Set("X-Forwarded-Host", "trusted.example") },
		func(r *http.Request) { r.Header.Set("Origin", "https://delivery.example.test") },
	} {
		req, err := http.NewRequest(http.MethodPost, f.backend+"/manager/api/continuity/v1/delivery/inspect", bytes.NewReader(request))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+string(f.bearer))
		change(req)
		response, err := f.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode == 200 {
			t.Fatal("spoofed transport authority admitted")
		}
	}
}

func TestContinuityDeliveryResumeAndWrongPrefix(t *testing.T) {
	f := newDeliveryHTTPFixture(t, "", "loopback_http", []byte("resume over actual HTTP after an authenticated first transfer"))
	target := f.backend + "/api/continuity/v1/delivery/transfer"
	emptySHA := sha256.Sum256(nil)
	first := delivery.TransferRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionID: f.selection.SelectionID, OperationID: "delivery-materialize", Offset: 0, PrefixSHA256: hex.EncodeToString(emptySHA[:])}
	response := f.post(t, f.client, target, first)
	full, err := io.ReadAll(response.Body)
	endText := response.Trailer.Get("X-JM-Transfer-End")
	closeErr := response.Body.Close()
	if response.StatusCode != 200 || err != nil || closeErr != nil || endText == "" || !bytes.Equal(full, f.payload) {
		t.Fatalf("first transfer lacked complete transport: %d %v %v", response.StatusCode, err, closeErr)
	}
	offset := int64(len(f.payload) / 2)
	prefix := sha256.Sum256(f.payload[:offset])
	resume := first
	resume.Offset = offset
	resume.PrefixSHA256 = hex.EncodeToString(prefix[:])
	response = f.post(t, f.client, target, resume)
	rawHeader, headerErr := base64.RawURLEncoding.DecodeString(response.Header.Get("X-JM-Delivery"))
	header, decodeHeaderErr := delivery.DecodeTransferHeader(rawHeader)
	tail, readErr := io.ReadAll(response.Body)
	rawEnd, endErr := base64.RawURLEncoding.DecodeString(response.Trailer.Get("X-JM-Transfer-End"))
	end, decodeEndErr := delivery.DecodeTransferEnd(rawEnd)
	closeErr = response.Body.Close()
	if response.StatusCode != 200 || headerErr != nil || decodeHeaderErr != nil || readErr != nil || endErr != nil || decodeEndErr != nil || closeErr != nil || !bytes.Equal(tail, f.payload[offset:]) || delivery.ValidateTransferResponse(resume, f.selection, header, &end) != nil {
		t.Fatalf("resume omitted End/EOF or wrong tail: %d %v %v %v %v %v %v", response.StatusCode, headerErr, decodeHeaderErr, readErr, endErr, decodeEndErr, closeErr)
	}
	wrong := resume
	wrong.PrefixSHA256 = strings.Repeat("f", 64)
	response = f.post(t, f.client, target, wrong)
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode == 200 || response.Header.Get("X-JM-Delivery") != "" {
		t.Fatal("wrong prefix admitted a stream")
	}
}

func TestContinuityDeliveryLateFailureOmitsEnd(t *testing.T) {
	for _, tc := range []struct {
		name          string
		statusOrdinal int32
		wantBody      int
	}{
		{"truncated_after_first_chunk", 4, 1},
		{"final_authority_after_all_bytes", 5, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryHTTPFixture(t, "", "loopback_http", bytes.Repeat([]byte("x"), delivery.MaxTransferChunkBytes+47))
			if _, attempt, err := f.c.delivery.Materialize(context.Background(), f.bearer, f.selection.SelectionID, "delivery-materialize"); err != nil || attempt.State != "COMPLETED" {
				t.Fatalf("prepare existing output: %+v %v", attempt, err)
			}
			f.gateEntered, f.gateRelease = make(chan struct{}), make(chan struct{})
			f.gateAt.Store(f.statusCalls.Load() + tc.statusOrdinal)
			defer func() {
				select {
				case <-f.gateRelease:
				default:
					close(f.gateRelease)
				}
			}()
			emptySHA := sha256.Sum256(nil)
			request := delivery.TransferRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionID: f.selection.SelectionID, OperationID: "delivery-materialize", Offset: 0, PrefixSHA256: hex.EncodeToString(emptySHA[:])}
			response := f.post(t, f.client, f.backend+"/api/continuity/v1/delivery/transfer", request)
			if response.StatusCode != 200 {
				t.Fatalf("failure occurred before stream header: %d", response.StatusCode)
			}
			select {
			case <-f.gateEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("late source authority gate was not reached")
			}
			f.failStatus.Store(true)
			close(f.gateRelease)
			body, readErr := io.ReadAll(response.Body)
			end := response.Trailer.Get("X-JM-Transfer-End")
			_ = response.Body.Close()
			if readErr == nil || end != "" {
				t.Fatalf("late failure fabricated complete transport: body=%q read=%v end=%q", body, readErr, end)
			}
			if tc.wantBody == 1 && (len(body) == 0 || len(body) >= len(f.payload)) {
				t.Fatalf("truncation did not follow a bounded chunk: %d", len(body))
			}
			if tc.wantBody == 2 && !bytes.Equal(body, f.payload) {
				t.Fatalf("final failure did not follow all bytes: %q", body)
			}
		})
	}
}

func TestContinuityDeliverySeparateAdmissionAndStrictBoundary(t *testing.T) {
	f := newDeliveryHTTPFixture(t, "", "loopback_http", []byte("capacity"))
	base := f.backend + "/api/continuity/v1/delivery/"
	inspect := catalog.DeliveryInspectRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionID: f.selection.SelectionID}
	for i := 0; i < 2; i++ {
		if !f.c.admitDelivery(true) {
			t.Fatal("heavy capacity unavailable")
		}
	}
	defer func() { f.c.leaveDelivery(true); f.c.leaveDelivery(true) }()
	response := f.post(t, f.client, base+"reconcile", deliveryReconcileRequest{Version: 1, LibraryID: deliveryTestLibrary, SelectionID: f.selection.SelectionID, OperationID: "delivery-materialize"})
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("third heavy operation: %d", response.StatusCode)
	}
	response = f.post(t, f.client, base+"inspect", inspect)
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("heavy slots blocked metadata: %d", response.StatusCode)
	}
	for i := 0; i < 16; i++ {
		if !f.c.admit() {
			t.Fatal("metadata capacity unavailable")
		}
	}
	response = f.post(t, f.client, base+"inspect", inspect)
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode == 200 {
		t.Fatal("metadata exceeded its 16 slots")
	}
	for i := 0; i < 16; i++ {
		f.c.leave()
	}
	f.c.leaveDelivery(true)
	f.c.leaveDelivery(true)
	// Keep the deferred releases balanced after the explicit capacity fence.
	for i := 0; i < 2; i++ {
		if !f.c.admitDelivery(true) {
			t.Fatal("heavy slots did not reopen")
		}
	}
	raw := deliveryTestJSON(t, inspect)
	malformed := append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"selection_id":"dsel_11111111111111111111111111111111"}`)...)
	req, err := http.NewRequest(http.MethodPost, base+"inspect", bytes.NewReader(malformed))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+string(f.bearer))
	response, err = f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("duplicate typed field admitted: %d", response.StatusCode)
	}
	req, err = http.NewRequest(http.MethodPost, base+"inspect", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-JuiceMount-Admin-Key", continuityTestAdmin)
	response, err = f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin credential substituted for bearer: %d", response.StatusCode)
	}
}

func TestContinuityDeliveryQuotaErrorEnvelope(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "direct", err: catalog.ErrLimit},
		{name: "wrapped", err: errors.Join(errors.New("private quota detail"), catalog.ErrLimit)},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeDeliveryError(w, deliveryErrorCode(test.err))
			}))
			defer server.Close()
			client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}}
			defer client.CloseIdleConnections()
			response, err := client.Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("quota envelope body: read=%v close=%v", readErr, closeErr)
			}
			if response.StatusCode != http.StatusTooManyRequests || string(body) != `{"version":1,"error":"admission_limit"}` {
				t.Fatalf("quota envelope: status=%d body=%q", response.StatusCode, body)
			}
			if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("X-Content-Type-Options") != "nosniff" {
				t.Fatalf("quota envelope headers: %#v", response.Header)
			}
		})
	}
}

func TestContinuityDeliveryCloseRetainsOriginalServiceWorker(t *testing.T) {
	f := newDeliveryHTTPFixture(t, "", "loopback_http", []byte("owned bytes"))
	// Wrap the qualified real Service.Close. The gate holds the Manager's
	// original Close worker after entry; it does not simulate Gateway I/O.
	original := f.c.closeDelivery
	if original == nil {
		t.Fatal("delivery Close owner missing")
	}
	entered, release, workerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	terminal := errors.New("delivery original close terminal result")
	f.expectedCloseError = terminal
	var calls atomic.Int32
	var enteredOnce, releaseOnce, doneOnce sync.Once
	underlyingResult := make(chan error, 1)
	f.c.closeDelivery = func() error {
		calls.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-release
		err := original()
		underlyingResult <- err
		doneOnce.Do(func() { close(workerDone) })
		return errors.Join(err, terminal)
	}
	firstResult, secondResult := make(chan error, 1), make(chan error, 1)
	firstFinished, secondFinished := make(chan struct{}), make(chan struct{})
	firstLaunched, secondLaunched := false, false
	// This cleanup runs before the fixture's HTTP/service cleanup, including
	// when a synchronous or bypassed Close regression fails an assertion.
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-workerDone:
		case <-time.After(5 * time.Second):
			t.Error("original Close worker did not join after release")
		}
		if firstLaunched {
			select {
			case <-firstFinished:
			case <-time.After(5 * time.Second):
				t.Error("first Close caller did not join after release")
			}
		}
		if secondLaunched {
			select {
			case <-secondFinished:
			case <-time.After(5 * time.Second):
				t.Error("retry Close caller did not join after release")
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.c.Close(ctx); errors.Is(err, catalog.ErrDiscoveryDraining) {
			t.Errorf("delivery Store still retained after cleanup: %v", err)
		}
	})
	firstCtx, firstCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer firstCancel()
	firstLaunched = true
	go func() { defer close(firstFinished); firstResult <- f.c.Close(firstCtx) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("original service Close did not begin")
	}
	select {
	case err := <-firstResult:
		if !errors.Is(err, catalog.ErrDiscoveryDraining) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first Close ignored caller deadline: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first Close blocked on original worker past caller deadline")
	}
	if calls.Load() != 1 {
		t.Fatalf("original Close calls after first timeout: %d", calls.Load())
	}
	if err := f.c.store.RequireDeliveryLifecycle(context.Background()); err != nil {
		t.Fatalf("Store closed before original owner joined: %v", err)
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer secondCancel()
	secondLaunched = true
	go func() { defer close(secondFinished); secondResult <- f.c.Close(secondCtx) }()
	select {
	case err := <-secondResult:
		if !errors.Is(err, catalog.ErrDiscoveryDraining) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("retry Close ignored caller deadline: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retry Close blocked on original worker past caller deadline")
	}
	if calls.Load() != 1 {
		t.Fatalf("retry started another original Close: %d", calls.Load())
	}
	if err := f.c.store.RequireDeliveryLifecycle(context.Background()); err != nil {
		t.Fatalf("retry closed Store before owner joined: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-workerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("original Close did not join")
	}
	if err := <-underlyingResult; err != nil {
		t.Fatalf("real delivery Service.Close failed: %v", err)
	}
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer joinCancel()
	joined := f.c.Close(joinCtx)
	cached := f.c.Close(joinCtx)
	if !errors.Is(joined, terminal) || cached != joined || calls.Load() != 1 {
		t.Fatalf("terminal Close result not retained: joined=%v cached=%v calls=%d", joined, cached, calls.Load())
	}
	if err := f.c.store.RequireDeliveryLifecycle(context.Background()); !errors.Is(err, catalog.ErrClosed) {
		t.Fatalf("Store remained live after original join: %v", err)
	}
}
