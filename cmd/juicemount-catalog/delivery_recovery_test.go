package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

type cliDeliveryFixture struct {
	path, config, root                    string
	bearer, payload, bindBody, selectBody []byte
	selection                             catalog.DeliverySelection
}

type cliDelivered struct {
	Path    string                         `json:"path"`
	Attempt catalog.MaterializationAttempt `json:"attempt"`
}

// All catalog setup uses production APIs. The TLS endpoint speaks the actual
// image protocol and requires a presented client certificate; it is synthetic
// and does not claim production Gateway authorization or native acceptance.
func newCLIDeliveryFixture(t *testing.T) cliDeliveryFixture {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f := cliDeliveryFixture{path: filepath.Join(dir, "catalog.sqlite"), config: filepath.Join(dir, "delivery.json"), root: filepath.Join(dir, "managed"), payload: []byte("actual CLI delivery exact immutable bytes")}
	for _, p := range []string{f.root, filepath.Join(f.root, "attempts"), filepath.Join(f.root, "ready")} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	sum := sha256.Sum256(f.payload)
	digest := hex.EncodeToString(sum[:])
	ref := mutationauthority.ImageRef{Version: 1, StoreID: "ims_11111111111111111111111111111111", LibraryID: string(cliLibrary), ImageID: "img_22222222222222222222222222222222"}
	view := mutationauthority.ImageView{Version: 1, StoreID: ref.StoreID, Volume: "cli_delivery_fixture", LibraryID: ref.LibraryID, ImageID: ref.ImageID, Incarnation: "cli_delivery_cohort", ImageDigest: strings.Repeat("a", 64), Content: mutationauthority.ImageContent{Size: int64(len(f.payload)), SHA256: digest}, Activation: 1, Generation: 1, Chunks: []mutationauthority.ImagePartView{{Index: 0, Offset: 0, Size: int64(len(f.payload)), SHA256: digest}}}
	if err := view.Validate(); err != nil {
		t.Fatal(err)
	}
	viewJSON := cliCollectionRaw(t, view)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 || r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || r.URL.RawQuery != "" {
			http.Error(w, "invalid request", 400)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, mutationauthority.MaxImageEnvelope+1))
		if err != nil || len(body) > mutationauthority.MaxImageEnvelope {
			http.Error(w, "invalid body", 400)
			return
		}
		switch r.URL.Path {
		case "/v1/images/status":
			var got mutationauthority.ImageRef
			if mutationauthority.DecodeImageCanonical(body, &got) != nil || got != ref {
				http.Error(w, "wrong reference", 400)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", strconv.Itoa(len(viewJSON)))
			_, _ = w.Write(viewJSON)
		case "/v1/images/read":
			var got mutationauthority.ImageRead
			want := mutationauthority.ImageRead{Version: 1, StoreID: ref.StoreID, LibraryID: ref.LibraryID, ImageID: ref.ImageID, ImageDigest: view.ImageDigest, Activation: 1, Generation: 1, Index: 0}
			if mutationauthority.DecodeImageCanonical(body, &got) != nil || got != want {
				http.Error(w, "wrong read", 400)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(f.payload)))
			for k, v := range map[string]string{"X-JM-Image-Digest": view.ImageDigest, "X-JM-Image-Activation": "1", "X-JM-Image-Generation": "1", "X-JM-Image-Index": "0", "X-JM-Chunk-SHA256": digest} {
				w.Header().Set(k, v)
			}
			_, _ = w.Write(f.payload)
		default:
			http.NotFound(w, r)
		}
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)
	certificate := server.TLS.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	certPath, keyPath, rootsPath := filepath.Join(dir, "reader.pem"), filepath.Join(dir, "reader-key.pem"), filepath.Join(dir, "roots.pem")
	for p, b := range map[string][]byte{certPath: certPEM, keyPath: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), rootsPath: certPEM} {
		if err := os.WriteFile(p, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	registry := struct {
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
	}{Version: 1, LibraryID: cliLibrary, MaterializationRoot: f.root}
	registry.Routes = append(registry.Routes, struct {
		ID              string `json:"id"`
		Endpoint        string `json:"endpoint"`
		CertificateFile string `json:"certificate_file"`
		KeyFile         string `json:"key_file"`
		RootsFile       string `json:"roots_file"`
		StoreID         string `json:"store_id"`
	}{"reader", server.URL, certPath, keyPath, rootsPath, ref.StoreID})
	if err := os.WriteFile(f.config, cliCollectionRaw(t, registry), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := catalog.CreateDelivery(ctx, f.path, cliLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	owner, err := s.LocalOwner(cliLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "CLI delivery owner"
	var bootRequest catalog.AdminRequest
	if err := json.Unmarshal(cliRequest(t, "delivery-bootstrap", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: cliOwner, CreateLabel: &label}), &bootRequest); err != nil {
		t.Fatal(err)
	}
	boot, err := owner.Bootstrap(ctx, bootRequest)
	if err != nil {
		t.Fatal(err)
	}
	inc := boot.Result.Incarnation
	manifest := continuity.Manifest{Version: 1, LibraryID: cliLibrary, AssetID: cliAssetA, Content: continuity.Content{SHA256: digest, Size: int64(len(f.payload))}}
	revision, err := manifest.RevisionID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(ctx, catalog.Operation{Version: 1, LibraryID: cliLibrary, ID: "delivery-metadata", Actor: "fixture", Changes: []catalog.Change{{CreateAsset: &catalog.CreateAsset{AssetID: cliAssetA, Label: "CLI asset"}}, {RecordRevision: &manifest}, {AdvanceHead: &catalog.AdvanceHead{AssetID: cliAssetA, Revision: revision}}}}); err != nil {
		t.Fatal(err)
	}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: cliLibrary, ID: "delivery-reader-policy", Administration: "local_owner", OperatorSubjectID: cliOwner, Changes: []catalog.PolicyChange{
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: cliReaderA, Value: catalog.SubjectValue{Kind: "person", Label: "Reader", Enabled: true}}),
		catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "delivery-metadata-grant", Value: catalog.GrantValue{SubjectID: cliReaderA, Scope: catalog.Scope{Kind: "asset", AssetID: cliAssetA}, Actions: []string{"assets.discover", "revisions.read"}}}),
	}}
	if _, err := owner.ApplyPolicy(ctx, catalog.AdminRequest{Version: 1, LibraryID: cliLibrary, ID: policy.ID, ExpectedIncarnation: inc, Kind: "apply_policy", Value: cliCollectionRaw(t, catalog.ApplyPolicyRequest{Operation: policy})}); err != nil {
		t.Fatal(err)
	}
	grant := catalog.ByteGrantValue{SubjectID: cliReaderA, Scope: catalog.Scope{Kind: "asset", AssetID: cliAssetA}, Action: "bytes.read"}
	if _, err := owner.ApplyBytePolicy(ctx, catalog.BytePolicyRequest{Version: 1, LibraryID: cliLibrary, ID: "delivery-byte-policy", ExpectedIncarnation: inc, Changes: []catalog.ByteGrantChange{{Kind: "put", ID: "bgr_11111111111111111111111111111111", Value: &grant}}}); err != nil {
		t.Fatal(err)
	}
	issued, err := owner.IssueCredentialV3(ctx, catalog.AdminRequestV3{Version: 3, LibraryID: cliLibrary, ID: "delivery-credential", ExpectedIncarnation: inc, Kind: "issue_credential", Value: cliCollectionRaw(t, catalog.IssueCredentialRequestV3{SubjectID: cliReaderA, Scope: catalog.Scope{Kind: "asset", AssetID: cliAssetA}, Actions: []string{"assets.discover", "bytes.read", "revisions.read"}, ExpiresUnix: time.Now().Add(time.Hour).Unix()})})
	if err != nil {
		t.Fatal(err)
	}
	f.bearer = issued.Bearer
	t.Cleanup(func() { clear(f.bearer) })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	f.bindBody = cliCollectionRaw(t, catalog.SourceBindRequest{Version: 1, LibraryID: cliLibrary, ID: "delivery-bind", ExpectedIncarnation: inc, AssetID: cliAssetA, RevisionID: revision, Image: ref})
	if code, _, _ := invokeCLI(f.path, f.flags(""), f.bindBody, "source", "bind"); code != 0 {
		t.Fatal("actual source bind failed")
	}
	selection := catalog.DeliverySelectRequest{Version: 1, LibraryID: cliLibrary, ID: "delivery-select", AssetID: cliAssetA, Mode: "latest", ExpiresUnix: time.Now().Add(10 * time.Minute).Unix(), MaxBytes: int64(len(f.payload))}
	f.selectBody = cliCollectionRaw(t, collectionMutationEnvelope{Version: 1, Bearer: string(f.bearer), Request: cliCollectionRaw(t, selection)})
	code, out, _ := invokeCLI(f.path, f.flags(""), f.selectBody, "delivery", "select")
	if code != 0 || json.Unmarshal(out, &f.selection) != nil || f.selection.SelectionID == "" {
		t.Fatal("actual selection failed")
	}
	return f
}

func (f cliDeliveryFixture) flags(operation string) []string {
	return []string{"--delivery-config", f.config, "--route-id", "reader", "--selection-id", f.selection.SelectionID, "--operation-id", operation}
}
func (f cliDeliveryFixture) materialize(t *testing.T, operation string) cliDelivered {
	t.Helper()
	code, out, _ := invokeCLI(f.path, f.flags(operation), f.bearer, "delivery", "materialize")
	var got cliDelivered
	if code != 0 || json.Unmarshal(out, &got) != nil || got.Path == "" || got.Attempt.State != "COMPLETED" || got.Attempt.OperationID != operation {
		t.Fatalf("actual materialization did not return completed path: code=%d", code)
	}
	b, err := os.ReadFile(got.Path)
	if err != nil || !bytes.Equal(b, f.payload) {
		t.Fatal("CLI delivered wrong bytes")
	}
	return got
}
func (f cliDeliveryFixture) markUncertain(t *testing.T, operation string) catalog.MaterializationAttempt {
	t.Helper()
	s, err := catalog.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	a, cleanup, err := s.ReserveOwnedMaterialization(context.Background(), f.bearer, f.selection.SelectionID, operation)
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanup.MarkUncertain(); err != nil {
		t.Fatal(err)
	}
	return a
}
func (f cliDeliveryFixture) state(t *testing.T, id string) string {
	t.Helper()
	s, err := catalog.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	b, err := s.MaterializationRecoveryBinding(context.Background(), f.bearer, id)
	if err != nil {
		t.Fatal(err)
	}
	return b.Attempt.State
}

func TestCLIDeliveryMaterializeRetryAndReconcile(t *testing.T) {
	f := newCLIDeliveryFixture(t)
	first := f.materialize(t, "cli-materialize")
	if again := f.materialize(t, "cli-materialize"); again != first {
		t.Fatal("completed exact retry changed identity or path")
	}
	f.markUncertain(t, "cli-materialize")
	code, out, _ := invokeCLI(f.path, f.flags("cli-materialize"), f.bearer, "delivery", "reconcile")
	var recovered cliDelivered
	if code != 0 || json.Unmarshal(out, &recovered) != nil || recovered != first || f.state(t, first.Attempt.ID) != "COMPLETED" {
		t.Fatal("actual CLI reconciliation failed")
	}
	changed := bytes.Repeat([]byte{'x'}, len(f.payload))
	if err := os.WriteFile(first.Path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	code, out, _ = invokeCLI(f.path, f.flags("cli-materialize"), f.bearer, "delivery", "materialize")
	if code != 3 || len(out) != 0 {
		t.Fatal("completed CLI retry skipped complete content verification")
	}
}

type cliDeliveryCloseFault struct {
	*delivery.Service
	calls  int
	fault  error
	before func()
}

func (s *cliDeliveryCloseFault) Close() error {
	s.calls++
	if s.before != nil {
		s.before()
	}
	return errors.Join(s.Service.Close(), s.fault)
}

func TestCLIDeliveryClosesOwnersBeforeReceipt(t *testing.T) {
	for _, command := range []string{"source bind", "delivery select", "delivery status", "delivery materialize", "delivery reconcile"} {
		for _, mode := range []string{"success", "service-fault", "store-fault"} {
			t.Run(command+"/"+mode, func(t *testing.T) {
				f := newCLIDeliveryFixture(t)
				operation := "cli-close"
				var prior cliDelivered
				if command == "delivery reconcile" {
					prior = f.materialize(t, operation)
					f.markUncertain(t, operation)
				}
				body := f.bearer
				if command == "source bind" {
					body = f.bindBody
				} else if command == "delivery select" {
					body = f.selectBody
				}
				var out, diagnostic bytes.Buffer
				files := defaultFiles()
				var service *cliDeliveryCloseFault
				storeCloses := 0
				fault := errors.New("DO-NOT-ECHO-close")
				files.deliveryService = func(ctx context.Context, s *catalog.Store, l continuity.LibraryID, p string) (deliveryCommandService, error) {
					real, err := delivery.NewService(ctx, s, l, p)
					if err != nil {
						return nil, err
					}
					service = &cliDeliveryCloseFault{Service: real, before: func() {
						if out.Len() != 0 {
							t.Error("receipt emitted before service close")
						}
					}}
					if mode == "service-fault" {
						service.fault = fault
					}
					return service, nil
				}
				files.closeCatalog = func(s *catalog.Store) error {
					storeCloses++
					if service == nil || service.calls != 1 || out.Len() != 0 {
						t.Error("receipt emitted before original service/catalog close")
					}
					err := s.Close()
					if mode == "store-fault" {
						return errors.Join(err, fault)
					}
					return err
				}
				code := runConfigured(context.Background(), cliArgs(f.path, f.flags(operation), strings.Split(command, " ")...), bytes.NewReader(body), &out, &diagnostic, false, files)
				if service == nil || service.calls != 1 || storeCloses != 1 {
					t.Fatal("original owners were not closed once")
				}
				if mode == "success" {
					if code != 0 || out.Len() == 0 || diagnostic.Len() != 0 {
						t.Fatal("actual CLI successful handoff failed")
					}
					return
				}
				if code != 3 || out.Len() != 0 || !bytes.Contains(diagnostic.Bytes(), []byte("handoff uncertain")) || bytes.Contains(diagnostic.Bytes(), []byte("DO-NOT-ECHO")) || bytes.Contains(diagnostic.Bytes(), f.bearer) {
					t.Fatal("close failure disclosed pending receipt or omitted uncertainty")
				}
				if command == "delivery materialize" || command == "delivery reconcile" {
					s, err := catalog.Open(f.path)
					if err != nil {
						t.Fatal(err)
					}
					a, _, lookupErr := s.ExistingOwnedMaterialization(context.Background(), f.bearer, f.selection.SelectionID, operation)
					closeErr := s.Close()
					if lookupErr != nil || closeErr != nil {
						t.Fatal("cannot inspect retained attempt")
					}
					want := "COMPLETED"
					if mode == "service-fault" {
						want = "UNCERTAIN"
					}
					if a.State != want || a.ID == "" || a.OperationID != operation || prior.Attempt.ID != "" && a.ID != prior.Attempt.ID {
						t.Fatal("close failure lost exact attempt or claimed wrong durable correction")
					}
					if !bytes.Contains(diagnostic.Bytes(), []byte(`"operation_id":"`+operation+`"`)) {
						t.Fatal("uncertainty lost operation ID")
					}
				}
			})
		}
	}
}

func TestCLIDeliveryOperationErrorJoinsServiceClose(t *testing.T) {
	f := newCLIDeliveryFixture(t)
	s, err := catalog.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	files := defaultFiles()
	fault := errors.New("original close fault")
	var wrapped *cliDeliveryCloseFault
	files.deliveryService = func(ctx context.Context, s *catalog.Store, l continuity.LibraryID, p string) (deliveryCommandService, error) {
		real, err := delivery.NewService(ctx, s, l, p)
		if err != nil {
			return nil, err
		}
		wrapped = &cliDeliveryCloseFault{Service: real, fault: fault}
		return wrapped, nil
	}
	known := ""
	pending, err := runDeliveryCLI(context.Background(), s, cliLibrary, "delivery select", f.config, "reader", "", "", strings.NewReader("malformed"), &known, files)
	if !errors.Is(err, catalog.ErrInvalid) || !errors.Is(err, fault) || pending != nil || wrapped == nil || wrapped.calls != 1 {
		t.Fatal("operation failure discarded original service-close cause")
	}
}

type shortDeliveryWriter struct{ bytes.Buffer }

func (w *shortDeliveryWriter) Write(p []byte) (int, error) {
	n := len(p) / 2
	_, _ = w.Buffer.Write(p[:n])
	return n, nil
}
func TestCLIDeliveryOutputFailureIsUncertain(t *testing.T) {
	for _, mode := range []string{"short-write", "owned-close"} {
		t.Run(mode, func(t *testing.T) {
			f := newCLIDeliveryFixture(t)
			operation := "cli-output"
			args := cliArgs(f.path, f.flags(operation), "delivery", "materialize")
			var diagnostic []byte
			if mode == "short-write" {
				out := &shortDeliveryWriter{}
				var errout bytes.Buffer
				code := runConfigured(context.Background(), args, bytes.NewReader(f.bearer), out, &errout, false, defaultFiles())
				if code != 3 || out.Len() == 0 {
					t.Fatal("short write was not a partial handoff failure")
				}
				diagnostic = errout.Bytes()
			} else {
				originals := make([]*os.File, 0, 3)
				for _, name := range []string{"input", "output", "diagnostic"} {
					file, err := os.CreateTemp(t.TempDir(), name)
					if err != nil {
						t.Fatal(err)
					}
					originals = append(originals, file)
					t.Cleanup(func() {
						if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
							t.Error(err)
						}
					})
				}
				if _, err := originals[0].Write(f.bearer); err != nil {
					t.Fatal(err)
				}
				if _, err := originals[0].Seek(0, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				input := &faultOutput{File: originals[0]}
				output := &faultOutput{File: originals[1], closeErr: errors.New("DO-NOT-ECHO-output-close")}
				errout := &faultOutput{File: originals[2]}
				files := defaultFiles()
				restores := 0
				files.stdio = func([]*os.File) ([]fileResource, func() error, error) {
					return []fileResource{input, output, errout}, func() error { restores++; return nil }, nil
				}
				code := runConfigured(context.Background(), args, originals[0], originals[1], originals[2], true, files)
				if code != 3 || input.closes != 1 || output.closes != 1 || errout.closes != 1 || restores != 1 {
					t.Fatal("owned output close did not join originals once")
				}
				body, err := os.ReadFile(originals[1].Name())
				if err != nil || len(body) == 0 {
					t.Fatal("close fault did not follow actual receipt")
				}
				diagnostic, err = os.ReadFile(originals[2].Name())
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(diagnostic, []byte(`"output_close_failed":true`)) {
					t.Fatal("owned output close fault lost disposition")
				}
			}
			if !bytes.Contains(diagnostic, []byte("handoff uncertain")) || !bytes.Contains(diagnostic, []byte(`"partial_output":true`)) || !bytes.Contains(diagnostic, []byte(`"operation_id":"`+operation+`"`)) || bytes.Contains(diagnostic, f.bearer) || bytes.Contains(diagnostic, []byte("DO-NOT-ECHO")) {
				t.Fatal("partial handoff diagnostic missing or leaked private cause")
			}
			// The result remains exactly retrievable; output failure did not delete it.
			f.materialize(t, operation)
		})
	}
}
