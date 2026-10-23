package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationclient"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

const recoveryLibrary continuity.LibraryID = "lib_000102030405060708090a0b0c0d0e0f"
const recoveryAsset continuity.AssetID = "asset_101112131415161718191a1b1c1d1e1f"
const recoveryOwner catalog.SubjectID = "sub_11111111111111111111111111111111"
const recoveryReader catalog.SubjectID = "sub_22222222222222222222222222222222"
const recoveryOperation = "service-recovery-materialize"

type serviceRecoveryFixture struct {
	store                 *catalog.Store
	owner                 *catalog.LocalOwnerService
	service               *Service
	root                  *privatefile.ManagedRoot
	rootPath              string
	catalogPath           string
	payload               []byte
	bearer                []byte
	selector, incarnation string
	selection             catalog.DeliverySelection
	calls                 *serviceRecoveryImageCalls
}

type serviceRecoveryImageCalls struct {
	status               atomic.Int32
	read                 atomic.Int32
	failStatus           atomic.Bool
	failRead             atomic.Bool
	failStatusAfterReads atomic.Int32
}

func serviceRecoveryJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func serviceRecoveryAdmin(t *testing.T, id, incarnation, kind string, value any) catalog.AdminRequest {
	t.Helper()
	return catalog.AdminRequest{Version: 1, LibraryID: recoveryLibrary, ID: id, ExpectedIncarnation: incarnation, Kind: kind, Value: serviceRecoveryJSON(t, value)}
}

// The endpoint serves canonical image wire data over real authenticated TLS.
// Its RequireAnyClientCert checks certificate presentation, not Gateway reader
// authorization. Public catalog/source/image client/file paths are real; this
// fixture does not qualify the Gateway server, CLI, native or NAS delivery.
func serviceRecoveryImageClient(t *testing.T, payload []byte) (*mutationclient.Client, mutationauthority.ImageRef, *serviceRecoveryImageCalls) {
	t.Helper()
	ref := mutationauthority.ImageRef{Version: 1, StoreID: "ims_11111111111111111111111111111111", LibraryID: string(recoveryLibrary), ImageID: "img_22222222222222222222222222222222"}
	split := len(payload) / 2
	chunks := [][]byte{payload[:split], payload[split:]}
	parts := make([]mutationauthority.ImagePartView, 0, len(chunks))
	var offset int64
	for index, chunk := range chunks {
		sum := sha256.Sum256(chunk)
		parts = append(parts, mutationauthority.ImagePartView{Index: int64(index), Offset: offset, Size: int64(len(chunk)), SHA256: hex.EncodeToString(sum[:])})
		offset += int64(len(chunk))
	}
	sum := sha256.Sum256(payload)
	view := mutationauthority.ImageView{Version: 1, StoreID: ref.StoreID, Volume: "service_recovery_volume", LibraryID: ref.LibraryID, ImageID: ref.ImageID, Incarnation: "service_recovery_cohort", ImageDigest: strings.Repeat("a", 64), Content: mutationauthority.ImageContent{Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}, Activation: 1, Generation: 1, Chunks: parts}
	if err := view.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded := serviceRecoveryJSON(t, view)
	calls := &serviceRecoveryImageCalls{}
	var handlers sync.WaitGroup
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		defer r.Body.Close()
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 || r.Method != http.MethodPost || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "invalid authenticated image request", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, mutationauthority.MaxImageEnvelope+1))
		if err != nil || len(body) > mutationauthority.MaxImageEnvelope {
			http.Error(w, "invalid image body", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/v1/images/status":
			var got mutationauthority.ImageRef
			if mutationauthority.DecodeImageCanonical(body, &got) != nil || got != ref {
				http.Error(w, "wrong image ref", http.StatusBadRequest)
				return
			}
			calls.status.Add(1)
			threshold := calls.failStatusAfterReads.Load()
			if calls.failStatus.Load() || threshold > 0 && calls.read.Load() >= threshold {
				http.Error(w, "injected service status failure", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
			_, _ = w.Write(encoded)
		case "/v1/images/read":
			var got mutationauthority.ImageRead
			if mutationauthority.DecodeImageCanonical(body, &got) != nil || got.Index < 0 || got.Index >= int64(len(parts)) {
				http.Error(w, "invalid image read", http.StatusBadRequest)
				return
			}
			want := mutationauthority.ImageRead{Version: 1, StoreID: ref.StoreID, LibraryID: ref.LibraryID, ImageID: ref.ImageID, ImageDigest: view.ImageDigest, Activation: 1, Generation: 1, Index: got.Index}
			if got != want {
				http.Error(w, "wrong pinned image read", http.StatusBadRequest)
				return
			}
			calls.read.Add(1)
			if calls.failRead.Load() {
				http.Error(w, "injected service read failure", http.StatusServiceUnavailable)
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
			http.Error(w, "unknown image route", http.StatusNotFound)
		}
	}))
	t.Cleanup(func() { server.Close(); handlers.Wait() })
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	client, err := mutationclient.NewClient(server.URL, server.TLS.Certificates[0], roots)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, ref, calls
}

func serviceRecoveryDirectory(t *testing.T, parent, name string) string {
	t.Helper()
	path := filepath.Join(parent, name)
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func newServiceRecoveryFixture(t *testing.T) *serviceRecoveryFixture {
	t.Helper()
	ctx := context.Background()
	f := &serviceRecoveryFixture{payload: []byte("service recovery exact bytes spanning two chunks")}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	var err error
	f.catalogPath = filepath.Join(dir, "catalog.sqlite")
	f.store, err = catalog.CreateDelivery(ctx, f.catalogPath, recoveryLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	f.owner, err = f.store.LocalOwner(recoveryLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "service recovery owner"
	boot, err := f.owner.Bootstrap(ctx, serviceRecoveryAdmin(t, "service-recovery-bootstrap", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: recoveryOwner, CreateLabel: &label}))
	if err != nil {
		t.Fatal(err)
	}
	f.incarnation = boot.Result.Incarnation
	sum := sha256.Sum256(f.payload)
	manifest := continuity.Manifest{Version: 1, LibraryID: recoveryLibrary, AssetID: recoveryAsset, Content: continuity.Content{Size: int64(len(f.payload)), SHA256: hex.EncodeToString(sum[:])}}
	revision, err := manifest.RevisionID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Apply(ctx, catalog.Operation{Version: 1, LibraryID: recoveryLibrary, ID: "service-recovery-metadata", Actor: "service-recovery-fixture", Changes: []catalog.Change{
		{CreateAsset: &catalog.CreateAsset{AssetID: recoveryAsset, Label: "service recovery asset"}},
		{RecordRevision: &manifest},
		{AdvanceHead: &catalog.AdvanceHead{AssetID: recoveryAsset, Revision: revision}},
	}}); err != nil {
		t.Fatal(err)
	}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: recoveryLibrary, ID: "service-recovery-policy", Administration: "local_owner", OperatorSubjectID: recoveryOwner, Changes: []catalog.PolicyChange{
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: recoveryReader, Value: catalog.SubjectValue{Kind: "person", Label: "service recovery reader", Enabled: true}}),
		catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "service-recovery-discovery", Value: catalog.GrantValue{SubjectID: recoveryReader, Scope: catalog.Scope{Kind: "asset", AssetID: recoveryAsset}, Actions: []string{"assets.discover", "revisions.read"}}}),
	}}
	if _, err := f.owner.ApplyPolicy(ctx, serviceRecoveryAdmin(t, policy.ID, f.incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy})); err != nil {
		t.Fatal(err)
	}
	grant := catalog.ByteGrantValue{SubjectID: recoveryReader, Scope: catalog.Scope{Kind: "asset", AssetID: recoveryAsset}, Action: "bytes.read"}
	if _, err := f.owner.ApplyBytePolicy(ctx, catalog.BytePolicyRequest{Version: 1, LibraryID: recoveryLibrary, ID: "service-recovery-byte-policy", ExpectedIncarnation: f.incarnation, Changes: []catalog.ByteGrantChange{{Kind: "put", ID: "bgr_11111111111111111111111111111111", Value: &grant}}}); err != nil {
		t.Fatal(err)
	}
	issued, err := f.owner.IssueCredentialV3(ctx, catalog.AdminRequestV3{Version: 3, LibraryID: recoveryLibrary, ID: "service-recovery-credential", ExpectedIncarnation: f.incarnation, Kind: "issue_credential", Value: serviceRecoveryJSON(t, catalog.IssueCredentialRequestV3{SubjectID: recoveryReader, Scope: catalog.Scope{Kind: "asset", AssetID: recoveryAsset}, Actions: []string{"assets.discover", "bytes.read", "revisions.read"}, ExpiresUnix: time.Now().Add(time.Hour).Unix()})})
	if err != nil {
		t.Fatal(err)
	}
	f.bearer = issued.Bearer
	t.Cleanup(func() { clear(f.bearer) })
	f.selector = issued.Receipt.Result.Selector
	client, ref, calls := serviceRecoveryImageClient(t, f.payload)
	f.calls = calls
	f.rootPath = serviceRecoveryDirectory(t, dir, "managed")
	serviceRecoveryDirectory(t, f.rootPath, "attempts")
	serviceRecoveryDirectory(t, f.rootPath, "ready")
	f.root, err = privatefile.OpenManagedRoot(f.rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.root.Close(); err != nil {
			t.Error(err)
		}
	})
	// Package-internal construction isolates orchestration. Production route
	// loading/config-file parsing is outside this fixture's coverage.
	f.service = &Service{store: f.store, library: recoveryLibrary, root: f.root, routes: map[string]route{"service-recovery-route": {client: client, digest: strings.Repeat("b", 64), leaf: strings.Repeat("c", 64), store: ref.StoreID}}, active: map[uint64]context.CancelFunc{}}
	t.Cleanup(func() {
		if err := f.service.Close(); err != nil {
			t.Error(err)
		}
	})
	bound, err := f.service.BindSource(ctx, catalog.SourceBindRequest{Version: 1, LibraryID: recoveryLibrary, ID: "service-recovery-bind", ExpectedIncarnation: f.incarnation, AssetID: recoveryAsset, RevisionID: revision, Image: ref}, "service-recovery-route")
	if err != nil || bound.Content != manifest.Content {
		t.Fatalf("real source binding: %+v %v", bound, err)
	}
	if calls.read.Load() != 2 || calls.status.Load() == 0 {
		t.Fatalf("fixture did not verify both image chunks: read=%d status=%d", calls.read.Load(), calls.status.Load())
	}
	f.selection, err = f.service.Select(ctx, f.bearer, catalog.DeliverySelectRequest{Version: 1, LibraryID: recoveryLibrary, ID: "service-recovery-select", AssetID: recoveryAsset, Mode: "latest", ExpiresUnix: time.Now().Add(10 * time.Minute).Unix(), MaxBytes: int64(len(f.payload))})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *serviceRecoveryFixture) readyPath(id string) string {
	return filepath.Join(f.rootPath, "ready", id+".payload")
}
func (f *serviceRecoveryFixture) assertBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("managed output bytes mismatch: %q %v", got, err)
	}
}
func (f *serviceRecoveryFixture) materialize(t *testing.T) (string, catalog.MaterializationAttempt) {
	t.Helper()
	reads := f.calls.read.Load()
	path, a, err := f.service.Materialize(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
	if err != nil || a.State != "COMPLETED" || a.ID == "" || path == "" || path != f.readyPath(a.ID) {
		t.Fatalf("actual first Materialize: path=%q attempt=%+v err=%v", path, a, err)
	}
	if f.calls.read.Load()-reads != 2 {
		t.Fatalf("Materialize did not read both chunks: before=%d after=%d", reads, f.calls.read.Load())
	}
	f.assertBytes(t, path, f.payload)
	return path, a
}

// Change bytes without replacing the inode or changing its length. A retry
// that checks only descriptor identity/size must fail the tamper assertions.
func serviceRecoveryTamper(t *testing.T, path string, payload []byte) []byte {
	t.Helper()
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := append([]byte(nil), payload...)
	changed[0] ^= 0x20
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if n, err := file.WriteAt(changed, 0); err != nil || n != len(changed) {
		t.Fatalf("tamper write: n=%d err=%v", n, err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	err = file.Close()
	closed = true
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() {
		t.Fatalf("tamper did not preserve inode and length: %v", err)
	}
	return changed
}

func TestServiceMaterializeCompletedRetry(t *testing.T) {
	t.Run("exact_completed_retry", func(t *testing.T) {
		f := newServiceRecoveryFixture(t)
		path, original := f.materialize(t)
		reads, status := f.calls.read.Load(), f.calls.status.Load()
		retryPath, retry, err := f.service.Materialize(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
		if err != nil || retry.ID != original.ID || retry.State != "COMPLETED" || retryPath == "" || retryPath != path {
			t.Fatalf("completed exact retry: path=%q attempt=%+v err=%v", retryPath, retry, err)
		}
		if f.calls.read.Load() != reads || f.calls.status.Load() <= status {
			t.Fatalf("retry must verify local bytes and sample fresh status without fetching chunks: reads=%d/%d status=%d/%d", reads, f.calls.read.Load(), status, f.calls.status.Load())
		}
		f.assertBytes(t, retryPath, f.payload)
	})
	t.Run("same_size_content_edit", func(t *testing.T) {
		f := newServiceRecoveryFixture(t)
		path, original := f.materialize(t)
		changed := serviceRecoveryTamper(t, path, f.payload)
		retryPath, retry, err := f.service.Materialize(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
		if err == nil || retryPath != "" || retry.ID != original.ID {
			t.Fatalf("same-size edited completed file reused or identity lost: path=%q attempt=%+v err=%v", retryPath, retry, err)
		}
		f.assertBytes(t, path, changed)
	})
	t.Run("revoked_credential", func(t *testing.T) {
		f := newServiceRecoveryFixture(t)
		path, _ := f.materialize(t)
		_, err := f.owner.RevokeCredential(context.Background(), serviceRecoveryAdmin(t, "service-recovery-revoke", f.incarnation, "revoke_credential", catalog.RevokeCredentialRequest{Selector: f.selector, ExpectedGeneration: 1}))
		if err != nil {
			t.Fatal(err)
		}
		reads, status := f.calls.read.Load(), f.calls.status.Load()
		retryPath, _, err := f.service.Materialize(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
		if !errors.Is(err, catalog.ErrUnauthenticated) || retryPath != "" {
			t.Fatalf("revoked credential reused completed bytes: path=%q err=%v", retryPath, err)
		}
		reconcilePath, _, err := f.service.Reconcile(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
		if !errors.Is(err, catalog.ErrUnauthenticated) || reconcilePath != "" {
			t.Fatalf("revoked credential reconciled: path=%q err=%v", reconcilePath, err)
		}
		if f.calls.read.Load() != reads || f.calls.status.Load() != status {
			t.Error("revoked operation reached image endpoint")
		}
		f.assertBytes(t, path, f.payload)
	})
}

// Set up the exact durable PREPARED binding with public catalog/privatefile
// calls. ExposeNoReplace models an original rename whose catalog commit was
// not made: it is actual file I/O, not a SQL state rewrite or a recovery rename.
func (f *serviceRecoveryFixture) prepareExisting(t *testing.T, state string, expose bool) catalog.MaterializationAttempt {
	t.Helper()
	ctx := context.Background()
	a, cleanup, err := f.store.ReserveOwnedMaterialization(ctx, f.bearer, f.selection.SelectionID, recoveryOperation)
	if err != nil || cleanup == nil {
		t.Fatalf("reserve exact attempt: %+v %v", a, err)
	}
	output, err := f.root.OpenAttempt(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := output.Close(); err != nil {
			t.Error(err)
		}
	})
	if n, err := output.Writer().Write(f.payload); err != nil || n != len(f.payload) {
		t.Fatalf("write prepared fixture: %d %v", n, err)
	}
	p, err := output.Finish()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	a, err = f.store.RecordPreparedMaterialization(ctx, f.bearer, p)
	if err != nil || a.State != "PREPARED" {
		t.Fatalf("real PREPARED binding: %+v %v", a, err)
	}
	if expose {
		exposed, err := p.ExposeNoReplace()
		if !exposed || err != nil {
			t.Fatalf("original ambiguous exposure: %v %v", exposed, err)
		}
	}
	if state == "UNCERTAIN" {
		if err := cleanup.MarkUncertain(); err != nil {
			t.Fatal(err)
		}
		a.State = "UNCERTAIN"
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestServiceReconcileExistingFinal(t *testing.T) {
	for _, state := range []string{"PREPARED", "UNCERTAIN"} {
		t.Run(state, func(t *testing.T) {
			for _, tampered := range []bool{false, true} {
				name := "matching_bytes"
				if tampered {
					name = "same_size_content_edit"
				}
				t.Run(name, func(t *testing.T) {
					f := newServiceRecoveryFixture(t)
					original := f.prepareExisting(t, state, true)
					originalPath := f.readyPath(original.ID)
					want := f.payload
					if tampered {
						want = serviceRecoveryTamper(t, originalPath, f.payload)
					}
					before, err := os.Stat(originalPath)
					if err != nil {
						t.Fatal(err)
					}
					// Ordinary retry must not silently reconcile PREPARED or
					// UNCERTAIN, even when an exact final is already present.
					path, a, err := f.service.Materialize(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
					if !errors.Is(err, catalog.ErrReconciliationRequired) || path != "" || a.ID != original.ID {
						t.Fatalf("nonterminal retry implicitly recovered: path=%q attempt=%+v err=%v", path, a, err)
					}
					reads := f.calls.read.Load()
					path, a, err = f.service.Reconcile(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
					if tampered {
						if err == nil || path != "" || a.ID != original.ID {
							t.Fatalf("same-size edited final reconciled or identity lost: path=%q attempt=%+v err=%v", path, a, err)
						}
					} else {
						if err != nil || path != originalPath || path == "" || a.ID != original.ID || a.State != "COMPLETED" {
							t.Fatalf("explicit exact-final recovery: path=%q attempt=%+v err=%v", path, a, err)
						}
						// Prove the reconciliation result was durable, then
						// exercise the ordinary COMPLETED service retry too.
						retryPath, retry, retryErr := f.service.Materialize(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
						if retryErr != nil || retryPath != originalPath || retry.ID != original.ID || retry.State != "COMPLETED" {
							t.Fatalf("reconciled durable retry: path=%q attempt=%+v err=%v", retryPath, retry, retryErr)
						}
					}
					if f.calls.read.Load() != reads {
						t.Error("existing-final recovery fetched replacement chunks")
					}
					after, statErr := os.Stat(originalPath)
					if statErr != nil || !os.SameFile(before, after) {
						t.Fatalf("recovery replaced or removed original final: %v", statErr)
					}
					f.assertBytes(t, originalPath, want)
				})
			}
		})
	}
}

func TestServiceIncompleteRetryRefuses(t *testing.T) {
	for _, state := range []string{"RESERVED", "PREPARED", "UNCERTAIN"} {
		t.Run(state, func(t *testing.T) {
			f := newServiceRecoveryFixture(t)
			var original catalog.MaterializationAttempt
			if state == "RESERVED" {
				var err error
				original, err = f.store.ReserveMaterialization(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
				if err != nil {
					t.Fatal(err)
				}
				output, err := f.root.OpenAttempt(original.ID)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := output.Close(); err != nil {
						t.Error(err)
					}
				})
				if err := output.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				original = f.prepareExisting(t, state, false)
			}
			reads, status := f.calls.read.Load(), f.calls.status.Load()
			for _, reconcile := range []bool{false, true} {
				var path string
				var a catalog.MaterializationAttempt
				var err error
				if reconcile {
					path, a, err = f.service.Reconcile(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
				} else {
					path, a, err = f.service.Materialize(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
				}
				if err == nil || path != "" || a.ID != original.ID {
					t.Fatalf("incomplete %s returned success or lost original identity (reconcile=%v): path=%q attempt=%+v err=%v", state, reconcile, path, a, err)
				}
			}
			if f.calls.read.Load() != reads || f.calls.status.Load() != status {
				t.Error("incomplete retry started a replacement image fetch")
			}
			if _, err := os.Stat(f.readyPath(original.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("incomplete retry exposed final: %v", err)
			}
			entries, err := os.ReadDir(filepath.Join(f.rootPath, "attempts"))
			if err != nil || len(entries) != 1 || entries[0].Name() != original.ID {
				t.Fatalf("retry created alternate attempt: %v %v", entries, err)
			}
			want := f.payload
			if state == "RESERVED" {
				want = nil
			}
			f.assertBytes(t, filepath.Join(f.rootPath, "attempts", original.ID, "payload.partial"), want)
		})
	}
}

func TestServicePostReservationFailureRetainsIdentity(t *testing.T) {
	for _, phase := range []string{"status", "read", "pre_exposure_status"} {
		t.Run(phase, func(t *testing.T) {
			f := newServiceRecoveryFixture(t)
			if phase == "status" {
				f.calls.failStatus.Store(true)
			} else if phase == "read" {
				f.calls.failRead.Store(true)
			} else {
				f.calls.failStatusAfterReads.Store(f.calls.read.Load() + 2)
			}
			path, original, err := f.service.Materialize(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
			if err == nil || path != "" || original.ID == "" || original.OperationID != recoveryOperation || original.SelectionID != f.selection.SelectionID {
				t.Fatalf("postreservation %s failure lost attempt or returned path: path=%q attempt=%+v err=%v", phase, path, original, err)
			}
			expectedCause := "injected service status failure"
			if phase == "read" {
				expectedCause = "injected service read failure"
			}
			if !strings.Contains(err.Error(), expectedCause) {
				t.Fatalf("wrong postreservation failure cause: %v", err)
			}
			durable, _, lookupErr := f.store.ReserveOwnedMaterialization(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
			if lookupErr != nil || durable.ID != original.ID {
				t.Fatalf("failure did not retain same durable attempt: original=%+v durable=%+v err=%v", original, durable, lookupErr)
			}
			f.calls.failStatus.Store(false)
			f.calls.failRead.Store(false)
			f.calls.failStatusAfterReads.Store(0)
			reads, status := f.calls.read.Load(), f.calls.status.Load()
			retryPath, retry, retryErr := f.service.Materialize(context.Background(), f.bearer, f.selection.SelectionID, recoveryOperation)
			if retryErr == nil || retryPath != "" || retry.ID != original.ID {
				t.Fatalf("failed attempt was silently replaced: path=%q attempt=%+v err=%v", retryPath, retry, retryErr)
			}
			if f.calls.read.Load() != reads || f.calls.status.Load() != status {
				t.Error("failed-attempt retry fetched again")
			}
			if _, err := os.Stat(f.readyPath(original.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("postreservation failure exposed final: %v", err)
			}
			want := []byte(nil)
			if phase == "pre_exposure_status" {
				want = f.payload
			}
			f.assertBytes(t, filepath.Join(f.rootPath, "attempts", original.ID, "payload.partial"), want)
		})
	}
}

// The observer is strictly read-only. All fixture creation and transitions use
// product APIs; SQL is used only because quota/row counts have no public getter.
func TestServiceReconcileMissingOperationDoesNotReserve(t *testing.T) {
	f := newServiceRecoveryFixture(t)
	observerURL := url.URL{Scheme: "file", Path: f.catalogPath}
	query := url.Values{}
	query.Set("mode", "ro")
	query.Set("_pragma", "query_only(1)")
	observerURL.RawQuery = query.Encode()
	observer, err := sql.Open("sqlite", observerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := observer.Close(); err != nil {
			t.Error(err)
		}
	})
	snapshot := func() [6]int64 {
		t.Helper()
		var counts [6]int64
		err := observer.QueryRow(`SELECT (SELECT count(*) FROM delivery_materializations), source_versions,selections,attempts,canonical_bytes,reserved_logical_bytes FROM delivery_quota WHERE singleton=1`).Scan(&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &counts[5])
		if err != nil {
			t.Fatal(err)
		}
		return counts
	}
	existing := func() {
		t.Helper()
		a, cleanup, err := f.store.ExistingOwnedMaterialization(context.Background(), f.bearer, f.selection.SelectionID, "missing-reconcile-only")
		if !errors.Is(err, catalog.ErrNotFound) || a.ID != "" || cleanup != nil {
			t.Fatalf("missing operation acquired reservation: attempt=%+v cleanup=%v err=%v", a, cleanup != nil, err)
		}
	}
	existing()
	before := snapshot()
	reads, status := f.calls.read.Load(), f.calls.status.Load()
	path, a, err := f.service.Reconcile(context.Background(), f.bearer, f.selection.SelectionID, "missing-reconcile-only")
	if !errors.Is(err, catalog.ErrNotFound) || path != "" || a.ID != "" {
		t.Fatalf("missing reconciliation allocated or returned path: path=%q attempt=%+v err=%v", path, a, err)
	}
	existing()
	if after := snapshot(); after != before {
		t.Fatalf("missing reconciliation changed rows/quota: before=%v after=%v", before, after)
	}
	if f.calls.read.Load() != reads || f.calls.status.Load() != status {
		t.Error("missing reconciliation reached image endpoint")
	}
	for _, name := range []string{"attempts", "ready"} {
		entries, err := os.ReadDir(filepath.Join(f.rootPath, name))
		if err != nil || len(entries) != 0 {
			t.Fatalf("missing reconciliation created filesystem output %s: %v %v", name, entries, err)
		}
	}
	// A different actual materialization operation still works, proving that
	// the failed reconciliation did not claim this selection's operation ID.
	f.materialize(t)
}
