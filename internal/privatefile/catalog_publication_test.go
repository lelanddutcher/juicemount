package privatefile_test

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	sqlite "modernc.org/sqlite"
	sqlitelib "modernc.org/sqlite/lib"
)

const publicationLibrary continuity.LibraryID = "lib_000102030405060708090a0b0c0d0e0f"
const publicationAsset continuity.AssetID = "asset_101112131415161718191a1b1c1d1e1f"
const publicationOwner catalog.SubjectID = "sub_11111111111111111111111111111111"
const publicationReader catalog.SubjectID = "sub_22222222222222222222222222222222"

type publicationFixture struct {
	store              *catalog.Store
	client             *mutationclient.Client
	expectedCloseFault *atomic.Bool
	other              *catalog.Store
	otherOwner         *catalog.LocalOwnerService
	path               string
	rootPath           string
	prepared           *privatefile.PreparedFile
	selection          catalog.DeliverySelection
	attempt            catalog.MaterializationAttempt
	bearer             []byte
	selector           string
	incarnation        string
	payload            []byte
	ready              string
	partial            string
}

func publicationJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func publicationAdmin(t *testing.T, id, incarnation, kind string, value any) catalog.AdminRequest {
	t.Helper()
	return catalog.AdminRequest{Version: 1, LibraryID: publicationLibrary, ID: id, ExpectedIncarnation: incarnation, Kind: kind, Value: publicationJSON(t, value)}
}

// This is a private synthetic TLS image service. BindDeliverySource still
// performs its real current-view, exact-chunk and whole-content verification;
// the fixture makes no claim about Gateway authorization or SDK custody.
type publicationImageCalls struct {
	status atomic.Int32
	read   atomic.Int32
}

func publicationImageClient(t *testing.T, payload []byte, digest string) (*mutationclient.Client, mutationauthority.ImageRef, *publicationImageCalls) {
	t.Helper()
	calls := &publicationImageCalls{}
	ref := mutationauthority.ImageRef{Version: 1, StoreID: "ims_11111111111111111111111111111111", LibraryID: string(publicationLibrary), ImageID: "img_22222222222222222222222222222222"}
	view := mutationauthority.ImageView{
		Version: 1, StoreID: ref.StoreID, Volume: "publication_test_volume", LibraryID: ref.LibraryID,
		ImageID: ref.ImageID, Incarnation: "publication_test_cohort", ImageDigest: strings.Repeat("a", 64),
		Content: mutationauthority.ImageContent{Size: int64(len(payload)), SHA256: digest}, Activation: 1, Generation: 1,
		Chunks: []mutationauthority.ImagePartView{{Index: 0, Offset: 0, Size: int64(len(payload)), SHA256: digest}},
	}
	if err := view.Validate(); err != nil {
		t.Fatalf("synthetic image view: %v", err)
	}
	viewJSON := publicationJSON(t, view)
	var handlers sync.WaitGroup
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		defer r.Body.Close()
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "invalid image request", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, mutationauthority.MaxImageEnvelope+1))
		if err != nil || len(body) > mutationauthority.MaxImageEnvelope {
			http.Error(w, "image request too large", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/v1/images/status":
			var got mutationauthority.ImageRef
			if mutationauthority.DecodeImageCanonical(body, &got) != nil || got != ref {
				http.Error(w, "image reference mismatch", http.StatusBadRequest)
				return
			}
			calls.status.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", strconv.Itoa(len(viewJSON)))
			_, _ = w.Write(viewJSON)
		case "/v1/images/read":
			var got mutationauthority.ImageRead
			want := mutationauthority.ImageRead{Version: 1, StoreID: ref.StoreID, LibraryID: ref.LibraryID, ImageID: ref.ImageID, ImageDigest: view.ImageDigest, Activation: 1, Generation: 1, Index: 0}
			if mutationauthority.DecodeImageCanonical(body, &got) != nil || got != want {
				http.Error(w, "image read mismatch", http.StatusBadRequest)
				return
			}
			calls.read.Add(1)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.Header().Set("X-JM-Image-Digest", view.ImageDigest)
			w.Header().Set("X-JM-Image-Activation", "1")
			w.Header().Set("X-JM-Image-Generation", "1")
			w.Header().Set("X-JM-Image-Index", "0")
			w.Header().Set("X-JM-Chunk-SHA256", digest)
			_, _ = w.Write(payload)
		default:
			http.Error(w, "unknown image route", http.StatusNotFound)
		}
	}))
	t.Cleanup(func() { server.Close(); handlers.Wait() })
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	client, err := mutationclient.NewClient(server.URL, server.TLS.Certificates[0], roots)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, ref, calls
}

func publicationDirectory(t *testing.T, parent, name string) string {
	t.Helper()
	path := filepath.Join(parent, name)
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func newPublicationFixture(t *testing.T) publicationFixture {
	t.Helper()
	ctx := context.Background()
	f := publicationFixture{path: filepath.Join(t.TempDir(), "publication.sqlite"), payload: []byte("publication exact managed bytes"), expectedCloseFault: &atomic.Bool{}}
	digestBytes := sha256.Sum256(f.payload)
	digest := hex.EncodeToString(digestBytes[:])
	var err error
	f.store, err = catalog.CreateDelivery(ctx, f.path, publicationLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	owner, err := f.store.LocalOwner(publicationLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "publication owner"
	boot, err := owner.Bootstrap(ctx, publicationAdmin(t, "publication-bootstrap", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: publicationOwner, CreateLabel: &label}))
	if err != nil {
		t.Fatal(err)
	}
	f.incarnation = boot.Result.Incarnation
	manifest := continuity.Manifest{Version: 1, LibraryID: publicationLibrary, AssetID: publicationAsset, Content: continuity.Content{SHA256: digest, Size: int64(len(f.payload))}}
	revision, err := manifest.RevisionID()
	if err != nil {
		t.Fatal(err)
	}
	operation := catalog.Operation{Version: 1, LibraryID: publicationLibrary, ID: "publication-metadata", Actor: "publication-fixture", Changes: []catalog.Change{
		{CreateAsset: &catalog.CreateAsset{AssetID: publicationAsset, Label: "publication asset"}},
		{RecordRevision: &manifest},
		{AdvanceHead: &catalog.AdvanceHead{AssetID: publicationAsset, Revision: revision}},
	}}
	if _, err := f.store.Apply(ctx, operation); err != nil {
		t.Fatal(err)
	}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: publicationLibrary, ID: "publication-reader-policy", Administration: "local_owner", OperatorSubjectID: publicationOwner, Changes: []catalog.PolicyChange{
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: publicationReader, Value: catalog.SubjectValue{Kind: "person", Label: "publication reader", Enabled: true}}),
		catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "publication-discovery", Value: catalog.GrantValue{SubjectID: publicationReader, Scope: catalog.Scope{Kind: "asset", AssetID: publicationAsset}, Actions: []string{"assets.discover", "revisions.read"}}}),
	}}
	if _, err := owner.ApplyPolicy(ctx, publicationAdmin(t, policy.ID, f.incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy})); err != nil {
		t.Fatal(err)
	}
	byteGrant := catalog.ByteGrantValue{SubjectID: publicationReader, Scope: catalog.Scope{Kind: "asset", AssetID: publicationAsset}, Action: "bytes.read"}
	if _, err := owner.ApplyBytePolicy(ctx, catalog.BytePolicyRequest{Version: 1, LibraryID: publicationLibrary, ID: "publication-byte-policy", ExpectedIncarnation: f.incarnation, Changes: []catalog.ByteGrantChange{{Kind: "put", ID: "bgr_11111111111111111111111111111111", Value: &byteGrant}}}); err != nil {
		t.Fatal(err)
	}
	issued, err := owner.IssueCredentialV3(ctx, catalog.AdminRequestV3{Version: 3, LibraryID: publicationLibrary, ID: "publication-credential", ExpectedIncarnation: f.incarnation, Kind: "issue_credential", Value: publicationJSON(t, catalog.IssueCredentialRequestV3{SubjectID: publicationReader, Scope: catalog.Scope{Kind: "asset", AssetID: publicationAsset}, Actions: []string{"assets.discover", "bytes.read", "revisions.read"}, ExpiresUnix: time.Now().Add(time.Hour).Unix()})})
	if err != nil {
		t.Fatal(err)
	}
	f.bearer = issued.Bearer
	f.selector = issued.Receipt.Result.Selector
	client, ref, imageCalls := publicationImageClient(t, f.payload, digest)
	f.client = client
	bind := catalog.SourceBindRequest{Version: 1, LibraryID: publicationLibrary, ID: "publication-bind", ExpectedIncarnation: f.incarnation, AssetID: publicationAsset, RevisionID: revision, RouteID: "publication-test-route", RouteDigest: strings.Repeat("b", 64), ReaderLeafSHA256: strings.Repeat("c", 64), Image: ref}
	bound, err := owner.BindDeliverySource(ctx, bind, client)
	if err != nil || bound.Content != manifest.Content {
		t.Fatalf("public source binding: %+v %v", bound, err)
	}
	if status, read := imageCalls.status.Load(), imageCalls.read.Load(); status != 3 || read != 1 {
		t.Fatalf("strict source binding requests: status=%d read=%d", status, read)
	}
	f.selection, err = f.store.SelectDelivery(ctx, f.bearer, catalog.DeliverySelectRequest{Version: 1, LibraryID: publicationLibrary, ID: "publication-select", AssetID: publicationAsset, Mode: "latest", ExpiresUnix: time.Now().Add(10 * time.Minute).Unix(), MaxBytes: int64(len(f.payload))})
	if err != nil {
		t.Fatal(err)
	}
	f.attempt, err = f.store.ReserveMaterialization(ctx, f.bearer, f.selection.SelectionID, "publication-reserve")
	if err != nil {
		t.Fatal(err)
	}
	f.rootPath = publicationDirectory(t, t.TempDir(), "managed")
	publicationDirectory(t, f.rootPath, "attempts")
	publicationDirectory(t, f.rootPath, "ready")
	root, err := privatefile.OpenManagedRoot(f.rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	attempt, err := root.OpenAttempt(f.attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := attempt.Close(); err != nil && !f.expectedCloseFault.Load() {
			t.Error(err)
		}
	})
	if _, err := attempt.Writer().Write(f.payload); err != nil {
		t.Fatal(err)
	}
	f.prepared, err = attempt.Finish()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.prepared.Close(); err != nil && !f.expectedCloseFault.Load() {
			t.Error(err)
		}
	})
	preparedState, err := f.store.RecordPreparedMaterialization(ctx, f.bearer, f.prepared)
	if err != nil || preparedState.State != "PREPARED" {
		t.Fatalf("public prepared record: %+v %v", preparedState, err)
	}
	f.ready = filepath.Join(f.rootPath, "ready", f.attempt.ID+".payload")
	f.partial = filepath.Join(f.rootPath, "attempts", f.attempt.ID, "payload.partial")
	f.other, err = catalog.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.other.Close(); err != nil {
			t.Error(err)
		}
	})
	f.otherOwner, err = f.other.LocalOwner(publicationLibrary)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f publicationFixture) revokeRequest(t *testing.T) catalog.AdminRequest {
	t.Helper()
	return publicationAdmin(t, "publication-revoke", f.incarnation, "revoke_credential", catalog.RevokeCredentialRequest{Selector: f.selector, ExpectedGeneration: 1})
}

func (f publicationFixture) reopen(t *testing.T) *catalog.Store {
	t.Helper()
	if err := f.other.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := catalog.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	return reopened
}

func publicationPersistedState(t *testing.T, path, attemptID string) (string, bool) {
	t.Helper()
	u := url.URL{Scheme: "file", Path: path}
	query := url.Values{}
	query.Set("mode", "ro")
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	var state string
	var ready []byte
	if err := db.QueryRow(`SELECT state,ready_identity FROM delivery_materializations WHERE attempt_id=?`, attemptID).Scan(&state, &ready); err != nil {
		t.Fatal(err)
	}
	return state, len(ready) != 0
}

func publicationExactReady(t *testing.T, f publicationFixture) {
	t.Helper()
	got, err := os.ReadFile(f.ready)
	if err != nil || !bytes.Equal(got, f.payload) {
		t.Fatalf("ready payload changed: %q %v", got, err)
	}
	if _, err := os.Lstat(f.partial); !os.IsNotExist(err) {
		t.Fatalf("partial remains after rename: %v", err)
	}
}

func publicationNoReady(t *testing.T, f publicationFixture) {
	t.Helper()
	if _, err := os.Lstat(f.ready); !os.IsNotExist(err) {
		t.Fatalf("ready appeared before admission: %v", err)
	}
	got, err := os.ReadFile(f.partial)
	if err != nil || !bytes.Equal(got, f.payload) {
		t.Fatalf("partial lost: %q %v", got, err)
	}
}

type publicationCall struct {
	done   chan struct{}
	result catalog.MaterializationAttempt
	err    error
}

type revokeCall struct {
	done chan struct{}
	err  error
}

func TestCatalogPublicationFenceAcrossOriginalFilePhases(t *testing.T) {
	for _, phase := range []struct {
		name                   string
		mode                   privatefile.PublicationTestPhase
		rename, ready, attempt int32
	}{
		{"rename", privatefile.PublicationTestRename, 1, 0, 0},
		{"ready sync", privatefile.PublicationTestReadySync, 1, 1, 0},
		{"attempt sync", privatefile.PublicationTestAttemptSync, 1, 1, 1},
	} {
		t.Run(phase.name, func(t *testing.T) {
			f := newPublicationFixture(t)
			gate, err := privatefile.InstallPublicationGateForTest(f.prepared, phase.mode)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			publication := &publicationCall{done: make(chan struct{})}
			// This cleanup is registered before the call starts; any Fatal path
			// releases the fixed phase and joins the original before descriptors.
			t.Cleanup(func() { gate.Release(); <-publication.done })
			go func() {
				publication.result, publication.err = f.store.PublishMaterialization(ctx, f.bearer, f.prepared)
				close(publication.done)
			}()
			select {
			case <-gate.Entered():
			case <-publication.done:
				t.Fatalf("publication finished before %s phase: %v", phase.name, publication.err)
			case <-time.After(10 * time.Second):
				cancel()
				gate.Release()
				<-publication.done
				t.Fatalf("publication did not reach %s phase", phase.name)
			}
			if rename, ready, attempt := gate.Calls(); rename != phase.rename || ready != phase.ready || attempt != phase.attempt {
				t.Fatalf("%s original syscall order: rename=%d ready=%d attempt=%d", phase.name, rename, ready, attempt)
			}
			publicationExactReady(t, f)
			cancel()
			revoke := &revokeCall{done: make(chan struct{})}
			t.Cleanup(func() { gate.Release(); <-revoke.done; <-publication.done })
			request := f.revokeRequest(t)
			go func() {
				_, revoke.err = f.otherOwner.RevokeCredential(context.Background(), request)
				close(revoke.done)
			}()
			select {
			case <-revoke.done:
			case <-time.After(10 * time.Second):
				gate.Release()
				<-publication.done
				<-revoke.done
				t.Fatal("revoke did not finish under production SQLite busy timeout")
			}
			var busy *sqlite.Error
			if !errors.As(revoke.err, &busy) || busy.Code()&0xff != int(sqlitelib.SQLITE_BUSY) {
				t.Fatalf("revoke did not observe actual SQLite BUSY while file phase held: %v", revoke.err)
			}
			select {
			case <-publication.done:
				t.Fatalf("publication returned before phase release: %+v %v", publication.result, publication.err)
			default:
			}
			gate.Release()
			<-publication.done
			if publication.err != nil || publication.result.State != "COMPLETED" {
				t.Fatalf("publication after phase release: %+v %v", publication.result, publication.err)
			}
			if rename, ready, attempt := gate.Calls(); rename != 1 || ready != 1 || attempt != 1 {
				t.Fatalf("completed syscall order: rename=%d ready=%d attempt=%d", rename, ready, attempt)
			}
			publicationExactReady(t, f)
			if _, err := f.otherOwner.RevokeCredential(context.Background(), request); err != nil {
				t.Fatalf("revoke retry after publication: %v", err)
			}
			if _, err := f.other.AdmitDelivery(context.Background(), f.bearer, f.selection.SelectionID); !errors.Is(err, catalog.ErrUnauthenticated) {
				t.Fatalf("fresh admission after revoke: %v", err)
			}
			_ = f.reopen(t)
			if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "COMPLETED" || !hasReady {
				t.Fatalf("reopened publication state=%s ready-identity=%v", state, hasReady)
			}
			publicationExactReady(t, f)
		})
	}
}

func TestCatalogPublicationRevokedBeforeExposure(t *testing.T) {
	f := newPublicationFixture(t)
	trace, err := privatefile.InstallPublicationTraceForTest(f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.otherOwner.RevokeCredential(context.Background(), f.revokeRequest(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PublishMaterialization(context.Background(), f.bearer, f.prepared); !errors.Is(err, catalog.ErrUnauthenticated) {
		t.Fatalf("revoke-first publication: %v", err)
	}
	if rename, ready, attempt := trace.Calls(); rename != 0 || ready != 0 || attempt != 0 {
		t.Fatalf("revoke-first exposure calls: %d %d %d", rename, ready, attempt)
	}
	publicationNoReady(t, f)
	_ = f.reopen(t)
	if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "PREPARED" || hasReady {
		t.Fatalf("revoke-first persisted state=%s ready-identity=%v", state, hasReady)
	}
}

func TestCatalogPublicationPreCanceledRefusesExposure(t *testing.T) {
	f := newPublicationFixture(t)
	trace, err := privatefile.InstallPublicationTraceForTest(f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.store.PublishMaterialization(ctx, f.bearer, f.prepared); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled publication: %v", err)
	}
	if rename, ready, attempt := trace.Calls(); rename != 0 || ready != 0 || attempt != 0 {
		t.Fatalf("pre-canceled exposure calls: %d %d %d", rename, ready, attempt)
	}
	publicationNoReady(t, f)
	_ = f.reopen(t)
	if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "PREPARED" || hasReady {
		t.Fatalf("pre-canceled persisted state=%s ready-identity=%v", state, hasReady)
	}
}

func TestCatalogPublicationReportedSyncFaultCommitsUncertain(t *testing.T) {
	for _, phase := range []struct {
		name         string
		mode         privatefile.PublicationTestPhase
		attemptCalls int32
	}{
		{"ready sync", privatefile.PublicationTestReadySync, 0},
		{"attempt sync", privatefile.PublicationTestAttemptSync, 1},
	} {
		t.Run(phase.name, func(t *testing.T) {
			f := newPublicationFixture(t)
			fault, err := privatefile.InstallPublicationSyncFaultForTest(f.prepared, phase.mode)
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := f.store.PublishMaterialization(context.Background(), f.bearer, f.prepared)
			if err == nil || !strings.Contains(err.Error(), "publication directory barrier uncertain") {
				t.Errorf("reported Sync fault error: %+v %v", outcome, err)
			}
			if outcome.State != "UNCERTAIN" {
				t.Errorf("reported Sync fault outcome state: %+v %v", outcome, err)
			}
			if rename, ready, attempt := fault.Calls(); rename != 1 || ready != 1 || attempt != phase.attemptCalls {
				t.Fatalf("reported Sync fault operation order: %d %d %d", rename, ready, attempt)
			}
			publicationExactReady(t, f)
			reopened := f.reopen(t)
			if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "UNCERTAIN" || !hasReady {
				t.Fatalf("reopened uncertain state=%s ready-identity=%v", state, hasReady)
			}
			prior, err := reopened.ReserveMaterialization(context.Background(), f.bearer, f.selection.SelectionID, f.attempt.OperationID)
			if err != nil || prior.ID != f.attempt.ID || prior.State != "UNCERTAIN" {
				t.Fatalf("reopened exact reservation outcome: %+v %v", prior, err)
			}
			if _, err := reopened.PublishMaterialization(context.Background(), f.bearer, f.prepared); !errors.Is(err, catalog.ErrConflict) {
				t.Fatalf("uncertain attempt promoted again: %v", err)
			}
			if rename, ready, attempt := fault.Calls(); rename != 1 || ready != 1 || attempt != phase.attemptCalls {
				t.Fatalf("retry repeated original operations: %d %d %d", rename, ready, attempt)
			}
			publicationExactReady(t, f)
		})
	}
}

// These cases use the same public source binding, credential and prepared-file
// fixture as publication. The PREPARED case deliberately performs only the
// original no-replace file operation to model an existing final whose SQL
// completion was interrupted; recovery itself never performs a rename.
func publicationOpenOwnedReady(t *testing.T, root *privatefile.ManagedRoot, attemptID string, bound catalog.RecoveryBinding) (*privatefile.OwnedReady, error) {
	t.Helper()
	owned, err := root.OpenOwnedReady(attemptID, bound.Root, bound.Directory, bound.Lock, bound.File)
	if owned != nil {
		t.Cleanup(func() {
			if err := owned.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	return owned, err
}

func TestCatalogExistingFinalRecoveryAndCompletedReuse(t *testing.T) {
	for _, state := range []string{"COMPLETED", "PREPARED", "UNCERTAIN"} {
		t.Run(state, func(t *testing.T) {
			f := newPublicationFixture(t)
			switch state {
			case "COMPLETED":
				published, err := f.store.PublishMaterialization(context.Background(), f.bearer, f.prepared)
				if err != nil || published.State != "COMPLETED" {
					t.Fatalf("first publication: %+v %v", published, err)
				}
			case "PREPARED":
				if exposed, err := f.prepared.ExposeNoReplace(); err != nil || !exposed {
					t.Fatalf("interrupted original exposure: %t %v", exposed, err)
				}
			case "UNCERTAIN":
				fault, err := privatefile.InstallPublicationSyncFaultForTest(f.prepared, privatefile.PublicationTestReadySync)
				if err != nil {
					t.Fatal(err)
				}
				published, err := f.store.PublishMaterialization(context.Background(), f.bearer, f.prepared)
				if err == nil || published.State != "UNCERTAIN" {
					t.Fatalf("first uncertain publication: %+v %v", published, err)
				}
				if rename, ready, attempt := fault.Calls(); rename != 1 || ready != 1 || attempt != 0 {
					t.Fatalf("fault original operations: %d %d %d", rename, ready, attempt)
				}
			}
			if err := f.prepared.Close(); err != nil {
				t.Fatal(err)
			}
			bound, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, f.attempt.ID)
			if err != nil || bound.Attempt.State != state {
				t.Fatalf("recorded recovery binding: %+v %v", bound, err)
			}
			root, err := privatefile.OpenManagedRoot(f.rootPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			})
			owned, err := publicationOpenOwnedReady(t, root, f.attempt.ID, bound)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owned.Close(); err != nil {
					t.Error(err)
				}
			})
			admission, err := f.store.AdmitDelivery(context.Background(), f.bearer, f.selection.SelectionID)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := f.store.RecoverMaterialization(context.Background(), f.bearer, owned, f.client, admission.View)
			if err != nil || recovered.State != "COMPLETED" || recovered.ID != f.attempt.ID {
				t.Fatalf("verified existing-final recovery: %+v %v", recovered, err)
			}
			path, err := owned.Path()
			if err != nil || path == "" || path != f.ready {
				t.Fatalf("verified ready path=%q err=%v", path, err)
			}
			publicationExactReady(t, f)
			if err := owned.Close(); err != nil {
				t.Fatal(err)
			}
			_ = f.reopen(t)
			if actual, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); actual != "COMPLETED" || !hasReady {
				t.Fatalf("reopened recovered row: %s ready=%v", actual, hasReady)
			}
		})
	}
}

func TestCatalogExistingFinalRecoveryRefusesTamperAndAbsentFinal(t *testing.T) {
	t.Run("same-size edited final", func(t *testing.T) {
		f := newPublicationFixture(t)
		if exposed, err := f.prepared.ExposeNoReplace(); err != nil || !exposed {
			t.Fatalf("original exposure: %t %v", exposed, err)
		}
		if err := f.prepared.Close(); err != nil {
			t.Fatal(err)
		}
		damaged := bytes.Repeat([]byte("x"), len(f.payload))
		if err := os.WriteFile(f.ready, damaged, 0600); err != nil {
			t.Fatal(err)
		}
		bound, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, f.attempt.ID)
		if err != nil {
			t.Fatal(err)
		}
		root, err := privatefile.OpenManagedRoot(f.rootPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := root.Close(); err != nil {
				t.Error(err)
			}
		})
		owned, err := publicationOpenOwnedReady(t, root, f.attempt.ID, bound)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := owned.Close(); err != nil {
				t.Error(err)
			}
		})
		admission, err := f.store.AdmitDelivery(context.Background(), f.bearer, f.selection.SelectionID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.RecoverMaterialization(context.Background(), f.bearer, owned, f.client, admission.View); err == nil {
			t.Fatal("same-size edit recovered")
		}
		if actual, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); actual != "PREPARED" || hasReady {
			t.Fatalf("tampered final changed row: %s ready=%v", actual, hasReady)
		}
		if got, err := os.ReadFile(f.ready); err != nil || !bytes.Equal(got, damaged) {
			t.Fatalf("tampered bytes modified: %q %v", got, err)
		}
	})
	t.Run("partial only and live owner", func(t *testing.T) {
		f := newPublicationFixture(t)
		bound, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, f.attempt.ID)
		if err != nil {
			t.Fatal(err)
		}
		root, err := privatefile.OpenManagedRoot(f.rootPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := root.Close(); err != nil {
				t.Error(err)
			}
		})
		if _, err := publicationOpenOwnedReady(t, root, f.attempt.ID, bound); err == nil {
			t.Fatal("live original owner or partial-only accepted")
		}
		if err := f.prepared.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := publicationOpenOwnedReady(t, root, f.attempt.ID, bound); err == nil {
			t.Fatal("partial-only attempt accepted after owner release")
		}
		publicationNoReady(t, f)
	})
	t.Run("revoked before recovery binding", func(t *testing.T) {
		f := newPublicationFixture(t)
		if published, err := f.store.PublishMaterialization(context.Background(), f.bearer, f.prepared); err != nil || published.State != "COMPLETED" {
			t.Fatalf("first publication: %+v %v", published, err)
		}
		if err := f.prepared.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := f.otherOwner.RevokeCredential(context.Background(), f.revokeRequest(t)); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, f.attempt.ID); !errors.Is(err, catalog.ErrUnauthenticated) {
			t.Fatalf("revoked recovery binding: %v", err)
		}
		publicationExactReady(t, f)
	})
}

func TestCatalogPublishedOwnerCloseFaultBoundCorrection(t *testing.T) {
	f := newPublicationFixture(t)
	attempt, cleanup, err := f.store.ReserveOwnedMaterialization(context.Background(), f.bearer, f.selection.SelectionID, f.attempt.OperationID)
	if err != nil || cleanup == nil || attempt.ID != f.attempt.ID {
		t.Fatalf("bound cleanup capability: %+v %v", attempt, err)
	}
	fault, err := privatefile.InstallPreparedCloseFaultForTest(f.prepared, privatefile.AttemptCloseTestLock)
	if err != nil {
		t.Fatal(err)
	}
	f.expectedCloseFault.Store(true)
	published, err := f.store.PublishMaterialization(context.Background(), f.bearer, f.prepared)
	if err != nil || published.State != "COMPLETED" {
		t.Fatalf("original publication: %+v %v", published, err)
	}
	first := f.prepared.Close()
	if first == nil || !strings.Contains(first.Error(), "injected original attempt close uncertainty") {
		t.Fatalf("lock close error lost: %v", first)
	}
	if second := f.prepared.Close(); second != first || fault.Calls() != 1 {
		t.Fatalf("unstable original close: first=%v second=%v calls=%d", first, second, fault.Calls())
	}
	if err := cleanup.MarkUncertain(); err != nil {
		t.Fatalf("post-release conservative correction: %v", err)
	}
	publicationExactReady(t, f)
	_ = f.reopen(t)
	if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "UNCERTAIN" || !hasReady {
		t.Fatalf("close-fault durable availability: %s ready=%v", state, hasReady)
	}
}

func TestCatalogExistingFinalRecoveryRepeatsBothDurabilityBarriers(t *testing.T) {
	for _, phase := range []privatefile.PublicationTestPhase{privatefile.PublicationTestReadySync, privatefile.PublicationTestAttemptSync} {
		t.Run(string(phase), func(t *testing.T) {
			f := newPublicationFixture(t)
			fault, err := privatefile.InstallPublicationSyncFaultForTest(f.prepared, phase)
			if err != nil {
				t.Fatal(err)
			}
			published, err := f.store.PublishMaterialization(context.Background(), f.bearer, f.prepared)
			if err == nil || published.State != "UNCERTAIN" {
				t.Fatalf("original barrier fault: %+v %v", published, err)
			}
			rename, ready, attempt := fault.Calls()
			if rename != 1 || ready != 1 || attempt != map[privatefile.PublicationTestPhase]int32{privatefile.PublicationTestReadySync: 0, privatefile.PublicationTestAttemptSync: 1}[phase] {
				t.Fatalf("original operations: %d %d %d", rename, ready, attempt)
			}
			if err := f.prepared.Close(); err != nil {
				t.Fatal(err)
			}
			bound, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, f.attempt.ID)
			if err != nil {
				t.Fatal(err)
			}
			root, err := privatefile.OpenManagedRoot(f.rootPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			})
			owned, err := publicationOpenOwnedReady(t, root, f.attempt.ID, bound)
			if err != nil {
				t.Fatal(err)
			}
			trace, err := privatefile.InstallRecoveryTraceForTest(owned)
			if err != nil {
				t.Fatal(err)
			}
			admission, err := f.store.AdmitDelivery(context.Background(), f.bearer, f.selection.SelectionID)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := f.store.RecoverMaterialization(context.Background(), f.bearer, owned, f.client, admission.View)
			if err != nil || recovered.State != "COMPLETED" || recovered.ID != f.attempt.ID {
				t.Fatalf("barrier recovery: %+v %v", recovered, err)
			}
			if ready, attempt := trace.Calls(); ready != 1 || attempt != 1 {
				t.Fatalf("recovery durability barriers: %d %d", ready, attempt)
			}
			publicationExactReady(t, f)
			if err := owned.Close(); err != nil {
				t.Fatal(err)
			}
			_ = f.reopen(t)
			if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "COMPLETED" || !hasReady {
				t.Fatalf("reopened recovery row: %s ready=%v", state, hasReady)
			}
		})
	}
}

func TestCatalogExistingFinalRecoveryBarrierFaultRetainsUncertain(t *testing.T) {
	for _, phase := range []privatefile.RecoveryTestPhase{privatefile.RecoveryTestReadySync, privatefile.RecoveryTestAttemptSync} {
		t.Run(string(phase), func(t *testing.T) {
			f := newPublicationFixture(t)
			if exposed, err := f.prepared.ExposeNoReplace(); err != nil || !exposed {
				t.Fatalf("original exact final: %t %v", exposed, err)
			}
			if err := f.prepared.Close(); err != nil {
				t.Fatal(err)
			}
			bound, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, f.attempt.ID)
			if err != nil {
				t.Fatal(err)
			}
			root, err := privatefile.OpenManagedRoot(f.rootPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			})
			owned, err := publicationOpenOwnedReady(t, root, f.attempt.ID, bound)
			if err != nil {
				t.Fatal(err)
			}
			fault, err := privatefile.InstallRecoverySyncFaultForTest(owned, phase)
			if err != nil {
				t.Fatal(err)
			}
			admission, err := f.store.AdmitDelivery(context.Background(), f.bearer, f.selection.SelectionID)
			if err != nil {
				t.Fatal(err)
			}
			result, err := f.store.RecoverMaterialization(context.Background(), f.bearer, owned, f.client, admission.View)
			if err == nil || result.State != "UNCERTAIN" || result.ID != f.attempt.ID || !strings.Contains(err.Error(), "recovery directory barrier uncertain") {
				t.Fatalf("reported recovery barrier: %+v %v", result, err)
			}
			ready, attempt := fault.Calls()
			if ready != 1 || attempt != map[privatefile.RecoveryTestPhase]int32{privatefile.RecoveryTestReadySync: 0, privatefile.RecoveryTestAttemptSync: 1}[phase] {
				t.Fatalf("recovery fault operations: %d %d", ready, attempt)
			}
			publicationExactReady(t, f)
			if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "UNCERTAIN" || hasReady {
				t.Fatalf("reported barrier changed durable outcome: %s ready=%v", state, hasReady)
			}
			if err := owned.Close(); err != nil {
				t.Fatal(err)
			}
			_ = f.reopen(t)
			if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "UNCERTAIN" || hasReady {
				t.Fatalf("reopened barrier outcome: %s ready=%v", state, hasReady)
			}
		})
	}
}

func TestCatalogKnownCloseFaultRecordsWhileOriginalLockHeld(t *testing.T) {
	f := newPublicationFixture(t)
	attempt, cleanup, err := f.store.ReserveOwnedMaterialization(context.Background(), f.bearer, f.selection.SelectionID, f.attempt.OperationID)
	if err != nil || cleanup == nil || attempt.ID != f.attempt.ID {
		t.Fatalf("bound cleanup: %+v %v", attempt, err)
	}
	if published, err := f.store.PublishMaterialization(context.Background(), f.bearer, f.prepared); err != nil || published.State != "COMPLETED" {
		t.Fatalf("original publication: %+v %v", published, err)
	}
	bound, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, f.attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	root, err := privatefile.OpenManagedRoot(f.rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	fault, err := privatefile.InstallPreparedCloseFaultForTest(f.prepared, privatefile.AttemptCloseTestRead)
	if err != nil {
		t.Fatal(err)
	}
	f.expectedCloseFault.Store(true)
	if err := f.prepared.CloseBeforeRelease(); err == nil || !strings.Contains(err.Error(), "injected original attempt close uncertainty") {
		t.Fatalf("known nonterminal close fault: %v", err)
	}
	if fault.Calls() != 1 {
		t.Fatalf("original read close count=%d", fault.Calls())
	}
	if _, err := publicationOpenOwnedReady(t, root, f.attempt.ID, bound); err == nil {
		t.Fatal("second owner acquired before durable close bookkeeping")
	}
	if err := cleanup.MarkUncertain(); err != nil {
		t.Fatalf("bound bookkeeping under original lock: %v", err)
	}
	if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "UNCERTAIN" || !hasReady {
		t.Fatalf("pre-release durable outcome: %s ready=%v", state, hasReady)
	}
	if err := f.prepared.ReleaseLock(); err != nil {
		t.Fatalf("terminal release: %v", err)
	}
	owned, err := publicationOpenOwnedReady(t, root, f.attempt.ID, bound)
	if err != nil {
		t.Fatalf("independent owner after release: %v", err)
	}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	publicationExactReady(t, f)
}

func TestCatalogRecoveryBarrierRetainsWriterFenceAfterCancellation(t *testing.T) {
	f := newPublicationFixture(t)
	fault, err := privatefile.InstallPublicationSyncFaultForTest(f.prepared, privatefile.PublicationTestReadySync)
	if err != nil {
		t.Fatal(err)
	}
	if first, err := f.store.PublishMaterialization(context.Background(), f.bearer, f.prepared); err == nil || first.State != "UNCERTAIN" {
		t.Fatalf("original interrupted barrier: %+v %v", first, err)
	}
	if rename, ready, attempt := fault.Calls(); rename != 1 || ready != 1 || attempt != 0 {
		t.Fatalf("original phases: %d %d %d", rename, ready, attempt)
	}
	if err := f.prepared.Close(); err != nil {
		t.Fatal(err)
	}
	bound, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, f.attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	root, err := privatefile.OpenManagedRoot(f.rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	owned, err := publicationOpenOwnedReady(t, root, f.attempt.ID, bound)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := privatefile.InstallRecoveryGateForTest(owned, privatefile.RecoveryTestReadySync)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := f.store.AdmitDelivery(context.Background(), f.bearer, f.selection.SelectionID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	recovery := &publicationCall{done: make(chan struct{})}
	t.Cleanup(func() { gate.Release(); <-recovery.done })
	go func() {
		recovery.result, recovery.err = f.store.RecoverMaterialization(ctx, f.bearer, owned, f.client, admission.View)
		close(recovery.done)
	}()
	select {
	case <-gate.Entered():
	case <-recovery.done:
		t.Fatalf("recovery returned before original ready sync: %v", recovery.err)
	case <-time.After(10 * time.Second):
		gate.Release()
		<-recovery.done
		t.Fatal("recovery did not reach original ready sync")
	}
	if ready, attempt := gate.Calls(); ready != 1 || attempt != 0 {
		t.Fatalf("held recovery syscall count: %d %d", ready, attempt)
	}
	cancel()
	revoke := &revokeCall{done: make(chan struct{})}
	t.Cleanup(func() { gate.Release(); <-revoke.done; <-recovery.done })
	request := f.revokeRequest(t)
	go func() {
		_, revoke.err = f.otherOwner.RevokeCredential(context.Background(), request)
		close(revoke.done)
	}()
	select {
	case <-revoke.done:
	case <-time.After(10 * time.Second):
		gate.Release()
		<-recovery.done
		<-revoke.done
		t.Fatal("revoke did not finish under SQLite busy timeout")
	}
	var busy *sqlite.Error
	if !errors.As(revoke.err, &busy) || busy.Code()&0xff != int(sqlitelib.SQLITE_BUSY) {
		t.Fatalf("revoke did not observe original recovery writer fence: %v", revoke.err)
	}
	select {
	case <-recovery.done:
		t.Fatalf("recovery returned during held barrier: %+v %v", recovery.result, recovery.err)
	default:
	}
	gate.Release()
	<-recovery.done
	if recovery.err != nil || recovery.result.State != "COMPLETED" {
		t.Fatalf("original recovery completion: %+v %v", recovery.result, recovery.err)
	}
	if ready, attempt := gate.Calls(); ready != 1 || attempt != 1 {
		t.Fatalf("completed recovery barriers: %d %d", ready, attempt)
	}
	publicationExactReady(t, f)
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.otherOwner.RevokeCredential(context.Background(), request); err != nil {
		t.Fatalf("revoke retry: %v", err)
	}
	if _, err := f.other.AdmitDelivery(context.Background(), f.bearer, f.selection.SelectionID); !errors.Is(err, catalog.ErrUnauthenticated) {
		t.Fatalf("fresh admission after revoke: %v", err)
	}
	_ = f.reopen(t)
	if state, hasReady := publicationPersistedState(t, f.path, f.attempt.ID); state != "COMPLETED" || !hasReady {
		t.Fatalf("reopened recovery state: %s ready=%v", state, hasReady)
	}
}
