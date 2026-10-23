package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

const deliveryClockBase int64 = 1800000000

type deliveryClockFixture struct {
	store  *Store
	path   string
	bearer []byte
	now    *atomic.Int64
}

// The metadata, policy and credential setup uses production catalog APIs.
// This clock-only fixture installs one explicitly synthetic, shape-valid source
// row to isolate catalog admission from Gateway HTTP; it does not prove source
// verification or an actual Gateway binding.
func newDeliveryClockFixture(t *testing.T, credentialLife int64) deliveryClockFixture {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "clock.sqlite")
	s, err := CreateDelivery(ctx, path, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	now := &atomic.Int64{}
	now.Store(deliveryClockBase)
	s.clock = func() time.Time { return time.Unix(now.Load(), 0) }
	owner, err := s.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "clock owner"
	boot, err := owner.Bootstrap(ctx, adminTestRequest("clock-bootstrap", "", "bootstrap", BootstrapRequest{ownerSubject, &label}))
	if err != nil {
		t.Fatal(err)
	}
	authority := AuthorityStatus{boot.Result.Incarnation, ownerSubject}
	applyTest(t, s, initialOperation())
	policyTest(t, owner, authority, "clock-metadata",
		NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "clock reader", true}}),
		NewPolicyChange("put_grant", PutGrant{"clock-discovery", nil, GrantValue{readerSubject, Scope{"asset", testAsset}, []string{"assets.discover", "revisions.read"}}}))
	byteValue := ByteGrantValue{readerSubject, Scope{"asset", testAsset}, "bytes.read"}
	if _, err := owner.ApplyBytePolicy(ctx, BytePolicyRequest{Version: 1, LibraryID: testLibrary, ID: "clock-byte-grant", ExpectedIncarnation: authority.Incarnation, Changes: []ByteGrantChange{{Kind: "put", ID: "bgr_11111111111111111111111111111111", Value: &byteValue}}}); err != nil {
		t.Fatal(err)
	}
	request := IssueCredentialRequestV3{readerSubject, Scope{"asset", testAsset}, []string{"assets.discover", "bytes.read", "revisions.read"}, deliveryClockBase + credentialLife}
	issued, err := owner.IssueCredentialV3(ctx, AdminRequestV3{Version: 3, LibraryID: testLibrary, ID: "clock-credential", ExpectedIncarnation: authority.Incarnation, Kind: "issue_credential", Value: collectionTxRaw(t, request)})
	if err != nil || len(issued.Bearer) != 102 {
		t.Fatalf("codec-3 fixture issuance: %v, bearer bytes=%d", err, len(issued.Bearer))
	}
	view := mutationauthority.ImageView{
		Version: 1, StoreID: "ims_11111111111111111111111111111111", Volume: "clock_volume", LibraryID: string(testLibrary),
		ImageID: "img_22222222222222222222222222222222", Incarnation: "clock_file_cohort", ImageDigest: strings.Repeat("a", 64),
		Content: mutationauthority.ImageContent{Size: 3, SHA256: testSHA}, Activation: 1, Generation: 1,
		Chunks: []mutationauthority.ImagePartView{{Index: 0, Offset: 0, Size: 3, SHA256: testSHA}},
	}
	if err := view.Validate(); err != nil {
		t.Fatalf("synthetic source view invalid: %v", err)
	}
	image, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	sourceID := "dsrc_33333333333333333333333333333333"
	if _, err := s.db.ExecContext(ctx, `INSERT INTO delivery_sources(source_id,source_version,library_id,asset_id,revision_id,content_size,content_sha256,authority_incarnation,route_id,route_digest,reader_leaf_sha256,image_view,enabled,created_unix_nano) VALUES(?,1,?,?,?,?,?,?,?,?,?,?,1,?)`,
		sourceID, testLibrary, testAsset, rootRevision, 3, testSHA, authority.Incarnation, "clock-route", strings.Repeat("b", 64), strings.Repeat("c", 64), image, deliveryClockBase*1e9); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO delivery_source_heads(library_id,asset_id,revision_id,source_id,source_version) VALUES(?,?,?,?,1)`, testLibrary, testAsset, rootRevision, sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE delivery_quota SET source_versions=1 WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
	return deliveryClockFixture{s, path, issued.Bearer, now}
}

func (f deliveryClockFixture) selectLatest(t *testing.T, operation string, expiry int64) (DeliverySelectRequest, DeliverySelection) {
	t.Helper()
	request := DeliverySelectRequest{Version: 1, LibraryID: testLibrary, ID: operation, AssetID: testAsset, Mode: "latest", ExpiresUnix: expiry, MaxBytes: 3}
	selection, err := f.store.SelectDelivery(context.Background(), f.bearer, request)
	if err != nil || selection.RevisionID != rootRevision {
		t.Fatalf("clock fixture selection: %+v %v", selection, err)
	}
	return request, selection
}

func clockFloor(t *testing.T, s *Store) int64 {
	t.Helper()
	var floor int64
	if err := s.db.QueryRow(`SELECT high_water_unix FROM delivery_clock WHERE singleton=1`).Scan(&floor); err != nil {
		t.Fatal(err)
	}
	return floor
}

func clockQuota(t *testing.T, s *Store) (int64, int64, int64, int64) {
	t.Helper()
	var selections, attempts, canonical, bytes int64
	if err := s.db.QueryRow(`SELECT selections,attempts,canonical_bytes,reserved_logical_bytes FROM delivery_quota WHERE singleton=1`).Scan(&selections, &attempts, &canonical, &bytes); err != nil {
		t.Fatal(err)
	}
	return selections, attempts, canonical, bytes
}

func clockPrepared(t *testing.T, f deliveryClockFixture, selectionID string) (*privatefile.PreparedFile, string, MaterializationAttempt) {
	t.Helper()
	reserved, err := f.store.ReserveMaterialization(context.Background(), f.bearer, selectionID, "clock-materialize")
	if err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	rootPath := filepath.Join(parent, "managed")
	for _, directory := range []string{rootPath, filepath.Join(rootPath, "attempts"), filepath.Join(rootPath, "ready")} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	root, err := privatefile.OpenManagedRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	attempt, err := root.OpenAttempt(reserved.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := attempt.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := attempt.Writer().Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	prepared, err := attempt.Finish()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	state, err := f.store.RecordPreparedMaterialization(context.Background(), f.bearer, prepared)
	if err != nil || state.State != "PREPARED" {
		t.Fatalf("prepare managed file: %+v %v", state, err)
	}
	return prepared, filepath.Join(rootPath, "ready", reserved.ID+".payload"), state
}

func TestDeliveryClockExpiryDenialSurvivesRollbackAndReopen(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	_, selected := f.selectLatest(t, "clock-expiry-select", deliveryClockBase+60)
	prepared, ready, _ := clockPrepared(t, f, selected.SelectionID)
	f.now.Store(deliveryClockBase + 60)
	if _, err := f.store.DeliveryStatus(context.Background(), f.bearer, selected.SelectionID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expiry status denial: %v", err)
	}
	if got := clockFloor(t, f.store); got != deliveryClockBase+60 {
		t.Fatalf("expiry floor=%d", got)
	}
	f.now.Store(deliveryClockBase)
	other, err := Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := other.Close(); err != nil {
			t.Error(err)
		}
	})
	other.clock = func() time.Time { return time.Unix(f.now.Load(), 0) }
	if _, err := other.DeliveryStatus(context.Background(), f.bearer, selected.SelectionID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other-store status revived: %v", err)
	}
	if _, err := other.AdmitDelivery(context.Background(), f.bearer, selected.SelectionID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other-store admission revived: %v", err)
	}
	if _, err := other.AdmitDeliveryChunk(context.Background(), f.bearer, selected.SelectionID, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other-store chunk revived: %v", err)
	}
	if _, err := other.PublishMaterialization(context.Background(), f.bearer, prepared); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other-store publication revived: %v", err)
	}
	if _, err := os.Lstat(ready); !os.IsNotExist(err) {
		t.Fatalf("expiry exposed ready file: %v", err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	reopened.clock = func() time.Time { return time.Unix(f.now.Load(), 0) }
	if _, err := reopened.DeliveryStatus(context.Background(), f.bearer, selected.SelectionID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reopened status revived: %v", err)
	}
	if got := clockFloor(t, reopened); got != deliveryClockBase+60 {
		t.Fatalf("reopened floor=%d", got)
	}
}

func TestDeliveryClockCredentialExpiryAndRetryFloors(t *testing.T) {
	t.Run("credential expiry", func(t *testing.T) {
		f := newDeliveryClockFixture(t, 60)
		_, selected := f.selectLatest(t, "clock-credential-expiry", deliveryClockBase+60)
		f.now.Store(deliveryClockBase + 60)
		if _, err := f.store.DeliveryStatus(context.Background(), f.bearer, selected.SelectionID); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("credential expiry denial: %v", err)
		}
		f.now.Store(deliveryClockBase)
		other, err := Open(f.path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := other.Close(); err != nil {
				t.Error(err)
			}
		})
		other.clock = func() time.Time { return time.Unix(f.now.Load(), 0) }
		if _, err := other.DeliveryStatus(context.Background(), f.bearer, selected.SelectionID); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("credential revived: %v", err)
		}
		if got := clockFloor(t, other); got != deliveryClockBase+60 {
			t.Fatalf("credential floor=%d", got)
		}
	})
	t.Run("successful exact retries", func(t *testing.T) {
		f := newDeliveryClockFixture(t, 3600)
		request, selected := f.selectLatest(t, "clock-exact-retry", deliveryClockBase+500)
		attempt, err := f.store.ReserveMaterialization(context.Background(), f.bearer, selected.SelectionID, "clock-reserve-retry")
		if err != nil {
			t.Fatal(err)
		}
		beforeSelections, beforeAttempts, beforeCanonical, beforeBytes := clockQuota(t, f.store)
		f.now.Store(deliveryClockBase + 100)
		retried, err := f.store.SelectDelivery(context.Background(), f.bearer, request)
		if err != nil || retried != selected {
			t.Fatalf("selection retry changed: %+v %v", retried, err)
		}
		if got := clockFloor(t, f.store); got != deliveryClockBase+100 {
			t.Fatalf("selection retry floor before reserve=%d", got)
		}
		f.now.Store(deliveryClockBase + 150)
		retriedAttempt, err := f.store.ReserveMaterialization(context.Background(), f.bearer, selected.SelectionID, "clock-reserve-retry")
		if err != nil || retriedAttempt != attempt {
			t.Fatalf("reservation retry changed: %+v %v", retriedAttempt, err)
		}
		if got := clockFloor(t, f.store); got != deliveryClockBase+150 {
			t.Fatalf("retry floor=%d", got)
		}
		if selections, attempts, canonical, bytes := clockQuota(t, f.store); selections != beforeSelections || attempts != beforeAttempts || canonical != beforeCanonical || bytes != beforeBytes {
			t.Fatalf("retry consumed quota: selections=%d attempts=%d canonical=%d bytes=%d", selections, attempts, canonical, bytes)
		}
		f.now.Store(deliveryClockBase)
		short := request
		short.ID = "clock-short-after-rollback"
		short.ExpiresUnix = deliveryClockBase + 150
		if _, err := f.store.SelectDelivery(context.Background(), f.bearer, short); err == nil {
			t.Fatal("rollback accepted expiry at persisted floor")
		}
	})
}

func TestDeliveryClockFailedAdmissionsAndCheckpointFault(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	request, selected := f.selectLatest(t, "clock-failed-base", deliveryClockBase+500)
	beforeSelections, beforeAttempts, beforeCanonical, beforeBytes := clockQuota(t, f.store)
	f.now.Store(deliveryClockBase + 20)
	changed := request
	changed.MaxBytes = 4
	if _, err := f.store.SelectDelivery(context.Background(), f.bearer, changed); !errors.Is(err, ErrChangedRequest) {
		t.Fatalf("changed request: %v", err)
	}
	f.now.Store(deliveryClockBase + 30)
	if _, err := f.store.AdmitDeliveryChunk(context.Background(), f.bearer, selected.SelectionID, 99); err == nil {
		t.Fatal("invalid chunk index accepted")
	}
	if got := clockFloor(t, f.store); got != deliveryClockBase+30 {
		t.Fatalf("failed-admission floor=%d", got)
	}
	f.now.Store(deliveryClockBase + 35)
	badBearer := bytes.Clone(f.bearer)
	if badBearer[len(badBearer)-1] == '0' {
		badBearer[len(badBearer)-1] = '1'
	} else {
		badBearer[len(badBearer)-1] = '0'
	}
	if _, err := f.store.DeliveryStatus(context.Background(), badBearer, selected.SelectionID); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("bad bearer admission: %v", err)
	}
	if got := clockFloor(t, f.store); got != deliveryClockBase+35 {
		t.Fatalf("permission-denial floor=%d", got)
	}
	if selections, attempts, canonical, bytes := clockQuota(t, f.store); selections != beforeSelections || attempts != beforeAttempts || canonical != beforeCanonical || bytes != beforeBytes {
		t.Fatal("failed admissions changed business quota")
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.store.clock = func() time.Time { cancel(); return time.Unix(deliveryClockBase+40, 0) }
	if _, err := f.store.DeliveryStatus(ctx, f.bearer, selected.SelectionID); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-observation cancellation: %v", err)
	}
	f.store.clock = func() time.Time { return time.Unix(f.now.Load(), 0) }
	if got := clockFloor(t, f.store); got != deliveryClockBase+40 {
		t.Fatalf("canceled observation floor=%d", got)
	}
	if _, err := f.store.db.Exec(`CREATE TEMP TRIGGER delivery_quota_fault BEFORE UPDATE ON main.delivery_quota BEGIN SELECT RAISE(ABORT,'quota fault'); END`); err != nil {
		t.Fatal(err)
	}
	f.now.Store(deliveryClockBase + 45)
	newRequest := request
	newRequest.ID = "clock-rolled-back-business"
	if _, err := f.store.SelectDelivery(context.Background(), f.bearer, newRequest); err == nil || !strings.Contains(err.Error(), "quota fault") {
		t.Fatalf("quota fault not reached: %v", err)
	}
	if _, err := f.store.db.Exec(`DROP TRIGGER delivery_quota_fault`); err != nil {
		t.Fatal(err)
	}
	var rowCount int64
	if err := f.store.db.QueryRow(`SELECT count(*) FROM delivery_selections WHERE operation_id=?`, newRequest.ID).Scan(&rowCount); err != nil || rowCount != 0 {
		t.Fatalf("failed business row persisted: rows=%d err=%v", rowCount, err)
	}
	if selections, attempts, canonical, bytes := clockQuota(t, f.store); selections != beforeSelections || attempts != beforeAttempts || canonical != beforeCanonical || bytes != beforeBytes {
		t.Fatal("failed business transaction changed quota")
	}
	if got := clockFloor(t, f.store); got != deliveryClockBase+45 {
		t.Fatalf("rolled-back business clock floor=%d", got)
	}
	if _, err := f.store.db.Exec(`CREATE TEMP TRIGGER delivery_clock_fault BEFORE UPDATE ON main.delivery_clock BEGIN SELECT RAISE(ABORT,'clock checkpoint fault'); END`); err != nil {
		t.Fatal(err)
	}
	f.now.Store(deliveryClockBase + 50)
	_, err := f.store.DeliveryStatus(context.Background(), f.bearer, selected.SelectionID)
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrUnauthenticated) || !strings.Contains(err.Error(), "delivery clock outcome uncertain") {
		t.Fatalf("checkpoint fault escaped as definitive result: %v", err)
	}
	if _, err := f.store.db.Exec(`DROP TRIGGER delivery_clock_fault`); err != nil {
		t.Fatal(err)
	}
	if got := clockFloor(t, f.store); got != deliveryClockBase+45 {
		t.Fatalf("failed checkpoint changed floor=%d", got)
	}
}

func TestDeliveryClockConcurrentStoreNeverLowersFloor(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	request, selected := f.selectLatest(t, "clock-overlap-selection", deliveryClockBase+500)
	other, err := Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := other.Close(); err != nil {
			t.Error(err)
		}
	})
	other.clock = func() time.Time { return time.Unix(deliveryClockBase+80, 0) }
	if _, err := other.DeliveryStatus(context.Background(), f.bearer, selected.SelectionID); err != nil {
		t.Fatal(err)
	}
	f.now.Store(deliveryClockBase + 60)
	request.MaxBytes++
	if _, err := f.store.SelectDelivery(context.Background(), f.bearer, request); !errors.Is(err, ErrChangedRequest) {
		t.Fatalf("lower concurrent observation: %v", err)
	}
	if got := clockFloor(t, f.store); got != deliveryClockBase+80 {
		t.Fatalf("concurrent floor lowered to %d", got)
	}
}

func TestDeliveryClockCanceledObservationDrainsBeforeClose(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	_, selected := f.selectLatest(t, "clock-close-selection", deliveryClockBase+500)
	entered := make(chan struct{})
	release := make(chan struct{})
	f.store.clock = func() time.Time {
		close(entered)
		<-release
		return time.Unix(deliveryClockBase+75, 0)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callDone := make(chan error, 1)
	go func() { _, err := f.store.DeliveryStatus(ctx, f.bearer, selected.SelectionID); callDone <- err }()
	select {
	case <-entered:
	case err := <-callDone:
		t.Fatalf("call finished before clock observation: %v", err)
	}
	cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- f.store.Close() }()
	closedEarly := false
	var closeErr error
	select {
	case closeErr = <-closeDone:
		closedEarly = true
	default:
	}
	close(release)
	callErr := <-callDone
	if !closedEarly {
		closeErr = <-closeDone
	}
	if closedEarly || closeErr != nil || !errors.Is(callErr, context.Canceled) {
		t.Fatalf("cancellation/Close ownership: early=%v call=%v close=%v", closedEarly, callErr, closeErr)
	}
	reopened, err := Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if got := clockFloor(t, reopened); got != deliveryClockBase+75 {
		t.Fatalf("Close preceded checkpoint: floor=%d", got)
	}
}

func TestDeliveryClockFinalExposureResamplesAndRetainsUncertainFile(t *testing.T) {
	t.Run("time advances before exposure", func(t *testing.T) {
		f := newDeliveryClockFixture(t, 3600)
		_, selected := f.selectLatest(t, "clock-late-exposure", deliveryClockBase+60)
		prepared, ready, _ := clockPrepared(t, f, selected.SelectionID)
		var samples atomic.Int64
		f.store.clock = func() time.Time {
			if samples.Add(1) == 1 {
				return time.Unix(deliveryClockBase+10, 0)
			}
			return time.Unix(deliveryClockBase+60, 0)
		}
		if _, err := f.store.PublishMaterialization(context.Background(), f.bearer, prepared); !errors.Is(err, ErrNotFound) {
			t.Fatalf("late expiry accepted: %v", err)
		}
		if samples.Load() < 2 {
			t.Fatal("publication did not resample before exposure")
		}
		if _, err := os.Lstat(ready); !os.IsNotExist(err) {
			t.Fatalf("late expiry exposed file: %v", err)
		}
		if got := clockFloor(t, f.store); got != deliveryClockBase+60 {
			t.Fatalf("late-exposure floor=%d", got)
		}
	})
	t.Run("post exposure SQL fault", func(t *testing.T) {
		f := newDeliveryClockFixture(t, 3600)
		_, selected := f.selectLatest(t, "clock-uncertain-exposure", deliveryClockBase+500)
		prepared, ready, prior := clockPrepared(t, f, selected.SelectionID)
		if _, err := f.store.db.Exec(`CREATE TEMP TRIGGER delivery_outcome_fault BEFORE UPDATE ON main.delivery_materializations WHEN NEW.state='COMPLETED' BEGIN SELECT RAISE(ABORT,'outcome fault'); END`); err != nil {
			t.Fatal(err)
		}
		f.now.Store(deliveryClockBase + 25)
		_, err := f.store.PublishMaterialization(context.Background(), f.bearer, prepared)
		if err == nil || !strings.Contains(err.Error(), "publication state uncertain") {
			t.Fatalf("post-exposure fault misreported: %v", err)
		}
		if _, err := f.store.db.Exec(`DROP TRIGGER delivery_outcome_fault`); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(ready)
		if err != nil || !bytes.Equal(got, []byte("abc")) {
			t.Fatalf("exposed file lost after uncertain SQL: %q %v", got, err)
		}
		var state string
		if err := f.store.db.QueryRow(`SELECT state FROM delivery_materializations WHERE attempt_id=?`, prior.ID).Scan(&state); err != nil || state != "PREPARED" {
			t.Fatalf("uncertain row state=%s err=%v", state, err)
		}
		if got := clockFloor(t, f.store); got != deliveryClockBase+25 {
			t.Fatalf("post-exposure floor=%d", got)
		}
	})
}
