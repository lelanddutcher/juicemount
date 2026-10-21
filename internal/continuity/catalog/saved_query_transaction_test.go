package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/lelanddutcher/juicemount/internal/continuity"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func savedQueryTestStore(t *testing.T) (*Store, *LocalOwnerService, AuthorityStatus, *DiscoveryService, []byte, []byte) {
	t.Helper()
	s, err := CreateSavedQueries(context.Background(), filepath.Join(t.TempDir(), "collections.sqlite"), testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	s.clock = func() time.Time { return time.Unix(1800000000, 0) }
	o, err := s.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "owner"
	boot, err := o.Bootstrap(context.Background(), adminTestRequest("tx-bootstrap", "", "bootstrap", BootstrapRequest{ownerSubject, &label}))
	if err != nil {
		t.Fatal(err)
	}
	a := AuthorityStatus{boot.Result.Incarnation, ownerSubject}
	applyTest(t, s, initialOperation())
	applyTest(t, s, testOperation("tx-other", Change{CreateAsset: &CreateAsset{otherAsset, "other"}}))
	policyTest(t, o, a, "tx-subjects", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}), NewPolicyChange("put_grant", PutGrant{"tx-owner-assets", nil, grantForTest(ownerSubject, Scope{"library", ""})}), NewPolicyChange("put_grant", PutGrant{"tx-reader-assets", nil, grantForTest(readerSubject, Scope{"asset", testAsset})}))
	value := CollectionGrantValue{ownerSubject, CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	policy := collectionTxPolicy(1, "", "library_create", CollectionGrantChange{"put", "cgr_11111111111111111111111111111111", nil, &value})
	if _, err := o.ApplyCollectionPolicy(context.Background(), collectionTxOwner(t, a, policy)); err != nil {
		t.Fatal(err)
	}
	owner, _ := collectionTxIssue(t, o, a, "tx-owner-issue", ownerSubject, collectionTxActions)
	reader, _ := collectionTxIssue(t, o, a, "tx-reader-issue", readerSubject, collectionTxActions)
	d, err := NewDiscoveryService(context.Background(), s, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return s, o, a, d, owner.Bearer, reader.Bearer
}
func savedQueryTestCreate(t *testing.T, n int, anchors ...continuity.AssetID) SavedQueryRequest {
	t.Helper()
	if anchors == nil {
		anchors = []continuity.AssetID{}
	}
	return SavedQueryRequest{2, testLibrary, CollectionOperationID(fmt.Sprintf("cop_%032x", n)), CollectionID(fmt.Sprintf("col_%032x", n)), "", "create", collectionTxRaw(t, CreateSavedQuery{"Saved", CollectionGrantID(fmt.Sprintf("cgr_%032x", n)), SavedQueryDefinition{1, "ranked_assets", "Alpha", anchors}})}
}
func savedQueryTestEdit(t *testing.T, n int, id CollectionID, version CollectionVersion, kind string, value any) SavedQueryRequest {
	return SavedQueryRequest{2, testLibrary, CollectionOperationID(fmt.Sprintf("cop_%032x", n)), id, version, kind, collectionTxRaw(t, value)}
}
func savedQueryTestCall(t *testing.T, d *DiscoveryService, bearer []byte, op SavedQueryRequest) SavedQueryReceipt {
	t.Helper()
	var out bytes.Buffer
	result, err := d.MutateSavedQuery(context.Background(), bearer, op, &out)
	if err != nil || !result.Started {
		t.Fatalf("saved query mutation: %+v %v", result, err)
	}
	receipt, err := DecodeSavedQueryReceipt(out.Bytes())
	if err != nil || receipt.OperationID != op.ID || receipt.CollectionID != op.CollectionID {
		t.Fatalf("saved query receipt: %+v %v", receipt, err)
	}
	return receipt
}
func savedQueryTestDenied(t *testing.T, d *DiscoveryService, bearer []byte, op SavedQueryRequest, want error) {
	t.Helper()
	before := savedQueryTestSnapshot(t, d.store, true)
	var out bytes.Buffer
	r, err := d.MutateSavedQuery(context.Background(), bearer, op, &out)
	if !errors.Is(err, want) || r.Started || out.Len() != 0 {
		t.Fatalf("saved query denial: %+v %v %s", r, err, out.String())
	}
	if !bytes.Equal(before, savedQueryTestSnapshot(t, d.store, true)) {
		t.Fatal("denial changed durable state")
	}
}

func TestSavedQueryMutationPersistenceCASAndHistoricalRetry(t *testing.T) {
	s, _, _, d, bearer, _ := savedQueryTestStore(t)
	ctx := context.Background()
	create := savedQueryTestCreate(t, 100, testAsset)
	first := savedQueryTestCall(t, d, bearer, create)
	before := savedQueryTestSnapshot(t, s, true)
	if got := savedQueryTestCall(t, d, bearer, create); got != first || !bytes.Equal(before, savedQueryTestSnapshot(t, s, true)) {
		t.Fatal("retry changed receipt or state")
	}
	renamedOp := savedQueryTestEdit(t, 101, create.CollectionID, first.ResultVersion, "rename", RenameCollection{"Renamed"})
	renamed := savedQueryTestCall(t, d, bearer, renamedOp)
	// A different operation must not reuse the old full definition CAS.
	stale := savedQueryTestEdit(t, 102, create.CollectionID, first.ResultVersion, "rename", RenameCollection{"Stale"})
	savedQueryTestDenied(t, d, bearer, stale, ErrConflict)
	savedQueryTestDenied(t, d, bearer, savedQueryTestEdit(t, 103, create.CollectionID, renamed.ResultVersion, "rename", RenameCollection{"Renamed"}), ErrConflict)
	blank := SavedQueryDefinition{1, "ranked_assets", "", []continuity.AssetID{}}
	replace := savedQueryTestEdit(t, 104, create.CollectionID, renamed.ResultVersion, "replace_definition", ReplaceSavedQueryDefinition{blank})
	replaced := savedQueryTestCall(t, d, bearer, replace)
	savedQueryTestDenied(t, d, bearer, savedQueryTestEdit(t, 105, create.CollectionID, replaced.ResultVersion, "replace_definition", ReplaceSavedQueryDefinition{blank}), ErrConflict)
	archive := savedQueryTestEdit(t, 106, create.CollectionID, replaced.ResultVersion, "archive", SetCollectionArchived{true})
	archived := savedQueryTestCall(t, d, bearer, archive)
	repair := savedQueryTestEdit(t, 107, create.CollectionID, archived.ResultVersion, "rename", RenameCollection{"Archived repair"})
	repaired := savedQueryTestCall(t, d, bearer, repair)
	if first.ResultVersion == renamed.ResultVersion || renamed.ResultVersion == replaced.ResultVersion || replaced.ResultVersion == archived.ResultVersion || archived.ResultVersion == repaired.ResultVersion {
		t.Fatal("full version did not advance")
	}
	if got := savedQueryTestCall(t, d, bearer, create); got != first {
		t.Fatal("historical create retry lost")
	}
	changed := create
	changed.Value = collectionTxRaw(t, CreateSavedQuery{"Changed", CollectionGrantID(fmt.Sprintf("cgr_%032x", 100)), blank})
	savedQueryTestDenied(t, d, bearer, changed, ErrChangedRequest)
	var entries int
	if err := s.db.QueryRow(`SELECT count(*) FROM collection_entries WHERE collection_id=?`, create.CollectionID).Scan(&entries); err != nil || entries != 0 {
		t.Fatalf("query allocated entries: %d %v", entries, err)
	}
	if err := d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := readSavedQueryDefinition(ctx, reopened.db, create.CollectionID)
	want, _, _ := CanonicalSavedQueryDefinition(blank)
	if err != nil || !got.archived || got.name != "Archived repair" || got.version != repaired.ResultVersion || !bytes.Equal(got.canonical, want) {
		t.Fatalf("reopened definition: %+v %v", got, err)
	}
}

type savedQueryFailWriter struct{}

func (savedQueryFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestSavedQueryLostPublicationAndGeneratedChildRollback(t *testing.T) {
	s, _, _, d, bearer, _ := savedQueryTestStore(t)
	create := savedQueryTestCreate(t, 120, testAsset)
	r, err := d.MutateSavedQuery(context.Background(), bearer, create, savedQueryFailWriter{})
	if !errors.Is(err, io.ErrClosedPipe) || !r.Started {
		t.Fatalf("publication failure: %+v %v", r, err)
	}
	savedQueryTestCall(t, d, bearer, create)
	before := savedQueryTestSnapshot(t, s, true)
	for _, statement := range []string{`CREATE TEMP TABLE saved_query_fault_parent(id INTEGER PRIMARY KEY)`, `CREATE TEMP TABLE saved_query_fault_child(id INTEGER REFERENCES saved_query_fault_parent(id) DEFERRABLE INITIALLY DEFERRED)`, `CREATE TEMP TRIGGER saved_query_fault AFTER INSERT ON main.collection_events BEGIN INSERT INTO saved_query_fault_child VALUES(1); END`} {
		if _, err := s.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	r, err = d.MutateSavedQuery(context.Background(), bearer, savedQueryTestCreate(t, 121), &out)
	if err == nil || !strings.Contains(err.Error(), "commit outcome uncertain") || r.Started || out.Len() != 0 {
		t.Fatalf("real deferred commit failure: %+v %v", r, err)
	}
	if !bytes.Equal(before, savedQueryTestSnapshot(t, s, true)) {
		t.Fatal("failed parent retained child, generation, declaration, ordinal or quota")
	}
	if _, err := s.db.Exec(`DROP TRIGGER saved_query_fault`); err != nil {
		t.Fatal(err)
	}
	savedQueryTestCall(t, d, bearer, savedQueryTestCreate(t, 121))
}

func TestSavedQuerySharedQuotaAndGeneratedRollback(t *testing.T) {
	s, _, _, d, bearer, _ := savedQueryTestStore(t)
	manual := collectionTxCreate(t, 130)
	collectionTxCall(t, d, bearer, manual)
	query := savedQueryTestCreate(t, 131)
	savedQueryTestCall(t, d, bearer, query)
	usage, err := readCollectionQuota(context.Background(), s.db, query.CollectionID)
	if err != nil || usage.Library.Collections != 2 || usage.Collection.LifetimeEntries != 0 || usage.Collection.LifetimeGrants != 1 || usage.Collection.ReservedBytes != CollectionRemovalReserveBytes {
		t.Fatalf("shared quota: %+v %v", usage, err)
	}
	// A quota-full fixture is deliberately not a valid complete history. It tests
	// live combined admission, and never claims the inflated fixture is a reopenable history.
	ceiling := int64(256<<20) - usage.Library.ReservedBytes
	if _, err := s.db.Exec(`UPDATE collection_library_quota SET canonical_bytes=?`, ceiling); err != nil {
		t.Fatal(err)
	}
	savedQueryTestDenied(t, d, bearer, savedQueryTestCreate(t, 132), ErrCollectionCapacity)
	// Monotonic accounting prevents lowering this fixture. It remains isolated.
}
