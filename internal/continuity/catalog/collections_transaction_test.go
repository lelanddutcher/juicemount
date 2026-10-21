package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func collectionTxRaw(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func collectionTxOwner(t *testing.T, a AuthorityStatus, p CollectionPolicyRequest) CollectionOwnerRequest {
	return CollectionOwnerRequest{1, testLibrary, string(p.ID), a.Incarnation, "apply_collection_policy", collectionTxRaw(t, ApplyCollectionPolicyRequest{p})}
}
func collectionTxPolicy(id int, collection CollectionID, category string, changes ...CollectionGrantChange) CollectionPolicyRequest {
	return CollectionPolicyRequest{2, testLibrary, CollectionPolicyOperationID(fmt.Sprintf("cpo_%032x", id)), category, collection, "", "", changes}
}
func collectionTxIssue(t *testing.T, o *LocalOwnerService, a AuthorityStatus, id string, subject SubjectID, actions []string) (IssuanceResultV2, AdminRequestV2) {
	t.Helper()
	op := AdminRequestV2{2, testLibrary, id, a.Incarnation, "issue_credential", collectionTxRaw(t, IssueCredentialRequestV2{subject, Scope{"library", ""}, actions, o.store.now().Unix() + 3600})}
	r, err := o.IssueCredentialV2(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Bearer) != 102 {
		t.Fatal("new credential secret missing")
	}
	return r, op
}

var collectionTxActions = []string{"assets.discover", "collections.admin", "collections.create", "collections.edit", "collections.read"}

func collectionTxStore(t *testing.T) (*Store, *LocalOwnerService, AuthorityStatus, *DiscoveryService, []byte, []byte) {
	t.Helper()
	s, err := CreateCollections(context.Background(), filepath.Join(t.TempDir(), "collections.sqlite"), testLibrary)
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
func collectionTxCreate(t *testing.T, id int) CollectionRequest {
	return CollectionRequest{1, testLibrary, CollectionOperationID(fmt.Sprintf("cop_%032x", id)), CollectionID(fmt.Sprintf("col_%032x", id)), "", "create", collectionTxRaw(t, CreateCollection{"Project", "cgr_22222222222222222222222222222222", []NewCollectionEntry{{"cent_11111111111111111111111111111111", testAsset}, {"cent_22222222222222222222222222222222", otherAsset}}})}
}
func collectionTxCall(t *testing.T, d *DiscoveryService, bearer []byte, op CollectionRequest) CollectionReceipt {
	t.Helper()
	var out bytes.Buffer
	result, err := d.MutateCollection(context.Background(), bearer, op, &out)
	if err != nil || !result.Started {
		t.Fatalf("collection mutation: %v %+v", err, result)
	}
	r, err := DecodeCollectionReceipt(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func collectionTxDenied(t *testing.T, d *DiscoveryService, bearer []byte, op CollectionRequest, want error) {
	t.Helper()
	var out bytes.Buffer
	r, err := d.MutateCollection(context.Background(), bearer, op, &out)
	if !errors.Is(err, want) || r.Started || out.Len() != 0 {
		t.Fatalf("refused mutation wrote/disclosed: %v %+v %d", err, r, out.Len())
	}
}
func TestCollectionTransactionCreateShareHiddenRetentionAndRetry(t *testing.T) {
	s, _, _, d, owner, reader := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 10)
	created := collectionTxCall(t, d, owner, create)
	if got := collectionTxCall(t, d, owner, create); got != created {
		t.Fatal("exact retry changed receipt")
	}
	shared := CollectionGrantValue{readerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{"collections.edit", "collections.read"}}
	share := CollectionRequest{1, testLibrary, "cop_0000000000000000000000000000000b", create.CollectionID, created.ResultVersion, "grants", collectionTxRaw(t, EditCollectionGrants{[]CollectionGrantChange{{"put", "cgr_33333333333333333333333333333333", nil, &shared}}})}
	current := collectionTxCall(t, d, owner, share)
	hiddenRemove := CollectionRequest{1, testLibrary, "cop_0000000000000000000000000000000c", create.CollectionID, current.ResultVersion, "entries", collectionTxRaw(t, EditCollectionEntries{[]CollectionEntryChange{{"remove", "cent_22222222222222222222222222222222", otherAsset}}})}
	collectionTxDenied(t, d, reader, hiddenRemove, ErrNotFound)
	rename := CollectionRequest{1, testLibrary, "cop_0000000000000000000000000000000d", create.CollectionID, current.ResultVersion, "rename", collectionTxRaw(t, RenameCollection{"Reader corrected"})}
	renamed := collectionTxCall(t, d, reader, rename)
	var removed bool
	if err := s.db.QueryRowContext(ctx, `SELECT removed FROM collection_entries WHERE entry_id='cent_22222222222222222222222222222222'`).Scan(&removed); err != nil || removed {
		t.Fatalf("hidden member changed: %v", err)
	}
	collectionTxDenied(t, d, owner, rename, ErrConflict) // Different actor namespace, stale definition CAS.
	if got := collectionTxCall(t, d, owner, create); got != created || got.ResultVersion == renamed.ResultVersion {
		t.Fatal("historical retry used current token")
	}
	share.ID = "cop_0000000000000000000000000000000e"
	share.ExpectedVersion = renamed.ResultVersion
	collectionTxDenied(t, d, reader, share, ErrNotFound) // read+edit grants do not imply admin.
	if err := checkCollectionOrder(ctx, s.db); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenContext(ctx, s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	def, err := readCollectionDefinition(ctx, reopened.db, create.CollectionID)
	if err != nil || def.name != "Reader corrected" || def.creator != ownerSubject {
		t.Fatalf("restart identity: %+v %v", def, err)
	}
	var out bytes.Buffer
	limits := Limits{MaxBytes: 1 << 20, MaxEvents: 100, MaxProjectionRows: 1000}
	if err := reopened.Export(ctx, testLibrary, &out, limits); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "portable.sqlite")
	if err := Restore(ctx, target, testLibrary, bytes.NewReader(out.Bytes()), limits); err != nil {
		t.Fatal(err)
	}
	imported, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Close()
	importedDef, err := readCollectionDefinition(ctx, imported.db, create.CollectionID)
	if err != nil || importedDef != def {
		t.Fatalf("portable collection identity/name/version changed: %+v %v", importedDef, err)
	}
	var retained int
	if err := imported.db.QueryRowContext(ctx, "SELECT count(*) FROM collection_entries WHERE collection_id=? AND removed=0", create.CollectionID).Scan(&retained); err != nil || retained != 2 {
		t.Fatalf("portable hidden membership: %d %v", retained, err)
	}
	var request, receipt []byte
	if err := imported.db.QueryRowContext(ctx, "SELECT request,receipt FROM collection_events WHERE actor_subject_id=? AND operation_id=?", ownerSubject, create.ID).Scan(&request, &receipt); err != nil {
		t.Fatal(err)
	}
	expectedRequest, _, _ := CanonicalCollectionRequest(create)
	expectedReceipt, _ := CanonicalCollectionReceipt(created)
	if !bytes.Equal(request, expectedRequest) || !bytes.Equal(receipt, expectedReceipt) {
		t.Fatal("portable original create retry state changed")
	}
	var again bytes.Buffer
	if err := imported.Export(ctx, testLibrary, &again, limits); err != nil || !bytes.Equal(again.Bytes(), out.Bytes()) {
		t.Fatalf("portable exact roundtrip: %v", err)
	}
}
func TestCollectionTransactionRollbackIncludesGeneratedPolicy(t *testing.T) {
	s, _, _, d, owner, _ := collectionTxStore(t)
	ctx := context.Background()
	snapshot := func() []int64 {
		var values []int64
		for _, table := range []string{"collection_events", "collection_policy_events", "collection_policy_results", "collection_grants", "collection_identities", "collection_entries", "collection_order"} {
			var n int64
			if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
				t.Fatal(err)
			}
			values = append(values, n)
		}
		sub, err := readSubject(ctx, s.db, testLibrary, ownerSubject)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, sub.AuthorizationGeneration)
		usage, err := readCollectionQuota(ctx, s.db, "")
		if err != nil {
			t.Fatal(err)
		}
		fields := quotaFields(usage.Library)
		return append(values, fields[:]...)
	}
	before := snapshot()
	for _, statement := range []string{`CREATE TEMP TABLE collection_fault_parent(id INTEGER PRIMARY KEY)`, `CREATE TEMP TABLE collection_fault_child(id INTEGER REFERENCES collection_fault_parent(id) DEFERRABLE INITIALLY DEFERRED)`, `CREATE TEMP TRIGGER collection_fault AFTER INSERT ON main.collection_events BEGIN INSERT INTO collection_fault_child VALUES(1); END`} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	r, err := d.MutateCollection(ctx, owner, collectionTxCreate(t, 20), &out)
	if err == nil || !strings.Contains(err.Error(), "commit outcome uncertain") || r.Started || out.Len() != 0 {
		t.Fatalf("late failure result: %v %+v", err, r)
	}
	if after := snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("partial compound transaction: before=%v after=%v", before, after)
	}
	if _, err := s.db.ExecContext(ctx, `DROP TRIGGER collection_fault`); err != nil {
		t.Fatal(err)
	}
	collectionTxCall(t, d, owner, collectionTxCreate(t, 20))
}
func TestCollectionMutationPostEncodingFence(t *testing.T) {
	s, o, a, d, owner, _ := collectionTxStore(t)
	create := collectionTxCreate(t, 30)
	created := collectionTxCall(t, d, owner, create)
	entered, release := make(chan struct{}), make(chan struct{})
	d.beforeFence = func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	rename := CollectionRequest{1, testLibrary, "cop_0000000000000000000000000000001f", create.CollectionID, created.ResultVersion, "rename", collectionTxRaw(t, RenameCollection{"committed before revocation"})}
	type result struct {
		r     DisclosureResult
		err   error
		bytes int
	}
	done := make(chan result, 1)
	go func() {
		var out bytes.Buffer
		r, err := d.MutateCollection(context.Background(), owner, rename, &out)
		done <- result{r, err, out.Len()}
	}()
	select {
	case <-entered:
	case got := <-done:
		t.Fatalf("mutation returned before fence barrier: %+v", got)
	}
	grant, err := readCollectionGrant(context.Background(), s.db, "cgr_22222222222222222222222222222222")
	if err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	policy := collectionTxPolicy(32, create.CollectionID, "security_remove", CollectionGrantChange{"remove", grant.id, &grant.expected, nil})
	_, removeErr := o.ApplyCollectionPolicy(context.Background(), collectionTxOwner(t, a, policy))
	close(release)
	got := <-done
	if removeErr != nil {
		t.Fatal(removeErr)
	}
	if got.err == nil || got.r.Started || got.bytes != 0 {
		t.Fatalf("revoked encoded receipt disclosed: %+v", got)
	}
	def, err := readCollectionDefinition(context.Background(), s.db, create.CollectionID)
	if err != nil || def.name != "committed before revocation" {
		t.Fatalf("accepted operation incorrectly rolled back: %+v %v", def, err)
	}
}
