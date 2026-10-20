package catalog

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Quota-full fixtures below increase logical historical counters monotonically
// to exact invariant-valid boundaries. They do not manufacture 131072 events and
// do not prove that this synthetic counter state has a matching replay history.
// The oracle targets LIVE request admission/atomic credit accounting separately
// from later format3 historical reconstruction acceptance.
func collectionAdditionalFullQuota(t *testing.T, s *Store, id CollectionID) CollectionQuotaUsage {
	t.Helper()
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, `UPDATE collection_library_quota SET policy_events=131072-reserved_policy_events,policy_receipts=131072-reserved_policy_receipts,canonical_bytes=268435456-reserved_bytes WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
	if id != "" {
		if _, err := s.db.ExecContext(ctx, `UPDATE collection_quota SET policy_events=8192-reserved_policy_events,policy_receipts=8192-reserved_policy_receipts,canonical_bytes=16777216-reserved_bytes WHERE collection_id=?`, id); err != nil {
			t.Fatal(err)
		}
	}
	usage, err := readCollectionQuota(ctx, s.db, id)
	if err != nil {
		t.Fatal(err)
	}
	if usage.Library.PolicyEvents+usage.Library.ReservedPolicyEvents != 131072 || usage.Library.CanonicalBytes+usage.Library.ReservedBytes != 256<<20 {
		t.Fatal("full library setup did not reach charged caps")
	}
	if usage.Collection != nil && (usage.Collection.PolicyEvents+usage.Collection.ReservedPolicyEvents != 8192 || usage.Collection.CanonicalBytes+usage.Collection.ReservedBytes != 16<<20) {
		t.Fatal("full collection setup did not reach charged caps")
	}
	return usage
}
func collectionAdditionalCounts(t *testing.T, s *Store) []int64 {
	t.Helper()
	var all []int64
	for _, table := range []string{"collection_events", "collection_policy_events", "collection_policy_results", "collection_order", "catalog_admin_events", "collection_grants"} {
		var n int64
		if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		all = append(all, n)
	}
	return all
}
func collectionAdditionalPolicyCost(t *testing.T, s *Store, id CollectionPolicyOperationID) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRowContext(context.Background(), `SELECT length(request)+length(receipt) FROM collection_policy_events WHERE administration='local_owner' AND operation_id=?`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCollectionOwnerSecurityRemovalAtLiveChargedCaps(t *testing.T) {
	for _, scope := range []string{"collection", "library_create"} {
		t.Run(scope, func(t *testing.T) {
			s, o, a, d, owner, _ := collectionTxStore(t)
			ctx := context.Background()
			var collection CollectionID
			grantID := CollectionGrantID("cgr_11111111111111111111111111111111")
			if scope == "collection" {
				create := collectionTxCreate(t, 101)
				collectionTxCall(t, d, owner, create)
				collection = create.CollectionID
				grantID = "cgr_22222222222222222222222222222222"
			}
			grant, err := readCollectionGrant(ctx, s.db, grantID)
			if err != nil {
				t.Fatal(err)
			}
			before := collectionAdditionalFullQuota(t, s, collection)
			counts := collectionAdditionalCounts(t, s)
			policy := collectionTxPolicy(102, collection, "security_remove", CollectionGrantChange{"remove", grantID, &grant.expected, nil})
			request := collectionTxOwner(t, a, policy)
			receipt, err := o.ApplyCollectionPolicy(ctx, request)
			if err != nil {
				t.Fatalf("prepaid live removal refused at charged caps: %v", err)
			}
			cost := collectionAdditionalPolicyCost(t, s, policy.ID)
			if cost <= 0 || cost > 3072 {
				t.Fatalf("security cost outside prepaid credit: %d", cost)
			}
			after, err := readCollectionQuota(ctx, s.db, collection)
			if err != nil {
				t.Fatal(err)
			}
			expected := before.Library
			expected.PolicyEvents++
			expected.PolicyReceipts++
			expected.CanonicalBytes += cost
			expected.ReservedPolicyEvents--
			expected.ReservedPolicyReceipts--
			expected.ReservedBytes -= 3072
			if after.Library != expected {
				t.Fatalf("live library removal delta: got=%+v want=%+v", after.Library, expected)
			}
			if before.Collection != nil {
				local := *before.Collection
				local.PolicyEvents++
				local.PolicyReceipts++
				local.CanonicalBytes += cost
				local.ReservedPolicyEvents--
				local.ReservedPolicyReceipts--
				local.ReservedBytes -= 3072
				if after.Collection == nil || *after.Collection != local {
					t.Fatalf("live local removal delta: %+v", after.Collection)
				}
			}
			removed, err := readCollectionGrant(ctx, s.db, grantID)
			if err != nil || !removed.expected.Removed {
				t.Fatalf("grant not tombstoned: %v", err)
			}
			currentCounts := collectionAdditionalCounts(t, s)
			if currentCounts[0] != counts[0] || currentCounts[1] != counts[1]+1 || currentCounts[2] != counts[2]+1 || currentCounts[3] != counts[3]+1 || currentCounts[4] != counts[4]+1 || currentCounts[5] != counts[5] {
				t.Fatalf("security removal fabricated content/identity rows: %v -> %v", counts, currentCounts)
			}
			retry, err := o.ApplyCollectionPolicy(ctx, request)
			if err != nil || retry != receipt {
				t.Fatalf("at-cap exact retry: %v", err)
			}
			repeated, err := readCollectionQuota(ctx, s.db, collection)
			if err != nil || !reflect.DeepEqual(repeated, after) || !reflect.DeepEqual(currentCounts, collectionAdditionalCounts(t, s)) {
				t.Fatal("exact retry charged history/reserve")
			}
			policy.ID = "cpo_00000000000000000000000000000067"
			if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, policy)); !errors.Is(err, ErrConflict) {
				t.Fatalf("second removal spent retired credit: %v", err)
			}
			// Reactivation is ordinary growth, even when it would restore the last admin.
			value := removed.expected.Value
			reactivate := collectionTxPolicy(104, collection, scope, CollectionGrantChange{"put", grantID, &removed.expected, &value})
			if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, reactivate)); !errors.Is(err, ErrCollectionCapacity) {
				t.Fatalf("ordinary activation escaped policy cap: %v", err)
			}
			unchanged, err := readCollectionQuota(ctx, s.db, collection)
			if err != nil || !reflect.DeepEqual(unchanged, after) || !reflect.DeepEqual(currentCounts, collectionAdditionalCounts(t, s)) {
				t.Fatal("refused activation partially changed state")
			}
		})
	}
}

func TestCollectionOrdinaryBatchUsesOneNetLiveQuotaPlan(t *testing.T) {
	s, o, a, d, owner, _ := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 110)
	created := collectionTxCall(t, d, owner, create)
	redundant := []CollectionGrantChange{}
	for i := 3; i <= 5; i++ {
		v := CollectionGrantValue{ownerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{"collections.read"}}
		redundant = append(redundant, CollectionGrantChange{"put", CollectionGrantID(fmt.Sprintf("cgr_%032x", i)), nil, &v})
	}
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(111, create.CollectionID, "collection", redundant...))); err != nil {
		t.Fatal(err)
	}
	changes := []CollectionGrantChange{}
	for _, change := range redundant {
		g, err := readCollectionGrant(ctx, s.db, change.ID)
		if err != nil {
			t.Fatal(err)
		}
		expected := g.expected
		changes = append(changes, CollectionGrantChange{"remove", g.id, &expected, nil})
	}
	value := CollectionGrantValue{ownerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{"collections.read"}}
	changes = append(changes, CollectionGrantChange{"put", "cgr_00000000000000000000000000000006", nil, &value})
	before := collectionAdditionalFullQuota(t, s, create.CollectionID)
	counts := collectionAdditionalCounts(t, s)
	op := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000070", create.CollectionID, created.ResultVersion, "grants", collectionTxRaw(t, EditCollectionGrants{changes})}
	// An artificial charge-before-release step would exceed both full caps.
	receipt := collectionTxCall(t, d, owner, op)
	var contentCost, policyCost int64
	if err := s.db.QueryRowContext(ctx, `SELECT length(request)+length(receipt) FROM collection_events WHERE actor_subject_id=? AND operation_id=?`, ownerSubject, op.ID).Scan(&contentCost); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT length(request)+length(receipt) FROM collection_policy_events WHERE administration='collection_service' AND namespace_subject_id=? AND operation_id=?`, ownerSubject, generatedCollectionPolicyID(op.ID)).Scan(&policyCost); err != nil {
		t.Fatal(err)
	}
	if contentCost+policyCost >= 6144 {
		t.Fatalf("fixture did not fit net released credit: %d", contentCost+policyCost)
	}
	after, err := readCollectionQuota(ctx, s.db, create.CollectionID)
	if err != nil {
		t.Fatal(err)
	}
	expected := before.Library
	expected.ContentEvents++
	expected.ContentReceipts++
	expected.PolicyEvents++
	expected.PolicyReceipts++
	expected.LifetimeGrants++
	expected.CanonicalBytes += contentCost + policyCost
	expected.ReservedPolicyEvents -= 2
	expected.ReservedPolicyReceipts -= 2
	expected.ReservedBytes -= 6144
	if after.Library != expected {
		t.Fatalf("combined actual/net credit delta: got=%+v want=%+v", after.Library, expected)
	}
	local := *before.Collection
	local.ContentEvents++
	local.ContentReceipts++
	local.PolicyEvents++
	local.PolicyReceipts++
	local.LifetimeGrants++
	local.CanonicalBytes += contentCost + policyCost
	local.ReservedPolicyEvents -= 2
	local.ReservedPolicyReceipts -= 2
	local.ReservedBytes -= 6144
	if after.Collection == nil || *after.Collection != local {
		t.Fatal("combined local delta differs")
	}
	now := collectionAdditionalCounts(t, s)
	if now[0] != counts[0]+1 || now[1] != counts[1]+1 || now[3] != counts[3]+1 {
		t.Fatalf("batch got per-grant events: %v -> %v", counts, now)
	}
	if retry := collectionTxCall(t, d, owner, op); retry != receipt {
		t.Fatal("batch retry receipt changed")
	}
	if !reflect.DeepEqual(now, collectionAdditionalCounts(t, s)) {
		t.Fatal("batch retry duplicated journal")
	}
}

func TestCollectionLastAdminUsesUnionOfDirectGrants(t *testing.T) {
	s, o, a, d, owner, reader := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 120)
	collectionTxCall(t, d, owner, create)
	changes := []CollectionGrantChange{}
	for i, action := range []string{"collections.admin", "collections.edit", "collections.read"} {
		v := CollectionGrantValue{readerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{action}}
		changes = append(changes, CollectionGrantChange{"put", CollectionGrantID(fmt.Sprintf("cgr_%032x", i+3)), nil, &v})
	}
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(121, create.CollectionID, "collection", changes...))); err != nil {
		t.Fatal(err)
	}
	creator, err := readCollectionGrant(ctx, s.db, "cgr_22222222222222222222222222222222")
	if err != nil {
		t.Fatal(err)
	}
	// No individual reader grant has all3 rights; their union must preserve admin.
	removeCreator := collectionTxPolicy(122, create.CollectionID, "collection", CollectionGrantChange{"remove", creator.id, &creator.expected, nil})
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, removeCreator)); err != nil {
		t.Fatalf("union administrator refused: %v", err)
	}
	definition, err := readCollectionDefinition(ctx, s.db, create.CollectionID)
	if err != nil {
		t.Fatal(err)
	}
	rename := CollectionRequest{1, testLibrary, "cop_0000000000000000000000000000007b", create.CollectionID, definition.version, "rename", collectionTxRaw(t, RenameCollection{"reader administers"})}
	current := collectionTxCall(t, d, reader, rename)
	adminGrant, err := readCollectionGrant(ctx, s.db, "cgr_00000000000000000000000000000003")
	if err != nil {
		t.Fatal(err)
	}
	// Reader can administer, but cannot remove the final enabled union admin.
	removal := CollectionRequest{1, testLibrary, "cop_0000000000000000000000000000007c", create.CollectionID, current.ResultVersion, "grants", collectionTxRaw(t, EditCollectionGrants{[]CollectionGrantChange{{"remove", adminGrant.id, &adminGrant.expected, nil}}})}
	before := collectionAdditionalCounts(t, s)
	collectionTxDenied(t, d, reader, removal, ErrConflict)
	if !reflect.DeepEqual(before, collectionAdditionalCounts(t, s)) {
		t.Fatal("last-admin refusal appended rows")
	}
}

func TestCollectionSameOperationIDHasTwoAuthenticatedNamespaces(t *testing.T) {
	s, o, a, d, owner, reader := collectionTxStore(t)
	ctx := context.Background()
	createRight := CollectionGrantValue{readerSubject, CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(132, "", "library_create", CollectionGrantChange{"put", "cgr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil, &createRight}))); err != nil {
		t.Fatal(err)
	}
	first := collectionTxCreate(t, 130)
	aReceipt := collectionTxCall(t, d, owner, first)
	second := CollectionRequest{1, testLibrary, first.ID, "col_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "", "create", collectionTxRaw(t, CreateCollection{"B project", "cgr_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", []NewCollectionEntry{{"cent_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", testAsset}}})}
	bReceipt := collectionTxCall(t, d, reader, second)
	if aReceipt.OperationID != bReceipt.OperationID || aReceipt.CollectionID == bReceipt.CollectionID {
		t.Fatal("fixture did not reuse only operation identity")
	}
	if retry := collectionTxCall(t, d, owner, first); retry != aReceipt {
		t.Fatal("A retry crossed namespace")
	}
	if retry := collectionTxCall(t, d, reader, second); retry != bReceipt {
		t.Fatal("B retry crossed namespace")
	}
	var contents, policies, namespaces int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM collection_events WHERE operation_id=?`, first.ID).Scan(&contents); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*),count(DISTINCT namespace_subject_id) FROM collection_policy_events WHERE administration='collection_service' AND operation_id=?`, generatedCollectionPolicyID(first.ID)).Scan(&policies, &namespaces); err != nil {
		t.Fatal(err)
	}
	if contents != 2 || policies != 2 || namespaces != 2 {
		t.Fatalf("actor-qualified storage collision: %d/%d/%d", contents, policies, namespaces)
	}
	aDef, err := readCollectionDefinition(ctx, s.db, first.CollectionID)
	if err != nil {
		t.Fatal(err)
	}
	bDef, err := readCollectionDefinition(ctx, s.db, second.CollectionID)
	if err != nil {
		t.Fatal(err)
	}
	if aDef.creator != ownerSubject || bDef.creator != readerSubject {
		t.Fatal("creator attribution crossed actors")
	}
	if err := checkCollectionOrder(ctx, s.db); err != nil {
		t.Fatal(err)
	}
	if err := validateCollectionHistory(ctx, s.db, testLibrary); err != nil {
		t.Fatal(err)
	}
}

func TestCollectionLiveCredentialAndSubjectDenials(t *testing.T) {
	for _, kind := range []string{"read-only-grant", "read-only-ceiling", "asset-credential", "service", "disabled"} {
		t.Run(kind, func(t *testing.T) {
			s, o, a, d, owner, reader := collectionTxStore(t)
			ctx := context.Background()
			create := collectionTxCreate(t, 140)
			created := collectionTxCall(t, d, owner, create)
			rename := CollectionRequest{1, testLibrary, "cop_0000000000000000000000000000008d", create.CollectionID, created.ResultVersion, "rename", collectionTxRaw(t, RenameCollection{"must remain unchanged"})}
			bearer := reader
			want := ErrNotFound
			switch kind {
			case "read-only-grant":
				value := CollectionGrantValue{readerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{"collections.read"}}
				if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(142, create.CollectionID, "collection", CollectionGrantChange{"put", "cgr_33333333333333333333333333333333", nil, &value}))); err != nil {
					t.Fatal(err)
				}
			case "read-only-ceiling":
				issued, _ := collectionTxIssue(t, o, a, "read-ceiling", ownerSubject, []string{"collections.read"})
				bearer = issued.Bearer
			case "asset-credential":
				issued, _ := issueTest(t, o, a, "asset-v1-credential", Scope{"asset", testAsset}, ownerSubject)
				bearer = issued.Bearer
				if _, err := s.Authenticate(ctx, testLibrary, bearer); err != nil {
					t.Fatalf("asset credential invalid before collection call: %v", err)
				}
			case "service":
				service := SubjectID("sub_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
				policyTest(t, o, a, "new-service", NewPolicyChange("create_subject", CreateSubject{service, SubjectValue{"service", "tool", true}}))
				issued, _ := collectionTxIssue(t, o, a, "service-v2-credential", service, collectionTxActions)
				bearer = issued.Bearer
				handle, err := s.Authenticate(ctx, testLibrary, bearer)
				if err != nil {
					t.Fatal(err)
				}
				identity, err := s.WhoAmI(ctx, handle)
				if err != nil || identity.Kind != "service" {
					t.Fatalf("service premise: %v", err)
				}
			case "disabled":
				value := CollectionGrantValue{readerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{"collections.edit", "collections.read"}}
				if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(142, create.CollectionID, "collection", CollectionGrantChange{"put", "cgr_33333333333333333333333333333333", nil, &value}))); err != nil {
					t.Fatal(err)
				}
				sub, err := readSubject(ctx, s.db, testLibrary, readerSubject)
				if err != nil {
					t.Fatal(err)
				}
				policyTest(t, o, a, "disable-reader", NewPolicyChange("update_subject", UpdateSubject{readerSubject, sub.Expected(), SubjectValue{"person", "reader", false}}))
				want = ErrUnauthenticated
			}
			before := collectionAdditionalCounts(t, s)
			collectionTxDenied(t, d, bearer, rename, want)
			if !reflect.DeepEqual(before, collectionAdditionalCounts(t, s)) {
				t.Fatal("denial changed durable history")
			}
			definition, err := readCollectionDefinition(ctx, s.db, create.CollectionID)
			if err != nil || definition.name != "Project" || definition.version != created.ResultVersion {
				t.Fatalf("denial changed definition: %+v %v", definition, err)
			}
			// Current enabled owner remains a positive control on the same target/token.
			collectionTxCall(t, d, owner, rename)
		})
	}
}

func TestCollectionEntryAndGrantABARetainFreshCAS(t *testing.T) {
	s, o, a, d, owner, _ := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 150)
	initial := collectionTxCall(t, d, owner, create)
	entry := CollectionEntryID("cent_11111111111111111111111111111111")
	remove := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000097", create.CollectionID, initial.ResultVersion, "entries", collectionTxRaw(t, EditCollectionEntries{[]CollectionEntryChange{{"remove", entry, testAsset}}})}
	removed := collectionTxCall(t, d, owner, remove)
	restore := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000098", create.CollectionID, removed.ResultVersion, "entries", collectionTxRaw(t, EditCollectionEntries{[]CollectionEntryChange{{"reactivate", entry, testAsset}}})}
	restored := collectionTxCall(t, d, owner, restore)
	if restored.ResultVersion == initial.ResultVersion || restored.ResultVersion == removed.ResultVersion {
		t.Fatal("definition ABA reused historical token")
	}
	stale := remove
	stale.ID = "cop_00000000000000000000000000000099"
	before := collectionAdditionalCounts(t, s)
	collectionTxDenied(t, d, owner, stale, ErrConflict)
	if !reflect.DeepEqual(before, collectionAdditionalCounts(t, s)) {
		t.Fatal("stale entry CAS changed state")
	}
	var target continuity.AssetID
	var tombstone bool
	if err := s.db.QueryRowContext(ctx, `SELECT asset_id,removed FROM collection_entries WHERE entry_id=?`, entry).Scan(&target, &tombstone); err != nil || target != testAsset || tombstone {
		t.Fatalf("entry identity/reactivation: %s %v %v", target, tombstone, err)
	}

	value := CollectionGrantValue{readerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{"collections.read"}}
	grantID := CollectionGrantID("cgr_33333333333333333333333333333333")
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(154, create.CollectionID, "collection", CollectionGrantChange{"put", grantID, nil, &value}))); err != nil {
		t.Fatal(err)
	}
	original, err := readCollectionGrant(ctx, s.db, grantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(155, create.CollectionID, "security_remove", CollectionGrantChange{"remove", grantID, &original.expected, nil}))); err != nil {
		t.Fatal(err)
	}
	tombstoned, err := readCollectionGrant(ctx, s.db, grantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(156, create.CollectionID, "collection", CollectionGrantChange{"put", grantID, &tombstoned.expected, &value}))); err != nil {
		t.Fatal(err)
	}
	active, err := readCollectionGrant(ctx, s.db, grantID)
	if err != nil {
		t.Fatal(err)
	}
	if active.expected.Removed || active.expected.Version == original.expected.Version || active.expected.Version == tombstoned.expected.Version || !reflect.DeepEqual(active.expected.Value, original.expected.Value) {
		t.Fatal("grant ABA premise/version failed")
	}
	before = collectionAdditionalCounts(t, s)
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(157, create.CollectionID, "security_remove", CollectionGrantChange{"remove", grantID, &original.expected, nil}))); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale full-value ABA accepted: %v", err)
	}
	if !reflect.DeepEqual(before, collectionAdditionalCounts(t, s)) {
		t.Fatal("stale grant CAS changed journal")
	}
}

func TestCollectionMigrationRefusalAndBarrierUncertainty(t *testing.T) {
	ctx := context.Background()
	s, _, _, d, _, _ := preferencesTestStore(t)
	path := s.path
	if err := d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := continuity.LibraryID("lib_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err := UpgradeCollections(ctx, path, wrong); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong-library upgrade accepted: %v", err)
	}
	stillFour, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if stillFour.version != 4 {
		t.Fatal("refused upgrade changed version")
	}
	if err := stillFour.Close(); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := UpgradeCollections(canceled, path, testLibrary); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled migration: %v", err)
	}
	sentinel := errors.New("synthetic directory barrier")
	if err := upgradeCatalogVersion(ctx, path, testLibrary, collectionsSchemaVersion, func(string) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("barrier failure not reported: %v", err)
	}
	five, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatalf("committed migration could not be inspected: %v", err)
	}
	if five.version != 5 {
		t.Fatal("uncertain barrier did not leave committed schema")
	}
	if err := five.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeCollections(ctx, path, testLibrary); err != nil {
		t.Fatalf("exact upgrade recovery: %v", err)
	}
	if err := UpgradePreferences(ctx, path, testLibrary); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("schema5 downgrade accepted: %v", err)
	}
	old, err := CreateAuthority(ctx, filepath.Join(t.TempDir(), "schema3.sqlite"), testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	oldPath := old.path
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeCollections(ctx, oldPath, testLibrary); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("implicit3-to5 chain accepted: %v", err)
	}
}
