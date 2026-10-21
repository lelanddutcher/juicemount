package catalog

import (
	"bytes"
	"context"
	"errors"
	"github.com/lelanddutcher/juicemount/internal/continuity"
	"testing"
)

func TestSavedQueryAnchorLossRetryRepairAndCurrentRights(t *testing.T) {
	s, o, a, d, owner, reader := savedQueryTestStore(t)
	ctx := context.Background()
	create := savedQueryTestCreate(t, 150, testAsset)
	created := savedQueryTestCall(t, d, owner, create)
	grant := collectionReadGrant(t, o, a, 151, create.CollectionID, readerSubject, []string{"collections.edit", "collections.read"})
	// The reader sees testAsset, but not otherAsset. New create and replacement
	// must reject the proposed anchor before retaining any part of a transaction.
	value := CollectionGrantValue{readerSubject, CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(152, "", "library_create", CollectionGrantChange{"put", "cgr_00000000000000000000000000000098", nil, &value}))); err != nil {
		t.Fatal(err)
	}
	savedQueryTestDenied(t, d, reader, savedQueryTestCreate(t, 153, otherAsset), ErrNotFound)
	hidden := SavedQueryDefinition{1, "ranked_assets", "hidden", []continuity.AssetID{otherAsset}}
	savedQueryTestDenied(t, d, reader, savedQueryTestEdit(t, 154, create.CollectionID, created.ResultVersion, "replace_definition", ReplaceSavedQueryDefinition{hidden}), ErrNotFound)
	replacement := SavedQueryDefinition{1, "ranked_assets", "changed", []continuity.AssetID{testAsset}}
	replace := savedQueryTestEdit(t, 155, create.CollectionID, created.ResultVersion, "replace_definition", ReplaceSavedQueryDefinition{replacement})
	replaced := savedQueryTestCall(t, d, reader, replace)
	assetGrant, err := readGrant(ctx, s.db, testLibrary, "tx-reader-assets")
	if err != nil {
		t.Fatal(err)
	}
	policyTest(t, o, a, "saved-remove-anchor", NewPolicyChange("remove_grant", RemoveGrant{assetGrant.ID, assetGrant.Expected()}))
	if got := savedQueryTestCall(t, d, reader, replace); got != replaced {
		t.Fatal("lost reply retry required historical anchor")
	}
	ownerGrant, err := readGrant(ctx, s.db, testLibrary, "tx-owner-assets")
	if err != nil {
		t.Fatal(err)
	}
	policyTest(t, o, a, "saved-remove-owner-anchor", NewPolicyChange("remove_grant", RemoveGrant{ownerGrant.ID, ownerGrant.Expected()}))
	if got := savedQueryTestCall(t, d, owner, create); got != created {
		t.Fatal("create retry required historical anchor")
	}
	blank := SavedQueryDefinition{1, "ranked_assets", "", []continuity.AssetID{}}
	repaired := savedQueryTestCall(t, d, reader, savedQueryTestEdit(t, 156, create.CollectionID, replaced.ResultVersion, "replace_definition", ReplaceSavedQueryDefinition{blank}))
	if repaired.ResultVersion == replaced.ResultVersion {
		t.Fatal("repair did not advance version")
	}
	// Historical anchors require existence, not final discover rights.
	if err := checkSavedQueryState(ctx, s.db, testLibrary); err != nil {
		t.Fatalf("history reauthorized final anchor rights: %v", err)
	}
	for _, actions := range [][]string{{"collections.read"}, {"collections.edit"}} {
		prior, err := readCollectionGrant(ctx, s.db, grant)
		if err != nil {
			t.Fatal(err)
		}
		changed := prior.expected.Value
		changed.Actions = actions
		id := 157
		if actions[0] == "collections.edit" {
			id = 158
		}
		if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(id, create.CollectionID, "collection", CollectionGrantChange{"put", grant, &prior.expected, &changed}))); err != nil {
			t.Fatal(err)
		}
		savedQueryTestDenied(t, d, reader, replace, ErrNotFound)
	}
}

func TestSavedQueryPublicationRightsPrecedeGenerationDrift(t *testing.T) {
	s, o, a, d, owner, _ := savedQueryTestStore(t)
	ctx := context.Background()
	create := savedQueryTestCreate(t, 170)
	created := savedQueryTestCall(t, d, owner, create)
	d.beforeFence = func(context.Context) error {
		d.beforeFence = nil
		grant, err := readCollectionGrant(ctx, s.db, "cgr_000000000000000000000000000000aa")
		if err != nil {
			return err
		}
		_, err = o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, collectionTxPolicy(172, create.CollectionID, "security_remove", CollectionGrantChange{"remove", grant.id, &grant.expected, nil})))
		return err
	}
	op := savedQueryTestEdit(t, 171, create.CollectionID, created.ResultVersion, "rename", RenameCollection{"Committed without publication"})
	var out bytes.Buffer
	result, err := d.MutateSavedQuery(ctx, owner, op, &out)
	if !errors.Is(err, ErrNotFound) || result.Started || out.Len() != 0 {
		t.Fatalf("right loss must be not_found: %+v %v", result, err)
	}
	def, err := readSavedQueryDefinition(ctx, s.db, create.CollectionID)
	if err != nil || def.name != "Committed without publication" {
		t.Fatalf("publication failure is not rollback: %+v %v", def, err)
	}
	savedQueryTestDenied(t, d, owner, op, ErrNotFound)
}

func TestSavedQueryManualFamilyDirectoryAndOwnerPolicy(t *testing.T) {
	s, o, a, d, owner, reader := savedQueryTestStore(t)
	query := savedQueryTestCreate(t, 180)
	savedQueryTestCall(t, d, owner, query)
	manual := collectionTxCreate(t, 181)
	created := collectionTxCall(t, d, owner, manual)
	collectionReadGrant(t, o, a, 182, query.CollectionID, readerSubject, []string{"collections.edit", "collections.read"})
	// The actual service performs initial construction AND its fresh directory
	// fence; both must exclude established query identities before row counts.
	page := collectionReadEnvelope(t, collectionReadCall(t, d, owner, collectionReadRequest("directory", "")))
	if page.MatchedCount != 1 || len(page.Items) != 1 || !bytes.Contains(page.Items[0], []byte(manual.CollectionID)) || bytes.Contains(page.Items[0], []byte(query.CollectionID)) {
		t.Fatalf("manual family directory: %+v", page)
	}
	page = collectionReadEnvelope(t, collectionReadCall(t, d, reader, collectionReadRequest("directory", "")))
	if page.MatchedCount != 0 || len(page.Items) != 0 {
		t.Fatal("query-only rights poisoned manual list")
	}
	for _, kind := range []string{"open", "members", "history", "access"} {
		collectionReadDenied(t, d, owner, collectionReadRequest(kind, query.CollectionID), "not_found")
	}
	savedQueryTestDenied(t, d, owner, savedQueryTestEdit(t, 183, manual.CollectionID, created.ResultVersion, "rename", RenameCollection{"wrong"}), ErrNotFound)
	def, err := readSavedQueryDefinition(context.Background(), s.db, query.CollectionID)
	if err != nil {
		t.Fatal(err)
	}
	wrong := CollectionRequest{1, testLibrary, "cop_000000000000000000000000000000b8", query.CollectionID, def.version, "rename", collectionTxRaw(t, RenameCollection{"wrong"})}
	collectionTxDenied(t, d, owner, wrong, ErrNotFound)
	// Same actor/operation ID across families is one namespace, not a second write.
	collision := savedQueryTestCreate(t, 185)
	collision.ID = manual.ID
	savedQueryTestDenied(t, d, owner, collision, ErrChangedRequest)
	if err := checkSavedQueryState(context.Background(), s.db, testLibrary); err != nil {
		t.Fatal(err)
	}
}

func TestSavedQueryGeneratedGrantVersionAndRetryWithoutAdmin(t *testing.T) {
	s, _, _, d, owner, reader := savedQueryTestStore(t)
	ctx := context.Background()
	create := savedQueryTestCreate(t, 260)
	created := savedQueryTestCall(t, d, owner, create)
	shared := CollectionGrantValue{readerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{"collections.admin", "collections.edit", "collections.read"}}
	share := savedQueryTestEdit(t, 261, create.CollectionID, created.ResultVersion, "grants", EditCollectionGrants{[]CollectionGrantChange{{"put", "cgr_00000000000000000000000000000105", nil, &shared}}})
	sharedReceipt := savedQueryTestCall(t, d, owner, share)
	if sharedReceipt.ResultVersion == created.ResultVersion {
		t.Fatal("grant change did not advance full cv")
	}
	record, _, child, err := readSavedQueryContentRecord(ctx, s.db, testLibrary, 2)
	if err != nil || child == 0 {
		t.Fatalf("missing generated child: %v", err)
	}
	policy, _, err := readCollectionPolicyRecord(ctx, s.db, testLibrary, child)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindSavedQueryChild(record, &policy); err != nil {
		t.Fatal(err)
	}
	grant, err := readCollectionGrant(ctx, s.db, "cgr_00000000000000000000000000000104")
	if err != nil {
		t.Fatal(err)
	}
	value := grant.expected.Value
	value.Actions = []string{"collections.edit", "collections.read"}
	downgrade := savedQueryTestEdit(t, 262, create.CollectionID, sharedReceipt.ResultVersion, "grants", EditCollectionGrants{[]CollectionGrantChange{{"put", grant.id, &grant.expected, &value}}})
	current := savedQueryTestCall(t, d, owner, downgrade)
	if got := savedQueryTestCall(t, d, owner, share); got != sharedReceipt {
		t.Fatal("exact grant retry required former admin")
	}
	savedQueryTestDenied(t, d, owner, savedQueryTestEdit(t, 263, create.CollectionID, current.ResultVersion, "archive", SetCollectionArchived{true}), ErrNotFound)
	archived := savedQueryTestCall(t, d, reader, savedQueryTestEdit(t, 264, create.CollectionID, current.ResultVersion, "archive", SetCollectionArchived{true}))
	if archived.ResultVersion == current.ResultVersion {
		t.Fatal("reader administrator could not change full cv")
	}
	if err := checkSavedQueryState(ctx, s.db, testLibrary); err != nil {
		t.Fatal(err)
	}
}
