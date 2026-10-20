package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

var discoveryAllActions = []string{"activity.record", "assets.discover", "locations.read", "relationships.read", "revisions.read"}

func discoveryServiceTest(t *testing.T, s *Store) *DiscoveryService {
	t.Helper()
	d, err := NewDiscoveryService(context.Background(), s, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return d
}
func discoveryIssueTest(t *testing.T, o *LocalOwnerService, a AuthorityStatus, id string, subject SubjectID, scope Scope, actions []string) []byte {
	t.Helper()
	r, err := o.IssueCredential(context.Background(), adminTestRequest("issue-"+id, a.Incarnation, "issue_credential", IssueCredentialRequest{subject, scope, actions, o.store.now().Unix() + 3600}))
	if err != nil {
		t.Fatal(err)
	}
	return r.Bearer
}
func discoveryGrantTest(t *testing.T, o *LocalOwnerService, a AuthorityStatus, id string, subject SubjectID, scope Scope, actions []string) {
	t.Helper()
	policyTest(t, o, a, "put-"+id, NewPolicyChange("put_grant", PutGrant{id, nil, GrantValue{subject, scope, actions}}))
}
func discoveryRequestTest(kind string, asset continuity.AssetID) DiscoveryRequest {
	r := DiscoveryRequest{Version: 1, Kind: kind, LibraryID: testLibrary, AssetID: asset}
	switch kind {
	case "search", "history", "revisions", "relationships", "locations":
		r.PageSize = 100
	}
	return r
}
func discoveryQueryTest(t *testing.T, d *DiscoveryService, bearer []byte, r DiscoveryRequest) []byte {
	t.Helper()
	var b bytes.Buffer
	result, err := d.Query(context.Background(), bearer, r, &b)
	if err != nil || !result.Started {
		t.Fatalf("query %s: started=%v err=%v", r.Kind, result.Started, err)
	}
	return b.Bytes()
}
func discoveryItemsTest(t *testing.T, b []byte) discoveryCollection {
	t.Helper()
	var c discoveryCollection
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if c.Version != 1 || c.CountScope != "frozen_window" || c.Sort != "discovery1" {
		t.Fatalf("collection envelope: %s", b)
	}
	return c
}
func discoveryNoFieldsTest(t *testing.T, b []byte) {
	t.Helper()
	for _, key := range []string{"sequence", "operation_id", "payload_sha256", "receipt", "updated_sequence", "authorization_generation", "selector", "subject_id", "incarnation"} {
		if bytes.Contains(b, []byte(`"`+key+`"`)) {
			t.Fatalf("internal field %s in %s", key, b)
		}
	}
}

func TestDiscoveryScopeCurrentGrantsAndProjection(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	policyTest(t, o, a, "reader", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}))
	discoveryGrantTest(t, o, a, "reader-discover", readerSubject, Scope{"library", ""}, []string{"assets.discover"})
	bearer := discoveryIssueTest(t, o, a, "asset-reader", readerSubject, Scope{"asset", testAsset}, discoveryAllActions)
	r := discoveryRequestTest("search", "")
	r.ContextAssetIDs = []continuity.AssetID{otherAsset, testAsset}
	b := discoveryQueryTest(t, d, bearer, r)
	c := discoveryItemsTest(t, b)
	if len(c.Items) != 1 || !c.Complete || c.MatchedCount != 1 || !bytes.Contains(c.Items[0], []byte(testAsset)) || bytes.Contains(b, []byte(otherAsset)) {
		t.Fatalf("credential scope widened: %s", b)
	}
	var asset struct {
		Asset discoveryAsset `json:"asset"`
	}
	if err := json.Unmarshal(discoveryQueryTest(t, d, bearer, discoveryRequestTest("asset", testAsset)), &asset); err != nil {
		t.Fatal(err)
	}
	if asset.Asset.Head != nil {
		t.Fatal("head leaked without revision grant")
	}
	var counts discoveryCounts
	if err := json.Unmarshal(discoveryQueryTest(t, d, bearer, discoveryRequestTest("counts", testAsset)), &counts); err != nil {
		t.Fatal(err)
	}
	if counts.Revisions != nil || counts.Relationships != nil || counts.Locations != nil {
		t.Fatal("missing-action counts became zero")
	}
	for _, id := range []continuity.AssetID{otherAsset, "asset_99999999999999999999999999999999"} {
		var out bytes.Buffer
		result, err := d.Query(context.Background(), bearer, discoveryRequestTest("asset", id), &out)
		if DiscoveryErrorCode(err) != "not_found" || result.Started || out.Len() != 0 {
			t.Fatalf("absent/hidden: %v %s", err, out.String())
		}
	}
	g, err := readGrant(context.Background(), s.db, testLibrary, "reader-discover")
	if err != nil {
		t.Fatal(err)
	}
	policyTest(t, o, a, "remove-reader", NewPolicyChange("remove_grant", RemoveGrant{g.ID, g.Expected()}))
	// Causal control removes only discoveryRights.allows' current-grant predicate.
	var out bytes.Buffer
	result, err := d.Query(context.Background(), bearer, discoveryRequestTest("asset", testAsset), &out)
	if DiscoveryErrorCode(err) != "not_found" || result.Started || out.Len() != 0 {
		t.Fatalf("removed current grant disclosed asset: %v %s", err, out.String())
	}
	discoveryNoFieldsTest(t, b)
}

func TestDiscoveryRelationshipsLocationsAnchorsAndCounts(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	policyTest(t, o, a, "reader", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}))
	actions := []string{"assets.discover", "locations.read", "relationships.read"}
	discoveryGrantTest(t, o, a, "reader-a", readerSubject, Scope{"asset", testAsset}, actions)
	bearer := discoveryIssueTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
	edge := RelationshipValue{SourceAssetID: testAsset, SourceRevision: rootRevision, TargetAssetID: otherAsset, Predicate: "derived", Provenance: Claimed, Source: "declared"}
	loc := LocationValue{AssetID: testAsset, Revision: rootRevision, Source: "declared", Locator: "never-open://private/path"}
	applyTest(t, s, testOperation("custody", Change{PutRelationship: &PutRelationship{"edge", nil, edge}}, Change{PutLocation: &PutLocation{"loc", nil, loc}}))
	c := discoveryItemsTest(t, discoveryQueryTest(t, d, bearer, discoveryRequestTest("relationships", testAsset)))
	if len(c.Items) != 0 {
		t.Fatal("hidden endpoint edge disclosed")
	}
	discoveryGrantTest(t, o, a, "reader-b", readerSubject, Scope{"asset", otherAsset}, actions)
	b := discoveryQueryTest(t, d, bearer, discoveryRequestTest("relationships", testAsset))
	c = discoveryItemsTest(t, b)
	if len(c.Items) != 1 || bytes.Contains(b, []byte(rootRevision)) {
		t.Fatalf("edge anchors not redacted: %s", b)
	}
	var got discoveryRelationship
	if err := json.Unmarshal(c.Items[0], &got); err != nil {
		t.Fatal(err)
	}
	if got.SourceRevision != nil || got.TargetRevision != nil {
		t.Fatal("revision anchors leaked")
	}
	l := discoveryItemsTest(t, discoveryQueryTest(t, d, bearer, discoveryRequestTest("locations", testAsset)))
	if len(l.Items) != 1 || !bytes.Contains(l.Items[0], []byte(loc.Locator)) || bytes.Contains(l.Items[0], []byte(rootRevision)) {
		t.Fatalf("literal location projection: %s", l.Items)
	}
	var counts discoveryCounts
	_ = json.Unmarshal(discoveryQueryTest(t, d, bearer, discoveryRequestTest("counts", testAsset)), &counts)
	if counts.Relationships == nil || *counts.Relationships != 1 || counts.Locations == nil || *counts.Locations != 1 || counts.Revisions != nil {
		t.Fatalf("authorized counts: %+v", counts)
	}
	discoveryGrantTest(t, o, a, "reader-revision", readerSubject, Scope{"asset", testAsset}, []string{"revisions.read"})
	b = discoveryQueryTest(t, d, bearer, discoveryRequestTest("relationships", testAsset))
	if !bytes.Contains(b, []byte(rootRevision)) {
		t.Fatal("authorized source anchor missing")
	}
	revs := discoveryItemsTest(t, discoveryQueryTest(t, d, bearer, discoveryRequestTest("revisions", testAsset)))
	if len(revs.Items) != 1 || !bytes.Contains(revs.Items[0], []byte(testSHA)) {
		t.Fatal("revision DTO missing content")
	}
	discoveryNoFieldsTest(t, b)
}

func TestDiscoveryHistoryOldEndpointAndMixedBatch(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	const hidden continuity.AssetID = "asset_33333333333333333333333333333333"
	applyTest(t, s, testOperation("hidden", Change{CreateAsset: &CreateAsset{hidden, "hidden label"}}))
	old := RelationshipValue{SourceAssetID: testAsset, TargetAssetID: hidden, Predicate: "before", Provenance: Claimed, Source: "old-source"}
	applyTest(t, s, testOperation("old-edge", Change{PutRelationship: &PutRelationship{"corrected", nil, old}}))
	state := getRelationship(t, s, "corrected")
	next := old
	next.TargetAssetID = otherAsset
	next.Predicate = "after"
	applyTest(t, s, testOperation("mixed-private-id", Change{PutRelationship: &PutRelationship{"corrected", relationshipState(state), next}}, Change{RenameAsset: &RenameAsset{testAsset, "original", "new label"}}, Change{RenameAsset: &RenameAsset{hidden, "hidden label", "hidden replacement"}}))
	discoveryGrantTest(t, o, a, "owner-a", ownerSubject, Scope{"asset", testAsset}, discoveryAllActions)
	discoveryGrantTest(t, o, a, "owner-b", ownerSubject, Scope{"asset", otherAsset}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	b := discoveryQueryTest(t, d, bearer, discoveryRequestTest("history", testAsset))
	c := discoveryItemsTest(t, b)
	if bytes.Contains(b, []byte("corrected")) || bytes.Contains(b, []byte(hidden)) || bytes.Contains(b, []byte("mixed-private-id")) {
		t.Fatalf("hidden OLD endpoint history disclosed: %s", b)
	}
	if len(c.Items) != 4 {
		t.Fatalf("mixed history changed filtered cardinality: %s", b)
	}
	var first discoveryHistory
	if err := json.Unmarshal(c.Items[0], &first); err != nil {
		t.Fatal(err)
	}
	if first.Kind != "rename_asset" || first.Actor != "owner-demo" {
		t.Fatalf("newest filtered change ordering: %s", b)
	}
	discoveryGrantTest(t, o, a, "owner-hidden", ownerSubject, Scope{"asset", hidden}, discoveryAllActions)
	b = discoveryQueryTest(t, d, bearer, discoveryRequestTest("history", testAsset))
	c = discoveryItemsTest(t, b)
	if len(c.Items) != 6 {
		t.Fatalf("authorized history omitted changes: %s", b)
	}
	var item map[string]json.RawMessage
	_ = json.Unmarshal(c.Items[0], &item)
	if string(item["kind"]) != `"put_relationship"` || !bytes.Contains(item["before"], []byte(hidden)) || !bytes.Contains(item["after"], []byte(otherAsset)) {
		t.Fatalf("old/new whitelisted DTOs: %s", c.Items[0])
	}
	discoveryNoFieldsTest(t, b)
}

type discoveryWriterFunc func([]byte) (int, error)

func (f discoveryWriterFunc) Write(b []byte) (int, error) { return f(b) }
func TestDiscoveryWriteResultsAndValidation(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	want := errors.New("writer failed")
	for _, full := range []bool{false, true} {
		calls := 0
		result, err := d.Query(context.Background(), bearer, discoveryRequestTest("asset", testAsset), discoveryWriterFunc(func(b []byte) (int, error) {
			calls++
			if full {
				return len(b), want
			}
			return 1, want
		}))
		if !result.Started || !errors.Is(err, want) || calls != 1 {
			t.Fatalf("write uncertainty full=%v calls=%d result=%+v err=%v", full, calls, result, err)
		}
	}
	result, err := d.Query(context.Background(), bearer, discoveryRequestTest("asset", testAsset), discoveryWriterFunc(func(b []byte) (int, error) { return len(b) - 1, nil }))
	if !result.Started || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write: %+v %v", result, err)
	}
	for i, r := range []DiscoveryRequest{{Version: 1, Kind: "search", LibraryID: testLibrary, PageSize: 101}, {Version: 1, Kind: "page", LibraryID: testLibrary, Token: "page1_" + strings.Repeat("A", 64)}, {Version: 1, Kind: "asset", LibraryID: testLibrary, AssetID: testAsset, Text: "unused"}, {Version: 1, Kind: "search", LibraryID: testLibrary, PageSize: 1, ContextAssetIDs: []continuity.AssetID{testAsset, testAsset}}} {
		var b bytes.Buffer
		result, err := d.Query(context.Background(), bearer, r, &b)
		if DiscoveryErrorCode(err) != "invalid" || result.Started || b.Len() != 0 {
			t.Fatalf("invalid %d: %+v %v", i, result, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var b bytes.Buffer
	result, err = d.Query(ctx, bearer, discoveryRequestTest("asset", testAsset), &b)
	if err == nil || result.Started || b.Len() != 0 {
		t.Fatalf("cancellation became success: %+v %v", result, err)
	}
}

func TestDiscoverySearchNormalizationOrderingAndWindow(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	applyTest(t, s, testOperation("labels", Change{RenameAsset: &RenameAsset{testAsset, "original", "Alpha"}}, Change{RenameAsset: &RenameAsset{otherAsset, "B", "alphabet"}}))
	r := discoveryRequestTest("search", "")
	r.Text = " \tALPHA\n"
	r.ContextAssetIDs = []continuity.AssetID{otherAsset}
	b := discoveryQueryTest(t, d, bearer, r)
	c := discoveryItemsTest(t, b)
	if len(c.Items) != 2 || !c.Complete || !bytes.Contains(c.Items[0], []byte(otherAsset)) || !bytes.Contains(c.Items[1], []byte(testAsset)) || !bytes.Contains(c.Items[0], []byte("label_prefix")) {
		t.Fatalf("deterministic within-window rank: %s", b)
	}
	changes := make([]Change, 0, 250)
	for i := 0; i < 1001; i++ {
		id := continuity.AssetID(fmt.Sprintf("asset_%032x", i+100))
		changes = append(changes, Change{CreateAsset: &CreateAsset{id, "window"}})
		if len(changes) == 250 || i == 1000 {
			applyTest(t, s, testOperation(fmt.Sprintf("window-%d", i), changes...))
			changes = nil
		}
	}
	r.Text = "window"
	r.ContextAssetIDs = nil
	c = discoveryItemsTest(t, discoveryQueryTest(t, d, bearer, r))
	if c.Complete || c.MatchedCount != 1000 || len(c.Items) != 100 || c.Next == nil {
		t.Fatalf("bounded frozen window: %+v", c)
	}
}

func TestDiscoveryHistoryWhitelistedValueVariants(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	edge := RelationshipValue{SourceAssetID: testAsset, TargetAssetID: otherAsset, Predicate: "derived", Provenance: Observed, Source: "metadata claim"}
	loc := LocationValue{AssetID: testAsset, Source: "metadata claim", Locator: "literal://never-probed"}
	applyTest(t, s, testOperation("values", Change{RenameAsset: &RenameAsset{testAsset, "original", "renamed"}}, Change{PutRelationship: &PutRelationship{"edge", nil, edge}}, Change{PutLocation: &PutLocation{"loc", nil, loc}}))
	e, l := getRelationship(t, s, "edge"), getLocation(t, s, "loc")
	applyTest(t, s, testOperation("removed", Change{RemoveRelationship: &RemoveRelationship{"edge", relationshipState(e)}}, Change{RemoveLocation: &RemoveLocation{"loc", locationState(l)}}))
	e, l = getRelationship(t, s, "edge"), getLocation(t, s, "loc")
	m := testManifest(testAsset, testSHA)
	child := testManifest(testAsset, testSHA, rootRevision)
	childID, err := child.RevisionID()
	if err != nil {
		t.Fatal(err)
	}
	applyTest(t, s, testOperation("reactivate-repeat", Change{PutRelationship: &PutRelationship{"edge", relationshipState(e), edge}}, Change{PutLocation: &PutLocation{"loc", locationState(l), loc}}, Change{RecordRevision: &m}, Change{RecordRevision: &child}, Change{AdvanceHead: &AdvanceHead{testAsset, rootRevision, childID}}))
	b := discoveryQueryTest(t, d, bearer, discoveryRequestTest("history", testAsset))
	c := discoveryItemsTest(t, b)
	kinds := make(map[string]int)
	for _, raw := range c.Items {
		var item struct {
			Kind   string                     `json:"kind"`
			Before map[string]json.RawMessage `json:"before"`
			After  map[string]json.RawMessage `json:"after"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			t.Fatal(err)
		}
		kinds[item.Kind]++
		if item.Kind == "put_relationship" || item.Kind == "put_location" {
			if item.Before != nil && string(item.Before["removed"]) != "true" {
				t.Fatalf("reactivation lost old tombstone: %s", raw)
			}
			if string(item.After["removed"]) != "false" {
				t.Fatalf("put value missing removed=false: %s", raw)
			}
		}
		if strings.HasPrefix(item.Kind, "remove_") && string(item.After["removed"]) != "true" {
			t.Fatalf("remove value missing tombstone: %s", raw)
		}
	}
	for _, kind := range []string{"create_asset", "rename_asset", "record_revision", "advance_head", "put_relationship", "remove_relationship", "put_location", "remove_location"} {
		if kinds[kind] == 0 {
			t.Fatalf("history omitted %s: %s", kind, b)
		}
	}
	if kinds["record_revision"] != 3 {
		t.Fatal("repeat revision declaration disappeared")
	}
	discoveryNoFieldsTest(t, b)
}
