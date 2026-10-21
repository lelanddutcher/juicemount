package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const readOracleLibrary continuity.LibraryID = "lib_11111111111111111111111111111111"
const readOracleQuery CollectionID = "col_22222222222222222222222222222222"
const readOracleA SubjectID = "sub_33333333333333333333333333333333"
const readOracleB SubjectID = "sub_44444444444444444444444444444444"

var readOracleAssets = []continuity.AssetID{"asset_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "asset_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "asset_cccccccccccccccccccccccccccccccc", "asset_dddddddddddddddddddddddddddddddd", "asset_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "asset_ffffffffffffffffffffffffffffffff"}
var readOracleActions = []string{"activity.record", "assets.discover", "collections.admin", "collections.create", "collections.edit", "collections.read", "revisions.read"}

type savedReadFixture struct {
	s             *Store
	o             *LocalOwnerService
	d             *DiscoveryService
	incarnation   string
	owner, reader []byte
	now           atomic.Int64
	version       CollectionVersion
}

func newSavedReadFixture(t *testing.T) *savedReadFixture {
	t.Helper()
	f := new(savedReadFixture)
	f.now.Store(1699999900)
	var err error
	f.s, err = CreateSavedQueries(context.Background(), filepath.Join(t.TempDir(), "reads.sqlite"), readOracleLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.s.Close(); err != nil {
			t.Error(err)
		}
	})
	f.s.clock = func() time.Time { return time.Unix(f.now.Load(), 0) }
	f.o, err = f.s.LocalOwner(readOracleLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "A"
	boot, err := f.o.Bootstrap(context.Background(), AdminRequest{1, readOracleLibrary, "read-boot", "", "bootstrap", collectionTxRaw(t, BootstrapRequest{readOracleA, &label})})
	if err != nil {
		t.Fatal(err)
	}
	f.incarnation = boot.Result.Incarnation
	f.policy(t, "read-authority", NewPolicyChange("create_subject", CreateSubject{readOracleB, SubjectValue{"person", "B", true}}), NewPolicyChange("put_grant", PutGrant{"read-A", nil, GrantValue{readOracleA, Scope{"library", ""}, []string{"activity.record", "assets.discover", "revisions.read"}}}), NewPolicyChange("put_grant", PutGrant{"read-B", nil, GrantValue{readOracleB, Scope{"library", ""}, []string{"activity.record", "assets.discover", "revisions.read"}}}))
	v := CollectionGrantValue{readOracleA, CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	f.collectionPolicy(t, 1, "", "library_create", CollectionGrantChange{"put", "cgr_11111111111111111111111111111111", nil, &v})
	f.owner = f.issue(t, "read-owner", readOracleA, readOracleActions)
	f.reader = f.issue(t, "read-reader", readOracleB, readOracleActions)
	f.d, err = NewDiscoveryService(context.Background(), f.s, readOracleLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.d.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	labels := []string{"Alpha", "Alphabet", "Alpha extra", "Alpha other", "Zed"}
	changes := make([]Change, 5)
	for i := range labels {
		changes[i] = Change{CreateAsset: &CreateAsset{readOracleAssets[i], labels[i]}}
	}
	f.apply(t, "read-assets", changes...)
	d := SavedQueryDefinition{1, "ranked_assets", " Alpha ", []continuity.AssetID{readOracleAssets[1], readOracleAssets[4]}}
	f.version = f.mutate(t, 10, "", "create", CreateSavedQuery{"Saved", "cgr_22222222222222222222222222222222", d})
	shared := CollectionGrantValue{readOracleB, CollectionGrantScope{"collection", readOracleQuery}, []string{"collections.admin", "collections.edit", "collections.read"}}
	f.collectionPolicy(t, 11, readOracleQuery, "collection", CollectionGrantChange{"put", "cgr_44444444444444444444444444444444", nil, &shared})
	return f
}
func (f *savedReadFixture) policy(t *testing.T, id string, changes ...PolicyChange) {
	t.Helper()
	p := PolicyOperation{1, readOracleLibrary, id, "local_owner", readOracleA, changes}
	if _, err := f.o.ApplyPolicy(context.Background(), AdminRequest{1, readOracleLibrary, id, f.incarnation, "apply_policy", collectionTxRaw(t, ApplyPolicyRequest{p})}); err != nil {
		t.Fatal(err)
	}
}
func (f *savedReadFixture) collectionPolicy(t *testing.T, n int, id CollectionID, category string, changes ...CollectionGrantChange) {
	t.Helper()
	p := CollectionPolicyRequest{2, readOracleLibrary, CollectionPolicyOperationID(fmt.Sprintf("cpo_%032x", n)), category, id, "", "", changes}
	r := CollectionOwnerRequest{1, readOracleLibrary, string(p.ID), f.incarnation, "apply_collection_policy", collectionTxRaw(t, ApplyCollectionPolicyRequest{p})}
	if _, err := f.o.ApplyCollectionPolicy(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}
func (f *savedReadFixture) issue(t *testing.T, id string, subject SubjectID, actions []string) []byte {
	t.Helper()
	r, err := f.o.IssueCredentialV2(context.Background(), AdminRequestV2{2, readOracleLibrary, id, f.incarnation, "issue_credential", collectionTxRaw(t, IssueCredentialRequestV2{subject, Scope{"library", ""}, actions, f.now.Load() + 3600})})
	if err != nil {
		t.Fatal(err)
	}
	return r.Bearer
}
func (f *savedReadFixture) apply(t *testing.T, id string, changes ...Change) {
	t.Helper()
	if _, err := f.s.Apply(context.Background(), Operation{1, readOracleLibrary, id, "fixture", changes}); err != nil {
		t.Fatal(err)
	}
}
func (f *savedReadFixture) mutate(t *testing.T, n int, version CollectionVersion, kind string, value any) CollectionVersion {
	t.Helper()
	op := SavedQueryRequest{2, readOracleLibrary, CollectionOperationID(fmt.Sprintf("cop_%032x", n)), readOracleQuery, version, kind, collectionTxRaw(t, value)}
	var out bytes.Buffer
	r, err := f.d.MutateSavedQuery(context.Background(), f.owner, op, &out)
	if err != nil || !r.Started {
		t.Fatalf("mutate: %+v %v", r, err)
	}
	receipt, err := DecodeSavedQueryReceipt(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return receipt.ResultVersion
}
func (f *savedReadFixture) request(kind string) SavedQueryReadRequest {
	r := SavedQueryReadRequest{Version: 2, Kind: kind, LibraryID: readOracleLibrary, CollectionID: readOracleQuery}
	if kind == "directory" {
		r.CollectionID = ""
		r.State = "active"
	}
	if kind != "open" && kind != "definition" {
		r.PageSize = 1
	}
	return r
}
func savedReadCall(t *testing.T, d *DiscoveryService, bearer []byte, r SavedQueryReadRequest) []byte {
	t.Helper()
	var out bytes.Buffer
	result, err := d.QuerySavedQueries(context.Background(), bearer, r, &out)
	if err != nil || !result.Started {
		t.Fatalf("%s: %+v %v", r.Kind, result, err)
	}
	return out.Bytes()
}
func savedReadDenied(t *testing.T, d *DiscoveryService, bearer []byte, r SavedQueryReadRequest, code string) {
	t.Helper()
	var out bytes.Buffer
	result, err := d.QuerySavedQueries(context.Background(), bearer, r, &out)
	if DiscoveryErrorCode(err) != code || result.Started || out.Len() != 0 {
		t.Fatalf("denied %s: %+v %v %s", r.Kind, result, err, out.String())
	}
}
func savedReadExecute(t *testing.T, f *savedReadFixture, bearer []byte) ([]SavedQueryResult, *string) {
	t.Helper()
	r := f.request("execute")
	var all []SavedQueryResult
	var first *string
	for i := 0; i < 1000; i++ {
		raw := savedReadCall(t, f.d, bearer, r)
		var page SavedQueryExecuteResponse
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		if page.QueryVersion != f.version || page.Collection.DefinitionVersion != f.version || page.MatchedCount != 4 || !page.WindowComplete {
			t.Fatalf("execution envelope: %s", raw)
		}
		all = append(all, page.Items...)
		if i == 0 {
			first = page.NextPageToken
		}
		if page.NextPageToken == nil {
			return all, first
		}
		r = SavedQueryReadRequest{Version: 2, Kind: "page", LibraryID: readOracleLibrary, QueryKind: "execute", CollectionID: readOracleQuery, Token: *page.NextPageToken}
	}
	t.Fatal("unbounded execution")
	return nil, nil
}
func readOracleExpected(order []int, final bool) []SavedQueryResult {
	out := make([]SavedQueryResult, 0, len(order))
	for _, i := range order {
		label := []string{"Alpha", "Alphabet", "Alpha extra", "Alpha other", "Zed", "x Alpha x"}[i]
		reasons := []string{"label_prefix"}
		if i == 0 {
			reasons = []string{"label_exact"}
		}
		if i == 1 {
			reasons = []string{"context", "label_prefix"}
		}
		if i == 2 || i == 3 {
			reasons = []string{"declared_selection", "label_prefix"}
		}
		if i == 5 {
			reasons = []string{"label_contains"}
		}
		var head *continuity.RevisionID
		if final && i == 0 {
			v := continuity.RevisionID("rev1_bcdd0cf551d476327093999faf973c1b99f940779ab3ce71c695abd4facb0352")
			head = &v
		}
		out = append(out, SavedQueryResult{SavedQueryResultID{readOracleQuery, readOracleAssets[i]}, readOracleAssets[i], label, head, reasons})
	}
	return out
}
func (f *savedReadFixture) remember(t *testing.T, bearer []byte, id string, asset continuity.AssetID) {
	t.Helper()
	recordPreferenceTest(t, f.d, bearer, PreferenceRequest{2, readOracleLibrary, id, "select", asset, nil, f.now.Load() + 600})
}

// These complete rows are literal personal expectations. Equal timestamps do
// not make A's [D,C] interchangeable with B's [C,D]. C remains in both Recents
// lists after it leaves the saved query's text matches.
func readOracleRecentsExpected(person string, final bool) RecentsResponse {
	cLabel := "Alpha extra"
	if final {
		cLabel = "unrelated"
	}
	var items []RecentsItem
	if person == "A" {
		items = []RecentsItem{
			{"asset_dddddddddddddddddddddddddddddddd", "Alpha other", nil, []string{"declared_selection"}, "1700001000", "1702593000"},
			{"asset_cccccccccccccccccccccccccccccccc", cLabel, nil, []string{"declared_selection"}, "1700000000", "1702592000"},
		}
	} else {
		items = []RecentsItem{
			{"asset_cccccccccccccccccccccccccccccccc", cLabel, nil, []string{"declared_selection"}, "1700001000", "1702593000"},
			{"asset_dddddddddddddddddddddddddddddddd", "Alpha other", nil, []string{"declared_selection"}, "1700000000", "1702592000"},
		}
	}
	return RecentsResponse{2, "recents", items, 2, "frozen_authorized_recent", true, nil, "recents1", "frozen_snapshot"}
}
func assertReadOracleRecents(t *testing.T, f *savedReadFixture, final bool) {
	t.Helper()
	for _, person := range []struct {
		name   string
		bearer []byte
	}{{"A", f.owner}, {"B", f.reader}} {
		got := readRecentsCall(t, f.d, person.bearer, RecentsReadRequest{2, "list", readOracleLibrary, 100, ""})
		want := readOracleRecentsExpected(person.name, final)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("literal %s Recents final=%v: got%+v want%+v", person.name, final, got, want)
		}
	}
}

// Filter the existing storage-typed, sorted snapshot triplets rather than
// introduce another SQL snapshot implementation. Tokens, row bytes, receipts,
// subject generations and the complete control row all remain in this capture.
func savedReadPrivateSnapshot(t *testing.T, s *Store) []byte {
	t.Helper()
	var all []json.RawMessage
	if err := json.Unmarshal(savedQueryTestSnapshot(t, s, true), &all); err != nil {
		t.Fatal(err)
	}
	if len(all)%3 != 0 {
		t.Fatal("snapshot triplet shape")
	}
	private := make([]json.RawMessage, 0, 12)
	for i := 0; i < len(all); i += 3 {
		var table string
		if err := json.Unmarshal(all[i], &table); err != nil {
			t.Fatal(err)
		}
		switch table {
		case "preferences", "preference_subjects", "preference_receipts", "preference_control":
			private = append(private, all[i:i+3]...)
		}
	}
	if len(private) != 12 {
		t.Fatalf("private snapshot table coverage: %d", len(private)/3)
	}
	b, err := json.Marshal(private)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSavedQueryReadLiteralSharedTransitionAndReadOnlyActivity(t *testing.T) {
	f := newSavedReadFixture(t)
	// Independent oracle setup: both first selections precede either second one.
	f.now.Store(1700000000)
	f.remember(t, f.owner, "A-first", readOracleAssets[2])
	f.remember(t, f.reader, "B-first", readOracleAssets[3])
	f.now.Store(1700001000)
	f.remember(t, f.owner, "A-second", readOracleAssets[3])
	f.remember(t, f.reader, "B-second", readOracleAssets[2])
	f.now.Store(1700001001)
	// Corrected oracle: the second explicit Remember prunes the expired first
	// request receipt. Both 30-day selections and person generation2 remain.
	for _, person := range []SubjectID{readOracleA, readOracleB} {
		state, err := readPreferenceSubject(context.Background(), f.s.db, readOracleLibrary, person)
		if err != nil {
			t.Fatal(err)
		}
		var entries, receipts int
		if err := f.s.db.QueryRowContext(context.Background(), `SELECT (SELECT count(*) FROM preferences WHERE subject_id=?),(SELECT count(*) FROM preference_receipts WHERE subject_id=?)`, person, person).Scan(&entries, &receipts); err != nil {
			t.Fatal(err)
		}
		if state.generation != 2 || state.entries != 2 || state.receipts != 1 || entries != 2 || receipts != 1 {
			t.Fatalf("post-setup private counts for %s: %+v rows%d receipts%d", person, state, entries, receipts)
		}
	}
	privateBefore := savedReadPrivateSnapshot(t, f.s)
	assertPrivateUnchanged := func(stage string) {
		t.Helper()
		if !bytes.Equal(privateBefore, savedReadPrivateSnapshot(t, f.s)) {
			t.Fatalf("private activity changed during %s", stage)
		}
	}
	manual := CollectionRequest{1, readOracleLibrary, "cop_77777777777777777777777777777777", "col_77777777777777777777777777777777", "", "create", collectionTxRaw(t, CreateCollection{"Snapshot", "cgr_77777777777777777777777777777777", []NewCollectionEntry{{"cent_00000000000000000000000000000001", readOracleAssets[0]}, {"cent_00000000000000000000000000000002", readOracleAssets[1]}, {"cent_00000000000000000000000000000003", readOracleAssets[2]}, {"cent_00000000000000000000000000000004", readOracleAssets[3]}}})}
	var receipt bytes.Buffer
	if _, err := f.d.MutateCollection(context.Background(), f.owner, manual, &receipt); err != nil {
		t.Fatal(err)
	}
	before := savedQueryTestSnapshot(t, f.s, true)
	initialA, old := savedReadExecute(t, f, f.owner)
	initialB, _ := savedReadExecute(t, f, f.reader)
	if !reflect.DeepEqual(initialA, readOracleExpected([]int{1, 0, 3, 2}, false)) || !reflect.DeepEqual(initialB, readOracleExpected([]int{1, 0, 2, 3}, false)) {
		t.Fatalf("literal initial order A=%+v B=%+v", initialA, initialB)
	}
	definition := savedReadCall(t, f.d, f.owner, f.request("definition"))
	_ = savedReadCall(t, f.d, f.reader, f.request("directory"))
	assertReadOracleRecents(t, f, false)
	if !bytes.Equal(before, savedQueryTestSnapshot(t, f.s, true)) {
		t.Fatal("reads changed preference/clock or durable state")
	}
	assertPrivateUnchanged("initial reads")
	f.apply(t, "new-F", Change{CreateAsset: &CreateAsset{readOracleAssets[5], "x Alpha x"}})
	assertPrivateUnchanged("new F metadata")
	f.apply(t, "rename-C", Change{RenameAsset: &RenameAsset{readOracleAssets[2], "Alpha extra", "unrelated"}})
	assertPrivateUnchanged("C rename metadata")
	m := continuity.Manifest{Version: 1, LibraryID: readOracleLibrary, AssetID: readOracleAssets[0], Content: continuity.Content{SHA256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", Size: 3}, Parents: []continuity.RevisionID{}}
	head := continuity.RevisionID("rev1_bcdd0cf551d476327093999faf973c1b99f940779ab3ce71c695abd4facb0352")
	f.apply(t, "head-A", Change{RecordRevision: &m}, Change{AdvanceHead: &AdvanceHead{readOracleAssets[0], "", head}})
	assertPrivateUnchanged("A head metadata")
	if old == nil {
		t.Fatal("missing old cursor")
	}
	savedReadDenied(t, f.d, f.owner, SavedQueryReadRequest{Version: 2, Kind: "page", LibraryID: readOracleLibrary, QueryKind: "execute", CollectionID: readOracleQuery, Token: *old}, "restart_query")
	a, _ := savedReadExecute(t, f, f.owner)
	b, _ := savedReadExecute(t, f, f.reader)
	want := readOracleExpected([]int{1, 0, 3, 5}, true)
	if !reflect.DeepEqual(a, want) || !reflect.DeepEqual(b, want) {
		t.Fatalf("literal final A=%+v B=%+v", a, b)
	}
	if !bytes.Equal(definition, savedReadCall(t, f.d, f.owner, f.request("definition"))) {
		t.Fatal("metadata changed saved definition/CAS")
	}
	var count int
	var ids string
	if err := f.s.db.QueryRow(`SELECT count(*),group_concat(asset_id,',') FROM (SELECT asset_id FROM collection_entries WHERE collection_id=? ORDER BY entry_id)`, manual.CollectionID).Scan(&count, &ids); err != nil || count != 4 || ids != string(readOracleAssets[0])+","+string(readOracleAssets[1])+","+string(readOracleAssets[2])+","+string(readOracleAssets[3]) {
		t.Fatalf("manual snapshot changed: %d %s %v", count, ids, err)
	}
	assertReadOracleRecents(t, f, true)
	assertPrivateUnchanged("final query and literal personal Recents reads")
}

func TestSavedQueryReadAnchorLossWholeHistoryAndRepair(t *testing.T) {
	f := newSavedReadFixture(t)
	// Reader keeps collection rights while its asset grant loses both anchors.
	g, err := readGrant(context.Background(), f.s.db, readOracleLibrary, "read-B")
	if err != nil {
		t.Fatal(err)
	}
	f.policy(t, "remove-B-assets", NewPolicyChange("remove_grant", RemoveGrant{g.ID, g.Expected()}))
	for _, kind := range []string{"open", "directory", "access"} {
		_ = savedReadCall(t, f.d, f.reader, f.request(kind))
	}
	for _, kind := range []string{"definition", "execute", "history"} {
		savedReadDenied(t, f.d, f.reader, f.request(kind), "not_found")
	}
	f.version = f.mutate(t, 20, f.version, "replace_definition", ReplaceSavedQueryDefinition{SavedQueryDefinition{1, "ranked_assets", "", []continuity.AssetID{}}})
	_ = savedReadCall(t, f.d, f.reader, f.request("definition"))
	f.version = f.mutate(t, 21, f.version, "rename", RenameCollection{"harmless newest"})
	// page_size=1 must still reject the older INCLUDED hidden create.
	savedReadDenied(t, f.d, f.reader, f.request("history"), "not_found")
	g, err = readGrant(context.Background(), f.s.db, readOracleLibrary, "read-B")
	if err != nil {
		t.Fatal(err)
	}
	f.policy(t, "restore-B-assets", NewPolicyChange("put_grant", PutGrant{"read-B", g.Expected(), GrantValue{readOracleB, Scope{"library", ""}, []string{"activity.record", "assets.discover", "revisions.read"}}}))
	_ = savedReadCall(t, f.d, f.reader, f.request("history"))
}
