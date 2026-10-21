package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func collectionReadRequest(kind string, id CollectionID) CollectionQueryRequest {
	r := CollectionQueryRequest{Version: 1, Kind: kind, LibraryID: testLibrary, CollectionID: id}
	switch kind {
	case "directory":
		r.State = "all"
		r.PageSize = 100
	case "members":
		r.State = "active"
		r.PageSize = 100
	case "history", "access":
		r.PageSize = 100
	}
	return r
}
func collectionReadCall(t *testing.T, d *DiscoveryService, bearer []byte, r CollectionQueryRequest) []byte {
	t.Helper()
	var out bytes.Buffer
	result, err := d.QueryCollections(context.Background(), bearer, r, &out)
	if err != nil || !result.Started {
		t.Fatalf("collection read %s: %+v %v", r.Kind, result, err)
	}
	return out.Bytes()
}

type collectionReadTestEnvelope struct {
	Kind         string             `json:"kind"`
	Collection   *CollectionSummary `json:"collection"`
	CanCreate    bool               `json:"can_create"`
	State        string             `json:"state"`
	Items        []json.RawMessage  `json:"items"`
	MatchedCount int                `json:"matched_count"`
	Complete     bool               `json:"window_complete"`
	Next         *string            `json:"next_page_token"`
	Sort         string             `json:"sort_version"`
	Snapshot     string             `json:"snapshot_scope"`
}

func collectionReadEnvelope(t *testing.T, body []byte) collectionReadTestEnvelope {
	t.Helper()
	var v collectionReadTestEnvelope
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatal(err)
	}
	if v.Sort != "collections1" || v.Items == nil {
		t.Fatalf("invalid collection list envelope: %s", body)
	}
	return v
}
func collectionReadDenied(t *testing.T, d *DiscoveryService, bearer []byte, r CollectionQueryRequest, code string) {
	t.Helper()
	var out bytes.Buffer
	result, err := d.QueryCollections(context.Background(), bearer, r, &out)
	if DiscoveryErrorCode(err) != code || result.Started || out.Len() != 0 {
		t.Fatalf("collection disclosure should be %s: %+v %v %s", code, result, err, out.String())
	}
}
func collectionReadGrant(t *testing.T, o *LocalOwnerService, a AuthorityStatus, id int, col CollectionID, subject SubjectID, actions []string) CollectionGrantID {
	t.Helper()
	grant := CollectionGrantID(fmt.Sprintf("cgr_%032x", id))
	value := CollectionGrantValue{subject, CollectionGrantScope{"collection", col}, actions}
	p := collectionTxPolicy(id, col, "collection", CollectionGrantChange{"put", grant, nil, &value})
	if _, err := o.ApplyCollectionPolicy(context.Background(), collectionTxOwner(t, a, p)); err != nil {
		t.Fatal(err)
	}
	return grant
}

func TestCollectionReadCanonicalRequestShapes(t *testing.T) {
	col := CollectionID("col_11111111111111111111111111111111")
	for _, kind := range []string{"directory", "open", "members", "history", "access"} {
		t.Run(kind, func(t *testing.T) {
			r := collectionReadRequest(kind, col)
			if kind == "directory" {
				r.CollectionID = ""
			}
			var wire any
			switch kind {
			case "directory":
				wire = CollectionCLIDirectoryRequest{1, kind, testLibrary, r.State, 100}
			case "open":
				wire = CollectionCLIOpenRequest{1, kind, testLibrary, col}
			case "members":
				wire = CollectionCLIMembersRequest{1, kind, testLibrary, col, r.State, 100}
			default:
				wire = CollectionCLIListRequest{1, kind, testLibrary, col, 100}
			}
			data, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeCollectionCLIReadRequest(data, testLibrary)
			if err != nil || got != r {
				t.Fatalf("exact read decode: %+v %v", got, err)
			}
			invalid := []string{string(data) + "{}", strings.Replace(string(data), `"version":1`, `"version":null`, 1), strings.Replace(string(data), `"version":1,`, "", 1), strings.Replace(string(data), `"version":1`, `"version":1,"version":1`, 1), strings.Replace(string(data), `"version":1`, `"version":1,"actor_subject_id":"forbidden"`, 1), " " + string(data)}
			for _, raw := range invalid {
				v, e := DecodeCollectionCLIReadRequest([]byte(raw), testLibrary)
				if !errors.Is(e, ErrInvalid) || v != (CollectionQueryRequest{}) {
					t.Fatalf("invalid decode retained payload: %+v %v", v, e)
				}
			}
		})
	}
	page := collectionReadRequest("page", "")
	page.Token = "page1_" + strings.Repeat("a", 64)
	if _, err := ValidateCollectionQueryRequest(page, testLibrary); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCollectionCLIReadRequest([]byte(fmt.Sprintf(`{"version":1,"kind":"page","library_id":"%s","token":"%s"}`, testLibrary, page.Token)), testLibrary); !errors.Is(err, ErrInvalid) {
		t.Fatal("external page accepted")
	}
	for _, mutate := range []func(*CollectionQueryRequest){func(r *CollectionQueryRequest) { r.PageSize = 1 }, func(r *CollectionQueryRequest) { r.CollectionID = col }, func(r *CollectionQueryRequest) { r.State = "all" }, func(r *CollectionQueryRequest) { r.Version = 2 }} {
		bad := page
		mutate(&bad)
		v, e := ValidateCollectionQueryRequest(bad, testLibrary)
		if !errors.Is(e, ErrInvalid) || v != (CollectionQueryRequest{}) {
			t.Fatalf("unused field accepted: %+v %v", v, e)
		}
	}
}

func TestCollectionReadVisibilityHistoryAndExactGrantCAS(t *testing.T) {
	s, o, a, d, owner, reader := collectionTxStore(t)
	create := collectionTxCreate(t, 901)
	created := collectionTxCall(t, d, owner, create)
	grant := collectionReadGrant(t, o, a, 902, create.CollectionID, readerSubject, []string{"collections.read"})
	raw := collectionReadCall(t, d, reader, collectionReadRequest("members", create.CollectionID))
	members := collectionReadEnvelope(t, raw)
	if members.MatchedCount != 1 || len(members.Items) != 1 || bytes.Contains(raw, []byte(otherAsset)) {
		t.Fatalf("hidden membership: %s", raw)
	}
	var member CollectionMember
	if json.Unmarshal(members.Items[0], &member) != nil || member.AssetID != testAsset || member.HeadRevisionID != nil {
		t.Fatalf("member/revision permission: %s", members.Items[0])
	}
	history := collectionReadEnvelope(t, collectionReadCall(t, d, reader, collectionReadRequest("history", create.CollectionID)))
	if len(history.Items) != 0 || history.MatchedCount != 0 {
		t.Fatal("mixed hidden create was partially disclosed")
	}
	collectionReadDenied(t, d, reader, collectionReadRequest("access", create.CollectionID), "not_found")
	access := collectionReadEnvelope(t, collectionReadCall(t, d, owner, collectionReadRequest("access", create.CollectionID)))
	var expected *ExpectedCollectionGrant
	for _, item := range access.Items {
		var value CollectionAccessItem
		if json.Unmarshal(item, &value) != nil {
			t.Fatal("access decode")
		}
		if value.GrantID == grant {
			copy := value.Expected
			expected = &copy
		}
	}
	if expected == nil || expected.Value.SubjectID != readerSubject || expected.Version.Validate() != nil || expected.Removed {
		t.Fatal("restart CAS material missing")
	}
	value := expected.Value
	value.Actions = []string{"collections.edit", "collections.read"}
	change := CollectionGrantChange{"put", grant, expected, &value}
	op := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000387", create.CollectionID, created.ResultVersion, "grants", collectionTxRaw(t, EditCollectionGrants{[]CollectionGrantChange{change}})}
	receipt := collectionTxCall(t, d, owner, op)
	var opened CollectionOpenResponse
	if json.Unmarshal(collectionReadCall(t, d, reader, collectionReadRequest("open", create.CollectionID)), &opened) != nil || !opened.Collection.Permissions.Edit {
		t.Fatal("read CAS did not enable real change")
	}
	stale := collectionTxPolicy(904, create.CollectionID, "collection", change)
	if _, err := o.ApplyCollectionPolicy(context.Background(), collectionTxOwner(t, a, stale)); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale full grant CAS: %v", err)
	}
	rename := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000389", create.CollectionID, receipt.ResultVersion, "rename", collectionTxRaw(t, RenameCollection{"Visible new name"})}
	collectionTxCall(t, d, reader, rename)
	history = collectionReadEnvelope(t, collectionReadCall(t, d, reader, collectionReadRequest("history", create.CollectionID)))
	if len(history.Items) != 1 || bytes.Contains(history.Items[0], []byte("grant_id")) || bytes.Contains(history.Items[0], []byte("subject_id")) {
		t.Fatalf("typed filtered history: %s", history.Items)
	}
	var h CollectionHistoryItem
	if json.Unmarshal(history.Items[0], &h) != nil || h.Kind != "rename" || h.Actor != "you" || h.RecordedUnixNano == "" {
		t.Fatal("history union")
	}
	current, err := readCollectionGrant(context.Background(), s.db, grant)
	if err != nil {
		t.Fatal(err)
	}
	p := collectionTxPolicy(906, create.CollectionID, "collection", CollectionGrantChange{"remove", grant, &current.expected, nil})
	if _, err := o.ApplyCollectionPolicy(context.Background(), collectionTxOwner(t, a, p)); err != nil {
		t.Fatal(err)
	}
	access = collectionReadEnvelope(t, collectionReadCall(t, d, owner, collectionReadRequest("access", create.CollectionID)))
	found := false
	for _, raw := range access.Items {
		var g CollectionAccessItem
		json.Unmarshal(raw, &g)
		if g.GrantID == grant {
			found = g.Expected.Removed
		}
	}
	if !found {
		t.Fatal("access lost tombstone identity")
	}
}

func TestCollectionReadEligibilityAndEmptyDirectory(t *testing.T) {
	_, o, a, d, owner, reader := collectionTxStore(t)
	directory := collectionReadRequest("directory", "")
	empty := collectionReadEnvelope(t, collectionReadCall(t, d, reader, directory))
	if len(empty.Items) != 0 || empty.CanCreate || empty.Collection != nil || !empty.Complete {
		t.Fatal("empty directory semantics")
	}
	owned := collectionReadEnvelope(t, collectionReadCall(t, d, owner, directory))
	if !owned.CanCreate {
		t.Fatal("library create right unavailable")
	}
	service := SubjectID("sub_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	policyTest(t, o, a, "read-service", NewPolicyChange("create_subject", CreateSubject{service, SubjectValue{"service", "worker", true}}))
	issued, _ := collectionTxIssue(t, o, a, "read-service-token", service, collectionTxActions)
	collectionReadDenied(t, d, issued.Bearer, directory, "not_found") // independent person guard: without it empty200 would pass.
	narrow, _ := collectionTxIssue(t, o, a, "read-no-ceiling", ownerSubject, []string{"collections.edit"})
	collectionReadDenied(t, d, narrow.Bearer, directory, "not_found")
	collectionReadDenied(t, d, []byte("invalid"), directory, "unauthenticated")
}

func TestCollectionReadAccessAll128AndReopen(t *testing.T) {
	s, o, a, d, owner, _ := collectionTxStore(t)
	create := collectionTxCreate(t, 920)
	collectionTxCall(t, d, owner, create)
	for start := 0; start < 127; start += 16 {
		changes := make([]CollectionGrantChange, 0, 16)
		for i := start; i < 127 && i < start+16; i++ {
			value := CollectionGrantValue{readerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{"collections.read"}}
			changes = append(changes, CollectionGrantChange{"put", CollectionGrantID(fmt.Sprintf("cgr_%032x", 10000+i)), nil, &value})
		}
		p := collectionTxPolicy(1000+start, create.CollectionID, "collection", changes...)
		if _, err := o.ApplyCollectionPolicy(context.Background(), collectionTxOwner(t, a, p)); err != nil {
			t.Fatal(err)
		}
	}
	// Open a second real store/service: no process-local grant token fixture probe.
	reopened, err := OpenContext(context.Background(), s.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	reopened.clock = s.clock
	next := discoveryServiceTest(t, reopened)
	r := collectionReadRequest("access", create.CollectionID)
	r.PageSize = 100
	first := collectionReadEnvelope(t, collectionReadCall(t, next, owner, r))
	if first.MatchedCount != 128 || len(first.Items) != 100 || first.Next == nil {
		t.Fatal("access cap paging")
	}
	r = collectionReadRequest("page", "")
	r.Token = *first.Next
	last := collectionReadEnvelope(t, collectionReadCall(t, next, owner, r))
	if len(last.Items) != 28 || last.Next != nil || last.MatchedCount != 128 {
		t.Fatal("access remainder")
	}
	var g CollectionAccessItem
	if err := json.Unmarshal(last.Items[0], &g); err != nil {
		t.Fatal(err)
	}
	current, err := readCollectionGrant(context.Background(), s.db, g.GrantID)
	if err != nil || !reflect.DeepEqual(current.expected, g.Expected) {
		t.Fatal("reopened grant CAS not exact")
	}
	p := collectionTxPolicy(1200, create.CollectionID, "collection", CollectionGrantChange{"remove", g.GrantID, &g.Expected, nil})
	if _, err := o.ApplyCollectionPolicy(context.Background(), collectionTxOwner(t, a, p)); err != nil {
		t.Fatal(err)
	}
	collectionReadDenied(t, next, owner, r, "restart_query")
}

func TestCollectionReadRevisionGrantAndCeilingRemainIndependent(t *testing.T) {
	_, o, a, d, owner, _ := collectionTxStore(t)
	create := collectionTxCreate(t, 2301)
	collectionTxCall(t, d, owner, create)
	discoveryGrantTest(t, o, a, "collection-revisions", ownerSubject, Scope{"library", ""}, []string{"revisions.read"})
	inspect := func(bearer []byte) *continuity.RevisionID {
		envelope := collectionReadEnvelope(t, collectionReadCall(t, d, bearer, collectionReadRequest("members", create.CollectionID)))
		for _, raw := range envelope.Items {
			var item CollectionMember
			if err := json.Unmarshal(raw, &item); err != nil {
				t.Fatal(err)
			}
			if item.AssetID == testAsset {
				return item.HeadRevisionID
			}
		}
		t.Fatal("authorized member absent")
		return nil
	}
	if inspect(owner) != nil {
		t.Fatal("head disclosed past credential revision ceiling")
	}
	actions := append(append([]string(nil), collectionTxActions...), "revisions.read")
	issued, _ := collectionTxIssue(t, o, a, "collection-revision-reader", ownerSubject, actions)
	head := inspect(issued.Bearer)
	if head == nil || *head != rootRevision {
		t.Fatal("explicit revision grant+ceiling did not disclose current head")
	}
}

func TestCollectionReadIndependentLiteralWireVectors(t *testing.T) {
	data, err := os.ReadFile("testdata/collections_read_wire.json")
	if err != nil {
		t.Fatal(err)
	}
	type expected struct {
		Version      int                  `json:"version"`
		Kind         string               `json:"kind"`
		LibraryID    continuity.LibraryID `json:"library_id"`
		CollectionID CollectionID         `json:"collection_id"`
		State        string               `json:"state"`
		PageSize     int                  `json:"page_size"`
		Token        string               `json:"token"`
	}
	var fixture struct {
		Version int `json:"version"`
		Vectors []struct {
			Name      string               `json:"name"`
			LibraryID continuity.LibraryID `json:"library_id"`
			Input     string               `json:"input"`
			Valid     bool                 `json:"valid"`
			Expected  *expected            `json:"expected"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Version != 1 || len(fixture.Vectors) != 59 {
		t.Fatal("literal wire fixture coverage")
	}
	valid, invalid := 0, 0
	for _, vector := range fixture.Vectors {
		if vector.Valid {
			valid++
		} else {
			invalid++
		}
		t.Run(vector.Name, func(t *testing.T) {
			got, err := DecodeCollectionCLIReadRequest([]byte(vector.Input), vector.LibraryID)
			if !vector.Valid {
				if !errors.Is(err, ErrInvalid) || got != (CollectionQueryRequest{}) {
					t.Fatalf("invalid literal retained payload: %+v %v", got, err)
				}
				return
			}
			if vector.Expected == nil {
				t.Fatal("missing literal expected scalars")
			}
			e := vector.Expected
			want := CollectionQueryRequest{Version: e.Version, Kind: e.Kind, LibraryID: e.LibraryID, CollectionID: e.CollectionID, State: e.State, PageSize: e.PageSize, Token: e.Token}
			if err != nil || got != want {
				t.Fatalf("literal wire scalar mismatch: got%+v want%+v err%v", got, want, err)
			}
		})
	}
	if valid != 6 || invalid != 53 {
		t.Fatal("literal wire valid/invalid inventory changed")
	}
}
