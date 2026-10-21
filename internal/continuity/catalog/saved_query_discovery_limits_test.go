package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Isolated defensive-counter parameters, not a quota-valid 8193-event history.
func TestSavedQueryReadHistoryScanAndExactBuildBoundaries(t *testing.T) {
	var scan savedQueryHistoryScan
	for i := 0; i < 8192; i++ {
		if err := scan.next(); err != nil {
			t.Fatal("8192 source rows refused")
		}
	}
	if DiscoveryErrorCode(scan.next()) != "work_budget" {
		t.Fatal("8193rd bounded probe accepted")
	}
	scan.bytes = 33553920
	if err := scan.charge(512); err != nil {
		t.Fatal("32MiB equality refused")
	}
	if DiscoveryErrorCode(scan.charge(1)) != "work_budget" {
		t.Fatal("32MiB+1 accepted")
	}
	p := savedReadPlanner{base: 16384 + 4228 + 142 + 256000, raw: 3145728, bodies: 2865134, pages: 10}
	if p.cost() != 8388608 {
		t.Fatalf("fixed exact cost %d", p.cost())
	}
	p.bodies++
	if p.cost() != 8388609 {
		t.Fatal("capacity byte omitted")
	}
}

func TestSavedQueryReadOlderSchemasRequireExplicitUpgrade(t *testing.T) {
	s, _, _, d, bearer, _ := preferencesTestStore(t)
	before, err := readPreferenceControl(context.Background(), s.db, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	result, err := d.QueryRecents(context.Background(), bearer, RecentsReadRequest{2, "list", testLibrary, 100, ""}, &out)
	if !errors.Is(err, ErrUpgradeRequired) || result.Started || out.Len() != 0 {
		t.Fatalf("schema4 Recents: %+v %v", result, err)
	}
	result, err = d.QuerySavedQueries(context.Background(), bearer, SavedQueryReadRequest{Version: 2, Kind: "directory", LibraryID: testLibrary, State: "active", PageSize: 100}, &out)
	if !errors.Is(err, ErrUpgradeRequired) || result.Started || out.Len() != 0 {
		t.Fatalf("schema4 saved queries: %+v %v", result, err)
	}
	after, err := readPreferenceControl(context.Background(), s.db, testLibrary)
	if err != nil || before != after || s.version != 4 {
		t.Fatalf("new reads migrated/wrote old schema: %+v %+v %v", before, after, err)
	}
	_ = preferenceStatusTest(t, d, bearer, testAsset) // old preference API remains usable
}

func TestSavedQueryReadHistoryByteCutoffExcludesOlderHiddenDefinition(t *testing.T) {
	f := newSavedReadFixture(t)
	anchors := make([]continuity.AssetID, 64)
	changes := make([]Change, 64)
	for i := range anchors {
		anchors[i] = continuity.AssetID(fmt.Sprintf("asset_%032x", i+1))
		changes[i] = Change{CreateAsset: &CreateAsset{anchors[i], "anchor"}}
	}
	f.apply(t, "large-history-anchors", changes...)
	// B loses the original B/E anchors but receives exactly the new64 anchors.
	g, err := readGrant(context.Background(), f.s.db, readOracleLibrary, "read-B")
	if err != nil {
		t.Fatal(err)
	}
	grants := []PolicyChange{NewPolicyChange("remove_grant", RemoveGrant{g.ID, g.Expected()})}
	for i, id := range anchors {
		grants = append(grants, NewPolicyChange("put_grant", PutGrant{fmt.Sprintf("history-anchor-%d", i), nil, GrantValue{readOracleB, Scope{"asset", id}, []string{"assets.discover"}}}))
	}
	f.policy(t, "large-history-authority", grants...)
	for i := 0; i < 1000; i++ {
		text := strings.Repeat("<", 256)
		if i%2 != 0 {
			text = strings.Repeat(">", 256)
		}
		f.version = f.mutate(t, 2000+i, f.version, "replace_definition", ReplaceSavedQueryDefinition{SavedQueryDefinition{1, "ranked_assets", text, anchors}})
	}
	// The old hidden create is beyond the deterministic output budget. It must
	// neither poison this INCLUDED prefix nor be disclosed/countable.
	r := f.request("history")
	var p SavedQueryListResponse[CollectionHistoryItem]
	if err := json.Unmarshal(savedReadCall(t, f.d, f.reader, r), &p); err != nil {
		t.Fatal(err)
	}
	if p.WindowComplete || p.MatchedCount <= 0 || p.MatchedCount >= 1000 || len(p.Items) != 1 || p.Items[0].Kind != "replace_definition" {
		t.Fatalf("byte cutoff semantics: %+v", p)
	}
}

func TestSavedQueryReadPoolLeaseEqualityAndForeignOwnership(t *testing.T) {
	f := newSavedReadFixture(t)
	var first SavedQueryExecuteResponse
	if err := json.Unmarshal(savedReadCall(t, f.d, f.owner, f.request("execute")), &first); err != nil {
		t.Fatal(err)
	}
	r := SavedQueryReadRequest{Version: 2, Kind: "page", LibraryID: readOracleLibrary, CollectionID: readOracleQuery, QueryKind: "execute", Token: *first.NextPageToken}
	// Literal contract amount, independent of the implementation constant. Every
	// unexpectedly admitted synthetic owner is unwound before reporting failure.
	const wantActive int64 = 13 << 20
	f.d.mu.Lock()
	w := f.d.tokens[r.Token].window
	b := cloneDiscoveryBinding(w.binding)
	original := f.d.bytes
	originalLeases := w.leases
	f.d.bytes = discoveryQuota - wantActive
	f.d.mu.Unlock()
	leased, _, reservation, err := f.d.beginSavedReadPage(context.Background(), r, savedQueryReadFamily, b)
	f.d.mu.Lock()
	atEquality := f.d.bytes
	leasesAtEquality := w.leases
	if leased != nil {
		f.d.bytes -= reservation
		leased.leases--
	}
	f.d.bytes = discoveryQuota - wantActive + 1
	beforeBytes, beforeLeases, beforeWindows, beforeTokens := f.d.bytes, w.leases, len(f.d.windows), len(f.d.tokens)
	f.d.mu.Unlock()
	if err != nil || leased != w || reservation != wantActive || atEquality != discoveryQuota || leasesAtEquality != originalLeases+1 {
		t.Errorf("13MiB equality reservation: got %d bytes/%d leases, reservation%d err%v", atEquality, leasesAtEquality, reservation, err)
	}
	refused, _, refusedReservation, refusedErr := f.d.beginSavedReadPage(context.Background(), r, savedQueryReadFamily, b)
	f.d.mu.Lock()
	unchanged := f.d.bytes == beforeBytes && w.leases == beforeLeases && len(f.d.windows) == beforeWindows && len(f.d.tokens) == beforeTokens && f.d.tokens[r.Token] != nil && f.d.tokens[r.Token].window == w && !w.retired
	// A compiling 2MiB reversion may admit this call. Do not strand that owner.
	if refused != nil {
		f.d.bytes -= refusedReservation
		refused.leases--
	}
	f.d.bytes = original
	finalLeases := w.leases
	f.d.mu.Unlock()
	if DiscoveryErrorCode(refusedErr) != "busy" || refused != nil || refusedReservation != 0 || !unchanged || finalLeases != originalLeases {
		t.Errorf("13MiB+1 must refuse without pool/window/lease effects: err%v reservation%d unchanged%v leases%d", refusedErr, refusedReservation, unchanged, finalLeases)
	}
	// A fresh foreign person's reservation cannot evict this retained window.
	f.d.mu.Lock()
	f.d.bytes = discoveryQuota
	f.d.mu.Unlock()
	foreign := b
	h := *b.handle
	h.subject = readOracleB
	foreign.handle = &h
	if err := f.d.reserveSavedReadNew(foreign); DiscoveryErrorCode(err) != "busy" {
		t.Fatalf("foreign pressure admission: %v", err)
	}
	f.d.mu.Lock()
	f.d.bytes = original
	if w.retired {
		t.Error("foreign window evicted")
	}
	f.d.mu.Unlock()
}

func TestSavedQueryReadTokenDrawBoundAndCollisionClasses(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pages     int
		values    []byte
		want      []string
		wantDraws int
		busy      bool
	}{
		{"unique", 2, []byte{2}, []string{fmt.Sprintf("page1_%064x", 2)}, 1, false},
		{"occupied_global", 2, []byte{1, 2}, []string{fmt.Sprintf("page1_%064x", 2)}, 2, false},
		{"repeated_local", 3, []byte{2, 2, 3}, []string{fmt.Sprintf("page1_%064x", 2), fmt.Sprintf("page1_%064x", 3)}, 3, false},
		{"eight_collisions_ninth_unique", 2, []byte{1, 1, 1, 1, 1, 1, 1, 1, 2}, nil, 8, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			global := fmt.Sprintf("page1_%064x", 1)
			d := &DiscoveryService{tokens: map[string]*discoveryPage{global: &discoveryPage{}}}
			draws := 0
			draw := func(raw []byte) (int, error) {
				if draws >= len(tc.values) {
					t.Fatalf("unexpected entropy draw %d", draws+1)
				}
				for i := range raw {
					raw[i] = 0
				}
				raw[len(raw)-1] = tc.values[draws]
				draws++
				return len(raw), nil
			}
			got, err := d.readTokensWithDraw(tc.pages, draw)
			if draws != tc.wantDraws {
				t.Fatalf("draw bound: %d want%d", draws, tc.wantDraws)
			}
			if tc.busy {
				if DiscoveryErrorCode(err) != "busy" || got != nil {
					t.Fatalf("eight collisions admitted ninth token: %v %v", got, err)
				}
			} else {
				if err != nil || len(got) != len(tc.want) {
					t.Fatalf("token positive: %v %v", got, err)
				}
				for i := range got {
					if got[i] != tc.want[i] {
						t.Fatalf("token%d: %s want%s", i, got[i], tc.want[i])
					}
				}
			}
			if len(d.tokens) != 1 || d.tokens[global] == nil {
				t.Fatal("token planning mutated published global index")
			}
		})
	}
}

func TestSavedQueryReadDirectoryArchivePermissionsAndHistoryCutoffPlanner(t *testing.T) {
	f := newSavedReadFixture(t)
	var dir SavedQueryDirectoryResponse
	if err := json.Unmarshal(savedReadCall(t, f.d, f.reader, f.request("directory")), &dir); err != nil {
		t.Fatal(err)
	}
	if dir.MatchedCount != 1 || dir.CanCreate || dir.Items[0].Kind != "saved_query" || !dir.Items[0].Permissions.Admin {
		t.Fatalf("summary rights: %+v", dir)
	}
	f.version = f.mutate(t, 40, f.version, "archive", SetCollectionArchived{true})
	if err := json.Unmarshal(savedReadCall(t, f.d, f.reader, f.request("directory")), &dir); err != nil {
		t.Fatal(err)
	}
	if dir.MatchedCount != 0 {
		t.Fatal("archived query in active directory")
	}
	r := f.request("directory")
	r.State = "archived"
	if err := json.Unmarshal(savedReadCall(t, f.d, f.reader, r), &dir); err != nil {
		t.Fatal(err)
	}
	if dir.MatchedCount != 1 || !dir.Items[0].Archived {
		t.Fatal("archived directory")
	}
	savedReadDenied(t, f.d, f.reader, f.request("execute"), "not_found")
	_ = savedReadCall(t, f.d, f.reader, f.request("definition"))
	// Frozen output budget is independent of source scan/probe. A prospective
	// oversized prefix is refused without consuming the prior included planner.
	w := &savedQueryReadWindow{binding: savedQueryReadBinding{request: f.request("history")}, summary: dir.Items[0], definition: make([]byte, 8192)}
	p, err := newSavedReadPlanner(w, 1000, false, 151, 1000)
	if err != nil {
		t.Fatal(err)
	}
	accepted := 0
	for {
		next := p
		if !next.add(65536, 65536) {
			break
		}
		p = next
		accepted++
	}
	if accepted == 0 || accepted >= 1000 || p.cost() > discoveryBuildBytes {
		t.Fatalf("output cutoff not distinct: %d/%d", accepted, p.cost())
	}
}
