package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func relationalStore(t *testing.T) (*Store, string) {
	t.Helper()
	s, path := newTestStore(t)
	applyTest(t, s, initialOperation())
	other := testManifest(otherAsset, testSHA)
	applyTest(t, s, testOperation("other", Change{CreateAsset: &CreateAsset{AssetID: otherAsset}}, Change{RecordRevision: &other}))
	return s, path
}

func relationshipState(r Relationship) *RelationshipExpected {
	return &RelationshipExpected{Value: r.Value, UpdatedSequence: r.UpdatedSequence, Removed: r.Removed}
}

func locationState(l Location) *LocationExpected {
	return &LocationExpected{Value: l.Value, UpdatedSequence: l.UpdatedSequence, Removed: l.Removed}
}

func getRelationship(t *testing.T, s *Store, id string) Relationship {
	t.Helper()
	r, err := s.GetRelationship(context.Background(), testLibrary, id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func getLocation(t *testing.T, s *Store, id string) Location {
	t.Helper()
	l, err := s.GetLocation(context.Background(), testLibrary, id)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestRelationalExpectedFullValueCAS(t *testing.T) {
	s, _ := relationalStore(t)
	applyTest(t, s, relationalOperation("put"))
	r := getRelationship(t, s, "edge_scene_001")
	l := getLocation(t, s, "location_scene_001")
	for _, kind := range []string{"relationship", "location"} {
		t.Run(kind, func(t *testing.T) {
			var c Change
			if kind == "relationship" {
				expected := relationshipState(r)
				expected.Value.Predicate = "forged expected value" // Sequence and Removed are exact.
				c.PutRelationship = &PutRelationship{ID: r.ID, Expected: expected, Value: r.Value}
			} else {
				expected := locationState(l)
				expected.Value.Locator = "forged expected value"
				c.PutLocation = &PutLocation{ID: l.ID, Expected: expected, Value: l.Value}
			}
			if _, err := s.Apply(context.Background(), testOperation("forged-"+kind, c)); !errors.Is(err, ErrConflict) {
				t.Fatalf("full-value CAS: %v", err)
			}
		})
	}
	wrongR, wrongL := relationshipState(r), locationState(l)
	wrongR.Removed, wrongL.Removed = true, true
	for name, c := range map[string]Change{
		"relationship": {PutRelationship: &PutRelationship{ID: r.ID, Expected: wrongR, Value: r.Value}},
		"location":     {PutLocation: &PutLocation{ID: l.ID, Expected: wrongL, Value: l.Value}},
	} {
		if _, err := s.Apply(context.Background(), testOperation("wrong-removed-"+name, c)); !errors.Is(err, ErrConflict) {
			t.Fatalf("removed-state CAS %s: %v", name, err)
		}
	}
}

func TestRelationalExpectedSequenceRejectsABA(t *testing.T) {
	s, _ := relationalStore(t)
	applyTest(t, s, relationalOperation("put"))
	r := getRelationship(t, s, "edge_scene_001")
	l := getLocation(t, s, "location_scene_001")
	rB, lB := r.Value, l.Value
	rB.Source, lB.Source = "B", "B"
	applyTest(t, s, testOperation("edit-B", Change{PutRelationship: &PutRelationship{ID: r.ID, Expected: relationshipState(r), Value: rB}}, Change{PutLocation: &PutLocation{ID: l.ID, Expected: locationState(l), Value: lB}}))
	currentR, currentL := getRelationship(t, s, r.ID), getLocation(t, s, l.ID)
	applyTest(t, s, testOperation("edit-back-A", Change{PutRelationship: &PutRelationship{ID: r.ID, Expected: relationshipState(currentR), Value: r.Value}}, Change{PutLocation: &PutLocation{ID: l.ID, Expected: locationState(currentL), Value: l.Value}}))
	for name, c := range map[string]Change{
		"relationship": {PutRelationship: &PutRelationship{ID: r.ID, Expected: relationshipState(r), Value: rB}},
		"location":     {PutLocation: &PutLocation{ID: l.ID, Expected: locationState(l), Value: lB}},
	} {
		if _, err := s.Apply(context.Background(), testOperation("ABA-"+name, c)); !errors.Is(err, ErrConflict) {
			t.Fatalf("ABA %s: %v", name, err)
		}
	}
	if got := getRelationship(t, s, r.ID); got.Value != r.Value || got.UpdatedSequence != 5 || got.CreatedSequence != 3 {
		t.Fatalf("relationship ABA state: %+v", got)
	}
	if got := getLocation(t, s, l.ID); got.Value != l.Value || got.UpdatedSequence != 5 || got.CreatedSequence != 3 {
		t.Fatalf("location ABA state: %+v", got)
	}
}

func TestRelationalTombstonesReactivationAndLifetimeOwner(t *testing.T) {
	s, _ := relationalStore(t)
	op := relationalOperation("put")
	first := applyTest(t, s, op)
	r, l := getRelationship(t, s, "edge_scene_001"), getLocation(t, s, "location_scene_001")
	remove := testOperation("remove", Change{RemoveRelationship: &RemoveRelationship{ID: r.ID, Expected: relationshipState(r)}}, Change{RemoveLocation: &RemoveLocation{ID: l.ID, Expected: locationState(l)}})
	receipt := applyTest(t, s, remove)
	rTomb, lTomb := getRelationship(t, s, r.ID), getLocation(t, s, l.ID)
	if !rTomb.Removed || !lTomb.Removed || rTomb.Value != r.Value || lTomb.Value != l.Value || rTomb.CreatedSequence != first.Sequence || lTomb.CreatedSequence != first.Sequence || rTomb.UpdatedSequence != receipt.Sequence || lTomb.UpdatedSequence != receipt.Sequence {
		t.Fatal("tombstone discarded identity/value/chronology")
	}
	if retry := applyTest(t, s, remove); retry != receipt {
		t.Fatal("removal retry changed receipt")
	}
	for name, c := range map[string]Change{
		"relationship absence":        {PutRelationship: &PutRelationship{ID: r.ID, Value: r.Value}},
		"location absence":            {PutLocation: &PutLocation{ID: l.ID, Value: l.Value}},
		"relationship double removal": {RemoveRelationship: &RemoveRelationship{ID: r.ID, Expected: relationshipState(rTomb)}},
		"location double removal":     {RemoveLocation: &RemoveLocation{ID: l.ID, Expected: locationState(lTomb)}},
	} {
		if _, err := s.Apply(context.Background(), testOperation(strings.ReplaceAll(name, " ", "-"), c)); !errors.Is(err, ErrConflict) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	other := l.Value
	other.AssetID, other.Revision = otherAsset, ""
	if _, err := s.Apply(context.Background(), testOperation("reassign-tombstone", Change{PutLocation: &PutLocation{ID: l.ID, Expected: locationState(lTomb), Value: other}})); !errors.Is(err, ErrConflict) {
		t.Fatalf("tombstone location reassigned: %v", err)
	}
	rels, err := s.ListRelationships(context.Background(), testLibrary, testAsset, "", 10)
	if err != nil || len(rels) != 0 {
		t.Fatalf("listed tombstone edge: %+v, %v", rels, err)
	}
	locs, err := s.ListLocations(context.Background(), testLibrary, testAsset, "", 10)
	if err != nil || len(locs) != 0 {
		t.Fatalf("listed tombstone location: %+v, %v", locs, err)
	}
	corrected := r.Value
	corrected.SourceAssetID, corrected.SourceRevision = otherAsset, ""
	corrected.TargetAssetID, corrected.TargetRevision = otherAsset, ""
	applyTest(t, s, testOperation("reactivate", Change{PutRelationship: &PutRelationship{ID: r.ID, Expected: relationshipState(rTomb), Value: corrected}}, Change{PutLocation: &PutLocation{ID: l.ID, Expected: locationState(lTomb), Value: l.Value}}))
	if got := getRelationship(t, s, r.ID); got.Removed || got.Value != corrected || got.CreatedSequence != first.Sequence {
		t.Fatalf("reactivation: %+v", got)
	}
	if got := getLocation(t, s, l.ID); got.Removed || got.Value.AssetID != testAsset || got.CreatedSequence != first.Sequence {
		t.Fatalf("reactivation: %+v", got)
	}
	// Historical endpoints and attribution remain in immutable event bytes.
	event, err := s.GetEvent(context.Background(), testLibrary, "put")
	if err != nil || event.Operation.Changes[0].PutRelationship.Value != r.Value {
		t.Fatalf("history changed: %+v, %v", event, err)
	}
	event.Operation.Changes[0].PutRelationship.Value.Source = "changed returned event"
	if retry := applyTest(t, s, op); retry != first {
		t.Fatal("input/event mutation changed retry")
	}
}

func TestRelationalMembershipRetryAndAtomicRollback(t *testing.T) {
	s, _ := relationalStore(t)
	put := relationalOperation("put")
	applyTest(t, s, put)
	put.Changes[0].PutRelationship.Value.Source = "changed actor claim"
	if _, err := s.Apply(context.Background(), put); !errors.Is(err, ErrChangedRequest) {
		t.Fatalf("changed relationship retry: %v", err)
	}
	otherRoot := testManifest(otherAsset, testSHA)
	otherRevision, _ := otherRoot.RevisionID()
	for name, change := range map[string]func(*Operation){
		"missing endpoint": func(o *Operation) {
			o.Changes[0].PutRelationship.Value.TargetAssetID = "asset_303132333435363738393a3b3c3d3e3f"
		},
		"wrong source anchor":   func(o *Operation) { o.Changes[0].PutRelationship.Value.SourceRevision = otherRevision },
		"wrong target anchor":   func(o *Operation) { o.Changes[0].PutRelationship.Value.TargetRevision = rootRevision },
		"wrong location anchor": func(o *Operation) { o.Changes[1].PutLocation.Value.Revision = otherRevision },
	} {
		t.Run(name, func(t *testing.T) {
			op := relationalOperation(strings.ReplaceAll(name, " ", "-"))
			op.Changes[0].PutRelationship.ID = "new-r"
			op.Changes[1].PutLocation.ID = "new-l"
			change(&op)
			if _, err := s.Apply(context.Background(), op); err == nil {
				t.Fatal("accepted wrong membership")
			}
		})
	}
	const third continuity.AssetID = "asset_303132333435363738393a3b3c3d3e3f"
	m := testManifest(third, testSHA)
	mID, _ := m.RevisionID()
	rv := testRelationshipValue()
	rv.SourceAssetID, rv.SourceRevision = third, mID
	lv := LocationValue{AssetID: third, Revision: mID, Source: "input", Locator: "opaque:never-open-this"}
	op := testOperation("mixed", Change{CreateAsset: &CreateAsset{AssetID: third}}, Change{RecordRevision: &m},
		Change{PutRelationship: &PutRelationship{ID: "mixed-r", Value: rv}}, Change{PutLocation: &PutLocation{ID: "mixed-l", Value: lv}},
		Change{RenameAsset: &RenameAsset{AssetID: testAsset, ExpectedLabel: "wrong", Label: "new"}})
	if _, err := s.Apply(context.Background(), op); !errors.Is(err, ErrConflict) {
		t.Fatalf("mixed batch: %v", err)
	}
	if _, err := s.GetAsset(context.Background(), testLibrary, third); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial asset: %v", err)
	}
	if _, err := s.GetRevision(context.Background(), testLibrary, mID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial revision: %v", err)
	}
	if _, err := s.GetRelationship(context.Background(), testLibrary, "mixed-r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial edge: %v", err)
	}
	if _, err := s.GetLocation(context.Background(), testLibrary, "mixed-l"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial location: %v", err)
	}
	op.Changes[4].RenameAsset.ExpectedLabel = "original"
	if receipt := applyTest(t, s, op); receipt.Sequence != 4 {
		t.Fatalf("failed request reserved event: %+v", receipt)
	}
}

func TestRelationalCompetingStores(t *testing.T) {
	s, path := relationalStore(t)
	applyTest(t, s, relationalOperation("put"))
	r, l := getRelationship(t, s, "edge_scene_001"), getLocation(t, s, "location_scene_001")
	other, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	type result struct {
		name string
		err  error
	}
	results := make(chan result, 2)
	for i, handle := range []*Store{s, other} {
		go func(handle *Store, i int) {
			<-start
			name := fmt.Sprintf("writer-%d", i)
			rv, lv := r.Value, l.Value
			rv.Source, lv.Source = name, name
			_, err := handle.Apply(ctx, testOperation(name, Change{PutRelationship: &PutRelationship{ID: r.ID, Expected: relationshipState(r), Value: rv}}, Change{PutLocation: &PutLocation{ID: l.ID, Expected: locationState(l), Value: lv}}))
			results <- result{name, err}
		}(handle, i)
	}
	close(start)
	success, conflict := 0, 0
	winner := ""
	for range 2 {
		got := <-results
		if got.err == nil {
			success++
			winner = got.name
		} else if errors.Is(got.err, ErrConflict) {
			conflict++
		} else {
			t.Errorf("writer: %v", got.err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
	if got := getRelationship(t, s, r.ID); got.Value.Source != winner || got.UpdatedSequence != 4 {
		t.Fatalf("edge: %+v", got)
	}
	if got := getLocation(t, s, l.ID); got.Value.Source != winner || got.UpdatedSequence != 4 {
		t.Fatalf("location: %+v", got)
	}
}

func TestRelationalListsDirectedSelfEdgesAndValidation(t *testing.T) {
	s, _ := relationalStore(t)
	out := testRelationshipValue()
	in, self := out, out
	in.SourceAssetID, in.SourceRevision, in.TargetAssetID, in.TargetRevision = otherAsset, "", testAsset, rootRevision
	self.TargetAssetID, self.TargetRevision = testAsset, rootRevision
	applyTest(t, s, testOperation("edges", Change{PutRelationship: &PutRelationship{ID: "a-in", Value: in}}, Change{PutRelationship: &PutRelationship{ID: "b-out", Value: out}}, Change{PutRelationship: &PutRelationship{ID: "c-self", Value: self}},
		Change{PutLocation: &PutLocation{ID: "a-in", Value: testLocationValue()}})) // Separate ID namespaces.
	rels, err := s.ListRelationships(context.Background(), testLibrary, testAsset, "", 10)
	if err != nil || len(rels) != 3 {
		t.Fatalf("directed/self list: %+v, %v", rels, err)
	}
	page, err := s.ListRelationships(context.Background(), testLibrary, testAsset, "a-middle", 1)
	if err != nil || len(page) != 1 || page[0].ID != "b-out" {
		t.Fatalf("exclusive absent cursor: %+v, %v", page, err)
	}
	locs, err := s.ListLocations(context.Background(), testLibrary, testAsset, "", 1)
	if err != nil || len(locs) != 1 || locs[0].ID != "a-in" {
		t.Fatalf("location list: %+v, %v", locs, err)
	}
	for name, read := range map[string]func() error{
		"wrong library":        func() error { _, e := s.GetRelationship(context.Background(), otherLibrary, "a-in"); return e },
		"invalid relationship": func() error { _, e := s.GetRelationship(context.Background(), testLibrary, "bad/id"); return e },
		"invalid location":     func() error { _, e := s.GetLocation(context.Background(), testLibrary, ""); return e },
		"zero page": func() error {
			_, e := s.ListRelationships(context.Background(), testLibrary, testAsset, "", 0)
			return e
		},
		"oversized page": func() error {
			_, e := s.ListLocations(context.Background(), testLibrary, testAsset, "", 1001)
			return e
		},
		"bad cursor": func() error {
			_, e := s.ListLocations(context.Background(), testLibrary, testAsset, "bad/key", 1)
			return e
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid read: %v", err)
			}
		})
	}
	if _, err := s.GetRelationship(context.Background(), testLibrary, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing identity: %v", err)
	}
	if _, err := s.ListRelationships(context.Background(), testLibrary, "asset_303132333435363738393a3b3c3d3e3f", "", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing selected asset: %v", err)
	}
}

func TestRelationalSQLIdentityAndDeleteGuards(t *testing.T) {
	s, _ := relationalStore(t)
	applyTest(t, s, relationalOperation("put"))
	for name, statement := range map[string]string{
		"relationship delete":       `DELETE FROM relationships`,
		"location delete":           `DELETE FROM locations`,
		"relationship id":           `UPDATE relationships SET relationship_id='different'`,
		"location id":               `UPDATE locations SET location_id='different'`,
		"relationship creation":     `UPDATE relationships SET created_sequence=1`,
		"location creation":         `UPDATE locations SET created_sequence=1`,
		"relationship library":      `UPDATE relationships SET library_id='lib_202122232425262728292a2b2c2d2e2f'`,
		"location library":          `UPDATE locations SET library_id='lib_202122232425262728292a2b2c2d2e2f'`,
		"location owner":            `UPDATE locations SET asset_id='asset_202122232425262728292a2b2c2d2e2f',revision=NULL`,
		"relationship provenance":   `UPDATE relationships SET provenance='authenticated'`,
		"location boolean":          `UPDATE locations SET removed=2`,
		"relationship wrong anchor": `UPDATE relationships SET target_revision='rev1_08360b41fa64c32ba1decf26ca750f4d74748870d2b3b9c8b682f22b8ca51b2b'`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.db.Exec(statement); err == nil {
				t.Fatal("SQL invariant bypassed")
			}
		})
	}
	r, l := getRelationship(t, s, "edge_scene_001"), getLocation(t, s, "location_scene_001")
	applyTest(t, s, testOperation("remove", Change{RemoveRelationship: &RemoveRelationship{ID: r.ID, Expected: relationshipState(r)}}, Change{RemoveLocation: &RemoveLocation{ID: l.ID, Expected: locationState(l)}}))
	if _, err := s.db.Exec(`DELETE FROM locations`); err == nil {
		t.Fatal("SQL deleted lifetime owner tombstone")
	}
	if _, err := s.db.Exec(`DELETE FROM relationships`); err == nil {
		t.Fatal("SQL deleted relationship tombstone")
	}
	if _, err := s.db.Exec(`UPDATE locations SET asset_id=?,revision=NULL`, otherAsset); err == nil {
		t.Fatal("SQL reassigned removed location owner")
	}
	if err := checkIntegrity(context.Background(), s.db); err != nil {
		t.Fatal(err)
	}
}
