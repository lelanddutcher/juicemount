package catalog

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestRelationalReadsDetectCorruptAnchorMembership(t *testing.T) {
	s, _ := relationalStore(t)
	applyTest(t, s, relationalOperation("put"))
	// Deliberately bypass FK admission on this isolated fixture. Reads must not
	// promote an anchor merely because its stored string has a valid format.
	if _, err := s.db.Exec(`PRAGMA foreign_keys=OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE relationships SET target_revision=?`, rootRevision); err != nil {
		t.Fatal(err)
	}
	other := testManifest(otherAsset, testSHA)
	otherID, _ := other.RevisionID()
	if _, err := s.db.Exec(`UPDATE locations SET revision=?`, otherID); err != nil {
		t.Fatal(err)
	}
	for name, read := range map[string]func() error{
		"relationship": func() error { _, e := s.GetRelationship(context.Background(), testLibrary, "edge_scene_001"); return e },
		"location":     func() error { _, e := s.GetLocation(context.Background(), testLibrary, "location_scene_001"); return e },
		"relationship list": func() error {
			_, e := s.ListRelationships(context.Background(), testLibrary, testAsset, "", 10)
			return e
		},
		"location list": func() error { _, e := s.ListLocations(context.Background(), testLibrary, testAsset, "", 10); return e },
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(); !errors.Is(err, ErrFormat) {
				t.Fatalf("corrupt membership read: %v", err)
			}
		})
	}
}

func TestRelationalListsKeepAtomicPairsConsistent(t *testing.T) {
	s, path := relationalStore(t)
	rv, lv := testRelationshipValue(), testLocationValue()
	rv.Source, lv.Source = "0", "0"
	applyTest(t, s, testOperation("pairs",
		Change{PutRelationship: &PutRelationship{ID: "r1", Value: rv}}, Change{PutRelationship: &PutRelationship{ID: "r2", Value: rv}},
		Change{PutLocation: &PutLocation{ID: "l1", Value: lv}}, Change{PutLocation: &PutLocation{ID: "l2", Value: lv}}))
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := make(chan struct{})
	type result struct {
		count int
		err   error
	}
	written, read := make(chan result, 1), make(chan result, 1)
	go func() {
		<-start
		for i := 1; i <= 32; i++ {
			valueR, valueL := rv, lv
			valueR.Source, valueL.Source = fmt.Sprint(i), fmt.Sprint(i)
			previousR, previousL := rv, lv
			previousR.Source, previousL.Source = fmt.Sprint(i-1), fmt.Sprint(i-1)
			sequence := int64(i + 2)
			op := testOperation(fmt.Sprintf("edit-pairs-%d", i),
				Change{PutRelationship: &PutRelationship{ID: "r1", Expected: &RelationshipExpected{Value: previousR, UpdatedSequence: sequence}, Value: valueR}},
				Change{PutRelationship: &PutRelationship{ID: "r2", Expected: &RelationshipExpected{Value: previousR, UpdatedSequence: sequence}, Value: valueR}},
				Change{PutLocation: &PutLocation{ID: "l1", Expected: &LocationExpected{Value: previousL, UpdatedSequence: sequence}, Value: valueL}},
				Change{PutLocation: &PutLocation{ID: "l2", Expected: &LocationExpected{Value: previousL, UpdatedSequence: sequence}, Value: valueL}})
			if _, err := s.Apply(ctx, op); err != nil {
				written <- result{i - 1, err}
				return
			}
		}
		written <- result{32, nil}
	}()
	go func() {
		<-start
		for i := 0; i < 128; i++ {
			relationships, err := reader.ListRelationships(ctx, testLibrary, testAsset, "", 2)
			if err != nil {
				read <- result{i, err}
				return
			}
			if len(relationships) != 2 || relationships[0].Value.Source != relationships[1].Value.Source || relationships[0].UpdatedSequence != relationships[1].UpdatedSequence {
				read <- result{i, fmt.Errorf("mixed relationship pair: %+v", relationships)}
				return
			}
			locations, err := reader.ListLocations(ctx, testLibrary, testAsset, "", 2)
			if err != nil {
				read <- result{i, err}
				return
			}
			if len(locations) != 2 || locations[0].Value.Source != locations[1].Value.Source || locations[0].UpdatedSequence != locations[1].UpdatedSequence {
				read <- result{i, fmt.Errorf("mixed location pair: %+v", locations)}
				return
			}
		}
		read <- result{128, nil}
	}()
	close(start)
	w, r := <-written, <-read // Join both actors before reporting any failure.
	if w.err != nil || r.err != nil || w.count != 32 || r.count != 128 {
		t.Fatalf("writer %+v, reader %+v", w, r)
	}
	if got := getRelationship(t, reader, "r1"); got.Value.Source != "32" {
		t.Fatalf("final relationship state: %+v", got)
	}
	if got := getLocation(t, reader, "l1"); got.Value.Source != "32" {
		t.Fatalf("final location state: %+v", got)
	}
}
