package catalog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	s, err := Create(path, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, path
}

func applyTest(t *testing.T, s *Store, op Operation) Receipt {
	t.Helper()
	r, err := s.Apply(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCatalogRestartAndExactRetry(t *testing.T) {
	s, path := newTestStore(t)
	op := initialOperation()
	receipt := applyTest(t, s, op)
	if receipt.Sequence != 1 {
		t.Fatalf("first sequence = %d", receipt.Sequence)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("database mode: %v, %v", info, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if library, err := s.LibraryID(); err != nil || library != testLibrary {
		t.Fatalf("library: %s, %v", library, err)
	}
	a, err := s.GetAsset(context.Background(), testLibrary, testAsset)
	if err != nil || a.Head != rootRevision || a.Label != "original" || a.CreatedSequence != 1 || a.UpdatedSequence != 1 {
		t.Fatalf("asset: %+v, %v", a, err)
	}
	r, err := s.GetRevision(context.Background(), testLibrary, rootRevision)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := op.Changes[1].RecordRevision.Canonical()
	if !bytes.Equal(r.Canonical, want) || r.CreatedSequence != 1 {
		t.Fatalf("revision: %+v", r)
	}
	// Mutating a returned blob/manifest does not alter subsequent reads.
	r.Canonical[0] ^= 0xff
	r.Manifest.Content.Size++
	readAgain, err := s.GetRevision(context.Background(), testLibrary, rootRevision)
	if err != nil || !bytes.Equal(readAgain.Canonical, want) {
		t.Fatalf("revision aliases returned memory: %v", err)
	}
	retry := applyTest(t, s, initialOperation())
	if retry != receipt {
		t.Fatalf("retry receipt changed: %+v, want %+v", retry, receipt)
	}
	events, err := s.Events(context.Background(), testLibrary, 0, 100)
	if err != nil || len(events) != 1 || events[0].Receipt != receipt {
		t.Fatalf("events: %+v, %v", events, err)
	}
	events[0].Operation.Changes[0].CreateAsset.Label = "changed returned event"
	event, err := s.GetEvent(context.Background(), testLibrary, op.ID)
	if err != nil || event.Operation.Changes[0].CreateAsset.Label != "original" {
		t.Fatalf("event aliases returned memory: %v", err)
	}
}

func TestCatalogChangedRetryAndRevisionReplay(t *testing.T) {
	s, _ := newTestStore(t)
	first := applyTest(t, s, initialOperation())
	for name, mutate := range map[string]func(*Operation){
		"actor":         func(o *Operation) { o.Actor = "another-actor" },
		"label":         func(o *Operation) { o.Changes[0].CreateAsset.Label = "changed" },
		"parents":       func(o *Operation) { o.Changes[1].RecordRevision.Parents = []continuity.RevisionID{rootRevision} },
		"expected head": func(o *Operation) { o.Changes[2].AdvanceHead.ExpectedRevision = rootRevision },
	} {
		t.Run(name, func(t *testing.T) {
			op := initialOperation()
			mutate(&op)
			if _, err := s.Apply(context.Background(), op); !errors.Is(err, ErrChangedRequest) {
				t.Fatalf("changed retry: %v", err)
			}
		})
	}
	m := testManifest(testAsset, testSHA)
	second := applyTest(t, s, testOperation("repeat-declaration", Change{RecordRevision: &m}))
	if second.Sequence != first.Sequence+1 {
		t.Fatal("new operation did not append an event")
	}
	r, err := s.GetRevision(context.Background(), testLibrary, rootRevision)
	if err != nil || r.CreatedSequence != first.Sequence {
		t.Fatalf("immutable revision creation changed: %+v, %v", r, err)
	}
	// Retry must precede checking the now-stale initial-head precondition.
	if got := applyTest(t, s, initialOperation()); got != first {
		t.Fatal("retry lost original receipt")
	}
}

func TestCatalogBatchRollbackDoesNotReserveOperation(t *testing.T) {
	s, _ := newTestStore(t)
	op := initialOperation()
	op.Changes = append(op.Changes, Change{RenameAsset: &RenameAsset{AssetID: testAsset, ExpectedLabel: "wrong", Label: "new"}})
	if _, err := s.Apply(context.Background(), op); !errors.Is(err, ErrConflict) {
		t.Fatalf("batch: %v", err)
	}
	if _, err := s.GetAsset(context.Background(), testLibrary, testAsset); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial asset: %v", err)
	}
	if _, err := s.GetRevision(context.Background(), testLibrary, rootRevision); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial revision: %v", err)
	}
	if _, err := s.GetEvent(context.Background(), testLibrary, op.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed ID was reserved: %v", err)
	}
	events, err := s.Events(context.Background(), testLibrary, 0, 10)
	if err != nil || len(events) != 0 {
		t.Fatalf("partial event: %+v, %v", events, err)
	}
	if got := applyTest(t, s, initialOperation()); got.Sequence != 1 {
		t.Fatalf("failed batch consumed sequence: %+v", got)
	}
	// Label CAS is exact and failed batches do not change the prior projection.
	rename := testOperation("rename", Change{RenameAsset: &RenameAsset{AssetID: testAsset, ExpectedLabel: "original", Label: "renamed"}})
	applyTest(t, s, rename)
	rename.ID = "rename-stale"
	if _, err := s.Apply(context.Background(), rename); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale label: %v", err)
	}
	a, err := s.GetAsset(context.Background(), testLibrary, testAsset)
	if err != nil || a.Label != "renamed" || a.CreatedSequence != 1 || a.UpdatedSequence != 2 {
		t.Fatalf("label chronology: %+v, %v", a, err)
	}
}

func TestCatalogCompetingHeadCAS(t *testing.T) {
	s, path := newTestStore(t)
	applyTest(t, s, initialOperation())
	left := testManifest(testAsset, strings.Repeat("1", 64), rootRevision)
	right := testManifest(testAsset, strings.Repeat("2", 64), rootRevision)
	leftID, _ := left.RevisionID()
	rightID, _ := right.RevisionID()
	applyTest(t, s, testOperation("branches", Change{RecordRevision: &left}, Change{RecordRevision: &right}))
	a, err := s.GetAsset(context.Background(), testLibrary, testAsset)
	if err != nil || a.Head != rootRevision {
		t.Fatalf("recording branch moved head: %+v, %v", a, err)
	}
	other, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	type result struct {
		id  continuity.RevisionID
		err error
	}
	results := make(chan result, 2)
	for i, handle := range []*Store{s, other} {
		id := []continuity.RevisionID{leftID, rightID}[i]
		opID := []string{"left", "right"}[i]
		go func(store *Store, revision continuity.RevisionID, operationID string) {
			<-start
			_, err := store.Apply(ctx, testOperation(operationID, Change{AdvanceHead: &AdvanceHead{AssetID: testAsset, ExpectedRevision: rootRevision, Revision: revision}}))
			results <- result{revision, err}
		}(handle, id, opID)
	}
	close(start)
	var winner continuity.RevisionID
	success, conflict := 0, 0
	for range 2 {
		r := <-results // Both database calls are bounded by ctx/busy_timeout.
		if r.err == nil {
			success++
			winner = r.id
		} else if errors.Is(r.err, ErrConflict) {
			conflict++
		} else {
			t.Errorf("competing update: %v", r.err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("CAS outcomes: success=%d conflict=%d", success, conflict)
	}
	a, err = s.GetAsset(context.Background(), testLibrary, testAsset)
	if err != nil || a.Head != winner || a.UpdatedSequence != 3 {
		t.Fatalf("head: %+v, %v", a, err)
	}
	for _, id := range []continuity.RevisionID{leftID, rightID} {
		if _, err := s.GetRevision(context.Background(), testLibrary, id); err != nil {
			t.Fatal("separately recorded branch lost:", err)
		}
	}
	events, err := s.Events(context.Background(), testLibrary, 0, 10)
	if err != nil || len(events) != 3 {
		t.Fatalf("events: %d, %v", len(events), err)
	}
}

func TestCatalogRejectsWrongMembershipAndHeadLineage(t *testing.T) {
	s, _ := newTestStore(t)
	applyTest(t, s, initialOperation())
	otherRoot := testManifest(otherAsset, testSHA)
	otherID, _ := otherRoot.RevisionID()
	applyTest(t, s, testOperation("other-asset", Change{CreateAsset: &CreateAsset{AssetID: otherAsset}}, Change{RecordRevision: &otherRoot}))
	child := testManifest(testAsset, strings.Repeat("1", 64), rootRevision)
	childID, _ := child.RevisionID()
	applyTest(t, s, testOperation("child", Change{RecordRevision: &child}))
	missing := continuity.RevisionID("rev1_" + strings.Repeat("0", 64))
	for name, op := range map[string]Operation{
		"duplicate asset":          testOperation("duplicate", Change{CreateAsset: &CreateAsset{AssetID: testAsset}}),
		"wrong library":            {Version: 1, LibraryID: otherLibrary, ID: "wrong-lib", Actor: "owner-demo", Changes: []Change{{CreateAsset: &CreateAsset{AssetID: testAsset}}}},
		"cross asset parent":       testOperation("cross-parent", Change{RecordRevision: manifestPointer(testManifest(testAsset, strings.Repeat("3", 64), otherID))}),
		"missing parent":           testOperation("missing-parent", Change{RecordRevision: manifestPointer(testManifest(testAsset, strings.Repeat("3", 64), missing))}),
		"missing asset":            testOperation("missing-asset", Change{RecordRevision: manifestPointer(testManifest("asset_303132333435363738393a3b3c3d3e3f", testSHA))}),
		"wrong asset head":         testOperation("wrong-head", Change{AdvanceHead: &AdvanceHead{AssetID: testAsset, ExpectedRevision: rootRevision, Revision: otherID}}),
		"missing head":             testOperation("missing-head", Change{AdvanceHead: &AdvanceHead{AssetID: testAsset, ExpectedRevision: rootRevision, Revision: missing}}),
		"initial head has parents": testOperation("initial-child", Change{AdvanceHead: &AdvanceHead{AssetID: testAsset, Revision: childID}}),
		"not direct parent":        testOperation("nonparent", Change{AdvanceHead: &AdvanceHead{AssetID: testAsset, ExpectedRevision: childID, Revision: rootRevision}}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.Apply(context.Background(), op); err == nil {
				t.Fatal("accepted invalid membership/state")
			}
		})
	}
	events, err := s.Events(context.Background(), testLibrary, 0, 10)
	if err != nil || len(events) != 3 {
		t.Fatalf("rejected operations appended events: %d, %v", len(events), err)
	}
}

func manifestPointer(m continuity.Manifest) *continuity.Manifest { return &m }

func TestCatalogParentReplayAndDetachedRecords(t *testing.T) {
	s, _ := newTestStore(t)
	applyTest(t, s, initialOperation())
	branch := testManifest(testAsset, strings.Repeat("1", 64))
	branchID, _ := branch.RevisionID()
	applyTest(t, s, testOperation("other-root", Change{RecordRevision: &branch}))
	m := testManifest(testAsset, strings.Repeat("2", 64), rootRevision, branchID)
	op := testOperation("merge", Change{RecordRevision: &m})
	parents := append([]continuity.RevisionID(nil), m.Parents...)
	receipt := applyTest(t, s, op)
	if !reflect.DeepEqual(parents, m.Parents) {
		t.Fatal("Apply mutated parents")
	}
	m.Parents[0], m.Parents[1] = m.Parents[1], m.Parents[0]
	if retry := applyTest(t, s, op); retry != receipt {
		t.Fatal("parent order changed retry")
	}
	id, _ := m.RevisionID()
	r, err := s.GetRevision(context.Background(), testLibrary, id)
	if err != nil {
		t.Fatal(err)
	}
	r.Manifest.Parents[0] = rootRevision
	r.Canonical[0] = 0
	event, err := s.GetEvent(context.Background(), testLibrary, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	event.Operation.Changes[0].RecordRevision.Parents[0] = "rev1_bad"
	if _, err := s.GetRevision(context.Background(), testLibrary, id); err != nil {
		t.Fatal("returned revision aliased:", err)
	}
	if retry := applyTest(t, s, op); retry != receipt {
		t.Fatal("returned event aliased")
	}
}

func TestCatalogPaginationAndReadValidation(t *testing.T) {
	s, _ := newTestStore(t)
	applyTest(t, s, initialOperation())
	applyTest(t, s, testOperation("second", Change{CreateAsset: &CreateAsset{AssetID: otherAsset}}))
	assets, err := s.ListAssets(context.Background(), testLibrary, "", 1)
	if err != nil || len(assets) != 1 || assets[0].ID != testAsset {
		t.Fatalf("first page: %+v, %v", assets, err)
	}
	assets, err = s.ListAssets(context.Background(), testLibrary, testAsset, 1)
	if err != nil || len(assets) != 1 || assets[0].ID != otherAsset {
		t.Fatalf("next page: %+v, %v", assets, err)
	}
	revisions, err := s.ListRevisions(context.Background(), testLibrary, testAsset, "", 1)
	if err != nil || len(revisions) != 1 || revisions[0].ID != rootRevision {
		t.Fatalf("revisions: %+v, %v", revisions, err)
	}
	revisions, err = s.ListRevisions(context.Background(), testLibrary, testAsset, rootRevision, 1)
	if err != nil || len(revisions) != 0 {
		t.Fatalf("revision cursor: %+v, %v", revisions, err)
	}
	events, err := s.Events(context.Background(), testLibrary, 1, 1)
	if err != nil || len(events) != 1 || events[0].Receipt.Sequence != 2 {
		t.Fatalf("event cursor: %+v, %v", events, err)
	}
	for name, read := range map[string]func() error{
		"wrong library":    func() error { _, e := s.GetAsset(context.Background(), otherLibrary, testAsset); return e },
		"bad asset":        func() error { _, e := s.GetAsset(context.Background(), testLibrary, "asset_bad"); return e },
		"bad revision":     func() error { _, e := s.GetRevision(context.Background(), testLibrary, "rev_bad"); return e },
		"bad operation":    func() error { _, e := s.GetEvent(context.Background(), testLibrary, "bad/id"); return e },
		"zero page":        func() error { _, e := s.ListAssets(context.Background(), testLibrary, "", 0); return e },
		"oversized page":   func() error { _, e := s.Events(context.Background(), testLibrary, 0, 1001); return e },
		"bad asset cursor": func() error { _, e := s.ListAssets(context.Background(), testLibrary, "bad", 1); return e },
		"bad revision cursor": func() error {
			_, e := s.ListRevisions(context.Background(), testLibrary, testAsset, "bad", 1)
			return e
		},
		"negative sequence": func() error { _, e := s.Events(context.Background(), testLibrary, -1, 1); return e },
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid read: %v", err)
			}
		})
	}
}
