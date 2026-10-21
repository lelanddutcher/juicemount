package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TEMP triggers are connection local. Store's one-connection write pool keeps
// the deferred constraint on the actual replay transaction, including Commit.
// This exercises the same seam as TestCollectionTransactionRollbackIncludesGeneratedPolicy.
func TestCollectionPortabilityReplayDeferredFKCommitRollback(t *testing.T) {
	f := readCollectionReplayFixture(t)
	for _, tc := range []struct {
		name, table     string
		prefix, ordinal int64
	}{
		{"legacy_policy", "policy_events", 1, 2},
		{"owner_policy", "collection_policy_events", 2, 3},
		{"content_parent", "collection_events", 3, 4},
		{"generated_child_result", "collection_policy_results", 3, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, rows := replayCollectionFixturePrefix(t, f, tc.prefix)
			before := collectionReplayState(t, s)
			for _, statement := range []string{
				`CREATE TEMP TABLE portability_fk_parent(id INTEGER PRIMARY KEY)`,
				`CREATE TEMP TABLE portability_fk_child(id INTEGER REFERENCES portability_fk_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
				`CREATE TEMP TRIGGER portability_fk_fault AFTER INSERT ON main.` + tc.table + ` BEGIN INSERT INTO portability_fk_child VALUES(1); END`,
			} {
				if _, err := s.db.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			next, err := replayCollectionFixtureFrame(t, s, collectionFixtureOrdinal(t, f, tc.ordinal), rows, MaxStreamProjectionRows)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "foreign key") || next != 0 || tc.ordinal != 2 && !strings.Contains(err.Error(), "commit uncertain") {
				t.Fatalf("real deferred replay Commit: rows=%d error=%v", next, err)
			}
			if !bytes.Equal(before, collectionReplayState(t, s)) || collectionReplayRowCount(t, s) != rows {
				t.Fatal("Commit failure retained part of parent, child, results, generations, quotas, or order")
			}
			var children, ready int
			if err := s.db.QueryRow(`SELECT count(*) FROM portability_fk_child`).Scan(&children); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT ready FROM catalog WHERE singleton=1`).Scan(&ready); err != nil {
				t.Fatal(err)
			}
			if children != 0 || ready != 0 || s.db.Stats().InUse != 0 {
				t.Fatalf("failed replay cleanup: children=%d ready=%d pool=%+v", children, ready, s.db.Stats())
			}
			if _, err := s.db.Exec(`DROP TRIGGER portability_fk_fault`); err != nil {
				t.Fatal(err)
			}
			next, err = replayCollectionFixtureFrame(t, s, collectionFixtureOrdinal(t, f, tc.ordinal), rows, MaxStreamProjectionRows)
			if err != nil {
				t.Fatalf("unchanged record after removing only fault trigger: %v", err)
			}
			assertCollectionReplaySnapshot(t, s, next, f.Snapshots[tc.ordinal-1])
		})
	}
}

func collectionFaultFirstContentEnd(t *testing.T, f collectionWireFixture) int {
	t.Helper()
	offset := len(streamMagic3)
	for _, frame := range f.Frames {
		offset += len(collectionFixtureHex(t, frame.FrameHex))
		if frame.Kind == 7 {
			return offset
		}
	}
	t.Fatal("fixture has no content frame")
	return 0
}

func TestCollectionPortabilityMidstreamRestoreCancellation(t *testing.T) {
	f := readCollectionWireFixture(t, "shared")
	wire := collectionFixtureHex(t, f.StreamHex)
	// C1 and its generated child commit together. C2 is cut either before its
	// header or inside its payload; neither cancellation may lose C1 or admit C2.
	first := collectionFaultFirstContentEnd(t, f)
	for _, cut := range []int{first, first + 6} {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			prefix := bytes.NewReader(wire[:cut])
			cancellations := 0
			reader := portReadFunc(func(p []byte) (int, error) {
				if prefix.Len() == 0 {
					cancellations++
					cancel()
					return 0, context.Canceled
				}
				return prefix.Read(p)
			})
			path := filepath.Join(t.TempDir(), "cancel.sqlite")
			if err := Restore(ctx, path, f.LibraryID, reader, collectionWireLimits()); !errors.Is(err, context.Canceled) {
				t.Fatalf("midstream cancellation lost: %v", err)
			}
			if cancellations != 1 || prefix.Len() != 0 {
				t.Fatalf("cancellation boundary: calls=%d remaining=%d", cancellations, prefix.Len())
			}
			assertIncomplete(t, path)
			db, err := openDB(path, true)
			if err != nil {
				t.Fatal(err)
			}
			view := &Store{db: db}
			expected := readCollectionReplayFixture(t).Snapshots[3]
			assertCollectionReplaySnapshot(t, view, collectionReplayRowCount(t, view), expected)
			before := collectionReplayState(t, view)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			// The failed call closed its owner. A second call still must not adopt the
			// reserved incomplete destination, even with a complete valid artifact.
			if err := Restore(context.Background(), path, f.LibraryID, bytes.NewReader(wire), collectionWireLimits()); !errors.Is(err, ErrConflict) {
				t.Fatalf("adopted canceled target: %v", err)
			}
			db, err = openDB(path, true)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if !bytes.Equal(before, collectionReplayState(t, &Store{db: db})) {
				t.Fatal("retry altered incomplete target")
			}
		})
	}
}

func TestCollectionPortabilityMidstreamExportCancellation(t *testing.T) {
	f := readCollectionWireFixture(t, "shared")
	wire := collectionFixtureHex(t, f.StreamHex)
	s := collectionProjectionFixtureStore(t, readCollectionProjectionFixture(t, "shared"))
	if err := changedOne(s.db.Exec(`UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0`)); err != nil {
		t.Fatal(err)
	}
	stop := collectionFaultFirstContentEnd(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out bytes.Buffer
	calls := 0
	writer := portWriteFunc(func(p []byte) (int, error) {
		calls++
		n, err := out.Write(p)
		if out.Len() >= stop {
			cancel()
		}
		return n, err
	})
	if err := s.Export(ctx, f.LibraryID, writer, collectionWireLimits()); !errors.Is(err, context.Canceled) {
		t.Fatalf("midstream export cancellation: %v", err)
	}
	if calls < 3 || !bytes.Equal(out.Bytes(), wire[:stop]) || s.db.Stats().InUse != 0 {
		t.Fatalf("canceled export leaked resources or crossed boundary: calls=%d bytes=%d pool=%+v", calls, out.Len(), s.db.Stats())
	}
	// Reacquiring and completing the same real read path proves the canceled
	// snapshot did not strand SQL resources or the Store admission lock.
	var retry bytes.Buffer
	if err := s.Export(context.Background(), f.LibraryID, &retry, collectionWireLimits()); err != nil || !bytes.Equal(retry.Bytes(), wire) {
		t.Fatalf("export after cancellation: %v", err)
	}
}

func TestCollectionPortabilityHeldOwnerLegacySnapshotAndCloseDrain(t *testing.T) {
	s, owner, authority, d, _, _ := collectionTxStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	limits := Limits{8 << 20, 100, 1000}
	var before bytes.Buffer
	if err := s.Export(ctx, testLibrary, &before, limits); err != nil {
		t.Fatal(err)
	}
	gate := &snapshotGateWriter{ctx: ctx, reached: make(chan struct{}), release: make(chan struct{})}
	exported := make(chan error, 1)
	go func() { exported <- s.Export(ctx, testLibrary, gate, limits) }()
	select {
	case <-gate.reached:
	case <-ctx.Done():
		close(gate.release)
		<-exported
		t.Fatal("format3 export did not select snapshot")
	}
	// Both writers go through real owner APIs while the export owns its exact
	// read snapshot. They produce two distinct mixed-ledger ordinals.
	value := CollectionGrantValue{readerSubject, CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	policy := collectionTxPolicy(601, "", "library_create", CollectionGrantChange{"put", "cgr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil, &value})
	_, ownerErr := owner.ApplyCollectionPolicy(ctx, collectionTxOwner(t, authority, policy))
	subject := SubjectID("sub_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	legacy := PolicyOperation{1, testLibrary, "held-legacy-create", "local_owner", authority.OwnerSubjectID, []PolicyChange{NewPolicyChange("create_subject", CreateSubject{subject, SubjectValue{"person", "after snapshot", true}})}}
	_, legacyErr := owner.ApplyPolicy(ctx, adminTestRequest(legacy.ID, authority.Incarnation, "apply_policy", ApplyPolicyRequest{legacy}))
	serviceErr := d.Close(ctx)
	attempted := make(chan struct{})
	closed := make(chan error, 1)
	go func() { close(attempted); closed <- s.Close() }()
	<-attempted
	pending, early := false, false
	var closeErr error
	for !pending && !early && ctx.Err() == nil {
		if !s.mu.TryRLock() {
			pending = true
			break
		}
		s.mu.RUnlock()
		select {
		case closeErr = <-closed:
			early = true
		default:
			runtime.Gosched()
		}
	}
	close(gate.release)
	exportErr := <-exported
	if !early {
		closeErr = <-closed
	}
	if ownerErr != nil || legacyErr != nil || serviceErr != nil || !pending || early || exportErr != nil || closeErr != nil {
		t.Fatalf("held owner/legacy/Close: owner=%v legacy=%v service=%v pending=%v early=%v export=%v close=%v", ownerErr, legacyErr, serviceErr, pending, early, exportErr, closeErr)
	}
	if !bytes.Equal(gate.buffer.Bytes(), before.Bytes()) {
		t.Fatal("format3 export mixed owner/legacy writes into held snapshot")
	}
	if _, err := s.LibraryID(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Close did not drain: %v", err)
	}
	fresh, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	var grants, subjects int
	if err := fresh.db.QueryRow(`SELECT (SELECT count(*) FROM collection_grants WHERE grant_id=?),(SELECT count(*) FROM policy_subjects WHERE subject_id=?)`, "cgr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", subject).Scan(&grants, &subjects); err != nil {
		t.Fatal(err)
	}
	if grants != 1 || subjects != 1 {
		t.Fatalf("concurrent writes did not commit: grant=%d subject=%d", grants, subjects)
	}
	path := filepath.Join(t.TempDir(), "held.sqlite")
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(gate.buffer.Bytes()), limits); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.db.QueryRow(`SELECT (SELECT count(*) FROM collection_grants WHERE grant_id=?),(SELECT count(*) FROM policy_subjects WHERE subject_id=?)`, "cgr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", subject).Scan(&grants, &subjects); err != nil {
		t.Fatal(err)
	}
	if grants != 0 || subjects != 0 {
		t.Fatal("held snapshot retained later policy state")
	}
	var roundtrip bytes.Buffer
	if err := restored.Export(context.Background(), testLibrary, &roundtrip, limits); err != nil || !bytes.Equal(roundtrip.Bytes(), before.Bytes()) {
		t.Fatalf("held artifact not independently restorable: %v", err)
	}
}

// This is a real Store.Close failure and sticky cleanup assertion, not evidence
// for the Restore-owned final Close branch (which has no injectable owner seam).
func TestCollectionPortabilityIncompleteStoreCloseErrorIsSticky(t *testing.T) {
	f := readCollectionWireFixture(t, "empty")
	path := filepath.Join(t.TempDir(), "close.sqlite")
	s, err := openStoreVersionContext(context.Background(), path, f.LibraryID, true, false, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.file.Close(); err != nil {
		t.Fatal(err)
	}
	first := s.Close()
	second := s.Close()
	if !errors.Is(first, os.ErrClosed) || first != second {
		t.Fatalf("Close error not retained: first=%v second=%v", first, second)
	}
	if s.db.Stats().OpenConnections != 0 {
		t.Fatal("file Close error skipped owned SQL pool cleanup")
	}
	if _, err := s.LibraryID(); !errors.Is(err, ErrClosed) {
		t.Fatalf("failed Close did not close admission: %v", err)
	}
	assertIncomplete(t, path)
}
