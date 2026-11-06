package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// withRestoreBatch makes every replayed event its own batch for tests that
// observe per-event commit boundaries.
func withRestoreBatch(t *testing.T, events int) {
	t.Helper()
	prior := restoreBatchEvents
	restoreBatchEvents = events
	t.Cleanup(func() { restoreBatchEvents = prior })
}

// seedEvents records count single-asset events in one transaction, with fixed
// receipts, so a large history is cheap to build.
func seedEvents(t *testing.T, s *Store, count int) {
	t.Helper()
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 1; i <= count; i++ {
		asset := continuity.AssetID(fmt.Sprintf("asset_%032x", i))
		op, request, digest, err := freezeOperation(testOperation(fmt.Sprintf("seed-%d", i),
			Change{CreateAsset: &CreateAsset{AssetID: asset, Label: fmt.Sprintf("asset %d", i)}}))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.applyChange(context.Background(), tx, op.Changes[0], int64(i)); err != nil {
			t.Fatal(err)
		}
		receipt := Receipt{OperationVersion, testLibrary, op.ID, int64(i), digest, int64(i) * 1000}
		receiptJSON, _ := json.Marshal(receipt)
		if _, err := tx.Exec(`INSERT INTO events(sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt) VALUES(?,?,?,?,?,?,?)`,
			receipt.Sequence, testLibrary, op.ID, request, digest, receipt.RecordedUnixNano, receiptJSON); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// Restore commits replayed events in batches rather than one durable commit per
// event; the restored catalog is still complete and identical to its source.
func TestRestoreCommitsEventsInBatches(t *testing.T) {
	const events = 2000
	source, _ := newTestStore(t)
	seedEvents(t, source, events)
	limits := Limits{MaxBytes: 64 << 20, MaxEvents: events, MaxProjectionRows: events + 1}
	var stream bytes.Buffer
	if err := source.Export(context.Background(), testLibrary, &stream, limits); err != nil {
		t.Fatal(err)
	}
	commits := 0
	restoreBatchCommitted = func() { commits++ }
	defer func() { restoreBatchCommitted = nil }()
	path := filepath.Join(t.TempDir(), "restore.sqlite")
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream.Bytes()), limits); err != nil {
		t.Fatal(err)
	}
	if commits == 0 || commits > events/100 {
		t.Fatalf("restore of %d events used %d replay commits", events, commits)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if !bytes.Equal(legacyRecords(t, source.db), legacyRecords(t, restored.db)) {
		t.Fatal("restored records differ from source")
	}
	var again bytes.Buffer
	if err := restored.Export(context.Background(), testLibrary, &again, limits); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stream.Bytes(), again.Bytes()) {
		t.Fatal("restored catalog exports a different stream")
	}
	// A restore that stops mid-stream keeps only whole committed batches.
	withRestoreBatch(t, 100)
	frames := streamFrames(t, stream.Bytes())
	truncated := filepath.Join(t.TempDir(), "truncated.sqlite")
	if err := Restore(context.Background(), truncated, testLibrary, bytes.NewReader(joinFrames(frames[:1+250])), limits); err == nil {
		t.Fatal("truncated restore succeeded")
	}
	if a, _, _, e := queryIncompleteCounts(t, truncated); a != 200 || e != 200 {
		t.Fatalf("truncated restore kept %d assets and %d events, want two whole batches", a, e)
	}
}
