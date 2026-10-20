package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

//go:embed testdata/continuity-c02c-*.json
var portabilityVectors embed.FS

type portabilityVector struct {
	LibraryID        string `json:"library_id"`
	EventCount       int64  `json:"event_count"`
	ProjectionRows   int64  `json:"projection_rows"`
	ProjectionSHA256 string `json:"projection_sha256"`
	StreamSHA256     string `json:"stream_sha256"`
	StreamBytes      int    `json:"stream_bytes"`
	FileSHA256       string `json:"file_sha256"`
	StreamHex        string `json:"stream_hex"`
	Events           []struct {
		Request string `json:"request_json"`
		Receipt string `json:"receipt_json"`
	} `json:"events"`
}

func loadPortabilityVector(t *testing.T, name string) (portabilityVector, []byte) {
	t.Helper()
	data, err := portabilityVectors.ReadFile("testdata/continuity-c02c-" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var v portabilityVector
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	stream, err := hex.DecodeString(v.StreamHex)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(stream)
	if len(stream) != v.StreamBytes || hex.EncodeToString(digest[:]) != v.FileSHA256 {
		t.Fatal("independent stream fixture damaged")
	}
	return v, stream
}

func portabilityLimits() Limits {
	return Limits{MaxBytes: 8 << 20, MaxEvents: 128, MaxProjectionRows: 4096}
}

func vectorSource(t *testing.T, name string) (*Store, portabilityVector, []byte) {
	t.Helper()
	v, expected := loadPortabilityVector(t, name)
	if name == "empty" {
		s, _ := newTestStore(t)
		return s, v, expected
	}
	path, _ := legacyFixture(t)
	if err := Upgrade(context.Background(), path, testLibrary); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	// Retain independent fixture timestamps/receipts rather than sampling Apply's
	// clock. The legacy two events came from the separate frozen SQL fixture.
	for _, record := range v.Events[2:] {
		var receipt Receipt
		if err := json.Unmarshal([]byte(record.Receipt), &receipt); err != nil {
			t.Fatal(err)
		}
		op, err := decodeOperation([]byte(record.Request))
		if err != nil {
			t.Fatal(err)
		}
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, change := range op.Changes {
			if err := s.applyChange(context.Background(), tx, change, receipt.Sequence); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
		}
		_, err = tx.Exec(`INSERT INTO events(sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt) VALUES(?,?,?,?,?,?,?)`,
			receipt.Sequence, testLibrary, receipt.OperationID, []byte(record.Request), receipt.PayloadSHA256, receipt.RecordedUnixNano, []byte(record.Receipt))
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	return s, v, expected
}

func assertIncomplete(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return
	} else if err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path); !errors.Is(err, ErrFormat) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("failed restore became ready: %v", err)
	}
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ready int
	if err := db.QueryRow(`SELECT ready FROM catalog`).Scan(&ready); err != nil || ready != 0 {
		t.Fatalf("incomplete readiness: %d, %v", ready, err)
	}
}

func TestPortabilityIndependentVectorsAndCompleteRoundtrip(t *testing.T) {
	for _, name := range []string{"empty", "legacy", "relational"} {
		t.Run(name, func(t *testing.T) {
			s, v, expected := vectorSource(t, name)
			var output bytes.Buffer
			limits := Limits{MaxBytes: int64(len(expected)), MaxEvents: max(1, v.EventCount), MaxProjectionRows: v.ProjectionRows}
			if err := s.Export(context.Background(), testLibrary, &output, limits); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(output.Bytes(), expected) {
				t.Fatalf("export differs from independent %s vector", name)
			}
			path := filepath.Join(t.TempDir(), "restored.sqlite")
			if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(expected), limits); err != nil {
				t.Fatal(err)
			}
			restored, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer restored.Close()
			if !bytes.Equal(legacyRecords(t, s.db), legacyRecords(t, restored.db)) {
				t.Fatal("restored original records/history bytes differ")
			}
			projection, rows, err := projectionDigest(context.Background(), restored.db, testLibrary, limits.MaxProjectionRows)
			if err != nil || projection != v.ProjectionSHA256 || rows != v.ProjectionRows {
				t.Fatalf("projection %s/%d: %v", projection, rows, err)
			}
			for _, record := range v.Events {
				var receipt Receipt
				if err := json.Unmarshal([]byte(record.Receipt), &receipt); err != nil {
					t.Fatal(err)
				}
				event, request, err := readEvent(context.Background(), restored.db, testLibrary, receipt.OperationID)
				if err != nil || string(request) != record.Request || event.Receipt != receipt {
					t.Fatalf("exact history readback: %v", err)
				}
				if got := applyTest(t, restored, event.Operation); got != receipt {
					t.Fatal("identical retry changed original receipt")
				}
			}
			if name == "relational" {
				if got := getLocation(t, restored, "location_scene_001"); !got.Removed || got.UpdatedSequence != 4 {
					t.Fatalf("tombstone lost: %+v", got)
				}
				if got := getRelationship(t, restored, "edge_scene_001"); got.Removed || got.UpdatedSequence != 5 || got.Value.Provenance != Claimed {
					t.Fatalf("relationship correction lost: %+v", got)
				}
			}
			var again bytes.Buffer
			if err := restored.Export(context.Background(), testLibrary, &again, limits); err != nil || !bytes.Equal(again.Bytes(), expected) {
				t.Fatalf("export after restore: %v", err)
			}
			next := testOperation("after-restore", Change{CreateAsset: &CreateAsset{AssetID: otherAsset, Label: "new"}})
			if receipt := applyTest(t, restored, next); receipt.Sequence != v.EventCount+1 {
				t.Fatalf("next sequence: %+v", receipt)
			}
		})
	}
}

func TestPortabilityRejectsSourceHistoryProjectionDivergence(t *testing.T) {
	for name, statement := range map[string]string{
		"label":     `UPDATE assets SET label='edited outside operation history'`,
		"head":      `UPDATE assets SET head=NULL`,
		"tombstone": `UPDATE locations SET removed=0`,
	} {
		t.Run(name, func(t *testing.T) {
			s, _, _ := vectorSource(t, "relational")
			if _, err := s.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := s.Export(context.Background(), testLibrary, &output, portabilityLimits()); err != nil {
				t.Fatal("structurally valid source snapshot:", err)
			}
			path := filepath.Join(t.TempDir(), "restore.sqlite")
			err := Restore(context.Background(), path, testLibrary, bytes.NewReader(output.Bytes()), portabilityLimits())
			if !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "restored projection") {
				t.Fatalf("source-history/projection divergence accepted: %v", err)
			}
			assertIncomplete(t, path)
		})
	}
}

func TestPortabilityLimitsAndBeforeReservationValidation(t *testing.T) {
	s, v, stream := vectorSource(t, "relational")
	for name, limits := range map[string]Limits{
		"zero": {}, "byte ceiling": {MaxBytes: MaxStreamBytes + 1, MaxEvents: 1, MaxProjectionRows: 1},
		"event ceiling": {MaxBytes: 1024, MaxEvents: MaxStreamEvents + 1, MaxProjectionRows: 1},
		"row ceiling":   {MaxBytes: 1024, MaxEvents: 1, MaxProjectionRows: MaxStreamProjectionRows + 1},
	} {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			if err := s.Export(context.Background(), testLibrary, &output, limits); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid export limits: %v", err)
			}
			path := filepath.Join(t.TempDir(), "restore.sqlite")
			if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream), limits); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid restore limits: %v", err)
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatal("invalid budgets reserved a file")
			}
		})
	}
	for name, limits := range map[string]Limits{
		"bytes":  {MaxBytes: int64(len(stream) - 1), MaxEvents: v.EventCount, MaxProjectionRows: v.ProjectionRows},
		"events": {MaxBytes: int64(len(stream)), MaxEvents: v.EventCount - 1, MaxProjectionRows: v.ProjectionRows},
		"rows":   {MaxBytes: int64(len(stream)), MaxEvents: v.EventCount, MaxProjectionRows: v.ProjectionRows - 1},
	} {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			if err := s.Export(context.Background(), testLibrary, &output, limits); !errors.Is(err, ErrLimit) {
				t.Fatalf("export limit: %v", err)
			}
			path := filepath.Join(t.TempDir(), "restore.sqlite")
			if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream), limits); !errors.Is(err, ErrLimit) {
				t.Fatalf("restore limit: %v", err)
			}
			assertIncomplete(t, path)
		})
	}
	if err := s.Export(context.Background(), testLibrary, nil, portabilityLimits()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil writer: %v", err)
	}
	if err := Restore(context.Background(), "unused", testLibrary, nil, portabilityLimits()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil reader: %v", err)
	}
	if err := s.Export(context.Background(), otherLibrary, &bytes.Buffer{}, portabilityLimits()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong export library: %v", err)
	}
	path := filepath.Join(t.TempDir(), "wrong.sqlite")
	if err := Restore(context.Background(), path, otherLibrary, bytes.NewReader(stream), portabilityLimits()); !errors.Is(err, ErrFormat) {
		t.Fatalf("wrong stream library: %v", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("wrong header reserved a file")
	}
}

func queryIncompleteCounts(t *testing.T, path string) (assets, revisions, parents, events int) {
	t.Helper()
	assertIncomplete(t, path)
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM assets),(SELECT count(*) FROM revisions),(SELECT count(*) FROM revision_parents),(SELECT count(*) FROM events)`).Scan(&assets, &revisions, &parents, &events); err != nil {
		t.Fatal(err)
	}
	return
}
