package catalog

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// The fixed shared format3 fixture supplies the complete historical prefix.
// Added records have fixed IDs/results/times; no live credentials or final-state
// reauthentication can explain whether their historical attribution is valid.
func collectionOperatorRecord(t *testing.T, library continuity.LibraryID, operator SubjectID) (CollectionPolicyRecord, []byte) {
	t.Helper()
	value := CollectionGrantValue{SubjectID("sub_00000000000000000000000000000001"), CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	request := CollectionPolicyRequest{2, library, CollectionPolicyOperationID("cpo_000000000000000000000000000000f1"), "library_create", "", "", "", []CollectionGrantChange{{"put", "cgr_000000000000000000000000000000f1", nil, &value}}}
	record := CollectionPolicyRecord{2, request, "local_owner", operator, []CollectionGrantResult{{"cgr_000000000000000000000000000000f1", "cgv_000000000000000000000000000000f1", false}}, 1700000000000001001}
	if _, _, err := CanonicalCollectionPolicyRecord(record); err != nil {
		t.Fatal(err)
	}
	return record, collectionReplayPolicyReceipt(t, record)
}

func collectionOperatorExport(t *testing.T, s *Store) []byte {
	t.Helper()
	ctx := context.Background()
	if s.version != 5 {
		t.Fatal("operator archive must exercise schema5")
	}
	if err := validateCollectionPortableState(ctx, s.db, s.library); err != nil {
		t.Fatal(err)
	}
	if err := changedOne(s.db.ExecContext(ctx, `UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0`)); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := s.Export(ctx, s.library, &out, collectionWireLimits()); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(out.Bytes(), []byte(streamMagic3)) {
		t.Fatal("operator archive must use format3")
	}
	return out.Bytes()
}

// Change only one owner attribution in a complete valid format3 stream. Owner
// attribution is outside logical projection rows and request/receipt quota;
// retain those exact bytes and recompute the real format3 stream digest.
func collectionOperatorWire(t *testing.T, library continuity.LibraryID, wire []byte, id CollectionPolicyOperationID, operator SubjectID) []byte {
	t.Helper()
	if !bytes.HasPrefix(wire, []byte(streamMagic3)) {
		t.Fatal("expected format3 operator source")
	}
	var header collectionStreamHeader
	var footer collectionStreamFooter
	out := append([]byte{}, []byte(streamMagic3)...)
	changed := 0
	for _, frame := range savedQueryPortableFrames(t, wire, len(streamMagic3)) {
		payload := frame.payload
		switch frame.kind {
		case 1:
			if err := json.Unmarshal(payload, &header); err != nil {
				t.Fatal(err)
			}
		case 3:
			if err := json.Unmarshal(payload, &footer); err != nil {
				t.Fatal(err)
			}
			continue
		case 6:
			record, receipt, err := decodeCollectionPolicyPair(payload, library)
			if err != nil {
				t.Fatal(err)
			}
			if record.Request.ID == id {
				record.OperatorSubjectID = operator
				data, _, err := CanonicalCollectionPolicyRecord(record)
				if err != nil {
					t.Fatal(err)
				}
				payload = appendCollectionPair(nil, data, receipt)
				changed++
			}
		}
		framing := frameHeader(frame.kind, len(payload))
		out = append(out, framing[:]...)
		out = append(out, payload...)
	}
	if changed != 1 || header.Version != 3 || header.SchemaVersion != 5 {
		t.Fatal("operator negative must change exactly one format3 owner")
	}
	h := collectionStreamHash()
	h.Write(out)
	var err error
	footer.StreamSHA256, err = finishCollectionStreamHash(h, header.collectionStreamCounts, footer.ProjectionRows, footer.ProjectionSHA256)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(footer)
	if err != nil {
		t.Fatal(err)
	}
	framing := frameHeader(3, len(data))
	out = append(out, framing[:]...)
	out = append(out, data...)
	// Check the complete canonical framing/digest separately. This intentionally
	// does not perform replay or substitute for the public Restore assertion.
	if stage, err := checkCollectionWireFixture(collectionWireFixture{LibraryID: library, StreamHex: hex.EncodeToString(out)}); err != nil || stage != "end" {
		t.Fatalf("operator negative is not a coherent format3 stream: %s %v", stage, err)
	}
	return out
}

func TestCollectionRestoreHistoricalOwnerOperatorRefusal(t *testing.T) {
	f := readCollectionReplayFixture(t)
	for _, spec := range []struct {
		name, kind string
		enabled    bool
	}{{"disabled_person", "person", false}, {"enabled_service", "service", true}} {
		t.Run(spec.name, func(t *testing.T) {
			ctx := context.Background()
			s, rows := replayCollectionFixturePrefix(t, f, 19)
			operatorID := SubjectID("sub_00000000000000000000000000000003")
			rows = savedQueryReplayLegacyOperatorChange(t, s, rows, "format3-create-invalid-operator", NewPolicyChange("create_subject", CreateSubject{operatorID, SubjectValue{spec.kind, "Historical operator", spec.enabled}}))
			operator, err := readSubject(ctx, s.db, s.library, operatorID)
			if err != nil || operator.Value.Kind != spec.kind || operator.Value.Enabled != spec.enabled {
				t.Fatalf("invalid operator must already exist: %+v %v", operator, err)
			}
			// Record a valid control to the same enabled-person target first.
			record, receipt := collectionOperatorRecord(t, s.library, "sub_00000000000000000000000000000001")
			target, err := readSubject(ctx, s.db, s.library, record.Request.Changes[0].Value.SubjectID)
			if err != nil || target.ID == operatorID || target.Value.Kind != "person" || !target.Value.Enabled {
				t.Fatalf("distinct enabled target: %+v %v", target, err)
			}
			// Complete typed schema5 snapshot before the owner frame, including
			// grants/results/order/quotas/subjects and inactive/private tables.
			before := savedQueryTestSnapshot(t, s, false)
			if _, err := s.replayCollectionOwnerPolicy(ctx, record, receipt, rows, MaxStreamProjectionRows); err != nil {
				t.Fatalf("valid enabled-person control: %v", err)
			}
			good := collectionOperatorExport(t, s)
			if err := Restore(ctx, filepath.Join(t.TempDir(), "valid-control.sqlite"), s.library, bytes.NewReader(good), collectionWireLimits()); err != nil {
				t.Fatalf("public valid owner control: %v", err)
			}
			bad := collectionOperatorWire(t, s.library, good, record.Request.ID, operatorID)
			path := filepath.Join(t.TempDir(), "invalid-operator.sqlite")
			err = Restore(ctx, path, s.library, bytes.NewReader(bad), collectionWireLimits())
			if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "restore owner policy replay") {
				t.Fatalf("public schema5 historical owner accepted or wrong refusal: %v", err)
			}
			savedQueryAssertRetained(t, path, 20)
			db, err := openDB(path, true)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			if !bytes.Equal(before, savedQueryTestSnapshot(t, &Store{db: db}, false)) {
				t.Fatal("public refusal changed complete schema5 pre-owner state")
			}
		})
	}
}

func TestCollectionRestoreOwnerLaterDisabledRoundtrip(t *testing.T) {
	ctx := context.Background()
	f := readCollectionReplayFixture(t)
	s, rows := replayCollectionFixturePrefix(t, f, 19)
	operatorID := SubjectID("sub_00000000000000000000000000000003")
	rows = savedQueryReplayLegacyOperatorChange(t, s, rows, "format3-create-enabled-operator", NewPolicyChange("create_subject", CreateSubject{operatorID, SubjectValue{"person", "Historical operator", true}}))
	record, receipt := collectionOperatorRecord(t, s.library, operatorID)
	original, _, err := CanonicalCollectionPolicyRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	rows, err = s.replayCollectionOwnerPolicy(ctx, record, receipt, rows, MaxStreamProjectionRows)
	if err != nil {
		t.Fatalf("enabled historical owner: %v", err)
	}
	operator, err := readSubject(ctx, s.db, s.library, operatorID)
	if err != nil {
		t.Fatal(err)
	}
	disabled := operator.Value
	disabled.Enabled = false
	rows = savedQueryReplayLegacyOperatorChange(t, s, rows, "format3-disable-later-operator", NewPolicyChange("update_subject", UpdateSubject{operatorID, operator.Expected(), disabled}))
	wire := collectionOperatorExport(t, s)
	path := filepath.Join(t.TempDir(), "later-disabled.sqlite")
	if err := Restore(ctx, path, s.library, bytes.NewReader(wire), collectionWireLimits()); err != nil {
		t.Fatalf("public schema5 replay used final operator state: %v", err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Error(err)
		}
	})
	if restored.version != 5 {
		t.Fatal("format3 must remain schema5")
	}
	final, err := readSubject(ctx, restored.db, s.library, operatorID)
	if err != nil || final.Value.Kind != "person" || final.Value.Enabled {
		t.Fatalf("later disabled state lost: %+v %v", final, err)
	}
	var sequence int64
	if err := restored.db.QueryRow(`SELECT sequence FROM collection_policy_events WHERE administration='local_owner' AND operation_id=?`, record.Request.ID).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	retained, retainedReceipt, err := readCollectionPolicyRecord(ctx, restored.db, s.library, sequence)
	if err != nil {
		t.Fatal(err)
	}
	retainedBytes, _, err := CanonicalCollectionPolicyRecord(retained)
	if err != nil || !bytes.Equal(original, retainedBytes) || !bytes.Equal(receipt, retainedReceipt) {
		t.Fatalf("original operator record/receipt changed: %v", err)
	}
	actualRows, err := projectionTotal(ctx, restored.db)
	if err != nil || actualRows != rows {
		t.Fatalf("historical projection row accounting: %d want%d %v", actualRows, rows, err)
	}
	if err := validateInactiveCollectionImport(ctx, restored.db); err != nil {
		t.Fatal(err)
	}
	var again bytes.Buffer
	if err := restored.Export(ctx, restored.library, &again, collectionWireLimits()); err != nil || !bytes.Equal(wire, again.Bytes()) {
		t.Fatalf("later-disabled exact format3 re-export: %v", err)
	}
}
