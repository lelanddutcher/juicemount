package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func savedQueryAssertRetained(t *testing.T, path string, ordinal int64) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("failed target not retained: %v", err)
	}
	assertIncomplete(t, path)
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var actual int64
	if err := db.QueryRow(`SELECT COALESCE(max(ordinal),0) FROM collection_order`).Scan(&actual); err != nil || actual != ordinal {
		t.Fatalf("retained prefix %d want%d: %v", actual, ordinal, err)
	}
	if err := validateInactiveCollectionImport(context.Background(), db); err != nil {
		t.Fatal(err)
	}
}

func TestSavedQueryPortabilityPublicFrozenNegatives(t *testing.T) {
	f := readSavedQueryPortableFixture(t)
	last := map[string]int64{"forged_query_projection": 26, "wrong_query_child_actor": 23, "stale_query_definition_CAS": 22, "legacy_generation_after_query": 25}
	for _, negative := range f.Negative {
		t.Run(negative.ID, func(t *testing.T) {
			want, ok := last[negative.ID]
			if !ok {
				t.Fatal("unknown negative")
			}
			path := filepath.Join(t.TempDir(), "refused.sqlite")
			v := negative.Vector
			err := Restore(context.Background(), path, v.LibraryID, bytes.NewReader(collectionFixtureHex(t, v.StreamHex)), collectionWireLimits())
			if err == nil {
				t.Fatal("accepted fixed format4 negative")
			}
			if negative.ID == "forged_query_projection" && !strings.Contains(err.Error(), "restored projection does not match") {
				t.Fatalf("did not reach final projection equality: %v", err)
			}
			if negative.ID == "wrong_query_child_actor" && !strings.Contains(err.Error(), "generated policy binding") {
				t.Fatalf("wrong child rejection: %v", err)
			}
			savedQueryAssertRetained(t, path, want)
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			good := f.Positive[1]
			if err := Restore(context.Background(), path, good.LibraryID, bytes.NewReader(collectionFixtureHex(t, good.StreamHex)), collectionWireLimits()); err == nil {
				t.Fatal("adopted failed target")
			}
			after, err := os.Stat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("replaced retained target: %v", err)
			}
			savedQueryAssertRetained(t, path, want)
		})
	}
}

func TestSavedQueryPortabilityQueryRowBudgetRollbackAndABA(t *testing.T) {
	v := readSavedQueryPortableFixture(t).Positive[1]
	t.Run("query_parent_and_child_atomic_row_budget", func(t *testing.T) {
		s, rows, next := savedQueryReplayPrefix(t, v, 21)
		before := savedQueryTestSnapshot(t, s, true)
		if rows != 55 {
			t.Fatal("literal pre-query row count")
		}
		if _, err := replaySavedQueryFixtureFrame(t, s, next, rows, 59); !errors.Is(err, ErrLimit) {
			t.Fatalf("expected query incremental row limit: %v", err)
		}
		if !bytes.Equal(before, savedQueryTestSnapshot(t, s, true)) {
			t.Fatal("failed query parent/child retained partial state")
		}
		if got, err := replaySavedQueryFixtureFrame(t, s, next, rows, 60); err != nil || got != 60 {
			t.Fatalf("exact admitted query create: %d %v", got, err)
		}
	})
	t.Run("query_object_token_reuse", func(t *testing.T) {
		s, rows, next := savedQueryReplayPrefix(t, v, 23)
		r, receipt, child, childReceipt, err := decodeSavedQueryContentPair(next.payload, v.LibraryID)
		if err != nil {
			t.Fatal(err)
		}
		bad := r
		bad.ResultVersion = "cv_00000000000000000000000000000001"
		badReceipt, err := DecodeSavedQueryReceipt(receipt)
		if err != nil {
			t.Fatal(err)
		}
		badReceipt.ResultVersion = bad.ResultVersion
		encoded, err := CanonicalSavedQueryReceipt(badReceipt)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := CanonicalSavedQueryRecord(bad); err != nil {
			t.Fatalf("negative must remain syntactically valid: %v", err)
		}
		before := savedQueryTestSnapshot(t, s, true)
		if _, err := s.replaySavedQueryContent(context.Background(), bad, encoded, child, childReceipt, rows, MaxStreamProjectionRows); !errors.Is(err, ErrConflict) {
			t.Fatalf("accepted historical query token reuse: %v", err)
		}
		if !bytes.Equal(before, savedQueryTestSnapshot(t, s, true)) {
			t.Fatal("ABA refusal changed state")
		}
		if got, err := s.replaySavedQueryContent(context.Background(), r, receipt, child, childReceipt, rows, MaxStreamProjectionRows); err != nil || got != 64 {
			t.Fatalf("unchanged grant child: rows=%d %v", got, err)
		}
	})
}

func TestSavedQueryPortabilityPublicLimitsAndFailureBoundaries(t *testing.T) {
	v := readSavedQueryPortableFixture(t).Positive[1]
	wire := collectionFixtureHex(t, v.StreamHex)
	frames := savedQueryPortableFrames(t, wire, len(streamMagic4))
	t.Run("event_budget_counts_generated_children", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "events.sqlite")
		if err := Restore(context.Background(), path, v.LibraryID, bytes.NewReader(wire), Limits{int64(len(wire)), 32, 67}); !errors.Is(err, ErrLimit) {
			t.Fatalf("combined event limit: %v", err)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatal("invalid header budget reserved target")
		}
	})
	t.Run("public_query_row_rollback", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "rows.sqlite")
		r := bytes.NewReader(wire)
		if err := Restore(context.Background(), path, v.LibraryID, r, Limits{int64(len(wire)), 33, 59}); !errors.Is(err, ErrLimit) {
			t.Fatalf("query row limit: %v", err)
		}
		savedQueryAssertRetained(t, path, 21)
		ordinal, stop := 0, 0
		for _, f := range frames {
			if f.kind == 4 || f.kind == 6 || f.kind == 7 {
				ordinal++
				if ordinal == 22 {
					stop = f.end
					break
				}
			}
		}
		if len(wire)-r.Len() != stop {
			t.Fatal("row failure consumed later transaction")
		}
		db, err := openDB(path, true)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var definitions, content, policy int
		if err := db.QueryRow(`SELECT (SELECT count(*) FROM saved_query_definitions),(SELECT count(*) FROM collection_events WHERE collection_id='col_00000000000000000000000000000003'),(SELECT count(*) FROM collection_policy_events WHERE collection_id='col_00000000000000000000000000000003')`).Scan(&definitions, &content, &policy); err != nil || definitions != 0 || content != 0 || policy != 0 {
			t.Fatalf("partial query parent/child: %d %d %d %v", definitions, content, policy, err)
		}
	})
	t.Run("byte_budget_one_short", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bytes.sqlite")
		if err := Restore(context.Background(), path, v.LibraryID, bytes.NewReader(wire), Limits{int64(len(wire) - 1), 33, 67}); !errors.Is(err, ErrLimit) {
			t.Fatalf("byte limit: %v", err)
		}
		savedQueryAssertRetained(t, path, 26)
	})
	// Select the query record/child frame and final footer, covering each
	// length/payload boundary without another exhaustive old-format matrix.
	var query savedQueryPortableFrame
	ordinal := 0
	for _, f := range frames {
		if f.kind == 4 || f.kind == 6 || f.kind == 7 {
			ordinal++
			if ordinal == 22 {
				query = f
			}
		}
	}
	for _, frame := range []savedQueryPortableFrame{query, frames[len(frames)-1]} {
		for _, cut := range []int{frame.start + 1, frame.start + 4, frame.start + 5, frame.end - 1} {
			t.Run(fmt.Sprintf("truncated_%d", cut), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "truncated.sqlite")
				if err := Restore(context.Background(), path, v.LibraryID, bytes.NewReader(wire[:cut]), collectionWireLimits()); err == nil {
					t.Fatal("accepted truncated format4 frame")
				}
				want := int64(21)
				if frame.kind == 3 {
					want = 26
				}
				savedQueryAssertRetained(t, path, want)
			})
		}
	}
	failure := errors.New("saved query trailing read failure")
	for _, withBytes := range []bool{false, true} {
		t.Run(fmt.Sprintf("final_read_error_%t", withBytes), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "error.sqlite")
			r := &collectionCompleteErrorReader{reader: bytes.NewReader(wire), err: failure, withBytes: withBytes}
			if err := Restore(context.Background(), path, v.LibraryID, r, collectionWireLimits()); !errors.Is(err, failure) {
				t.Fatalf("lost reader error: %v", err)
			}
			savedQueryAssertRetained(t, path, 26)
		})
	}
	t.Run("trailing_byte", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "trailing.sqlite")
		if err := Restore(context.Background(), path, v.LibraryID, bytes.NewReader(append(append([]byte{}, wire...), 0)), collectionWireLimits()); err == nil {
			t.Fatal("accepted trailing byte")
		}
		savedQueryAssertRetained(t, path, 26)
	})
}

func TestSavedQueryPortabilityTypedDispatchAndFrameCap(t *testing.T) {
	v := readSavedQueryPortableFixture(t).Positive[1]
	s, rows, query := savedQueryReplayPrefix(t, v, 21)
	if _, _, err := decodePortableCollectionContent(query.payload, v.LibraryID, 3); err == nil {
		t.Fatal("format3 accepted query codec")
	}
	bad := bytes.Replace(query.payload, []byte(`"version":2`), []byte(`"version":3`), 1)
	if _, _, err := decodePortableCollectionContent(bad, v.LibraryID, 4); err == nil {
		t.Fatal("accepted future content codec")
	}
	if _, _, err := decodePortableCollectionContent(query.payload, v.LibraryID, 5); err != nil {
		t.Fatalf("format5 rejected supported query codec: %v", err)
	}
	if _, _, err := decodePortableCollectionContent(query.payload, v.LibraryID, 6); err == nil {
		t.Fatal("accepted future stream format")
	}
	if got, err := replaySavedQueryFixtureFrame(t, s, query, rows, MaxStreamProjectionRows); err != nil || got != 60 {
		t.Fatalf("typed positive: %d %v", got, err)
	}
	// Public parsing must refuse the shared content frame cap before its body.
	wire := collectionFixtureHex(t, v.StreamHex)
	for _, size := range []uint32{0, 72721} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			prefix := append([]byte{}, wire[:query.start]...)
			header := []byte{7, 0, 0, 0, 0}
			binary.BigEndian.PutUint32(header[1:], size)
			r := bytes.NewReader(append(append(prefix, header...), 0xab))
			path := filepath.Join(t.TempDir(), "oversize.sqlite")
			if err := Restore(context.Background(), path, v.LibraryID, r, collectionWireLimits()); err == nil || r.Len() != 1 {
				t.Fatalf("frame cap consumed body: remaining%d err%v", r.Len(), err)
			}
			savedQueryAssertRetained(t, path, 21)
		})
	}
}

func TestSavedQueryPortabilityPublicHeaderBeforeReservation(t *testing.T) {
	v := readSavedQueryPortableFixture(t).Positive[0]
	frames := savedQueryPortableFrames(t, collectionFixtureHex(t, v.StreamHex), len(streamMagic4))
	header := string(frames[0].payload)
	for name, value := range map[string]string{
		"wrong_schema":  strings.Replace(header, `"schema_version":6`, `"schema_version":5`, 1),
		"wrong_format":  strings.Replace(header, `"version":4`, `"version":3`, 1),
		"future_schema": strings.Replace(header, `"schema_version":6`, `"schema_version":7`, 1),
		"missing_zero":  strings.Replace(header, `"collection_event_count":0,`, "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			frame := frameHeader(1, len(value))
			wire := append(append([]byte(streamMagic4), frame[:]...), []byte(value)...)
			path := filepath.Join(t.TempDir(), "header.sqlite")
			if err := Restore(context.Background(), path, v.LibraryID, bytes.NewReader(wire), collectionWireLimits()); err == nil || !errors.Is(err, ErrFormat) {
				t.Fatalf("header admission: %v", err)
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatal("malformed header reserved a target")
			}
		})
	}
}

func savedQueryReplayLegacyOperatorChange(t *testing.T, s *Store, rows int64, id string, change PolicyChange) int64 {
	t.Helper()
	ctx := context.Background()
	sequence, err := nextSequence(ctx, s.db, "policy_events")
	if err != nil {
		t.Fatal(err)
	}
	op := PolicyOperation{1, s.library, id, "local_owner", SubjectID("sub_00000000000000000000000000000001"), []PolicyChange{change}}
	request, digest, err := CanonicalPolicyRequest(op)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := json.Marshal(Receipt{1, s.library, op.ID, sequence, digest, 1000 + sequence})
	if err != nil {
		t.Fatal(err)
	}
	event, decoded, err := decodeStreamPolicyEvent(encodeEvent(eventRow{request: request, receipt: receipt}), s.library, sequence)
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.replayPolicyEvent(ctx, event, decoded, rows, MaxStreamProjectionRows)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func savedQueryHistoricalOwnerRecord(t *testing.T, library continuity.LibraryID, operator SubjectID) (CollectionPolicyRecord, []byte) {
	t.Helper()
	value := CollectionGrantValue{SubjectID("sub_00000000000000000000000000000001"), CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	id := CollectionGrantID("cgr_00000000000000000000000000000008")
	request := CollectionPolicyRequest{2, library, CollectionPolicyOperationID("cpo_000000000000000000000000000000f1"), "library_create", "", "", "", []CollectionGrantChange{{"put", id, nil, &value}}}
	record := CollectionPolicyRecord{2, request, "local_owner", operator, []CollectionGrantResult{{id, CollectionGrantVersion("cgv_00000000000000000000000000000008"), false}}, 1001}
	if _, _, err := CanonicalCollectionPolicyRecord(record); err != nil {
		t.Fatal(err)
	}
	_, digest, err := CanonicalCollectionPolicyRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := CanonicalCollectionPolicyReceipt(CollectionPolicyReceipt{2, library, request.ID, digest, record.RecordedUnixNano})
	if err != nil {
		t.Fatal(err)
	}
	return record, receipt
}

func TestSavedQueryPortabilityHistoricalOwnerOperatorRefusal(t *testing.T) {
	fixture := readSavedQueryPortableFixture(t).Positive[1]
	for _, spec := range []struct {
		name, kind string
		enabled    bool
	}{{"disabled_person", "person", false}, {"enabled_service", "service", true}} {
		t.Run(spec.name, func(t *testing.T) {
			ctx := context.Background()
			s, rows, _ := savedQueryReplayPrefix(t, fixture, 26)
			operatorID := SubjectID("sub_00000000000000000000000000000003")
			rows = savedQueryReplayLegacyOperatorChange(t, s, rows, "create-historical-operator", NewPolicyChange("create_subject", CreateSubject{operatorID, SubjectValue{spec.kind, "Historical operator", spec.enabled}}))
			operator, err := readSubject(ctx, s.db, s.library, operatorID)
			if err != nil || operator.Value.Kind != spec.kind || operator.Value.Enabled != spec.enabled {
				t.Fatalf("operator must actually exist at the prefix: %+v %v", operator, err)
			}
			record, receipt := savedQueryHistoricalOwnerRecord(t, s.library, operatorID)
			// The target is a different, existing enabled person. Thus neither an
			// unknown-operator FK nor a disabled-target check explains refusal.
			target, err := readSubject(ctx, s.db, s.library, record.Request.Changes[0].Value.SubjectID)
			if err != nil || target.Value.Kind != "person" || !target.Value.Enabled {
				t.Fatalf("valid target: %+v %v", target, err)
			}
			before := savedQueryTestSnapshot(t, s, true)
			if _, err := s.replayCollectionOwnerPolicy(ctx, record, receipt, rows, MaxStreamProjectionRows); !errors.Is(err, ErrConflict) {
				t.Fatalf("historical owner operator accepted: %v", err)
			}
			if !bytes.Equal(before, savedQueryTestSnapshot(t, s, true)) {
				t.Fatal("operator refusal changed complete parent/order/quota/subject/query state")
			}
			savedQueryAssertRetained(t, s.path, 27)
			// Changing attribution alone to the enabled person must admit this
			// identical request/receipt and result, proving an otherwise valid event.
			record.OperatorSubjectID = target.ID
			if _, err := s.replayCollectionOwnerPolicy(ctx, record, receipt, rows, MaxStreamProjectionRows); err != nil {
				t.Fatalf("valid owner control: %v", err)
			}
			if err := changedOne(s.db.Exec(`UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0`)); err != nil {
				t.Fatal(err)
			}
			var good bytes.Buffer
			if err := s.Export(ctx, s.library, &good, collectionWireLimits()); err != nil {
				t.Fatal(err)
			}
			bad := savedQueryOwnerOperatorStream(t, s.library, good.Bytes(), record.Request.ID, operatorID)
			path := filepath.Join(t.TempDir(), "invalid-operator.sqlite")
			if err := Restore(ctx, path, s.library, bytes.NewReader(bad), collectionWireLimits()); !errors.Is(err, ErrConflict) {
				t.Fatalf("public owner operator accepted: %v", err)
			}
			savedQueryAssertRetained(t, path, 27)
			db, err := openDB(path, true)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			// The wrapper is used only by the existing read-only snapshot helper.
			if !bytes.Equal(before, savedQueryTestSnapshot(t, &Store{db: db}, true)) {
				t.Fatal("public owner refusal changed complete prior state")
			}
		})
	}
}

func TestSavedQueryPortabilityOwnerLaterDisabledPublicRoundtrip(t *testing.T) {
	ctx := context.Background()
	fixture := readSavedQueryPortableFixture(t).Positive[1]
	s, rows, _ := savedQueryReplayPrefix(t, fixture, 26)
	operatorID := SubjectID("sub_00000000000000000000000000000003")
	rows = savedQueryReplayLegacyOperatorChange(t, s, rows, "create-historical-operator", NewPolicyChange("create_subject", CreateSubject{operatorID, SubjectValue{"person", "Historical operator", true}}))
	record, receipt := savedQueryHistoricalOwnerRecord(t, s.library, operatorID)
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
	rows = savedQueryReplayLegacyOperatorChange(t, s, rows, "disable-later-operator", NewPolicyChange("update_subject", UpdateSubject{operatorID, operator.Expected(), disabled}))
	if err := validateCollectionPortableState(ctx, s.db, s.library); err != nil {
		t.Fatal(err)
	}
	if err := changedOne(s.db.Exec(`UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0`)); err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if err := s.Export(ctx, s.library, &wire, collectionWireLimits()); err != nil {
		t.Fatalf("export later-disabled history: %v", err)
	}
	path := filepath.Join(t.TempDir(), "later-disabled.sqlite")
	if err := Restore(ctx, path, s.library, bytes.NewReader(wire.Bytes()), collectionWireLimits()); err != nil {
		t.Fatalf("public replay used final operator state: %v", err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	final, err := readSubject(ctx, restored.db, s.library, operatorID)
	if err != nil || final.Value.Kind != "person" || final.Value.Enabled {
		t.Fatalf("later disable lost: %+v %v", final, err)
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
		t.Fatalf("historical owner record/receipt changed: %v", err)
	}
	actualRows, err := projectionTotal(ctx, restored.db)
	if err != nil || actualRows != rows {
		t.Fatalf("historical row accounting: %d want%d %v", actualRows, rows, err)
	}
	if err := validateInactiveCollectionImport(ctx, restored.db); err != nil {
		t.Fatal(err)
	}
	var again bytes.Buffer
	if err := restored.Export(ctx, restored.library, &again, collectionWireLimits()); err != nil || !bytes.Equal(wire.Bytes(), again.Bytes()) {
		t.Fatalf("later-disabled exact re-export: %v", err)
	}
}

// Derive a complete negative from the valid control archive by changing only
// recorded owner attribution. Operator attribution is absent from the logical
// projection; request/receipt/quota bytes remain identical. Rehash the stream
// so the intended rejection cannot be explained by an invalid footer hash.
func savedQueryOwnerOperatorStream(t *testing.T, library continuity.LibraryID, wire []byte, id CollectionPolicyOperationID, operator SubjectID) []byte {
	t.Helper()
	var header collectionStreamHeader
	var footer collectionStreamFooter
	out := append([]byte{}, []byte(streamMagic4)...)
	changed := 0
	for _, frame := range savedQueryPortableFrames(t, wire, len(streamMagic4)) {
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
	if changed != 1 {
		t.Fatal("owner attribution control did not change exactly one frame")
	}
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.export.stream\x00\x04"))
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
	return append(out, data...)
}
