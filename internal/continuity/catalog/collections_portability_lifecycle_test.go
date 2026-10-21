package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type collectionReplaySnapshot struct {
	Ordinal     int64                             `json:"ordinal"`
	Rows        int64                             `json:"projection_rows"`
	Generations []*int64                          `json:"generations_A_B_X_Y"`
	Library     map[string]int64                  `json:"library_quota"`
	Locals      map[CollectionID]map[string]int64 `json:"local_quotas"`
}
type collectionReplayFrame struct {
	Kind       byte   `json:"kind"`
	Label      string `json:"label"`
	Ordinal    int64  `json:"ordinal"`
	PayloadHex string `json:"payload_hex"`
}
type collectionReplayFixture struct {
	LibraryID continuity.LibraryID       `json:"library_id"`
	Frames    []collectionReplayFrame    `json:"frames"`
	Snapshots []collectionReplaySnapshot `json:"expected_after_each_ordinal"`
	Events    []struct {
		Family   string `json:"family"`
		Sequence int64  `json:"sequence"`
		Request  string `json:"request_json"`
		Receipt  string `json:"receipt_json"`
		Record   string `json:"record_json"`
	} `json:"events"`
}

func readCollectionReplayFixture(t *testing.T) collectionReplayFixture {
	t.Helper()
	b, err := os.ReadFile("testdata/continuity-c04b-shared.json")
	if err != nil {
		t.Fatal(err)
	}
	var f collectionReplayFixture
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	return f
}
func newCollectionReplayStore(t *testing.T, library continuity.LibraryID) *Store {
	t.Helper()
	s, err := openStoreVersionContext(context.Background(), filepath.Join(t.TempDir(), "replay.sqlite"), library, true, false, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func replayCollectionFixtureFrame(t *testing.T, s *Store, frame collectionReplayFrame, rows, maxRows int64) (int64, error) {
	t.Helper()
	ctx := context.Background()
	payload := collectionFixtureHex(t, frame.PayloadHex)
	switch frame.Kind {
	case 2:
		e, op, err := decodeStreamEvent(payload, s.library, 1)
		if err != nil {
			return 0, err
		}
		return s.replayEvent(ctx, e, op, rows, maxRows)
	case 5:
		var d collectionDeclaration
		if err := canonicalJSON(payload, &d); err != nil {
			return 0, err
		}
		return s.replayCollectionDeclaration(ctx, d, rows, maxRows)
	case 4:
		var receipt Receipt
		_, raw, _, err := takeCollectionPair(payload, MaxRequestBytes, false)
		if err != nil {
			return 0, err
		}
		if err := json.Unmarshal(raw, &receipt); err != nil {
			return 0, err
		}
		e, op, err := decodeStreamPolicyEvent(payload, s.library, receipt.Sequence)
		if err != nil {
			return 0, err
		}
		return s.replayPolicyEvent(ctx, e, op, rows, maxRows)
	case 6:
		r, receipt, err := decodeCollectionPolicyPair(payload, s.library)
		if err != nil {
			return 0, err
		}
		return s.replayCollectionOwnerPolicy(ctx, r, receipt, rows, maxRows)
	case 7:
		r, receipt, p, childReceipt, err := decodeCollectionContentPair(payload, s.library)
		if err != nil {
			return 0, err
		}
		return s.replayCollectionContent(ctx, r, receipt, p, childReceipt, rows, maxRows)
	default:
		return rows, nil
	}
}

var collectionReplayTables = []string{"catalog", "assets", "revisions", "revision_parents", "relationships", "locations", "policy_subjects", "policy_grants", "collection_identities", "collection_definitions", "collection_entries", "collection_grants", "collection_policy_results", "collection_order", "collection_library_quota", "collection_quota"}

func collectionReplayRowCount(t *testing.T, s *Store) int64 {
	t.Helper()
	var total int64
	for _, table := range collectionReplayTables {
		var n int64
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		total += n
	}
	return total
}
func collectionReplayState(t *testing.T, s *Store) []byte {
	t.Helper()
	tables := append(append([]string{}, collectionReplayTables...), "events", "policy_events", "collection_events", "collection_policy_events")
	var state []any
	for _, table := range tables {
		rows, err := s.db.Query("SELECT * FROM " + table + " ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			args := make([]any, len(columns))
			for i := range values {
				args[i] = &values[i]
			}
			if err := rows.Scan(args...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for i, v := range values {
				if b, ok := v.([]byte); ok {
					values[i] = append([]byte{}, b...)
				}
			}
			state = append(state, []any{table, values})
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
	}
	b, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func assertCollectionReplayQuota(t *testing.T, actual CollectionQuotaCounters, want map[string]int64) {
	t.Helper()
	names := strings.Split(collectionQuotaColumns, ",")
	fields := quotaFields(actual)
	if len(want) != len(names) {
		t.Fatalf("independent quota dimensions %d", len(want))
	}
	for i, name := range names {
		if fields[i] != want[name] {
			t.Fatalf("quota %s=%d, independent literal=%d", name, fields[i], want[name])
		}
	}
}
func assertCollectionReplaySnapshot(t *testing.T, s *Store, rows int64, want collectionReplaySnapshot) {
	t.Helper()
	if rows != want.Rows || collectionReplayRowCount(t, s) != want.Rows {
		t.Fatalf("ordinal %d row admission=%d actual=%d want=%d", want.Ordinal, rows, collectionReplayRowCount(t, s), want.Rows)
	}
	usage, err := readCollectionQuota(context.Background(), s.db, "")
	if err != nil {
		t.Fatal(err)
	}
	assertCollectionReplayQuota(t, usage.Library, want.Library)
	for id, counters := range want.Locals {
		usage, err := readCollectionQuota(context.Background(), s.db, id)
		if err != nil {
			t.Fatal(err)
		}
		if usage.Collection == nil {
			t.Fatal("missing local quota")
		}
		assertCollectionReplayQuota(t, *usage.Collection, counters)
	}
	for i, wantGeneration := range want.Generations {
		var generation int64
		var err error
		if i < 2 {
			err = s.db.QueryRow("SELECT authorization_generation FROM policy_subjects WHERE subject_id=?", fmt.Sprintf("sub_%032x", i+1)).Scan(&generation)
		} else {
			err = s.db.QueryRow("SELECT policy_generation FROM collection_identities WHERE collection_id=? AND EXISTS(SELECT 1 FROM collection_definitions d WHERE d.collection_id=collection_identities.collection_id)", fmt.Sprintf("col_%032x", i-1)).Scan(&generation)
		}
		if wantGeneration == nil {
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("generation %d should be historically absent: %v", i, err)
			}
		} else if err != nil || generation != *wantGeneration {
			t.Fatalf("generation %d=%d want=%d: %v", i, generation, *wantGeneration, err)
		}
	}
}
func replayCollectionFixturePrefix(t *testing.T, f collectionReplayFixture, ordinal int64) (*Store, int64) {
	t.Helper()
	s := newCollectionReplayStore(t, f.LibraryID)
	rows := int64(2)
	for _, frame := range f.Frames {
		if frame.Ordinal > ordinal {
			break
		}
		var err error
		rows, err = replayCollectionFixtureFrame(t, s, frame, rows, MaxStreamProjectionRows)
		if err != nil {
			t.Fatalf("fixture prefix %s: %v", frame.Label, err)
		}
	}
	return s, rows
}
func collectionFixtureOrdinal(t *testing.T, f collectionReplayFixture, ordinal int64) collectionReplayFrame {
	t.Helper()
	for _, frame := range f.Frames {
		if frame.Ordinal == ordinal && ordinal > 0 {
			return frame
		}
	}
	t.Fatalf("missing independent ordinal %d", ordinal)
	return collectionReplayFrame{}
}
func TestCollectionReplayHelpersIndependentHistory(t *testing.T) {
	f := readCollectionReplayFixture(t)
	s := newCollectionReplayStore(t, f.LibraryID)
	rows := int64(2)
	snapshots := 0
	for _, frame := range f.Frames {
		var err error
		rows, err = replayCollectionFixtureFrame(t, s, frame, rows, MaxStreamProjectionRows)
		if err != nil {
			t.Fatalf("%s: %v", frame.Label, err)
		}
		if frame.Ordinal > 0 {
			if frame.Ordinal != int64(snapshots+1) {
				t.Fatal("fixture ordinal discontinuity")
			}
			assertCollectionReplaySnapshot(t, s, rows, f.Snapshots[snapshots])
			snapshots++
		}
	}
	if snapshots != 19 || rows != 51 {
		t.Fatalf("history checkpoints %d rows %d", snapshots, rows)
	}
	if err := checkCollectionState(context.Background(), s.db, f.LibraryID); err != nil {
		t.Fatal(err)
	}
	for _, event := range f.Events {
		table := "events"
		switch event.Family {
		case "legacy_policy":
			table = "policy_events"
		case "content":
			table = "collection_events"
		case "owner_policy", "generated_policy":
			table = "collection_policy_events"
		}
		var request, receipt []byte
		if err := s.db.QueryRow("SELECT request,receipt FROM "+table+" WHERE sequence=?", event.Sequence).Scan(&request, &receipt); err != nil {
			t.Fatal(err)
		}
		if string(request) != event.Request || string(receipt) != event.Receipt {
			t.Fatalf("exact original %s request/receipt lost", event.Family)
		}
		if event.Record != "" {
			var timestamp int64
			if err := s.db.QueryRow("SELECT recorded_unix_nano FROM "+table+" WHERE sequence=?", event.Sequence).Scan(&timestamp); err != nil {
				t.Fatal(err)
			}
			if event.Family == "content" {
				var record CollectionRecord
				if err := json.Unmarshal([]byte(event.Record), &record); err != nil {
					t.Fatal(err)
				}
				var actor SubjectID
				var token CollectionVersion
				if err := s.db.QueryRow("SELECT actor_subject_id,result_version FROM collection_events WHERE sequence=?", event.Sequence).Scan(&actor, &token); err != nil {
					t.Fatal(err)
				}
				if actor != record.ActorSubjectID || token != record.ResultVersion || timestamp != record.RecordedUnixNano {
					t.Fatal("content attribution/time/token changed")
				}
			} else {
				var record CollectionPolicyRecord
				if err := json.Unmarshal([]byte(event.Record), &record); err != nil {
					t.Fatal(err)
				}
				var actor SubjectID
				if err := s.db.QueryRow("SELECT operator_subject_id FROM collection_policy_events WHERE sequence=?", event.Sequence).Scan(&actor); err != nil {
					t.Fatal(err)
				}
				if actor != record.OperatorSubjectID || timestamp != record.RecordedUnixNano {
					t.Fatal("policy attribution/time changed")
				}
				var results []CollectionGrantResult
				err := walkRows(context.Background(), s.db, "SELECT grant_id,result_version,removed FROM collection_policy_results WHERE policy_sequence=? ORDER BY grant_id", func(rows *sql.Rows) error {
					var result CollectionGrantResult
					if err := rows.Scan(&result.ID, &result.Version, &result.Removed); err != nil {
						return err
					}
					results = append(results, result)
					return nil
				}, event.Sequence)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(results, record.Results) {
					t.Fatal("recorded result tokens or tombstones changed")
				}
			}
		}
	}
	for _, table := range []string{"catalog_authority", "catalog_admin_events", "catalog_credentials", "credential_issuance_receipts", "credential_codecs", "catalog_admin_codecs"} {
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("operational import %s=%d: %v", table, n, err)
		}
	}
	if err := requireIncomplete(context.Background(), s.db); err != nil {
		t.Fatal(err)
	}
	// C1/C2 intentionally reuse one actor-scoped operation ID and cv_ value;
	// P1/P2/P3 share cgv_ values on distinct grants. Successful replay is positive
	// coverage of those namespaces, not permission for same-object ABA.
}
func TestCollectionReplayHelpersIncrementalRollback(t *testing.T) {
	f := readCollectionReplayFixture(t)
	for _, span := range []struct{ ordinal, before, after int64 }{{2, 8, 12}, {3, 12, 17}, {4, 17, 24}, {7, 31, 34}} {
		for limit := span.before; limit < span.after; limit++ {
			t.Run(fmt.Sprintf("ordinal%d_limit%d", span.ordinal, limit), func(t *testing.T) {
				s, rows := replayCollectionFixturePrefix(t, f, span.ordinal-1)
				if rows != span.before {
					t.Fatal("independent starting rows")
				}
				before := collectionReplayState(t, s)
				if _, err := replayCollectionFixtureFrame(t, s, collectionFixtureOrdinal(t, f, span.ordinal), rows, limit); !errors.Is(err, ErrLimit) {
					t.Fatalf("expected incremental row limit: %v", err)
				}
				if !bytes.Equal(before, collectionReplayState(t, s)) {
					t.Fatal("failed parent/child/legacy transaction changed retained incomplete state")
				}
				next, err := replayCollectionFixtureFrame(t, s, collectionFixtureOrdinal(t, f, span.ordinal), rows, span.after)
				if err != nil || next != span.after {
					t.Fatalf("exact row budget: %d %v", next, err)
				}
			})
		}
	}
}
func TestCollectionReplayHelpersFrozenSemanticNegatives(t *testing.T) {
	data, err := os.ReadFile("testdata/continuity-c04b-operations.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Negative []struct {
			ID     string                  `json:"id"`
			Vector collectionReplayFixture `json:"vector"`
		} `json:"negative_streams"`
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	wanted := map[string]int64{"content_same_object_ABA": 14, "grant_same_object_ABA": 16, "mixed_generation_CAS_uses_policy_first_value": 6, "declaration_actor_alias": 0}
	seen := 0
	for _, vector := range vectors.Negative {
		ordinal, ok := wanted[vector.ID]
		if !ok {
			continue
		}
		seen++
		t.Run(vector.ID, func(t *testing.T) {
			s := newCollectionReplayStore(t, vector.Vector.LibraryID)
			rows := int64(2)
			rejected := false
			for _, frame := range vector.Vector.Frames {
				before := collectionReplayState(t, s)
				next, err := replayCollectionFixtureFrame(t, s, frame, rows, MaxStreamProjectionRows)
				if err != nil {
					if frame.Ordinal != ordinal || ordinal == 0 && frame.Kind != 5 {
						t.Fatalf("negative rejected at unexpected ordinal %d kind %d: %v", frame.Ordinal, frame.Kind, err)
					}
					if !bytes.Equal(before, collectionReplayState(t, s)) {
						t.Fatal("failed semantic replay changed prior state")
					}
					rejected = true
					break
				}
				rows = next
			}
			if !rejected {
				t.Fatal("accepted frozen historical semantic negative")
			}
		})
	}
	if seen != len(wanted) {
		t.Fatalf("frozen semantic cases %d", seen)
	}
}
func TestCollectionReplayHelpersSecurityRemovalAtChargedCaps(t *testing.T) {
	f := readCollectionReplayFixture(t)
	s, rows := replayCollectionFixturePrefix(t, f, 17)
	ctx := context.Background()
	id := CollectionID("col_00000000000000000000000000000002")
	for _, spec := range []struct {
		table, where  string
		events, bytes int64
		args          []any
	}{{"collection_library_quota", "singleton=1", 131072, 256 << 20, nil}, {"collection_quota", "collection_id=?", 8192, 16 << 20, []any{id}}} {
		args := []any{spec.events, spec.events, spec.bytes}
		args = append(args, spec.args...)
		if _, err := s.db.ExecContext(ctx, "UPDATE "+spec.table+" SET policy_events=?-reserved_policy_events,policy_receipts=?-reserved_policy_receipts,canonical_bytes=?-reserved_bytes WHERE "+spec.where, args...); err != nil {
			t.Fatal(err)
		}
	}
	before, err := readCollectionQuota(ctx, s.db, id)
	if err != nil {
		t.Fatal(err)
	}
	frame := collectionFixtureOrdinal(t, f, 18)
	next, err := replayCollectionFixtureFrame(t, s, frame, rows, MaxStreamProjectionRows)
	if err != nil || next != 50 {
		t.Fatalf("attached-credit removal at charged cap: %d %v", next, err)
	}
	after, err := readCollectionQuota(ctx, s.db, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]CollectionQuotaCounters{{before.Library, after.Library}, {*before.Collection, *after.Collection}} {
		a, b := pair[0], pair[1]
		if b.PolicyEvents != a.PolicyEvents+1 || b.PolicyReceipts != a.PolicyReceipts+1 || b.ReservedPolicyEvents != a.ReservedPolicyEvents-1 || b.ReservedPolicyReceipts != a.ReservedPolicyReceipts-1 || b.ReservedBytes != a.ReservedBytes-3072 || b.CanonicalBytes != a.CanonicalBytes+889 {
			t.Fatal("independent P9 cost/credit transition")
		}
	}
	state := collectionReplayState(t, s)
	if _, err := replayCollectionFixtureFrame(t, s, frame, next, MaxStreamProjectionRows); err == nil {
		t.Fatal("spent attached credit twice")
	}
	if !bytes.Equal(state, collectionReplayState(t, s)) {
		t.Fatal("double-spend changed state")
	}
}

func collectionReplayPolicyReceipt(t *testing.T, record CollectionPolicyRecord) []byte {
	t.Helper()
	_, digest, err := CanonicalCollectionPolicyRequest(record.Request)
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalCollectionPolicyReceipt(CollectionPolicyReceipt{2, record.Request.LibraryID, record.Request.ID, digest, record.RecordedUnixNano})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestCollectionReplayHelpersHistoricalPolicyGuards(t *testing.T) {
	f := readCollectionReplayFixture(t)
	t.Run("future_declaration_is_not_definition", func(t *testing.T) {
		s, rows := replayCollectionFixturePrefix(t, f, 3)
		frame := collectionFixtureOrdinal(t, f, 15)
		record, receipt, err := decodeCollectionPolicyPair(collectionFixtureHex(t, frame.PayloadHex), f.LibraryID)
		if err != nil {
			t.Fatal(err)
		}
		before := collectionReplayState(t, s)
		if _, err := s.replayCollectionOwnerPolicy(context.Background(), record, receipt, rows, MaxStreamProjectionRows); !errors.Is(err, ErrNotFound) {
			t.Fatalf("future owner target: %v", err)
		}
		if !bytes.Equal(before, collectionReplayState(t, s)) {
			t.Fatal("future-target failure changed declaration")
		}
	})
	t.Run("full_historical_grant_CAS", func(t *testing.T) {
		s, rows := replayCollectionFixturePrefix(t, f, 14)
		frame := collectionFixtureOrdinal(t, f, 15)
		record, _, err := decodeCollectionPolicyPair(collectionFixtureHex(t, frame.PayloadHex), f.LibraryID)
		if err != nil {
			t.Fatal(err)
		}
		record.Request.Changes[0].Expected.Value.Actions = []string{"collections.read"}
		receipt := collectionReplayPolicyReceipt(t, record)
		before := collectionReplayState(t, s)
		if _, err := s.replayCollectionOwnerPolicy(context.Background(), record, receipt, rows, MaxStreamProjectionRows); !errors.Is(err, ErrConflict) {
			t.Fatalf("whole expected grant CAS: %v", err)
		}
		if !bytes.Equal(before, collectionReplayState(t, s)) {
			t.Fatal("stale whole-grant CAS changed state")
		}
	})
	t.Run("ordinary_last_admin_removal", func(t *testing.T) {
		s, rows := replayCollectionFixturePrefix(t, f, 17)
		frame := collectionFixtureOrdinal(t, f, 18)
		record, _, err := decodeCollectionPolicyPair(collectionFixtureHex(t, frame.PayloadHex), f.LibraryID)
		if err != nil {
			t.Fatal(err)
		}
		record.Request.Category = "collection"
		receipt := collectionReplayPolicyReceipt(t, record)
		before := collectionReplayState(t, s)
		if _, err := s.replayCollectionOwnerPolicy(context.Background(), record, receipt, rows, MaxStreamProjectionRows); !errors.Is(err, ErrConflict) {
			t.Fatalf("ordinary last-admin guard: %v", err)
		}
		if !bytes.Equal(before, collectionReplayState(t, s)) {
			t.Fatal("last-admin rejection changed state")
		}
		if _, err := replayCollectionFixtureFrame(t, s, frame, rows, MaxStreamProjectionRows); err != nil {
			t.Fatalf("unchanged security-removal positive: %v", err)
		}
	})
	for _, target := range []int{1, 2} {
		t.Run(fmt.Sprintf("enabled_target_and_used_reserved_%d", target), func(t *testing.T) {
			s, rows := replayCollectionFixturePrefix(t, f, 19)
			if target == 1 {
				if _, err := s.db.Exec("UPDATE collection_library_quota SET policy_events=131072-reserved_policy_events,policy_receipts=131072-reserved_policy_receipts"); err != nil {
					t.Fatal(err)
				}
			}
			value := CollectionGrantValue{SubjectID(fmt.Sprintf("sub_%032x", target)), CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
			op := CollectionPolicyRequest{2, f.LibraryID, "cpo_000000000000000000000000000000f0", "library_create", "", "", "", []CollectionGrantChange{{"put", "cgr_000000000000000000000000000000f0", nil, &value}}}
			record := CollectionPolicyRecord{2, op, "local_owner", "sub_00000000000000000000000000000001", []CollectionGrantResult{{"cgr_000000000000000000000000000000f0", "cgv_000000000000000000000000000000f0", false}}, 1700000000000000100}
			receipt := collectionReplayPolicyReceipt(t, record)
			before := collectionReplayState(t, s)
			want := ErrConflict
			if target == 1 {
				want = ErrCollectionCapacity
			}
			if _, err := s.replayCollectionOwnerPolicy(context.Background(), record, receipt, rows, MaxStreamProjectionRows); !errors.Is(err, want) {
				t.Fatalf("target/quota historical admission: %v want %v", err, want)
			}
			if !bytes.Equal(before, collectionReplayState(t, s)) {
				t.Fatal("rejected target/quota transaction changed state")
			}
		})
	}
}

func TestCollectionPortabilityPublicIndependentRoundtrip(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"empty", "shared"} {
		t.Run(name, func(t *testing.T) {
			f := readCollectionWireFixture(t, name)
			projection := readCollectionProjectionFixture(t, name)
			source := collectionProjectionFixtureStore(t, projection)
			if err := changedOne(source.db.ExecContext(ctx, "UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0")); err != nil {
				t.Fatal(err)
			}
			wire := collectionFixtureHex(t, f.StreamHex)
			limits := Limits{int64(len(wire)), MaxStreamEvents, f.ProjectionRows}
			var out bytes.Buffer
			if err := source.Export(ctx, f.LibraryID, &out, limits); err != nil || !bytes.Equal(out.Bytes(), wire) {
				t.Fatalf("independent public source export: %v", err)
			}
			path := filepath.Join(t.TempDir(), "restored.sqlite")
			if err := Restore(ctx, path, f.LibraryID, bytes.NewReader(wire), limits); err != nil {
				t.Fatal(err)
			}
			restored, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer restored.Close()
			if restored.version != 5 {
				t.Fatal("format3 restored into wrong schema")
			}
			if err := validateInactiveCollectionImport(ctx, restored.db); err != nil {
				t.Fatal(err)
			}
			out.Reset()
			if err := restored.Export(ctx, f.LibraryID, &out, limits); err != nil || !bytes.Equal(out.Bytes(), wire) {
				t.Fatalf("exact public export after restore: %v", err)
			}
			for _, event := range projection.Events {
				table := "events"
				switch event.Family {
				case "legacy_policy":
					table = "policy_events"
				case "content":
					table = "collection_events"
				case "owner_policy", "generated_policy":
					table = "collection_policy_events"
				}
				var request, receipt []byte
				if err := restored.db.QueryRowContext(ctx, "SELECT request,receipt FROM "+table+" WHERE sequence=?", event.Sequence).Scan(&request, &receipt); err != nil {
					t.Fatal(err)
				}
				if string(request) != event.Request || string(receipt) != event.Receipt {
					t.Fatal("original request/receipt changed")
				}
			}
			digest, rows, err := projectionDigest(ctx, restored.db, f.LibraryID, f.ProjectionRows)
			if err != nil || digest != f.ProjectionSHA256 || rows != f.ProjectionRows {
				t.Fatalf("public restored projection: %s/%d %v", digest, rows, err)
			}
		})
	}
}
func TestCollectionPortabilityLiveSharedRecoveryAndCurrentRetry(t *testing.T) {
	s, _, _, d, oldA, oldB := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 70)
	created := collectionTxCall(t, d, oldA, create)
	bValue := CollectionGrantValue{readerSubject, CollectionGrantScope{"collection", create.CollectionID}, []string{"collections.admin", "collections.edit", "collections.read"}}
	share := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000047", create.CollectionID, created.ResultVersion, "grants", collectionTxRaw(t, EditCollectionGrants{[]CollectionGrantChange{{"put", "cgr_33333333333333333333333333333333", nil, &bValue}}})}
	shared := collectionTxCall(t, d, oldA, share)
	rename := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000048", create.CollectionID, shared.ResultVersion, "rename", collectionTxRaw(t, RenameCollection{"B retained hidden membership"})}
	renamed := collectionTxCall(t, d, oldB, rename)
	archive := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000049", create.CollectionID, renamed.ResultVersion, "archive", collectionTxRaw(t, SetCollectionArchived{true})}
	archived := collectionTxCall(t, d, oldA, archive)
	grant, err := readCollectionGrant(ctx, s.db, "cgr_22222222222222222222222222222222")
	if err != nil {
		t.Fatal(err)
	}
	aValue := grant.expected.Value
	aValue.Actions = []string{"collections.edit", "collections.read"}
	downgrade := CollectionRequest{1, testLibrary, "cop_0000000000000000000000000000004a", create.CollectionID, archived.ResultVersion, "grants", collectionTxRaw(t, EditCollectionGrants{[]CollectionGrantChange{{"put", grant.id, &grant.expected, &aValue}}})}
	current := collectionTxCall(t, d, oldA, downgrade)
	limits := Limits{8 << 20, 100, 1000}
	var portable bytes.Buffer
	if err := s.Export(ctx, testLibrary, &portable, limits); err != nil {
		t.Fatal(err)
	}
	wire := append([]byte{}, portable.Bytes()...)
	path := filepath.Join(t.TempDir(), "shared.sqlite")
	if err := Restore(ctx, path, testLibrary, bytes.NewReader(wire), limits); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	restored.clock = s.clock
	for _, token := range [][]byte{oldA, oldB} {
		if _, err := restored.Authenticate(ctx, testLibrary, token); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("old source bearer active after import: %v", err)
		}
	}
	owner, err := restored.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Recover(ctx, adminTestRequest("import-premature-recover", "", "recover", RecoverRequest{readerSubject})); err == nil {
		t.Fatal("Recover fabricated inactive owner binding")
	}
	boot, err := owner.Bootstrap(ctx, adminTestRequest("import-bind-person", "", "bootstrap", BootstrapRequest{readerSubject, nil}))
	if err != nil {
		t.Fatal(err)
	}
	authority := AuthorityStatus{boot.Result.Incarnation, readerSubject}
	a, _ := collectionTxIssue(t, owner, authority, "import-fresh-A", ownerSubject, collectionTxActions)
	b, _ := collectionTxIssue(t, owner, authority, "import-fresh-B", readerSubject, collectionTxActions)
	for _, token := range [][]byte{oldA, oldB} {
		if _, err := restored.Authenticate(ctx, testLibrary, token); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("old bearer active after fresh binding: %v", err)
		}
	}
	portable.Reset()
	if err := restored.Export(ctx, testLibrary, &portable, limits); err != nil || !bytes.Equal(portable.Bytes(), wire) {
		t.Fatalf("no-policy binding/issuance changed portable state: %v", err)
	}
	rd, err := NewDiscoveryService(ctx, restored, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close(ctx)
	if got := collectionTxCall(t, rd, a.Bearer, archive); got != archived || got.ResultVersion == current.ResultVersion {
		t.Fatal("historical archive retry lost after current admin removal")
	}
	if got := collectionTxCall(t, rd, b.Bearer, rename); got != renamed {
		t.Fatal("B actor-scoped historical retry changed")
	}
	freshArchive := archive
	freshArchive.ID = "cop_0000000000000000000000000000004b"
	freshArchive.ExpectedVersion = current.ResultVersion
	freshArchive.Value = collectionTxRaw(t, SetCollectionArchived{false})
	collectionTxDenied(t, rd, a.Bearer, freshArchive, ErrNotFound)
	def, err := readCollectionDefinition(ctx, restored.db, create.CollectionID)
	if err != nil || def.creator != ownerSubject || def.name != "B retained hidden membership" || !def.archived {
		t.Fatalf("shared imported definition: %+v %v", def, err)
	}
	var members int
	if err := restored.db.QueryRowContext(ctx, "SELECT count(*) FROM collection_entries WHERE collection_id=? AND removed=0", create.CollectionID).Scan(&members); err != nil || members != 2 {
		t.Fatalf("hidden member retained: %d %v", members, err)
	}
	recovered, err := owner.Recover(ctx, adminTestRequest("import-rotate", authority.Incarnation, "recover", RecoverRequest{readerSubject}))
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Result.OwnerSubjectID != readerSubject || recovered.Result.Incarnation == authority.Incarnation {
		t.Fatal("Recover changed owner or failed to rotate")
	}
	for _, token := range [][]byte{a.Bearer, b.Bearer} {
		if _, err := restored.Authenticate(ctx, testLibrary, token); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal("recovery retained old fresh credential")
		}
	}
	authority.Incarnation = recovered.Result.Incarnation
	a, _ = collectionTxIssue(t, owner, authority, "import-rotated-A", ownerSubject, collectionTxActions)
	b, _ = collectionTxIssue(t, owner, authority, "import-rotated-B", readerSubject, collectionTxActions)
	if got := collectionTxCall(t, rd, a.Bearer, archive); got != archived {
		t.Fatal("retry after rotation changed")
	}
	grant, err = readCollectionGrant(ctx, restored.db, "cgr_22222222222222222222222222222222")
	if err != nil {
		t.Fatal(err)
	}
	readOnly := grant.expected.Value
	readOnly.Actions = []string{"collections.read"}
	revokeEdit := CollectionRequest{1, testLibrary, "cop_0000000000000000000000000000004c", create.CollectionID, current.ResultVersion, "grants", collectionTxRaw(t, EditCollectionGrants{[]CollectionGrantChange{{"put", grant.id, &grant.expected, &readOnly}}})}
	collectionTxCall(t, rd, b.Bearer, revokeEdit)
	collectionTxDenied(t, rd, a.Bearer, archive, ErrNotFound)
}

type collectionSnapshotHoldWriter struct {
	bytes.Buffer
	entered chan struct{}
	release chan struct{}
	held    bool
}

func (w *collectionSnapshotHoldWriter) Write(p []byte) (int, error) {
	if !w.held {
		w.held = true
		close(w.entered)
		<-w.release
	}
	return w.Buffer.Write(p)
}
func TestCollectionPortabilityHeldLiveSnapshot(t *testing.T) {
	s, _, _, d, a, _ := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 80)
	created := collectionTxCall(t, d, a, create)
	held := &collectionSnapshotHoldWriter{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- s.Export(ctx, testLibrary, held, Limits{8 << 20, 100, 1000}) }()
	select {
	case <-held.entered:
	case err := <-done:
		t.Fatalf("export returned before held writer: %v", err)
	}
	rename := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000051", create.CollectionID, created.ResultVersion, "rename", collectionTxRaw(t, RenameCollection{"After held snapshot"})}
	var out bytes.Buffer
	_, mutationErr := d.MutateCollection(ctx, a, rename, &out)
	close(held.release)
	exportErr := <-done
	if mutationErr != nil || exportErr != nil {
		t.Fatalf("held snapshot/live write: %v %v", mutationErr, exportErr)
	}
	path := filepath.Join(t.TempDir(), "held.sqlite")
	if err := Restore(ctx, path, testLibrary, bytes.NewReader(held.Bytes()), Limits{8 << 20, 100, 1000}); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	old, err := readCollectionDefinition(ctx, restored.db, create.CollectionID)
	if err != nil || old.name != "Project" || old.version != created.ResultVersion {
		t.Fatalf("mixed exporter snapshot: %+v %v", old, err)
	}
	current, err := readCollectionDefinition(ctx, s.db, create.CollectionID)
	if err != nil || current.name != "After held snapshot" {
		t.Fatal("live write lost")
	}
}
