package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type deliveryRichFixture struct {
	LibraryID        continuity.LibraryID `json:"library_id"`
	StreamHex        string               `json:"stream_hex"`
	FileSHA256       string               `json:"file_sha256"`
	ProjectionRows   int64                `json:"projection_rows"`
	ProjectionSHA256 string               `json:"projection_sha256"`
	ProjectionHex    string               `json:"projection_hex"`
	Projections      []struct {
		Kind     byte   `json:"kind"`
		JSON     string `json:"json"`
		FrameHex string `json:"frame_hex"`
	} `json:"projections"`
	Checkpoints []struct {
		Ordinal     int64    `json:"ordinal"`
		Rows        int64    `json:"projection_rows"`
		Generations []*int64 `json:"generations_A_B_C_D"`
	} `json:"expected_after_each_ordinal"`
}

func deliveryRichTest(t *testing.T) (deliveryRichFixture, []byte) {
	t.Helper()
	data, err := os.ReadFile("testdata/continuity-c05-rich.json")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != "16a9ab23d94a89ccf7fca6d849647a7e7ae3a7035dabe52515fd080259dcc90f" {
		t.Fatal("frozen rich fixture file changed")
	}
	var f deliveryRichFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	wire := collectionFixtureHex(t, f.StreamHex)
	if fmt.Sprintf("%x", sha256.Sum256(wire)) != f.FileSHA256 || f.LibraryID != testLibrary || f.ProjectionRows != 25 || len(f.Projections) != 25 || len(f.Checkpoints) != 8 {
		t.Fatal("rich literal fixture identity/count changed")
	}
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.projection\x00\x05"))
	h.Write(collectionFixtureHex(t, f.ProjectionHex))
	if hex.EncodeToString(h.Sum(nil)) != f.ProjectionSHA256 {
		t.Fatal("independent rich projection digest")
	}
	return f, wire
}

// This expected prefix uses typed record replay directly, never public Restore
// or Export. Its scalar rows/generations and all final projection frames come
// from the independent Python recipe's frozen literal claims.
func deliveryOraclePrefix(t *testing.T, wire []byte, through int64) (*Store, int64) {
	t.Helper()
	ctx := context.Background()
	s, err := openStoreVersionContext(ctx, filepath.Join(t.TempDir(), "prefix.sqlite"), testLibrary, true, false, 7)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	rows, metadata, legacy, ordinal := int64(3), int64(0), int64(0), int64(0)
	for _, f := range savedQueryPortableFrames(t, wire, len(streamMagic5)) {
		if f.kind == 1 || f.kind == 3 {
			continue
		}
		if f.kind == 4 || f.kind == 6 || f.kind == 7 || f.kind == 8 {
			ordinal++
			if ordinal > through {
				return s, rows
			}
		}
		switch f.kind {
		case 2:
			metadata++
			e, op, eerr := decodeStreamEvent(f.payload, testLibrary, metadata)
			if eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = s.replayEvent(ctx, e, op, rows, MaxStreamProjectionRows)
		case 4:
			legacy++
			e, op, eerr := decodeStreamPolicyEvent(f.payload, testLibrary, legacy)
			if eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = s.replayPolicyEvent(ctx, e, op, rows, MaxStreamProjectionRows)
		case 5:
			var d collectionDeclaration
			if eerr := canonicalJSON(f.payload, &d); eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = s.replayCollectionDeclaration(ctx, d, rows, MaxStreamProjectionRows)
		case 6:
			r, receipt, eerr := decodeCollectionPolicyPair(f.payload, testLibrary)
			if eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = s.replayCollectionOwnerPolicy(ctx, r, receipt, rows, MaxStreamProjectionRows)
		case 7:
			replay, _, eerr := decodePortableCollectionContent(f.payload, testLibrary, 5)
			if eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = replay(ctx, s, rows, MaxStreamProjectionRows)
		case 8:
			r, eerr := DecodeBytePolicyRecord(f.payload)
			if eerr != nil {
				t.Fatal(eerr)
			}
			rows, err = s.replayBytePolicy(ctx, r, rows, MaxStreamProjectionRows)
		default:
			t.Fatal("unexpected oracle prefix frame")
		}
		if err != nil {
			t.Fatalf("valid prefix ordinal %d: %v", ordinal, err)
		}
	}
	return s, rows
}
func deliveryByteOrdinalFrame(t *testing.T, fs []savedQueryPortableFrame, sequence int64) (int, int64) {
	t.Helper()
	var ordinal int64
	for i, f := range fs {
		if f.kind == 4 || f.kind == 6 || f.kind == 7 || f.kind == 8 {
			ordinal++
		}
		if f.kind == 8 {
			r, err := DecodeBytePolicyRecord(f.payload)
			if err != nil {
				t.Fatal(err)
			}
			if r.Receipt.Sequence == sequence {
				return i, ordinal
			}
		}
	}
	t.Fatal("missing byte ordinal")
	return 0, 0
}
func deliveryAssertIncompletePrefix(t *testing.T, path string, expected []byte, ordinal int64) {
	t.Helper()
	db, err := openDB(path, true)
	if err != nil {
		t.Fatalf("missing retained target after ordinal %d refusal: %v", ordinal, err)
	}
	defer db.Close()
	var ready, last int64
	if err := db.QueryRow(`SELECT ready,(SELECT COALESCE(max(ordinal),0) FROM collection_order) FROM catalog WHERE singleton=1`).Scan(&ready, &last); err != nil {
		t.Fatal(err)
	}
	if ready != 0 || last != ordinal {
		t.Fatalf("wrong retained prefix: ready=%d ordinal=%d want %d", ready, last, ordinal)
	}
	if !bytes.Equal(expected, deliveryPortableSnapshot(t, &Store{db: db})) {
		t.Fatal("refused byte ordinal committed part of grants/results/generations/quota/order or changed other tables")
	}
	if s, err := Open(path); !errors.Is(err, ErrFormat) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("incomplete target must refuse ready validation: %v", err)
	}
}

func TestDeliveryPortabilityRichIndependentWireAndProjection(t *testing.T) {
	f, wire := deliveryRichTest(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rich.sqlite")
	if err := Restore(ctx, path, testLibrary, bytes.NewReader(wire), Limits{int64(len(wire)), 9, 25}); err != nil {
		t.Fatal(err)
	}
	s := deliveryOpenTest(t, path)
	if !bytes.Equal(deliveryExportTest(t, s), wire) {
		t.Fatal("nonempty literal request/receipt/wire bytes changed")
	}
	digest, rows, err := projectionDigest(ctx, s.db, testLibrary, 25)
	if err != nil || rows != 25 || digest != f.ProjectionSHA256 {
		t.Fatalf("independent nonempty projection: %s/%d %v", digest, rows, err)
	}
	// Exact new-row tags, field order, values, tombstones and immutable results
	// are compared directly with literal independently authored projection rows.
	var expected []struct {
		kind byte
		data string
	}
	for _, p := range f.Projections {
		if p.Kind >= 0x22 {
			expected = append(expected, struct {
				kind byte
				data string
			}{p.Kind, p.JSON})
		}
	}
	seen := 0
	if err := walkDeliveryProjections(ctx, s.db, testLibrary, func(kind byte, value any) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if seen >= len(expected) || kind != expected[seen].kind || string(data) != expected[seen].data {
			t.Fatalf("literal new policy projection row %d differs: kind%x %s", seen, kind, data)
		}
		seen++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 10 || seen != len(expected) {
		t.Fatal("rich byte projection coverage")
	}
	for _, cp := range f.Checkpoints {
		prefix, observed := deliveryOraclePrefix(t, wire, cp.Ordinal)
		actual, err := projectionTotal(ctx, prefix.db)
		if err != nil || observed != cp.Rows || actual != cp.Rows {
			t.Fatalf("literal ordinal %d row budget: %d/%d want%d %v", cp.Ordinal, observed, actual, cp.Rows, err)
		}
		for i, generation := range cp.Generations {
			id := SubjectID(fmt.Sprintf("sub_%s", strings.Repeat(fmt.Sprint(i+1), 32)))
			sub, err := readSubject(ctx, prefix.db, testLibrary, id)
			if generation == nil {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("ordinal%d absent subject%d: %v", cp.Ordinal, i, err)
				}
			} else if err != nil || sub.AuthorizationGeneration != *generation {
				t.Fatalf("ordinal%d literal generation%d: %d want%d %v", cp.Ordinal, i, sub.AuthorizationGeneration, *generation, err)
			}
		}
	}
	// Independent direct inventory: every nonportable operational table is empty,
	// and fresh singletons contain zero scalars with the correct library.
	for _, table := range []string{"catalog_authority", "catalog_admin_events", "catalog_admin_codecs", "catalog_credentials", "credential_issuance_receipts", "credential_codecs", "preference_subjects", "preferences", "preference_receipts", "delivery_sources", "delivery_source_heads", "delivery_source_events", "delivery_selections", "delivery_materializations"} {
		var n int64
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("nonportable table %s: %d %v", table, n, err)
		}
	}
	var nonzero int64
	if err := s.db.QueryRow(`SELECT (SELECT clock_floor_unix+subject_count+entry_count+receipt_count FROM preference_control WHERE singleton=1 AND library_id=?)+(SELECT high_water_unix FROM delivery_clock WHERE singleton=1 AND library_id=?)+(SELECT source_versions+selections+attempts+canonical_bytes+reserved_logical_bytes FROM delivery_quota WHERE singleton=1 AND library_id=?)`, testLibrary, testLibrary, testLibrary).Scan(&nonzero); err != nil || nonzero != 0 {
		t.Fatalf("fresh operational singletons: %d %v", nonzero, err)
	}
}

func TestDeliveryPortabilitySemanticRefusalPreservesOrdinal(t *testing.T) {
	_, wire := deliveryRichTest(t)
	for _, tc := range []struct {
		name     string
		sequence int64
		want     error
		stage    string
		mutate   func(*BytePolicyRecord)
	}{
		{"owner_absent_at_prefix", 1, ErrNotFound, "restore byte policy replay", func(r *BytePolicyRecord) { r.OperatorSubjectID = "sub_99999999999999999999999999999999" }},
		{"owner_disabled_at_prefix", 4, ErrConflict, "restore byte policy replay", func(r *BytePolicyRecord) { r.OperatorSubjectID = "sub_11111111111111111111111111111111" }},
		{"enabled_service_owner", 1, ErrConflict, "restore byte policy replay", func(r *BytePolicyRecord) { r.OperatorSubjectID = "sub_33333333333333333333333333333333" }},
		{"stale_expected_grant", 2, ErrConflict, "restore byte policy replay", func(r *BytePolicyRecord) {
			r.Request.Changes[0].Expected.Version = "bgv_11111111111111111111111111111111"
		}},
		{"reused_historical_token", 4, ErrConflict, "restore byte policy replay", func(r *BytePolicyRecord) { r.Receipt.Results[0].Version = "bgv_11111111111111111111111111111111" }},
		{"altered_receipt_binding", 1, ErrInvalid, "byte policy record", func(r *BytePolicyRecord) { r.Receipt.PayloadSHA256 = strings.Repeat("0", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := savedQueryPortableFrames(t, wire, len(streamMagic5))
			index, ordinal := deliveryByteOrdinalFrame(t, fs, tc.sequence)
			prefix, rows := deliveryOraclePrefix(t, wire, ordinal-1)
			before := deliveryPortableSnapshot(t, prefix)
			original, err := DecodeBytePolicyRecord(fs[index].payload)
			if err != nil {
				t.Fatal(err)
			}
			// Every negative has a same-prefix unmodified positive that commits
			// the intended byte record, with an exact row-count consequence.
			next, err := prefix.replayBytePolicy(context.Background(), original, rows, MaxStreamProjectionRows)
			cost := int64(2)
			if tc.sequence == 1 {
				cost = 7
			}
			if err != nil || next != rows+cost {
				t.Fatalf("same-prefix valid byte control: %d %v", next, err)
			}
			bad := deliveryRewriteTest(t, wire, func(frames []savedQueryPortableFrame) []savedQueryPortableFrame {
				r, err := DecodeBytePolicyRecord(frames[index].payload)
				if err != nil {
					t.Fatal(err)
				}
				tc.mutate(&r)
				if tc.name != "altered_receipt_binding" {
					_, digest, err := CanonicalBytePolicyRequest(r.Request)
					if err != nil {
						t.Fatal(err)
					}
					r.Receipt.PayloadSHA256 = digest
					if _, _, err := CanonicalBytePolicyRecord(r); err != nil {
						t.Fatalf("negative fails canonical shape before intended replay: %v", err)
					}
				}
				frames[index].payload, err = json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				return frames
			})
			path := filepath.Join(t.TempDir(), "refused.sqlite")
			err = Restore(context.Background(), path, testLibrary, bytes.NewReader(bad), collectionWireLimits())
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.stage) {
				t.Fatalf("wrong ordinal refusal (wanted %v at %s): %v", tc.want, tc.stage, err)
			}
			deliveryAssertIncompletePrefix(t, path, before, ordinal-1)
		})
	}
}

func TestDeliveryPortabilityGenerationOverflowAtomicRollback(t *testing.T) {
	_, wire := deliveryRichTest(t)
	fs := savedQueryPortableFrames(t, wire, len(streamMagic5))
	index, ordinal := deliveryByteOrdinalFrame(t, fs, 1)
	s, rows := deliveryOraclePrefix(t, wire, ordinal-1)
	r, err := DecodeBytePolicyRecord(fs[index].payload)
	if err != nil {
		t.Fatal(err)
	}
	// Explicit synthetic overflow setup on an incomplete prefix. Production
	// history cannot practically contain MaxInt64 generation increments.
	if _, err := s.db.Exec(`UPDATE policy_subjects SET authorization_generation=? WHERE subject_id=?`, int64(math.MaxInt64), r.Request.Changes[0].Value.SubjectID); err != nil {
		t.Fatal(err)
	}
	before := deliveryPortableSnapshot(t, s)
	if next, err := s.replayBytePolicy(context.Background(), r, rows, MaxStreamProjectionRows); !errors.Is(err, ErrConflict) || next != 0 {
		t.Fatalf("generation overflow refusal: %d %v", next, err)
	}
	if !bytes.Equal(before, deliveryPortableSnapshot(t, s)) || s.db.Stats().InUse != 0 {
		t.Fatal("overflow retained any byte ordinal mutation or SQL owner")
	}
	valid, validRows := deliveryOraclePrefix(t, wire, ordinal-1)
	if _, err := valid.replayBytePolicy(context.Background(), r, validRows, MaxStreamProjectionRows); err != nil {
		t.Fatalf("unchanged non-overflow positive: %v", err)
	}
}

func TestDeliveryPortabilityPublicBoundsBeforeReservation(t *testing.T) {
	_, wire := deliveryRichTest(t)
	for _, tc := range []struct {
		name   string
		limits Limits
		want   error
		stage  string
		change func(*deliveryStreamHeader)
	}{
		{"negative_byte_count", collectionWireLimits(), ErrFormat, "byte policy stream counts", func(h *deliveryStreamHeader) { h.BytePolicyEventCount = -1 }},
		{"excess_byte_count", collectionWireLimits(), ErrFormat, "byte policy stream counts", func(h *deliveryStreamHeader) { h.BytePolicyEventCount = 131073 }},
		{"transaction_under_count", collectionWireLimits(), ErrFormat, "collection transaction count", func(h *deliveryStreamHeader) { h.TransactionCount = 7 }},
		{"wrong_schema", collectionWireLimits(), ErrFormat, "delivery stream identity", func(h *deliveryStreamHeader) { h.SchemaVersion = 6 }},
		{"combined_event_limit", Limits{MaxStreamBytes, 8, 25}, ErrLimit, "limit", nil},
		{"initial_projection_limit", Limits{MaxStreamBytes, 9, 2}, ErrLimit, "limit", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := deliveryRewriteTest(t, wire, func(fs []savedQueryPortableFrame) []savedQueryPortableFrame {
				if tc.change != nil {
					var h deliveryStreamHeader
					if err := json.Unmarshal(fs[0].payload, &h); err != nil {
						t.Fatal(err)
					}
					tc.change(&h)
					fs[0].payload, _ = json.Marshal(h)
				}
				return fs
			})
			path := filepath.Join(t.TempDir(), "unreserved.sqlite")
			err := Restore(context.Background(), path, testLibrary, bytes.NewReader(bad), tc.limits)
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.stage) {
				t.Fatalf("wrong header/budget refusal: %v", err)
			}
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid header reserved target: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		limits Limits
		want   error
		stage  string
	}{
		{"exact", Limits{int64(len(wire)), 9, 25}, nil, ""},
		{"one_byte_short", Limits{int64(len(wire)) - 1, 9, 25}, ErrLimit, "limit"},
		{"one_projection_row_short", Limits{int64(len(wire)), 9, 24}, ErrLimit, "restore byte policy replay"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bounded.sqlite")
			err := Restore(context.Background(), path, testLibrary, bytes.NewReader(wire), tc.limits)
			if tc.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.stage) {
				t.Fatalf("wrong exact-bound refusal: %v", err)
			}
			through := int64(8)
			if tc.name == "one_projection_row_short" {
				through = 7
			}
			prefix, _ := deliveryOraclePrefix(t, wire, through)
			deliveryAssertIncompletePrefix(t, path, deliveryPortableSnapshot(t, prefix), through)
		})
	}
}
