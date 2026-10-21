package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type collectionWireFixture struct {
	LibraryID        continuity.LibraryID `json:"library_id"`
	StreamHex        string               `json:"stream_hex"`
	FileSHA256       string               `json:"file_sha256"`
	StreamSHA256     string               `json:"stream_sha256"`
	HeaderJSON       string               `json:"header_json"`
	FooterJSON       string               `json:"footer_json"`
	ProjectionRows   int64                `json:"projection_rows"`
	ProjectionSHA256 string               `json:"projection_sha256"`
	Frames           []struct {
		Kind       byte   `json:"kind"`
		PayloadHex string `json:"payload_hex"`
		FrameHex   string `json:"frame_hex"`
	} `json:"frames"`
}

func readCollectionWireFixture(t *testing.T, name string) collectionWireFixture {
	t.Helper()
	b, err := os.ReadFile("testdata/continuity-c04b-" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var f collectionWireFixture
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	return f
}
func collectionFixtureHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func collectionWireLimits() Limits {
	return Limits{MaxStreamBytes, MaxStreamEvents, MaxStreamProjectionRows}
}

// These tests exercise literal wire primitives only. Full Store replay and
// lifecycle acceptance have separate requirements and are not claimed here.
func TestCollectionPortabilityLiteralWire(t *testing.T) {
	for _, name := range []string{"empty", "shared"} {
		t.Run(name, func(t *testing.T) {
			f := readCollectionWireFixture(t, name)
			raw := collectionFixtureHex(t, f.StreamHex)
			sum := sha256.Sum256(raw)
			if hex.EncodeToString(sum[:]) != f.FileSHA256 {
				t.Fatal("independent stream file digest")
			}
			in := streamInput{ctx: context.Background(), r: bytes.NewReader(raw), max: int64(len(raw))}
			magic := make([]byte, len(streamMagic3))
			if err := in.exact(magic); err != nil {
				t.Fatal(err)
			}
			if string(magic) != streamMagic3 {
				t.Fatal("format3 magic")
			}
			h := collectionStreamHash()
			h.Write(magic)
			var out bytes.Buffer
			output := streamOutput{ctx: context.Background(), w: &out, max: int64(len(raw))}
			if err := output.write(magic, nil); err != nil {
				t.Fatal(err)
			}
			var header collectionStreamHeader
			var footer collectionStreamFooter
			var metadata, legacy, content, policy, ordinal int64
			for i, frame := range f.Frames {
				var payload []byte
				var kind byte
				var err error
				transaction := frame.Kind == 4 || frame.Kind == 6 || frame.Kind == 7
				frameHash := h
				if frame.Kind == 3 {
					frameHash = nil
				}
				kind, payload, err = in.collectionFrame(transaction, frame.Kind, frameHash)
				if err != nil {
					t.Fatalf("frame %d: %v", i, err)
				}
				if kind != frame.Kind || !bytes.Equal(payload, collectionFixtureHex(t, frame.PayloadHex)) {
					t.Fatalf("frame %d differs from literal", i)
				}
				switch kind {
				case 1:
					if string(payload) != f.HeaderJSON {
						t.Fatal("header literal")
					}
					header, err = decodeCollectionHeader(payload, f.LibraryID, collectionWireLimits())
					if err != nil {
						t.Fatal(err)
					}
					b, _ := json.Marshal(header)
					if !bytes.Equal(b, payload) {
						t.Fatal("header bytes")
					}
				case 2:
					metadata++
					if _, _, err = decodeStreamEvent(payload, f.LibraryID, metadata); err != nil {
						t.Fatal(err)
					}
				case 5:
					var d collectionDeclaration
					if err := canonicalJSON(payload, &d); err != nil {
						t.Fatal(err)
					}
					if err := d.validate(f.LibraryID); err != nil {
						t.Fatal(err)
					}
				case 4:
					ordinal++
					legacy++
					if _, _, err = decodeStreamPolicyEvent(payload, f.LibraryID, legacy); err != nil {
						t.Fatal(err)
					}
				case 6:
					ordinal++
					policy++
					p, receipt, err := decodeCollectionPolicyPair(payload, f.LibraryID)
					if err != nil {
						t.Fatal(err)
					}
					if p.Administration != "local_owner" {
						t.Fatal("standalone generated event")
					}
					a, _, _ := CanonicalCollectionPolicyRecord(p)
					if !bytes.Equal(appendCollectionPair(nil, a, receipt), payload) {
						t.Fatal("owner pair bytes")
					}
				case 7:
					ordinal++
					content++
					r, receipt, p, childReceipt, err := decodeCollectionContentPair(payload, f.LibraryID)
					if err != nil {
						t.Fatal(err)
					}
					a, _, _ := CanonicalCollectionRecord(r)
					rebuilt := appendCollectionPair(nil, a, receipt)
					if p != nil {
						policy++
						a, _, _ := CanonicalCollectionPolicyRecord(*p)
						rebuilt = appendCollectionPair(rebuilt, a, childReceipt)
					} else {
						rebuilt = appendCollectionPair(rebuilt, nil, nil)
					}
					if !bytes.Equal(rebuilt, payload) {
						t.Fatal("content pair bytes")
					}
				case 3:
					if string(payload) != f.FooterJSON {
						t.Fatal("footer literal")
					}
					footer, err = decodeCollectionFooter(payload, header.collectionStreamCounts, collectionWireLimits())
					if err != nil {
						t.Fatal(err)
					}
					b, _ := json.Marshal(footer)
					if !bytes.Equal(b, payload) {
						t.Fatal("footer bytes")
					}
				}
				if err := output.frame(kind, payload, nil); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(out.Bytes()[out.Len()-5-len(payload):], collectionFixtureHex(t, frame.FrameHex)) {
					t.Fatalf("frame %d framing bytes", i)
				}
			}
			if err := in.end(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, out.Bytes()) {
				t.Fatal("complete literal roundtrip")
			}
			c := header.collectionStreamCounts
			if metadata != c.EventCount || legacy != c.PolicyEventCount || content != c.CollectionEventCount || policy != c.CollectionPolicyEventCount || ordinal != c.TransactionCount {
				t.Fatal("literal family coverage")
			}
			digest, err := finishCollectionStreamHash(h, c, footer.ProjectionRows, footer.ProjectionSHA256)
			if err != nil {
				t.Fatal(err)
			}
			if digest != f.StreamSHA256 || digest != footer.StreamSHA256 || footer.ProjectionRows != f.ProjectionRows || footer.ProjectionSHA256 != f.ProjectionSHA256 {
				t.Fatal("literal final digest")
			}
		})
	}
}

type collectionProjectionFixture struct {
	LibraryID        continuity.LibraryID `json:"library_id"`
	ProjectionRows   int64                `json:"projection_rows"`
	ProjectionSHA256 string               `json:"projection_sha256"`
	StreamHex        string               `json:"stream_hex"`
	Projections      []struct {
		Kind     byte   `json:"kind"`
		JSON     string `json:"json"`
		FrameHex string `json:"frame_hex"`
	} `json:"projection_frames"`
	Events []struct {
		Family   string `json:"family"`
		Sequence int64  `json:"sequence"`
		Request  string `json:"request_json"`
		Receipt  string `json:"receipt_json"`
		Record   string `json:"record_json"`
		Digest   string `json:"payload_sha256"`
	} `json:"events"`
}

func readCollectionProjectionFixture(t *testing.T, name string) collectionProjectionFixture {
	t.Helper()
	data, err := os.ReadFile("testdata/continuity-c04b-" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var f collectionProjectionFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}
func projectionFixtureJSON(t *testing.T, data string, value any) {
	t.Helper()
	if err := json.Unmarshal([]byte(data), value); err != nil {
		t.Fatal(err)
	}
}

// Seed the literal source projections and original events directly. This is an
// independent projection-reader test, not candidate replay or a restore oracle.
// The empty/shared vectors intentionally have no revision/edge/location rows.
func collectionProjectionFixtureStore(t *testing.T, f collectionProjectionFixture) *Store {
	t.Helper()
	ctx := context.Background()
	s, err := openStoreVersionContext(ctx, filepath.Join(t.TempDir(), "projection.sqlite"), f.LibraryID, true, false, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	exec := func(statement string, args ...any) {
		t.Helper()
		if _, err := tx.ExecContext(ctx, statement, args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range f.Events {
		if e.Family != "metadata" && e.Family != "legacy_policy" {
			continue
		}
		var receipt Receipt
		projectionFixtureJSON(t, e.Receipt, &receipt)
		table := "events"
		if e.Family == "legacy_policy" {
			table = "policy_events"
		}
		exec("INSERT INTO "+table+"(sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt) VALUES(?,?,?,?,?,?,?)", e.Sequence, f.LibraryID, receipt.OperationID, []byte(e.Request), e.Digest, receipt.RecordedUnixNano, []byte(e.Receipt))
	}
	for _, row := range f.Projections {
		switch row.Kind {
		case 0x11:
		case 0x12:
			var p assetProjection
			projectionFixtureJSON(t, row.JSON, &p)
			var head any
			if p.Head != "" {
				head = p.Head
			}
			exec("INSERT INTO assets(asset_id,library_id,label,head,created_sequence,updated_sequence) VALUES(?,?,?,?,?,?)", p.AssetID, p.LibraryID, p.Label, head, p.CreatedSequence, p.UpdatedSequence)
		case 0x17:
			var p Subject
			projectionFixtureJSON(t, row.JSON, &p)
			exec("INSERT INTO policy_subjects(subject_id,library_id,kind,label,enabled,authorization_generation,created_sequence,updated_sequence) VALUES(?,?,?,?,?,?,?,?)", p.ID, p.LibraryID, p.Value.Kind, p.Value.Label, p.Value.Enabled, p.AuthorizationGeneration, p.CreatedSequence, p.UpdatedSequence)
		case 0x18:
			var p Grant
			projectionFixtureJSON(t, row.JSON, &p)
			exec("INSERT INTO policy_grants(grant_id,library_id,subject_id,scope_kind,asset_id,actions,created_sequence,updated_sequence,removed) VALUES(?,?,?,?,?,?,?,?,?)", p.ID, p.LibraryID, p.Value.SubjectID, p.Value.Scope.Kind, nullableAsset(p.Value.Scope), actionsBytes(p.Value.Actions), p.CreatedSequence, p.UpdatedSequence, p.Removed)
		case 0x19:
			var p collectionIdentityProjection
			projectionFixtureJSON(t, row.JSON, &p)
			exec("INSERT INTO collection_identities(collection_id,library_id,creator_subject_id,creation_operation_id,creation_payload_sha256,policy_generation) VALUES(?,?,?,?,?,?)", p.CollectionID, p.LibraryID, p.CreatorSubjectID, p.CreationOperationID, p.CreationPayloadSHA256, p.PolicyGeneration)
		default:
			if row.Kind < 0x1a || row.Kind > 0x20 {
				t.Fatalf("unsupported independent projection fixture kind %x", row.Kind)
			}
		}
	}
	children := make(map[string]int64)
	for _, e := range f.Events {
		if e.Family != "owner_policy" && e.Family != "generated_policy" {
			continue
		}
		var p CollectionPolicyRecord
		projectionFixtureJSON(t, e.Record, &p)
		namespace := ""
		if p.Administration == "collection_service" {
			namespace = string(p.OperatorSubjectID)
			children[namespace+"/"+string(p.Request.ID)] = e.Sequence
		}
		exec("INSERT INTO collection_policy_events(sequence,library_id,administration,namespace_subject_id,operator_subject_id,operation_id,category,collection_id,request,payload_sha256,receipt,recorded_unix_nano) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", e.Sequence, f.LibraryID, p.Administration, namespace, p.OperatorSubjectID, p.Request.ID, p.Request.Category, collectionNullable(p.Request.CollectionID), []byte(e.Request), e.Digest, []byte(e.Receipt), p.RecordedUnixNano)
	}
	for _, e := range f.Events {
		if e.Family != "content" {
			continue
		}
		var p CollectionRecord
		projectionFixtureJSON(t, e.Record, &p)
		var child any
		if p.GeneratedPolicyOperationID != "" {
			n, ok := children[string(p.ActorSubjectID)+"/"+string(p.GeneratedPolicyOperationID)]
			if !ok {
				t.Fatal("literal fixture missing child")
			}
			child = n
		}
		exec("INSERT INTO collection_events(sequence,library_id,actor_subject_id,operation_id,collection_id,request,payload_sha256,receipt,result_version,generated_policy_sequence,recorded_unix_nano) VALUES(?,?,?,?,?,?,?,?,?,?,?)", e.Sequence, f.LibraryID, p.ActorSubjectID, p.Request.ID, p.Request.CollectionID, []byte(e.Request), e.Digest, []byte(e.Receipt), p.ResultVersion, child, p.RecordedUnixNano)
	}
	for _, row := range f.Projections {
		switch row.Kind {
		case 0x1a:
			var p collectionDefinitionProjection
			projectionFixtureJSON(t, row.JSON, &p)
			exec("INSERT INTO collection_definitions(collection_id,kind,name,archived,definition_version,created_sequence,updated_sequence) VALUES(?,?,?,?,?,?,?)", p.CollectionID, p.Kind, p.Name, p.Archived, p.DefinitionVersion, p.CreatedSequence, p.UpdatedSequence)
		case 0x1b:
			var p collectionEntryProjection
			projectionFixtureJSON(t, row.JSON, &p)
			exec("INSERT INTO collection_entries(entry_id,library_id,collection_id,asset_id,removed,created_sequence,updated_sequence) VALUES(?,?,?,?,?,?,?)", p.EntryID, p.LibraryID, p.CollectionID, p.AssetID, p.Removed, p.CreatedSequence, p.UpdatedSequence)
		case 0x1c:
			var p collectionGrantProjection
			projectionFixtureJSON(t, row.JSON, &p)
			exec("INSERT INTO collection_grants(grant_id,library_id,subject_id,scope_kind,collection_id,actions,grant_version,removed,created_sequence,updated_sequence) VALUES(?,?,?,?,?,?,?,?,?,?)", p.GrantID, p.LibraryID, p.Value.SubjectID, p.Value.Scope.Kind, collectionNullable(p.Value.Scope.CollectionID), actionsBytes(p.Value.Actions), p.Version, p.Removed, p.CreatedSequence, p.UpdatedSequence)
		case 0x1d:
			var p collectionResultProjection
			projectionFixtureJSON(t, row.JSON, &p)
			exec("INSERT INTO collection_policy_results(policy_sequence,grant_id,result_version,removed) VALUES(?,?,?,?)", p.PolicySequence, p.GrantID, p.ResultVersion, p.Removed)
		case 0x1e:
			var p collectionOrderProjection
			projectionFixtureJSON(t, row.JSON, &p)
			column := ""
			switch p.Kind {
			case "legacy_policy":
				column = "legacy_sequence"
			case "owner_policy":
				column = "owner_policy_sequence"
			case "content":
				column = "content_sequence"
			default:
				t.Fatal("literal order kind")
			}
			exec("INSERT INTO collection_order(ordinal,"+column+") VALUES(?,?)", p.Ordinal, p.Sequence)
		case 0x1f:
			var p collectionLibraryQuotaProjection
			projectionFixtureJSON(t, row.JSON, &p)
			fields := quotaFields(p.counters())
			args := make([]any, len(fields))
			for i, n := range fields {
				args[i] = n
			}
			exec("UPDATE collection_library_quota SET "+strings.ReplaceAll(collectionQuotaColumns, ",", "=?,")+"=? WHERE singleton=1", args...)
		case 0x20:
			var p collectionLocalQuotaProjection
			projectionFixtureJSON(t, row.JSON, &p)
			fields := quotaFields(p.counters())
			args := []any{p.CollectionID}
			for _, n := range fields {
				args = append(args, n)
			}
			exec("INSERT INTO collection_quota(collection_id,"+collectionQuotaColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", args...)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return s
}
func collectionProjectionReadSnapshot(t *testing.T, s *Store, fn func(*sql.Tx)) {
	t.Helper()
	db, err := openDB(s.path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	fn(tx)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func TestCollectionPortabilityIndependentProjections(t *testing.T) {
	for _, name := range []string{"empty", "shared"} {
		t.Run(name, func(t *testing.T) {
			f := readCollectionProjectionFixture(t, name)
			s := collectionProjectionFixtureStore(t, f)
			collectionProjectionReadSnapshot(t, s, func(tx *sql.Tx) {
				total, err := projectionTotal(context.Background(), tx)
				if err != nil || total != f.ProjectionRows {
					t.Fatalf("logical rows %d want%d: %v", total, f.ProjectionRows, err)
				}
				digest, rows, err := projectionDigest(context.Background(), tx, f.LibraryID, f.ProjectionRows)
				if err != nil || rows != f.ProjectionRows || digest != f.ProjectionSHA256 {
					t.Fatalf("independent projection %s/%d want %s/%d: %v", digest, rows, f.ProjectionSHA256, f.ProjectionRows, err)
				}
				if _, _, err := projectionDigest(context.Background(), tx, f.LibraryID, f.ProjectionRows-1); !errors.Is(err, ErrLimit) {
					t.Fatalf("one-row-short limit: %v", err)
				}
				var expected []struct {
					Kind           byte
					JSON, FrameHex string
				}
				for _, p := range f.Projections {
					if p.Kind >= 0x19 {
						expected = append(expected, struct {
							Kind           byte
							JSON, FrameHex string
						}{p.Kind, p.JSON, p.FrameHex})
					}
				}
				seen := 0
				err = walkCollectionProjections(context.Background(), tx, f.LibraryID, func(kind byte, value any) error {
					if seen >= len(expected) {
						t.Fatal("unexpected new projection row")
					}
					p := expected[seen]
					data, err := json.Marshal(value)
					if err != nil {
						return err
					}
					if kind != p.Kind || string(data) != p.JSON {
						t.Fatalf("row%d kind%x exact independent JSON mismatch", seen, kind)
					}
					header := frameHeader(kind, len(data))
					frame := append(append([]byte{}, header[:]...), data...)
					if !bytes.Equal(frame, collectionFixtureHex(t, p.FrameHex)) {
						t.Fatalf("row%d exact independent frame mismatch", seen)
					}
					if len(data) > 64<<10 {
						t.Fatal("oversized projection frame")
					}
					seen++
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if seen != len(expected) {
					t.Fatalf("new logical rows%d want%d", seen, len(expected))
				}
			})
		})
	}
}
func TestCollectionPortabilityOldProjectionFixturesUnchanged(t *testing.T) {
	for _, name := range []string{"continuity-c02c-empty", "continuity-c02c-legacy", "continuity-c02c-relational", "continuity-c03a-empty", "continuity-c03a-policy-rich"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + name + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var f collectionProjectionFixture
			if err := json.Unmarshal(data, &f); err != nil {
				t.Fatal(err)
			}
			wire := collectionFixtureHex(t, f.StreamHex)
			path := filepath.Join(t.TempDir(), "old.sqlite")
			if err := Restore(context.Background(), path, f.LibraryID, bytes.NewReader(wire), Limits{MaxStreamBytes, MaxStreamEvents, MaxStreamProjectionRows}); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			collectionProjectionReadSnapshot(t, s, func(tx *sql.Tx) {
				total, err := projectionTotal(context.Background(), tx)
				if err != nil || total != f.ProjectionRows {
					t.Fatalf("old logical count %d: %v", total, err)
				}
				digest, rows, err := projectionDigest(context.Background(), tx, f.LibraryID, f.ProjectionRows)
				if err != nil || rows != f.ProjectionRows || digest != f.ProjectionSHA256 {
					t.Fatalf("old independent projection changed %s/%d: %v", digest, rows, err)
				}
			})
		})
	}
}
func TestCollectionPortabilityProjectionBindsFrozenClaims(t *testing.T) {
	data, err := os.ReadFile("testdata/continuity-c04b-operations.json")
	if err != nil {
		t.Fatal(err)
	}
	var inputs struct {
		Negatives []struct {
			ID     string                      `json:"id"`
			Vector collectionProjectionFixture `json:"vector"`
		} `json:"negative_streams"`
	}
	if err := json.Unmarshal(data, &inputs); err != nil {
		t.Fatal(err)
	}
	original := readCollectionProjectionFixture(t, "shared")
	seen := 0
	for _, negative := range inputs.Negatives {
		if negative.ID != "forged_in_cap_policy_generation" {
			continue
		}
		seen++
		t.Run(negative.ID, func(t *testing.T) {
			s := collectionProjectionFixtureStore(t, negative.Vector)
			collectionProjectionReadSnapshot(t, s, func(tx *sql.Tx) {
				digest, rows, err := projectionDigest(context.Background(), tx, negative.Vector.LibraryID, negative.Vector.ProjectionRows)
				if err != nil || rows != negative.Vector.ProjectionRows || digest != negative.Vector.ProjectionSHA256 || digest == original.ProjectionSHA256 {
					t.Fatalf("exact independent forged projection claim was not bound: %s/%d %v", digest, rows, err)
				}
			})
		})
	}
	if seen != 1 {
		t.Fatalf("frozen generation projection claim cases%d", seen)
	}
	// A source projection digest legitimately hashes a structurally valid claim.
	// Only final comparison with independently replayed state can reject a forged
	// source claim. This test does not claim that dispatcher/finalization work.
}
func TestCollectionPortabilityProjectionCancellationAndEmitterFailure(t *testing.T) {
	f := readCollectionProjectionFixture(t, "shared")
	s := collectionProjectionFixtureStore(t, f)
	collectionProjectionReadSnapshot(t, s, func(tx *sql.Tx) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := projectionDigest(ctx, tx, f.LibraryID, f.ProjectionRows); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled projection: %v", err)
		}
		sentinel := errors.New("owned projection callback failure")
		if err := walkCollectionProjections(context.Background(), tx, f.LibraryID, func(kind byte, value any) error { return sentinel }); !errors.Is(err, sentinel) {
			t.Fatalf("callback error lost: %v", err)
		}
		var count int
		if err := tx.QueryRow("SELECT count(*) FROM collection_identities").Scan(&count); err != nil || count != 2 {
			t.Fatalf("projection rows not released: %v", err)
		}
	})
}

func TestCollectionPortabilityProjectionBindsReservedCounterChange(t *testing.T) {
	f := readCollectionProjectionFixture(t, "shared")
	s := collectionProjectionFixtureStore(t, f)
	collectionProjectionReadSnapshot(t, s, func(tx *sql.Tx) {
		digest, rows, err := projectionDigest(context.Background(), tx, f.LibraryID, f.ProjectionRows)
		if err != nil || rows != f.ProjectionRows || digest != f.ProjectionSHA256 {
			t.Fatalf("unchanged independent positive: %s/%d %v", digest, rows, err)
		}
	})
	// Library lifetime_grants=5 and reserve3. A library-only increase to4 remains
	// in-cap and internally well-formed, while lacking backing grant history.
	// No expected digest is regenerated: the unchanged positive literal is the
	// independent discriminator. Final replay equivalence remains separate work.
	if _, err := s.db.Exec("UPDATE collection_library_quota SET reserved_policy_events=reserved_policy_events+1,reserved_policy_receipts=reserved_policy_receipts+1,reserved_bytes=reserved_bytes+3072 WHERE singleton=1"); err != nil {
		t.Fatal(err)
	}
	collectionProjectionReadSnapshot(t, s, func(tx *sql.Tx) {
		digest, rows, err := projectionDigest(context.Background(), tx, f.LibraryID, f.ProjectionRows)
		if err != nil || rows != f.ProjectionRows || digest == f.ProjectionSHA256 {
			t.Fatalf("changed valid reserve claim not bound: %s/%d %v", digest, rows, err)
		}
	})
}
