package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func collectionCoverageMetadata(t *testing.T) (eventRow, Operation) {
	t.Helper()
	op := initialOperation()
	request, digest, err := CanonicalRequest(op)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := json.Marshal(Receipt{1, testLibrary, op.ID, 1, digest, 1})
	if err != nil {
		t.Fatal(err)
	}
	e, decoded, err := decodeStreamEvent(encodeEvent(eventRow{request: request, receipt: receipt}), testLibrary, 1)
	if err != nil {
		t.Fatal(err)
	}
	return e, decoded
}

func TestCollectionPortabilityMetadataReplayActualSchemaAndLibraryFence(t *testing.T) {
	ctx := context.Background()
	e, op := collectionCoverageMetadata(t)
	t.Run("actual_upgrade_retained_handle", func(t *testing.T) {
		s, _, _, d, _, _ := preferencesTestStore(t)
		if err := d.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err := UpgradeCollections(ctx, s.path, testLibrary); err != nil {
			t.Fatal(err)
		}
		before := collectionReplayState(t, s)
		if _, err := s.replayEvent(ctx, e, op, 2, MaxStreamProjectionRows); !errors.Is(err, ErrUpgradeRequired) {
			t.Fatalf("stale metadata dispatched after actual schema4-to5 upgrade: %v", err)
		}
		if !bytes.Equal(before, collectionReplayState(t, s)) {
			t.Fatal("stale metadata altered upgraded state")
		}
	})
	for _, field := range []string{"stored_event", "decoded_operation"} {
		t.Run(field, func(t *testing.T) {
			s := newCollectionReplayStore(t, testLibrary)
			before := collectionReplayState(t, s)
			badEvent, badOperation := e, op
			if field == "stored_event" {
				badEvent.library = otherLibrary
			} else {
				badOperation.LibraryID = otherLibrary
			}
			if _, err := s.replayEvent(ctx, badEvent, badOperation, 2, MaxStreamProjectionRows); !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "metadata replay library") {
				t.Fatalf("schema5 metadata library fence: %v", err)
			}
			if !bytes.Equal(before, collectionReplayState(t, s)) {
				t.Fatal("foreign-library metadata changed target")
			}
			rows, err := s.replayEvent(ctx, e, op, 2, MaxStreamProjectionRows)
			if err != nil || rows != 4 {
				t.Fatalf("unchanged exact-library schema5 metadata: rows=%d error=%v", rows, err)
			}
		})
	}
}

func collectionCoverageReady(t *testing.T, s *Store) {
	t.Helper()
	if err := changedOne(s.db.Exec(`UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0`)); err != nil {
		t.Fatal(err)
	}
}
func collectionCoverageExportRefuses(t *testing.T, s *Store, want string) {
	t.Helper()
	var out bytes.Buffer
	err := s.Export(context.Background(), s.library, &out, collectionWireLimits())
	if !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), want) || out.Len() != 0 {
		t.Fatalf("source refusal = %v, wrote %d, want %q before magic", err, out.Len(), want)
	}
	if s.db.Stats().InUse != 0 {
		t.Fatal("source validation failure retained Store pool resource")
	}
}

func TestCollectionPortabilitySourceReverseCreateAndChildOwnership(t *testing.T) {
	t.Run("definition_created_sequence_is_not_create", func(t *testing.T) {
		f := readCollectionProjectionFixture(t, "shared")
		changed := 0
		for i := range f.Projections {
			row := &f.Projections[i]
			if row.Kind != 0x1a {
				continue
			}
			var p collectionDefinitionProjection
			projectionFixtureJSON(t, row.JSON, &p)
			if p.CollectionID != "col_00000000000000000000000000000001" {
				continue
			}
			// Sequence10 exists in X and is <= updated_sequence, but is an entry
			// removal. FK existence and the forward create alone cannot prove this link.
			p.CreatedSequence = 10
			b, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			row.JSON = string(b)
			changed++
		}
		if changed != 1 {
			t.Fatal("did not isolate definition reverse-create link")
		}
		s := collectionProjectionFixtureStore(t, f)
		collectionCoverageReady(t, s)
		collectionCoverageExportRefuses(t, s, "collection declaration create binding")
	})
	t.Run("generated_children_swapped_between_actor_namespaces", func(t *testing.T) {
		f := readCollectionProjectionFixture(t, "shared")
		changed := 0
		for i := range f.Events {
			e := &f.Events[i]
			if e.Family != "generated_policy" || (e.Sequence != 2 && e.Sequence != 3) {
				continue
			}
			var p CollectionPolicyRecord
			projectionFixtureJSON(t, e.Record, &p)
			if e.Sequence == 2 {
				p.OperatorSubjectID = "sub_00000000000000000000000000000002"
				e.Sequence = 3
			} else {
				p.OperatorSubjectID = "sub_00000000000000000000000000000001"
				e.Sequence = 2
			}
			// The seeder resolves actor-qualified identical generated op IDs. Swap
			// namespaces AND policy sequence placement so contiguous ledger order
			// remains valid, but each parent points at the other create's request.
			// Requests/receipts/results and parent actors remain present; the product
			// must reject the cross-record ownership rather than an order gap.
			b, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			e.Record = string(b)
			changed++
		}
		if changed != 2 {
			t.Fatal("did not swap exactly the independent create children")
		}
		s := collectionProjectionFixtureStore(t, f)
		collectionCoverageReady(t, s)
		collectionCoverageExportRefuses(t, s, "collection child binding")
	})
}

func TestCollectionPortabilitySourceLeftoverDeclarationAndDanglingMembership(t *testing.T) {
	t.Run("unconsumed_identity", func(t *testing.T) {
		f := readCollectionProjectionFixture(t, "shared")
		s := collectionProjectionFixtureStore(t, f)
		if _, err := s.db.Exec(`INSERT INTO collection_identities(collection_id,library_id,creator_subject_id,creation_operation_id,creation_payload_sha256,policy_generation) VALUES(?,?,?,?,?,0)`, "col_00000000000000000000000000000003", f.LibraryID, "sub_00000000000000000000000000000001", "cop_ffffffffffffffffffffffffffffffff", strings.Repeat("e", 64)); err != nil {
			t.Fatal(err)
		}
		collectionCoverageReady(t, s)
		collectionCoverageExportRefuses(t, s, "collection local quota")
	})
	t.Run("missing_asset", func(t *testing.T) {
		f := readCollectionProjectionFixture(t, "shared")
		s := collectionProjectionFixtureStore(t, f)
		// Corrupt data with foreign-key enforcement temporarily disabled on the
		// source connection only. Restore enforcement before the product call; no
		// schema/triggers are dropped, and Export's separate read owner checks FKs.
		if _, err := s.db.Exec(`PRAGMA foreign_keys=OFF`); err != nil {
			t.Fatal(err)
		}
		_, insertErr := s.db.Exec(`INSERT INTO collection_entries(entry_id,library_id,collection_id,asset_id,removed,created_sequence,updated_sequence) VALUES(?,?,?,?,0,1,1)`, "cent_ffffffffffffffffffffffffffffffff", f.LibraryID, "col_00000000000000000000000000000001", "asset_ffffffffffffffffffffffffffffffff")
		_, enableErr := s.db.Exec(`PRAGMA foreign_keys=ON`)
		if err := errors.Join(insertErr, enableErr); err != nil {
			t.Fatal(err)
		}
		var enabled int
		if err := s.db.QueryRow(`PRAGMA foreign_keys`).Scan(&enabled); err != nil || enabled != 1 {
			t.Fatalf("source FK enforcement not restored: %d %v", enabled, err)
		}
		collectionCoverageReady(t, s)
		collectionCoverageExportRefuses(t, s, "foreign key violation")
	})
}

// Independently spell the frozen outer-hash formula. This is only a negative
// stream constructor; it does not derive a successful expected projection from
// the candidate. The unchanged stream must first match the independent literal.
func collectionCoverageOuterDigest(t *testing.T, frames [][]byte, footer collectionStreamFooter) string {
	t.Helper()
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.export.stream\x00\x03"))
	h.Write([]byte("juicemount.catalog.export\x00\x03"))
	for _, frame := range frames {
		h.Write(frame)
	}
	for _, n := range []int64{footer.EventCount, footer.PolicyEventCount, footer.CollectionCount, footer.CollectionEventCount, footer.CollectionPolicyEventCount, footer.TransactionCount, footer.ProjectionRows} {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		h.Write(b[:])
	}
	raw, err := hex.DecodeString(footer.ProjectionSHA256)
	if err != nil || len(raw) != 32 {
		t.Fatal("invalid independent projection digest", err)
	}
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))
}
func TestCollectionPortabilityPublicLeftoverDeclarationBeforeReady(t *testing.T) {
	f := readCollectionWireFixture(t, "shared")
	var frames [][]byte
	for _, frame := range f.Frames {
		if frame.Kind != 3 {
			frames = append(frames, collectionFixtureHex(t, frame.FrameHex))
		}
	}
	var header collectionStreamHeader
	var footer collectionStreamFooter
	projectionFixtureJSON(t, f.HeaderJSON, &header)
	projectionFixtureJSON(t, f.FooterJSON, &footer)
	if got := collectionCoverageOuterDigest(t, frames, footer); got != f.StreamSHA256 {
		t.Fatalf("negative harness disagrees with frozen stream digest: %s", got)
	}
	header.CollectionCount++
	footer.CollectionCount++
	footer.ProjectionRows++
	raw, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	frames[0] = testFrame(1, raw)
	declaration := collectionDeclaration{1, f.LibraryID, "col_00000000000000000000000000000003", "sub_00000000000000000000000000000001", "cop_ffffffffffffffffffffffffffffffff", strings.Repeat("e", 64)}
	raw, err = json.Marshal(declaration)
	if err != nil {
		t.Fatal(err)
	}
	// Declarations are before mixed chronology and sorted. This extra one is
	// syntactically valid, has a distinct creator/op identity, and is never used.
	insert := 1
	for insert < len(frames) && (frames[insert][0] == 2 || frames[insert][0] == 5) {
		insert++
	}
	frames = append(frames, nil)
	copy(frames[insert+1:], frames[insert:])
	frames[insert] = testFrame(5, raw)
	footer.StreamSHA256 = collectionCoverageOuterDigest(t, frames, footer)
	raw, err = json.Marshal(footer)
	if err != nil {
		t.Fatal(err)
	}
	wire := []byte(streamMagic3)
	for _, frame := range frames {
		wire = append(wire, frame...)
	}
	wire = append(wire, testFrame(3, raw)...)
	path := filepath.Join(t.TempDir(), "unused.sqlite")
	err = Restore(context.Background(), path, f.LibraryID, bytes.NewReader(wire), collectionWireLimits())
	if !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "collection local quota") {
		t.Fatalf("unused declaration missed complete-state validation before ready: %v", err)
	}
	assertIncomplete(t, path)
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ready, identities, definitions, events, policies, ordinals int
	if err := db.QueryRow(`SELECT (SELECT ready FROM catalog),(SELECT count(*) FROM collection_identities),(SELECT count(*) FROM collection_definitions),(SELECT count(*) FROM collection_events),(SELECT count(*) FROM collection_policy_events),(SELECT count(*) FROM collection_order)`).Scan(&ready, &identities, &definitions, &events, &policies, &ordinals); err != nil {
		t.Fatal(err)
	}
	if ready != 0 || identities != 3 || definitions != 2 || events != 10 || policies != 9 || ordinals != 19 {
		t.Fatalf("final failure lost prior replay or became ready: %d/%d/%d/%d/%d/%d", ready, identities, definitions, events, policies, ordinals)
	}
}

// Counter inflation below is an admission boundary setup, not a valid source
// projection or final-equivalence oracle. Replay must use the actual attached
// grant credit despite every charged resource being full; it may not initialize
// import accounting from source claims. Expected costs 784/889 are frozen P8/P9.
func collectionCoverageChargeCaps(t *testing.T, s *Store, id CollectionID, profile string) {
	t.Helper()
	for _, spec := range []struct {
		table, where  string
		events, bytes int64
		local         bool
		args          []any
	}{
		{"collection_library_quota", "singleton=1", 131072, 256 << 20, false, nil},
		{"collection_quota", "collection_id=?", 8192, 16 << 20, true, []any{id}},
	} {
		if spec.local && id == "" {
			continue
		}
		assignments := []string{}
		args := []any{}
		if profile == "events_receipts" || profile == "both" || profile == "all_caps" {
			assignments = append(assignments, "policy_events=?-reserved_policy_events", "policy_receipts=?-reserved_policy_receipts")
			args = append(args, spec.events, spec.events)
		}
		if profile == "canonical_bytes" || profile == "both" || profile == "all_caps" {
			assignments = append(assignments, "canonical_bytes=?-reserved_bytes")
			args = append(args, spec.bytes)
		}
		if profile == "all_caps" {
			if spec.local {
				assignments = append(assignments, "active_entries=4096", "lifetime_entries=8192", "lifetime_grants=128", "content_events=8192", "content_receipts=8192")
			} else {
				assignments = append(assignments, "collections=1024", "active_entries=65536", "lifetime_entries=131072", "lifetime_grants=16384", "content_events=131072", "content_receipts=131072")
			}
		}
		args = append(args, spec.args...)
		if _, err := s.db.Exec("UPDATE "+spec.table+" SET "+strings.Join(assignments, ",")+" WHERE "+spec.where, args...); err != nil {
			t.Fatal(err)
		}
	}
}
func collectionCoverageQuotaDelta(t *testing.T, a, b CollectionQuotaCounters, cost int64) {
	t.Helper()
	before, after := quotaFields(a), quotaFields(b)
	want := [12]int64{0, 0, 0, 0, 0, 0, 1, 1, cost, -1, -1, -3072}
	for i := range before {
		if after[i]-before[i] != want[i] {
			t.Fatalf("quota field%d delta=%d, independent removal delta=%d", i, after[i]-before[i], want[i])
		}
	}
}
func TestCollectionPortabilityReplaySecurityRemovalChargedCapProfiles(t *testing.T) {
	f := readCollectionReplayFixture(t)
	for _, scope := range []struct {
		name                  string
		prefix, ordinal, cost int64
		id                    CollectionID
	}{
		{"library_create", 16, 17, 784, ""},
		{"collection_last_admin", 17, 18, 889, "col_00000000000000000000000000000002"},
	} {
		for _, profile := range []string{"events_receipts", "canonical_bytes", "both", "all_caps"} {
			t.Run(scope.name+"/"+profile, func(t *testing.T) {
				s, rows := replayCollectionFixturePrefix(t, f, scope.prefix)
				collectionCoverageChargeCaps(t, s, scope.id, profile)
				before, err := readCollectionQuota(context.Background(), s.db, scope.id)
				if err != nil {
					t.Fatal(err)
				}
				var otherBefore CollectionQuotaCounters
				usage, err := readCollectionQuota(context.Background(), s.db, "col_00000000000000000000000000000001")
				if err != nil {
					t.Fatal(err)
				}
				otherBefore = *usage.Collection
				frame := collectionFixtureOrdinal(t, f, scope.ordinal)
				next, err := replayCollectionFixtureFrame(t, s, frame, rows, MaxStreamProjectionRows)
				if err != nil || next != rows+2 {
					t.Fatalf("historical attached-credit removal at %s: rows=%d error=%v", profile, next, err)
				}
				after, err := readCollectionQuota(context.Background(), s.db, scope.id)
				if err != nil {
					t.Fatal(err)
				}
				collectionCoverageQuotaDelta(t, before.Library, after.Library, scope.cost)
				if scope.id != "" {
					collectionCoverageQuotaDelta(t, *before.Collection, *after.Collection, scope.cost)
				}
				usage, err = readCollectionQuota(context.Background(), s.db, "col_00000000000000000000000000000001")
				if err != nil {
					t.Fatal(err)
				}
				if quotaFields(*usage.Collection) != quotaFields(otherBefore) {
					t.Fatal("security removal charged unrelated collection")
				}
				state := collectionReplayState(t, s)
				if _, err := replayCollectionFixtureFrame(t, s, frame, next, MaxStreamProjectionRows); err == nil {
					t.Fatal("replayed the same spent credit twice")
				}
				if !bytes.Equal(state, collectionReplayState(t, s)) {
					t.Fatal("spent-credit refusal changed historical state")
				}
			})
		}
	}
}

func TestCollectionPortabilityImportedContentRetryAtChargedCapacity(t *testing.T) {
	f := readCollectionWireFixture(t, "shared")
	wire := collectionFixtureHex(t, f.StreamHex)
	history := readCollectionReplayFixture(t)
	var retry CollectionRequest
	var receipt []byte
	for _, e := range history.Events {
		if e.Family == "content" && e.Sequence == 4 {
			var err error
			retry, err = DecodeCollectionRequest([]byte(e.Request))
			if err != nil {
				t.Fatal(err)
			}
			receipt = []byte(e.Receipt)
		}
	}
	if retry.Kind != "archive" || len(receipt) == 0 {
		t.Fatal("missing exact historical C4 retry")
	}
	for _, profile := range []string{"content_events_receipts", "canonical_bytes", "both"} {
		t.Run(profile, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "retry.sqlite")
			if err := Restore(ctx, path, f.LibraryID, bytes.NewReader(wire), collectionWireLimits()); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			owner, err := s.LocalOwner(f.LibraryID)
			if err != nil {
				t.Fatal(err)
			}
			bind := adminTestRequest("cap-bind", "", "bootstrap", BootstrapRequest{"sub_00000000000000000000000000000001", nil})
			bind.LibraryID = f.LibraryID
			boot, err := owner.Bootstrap(ctx, bind)
			if err != nil {
				t.Fatal(err)
			}
			issue := AdminRequestV2{2, f.LibraryID, "cap-fresh", boot.Result.Incarnation, "issue_credential", collectionTxRaw(t, IssueCredentialRequestV2{"sub_00000000000000000000000000000001", Scope{"library", ""}, collectionTxActions, s.now().Unix() + 3600})}
			credential, err := owner.IssueCredentialV2(ctx, issue)
			if err != nil {
				t.Fatal(err)
			}
			d, err := NewDiscoveryService(ctx, s, f.LibraryID)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			for _, spec := range []struct {
				table, where  string
				events, bytes int64
				args          []any
			}{{"collection_library_quota", "singleton=1", 131072, 256 << 20, nil}, {"collection_quota", "collection_id=?", 8192, 16 << 20, []any{retry.CollectionID}}} {
				assignments := []string{}
				args := []any{}
				if profile == "content_events_receipts" || profile == "both" {
					assignments = append(assignments, "content_events=?", "content_receipts=?")
					args = append(args, spec.events, spec.events)
				}
				if profile == "canonical_bytes" || profile == "both" {
					assignments = append(assignments, "canonical_bytes=?-reserved_bytes")
					args = append(args, spec.bytes)
				}
				args = append(args, spec.args...)
				if _, err := s.db.Exec("UPDATE "+spec.table+" SET "+strings.Join(assignments, ",")+" WHERE "+spec.where, args...); err != nil {
					t.Fatal(err)
				}
			}
			before := collectionReplayState(t, s)
			var out bytes.Buffer
			result, err := d.MutateCollection(ctx, credential.Bearer, retry, &out)
			if err != nil || !result.Started || !bytes.Equal(out.Bytes(), receipt) {
				t.Fatalf("exact imported retry allocated at %s: result=%+v error=%v bytes=%q", profile, result, err, out.Bytes())
			}
			if !bytes.Equal(before, collectionReplayState(t, s)) {
				t.Fatal("no-allocation historical retry changed portable state")
			}
			definition, err := readCollectionDefinition(ctx, s.db, retry.CollectionID)
			if err != nil {
				t.Fatal(err)
			}
			fresh := CollectionRequest{1, f.LibraryID, "cop_fffffffffffffffffffffffffffffffe", retry.CollectionID, definition.version, "rename", collectionTxRaw(t, RenameCollection{"would allocate at capacity"})}
			out.Reset()
			result, err = d.MutateCollection(ctx, credential.Bearer, fresh, &out)
			if !errors.Is(err, ErrCollectionCapacity) || result.Started || out.Len() != 0 {
				t.Fatalf("fresh allocation escaped %s: result=%+v error=%v bytes=%d", profile, result, err, out.Len())
			}
			if !bytes.Equal(before, collectionReplayState(t, s)) {
				t.Fatal("over-cap fresh operation retained partial state")
			}
		})
	}
}
