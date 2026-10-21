package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type savedQueryPortableVector struct {
	collectionWireFixture
	Name           string `json:"name"`
	CombinedEvents int64  `json:"combined_event_count"`
	StreamBytes    int64  `json:"stream_bytes"`
	Checkpoints    []struct {
		collectionReplaySnapshot
		Generations []*int64 `json:"generations_A_B_X_Y_Z"`
	} `json:"expected_after_each_ordinal"`
}
type savedQueryPortableFixture struct {
	Positive []savedQueryPortableVector `json:"positive_streams"`
	Negative []struct {
		ID     string                   `json:"id"`
		Vector savedQueryPortableVector `json:"vector"`
	} `json:"negative_streams"`
	Projections []struct {
		Rows   int64  `json:"projection_rows"`
		Digest string `json:"projection_sha256"`
		Hex    string `json:"projection_bytes_hex"`
	} `json:"projections"`
}
type savedQueryPortableFrame struct {
	kind       byte
	payload    []byte
	start, end int
}

func savedQueryPortableFrames(t *testing.T, raw []byte, start int) []savedQueryPortableFrame {
	t.Helper()
	var frames []savedQueryPortableFrame
	for offset := start; offset < len(raw); {
		if len(raw)-offset < 5 {
			t.Fatal("short literal frame header")
		}
		n := uint64(binary.BigEndian.Uint32(raw[offset+1 : offset+5]))
		if n > uint64(len(raw)-offset-5) {
			t.Fatal("short literal frame")
		}
		end := offset + 5 + int(n)
		frames = append(frames, savedQueryPortableFrame{raw[offset], raw[offset+5 : end], offset, end})
		offset = end
	}
	return frames
}
func readSavedQueryPortableFixture(t *testing.T) savedQueryPortableFixture {
	t.Helper()
	data, err := os.ReadFile("testdata/continuity-c04c-portability.json")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != "b648061f0c8799c9ccf1be0eeb049784a40fd5f4a40209c02acfe1007a3571af" {
		t.Fatal("frozen fixture changed")
	}
	var f savedQueryPortableFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Positive) != 2 || len(f.Negative) != 4 || len(f.Projections) != 3 || len(f.Positive[1].Checkpoints) != 26 {
		t.Fatal("fixture coverage")
	}
	all := append([]savedQueryPortableVector{}, f.Positive...)
	for _, negative := range f.Negative {
		all = append(all, negative.Vector)
	}
	for _, v := range all {
		raw := collectionFixtureHex(t, v.StreamHex)
		if int64(len(raw)) != v.StreamBytes || fmt.Sprintf("%x", sha256.Sum256(raw)) != v.FileSHA256 || !bytes.HasPrefix(raw, []byte("juicemount.catalog.export\x00\x04")) {
			t.Fatalf("literal stream %s", v.Name)
		}
	}
	for _, p := range f.Projections {
		raw := collectionFixtureHex(t, p.Hex)
		h := sha256.New()
		h.Write([]byte("juicemount.catalog.projection\x00\x04"))
		h.Write(raw)
		if hex.EncodeToString(h.Sum(nil)) != p.Digest || int64(len(savedQueryPortableFrames(t, raw, 0))) != p.Rows {
			t.Fatal("literal projection digest/rows")
		}
	}
	return f
}
func newSavedQueryReplayStore(t *testing.T, library continuity.LibraryID) *Store {
	t.Helper()
	s, err := openStoreVersionContext(context.Background(), filepath.Join(t.TempDir(), "replay.sqlite"), library, true, false, 6)
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
func replaySavedQueryFixtureFrame(t *testing.T, s *Store, f savedQueryPortableFrame, rows, max int64) (int64, error) {
	t.Helper()
	if f.kind == 7 {
		replay, _, err := decodePortableCollectionContent(f.payload, s.library, 4)
		if err != nil {
			return 0, err
		}
		return replay(context.Background(), s, rows, max)
	}
	return replayCollectionFixtureFrame(t, s, collectionReplayFrame{Kind: f.kind, PayloadHex: hex.EncodeToString(f.payload)}, rows, max)
}
func savedQueryReplayPrefix(t *testing.T, v savedQueryPortableVector, through int64) (*Store, int64, savedQueryPortableFrame) {
	t.Helper()
	s := newSavedQueryReplayStore(t, v.LibraryID)
	rows, ordinal := int64(2), int64(0)
	for _, f := range savedQueryPortableFrames(t, collectionFixtureHex(t, v.StreamHex), len(streamMagic4)) {
		if f.kind == 1 || f.kind == 3 {
			continue
		}
		if f.kind == 4 || f.kind == 6 || f.kind == 7 {
			ordinal++
			if ordinal > through {
				return s, rows, f
			}
		}
		var err error
		rows, err = replaySavedQueryFixtureFrame(t, s, f, rows, MaxStreamProjectionRows)
		if err != nil {
			t.Fatalf("ordinal %d prefix: %v", ordinal, err)
		}
	}
	return s, rows, savedQueryPortableFrame{}
}

func TestSavedQueryPortabilityPublicFrozenRoundtrip(t *testing.T) {
	ctx := context.Background()
	fixture := readSavedQueryPortableFixture(t)
	for index, v := range fixture.Positive {
		t.Run(v.Name, func(t *testing.T) {
			wire := collectionFixtureHex(t, v.StreamHex)
			limits := Limits{int64(len(wire)), v.CombinedEvents, v.ProjectionRows}
			// Limits require a positive event bound even for the empty archive.
			if limits.MaxEvents == 0 {
				limits.MaxEvents = 1
			}
			path := filepath.Join(t.TempDir(), "restored.sqlite")
			if err := Restore(ctx, path, v.LibraryID, bytes.NewReader(wire), limits); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if s.version != 6 {
				t.Fatal("format4 restored wrong schema")
			}
			if err := validateInactiveCollectionImport(ctx, s.db); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := s.Export(ctx, v.LibraryID, &out, limits); err != nil || !bytes.Equal(out.Bytes(), wire) {
				t.Fatalf("public exact format4 export: %v", err)
			}
			digest, rows, err := projectionDigest(ctx, s.db, v.LibraryID, v.ProjectionRows)
			if err != nil || digest != v.ProjectionSHA256 || rows != v.ProjectionRows {
				t.Fatalf("fixed full projection: %s/%d %v", digest, rows, err)
			}
			var observed []savedQueryPortableFrame
			emit := func(kind byte, value any) error {
				data, err := json.Marshal(value)
				if err == nil {
					observed = append(observed, savedQueryPortableFrame{kind: kind, payload: data})
				}
				return err
			}
			if err := walkCollectionProjections(ctx, s.db, v.LibraryID, emit); err != nil {
				t.Fatal(err)
			}
			if err := walkSavedQueryProjections(ctx, s.db, v.LibraryID, emit); err != nil {
				t.Fatal(err)
			}
			var expected []savedQueryPortableFrame
			for _, frame := range savedQueryPortableFrames(t, collectionFixtureHex(t, fixture.Projections[index].Hex), 0) {
				if frame.kind >= 0x19 {
					expected = append(expected, frame)
				}
			}
			if len(observed) != len(expected) {
				t.Fatal("collection/query frame coverage")
			}
			for i := range expected {
				if observed[i].kind != expected[i].kind || !bytes.Equal(observed[i].payload, expected[i].payload) {
					t.Fatalf("fixed logical frame %d", i)
				}
			}
			// A second public restore proves the exported bytes stay usable.
			if err := Restore(ctx, filepath.Join(t.TempDir(), "again.sqlite"), v.LibraryID, bytes.NewReader(out.Bytes()), limits); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSavedQueryPortabilityFrozenOrdinalAccounting(t *testing.T) {
	v := readSavedQueryPortableFixture(t).Positive[1]
	s := newSavedQueryReplayStore(t, v.LibraryID)
	rows, ordinal := int64(2), int64(0)
	for _, f := range savedQueryPortableFrames(t, collectionFixtureHex(t, v.StreamHex), len(streamMagic4)) {
		if f.kind == 1 || f.kind == 3 {
			continue
		}
		var err error
		rows, err = replaySavedQueryFixtureFrame(t, s, f, rows, MaxStreamProjectionRows)
		if err != nil {
			t.Fatalf("replay before ordinal %d: %v", ordinal+1, err)
		}
		if f.kind != 4 && f.kind != 6 && f.kind != 7 {
			continue
		}
		want := v.Checkpoints[ordinal]
		ordinal++
		actual, err := projectionTotal(context.Background(), s.db)
		if err != nil || rows != want.Rows || actual != want.Rows || want.Ordinal != ordinal {
			t.Fatalf("ordinal %d: rows %d/%d want%d: %v", ordinal, rows, actual, want.Rows, err)
		}
		usage, err := readCollectionQuota(context.Background(), s.db, "")
		if err != nil {
			t.Fatal(err)
		}
		assertCollectionReplayQuota(t, usage.Library, want.Library)
		var locals int
		if err := s.db.QueryRow(`SELECT count(*) FROM collection_quota`).Scan(&locals); err != nil || locals != len(want.Locals) {
			t.Fatalf("local quota coverage: %d %v", locals, err)
		}
		for id, expected := range want.Locals {
			usage, err := readCollectionQuota(context.Background(), s.db, id)
			if err != nil || usage.Collection == nil {
				t.Fatalf("local quota: %v", err)
			}
			assertCollectionReplayQuota(t, *usage.Collection, expected)
		}
		if len(want.Generations) != 5 {
			t.Fatal("generation dimensions")
		}
		for i, expected := range want.Generations {
			var generation int64
			var err error
			switch {
			case i < 2:
				err = s.db.QueryRow(`SELECT authorization_generation FROM policy_subjects WHERE subject_id=?`, fmt.Sprintf("sub_%032x", i+1)).Scan(&generation)
			case i < 4:
				err = s.db.QueryRow(`SELECT policy_generation FROM collection_identities i JOIN collection_definitions d USING(collection_id) WHERE i.collection_id=?`, fmt.Sprintf("col_%032x", i-1)).Scan(&generation)
			default:
				err = s.db.QueryRow(`SELECT policy_generation FROM collection_identities WHERE collection_id='col_00000000000000000000000000000003'`).Scan(&generation)
			}
			if expected == nil {
				if !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("ordinal%d generation%d should be absent: %v", ordinal, i, err)
				}
			} else if err != nil || generation != *expected {
				t.Fatalf("ordinal%d generation%d: %d want%d %v", ordinal, i, generation, *expected, err)
			}
		}
	}
	if ordinal != 26 {
		t.Fatal("mixed chronology coverage")
	}
	if err := validateCollectionPortableState(context.Background(), s.db, s.library); err != nil {
		t.Fatal(err)
	}
}

func TestSavedQueryPortabilityLiveMutationExcludesPrivateRecency(t *testing.T) {
	s, o, authority, d, owner, _ := savedQueryTestStore(t)
	ctx := context.Background()
	created := savedQueryTestCreate(t, 501, testAsset)
	first := savedQueryTestCall(t, d, owner, created)
	savedQueryTestCall(t, d, owner, savedQueryTestEdit(t, 502, created.CollectionID, first.ResultVersion, "replace_definition", ReplaceSavedQueryDefinition{SavedQueryDefinition{1, "ranked_assets", "", []continuity.AssetID{otherAsset}}}))
	// Grant and credential issuance precede the portable baseline; only the
	// explicit private preference changes between the two public exports.
	discoveryGrantTest(t, o, authority, "portable-activity", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	issued, _ := collectionTxIssue(t, o, authority, "portable-activity", ownerSubject, discoveryAllActions)
	bearer := issued.Bearer
	var before, after bytes.Buffer
	if err := s.Export(ctx, testLibrary, &before, collectionWireLimits()); err != nil {
		t.Fatal(err)
	}
	recordPreferenceTest(t, d, bearer, preferenceRequestTest("portable-remember", "select", testAsset, nil, 1800000000))
	var entries int
	if err := s.db.QueryRow(`SELECT count(*) FROM preferences`).Scan(&entries); err != nil || entries != 1 {
		t.Fatalf("private recency setup: %d %v", entries, err)
	}
	if err := s.Export(ctx, testLibrary, &after, collectionWireLimits()); err != nil || !bytes.Equal(before.Bytes(), after.Bytes()) {
		t.Fatalf("private recency changed portable bytes: %v", err)
	}
	path := filepath.Join(t.TempDir(), "inactive.sqlite")
	if err := Restore(ctx, path, testLibrary, bytes.NewReader(after.Bytes()), collectionWireLimits()); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := validateInactiveCollectionImport(ctx, restored.db); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Authenticate(ctx, testLibrary, owner); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("old source credential survived import: %v", err)
	}
}
