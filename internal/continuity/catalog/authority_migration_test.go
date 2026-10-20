package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type authorityMigrationGolden struct {
	Fingerprints   map[string]string            `json:"schema_fingerprints"`
	Objects        []struct{ Name, SQL string } `json:"schema_objects"`
	ProjectionRows int64                        `json:"projection_rows"`
	ProjectionSHA  string                       `json:"projection_sha256"`
	WireSHA        string                       `json:"format1_stream_sha256"`
	Events         []struct {
		Request string `json:"request_json"`
		Receipt string `json:"receipt_json"`
	} `json:"events"`
	Projections []struct {
		Frame string `json:"frame_hex"`
	} `json:"projections"`
}

func authorityFrozenV2(t *testing.T, incomplete bool) (string, authorityMigrationGolden) {
	t.Helper()
	sqlBytes, err := os.ReadFile("testdata/continuity-c03a-v2-fixture.sql")
	if err != nil {
		t.Fatal(err)
	}
	goldenBytes, err := os.ReadFile("testdata/continuity-c03a-v2-fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		data []byte
		want string
	}{
		{sqlBytes, "4e1c925a989237bd676a966a80bdfc238929375c6900866526702cb98b827681"},
		{goldenBytes, "45bc895ba479531ad647fb972de54bebe0aecac97ae55a6069013dc49c1e12f4"},
	} {
		sum := sha256.Sum256(fixture.data)
		if hex.EncodeToString(sum[:]) != fixture.want {
			t.Fatal("frozen v2 fixture bytes changed")
		}
	}
	var golden authorityMigrationGolden
	if err := json.Unmarshal(goldenBytes, &golden); err != nil {
		t.Fatal(err)
	}
	if incomplete {
		ready := []byte("UPDATE catalog SET ready=1 WHERE singleton=1;")
		if bytes.Count(sqlBytes, ready) != 1 {
			t.Fatal("fixture ready transition missing")
		}
		sqlBytes = bytes.Replace(sqlBytes, ready, nil, 1)
	}
	path := filepath.Join(t.TempDir(), "frozen-v2.sqlite")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := db.Exec(string(sqlBytes))
	if err := errors.Join(executeErr, db.Close()); err != nil {
		t.Fatal(err)
	}
	return path, golden
}

// Includes every old column and BLOB byte, with both old ledger rows. New v3
// tables and its additional ledger row are deliberately outside this snapshot.
func authorityOldRows(t *testing.T, q queryer) []byte {
	t.Helper()
	var all []any
	for _, query := range []string{
		"SELECT * FROM schema_migrations WHERE version<=2 ORDER BY version",
		"SELECT * FROM catalog ORDER BY singleton",
		"SELECT * FROM assets ORDER BY asset_id",
		"SELECT * FROM revisions ORDER BY revision_id",
		"SELECT * FROM revision_parents ORDER BY revision_id,parent_id",
		"SELECT * FROM relationships ORDER BY relationship_id",
		"SELECT * FROM locations ORDER BY location_id",
		"SELECT * FROM events ORDER BY sequence",
	} {
		rows, err := q.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		var records [][]any
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for i, value := range values {
				if blob, ok := value.([]byte); ok {
					values[i] = append([]byte(nil), blob...)
				}
			}
			records = append(records, values)
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, records)
	}
	data, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func authorityRequireOperationalEmpty(t *testing.T, s *Store) {
	t.Helper()
	for _, table := range []string{"catalog_authority", "catalog_credentials", "catalog_admin_events", "credential_issuance_receipts"} {
		var count int
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("operational table %s populated or unreadable: %d %v", table, count, err)
		}
	}
	owner, err := s.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	status, err := owner.Status(context.Background())
	if err != nil || status != (AuthorityStatus{}) {
		t.Fatalf("authority activated implicitly: %+v %v", status, err)
	}
}

func TestAuthorityFrozenV2MigrationPreservesExactHistory(t *testing.T) {
	path, golden := authorityFrozenV2(t, false)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	initialStore := s
	t.Cleanup(func() {
		if err := initialStore.Close(); err != nil {
			t.Error(err)
		}
	})
	before := authorityOldRows(t, s.db)
	if _, err := s.LocalOwner(testLibrary); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("v2 authority opt-in: %v", err)
	}
	p, rows, err := projectionDigest(context.Background(), s.db, testLibrary, 100)
	if err != nil || p != golden.ProjectionSHA || rows != golden.ProjectionRows {
		t.Fatalf("frozen v2 projection: %s %d %v", p, rows, err)
	}
	var oldExport bytes.Buffer
	if err := s.Export(context.Background(), testLibrary, &oldExport, portabilityLimits()); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(oldExport.Bytes())
	if hex.EncodeToString(sum[:]) != golden.WireSHA {
		t.Fatal("schema2 export no longer matches historical format1")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeAuthority(context.Background(), path, testLibrary); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !bytes.Equal(before, authorityOldRows(t, s.db)) {
		t.Fatal("authority upgrade changed historical ledger/projections/event bytes")
	}
	for _, object := range golden.Objects {
		var ddl string
		if err := s.db.QueryRow("SELECT sql FROM sqlite_schema WHERE name=?", object.Name).Scan(&ddl); err != nil || ddl != object.SQL {
			t.Fatalf("historical schema object changed: %s: %v", object.Name, err)
		}
	}
	for version, fingerprint := range golden.Fingerprints {
		var actual string
		if err := s.db.QueryRow("SELECT fingerprint FROM schema_migrations WHERE version=?", version).Scan(&actual); err != nil || actual != fingerprint {
			t.Fatalf("historical ledger %s changed: %s %v", version, actual, err)
		}
	}
	var version, ledgerCount int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 3 {
		t.Fatalf("upgraded version: %d %v", version, err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&ledgerCount); err != nil || ledgerCount != 3 {
		t.Fatalf("ledger count: %d %v", ledgerCount, err)
	}
	// Schema3 changes the projection domain, not any historical projection frame.
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.projection\x00\x02"))
	for _, projection := range golden.Projections {
		frame, err := hex.DecodeString(projection.Frame)
		if err != nil {
			t.Fatal(err)
		}
		h.Write(frame)
	}
	p, rows, err = projectionDigest(context.Background(), s.db, testLibrary, 100)
	if err != nil || p != hex.EncodeToString(h.Sum(nil)) || rows != 7 {
		t.Fatalf("migrated metadata projection bytes: %s %d %v", p, rows, err)
	}
	for i, event := range golden.Events {
		var request, receipt []byte
		if err := s.db.QueryRow("SELECT request,receipt FROM events WHERE sequence=?", i+1).Scan(&request, &receipt); err != nil ||
			string(request) != event.Request || string(receipt) != event.Receipt {
			t.Fatalf("frozen event %d changed: %v", i+1, err)
		}
		op, err := decodeOperation(request)
		if err != nil {
			t.Fatal(err)
		}
		retried, err := s.Apply(context.Background(), op)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(retried)
		if err != nil || !bytes.Equal(encoded, receipt) {
			t.Fatalf("old exact retry receipt changed: %v", err)
		}
	}
	if !bytes.Equal(before, authorityOldRows(t, s.db)) {
		t.Fatal("old retries changed history")
	}
	authorityRequireOperationalEmpty(t, s)
	for _, table := range []string{"policy_subjects", "policy_grants", "policy_events"} {
		var count int
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("upgrade invented policy: %s %d %v", table, count, err)
		}
	}
}

func TestAuthorityUpgradeRetriesBarrierWithoutAdoptingAuthority(t *testing.T) {
	path, _ := authorityFrozenV2(t, false)
	barrierErr := errors.New("authority directory barrier failed")
	calls := 0
	barrier := func(string) error {
		calls++
		if calls == 1 {
			return barrierErr
		}
		return nil
	}
	if err := upgradeCatalogVersion(context.Background(), path, testLibrary, 3, barrier); !errors.Is(err, barrierErr) {
		t.Fatalf("first barrier: %v", err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatalf("committed v3 lost after uncertain barrier: %v", err)
	}
	initialStore := s
	t.Cleanup(func() {
		if err := initialStore.Close(); err != nil {
			t.Error(err)
		}
	})
	before := authorityOldRows(t, s.db)
	authorityRequireOperationalEmpty(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := upgradeCatalogVersion(context.Background(), path, testLibrary, 3, barrier); err != nil || calls != 2 {
		t.Fatalf("v3 retry skipped barrier: %d %v", calls, err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	retriedStore := s
	t.Cleanup(func() {
		if err := retriedStore.Close(); err != nil {
			t.Error(err)
		}
	})
	if !bytes.Equal(before, authorityOldRows(t, s.db)) {
		t.Fatal("v3 retry changed old records")
	}
	authorityRequireOperationalEmpty(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := CreateAuthority(context.Background(), path, testLibrary)
	if adopted != nil {
		adopted.Close()
	}
	if err == nil {
		t.Fatal("CreateAuthority adopted existing catalog")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(content, after) {
		t.Fatalf("create changed existing catalog: %v", err)
	}
}

func TestAuthorityUpgradeRefusesUnselectedOrIncompleteInputs(t *testing.T) {
	for _, name := range []string{"v1_requires_prior_upgrade", "wrong_library", "incomplete", "foreign", "missing"} {
		t.Run(name, func(t *testing.T) {
			var path string
			switch name {
			case "v1_requires_prior_upgrade":
				path, _ = legacyFixture(t)
			case "missing":
				path = filepath.Join(t.TempDir(), "absent.sqlite")
			case "foreign":
				path = filepath.Join(t.TempDir(), "foreign.sqlite")
				if err := os.WriteFile(path, []byte("foreign sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
			default:
				path, _ = authorityFrozenV2(t, name == "incomplete")
			}
			library := testLibrary
			if name == "wrong_library" {
				library = otherLibrary
			}
			before, readErr := os.ReadFile(path)
			err := UpgradeAuthority(context.Background(), path, library)
			if err == nil {
				t.Fatal("unselected migration accepted")
			}
			if name == "v1_requires_prior_upgrade" && !errors.Is(err, ErrUpgradeRequired) {
				t.Fatalf("v1 bypass: %v", err)
			}
			if name == "missing" {
				if !os.IsNotExist(readErr) {
					t.Fatal(readErr)
				}
				authorityRequireAbsentTarget(t, path)
			} else {
				if readErr != nil {
					t.Fatal(readErr)
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("refusal modified source bytes: %v", err)
				}
			}
		})
	}
}

func TestAuthorityUpgradeRefusesEveryUnsafeFileMode(t *testing.T) {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		name := strings.TrimPrefix(suffix, "-")
		if name == "" {
			name = "database"
		}
		t.Run(name, func(t *testing.T) {
			path, _ := authorityFrozenV2(t, false)
			target := path + suffix
			if suffix != "" {
				if err := os.WriteFile(target, []byte("unsafe sidecar sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Chmod(target, 0640); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(target)
			if err != nil {
				t.Fatal(err)
			}
			err = UpgradeAuthority(context.Background(), path, testLibrary)
			if err == nil || !strings.Contains(err.Error(), "authority file permissions") {
				t.Fatalf("unsafe mode accepted or unrelated error: %v", err)
			}
			after, err := os.ReadFile(target)
			info, statErr := os.Stat(target)
			if err != nil || statErr != nil || !bytes.Equal(before, after) || info.Mode().Perm() != 0640 {
				t.Fatalf("unsafe input changed: %v %v", err, statErr)
			}
		})
	}
}

func TestAuthorityOpenRefusesUnsafeV3DatabaseMode(t *testing.T) {
	path, _ := authorityFrozenV2(t, false)
	if err := UpgradeAuthority(context.Background(), path, testLibrary); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if s != nil {
		s.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "authority file permissions") {
		t.Fatalf("unsafe v3 Open: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0644 {
		t.Fatalf("Open silently changed mode: %v", err)
	}
	if schemaVersion != 2 {
		t.Fatal("Create/Upgrade default silently promoted beyond v2")
	}
}
