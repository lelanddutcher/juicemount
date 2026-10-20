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
	"sort"
	"testing"
)

func collectionMigrationSnapshot(t *testing.T, s *Store) []byte {
	t.Helper()
	ctx := context.Background()
	var all []any
	for _, table := range []string{"catalog", "assets", "events", "revisions", "revision_parents", "relationships", "locations", "policy_events", "policy_subjects", "policy_grants", "catalog_authority", "catalog_admin_events", "catalog_credentials", "credential_issuance_receipts", "preference_control", "preference_subjects", "preferences", "preference_receipts"} {
		rows, err := s.db.QueryContext(ctx, `SELECT * FROM `+table+` ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		var tableRows []string
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(values))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for i, v := range values {
				if b, ok := v.([]byte); ok {
					values[i] = append([]byte(nil), b...)
				}
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			tableRows = append(tableRows, string(encoded))
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		sort.Strings(tableRows)
		all = append(all, table, tableRows)
	}
	// The migration adds only ledger5; old ledger fingerprints stay exact.
	rows, err := s.db.QueryContext(ctx, `SELECT version,fingerprint FROM schema_migrations WHERE version<=4 ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var n int
		var hash string
		if err := rows.Scan(&n, &hash); err != nil {
			t.Fatal(err)
		}
		all = append(all, n, hash)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestCollectionMigrationFrozenV4PreservesRows(t *testing.T) {
	data, err := os.ReadFile("testdata/continuity-c04b-v4-fixture.sql")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != "3a3f419e55e7c9a86112bb5b4b550d23e0433b7838ae2fd65c88b94f7d8ccc74" {
		t.Fatal("frozen v4 SQL changed")
	}
	manifest, err := os.ReadFile("testdata/continuity-c04b-v4-fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	sum = sha256.Sum256(manifest)
	if hex.EncodeToString(sum[:]) != "d894b3a8782d9874bc1ac6c23ab196df3e7913be885dfef903d2466687d93e3d" {
		t.Fatal("frozen v4 manifest changed")
	}
	path := filepath.Join(t.TempDir(), "frozen.sqlite")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := db.Exec(string(data))
	if err := errors.Join(execErr, db.Close()); err != nil {
		t.Fatal(err)
	}
	s, err := OpenContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	library := s.library
	before := collectionMigrationSnapshot(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeCollections(context.Background(), path, library); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeCollections(context.Background(), path, library); err != nil {
		t.Fatalf("idempotent upgrade: %v", err)
	}
	s, err = OpenContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.version != 5 || !bytes.Equal(before, collectionMigrationSnapshot(t, s)) {
		t.Fatal("v4 rows changed during upgrade")
	}
	if err := checkCollectionOrder(context.Background(), s.db); err != nil {
		t.Fatal(err)
	}
}
func TestCollectionMigrationActiveCredentialAndStaleHandle(t *testing.T) {
	s, o, a, d, bearer, _ := preferencesTestStore(t)
	ctx := context.Background()
	issue, request := issueTest(t, o, a, "before-five", Scope{"library", ""}, ownerSubject)
	if err := d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// New-code cached4 is deliberately retained to prove the in-snapshot fence.
	if err := UpgradeCollections(ctx, s.path, testLibrary); err != nil {
		t.Fatal(err)
	}
	if _, err := o.IssueCredential(ctx, request); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("stale issuance committed: %v", err)
	}
	if _, err := s.Authenticate(ctx, testLibrary, bearer); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("stale auth dispatched codec: %v", err)
	}
	p := PolicyOperation{1, testLibrary, "stale-policy", "local_owner", ownerSubject, []PolicyChange{NewPolicyChange("create_subject", CreateSubject{"sub_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SubjectValue{"person", "stale", true}})}}
	if _, err := o.ApplyPolicy(ctx, adminTestRequest(p.ID, a.Incarnation, "apply_policy", ApplyPolicyRequest{p})); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("stale policy committed: %v", err)
	}
	reopened, err := OpenContext(ctx, s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.clock = s.clock
	if _, err := reopened.Authenticate(ctx, testLibrary, issue.Bearer); err != nil {
		t.Fatalf("codec1 bearer lost: %v", err)
	}
	owner, err := reopened.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := owner.IssueCredential(ctx, request)
	if err != nil || len(retry.Bearer) != 0 || retry.Receipt != issue.Receipt {
		t.Fatalf("old issuance retry: %v", err)
	}
	if _, err := owner.RevokeCredential(ctx, adminTestRequest("revoke-after-five", a.Incarnation, "revoke_credential", RevokeCredentialRequest{issue.Receipt.Result.Selector, 1})); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Authenticate(ctx, testLibrary, issue.Bearer); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("old revocation failed: %v", err)
	}
	var missing int
	if err := reopened.db.QueryRowContext(ctx, `SELECT count(*) FROM catalog_credentials c WHERE NOT EXISTS(SELECT 1 FROM credential_codecs d WHERE d.selector=c.selector)`).Scan(&missing); err != nil || missing != 0 {
		t.Fatalf("codec backfill: %d %v", missing, err)
	}
}
