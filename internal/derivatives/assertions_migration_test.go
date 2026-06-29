package derivatives

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestAssertionsMigration_AddsValueJSONToOldTable reproduces W1 — the migration
// drift OpenLoupe hit live: a deployed assertions table created by an older
// binary lacks the value_json column, so `CREATE TABLE IF NOT EXISTS` is a no-op
// and every POST /assertions 500s "table assertions has no column named
// value_json". Open() must ALTER the column in, after which the write succeeds.
func TestAssertionsMigration_AddsValueJSONToOldTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "deriv-old.db")

	// Stand up an OLD-schema assertions table: every original column EXCEPT
	// value_json (this is exactly the deployed shape — GET /assertions read it
	// fine, but writes 500'd).
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE assertions (
		asset_key TEXT NOT NULL, namespace TEXT NOT NULL, key TEXT NOT NULL,
		asserted_by TEXT NOT NULL, asserted_at TEXT NOT NULL, inode INTEGER,
		PRIMARY KEY (asset_key, namespace, key));`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()

	// Open via the Store — initAssertions must migrate value_json in.
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open on an old-schema (no value_json) DB failed: %v", err)
	}
	defer s.Close()

	// The exact write that used to 500.
	r, err := s.AssertLWW("xxh3:deadbeefdeadbeef", "rating", "stars", `5`, "ol:test", "2026-06-28T12:00:00Z", 840383)
	if err != nil {
		t.Fatalf("AssertLWW after migration failed (W1 not fixed): %v", err)
	}
	if !r.Accepted {
		t.Fatalf("expected accept after migration, got %+v", r)
	}
	got, err := s.AssertionsByAssetKey("xxh3:deadbeefdeadbeef")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("readback after migration: got %d rows, want 1", len(got))
	}
}

// TestAssertionsMigration_MissingValueJSONAndInode covers the worst drift: an
// even older table missing BOTH value_json and inode. The inode index must be
// created only AFTER both columns are ALTERed in (table-then-columns-then-index
// ordering), or Open would error on the index referencing a missing column.
func TestAssertionsMigration_MissingValueJSONAndInode(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "deriv-older.db")

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE assertions (
		asset_key TEXT NOT NULL, namespace TEXT NOT NULL, key TEXT NOT NULL,
		asserted_by TEXT NOT NULL, asserted_at TEXT NOT NULL,
		PRIMARY KEY (asset_key, namespace, key));`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()

	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open on a no-value_json/no-inode DB failed: %v", err)
	}
	defer s.Close()

	if _, err := s.AssertLWW("xxh3:abc", "pick", "flag", `true`, "ol:test", "2026-06-28T12:00:00Z", 7); err != nil {
		t.Fatalf("write after dual-column migration failed: %v", err)
	}
}

// TestAssertionsMigration_FreshDBUnaffected confirms the migration path doesn't
// disturb the green-in-test case: a brand-new DB still writes fine.
func TestAssertionsMigration_FreshDBUnaffected(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "deriv-fresh.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.AssertLWW("xxh3:fresh", "color", "label", `"red"`, "ol:test", "2026-06-28T12:00:00Z", 0); err != nil {
		t.Fatalf("fresh DB write failed: %v", err)
	}
}
