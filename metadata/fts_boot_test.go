package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

func TestOpenSkipsFullFTSRebuildWhenPendingIsEmpty(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "steady.db")
	now := time.Now().Truncate(time.Second)

	s1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Insert(MakeEntry("Steady/NOBOOTREINDEX.mov", false, 1, now, 1)); err != nil {
		s1.Close()
		t.Fatal(err)
	}
	// Establish the durable epoch exactly as an initial bulk build or the
	// one-time migration rebuild does in production.
	if err := s1.RebuildFTS(); err != nil {
		s1.Close()
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	if got := s2.ftsFullRebuilds.Load(); got != 0 {
		t.Fatalf("steady-state reopen ran %d full FTS rebuild(s), want 0", got)
	}
	if !s2.ftsInitialized.Load() {
		t.Fatal("existing transactionally-current FTS must be marked initialized")
	}
	hits, err := s2.Search("NOBOOTREINDEX", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Entry.Path != "Steady/NOBOOTREINDEX.mov" {
		t.Fatalf("search after skipped boot rebuild = %#v, want the persisted entry", hits)
	}
}

func TestOpenRebuildsOnceWhenFTSEpochIsMissing(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "upgrade.db")
	now := time.Now().Truncate(time.Second)

	s1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Insert(MakeEntry("Upgrade/MIGRATEFTS.mov", false, 1, now, 2)); err != nil {
		s1.Close()
		t.Fatal(err)
	}
	if _, err := s1.db.Exec(`DELETE FROM store_meta WHERE key = ?`, ftsIndexEpochKey); err != nil {
		s1.Close()
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	if got := s2.ftsFullRebuilds.Load(); got != 1 {
		t.Fatalf("epoch migration ran %d full FTS rebuild(s), want 1", got)
	}
	hits, err := s2.Search("MIGRATEFTS", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Entry.Path != "Upgrade/MIGRATEFTS.mov" {
		t.Fatalf("search after epoch migration = %#v, want the persisted entry", hits)
	}
}

func TestOpenRebuildsAfterNamespaceGCInvalidatesFTSEpoch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "gc.db")
	now := time.Now().Truncate(time.Second)

	s1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Insert(MakeEntry(".juicemount/stale/REMOVEFROMFTS.mov", false, 1, now, 3)); err != nil {
		s1.Close()
		t.Fatal(err)
	}
	if err := s1.RebuildFTS(); err != nil {
		s1.Close()
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	if got := s2.ftsFullRebuilds.Load(); got != 1 {
		t.Fatalf("namespace GC recovery ran %d full FTS rebuild(s), want 1", got)
	}
	hits, err := s2.Search("REMOVEFROMFTS", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("namespace-GC deleted row remained searchable: %#v", hits)
	}
}

func TestOpenForceFTSRebuildRepairLever(t *testing.T) {
	t.Setenv("JM_FTS_REBUILD_ON_BOOT", "1")
	s, err := Open(filepath.Join(t.TempDir(), "forced.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if got := s.ftsFullRebuilds.Load(); got != 1 {
		t.Fatalf("forced open ran %d full FTS rebuild(s), want 1", got)
	}
}

func TestOpenEmptyDatabaseLeavesFTSUninitializedForFirstBulkLoad(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "new.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if s.ftsInitialized.Load() {
		t.Fatal("empty database marked FTS initialized; first large sync would take the slow per-row path")
	}
	if got := s.ftsFullRebuilds.Load(); got != 0 {
		t.Fatalf("empty open ran %d full rebuild(s), want 0", got)
	}
}
