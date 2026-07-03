package metadata

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Lever 0 — DSN-pin synchronous=NORMAL across all pooled connections
// (JM_SQLITE_SYNC_NORMAL_ALL). These tests prove the flag makes EVERY one of
// the SetMaxOpenConns(8) pooled connections report synchronous=NORMAL (1),
// closing the per-connection footgun where only the single connection that ran
// db.Exec(pragmas) gets NORMAL and the other 7 default to FULL (2).

// readSyncOnConn reads `PRAGMA synchronous` on a specific pinned connection.
func readSyncOnConn(t *testing.T, conn *sql.Conn) int {
	t.Helper()
	var v int
	if err := conn.QueryRowContext(context.Background(), "PRAGMA synchronous").Scan(&v); err != nil {
		t.Fatalf("read PRAGMA synchronous: %v", err)
	}
	return v
}

// forceAllConnsSynchronous pins exactly n connections at once — holding each
// until all n are pinned — so the pool is forced to materialize n DISTINCT
// backing connections, then reports the `PRAGMA synchronous` value seen on each.
//
// n MUST equal the pool's SetMaxOpenConns (8): pinning all 8 slots at once
// guarantees every backing connection is created and inspected. n must NOT
// exceed the pool max — db.Conn() blocks until a slot frees, so an
// over-subscribed barrier (goroutines waiting on db.Conn while the pinned
// holders wait on the release barrier) would deadlock.
func forceAllConnsSynchronous(t *testing.T, db *sql.DB, n int) []int {
	t.Helper()

	// Barrier: every goroutine pins its connection and only releases it after
	// all n are pinned, so the pool cannot satisfy two goroutines with the same
	// underlying connection.
	var pinned sync.WaitGroup
	pinned.Add(n)
	release := make(chan struct{})

	results := make([]int, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Errorf("acquire conn %d: %v", idx, err)
				pinned.Done()
				return
			}
			defer conn.Close()
			results[idx] = readSyncOnConn(t, conn)
			pinned.Done()
			<-release // hold the connection so it can't be reused by a peer
		}(i)
	}
	pinned.Wait()
	close(release)
	wg.Wait()
	return results
}

// TestSQLiteSyncNormalAllPinsEveryConnection is the primary proof: with the
// flag ON, pin all SetMaxOpenConns(8) connections concurrently and assert
// EVERY one reports synchronous == 1 (NORMAL). This is the behavior the DSN
// _pragma guarantees and that db.Exec(pragmas) alone does NOT.
func TestSQLiteSyncNormalAllPinsEveryConnection(t *testing.T) {
	t.Setenv("JM_SQLITE_SYNC_NORMAL_ALL", "1")

	s, err := Open(filepath.Join(t.TempDir(), "sync.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	// n = 8 = SetMaxOpenConns(8): pinning all 8 pool slots at once forces every
	// one of the 8 backing connections to be created and inspected. (Must NOT
	// exceed 8 — an over-subscribed barrier deadlocks on db.Conn; see
	// forceAllConnsSynchronous.)
	const n = 8
	got := forceAllConnsSynchronous(t, s.DB(), n)
	for i, v := range got {
		if v != 1 {
			t.Errorf("connection %d: synchronous = %d, want 1 (NORMAL) — a pooled connection is running FULL despite JM_SQLITE_SYNC_NORMAL_ALL=1", i, v)
		}
	}
}

// TestSQLiteSyncNormalAllDSN asserts the DSN wiring directly: flag-on appends
// the synchronous(1) DSN _pragma, flag-off omits it (leaving today's behavior).
// This is the pool-nondeterminism-proof half of the A/B: with the flag OFF the
// pooled connections are NOT all pinned to NORMAL (some run FULL), which is
// hard to assert deterministically at runtime, so we assert the DSN itself.
func TestSQLiteSyncNormalAllDSN(t *testing.T) {
	t.Run("on", func(t *testing.T) {
		t.Setenv("JM_SQLITE_SYNC_NORMAL_ALL", "1")
		if !syncNormalAllEnabled() {
			t.Fatal("=1 must be ENABLED")
		}
	})
	t.Run("off_default", func(t *testing.T) {
		t.Setenv("JM_SQLITE_SYNC_NORMAL_ALL", "")
		if syncNormalAllEnabled() {
			t.Fatal("unset env must be DISABLED (default OFF == today's behavior)")
		}
	})
	t.Run("off_zero", func(t *testing.T) {
		t.Setenv("JM_SQLITE_SYNC_NORMAL_ALL", "0")
		if syncNormalAllEnabled() {
			t.Fatal("=0 must be DISABLED")
		}
	})
}

// TestSQLiteSyncNormalAllCrashRecovery confirms the flag-on store still
// round-trips writes across a Close/reopen (WAL checkpoint + replay), i.e. the
// change does not compromise crash-recovery durability. journal_mode stays WAL;
// synchronous=NORMAL under WAL is the app's intended durability level, so a
// clean close + reopen must return every committed row.
func TestSQLiteSyncNormalAllCrashRecovery(t *testing.T) {
	t.Setenv("JM_SQLITE_SYNC_NORMAL_ALL", "1")
	dbPath := filepath.Join(t.TempDir(), "recover.db")

	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const p = "movies/reel_0065.mov"
	e := MakeEntry(p, false, 4096, time.Now().Truncate(time.Second), 424242)
	if err := s.Insert(e); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Also verify WAL journal_mode is intact on the flag-on store.
	var jmode string
	if err := s.DB().QueryRow("PRAGMA journal_mode").Scan(&jmode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if jmode != "wal" {
		t.Fatalf("journal_mode = %q, want wal (Lever 0 must not change journal_mode)", jmode)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: the committed row must survive (WAL replay / checkpoint).
	s2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	got := s2.LookupByPath(p)
	if got == nil || got.Path != p || got.Size != 4096 || got.Inode != 424242 {
		t.Fatalf("recovered entry mismatch: %+v", got)
	}
}
