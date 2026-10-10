package metadata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Preserve the interfaces implemented by the pinned modernc SQLite driver;
// only transaction Commit is intercepted. No production driver is registered.
type pruneSQLiteConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.SessionResetter
	driver.Validator
	driver.Pinger
}

type pruneCommitGate struct {
	entered     chan struct{}
	released    chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
	fail        error // immutable before the transaction starts
}

func (g *pruneCommitGate) release() { g.releaseOnce.Do(func() { close(g.released) }) }
func (g *pruneCommitGate) commit(tx driver.Tx) error {
	first := false
	g.enterOnce.Do(func() { first = true })
	if first {
		close(g.entered)
		<-g.released
		if g.fail != nil {
			// database/sql marks Tx done on Commit; explicitly roll back the real
			// driver transaction to model a definite pre-commit failure.
			if err := tx.Rollback(); err != nil {
				return fmt.Errorf("injected commit rollback: %w", err)
			}
			return g.fail
		}
	}
	return tx.Commit()
}

type pruneCommitConnector struct {
	base driver.Driver
	dsn  string
	gate *pruneCommitGate
}

func (c *pruneCommitConnector) Driver() driver.Driver { return c.base }
func (c *pruneCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	sqlite, ok := conn.(pruneSQLiteConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("SQLite connection lacks expected context/session interfaces")
	}
	return &pruneCommitConn{pruneSQLiteConn: sqlite, gate: c.gate}, nil
}

type pruneCommitConn struct {
	pruneSQLiteConn
	gate *pruneCommitGate
}

func (c *pruneCommitConn) Begin() (driver.Tx, error) {
	tx, err := c.pruneSQLiteConn.Begin()
	if err != nil {
		return nil, err
	}
	return &pruneCommitTx{Tx: tx, gate: c.gate}, nil
}
func (c *pruneCommitConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.pruneSQLiteConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &pruneCommitTx{Tx: tx, gate: c.gate}, nil
}

type pruneCommitTx struct {
	driver.Tx
	gate *pruneCommitGate
}

func (tx *pruneCommitTx) Commit() error { return tx.gate.commit(tx.Tx) }

func installPruneCommitGate(t *testing.T, s *Store, dbPath string, fail error) *pruneCommitGate {
	t.Helper()
	// No worker may read Store.db while the test replaces the handle. Retain
	// the Store file lock, maps and real WAL database throughout replacement.
	if s.ftsStop != nil || (s.serve != nil && (s.serve.err != nil || s.serve.byPath != nil || s.serve.byInode != nil || s.serve.childrenPg != nil || s.serve.childrenAll != nil)) {
		t.Fatal("commit fixture requires FTS off and unprepared serve statements")
	}
	s.pruneLadderGCStopOnce.Do(func() { close(s.pruneLadderGCStop) })
	select {
	case <-s.pruneLadderGCDone:
	case <-time.After(5 * time.Second):
		t.Fatal("GC worker did not stop")
	}
	base := s.db.Driver()
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s.serve = &serveStmts{} // quiescent reset of the lazy initializer for the new pool
	gate := &pruneCommitGate{entered: make(chan struct{}), released: make(chan struct{}), fail: fail}
	s.db = sql.OpenDB(&pruneCommitConnector{base: base, dsn: dbPath + "?_pragma=busy_timeout(30000)&_txlock=immediate", gate: gate})
	s.db.SetMaxOpenConns(1)
	var mode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("wrapped DB journal mode=%q err=%v", mode, err)
	}
	t.Cleanup(gate.release)
	return gate
}

func TestConditionalPruneCommitWaitKeepsCachedNavigationResponsive(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	t.Setenv("JM_READDIR_PAGINATED", "0")
	file := filepath.Join(t.TempDir(), "mirror.db")
	s, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	stamp := time.Unix(1700000000, 0)
	if err := s.BulkInsert([]*Entry{
		MakeEntry("known", true, 0, stamp, 10), MakeEntry("known/clip.mov", false, 4096, stamp, 11),
		MakeEntry("pruned", true, 0, stamp, 20), MakeEntry("pruned/old.mov", false, 42, stamp, 21),
	}, 500); err != nil {
		t.Fatal(err)
	}
	expected := []*Entry{s.lookupByPathRAM("pruned"), s.lookupByPathRAM("pruned/old.mov")}
	var invalidated []uint64
	s.SetOnScanContentInvalidated(func(name string, inode uint64) {
		if name != "pruned/old.mov" {
			t.Errorf("invalidated unexpected path %q", name)
		}
		invalidated = append(invalidated, inode)
	})
	gate := installPruneCommitGate(t, s, file, nil)
	done := make(chan error, 1)
	settled := false
	go func() { done <- s.DeletePathsConditional(expected) }()
	defer func() {
		gate.release()
		if !settled {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("prune did not settle")
			}
		}
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("prune did not reach real driver Commit")
	}
	if s.mu.TryLock() {
		s.mu.Unlock()
	} else {
		t.Error("prune retained cache lock during Commit")
	}
	type navigation struct {
		p, i     *Entry
		children []*Entry
		err      error
		elapsed  time.Duration
	}
	navDone := make(chan navigation, 1)
	go func() {
		start := time.Now()
		p := s.LookupByPath("known/clip.mov")
		i := s.LookupByInode(11)
		children, err := s.ListChildrenForReadDir("known")
		navDone <- navigation{p, i, children, err, time.Since(start)}
	}()
	var nav navigation
	select {
	case nav = <-navDone:
		t.Logf("cached navigation before Commit release: %s", nav.elapsed)
	case <-time.After(250 * time.Millisecond):
		t.Error("cached navigation waited >250ms behind scoped-prune Commit")
		gate.release()
		select {
		case nav = <-navDone:
		case <-time.After(5 * time.Second):
			t.Fatal("cached navigation did not settle")
		}
	}
	if nav.err != nil || nav.p == nil || nav.p.Size != 4096 || nav.i == nil || nav.i.Path != "known/clip.mov" || len(nav.children) != 1 || nav.children[0].Inode != 11 {
		t.Fatalf("incorrect warm navigation: %+v", nav)
	}
	gate.release()
	select {
	case err = <-done:
		settled = true
	case <-time.After(5 * time.Second):
		t.Fatal("prune did not complete")
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(invalidated) != 1 || invalidated[0] != 21 {
		t.Fatalf("incorrect old-inode invalidation: %v", invalidated)
	}
	for _, entry := range expected {
		if s.lookupByPathRAM(entry.Path) != nil || s.lookupByInodeRAM(entry.Inode) != nil || mirrorDurableEntry(t, s, entry.Path) != nil {
			t.Fatalf("pruned row survived: %q", entry.Path)
		}
	}
}
