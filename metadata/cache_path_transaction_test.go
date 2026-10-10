package metadata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

// This driver delegates real statements and transactions to modernc SQLite.
// Only the actual driver Tx settlement is gated; a database/sql pool wait or
// a mock transaction would not prove that Store retains custody of SQLite.
type cachePathSettlementDriver struct{ gate *cachePathSettlementGate }

func (d *cachePathSettlementDriver) Open(name string) (driver.Conn, error) {
	conn, err := (&sqlite.Driver{}).Open(name)
	if err != nil {
		return nil, err
	}
	return &cachePathSettlementConn{Conn: conn, gate: d.gate}, nil
}

type cachePathSettlementConn struct {
	driver.Conn
	gate *cachePathSettlementGate
}

func (c *cachePathSettlementConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginner, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New("modernc SQLite no longer implements driver.ConnBeginTx")
	}
	tx, err := beginner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	if c.gate.pauseBegin {
		close(c.gate.beginEntered)
		<-c.gate.releaseBegin
	}
	return &cachePathSettlementTx{Tx: tx, gate: c.gate}, nil
}

func (c *cachePathSettlementConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

func (c *cachePathSettlementConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *cachePathSettlementConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type cachePathSettlementTx struct {
	driver.Tx
	gate *cachePathSettlementGate
}

func (tx *cachePathSettlementTx) Commit() error {
	if tx.gate.pauseCommit {
		close(tx.gate.settlementEntered)
		<-tx.gate.releaseSettlement
	}
	err := tx.Tx.Commit()
	if tx.gate.pauseCommit {
		close(tx.gate.settled)
	}
	return err
}

func (tx *cachePathSettlementTx) Rollback() error {
	if tx.gate.pauseRollback {
		close(tx.gate.settlementEntered)
		<-tx.gate.releaseSettlement
	}
	err := tx.Tx.Rollback()
	if tx.gate.pauseRollback {
		close(tx.gate.settled)
	}
	return err
}

type cachePathSettlementGate struct {
	pauseBegin, pauseCommit, pauseRollback bool
	beginEntered, releaseBegin             chan struct{}
	settlementEntered, releaseSettlement   chan struct{}
	settled                                chan struct{}
	releaseBeginOnce, releaseOnce          sync.Once
}

func (g *cachePathSettlementGate) release() {
	g.releaseBeginOnce.Do(func() { close(g.releaseBegin) })
	g.releaseOnce.Do(func() { close(g.releaseSettlement) })
}

var cachePathSettlementDriverID atomic.Uint64

func cachePathAwaitSettlement(t *testing.T, stage string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("driver never reached %s", stage)
	}
}

func TestCachePathConfirmRetainsStoreAndConnectionUntilDriverSettlement(t *testing.T) {
	for _, phase := range []string{"rollback", "commit"} {
		t.Run(phase, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "mirror.db")
			s, eligibility, old, oldID := qualifiedCachePathFixture(t, filename)
			if err := eligibility.ConfirmCachePath(testCachePathBinding, old, cachePathTestProof(old, testCachePathBinding.FormatUUID)); err != nil {
				t.Fatal(err)
			}
			newEntry := cachePathTestEntry("a/new.mov", false, 23, 1700000002)
			if err := s.Insert(newEntry); err != nil {
				t.Fatal(err)
			}
			observed, err := eligibility.CaptureCachePath(newEntry.Path)
			if err != nil {
				t.Fatal(err)
			}
			newID, ok := s.LookupCachedFileIdentity(newEntry.Path)
			if !ok {
				t.Fatal("missing resident candidate identity")
			}
			gate := &cachePathSettlementGate{
				pauseBegin: phase == "rollback", pauseCommit: phase == "commit", pauseRollback: phase == "rollback",
				beginEntered: make(chan struct{}), releaseBegin: make(chan struct{}),
				settlementEntered: make(chan struct{}), releaseSettlement: make(chan struct{}),
				settled: make(chan struct{}),
			}
			name := fmt.Sprintf("cache-path-settlement-%d", cachePathSettlementDriverID.Add(1))
			sql.Register(name, &cachePathSettlementDriver{gate: gate})
			wrapped, err := sql.Open(name, filename+"?_pragma=busy_timeout(30000)&_txlock=immediate")
			if err != nil {
				t.Fatal(err)
			}
			wrapped.SetMaxOpenConns(1)
			original := s.db
			s.db = wrapped
			t.Cleanup(func() {
				s.db = original
				_ = wrapped.Close()
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- eligibility.ConfirmCachePathContext(ctx, testCachePathBinding, observed, cachePathTestProof(observed, testCachePathBinding.FormatUUID))
			}()
			finished := false
			defer func() {
				gate.release()
				if !finished {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("Confirm did not settle during test cleanup")
					}
				}
			}()
			if phase == "rollback" {
				cachePathAwaitSettlement(t, "driver BeginTx", gate.beginEntered)
				cancel()
				gate.releaseBeginOnce.Do(func() { close(gate.releaseBegin) })
			} else {
				cachePathAwaitSettlement(t, "driver Commit", gate.settlementEntered)
				cancel()
			}
			cachePathAwaitSettlement(t, "driver "+phase, gate.settlementEntered)
			select {
			case err := <-done:
				t.Fatalf("Confirm returned before driver %s settled: %v", phase, err)
			default:
			}
			if s.writeMu.TryLock() {
				s.writeMu.Unlock()
				t.Fatalf("Store write lock released before driver %s settled", phase)
			}
			if s.cachePublicationMu.TryLock() {
				s.cachePublicationMu.Unlock()
				t.Fatalf("cache publication lock released before driver %s settled", phase)
			}
			if inUse := wrapped.Stats().InUse; inUse != 1 {
				t.Fatalf("SQL connection returned while driver %s paused: inUse=%d", phase, inUse)
			}
			hot := make(chan bool, 1)
			go func() { hot <- eligibility.CachePathEligible(testCachePathBinding, oldID) }()
			select {
			case eligible := <-hot:
				if !eligible {
					t.Fatal("RAM-only qualified hot check lost eligibility during settlement")
				}
			case <-time.After(250 * time.Millisecond):
				t.Fatal("RAM-only qualified hot check waited for SQL settlement")
			}
			gate.releaseOnce.Do(func() { close(gate.releaseSettlement) })
			cachePathAwaitSettlement(t, "driver settlement completion", gate.settled)
			select {
			case err := <-done:
				finished = true
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled confirmation returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Confirm did not return after driver settlement")
			}
			if !s.writeMu.TryLock() {
				t.Fatal("Store write lock remained held after Confirm returned")
			}
			s.writeMu.Unlock()
			if !s.cachePublicationMu.TryLock() {
				t.Fatal("cache publication lock remained held after Confirm returned")
			}
			s.cachePublicationMu.Unlock()
			if inUse := wrapped.Stats().InUse; inUse != 0 {
				t.Fatalf("SQL connection still owned after Confirm returned: inUse=%d", inUse)
			}
			if eligibility.CachePathEligible(testCachePathBinding, newID) {
				t.Fatal("canceled confirmation published RAM eligibility")
			}
			var count int
			if err := original.QueryRow(`SELECT count(*) FROM cache_path_eligibility WHERE path=?`, newEntry.Path).Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 0
			if phase == "commit" {
				want = 1
			}
			if count != want {
				t.Fatalf("driver %s durable row count=%d, want %d", phase, count, want)
			}
		})
	}
}
