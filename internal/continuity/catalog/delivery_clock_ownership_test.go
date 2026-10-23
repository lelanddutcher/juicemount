package catalog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	sqlite "modernc.org/sqlite"
)

// This wrapper forwards every SQL operation to modernc SQLite. It controls only
// the return boundary of the real original rollback or checkpoint commit, or
// reports a fault at one checkpoint-specific driver call. In particular, a
// reported commit error is injected only AFTER the underlying commit returns.
type clockOwnershipHooks struct {
	mu                sync.Mutex
	begins            int
	originalRollbacks int
	checkpointCommits int
	rollbackGate      *clockOwnershipGate
	commitGate        *clockOwnershipGate
	fault             string
}

func (h *clockOwnershipHooks) nextBegin() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.begins++
	return h.begins
}

func (h *clockOwnershipHooks) recordRollback(role int) {
	if role != 1 {
		return
	}
	h.mu.Lock()
	h.originalRollbacks++
	h.mu.Unlock()
}

func (h *clockOwnershipHooks) recordCommit(role int) {
	if role != 2 {
		return
	}
	h.mu.Lock()
	h.checkpointCommits++
	h.mu.Unlock()
}

func (h *clockOwnershipHooks) counts() (int, int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.begins, h.originalRollbacks, h.checkpointCommits
}

type clockOwnershipGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newClockOwnershipGate() *clockOwnershipGate {
	return &clockOwnershipGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *clockOwnershipGate) pause() {
	close(g.entered)
	<-g.release
}

func (g *clockOwnershipGate) open() {
	g.once.Do(func() { close(g.release) })
}

type clockOwnershipConnector struct {
	underlying driver.Driver
	dsn        string
	hooks      *clockOwnershipHooks
}

func (c *clockOwnershipConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	underlying, err := c.underlying.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &clockOwnershipConn{underlying: underlying, hooks: c.hooks}, nil
}

func (c *clockOwnershipConnector) Driver() driver.Driver { return c.underlying }

type clockOwnershipConn struct {
	underlying driver.Conn
	hooks      *clockOwnershipHooks
	role       int
}

var _ driver.Validator = (*clockOwnershipConn)(nil)

func (c *clockOwnershipConn) Prepare(query string) (driver.Stmt, error) {
	return c.underlying.Prepare(query)
}

func (c *clockOwnershipConn) Close() error { return c.underlying.Close() }

func (c *clockOwnershipConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *clockOwnershipConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	role := c.hooks.nextBegin()
	if role == 2 && c.hooks.fault == "acquire" {
		return nil, errors.New("injected checkpoint acquire fault")
	}
	underlying, err := c.underlying.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	c.role = role
	return &clockOwnershipTx{underlying: underlying, conn: c, role: role}, nil
}

func (c *clockOwnershipConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.underlying.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

func (c *clockOwnershipConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.role == 2 && c.hooks.fault == "write" && strings.HasPrefix(query, "UPDATE delivery_clock SET high_water_unix=") {
		return nil, errors.New("injected checkpoint write fault")
	}
	return c.underlying.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *clockOwnershipConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.underlying.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *clockOwnershipConn) Ping(ctx context.Context) error {
	return c.underlying.(driver.Pinger).Ping(ctx)
}

func (c *clockOwnershipConn) ResetSession(ctx context.Context) error {
	return c.underlying.(driver.SessionResetter).ResetSession(ctx)
}

func (c *clockOwnershipConn) IsValid() bool {
	return c.underlying.(driver.Validator).IsValid()
}

type clockOwnershipTx struct {
	underlying driver.Tx
	conn       *clockOwnershipConn
	role       int
}

func (tx *clockOwnershipTx) Commit() error {
	if tx.role == 2 && tx.conn.hooks.commitGate != nil {
		tx.conn.hooks.commitGate.pause()
	}
	err := tx.underlying.Commit()
	tx.conn.role = 0
	if err != nil {
		return err
	}
	tx.conn.hooks.recordCommit(tx.role)
	if tx.role == 2 && tx.conn.hooks.fault == "commit-after-underlying" {
		return errors.New("injected checkpoint commit report fault after underlying commit")
	}
	return nil
}

func (tx *clockOwnershipTx) Rollback() error {
	err := tx.underlying.Rollback()
	tx.conn.role = 0
	if err == nil {
		tx.conn.hooks.recordRollback(tx.role)
		if tx.role == 1 && tx.conn.hooks.rollbackGate != nil {
			tx.conn.hooks.rollbackGate.pause()
		}
	}
	return err
}

func installClockOwnershipDB(t *testing.T, s *Store, hooks *clockOwnershipHooks) {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: s.path}
	query := url.Values{}
	query.Set("mode", "rw")
	query.Set("_txlock", "immediate")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "busy_timeout(2000)")
	query.Add("_pragma", "synchronous(FULL)")
	uri.RawQuery = query.Encode()
	wrapped := sql.OpenDB(&clockOwnershipConnector{underlying: &sqlite.Driver{}, dsn: uri.String(), hooks: hooks})
	wrapped.SetMaxOpenConns(1)
	wrapped.SetMaxIdleConns(1)
	original := s.db
	s.db = wrapped
	t.Cleanup(func() {
		if err := wrapped.Close(); err != nil {
			t.Error(err)
		}
		s.db = original
		// Store.Close may already have closed the substituted pool and file.
		// In that case the original pool still needs its own checked close.
		if s.closed {
			if err := original.Close(); err != nil {
				t.Error(err)
			}
		}
	})
}

type clockOwnershipCall struct {
	done chan struct{}
	err  error
}

func startClockOwnershipCall(run func() error) *clockOwnershipCall {
	call := &clockOwnershipCall{done: make(chan struct{})}
	go func() {
		call.err = run()
		close(call.done)
	}()
	return call
}

func (call *clockOwnershipCall) wait() error {
	<-call.done
	return call.err
}

func awaitClockOwnershipGate(t *testing.T, gate *clockOwnershipGate, call *clockOwnershipCall) {
	t.Helper()
	select {
	case <-gate.entered:
	case <-call.done:
		t.Fatalf("catalog call finished before driver gate: %v", call.err)
	}
}

func TestDeliveryClockRollbackCheckpointOverlappingWriter(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	_, selection := f.selectLatest(t, "clock-interleaving", deliveryClockBase+60)
	other, err := Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := other.Close(); err != nil {
			t.Error(err)
		}
	})
	other.clock = func() time.Time { return time.Unix(deliveryClockBase+80, 0) }
	beforeSelections, beforeAttempts, beforeCanonical, beforeBytes := clockQuota(t, f.store)
	rollback := newClockOwnershipGate()
	checkpoint := newClockOwnershipGate()
	hooks := &clockOwnershipHooks{rollbackGate: rollback, commitGate: checkpoint}
	installClockOwnershipDB(t, f.store, hooks)
	f.now.Store(deliveryClockBase + 60)
	denial := startClockOwnershipCall(func() error {
		_, err := f.store.DeliveryStatus(context.Background(), f.bearer, selection.SelectionID)
		return err
	})
	t.Cleanup(func() { rollback.open(); checkpoint.open(); denial.wait() })
	awaitClockOwnershipGate(t, rollback, denial)
	// The first SQLite Rollback has actually returned, releasing its writer
	// lock. The first Store still owns finalization and has not responded.
	_, err = other.DeliveryStatus(context.Background(), f.bearer, selection.SelectionID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("intervening Store writer: %v", err)
	}
	if got := clockFloor(t, other); got != deliveryClockBase+80 {
		t.Fatalf("intervening Store floor=%d", got)
	}
	rollback.open()
	awaitClockOwnershipGate(t, checkpoint, denial)
	select {
	case <-denial.done:
		t.Fatal("definitive denial returned before checkpoint commit")
	default:
	}
	checkpoint.open()
	if err := denial.wait(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("final denial after checkpoint: %v", err)
	}
	if begins, rollbacks, commits := hooks.counts(); begins != 2 || rollbacks != 1 || commits != 1 {
		t.Fatalf("driver transaction sequence: begins=%d rollbacks=%d commits=%d", begins, rollbacks, commits)
	}
	if got := clockFloor(t, f.store); got != deliveryClockBase+80 {
		t.Fatalf("checkpoint lowered concurrent floor=%d", got)
	}
	if selections, attempts, canonical, bytes := clockQuota(t, f.store); selections != beforeSelections || attempts != beforeAttempts || canonical != beforeCanonical || bytes != beforeBytes {
		t.Fatal("denial or intervening status changed business quota")
	}
	f.now.Store(deliveryClockBase)
	other.clock = func() time.Time { return time.Unix(deliveryClockBase, 0) }
	if _, err := other.AdmitDelivery(context.Background(), f.bearer, selection.SelectionID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("post-response wall rollback revived admission: %v", err)
	}
}

func TestDeliveryClockCheckpointHeldThroughCancellationAndClose(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	_, selection := f.selectLatest(t, "clock-close-checkpoint", deliveryClockBase+60)
	checkpoint := newClockOwnershipGate()
	hooks := &clockOwnershipHooks{commitGate: checkpoint}
	installClockOwnershipDB(t, f.store, hooks)
	f.now.Store(deliveryClockBase + 60)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	denial := startClockOwnershipCall(func() error {
		_, err := f.store.DeliveryStatus(ctx, f.bearer, selection.SelectionID)
		return err
	})
	t.Cleanup(func() { checkpoint.open(); denial.wait() })
	awaitClockOwnershipGate(t, checkpoint, denial)
	cancel()
	closing := startClockOwnershipCall(f.store.Close)
	t.Cleanup(func() { checkpoint.open(); closing.wait() })
	// The original call still owns a reader lock. TryRLock can fail here only
	// after Close has actually queued its exclusive lock; launching its
	// goroutine alone would not establish the overlap.
	deadline := time.Now().Add(5 * time.Second)
	contended := false
	observationFailure := "Close never contended on Store.mu"
	for time.Now().Before(deadline) {
		select {
		case <-closing.done:
			observationFailure = "Store.Close returned while checkpoint commit was held"
			goto observationDone
		case <-denial.done:
			observationFailure = "denial returned while checkpoint commit was held"
			goto observationDone
		default:
		}
		if !f.store.mu.TryRLock() {
			contended = true
			break
		}
		f.store.mu.RUnlock()
		runtime.Gosched()
	}
observationDone:
	if !contended {
		checkpoint.open()
		denialErr := denial.wait()
		closeErr := closing.wait()
		t.Fatalf("%s; original=%v close=%v", observationFailure, denialErr, closeErr)
	}
	premature := ""
	select {
	case <-closing.done:
		premature = "Store.Close returned despite observed contention"
	case <-denial.done:
		premature = "denial returned while checkpoint commit was held"
	default:
	}
	if premature != "" {
		checkpoint.open()
		denialErr := denial.wait()
		closeErr := closing.wait()
		t.Fatalf("%s; original=%v close=%v", premature, denialErr, closeErr)
	}
	checkpoint.open()
	denialErr := denial.wait()
	closeErr := closing.wait()
	if !errors.Is(denialErr, ErrNotFound) {
		t.Fatalf("request cancellation erased original denial: %v", denialErr)
	}
	if closeErr != nil {
		t.Fatalf("Store.Close after checkpoint: %v", closeErr)
	}
	if begins, rollbacks, commits := hooks.counts(); begins != 2 || rollbacks != 1 || commits != 1 {
		t.Fatalf("checkpoint/Close transaction sequence: begins=%d rollbacks=%d commits=%d", begins, rollbacks, commits)
	}
	reopened, err := Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if got := clockFloor(t, reopened); got != deliveryClockBase+60 {
		t.Fatalf("Close preceded durable checkpoint: floor=%d", got)
	}
}

func TestDeliveryClockCheckpointFaultsMaskDefinitiveDenials(t *testing.T) {
	for _, denial := range []struct {
		name           string
		credentialLife int64
		want           error
	}{
		{"selection expiry", 3600, ErrNotFound},
		{"credential expiry", 60, ErrUnauthenticated},
	} {
		for _, fault := range []string{"acquire", "write", "commit-after-underlying"} {
			t.Run(denial.name+"/"+fault, func(t *testing.T) {
				f := newDeliveryClockFixture(t, denial.credentialLife)
				_, selection := f.selectLatest(t, "clock-checkpoint-fault", deliveryClockBase+60)
				beforeSelections, beforeAttempts, beforeCanonical, beforeBytes := clockQuota(t, f.store)
				hooks := &clockOwnershipHooks{fault: fault}
				installClockOwnershipDB(t, f.store, hooks)
				f.now.Store(deliveryClockBase + 60)
				_, err := f.store.DeliveryStatus(context.Background(), f.bearer, selection.SelectionID)
				if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrUnauthenticated) || !strings.Contains(err.Error(), "delivery clock outcome uncertain") || !strings.Contains(err.Error(), "prior outcome: "+denial.want.Error()) {
					t.Fatalf("checkpoint %s escaped as a definitive denial: %v", fault, err)
				}
				wantCommits := 0
				if fault == "commit-after-underlying" {
					wantCommits = 1
				}
				if begins, rollbacks, commits := hooks.counts(); begins != 2 || rollbacks != 1 || commits != wantCommits {
					t.Fatalf("checkpoint %s transaction sequence: begins=%d rollbacks=%d commits=%d", fault, begins, rollbacks, commits)
				}
				wantFloor := deliveryClockBase
				if fault == "commit-after-underlying" {
					wantFloor += 60
				}
				if got := clockFloor(t, f.store); got != wantFloor {
					t.Fatalf("checkpoint %s floor=%d, want %d", fault, got, wantFloor)
				}
				if selections, attempts, canonical, bytes := clockQuota(t, f.store); selections != beforeSelections || attempts != beforeAttempts || canonical != beforeCanonical || bytes != beforeBytes {
					t.Fatalf("checkpoint %s changed business quota", fault)
				}
				var rowCount int
				if err := f.store.db.QueryRow(`SELECT count(*) FROM delivery_materializations`).Scan(&rowCount); err != nil || rowCount != 0 {
					t.Fatalf("checkpoint %s created business row: %d %v", fault, rowCount, err)
				}
				if fault == "commit-after-underlying" {
					other, err := Open(f.path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := other.Close(); err != nil {
							t.Error(err)
						}
					})
					if got := clockFloor(t, other); got != wantFloor {
						t.Fatalf("reported commit fault rolled back real checkpoint: %d", got)
					}
				}
			})
		}
	}
}
