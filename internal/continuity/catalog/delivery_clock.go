package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// A deliveryClockTxn owns one IMMEDIATE business transaction. Its lifetime is
// independent of the request: a canceled request cannot silently roll back a
// sampled clock floor or release a publication writer fence. Callers retain
// Store.enter until finish returns and check their request context manually.
type deliveryClockTxn struct {
	store     *Store
	tx        *sql.Tx
	observed  int64
	sampled   bool
	ended     bool
	committed bool
}

func (s *Store) beginDeliveryClockTxn(ctx context.Context) (*deliveryClockTxn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	return &deliveryClockTxn{store: s, tx: tx}, nil
}

// observe records the wall sample before any SQL that might fail. The same
// effective instant is used for credential and selection expiry checks.
func (f *deliveryClockTxn) observe(ctx context.Context) (int64, error) {
	now := f.store.now().Unix()
	if now < 0 {
		now = 0
	}
	f.record(now)
	var floor int64
	if err := f.tx.QueryRowContext(ctx, `SELECT high_water_unix FROM delivery_clock WHERE singleton=1 AND library_id=?`, f.store.library).Scan(&floor); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, corrupt("delivery clock")
		}
		return 0, err
	}
	if floor > now {
		now = floor
	}
	f.record(now)
	if _, err := f.tx.ExecContext(ctx, `UPDATE delivery_clock SET high_water_unix=? WHERE singleton=1 AND high_water_unix<?`, now, now); err != nil {
		return 0, err
	}
	return now, nil
}

func (f *deliveryClockTxn) record(now int64) {
	if !f.sampled || now > f.observed {
		f.observed = now
	}
	f.sampled = true
}

func (f *deliveryClockTxn) commit() error {
	if f.ended {
		return errors.New("delivery transaction already ended")
	}
	err := f.tx.Commit()
	f.ended = true // Commit has returned; its outcome may still be uncertain.
	f.committed = err == nil
	return err
}

// finish is deferred by every clock-bearing entry point. Failed business work
// is rolled back first; only after that original owner finishes can the
// separate max-one-connection clock checkpoint acquire a new transaction.
// Successful commits, including a committed UNCERTAIN publication outcome,
// never take this rollback branch merely because the method returns an error.
func (f *deliveryClockTxn) finish(resultErr *error) {
	if f.committed {
		return
	}
	if !f.ended {
		if err := f.tx.Rollback(); err != nil {
			f.ended = true
			*resultErr = clockUncertain(*resultErr, fmt.Errorf("business rollback: %v", err))
			return
		}
		f.ended = true
	}
	if f.sampled {
		if err := f.store.checkpointDeliveryClock(f.observed); err != nil {
			*resultErr = clockUncertain(*resultErr, err)
			return
		}
	}
	if *resultErr == nil {
		*resultErr = errors.New("delivery transaction ended without a business commit")
	}
}

// checkpointDeliveryClock is called only after the original transaction has
// ended, while the outer Store.enter remains owned. It never recursively
// enters the Store, inherits request cancellation, or commits business rows.
func (s *Store) checkpointDeliveryClock(observed int64) (resultErr error) {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("clock checkpoint acquire: %v", err)
	}
	ended := false
	defer func() {
		if !ended {
			resultErr = errors.Join(resultErr, tx.Rollback())
		}
	}()
	if err := s.checkVersion(ctx, tx); err != nil {
		return err
	}
	if err := s.checkPaths(); err != nil {
		return err
	}
	var floor int64
	if err := tx.QueryRowContext(ctx, `SELECT high_water_unix FROM delivery_clock WHERE singleton=1 AND library_id=?`, s.library).Scan(&floor); err != nil {
		return corrupt("delivery clock checkpoint")
	}
	if floor < observed {
		if err := changedOne(tx.ExecContext(ctx, `UPDATE delivery_clock SET high_water_unix=? WHERE singleton=1 AND library_id=? AND high_water_unix<?`, observed, s.library, observed)); err != nil {
			return err
		}
	}
	if err := s.checkPaths(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		ended = true
		return fmt.Errorf("clock checkpoint commit uncertain: %v", err)
	}
	ended = true
	return nil
}

// This error deliberately has no Unwrap method. A failed checkpoint cannot
// expose ErrNotFound or ErrUnauthenticated as a definitive, durable denial.
type deliveryClockUncertain struct {
	prior string
	cause string
}

func (e deliveryClockUncertain) Error() string {
	if e.prior == "" {
		return "delivery clock outcome uncertain: " + e.cause
	}
	return "delivery clock outcome uncertain: " + e.cause + "; prior outcome: " + e.prior
}

func clockUncertain(prior, cause error) error {
	bounded := func(err error) string {
		if err == nil {
			return ""
		}
		message := err.Error()
		if len(message) > 240 {
			message = message[:240]
		}
		return message
	}
	return deliveryClockUncertain{prior: bounded(prior), cause: bounded(cause)}
}
