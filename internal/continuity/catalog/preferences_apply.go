package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func (d *DiscoveryService) enterPreferences(parent context.Context) (context.Context, func(), error) {
	ctx, leave, err := d.admit(parent)
	if err != nil {
		return nil, nil, err
	}
	if err := d.store.enter(); err != nil {
		leave()
		return nil, nil, err
	}
	if d.store.version != 4 {
		d.store.leave()
		leave()
		return nil, nil, ErrUpgradeRequired
	}
	d.mu.Lock()
	cost := discoveryWorkBytes + discoveryCopyBytes
	if d.bytes+cost > discoveryQuota {
		d.mu.Unlock()
		d.store.leave()
		leave()
		return nil, nil, errDiscoveryBusy
	}
	d.bytes += cost
	d.mu.Unlock()
	select {
	case d.sql <- struct{}{}:
	default:
		d.mu.Lock()
		d.bytes -= cost
		d.mu.Unlock()
		d.store.leave()
		leave()
		return nil, nil, errDiscoveryBusy
	}
	return ctx, func() {
		<-d.sql
		d.mu.Lock()
		d.bytes -= cost
		d.mu.Unlock()
		d.store.leave()
		leave()
	}, nil
}

func (d *DiscoveryService) authorizePreference(ctx context.Context, q queryer, bearer []byte, asset continuity.AssetID) (discoveryBinding, credential, error) {
	h, c, subject, err := d.store.authenticateSnapshot(ctx, q, d.library, bearer)
	if err != nil {
		return discoveryBinding{}, c, err
	}
	b := discoveryBinding{h, subject.AuthorizationGeneration, c.issued, c.expires, nil}
	// Person identity is an independent guard, not inferred from an action set.
	if subject.Value.Kind != "person" {
		return b, c, ErrNotFound
	}
	rights, err := readDiscoveryRights(ctx, q, d.library, c)
	if err != nil {
		return b, c, err
	}
	if !rights.allows(asset, "assets.discover") || !rights.allows(asset, "activity.record") {
		return b, c, ErrNotFound
	}
	_, err = readAsset(ctx, q, d.library, asset)
	return b, c, err
}

func (s *Store) finishPreferenceTx(ctx context.Context, tx *sql.Tx, c *credential, expiry int64) error {
	control, err := readPreferenceControl(ctx, tx, s.library)
	if err != nil {
		return err
	}
	now := s.now().Unix()
	if err := preferenceClock(now, control.floor); err != nil {
		return err
	}
	if c != nil && (now < c.issued || now >= c.expires) {
		return ErrUnauthenticated
	}
	if expiry != 0 {
		if err := preferenceRequestTime(now, expiry, false); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE preference_control SET clock_floor_unix=? WHERE singleton=1`, now); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("preference commit outcome uncertain: %w", err)
	}
	return nil
}

func readPreferenceRetry(ctx context.Context, q queryer, subject SubjectID, r PreferenceRequest, request []byte) ([]byte, bool, error) {
	var original, receipt []byte
	var digest string
	var expiry int64
	err := q.QueryRowContext(ctx, `SELECT CASE WHEN length(request)<=1024 THEN request END,payload_sha256,CASE WHEN length(receipt)<=512 THEN receipt END,expires_unix FROM preference_receipts WHERE subject_id=? AND operation_id=?`, subject, r.OperationID).Scan(&original, &digest, &receipt, &expiry)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	parsed, err := decodePreferenceRequest(original)
	if err != nil {
		return nil, false, err
	}
	if parsed.LibraryID != r.LibraryID || parsed.OperationID != r.OperationID || parsed.ExpiresUnix != expiry || payloadDigest(original) != digest {
		return nil, false, corrupt("preference retry identity")
	}
	if !bytes.Equal(original, request) {
		return nil, false, ErrChangedRequest
	}
	if _, err := decodePreferenceReceipt(receipt, r); err != nil {
		return nil, false, err
	}
	return receipt, true, nil
}

func prunePreferenceRows(ctx context.Context, tx *sql.Tx, subject SubjectID, ownEntries, ownReceipts, globalEntries, globalReceipts int, now int64) error {
	for _, v := range []struct {
		table, index, key string
		own, global       int
	}{{"preferences", "preferences_expiry", "asset_id", ownEntries, globalEntries}, {"preference_receipts", "preference_receipts_expiry", "operation_id", ownReceipts, globalReceipts}} {
		for _, own := range []bool{true, false} {
			limit := v.global
			if own {
				limit = v.own
			}
			if limit == 0 {
				continue
			}
			query := `SELECT subject_id,` + v.key + ` FROM ` + v.table + ` INDEXED BY ` + v.index + ` WHERE expires_unix<=? ORDER BY expires_unix,subject_id,` + v.key + ` LIMIT ?`
			args := []any{now, limit}
			if own {
				query = `SELECT subject_id,` + v.key + ` FROM ` + v.table + ` WHERE subject_id=? AND expires_unix<=? LIMIT ?`
				args = []any{subject, now, limit}
			}
			type key struct {
				subject SubjectID
				id      string
			}
			keys := make([]key, 0, limit)
			if err := walkRows(ctx, tx, query, func(rows *sql.Rows) error {
				var k key
				if err := rows.Scan(&k.subject, &k.id); err != nil {
					return err
				}
				keys = append(keys, k)
				return nil
			}, args...); err != nil {
				return err
			}
			for _, k := range keys {
				if _, err := tx.ExecContext(ctx, `DELETE FROM `+v.table+` WHERE subject_id=? AND `+v.key+`=? AND expires_unix<=?`, k.subject, k.id, now); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (o *LocalOwnerService) PrunePreferences(ctx context.Context) (resultErr error) {
	if err := o.enter(); err != nil {
		return err
	}
	defer o.store.leave()
	s := o.store
	if s.version != 4 {
		return ErrUpgradeRequired
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		err := tx.Rollback()
		if !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	c, err := readPreferenceControl(ctx, tx, s.library)
	if err != nil {
		return err
	}
	now := s.now().Unix()
	if err := preferenceClock(now, c.floor); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE preference_control SET clock_floor_unix=? WHERE singleton=1`, now); err != nil {
		return err
	}
	if err := prunePreferenceRows(ctx, tx, "", 0, 0, 64, 64, now); err != nil {
		return err
	}
	return s.finishPreferenceTx(ctx, tx, nil, 0)
}

func (d *DiscoveryService) RecordPreference(parent context.Context, bearer []byte, input PreferenceRequest, output io.Writer) (result DisclosureResult, resultErr error) {
	if d == nil || output == nil {
		return result, invalid("preference service or writer")
	}
	ctx, leave, err := d.enterPreferences(parent)
	if err != nil {
		return result, err
	}
	defer leave()
	r, request, err := canonicalPreferenceRequest(input)
	if err != nil {
		return result, err
	}
	if r.LibraryID != d.library {
		return result, invalid("preference library")
	}
	var binding discoveryBinding
	var body []byte
	for phase := 0; phase < 2; phase++ {
		tx, err := d.store.db.BeginTx(ctx, nil)
		if err != nil {
			return result, err
		}
		// Every exit owns this immediate transaction; maintenance stays inside the
		// same transaction that authenticated and authorized its caller.
		body, binding, err = d.preferencePhase(ctx, tx, bearer, r, request, phase == 0)
		rollback := tx.Rollback()
		if errors.Is(rollback, sql.ErrTxDone) {
			rollback = nil
		}
		err = errors.Join(err, rollback)
		if binding.handle != nil {
			result.SubjectID, result.Selector = binding.handle.subject, binding.handle.selector
		}
		if err != nil {
			return result, err
		}
		if body != nil {
			break
		}
	}
	if body == nil {
		return result, corrupt("missing preference result")
	}
	if d.beforeFence != nil {
		if err := d.beforeFence(ctx); err != nil {
			return result, err
		}
	}
	if err := d.fence(ctx, binding); err != nil {
		return result, err
	}
	now := d.store.now().Unix()
	if now < binding.issued || now >= binding.expires {
		return result, ErrUnauthenticated
	}
	if err := binding.preference.checkTime(now); err != nil {
		return result, err
	}
	if err := preferenceRequestTime(now, r.ExpiresUnix, false); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.Started = true
	n, err := output.Write(body)
	if err != nil {
		return result, err
	}
	if n != len(body) {
		return result, io.ErrShortWrite
	}
	return result, nil
}

func (d *DiscoveryService) preferencePhase(ctx context.Context, tx *sql.Tx, bearer []byte, r PreferenceRequest, request []byte, maintenance bool) ([]byte, discoveryBinding, error) {
	b, c, err := d.authorizePreference(ctx, tx, bearer, r.AssetID)
	if err != nil {
		return nil, b, err
	}
	control, err := readPreferenceControl(ctx, tx, d.library)
	if err != nil {
		return nil, b, err
	}
	now := d.store.now().Unix()
	if err := preferenceClock(now, control.floor); err != nil {
		return nil, b, err
	}
	if err := preferenceRequestTime(now, r.ExpiresUnix, false); err != nil {
		return nil, b, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE preference_control SET clock_floor_unix=? WHERE singleton=1`, now); err != nil {
		return nil, b, err
	}
	// Acknowledgements fence time/floor but deliberately not mutable preference
	// generation: the receipt says operation_result, not current selection.
	b.preference = &preferenceBinding{generation: -1, lower: now, floor: control.floor}
	if receipt, found, err := readPreferenceRetry(ctx, tx, c.subject, r, request); err != nil || found {
		if err != nil {
			return nil, b, err
		}
		if err := d.store.finishPreferenceTx(ctx, tx, &c, r.ExpiresUnix); err != nil {
			return nil, b, err
		}
		return receipt, b, nil
	}
	if err := preferenceRequestTime(now, r.ExpiresUnix, true); err != nil {
		return nil, b, err
	}
	if maintenance {
		if _, err := tx.ExecContext(ctx, `UPDATE preference_control SET clock_floor_unix=? WHERE singleton=1`, now); err != nil {
			return nil, b, err
		}
		if err := prunePreferenceRows(ctx, tx, c.subject, 256, 128, 64, 64, now); err != nil {
			return nil, b, err
		}
		return nil, b, d.store.finishPreferenceTx(ctx, tx, &c, r.ExpiresUnix)
	}
	subject, err := readPreferenceSubject(ctx, tx, d.library, c.subject)
	if err != nil {
		return nil, b, err
	}
	old, oldErr := readPreference(ctx, tx, c.subject, r.AssetID)
	if oldErr != nil && !errors.Is(oldErr, ErrNotFound) {
		return nil, b, oldErr
	}
	exists := oldErr == nil
	if r.Kind == "clear" && (!exists || now >= old.expires || old.token != *r.ExpectedToken) {
		return nil, b, ErrConflict
	}
	if subject.generation == math.MaxInt64 {
		return nil, b, ErrLimit
	}
	if subject.receipts >= 128 || control.receipts >= preferenceReceipts || subject.generation == 0 && control.subjects >= preferenceSubjects || r.Kind == "select" && !exists && (subject.entries >= 256 || control.entries >= preferenceEntries) {
		return nil, b, errDiscoveryBusy
	}
	if subject.generation == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO preference_subjects(subject_id,library_id,generation,entry_count,receipt_count) VALUES(?,?,1,0,0)`, c.subject, d.library)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE preference_subjects SET generation=generation+1 WHERE subject_id=?`, c.subject)
	}
	if err != nil {
		return nil, b, err
	}
	var token *string
	if r.Kind == "select" {
		if now > math.MaxInt64-preferenceAge {
			return nil, b, ErrLimit
		}
		for attempt := 0; attempt < 8; attempt++ {
			candidate, err := randomSelectionToken()
			if err != nil {
				return nil, b, err
			}
			if exists && candidate == old.token {
				continue
			}
			var used int
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM preferences WHERE selection_token=?)`, candidate).Scan(&used); err != nil {
				return nil, b, err
			}
			if used == 0 {
				token = &candidate
				break
			}
		}
		if token == nil {
			return nil, b, errDiscoveryBusy
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO preferences(subject_id,asset_id,selection_token,person_order,selected_unix,expires_unix) VALUES(?,?,?,?,?,?) ON CONFLICT(subject_id,asset_id) DO UPDATE SET selection_token=excluded.selection_token,person_order=excluded.person_order,selected_unix=excluded.selected_unix,expires_unix=excluded.expires_unix`, c.subject, r.AssetID, *token, subject.generation+1, now, now+preferenceAge)
	} else {
		_, err = tx.ExecContext(ctx, `DELETE FROM preferences WHERE subject_id=? AND asset_id=? AND selection_token=?`, c.subject, r.AssetID, *r.ExpectedToken)
	}
	if err != nil {
		return nil, b, err
	}
	receipt, err := encodeDiscoveryValue(preferenceReceipt{2, r.OperationID, r.Kind, token, "operation_result"}, 512)
	if err != nil {
		return nil, b, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO preference_receipts(subject_id,operation_id,request,payload_sha256,receipt,expires_unix) VALUES(?,?,?,?,?,?)`, c.subject, r.OperationID, request, payloadDigest(request), receipt, r.ExpiresUnix); err != nil {
		return nil, b, err
	}
	if err := d.store.finishPreferenceTx(ctx, tx, &c, r.ExpiresUnix); err != nil {
		return nil, b, err
	}
	return receipt, b, nil
}
