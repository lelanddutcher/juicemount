package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const terminalSlotColumns = `library_id,target_kind,COALESCE(source_id,''),COALESCE(source_version,0),COALESCE(selection_id,''),terminal_bit,legacy_terminal,COALESCE(operation_id,''),COALESCE(actor_subject_id,''),COALESCE(authority_incarnation,''),COALESCE(recorded_unix_nano,0),CASE WHEN length(request) BETWEEN 1 AND 1024 THEN request END,COALESCE(payload_sha256,''),CASE WHEN length(receipt) BETWEEN 1 AND 2048 THEN receipt END`

type terminalSlot struct {
	library                     continuity.LibraryID
	kind, sourceID, selectionID string
	sourceVersion               int64
	bit, legacy                 bool
	receipt                     TerminalReceipt
	request, receiptBytes       []byte
}

func scanTerminalSlot(row interface{ Scan(...any) error }, library continuity.LibraryID) (terminalSlot, error) {
	var s terminalSlot
	var r TerminalReceipt
	err := row.Scan(&s.library, &s.kind, &s.sourceID, &s.sourceVersion, &s.selectionID, &s.bit, &s.legacy, &r.OperationID, &r.ActorSubjectID, &r.AuthorityIncarnation, &r.RecordedUnixNano, &s.request, &r.PayloadSHA256, &s.receiptBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return s, corrupt("missing terminal reservation")
	}
	if err != nil {
		return s, corrupt("terminal reservation fields")
	}
	if s.library != library || !(s.kind == "source_disable" && validRandomID(s.sourceID, "dsrc_") && s.sourceVersion > 0 && s.selectionID == "" || s.kind == "selection_cancel" && validRandomID(s.selectionID, "dsel_") && s.sourceID == "" && s.sourceVersion == 0) {
		return s, corrupt("terminal target")
	}
	if r.OperationID == "" {
		if s.bit != s.legacy || len(s.request) != 0 || len(s.receiptBytes) != 0 || r.ActorSubjectID != "" || r.AuthorityIncarnation != "" || r.RecordedUnixNano != 0 || r.PayloadSHA256 != "" {
			return s, corrupt("empty terminal reservation")
		}
		return s, nil
	}
	if !s.bit || s.legacy {
		return s, corrupt("terminal state")
	}
	r.Version, r.LibraryID, r.Kind, r.SourceID, r.SourceVersion, r.SelectionID = 1, library, s.kind, s.sourceID, s.sourceVersion, s.selectionID
	actual, err := DecodeTerminalReceipt(s.receiptBytes)
	if err != nil || actual != r {
		return s, corrupt("terminal receipt binding")
	}
	switch s.kind {
	case "source_disable":
		op, err := DecodeSourceDisableRequest(s.request)
		if err != nil {
			return s, corrupt("terminal source request")
		}
		_, digest, err := CanonicalSourceDisableRequest(op)
		if err != nil || op.LibraryID != library || op.ID != r.OperationID || op.SourceID != s.sourceID || op.ExpectedSourceVersion != s.sourceVersion || op.ExpectedIncarnation != r.AuthorityIncarnation || digest != r.PayloadSHA256 {
			return s, corrupt("terminal source binding")
		}
	case "selection_cancel":
		op, err := DecodeDeliveryCancelRequest(s.request)
		if err != nil {
			return s, corrupt("terminal selection request")
		}
		_, digest, err := CanonicalDeliveryCancelRequest(op)
		if err != nil || op.LibraryID != library || op.ID != r.OperationID || op.SelectionID != s.selectionID || digest != r.PayloadSHA256 {
			return s, corrupt("terminal selection binding")
		}
	}
	s.receipt = r
	return s, nil
}
func readSourceTerminal(ctx context.Context, q queryer, library continuity.LibraryID, id string, version int64) (terminalSlot, error) {
	return scanTerminalSlot(q.QueryRowContext(ctx, `SELECT `+terminalSlotColumns+` FROM delivery_terminal_slots WHERE source_id=? AND source_version=?`, id, version), library)
}
func readSelectionTerminal(ctx context.Context, q queryer, library continuity.LibraryID, id string, canceled bool) (terminalSlot, error) {
	s, err := scanTerminalSlot(q.QueryRowContext(ctx, `SELECT `+terminalSlotColumns+` FROM delivery_terminal_slots WHERE selection_id=?`, id), library)
	if err != nil {
		return s, err
	}
	if s.bit != canceled {
		return s, corrupt("selection terminal coupling")
	}
	if s.receipt.OperationID != "" {
		var actor SubjectID
		if err := q.QueryRowContext(ctx, `SELECT subject_id FROM delivery_selections WHERE selection_id=? AND library_id=?`, id, library).Scan(&actor); err != nil {
			return s, corrupt("terminal selection actor")
		}
		if actor != s.receipt.ActorSubjectID {
			return s, corrupt("terminal selection actor")
		}
	}
	return s, nil
}
func terminalPrior(ctx context.Context, q queryer, library continuity.LibraryID, kind, id string, actor SubjectID, request []byte) (TerminalReceipt, bool, error) {
	query := `SELECT ` + terminalSlotColumns + ` FROM delivery_terminal_slots WHERE library_id=? AND target_kind=? AND operation_id=?`
	args := []any{library, kind, id}
	if kind == "selection_cancel" {
		query += ` AND actor_subject_id=?`
		args = append(args, actor)
	}
	s, err := scanTerminalSlot(q.QueryRowContext(ctx, query, args...), library)
	if err != nil {
		// Missing target slots are corruption; missing operation is normal. Do
		// not confuse the two by weakening the shared target reader.
		var exists bool
		check := `SELECT EXISTS(SELECT 1 FROM delivery_terminal_slots WHERE library_id=? AND target_kind=? AND operation_id=?`
		if kind == "selection_cancel" {
			check += ` AND actor_subject_id=?`
		}
		check += `)`
		if e := q.QueryRowContext(ctx, check, args...).Scan(&exists); e != nil {
			return TerminalReceipt{}, false, e
		}
		if !exists {
			return TerminalReceipt{}, false, nil
		}
		return TerminalReceipt{}, false, err
	}
	if !bytes.Equal(s.request, request) {
		return TerminalReceipt{}, false, ErrChangedRequest
	}
	return s.receipt, true, nil
}
func fillTerminal(ctx context.Context, tx *sql.Tx, r TerminalReceipt, request []byte) error {
	receipt, err := CanonicalTerminalReceipt(r)
	if err != nil {
		return err
	}
	query := `UPDATE delivery_terminal_slots SET terminal_bit=1,operation_id=?,actor_subject_id=?,authority_incarnation=?,recorded_unix_nano=?,request=?,payload_sha256=?,receipt=? WHERE library_id=? AND target_kind=? AND operation_id IS NULL AND terminal_bit=0 AND legacy_terminal=0 AND `
	args := []any{r.OperationID, r.ActorSubjectID, r.AuthorityIncarnation, r.RecordedUnixNano, request, r.PayloadSHA256, receipt, r.LibraryID, r.Kind}
	if r.Kind == "source_disable" {
		query += `source_id=? AND source_version=?`
		args = append(args, r.SourceID, r.SourceVersion)
	} else {
		query += `selection_id=?`
		args = append(args, r.SelectionID)
	}
	return changedOne(tx.ExecContext(ctx, query, args...))
}

// Current privileged OS owner access never comes from a historical receipt.
func (o *LocalOwnerService) currentDeliveryOwner(ctx context.Context, q queryer) (AuthorityStatus, error) {
	if err := o.store.checkVersion(ctx, q); err != nil {
		return AuthorityStatus{}, err
	}
	a, err := readAuthority(ctx, q, o.library)
	if err != nil {
		return a, err
	}
	if a.Incarnation == "" {
		return a, ErrConflict
	}
	return a, nil // readAuthority checks enabled-person owner in this library.
}
func sourceStatusSnapshot(ctx context.Context, q queryer, s *Store, id string, version int64) (SourceStatus, error) {
	result := SourceStatus{Version: 1, LibraryID: s.library, SourceID: id, SourceVersion: version, Terminal: "none"}
	var library continuity.LibraryID
	var enabled bool
	var incarnation string
	err := q.QueryRowContext(ctx, `SELECT d.library_id,d.asset_id,d.revision_id,d.content_size,d.content_sha256,d.enabled,d.authority_incarnation,COALESCE(h.source_version,0) FROM delivery_sources d LEFT JOIN delivery_source_heads h ON h.source_id=d.source_id AND h.library_id=d.library_id AND h.asset_id=d.asset_id AND h.revision_id=d.revision_id WHERE d.source_id=? AND d.source_version=?`, id, version).Scan(&library, &result.AssetID, &result.RevisionID, &result.Content.Size, &result.Content.SHA256, &enabled, &incarnation, &result.CurrentSourceVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return result, ErrNotFound
	}
	if err != nil {
		return result, err
	}
	if library != s.library || result.AssetID.Validate() != nil || result.RevisionID.Validate() != nil || result.Content.Validate() != nil || !validRandomID(incarnation, "auth_") || result.CurrentSourceVersion < 1 {
		return result, corrupt("source status binding")
	}
	terminal := false
	if supportsDeliveryLifecycle(s.version) {
		slot, err := readSourceTerminal(ctx, q, s.library, id, version)
		if err != nil {
			return result, err
		}
		if slot.legacy == enabled {
			return result, corrupt("source terminal coupling")
		}
		terminal = slot.bit
		if slot.legacy {
			result.Terminal = "legacy"
		} else if slot.bit {
			result.Terminal = "committed"
		}
	} else if !enabled {
		result.Terminal = "legacy"
	}
	a, err := readAuthority(ctx, q, s.library)
	if err != nil {
		return result, err
	}
	switch {
	case !enabled || terminal:
		result.Eligibility = "disabled"
	case result.CurrentSourceVersion != version:
		result.Eligibility = "superseded"
	case a.Incarnation == "" || a.Incarnation != incarnation:
		result.Eligibility = "authority_changed"
	default:
		result.Enabled = true
		result.Eligibility = "eligible"
	}
	return result, nil
}
func (o *LocalOwnerService) DeliverySourceStatus(ctx context.Context, request SourceStatusRequest) (result SourceStatus, resultErr error) {
	if err := o.enter(); err != nil {
		return result, err
	}
	defer o.store.leave()
	if !supportsDelivery(o.store.version) {
		return result, ErrUpgradeRequired
	}
	if _, err := CanonicalSourceStatusRequest(request); err != nil {
		return result, err
	}
	if request.LibraryID != o.library {
		return result, ErrNotFound
	}
	tx, done, err := o.store.readSnapshot(ctx)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	if _, err := o.currentDeliveryOwner(ctx, tx); err != nil {
		return result, err
	}
	return sourceStatusSnapshot(ctx, tx, o.store, request.SourceID, request.SourceVersion)
}
func (o *LocalOwnerService) DisableDeliverySource(ctx context.Context, op SourceDisableRequest) (result SourceDisableResult, resultErr error) {
	if err := o.enter(); err != nil {
		return result, err
	}
	defer o.store.leave()
	s := o.store
	if !supportsDeliveryLifecycle(s.version) {
		return result, ErrUpgradeRequired
	}
	request, digest, err := CanonicalSourceDisableRequest(op)
	if err != nil {
		return result, err
	}
	if op.LibraryID != o.library {
		return result, ErrNotFound
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	a, err := o.currentDeliveryOwner(ctx, tx)
	if err != nil {
		return result, err
	}
	prior, found, err := terminalPrior(ctx, tx, s.library, "source_disable", op.ID, "", request)
	if err != nil {
		return result, err
	}
	status, err := sourceStatusSnapshot(ctx, tx, s, op.SourceID, op.ExpectedSourceVersion)
	if err != nil {
		return result, err
	}
	if found {
		result = SourceDisableResult{prior, status}
	} else {
		if op.ExpectedIncarnation != a.Incarnation || status.CurrentSourceVersion != op.ExpectedSourceVersion {
			return result, ErrConflict
		}
		slot, err := readSourceTerminal(ctx, tx, s.library, op.SourceID, op.ExpectedSourceVersion)
		if err != nil {
			return result, err
		}
		if slot.bit {
			return result, ErrConflict
		}
		r := TerminalReceipt{Version: 1, LibraryID: s.library, OperationID: op.ID, Kind: "source_disable", PayloadSHA256: digest, ActorSubjectID: a.OwnerSubjectID, AuthorityIncarnation: a.Incarnation, RecordedUnixNano: s.now().UnixNano(), SourceID: op.SourceID, SourceVersion: op.ExpectedSourceVersion}
		if err := fillTerminal(ctx, tx, r, request); err != nil {
			return result, err
		}
		status, err = sourceStatusSnapshot(ctx, tx, s, op.SourceID, op.ExpectedSourceVersion)
		if err != nil {
			return result, err
		}
		result = SourceDisableResult{r, status}
	}
	if err := ctx.Err(); err != nil {
		return SourceDisableResult{}, err
	}
	if err := s.checkPaths(); err != nil {
		return SourceDisableResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourceDisableResult{}, terminalCommitError("source disable", err)
	}
	return result, nil
}
