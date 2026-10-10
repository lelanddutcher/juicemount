package metadata

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	ReplacementWaitingWriter = "waiting_writer"
	ReplacementWriterChanged = "writer_changed"
)

// ReplacementDisposition is a durable explanation for a refused, still-ready
// replacement. The publication journal and saved image remain authoritative.
type ReplacementDisposition struct {
	RowID             int64
	OperationID       string
	WitnessSHA256     string
	JournalRevision   int64
	State             string
	ObservedPrincipal string
	ObservedEpoch     uint64
}

// ReplacementRefusalError keeps the caller's ordinary conflict handling while
// exposing the confirmed reason without parsing log text.
type ReplacementRefusalError struct {
	State             string
	ObservedPrincipal string
	ObservedEpoch     uint64
}

func (e *ReplacementRefusalError) Error() string {
	return "replacement " + e.State + ": " + ErrMirrorConflict.Error()
}
func (e *ReplacementRefusalError) Unwrap() error { return ErrMirrorConflict }

func validReplacementDisposition(state, principal string, epoch uint64) bool {
	if (state != ReplacementWaitingWriter && state != ReplacementWriterChanged) || epoch == 0 || epoch > 1<<63-1 || len(principal) > 128 {
		return false
	}
	if state == ReplacementWaitingWriter && strings.TrimSpace(principal) == "" {
		return false
	}
	for _, r := range principal {
		if r < ' ' || r == 127 {
			return false
		}
	}
	return true
}

func readReplacementDisposition(q versionQuerier, id int64, p SpoolReplacementPublication, raw []byte) (*ReplacementDisposition, error) {
	var d ReplacementDisposition
	var saved []byte
	var epoch int64
	err := q.QueryRow(`SELECT spool_id,operation_id,witness_sha256,journal_revision,journal_payload,state,observed_principal,observed_epoch
 FROM spool_replacement_dispositions WHERE spool_id=?`, id).Scan(&d.RowID, &d.OperationID, &d.WitnessSHA256, &d.JournalRevision, &saved, &d.State, &d.ObservedPrincipal, &epoch)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if epoch < 1 || d.RowID != id || d.OperationID != p.OperationID || d.WitnessSHA256 != p.WitnessSHA256 ||
		d.JournalRevision != p.Revision || !bytes.Equal(saved, raw) || !validReplacementDisposition(d.State, d.ObservedPrincipal, uint64(epoch)) {
		return nil, ErrMirrorConflict
	}
	d.ObservedEpoch = uint64(epoch)
	return &d, nil
}

// ReplacementDisposition reads the bound refusal. A stale or malformed row
// fails closed; it never authorizes publication by disappearing from a join.
func (s *SpoolStore) ReplacementDisposition(id int64) (*ReplacementDisposition, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return replacementDispositionInSnapshot(tx, id)
}

func replacementDispositionInSnapshot(q versionQuerier, id int64) (*ReplacementDisposition, error) {
	p, raw, _, err := readReplacementPublication(q, id)
	if err != nil || p == nil {
		return nil, errors.Join(ErrMirrorConflict, err)
	}
	if p.Phase == ReplacementPublicationCommitted || p.Phase == ReplacementPublicationSettled || p.Phase == ReplacementPublicationCleaning || p.Phase == ReplacementPublicationCleaned {
		return nil, nil
	}
	return readReplacementDisposition(q, id, *p, raw)
}

// ReplacementDispositionsForStatus returns only relevant listed rows. One
// indexed scan locates refusals; exact journal/witness validation is paid only
// for those rows, rather than for every ordinary spool entry at each poll.
func (s *SpoolStore) ReplacementDispositionsForStatus(list []*SpoolRow) (map[int64]*ReplacementDisposition, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	out := make(map[int64]*ReplacementDisposition)
	listed := make(map[int64]bool, len(list))
	for _, row := range list {
		listed[row.ID] = true
	}
	rows, err := tx.Query(`SELECT d.spool_id FROM spool_replacement_dispositions d
 JOIN spool_entries s ON s.id=d.spool_id JOIN spool_replacement_publications p ON p.spool_id=d.spool_id
 WHERE s.drain_state='ready' AND s.retired_by=0 AND p.phase IN ('queued','intent','arming','armed')`)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if listed[id] {
			ids = append(ids, id)
		}
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	for _, id := range ids {
		d, err := replacementDispositionInSnapshot(tx, id)
		if err != nil || d == nil {
			return nil, errors.Join(ErrMirrorConflict, err)
		}
		out[id] = d
	}
	return out, nil
}

// RecordReplacementDisposition is a SQLite CAS on the exact witness and raw
// journal revision. Only a confirmed lease reply may call it. It does not
// change the accepted image, journal, row state, attempt count or charge.
func (s *SpoolStore) RecordReplacementDisposition(expected SpoolReplacementPublication, state, principal string, epoch uint64, project string) (retErr error) {
	if !validReplacementDisposition(state, principal, epoch) || (state == ReplacementWaitingWriter && project == "") || expected.SourceRowID < 1 {
		return ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	owner := s.Owner()
	if owner == nil {
		return ErrMirrorConflict
	}
	tx, release, err := owner.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return err
	}
	discard, commitStarted := false, false
	defer func() { release(discard) }()
	defer func() {
		if !commitStarted {
			if err := tx.Rollback(); err != nil {
				discard = true
				retErr = &PendingSpoolReplacement{Cause: errors.Join(retErr, err)}
			}
		}
	}()
	p, raw, row, err := readReplacementPublication(tx, expected.SourceRowID)
	if err != nil || p == nil || *p != expected || row.state != string(DrainReady) || row.retired != 0 ||
		(p.Phase != ReplacementPublicationIntent && p.Phase != ReplacementPublicationArming && p.Phase != ReplacementPublicationArmed) {
		return errors.Join(ErrMirrorConflict, err)
	}
	if state == ReplacementWriterChanged && (row.epoch < 0 || epoch <= uint64(row.epoch)) {
		return ErrMirrorConflict
	}
	prior, err := readReplacementDisposition(tx, expected.SourceRowID, *p, raw)
	if err != nil {
		return err
	}
	if prior != nil && prior.State == ReplacementWriterChanged {
		if state == ReplacementWriterChanged {
			return nil
		}
		return ErrMirrorConflict
	}
	if prior != nil && prior.ObservedEpoch > epoch {
		return ErrMirrorConflict
	}
	wait := ""
	message := "Your save is kept on this Mac. Another Mac held the project after this save was accepted, so the server file was not replaced. Save a copy to a local folder to keep working."
	if state == ReplacementWaitingWriter {
		wait = project
		message = "Your save is kept on this Mac while another Mac holds the project. It will be checked again when the project is available."
	}
	if _, err := tx.Exec(`INSERT INTO spool_replacement_dispositions
 (spool_id,operation_id,witness_sha256,journal_revision,journal_payload,state,observed_principal,observed_epoch,updated_at)
 VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(spool_id) DO UPDATE SET state=excluded.state,
 observed_principal=excluded.observed_principal,observed_epoch=excluded.observed_epoch,updated_at=excluded.updated_at`,
		expected.SourceRowID, expected.OperationID, expected.WitnessSHA256, expected.Revision, raw, state, principal, epoch, time.Now().Unix()); err != nil {
		return err
	}
	res, err := tx.Exec(`UPDATE spool_entries SET wait_project=?,last_error=?,updated_at=? WHERE id=? AND drain_state='ready'
 AND retired_by=0 AND replacement_witness IS NOT NULL AND replacement_publication_required=1`,
		wait, message, time.Now().Unix(), expected.SourceRowID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return errors.Join(ErrMirrorConflict, err)
	}
	commitStarted = true
	if err := tx.Commit(); err != nil {
		discard = true
		return &PendingSpoolReplacement{Cause: fmt.Errorf("replacement disposition commit: %w", err)}
	}
	return nil
}
