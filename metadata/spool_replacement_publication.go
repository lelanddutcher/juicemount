package metadata

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"

	"github.com/juicedata/juicefs/pkg/replaceguard"
)

const (
	ReplacementPublicationQueued     = "queued"
	ReplacementPublicationIntent     = "intent"
	ReplacementPublicationArming     = "arming"
	ReplacementPublicationArmed      = "armed"
	ReplacementPublicationCommitted  = "committed"
	ReplacementPublicationSettled    = "settled"
	ReplacementPublicationCleaning   = "cleaning"
	ReplacementPublicationCleaned    = "cleaned"
	ReplacementPublicationSuperseded = "superseded"
	maxReplacementPublicationBytes   = 128 << 10
)

// SpoolReplacementPublication is durable local intent only. Its queued phase
// names no backend stage and does not authorize a remote write or cleanup.
type SpoolReplacementPublication struct {
	Version        uint32      `json:"version"`
	OperationID    string      `json:"operation_id"`
	SourceRowID    int64       `json:"source_row_id"`
	WitnessSHA256  string      `json:"witness_sha256"`
	SourceFile     string      `json:"source_file"`
	SourceIdentity string      `json:"source_identity"`
	Expected       MirrorToken `json:"expected"`
	Phase          string      `json:"phase"`
	StagePath      string      `json:"stage_path"`
	Revision       int64       `json:"revision"`
	Record         string      `json:"record,omitempty"`
	Settlement     string      `json:"settlement,omitempty"`
}

func replacementStagePath(destination, operation string) string {
	return path.Join(path.Dir(destination), ".juicemount-streaming-drain-replacement-"+operation)
}

func (p SpoolReplacementPublication) validate() error {
	if p.Version != 1 || !validCustodyID(p.OperationID) || p.SourceRowID <= 0 ||
		len(p.WitnessSHA256) != 64 || !validPrivatePath(p.SourceFile) || !validIdentity(p.SourceIdentity) ||
		!validCustodyPath(p.Expected.Path) || p.Expected.Incarnation == "" || p.Expected.Revision <= 0 ||
		p.Expected.StoreID != "" || p.Expected.Epoch != 0 {
		return ErrMirrorConflict
	}
	if raw, err := hex.DecodeString(p.WitnessSHA256); err != nil || len(raw) != sha256.Size || hex.EncodeToString(raw) != p.WitnessSHA256 {
		return ErrMirrorConflict
	}
	switch p.Phase {
	case ReplacementPublicationQueued:
		if p.Revision != 1 || p.StagePath != "" || p.Record != "" || p.Settlement != "" {
			return ErrMirrorConflict
		}
	case ReplacementPublicationIntent:
		if p.Revision != 2 || p.StagePath != replacementStagePath(p.Expected.Path, p.OperationID) || p.Record != "" || p.Settlement != "" {
			return ErrMirrorConflict
		}
	case ReplacementPublicationArming, ReplacementPublicationArmed, ReplacementPublicationCommitted, ReplacementPublicationSettled, ReplacementPublicationCleaning, ReplacementPublicationCleaned:
		if p.Revision < 3 || p.StagePath != replacementStagePath(p.Expected.Path, p.OperationID) || p.Record == "" {
			return ErrMirrorConflict
		}
		r, err := replaceguard.Decode([]byte(p.Record))
		if err != nil || r.OperationID != p.OperationID ||
			r.Stage.Name != path.Base(p.StagePath) || r.Destination.Name != path.Base(p.Expected.Path) ||
			((p.Phase == ReplacementPublicationArming || p.Phase == ReplacementPublicationArmed) != (r.Phase == "prepared")) {
			return ErrMirrorConflict
		}
		if p.Phase == ReplacementPublicationArming || p.Phase == ReplacementPublicationArmed || p.Phase == ReplacementPublicationCommitted {
			if p.Settlement != "" {
				return ErrMirrorConflict
			}
		} else if _, err := p.SettlementRecord(); err != nil {
			return err
		}
	case ReplacementPublicationSuperseded:
		if p.Revision != 2 || p.StagePath != "" || p.Record != "" || p.Settlement != "" {
			return ErrMirrorConflict
		}
	default:
		return ErrMirrorConflict
	}
	return nil
}

func encodeReplacementPublication(p SpoolReplacementPublication) ([]byte, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(p)
	if err != nil || len(b) > maxReplacementPublicationBytes {
		return nil, ErrMirrorConflict
	}
	return b, nil
}

func decodeReplacementPublication(raw []byte) (SpoolReplacementPublication, error) {
	var p SpoolReplacementPublication
	if len(raw) == 0 || len(raw) > maxReplacementPublicationBytes || json.Unmarshal(raw, &p) != nil {
		return p, ErrMirrorConflict
	}
	canon, err := encodeReplacementPublication(p)
	if err != nil || !bytes.Equal(canon, raw) {
		return p, ErrMirrorConflict
	}
	return p, nil
}

type replacementPublicationRow struct {
	path, file, state, clone, qualification string
	size, punched, retired, epoch           int64
	sha, witness                            []byte
	required                                int
}

func readReplacementPublication(q versionQuerier, id int64) (*SpoolReplacementPublication, []byte, replacementPublicationRow, error) {
	var row replacementPublicationRow
	err := q.QueryRow(`SELECT nfs_path,spool_file,drain_state,size,sha256,punched_end,retired_by,accepted_epoch,clone_hold,qualification_hold,replacement_witness,replacement_publication_required FROM spool_entries WHERE id=?`, id).
		Scan(&row.path, &row.file, &row.state, &row.size, &row.sha, &row.punched, &row.retired, &row.epoch, &row.clone, &row.qualification, &row.witness, &row.required)
	if err != nil {
		return nil, nil, row, err
	}
	var operation, phase string
	var raw []byte
	err = q.QueryRow(`SELECT operation_id,phase,payload FROM spool_replacement_publications WHERE spool_id=?`, id).Scan(&operation, &phase, &raw)
	if err == sql.ErrNoRows {
		if row.required == 0 {
			return nil, nil, row, nil
		}
		return nil, nil, row, ErrMirrorConflict
	}
	if err != nil || row.required != 1 {
		return nil, nil, row, ErrMirrorConflict
	}
	p, err := decodeReplacementPublication(raw)
	if err != nil || p.OperationID != operation || p.Phase != phase || p.SourceRowID != id || p.SourceFile != row.file || p.Expected.Path != row.path {
		return nil, nil, row, ErrMirrorConflict
	}
	w, err := replacementWitnessByID(q, id)
	if err != nil || w == nil || row.epoch < 0 || uint64(row.epoch) != w.AcceptedEpoch ||
		row.size != w.Image.Size || !bytes.Equal(row.sha, w.Image.SHA256) ||
		p.OperationID != w.OperationID || p.SourceIdentity != w.SourceIdentity ||
		p.Expected.Incarnation != w.Source.Incarnation || p.Expected.Revision != w.Source.Revision+1 {
		return nil, nil, row, ErrMirrorConflict
	}
	digest := sha256.Sum256(row.witness)
	if p.WitnessSHA256 != hex.EncodeToString(digest[:]) || row.punched != 0 || row.clone != "" || row.qualification != "" {
		return nil, nil, row, ErrMirrorConflict
	}
	if p.Record != "" && validateReplacementRecord(p, *w) != nil {
		return nil, nil, row, ErrMirrorConflict
	}
	switch p.Phase {
	case ReplacementPublicationSuperseded:
		if row.retired == 0 {
			return nil, nil, row, ErrMirrorConflict
		}
	case ReplacementPublicationSettled, ReplacementPublicationCleaning:
		if row.retired != 0 || row.state != string(DrainDone) || validateReplacementSettlementEvidence(p, *w) != nil {
			return nil, nil, row, ErrMirrorConflict
		}
	case ReplacementPublicationCleaned:
		// Historical receipt: later edits may replace or remove the mirror.
		if validateReplacementSettlementEvidence(p, *w) != nil {
			return nil, nil, row, ErrMirrorConflict
		}
		if row.state != string(DrainDone) {
			return nil, nil, row, ErrMirrorConflict
		}
	default:
		if row.retired != 0 || row.state != string(DrainReady) {
			return nil, nil, row, ErrMirrorConflict
		}
		if err := validateReplacementPublicationMirror(q, p, *w); err != nil {
			return nil, nil, row, err
		}
	}
	return &p, raw, row, nil
}

func validateReplacementPublicationMirror(q versionQuerier, p SpoolReplacementPublication, w SpoolReplacementWitness) error {
	got, err := captureToken(q, p.Expected.Path)
	if err != nil || got != p.Expected {
		return ErrMirrorConflict
	}
	e, err := scanEntry(q.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, p.Expected.Path))
	if err != nil || !e.LocalOnly || e.IsDir || !e.MtimeNsecKnown || e.Size != w.Image.Size ||
		e.Mtime.UnixNano() != w.Image.MtimeNS || e.Version != p.Expected {
		return ErrMirrorConflict
	}
	return nil
}

// ReplacementPublication reads one canonical journal and its row/witness and
// exact current mirror in one SQLite snapshot. Nil means only legacy/no journal.
func (s *SpoolStore) ReplacementPublication(row *SpoolRow) (*SpoolReplacementPublication, error) {
	if row == nil {
		return nil, ErrMirrorConflict
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	p, _, durable, err := readReplacementPublication(tx, row.ID)
	if err != nil {
		return nil, err
	}
	if durable.path != row.NFSPath || durable.file != row.SpoolFile || durable.size != row.Size ||
		!bytes.Equal(durable.sha, row.SHA256) || durable.state != string(row.DrainState) || durable.retired != row.RetiredBy {
		return nil, ErrMirrorConflict
	}
	return p, nil
}

func queuedReplacementPublication(w SpoolReplacementWitness, expected MirrorToken, witness []byte) SpoolReplacementPublication {
	digest := sha256.Sum256(witness)
	return SpoolReplacementPublication{Version: 1, OperationID: w.OperationID, SourceRowID: w.SourceRowID,
		WitnessSHA256: hex.EncodeToString(digest[:]), SourceFile: w.SourceFile, SourceIdentity: w.SourceIdentity,
		Expected: expected, Phase: ReplacementPublicationQueued, Revision: 1}
}

func insertReplacementPublication(tx *sql.Tx, p SpoolReplacementPublication) error {
	raw, err := encodeReplacementPublication(p)
	if err != nil {
		return err
	}
	result, err := tx.Exec(`INSERT INTO spool_replacement_publications(spool_id,operation_id,phase,payload) VALUES(?,?,?,?)`,
		p.SourceRowID, p.OperationID, p.Phase, raw)
	if err != nil {
		return err
	}
	return exactlyOneRow(result)
}

// Supersede the exact prior queued operation before acceptance updates a new
// writing row to ready; the existing ready-trigger may retire this predecessor.
func supersedeReplacementPublication(tx *sql.Tx, prior SpoolReplacementWitness, expected MirrorToken) error {
	p, raw, row, err := readReplacementPublication(tx, prior.SourceRowID)
	if err != nil {
		return err
	}
	if row.retired != 0 || row.state != string(DrainReady) || row.path != prior.Destination.Path ||
		row.file != prior.SourceFile || row.size != prior.Image.Size || !bytes.Equal(row.sha, prior.Image.SHA256) {
		return ErrMirrorConflict
	}
	if p == nil { // legacy accepted carrier: bind its canonical witness first.
		witness, err := json.Marshal(prior)
		if err != nil || !bytes.Equal(witness, row.witness) || expected.Path != prior.Destination.Path {
			return ErrMirrorConflict
		}
		p0 := queuedReplacementPublication(prior, expected, witness)
		if err := validateReplacementPublicationMirror(tx, p0, prior); err != nil {
			return err
		}
		result, err := tx.Exec(`UPDATE spool_entries SET replacement_publication_required=1 WHERE id=? AND replacement_publication_required=0 AND retired_by=0`, prior.SourceRowID)
		if err != nil {
			return err
		}
		if err := exactlyOneRow(result); err != nil {
			return err
		}
		if err := insertReplacementPublication(tx, p0); err != nil {
			return err
		}
		p, raw, err = &p0, nil, nil
		if raw, err = encodeReplacementPublication(p0); err != nil {
			return err
		}
	}
	if p.Phase != ReplacementPublicationQueued || p.Expected != expected {
		return ErrMirrorConflict
	}
	next := *p
	next.Phase, next.Revision = ReplacementPublicationSuperseded, 2
	encoded, err := encodeReplacementPublication(next)
	if err != nil {
		return err
	}
	result, err := tx.Exec(`UPDATE spool_replacement_publications SET phase=?,payload=? WHERE spool_id=? AND phase='queued' AND payload=?`,
		next.Phase, encoded, prior.SourceRowID, raw)
	if err != nil {
		return err
	}
	return exactlyOneRow(result)
}

// BeginSpoolReplacementPublication durably plans one exact hidden stage path.
// No remote operation or file mutation occurs here; the caller owns worker
// admission and retains the private image on any uncertain result.
func (s *Store) BeginSpoolReplacementPublication(rowID int64, expected MirrorToken, verifySource func() error) (_ *SpoolReplacementPublication, retErr error) {
	if s.spoolStore == nil || rowID <= 0 || verifySource == nil || expected.IsZero() || !validCustodyPath(expected.Path) {
		return nil, ErrMirrorConflict
	}
	s.replacementObservationMu.Lock()
	defer s.replacementObservationMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	spool := s.spoolStore
	spool.writeMu.Lock()
	defer spool.writeMu.Unlock()
	s.mu.RLock()
	cached := s.pathCache[expected.Path]
	validCache := cached != nil && cached.Version == expected && cached.LocalOnly && !cached.IsDir && !s.preparedCreateConflictLocked(expected.Path)
	var cachedSnapshot *Entry
	if validCache {
		cachedSnapshot = cached.snapshot()
	}
	s.mu.RUnlock()
	if !validCache {
		return nil, ErrMirrorConflict
	}
	tx, release, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return nil, err
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
	p, raw, row, err := readReplacementPublication(tx, rowID)
	if err != nil {
		return nil, err
	}
	if row.path != expected.Path || row.state != string(DrainReady) || row.retired != 0 ||
		row.punched != 0 || row.clone != "" || row.qualification != "" {
		return nil, ErrMirrorConflict
	}
	w, err := replacementWitnessByID(tx, rowID)
	if err != nil || w == nil || w.SourceFile != row.file {
		return nil, ErrMirrorConflict
	}
	if err := validateExpected(tx, expected); err != nil {
		return nil, err
	}
	durableMirror, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, expected.Path))
	if err != nil || !sameReplacementEntry(cachedSnapshot, durableMirror) {
		return nil, ErrMirrorConflict
	}
	if p == nil { // only a canonical legacy accepted row may be lazily upgraded.
		p0 := queuedReplacementPublication(*w, expected, row.witness)
		if err := validateReplacementPublicationMirror(tx, p0, *w); err != nil {
			return nil, err
		}
		p = &p0
	} else if p.Expected != expected {
		return nil, ErrMirrorConflict
	}
	var holds int
	if err := tx.QueryRow(`SELECT
 (SELECT count(*) FROM spool_bindings WHERE spool_id=?) +
 (SELECT count(*) FROM spool_custody WHERE spool_id=?) +
 (SELECT count(*) FROM spool_completion_holds WHERE spool_id=?)`, rowID, rowID, rowID).Scan(&holds); err != nil || holds != 0 {
		return nil, ErrMirrorConflict
	}
	if p.Phase != ReplacementPublicationQueued && p.Phase != ReplacementPublicationIntent {
		return nil, ErrMirrorConflict
	}
	if err := verifySource(); err != nil {
		return nil, err
	}
	if p.Phase == ReplacementPublicationIntent {
		if err := verifySource(); err != nil {
			return nil, &PendingSpoolReplacement{Cause: err}
		}
		return p, nil
	}
	if row.required == 0 {
		result, err := tx.Exec(`UPDATE spool_entries SET replacement_publication_required=1 WHERE id=? AND replacement_publication_required=0 AND retired_by=0`, rowID)
		if err != nil {
			return nil, err
		}
		if err := exactlyOneRow(result); err != nil {
			return nil, err
		}
		if err := insertReplacementPublication(tx, *p); err != nil {
			return nil, err
		}
		raw, err = encodeReplacementPublication(*p)
		if err != nil {
			return nil, err
		}
	}
	next := *p
	next.Phase, next.Revision = ReplacementPublicationIntent, 2
	next.StagePath = replacementStagePath(expected.Path, next.OperationID)
	encoded, err := encodeReplacementPublication(next)
	if err != nil {
		return nil, err
	}
	result, err := tx.Exec(`UPDATE spool_replacement_publications SET phase=?,payload=? WHERE spool_id=? AND phase='queued' AND payload=?`,
		next.Phase, encoded, rowID, raw)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(result); err != nil {
		return nil, err
	}
	stored, storedRaw, storedRow, err := readReplacementPublication(tx, rowID)
	if err != nil || stored == nil || *stored != next || !bytes.Equal(storedRaw, encoded) || storedRow.required != 1 {
		return nil, ErrMirrorConflict
	}
	commitStarted = true
	if err := tx.Commit(); err != nil {
		discard = true
		return nil, &PendingSpoolReplacement{Cause: err}
	}
	if err := verifySource(); err != nil {
		return nil, &PendingSpoolReplacement{Cause: err}
	}
	return &next, nil
}
