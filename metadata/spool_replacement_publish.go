package metadata

import (
	"bytes"
	"encoding/hex"
	"errors"
	"path"
	"reflect"

	"github.com/juicedata/juicefs/pkg/replaceguard"
)

// validateReplacementRecord binds remote authority bytes to the original
// accepted image and namespace. The destination ctime is a supplemental
// pre-rename fence captured by the coordinator once and retained thereafter.
func validateReplacementRecord(p SpoolReplacementPublication, w SpoolReplacementWitness) error {
	r, err := replaceguard.Decode([]byte(p.Record))
	if err != nil || r.OperationID != w.OperationID || r.BackendUUID != w.Binding.FormatUUID ||
		r.BackendDigest != w.Binding.Digest || r.GraphDigest != w.GraphDigest ||
		r.ImageSHA256 != hex.EncodeToString(w.Image.SHA256) || r.AcceptedEpoch != w.AcceptedEpoch ||
		r.Stage.Name != path.Base(p.StagePath) || r.Stage.Length != uint64(w.Image.Size) ||
		r.Stage.MtimeNS != w.Image.MtimeNS || r.Stage.CtimeNS <= 0 ||
		r.Destination.Name != path.Base(w.Destination.Path) || r.Destination.CtimeNS <= 0 {
		return ErrMirrorConflict
	}
	parents := []replaceguard.Parent{{Inode: 1}}
	for _, node := range w.Destination.Chain[:len(w.Destination.Chain)-1] {
		parents = append(parents, replaceguard.Parent{Name: path.Base(node.Path), Inode: node.Inode})
	}
	leaf := w.Destination.Chain[len(w.Destination.Chain)-1]
	parentInode := parents[len(parents)-1].Inode
	if !reflect.DeepEqual(r.Parents, parents) || r.Stage.Parent != parentInode ||
		r.Destination.Parent != parentInode || r.Destination.Inode != leaf.Inode ||
		r.Destination.Length != uint64(leaf.Size) ||
		leaf.MtimeSec != r.Destination.MtimeNS/1_000_000_000 ||
		int64(leaf.MtimeNsec) != r.Destination.MtimeNS%1_000_000_000 {
		return ErrMirrorConflict
	}
	return nil
}

// replacementPublicationTransition admits only the exact next custody state.
// The first ARM reply may normalize Owner/Proxy; subsequent refreshes preserve
// Owner and may advance only Proxy/Epoch. A committed receipt is terminal.
func replacementPublicationTransition(expected, next SpoolReplacementPublication) error {
	if expected.validate() != nil || next.validate() != nil || next.Revision != expected.Revision+1 {
		return ErrMirrorConflict
	}
	base := next
	base.Phase, base.Revision, base.Record = expected.Phase, expected.Revision, expected.Record
	if base != expected {
		return ErrMirrorConflict
	}
	switch {
	case expected.Phase == ReplacementPublicationIntent && next.Phase == ReplacementPublicationArming:
		return nil
	case expected.Phase == ReplacementPublicationArming && next.Phase == ReplacementPublicationArming:
		old, err := replaceguard.Decode([]byte(expected.Record))
		if err != nil {
			return ErrMirrorConflict
		}
		newRecord, err := replaceguard.Decode([]byte(next.Record))
		if err != nil || old.Phase != "prepared" || newRecord.Phase != "prepared" ||
			newRecord.Epoch <= old.Epoch {
			return ErrMirrorConflict
		}
		newRecord.Epoch = old.Epoch
		if !reflect.DeepEqual(old, newRecord) {
			return ErrMirrorConflict
		}
		return nil
	case expected.Phase == ReplacementPublicationArming && next.Phase == ReplacementPublicationArmed:
		old, err := replaceguard.Decode([]byte(expected.Record))
		if err != nil {
			return ErrMirrorConflict
		}
		newRecord, err := replaceguard.Decode([]byte(next.Record))
		if err != nil || old.Phase != "prepared" || newRecord.Phase != "prepared" || newRecord.Epoch < old.Epoch {
			return ErrMirrorConflict
		}
		newRecord.Owner, newRecord.Proxy, newRecord.Epoch = old.Owner, old.Proxy, old.Epoch
		if !reflect.DeepEqual(old, newRecord) {
			return ErrMirrorConflict
		}
		return nil
	case expected.Phase == ReplacementPublicationArmed && next.Phase == ReplacementPublicationArmed:
		old, err := replaceguard.Decode([]byte(expected.Record))
		if err != nil {
			return ErrMirrorConflict
		}
		newRecord, err := replaceguard.Decode([]byte(next.Record))
		if err != nil || old.Phase != "prepared" || newRecord.Phase != "prepared" ||
			newRecord.Owner != old.Owner || newRecord.Epoch < old.Epoch ||
			(newRecord.Epoch == old.Epoch && newRecord.Proxy == old.Proxy) {
			return ErrMirrorConflict
		}
		newRecord.Proxy, newRecord.Epoch = old.Proxy, old.Epoch
		if !reflect.DeepEqual(old, newRecord) {
			return ErrMirrorConflict
		}
		return nil
	case expected.Phase == ReplacementPublicationArmed && next.Phase == ReplacementPublicationCommitted:
		old, err := replaceguard.Decode([]byte(expected.Record))
		if err != nil {
			return ErrMirrorConflict
		}
		newRecord, err := replaceguard.Decode([]byte(next.Record))
		if err != nil || old.Phase != "prepared" || newRecord.Phase != "committed" {
			return ErrMirrorConflict
		}
		newRecord.Phase = old.Phase
		if !reflect.DeepEqual(old, newRecord) {
			return ErrMirrorConflict
		}
		return nil
	}
	return ErrMirrorConflict
}

// AdvanceSpoolReplacementPublication persists stage custody, exact ARM reply,
// refresh, and the exact committed backend receipt. The accepted READY row,
// private image, and local mirror remain untouched until coupled settlement.
// A stale expected state is a conflict; callers reconcile by canonical reread.
func (s *Store) AdvanceSpoolReplacementPublication(expected, next SpoolReplacementPublication, verifySource func() error) (_ *SpoolReplacementPublication, retErr error) {
	if s.spoolStore == nil || verifySource == nil || replacementPublicationTransition(expected, next) != nil {
		return nil, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	spool := s.spoolStore
	spool.writeMu.Lock()
	defer spool.writeMu.Unlock()
	s.mu.RLock()
	cached := s.pathCache[expected.Expected.Path]
	validCache := cached != nil && cached.Version == expected.Expected && cached.LocalOnly &&
		!cached.IsDir && !s.preparedCreateConflictLocked(expected.Expected.Path)
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
	p, raw, row, err := readReplacementPublication(tx, expected.SourceRowID)
	if err != nil || p == nil || *p != expected || row.retired != 0 || row.state != string(DrainReady) || row.required != 1 {
		return nil, ErrMirrorConflict
	}
	expectedRaw, err := encodeReplacementPublication(expected)
	if err != nil || !bytes.Equal(raw, expectedRaw) {
		return nil, ErrMirrorConflict
	}
	w, err := replacementWitnessByID(tx, expected.SourceRowID)
	if err != nil || w == nil || validateReplacementRecord(next, *w) != nil {
		return nil, ErrMirrorConflict
	}
	if err := validateExpected(tx, expected.Expected); err != nil {
		return nil, err
	}
	durableMirror, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, expected.Expected.Path))
	if err != nil || !sameReplacementEntry(cachedSnapshot, durableMirror) {
		return nil, ErrMirrorConflict
	}
	var holds int
	if err := tx.QueryRow(`SELECT
 (SELECT count(*) FROM spool_bindings WHERE spool_id=?) +
 (SELECT count(*) FROM spool_custody WHERE spool_id=?) +
 (SELECT count(*) FROM spool_completion_holds WHERE spool_id=?)`, expected.SourceRowID, expected.SourceRowID, expected.SourceRowID).Scan(&holds); err != nil || holds != 0 {
		return nil, ErrMirrorConflict
	}
	if err := verifySource(); err != nil {
		return nil, err
	}
	encoded, err := encodeReplacementPublication(next)
	if err != nil {
		return nil, err
	}
	result, err := tx.Exec(`UPDATE spool_replacement_publications SET phase=?,payload=? WHERE spool_id=? AND phase=? AND payload=?`,
		next.Phase, encoded, expected.SourceRowID, expected.Phase, expectedRaw)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(result); err != nil {
		return nil, err
	}
	stored, storedRaw, storedRow, err := readReplacementPublication(tx, expected.SourceRowID)
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
