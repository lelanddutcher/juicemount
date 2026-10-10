package metadata

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/slicegraph"
)

// SpoolReplacementImage describes the fsynced new private image, independent of
// the original remote graph length.
type SpoolReplacementImage struct {
	Size    int64
	MtimeNS int64
	SHA256  []byte
}

// SpoolReplacementWitness is immutable evidence captured before local rename
// acceptance. It does not authorize a remote overwrite: publication must
// compare this original graph and namespace with lease/backend fencing in the
// same remote transaction as the replacing rename.
type SpoolReplacementWitness struct {
	Version            uint32
	OperationID        string
	SourceRowID        int64
	SourceFile         string
	SourceIdentity     string
	Source             MirrorToken
	Image              SpoolReplacementImage
	CurrentDestination MirrorToken
	PredecessorRowID   int64
	SourceParents      []SpoolReplacementParent
	Destination        CachePathObservation
	Binding            CachePathBinding
	Graph              []byte
	GraphDigest        string
	SourceEpoch        uint64
	AcceptedEpoch      uint64
}

// SpoolReplacementParent binds a captured source ancestor's directory identity.
type SpoolReplacementParent struct {
	Token      MirrorToken
	Inode      uint64
	ParentPath string
	LocalOnly  bool
}

func replacementParent(e *Entry) SpoolReplacementParent {
	return SpoolReplacementParent{Token: e.Version, Inode: e.Inode, ParentPath: e.ParentPath, LocalOnly: e.LocalOnly}
}

func sameReplacementEntry(a, b *Entry) bool {
	return a != nil && b != nil && a.Path == b.Path && a.Name == b.Name && a.ParentPath == b.ParentPath &&
		a.IsDir == b.IsDir && a.Size == b.Size && a.Mtime.Unix() == b.Mtime.Unix() && a.MtimeNsecKnown == b.MtimeNsecKnown &&
		(!a.MtimeNsecKnown || a.Mtime.Nanosecond() == b.Mtime.Nanosecond()) && a.Inode == b.Inode && a.Mode == b.Mode && a.LocalOnly == b.LocalOnly && a.Version == b.Version
}

// PendingSpoolReplacement retains both names and source custody after an
// uncertain acceptance. A failed acknowledgement must not trigger a new save.
type PendingSpoolReplacement struct{ Cause error }

func (e *PendingSpoolReplacement) Error() string {
	return fmt.Sprintf("offline replacement acceptance uncertain: %v", e.Cause)
}
func (e *PendingSpoolReplacement) Unwrap() error { return e.Cause }

func (w SpoolReplacementWitness) validate() error {
	if w.Version != 1 || !validCustodyID(w.OperationID) || w.SourceRowID <= 0 ||
		w.PredecessorRowID < 0 || w.PredecessorRowID >= w.SourceRowID || w.Image.Size < 0 || len(w.Image.SHA256) != sha256.Size ||
		!validPrivatePath(w.SourceFile) || !validIdentity(w.SourceIdentity) ||
		!validCustodyPath(w.Source.Path) || w.Source.Incarnation == "" || w.Source.Revision <= 0 ||
		w.Source.StoreID != "" || w.Source.Epoch != 0 || !validCachePathBinding(w.Binding) ||
		w.CurrentDestination.Path != w.Destination.Path || w.CurrentDestination.Incarnation == "" || w.CurrentDestination.Revision <= 0 ||
		w.CurrentDestination.StoreID != "" || w.CurrentDestination.Epoch != 0 ||
		!validCustodyPath(w.Destination.Path) || w.Source.Path == w.Destination.Path ||
		w.SourceEpoch > math.MaxInt64 || w.AcceptedEpoch > math.MaxInt64 || w.Source.Revision == math.MaxInt64 ||
		len(w.Destination.Chain) == 0 || len(w.Graph) > slicegraph.MaxGraphBytes {
		return ErrMirrorConflict
	}
	g, err := slicegraph.Decode(w.Graph)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(w.Graph)
	if hex.EncodeToString(digest[:]) != w.GraphDigest {
		return ErrMirrorConflict
	}
	last := w.Destination.Chain[len(w.Destination.Chain)-1]
	if last.Path != w.Destination.Path || last.IsDir || last.Size < 0 || uint64(last.Size) != g.Length {
		return ErrMirrorConflict
	}
	parent := "."
	for i, n := range w.Destination.Chain {
		if n.ParentPath != parent || path.Dir(n.Path) != parent || n.Token.Path != n.Path ||
			n.Inode <= 1 || n.Token.Incarnation == "" || n.Token.Revision <= 0 ||
			n.Token.StoreID != "" || n.Token.Epoch != 0 || (i < len(w.Destination.Chain)-1 && !n.IsDir) {
			return ErrMirrorConflict
		}
		parent = n.Path
	}
	if w.PredecessorRowID == 0 && w.CurrentDestination != last.Token {
		return ErrMirrorConflict
	}
	parent = path.Dir(w.Source.Path)
	for i := len(w.SourceParents) - 1; i >= 0; i-- {
		node := w.SourceParents[i]
		p := node.Token
		if node.Inode <= 1 || node.ParentPath != path.Dir(parent) || p.Path != parent || p.Incarnation == "" || p.Revision <= 0 || p.StoreID != "" || p.Epoch != 0 {
			return ErrMirrorConflict
		}
		parent = path.Dir(parent)
	}
	if parent != "." {
		return ErrMirrorConflict
	}
	return nil
}

// CaptureSpoolRenameParents captures source ancestors locally; the destination
// uses its existing qualified cache-path chain. Root is the implicit inode 1.
func (s *Store) CaptureSpoolRenameParents(name string) ([]SpoolReplacementParent, error) {
	if !validCustodyPath(name) {
		return nil, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	var out []SpoolReplacementParent
	parts := strings.Split(path.Dir(name), "/")
	if path.Dir(name) == "." {
		return out, nil
	}
	for i := range parts {
		name := strings.Join(parts[:i+1], "/")
		token, err := captureToken(s.db, name)
		if err != nil || token.Incarnation == "" {
			return nil, ErrMirrorConflict
		}
		s.mu.RLock()
		e := s.pathCache[name]
		valid := e != nil && e.IsDir && e.Mode.IsDir() && e.Version == token && e.ParentPath == path.Dir(name)
		var node SpoolReplacementParent
		if valid {
			node = replacementParent(e)
		}
		s.mu.RUnlock()
		if !valid {
			return nil, ErrMirrorConflict
		}
		durable, err := scanEntry(s.db.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, name))
		if err != nil || !durable.IsDir || !durable.Mode.IsDir() || replacementParent(durable) != node {
			return nil, ErrMirrorConflict
		}
		out = append(out, node)
	}
	return out, nil
}

// AcceptSpoolReplacement moves one local writer, its mirror entry and its
// original-destination evidence in one FULL transaction. Caller owns namespace,
// open shards and the source descriptor. verifySource rechecks that retained
// descriptor and private name, and must not enter Store or SpoolStore locks.
func (s *Store) AcceptSpoolReplacement(w SpoolReplacementWitness, verifySource func() error) (_ *Entry, retErr error) {
	if s.spoolStore == nil || verifySource == nil || w.validate() != nil {
		return nil, ErrMirrorConflict
	}
	payload, err := json.Marshal(w)
	if err != nil {
		return nil, err
	}
	if len(payload) > 2*slicegraph.MaxGraphBytes {
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
	destinationParents := w.Destination.Chain
	if w.PredecessorRowID != 0 {
		destinationParents = destinationParents[:len(destinationParents)-1]
	}
	tokens := append(append([]MirrorToken{w.Source, w.CurrentDestination}, sourceParentTokens(w.SourceParents)...), destinationTokens(CachePathObservation{Chain: destinationParents})...)
	for _, token := range tokens {
		if e := s.pathCache[token.Path]; e == nil || e.Version != token || s.preparedCreateConflictLocked(token.Path) {
			s.mu.RUnlock()
			return nil, ErrMirrorConflict
		}
	}
	for _, node := range destinationParents {
		got, ok := cachePathNodeFromEntry(s.pathCache[node.Path])
		if !ok || got != node {
			s.mu.RUnlock()
			return nil, ErrMirrorConflict
		}
	}
	for _, parent := range w.SourceParents {
		e := s.pathCache[parent.Token.Path]
		if !e.IsDir || !e.Mode.IsDir() || replacementParent(e) != parent {
			s.mu.RUnlock()
			return nil, ErrMirrorConflict
		}
	}
	source := s.pathCache[w.Source.Path].snapshot()
	currentDestination := s.pathCache[w.Destination.Path].snapshot()
	s.mu.RUnlock()
	if !source.LocalOnly || source.IsDir || source.Mode.Type() != 0 || source.Size < 0 || source.Name != path.Base(source.Path) || source.ParentPath != path.Dir(source.Path) {
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
	if err := validateExpected(tx, w.Source); err != nil {
		return nil, err
	}
	durableSource, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, w.Source.Path))
	if err != nil || !sameReplacementEntry(source, durableSource) {
		return nil, ErrMirrorConflict
	}
	for _, parent := range w.SourceParents {
		if err := validateExpected(tx, parent.Token); err != nil {
			return nil, err
		}
		e, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, parent.Token.Path))
		if err != nil || !e.IsDir || !e.Mode.IsDir() || replacementParent(e) != parent {
			return nil, ErrMirrorConflict
		}
	}
	for _, node := range destinationParents {
		got, err := durableCachePathNode(tx, node.Path)
		if err != nil || got != node {
			return nil, ErrMirrorConflict
		}
	}
	if err := validateExpected(tx, w.CurrentDestination); err != nil {
		return nil, err
	}
	if w.PredecessorRowID != 0 {
		durableDestination, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, w.Destination.Path))
		if err != nil || !sameReplacementEntry(currentDestination, durableDestination) || !currentDestination.LocalOnly ||
			currentDestination.IsDir || currentDestination.Mode.Type() != 0 {
			return nil, ErrMirrorConflict
		}
	}
	var stored cachePathRecord
	var chain []byte
	if err := tx.QueryRow(`SELECT store_id,backend_digest,format_uuid,path,chain_json FROM cache_path_eligibility WHERE path=?`, w.Destination.Path).
		Scan(&stored.StoreID, &stored.Digest, &stored.FormatUUID, &stored.Path, &chain); err != nil ||
		json.Unmarshal(chain, &stored.Chain) != nil || stored.Digest != w.Binding.Digest || stored.FormatUUID != w.Binding.FormatUUID {
		return nil, ErrMirrorConflict
	}
	epoch, err := captureStoreEpoch(tx, 0)
	if err != nil || stored.StoreID != epoch.StoreID || len(stored.Chain) != len(w.Destination.Chain) {
		return nil, ErrMirrorConflict
	}
	for i := range stored.Chain {
		if stored.Chain[i] != w.Destination.Chain[i] {
			return nil, ErrMirrorConflict
		}
	}
	if w.PredecessorRowID != 0 {
		prior, err := replacementWitnessByID(tx, w.PredecessorRowID)
		if err != nil || prior == nil || !sameOriginalReplacement(*prior, w) ||
			prior.Source.Incarnation != w.CurrentDestination.Incarnation ||
			w.CurrentDestination.Revision < prior.Source.Revision+1 || prior.OperationID == w.OperationID {
			return nil, ErrMirrorConflict
		}
		var state, clone, qualification string
		var retired, punched, holds int64
		if err := tx.QueryRow(`SELECT drain_state,retired_by,punched_end,clone_hold,qualification_hold FROM spool_entries WHERE id=?`, w.PredecessorRowID).
			Scan(&state, &retired, &punched, &clone, &qualification); err != nil ||
			state != string(DrainReady) || retired != 0 || punched != 0 || clone != "" || qualification != "" {
			return nil, ErrMirrorConflict
		}
		publication, journalRaw, _, err := readReplacementPublication(tx, w.PredecessorRowID)
		if err != nil || (publication != nil && (publication.Phase != ReplacementPublicationQueued || publication.Expected != w.CurrentDestination)) {
			return nil, ErrMirrorConflict
		}
		if publication != nil {
			disposition, err := readReplacementDisposition(tx, w.PredecessorRowID, *publication, journalRaw)
			if err != nil || disposition != nil {
				return nil, ErrMirrorConflict
			}
		}
		if err := tx.QueryRow(`SELECT
 (SELECT count(*) FROM spool_bindings WHERE spool_id=?) +
 (SELECT count(*) FROM spool_custody WHERE spool_id=?) +
 (SELECT count(*) FROM spool_completion_holds WHERE spool_id=?)`, w.PredecessorRowID, w.PredecessorRowID, w.PredecessorRowID).Scan(&holds); err != nil || holds != 0 {
			return nil, ErrMirrorConflict
		}
		var newer int
		if err := tx.QueryRow(`SELECT count(*) FROM spool_entries WHERE nfs_path=? AND id>? AND retired_by=0`, w.Destination.Path, w.PredecessorRowID).Scan(&newer); err != nil || newer != 0 {
			return nil, ErrMirrorConflict
		}
	}
	for _, name := range []string{w.Source.Path, w.Destination.Path} {
		if err := rejectCompletionPath(tx, name, true); err != nil {
			return nil, err
		}
		var held int
		if err := tx.QueryRow(`SELECT
 (SELECT count(*) FROM spool_bindings b LEFT JOIN spool_entries r ON r.id=b.spool_id
  WHERE (b.publish_path=? AND b.phase='publishing') OR b.birth_path=? OR r.nfs_path=?) +
 (SELECT count(*) FROM spool_custody WHERE target_path=?)`, name, name, name, name).Scan(&held); err != nil || held != 0 {
			return nil, ErrMirrorConflict
		}
	}
	var rowPath, file, state, clone, qualification string
	var rowSize int64
	var rowSHA []byte
	var punched, retired, baseIno, baseMtime, baseCtime, baseLength, originalEpoch int64
	var noExisting bool
	if err := tx.QueryRow(`SELECT nfs_path,spool_file,drain_state,size,sha256,punched_end,retired_by,base_ino,base_mtime_ns,base_ctime_ns,base_len,accepted_epoch,clone_hold,qualification_hold,replacement_witness IS NULL FROM spool_entries WHERE id=?`, w.SourceRowID).
		Scan(&rowPath, &file, &state, &rowSize, &rowSHA, &punched, &retired, &baseIno, &baseMtime, &baseCtime, &baseLength, &originalEpoch, &clone, &qualification, &noExisting); err != nil {
		return nil, err
	}
	if rowPath != w.Source.Path || file != w.SourceFile || (state != string(DrainWriting) && state != string(DrainReady)) ||
		punched != 0 || retired != 0 || baseIno != spoolBaseInoAbsent || baseMtime != 0 || baseCtime != 0 || baseLength != 0 || clone != "" || qualification != "" || !noExisting ||
		originalEpoch < 0 || uint64(originalEpoch) != w.SourceEpoch ||
		(state == string(DrainReady) && (rowSize != w.Image.Size || !bytes.Equal(rowSHA, w.Image.SHA256))) {
		return nil, ErrMirrorConflict
	}
	var incompatible int
	if err := tx.QueryRow(`SELECT
 (SELECT count(*) FROM spool_bindings WHERE spool_id=?) +
 (SELECT count(*) FROM spool_custody WHERE spool_id=?) +
 (SELECT count(*) FROM spool_entries WHERE retired_by=0 AND id<>? AND id<>? AND
 ((nfs_path=? AND (id>? OR drain_state IN ('writing','ready','draining','failed'))) OR (nfs_path=? AND drain_state IN ('writing','ready','draining','failed'))))`,
		w.SourceRowID, w.SourceRowID, w.SourceRowID, w.PredecessorRowID, w.Source.Path, w.SourceRowID, w.Destination.Path).Scan(&incompatible); err != nil || incompatible != 0 {
		return nil, ErrMirrorConflict
	}
	if err := verifySource(); err != nil {
		return nil, err
	}
	if w.PredecessorRowID != 0 {
		prior, err := replacementWitnessByID(tx, w.PredecessorRowID)
		if err != nil || prior == nil || supersedeReplacementPublication(tx, *prior, w.CurrentDestination) != nil {
			return nil, ErrMirrorConflict
		}
	}
	// Conservative publication until the final graph/lease/backend EXEC path
	// is connected. Older binaries also preserve the existing server copy.
	result, err := tx.Exec(`UPDATE spool_entries SET nfs_path=?,replacement_witness=?,replacement_publication_required=1,drain_state='ready',size=?,sha256=?,base_ino=?,base_ctime_ns=?,base_mtime_ns=0,base_len=0,accepted_epoch=?,updated_at=?,wait_project='' WHERE id=?`,
		w.Destination.Path, payload, w.Image.Size, w.Image.SHA256, spoolBaseInoAbsent, spoolBaseCtimeAtomicConflict, w.AcceptedEpoch, time.Now().Unix(), w.SourceRowID)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(result); err != nil {
		return nil, err
	}
	var saved, savedSHA []byte
	var accepted, savedSize int64
	var savedState string
	if err := tx.QueryRow(`SELECT nfs_path,replacement_witness,accepted_epoch,drain_state,size,sha256 FROM spool_entries WHERE id=?`, w.SourceRowID).Scan(&rowPath, &saved, &accepted, &savedState, &savedSize, &savedSHA); err != nil || rowPath != w.Destination.Path || !bytes.Equal(saved, payload) || uint64(accepted) != w.AcceptedEpoch || savedState != string(DrainReady) || savedSize != w.Image.Size || !bytes.Equal(savedSHA, w.Image.SHA256) {
		return nil, ErrMirrorConflict
	}
	if _, err := tx.Exec(`UPDATE spool_entries SET retired_by=? WHERE id<>? AND retired_by=0 AND (nfs_path=? OR nfs_path=?)`,
		w.SourceRowID, w.SourceRowID, w.Source.Path, w.Destination.Path); err != nil {
		return nil, err
	}
	var unretired int
	if err := tx.QueryRow(`SELECT count(*) FROM spool_entries WHERE id<>? AND retired_by=0 AND (nfs_path=? OR nfs_path=?)`, w.SourceRowID, w.Source.Path, w.Destination.Path).Scan(&unretired); err != nil || unretired != 0 {
		return nil, ErrMirrorConflict
	}
	if err := deleteMirrorRow(tx, source.Path, s.ftsDefer); err != nil {
		return nil, err
	}
	// Retire the qualified destination inside the same transaction. Inserting
	// fresh avoids mixing its revision with the moved source incarnation.
	if err := deleteMirrorRow(tx, w.Destination.Path, s.ftsDefer); err != nil {
		return nil, err
	}
	source.Path, source.Name, source.ParentPath = w.Destination.Path, path.Base(w.Destination.Path), path.Dir(w.Destination.Path)
	source.Size = w.Image.Size
	source.Mtime = time.Unix(0, w.Image.MtimeNS)
	source.MtimeNsecKnown = true
	source.ResetGetAttrCache()
	if err := ftsExternalUpsert(tx, source, s.ftsDefer); err != nil {
		return nil, err
	}
	result, err = tx.Exec(`UPDATE entries SET incarnation=?,revision=? WHERE path=?`, w.Source.Incarnation, w.Source.Revision+1, source.Path)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(result); err != nil {
		return nil, err
	}
	if err := loadEntryVersion(tx, source); err != nil {
		return nil, err
	}
	source.Version = MirrorToken{Path: source.Path, Incarnation: w.Source.Incarnation, Revision: w.Source.Revision + 1}
	durableResult, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, source.Path))
	if err != nil || !sameReplacementEntry(source, durableResult) {
		return nil, ErrMirrorConflict
	}
	var oldCount int
	if err := tx.QueryRow(`SELECT count(*) FROM entries WHERE path=?`, w.Source.Path).Scan(&oldCount); err != nil || oldCount != 0 {
		return nil, ErrMirrorConflict
	}
	newPublication := queuedReplacementPublication(w, source.Version, payload)
	if err := insertReplacementPublication(tx, newPublication); err != nil {
		return nil, err
	}
	storedPublication, _, storedRow, err := readReplacementPublication(tx, w.SourceRowID)
	if err != nil || storedPublication == nil || *storedPublication != newPublication || storedRow.required != 1 {
		return nil, ErrMirrorConflict
	}
	if w.PredecessorRowID != 0 {
		priorPublication, _, priorRow, err := readReplacementPublication(tx, w.PredecessorRowID)
		if err != nil || priorPublication == nil || priorPublication.Phase != ReplacementPublicationSuperseded ||
			priorRow.retired != w.SourceRowID {
			return nil, ErrMirrorConflict
		}
	}
	commitStarted = true
	if err := tx.Commit(); err != nil {
		discard = true
		return nil, &PendingSpoolReplacement{Cause: err}
	}
	if err := verifySource(); err != nil {
		return nil, &PendingSpoolReplacement{Cause: err}
	}
	s.mu.Lock()
	if old := s.pathCache[w.Source.Path]; old != nil {
		s.removeFromChildrenIdx(old)
		s.subtreeRemoveLocked(old)
		if s.inodeCache[old.Inode] == old {
			delete(s.inodeCache, old.Inode)
		}
		delete(s.pathCache, old.Path)
	}
	s.publishEntryLocked(source)
	s.recomputeSubtreeAggregatesLocked()
	s.mu.Unlock()
	s.wakeFTSCompactor()
	return source.snapshot(), nil
}

func sourceParentTokens(parents []SpoolReplacementParent) []MirrorToken {
	out := make([]MirrorToken, len(parents))
	for i := range parents {
		out[i] = parents[i].Token
	}
	return out
}

func destinationTokens(o CachePathObservation) []MirrorToken {
	out := make([]MirrorToken, len(o.Chain))
	for i := range o.Chain {
		out[i] = o.Chain[i].Token
	}
	return out
}

// sameOriginalReplacement compares the immutable remote preimage. A successor
// must inherit it exactly; the current local destination is separate evidence.
func sameOriginalReplacement(prior, next SpoolReplacementWitness) bool {
	return reflect.DeepEqual(prior.Destination, next.Destination) && prior.Binding == next.Binding &&
		bytes.Equal(prior.Graph, next.Graph) && prior.GraphDigest == next.GraphDigest &&
		prior.AcceptedEpoch == next.AcceptedEpoch
}

func replacementWitnessByID(q versionQuerier, id int64) (*SpoolReplacementWitness, error) {
	var payload []byte
	var absent bool
	var name, file string
	var size, epoch int64
	var digest []byte
	if err := q.QueryRow(`SELECT replacement_witness,replacement_witness IS NULL,nfs_path,spool_file,size,sha256,accepted_epoch FROM spool_entries WHERE id=?`, id).
		Scan(&payload, &absent, &name, &file, &size, &digest, &epoch); err != nil {
		return nil, err
	}
	if absent {
		return nil, nil
	}
	if len(payload) > 2*slicegraph.MaxGraphBytes {
		return nil, ErrMirrorConflict
	}
	var w SpoolReplacementWitness
	if err := json.Unmarshal(payload, &w); err != nil || w.validate() != nil ||
		w.SourceRowID != id || w.SourceFile != file || w.Destination.Path != name ||
		size != w.Image.Size || !bytes.Equal(digest, w.Image.SHA256) || epoch < 0 || uint64(epoch) != w.AcceptedEpoch {
		return nil, ErrMirrorConflict
	}
	encoded, err := json.Marshal(w)
	if err != nil || !bytes.Equal(encoded, payload) {
		return nil, ErrMirrorConflict
	}
	return &w, nil
}

func (s *SpoolStore) ReplacementWitness(row *SpoolRow) (*SpoolReplacementWitness, error) {
	if row == nil {
		return nil, ErrMirrorConflict
	}
	w, err := replacementWitnessByID(s.db, row.ID)
	if err != nil || w == nil {
		return w, err
	}
	if w.Destination.Path != row.NFSPath || w.SourceFile != row.SpoolFile {
		return nil, ErrMirrorConflict
	}
	return w, nil
}
