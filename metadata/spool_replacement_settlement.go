package metadata

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/juicedata/juicefs/pkg/slicegraph"
)

// SpoolReplacementSettlement is the published generation, separate from the
// immutable accepted original and remote receipt. Directory identities appear
// only once exact private cleanup has been durably claimed.
type SpoolReplacementSettlement struct {
	Version       uint32          `json:"version"`
	Published     MirrorToken     `json:"published"`
	GraphDigest   string          `json:"graph_digest"`
	Chain         []CachePathNode `json:"chain"`
	RootIdentity  string          `json:"root_identity,omitempty"`
	FilesIdentity string          `json:"files_identity,omitempty"`
}

func (p SpoolReplacementPublication) SettlementRecord() (SpoolReplacementSettlement, error) {
	var r SpoolReplacementSettlement
	if p.Settlement == "" || json.Unmarshal([]byte(p.Settlement), &r) != nil ||
		r.Version != 1 || r.Published.Path != p.Expected.Path ||
		r.Published.Incarnation != p.Expected.Incarnation || r.Published.Revision != p.Expected.Revision+1 ||
		r.Published.StoreID != "" || r.Published.Epoch != 0 || !validCachePathChain(p.Expected.Path, r.Chain) ||
		r.Chain[len(r.Chain)-1].Token != r.Published || r.Chain[len(r.Chain)-1].IsDir ||
		!validReplacementDigest(r.GraphDigest) {
		return r, ErrMirrorConflict
	}
	switch p.Phase {
	case ReplacementPublicationSettled:
		if r.RootIdentity != "" || r.FilesIdentity != "" {
			return r, ErrMirrorConflict
		}
	case ReplacementPublicationCleaning, ReplacementPublicationCleaned:
		if !validIdentity(r.RootIdentity) || !validIdentity(r.FilesIdentity) || strings.Contains(r.RootIdentity, "/") || strings.Contains(r.FilesIdentity, "/") {
			return r, ErrMirrorConflict
		}
	default:
		return r, ErrMirrorConflict
	}
	canonical, err := json.Marshal(r)
	if err != nil || string(canonical) != p.Settlement {
		return r, ErrMirrorConflict
	}
	return r, nil
}

func validReplacementDigest(s string) bool {
	raw, err := hex.DecodeString(s)
	return err == nil && len(raw) == sha256.Size && hex.EncodeToString(raw) == s
}

func validateReplacementSettlementEvidence(p SpoolReplacementPublication, w SpoolReplacementWitness) error {
	r, err := p.SettlementRecord()
	if err != nil {
		return err
	}
	remote, err := replaceguard.Decode([]byte(p.Record))
	if err != nil || remote.Phase != "committed" || len(r.Chain) != len(w.Destination.Chain) ||
		len(remote.Parents) != len(r.Chain) || remote.Parents[0].Inode != 1 {
		return ErrMirrorConflict
	}
	leaf := r.Chain[len(r.Chain)-1]
	if leaf.Inode != remote.Stage.Inode || leaf.Size != w.Image.Size ||
		leaf.MtimeSec != w.Image.MtimeNS/1_000_000_000 || int64(leaf.MtimeNsec) != w.Image.MtimeNS%1_000_000_000 {
		return ErrMirrorConflict
	}
	for i, n := range r.Chain {
		if i < len(r.Chain)-1 && (remote.Parents[i+1].Inode != n.Inode || remote.Parents[i+1].Name != path.Base(n.Path)) {
			return ErrMirrorConflict
		}
		original := w.Destination.Chain[i]
		if n.Path != original.Path || n.ParentPath != original.ParentPath {
			return ErrMirrorConflict
		}
		if i < len(r.Chain)-1 && (n.Token.Incarnation != original.Token.Incarnation || n.Inode != original.Inode) {
			return ErrMirrorConflict
		}
	}
	parent := uint64(1)
	if len(r.Chain) > 1 {
		parent = r.Chain[len(r.Chain)-2].Inode
	}
	if remote.Stage.Parent != parent || remote.Destination.Parent != parent {
		return ErrMirrorConflict
	}
	return nil
}

func validateReplacementSettlementMirror(q versionQuerier, p SpoolReplacementPublication, w SpoolReplacementWitness) error {
	if err := validateReplacementSettlementEvidence(p, w); err != nil {
		return err
	}
	r, _ := p.SettlementRecord()
	token, err := captureToken(q, p.Expected.Path)
	if err != nil || token != r.Published {
		return ErrMirrorConflict
	}
	for _, want := range r.Chain {
		e, err := scanEntry(q.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, want.Path))
		if err != nil {
			return ErrMirrorConflict
		}
		got, ok := cachePathNodeFromEntry(e)
		if !ok || got != want {
			return ErrMirrorConflict
		}
	}
	var storeID, digest, uuid string
	var raw []byte
	if err := q.QueryRow(`SELECT store_id,backend_digest,format_uuid,chain_json FROM cache_path_eligibility WHERE path=?`, p.Expected.Path).
		Scan(&storeID, &digest, &uuid, &raw); err != nil || storeID == "" || digest != w.Binding.Digest || uuid != w.Binding.FormatUUID {
		return ErrMirrorConflict
	}
	var currentStoreID string
	if err := q.QueryRow(`SELECT value FROM store_meta WHERE key='mirror_store_id'`).Scan(&currentStoreID); err != nil || storeID != currentStoreID {
		return ErrMirrorConflict
	}
	var chain []CachePathNode
	if json.Unmarshal(raw, &chain) != nil || !reflect.DeepEqual(chain, r.Chain) {
		return ErrMirrorConflict
	}
	return nil
}

func replacementLocalNode(e *Entry, allowLocal bool) (CachePathNode, bool) {
	if e == nil || (!allowLocal && e.LocalOnly) || !e.MtimeNsecKnown || e.Inode <= 1 || e.Size < 0 ||
		e.Version.Incarnation == "" || e.Version.Revision <= 0 || e.Version.Path != e.Path ||
		e.Version.StoreID != "" || e.Version.Epoch != 0 ||
		(e.IsDir && e.Mode.Type() != fs.ModeDir) || (!e.IsDir && e.Mode.Type() != 0) {
		return CachePathNode{}, false
	}
	return CachePathNode{Path: e.Path, ParentPath: e.ParentPath, Inode: e.Inode, Size: e.Size,
		MtimeSec: e.Mtime.Unix(), MtimeNsec: uint32(e.Mtime.Nanosecond()), IsDir: e.IsDir, Token: e.Version}, true
}

// validReplacementCaptureChain applies the ordinary path and ancestry grammar
// while allowing the exact accepted LocalOnly leaf's synthetic high-bit inode.
// The published chain is validated separately with validCachePathChain.
func validReplacementCaptureChain(name string, chain []CachePathNode) bool {
	if len(chain) == 0 || chain[len(chain)-1].Inode <= 1 {
		return false
	}
	seen := make(map[uint64]bool, len(chain))
	for _, n := range chain[:len(chain)-1] {
		if n.Inode == chain[len(chain)-1].Inode {
			return false
		}
		seen[n.Inode] = true
	}
	probe := uint64(2)
	for seen[probe] {
		probe++
	}
	ordinary := append([]CachePathNode(nil), chain...)
	ordinary[len(ordinary)-1].Inode = probe
	return validCachePathChain(name, ordinary)
}

// CaptureReplacementSettlement is the one permitted LocalOnly leaf capture.
// It admits no provisional ancestor and never publishes general cache eligibility.
func (g *CachePathEligibility) CaptureReplacementSettlement(expected SpoolReplacementPublication) (CachePathObservation, error) {
	if g == nil || g.store == nil || expected.Phase != ReplacementPublicationCommitted {
		return CachePathObservation{}, ErrMirrorConflict
	}
	s := g.store
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	if s.spoolStore == nil {
		return CachePathObservation{}, ErrMirrorConflict
	}
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	p, raw, row, err := readReplacementPublication(s.db, expected.SourceRowID)
	wantRaw, encErr := encodeReplacementPublication(expected)
	if err != nil || encErr != nil || p == nil || *p != expected || !bytes.Equal(raw, wantRaw) || row.state != string(DrainReady) {
		return CachePathObservation{}, ErrMirrorConflict
	}
	parts, err := readProofParts(expected.Expected.Path, makeDummyCachePathIDs(expected.Expected.Path))
	if err != nil {
		return CachePathObservation{}, ErrMirrorConflict
	}
	out := CachePathObservation{Path: expected.Expected.Path, Chain: make([]CachePathNode, 0, len(parts))}
	s.mu.RLock()
	prefix := ""
	for i, part := range parts {
		if prefix == "" {
			prefix = part
		} else {
			prefix += "/" + part
		}
		e := s.pathCache[prefix]
		n, ok := replacementLocalNode(e, i == len(parts)-1)
		if !ok || e.LocalOnly != (i == len(parts)-1) || s.preparedCreateConflictLocked(prefix) {
			s.mu.RUnlock()
			return CachePathObservation{}, ErrMirrorConflict
		}
		out.Chain = append(out.Chain, n)
	}
	generation := s.cacheGeneration
	s.mu.RUnlock()
	if !validReplacementCaptureChain(out.Path, out.Chain) || out.Chain[len(out.Chain)-1].Token != expected.Expected {
		return CachePathObservation{}, ErrMirrorConflict
	}
	out.Epoch, err = captureStoreEpoch(s.db, generation)
	if err != nil {
		return CachePathObservation{}, err
	}
	for i, n := range out.Chain {
		e, err := scanEntry(s.db.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, n.Path))
		got, ok := replacementLocalNode(e, i == len(out.Chain)-1)
		if err != nil || !ok || got != n || e.LocalOnly != (i == len(out.Chain)-1) {
			return CachePathObservation{}, ErrMirrorConflict
		}
	}
	return out, nil
}

func sameReplacementPathProof(a, b RedisPathProof) bool {
	a.RequestBytes, a.ResponseBytes, a.RedisOps, a.Elapsed = 0, 0, 0, 0
	b.RequestBytes, b.ResponseBytes, b.RedisOps, b.Elapsed = 0, 0, 0, 0
	return reflect.DeepEqual(a, b)
}

// SettleSpoolReplacement changes publication, row, mirror and path eligibility
// in one FULL transaction. verifyCurrent is an eager caller-owned descriptor/
// session check; the caller holds namespace and path-shard admission throughout.
// The callback must not re-enter metadata APIs: it runs once before admission,
// once immediately before SQL commit under metadata locks, and once after release.
func (g *CachePathEligibility) SettleSpoolReplacement(expected SpoolReplacementPublication, captured CachePathObservation, before, after RedisPathProof, publishedGraph []byte, verifyCurrent func() error) (*SpoolReplacementPublication, error) {
	next, err := g.settleSpoolReplacement(expected, captured, before, after, publishedGraph, verifyCurrent)
	if err != nil {
		return nil, err
	}
	if err := verifyCurrent(); err != nil {
		return nil, &PendingSpoolReplacement{Cause: err}
	}
	return next, nil
}

func (g *CachePathEligibility) settleSpoolReplacement(expected SpoolReplacementPublication, captured CachePathObservation, before, after RedisPathProof, publishedGraph []byte, verifyCurrent func() error) (_ *SpoolReplacementPublication, retErr error) {
	if g == nil || g.store == nil || verifyCurrent == nil || expected.Phase != ReplacementPublicationCommitted ||
		captured.Path != expected.Expected.Path || len(captured.Chain) == 0 || captured.Chain[len(captured.Chain)-1].Token != expected.Expected ||
		!validReplacementCaptureChain(captured.Path, captured.Chain) || !sameReplacementPathProof(before, after) {
		return nil, ErrMirrorConflict
	}
	if err := verifyCurrent(); err != nil {
		return nil, err
	}
	s := g.store
	if s.spoolStore == nil {
		return nil, ErrMirrorConflict
	}
	w, err := replacementWitnessByID(s.db, expected.SourceRowID)
	if err != nil || w == nil {
		return nil, ErrMirrorConflict
	}
	remote, err := replaceguard.Decode([]byte(expected.Record))
	if err != nil || remote.Phase != "committed" || before.FormatUUID != w.Binding.FormatUUID || before.Path != captured.Path ||
		len(before.Nodes) != len(captured.Chain) || remote.Stage.Inode <= 1 || remote.Stage.Inode > math.MaxInt64 || remote.Stage.Inode == remote.Destination.Inode {
		return nil, ErrMirrorConflict
	}
	graph, err := slicegraph.Decode(publishedGraph)
	if err != nil || graph.Length != uint64(w.Image.Size) || len(publishedGraph) > slicegraph.MaxGraphBytes {
		return nil, ErrMirrorConflict
	}
	canonical, err := graph.Canonical()
	if err != nil || !bytes.Equal(canonical, publishedGraph) {
		return nil, ErrMirrorConflict
	}
	digest := sha256.Sum256(canonical)
	graphDigest := hex.EncodeToString(digest[:])
	target := captured
	target.Chain = append([]CachePathNode(nil), captured.Chain...)
	for i := range target.Chain {
		src := captured.Chain[i]
		proof := before.Nodes[i]
		if proof.Path != src.Path || proof.Inode != src.Inode && i < len(target.Chain)-1 ||
			proof.ParentInode != func() uint64 {
				if i == 0 {
					return 1
				}
				return before.Nodes[i-1].Inode
			}() ||
			proof.Size > uint64(^uint64(0)>>1) || proof.MtimeNsec >= 1e9 || proof.MtimeSec < 0 ||
			proof.MtimeSec > (int64(^uint64(0)>>1)-int64(proof.MtimeNsec))/1_000_000_000 {
			return nil, ErrMirrorConflict
		}
		if i == len(target.Chain)-1 {
			if proof.Inode != remote.Stage.Inode || int64(proof.Size) != w.Image.Size ||
				proof.MtimeSec != w.Image.MtimeNS/1_000_000_000 || int64(proof.MtimeNsec) != w.Image.MtimeNS%1_000_000_000 {
				return nil, ErrMirrorConflict
			}
		}
		target.Chain[i].Inode = proof.Inode
		target.Chain[i].Size = int64(proof.Size)
		target.Chain[i].MtimeSec = proof.MtimeSec
		target.Chain[i].MtimeNsec = proof.MtimeNsec
		if i == len(target.Chain)-1 || src.Size != int64(proof.Size) || src.MtimeSec != proof.MtimeSec || src.MtimeNsec != proof.MtimeNsec {
			target.Chain[i].Token.Revision++
		}
	}
	if !proofMatchesCachePath(w.Binding, target, before) || !proofMatchesCachePath(w.Binding, target, after) {
		return nil, ErrMirrorConflict
	}
	next := expected
	next.Phase = ReplacementPublicationSettled
	next.Revision++
	settlement := SpoolReplacementSettlement{Version: 1, Published: target.Chain[len(target.Chain)-1].Token, GraphDigest: graphDigest, Chain: target.Chain}
	encodedSettlement, err := json.Marshal(settlement)
	if err != nil {
		return nil, err
	}
	next.Settlement = string(encodedSettlement)
	if next.validate() != nil {
		return nil, ErrMirrorConflict
	}
	s.replacementObservationMu.Lock()
	defer s.replacementObservationMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	if s.spoolStore == nil || s.spoolStore.db != s.db {
		return nil, ErrMirrorConflict
	}
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	s.mu.RLock()
	generation := s.cacheGeneration
	snapshots := make([]*Entry, len(captured.Chain))
	valid := !s.preparedCreateConflictLocked(captured.Path)
	for i, n := range captured.Chain {
		e := s.pathCache[n.Path]
		got, ok := replacementLocalNode(e, i == len(captured.Chain)-1)
		if !ok || got != n || e.LocalOnly != (i == len(captured.Chain)-1) {
			valid = false
			break
		}
		snapshots[i] = e.snapshot()
	}
	s.mu.RUnlock()
	if !valid {
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
			if rb := tx.Rollback(); rb != nil {
				discard = true
				retErr = &PendingSpoolReplacement{Cause: errors.Join(retErr, rb)}
			}
		}
	}()
	epoch, err := captureStoreEpoch(tx, generation)
	if err != nil || epoch != captured.Epoch {
		return nil, ErrMirrorConflict
	}
	p, raw, row, err := readReplacementPublication(tx, expected.SourceRowID)
	expectedRaw, encErr := encodeReplacementPublication(expected)
	if err != nil || encErr != nil || p == nil || *p != expected || !bytes.Equal(raw, expectedRaw) || row.state != string(DrainReady) || row.retired != 0 {
		return nil, ErrMirrorConflict
	}
	for i, n := range captured.Chain {
		e, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, n.Path))
		if err != nil || !sameReplacementEntry(e, snapshots[i]) {
			return nil, ErrMirrorConflict
		}
		if i < len(captured.Chain)-1 && target.Chain[i].Token.Revision == n.Token.Revision {
			continue
		}
		if i < len(captured.Chain)-1 {
			result, err := tx.Exec(`UPDATE entries SET size=?,mtime=?,mtime_nsec=?,revision=revision+1 WHERE path=? AND incarnation=? AND revision=?`,
				target.Chain[i].Size, target.Chain[i].MtimeSec, target.Chain[i].MtimeNsec, n.Path, n.Token.Incarnation, n.Token.Revision)
			if err != nil {
				return nil, err
			}
			if err := exactlyOneRow(result); err != nil {
				return nil, err
			}
		} else {
			result, err := tx.Exec(`UPDATE entries SET inode=?,size=?,mtime=?,mtime_nsec=?,local_only=0,revision=revision+1 WHERE path=? AND incarnation=? AND revision=? AND local_only=1`,
				int64(remote.Stage.Inode), w.Image.Size, target.Chain[i].MtimeSec, target.Chain[i].MtimeNsec, n.Path, n.Token.Incarnation, n.Token.Revision)
			if err != nil {
				return nil, err
			}
			if err := exactlyOneRow(result); err != nil {
				return nil, err
			}
		}
	}
	publishedEntries := make([]*Entry, len(target.Chain))
	for i, n := range target.Chain {
		e, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, n.Path))
		got, ok := cachePathNodeFromEntry(e)
		if err != nil || !ok || got != n {
			return nil, ErrMirrorConflict
		}
		publishedEntries[i] = e
	}
	chainRaw, err := json.Marshal(target.Chain)
	if err != nil {
		return nil, err
	}
	result, err := tx.Exec(`INSERT INTO cache_path_eligibility(path,store_id,backend_digest,format_uuid,chain_json) VALUES(?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET store_id=excluded.store_id,backend_digest=excluded.backend_digest,format_uuid=excluded.format_uuid,chain_json=excluded.chain_json`,
		target.Path, epoch.StoreID, w.Binding.Digest, w.Binding.FormatUUID, chainRaw)
	if err != nil {
		return nil, err
	}
	_ = result
	result, err = tx.Exec(`UPDATE spool_entries SET drain_state='done',updated_at=? WHERE id=? AND drain_state='ready' AND retired_by=0 AND replacement_publication_required=1 AND spool_file=? AND size=? AND sha256=?`,
		time.Now().Unix(), expected.SourceRowID, expected.SourceFile, w.Image.Size, w.Image.SHA256)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(result); err != nil {
		return nil, err
	}
	nextRaw, err := encodeReplacementPublication(next)
	if err != nil {
		return nil, err
	}
	result, err = tx.Exec(`UPDATE spool_replacement_publications SET phase=?,payload=? WHERE spool_id=? AND phase=? AND payload=?`,
		next.Phase, nextRaw, expected.SourceRowID, expected.Phase, expectedRaw)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(result); err != nil {
		return nil, err
	}
	stored, storedRaw, _, err := readReplacementPublication(tx, expected.SourceRowID)
	if err != nil || stored == nil || *stored != next || !bytes.Equal(storedRaw, nextRaw) || validateReplacementSettlementMirror(tx, next, *w) != nil {
		return nil, ErrMirrorConflict
	}
	if err := verifyCurrent(); err != nil {
		return nil, err
	}
	commitStarted = true
	if err := tx.Commit(); err != nil {
		discard = true
		return nil, &PendingSpoolReplacement{Cause: err}
	}
	s.mu.Lock()
	for _, e := range publishedEntries {
		s.publishEntryLocked(e)
	}
	s.mu.Unlock()
	g.mu.Lock()
	g.records[target.Path] = cachePathRecord{StoreID: epoch.StoreID, Digest: w.Binding.Digest, FormatUUID: w.Binding.FormatUUID, Path: target.Path, Chain: append([]CachePathNode(nil), target.Chain...)}
	g.mu.Unlock()
	return &next, nil
}

func (s *Store) advanceReplacementCleanup(expected SpoolReplacementPublication, next SpoolReplacementPublication, verify func() error) (*SpoolReplacementPublication, error) {
	result, err := s.advanceReplacementCleanupLocked(expected, next, verify)
	if err != nil {
		return nil, err
	}
	if err := verify(); err != nil {
		return nil, &PendingSpoolReplacement{Cause: err}
	}
	return result, nil
}

func (s *Store) advanceReplacementCleanupLocked(expected SpoolReplacementPublication, next SpoolReplacementPublication, verify func() error) (_ *SpoolReplacementPublication, retErr error) {
	if s == nil || s.spoolStore == nil || verify == nil || expected.validate() != nil || next.validate() != nil ||
		next.Revision != expected.Revision+1 || next.Record != expected.Record || next.Expected != expected.Expected ||
		next.OperationID != expected.OperationID || next.SourceRowID != expected.SourceRowID || next.WitnessSHA256 != expected.WitnessSHA256 ||
		next.SourceFile != expected.SourceFile || next.SourceIdentity != expected.SourceIdentity || next.StagePath != expected.StagePath ||
		!((expected.Phase == ReplacementPublicationSettled && next.Phase == ReplacementPublicationCleaning) ||
			(expected.Phase == ReplacementPublicationCleaning && next.Phase == ReplacementPublicationCleaned)) {
		return nil, ErrMirrorConflict
	}
	oldRec, err := expected.SettlementRecord()
	if err != nil {
		return nil, err
	}
	newRec, err := next.SettlementRecord()
	if err != nil {
		return nil, err
	}
	if expected.Phase == ReplacementPublicationSettled {
		if newRec.Version != oldRec.Version || newRec.Published != oldRec.Published || newRec.GraphDigest != oldRec.GraphDigest || !reflect.DeepEqual(newRec.Chain, oldRec.Chain) {
			return nil, ErrMirrorConflict
		}
	} else if next.Settlement != expected.Settlement {
		return nil, ErrMirrorConflict
	}
	if err := verify(); err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	tx, release, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return nil, err
	}
	discard, commitStarted := false, false
	defer func() { release(discard) }()
	defer func() {
		if !commitStarted {
			if rb := tx.Rollback(); rb != nil {
				discard = true
				retErr = &PendingSpoolReplacement{Cause: errors.Join(retErr, rb)}
			}
		}
	}()
	p, raw, row, err := readReplacementPublication(tx, expected.SourceRowID)
	expectedRaw, encErr := encodeReplacementPublication(expected)
	if err != nil || encErr != nil || p == nil || *p != expected || !bytes.Equal(raw, expectedRaw) || row.state != string(DrainDone) || row.retired != 0 {
		return nil, ErrMirrorConflict
	}
	w, err := replacementWitnessByID(tx, expected.SourceRowID)
	if err != nil || w == nil || validateReplacementSettlementEvidence(expected, *w) != nil {
		return nil, ErrMirrorConflict
	}
	// Only the initial claim needs current serving authority. Once cleaning
	// is durable, exact private-directory custody is historical; a later
	// scan or rename must not strand an already unlinked backup.
	if expected.Phase == ReplacementPublicationSettled && validateReplacementSettlementMirror(tx, expected, *w) != nil {
		return nil, ErrMirrorConflict
	}
	nextRaw, err := encodeReplacementPublication(next)
	if err != nil {
		return nil, err
	}
	result, err := tx.Exec(`UPDATE spool_replacement_publications SET phase=?,payload=? WHERE spool_id=? AND phase=? AND payload=?`, next.Phase, nextRaw, expected.SourceRowID, expected.Phase, expectedRaw)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(result); err != nil {
		return nil, err
	}
	stored, storedRaw, _, err := readReplacementPublication(tx, expected.SourceRowID)
	if err != nil || stored == nil || *stored != next || !bytes.Equal(storedRaw, nextRaw) {
		return nil, ErrMirrorConflict
	}
	if err := verify(); err != nil {
		return nil, err
	}
	commitStarted = true
	if err := tx.Commit(); err != nil {
		discard = true
		return nil, &PendingSpoolReplacement{Cause: err}
	}
	return &next, nil
}

func (s *Store) BeginReplacementCleanup(expected SpoolReplacementPublication, rootID, filesID string, verifyCurrent func() error) (*SpoolReplacementPublication, error) {
	if expected.Phase != ReplacementPublicationSettled || !validIdentity(rootID) || !validIdentity(filesID) || strings.Contains(rootID, "/") || strings.Contains(filesID, "/") {
		return nil, ErrMirrorConflict
	}
	rec, err := expected.SettlementRecord()
	if err != nil {
		return nil, err
	}
	rec.RootIdentity, rec.FilesIdentity = rootID, filesID
	raw, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	next := expected
	next.Phase = ReplacementPublicationCleaning
	next.Revision++
	next.Settlement = string(raw)
	return s.advanceReplacementCleanup(expected, next, verifyCurrent)
}

func (s *Store) FinishReplacementCleanup(expected SpoolReplacementPublication, verifyAbsentAndSynced func() error) (*SpoolReplacementPublication, error) {
	if expected.Phase != ReplacementPublicationCleaning {
		return nil, ErrMirrorConflict
	}
	next := expected
	next.Phase = ReplacementPublicationCleaned
	next.Revision++
	return s.advanceReplacementCleanup(expected, next, verifyAbsentAndSynced)
}
