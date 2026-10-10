package metadata

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"strings"
	"sync"
	"time"
)

type cachePathContextQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// No waiter goroutine is left behind on deadline. The acquired lock is
// released if cancellation races the final successful TryLock/TryRLock.
func cachePathLockContext(ctx context.Context, try func() bool, unlock func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if try() {
		if err := ctx.Err(); err != nil {
			unlock()
			return err
		}
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if try() {
				if err := ctx.Err(); err != nil {
					unlock()
					return err
				}
				return nil
			}
		}
	}
}

func captureCachePathEpoch(ctx context.Context, q cachePathContextQuerier, generation uint64) (StoreEpoch, error) {
	v := StoreEpoch{CacheGeneration: generation}
	err := q.QueryRowContext(ctx, `SELECT
 (SELECT value FROM store_meta WHERE key='mirror_store_id'),
 (SELECT CAST(value AS INTEGER) FROM store_meta WHERE key='mirror_epoch')`).Scan(&v.StoreID, &v.Epoch)
	return v, err
}

// CachePathBinding is supplied by the qualified backend session. Digest is
// the SHA-256 identity of its selected backend, not a path to a cache volume.
// UUID alone cannot distinguish a dataset restored with the same format UUID.
type CachePathBinding struct {
	Digest     string
	FormatUUID string
}

// CachePathNode is a durable local mirror observation, captured before Redis
// ReadParentPathProof. The implicit root has inode 1 and no entries row.
type CachePathNode struct {
	Path, ParentPath string
	Inode            uint64
	Size             int64
	MtimeSec         int64
	MtimeNsec        uint32
	IsDir            bool
	Token            MirrorToken
}

type CachePathObservation struct {
	Path  string
	Epoch StoreEpoch
	Chain []CachePathNode // first component through leaf, excluding implicit root
}

// ExpectedInodes is the full edge sequence required by ReadParentPathProof.
func (o CachePathObservation) ExpectedInodes() []uint64 {
	ids := make([]uint64, len(o.Chain))
	for i, node := range o.Chain {
		ids[i] = node.Inode
	}
	return ids
}

type cachePathRecord struct {
	StoreID, Digest, FormatUUID, Path string
	Chain                             []CachePathNode
}

// CachePathEligibility owns an additive SQLite index and its RAM hot index.
// It is explicitly attached to one Store; constructing it may use SQL, while
// CachePathEligible never uses SQL, network, or filesystem I/O.
type CachePathEligibility struct {
	store   *Store
	storeID string
	mu      sync.RWMutex
	records map[string]cachePathRecord
}

var ErrCachePathIneligible = errors.New("cache path is not qualified")

const cachePathSchema = `CREATE TABLE IF NOT EXISTS cache_path_eligibility (
 path TEXT PRIMARY KEY,
 store_id TEXT NOT NULL,
 backend_digest TEXT NOT NULL,
 format_uuid TEXT NOT NULL,
 chain_json BLOB NOT NULL
)`

func OpenCachePathEligibility(s *Store) (*CachePathEligibility, error) {
	if s == nil {
		return nil, ErrCachePathIneligible
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.db.Exec(cachePathSchema); err != nil {
		return nil, err
	}
	epoch, err := captureStoreEpoch(s.db, 0)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT path, store_id, backend_digest, format_uuid, chain_json FROM cache_path_eligibility`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	index := &CachePathEligibility{store: s, storeID: epoch.StoreID, records: make(map[string]cachePathRecord)}
	for rows.Next() {
		var rec cachePathRecord
		var encoded []byte
		if err := rows.Scan(&rec.Path, &rec.StoreID, &rec.Digest, &rec.FormatUUID, &encoded); err != nil {
			return nil, err
		}
		if rec.StoreID != epoch.StoreID || json.Unmarshal(encoded, &rec.Chain) != nil || !validCachePathChain(rec.Path, rec.Chain) {
			continue
		}
		index.records[rec.Path] = rec
	}
	return index, rows.Err()
}

func validCachePathBinding(b CachePathBinding) bool {
	if len(b.Digest) != 64 || b.FormatUUID == "" || len(b.FormatUUID) > 128 || strings.ContainsRune(b.FormatUUID, 0) {
		return false
	}
	decoded, err := hex.DecodeString(b.Digest)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == b.Digest
}

func validCachePathChain(name string, chain []CachePathNode) bool {
	ids := make([]uint64, len(chain))
	for i, node := range chain {
		ids[i] = node.Inode
	}
	parts, err := readProofParts(name, ids)
	if err != nil || len(parts) != len(chain) {
		return false
	}
	prefix, parent := "", "."
	for i, part := range parts {
		if prefix == "" {
			prefix = part
		} else {
			prefix += "/" + part
		}
		n := chain[i]
		if n.Path != prefix || n.ParentPath != parent || n.IsDir != (i != len(chain)-1) || n.MtimeNsec >= 1e9 || n.Size < 0 || n.Token.Path != prefix || n.Token.Incarnation == "" || n.Token.Revision <= 0 || n.Token.StoreID != "" || n.Token.Epoch != 0 {
			return false
		}
		parent = prefix
	}
	return true
}

func cachePathNodeFromEntry(e *Entry) (CachePathNode, bool) {
	if e == nil || e.LocalOnly || !e.MtimeNsecKnown || e.Inode <= 1 || e.Size < 0 || e.Version.Incarnation == "" || e.Version.Revision <= 0 || e.Version.Path != e.Path || e.Version.StoreID != "" || e.Version.Epoch != 0 {
		return CachePathNode{}, false
	}
	// IsDir alone cannot distinguish a regular file from a symlink, socket,
	// device, or named pipe. The Redis proof only admits a regular leaf.
	if (e.IsDir && e.Mode.Type() != fs.ModeDir) || (!e.IsDir && e.Mode.Type() != 0) {
		return CachePathNode{}, false
	}
	return CachePathNode{Path: e.Path, ParentPath: e.ParentPath, Inode: e.Inode, Size: e.Size, MtimeSec: e.Mtime.Unix(), MtimeNsec: uint32(e.Mtime.Nanosecond()), IsDir: e.IsDir, Token: e.Version}, true
}

func equalCachePathNode(a, b CachePathNode) bool { return a == b }

func durableCachePathNode(q versionQuerier, name string) (CachePathNode, error) {
	cq, ok := q.(cachePathContextQuerier)
	if !ok {
		return CachePathNode{}, ErrCachePathIneligible
	}
	return durableCachePathNodeContext(context.Background(), cq, name)
}

func durableCachePathNodeContext(ctx context.Context, q cachePathContextQuerier, name string) (CachePathNode, error) {
	var n CachePathNode
	var inode, size, seconds, nanos, mode, revision int64
	var isDir, localOnly int
	var incarnation string
	err := q.QueryRowContext(ctx, `SELECT parent_path,inode,size,mtime,mtime_nsec,is_dir,mode,local_only,incarnation,revision FROM entries WHERE path=?`, name).
		Scan(&n.ParentPath, &inode, &size, &seconds, &nanos, &isDir, &mode, &localOnly, &incarnation, &revision)
	if err != nil {
		return CachePathNode{}, err
	}
	if inode <= 1 || size < 0 || nanos < 0 || nanos >= 1e9 || localOnly != 0 || (isDir != 0 && isDir != 1) || mode < 0 || mode > int64(^uint32(0)) || incarnation == "" || revision <= 0 {
		return CachePathNode{}, ErrCachePathIneligible
	}
	storedType := fs.FileMode(uint32(mode)).Type()
	if (isDir == 1 && storedType != fs.ModeDir) || (isDir == 0 && storedType != 0) {
		return CachePathNode{}, ErrCachePathIneligible
	}
	n.Path, n.Inode, n.Size, n.MtimeSec, n.MtimeNsec, n.IsDir = name, uint64(inode), size, seconds, uint32(nanos), isDir == 1
	n.Token = MirrorToken{Path: name, Incarnation: incarnation, Revision: revision}
	return n, nil
}

// CaptureCachePath fences the complete local chain before any external proof.
// A root or locally provisional entry cannot become eligible.
func (g *CachePathEligibility) CaptureCachePath(name string) (CachePathObservation, error) {
	return g.CaptureCachePathContext(context.Background(), name)
}

// CaptureCachePathContext cancels lock admission and SQLite queries using ctx.
// It owns no detached work: cancellation returns only after held locks are
// released and the driver has completed or canceled the synchronous query.
func (g *CachePathEligibility) CaptureCachePathContext(ctx context.Context, name string) (CachePathObservation, error) {
	if g == nil || g.store == nil {
		return CachePathObservation{}, ErrCachePathIneligible
	}
	if err := ctx.Err(); err != nil {
		return CachePathObservation{}, err
	}
	parts, err := readProofParts(name, makeDummyCachePathIDs(name))
	if err != nil {
		return CachePathObservation{}, ErrCachePathIneligible
	}
	s := g.store
	if err := cachePathLockContext(ctx, s.writeMu.TryLock, s.writeMu.Unlock); err != nil {
		return CachePathObservation{}, err
	}
	defer s.writeMu.Unlock()
	if err := cachePathLockContext(ctx, s.cachePublicationMu.TryLock, s.cachePublicationMu.Unlock); err != nil {
		return CachePathObservation{}, err
	}
	defer s.cachePublicationMu.Unlock()
	if err := cachePathLockContext(ctx, s.mu.TryRLock, s.mu.RUnlock); err != nil {
		return CachePathObservation{}, err
	}
	generation := s.cacheGeneration
	out := CachePathObservation{Path: name, Chain: make([]CachePathNode, 0, len(parts))}
	prefix := ""
	for _, part := range parts {
		if prefix == "" {
			prefix = part
		} else {
			prefix += "/" + part
		}
		cached, ok := cachePathNodeFromEntry(s.pathCache[prefix])
		if !ok {
			s.mu.RUnlock()
			return CachePathObservation{}, ErrCachePathIneligible
		}
		out.Chain = append(out.Chain, cached)
	}
	s.mu.RUnlock()
	if !validCachePathChain(name, out.Chain) {
		return CachePathObservation{}, ErrCachePathIneligible
	}
	// writeMu and cachePublicationMu keep both durable writes and cache-only
	// publications fenced while SQL runs. No Store RAM lock crosses SQL.
	epoch, err := captureCachePathEpoch(ctx, s.db, generation)
	if err != nil {
		return CachePathObservation{}, err
	}
	for _, cached := range out.Chain {
		durable, err := durableCachePathNodeContext(ctx, s.db, cached.Path)
		if ctx.Err() != nil {
			return CachePathObservation{}, ctx.Err()
		}
		if err != nil || !equalCachePathNode(cached, durable) {
			return CachePathObservation{}, ErrCachePathIneligible
		}
	}
	out.Epoch = epoch
	if err := ctx.Err(); err != nil {
		return CachePathObservation{}, err
	}
	return out, nil
}

// This uses synthetic IDs only to apply the path grammar before reading rows;
// actual IDs are validated after capture and supplied to Redis separately.
func makeDummyCachePathIDs(name string) []uint64 {
	return func() []uint64 {
		parts := strings.Split(name, "/")
		ids := make([]uint64, len(parts))
		for i := range ids {
			ids[i] = uint64(i + 2)
		}
		return ids
	}()
}

func proofMatchesCachePath(b CachePathBinding, o CachePathObservation, proof RedisPathProof) bool {
	if !validCachePathBinding(b) || !validCachePathChain(o.Path, o.Chain) || o.Epoch.StoreID == "" || proof.FormatUUID != b.FormatUUID || proof.Path != o.Path || proof.Root.Path != "." || proof.Root.Inode != 1 || proof.Root.Type != jfsTypeDir || len(proof.Nodes) != len(o.Chain) {
		return false
	}
	parent := uint64(1)
	for i, n := range o.Chain {
		p := proof.Nodes[i]
		typ := uint8(jfsTypeFile)
		if n.IsDir {
			typ = jfsTypeDir
		}
		if p.Path != n.Path || p.ParentInode != parent || p.Inode != n.Inode || p.Size > uint64(^uint64(0)>>1) || int64(p.Size) != n.Size || p.Type != typ || p.MtimeSec != n.MtimeSec || p.MtimeNsec != n.MtimeNsec {
			return false
		}
		parent = n.Inode
	}
	return true
}

// ConfirmCachePath persists one path's eligibility only after the caller has
// obtained ReadParentPathProof outside Store locks. Publication rechecks both
// RAM and durable chain, plus the captured StoreEpoch, in one SQLite tx.
func (g *CachePathEligibility) ConfirmCachePath(b CachePathBinding, o CachePathObservation, proof RedisPathProof) error {
	return g.ConfirmCachePathContext(context.Background(), b, o, proof)
}

// ConfirmCachePathContext cancels admission and queries using ctx. Transaction
// settlement is synchronous and may outlast ctx; request callers must own and
// join that work. A committed row precedes RAM publication. No RAM lock crosses SQL.
func (g *CachePathEligibility) ConfirmCachePathContext(ctx context.Context, b CachePathBinding, o CachePathObservation, proof RedisPathProof) error {
	if g == nil || g.store == nil || !proofMatchesCachePath(b, o, proof) {
		return ErrCachePathIneligible
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s := g.store
	if err := cachePathLockContext(ctx, s.writeMu.TryLock, s.writeMu.Unlock); err != nil {
		return err
	}
	defer s.writeMu.Unlock()
	if err := cachePathLockContext(ctx, s.cachePublicationMu.TryLock, s.cachePublicationMu.Unlock); err != nil {
		return err
	}
	defer s.cachePublicationMu.Unlock()
	if err := cachePathLockContext(ctx, s.mu.TryRLock, s.mu.RUnlock); err != nil {
		return err
	}
	generation := s.cacheGeneration
	for _, want := range o.Chain {
		got, ok := cachePathNodeFromEntry(s.pathCache[want.Path])
		if !ok || !equalCachePathNode(got, want) {
			s.mu.RUnlock()
			return ErrCachePathIneligible
		}
	}
	s.mu.RUnlock()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// Explicit ownership avoids database/sql's cancellation goroutine winning
	// Rollback and continuing after this method releases Store's write locks.
	// Individual queries still use ctx. Commit/Rollback are joined here.
	tx, err := conn.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := captureCachePathEpoch(ctx, tx, generation)
	if err != nil {
		return err
	}
	if current != o.Epoch {
		return ErrCachePathIneligible
	}
	for _, want := range o.Chain {
		got, err := durableCachePathNodeContext(ctx, tx, want.Path)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || !equalCachePathNode(got, want) {
			return ErrCachePathIneligible
		}
	}
	encoded, err := json.Marshal(o.Chain)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO cache_path_eligibility(path,store_id,backend_digest,format_uuid,chain_json) VALUES(?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET store_id=excluded.store_id,backend_digest=excluded.backend_digest,format_uuid=excluded.format_uuid,chain_json=excluded.chain_json`, o.Path, o.Epoch.StoreID, b.Digest, b.FormatUUID, encoded)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	rec := cachePathRecord{StoreID: o.Epoch.StoreID, Digest: b.Digest, FormatUUID: b.FormatUUID, Path: o.Path, Chain: append([]CachePathNode(nil), o.Chain...)}
	if err := cachePathLockContext(ctx, g.mu.TryLock, g.mu.Unlock); err != nil {
		return err
	}
	g.records[o.Path] = rec
	g.mu.Unlock()
	return nil
}

// CachePathEligible is RAM only. current must be the caller's retained file
// identity, not a fresh lookup here: a renamed/replaced open handle must not
// inherit the successor's qualified path. All ancestors must remain resident.
func (g *CachePathEligibility) CachePathEligible(b CachePathBinding, current CachedFileIdentity) bool {
	if g == nil || g.store == nil || !validCachePathBinding(b) || current.Path == "" {
		return false
	}
	s := g.store
	s.mu.RLock()
	defer s.mu.RUnlock()
	g.mu.RLock()
	rec, ok := g.records[current.Path]
	g.mu.RUnlock()
	if !ok || rec.StoreID != g.storeID || rec.Digest != b.Digest || rec.FormatUUID != b.FormatUUID || rec.Path != current.Path || len(rec.Chain) == 0 {
		return false
	}
	for _, want := range rec.Chain {
		got, ok := cachePathNodeFromEntry(s.pathCache[want.Path])
		if !ok || !equalCachePathNode(got, want) {
			return false
		}
	}
	leaf := s.pathCache[current.Path]
	return leaf != nil && !leaf.IsDir && leaf.cacheIdentity != 0 && current.Publication == leaf.cacheIdentity && current.Version == leaf.Version && current.Inode == leaf.Inode && current.Size == leaf.Size && current.MtimeNS == leaf.Mtime.UnixNano()
}

// SnapshotEligibleCachePath copies the already-qualified RAM ancestry under
// the same locks used by CachePathEligible. This is a local continuity witness,
// not a new remote proof or permission to call ConfirmCachePath. In particular,
// no SQL or network work is performed by a foreground read.
func (g *CachePathEligibility) SnapshotEligibleCachePath(b CachePathBinding, current CachedFileIdentity) (CachePathObservation, bool) {
	if g == nil || g.store == nil || !validCachePathBinding(b) || current.Path == "" {
		return CachePathObservation{}, false
	}
	s := g.store
	s.mu.RLock()
	defer s.mu.RUnlock()
	g.mu.RLock()
	defer g.mu.RUnlock()
	rec, ok := g.records[current.Path]
	if !ok || rec.StoreID != g.storeID || rec.Digest != b.Digest || rec.FormatUUID != b.FormatUUID || rec.Path != current.Path || len(rec.Chain) == 0 {
		return CachePathObservation{}, false
	}
	for _, want := range rec.Chain {
		got, ok := cachePathNodeFromEntry(s.pathCache[want.Path])
		if !ok || !equalCachePathNode(got, want) {
			return CachePathObservation{}, false
		}
	}
	leaf := s.pathCache[current.Path]
	if leaf == nil || leaf.IsDir || leaf.cacheIdentity == 0 || current.Publication != leaf.cacheIdentity || current.Version != leaf.Version || current.Inode != leaf.Inode || current.Size != leaf.Size || current.MtimeNS != leaf.Mtime.UnixNano() {
		return CachePathObservation{}, false
	}
	return CachePathObservation{Path: current.Path, Chain: append([]CachePathNode(nil), rec.Chain...)}, true
}

// CachePathObservationCurrent is a RAM-only postcondition for an in-flight
// remote proof or Reader attempt. It does not confer eligibility: the caller
// must separately compare its retained leaf CachedFileIdentity and session
// epoch. A changed ancestor is visible here even when the leaf is unchanged.
func (g *CachePathEligibility) CachePathObservationCurrent(o CachePathObservation) bool {
	if g == nil || g.store == nil || !validCachePathChain(o.Path, o.Chain) {
		return false
	}
	s := g.store
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, want := range o.Chain {
		got, ok := cachePathNodeFromEntry(s.pathCache[want.Path])
		if !ok || !equalCachePathNode(got, want) {
			return false
		}
	}
	return true
}
