package metadata

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const onlineMutationSchema = `
CREATE TABLE IF NOT EXISTS online_mutations (
 op_id TEXT PRIMARY KEY,
 kind TEXT NOT NULL,
 path TEXT NOT NULL,
 store_id TEXT NOT NULL,
 incarnation TEXT NOT NULL,
 revision INTEGER NOT NULL,
 backing_identity TEXT NOT NULL,
 mount_generation TEXT NOT NULL,
 old_eof INTEGER NOT NULL,
 target_eof INTEGER NOT NULL,
 write_offset INTEGER NOT NULL,
 write_length INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_online_mutations_path ON online_mutations(path);
`

// OnlineMutation is an unresolved backing operation. BackingIdentity must be
// evidence from the open backing object, not a pathname. MountGeneration must
// identify the mount in which that evidence was captured. The mutation caller
// owns namespace ordering, backing verification and Sync before Complete.
type OnlineMutation struct {
	OpID            string
	Kind            string // "truncate" or "write"
	Path            string
	StoreID         string
	Incarnation     string
	Revision        int64
	BackingIdentity string
	MountGeneration string
	OldEOF          int64
	TargetEOF       int64
	WriteOffset     int64
	WriteLength     int64
}

// PendingOnlineMutation is a typed startup refusal. InspectOnlineMutations
// returns the complete records for operator-directed recovery; neither method
// repairs or deletes an intent.
type PendingOnlineMutation struct{ Mutations []OnlineMutation }

func (e *PendingOnlineMutation) Error() string {
	if len(e.Mutations) == 0 {
		return "online mutation requires reconciliation"
	}
	return fmt.Sprintf("online mutation requires reconciliation for %d operation(s), first %s at %q",
		len(e.Mutations), e.Mutations[0].OpID, e.Mutations[0].Path)
}

func validOnlineMutation(m OnlineMutation, persisted bool) error {
	if m.Path == "" || m.Path == "." || m.Path == ".." || strings.HasPrefix(m.Path, "../") ||
		strings.HasPrefix(m.Path, "/") || strings.ContainsRune(m.Path, 0) || path.Clean(m.Path) != m.Path ||
		m.BackingIdentity == "" || m.MountGeneration == "" || m.OldEOF < 0 || m.TargetEOF < 0 ||
		m.WriteOffset < 0 || m.WriteLength < 0 {
		return ErrMirrorConflict
	}
	if m.Kind == "truncate" {
		if m.WriteOffset != 0 || m.WriteLength != 0 {
			return ErrMirrorConflict
		}
	} else if m.Kind == "write" {
		if m.WriteLength == 0 || m.WriteOffset > int64(^uint64(0)>>1)-m.WriteLength ||
			m.TargetEOF != max(m.OldEOF, m.WriteOffset+m.WriteLength) {
			return ErrMirrorConflict
		}
	} else {
		return ErrMirrorConflict
	}
	if persisted {
		id, err := hex.DecodeString(m.OpID)
		if err != nil || len(id) != 16 || m.StoreID == "" || m.Incarnation == "" || m.Revision < 1 {
			return fmt.Errorf("malformed online mutation %q: %w", m.OpID, ErrMirrorConflict)
		}
	} else if m.OpID != "" || m.StoreID != "" || m.Incarnation != "" || m.Revision != 0 {
		return ErrMirrorConflict
	}
	return nil
}

func onlineMutationColumns() string {
	return `op_id,kind,path,store_id,incarnation,revision,backing_identity,mount_generation,old_eof,target_eof,write_offset,write_length`
}

func inspectOnlineMutations(db *sql.DB) ([]OnlineMutation, error) {
	rows, err := db.Query(`SELECT ` + onlineMutationColumns() + ` FROM online_mutations ORDER BY op_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []OnlineMutation
	for rows.Next() {
		var m OnlineMutation
		if err := rows.Scan(&m.OpID, &m.Kind, &m.Path, &m.StoreID, &m.Incarnation, &m.Revision,
			&m.BackingIdentity, &m.MountGeneration, &m.OldEOF, &m.TargetEOF, &m.WriteOffset, &m.WriteLength); err != nil {
			return nil, err
		}
		if err := validOnlineMutation(m, true); err != nil {
			return nil, err
		}
		pending = append(pending, m)
	}
	return pending, rows.Err()
}

func (s *Store) InspectOnlineMutations() ([]OnlineMutation, error) {
	return inspectOnlineMutations(s.db)
}

// InspectOnlineMutationsAt opens only the journal for offline recovery. It
// works when ordinary Store Open refuses a pending operation and never starts
// mirror migration, cache hydration, workers or a listener.
func InspectOnlineMutationsAt(dbPath string) ([]OnlineMutation, error) {
	if dbPath == "" || dbPath == ":memory:" {
		return nil, ErrMirrorConflict
	}
	absPath, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, err
	}
	dsn := (&url.URL{Scheme: "file", Path: absPath, RawQuery: "mode=ro"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return inspectOnlineMutations(db)
}

func (s *Store) CheckOnlineMutationRecovery() error {
	pending, err := s.InspectOnlineMutations()
	if err != nil {
		return fmt.Errorf("check online mutation recovery: %w", err)
	}
	if len(pending) != 0 {
		return &PendingOnlineMutation{Mutations: pending}
	}
	return nil
}

// beginOnlineMutationTx keeps the same SQLite connection pinned through the
// transaction. synchronous is connection-scoped, including under the NORMAL_ALL
// flag, so setting and verifying it on a pooled *DB is insufficient.
func (s *Store) beginOnlineMutationTx() (*sql.Tx, func(), error) {
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		return nil, nil, err
	}
	closeConn := func() { _ = conn.Close() }
	if _, err := conn.ExecContext(context.Background(), `PRAGMA synchronous=FULL`); err != nil {
		closeConn()
		return nil, nil, err
	}
	var mode int
	if err := conn.QueryRowContext(context.Background(), `PRAGMA synchronous`).Scan(&mode); err != nil {
		closeConn()
		return nil, nil, err
	}
	if mode != 2 {
		closeConn()
		return nil, nil, fmt.Errorf("online mutation requires SQLite synchronous=FULL, got %d", mode)
	}
	var journalMode string
	if err := conn.QueryRowContext(context.Background(), `PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		closeConn()
		return nil, nil, err
	}
	if !strings.EqualFold(journalMode, "wal") {
		closeConn()
		return nil, nil, fmt.Errorf("online mutation requires durable SQLite WAL, got %q", journalMode)
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		closeConn()
		return nil, nil, err
	}
	return tx, closeConn, nil
}

// BeginOnlineMutation commits the intent before its caller may mutate backing
// bytes. A captured present-row token and the durable store ID are required.
// Pending ancestor, descendant and same-path intents exclude a new operation.
func (s *Store) BeginOnlineMutation(expected MirrorToken, m OnlineMutation) (OnlineMutation, error) {
	if expected.Path != m.Path || expected.Incarnation == "" || validOnlineMutation(m, false) != nil {
		return OnlineMutation{}, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	cached := s.pathCache[m.Path]
	cachedConflict := cached != nil && cached.Version != expected
	s.mu.RUnlock()
	if cachedConflict {
		return OnlineMutation{}, ErrMirrorConflict
	}
	tx, closeConn, err := s.beginOnlineMutationTx()
	if err != nil {
		return OnlineMutation{}, err
	}
	defer closeConn()
	defer tx.Rollback()
	if err := validateExpected(tx, expected); err != nil {
		return OnlineMutation{}, err
	}
	var size int64
	var isDir int
	if err := tx.QueryRow(`SELECT size,is_dir FROM entries WHERE path=?`, m.Path).Scan(&size, &isDir); err != nil {
		return OnlineMutation{}, err
	}
	if isDir != 0 || size != m.OldEOF {
		return OnlineMutation{}, ErrMirrorConflict
	}
	var overlaps int
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM online_mutations WHERE path=?
 OR substr(path,1,length(?)+1)=?||'/'
 OR substr(?,1,length(path)+1)=path||'/')`, m.Path, m.Path, m.Path, m.Path).Scan(&overlaps); err != nil {
		return OnlineMutation{}, err
	}
	if overlaps != 0 {
		return OnlineMutation{}, ErrMirrorConflict
	}
	if err := tx.QueryRow(`SELECT value FROM store_meta WHERE key='mirror_store_id'`).Scan(&m.StoreID); err != nil {
		return OnlineMutation{}, err
	}
	if m.StoreID == "" {
		return OnlineMutation{}, ErrMirrorConflict
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return OnlineMutation{}, err
	}
	m.OpID = hex.EncodeToString(id[:])
	m.Incarnation, m.Revision = expected.Incarnation, expected.Revision
	res, err := tx.Exec(`INSERT INTO online_mutations (`+onlineMutationColumns()+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		m.OpID, m.Kind, m.Path, m.StoreID, m.Incarnation, m.Revision, m.BackingIdentity,
		m.MountGeneration, m.OldEOF, m.TargetEOF, m.WriteOffset, m.WriteLength)
	if err != nil {
		return OnlineMutation{}, err
	}
	if err := exactlyOneRow(res); err != nil {
		return OnlineMutation{}, err
	}
	var stored OnlineMutation
	err = tx.QueryRow(`SELECT `+onlineMutationColumns()+` FROM online_mutations WHERE op_id=?`, m.OpID).
		Scan(&stored.OpID, &stored.Kind, &stored.Path, &stored.StoreID, &stored.Incarnation,
			&stored.Revision, &stored.BackingIdentity, &stored.MountGeneration, &stored.OldEOF,
			&stored.TargetEOF, &stored.WriteOffset, &stored.WriteLength)
	if err != nil {
		return OnlineMutation{}, err
	}
	if stored != m {
		return OnlineMutation{}, ErrMirrorConflict
	}
	if err := tx.Commit(); err != nil {
		return OnlineMutation{}, err
	}
	return m, nil
}

// CompleteOnlineMutation must only be called after backing identity/EOF have
// been verified on the open descriptor and the backing mutation has been
// durably synced. Any uncertainty leaves the intent pending for inspection.
func (s *Store) CompleteOnlineMutation(m OnlineMutation, mtime time.Time) (*Entry, error) {
	if err := validOnlineMutation(m, true); err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	cached := s.pathCache[m.Path]
	cachedConflict := cached != nil && cached.Version != (MirrorToken{Path: m.Path, Incarnation: m.Incarnation, Revision: m.Revision})
	s.mu.RUnlock()
	if cachedConflict {
		return nil, ErrMirrorConflict
	}
	tx, closeConn, err := s.beginOnlineMutationTx()
	if err != nil {
		return nil, err
	}
	defer closeConn()
	defer tx.Rollback()
	var storeID string
	if err := tx.QueryRow(`SELECT value FROM store_meta WHERE key='mirror_store_id'`).Scan(&storeID); err != nil {
		return nil, err
	}
	if storeID != m.StoreID {
		return nil, ErrMirrorConflict
	}
	res, err := tx.Exec(`UPDATE entries SET size=?,mtime=?,mtime_nsec=NULL,revision=revision+1
 WHERE path=? AND incarnation=? AND revision=? AND size=? AND is_dir=0`,
		m.TargetEOF, mtime.Unix(), m.Path, m.Incarnation, m.Revision, m.OldEOF)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(res); err != nil {
		return nil, err
	}
	res, err = tx.Exec(`DELETE FROM online_mutations WHERE op_id=? AND kind=? AND path=? AND store_id=?
 AND incarnation=? AND revision=? AND backing_identity=? AND mount_generation=?
 AND old_eof=? AND target_eof=? AND write_offset=? AND write_length=?`,
		m.OpID, m.Kind, m.Path, m.StoreID, m.Incarnation, m.Revision, m.BackingIdentity,
		m.MountGeneration, m.OldEOF, m.TargetEOF, m.WriteOffset, m.WriteLength)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(res); err != nil {
		return nil, err
	}
	e, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, m.Path))
	if err != nil {
		return nil, err
	}
	if e.Size != m.TargetEOF || e.Version.Incarnation != m.Incarnation || e.Version.Revision != m.Revision+1 {
		return nil, ErrMirrorConflict
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.publishEntryLocked(e)
	s.mu.Unlock()
	return e.snapshot(), nil
}
