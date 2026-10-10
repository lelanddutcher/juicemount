package metadata

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"
)

const preparedCreateSchema = `
CREATE TABLE IF NOT EXISTS prepared_creates (
 op_id TEXT PRIMARY KEY,
 path TEXT NOT NULL UNIQUE,
 store_id TEXT NOT NULL,
 mirror_epoch INTEGER NOT NULL,
 parent_path TEXT NOT NULL,
 parent_incarnation TEXT NOT NULL,
 mount_generation TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS prepared_create_entry_insert BEFORE INSERT ON entries
WHEN EXISTS(SELECT 1 FROM prepared_creates WHERE path=NEW.path)
BEGIN SELECT RAISE(ABORT, 'prepared create owns path'); END;
CREATE TRIGGER IF NOT EXISTS prepared_create_entry_move BEFORE UPDATE OF path ON entries
WHEN EXISTS(SELECT 1 FROM prepared_creates WHERE path=NEW.path)
BEGIN SELECT RAISE(ABORT, 'prepared create owns path'); END;
`

// PreparedCreate reserves an absent mirror name before an external CREATE.
// It is not permission to mutate a different mount or a remote-writer fence.
type PreparedCreate struct {
	OpID            string
	Path            string
	StoreID         string
	MirrorEpoch     int64
	ParentPath      string
	ParentIdentity  string
	MountGeneration string
}

type PendingPreparedCreate struct{ Creates []PreparedCreate }

func (e *PendingPreparedCreate) Error() string {
	if len(e.Creates) == 0 {
		return "prepared create requires reconciliation"
	}
	return fmt.Sprintf("prepared create requires reconciliation for %d operation(s), first %s at %q",
		len(e.Creates), e.Creates[0].OpID, e.Creates[0].Path)
}

func validPreparedCreate(c PreparedCreate) bool {
	if c.Path == "" || c.Path == "." || c.Path == ".." || strings.HasPrefix(c.Path, "../") ||
		strings.HasPrefix(c.Path, "/") || strings.ContainsRune(c.Path, 0) || path.Clean(c.Path) != c.Path ||
		c.StoreID == "" || c.MirrorEpoch < 0 || c.ParentPath != path.Dir(c.Path) ||
		c.ParentIdentity == "" || c.MountGeneration == "" {
		return false
	}
	id, err := hex.DecodeString(c.OpID)
	return err == nil && len(id) == 16
}

func inspectPreparedCreates(db *sql.DB) ([]PreparedCreate, error) {
	rows, err := db.Query(`SELECT op_id,path,store_id,mirror_epoch,parent_path,parent_incarnation,mount_generation FROM prepared_creates ORDER BY op_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []PreparedCreate
	for rows.Next() {
		var c PreparedCreate
		if err := rows.Scan(&c.OpID, &c.Path, &c.StoreID, &c.MirrorEpoch,
			&c.ParentPath, &c.ParentIdentity, &c.MountGeneration); err != nil {
			return nil, err
		}
		if !validPreparedCreate(c) {
			return nil, ErrMirrorConflict
		}
		pending = append(pending, c)
	}
	return pending, rows.Err()
}

func (s *Store) InspectPreparedCreates() ([]PreparedCreate, error) {
	return inspectPreparedCreates(s.db)
}

// InspectPreparedCreatesAt reads pending names without starting a Store. It
// remains available when Open correctly refuses unresolved CREATE custody.
func InspectPreparedCreatesAt(dbPath string) ([]PreparedCreate, error) {
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
	return inspectPreparedCreates(db)
}

// PrepareCreate commits custody before the caller may issue O_EXCL. The
// namespace owner must keep overlapping reads and writes excluded until it
// completes or definitively aborts this operation.
func (s *Store) PrepareCreate(expected MirrorToken, mountGeneration string) (PreparedCreate, error) {
	if expected.Path == "" || expected.Incarnation != "" || expected.StoreID == "" || mountGeneration == "" {
		return PreparedCreate{}, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	cachedPresent := s.pathCache[expected.Path] != nil
	s.mu.RUnlock()
	if cachedPresent {
		return PreparedCreate{}, ErrMirrorConflict
	}
	tx, closeConn, err := s.beginOnlineMutationTx()
	if err != nil {
		return PreparedCreate{}, err
	}
	defer closeConn()
	defer tx.Rollback()
	if err := validateExpected(tx, expected); err != nil {
		return PreparedCreate{}, err
	}
	parentPath := path.Dir(expected.Path)
	parentIdentity, err := preparedCreateParentIdentity(tx, parentPath)
	if err != nil {
		return PreparedCreate{}, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return PreparedCreate{}, err
	}
	c := PreparedCreate{OpID: hex.EncodeToString(id[:]), Path: expected.Path,
		StoreID: expected.StoreID, MirrorEpoch: expected.Epoch, ParentPath: parentPath,
		ParentIdentity: parentIdentity, MountGeneration: mountGeneration}
	if !validPreparedCreate(c) {
		return PreparedCreate{}, ErrMirrorConflict
	}
	res, err := tx.Exec(`INSERT INTO prepared_creates(op_id,path,store_id,mirror_epoch,parent_path,parent_incarnation,mount_generation) VALUES(?,?,?,?,?,?,?)`,
		c.OpID, c.Path, c.StoreID, c.MirrorEpoch, c.ParentPath, c.ParentIdentity, c.MountGeneration)
	if err != nil {
		return PreparedCreate{}, err
	}
	if err := exactlyOneRow(res); err != nil {
		return PreparedCreate{}, err
	}
	var stored PreparedCreate
	if err := tx.QueryRow(`SELECT op_id,path,store_id,mirror_epoch,parent_path,parent_incarnation,mount_generation FROM prepared_creates WHERE op_id=?`, c.OpID).
		Scan(&stored.OpID, &stored.Path, &stored.StoreID, &stored.MirrorEpoch,
			&stored.ParentPath, &stored.ParentIdentity, &stored.MountGeneration); err != nil {
		return PreparedCreate{}, err
	}
	if stored != c {
		return PreparedCreate{}, ErrMirrorConflict
	}
	if err := tx.Commit(); err != nil {
		return PreparedCreate{}, err
	}
	s.mu.Lock()
	s.preparedCreates[c.Path] = c
	s.cacheGeneration++
	s.mu.Unlock()
	return c, nil
}

func (s *Store) validatePreparedCreate(tx *sql.Tx, c, cached PreparedCreate) error {
	if !validPreparedCreate(c) || cached != c {
		return ErrMirrorConflict
	}
	var stored PreparedCreate
	err := tx.QueryRow(`SELECT op_id,path,store_id,mirror_epoch,parent_path,parent_incarnation,mount_generation FROM prepared_creates WHERE op_id=?`, c.OpID).
		Scan(&stored.OpID, &stored.Path, &stored.StoreID, &stored.MirrorEpoch,
			&stored.ParentPath, &stored.ParentIdentity, &stored.MountGeneration)
	if err != nil {
		return err
	}
	if stored != c {
		return ErrMirrorConflict
	}
	return nil
}

// AbortPreparedCreate is only for a definitive no-mutation outcome, such as
// O_EXCL returning EEXIST. Ambiguous failures must leave the record pending.
func (s *Store) AbortPreparedCreate(c PreparedCreate) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	cached := s.preparedCreates[c.Path]
	s.mu.RUnlock()
	tx, closeConn, err := s.beginOnlineMutationTx()
	if err != nil {
		return err
	}
	defer closeConn()
	defer tx.Rollback()
	if err := s.validatePreparedCreate(tx, c, cached); err != nil {
		return err
	}
	res, err := tx.Exec(`DELETE FROM prepared_creates WHERE op_id=? AND path=?`, c.OpID, c.Path)
	if err != nil {
		return err
	}
	if err := exactlyOneRow(res); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.preparedCreates, c.Path)
	s.cacheGeneration++
	s.mu.Unlock()
	return nil
}

// CompletePreparedCreate publishes the mirror and retires custody atomically.
// The caller must first verify the open backing object and mount identity.
func (s *Store) CompletePreparedCreate(c PreparedCreate, entry *Entry) (*Entry, error) {
	if entry == nil || entry.Path != c.Path || entry.IsDir || !entry.Version.IsZero() || StreamPartialName(entry.Name) {
		return nil, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	cachedPresent := s.pathCache[c.Path] != nil
	cachedReservation := s.preparedCreates[c.Path]
	s.mu.RUnlock()
	if cachedPresent {
		return nil, ErrMirrorConflict
	}
	tx, closeConn, err := s.beginOnlineMutationTx()
	if err != nil {
		return nil, err
	}
	defer closeConn()
	defer tx.Rollback()
	if err := s.validatePreparedCreate(tx, c, cachedReservation); err != nil {
		return nil, err
	}
	var storeID string
	if err := tx.QueryRow(`SELECT value FROM store_meta WHERE key='mirror_store_id'`).Scan(&storeID); err != nil {
		return nil, err
	}
	if storeID != c.StoreID {
		return nil, ErrMirrorConflict
	}
	var occupied int
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM entries WHERE path=?)`, c.Path).Scan(&occupied); err != nil {
		return nil, err
	}
	if occupied != 0 {
		return nil, ErrMirrorConflict
	}
	parentIdentity, err := preparedCreateParentIdentity(tx, c.ParentPath)
	if err != nil || parentIdentity != c.ParentIdentity {
		if err != nil {
			return nil, err
		}
		return nil, ErrMirrorConflict
	}
	res, err := tx.Exec(`DELETE FROM prepared_creates WHERE op_id=? AND path=?`, c.OpID, c.Path)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(res); err != nil {
		return nil, err
	}
	e := entry.snapshot()
	e.ResetGetAttrCache()
	if err := ftsExternalUpsert(tx, e, s.ftsDefer); err != nil {
		return nil, err
	}
	if err := loadEntryVersion(tx, e); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	delete(s.preparedCreates, c.Path)
	s.cacheGeneration++
	s.publishEntryLocked(e)
	s.mu.Unlock()
	s.wakeFTSCompactor()
	return e.snapshot(), nil
}

func (s *Store) preparedCreateConflictLocked(name string) bool {
	for reserved := range s.preparedCreates {
		if name == reserved || strings.HasPrefix(name, reserved+"/") || strings.HasPrefix(reserved, name+"/") {
			return true
		}
	}
	return false
}

func preparedCreateParentIdentity(tx *sql.Tx, parentPath string) (string, error) {
	if parentPath == "." {
		return "root", nil
	}
	var incarnation string
	var isDir int
	if err := tx.QueryRow(`SELECT incarnation,is_dir FROM entries WHERE path=?`, parentPath).
		Scan(&incarnation, &isDir); err != nil {
		return "", err
	}
	if incarnation == "" || isDir != 1 {
		return "", ErrMirrorConflict
	}
	return incarnation, nil
}
