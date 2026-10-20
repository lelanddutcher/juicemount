package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	_ "modernc.org/sqlite"
)

// Store owns one immediate-transaction write pool. Point reads are standalone
// queries. A future snapshot exporter must own a separate deferred read pool.
type Store struct {
	mu       sync.RWMutex
	db       *sql.DB
	file     *os.File
	path     string
	library  continuity.LibraryID
	closed   bool
	closeErr error
}

// Create reserves a new 0600 database in a caller-provisioned private directory.
// It never removes or replaces the reserved file. Incomplete files refuse Open;
// a final commit/sync error may be uncertain, so recover via Open and check the
// exact library identity. An existing destination is never adopted by Create.
func Create(path string, library continuity.LibraryID) (*Store, error) {
	if err := library.Validate(); err != nil {
		return nil, invalid("library identity")
	}
	return openStore(path, library, true)
}

// Open recognizes an existing complete catalog; it never initializes or repairs.
func Open(path string) (*Store, error) { return openStore(path, "", false) }

func canonicalPath(path string) (string, error) {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") || strings.ContainsRune(path, 0) {
		return "", invalid("a filesystem catalog path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("%w: path: %v", ErrInvalid, err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", fmt.Errorf("catalog parent: %w", err)
	}
	return filepath.Join(parent, filepath.Base(abs)), nil
}

func checkSidecars(path string) error {
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		info, err := os.Lstat(path + suffix)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if err := checkFileIdentity(info); err != nil {
			return err
		}
	}
	return nil
}

func openDB(path string, readonly bool) (*sql.DB, error) {
	u := url.URL{Scheme: "file", Path: path}
	query := url.Values{}
	query.Set("mode", "rw")
	if readonly {
		query.Set("mode", "ro")
	} else {
		query.Set("_txlock", "immediate")
	}
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "busy_timeout(2000)")
	query.Add("_pragma", "synchronous(FULL)")
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func openStore(input string, library continuity.LibraryID, create bool) (_ *Store, resultErr error) {
	path, err := canonicalPath(input)
	if err != nil {
		return nil, err
	}
	if create {
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			if _, err := os.Lstat(path + suffix); err == nil {
				return nil, fmt.Errorf("%w: preexisting catalog sidecar", ErrConflict)
			} else if !os.IsNotExist(err) {
				return nil, err
			}
		}
	}
	if err := checkSidecars(path); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(path); err == nil {
		if err := checkFileIdentity(info); err != nil {
			return nil, err
		}
		if create {
			return nil, fmt.Errorf("%w: destination exists", ErrConflict)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	} else if !create {
		return nil, fmt.Errorf("%w: catalog path: %w", ErrNotFound, err)
	}
	file, err := openRegular(path, create)
	if err != nil {
		if create && os.IsExist(err) {
			return nil, fmt.Errorf("%w: destination exists", ErrConflict)
		}
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			file.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if !create {
		// Refuse unrecognized databases before any writable SQLite connection.
		probe, err := openDB(path, true)
		if err != nil {
			return nil, formatError(err)
		}
		library, err = validateSchema(ctx, probe)
		if err == nil {
			err = checkIntegrity(ctx, probe)
		}
		closeErr := probe.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	db, err := openDB(path, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			db.Close()
		}
	}()
	if create {
		if err := initialize(ctx, db, library); err != nil {
			return nil, err
		}
	}
	actual, err := validateSchemaState(ctx, db, create)
	if err != nil {
		return nil, err
	}
	if actual != library {
		return nil, corrupt("library changed while opening")
	}
	if err := checkIntegrity(ctx, db); err != nil {
		return nil, err
	}
	if err := checkSettings(ctx, db); err != nil {
		return nil, err
	}
	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&mode); err != nil {
		return nil, err
	}
	if mode != "wal" {
		return nil, corrupt("WAL unavailable")
	}
	store := &Store{db: db, file: file, path: path, library: library}
	if err := store.checkPaths(); err != nil {
		return nil, err
	}
	if create {
		if err := file.Sync(); err != nil {
			return nil, err
		}
		if err := syncParent(path); err != nil {
			return nil, err
		}
		// Readiness is the final atomic initialization step, after directory sync.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0`); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		if err := syncParent(path); err != nil {
			return nil, fmt.Errorf("catalog initialization outcome uncertain: %w", err)
		}
	}
	return store, nil
}

func syncParent(path string) error {
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(parent.Sync(), parent.Close())
}

func checkSettings(ctx context.Context, q queryer) error {
	for _, setting := range []struct {
		query string
		want  int
	}{
		{"PRAGMA foreign_keys", 1}, {"PRAGMA busy_timeout", 2000}, {"PRAGMA synchronous", 2},
	} {
		var got int
		if err := q.QueryRowContext(ctx, setting.query).Scan(&got); err != nil {
			return err
		}
		if got != setting.want {
			return corrupt("connection setting mismatch: " + setting.query)
		}
	}
	return nil
}

func (s *Store) checkPaths() error {
	info, err := os.Lstat(s.path)
	if err != nil {
		return formatError(err)
	}
	if err := checkFileIdentity(info); err != nil {
		return err
	}
	held, err := s.file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, held) {
		return corrupt("catalog path identity changed")
	}
	return checkSidecars(s.path)
}

func (s *Store) enter() error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrClosed
	}
	if err := s.checkPaths(); err != nil {
		s.mu.RUnlock()
		return err
	}
	return nil
}

func (s *Store) leave() { s.mu.RUnlock() }

// Close refuses new work and drains admitted calls before closing both owners.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.closeErr = errors.Join(s.db.Close(), s.file.Close())
	}
	return s.closeErr
}

func (s *Store) LibraryID() (continuity.LibraryID, error) {
	if err := s.enter(); err != nil {
		return "", err
	}
	defer s.leave()
	return s.library, nil
}

func (s *Store) requireLibrary(library continuity.LibraryID) error {
	if library.Validate() != nil || library != s.library {
		return invalid("library mismatch")
	}
	return nil
}
