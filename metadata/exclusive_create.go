package metadata

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// exclusiveCreateSchema keeps the verifier of each file made by an NFSv3
// EXCLUSIVE CREATE until the name next changes. RFC 1813 section 3.3.8 has
// the server store it with the file so that a resend of that create (same
// verifier) succeeds while any other create of the name fails EEXIST. A
// resend can follow a restart or a lost reply, so the record is durable. It
// cannot ride on the file's times (the knfsd convention): a spool file has
// none on the backend yet, and the client's SETATTR after the create
// overwrites them, which is also when the record is cleared here.
//
// The triggers drop the record when the name's row goes away or moves,
// whoever does it (a remote delete through the keyspace included). An
// upsert of the same path (INSERT OR REPLACE without recursive_triggers)
// fires neither. The table and triggers are additive; a build that does not
// know them leaves them alone, and the triggers only touch this table.
const exclusiveCreateSchema = `
CREATE TABLE IF NOT EXISTS exclusive_creates (
 path TEXT PRIMARY KEY,
 verifier BLOB NOT NULL
);
CREATE TRIGGER IF NOT EXISTS exclusive_create_entry_delete AFTER DELETE ON entries
BEGIN DELETE FROM exclusive_creates WHERE path=OLD.path; END;
CREATE TRIGGER IF NOT EXISTS exclusive_create_entry_move AFTER UPDATE OF path ON entries
WHEN OLD.path <> NEW.path
BEGIN DELETE FROM exclusive_creates WHERE path=OLD.path OR path=NEW.path; END;
`

// exclusiveCreates is the in-memory superset of the table's paths, so the
// clear on every WRITE and SETATTR costs an atomic load while nothing is
// recorded. A trigger can drop a row behind it; the stale path only costs one
// no-op DELETE.
type exclusiveCreates struct {
	mu    sync.Mutex
	paths map[string]struct{}
	count atomic.Int64
}

func (s *Store) loadExclusiveCreates() error {
	rows, err := s.db.Query(`SELECT path FROM exclusive_creates`)
	if err != nil {
		return err
	}
	defer rows.Close()
	s.exclusive.paths = make(map[string]struct{})
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return err
		}
		s.exclusive.paths[p] = struct{}{}
	}
	s.exclusive.count.Store(int64(len(s.exclusive.paths)))
	return rows.Err()
}

// RecordExclusiveCreate remembers that an EXCLUSIVE CREATE with verifier made
// the file at path.
func (s *Store) RecordExclusiveCreate(path string, verifier [8]byte) error {
	c := &s.exclusive
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := s.db.Exec(`INSERT OR REPLACE INTO exclusive_creates(path, verifier) VALUES(?, ?)`, path, verifier[:]); err != nil {
		return fmt.Errorf("record exclusive create %q: %w", path, err)
	}
	if c.paths == nil {
		c.paths = make(map[string]struct{})
	}
	if _, ok := c.paths[path]; !ok {
		c.paths[path] = struct{}{}
		c.count.Add(1)
	}
	return nil
}

// ExclusiveCreateMatches reports whether the file at path was made by an
// EXCLUSIVE CREATE with verifier and the name has not changed since.
func (s *Store) ExclusiveCreateMatches(path string, verifier [8]byte) (bool, error) {
	if s.exclusive.count.Load() == 0 {
		return false, nil
	}
	var stored []byte
	err := s.db.QueryRow(`SELECT verifier FROM exclusive_creates WHERE path=?`, path).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read exclusive create %q: %w", path, err)
	}
	return bytes.Equal(stored, verifier[:]), nil
}

// ClearExclusiveCreate forgets the record for path. Every local mutation of
// the name calls it; without a record it does no I/O.
func (s *Store) ClearExclusiveCreate(path string) error {
	c := &s.exclusive
	if c.count.Load() == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.paths[path]; !ok {
		return nil
	}
	if _, err := s.db.Exec(`DELETE FROM exclusive_creates WHERE path=?`, path); err != nil {
		return fmt.Errorf("clear exclusive create %q: %w", path, err)
	}
	delete(c.paths, path)
	c.count.Add(-1)
	return nil
}
