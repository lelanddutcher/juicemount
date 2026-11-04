package metadata

import (
	"database/sql"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// exclusiveCreateSchema keeps the verifier of each file made by an NFSv3
// EXCLUSIVE CREATE. RFC 1813 section 3.3.8 has the server store it with the
// file so that a resend of that create (same verifier) succeeds while any
// other create of the name fails EEXIST. A resend can follow a restart or a
// lost reply, so the record is durable. It cannot ride on the file's times
// (the knfsd convention): a spool file has none on the backend yet, and the
// client's SETATTR after the create overwrites them.
//
// The table is additive; a build that does not know it leaves it alone.
const exclusiveCreateSchema = `
CREATE TABLE IF NOT EXISTS exclusive_create_verifiers (
 path TEXT PRIMARY KEY,
 verifier BLOB NOT NULL,
 inode INTEGER NOT NULL,
 backend_inode INTEGER NOT NULL,
 created_ns INTEGER NOT NULL
);
`

// ExclusiveCreateMaxAge bounds how long a record can answer a resend. The
// client resends a create whose reply it lost on reconnect, so seconds after
// it, or once the server is back after a restart. Beyond that a matching
// verifier is more likely a new O_EXCL create after the client rebooted
// (macOS restarts its verifier counter) against a file nobody changed since,
// for example a lock file.
const ExclusiveCreateMaxAge = 10 * time.Minute

// ExclusiveCreate is the file an EXCLUSIVE CREATE made: its mirror inode
// and, when the create already knew it, the backend inode the mirror row
// takes once reconciled. A row with any other inode is another file.
type ExclusiveCreate struct {
	Verifier     [8]byte
	Inode        uint64
	BackendInode uint64
	created      time.Time
}

// exclusiveCreates serves the records from memory, so neither a CREATE nor
// the clear on every WRITE and SETATTR waits for SQLite: the table is written
// behind, by one goroutine, newest operation per path, in order. A record
// lost to a crash before it was written only costs that create's resend an
// EEXIST.
type exclusiveCreates struct {
	mu      sync.RWMutex
	records map[string]ExclusiveCreate
	count   atomic.Int64
	now     func() time.Time // tests only

	pendMu  sync.Mutex
	pending map[string]*ExclusiveCreate // nil: delete
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (c *exclusiveCreates) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (s *Store) loadExclusiveCreates() error {
	c := &s.exclusive
	c.records = make(map[string]ExclusiveCreate)
	rows, err := s.db.Query(`SELECT path, verifier, inode, backend_inode, created_ns FROM exclusive_create_verifiers`)
	if err != nil {
		return err
	}
	defer rows.Close()
	now := c.clock()
	for rows.Next() {
		var p string
		var verifier []byte
		var inode, backend, created int64
		if err := rows.Scan(&p, &verifier, &inode, &backend, &created); err != nil {
			return err
		}
		rec := ExclusiveCreate{Inode: uint64(inode), BackendInode: uint64(backend), created: time.Unix(0, created)}
		if len(verifier) != len(rec.Verifier) || now.Sub(rec.created) >= ExclusiveCreateMaxAge {
			c.queue(p, nil)
			continue
		}
		copy(rec.Verifier[:], verifier)
		c.records[p] = rec
	}
	if err := rows.Err(); err != nil {
		return err
	}
	c.count.Store(int64(len(c.records)))
	c.wake = make(chan struct{}, 1)
	c.stop = make(chan struct{})
	c.done = make(chan struct{})
	c.wake <- struct{}{} // expired rows queued above
	go c.persist(s.db)
	return nil
}

// stopExclusiveCreates writes what is still pending and stops the writer.
func (s *Store) stopExclusiveCreates() {
	c := &s.exclusive
	if c.stop == nil {
		return
	}
	c.once.Do(func() { close(c.stop) })
	<-c.done
}

func (c *exclusiveCreates) queue(path string, rec *ExclusiveCreate) {
	c.pendMu.Lock()
	if c.pending == nil {
		c.pending = make(map[string]*ExclusiveCreate)
	}
	c.pending[path] = rec
	c.pendMu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *exclusiveCreates) persist(db *sql.DB) {
	defer close(c.done)
	for {
		select {
		case <-c.wake:
			c.flush(db)
		case <-c.stop:
			c.flush(db)
			return
		}
	}
}

func (c *exclusiveCreates) flush(db *sql.DB) {
	c.pendMu.Lock()
	batch := c.pending
	c.pending = nil
	c.pendMu.Unlock()
	for p, rec := range batch {
		var err error
		if rec == nil {
			_, err = db.Exec(`DELETE FROM exclusive_create_verifiers WHERE path=?`, p)
		} else {
			_, err = db.Exec(`INSERT OR REPLACE INTO exclusive_create_verifiers(path, verifier, inode, backend_inode, created_ns) VALUES(?, ?, ?, ?, ?)`,
				p, rec.Verifier[:], int64(rec.Inode), int64(rec.BackendInode), rec.created.UnixNano())
		}
		if err != nil {
			log.Printf("metadata: exclusive create record for %q not written: %v", p, err)
		}
	}
}

// RecordExclusiveCreate remembers that an EXCLUSIVE CREATE made the file at
// path. It does no I/O; the record reaches SQLite behind it.
func (s *Store) RecordExclusiveCreate(path string, rec ExclusiveCreate) {
	c := &s.exclusive
	rec.created = c.clock()
	c.mu.Lock()
	if c.records == nil {
		c.records = make(map[string]ExclusiveCreate)
	}
	if _, ok := c.records[path]; !ok {
		c.count.Add(1)
	}
	c.records[path] = rec
	// Records nobody cleared (a file never written or set) age out here, so
	// the set does not stay non-empty for the clear's fast path.
	for p, r := range c.records {
		if rec.created.Sub(r.created) >= ExclusiveCreateMaxAge {
			delete(c.records, p)
			c.count.Add(-1)
			c.queue(p, nil)
		}
	}
	c.mu.Unlock()
	c.queue(path, &rec)
}

// ExclusiveCreateMatches reports whether the file at path, whose mirror row
// now has inode, is the one an EXCLUSIVE CREATE with verifier made, within
// ExclusiveCreateMaxAge and with the name unchanged since. A record that can
// no longer match (another file at the name, or too old) is dropped.
func (s *Store) ExclusiveCreateMatches(path string, verifier [8]byte, inode uint64) bool {
	c := &s.exclusive
	if c.count.Load() == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.records[path]
	if !ok {
		return false
	}
	sameFile := inode != 0 && (inode == rec.Inode || inode == rec.BackendInode)
	if !sameFile || c.clock().Sub(rec.created) >= ExclusiveCreateMaxAge {
		delete(c.records, path)
		c.count.Add(-1)
		c.queue(path, nil)
		return false
	}
	return rec.Verifier == verifier
}

// ClearExclusiveCreate forgets the record for path. Every local mutation of
// the name calls it, the WRITE path included, so it never waits for SQLite
// and is a single atomic load while nothing is recorded.
func (s *Store) ClearExclusiveCreate(path string) {
	c := &s.exclusive
	if c.count.Load() == 0 {
		return
	}
	c.mu.RLock()
	_, ok := c.records[path]
	c.mu.RUnlock()
	if !ok {
		return
	}
	c.mu.Lock()
	if _, ok := c.records[path]; ok {
		delete(c.records, path)
		c.count.Add(-1)
		c.queue(path, nil)
	}
	c.mu.Unlock()
}
