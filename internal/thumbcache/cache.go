// Package thumbcache provides a bounded, persistent, LRU disk cache for tiny
// farm-generated derivative blobs (poster frames, filmstrips, waveform
// sidecars — never proxy media). It makes preview bytes local so browsing on
// slow links does not pull source bytes or repeat FUSE round-trips.
//
// This is a background/HTTP-path component, not an NFS hot-path one: all
// state sits under a single mutex, there are no goroutines or background
// loops, and eviction runs inline in Put. Blobs are written to a temp file
// and renamed into place, so a crash never leaves a partial blob visible
// under its canonical name.
//
// On-disk layout: <dir>/<%02x of inode&0xff>/<inode>_<kind>. The 256-way
// shard keeps directories small at 100k+ entries. Kind strings are opaque to
// the cache but must be filename-safe; consumers know media types from the
// manifest, so no extension is needed.
package thumbcache

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultMaxBytes is the cache bound applied when Open is given maxBytes <= 0.
// An unbounded cache is never allowed.
const DefaultMaxBytes = 2 << 30 // 2 GiB

// tmpPrefix marks in-flight Put temp files. Blob filenames always start with
// a decimal inode, so the prefix can never collide with a stored blob.
const tmpPrefix = ".tmp-"

var (
	errClosed   = errors.New("thumbcache: cache is closed")
	errTooLarge = errors.New("thumbcache: blob exceeds cache size limit")
	errBadKind  = errors.New("thumbcache: kind is not filename-safe")
)

// Stats is a point-in-time snapshot of cache counters.
type Stats struct {
	Bytes     int64 // current resident bytes
	MaxBytes  int64
	Files     int
	Hits      uint64 // Path() hits
	Misses    uint64 // Path() misses
	Puts      uint64
	Evictions uint64
}

// ckey identifies one cached blob.
type ckey struct {
	inode uint64
	kind  string
}

// entry is the in-RAM index record for one on-disk blob.
type entry struct {
	path     string
	size     int64
	lastUsed time.Time
}

// Cache is a bounded, persistent, LRU disk cache of derivative blobs.
// It is safe for concurrent use.
type Cache struct {
	dir      string
	maxBytes int64 // immutable after Open

	mu        sync.Mutex
	closed    bool
	index     map[ckey]*entry
	bytes     int64 // sum of indexed blob sizes
	hits      uint64
	misses    uint64
	puts      uint64
	evictions uint64
	lastTick  time.Time // last recency timestamp handed out; enforces strict ordering
}

// Open opens (creating if missing) the cache rooted at dir, bounded to
// maxBytes resident bytes (maxBytes <= 0 selects DefaultMaxBytes). It walks
// the directory tree once to rebuild the in-RAM index, so the cache survives
// restarts: each blob's size and mtime are read from disk, with mtime seeding
// LRU recency. Stray temp files left by a crash are deleted; unrecognized
// files (e.g. .DS_Store) are ignored. If the resident bytes found on disk
// exceed maxBytes (e.g. the cache was previously run with a larger limit),
// the oldest entries are evicted immediately to re-establish the bound.
func Open(dir string, maxBytes int64) (*Cache, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("thumbcache: create cache dir: %w", err)
	}
	c := &Cache{
		dir:      dir,
		maxBytes: maxBytes,
		index:    make(map[ckey]*entry),
	}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if p == dir {
				return walkErr // cache root itself unreadable
			}
			return nil // tolerate unreadable children
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, tmpPrefix) {
			os.Remove(p) // leftover from a crash mid-Put
			return nil
		}
		sep := strings.IndexByte(name, '_')
		if sep <= 0 {
			return nil // not a blob we wrote; leave it alone
		}
		inode, perr := strconv.ParseUint(name[:sep], 10, 64)
		if perr != nil {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		k := ckey{inode: inode, kind: name[sep+1:]}
		if old, dup := c.index[k]; dup {
			// Same (inode,kind) found in a mis-sharded dir; last one wins,
			// keep byte accounting exact.
			c.bytes -= old.size
		}
		c.index[k] = &entry{path: p, size: info.Size(), lastUsed: info.ModTime()}
		c.bytes += info.Size()
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("thumbcache: index cache dir: %w", err)
	}
	// Re-establish the bound before returning. No lock needed: the cache is
	// not yet published to any other goroutine.
	for c.bytes > c.maxBytes && len(c.index) > 0 {
		c.evictOldestLocked()
	}
	return c, nil
}

// Has reports whether a blob for (inode, kind) is resident. It does not touch
// LRU recency and does not count toward Hits/Misses (those are Path()-only).
func (c *Cache) Has(inode uint64, kind string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	_, ok := c.index[ckey{inode: inode, kind: kind}]
	return ok
}

// Path returns the on-disk path of a cached blob and touches its LRU recency.
// The file's mtime is also bumped (best-effort) so recency survives a restart.
func (c *Cache) Path(inode uint64, kind string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return "", false
	}
	e, ok := c.index[ckey{inode: inode, kind: kind}]
	if !ok {
		c.misses++
		return "", false
	}
	c.hits++
	now := c.tickLocked()
	e.lastUsed = now
	// Best-effort: Open seeds recency from mtime, so persist the touch.
	_ = os.Chtimes(e.path, now, now)
	return e.path, true
}

// Put atomically stores a blob (tmp file + rename), evicting LRU entries as
// needed to stay under MaxBytes. Returns bytes written.
//
// Byte accounting: when replacing an existing (inode, kind) entry, the old
// size is subtracted (and the entry de-indexed) before the eviction loop
// runs, so the entry being replaced is never chosen as an eviction victim
// and Bytes only ever reflects the surviving blob. Eviction then removes
// oldest-lastUsed entries while bytes+incoming > MaxBytes. A single blob
// strictly larger than MaxBytes is rejected without disturbing cache state.
// The bound covers indexed blobs; an in-flight Put may transiently use up to
// one extra blob of temp space on disk.
func (c *Cache) Put(inode uint64, kind string, r io.Reader) (int64, error) {
	if err := checkKind(kind); err != nil {
		return 0, err
	}
	shardDir, dst := c.blobPath(inode, kind)
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return 0, fmt.Errorf("thumbcache: create shard dir: %w", err)
	}

	// Stream the blob into a temp file in the destination shard WITHOUT
	// holding the lock: the reader may be slow (network / FUSE), and
	// Path/Has must stay responsive. The temp file is private state until
	// the rename below, and Open deletes strays after a crash.
	tmp, err := os.CreateTemp(shardDir, tmpPrefix+"*")
	if err != nil {
		return 0, fmt.Errorf("thumbcache: create temp file: %w", err)
	}
	tmpName := tmp.Name()

	// Never spool more than maxBytes+1 bytes to disk: one extra byte is
	// enough to detect an oversized blob and reject it.
	limit := c.maxBytes
	if limit < math.MaxInt64 {
		limit++
	}
	// No fsync: rename is atomic against process crash (the only partial
	// artifact is a .tmp-* stray, deleted at next Open), and this is
	// rebuildable derivative data — a full disk barrier per thumbnail
	// (F_FULLFSYNC on darwin, ~ms each) is not worth it. Durability across
	// power loss is explicitly not guaranteed.
	size, err := io.Copy(tmp, io.LimitReader(r, limit))
	if err == nil && size > c.maxBytes {
		err = fmt.Errorf("%w: blob is at least %d bytes, limit %d", errTooLarge, size, c.maxBytes)
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmpName)
		return 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		os.Remove(tmpName)
		return 0, errClosed
	}
	k := ckey{inode: inode, kind: kind}
	if old, replacing := c.index[k]; replacing {
		// Subtract the old size up front so the eviction loop sees only the
		// incoming size and can never pick the entry being replaced.
		c.bytes -= old.size
		delete(c.index, k)
		if old.path != dst {
			os.Remove(old.path) // mis-sharded stray; keep disk honest
		}
	}
	for c.bytes+size > c.maxBytes && len(c.index) > 0 {
		c.evictOldestLocked()
	}
	if err := os.Rename(tmpName, dst); err != nil {
		os.Remove(tmpName)
		// If we were replacing and the old blob is still on disk at dst,
		// re-index it so Stats stays consistent with disk.
		if info, statErr := os.Stat(dst); statErr == nil {
			c.index[k] = &entry{path: dst, size: info.Size(), lastUsed: info.ModTime()}
			c.bytes += info.Size()
		}
		return 0, fmt.Errorf("thumbcache: finalize blob: %w", err)
	}
	c.index[k] = &entry{path: dst, size: size, lastUsed: c.tickLocked()}
	c.bytes += size
	c.puts++
	return size, nil
}

// Stats returns a snapshot of cache counters.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Bytes:     c.bytes,
		MaxBytes:  c.maxBytes,
		Files:     len(c.index),
		Hits:      c.hits,
		Misses:    c.misses,
		Puts:      c.puts,
		Evictions: c.evictions,
	}
}

// Close marks the cache closed: subsequent Put calls fail and Has/Path report
// not-found. Cached files stay on disk for the next Open. Close is idempotent
// and always returns nil.
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// checkKind rejects kind strings that could escape the cache directory or
// produce unsafe filenames. Kinds are otherwise opaque.
func checkKind(kind string) error {
	if kind == "" || strings.ContainsAny(kind, `/\`) || strings.Contains(kind, "..") {
		return fmt.Errorf("%w: %q", errBadKind, kind)
	}
	return nil
}

// blobPath returns the shard directory and canonical blob path for a key:
// <dir>/<%02x of inode&0xff>/<inode>_<kind>.
func (c *Cache) blobPath(inode uint64, kind string) (shardDir, blob string) {
	shardDir = filepath.Join(c.dir, fmt.Sprintf("%02x", byte(inode)))
	return shardDir, filepath.Join(shardDir, strconv.FormatUint(inode, 10)+"_"+kind)
}

// tickLocked returns a strictly increasing timestamp for LRU recency, so two
// operations within the same clock tick still order deterministically.
// Caller must hold c.mu (or have exclusive ownership during Open).
func (c *Cache) tickLocked() time.Time {
	now := time.Now()
	if !now.After(c.lastTick) {
		now = c.lastTick.Add(time.Nanosecond)
	}
	c.lastTick = now
	return now
}

// evictOldestLocked removes the least-recently-used entry: file removed
// (best-effort), index entry deleted, bytes subtracted, Evictions counted.
// Caller must hold c.mu (or have exclusive ownership during Open).
func (c *Cache) evictOldestLocked() {
	var (
		victimKey ckey
		victim    *entry
	)
	for k, e := range c.index {
		if victim == nil || e.lastUsed.Before(victim.lastUsed) {
			victimKey, victim = k, e
		}
	}
	if victim == nil {
		return
	}
	os.Remove(victim.path)
	delete(c.index, victimKey)
	c.bytes -= victim.size
	c.evictions++
}
