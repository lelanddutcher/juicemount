package nfs

import (
	"crypto/sha256"
	"encoding/gob"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// Sidecar cache persistence (2026-07-13, cellular). The cache made repeat
// folder visits RAM-fast, but it was RAM-ONLY: every app restart re-cooled
// every folder, and on a tunnel the re-warm is the dominant first-visit cost
// (measured live: 138s for a 255-entry dir that then lists in ms). Persisting
// the cache turns "first visit per RESTART" into "first visit EVER".
//
// V3 stores only complete strong-source captures as read-only snapshots with
// body integrity, exact attributes, a durable local mirror revision and the
// selected logical volume. Restored records are admitted lazily from resident
// RAM mirror rows. Unobserved remote changes may remain absent until normal
// reconciliation; local successor publications revoke reads before reply.
// V1/V2 records retain legacy weak behavior and never gain terminal proof.
//
// Loss model: saved every sidecarPersistInterval when dirty, plus a final
// save on clean shutdown. A hard kill loses at most the last interval's
// puts — they re-warm on demand exactly as before.
const (
	sidecarPersistVersion  = 3
	sidecarPersistInterval = 2 * time.Minute
)

type sidecarPersistEntry struct {
	Path     string
	Mtime    int64
	Size     int64
	Data     []byte // v1 compatibility; v2 stores the shared body in Blobs
	Blob     uint32
	Snapshot *sidecarSnapshot // v3 only; immutable read authority, never a write seed
}

type sidecarPersistFile struct {
	Version int
	Entries []sidecarPersistEntry
	Blobs   [][]byte
}

// dirty counts puts/invalidates since the last save (checked by the saver).
// persistPath is set once by enablePersist before the saver goroutine starts.

func sidecarPersistIntervalFromEnv() time.Duration {
	if raw := os.Getenv("JM_SIDECAR_PERSIST_SEC"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 5 {
			return time.Duration(n) * time.Second
		}
	}
	return sidecarPersistInterval
}

// enablePersist loads any prior snapshot into the cache and starts the
// periodic saver. Call once at startup, after the cache is constructed and
// before serving begins (a concurrent early serve is still safe — load holds
// the cache lock per insert). No-op when the cache is disabled.
func (c *sidecarCache) enablePersist(path string) {
	if c == nil || !c.enabled || path == "" {
		return
	}
	c.persistPath = path
	c.loadFromDisk()
	c.persistStop = make(chan struct{})
	c.persistDone = make(chan struct{})
	go c.persistLoop()
}

// stopPersist halts the saver and writes a final snapshot.
func (c *sidecarCache) stopPersist() {
	if c == nil || c.persistPath == "" {
		return
	}
	c.persistStopOnce.Do(func() {
		if c.persistStop != nil {
			close(c.persistStop)
			<-c.persistDone // periodic save has released the shared temp path
		}
		c.saveToDisk()
	})
}

func (c *sidecarCache) persistLoop() {
	defer close(c.persistDone)
	t := time.NewTicker(sidecarPersistIntervalFromEnv())
	defer t.Stop()
	for {
		select {
		case <-c.persistStop:
			return
		case <-t.C:
			if atomic.LoadInt64(&c.dirty) == 0 {
				continue
			}
			c.saveToDisk()
		}
	}
}

// loadFromDisk restores a snapshot through the normal put-side guards. Any
// error (missing file, version skew, decode failure) degrades to a cold
// cache with one log line — never a startup failure.
func (c *sidecarCache) loadFromDisk() {
	f, err := os.Open(c.persistPath)
	if err != nil {
		return // first run / no snapshot — cold start, normal
	}
	defer f.Close()
	var pf sidecarPersistFile
	if err := gob.NewDecoder(f).Decode(&pf); err != nil || (pf.Version != 1 && pf.Version != 2 && pf.Version != sidecarPersistVersion) {
		jmlog.Info("sidecar cache: snapshot unreadable — starting cold", "path", c.persistPath)
		return
	}
	c.mu.Lock()
	before := len(c.m)
	for _, e := range pf.Entries {
		data := e.Data
		if pf.Version >= 2 {
			if int(e.Blob) >= len(pf.Blobs) {
				continue
			}
			data = pf.Blobs[e.Blob]
		}
		// Re-apply the put guards: complete body, positive bounded size.
		if e.Size <= 0 || e.Size > sidecarMaxFile || int64(len(data)) != e.Size {
			continue
		}
		if e.Snapshot != nil && (pf.Version != sidecarPersistVersion ||
			e.Snapshot.Token.Path != e.Path || e.Snapshot.Token.Incarnation == "" ||
			e.Snapshot.Token.Revision <= 0 || e.Snapshot.Inode == 0 ||
			e.Snapshot.Size != e.Size || e.Snapshot.MtimeNS/1e9 != e.Mtime ||
			e.Snapshot.Name != filepath.Base(e.Path) || !e.Snapshot.Mode.IsRegular() ||
			sha256.Sum256(data) != e.Snapshot.SHA256) {
			continue
		}
		if _, exists := c.m[e.Path]; exists {
			continue
		}
		c.putLocked(e.Path, data, e.Mtime, e.Size)
		if e.Snapshot != nil {
			copyOf := *e.Snapshot
			c.m[e.Path].snapshot = &copyOf
		}
		for (c.bytes > c.maxBytes || len(c.m) > c.fileLimitLocked()) && len(c.m) > 1 {
			c.evictOneLRULocked()
		}
	}
	loaded := len(c.m) - before
	uniqueBytes := c.bytes
	uniqueBodies := len(c.blobs)
	c.mu.Unlock()
	atomic.StoreInt64(&c.dirty, 0)
	if loaded > 0 {
		jmlog.Info("sidecar cache: snapshot restored — repeat-visit folders stay warm across restarts",
			"entries", loaded, "unique_bodies", uniqueBodies, "unique_bytes", uniqueBytes, "path", c.persistPath)
	}
}

// saveToDisk snapshots the cache atomically (temp+rename). Entry data slices
// are immutable after put, so referencing them outside the lock is safe; the
// lock is held only to copy the map references.
func (c *sidecarCache) saveToDisk() {
	if c.persistPath == "" {
		return
	}
	c.persistWriteMu.Lock()
	defer c.persistWriteMu.Unlock()
	c.mu.Lock()
	dirtyAtSnapshot := atomic.LoadInt64(&c.dirty)
	c.ensureInternLocked()
	entries := make([]sidecarPersistEntry, 0, len(c.m))
	blobs := make([][]byte, 0, len(c.blobs))
	blobIndex := make(map[string]uint32, len(c.blobs))
	for p, e := range c.m {
		idx, ok := blobIndex[e.contentKey]
		if !ok {
			idx = uint32(len(blobs))
			blobIndex[e.contentKey] = idx
			blobs = append(blobs, e.data)
		}
		entries = append(entries, sidecarPersistEntry{Path: p, Mtime: e.mtime, Size: e.size, Blob: idx, Snapshot: e.snapshot})
	}
	c.mu.Unlock()

	tmp := c.persistPath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(c.persistPath), 0o755); err != nil {
		return
	}
	f, err := os.Create(tmp)
	if err != nil {
		return
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(sidecarPersistFile{Version: sidecarPersistVersion, Entries: entries, Blobs: blobs}); err != nil {
		f.Close()
		os.Remove(tmp)
		return
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return
	}
	if err := os.Rename(tmp, c.persistPath); err != nil {
		os.Remove(tmp)
		return
	}
	atomic.CompareAndSwapInt64(&c.dirty, dirtyAtSnapshot, 0)
	jmlog.Debug("sidecar cache: snapshot saved", "entries", len(entries), "unique_bodies", len(blobs))
}

// ---- handler wrappers (bridge-facing) ----

// SidecarPersistEnable wires disk persistence for the sidecar cache.
func (h *JuiceMountHandler) SidecarPersistEnable(path string) {
	if h == nil || h.sidecar == nil {
		return
	}
	h.sidecar.enablePersist(path)
}

// SidecarPersistStop saves a final snapshot and stops the saver.
func (h *JuiceMountHandler) SidecarPersistStop() {
	if h == nil || h.sidecar == nil {
		return
	}
	h.sidecar.stopPersist()
}
