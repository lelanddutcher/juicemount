package nfs

import (
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
// SAFETY: persistence adds zero staleness risk by construction — every serve
// already validates the entry against the MIRROR's live (mtime,size)
// (sidecarServe → sidecarCache.get), and write-opens invalidate. A loaded
// entry whose file changed while the app was down simply never matches and
// falls through to a fresh read. Load also re-applies the put-side guards
// (complete body, size caps), so a corrupt/truncated file degrades to a cold
// cache, never to a wrong serve.
//
// Loss model: saved every sidecarPersistInterval when dirty, plus a final
// save on clean shutdown. A hard kill loses at most the last interval's
// puts — they re-warm on demand exactly as before.
const (
	sidecarPersistVersion  = 1
	sidecarPersistInterval = 2 * time.Minute
)

type sidecarPersistEntry struct {
	Path  string
	Mtime int64
	Size  int64
	Data  []byte
}

type sidecarPersistFile struct {
	Version int
	Entries []sidecarPersistEntry
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
	go c.persistLoop()
}

// stopPersist halts the saver and writes a final snapshot.
func (c *sidecarCache) stopPersist() {
	if c == nil || c.persistPath == "" {
		return
	}
	if c.persistStop != nil {
		close(c.persistStop)
	}
	c.saveToDisk()
}

func (c *sidecarCache) persistLoop() {
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
	if err := gob.NewDecoder(f).Decode(&pf); err != nil || pf.Version != sidecarPersistVersion {
		jmlog.Info("sidecar cache: snapshot unreadable — starting cold", "path", c.persistPath)
		return
	}
	loaded, bytes := 0, int64(0)
	for _, e := range pf.Entries {
		// Re-apply the put guards: complete body, positive bounded size.
		if e.Size <= 0 || e.Size > sidecarMaxFile || int64(len(e.Data)) != e.Size {
			continue
		}
		c.mu.Lock()
		if _, exists := c.m[e.Path]; !exists && c.bytes+e.Size <= c.maxBytes {
			c.tick++
			c.m[e.Path] = &sidecarEntry{data: e.Data, mtime: e.Mtime, size: e.Size, lastUsed: c.tick}
			c.bytes += e.Size
			loaded++
			bytes += e.Size
		}
		c.mu.Unlock()
	}
	if loaded > 0 {
		jmlog.Info("sidecar cache: snapshot restored — repeat-visit folders stay warm across restarts",
			"entries", loaded, "bytes", bytes, "path", c.persistPath)
	}
}

// saveToDisk snapshots the cache atomically (temp+rename). Entry data slices
// are immutable after put, so referencing them outside the lock is safe; the
// lock is held only to copy the map references.
func (c *sidecarCache) saveToDisk() {
	if c.persistPath == "" {
		return
	}
	c.mu.Lock()
	entries := make([]sidecarPersistEntry, 0, len(c.m))
	for p, e := range c.m {
		entries = append(entries, sidecarPersistEntry{Path: p, Mtime: e.mtime, Size: e.size, Data: e.data})
	}
	c.mu.Unlock()
	atomic.StoreInt64(&c.dirty, 0)

	tmp := c.persistPath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(c.persistPath), 0o755); err != nil {
		return
	}
	f, err := os.Create(tmp)
	if err != nil {
		return
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(sidecarPersistFile{Version: sidecarPersistVersion, Entries: entries}); err != nil {
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
	jmlog.Debug("sidecar cache: snapshot saved", "entries", len(entries))
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
