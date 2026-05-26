package nfs

import (
	"log"
	"os"
	"sync"
	"time"
)

const (
	fdIdleTimeout = 2 * time.Minute
	fdEvictTick   = 30 * time.Second
)

// FDPool manages a pool of reusable file descriptors for JuiceFS FUSE reads.
// Opening files on JuiceFS FUSE is expensive (~60ms); this pool amortizes
// that cost across many pread() calls on the same file.
type FDPool struct {
	mu      sync.Mutex
	entries map[string]*poolEntry
	stopCh  chan struct{}
}

type poolEntry struct {
	fd       *os.File
	lastUsed time.Time
	refCount int
}

func NewFDPool() *FDPool {
	p := &FDPool{
		entries: make(map[string]*poolEntry),
		stopCh:  make(chan struct{}),
	}
	go p.evictLoop()
	return p
}

// Get returns a pooled fd for the given path, opening it if necessary.
func (p *FDPool) Get(path string) (*os.File, error) {
	p.mu.Lock()
	if entry, ok := p.entries[path]; ok {
		entry.lastUsed = time.Now()
		entry.refCount++
		fd := entry.fd
		p.mu.Unlock()
		return fd, nil
	}
	p.mu.Unlock()

	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	// Double-check under lock — another goroutine may have inserted
	if entry, ok := p.entries[path]; ok {
		entry.lastUsed = time.Now()
		entry.refCount++
		existingFD := entry.fd
		p.mu.Unlock()
		fd.Close() // close the one we just opened
		return existingFD, nil
	}
	p.entries[path] = &poolEntry{
		fd:       fd,
		lastUsed: time.Now(),
		refCount: 1,
	}
	p.mu.Unlock()
	return fd, nil
}

// GetWrite returns a pooled fd for writing, opening with the given flags if necessary.
func (p *FDPool) GetWrite(path string, flag int, perm os.FileMode) (*os.File, error) {
	p.mu.Lock()
	if entry, ok := p.entries[path]; ok {
		entry.lastUsed = time.Now()
		entry.refCount++
		fd := entry.fd
		p.mu.Unlock()
		return fd, nil
	}
	p.mu.Unlock()

	fd, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if entry, ok := p.entries[path]; ok {
		entry.lastUsed = time.Now()
		entry.refCount++
		existingFD := entry.fd
		p.mu.Unlock()
		fd.Close()
		return existingFD, nil
	}
	p.entries[path] = &poolEntry{
		fd:       fd,
		lastUsed: time.Now(),
		refCount: 1,
	}
	p.mu.Unlock()
	return fd, nil
}

// Release decrements the refcount for a path.
func (p *FDPool) Release(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.entries[path]; ok {
		entry.refCount--
	}
}

// HasOpenRefs returns true if there is at least one outstanding Get
// without a matching Release for `path` — i.e. somebody currently
// holds a FD on this file.
//
// QA-35 (2026-05-26): used by juiceFS.Stat to skip the phantom-purge
// FUSE Lstat gate when an active reader holds the file open. If a FD
// is open, the file is not a phantom — the reader proves it exists.
// Eliminates a per-metadata-RPC FUSE round-trip during sustained
// reads of a held file (Resolve playback, Finder Quick Look, etc.).
func (p *FDPool) HasOpenRefs(path string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.entries[path]
	return ok && entry.refCount > 0
}

func (p *FDPool) evictLoop() {
	ticker := time.NewTicker(fdEvictTick)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			p.mu.Lock()
			for path, entry := range p.entries {
				if entry.refCount <= 0 && now.Sub(entry.lastUsed) > fdIdleTimeout {
					entry.fd.Close()
					delete(p.entries, path)
				}
			}
			p.mu.Unlock()
		}
	}
}

// Stop closes all pooled fds.
func (p *FDPool) Stop() {
	close(p.stopCh)
	p.mu.Lock()
	for _, entry := range p.entries {
		entry.fd.Close()
	}
	p.entries = nil
	p.mu.Unlock()
	log.Printf("fdpool: stopped")
}

// Stats returns pool statistics.
func (p *FDPool) Stats() (open, active int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, entry := range p.entries {
		open++
		if entry.refCount > 0 {
			active++
		}
	}
	return
}
