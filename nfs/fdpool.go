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
//
// QA-37 fix: keyed by {path, write} so a previously-opened read fd does
// NOT get reused for a write call. Pre-fix, GetWrite would silently
// return a cached O_RDONLY fd if Get had opened one earlier (e.g. via
// Stat), and the next WriteAt would EBADF — surfacing to Finder as -36.
type fdKey struct {
	path  string
	write bool
}

type FDPool struct {
	mu      sync.Mutex
	entries map[fdKey]*poolEntry
	stopCh  chan struct{}
}

type poolEntry struct {
	fd       *os.File
	lastUsed time.Time
	refCount int
}

func NewFDPool() *FDPool {
	p := &FDPool{
		entries: make(map[fdKey]*poolEntry),
		stopCh:  make(chan struct{}),
	}
	go p.evictLoop()
	return p
}

// Get returns a pooled read-only fd for the given path, opening it if
// necessary. The returned fd MUST NOT be used for writes; use GetWrite
// for that and Release/ReleaseWrite accordingly so the read+write fds
// stay segregated.
func (p *FDPool) Get(path string) (*os.File, error) {
	k := fdKey{path: path, write: false}
	p.mu.Lock()
	if entry, ok := p.entries[k]; ok {
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
	if entry, ok := p.entries[k]; ok {
		entry.lastUsed = time.Now()
		entry.refCount++
		existingFD := entry.fd
		p.mu.Unlock()
		fd.Close() // close the one we just opened
		return existingFD, nil
	}
	p.entries[k] = &poolEntry{
		fd:       fd,
		lastUsed: time.Now(),
		refCount: 1,
	}
	p.mu.Unlock()
	return fd, nil
}

// GetWrite returns a pooled fd for writing, opening with the given flags
// if necessary. Lives in its own keyspace slot (write=true) so a
// previously-cached read fd never satisfies a write call.
func (p *FDPool) GetWrite(path string, flag int, perm os.FileMode) (*os.File, error) {
	k := fdKey{path: path, write: true}
	p.mu.Lock()
	if entry, ok := p.entries[k]; ok {
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
	if entry, ok := p.entries[k]; ok {
		entry.lastUsed = time.Now()
		entry.refCount++
		existingFD := entry.fd
		p.mu.Unlock()
		fd.Close()
		return existingFD, nil
	}
	p.entries[k] = &poolEntry{
		fd:       fd,
		lastUsed: time.Now(),
		refCount: 1,
	}
	p.mu.Unlock()
	return fd, nil
}

// Release decrements the refcount for a path on the READ-side slot.
// Use ReleaseWrite for fds obtained via GetWrite.
func (p *FDPool) Release(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.entries[fdKey{path: path, write: false}]; ok {
		entry.refCount--
	}
}

// ReleaseWrite decrements the refcount for a path on the WRITE-side slot.
func (p *FDPool) ReleaseWrite(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.entries[fdKey{path: path, write: true}]; ok {
		entry.refCount--
	}
}

// HasOpenRefs returns true if there is at least one outstanding Get
// or GetWrite without a matching Release for `path` — i.e. somebody
// currently holds a FD (read OR write) on this file.
//
// QA-35 (2026-05-26): used by juiceFS.Stat to skip the phantom-purge
// FUSE Lstat gate when an active reader holds the file open. If a FD
// is open, the file is not a phantom — the reader proves it exists.
// Eliminates a per-metadata-RPC FUSE round-trip during sustained
// reads of a held file (Resolve playback, Finder Quick Look, etc.).
//
// QA-37: after the read/write keyspace split, check BOTH slots — an
// active writer is just as good as an active reader for proving the
// file isn't a phantom.
func (p *FDPool) HasOpenRefs(path string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.entries[fdKey{path: path, write: false}]; ok && entry.refCount > 0 {
		return true
	}
	if entry, ok := p.entries[fdKey{path: path, write: true}]; ok && entry.refCount > 0 {
		return true
	}
	return false
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
			// Collect the idle fds under the lock, but CLOSE them outside it.
			// Closing a JuiceFS FUSE fd flushes pending data and can BLOCK for
			// seconds under write load; holding p.mu across that Close convoys
			// every concurrent GetWrite/Get/Release on the single pool mutex.
			// A deep-tree Finder copy generates dozens of parallel WRITE RPCs,
			// all calling GetWrite — 93 were observed wedged on this lock while
			// evictLoop held it inside a stuck Close, saturating the NFS server's
			// in-flight budget and timing out the copy with "error 100060"
			// (2026-06-14, reproduced via ditto of a 5598-file subtree).
			var toClose []*os.File
			p.mu.Lock()
			for path, entry := range p.entries {
				if entry.refCount <= 0 && now.Sub(entry.lastUsed) > fdIdleTimeout {
					toClose = append(toClose, entry.fd)
					delete(p.entries, path)
				}
			}
			p.mu.Unlock()
			for _, fd := range toClose {
				fd.Close()
			}
		}
	}
}

// Stop closes all pooled fds.
func (p *FDPool) Stop() {
	close(p.stopCh)
	// Detach the fds under the lock, close them outside it (a FUSE fd Close can
	// block; don't hold p.mu across it — see evictLoop).
	p.mu.Lock()
	entries := p.entries
	p.entries = nil
	p.mu.Unlock()
	for _, entry := range entries {
		entry.fd.Close()
	}
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
