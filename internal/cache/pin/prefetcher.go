package pin

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Prefetcher reads pinned files through the FUSE mount, which causes JuiceFS
// to populate its own LRU cache. It does NOT manage a separate cache directory
// — JuiceFS owns that — we just trigger the right reads.
//
// Concurrency model: a bounded pool of N workers pulls from a job queue.
// Each worker reads its assigned file in 1 MB chunks and discards the bytes;
// the side effect we care about is the read travelling through JuiceFS's
// download + cache pipeline.
type Prefetcher struct {
	store      *Store
	fusePath   string // root of the FUSE mount, e.g. ~/.juicemount/fuse-internal
	mountPoint string // user-facing mount, e.g. /Volumes/zpool — used to strip
	//                   the prefix from canonical pin keys back to FUSE-relative
	workers int

	jobs   chan jobReq
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Live progress counters (atomic for cheap concurrent reads)
	bytesPrefetched atomic.Int64
	filesPrefetched atomic.Int64
	currentFile     atomic.Pointer[string]
}

type jobReq struct {
	entry Entry
	done  chan error // optional; nil if fire-and-forget
}

// NewPrefetcher constructs a worker pool. workers <= 0 picks a sensible default.
//
// mountPoint is the user-facing path (e.g. "/Volumes/zpool") that pin store
// keys are anchored to. The prefetcher strips this prefix to translate keys
// back to FUSE-relative paths. Empty string falls back to the legacy default
// for backward compatibility.
func NewPrefetcher(store *Store, fusePath, mountPoint string, workers int) *Prefetcher {
	if workers <= 0 {
		workers = 4 // good default: parallel enough to saturate WAN, not so many as to thrash
	}
	p := &Prefetcher{
		store:      store,
		fusePath:   fusePath,
		mountPoint: mountPoint,
		workers:    workers,
		jobs:       make(chan jobReq, 256),
		stopCh:     make(chan struct{}),
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.workerLoop()
	}
	return p
}

// Stop drains active workers and closes the prefetcher. Pending queue items
// are dropped; in-flight reads complete naturally.
func (p *Prefetcher) Stop() {
	close(p.stopCh)
	close(p.jobs)
	p.wg.Wait()
}

// Enqueue queues a single file for prefetch. Non-blocking — returns false if
// the queue is full (caller can retry or backpressure).
func (p *Prefetcher) Enqueue(e Entry) bool {
	select {
	case p.jobs <- jobReq{entry: e}:
		return true
	default:
		return false
	}
}

// EnqueueWait queues a file and blocks until it's prefetched (or fails).
// Used by the CLI when the user wants synchronous "wait for it" behavior.
func (p *Prefetcher) EnqueueWait(ctx context.Context, e Entry) error {
	done := make(chan error, 1)
	select {
	case p.jobs <- jobReq{entry: e, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PullPending repeatedly drains the store's Pending queue and enqueues
// jobs. Runs until ctx is cancelled. Call this from a daemon goroutine.
func (p *Prefetcher) PullPending(ctx context.Context, batchSize int) {
	if batchSize <= 0 {
		batchSize = 100
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-tick.C:
			pending, err := p.store.Pending(batchSize)
			if err != nil || len(pending) == 0 {
				continue
			}
			for _, e := range pending {
				if !p.Enqueue(e) {
					break // queue full, try next tick
				}
				p.store.UpdateStatus(e.Path, StatusPrefetching, 0, "")
			}
		}
	}
}

// ReWarmupLoop runs periodically and re-reads pinned-and-ready files to
// keep them at the front of JuiceFS's LRU. Without this, eventually any
// pinned file falls off the LRU under cache pressure and gets evicted.
//
// ttl is how stale a file can get before re-warmup. Recommended: 6 hours.
func (p *Prefetcher) ReWarmupLoop(ctx context.Context, ttl time.Duration, batchSize int) {
	if batchSize <= 0 {
		batchSize = 50
	}
	tick := time.NewTicker(15 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-tick.C:
			stale, err := p.store.Stale(ttl, batchSize)
			if err != nil || len(stale) == 0 {
				continue
			}
			for _, e := range stale {
				p.Enqueue(e)
			}
		}
	}
}

// workerLoop pulls jobs and runs the prefetch.
func (p *Prefetcher) workerLoop() {
	defer p.wg.Done()
	for j := range p.jobs {
		err := p.prefetch(j.entry)
		if j.done != nil {
			j.done <- err
		}
	}
}

// prefetch reads the file through FUSE, discarding the bytes. Updates the
// store with the result.
func (p *Prefetcher) prefetch(e Entry) error {
	full := filepath.Join(p.fusePath, p.stripMountPrefix(e.Path))

	currentName := full
	p.currentFile.Store(&currentName)
	defer p.currentFile.Store(nil)

	f, err := os.Open(full)
	if err != nil {
		p.store.UpdateStatus(e.Path, StatusFailed, 0, err.Error())
		return err
	}
	defer f.Close()

	buf := make([]byte, 1024*1024) // 1 MB read chunks
	var totalRead int64
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			totalRead += int64(n)
			p.bytesPrefetched.Add(int64(n))
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			p.store.UpdateStatus(e.Path, StatusFailed, totalRead, rerr.Error())
			return rerr
		}
	}

	p.filesPrefetched.Add(1)
	p.store.UpdateStatus(e.Path, StatusReady, totalRead, "")
	return nil
}

// LiveStats reports counters since process start. For UI progress bars.
type LiveStats struct {
	BytesPrefetched int64
	FilesPrefetched int64
	CurrentFile     string // empty if idle
	Workers         int
}

func (p *Prefetcher) LiveStats() LiveStats {
	cur := ""
	if cp := p.currentFile.Load(); cp != nil {
		cur = *cp
	}
	return LiveStats{
		BytesPrefetched: p.bytesPrefetched.Load(),
		FilesPrefetched: p.filesPrefetched.Load(),
		CurrentFile:     cur,
		Workers:         p.workers,
	}
}

// stripMountPrefix turns canonical pin-store paths (anchored to the user's
// mount point) into FUSE-relative paths. For the default "/Volumes/zpool":
//
//   "/Volumes/zpool/foo/bar" → "foo/bar"
//   "/Volumes/zpool"         → ""
//   "/other/path"            → "/other/path" (no-op fallback)
func (p *Prefetcher) stripMountPrefix(path string) string {
	mp := p.mountPoint
	if mp == "" {
		mp = "/Volumes/zpool"
	}
	for len(mp) > 1 && mp[len(mp)-1] == '/' {
		mp = mp[:len(mp)-1]
	}
	if path == mp {
		return ""
	}
	withSlash := mp + "/"
	if len(path) > len(withSlash) && path[:len(withSlash)] == withSlash {
		return path[len(withSlash):]
	}
	return path
}

// stripVolumePrefix is the legacy helper retained for the existing test.
// New code should use Prefetcher.stripMountPrefix which respects the
// configured mount point.
func stripVolumePrefix(path string) string {
	const vol = "/Volumes/zpool/"
	if len(path) > len(vol) && path[:len(vol)] == vol {
		return path[len(vol):]
	}
	const v2 = "/Volumes/zpool"
	if path == v2 {
		return ""
	}
	return path
}

// CountFilesUnder walks a directory in the JuiceMount volume and returns
// every regular file path + size. Used by Pin() when the user pins a
// whole subtree.
func CountFilesUnder(rootPath string) ([]Entry, error) {
	var out []Entry
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // best effort — keep going past permission errors etc.
		}
		if info.IsDir() {
			return nil
		}
		// Skip Apple-Double and other macOS bookkeeping files
		base := filepath.Base(path)
		if len(base) >= 2 && base[:2] == "._" {
			return nil
		}
		switch base {
		case ".DS_Store", ".Spotlight-V100", ".Trashes", ".fseventsd",
			".TemporaryItems":
			return nil
		}
		out = append(out, Entry{
			Path:    path,
			Size:    info.Size(),
			PinRoot: rootPath,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %q: %w", rootPath, err)
	}
	return out, nil
}
