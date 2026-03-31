package nfs

import (
	"log"
	"os"
	"sync"
	"time"
)

const (
	// Readahead triggers after this many consecutive sequential reads on the same inode.
	sequentialThreshold = 3

	// Number of blocks to prefetch ahead of the current read position.
	readaheadBlocks = 8 // 8 * 4MB = 32MB ahead

	// Max concurrent readahead goroutines across all files.
	maxReadaheadWorkers = 4

	// Block size for readahead (matches JuiceFS block size).
	readaheadBlockSize = 4 << 20 // 4MB

	// Stale tracker entries are cleaned up after this duration.
	trackerTTL = 30 * time.Second
)

// ReadaheadManager detects sequential read patterns and triggers
// background prefetch to warm the JuiceFS SSD cache.
type ReadaheadManager struct {
	fusePath string
	fdPool   *FDPool // reuse pooled fds instead of opening new ones

	mu       sync.Mutex
	trackers map[uint64]*readTracker // inode → tracker

	// Semaphore for concurrent readahead goroutines
	sem    chan struct{}
	stopCh chan struct{}

	// Stats
	statsMu    sync.Mutex
	triggered  int64
	prefetched int64
}

type readTracker struct {
	lastOffset     int64
	sequentialHits int
	lastAccess     time.Time
	prefetchedTo   int64 // highest offset we've already prefetched to
}

func NewReadaheadManager(fusePath string, fdPool *FDPool) *ReadaheadManager {
	rm := &ReadaheadManager{
		fusePath: fusePath,
		fdPool:   fdPool,
		trackers: make(map[uint64]*readTracker),
		sem:      make(chan struct{}, maxReadaheadWorkers),
		stopCh:   make(chan struct{}),
	}
	go rm.cleanupLoop()
	return rm
}

// OnRead is called by the NFS read handler after every READ operation.
// It tracks the read pattern and triggers readahead if sequential.
func (rm *ReadaheadManager) OnRead(inode uint64, offset int64, size int, filePath string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	tracker, ok := rm.trackers[inode]
	if !ok {
		tracker = &readTracker{}
		rm.trackers[inode] = tracker
	}

	tracker.lastAccess = time.Now()

	// Check if this read is sequential (follows the previous read)
	expectedOffset := tracker.lastOffset + readaheadBlockSize
	// Allow some tolerance for reads within the same block or slightly ahead
	isSequential := offset >= tracker.lastOffset && offset <= expectedOffset+readaheadBlockSize

	if isSequential && offset > tracker.lastOffset {
		tracker.sequentialHits++
	} else {
		tracker.sequentialHits = 0
	}
	tracker.lastOffset = offset

	// Trigger readahead after threshold consecutive sequential reads
	if tracker.sequentialHits >= sequentialThreshold {
		// Calculate the range to prefetch
		prefetchStart := offset + int64(size)
		prefetchEnd := prefetchStart + int64(readaheadBlocks)*readaheadBlockSize

		// Don't re-prefetch ranges we've already covered
		if prefetchEnd <= tracker.prefetchedTo {
			return
		}
		if prefetchStart < tracker.prefetchedTo {
			prefetchStart = tracker.prefetchedTo
		}
		tracker.prefetchedTo = prefetchEnd

		rm.statsMu.Lock()
		rm.triggered++
		rm.statsMu.Unlock()

		// Fire background prefetch (non-blocking)
		go rm.prefetch(filePath, prefetchStart, prefetchEnd)
	}
}

// prefetch reads blocks from the JuiceFS FUSE mount to warm the SSD cache.
func (rm *ReadaheadManager) prefetch(filePath string, start, end int64) {
	select {
	case rm.sem <- struct{}{}:
		defer func() { <-rm.sem }()
	default:
		return // all workers busy, skip this round
	}

	select {
	case <-rm.stopCh:
		return
	default:
	}

	fusePath := rm.fusePath + "/" + filePath

	// Use FDPool if available, otherwise fall back to direct open
	var fd *os.File
	var usePool bool
	if rm.fdPool != nil {
		var err error
		fd, err = rm.fdPool.Get(fusePath)
		if err != nil {
			return
		}
		usePool = true
	} else {
		var err error
		fd, err = os.Open(fusePath)
		if err != nil {
			return
		}
	}
	defer func() {
		if usePool {
			rm.fdPool.Release(fusePath)
		} else {
			fd.Close()
		}
	}()

	buf := make([]byte, readaheadBlockSize)
	for off := start; off < end; off += readaheadBlockSize {
		select {
		case <-rm.stopCh:
			return
		default:
		}

		n, err := fd.ReadAt(buf, off)
		if n > 0 {
			rm.statsMu.Lock()
			rm.prefetched++
			rm.statsMu.Unlock()
		}
		if err != nil {
			break // EOF or error
		}
	}
}

// Stats returns readahead statistics.
func (rm *ReadaheadManager) Stats() (triggered, prefetched int64) {
	rm.statsMu.Lock()
	defer rm.statsMu.Unlock()
	return rm.triggered, rm.prefetched
}

// cleanupLoop removes stale tracker entries periodically.
func (rm *ReadaheadManager) cleanupLoop() {
	ticker := time.NewTicker(trackerTTL)
	defer ticker.Stop()
	for {
		select {
		case <-rm.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			rm.mu.Lock()
			for inode, t := range rm.trackers {
				if now.Sub(t.lastAccess) > trackerTTL {
					delete(rm.trackers, inode)
				}
			}
			rm.mu.Unlock()
		}
	}
}

// Stop shuts down the readahead manager.
func (rm *ReadaheadManager) Stop() {
	close(rm.stopCh)
	log.Printf("readahead: stopped (triggered=%d, prefetched=%d blocks)",
		rm.triggered, rm.prefetched)
}
