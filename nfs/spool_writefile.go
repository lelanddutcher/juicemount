package nfs

import (
	"fmt"
	"io"
	"sync"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// spoolWriteFile is the billy.File returned by juiceFS.OpenFile for
// spool-routed writes (slice C). Implements billy.File AND io.WriterAt
// — go-nfs uses a type assertion to call WriteAt on the underlying
// file when available, so it has to be a method on *spoolWriteFile,
// not just on an embedded type.
//
// Lifecycle:
//   - juiceFS.OpenFile constructs this with a fresh SpoolEntry from
//     SpoolStore.OpenWrite (or a re-opened entry under same-path
//     dedupe). incActiveWriter is called in OpenFile to feed the
//     QA-19 phantom-purge gate.
//   - Each NFS WRITE RPC arrives as a WriteAt with offset + bytes.
//     We forward to entry.WriteAt which appends to the on-disk spool
//     file and folds bytes into the streaming SHA-256.
//   - Close decrements the active-writer refcount and closes the
//     spool entry — which fsyncs, finalizes the SHA, and marks the
//     SQL row `ready`. The drainer's wake callback fires.
//
// Read methods are implemented so an O_RDWR client (rare, but
// possible) that writes then reads back the same file gets coherent
// data from the spool file. Slice D adds the cross-process read path
// (OpenFile for read consulting the spool); this slice only covers
// the in-process read-after-write-on-the-same-handle case.
//
// Truncate is intentionally minimal in slice C: zero-size at open
// time is a no-op (the spool file is fresh and empty); any other
// truncation returns an error. Finder copy / Save-As don't need
// arbitrary truncation. Slice F can extend if needed.
type spoolWriteFile struct {
	name    string
	entry   *SpoolEntry
	handler *JuiceMountHandler

	mu  sync.Mutex
	pos int64 // logical seek position for Write/Read
}

// Name returns the in-mount path this file shadows.
func (f *spoolWriteFile) Name() string { return f.name }

// Lock / Unlock are no-ops — NFS doesn't carry flock semantics.
func (f *spoolWriteFile) Lock() error   { return nil }
func (f *spoolWriteFile) Unlock() error { return nil }

// Write appends bytes at the current seek position and advances pos.
// Used by clients that issue write() syscalls (Go io.Writer surface);
// NFS WRITE RPCs use WriteAt directly.
//
// NOT safe for concurrent use by multiple goroutines: the snapshot of
// pos is taken before the WriteAt call and the post-write pos update
// happens after, so two concurrent Writes could read the same pos,
// issue overlapping WriteAt calls, and corrupt the next sequential
// Write's destination offset. Concurrent callers must use WriteAt with
// explicit offsets. (NFS dispatch always uses WriteAt, so this caveat
// only matters if a non-NFS caller composes the io.Writer surface.)
func (f *spoolWriteFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	off := f.pos
	f.mu.Unlock()
	n, err := f.WriteAt(p, off)
	if n > 0 {
		f.mu.Lock()
		f.pos = off + int64(n)
		f.mu.Unlock()
	}
	return n, err
}

// WriteAt is the hot path. NFS WRITE RPCs land here with the protocol
// offset. Forwards to SpoolEntry.WriteAt (which handles capacity
// reservation + streaming SHA + out-of-order detection) and updates
// the handler's writeSizes tracking so Stat/Lstat report the in-flight
// high-water mark before the drainer finishes.
func (f *spoolWriteFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.entry.WriteAt(p, off)
	if n > 0 {
		end := off + int64(n)
		f.handler.trackWriteSize(f.name, end)
		metrics.Default().AddBytesWritten(int64(n))
	}
	return n, err
}

// Read reads at the current seek position and advances pos.
func (f *spoolWriteFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	off := f.pos
	f.mu.Unlock()
	n, err := f.ReadAt(p, off)
	if n > 0 {
		f.mu.Lock()
		f.pos = off + int64(n)
		f.mu.Unlock()
	}
	return n, err
}

// ReadAt opens a fresh read fd on the spool file and reads. Inefficient
// for repeated reads (each call opens + closes); slice C accepts the
// cost because the read-from-write-handle case is rare (Finder copy
// doesn't read what it just wrote). If profiling shows a hot spot,
// cache the read fd on the entry.
func (f *spoolWriteFile) ReadAt(p []byte, off int64) (int, error) {
	rfd, err := f.entry.OpenForRead()
	if err != nil {
		return 0, err
	}
	defer rfd.Close()
	return rfd.ReadAt(p, off)
}

// Seek updates the logical position. Whence behaves per the io.Seeker
// contract. SeekEnd uses the spool entry's current high-water mark.
func (f *spoolWriteFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = f.entry.WrittenEnd() + offset
	default:
		return 0, fmt.Errorf("spoolWriteFile.Seek: invalid whence %d", whence)
	}
	return f.pos, nil
}

// Truncate is intentionally minimal: zero-size is a no-op on a fresh
// entry; any other truncation returns an error. See type doc.
func (f *spoolWriteFile) Truncate(size int64) error {
	if size == 0 {
		if f.entry.WrittenEnd() > 0 {
			return fmt.Errorf("spoolWriteFile.Truncate: nonzero-to-zero truncation not supported in slice C")
		}
		return nil
	}
	return fmt.Errorf("spoolWriteFile.Truncate: arbitrary truncation (size=%d) not supported in slice C", size)
}

// Close finalizes the spool entry, then releases the active-writer
// refcount.
//
// Reviewer fix (slice C HIGH-1): decActiveWriter is deferred, NOT
// called inline before entry.Close. This achieves both correctness
// goals at once:
//   - The phantom-purge gate stays UP throughout entry.Close (fsync +
//     SQL MarkReady). A concurrent Stat that fires during the close
//     window sees an active writer and skips phantom-purge — no
//     transient ENOENT during Finder save/close.
//   - A panic anywhere inside entry.Close still triggers the deferred
//     dec, so the refcount is leak-free even under abnormal exit.
//
// entry.Close itself: fsyncs the spool file, finalizes the streaming
// SHA, marks the SQL row `ready`, and signals the drainer.
func (f *spoolWriteFile) Close() error {
	defer f.handler.decActiveWriter(f.name)
	return f.entry.Close()
}
