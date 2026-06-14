package nfs

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// spoolReadFile is the billy.File returned by juiceFS.OpenFile when a
// read open hits an active spool entry (slice D). Serves bytes from the
// on-disk spool file via a single lazily-opened fd that's reused for
// the lifetime of the file handle.
//
// Distinct from spoolWriteFile: read-only, no SHA tracking, no
// active-writer refcount. Multiple concurrent readers can each hold
// their own spoolReadFile against the same SpoolEntry — the underlying
// spool file supports concurrent pread independent of the writer's
// pwrite path.
//
// Write methods return io.ErrClosedPipe (we're explicitly read-only;
// O_RDWR opens that fall into the spool branch are slice-D-out-of-scope;
// the OpenFile dispatcher routes only read flags here).
type spoolReadFile struct {
	name  string
	entry *SpoolEntry

	mu  sync.Mutex
	fd  *os.File // lazily opened on first Read/ReadAt
	pos int64
}

// Name returns the in-mount path.
func (f *spoolReadFile) Name() string { return f.name }

// Lock / Unlock are no-ops — NFS doesn't carry flock semantics.
func (f *spoolReadFile) Lock() error   { return nil }
func (f *spoolReadFile) Unlock() error { return nil }

// Write / Truncate are explicitly rejected. spoolReadFile is the
// read-side billy.File; writes go through spoolWriteFile.
func (f *spoolReadFile) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("spoolReadFile.Write: read-only file")
}

func (f *spoolReadFile) Truncate(size int64) error {
	return fmt.Errorf("spoolReadFile.Truncate: read-only file")
}

// ensureFD opens the spool file fd on demand. Holding it across reads
// (vs reopen-per-read) avoids the per-RPC open() cost — exactly what
// fdPool does for the legacy FUSE read path. Called under f.mu.
func (f *spoolReadFile) ensureFDLocked() error {
	if f.fd != nil {
		return nil
	}
	fd, err := f.entry.OpenForRead()
	if err != nil {
		return err
	}
	f.fd = fd
	return nil
}

// Read reads at the current seek position and advances pos.
func (f *spoolReadFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ensureFDLocked(); err != nil {
		return 0, err
	}
	n, err := f.fd.ReadAt(p, f.pos)
	if n > 0 {
		f.pos += int64(n)
	}
	if err == io.EOF && n > 0 {
		err = nil // emit bytes first; next call returns EOF
	}
	return n, err
}

// ReadAt is the hot read path used by NFS READ RPCs.
func (f *spoolReadFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ensureFDLocked(); err != nil {
		return 0, err
	}
	// Never serve bytes from an unwritten region of an in-flight file. A
	// preallocated (ftruncate) or out-of-order write leaves a hole that a raw
	// pread returns as ZEROS with err=nil — indistinguishable from real data,
	// so an NLE reading a still-copying clip renders black frames / corrupt
	// RAW. Clamp every read to the contiguous-written prefix; anything beyond
	// it is not on disk yet, so report EOF at that boundary (the file appears
	// to grow, the client re-stats and reissues) rather than fabricate zeros.
	cend := f.entry.ContiguousEnd()
	if off >= cend {
		return 0, io.EOF
	}
	if int64(len(p)) > cend-off {
		p = p[:cend-off]
	}
	n, err := f.fd.ReadAt(p, off)
	// A short read because we clamped at the contiguous boundary is NOT a real
	// EOF if the file is still growing; the FileInfo.Size (also clamped to
	// contiguousEnd) keeps the client coherent, so a true past-end read above
	// returns io.EOF and a clamped read returns the available bytes.
	return n, err
}

// Seek updates the logical position.
func (f *spoolReadFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		// SeekEnd is relative to the READABLE end (contiguous-written), not the
		// preallocated high-water, so a seek-to-end never lands in a hole.
		f.pos = f.entry.ContiguousEnd() + offset
	default:
		return 0, fmt.Errorf("spoolReadFile.Seek: invalid whence %d", whence)
	}
	return f.pos, nil
}

// Close releases the underlying fd. Idempotent.
func (f *spoolReadFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fd == nil {
		return nil
	}
	err := f.fd.Close()
	f.fd = nil
	return err
}

// spoolFileInfo is the os.FileInfo returned by Stat/Lstat for a path
// currently in the spool. Surfaces the entry's writtenEnd as Size()
// and lastWrite as ModTime() so Finder sees the file growing in
// real time during a copy.
type spoolFileInfo struct {
	name  string
	size  int64
	mtime time.Time
	inode uint64
}

func (i *spoolFileInfo) Name() string       { return i.name }
func (i *spoolFileInfo) Size() int64        { return i.size }
func (i *spoolFileInfo) Mode() os.FileMode  { return 0o644 }
func (i *spoolFileInfo) ModTime() time.Time { return i.mtime }
func (i *spoolFileInfo) IsDir() bool        { return false }
func (i *spoolFileInfo) Sys() any           { return nil }

// spoolFileInfoForEntry constructs a FileInfo snapshot from a SpoolEntry.
// Filename is `base` — the trailing component of the NFS path — so
// Finder sees the right name; the full path is in the spoolReadFile.
func spoolFileInfoForEntry(base string, e *SpoolEntry) *spoolFileInfo {
	return &spoolFileInfo{
		name: base,
		// Report the contiguous-written size, NOT the preallocated high-water:
		// onRead clamps reads to this, so a read can never be directed into an
		// unwritten hole (which pread would return as zeros). The file appears
		// to grow as data lands; once drained, reads come from FUSE at full
		// size. Keeps the read clamp and the no-zeros guarantee consistent.
		size:  e.ContiguousEnd(),
		mtime: e.LastWrite(),
		inode: e.Inode(),
	}
}
