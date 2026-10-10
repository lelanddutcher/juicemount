package nfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
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
	// isAppleDouble marks a ._ AppleDouble sidecar. Such a read NEVER
	// JUKEBOX-holds an in-flight hole (see ReadAt / IncompleteAt, #100).
	isAppleDouble bool

	mu sync.Mutex
	fd spoolReadDescriptor // lazily opened on first Read/ReadAt
	// destFD is the in-flight backend copy, opened lazily the first time a read
	// lands below punchedEnd (see readFromStreamDestLocked). Nil for the
	// overwhelming majority of handles, which never touch a punched range.
	destFD spoolReadDescriptor
	pos    int64
}

// Production descriptors are local spool or destination *os.File handles.
// Keeping the read/close contract narrow also permits deterministic I/O barriers.
type spoolReadDescriptor interface {
	io.ReaderAt
	io.Closer
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
	if len(p) == 0 {
		// A zero-byte read does not enter an incomplete or punched range.
		return 0, f.ensureFDLocked()
	}
	n, err := f.readAtLocked(p, f.pos)
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
	return f.readAtLocked(p, off)
}

// readAtLocked applies the same admission and routing to sequential and
// positional reads. Caller holds f.mu.
func (f *spoolReadFile) readAtLocked(p []byte, off int64) (int, error) {
	if f.entry.readBlocked.Load() {
		return 0, syscall.EIO
	}
	if err := f.ensureFDLocked(); err != nil {
		// GAP A (task #65) — the drain-evict race: the spool file was unlinked
		// after our LookupActive hit but before this first read opened its fd. The
		// bytes are already in FUSE at the PUBLISHED size (the drainer publishes
		// the real size BEFORE it evicts+unlinks), so signal the protocol layer to
		// make the client REOPEN onto the drained copy (NFS3ERR_NOENT) rather than
		// fail the read — a terminal NFSStatusIO here would abort the copy (100060).
		// errors.Is (NOT os.IsNotExist): OpenForRead wraps the open failure with
		// %w ("spool: open read: %w"), and os.IsNotExist does NOT unwrap — so it
		// returned false on the drain-evict race, dropping through to a terminal
		// NFS3ERR_IO that ABORTS the copy instead of NFS3ERR_NOENT (which makes
		// the client reopen onto the drained FUSE copy). errors.Is unwraps.
		if errors.Is(err, os.ErrNotExist) {
			if f.entry.retired.Load() {
				return 0, pin.ErrSpoolDrained
			}
			return 0, syscall.EIO
		}
		return 0, err
	}
	// A routing snapshot alone is insufficient: publishPunchedEnd could move
	// and the spool could be punched before pread uses it. Hold the entry's
	// read lock continuously through LOCAL spool I/O. OpenForRead above must
	// precede this lock because it takes its own RLock.
	total := 0
	for {
		f.entry.mu.RLock()
		cend, wend := f.entry.contiguousEnd, f.entry.writtenEnd
		punched, streamDest := f.entry.punchedEnd, f.entry.streamDest
		switch planReadAt(off, readPlanInputs{
			punchedEnd:    punched,
			contiguousEnd: cend,
			writtenEnd:    wend,
			writerActive:  time.Since(f.entry.LastWrite()) < pin.SpoolIncompleteStallWindow,
			isAppleDouble: f.isAppleDouble,
		}) {
		case planDest:
			// Only the published prefix belongs to this destination snapshot.
			// Never hold entry.mu across a destination open/read: it may be
			// remote FUSE I/O. A remaining tail gets a fresh routing decision.
			f.entry.mu.RUnlock()
			if streamDest == "" {
				return total, fmt.Errorf("spool: read at %d is below punchedEnd %d but the "+
					"entry has no stream destination: refusing to serve a punched range",
					off, punched)
			}
			count := len(p)
			if int64(count) > punched-off {
				count = int(punched - off)
			}
			n, err := f.readFromStreamDestLocked(p[:count], off, streamDest)
			total += n
			// EOF inside a promised destination range is a failed read, not
			// logical file EOF. NFS otherwise accepts it as a truncated file.
			if errors.Is(err, io.EOF) || (err == nil && n != count) {
				err = fmt.Errorf("spool: short acknowledged destination read at %d: %w", off, io.ErrUnexpectedEOF)
			}
			if err != nil || n == len(p) {
				return total, err
			}
			p, off = p[n:], off+int64(n)
		case planHold:
			f.entry.mu.RUnlock()
			f.entry.logInflightJukebox(f.name, off, cend, wend)
			return total, pin.ErrSpoolIncomplete
		case planEOF:
			f.entry.mu.RUnlock()
			return total, io.EOF
		default:
			// Match planReadAt's readable bound, including the ._ exemption.
			// Preserve short non-EOF reads at a growing contiguous prefix;
			// the next request performs the hole/EOF classification.
			readEnd := cend
			if f.isAppleDouble {
				readEnd = wend
			}
			if int64(len(p)) > readEnd-off {
				p = p[:readEnd-off]
			}
			n, err := f.fd.ReadAt(p, off)
			f.entry.mu.RUnlock()
			return total + n, err
		}
	}
}

// readFromStreamDestLocked serves a punched range from the in-flight backend
// copy. Caller holds f.mu.
//
// The fd is opened lazily and kept for the handle's lifetime, mirroring the
// spool fd: a punched read is most often a Finder thumbnail probe walking the
// head of a file, so reopening per RPC would pay a FUSE open (~60ms) for every
// one of them.
func (f *spoolReadFile) readFromStreamDestLocked(p []byte, off int64, destPath string) (int, error) {
	if f.destFD == nil {
		fd, err := os.Open(destPath)
		if err != nil {
			// The rename window: finishStream has published the file at its real
			// path and the temp name is gone, but we read streamDest a moment
			// before it was re-pointed. Deliberately not closed by holding e.mu
			// across the rename — that would put an unbounded FUSE syscall under a
			// hot lock (the cachedCeiling mistake, 2026-08-04 HIGH).
			//
			// ErrSpoolDrained makes the client REOPEN onto the real file, which is
			// exactly the recovery the drain-evict race (GAP A) already uses. The
			// bytes are whole and present; only this handle is stale.
			if os.IsNotExist(err) {
				return 0, pin.ErrSpoolDrained
			}
			// Anything else is a real failure. No fallback to the spool — those
			// bytes are holes, and serving them would be silent corruption.
			return 0, fmt.Errorf("spool: open stream destination %q: %w", destPath, err)
		}
		f.destFD = fd
	}
	return f.destFD.ReadAt(p, off)
}

// IncompleteAt implements the internal/nfs incompleteReader gate. It reports
// whether a read at off would land in a not-yet-written hole of a STILL-ARRIVING
// file: at/past the readable contiguous prefix (off>=cend) but below the
// high-water of expected bytes (off<wend), with the writer still active. Post-#85
// the reported size is writtenEnd, so onRead's size-clamp EOF branch no longer
// zeroes Count for spool holes (off<size there) and spoolReadFile.ReadAt now
// decides the hole JUKEBOX; onRead retains its IncompleteAt call only as a
// defensive backstop for the off>=size path. Mirrors ReadAt's boundary
// classification exactly (task #65). Cheap: one RLock via ReadableBounds + an
// atomic LastWrite load.
func (f *spoolReadFile) IncompleteAt(off int64) bool {
	// #100: ._ AppleDouble sidecars never hold — mirror ReadAt's readEnd==wend.
	if f.isAppleDouble {
		return false
	}
	cend, wend := f.entry.ReadableBounds()
	if off < cend || off >= wend {
		return false
	}
	return time.Since(f.entry.LastWrite()) < pin.SpoolIncompleteStallWindow
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
	// Release BOTH descriptors unconditionally. The previous early return on
	// `f.fd == nil` would have leaked destFD; that is unreachable today only
	// because ReadAt opens fd before it can ever open destFD, which is an
	// ordering coincidence rather than a guarantee. An fd leak here is
	// particularly bad: destFD is a FUSE handle, the repo has no Setrlimit
	// anywhere, and the audit already measured orphaned fds accumulating under
	// rename storms.
	var firstErr error
	if f.fd != nil {
		if err := f.fd.Close(); err != nil {
			firstErr = err
		}
		f.fd = nil
	}
	if f.destFD != nil {
		if err := f.destFD.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		f.destFD = nil
	}
	return firstErr
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

// Sys MUST return a *syscall.Stat_t, not nil.
//
// THE BUG THIS FIXES (measured live 2026-08-05). nfslib's ToFileAttribute asks
// file.GetInfo(info) for the real attributes; that returns nil unless Sys() is a
// *syscall.Stat_t (internal/nfs/file/file_unix.go), and the nil branch
// (internal/nfs/file.go:122) FABRICATES the fileid as `fnv.New64(path)` and
// leaves UID/GID at 0. So for the whole spool window every GETATTR on an
// in-flight file reported:
//
//   - an inode that is a hash of the PATH — no relation to any real inode, and
//     not a key in any store, so nothing can ever look it up; and
//   - owner root:wheel, because 0 is the zero value.
//
// Both were invisible because READDIR takes a different path
// (metadata.FileInfo.Sys() does return a Stat_t), so `ls` looked right while
// `stat` did not. Measured on the live volume: stat said 8983717010171850480
// (0x7cac9305e1ed6ef0) for a file READDIR correctly reported as 2002031.
//
// This is not merely a derivative-registration problem. Any client that uses
// st_ino for file identity — hardlink detection, backup/rsync change detection,
// an NLE relinking media — was handed a fabricated value during the window.
//
// The inode was already sitting in the struct (spoolFileInfoForEntry sets it);
// it was simply being thrown away here. It may still be a SYNTHETIC inode when
// the file was created offline or is not yet reconciled, but a synthetic inode
// is at least honest and detectable (high bit set) and agrees with READDIR — a
// path hash is neither.
func (i *spoolFileInfo) Sys() any {
	return &syscall.Stat_t{
		Ino:   i.inode,
		Nlink: 1,
		Uid:   spoolOwnerUID,
		Gid:   spoolOwnerGID,
	}
}

// The spool file is written through the mount by the logged-in user, and the
// mount is single-user, so the process identity is the file's owner. Resolved
// once: os.Getuid/os.Getgid are syscalls and this sits on the GETATTR hot path.
var (
	spoolOwnerUID = uint32(os.Getuid())
	spoolOwnerGID = uint32(os.Getgid())
)

// spoolFileInfoForEntry constructs a FileInfo snapshot from a SpoolEntry.
// Filename is `base` — the trailing component of the NFS path — so
// Finder sees the right name; the full path is in the spoolReadFile.
func spoolFileInfoForEntry(base string, e *SpoolEntry) *spoolFileInfo {
	return &spoolFileInfo{
		name: base,
		// Report the WRITTEN HIGH-WATER (writtenEnd), NOT the contiguous prefix
		// (#85/#65/#38). Under macOS out-of-order async WRITE dispatch, contiguousEnd
		// pins at a block boundary (~4 MiB) while the true high-water races to full,
		// so reporting contiguousEnd made GETATTR/Stat/Lstat return a truncated size
		// after a large write until the macOS attr cache refreshed — and that stale
		// size also rode out in WRITE/COMMIT reply post-op attrs (onWrite/onCommit →
		// tryStat → Lstat → here), seeding the client's attr cache wrong.
		//
		// This is SAFE to decouple from the readable prefix: the read path
		// (spoolReadFile.ReadAt/ReadableBounds/IncompleteAt) independently clamps to
		// contiguousEnd and JUKEBOX-holds an in-flight hole rather than serving zeros,
		// so a read directed at [contiguousEnd,writtenEnd) never fabricates data even
		// though the reported size now covers it. writtenEnd is monotonic on WriteAt
		// and an authoritative SETATTR{size}/Truncate shrink lowers it (spool.go
		// Truncate sets writtenEnd=size), so a client-commanded shrink is still
		// honored — never masked by a stale high-water. A preallocate-then-fill writer
		// (fio ftruncate up-front) over-reports here, which is acceptable: reads into
		// the unfilled region hold/JUKEBOX or read zeros correctly, no truncation.
		size:  e.WrittenEnd(),
		mtime: e.LastWrite(),
		inode: e.Inode(),
	}
}
