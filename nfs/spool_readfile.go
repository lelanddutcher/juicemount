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
	fd *os.File // lazily opened on first Read/ReadAt
	// destFD is the in-flight backend copy, opened lazily the first time a read
	// lands below punchedEnd (see readFromStreamDestLocked). Nil for the
	// overwhelming majority of handles, which never touch a punched range.
	destFD *os.File
	pos    int64
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
			return 0, pin.ErrSpoolDrained
		}
		return 0, err
	}
	// Never serve bytes from an unwritten region of an in-flight file. A
	// preallocated (ftruncate) or out-of-order write leaves a hole that a raw
	// pread returns as ZEROS with err=nil — indistinguishable from real data,
	// so an NLE reading a still-copying clip renders black frames / corrupt
	// RAW. Clamp every read to the contiguous-written prefix and classify a
	// past-prefix offset against a CONSISTENT (cend,wend) snapshot.
	cend, wend, punched, streamDest := f.entry.readRouting()
	// Route BEFORE touching the spool file. planReadAt puts the punched check
	// ahead of every other classification for the reason documented there: a
	// punched range is below contiguousEnd by construction, so any path that
	// consults the prefix first serves zeros as file content.
	//
	// punched is 0 unless the streaming drain is running, so with streaming off
	// every branch below resolves exactly as it did before this routing existed.
	switch planReadAt(off, readPlanInputs{
		punchedEnd:    punched,
		contiguousEnd: cend,
		writtenEnd:    wend,
		writerActive:  time.Since(f.entry.LastWrite()) < pin.SpoolIncompleteStallWindow,
		isAppleDouble: f.isAppleDouble,
	}) {
	case planDest:
		// These bytes were drained to the backend and punched out of the spool.
		// Serve them from the destination copy.
		//
		// READ COHERENCE IS ALREADY GUARANTEED BY THE WRITE ORDERING, and that is
		// not an accident. advanceStream fsyncs the destination BEFORE it
		// publishes punchedEnd, so any offset that can reach this branch has been
		// fsynced. That matters because a plain read-after-write against JuiceFS
		// is NOT reliable: the drainer's own note records ~0.3% of files in a
		// 2000-file storm returning inconsistent bytes on an unsynced readback,
		// which its SHA check then mis-diagnosed as a bit flip. If anyone moves
		// that fsync after the publish, they break read coherence as well as
		// crash durability.
		//
		// Still fails closed when the path is unset: a punched range with no
		// destination is unservable, and falling through to the spool would
		// return zeros as file content.
		if streamDest == "" {
			return 0, fmt.Errorf("spool: read at %d is below punchedEnd %d but the "+
				"entry has no stream destination: refusing to serve a punched range",
				off, punched)
		}
		return f.readFromStreamDestLocked(p, off, streamDest)
	case planHold:
		// Diagnostic (throttled): a real (non-._) file JUKEBOX-held because the
		// read offset is past the contiguous prefix but below the high-water.
		// Post-coalesce-fix this should be rare (a genuine not-yet-arrived hole);
		// a burst keyed to an export path at its end is the "connection
		// interrupted" smoking gun.
		f.entry.logInflightJukebox(f.name, off, cend, wend)
		return 0, pin.ErrSpoolIncomplete
	case planEOF:
		return 0, io.EOF
	}
	// planSpool: the bytes are present and safe to read.
	//
	// GAP B (task #65) — the at/past-prefix classification that used to live here
	// (hold vs EOF) and the #100 ._ sidecar exemption both moved into planReadAt,
	// which decides them from the same (cend, wend, punched) snapshot. The
	// rationale for each lives there; see #100 in particular before changing the
	// sidecar branch, because that exemption is what fixed the ~73s-per-file copy
	// stall and it is not an optimisation.
	//
	// readEnd is recomputed here only to CLAMP the buffer. It must mirror
	// planReadAt's readEnd exactly — if the two ever disagree, a read classified
	// as planSpool could still be clamped against the wrong boundary.
	readEnd := cend
	if f.isAppleDouble {
		readEnd = wend
	}
	if int64(len(p)) > readEnd-off {
		p = p[:readEnd-off]
	}
	n, err := f.fd.ReadAt(p, off)
	// A short read because we clamped at the contiguous boundary is NOT a real
	// EOF if the file is still growing. Post-#85 FileInfo.Size reports writtenEnd
	// (the full high-water), DECOUPLED from this contiguous prefix — so a read CAN
	// be directed into [contiguousEnd, writtenEnd), a genuine in-flight hole. The
	// cend clamp + JUKEBOX hold above is therefore the SOLE authoritative guard
	// against serving zeros from a hole; do NOT remove it on the assumption that
	// the reported size already keeps reads within the readable prefix — it no
	// longer does.
	return n, err
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
			// No fallback to the spool here — those bytes are holes. An error is
			// the honest answer; the client retries or reopens.
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
