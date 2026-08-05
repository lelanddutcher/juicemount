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
	cend, wend := f.entry.ReadableBounds()
	// #100 — ._ AppleDouble sidecars must NEVER JUKEBOX-hold an in-flight hole.
	// A large xattr (e.g. a camera's com.blackmagicdesign.thumbnail, ~20KB) makes
	// copyfile write the ._ sidecar with SEEKS — header at 0, FinderInfo at 0x20,
	// the resource fork far later — so the spool file has a sparse hole BELOW
	// writtenEnd. During that same fsetxattr the macOS Quarantine kext reads
	// com.apple.quarantine from the very ._ being written (qtn_track_vnode_if_
	// needed → mac_vnop_getxattr). If that read lands in the hole we return
	// ErrSpoolIncomplete → NFS3ERR_JUKEBOX → the client retries to its ~40s
	// soft-mount timeout → the notorious ~73s-per-file "copies take forever"
	// hang (a plain-disk NFS server has no such window and copies the identical
	// file in ~1s). ._ sidecars are tiny SAME-CLIENT metadata — never the NLE
	// torn-read / black-frame concern #65's JUKEBOX guards — so serve them exactly
	// like a plain server: straight from the (sparse) spool file up to writtenEnd,
	// real bytes where written and zeros in holes, with NO hold. The concurrent
	// xattr WRITES still land fully in the spool and drain intact, so the final ._
	// content is unaffected; only the racing quarantine READ sees a transient
	// zero-hole, which is harmless (it just reads quarantine as absent).
	readEnd := cend
	if f.isAppleDouble {
		readEnd = wend
	}
	if off >= readEnd {
		// GAP B (task #65): at/past the readable prefix. If there are still-
		// expected bytes below the high-water (a not-yet-filled in-flight hole)
		// and the writer is still active, HOLD (JUKEBOX via the sentinel) — a
		// short/EOF read here would let the client treat a partially-arrived file
		// as COMPLETE (silent truncation). A genuine past-end (off>=wend, the file
		// merely appears to grow) — or a writer gone silent past the stall window
		// (wedged/abandoned, the partial is the best available) — reports io.EOF
		// as before, so the client re-stats and reissues / accepts the partial.
		// ._ sidecars skip the hold entirely (readEnd==wend already, so off>=wend
		// here → plain EOF, matching a durable local server).
		if !f.isAppleDouble && off < wend && time.Since(f.entry.LastWrite()) < pin.SpoolIncompleteStallWindow {
			// Diagnostic (throttled): a real (non-._) file JUKEBOX-held because
			// the read offset is past the contiguous prefix but below the
			// high-water. Post-coalesce-fix this should be rare (a genuine
			// not-yet-arrived hole); a burst keyed to an export path at its end
			// is the "connection interrupted" smoking gun.
			f.entry.logInflightJukebox(f.name, off, cend, wend)
			return 0, pin.ErrSpoolIncomplete
		}
		return 0, io.EOF
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
