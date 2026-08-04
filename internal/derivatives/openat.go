package derivatives

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// This package deliberately offers NO path-only open helper.
//
// It used to: OpenRegularNoSymlink/StatRegularNoSymlink took a single joined
// path and applied Lstat + O_NOFOLLOW + a regular-file check. That is a correct
// guard for the FINAL component and a silent no-op for every ancestor, and the
// gap cost three consecutive review rounds — each one fixed the call sites the
// report happened to name, and the next round found the siblings it did not.
// The last of those was the background thumb warmer, which needs no HTTP
// request at all: an ordinary Finder directory listing was the whole exploit.
//
// So the unsafe shape is gone rather than deprecated. Taking a ROOT and a
// RELATIVE path separately is not ergonomic bookkeeping — it is the only way
// the walk below can check every component, and a function that accepts an
// already-joined path cannot do it no matter how carefully it is written.
// If you find yourself wanting to add one back, that is the bug.

// OpenRegularUnder opens root/rel for reading, refusing a symlink at ANY
// component of rel — not merely the last one.
//
// WHY THIS EXISTS (2026-08-04 round 3, HIGH). O_NOFOLLOW binds only the FINAL
// path component, so the earlier guard was defeated end-to-end from the
// filesystem alone, with no forged DB row: write a real
// .juicemount/derivatives/<N>/ so an ordinary GET /derivatives persists the row
// via the on-miss reconcile, then replace that directory with a symlink to
// anywhere. GET /blob then byte-range-streamed an out-of-tree file, labelled
// with the row's media type, from an unauthenticated 127.0.0.1 origin.
// `poster.jpg` and `proxy.mp4` are ubiquitous names on a video-production Mac.
//
// Each component is opened relative to the previously opened directory's
// descriptor, so there is no window in which a resolved path can be swapped —
// unlike an EvalSymlinks-then-open check, which is inherently TOCTOU.
//
// `root` is the trust anchor and is NOT itself walked: pass a path you control
// (the mount point), not one assembled from request data.
func OpenRegularUnder(root, rel string) (*os.File, error) {
	// Clean through an absolute form so "..", ".", and doubled separators cannot
	// escape upward, then strip the anchor.
	clean := strings.TrimPrefix(filepath.Clean("/"+rel), "/")
	if clean == "" || clean == "." {
		return nil, fmt.Errorf("derivatives: empty relative path under %q", root)
	}

	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("derivatives: open root %q: %w", root, err)
	}

	parts := strings.Split(clean, string(filepath.Separator))
	for i, part := range parts {
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		if i == len(parts)-1 {
			// O_NONBLOCK so a FIFO planted at a blob name returns immediately
			// instead of blocking this goroutine forever waiting for a writer.
			flags |= unix.O_NONBLOCK
		} else {
			flags |= unix.O_DIRECTORY
		}
		next, oerr := unix.Openat(fd, part, flags, 0)
		unix.Close(fd)
		if oerr != nil {
			return nil, fmt.Errorf("derivatives: open %q under %q: %w", clean, root, oerr)
		}
		fd = next
	}

	f := os.NewFile(uintptr(fd), filepath.Join(root, clean))
	// O_NOFOLLOW rejects symlinks; it says nothing about directories, devices or
	// sockets, so the mode is still checked on the descriptor we actually hold.
	fi, serr := f.Stat()
	if serr != nil {
		f.Close()
		return nil, serr
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("refusing non-regular derivative %q (mode %s)", clean, fi.Mode())
	}
	return f, nil
}

// StatRegularUnder is the stat-only companion to OpenRegularUnder, for callers
// that need a size and not the bytes.
//
// It walks the DIRECTORIES exactly as OpenRegularUnder does — so no component
// can be a symlink — but finishes with fstatat(AT_SYMLINK_NOFOLLOW) rather than
// opening the leaf. The first version opened it, which is correct but turns a
// one-syscall lstat into an open per row in ComputeProxyEconomics, a rollup that
// loops over EVERY ready proxy in the index. An open is materially more
// expensive than a stat on JuiceFS/macFUSE, so that traded a real per-row cost
// for no extra safety.
func StatRegularUnder(root, rel string) (os.FileInfo, error) {
	clean := strings.TrimPrefix(filepath.Clean("/"+rel), "/")
	if clean == "" || clean == "." {
		return nil, fmt.Errorf("derivatives: empty relative path under %q", root)
	}
	parts := strings.Split(clean, string(filepath.Separator))

	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("derivatives: open root %q: %w", root, err)
	}
	// Walk every DIRECTORY component; the leaf is stat'ed, never opened.
	for _, part := range parts[:len(parts)-1] {
		next, oerr := unix.Openat(fd, part,
			unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if oerr != nil {
			return nil, fmt.Errorf("derivatives: walk %q under %q: %w", clean, root, oerr)
		}
		fd = next
	}
	defer unix.Close(fd)

	var st unix.Stat_t
	leaf := parts[len(parts)-1]
	if err := unix.Fstatat(fd, leaf, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, fmt.Errorf("derivatives: stat %q under %q: %w", clean, root, err)
	}
	// AT_SYMLINK_NOFOLLOW makes this the LINK's own mode, so a symlink shows up
	// as a symlink here rather than as whatever it points at.
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, fmt.Errorf("refusing non-regular derivative %q (mode %#o)", clean, st.Mode&unix.S_IFMT)
	}
	return &statInfo{name: leaf, size: st.Size, mode: os.FileMode(st.Mode & 0o777),
		mtime: time.Unix(st.Mtim.Sec, st.Mtim.Nsec)}, nil
}

// statInfo adapts a raw unix.Stat_t to os.FileInfo for the stat-only path.
type statInfo struct {
	name  string
	size  int64
	mode  os.FileMode
	mtime time.Time
}

func (s *statInfo) Name() string       { return s.name }
func (s *statInfo) Size() int64        { return s.size }
func (s *statInfo) Mode() os.FileMode  { return s.mode }
func (s *statInfo) ModTime() time.Time { return s.mtime }
func (s *statInfo) IsDir() bool        { return false }
func (s *statInfo) Sys() any           { return nil }

// EnsureDirUnder creates root/rel as a directory chain, refusing to traverse a
// symlink at ANY component — the write-side counterpart to OpenRegularUnder.
//
// os.MkdirAll happily FOLLOWS a symlinked component, so a planted
// .juicemount/derivatives/<inode> -> /somewhere link redirects everything the
// farm subsequently writes (ffmpeg output, atomic sidecar writes) out of the
// volume, on a host where the farm runs as root. Same class as the read-side
// hole, opposite direction.
//
// Each level is created with mkdirat and then re-opened with
// O_NOFOLLOW|O_DIRECTORY relative to its parent's descriptor, so an existing
// symlink is refused rather than traversed.
func EnsureDirUnder(root, rel string) error {
	clean := strings.TrimPrefix(filepath.Clean("/"+rel), "/")
	if clean == "" || clean == "." {
		return fmt.Errorf("derivatives: empty relative dir under %q", root)
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("derivatives: open root %q: %w", root, err)
	}
	defer func() { unix.Close(fd) }()

	for _, part := range strings.Split(clean, string(filepath.Separator)) {
		if err := unix.Mkdirat(fd, part, 0o755); err != nil && err != unix.EEXIST {
			return fmt.Errorf("derivatives: mkdir %q under %q: %w", clean, root, err)
		}
		next, oerr := unix.Openat(fd, part,
			unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if oerr != nil {
			// ELOOP/ENOTDIR here means the component exists but is a symlink (or
			// a file) — exactly the case MkdirAll would have followed.
			return fmt.Errorf("derivatives: %q under %q is not a real directory: %w", part, root, oerr)
		}
		unix.Close(fd)
		fd = next
	}
	return nil
}

// DerivDirRel is the mount-relative directory holding one asset's derivatives.
func DerivDirRel(inode uint64) string {
	return filepath.Join(".juicemount", "derivatives", fmt.Sprintf("%d", inode))
}

// DerivBlobRel is the mount-relative path of one derivative blob. Callers pass
// this to OpenRegularUnder with the mount point as the anchor, so every
// component below the mount is symlink-checked.
func DerivBlobRel(inode uint64, blobName string) string {
	return filepath.Join(".juicemount", "derivatives",
		fmt.Sprintf("%d", inode), filepath.Clean("/"+blobName))
}
