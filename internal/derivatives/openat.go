package derivatives

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

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
// that need a size and not the bytes. It still opens the descriptor (that is the
// only way to make the symlink check binding) and stats THAT, never the name.
func StatRegularUnder(root, rel string) (os.FileInfo, error) {
	f, err := OpenRegularUnder(root, rel)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Stat()
}

// DerivBlobRel is the mount-relative path of one derivative blob. Callers pass
// this to OpenRegularUnder with the mount point as the anchor, so every
// component below the mount is symlink-checked.
func DerivBlobRel(inode uint64, blobName string) string {
	return filepath.Join(".juicemount", "derivatives",
		fmt.Sprintf("%d", inode), filepath.Clean("/"+blobName))
}
