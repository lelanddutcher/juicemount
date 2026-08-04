package derivatives

import (
	"fmt"
	"os"
	"syscall"
)

// OpenRegularNoSymlink opens a file under the derivative tree, refusing anything
// that is not a plain regular file.
//
// SECURITY (2026-08-04, second review — CRITICAL). The derivative tree is
// writable by any consumer with mount access; that is the premise of
// contribute-back, not a hypothetical. A symlink planted at a reserved blob name
// therefore turns EVERY reader of that tree into an arbitrary local-file read.
//
// The first fix guarded only the two /blob call sites that the first review
// named. That was reported-site-specific rather than threat-model-complete: the
// background thumb warmer and the /thumb-local populate path followed symlinks
// too, and the warmer fires off an ordinary Finder directory listing with NO
// HTTP request at all — it copied the target's bytes into the local persistent
// thumb cache, which the "fixed" serve path then served happily, because by then
// the cached file is a perfectly regular file. A guard that checks file TYPE
// does not check PROVENANCE.
//
// LIMITATION, stated so nobody assumes more than this gives: O_NOFOLLOW binds
// only the FINAL path component. An attacker who can symlink an ANCESTOR
// directory (e.g. `.juicemount/derivatives/<inode>` itself) still redirects the
// read, because the kernel resolves intermediate components normally. Closing
// that needs namespace protection for `.juicemount/` at the NFS layer, which
// does not exist today — see task #4.
func OpenRegularNoSymlink(path string) (*os.File, error) {
	if li, err := os.Lstat(path); err != nil {
		return nil, err
	} else if !li.Mode().IsRegular() {
		return nil, fmt.Errorf("refusing non-regular derivative %q (mode %s)", path, li.Mode())
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	fi, serr := f.Stat()
	if serr != nil {
		f.Close()
		return nil, serr
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("refusing non-regular derivative %q (mode %s)", path, fi.Mode())
	}
	return f, nil
}

// StatRegularNoSymlink is the stat-only sibling: it refuses a non-regular file
// rather than silently reporting the symlink target's metadata.
func StatRegularNoSymlink(path string) (os.FileInfo, error) {
	li, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !li.Mode().IsRegular() {
		return nil, fmt.Errorf("refusing non-regular derivative %q (mode %s)", path, li.Mode())
	}
	return li, nil
}
