/*
 * JuiceMount: a read error on an open file is not kept for good (patch 0030).
 *
 * Upstream keeps the first read error of an open file in fileReader.err and
 * answers every later read of that handle with it at once; only a close and a
 * new open clear it. A read that fails because the network is gone (every
 * try fails fast with "network is unreachable", so the retry budget is spent
 * in about 2 s on a slow link) therefore leaves the file unreadable after the
 * network returns, for as long as the handle stays open.
 *
 * On macOS that is until the app restarts: macFUSE gives every open
 * descriptor of a file one shared FUSE handle, so a new open() reaches juicefs
 * only when no descriptor of the file is open anywhere in the process. The
 * app's descriptor pool and the macOS NFS client keep the clip open, so no
 * new open, and no new fileReader, ever comes.
 *
 * Read now clears a stored EIO that is older than jmReadErrRetryAfter, with
 * the retry count, and reads again: the backend is asked again, at most once
 * per jmReadErrRetryAfter while it keeps failing. A missing or corrupt block
 * still fails every try, so that read still returns EIO (after the same retry
 * budget a new open would spend). ENOENT (the file was deleted) stays.
 */

package vfs

import (
	"syscall"
	"time"
)

// jmReadErrRetryAfter: reads within this time of the failure still get the
// stored error at once, as upstream (a burst of reads does not retry the
// backend once each).
var jmReadErrRetryAfter = time.Second

// jmRetryAfterError clears an old stored EIO. Called with f locked.
func (f *fileReader) jmRetryAfterError() {
	if f.err != syscall.EIO || f.closing || time.Since(f.errAt) < jmReadErrRetryAfter {
		return
	}
	logger.Infof("read file %d: retrying after an error %s ago", f.inode, time.Since(f.errAt).Round(time.Millisecond))
	f.err = 0
	f.tried = 0
}
