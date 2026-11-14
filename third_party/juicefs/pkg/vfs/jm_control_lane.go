/*
 * JuiceMount: a second control file, the control lane (patch 0033).
 *
 * On macOS juicefs keeps one control handle per process (getControlHandle
 * keys it by pid: macFUSE gives every descriptor of a file one FUSE handle,
 * so the kernel's handle cannot tell two opens apart). A close of any
 * descriptor of .control releases that handle and cancels its message. So a
 * process can have only one control message in flight, and the JuiceMount
 * app sends all of them over one channel: the drain's create-files messages
 * (each holds the channel until its uploads end), pins, link policy and the
 * content refresh. Abroad, a refresh waited 1.5-6.7 s behind them, 67-83 s
 * per session, and the player's READs waited for the refresh (laptop run
 * 2026-11-14, bug F2).
 *
 * The lane is another internal file in the root, ".jmcontrol". It speaks
 * exactly the .control protocol and dispatches the same messages. Its
 * handles are keyed by (file, pid), so on macOS a process has one handle
 * per control file, and a message on the lane runs beside one on .control.
 * (On Linux every open already has its own handle.) The file is not listed
 * in the root (readdir lists internalNodes only), and a create of its name
 * in the root fails with EEXIST, as for .control. A juicefs without the
 * patch answers ENOENT to its lookup, and the app falls back to .control.
 */

package vfs

import (
	"os"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// jmControlLaneInode is far from upstream's internal inodes (1-5) and below
// the trash inode.
const jmControlLaneInode = minInternalNode + 0x100

var jmControlLaneNode = &internalNode{jmControlLaneInode, ".jmcontrol", &Attr{Mode: 0666}}

func init() {
	a := jmControlLaneNode.attr
	a.Typ = meta.TypeFile
	a.Nlink = 1
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	now := time.Now().Unix()
	a.Atime, a.Mtime, a.Ctime = now, now, now
	a.Full = true
}

// jmIsControl reports whether ino is a control file: .control or the lane.
func jmIsControl(ino Ino) bool {
	return ino == controlInode || ino == jmControlLaneInode
}

// jmControlKey keys a process's control handle on macOS.
type jmControlKey struct {
	ino Ino
	pid uint32
}
