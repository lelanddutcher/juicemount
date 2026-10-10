//go:build linux

package nfs

import "golang.org/x/sys/unix"

func replacementFDSetMtime(fd int, ns int64) error {
	stamp := unix.NsecToTimespec(ns)
	return unix.UtimesNanoAt(fd, "", []unix.Timespec{stamp, stamp}, unix.AT_EMPTY_PATH)
}
