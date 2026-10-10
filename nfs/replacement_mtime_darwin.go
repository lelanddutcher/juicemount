//go:build darwin

package nfs

import (
	"golang.org/x/sys/unix"
	"unsafe"
)

// fsetattrlist preserves nanoseconds on the retained descriptor. Futimes
// truncates to microseconds; a pathname timestamp update could hit a substitute.
func replacementFDSetMtime(fd int, ns int64) error {
	attrs := struct {
		BitmapCount                           uint16
		Reserved                              uint16
		Common, Volume, Directory, File, Fork uint32
	}{BitmapCount: 5, Common: unix.ATTR_CMN_MODTIME}
	stamp := unix.NsecToTimespec(ns)
	_, _, err := unix.Syscall6(unix.SYS_FSETATTRLIST, uintptr(fd), uintptr(unsafe.Pointer(&attrs)), uintptr(unsafe.Pointer(&stamp)), unsafe.Sizeof(stamp), 0, 0)
	if err != 0 {
		return err
	}
	return nil
}
