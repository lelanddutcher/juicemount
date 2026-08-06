//go:build darwin

package nfs

import (
	"os"
	"syscall"
	"unsafe"
)

// PREFIX PUNCHING — return already-drained spool bytes to the filesystem.
//
// This is the primitive the streaming drain is built on. Once a prefix of a
// spool file has been copied to the backend and fsynced there, its blocks are
// dead weight on a disk we are trying not to exhaust. F_PUNCHHOLE deallocates
// them while leaving the file's LOGICAL size untouched — which matters, because
// the spool index, the Stat shadow and the capacity accounting all key off that
// size.
//
// MEASURED on APFS 2026-08-06 (.research/punch), 4096-byte allocation blocks:
//
//	512 MiB file, punch [0,256MiB)  ->  physical 512->256 MiB
//	                                    statfs avail +257 MiB
//	                                    logical size unchanged
//	                                    tail past the punch intact
//
// TWO PROPERTIES THAT DRIVE THE DESIGN, both measured rather than assumed:
//
//  1. A punched range reads back as ZEROS, with no error. It is
//     indistinguishable from legitimately-written zeros. This is the same
//     silent-hole class as the 2026-06-15 black-frame bug, so anything that can
//     read a punched range MUST be routed elsewhere first. Punching is not a
//     local decision.
//
//  2. An unaligned offset or length fails with EINVAL and frees NOTHING. It does
//     not round, and it does not partially apply. That is the safe failure mode,
//     and it means alignment is OUR responsibility.
//
// Alignment therefore always moves INWARD — offset up, end down. Punching one
// block too few wastes 4 KiB; punching one block too many destroys data that was
// never drained. The asymmetry is total, so the rounding direction is not a
// tuning choice.
const (
	// fPunchHole is F_PUNCHHOLE from <sys/fcntl.h>.
	fPunchHole = 99

	// punchBlockSize is the APFS allocation block size. Reads of a file's
	// st_blksize would be more general, but every target volume is APFS at 4 KiB
	// and a wrong-but-larger value here only under-punches (safe direction).
	punchBlockSize = 4096
)

// fpunchhole mirrors fpunchhole_t from <sys/fcntl.h>.
type fpunchhole struct {
	flags    uint32
	reserved uint32
	offset   int64
	length   int64
}

// punchRange deallocates [off,end) from f, rounded INWARD to block boundaries.
//
// Returns the number of bytes actually punched, which may be 0 when the range is
// smaller than a block after alignment — that is a normal outcome, not an error.
//
// The caller is responsible for having durably written this range elsewhere
// first, and for ensuring no reader can still be routed at it. This function
// deliberately performs neither check: it cannot see the drain state or the read
// shadow, and a primitive that pretended to would be worse than one that is
// honestly dumb.
func punchRange(f *os.File, off, end int64) (int64, error) {
	if f == nil || end <= off || off < 0 {
		return 0, nil
	}
	// Inward alignment. See the asymmetry note above.
	alignedOff := (off + punchBlockSize - 1) &^ (punchBlockSize - 1)
	alignedEnd := end &^ (punchBlockSize - 1)
	if alignedEnd <= alignedOff {
		return 0, nil // sub-block range; nothing safely punchable
	}
	length := alignedEnd - alignedOff

	arg := fpunchhole{offset: alignedOff, length: length}
	_, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL, f.Fd(), fPunchHole, uintptr(unsafe.Pointer(&arg)),
	)
	if errno != 0 {
		return 0, errno
	}
	return length, nil
}

// punchSupported reports whether prefix punching can free space on this file's
// filesystem. Determined by TRYING it on a throwaway range rather than by
// inspecting the filesystem type: the streaming drain must never assume it can
// reclaim space it cannot, and a name check would not survive a volume format we
// have not seen.
func punchSupported(f *os.File) bool {
	if f == nil {
		return false
	}
	// Probe a single block far past any plausible content. On a filesystem that
	// supports punching this is a no-op on a sparse region; on one that does not
	// it returns ENOTTY/EINVAL and we fall back to the whole-file drain.
	arg := fpunchhole{offset: 0, length: punchBlockSize}
	_, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL, f.Fd(), fPunchHole, uintptr(unsafe.Pointer(&arg)),
	)
	return errno == 0
}
