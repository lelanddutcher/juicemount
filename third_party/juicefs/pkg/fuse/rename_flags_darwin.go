package fuse

import (
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/meta"
	"golang.org/x/sys/unix"
)

func renameCapabilities() uint32 { return fuse.CAP_RENAME_EXCL }

// macFUSE carries renamex_np flags unchanged in the Darwin FUSE rename request.
// JuiceFS metadata uses Linux renameat2 flag values; raw RENAME_EXCL (4) would
// otherwise be interpreted as RenameWhiteout and fail with ENOTSUP.
func normalizeRenameFlags(flags uint32) (uint32, syscall.Errno) {
	switch flags {
	case 0:
		return 0, 0
	case unix.RENAME_SWAP:
		return meta.RenameExchange, 0
	case unix.RENAME_EXCL:
		return meta.RenameNoReplace, 0
	default:
		return 0, syscall.EINVAL
	}
}
