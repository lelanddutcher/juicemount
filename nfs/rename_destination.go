package nfs

import (
	"syscall"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Refuse conflicts already visible in the local namespace before canceling
// destination uploads or moving source uploads. Backing rename still decides
// unknown destinations; this check does not exclude external backend writers.
func (jfs *juiceFS) checkKnownRenameDestination(source, destination *metadata.Entry, newpath string) error {
	if source == nil {
		return nil
	}
	if destination != nil {
		if source.IsDir && !destination.IsDir {
			return syscall.ENOTDIR
		}
		if !source.IsDir && destination.IsDir {
			return syscall.EISDIR
		}
	}
	if source.IsDir {
		present, err := jfs.handler.store.HasKnownChildren(newpath)
		if err != nil {
			return err
		}
		if present {
			return syscall.ENOTEMPTY
		}
	}
	return nil
}
