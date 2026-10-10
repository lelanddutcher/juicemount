package nfs

import (
	"fmt"
	"path"
	"strings"
	"syscall"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
)

// RenameWithParentObservations rechecks both original parent handles and the
// parent file IDs observed by onRename, after admission and before any effects.
func (jfs *juiceFS) RenameWithParentObservations(oldpath, newpath string, from, to nfslib.ParentRenameObservation) error {
	oldpath = strings.TrimPrefix(oldpath, "/")
	newpath = strings.TrimPrefix(newpath, "/")
	// The namespace worker may outlive the RPC's request buffer.
	from.Handle = append([]byte(nil), from.Handle...)
	to.Handle = append([]byte(nil), to.Handle...)
	return jfs.namespaceMutation([]string{oldpath, newpath}, func(op *namespaceOperation) error {
		if !from.Proven || !to.Proven {
			return fmt.Errorf("rename parent file ID unproved: %w", syscall.EAGAIN)
		}
		fromInode, err := jfs.handler.export.inode(from.Handle)
		if err != nil {
			return fmt.Errorf("rename source parent export: %w", syscall.ESTALE)
		}
		toInode, err := jfs.handler.export.inode(to.Handle)
		if err != nil {
			return fmt.Errorf("rename destination parent export: %w", syscall.ESTALE)
		}
		matched, err := jfs.handler.store.RenameParentsMatchObservation(
			metadata.RenameParentObservation{Path: path.Dir(oldpath), HandleInode: fromInode, ObservedFileid: from.Fileid},
			metadata.RenameParentObservation{Path: path.Dir(newpath), HandleInode: toInode, ObservedFileid: to.Fileid},
		)
		if err != nil {
			return fmt.Errorf("rename parent ownership: %w: %v", syscall.EAGAIN, err)
		}
		if !matched {
			return fmt.Errorf("rename parent changed after validation: %w", syscall.EAGAIN)
		}
		return jfs.rename(oldpath, newpath, op)
	})
}
