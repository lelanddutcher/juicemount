package meta

import "syscall"

// JuiceMount patch 0031: the metadata half of the JMRefreshInode control
// message (pkg/vfs/jm_refresh.go).
//
// juicefs keeps an open file's slice list (openfiles) until it sees a new
// mtime, and it looks for one only on an open or a GETATTR. On macOS, macFUSE
// gives every open descriptor of a file one shared FUSE handle, so a second
// open() never reaches juicefs while the first is open, and the kernel's
// attribute cache hides GETATTRs for --attr-cache. A file the app keeps open
// therefore kept serving the slices it had before another client's write.

// JMRefreshOpenFile drops every cached slice list of inode and reads its
// attributes from the engine (one metadata round trip). The drop comes
// first, so a Read that starts after it fetches the slices again. The fresh
// attributes are stored for the open file as GetAttr does: when nothing else
// invalidated the file meanwhile, a GETATTR within openAttrReuse is answered
// from them without another round trip (patch 0028).
func (m *baseMeta) JMRefreshOpenFile(ctx Context, inode Ino, attr *Attr) syscall.Errno {
	if attr == nil {
		return syscall.EINVAL
	}
	inode = m.checkRoot(inode)
	m.of.InvalidateChunk(inode, invalidateAllChunks)
	gen := m.of.chunkGen()
	if err := m.en.doGetAttr(ctx, inode, attr); err != 0 {
		return err
	}
	m.of.UpdateFresh(inode, attr, gen)
	return 0
}
