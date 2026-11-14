package meta

import (
	"syscall"
	"time"
)

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

// JuiceMount patch 0032: slice lists that a refresh can trust.
//
// A Read that went to the engine before a refresh dropped the cached lists
// could store its (older) list after the drop, and later Reads would use it.
// jmChunkGen and jmCacheChunkAt close that window: Read stores a list only
// when no invalidation of the file happened while it was in flight.

// jmChunkGen returns the open file and its invalidation count, for
// jmCacheChunkAt. of is nil when the file is not open.
func (o *openfiles) jmChunkGen(ino Ino) (*openFile, uint64) {
	o.Lock()
	defer o.Unlock()
	of := o.files[ino]
	if of == nil {
		return nil, 0
	}
	return of, of.jmGen
}

// jmCacheChunkAt is CacheChunk for a list read after jmChunkGen returned
// (of, gen): it stores the list only if the same open file has had no
// invalidation since.
func (o *openfiles) jmCacheChunkAt(ino Ino, indx uint32, cs []Slice, of *openFile, gen uint64) {
	if of == nil {
		return
	}
	o.Lock()
	defer o.Unlock()
	if o.files[ino] != of || of.jmGen != gen {
		return
	}
	if indx == 0 {
		of.first = cs
		of.firstAt = time.Time{}
		return
	}
	if of.chunks == nil {
		of.chunks = make(map[uint32][]Slice)
	}
	of.chunks[indx] = cs
}

// JMReadChunkFresh reads one chunk's slice list from the engine, never from
// the cache, and caches it as Read does. The refresh message (pkg/vfs) calls
// it after JMRefreshOpenFile, to learn which read buffers still hold the
// file's current bytes.
func (m *baseMeta) JMReadChunkFresh(ctx Context, inode Ino, indx uint32, slices *[]Slice) syscall.Errno {
	inode = m.checkRoot(inode)
	of, gen := m.of.jmChunkGen(inode)
	ss, st := m.en.doRead(ctx, inode, indx)
	if st != 0 {
		return st
	}
	if ss == nil {
		return syscall.EIO
	}
	*slices = buildSlice(ss)
	m.of.jmCacheChunkAt(inode, indx, *slices, of, gen)
	return 0
}
