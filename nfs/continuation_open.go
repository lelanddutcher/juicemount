package nfs

import (
	"os"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/metadata"
)

// continuationFS binds an ordinary NFS WRITE's pathname to its resolved
// durable handle and original spool cohort before it may queue.
type continuationFS struct {
	*juiceFS
	spool       *SpoolStore
	entry       *SpoolEntry
	rowID       int64
	path        string
	incarnation string
	inode       uint64
}

func (h *JuiceMountHandler) continuationFS(inode uint64, e *metadata.Entry) billy.Filesystem {
	base := &juiceFS{handler: h}
	if e == nil || e.IsDir || e.Inode != inode || e.Version.Revision <= 0 || e.Version.Incarnation == "" || e.Version.Path != e.Path {
		return base
	}
	spool := h.spool.Load()
	if spool == nil {
		return base
	}
	entry, ok := spool.LookupActive(e.Path)
	if !ok || entry.Inode() != inode || !entry.writing.Load() {
		return base
	}
	rowID := entry.cohortID.Load()
	if rowID <= 0 {
		return base
	}
	return &continuationFS{juiceFS: base, spool: spool, entry: entry, rowID: rowID, path: e.Path, incarnation: e.Version.Incarnation, inode: inode}
}

func (fs *continuationFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	if flag != os.O_RDWR || cleanNamespaceName(name) != cleanNamespaceName(fs.path) {
		return fs.juiceFS.OpenFile(name, flag, perm)
	}
	deadline := time.Now().Add(mutationOpTimeout())
	release, err := fs.handler.continuations.acquire(fs.path, deadline, func() {
		if fs.handler.namespace.phase != nil {
			fs.handler.namespace.phase("continuation-queued", fs.path)
		}
	})
	if err != nil {
		return nil, err
	}
	defer release()
	return fs.juiceFS.openFileBound(fs.path, flag, perm, fs, deadline)
}
