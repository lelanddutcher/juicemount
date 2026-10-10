package farm

import (
	"os"
	"path/filepath"
	"sync/atomic"
)

// tempSeq monotonically distinguishes concurrent temp paths in the same
// directory so two in-flight producers never collide on a temp name.
var tempSeq atomic.Uint64

func nextTempSeq() uint64 { return tempSeq.Add(1) }

// atomicWriteFile writes data to path durably and atomically: it writes to a
// uniquely-named temp file in the SAME directory, fsyncs the file's bytes,
// closes it, fsyncs the parent directory, then os.Renames it onto the final
// path. Rename is atomic on the same filesystem, so a concurrent reader of
// `path` (e.g. OpenLoupe pulling a farm derivative blob) ever sees either the
// previous complete file or the new complete file — NEVER a truncated /
// half-written blob. The temp file shares the destination directory so the
// rename stays intra-filesystem; on any error before the rename the temp file
// is removed so we never leak partial ".tmp-*" siblings.
//
// Use this for every in-process derivative-blob write. ffmpeg-produced blobs
// (proxy/thumbnail/filmstrip) can't hand us their bytes, so they encode to a
// temp path and finish with a descriptor-relative commit instead.
//
// SCOPE: this writes by PATH, so it is only safe for targets OUTSIDE the
// consumer-writable derivative tree — the changes feed, the farm status file,
// per-asset assertion sidecars. Anything under .juicemount/derivatives must use
// internal/derivatives' descriptor-relative helpers. The temp-sibling pair that
// used to live here (atomicTempPath + atomicCommitFile) was deleted with the
// ffmpeg generators that called it: the sibling was not created by us, so it
// could already BE a planted symlink, and rename/open/syncDir all resolve by
// name — a root-privileged write out of the volume on the farm host.
func atomicWriteFile(path string, data []byte, perm os.FileMode) (err error) {
	dir := filepath.Dir(path)
	tmp, terr := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if terr != nil {
		return terr
	}
	tmpName := tmp.Name()
	// On any failure path, drop the temp file so no partial sibling lingers.
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	if err = tmp.Chmod(perm); err != nil {
		return err
	}
	if _, err = tmp.Write(data); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return err
	}
	// fsync the parent dir so the rename (the namespace change) is durable too.
	syncDir(dir)
	return nil
}

// syncDir best-effort fsyncs a directory so a contained rename/create is durable.
// Directory fsync is advisory on some platforms; failures are non-fatal because
// the rename itself already gave us atomicity for the reader-visibility guarantee.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}
