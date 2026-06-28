package farm

import (
	"fmt"
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
// temp path and finish with atomicCommitFile instead.
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

// atomicCommitFile durably and atomically publishes a fully-written temp file
// onto finalPath via os.Rename, then fsyncs the parent directory. tmpPath MUST
// be on the same filesystem as finalPath (callers create it in the same
// directory) so the rename is atomic. Used by the ffmpeg derivative producers,
// which encode to tmpPath and then commit, so OpenLoupe never observes a
// partially-encoded proxy/thumbnail/filmstrip at finalPath.
func atomicCommitFile(tmpPath, finalPath string) error {
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	syncDir(filepath.Dir(finalPath))
	return nil
}

// atomicTempPath returns a unique sibling path of finalPath suitable for an
// ffmpeg output target. It lives in the same directory (same filesystem) so the
// subsequent atomicCommitFile rename is atomic. The ".tmp-<pid>-<unique>" suffix
// keeps it out of the way of directory listings that match the blob's real name.
func atomicTempPath(finalPath string) string {
	dir := filepath.Dir(finalPath)
	base := filepath.Base(finalPath)
	return filepath.Join(dir, fmt.Sprintf(".%s.tmp-%d-%d", base, os.Getpid(), nextTempSeq()))
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
