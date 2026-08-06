package nfs

import (
	"fmt"
	"path/filepath"
	"strings"
)

// WHERE A STREAMED FILE IS WRITTEN BEFORE IT IS FINISHED (S4).
//
// THE PROBLEM THIS SOLVES. The existing drainer writes its destination IN PLACE
// at the final path — `os.Create(filepath.Join(fuseRoot, row.NFSPath))`, with no
// temp path and no rename anywhere in drainer.go. That is acceptable today
// because the drain runs AFTER finalize: the spool file is already complete, so
// the destination is only partial for the seconds it takes to copy, and local
// readers are served from the spool shadow throughout.
//
// Streaming breaks that assumption completely. The destination is created when
// the copy STARTS and stays partial for the whole transfer — potentially hours
// for a multi-hundred-GB file on a slow link. For that entire window a real file
// sits at the real path with no marker that it is incomplete, and everything
// that is NOT this client sees it:
//
//   - another Mac, or the farm, or ClipLogger reading through JuiceFS gets a
//     truncated file that looks finished
//   - Redis keyspace events fire for it, so the metadata mirror can publish its
//     partial size as authoritative
//   - a derivative generator could key a thumbnail off half a clip
//
// Our own reads are safe (the spool shadow serves them), which is exactly what
// makes this dangerous: the failure is invisible from the machine doing the
// copy.
//
// So a streamed file is written to a HIDDEN sibling and renamed into place only
// once complete. Rename on JuiceFS is a metadata operation, so the publish is
// atomic from every reader's perspective — the file appears whole or not at all.

// streamTempPrefix marks an in-progress streamed destination.
//
// Leading dot so it is hidden from Finder and from anything walking the tree for
// user content. The `.juicemount-` qualifier makes provenance obvious in a
// directory listing during an incident, and matches the internal-namespace
// convention the scan filter already recognises.
const streamTempPrefix = ".juicemount-streaming-"

// streamTempPath returns the hidden sibling path a streamed destination is
// written to before it is renamed into place.
//
// SIBLING, not a separate staging directory: the rename must be within one
// directory so it is a pure metadata operation with no data movement, and so the
// partial file shares the destination's parent (and therefore its quota,
// permissions and backend placement) exactly as the finished file will.
//
// The entry ID is part of the name because two writers can target the same path
// — a re-copy over an existing file, or a retry after a failed stream — and two
// concurrent streams must never share a partial. Nothing here is a content path,
// so no user-visible name is derived from it.
func streamTempPath(destPath string, entryID int64) (string, error) {
	if destPath == "" {
		return "", fmt.Errorf("stream temp path: empty destination")
	}
	dir := filepath.Dir(destPath)
	base := filepath.Base(destPath)
	if base == "." || base == string(filepath.Separator) {
		return "", fmt.Errorf("stream temp path: %q has no filename component", destPath)
	}
	// Refuse to build a temp name from something that is ALREADY a temp name.
	// A retry that stacked prefixes would create ".juicemount-streaming-42-
	// .juicemount-streaming-41-clip.mov" and, worse, would no longer rename back
	// onto the real path.
	if strings.HasPrefix(base, streamTempPrefix) {
		return "", fmt.Errorf("stream temp path: %q is already a streaming temp name", base)
	}
	return filepath.Join(dir, fmt.Sprintf("%s%d-%s", streamTempPrefix, entryID, base)), nil
}

// isStreamTempName reports whether a directory entry is an in-progress streamed
// destination.
//
// Used to keep partials out of anything that enumerates real content — readdir
// results, the metadata mirror, derivative generation. A partial that leaked
// into a listing would be indistinguishable from a finished file, which is the
// whole failure this file exists to prevent.
func isStreamTempName(name string) bool {
	return strings.HasPrefix(name, streamTempPrefix)
}
