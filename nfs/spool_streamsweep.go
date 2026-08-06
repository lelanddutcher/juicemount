package nfs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
)

// RECLAIMING ORPHANED STREAM PARTIALS AFTER A CRASH.
//
// THE LEAK THIS CLOSES, AND WHY IT EXISTS AT ALL. A streamed copy writes to a
// hidden sibling of its destination and renames it into place when complete. If
// the process dies mid-copy, that partial is left behind — and we have just
// finished making it invisible to everything that would otherwise notice it:
//
//   - the metadata mirror rejects it (metadata.StreamPartialName), so it never
//     appears in a listing and no prune ladder tracks it
//   - the farm skips it (farm.ExcludeReason), so no derivative job touches it
//   - it is dot-prefixed, so no user browsing the volume will see it
//
// Every one of those is correct in isolation and together they mean NOTHING
// WILL EVER CLEAN IT UP. A partial can be tens or hundreds of gigabytes, and it
// sits on the backend consuming exactly the space this feature exists to free.
// Invisibility and garbage collection have to be added in the same breath.
//
// ROW-DRIVEN, NOT A VOLUME WALK. The temp path is a pure function of
// (destination, entry ID), and a partial can only exist for an entry that had
// not finished — which is precisely an entry whose spool row is not yet `done`.
// So the sweep is bounded by the number of live spool rows, not by the size of
// the volume. Walking the whole mount to find dot-files would be O(volume) on a
// tree that may hold millions of files, over a link that may be cellular.
//
// A row that IS done cannot have a live partial: completion is the rename, and
// after the rename the temp name no longer exists.

// streamPartialCandidates returns the temp paths that may have been orphaned by
// a crash, given the live spool rows and the FUSE root.
//
// Pure so the selection logic is testable without a mount: the decision of WHICH
// paths to delete is the part that can go wrong destructively, and it should not
// require a filesystem to exercise.
func streamPartialCandidates(rows []*metadata.SpoolRow, fuseRoot string) []string {
	if fuseRoot == "" {
		return nil
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if r == nil || r.NFSPath == "" {
			continue
		}
		// A completed entry's partial was renamed into place; there is nothing
		// left under the temp name. Skipping `done` also means a GC'd audit row
		// can never cause us to delete something.
		if r.DrainState == metadata.DrainDone {
			continue
		}
		tmp, err := streamTempPath(filepath.Join(fuseRoot, r.NFSPath), r.ID)
		if err != nil {
			continue
		}
		out = append(out, tmp)
	}
	return out
}

// sweepOrphanedStreamPartials removes stream partials left by a crash.
//
// Returns the number removed and the bytes reclaimed. Missing files are the
// normal case by far — almost no row will have a partial — so a non-existent
// path is not an error and is not logged.
//
// Deliberately conservative about what it deletes: only paths produced by
// streamTempPath for a known, not-yet-done row. It never enumerates a directory
// and never deletes by pattern, so a user file that merely resembles a partial
// cannot be caught by it — the name filter elsewhere is a visibility rule, but
// deletion is driven by identity.
func sweepOrphanedStreamPartials(rows []*metadata.SpoolRow, fuseRoot string) (removed int, bytes int64) {
	for _, p := range streamPartialCandidates(rows, fuseRoot) {
		fi, err := os.Stat(p)
		if err != nil {
			continue // the overwhelmingly normal case: no partial for this row
		}
		if fi.IsDir() {
			// Cannot happen via streamTempPath, but deleting a directory tree on
			// the backend is not a mistake worth being able to make.
			jmlog.Warn("stream sweep: refusing to remove a DIRECTORY at a partial path",
				"path", p)
			continue
		}
		size := fi.Size()
		if err := os.Remove(p); err != nil {
			jmlog.Warn("stream sweep: could not remove orphaned partial",
				"path", p, "bytes", size, "error", err.Error())
			continue
		}
		removed++
		bytes += size
		jmlog.Info("stream sweep: reclaimed orphaned partial from an interrupted copy",
			"path", p, "bytes", size)
	}
	if removed > 0 {
		jmlog.Info("stream sweep: complete", "removed", removed, "bytes_reclaimed", bytes)
	}
	return removed, bytes
}

// SweepStreamPartials runs the orphan sweep for this drainer's mount.
//
// Called at boot, after spool recovery has settled the rows: recovery decides
// which entries are still live, and this only ever removes partials belonging to
// entries that are.
func (d *Drainer) SweepStreamPartials() error {
	if d == nil || d.spool == nil {
		return nil
	}
	rows, err := d.spool.Meta().ListAll()
	if err != nil {
		return fmt.Errorf("stream sweep: list spool rows: %w", err)
	}
	sweepOrphanedStreamPartials(rows, d.fuseRoot)
	return nil
}
