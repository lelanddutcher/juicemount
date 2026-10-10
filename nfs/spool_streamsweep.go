package nfs

import (
	"fmt"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/metadata"
)

type bootSweepInventory struct{ rows []*metadata.SpoolRow }

// Stream stages are row-derived, but an unfenced backend pathname cannot prove
// ownership at deletion time. Boot cleanup remains deferred until publication
// and cleanup share an enforceable writer-authority contract.

type DeferredStreamCleanup struct{ Candidates int }

func (e *DeferredStreamCleanup) Error() string {
	return fmt.Sprintf("stream cleanup deferred: %d possible orphan stages preserved", e.Candidates)
}

// streamPartialCandidates returns the temp paths that may have been orphaned by
// a crash, given the live spool rows and the FUSE root.
//
// Pure selection is useful for accounting; it grants no permission to delete.
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
		if r.DrainState == metadata.DrainDone || r.PunchedEnd > 0 {
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

// SweepStreamPartials checks cold-start custody without deleting backend files.
// Orphan cleanup is deliberately deferred until ownership can be fenced.
func (d *Drainer) SweepStreamPartials() error {
	if d == nil || d.spool == nil {
		return nil
	}
	rows, err := d.spool.Meta().ListAll()
	if err != nil {
		return fmt.Errorf("stream sweep: list spool rows: %w", err)
	}
	cohort := rows
	if boot := d.spool.bootSweep.Load(); boot != nil {
		current := make(map[int64]*metadata.SpoolRow, len(rows))
		for _, row := range rows {
			current[row.ID] = row
		}
		cohort = make([]*metadata.SpoolRow, 0, len(boot.rows))
		for _, original := range boot.rows {
			row := current[original.ID]
			if row == nil || row.NFSPath != original.NFSPath ||
				row.SpoolFile != original.SpoolFile || !row.CreatedAt.Equal(original.CreatedAt) {
				continue
			}
			cohort = append(cohort, row)
		}
	}
	var pending []metadata.PunchedSpoolRow
	for _, row := range cohort {
		if row.PunchedEnd > 0 && row.DrainState != metadata.DrainDone {
			pending = append(pending, metadata.PunchedSpoolRow{
				ID: row.ID, Path: row.NFSPath, SpoolFile: row.SpoolFile, PunchedEnd: row.PunchedEnd,
			})
		}
	}
	if len(pending) > 0 {
		return fmt.Errorf("stream sweep: %w", &metadata.PendingPunchedSpool{Rows: pending})
	}
	if candidates := len(streamPartialCandidates(cohort, d.fuseRoot)); candidates > 0 {
		return &DeferredStreamCleanup{Candidates: candidates}
	}
	return nil
}
