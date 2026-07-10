package farm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// ChangesFeedBasename is the pre-aggregated /derivatives/changes feed the farm
// publishes NEXT TO farm-status.json on the shared juicefarm-state volume
// (JM-15 #56, server half). The file is the contract's changes-feed array
// verbatim — spec/schema/derivatives-changes.schema.json rows ascending by
// (updated_at, inode, kind) — so the manager can relay+filter it without ever
// opening the farm's SQLite.
//
// Why a pre-aggregated file and not the manager reading derivatives.db
// directly: the manager is deliberately sqlite-free (see FarmStatus's doc +
// the juicemount-manager Dockerfile), the state volume is mounted read-only
// into it, and a live WAL-mode SQLite db is not reliably readable from a
// second process through a read-only mount (the reader must map the -shm
// read-write while a writer is active). The farm owns the ONLY db handle and
// publishes plain JSON — the exact split JM15_DESIGN.md prescribes ("keeps
// every SQLite handle local to its host; the volume only ever carries plain
// JSON + blob files").
const ChangesFeedBasename = "derivatives-changes.json"

// changesPageSize is how many rows each ListChangedSince page pulls while
// collecting the full feed. The store clamps limits to 10000; a package var so
// tests can force multi-page collection with tiny pages.
var changesPageSize = 10000

// CollectChanges returns EVERY derivative row in the index as contract
// ChangeRows, ascending by (updated_at, inode, kind) — the full history a
// consumer with no cursor (since=0) is promised. ListChangedSince caps each
// query at 10k rows, so this paginates: after a full page it re-queries from
// lastUpdatedAt-1 (the strict `>` would otherwise skip rows sharing the
// boundary second) and drops the overlap by (inode, kind, updated_at).
//
// Loss bound: the (since, limit) API can't cursor WITHIN one second, so a
// single second holding MORE than changesPageSize rows can't be fully
// enumerated — its overflow is skipped and collection continues with the next
// second. That needs >10k derivative commits inside one wall-clock second;
// generation takes seconds per asset, so this is a theoretical bound, not an
// expected state. Every same-second cluster ≤ changesPageSize is delivered
// exactly once, in order.
func CollectChanges(store *derivatives.Store) ([]derivatives.ChangeRow, error) {
	out := []derivatives.ChangeRow{}
	seen := make(map[string]bool) // boundary-overlap dedup keys
	since := int64(-1)            // strict > -1 admits updated_at=0 rows too
	for {
		page, err := store.ListChangedSince(since, changesPageSize)
		if err != nil {
			return nil, err
		}
		added := 0
		for _, r := range page {
			k := fmt.Sprintf("%d|%s|%d", r.Inode, r.Kind, r.UpdatedAt)
			if seen[k] {
				continue // overlap from the boundary re-query
			}
			seen[k] = true
			out = append(out, r)
			added++
		}
		if len(page) < changesPageSize {
			return out, nil // final (short) page
		}
		last := page[len(page)-1].UpdatedAt
		if added == 0 {
			// The whole page was overlap: the boundary second's cluster is at
			// least a full page, so re-reading from last-1 can never surface
			// anything new. Skip PAST that second (strict > excludes it) —
			// pathological overflow within it is dropped (see doc comment),
			// but every later second is still collected.
			since = last
			continue
		}
		if last-1 <= since {
			// Progress was made but the page still ends inside the boundary
			// second; re-run the same window once more — the next pass dedups
			// to added==0 and the branch above advances past the second.
			continue
		}
		since = last - 1
	}
}

// WriteChangesFeed collects the full changes feed and atomically publishes it
// as <dir(statusPath)>/derivatives-changes.json. It is hooked into
// WriteFarmStatus + WriteFarmProgress so every existing status write (post-job,
// post-sweep, and the ~3s in-progress tick) refreshes the feed too — the
// manager's GET /api/farm/derivatives/changes then serves deltas without any
// sidecar re-sweep. Rows marshal as a bare JSON array ([] when empty, never
// null) exactly per the contract schema.
func WriteChangesFeed(store *derivatives.Store, statusPath string) error {
	if store == nil || statusPath == "" {
		return nil
	}
	rows, err := CollectChanges(store)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(filepath.Dir(statusPath), ChangesFeedBasename)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return atomicWriteFile(path, b, 0o644)
}
