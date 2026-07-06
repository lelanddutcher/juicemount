package metadata

import (
	"fmt"
	"testing"
	"time"
)

// seedStore inserts n children under parent (names zero-padded so lexical order
// is well-defined) plus the parent dir itself, and returns the store. Inodes are
// unique and dense.
func seedStore(t *testing.T, parent string, n int) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	now := time.Unix(1_700_000_000, 0)
	if parent != "." && parent != "" {
		if err := s.Insert(MakeEntry(parent, true, 0, now, 1)); err != nil {
			t.Fatalf("insert parent: %v", err)
		}
	}
	ents := make([]*Entry, 0, n)
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("%s/file_%06d.mov", parent, i)
		ents = append(ents, MakeEntry(p, false, int64(i*1000), now, uint64(1000+i)))
	}
	if err := s.BulkInsert(ents, 5000); err != nil {
		t.Fatalf("bulk insert: %v", err)
	}
	return s
}

// --- Item 1: ListChildrenPage ---

func TestListChildrenPage_MultiPage(t *testing.T) {
	const total = 25
	const limit = 10
	s := seedStore(t, "dir", total)
	defer s.Close()

	var got []*Entry
	cursor := ""
	pages := 0
	for {
		page, next, err := s.ListChildrenPage("dir", cursor, limit)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		pages++
		got = append(got, page...)
		// Every full page except possibly the last must be exactly `limit`.
		if next != "" && len(page) != limit {
			t.Fatalf("non-final page %d had %d entries, want %d", pages, len(page), limit)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(got) != total {
		t.Fatalf("collected %d entries across %d pages, want %d", len(got), pages, total)
	}
	// Pages must be strictly name-ordered and complete (no dupes, no gaps).
	for i := 0; i < total; i++ {
		want := fmt.Sprintf("file_%06d.mov", i)
		if got[i].Name != want {
			t.Fatalf("entry %d = %q, want %q", i, got[i].Name, want)
		}
	}
	// 25 items / 10 per page => 3 pages (10,10,5). The final short page ends
	// pagination via empty cursor.
	if pages != 3 {
		t.Fatalf("paginated in %d pages, want 3", pages)
	}
}

func TestListChildrenPage_ExactMultiple(t *testing.T) {
	// A dir whose child count is an exact multiple of limit: the last FULL page
	// must still be followed by one empty page (cursor non-empty after the last
	// full page, then a zero-row page returns "").
	const total = 20
	const limit = 10
	s := seedStore(t, "dir", total)
	defer s.Close()

	cursor := ""
	var count, pages int
	sawEmptyTail := false
	for {
		page, next, err := s.ListChildrenPage("dir", cursor, limit)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		pages++
		count += len(page)
		if len(page) == 0 {
			sawEmptyTail = true
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if count != total {
		t.Fatalf("collected %d, want %d", count, total)
	}
	// 2 full pages of 10, then a 3rd empty page terminates. The full page of
	// exactly `limit` returns a non-empty cursor by design (can't know it's the
	// end without one more query), so an empty tail page is expected+correct.
	if !sawEmptyTail {
		t.Fatalf("exact-multiple pagination did not terminate via an empty tail page (pages=%d)", pages)
	}
}

func TestListChildrenPage_EmptyDir(t *testing.T) {
	s := seedStore(t, "dir", 0)
	defer s.Close()
	page, next, err := s.ListChildrenPage("dir", "", 10)
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	if len(page) != 0 || next != "" {
		t.Fatalf("empty dir returned %d entries / cursor %q, want 0 / \"\"", len(page), next)
	}
}

func TestListChildrenPage_OverLimit(t *testing.T) {
	// A >limit dir paginates exactly: sum of page sizes == total, no dupes.
	const total = 100
	const limit = 7
	s := seedStore(t, "dir", total)
	defer s.Close()

	seen := map[string]bool{}
	cursor := ""
	for {
		page, next, err := s.ListChildrenPage("dir", cursor, limit)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		for _, e := range page {
			if seen[e.Name] {
				t.Fatalf("duplicate entry across pages: %q", e.Name)
			}
			seen[e.Name] = true
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != total {
		t.Fatalf("saw %d distinct entries, want %d", len(seen), total)
	}
}

// TestListChildrenPage_BigDirUnderBudget is the veto-mitigation REGRESSION GATE
// (serving-layer-decision.md §Item 1): a realistic READDIR page against a
// 10,774-child dir must stay well under the RPC budget. This guards the
// composite idx_parent_name index — if that index is dropped (or the query
// changes so the planner reverts to a TEMP B-TREE ORDER BY that sorts all
// children per page), the per-page cost jumps ~7x (measured ~2.2ms without the
// index vs ~0.3ms with it) and this test fails. Threshold is deliberately
// loose (2ms) so it does not flake on a busy CI box while still catching the
// index-loss regression (which lands well above 2ms on this dir size).
func TestListChildrenPage_BigDirUnderBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping big-dir latency gate in -short")
	}
	const children = 10774
	const pageSize = 256 // realistic per-RPC READDIR page
	s := seedStore(t, "DCIM", children)
	defer s.Close()

	// Warm the page cache + prepared stmt.
	if _, _, err := s.ListChildrenPage("DCIM", "", pageSize); err != nil {
		t.Fatalf("warm: %v", err)
	}
	const iters = 200
	start := time.Now()
	for i := 0; i < iters; i++ {
		if _, _, err := s.ListChildrenPage("DCIM", "file_005000.mov", pageSize); err != nil {
			t.Fatalf("page: %v", err)
		}
	}
	perPage := time.Since(start) / iters
	t.Logf("big-dir (%d children) %d-row page: %v/page", children, pageSize, perPage)
	budget := 2 * time.Millisecond * bigDirPageBudgetMultiplier
	if perPage > budget {
		t.Fatalf("page latency %v exceeds %v budget — idx_parent_name likely missing "+
			"(temp-b-tree ORDER BY sorts all children per page)", perPage, budget)
	}
}
