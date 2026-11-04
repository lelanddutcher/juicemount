package metadata

import (
	"fmt"
	"strings"
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
// (serving-layer-decision.md §Item 1): a READDIR page against a 10,774-child
// dir must be an O(limit) seek on the composite idx_parent_name index. If that
// index is dropped, or the query changes so the planner reverts to a TEMP
// B-TREE ORDER BY that sorts all children per page, each page becomes
// O(all-children) (measured ~2.2ms without the index vs ~0.3ms with it).
//
// The gate asserts the cause, the query plan, rather than a wall-clock budget:
// a fixed 2ms budget measured 6.2ms with the index present on a loaded -race CI
// host, while the plan is a deterministic property of the schema and the SQL.
// The negative control drops the index and requires the check to see the
// regression, so the assertion cannot pass vacuously.
func TestListChildrenPage_BigDirUnderBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping big-dir page gate in -short")
	}
	const children = 10774
	const pageSize = 256 // realistic per-RPC READDIR page
	s := seedStore(t, "DCIM", children)
	defer s.Close()

	if problem := childrenPagePlanProblem(t, s); problem != "" {
		t.Fatalf("READDIR page is not an O(limit) idx_parent_name seek: %s", problem)
	}
	page, next, err := s.ListChildrenPage("DCIM", "file_005000.mov", pageSize)
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	if len(page) != pageSize || page[0].Name != "file_005001.mov" || next != page[pageSize-1].Name {
		t.Fatalf("page from file_005000.mov: %d rows, first %q, next %q", len(page), page[0].Name, next)
	}

	if _, err := s.db.Exec(`DROP INDEX idx_parent_name`); err != nil {
		t.Fatalf("drop idx_parent_name: %v", err)
	}
	if problem := childrenPagePlanProblem(t, s); problem == "" {
		t.Fatal("plan check passed with idx_parent_name dropped; it cannot detect the regression it guards")
	}
}

// childrenPagePlanProblem returns why the ListChildrenPage query plan is not a
// bounded index seek, or "" when it is.
func childrenPagePlanProblem(t *testing.T, s *Store) string {
	t.Helper()
	rows, err := s.db.Query(`EXPLAIN QUERY PLAN `+childrenPageSQL, "DCIM", "file_005000.mov", 256)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("explain scan: %v", err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("explain rows: %v", err)
	}
	joined := strings.Join(plan, "; ")
	t.Logf("ListChildrenPage plan: %s", joined)
	switch {
	case strings.Contains(joined, "TEMP B-TREE"):
		return "plan sorts the whole child set per page: " + joined
	case !strings.Contains(joined, "USING INDEX idx_parent_name (parent_path=? AND name>?)"):
		return "plan does not seek idx_parent_name on (parent_path, name): " + joined
	}
	return ""
}
