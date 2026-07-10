package farm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// seedDeriv inserts one derivative row with a controlled updated_at (PutDeriv
// only stamps "now" when UpdatedAt is zero, so tests own the cursor).
func seedDeriv(t *testing.T, store *derivatives.Store, inode uint64, kind, status string, hash *string, ts int64) {
	t.Helper()
	if err := store.PutDeriv(inode, derivatives.DerivRow{
		Kind: kind, Status: status, Producer: "test-farm", Version: 1,
		Hash: hash, UpdatedAt: ts,
	}); err != nil {
		t.Fatalf("PutDeriv(%d,%s): %v", inode, kind, err)
	}
}

func openTestStore(t *testing.T) *derivatives.Store {
	t.Helper()
	store, err := derivatives.Open(filepath.Join(t.TempDir(), "derivatives.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func compileChangesSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	path := filepath.Join("..", "..", "contract", "spec", "schema", "derivatives-changes.schema.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var meta struct {
		ID string `json:"$id"`
	}
	_ = json.Unmarshal(b, &meta)
	c := jsonschema.NewCompiler()
	if err := c.AddResource(meta.ID, bytes.NewReader(b)); err != nil {
		t.Fatalf("add schema: %v", err)
	}
	sch, err := c.Compile(meta.ID)
	if err != nil {
		t.Fatalf("compile schema: %v", err)
	}
	return sch
}

// TestCollectChangesFullHistoryAscending seeds a small index and asserts the
// collected feed is the complete row set in the contract's (updated_at, inode,
// kind) ascending order — identical to a single unpaginated ListChangedSince.
func TestCollectChangesFullHistoryAscending(t *testing.T) {
	store := openTestStore(t)
	h := "2e9cf3ae98300fda"
	seedDeriv(t, store, 900, "proxy", "ready", &h, 300)
	seedDeriv(t, store, 100, "tech", "ready", &h, 100)
	seedDeriv(t, store, 100, "ai", "failed", nil, 300)
	seedDeriv(t, store, 200, "thumbnail", "ready", &h, 200)

	got, err := CollectChanges(store)
	if err != nil {
		t.Fatalf("CollectChanges: %v", err)
	}
	want, err := store.ListChangedSince(0, 10000)
	if err != nil {
		t.Fatalf("ListChangedSince: %v", err)
	}
	if len(got) != len(want) || len(got) != 4 {
		t.Fatalf("collected %d rows, want %d", len(got), len(want))
	}
	// Compare by VALUE (Hash is a *string; pointer identity differs per query).
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("collected rows differ from ListChangedSince:\ngot  %s\nwant %s", gotJSON, wantJSON)
	}
	for i := 1; i < len(got); i++ {
		a, b := got[i-1], got[i]
		if a.UpdatedAt > b.UpdatedAt {
			t.Fatalf("rows not ascending by updated_at at %d: %d > %d", i, a.UpdatedAt, b.UpdatedAt)
		}
		if a.UpdatedAt == b.UpdatedAt && a.Inode > b.Inode {
			t.Fatalf("rows not ascending by inode within second at %d", i)
		}
	}
}

// TestCollectChangesPaginatesAcrossSameSecondBoundaries forces multi-page
// collection with a tiny page size and same-second clusters straddling page
// boundaries: the boundary re-query (since = last-1) plus (inode,kind,ts)
// dedup must deliver every row exactly once, still ascending. Clusters here
// respect the supported invariant (≤ page size); the overflow case has its
// own termination test below.
func TestCollectChangesPaginatesAcrossSameSecondBoundaries(t *testing.T) {
	store := openTestStore(t)
	old := changesPageSize
	changesPageSize = 3
	t.Cleanup(func() { changesPageSize = old })

	// 10 rows; the ts=100 (page-sized) and ts=300 clusters straddle the 3-row
	// pages, forcing the boundary re-query + dedup on every transition.
	ts := []int64{100, 100, 100, 200, 200, 300, 300, 300, 400, 500}
	for i, s := range ts {
		seedDeriv(t, store, uint64(1000+i), "tech", "ready", nil, s)
	}

	got, err := CollectChanges(store)
	if err != nil {
		t.Fatalf("CollectChanges: %v", err)
	}
	if len(got) != len(ts) {
		t.Fatalf("collected %d rows, want %d (lost or duplicated at a page boundary)", len(got), len(ts))
	}
	seen := map[string]bool{}
	for i, r := range got {
		k := fmt.Sprintf("%d|%s", r.Inode, r.Kind)
		if seen[k] {
			t.Fatalf("duplicate row for %s", k)
		}
		seen[k] = true
		if r.UpdatedAt != ts[i] {
			t.Fatalf("row %d updated_at = %d, want %d (order broken)", i, r.UpdatedAt, ts[i])
		}
	}
}

// TestCollectChangesOverflowSecondSkipsButContinues pins the pathological
// bound: a single second holding MORE rows than a page can't be fully
// enumerated through the (since, limit) API — its overflow is skipped — but
// collection must terminate AND still deliver every later second (a stalled
// boundary must not truncate the rest of history). >pageSize rows in one
// second is unreachable in production (generation takes seconds per asset).
func TestCollectChangesOverflowSecondSkipsButContinues(t *testing.T) {
	store := openTestStore(t)
	old := changesPageSize
	changesPageSize = 3
	t.Cleanup(func() { changesPageSize = old })

	// 5 rows crammed into second 777 (overflows the 3-row page), then normal
	// history after it.
	for i := 0; i < 5; i++ {
		seedDeriv(t, store, uint64(2000+i), "tech", "ready", nil, 777)
	}
	seedDeriv(t, store, 3000, "proxy", "ready", nil, 900)
	seedDeriv(t, store, 3001, "ai", "ready", nil, 950)

	got, err := CollectChanges(store) // must return, not hang
	if err != nil {
		t.Fatalf("CollectChanges: %v", err)
	}
	// The overflow second yields its first page (3 of 5); later seconds are
	// intact. Nothing duplicated, order preserved.
	var after777 int
	for _, r := range got {
		if r.UpdatedAt > 777 {
			after777++
		}
	}
	if after777 != 2 {
		t.Fatalf("rows after the overflow second = %d, want 2 (overflow stall truncated later history)", after777)
	}
	if len(got) < 5 || len(got) > 7 {
		t.Fatalf("degenerate collection returned %d rows, want 5..7 (page of the overflow second + the 2 later rows)", len(got))
	}
}

// TestWriteChangesFeedContractShape publishes the feed and validates the file
// IS the contract wire format: a bare JSON array matching
// derivatives-changes.schema.json (closed row objects, hash null when unset),
// [] when the index is empty — never null. Also proves both status writers
// refresh the feed (the hook that makes the feed live during sweeps).
func TestWriteChangesFeedContractShape(t *testing.T) {
	store := openTestStore(t)
	dir := t.TempDir()
	statusPath := filepath.Join(dir, "farm-status.json")
	feedPath := filepath.Join(dir, ChangesFeedBasename)
	sch := compileChangesSchema(t)

	// Empty index → [] (never null), still schema-valid.
	if err := WriteChangesFeed(store, statusPath); err != nil {
		t.Fatalf("WriteChangesFeed(empty): %v", err)
	}
	raw, err := os.ReadFile(feedPath)
	if err != nil {
		t.Fatalf("feed not written: %v", err)
	}
	var tree any
	if err := json.Unmarshal(raw, &tree); err != nil {
		t.Fatalf("feed is not JSON: %v", err)
	}
	arr, ok := tree.([]any)
	if !ok || arr == nil {
		t.Fatalf("empty feed is %T, want [] array", tree)
	}
	if err := sch.Validate(tree); err != nil {
		t.Fatalf("empty feed violates schema: %v", err)
	}

	// Rows (incl. a null hash) → schema-valid array in order.
	h := "9f2b1c0a4e7d8b30"
	seedDeriv(t, store, 695453, "ai", "ready", &h, 1750000100)
	seedDeriv(t, store, 1180417, "proxy", "failed", nil, 1750000300)
	if err := WriteChangesFeed(store, statusPath); err != nil {
		t.Fatalf("WriteChangesFeed: %v", err)
	}
	raw, err = os.ReadFile(feedPath)
	if err != nil {
		t.Fatalf("read feed: %v", err)
	}
	tree = nil
	if err := json.Unmarshal(raw, &tree); err != nil {
		t.Fatalf("feed is not JSON: %v", err)
	}
	if err := sch.Validate(tree); err != nil {
		t.Fatalf("feed violates contract schema: %v\n%s", err, raw)
	}
	rows := tree.([]any)
	if len(rows) != 2 {
		t.Fatalf("feed has %d rows, want 2", len(rows))
	}
	second := rows[1].(map[string]any)
	if second["hash"] != nil {
		t.Errorf("unset hash serialized as %v, want null", second["hash"])
	}

	// The status writers refresh the feed as a side effect.
	if err := os.Remove(feedPath); err != nil {
		t.Fatalf("rm feed: %v", err)
	}
	sweep := SweepInfo{Mode: "derivatives", Producer: "test-farm", Target: "/jfs/x"}
	if err := WriteFarmStatus(store, statusPath, "", sweep, Governor{}); err != nil {
		t.Fatalf("WriteFarmStatus: %v", err)
	}
	if _, err := os.Stat(feedPath); err != nil {
		t.Fatalf("WriteFarmStatus did not refresh the changes feed: %v", err)
	}
	if err := os.Remove(feedPath); err != nil {
		t.Fatalf("rm feed: %v", err)
	}
	if err := WriteFarmProgress(store, statusPath, "", sweep, Governor{}, InProgress{Pass: "derivatives", Total: 1}); err != nil {
		t.Fatalf("WriteFarmProgress: %v", err)
	}
	if _, err := os.Stat(feedPath); err != nil {
		t.Fatalf("WriteFarmProgress did not refresh the changes feed: %v", err)
	}
}
