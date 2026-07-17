package manager

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// TestFarmChangeRowParity pins the manager's local wire struct to
// derivatives.ChangeRow byte-for-byte. The manager duplicates the type so its
// binary stays sqlite-free (importing internal/derivatives would pull the
// modernc driver in); this test — a TEST-only import — is what makes that
// duplication safe: any drift in either type fails here.
func TestFarmChangeRowParity(t *testing.T) {
	h := "2e9cf3ae98300fda"
	pairs := []struct {
		theirs derivatives.ChangeRow
		ours   farmChangeRow
	}{
		{
			derivatives.ChangeRow{Inode: 695453, Kind: "ai", Status: "ready", Hash: &h, UpdatedAt: 1750000100},
			farmChangeRow{Inode: 695453, Kind: "ai", Status: "ready", Hash: &h, UpdatedAt: 1750000100},
		},
		{
			// nil hash must serialize as null on both sides.
			derivatives.ChangeRow{Inode: 7, Kind: "proxy", Status: "failed", Hash: nil, UpdatedAt: 42},
			farmChangeRow{Inode: 7, Kind: "proxy", Status: "failed", Hash: nil, UpdatedAt: 42},
		},
	}
	for i, p := range pairs {
		a, err := json.Marshal(p.theirs)
		if err != nil {
			t.Fatalf("marshal theirs: %v", err)
		}
		b, err := json.Marshal(p.ours)
		if err != nil {
			t.Fatalf("marshal ours: %v", err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("pair %d wire drift:\nderivatives.ChangeRow %s\nmanager.farmChangeRow %s", i, a, b)
		}
	}
	// And the farm's published basename is the one the manager derives.
	// (internal/farm.ChangesFeedBasename — string-pinned, not imported, for
	// the same sqlite-free reason; update BOTH if the name ever changes.)
	if farmChangesBasename != "derivatives-changes.json" {
		t.Fatalf("farmChangesBasename = %q", farmChangesBasename)
	}
}

func TestDeriveFarmChangesPath(t *testing.T) {
	cases := []struct {
		name, explicit, env, status, want string
	}{
		{"explicit wins", "/x/changes.json", "/y/c.json", "/state/farm-status.json", "/x/changes.json"},
		{"env second", "", "/y/c.json", "/state/farm-status.json", "/y/c.json"},
		{"status sibling", "", "", "/state/farm-status.json", "/state/derivatives-changes.json"},
		{"nothing", "", "", "", ""},
	}
	for _, c := range cases {
		if got := deriveFarmChangesPath(c.explicit, c.env, c.status); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// writeChangesFile drops a feed file the handler will read.
func writeChangesFile(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), farmChangesBasename)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write feed: %v", err)
	}
	return path
}

func getChanges(t *testing.T, a *API, query string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/farm/derivatives/changes"+query, nil)
	rec := httptest.NewRecorder()
	a.handleFarmDerivativesChanges(rec, req)
	return rec
}

// TestFarmDerivativesChangesFixtureConformance serves the contract's golden
// changes.json through the relay and holds the response to the contract:
// schema-valid (derivatives-changes.schema.json), and for since=0 (or a
// missing/garbage cursor) byte-equivalent to the full fixture. Cursor + limit
// behavior matches the Mac control plane's /derivatives/changes exactly:
// strict `>`, ascending, missing/garbage since ⇒ full history.
func TestFarmDerivativesChangesFixtureConformance(t *testing.T) {
	fixture := readContractFixture(t, "derivatives/changes.json")
	a := &API{farmChangesPath: writeChangesFile(t, fixture)}
	sch := compileContractSchema(t, "derivatives-changes.schema.json")
	fixTree := decodeJSON(t, fixture)

	// The vendored fixture satisfies its own schema (stale-pair guard).
	if err := sch.Validate(fixTree); err != nil {
		t.Fatalf("contract fixture violates its schema: %v", err)
	}

	for _, q := range []string{"", "?since=0", "?since=abc"} {
		rec := getChanges(t, a, q)
		if rec.Code != http.StatusOK {
			t.Fatalf("%q: status = %d, want 200 (%s)", q, rec.Code, rec.Body.String())
		}
		tree := decodeJSON(t, rec.Body.Bytes())
		if err := sch.Validate(tree); err != nil {
			t.Fatalf("%q: response violates contract schema: %v\n%s", q, err, rec.Body.String())
		}
		// Full history == the fixture, verbatim.
		got, _ := json.Marshal(tree)
		want, _ := json.Marshal(fixTree)
		if !bytes.Equal(got, want) {
			t.Fatalf("%q: response != fixture\ngot  %s\nwant %s", q, got, want)
		}
	}

	// Strict-> cursor: since = first row's updated_at excludes it.
	rec := getChanges(t, a, "?since=1750000100")
	rows := decodeJSON(t, rec.Body.Bytes()).([]any)
	if len(rows) != 2 {
		t.Fatalf("since=1750000100: %d rows, want 2 (strict > violated?)", len(rows))
	}
	if ts := rows[0].(map[string]any)["updated_at"].(float64); ts != 1750000200 {
		t.Fatalf("since=1750000100: first row updated_at = %v, want 1750000200", ts)
	}

	// Cursor at the newest row → empty array (the caught-up poll), never null.
	rec = getChanges(t, a, "?since=1750000300")
	if body := rec.Body.String(); rec.Code != http.StatusOK {
		t.Fatalf("caught-up poll: status %d (%s)", rec.Code, body)
	}
	if rows := decodeJSON(t, rec.Body.Bytes()).([]any); rows == nil || len(rows) != 0 {
		t.Fatalf("caught-up poll: got %v, want []", rows)
	}

	// limit bounds the page but keeps ascending order from the cursor.
	rec = getChanges(t, a, "?since=0&limit=1")
	rows = decodeJSON(t, rec.Body.Bytes()).([]any)
	if len(rows) != 1 {
		t.Fatalf("limit=1: %d rows", len(rows))
	}
	if ts := rows[0].(map[string]any)["updated_at"].(float64); ts != 1750000100 {
		t.Fatalf("limit=1: got updated_at %v, want the OLDEST row (1750000100)", ts)
	}
}

// TestFarmDerivativesChangesReordersDefensively feeds a shuffled file and
// asserts the relay re-asserts the contract's (updated_at, inode, kind)
// ascending order — the cursor-advancing consumer must never see descending
// timestamps, whatever a partially-written or hand-edited file contains.
func TestFarmDerivativesChangesReordersDefensively(t *testing.T) {
	shuffled := []byte(`[
	  {"inode": 3, "kind": "ai",    "status": "ready", "hash": null, "updated_at": 300},
	  {"inode": 2, "kind": "proxy", "status": "ready", "hash": null, "updated_at": 100},
	  {"inode": 1, "kind": "tech",  "status": "ready", "hash": null, "updated_at": 200},
	  {"inode": 1, "kind": "ai",    "status": "ready", "hash": null, "updated_at": 200}
	]`)
	a := &API{farmChangesPath: writeChangesFile(t, shuffled)}
	rec := getChanges(t, a, "")
	rows := decodeJSON(t, rec.Body.Bytes()).([]any)
	if len(rows) != 4 {
		t.Fatalf("%d rows", len(rows))
	}
	wantOrder := []struct {
		ts    float64
		inode float64
		kind  string
	}{{100, 2, "proxy"}, {200, 1, "ai"}, {200, 1, "tech"}, {300, 3, "ai"}}
	for i, w := range wantOrder {
		m := rows[i].(map[string]any)
		if m["updated_at"].(float64) != w.ts || m["inode"].(float64) != w.inode || m["kind"].(string) != w.kind {
			t.Fatalf("row %d = %v, want ts=%v inode=%v kind=%s", i, m, w.ts, w.inode, w.kind)
		}
	}
}

// TestFarmDerivativesChangesFailureModes pins the availability contract:
// unconfigured → 503; absent feed → 200 [] (farm never ran; consumer cursor
// safely stays put); corrupt feed → 503 (fail closed, retryable); non-GET → 405.
func TestFarmDerivativesChangesFailureModes(t *testing.T) {
	// Unconfigured.
	rec := getChanges(t, &API{}, "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured: status %d, want 503", rec.Code)
	}

	// Configured but absent (farm hasn't published yet).
	a := &API{farmChangesPath: filepath.Join(t.TempDir(), farmChangesBasename)}
	rec = getChanges(t, a, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("absent feed: status %d, want 200", rec.Code)
	}
	if rows := decodeJSON(t, rec.Body.Bytes()).([]any); len(rows) != 0 {
		t.Fatalf("absent feed: got %v, want []", rows)
	}

	// Corrupt.
	a = &API{farmChangesPath: writeChangesFile(t, []byte(`{"not":"an array"`))}
	rec = getChanges(t, a, "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("corrupt feed: status %d, want 503", rec.Code)
	}
	var errBody map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil || errBody["error"] == nil {
		t.Fatalf("corrupt feed: 503 body should be a JSON error, got %q", rec.Body.String())
	}

	// Method guard.
	req := httptest.NewRequest(http.MethodPost, "/api/farm/derivatives/changes", nil)
	rr := httptest.NewRecorder()
	a.handleFarmDerivativesChanges(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST: status %d, want 405", rr.Code)
	}
}

// TestFarmChangesEndToEndFarmToManager is the #56 integration seam in one
// process: a real derivatives.Store (what the farm owns) → farm.WriteChangesFeed
// equivalent file → the manager relay — proving the farm-written bytes ARE the
// manager-served contract feed. The farm half lives in internal/farm; here we
// only assert the manager serves a farm-shaped file unmodified, so the pair of
// packages can't drift apart silently.
func TestFarmChangesEndToEndFarmToManager(t *testing.T) {
	// Farm-shaped feed: what internal/farm.CollectChanges marshals (the
	// derivatives.ChangeRow wire type — parity-pinned above).
	h := "9f2b1c0a4e7d8b30"
	rows := []derivatives.ChangeRow{
		{Inode: 695453, Kind: "ai", Status: "ready", Hash: &h, UpdatedAt: 1750000100},
		{Inode: 1180417, Kind: "proxy", Status: "pending", Hash: nil, UpdatedAt: 1750000300},
	}
	b, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	a := &API{farmChangesPath: writeChangesFile(t, b)}

	rec := getChanges(t, a, "?since=1750000099")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	sch := compileContractSchema(t, "derivatives-changes.schema.json")
	tree := decodeJSON(t, rec.Body.Bytes())
	if err := sch.Validate(tree); err != nil {
		t.Fatalf("farm-written feed relayed by the manager violates the contract: %v\n%s", err, rec.Body.String())
	}
	got := tree.([]any)
	if len(got) != 2 {
		t.Fatalf("%d rows, want 2", len(got))
	}
	if got[1].(map[string]any)["hash"] != nil {
		t.Fatalf("nil hash did not survive farm→manager round-trip as null")
	}
}
