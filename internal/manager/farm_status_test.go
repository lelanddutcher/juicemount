package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func writeFarmStatusFixture(t *testing.T, raw string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "farm-status.json")
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func getFarmStatus(t *testing.T, a *API) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	a.handleFarm(rec, httptest.NewRequest(http.MethodGet, "/api/farm", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v (%s)", err, rec.Body.String())
	}
	return body
}

func TestHandleFarmSuppressesStaleLiveProgress(t *testing.T) {
	old := time.Now().Add(-2 * farmProgressStaleAfter).Unix()
	path := writeFarmStatusFixture(t, `{
		"index":{"total_assets":12037},
		"last_sweep":{"mode":"derivatives","processed":100},
		"governor":{},
		"in_progress":{"pass":"derivatives","total":12037,"done":3205,"failed":1,"started_at":1700000000},
		"written_at":`+jsonInt(old)+`
	}`)
	body := getFarmStatus(t, &API{farmStatusPath: path})
	status := body["status"].(map[string]any)
	if _, ok := status["in_progress"]; ok {
		t.Fatal("stale in_progress was still presented as live")
	}
	stale, ok := body["stale_progress"].(map[string]any)
	if !ok {
		t.Fatalf("stale_progress missing: %#v", body)
	}
	progress := stale["progress"].(map[string]any)
	if progress["done"] != float64(3205) || progress["total"] != float64(12037) {
		t.Fatalf("interrupted progress not preserved: %#v", progress)
	}
	if stale["reason"] != "worker stopped refreshing live progress" {
		t.Fatalf("unexpected stale reason: %#v", stale)
	}
	if status["index"].(map[string]any)["total_assets"] != float64(12037) {
		t.Fatal("stable farm coverage was not preserved")
	}
}

func TestPrepareFarmStatusKeepsFreshLiveProgress(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	raw := []byte(`{"in_progress":{"pass":"proxy","total":2,"done":1},"written_at":1999999995}`)
	status, stale, err := prepareFarmStatus(raw, now)
	if err != nil {
		t.Fatal(err)
	}
	if stale != nil {
		t.Fatalf("fresh progress classified stale: %#v", stale)
	}
	if _, ok := status["in_progress"]; !ok {
		t.Fatal("fresh in_progress was removed")
	}
}

func TestPrepareFarmStatusTreatsUndatedLiveProgressAsStale(t *testing.T) {
	status, stale, err := prepareFarmStatus([]byte(`{"in_progress":{"total":1,"done":0}}`), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if stale == nil {
		t.Fatal("undated progress cannot substantiate live state")
	}
	if _, ok := status["in_progress"]; ok {
		t.Fatal("undated in_progress was retained")
	}
}

func TestHandleFarmRejectsUnreadableStatus(t *testing.T) {
	body := getFarmStatus(t, &API{farmStatusPath: writeFarmStatusFixture(t, `{not-json`)})
	if body["available"] != false || body["reason"] != "farm status is unreadable" {
		t.Fatalf("unexpected unreadable response: %#v", body)
	}
}

func jsonInt(v int64) string {
	return strconv.FormatInt(v, 10)
}
