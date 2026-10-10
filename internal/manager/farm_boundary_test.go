package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFarmSweepKeepsQueuedScopeInsideConfiguredMount(t *testing.T) {
	base := t.TempDir()
	mount := filepath.Join(base, "jfs")
	inside := filepath.Join(mount, "Film Projects", "nested")
	outside := filepath.Join(base, "outside")
	for _, dir := range []string{inside, outside} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	queue := &fakeFarmQueue{}
	a := &API{destMount: mount, farmQ: queue}
	cases := []struct {
		name, path string
		status     int
	}{
		{"nested media", inside, http.StatusOK},
		{"outside", outside, http.StatusBadRequest},
		{"prefix sibling", mount + "-other", http.StatusBadRequest},
		{"parent traversal", filepath.Join(mount, "..", "outside") + "/../outside", http.StatusBadRequest},
		{"relative", "Film Projects/nested", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := len(queue.enqueued)
			body, err := json.Marshal(map[string]any{"path": tc.path, "kinds": []string{"derivatives"}})
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			a.handleFarmSweep(rec, httptest.NewRequest(http.MethodPost, "/api/farm/sweep", strings.NewReader(string(body))))
			if rec.Code != tc.status {
				t.Fatalf("path=%q status=%d want=%d body=%q", tc.path, rec.Code, tc.status, rec.Body.String())
			}
			if tc.status != http.StatusOK && len(queue.enqueued) != before {
				t.Fatalf("unsafe path %q enqueued", tc.path)
			}
		})
	}
}
