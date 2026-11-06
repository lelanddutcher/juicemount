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

// TestMigrateRepliesWithJobSnapshot queues real jobs through /api/migrate and
// encodes the reply while each job's runner is already writing to it. Run
// with -race: replying with the live *Job raced with the runner.
func TestMigrateRepliesWithJobSnapshot(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{root},
		DestMount:   "/jfs",
		AdminKey:    "the-admin-key-for-tests",
		StateFile:   filepath.Join(t.TempDir(), "state.json"),
	})
	defer mgr.StopAll()

	body, _ := json.Marshal(map[string]string{"source": srcDir, "destination": "/jfs/project", "direction": "in"})
	for i := 0; i < 20; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/migrate", strings.NewReader(string(body)))
		req.Header.Set("X-JuiceMount-Admin-Key", "the-admin-key-for-tests")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusAccepted {
			t.Fatalf("migrate: got %d body=%s", w.Code, w.Body.String())
		}
		var got struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.ID == "" {
			t.Fatalf("migrate reply has no job id: %v body=%s", err, w.Body.String())
		}
	}
}
