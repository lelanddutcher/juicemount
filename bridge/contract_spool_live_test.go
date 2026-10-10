package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func TestContractLiveSpoolStatusConforms(t *testing.T) {
	schema := compileSchema(t, "spool-status.schema.json")
	check := func(wantCode, wantLinks int) {
		t.Helper()
		w := httptest.NewRecorder()
		handleSpoolHTTP(w, httptest.NewRequest(http.MethodGet, "/spool", nil))
		if w.Code != wantCode {
			t.Fatalf("/spool HTTP %d, want %d: %s", w.Code, wantCode, w.Body.String())
		}
		var value any
		if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(value); err != nil {
			t.Fatalf("live /spool response violates schema: %v", err)
		}
		got := int(value.(map[string]any)["pending_symlinks"].(float64))
		if got != wantLinks {
			t.Fatalf("pending_symlinks=%d, want %d", got, wantLinks)
		}
	}

	globalMu.Lock()
	oldSpool, oldDrainer := globalSpool, globalDrainer
	globalSpool, globalDrainer = nil, nil
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalSpool, globalDrainer = oldSpool, oldDrainer
		globalMu.Unlock()
	})
	check(http.StatusServiceUnavailable, 0)

	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	meta := metadata.NewSpoolStore(store.DB())
	spool, err := jmnfs.NewSpoolStore(t.TempDir(), 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(spool.Stop)
	if err := meta.PutPendingSymlink("links/reel", "../reel.mov"); err != nil {
		t.Fatal(err)
	}
	globalMu.Lock()
	globalSpool = spool
	globalMu.Unlock()
	check(http.StatusOK, 1)

	if _, err := store.DB().Exec(`DROP TABLE pending_symlinks`); err != nil {
		t.Fatal(err)
	}
	check(http.StatusInternalServerError, 0)
}
