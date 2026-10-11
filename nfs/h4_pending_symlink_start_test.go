package nfs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func waitH4PendingLink(t *testing.T, spool *SpoolStore, path string, wantPending bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := spool.Meta().GetPendingSymlink(path)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("query pending symlink %q: %v", path, err)
		}
		if (wantPending && err == nil) || (!wantPending && errors.Is(err, sql.ErrNoRows)) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pending symlink %q did not reach pending=%v", path, wantPending)
}

func TestH4OnlineStartMaterializesPendingOnlySymlink(t *testing.T) {
	wasOffline := pin.IsOffline()
	t.Cleanup(func() { pin.SetOffline(wasOffline) })
	pin.SetOffline(false)
	_, fuseRoot, spool, drainer := newSymlinkSpoolTestFS(t)
	const link, target = "Frameworks/Current", "Versions/A"
	if err := spool.Meta().PutPendingSymlink(link, target); err != nil {
		t.Fatal(err)
	}
	drainer.pollFallback = 20 * time.Millisecond
	drainer.Start()
	waitH4PendingLink(t, spool, link, false, 3*time.Second)
	got, err := os.Readlink(filepath.Join(fuseRoot, link))
	if err != nil || got != target {
		t.Fatalf("online-start symlink = (%q, %v), want %q", got, err, target)
	}
}

func TestH4OnlineStartRetriesPendingSymlinkFailure(t *testing.T) {
	wasOffline := pin.IsOffline()
	t.Cleanup(func() { pin.SetOffline(wasOffline) })
	pin.SetOffline(false)
	_, fuseRoot, spool, drainer := newSymlinkSpoolTestFS(t)
	const link, target = "Blocked/Current", "Versions/B"
	if err := spool.Meta().PutPendingSymlink(link, target); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(fuseRoot, "Blocked")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	const sentinel, sentinelTarget = "Sentinel/Current", "Versions/Ready"
	if err := spool.Meta().PutPendingSymlink(sentinel, sentinelTarget); err != nil {
		t.Fatal(err)
	}
	pending, err := spool.Meta().ListPendingSymlinks()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].LinkPath != link || pending[1].LinkPath != sentinel {
		t.Fatalf("pending order = %+v, want blocked link before sentinel", pending)
	}

	// The serial materializer must attempt the blocked link before this
	// sentinel. Preserve the handler callback and never block the worker.
	sentinelMaterialized := make(chan struct{})
	var sentinelOnce sync.Once
	originalMaterialized := drainer.onSymlinkMaterialized
	drainer.onSymlinkMaterialized = func(path string) {
		if originalMaterialized != nil {
			originalMaterialized(path)
		}
		if path == sentinel {
			sentinelOnce.Do(func() { close(sentinelMaterialized) })
		}
	}
	drainer.Start()
	select {
	case <-sentinelMaterialized:
	case <-time.After(2*drainOfflineRecheck + time.Second):
		t.Fatal("sentinel did not witness the initial blocked-link attempt")
	}
	if got, err := spool.Meta().GetPendingSymlink(link); err != nil || got != target {
		t.Fatalf("failed attempt pending symlink = (%q, %v), want %q", got, err, target)
	}
	if got, err := os.ReadFile(blocker); err != nil || string(got) != "not a directory" {
		t.Fatalf("failure witness blocker = (%q, %v), want original regular file", got, err)
	}
	if got, err := os.Readlink(filepath.Join(fuseRoot, sentinel)); err != nil || got != sentinelTarget {
		t.Fatalf("sentinel symlink = (%q, %v), want %q", got, err, sentinelTarget)
	}
	t.Log("initial failed attempt witnessed: sentinel materialized, blocked intent and physical blocker retained")
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	waitH4PendingLink(t, spool, link, false, 2*drainOfflineRecheck+time.Second)
	got, err := os.Readlink(filepath.Join(fuseRoot, link))
	if err != nil || got != target {
		t.Fatalf("retried symlink = (%q, %v), want %q", got, err, target)
	}
}

func TestH4SpoolStatusCountsPendingSymlinksAndFailsOnQueryError(t *testing.T) {
	spool, _, db := newSpoolStoreForRecovery(t)
	t.Cleanup(spool.Stop)
	if err := spool.Meta().PutPendingSymlink("Frameworks/Current", "Versions/A"); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	WriteSpoolStatusJSON(recorder, spool, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("symlink-only /spool status = %d, want 200", recorder.Code)
	}
	var status SpoolStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.PendingFiles != 0 || status.PendingSymlinks != 1 || status.Error != "" {
		t.Fatalf("symlink-only /spool = %+v", status)
	}
	if _, err := db.Exec(`DROP TABLE pending_symlinks`); err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	WriteSpoolStatusJSON(recorder, spool, nil)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("unreadable pending symlinks /spool status = %d, want 500", recorder.Code)
	}
	status = SpoolStatusResponse{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil || status.Error == "" {
		t.Fatalf("unreadable pending symlinks /spool body = (%+v, %v)", status, err)
	}
}
