package nfs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func waitH4PendingLink(t *testing.T, spool *SpoolStore, path string, wantPending bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, err := spool.Meta().GetPendingSymlink(path)
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
	waitH4PendingLink(t, spool, link, false)
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
	drainer.pollFallback = 20 * time.Millisecond
	drainer.Start()
	time.Sleep(50 * time.Millisecond)
	if _, err := spool.Meta().GetPendingSymlink(link); err != nil {
		t.Fatalf("failed attempt discarded pending symlink: %v", err)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	waitH4PendingLink(t, spool, link, false)
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
