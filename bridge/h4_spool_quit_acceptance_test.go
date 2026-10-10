package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func h4SpoolHTTPStatus(t *testing.T, spool *jmnfs.SpoolStore) (int, jmnfs.SpoolStatusResponse) {
	t.Helper()
	globalMu.Lock()
	previousSpool, previousDrainer := globalSpool, globalDrainer
	globalSpool, globalDrainer = spool, nil
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalSpool, globalDrainer = previousSpool, previousDrainer
		globalMu.Unlock()
	}()

	recorder := httptest.NewRecorder()
	handleSpoolHTTP(recorder, httptest.NewRequest(http.MethodGet, "/spool", nil))
	resp := recorder.Result()
	defer resp.Body.Close()
	var status jmnfs.SpoolStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("/spool response was not JSON: %v", err)
	}
	return resp.StatusCode, status
}

func TestH4SpoolHTTPDisabledIsUnavailable(t *testing.T) {
	code, status := h4SpoolHTTPStatus(t, nil)
	if code != http.StatusServiceUnavailable || status.Enabled || status.Error == "" {
		t.Fatalf("disabled /spool = (%d, %+v), want 503 with enabled:false and error", code, status)
	}
}

func TestH4SpoolHTTPOverLoopback(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback listener unavailable in this environment: %v", err)
	}
	server := &http.Server{Handler: http.HandlerFunc(handleSpoolHTTP)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	globalMu.Lock()
	previousSpool, previousDrainer := globalSpool, globalDrainer
	globalSpool, globalDrainer = nil, nil
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalSpool, globalDrainer = previousSpool, previousDrainer
		globalMu.Unlock()
	})
	get := func() (int, jmnfs.SpoolStatusResponse) {
		t.Helper()
		resp, err := http.Get("http://" + listener.Addr().String() + "/spool")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body jmnfs.SpoolStatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body
	}
	if code, body := get(); code != http.StatusServiceUnavailable || body.Enabled {
		t.Fatalf("disabled loopback /spool = (%d, %+v), want 503", code, body)
	}

	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := jmnfs.NewSpoolStore(t.TempDir(), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(spool.Stop)
	if err := spool.Meta().PutPendingSymlink("Frameworks/Current", "Versions/A"); err != nil {
		t.Fatal(err)
	}
	globalMu.Lock()
	globalSpool = spool
	globalMu.Unlock()
	if code, body := get(); code != http.StatusOK || body.PendingFiles != 0 || body.PendingSymlinks != 1 {
		t.Fatalf("pending-only loopback /spool = (%d, %+v), want 200 with pending_symlinks:1", code, body)
	}
	if _, err := store.DB().Exec("DROP TABLE pending_symlinks"); err != nil {
		t.Fatal(err)
	}
	if code, body := get(); code != http.StatusInternalServerError || body.Error == "" {
		t.Fatalf("broken loopback /spool = (%d, %+v), want 500 with error", code, body)
	}
}

func TestH4StartupSpoolHistoryProbe(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	count, err := startupUnfinishedSpoolRows(store.DB())
	if err != nil || count != 0 {
		t.Fatalf("new database probe = (%d, %v), want empty", count, err)
	}

	id, err := metadata.NewSpoolStore(store.DB()).Insert("/failed.bin", "failed.bin")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := metadata.NewSpoolStore(store.DB()).MarkFailed(id, "upload exhausted"); err != nil || !changed {
		t.Fatalf("mark failed = (%v, %v)", changed, err)
	}
	count, err = startupUnfinishedSpoolRows(store.DB())
	if err != nil || count != 1 {
		t.Fatalf("failed-row probe = (%d, %v), want one unfinished row", count, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if count, err := startupUnfinishedSpoolRows(store.DB()); err == nil {
		t.Fatalf("unreadable database probe = (%d, nil), want startup refusal", count)
	}
}

func TestH4PendingOnlySymlinkForcesRecoveryAndBlocksOrdinaryQuit(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if count, err := startupUnfinishedSpoolRows(store.DB()); err != nil || count != 0 {
		t.Fatalf("pristine spool history = (%d, %v), want empty", count, err)
	}
	if err := metadata.NewSpoolStore(store.DB()).PutPendingSymlink("/project/latest", "clip.mov"); err != nil {
		t.Fatal(err)
	}
	if count, err := startupUnfinishedSpoolRows(store.DB()); err != nil || count != 1 {
		t.Fatalf("pending-only symlink history = (%d, %v), want one", count, err)
	}
	if pendingSymlinksClearAfterUnmount(store) {
		t.Fatal("pending symlink was declared safe for ordinary quit")
	}
	globalMu.Lock()
	previousStore, previousSpool := globalStore, globalSpool
	globalStore, globalSpool = store, nil
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalStore, globalSpool = previousStore, previousSpool
		globalMu.Unlock()
	})
	if shutdownServer(false) != 0 {
		t.Fatal("ordinary quit accepted a pending-only offline symlink")
	}
	links, err := metadata.NewSpoolStore(store.DB()).ListPendingSymlinks()
	if err != nil || len(links) != 1 {
		t.Fatalf("pending symlink after refused quit = (%+v, %v), want preserved", links, err)
	}
}

func TestH4SpoolHTTPSQLFailureIsNotClean(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := jmnfs.NewSpoolStore(t.TempDir(), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(spool.Stop)
	if _, err := store.DB().Exec("DROP TABLE spool_entries"); err != nil {
		t.Fatal(err)
	}
	code, status := h4SpoolHTTPStatus(t, spool)
	if code != http.StatusInternalServerError || !status.Enabled || status.Error == "" {
		t.Fatalf("broken /spool = (%d, %+v), want 500 with an error", code, status)
	}
	if spoolClearAfterUnmount(spool) {
		t.Fatal("shutdown accepted a spool whose SQLite status query failed")
	}
}

func TestH4FailedRowAndLateWriteBlockOrdinaryQuit(t *testing.T) {
	spool, drainer := shutdownTestDrainer(t)
	publishShutdownTestDrainer(t, spool, drainer, nil)
	code, status := h4SpoolHTTPStatus(t, spool)
	if code != http.StatusOK || status.PendingFiles != 0 || status.FailedFiles != 0 {
		t.Fatalf("initial /spool = (%d, %+v), want a clean snapshot", code, status)
	}

	spoolFile := filepath.Join(t.TempDir(), "failed.bin")
	if err := os.WriteFile(spoolFile, []byte("copied bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	id, err := spool.Meta().Insert("/failed.bin", spoolFile)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := spool.Meta().MarkFailed(id, "upload exhausted"); err != nil || !changed {
		t.Fatalf("mark failed = (%v, %v)", changed, err)
	}
	code, status = h4SpoolHTTPStatus(t, spool)
	if code != http.StatusOK || status.FailedFiles != 1 {
		t.Fatalf("failed-row /spool = (%d, %+v), want failed_files:1", code, status)
	}
	if shutdownServer(false) != 0 {
		t.Fatal("ordinary quit discarded a failed spool row")
	}

	// A status snapshot cannot reserve the empty queue. A write admitted
	// afterward must be caught by the post-unmount Go-side check.
	if err := spool.Meta().Delete(id); err != nil {
		t.Fatal(err)
	}
	code, status = h4SpoolHTTPStatus(t, spool)
	if code != http.StatusOK || status.PendingFiles != 0 || status.FailedFiles != 0 {
		t.Fatalf("second /spool = (%d, %+v), want a clean snapshot", code, status)
	}
	entry, err := spool.OpenWrite("/late.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.WriteAt([]byte("late bytes"), 0); err != nil {
		t.Fatal(err)
	}
	if shutdownServer(false) != 0 {
		t.Fatal("ordinary quit ignored a write after its clean /spool snapshot")
	}
	if err := entry.Close(); err != nil {
		t.Fatal(err)
	}
}
