//go:build !serviceclient

package main

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func controlIsolationFixture(t *testing.T) string {
	t.Helper()
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(account.HomeDir, "Developer", ".state")
	if st, err := os.Stat(base); err != nil || !st.IsDir() {
		t.Fatal("private test state directory unavailable")
	}
	root, err := os.MkdirTemp(base, "jm-control-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Mkdir(filepath.Join(root, "tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, appisolation.Marker), []byte("JuiceMount isolated state v1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_APP_ISOLATION", "1")
	t.Setenv("JM_APP_ISOLATION_ROOT", root)
	t.Setenv("TMPDIR", filepath.Join(root, "tmp"))
	oldMode := servingProcessMode.Load()
	servingProcessMode.Store(true)
	t.Cleanup(func() {
		servingProcessMode.Store(oldMode)
		servingDesiredMu.Lock()
		servingDesiredActiveRevision = 0
		servingDesiredMu.Unlock()
	})
	return root
}

func TestDesiredStopPersistsWithUnreadableVolatileChild(t *testing.T) {
	root := controlIsolationFixture(t)
	// A linked volume deterministically exercises the same full-child gate as
	// a mounted volume whose lstat returns EIO, without touching any mount.
	if err := os.Symlink(filepath.Join(root, "absent"), filepath.Join(root, "volume")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := selectedOwnerScope(); err == nil {
		t.Fatal("full owner admission accepted invalid volatile child")
	}
	if scope, isolated, err := selectedOwnerControlScope(); err != nil || !isolated || scope.Root != root {
		t.Fatalf("control scope refused valid root: %+v %v %v", scope, isolated, err)
	}
	cfg := ServerConfig{DBPath: filepath.Join(root, "metadata.db")}
	if revision, err := beginServingDesired(cfg); err != nil || revision != 1 {
		t.Fatalf("desired write failed: revision=%d err=%v", revision, err)
	}
	if revision, err := stopServingDesired(); err != nil || revision != 2 {
		t.Fatalf("durable Stop failed: revision=%d err=%v", revision, err)
	}
	if got, err := readServingDesired(); err != nil || got.Desired != "stopped" || got.Revision != 2 || got.Root != root {
		t.Fatalf("desired state changed: %+v %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(root, servingDesiredLeaf), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readServingDesired(); err == nil {
		t.Fatal("corrupt control record admitted")
	}
}

func TestDesiredControlRecordRejectsLinkedLeaf(t *testing.T) {
	root := controlIsolationFixture(t)
	if err := os.Symlink(filepath.Join(root, "other"), filepath.Join(root, servingDesiredLeaf)); err != nil {
		t.Fatal(err)
	}
	if _, err := stopServingDesired(); err == nil {
		t.Fatal("linked desired record accepted")
	}
}

func TestReplacementControlPathsAvoidFullCurrent(t *testing.T) {
	root := controlIsolationFixture(t)
	volume := filepath.Join(root, "volume")
	if err := os.Symlink(filepath.Join(root, "absent"), volume); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(root, "mount-transactions")
	if err := os.Mkdir(journal, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := ownerSocketPath(); err == nil {
		t.Fatal("general owner socket bypassed full isolation")
	}
	if path, err := shutdownControlOwnerSocketPath(); err != nil || path != filepath.Join(root, "o.sock") {
		t.Fatalf("replacement control socket: %q %v", path, err)
	}
	previous := observeOwnedNFS
	snapshot := mounttable.Snapshot{Version: 1, BootID: "same-boot", Entries: []mounttable.MountEntry{}}
	observeOwnedNFS = func(context.Context) (mounttable.Snapshot, error) {
		return snapshot, nil
	}
	t.Cleanup(func() { observeOwnedNFS = previous })
	desired := servingDesiredRecord{Config: ServerConfig{MountPoint: volume}, NFSPhase: "none"}
	if ready, err := priorServingShutdownNFSRetired(context.Background(), desired); err != nil || !ready {
		t.Fatalf("control journal could not prove exact absence: %v %v", ready, err)
	}
	if _, err := priorServingNFSRetired(context.Background(), desired); err == nil {
		t.Fatal("normal recovery bypassed full isolation")
	}
	snapshot.Entries = []mounttable.MountEntry{{Path: volume, Source: "foreign:/", Type: "nfs", FSID: [2]int32{8, 9}}}
	if ready, err := priorServingShutdownNFSRetired(context.Background(), desired); err != nil || ready {
		t.Fatalf("foreign mount treated as absence: %v %v", ready, err)
	}
	snapshot.Entries = nil
	if ready, err := priorServingShutdownNFSRetired(context.Background(), desired); err == nil || ready {
		t.Fatalf("incomplete inventory treated as absence: %v %v", ready, err)
	}
	if err := os.Remove(journal); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(root, journal); err != nil {
		t.Fatal(err)
	}
	if _, err := priorServingShutdownNFSRetired(context.Background(), desired); err == nil {
		t.Fatal("linked control journal accepted")
	}
}

func privateStorageConfig(t *testing.T, root string) ServerConfig {
	t.Helper()
	t.Setenv("JM_CACHE_DIR", "")
	t.Setenv("JM_INFLIGHT_DUMP_DIR", "")
	t.Setenv("JM_APP_ISOLATION_FUSE_METRICS_ADDR", "127.0.0.1:23453")
	return ServerConfig{RedisURL: "redis://127.0.0.1:1/0", FUSEPath: filepath.Join(root, "fuse-internal"),
		MountPoint: filepath.Join(root, "volume"), DBPath: filepath.Join(root, "data", "metadata.db"),
		LogFile: filepath.Join(root, "logs", "test.log"), SpoolDir: filepath.Join(root, "spool"),
		ListenAddr: "127.0.0.1:23451", MetricsAddr: "127.0.0.1:23452", FUSEMetricsAddr: "127.0.0.1:23453"}
}

func TestPrivateStorageSQLiteRestartAndSiblingRefusal(t *testing.T) {
	root := controlIsolationFixture(t)
	cfg := privateStorageConfig(t, root)
	if err := validateIsolatedServerConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0755); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{cfg.DBPath, pinStorePath(cfg.DBPath), derivStorePath(cfg.DBPath)} {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("PRAGMA journal_mode=WAL; CREATE TABLE witness (v TEXT); INSERT INTO witness VALUES ('resident')"); err != nil {
			db.Close()
			t.Fatal(err)
		}
		if err := validateIsolatedServerConfig(cfg); err != nil {
			db.Close()
			t.Fatalf("live SQLite state refused: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		err = db.QueryRow("SELECT v FROM witness").Scan(&got)
		closeErr := db.Close()
		if err != nil || closeErr != nil || got != "resident" {
			t.Fatalf("reopen lost state: value=%q err=%v close=%v", got, err, closeErr)
		}
	}
	if err := validateIsolatedServerConfig(cfg); err != nil {
		t.Fatalf("reopened SQLite state refused: %v", err)
	}
	outside := filepath.Join(root, "outside-sentinel")
	if err := os.WriteFile(outside, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, base := range []string{cfg.DBPath, pinStorePath(cfg.DBPath), derivStorePath(cfg.DBPath)} {
		for _, suffix := range []string{"-wal", "-shm"} {
			t.Run(filepath.Base(base)+suffix, func(t *testing.T) {
				path := base + suffix
				if err := os.Symlink(outside, path); err != nil {
					t.Fatal(err)
				}
				defer os.Remove(path)
				if err := validateIsolatedServerConfig(cfg); err == nil {
					t.Fatal("linked SQLite companion accepted")
				}
			})
		}
	}
	if err := os.MkdirAll(filepath.Dir(linkStateDir(cfg)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "tmp"), linkStateDir(cfg)); err != nil {
		t.Fatal(err)
	}
	if err := prepareLinkStateDir(cfg); err == nil {
		t.Fatal("linked Link identity accepted")
	}
	if err := validateIsolatedServerConfig(cfg); err == nil {
		t.Fatal("linked Link config accepted")
	}
	if got, err := os.ReadFile(outside); err != nil || string(got) != "keep" {
		t.Fatal("outside sentinel changed")
	}
}

func TestPrivateCacheSelectionAndClear(t *testing.T) {
	root := controlIsolationFixture(t)
	_ = privateStorageConfig(t, root)
	const uuid = "12345678-1234-abcd-5678-123456789abc"
	base := filepath.Join(root, ".juicefs", "cache")
	volume := filepath.Join(base, uuid)
	chunks := filepath.Join(volume, "raw", "chunks")
	if err := os.MkdirAll(chunks, 0755); err != nil {
		t.Fatal(err)
	}
	if got, err := cache.VolumeCacheDir(uuid); err != nil || got != chunks {
		t.Fatalf("private binding: %q %v", got, err)
	}
	if cache.DetectCacheDir() != chunks || health.DetectJuiceFSCacheDir() != volume {
		t.Fatal("private cache detectors disagree")
	}
	outside := filepath.Join(root, "outside-sentinel")
	if err := os.WriteFile(outside, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	clear := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "http://127.0.0.1/cache/clear", nil)
		req.Header.Set(localControlHeader, "1")
		rec := httptest.NewRecorder()
		handleCacheClearHTTP(rec, req)
		return rec
	}
	block := filepath.Join(chunks, "block")
	if err := os.WriteFile(block, []byte("cached"), 0644); err != nil {
		t.Fatal(err)
	}
	if rec := clear(); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"files_removed":1`) {
		t.Fatalf("private clear: %d %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(block); !os.IsNotExist(err) {
		t.Fatal("private clear retained block")
	}
	if err := os.Symlink(outside, block); err != nil {
		t.Fatal(err)
	}
	if rec := clear(); rec.Code != 500 {
		t.Fatal("private clear accepted linked block")
	}
	if err := os.Remove(block); err != nil {
		t.Fatal(err)
	}
	for _, leaf := range []string{chunks, filepath.Dir(chunks)} {
		t.Run(filepath.Base(leaf), func(t *testing.T) {
			saved := leaf + "-owned"
			if err := os.Rename(leaf, saved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(saved, leaf); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.Remove(leaf); _ = os.Rename(saved, leaf) }()
			if _, err := cache.VolumeCacheDir(uuid); err == nil {
				t.Fatal("linked cache descendant accepted")
			}
			if cache.DetectCacheDir() != "" || health.DetectJuiceFSCacheDir() != "" {
				t.Fatal("detector followed linked descendant")
			}
			t.Setenv("JM_CACHE_DIR", chunks)
			if _, err := cache.VolumeCacheDir(uuid); err == nil || cache.DetectCacheDir() != "" {
				t.Fatal("override followed linked descendant")
			}
		})
	}
	if got, err := os.ReadFile(outside); err != nil || string(got) != "keep" {
		t.Fatal("outside sentinel changed")
	}
}
