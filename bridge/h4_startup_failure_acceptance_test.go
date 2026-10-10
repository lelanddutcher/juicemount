//go:build h4startupacceptance

package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func h4UnusedLoopbackAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release loopback port: %v", err)
	}
	return addr
}

func h4StartupConfig(t *testing.T) (ServerConfig, string) {
	t.Helper()
	root := t.TempDir()
	t.Setenv("HOME", root)
	t.Setenv("JM_SPOOL_ENABLE", "0")
	t.Setenv("JM_SPOOL_DIR", filepath.Join(root, "spool"))
	t.Setenv("JM_DEBUG_META_ADDR", "")
	t.Setenv("JM_CACHE_DIR", filepath.Join(root, "cache"))
	t.Setenv("JM_QL_PREWARM", "0")
	t.Setenv("JM_BOOT_PARALLEL", "0")
	previousFuseRoot := startupDrainerFuseRoot
	startupDrainerFuseRoot = func(string) string { return root }
	t.Cleanup(func() { startupDrainerFuseRoot = previousFuseRoot })
	return ServerConfig{
		RedisURL:    "redis://127.0.0.1:0/0",
		DBPath:      filepath.Join(root, "metadata.db"),
		ListenAddr:  h4UnusedLoopbackAddr(t),
		MetricsAddr: "127.0.0.1:0",
		LogLevel:    "error",
	}, root
}

func h4Start(t *testing.T, cfg ServerConfig) string {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return h4StartFromJSON(string(raw))
}

func h4AssertNoNFS(t *testing.T, addr string) bool {
	t.Helper()
	clean := true
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Errorf("NFS accepted a connection at %s after failed startup", addr)
		clean = false
	}
	globalMu.Lock()
	server, spool, store, rc, metricsServer, fuse, stopPending := globalServer, globalSpool, globalStore, globalRC, globalMetrics, globalFUSE, globalStopPending
	globalMu.Unlock()
	if server != nil || spool != nil || store != nil || rc != nil || metricsServer != nil || fuse != nil || stopPending {
		t.Errorf("failed startup retained published state: server=%p spool=%p store=%p redis=%p metrics=%p fuse=%p stopPending=%v",
			server, spool, store, rc, metricsServer, fuse, stopPending)
		clean = false
	}
	return clean
}

func h4OpenFixtureDB(t *testing.T, path string) *metadata.Store {
	t.Helper()
	store, err := metadata.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestH4NFSServerStartFaultsBeforeNFSPublication(t *testing.T) {
	for _, tc := range []struct {
		name      string
		wantError string
		prepare   func(*testing.T, *ServerConfig, string) func(*testing.T)
	}{
		{
			name:      "spool history query",
			wantError: "inspect local write spool before serving",
			prepare: func(t *testing.T, cfg *ServerConfig, _ string) func(*testing.T) {
				store := h4OpenFixtureDB(t, cfg.DBPath)
				if _, err := store.DB().Exec(`CREATE VIEW pending_symlinks AS SELECT * FROM h4_missing_history_table`); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) {
					store := h4OpenFixtureDB(t, cfg.DBPath)
					if _, err := store.DB().Exec(`DROP VIEW pending_symlinks`); err != nil {
						t.Fatal(err)
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
		{
			name:      "spool open",
			wantError: "open spool store",
			prepare: func(t *testing.T, cfg *ServerConfig, root string) func(*testing.T) {
				cfg.SpoolEnable = true
				path := filepath.Join(root, "spool")
				if err := os.WriteFile(path, []byte("blocked directory"), 0600); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) {
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
		{
			name:      "spool recovery",
			wantError: "recover local spool before serving",
			prepare: func(t *testing.T, cfg *ServerConfig, root string) func(*testing.T) {
				spoolRoot := filepath.Join(root, "spool")
				if err := os.MkdirAll(filepath.Join(spoolRoot, "files"), 0700); err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(spoolRoot, "files", "last-local-copy")
				content := []byte("possibly acknowledged footage")
				if err := os.WriteFile(file, content, 0600); err != nil {
					t.Fatal(err)
				}
				store := h4OpenFixtureDB(t, cfg.DBPath)
				if err := metadata.InitSpoolSchema(store.DB()); err != nil {
					t.Fatal(err)
				}
				meta := metadata.NewSpoolStore(store.DB())
				id, err := meta.Insert("/last-local-copy", file)
				if err != nil {
					t.Fatal(err)
				}
				if changed, err := meta.MarkFailed(id, "injected failed state"); err != nil || !changed {
					t.Fatalf("mark failed: changed=%v err=%v", changed, err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) {
					got, err := os.ReadFile(file)
					if err != nil || string(got) != string(content) {
						t.Fatalf("spool bytes after refusal: %q, %v", got, err)
					}
					store := h4OpenFixtureDB(t, cfg.DBPath)
					row, err := metadata.NewSpoolStore(store.DB()).Get(id)
					if err != nil || row == nil || row.DrainState != metadata.DrainFailed {
						t.Fatalf("spool row after refusal: %+v, %v", row, err)
					}
					if _, err := store.DB().Exec(`UPDATE spool_entries SET size = ? WHERE id = ?`, len(content), id); err != nil {
						t.Fatal(err)
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
		{
			name:      "unindexed spool bytes",
			wantError: "unindexed spool entry",
			prepare: func(t *testing.T, cfg *ServerConfig, root string) func(*testing.T) {
				cfg.SpoolEnable = true
				filesDir := filepath.Join(root, "spool", "files")
				if err := os.MkdirAll(filesDir, 0700); err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(filesDir, "unindexed")
				content := []byte("only local copy")
				if err := os.WriteFile(file, content, 0600); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) {
					got, err := os.ReadFile(file)
					if err != nil || string(got) != string(content) {
						t.Fatalf("unindexed bytes after refusal: %q, %v", got, err)
					}
					if err := os.Remove(file); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, root := h4StartupConfig(t)
			startAttempts := 0
			previousStart := startupNFSListener
			startupNFSListener = func(server *jmnfs.Server) error {
				startAttempts++
				return previousStart(server)
			}
			t.Cleanup(func() { startupNFSListener = previousStart })
			// FUSE and NFS mount paths stay empty. The tagged drainer-root
			// override points only at this test's temp directory.
			correct := tc.prepare(t, &cfg, root)
			t.Cleanup(func() { _ = NFSServerStop() })
			got := h4Start(t, cfg)
			if !strings.HasPrefix(got, "error:") || !strings.Contains(got, tc.wantError) {
				t.Errorf("NFSServerStart = %q, want %q failure", got, tc.wantError)
			}
			if startAttempts != 0 {
				t.Errorf("NFS listener start attempted %d times before spool refusal", startAttempts)
			}
			if !h4AssertNoNFS(t, cfg.ListenAddr) {
				t.FailNow()
			}
			correct(t)
			got = h4Start(t, cfg)
			if strings.HasPrefix(got, "error:") {
				t.Errorf("corrected retry failed: %s", got)
			} else {
				if startAttempts == 0 {
					t.Error("corrected retry never attempted NFS listener start")
				}
				globalMu.Lock()
				server := globalServer
				globalMu.Unlock()
				if server == nil || !server.IsRunning() {
					t.Errorf("corrected retry returned %q without publishing NFS", got)
				}
			}
			if stopped := NFSServerStop(); stopped == 0 {
				t.Errorf("retry server did not stop cleanly: %d", stopped)
			}
		})
	}
}
