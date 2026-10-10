//go:build h4startupacceptance

package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func TestH8PersistedPublicationRefusesBridgeStartup(t *testing.T) {
	for _, tc := range []struct {
		name     string
		spoolRow bool
		spoolOn  bool
	}{
		{name: "disabled with surviving row", spoolRow: true},
		{name: "disabled without surviving row"},
		{name: "enabled without surviving row", spoolOn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := h4StartupConfig(t)
			cfg.SpoolEnable = tc.spoolOn
			store := h4OpenFixtureDB(t, cfg.DBPath)
			if err := metadata.InitSpoolSchema(store.DB()); err != nil {
				t.Fatal(err)
			}
			id := int64(932)
			if tc.spoolRow {
				var err error
				id, err = metadata.NewSpoolStore(store.DB()).Insert("reel.mov", t.TempDir()+"/reel")
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.DB().Exec(`INSERT INTO spool_bindings(spool_id,incarnation,phase,stage_path,publish_path)
				VALUES (?, 'original-incarnation', 'publishing', '/stage/reel', 'reel.mov')`, id); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			attempts := 0
			previous := startupNFSListener
			startupNFSListener = func(server *jmnfs.Server) error {
				attempts++
				return previous(server)
			}
			t.Cleanup(func() { startupNFSListener = previous })
			t.Cleanup(func() { _ = NFSServerStop() })
			got := h4Start(t, cfg)
			if !strings.Contains(got, "inspect spool publication before serving") ||
				!strings.Contains(got, "spool publication requires reconciliation") {
				t.Fatalf("startup response = %q", got)
			}
			// A later remount cannot publish a refused startup: the callback is
			// installed only after the store gate and no server remains armed.
			time.Sleep(20 * time.Millisecond)
			if attempts != 0 || !h4AssertNoNFS(t, cfg.ListenAddr) {
				t.Fatalf("refusal left listener attempts=%d", attempts)
			}
			store = h4OpenFixtureDB(t, cfg.DBPath)
			defer store.Close()
			var pending *metadata.PendingSpoolPublication
			if err := store.CheckSpoolRecovery(); !errors.As(err, &pending) ||
				len(pending.Paths) != 1 || pending.Paths[0] != "reel.mov" {
				t.Fatalf("publication marker changed after refusal: %v", err)
			}
		})
	}
}

func TestH8BridgePublicationQueryFailureCanRetry(t *testing.T) {
	cfg, _ := h4StartupConfig(t)
	store := h4OpenFixtureDB(t, cfg.DBPath)
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`ALTER TABLE spool_bindings RENAME COLUMN phase TO phase_missing`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	previous := startupNFSListener
	startupNFSListener = func(server *jmnfs.Server) error {
		attempts++
		return previous(server)
	}
	t.Cleanup(func() { startupNFSListener = previous })
	t.Cleanup(func() { _ = NFSServerStop() })
	if got := h4Start(t, cfg); !strings.Contains(got, "inspect spool publication before serving") {
		t.Fatalf("query failure response = %q", got)
	}
	if attempts != 0 || !h4AssertNoNFS(t, cfg.ListenAddr) {
		t.Fatalf("query failure left listener attempts=%d", attempts)
	}
	store = h4OpenFixtureDB(t, cfg.DBPath)
	if _, err := store.DB().Exec(`ALTER TABLE spool_bindings RENAME COLUMN phase_missing TO phase`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if got := h4Start(t, cfg); strings.HasPrefix(got, "error:") {
		t.Fatalf("corrected retry failed: %q", got)
	}
	if attempts == 0 {
		t.Fatal("corrected retry did not start NFS")
	}
}
