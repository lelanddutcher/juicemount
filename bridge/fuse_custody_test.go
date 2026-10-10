//go:build !serviceclient

package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func TestFUSEShutdownRetainsDependenciesUntilTypedCompletion(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	manager := health.NewFUSEManager(health.FUSEConfig{MountPoint: t.TempDir(), JuiceFSBin: "/unused-fixture-binary"})
	node := &jmnfs.LinkNode{}
	globalMu.Lock()
	if globalFUSE != nil || globalStore != nil || globalServer != nil || globalStopPending || globalMountPath != "" {
		globalMu.Unlock()
		t.Fatal("fixture requires idle bridge")
	}
	oldPath, oldConfig := globalFUSEPath, globalFUSEConfig
	globalStore, globalFUSE, globalFUSEPath = store, manager, "/fixture-retained-fuse"
	globalMu.Unlock()
	linkMu.Lock()
	oldNode, oldIdentity := globalLinkNode, globalLinkID
	globalLinkNode = node
	linkMu.Unlock()
	oldStop := stopManagedFUSE
	t.Cleanup(func() {
		stopManagedFUSE = oldStop
		globalMu.Lock()
		globalStore, globalFUSE, globalFUSEPath, globalStopPending = nil, nil, oldPath, false
		globalFUSEConfig = oldConfig
		globalMu.Unlock()
		linkMu.Lock()
		globalLinkNode, globalLinkID = oldNode, oldIdentity
		linkMu.Unlock()
	})
	calls := 0
	stopManagedFUSE = func(got *health.FUSEManager) health.FUSEStopResult {
		calls++
		if got != manager {
			t.Fatal("wrong retained manager")
		}
		if err := store.DB().Ping(); err != nil {
			t.Fatalf("store closed before FUSE decision: %v", err)
		}
		linkMu.Lock()
		retained := globalLinkNode == node
		linkMu.Unlock()
		if !retained {
			t.Fatal("Link retired before FUSE decision")
		}
		if calls == 1 {
			return health.FUSEStopResult{State: health.FUSEPresent, Detail: "mount still present"}
		}
		return health.FUSEStopResult{State: health.FUSEStopped}
	}
	if shutdownServer(false) != 0 {
		t.Fatal("incomplete FUSE reported shutdown success")
	}
	globalMu.Lock()
	retained := globalFUSE == manager && globalStore == store && globalFUSEPath == "/fixture-retained-fuse" && globalStopPending
	globalMu.Unlock()
	if !retained {
		t.Fatal("incomplete FUSE lost published custody")
	}
	if err := store.DB().Ping(); err != nil {
		t.Fatalf("retained store closed: %v", err)
	}
	if shutdownServer(false) != 1 {
		t.Fatal("settled FUSE did not permit dependency release")
	}
	globalMu.Lock()
	closed := globalFUSE == nil && globalStore == nil && globalFUSEPath == "" && !globalStopPending
	globalMu.Unlock()
	linkMu.Lock()
	closed = closed && globalLinkNode == nil
	linkMu.Unlock()
	if !closed || calls != 2 {
		t.Fatalf("final custody: closed=%v attempts=%d", closed, calls)
	}
	if store.DB().Ping() == nil {
		t.Fatal("store not closed after final FUSE completion")
	}
}

func TestFUSEStartupRefusesRedirectingRetainedManager(t *testing.T) {
	base := ServerConfig{FUSEPath: "/retained", DBPath: "/retained/metadata.db", RedisURL: "redis://fixture.invalid:6379/1"}
	manager := health.NewFUSEManager(health.FUSEConfig{MountPoint: base.FUSEPath, RedisURL: base.RedisURL, JuiceFSBin: "/unused-fixture-binary"})
	globalMu.Lock()
	oldManager, oldConfig, oldPath, oldDB, oldRedis := globalFUSE, globalFUSEConfig, globalFUSEPath, globalDBPath, globalRedisURL
	globalFUSE, globalFUSEConfig, globalFUSEPath = manager, &base, base.FUSEPath
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalFUSE, globalFUSEConfig, globalFUSEPath = oldManager, oldConfig, oldPath
		globalMu.Unlock()
	})
	for _, edit := range []struct {
		name   string
		change func(*ServerConfig)
	}{
		{"mount path", func(c *ServerConfig) { c.FUSEPath = "/replacement" }},
		{"metadata path", func(c *ServerConfig) { c.DBPath = "/must-not-open/metadata.db" }},
		{"backend", func(c *ServerConfig) { c.RedisURL = "redis://other.invalid:6379/2" }},
	} {
		t.Run(edit.name, func(t *testing.T) {
			cfg := base
			edit.change(&cfg)
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			result := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "start", Strings: []string{string(raw)}})
			if result.Text != "error: volume resources are still in use; saved settings remain pending until the volume can be safely restarted" {
				t.Fatalf("Start = %+v", result)
			}
			globalMu.Lock()
			retained := globalFUSE == manager && globalFUSEConfig == &base && globalFUSEPath == base.FUSEPath && globalDBPath == oldDB && globalRedisURL == oldRedis
			globalMu.Unlock()
			if !retained {
				t.Fatal("refused Start changed retained dependencies or configuration")
			}
		})
	}
}
