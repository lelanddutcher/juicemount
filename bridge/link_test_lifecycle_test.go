//go:build !serviceclient

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// Each fixture calls the actual serving dispatch and C ABI. The private paths
// and loopback endpoints keep a regression from reaching saved pairing state.
func dispatchLinkTestFixture(t *testing.T, cfg ServerConfig) linkTestResult {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	response := dispatchServingRequest(servicecontrol.Request{
		Version: 1, Build: version.Commit, Method: "link-test", Strings: []string{string(raw)},
	})
	if response.Error != "" {
		t.Fatalf("request failed before reaching Link test: %s", response.Error)
	}
	var result linkTestResult
	if err := json.Unmarshal([]byte(response.Text), &result); err != nil {
		t.Fatalf("decode Link test result: %v", err)
	}
	return result
}

func isolatedLinkTestConfig(t *testing.T) ServerConfig {
	t.Helper()
	root := t.TempDir()
	return ServerConfig{
		DBPath: filepath.Join(root, "metadata.db"), FUSEPath: filepath.Join(root, "fuse"),
		MountPoint: filepath.Join(root, "mount"), NetControlURL: "https://127.0.0.1:1",
		NetAuthKey: "test-pairing", NetHostname: "test-link",
		RedisURL: "redis://127.0.0.1:1/0", BucketOverride: "http://127.0.0.1:1/bucket",
	}
}

// No bridge tests run in parallel. Save every field this fixture changes so
// package test order cannot carry a synthetic owner into another test.
func clearLinkTestOwners(t *testing.T) {
	t.Helper()
	globalMu.Lock()
	pending, server, fuse, config := globalStopPending, globalServer, globalFUSE, globalFUSEConfig
	fusePath, mountPath, flights := globalFUSEPath, globalMountPath, globalMountFlights
	globalStopPending, globalServer, globalFUSE, globalFUSEConfig = false, nil, nil, nil
	globalFUSEPath, globalMountPath, globalMountFlights = "", "", nil
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalStopPending, globalServer, globalFUSE, globalFUSEConfig = pending, server, fuse, config
		globalFUSEPath, globalMountPath, globalMountFlights = fusePath, mountPath, flights
		globalMu.Unlock()
	})
}

func TestLinkTestRetainedOwnersPreservePairing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		install func(ServerConfig)
	}{
		{"pending stop", func(ServerConfig) { globalStopPending = true }},
		{"server", func(ServerConfig) { globalServer = &jmnfs.Server{} }},
		{"FUSE manager", func(ServerConfig) { globalFUSE = &health.FUSEManager{} }},
		{"FUSE configuration", func(c ServerConfig) { globalFUSEConfig = &c }},
		{"FUSE path", func(c ServerConfig) { globalFUSEPath = c.FUSEPath }},
		{"NFS path", func(c ServerConfig) { globalMountPath = c.MountPoint }},
		{"mount flights", func(ServerConfig) { globalMountFlights = &nfsMountFlights{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearLinkTestOwners(t)
			cfg := isolatedLinkTestConfig(t)
			globalMu.Lock()
			tc.install(cfg)
			pending, server, fuse, config := globalStopPending, globalServer, globalFUSE, globalFUSEConfig
			fusePath, mountPath, flights := globalFUSEPath, globalMountPath, globalMountFlights
			globalMu.Unlock()
			linkMu.Lock()
			oldNode, oldID := globalLinkNode, globalLinkID
			node := &jmnfs.LinkNode{}
			id := linkNodeIdentity{ControlURL: "https://prior.invalid", StateDir: "prior-identity"}
			globalLinkNode, globalLinkID = node, id
			linkMu.Unlock()
			t.Cleanup(func() { linkMu.Lock(); globalLinkNode, globalLinkID = oldNode, oldID; linkMu.Unlock() })

			result := dispatchLinkTestFixture(t, cfg)
			if !strings.Contains(result.Error, "saved") || strings.Contains(result.Error, "mount absence") {
				t.Fatalf("retained owner was not refused before probing: %+v", result)
			}
			linkMu.Lock()
			unchanged := globalLinkNode == node && globalLinkID == id
			linkMu.Unlock()
			globalMu.Lock()
			unchanged = unchanged && globalStopPending == pending && globalServer == server && globalFUSE == fuse && globalFUSEConfig == config && globalFUSEPath == fusePath && globalMountPath == mountPath && globalMountFlights == flights
			globalMu.Unlock()
			if !unchanged {
				t.Fatal("rejected pairing changed a retained owner or Link identity")
			}
			if _, err := os.Stat(linkStateRoot(cfg)); !os.IsNotExist(err) {
				t.Fatalf("rejected pairing touched its state directory: %v", err)
			}
		})
	}
}

func TestLinkTestMissingMountHelperPreservesPairing(t *testing.T) {
	clearLinkTestOwners(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(filepath.Dir(executable), "juicemount-mount-probe")
	if _, err := os.Lstat(helper); !os.IsNotExist(err) {
		t.Fatalf("missing-helper fixture requires absent sibling helper: %s: %v", helper, err)
	}
	cfg := isolatedLinkTestConfig(t)
	linkMu.Lock()
	oldNode, oldID := globalLinkNode, globalLinkID
	linkMu.Unlock()
	result := dispatchLinkTestFixture(t, cfg)
	if !strings.Contains(result.Error, "mount absence cannot be verified") || !strings.Contains(result.Error, "start helper") {
		t.Fatalf("missing helper was not refused: %+v", result)
	}
	linkMu.Lock()
	unchanged := globalLinkNode == oldNode && globalLinkID == oldID
	linkMu.Unlock()
	if !unchanged {
		t.Fatal("missing-helper refusal changed Link identity")
	}
	if _, err := os.Stat(linkStateRoot(cfg)); !os.IsNotExist(err) {
		t.Fatalf("missing-helper refusal touched pairing state: %v", err)
	}
}
