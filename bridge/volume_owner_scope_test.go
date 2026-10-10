//go:build !serviceclient

package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func TestServingOwnerScopeDistinguishesOrdinaryServiceAndLocalCLI(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	oldPath := selectedOwnerServiceSocketPath
	selectedOwnerServiceSocketPath = func() (string, error) { return filepath.Join(base, "control.sock"), nil }
	t.Cleanup(func() { selectedOwnerServiceSocketPath = oldPath })
	for _, key := range []string{"JM_APP_ISOLATION", "JM_APP_ISOLATION_ROOT"} {
		old, present := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(key, old)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
	root := base
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	oldMode := servingProcessMode.Load()
	t.Cleanup(func() { servingProcessMode.Store(oldMode) })
	servingProcessMode.Store(false)
	if use, err := selectedOwnerMode(); err != nil || use {
		t.Fatalf("local CLI selected owner: %v %v", use, err)
	}
	servingProcessMode.Store(true)
	if use, err := selectedOwnerMode(); err != nil || !use {
		t.Fatalf("ordinary serving process did not select owner: %v %v", use, err)
	}
	got, isolated, err := selectedOwnerScope()
	if err != nil || isolated || got.Root != root {
		t.Fatalf("ordinary owner namespace: %+v isolated=%v err=%v", got, isolated, err)
	}
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	if use, err := selectedOwnerMode(); err == nil || use {
		t.Fatalf("insecure ordinary owner namespace admitted: %v %v", use, err)
	}
}

func TestServingOwnerScopeRejectsMalformedIsolationWithoutFallback(t *testing.T) {
	oldMode := servingProcessMode.Load()
	t.Cleanup(func() { servingProcessMode.Store(oldMode) })
	servingProcessMode.Store(true)
	t.Setenv("JM_APP_ISOLATION", "1")
	t.Setenv("JM_APP_ISOLATION_ROOT", "")
	if use, err := selectedOwnerMode(); err == nil || use {
		t.Fatalf("malformed opt-in fell back to ordinary namespace: %v %v", use, err)
	}
	if _, _, err := selectedOwnerScope(); err == nil {
		t.Fatal("malformed opt-in selected a namespace")
	}
}

func TestOrdinaryServingDispatchSelectsOwnerBeforeLocalLink(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	oldPath := selectedOwnerServiceSocketPath
	selectedOwnerServiceSocketPath = func() (string, error) { return filepath.Join(base, "control.sock"), nil }
	t.Cleanup(func() { selectedOwnerServiceSocketPath = oldPath })
	for _, key := range []string{"JM_APP_ISOLATION", "JM_APP_ISOLATION_ROOT"} {
		old, present := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(key, old)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
	globalMu.Lock()
	occupied := globalServer != nil || globalFUSE != nil || globalOwner != nil || globalNFSController != nil || globalStopPending
	globalMu.Unlock()
	if occupied {
		t.Fatal("dispatch test requires empty serving runtime")
	}
	oldMode := servingProcessMode.Load()
	oldSelect, oldLink := selectServingOwner, startServingLink
	selected := &ownerFacade{}
	const sentinel = "selected ordinary owner claim retained"
	claims, localLinks := 0, 0
	selectServingOwner = func(ServerConfig) (*ownerFacade, error) {
		claims++
		return selected, errors.New(sentinel)
	}
	startServingLink = func(*ServerConfig) (*jmnfs.LinkNode, bool, error) {
		localLinks++
		return nil, false, errors.New("local Link entered")
	}
	servingProcessMode.Store(true)
	t.Cleanup(func() {
		globalMu.Lock()
		if globalOwner == selected {
			globalOwner = nil
		}
		globalMu.Unlock()
		_ = NFSServerStop()
		selectServingOwner, startServingLink = oldSelect, oldLink
		servingProcessMode.Store(oldMode)
	})
	cfg := ServerConfig{RedisURL: "redis://127.0.0.1:1/0", FUSEPath: filepath.Join(base, "fuse"),
		MountPoint: filepath.Join(base, "volume"), DBPath: filepath.Join(base, "metadata.db"),
		LogFile: filepath.Join(base, "service.log"), ListenAddr: "127.0.0.1:0",
		MetricsAddr: "127.0.0.1:0", LogLevel: "error"}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	resp := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "start", Strings: []string{string(raw)}})
	if claims != 1 || localLinks != 0 || resp.Error != "" || !strings.Contains(resp.Text, sentinel) {
		t.Fatalf("ordinary dispatch bypassed selected owner: claim=%d link=%d response=%+v", claims, localLinks, resp)
	}
	globalMu.Lock()
	retained := globalOwner == selected && globalFUSE == nil && globalServer == nil && globalNFSController == nil
	globalMu.Unlock()
	if !retained {
		t.Fatal("ordinary dispatcher did not retain ambiguous owner before local resources")
	}
}
