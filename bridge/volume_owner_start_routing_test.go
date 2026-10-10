//go:build !serviceclient

package main

import (
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// This is the real serving dispatcher and NFSServerStart, stopped at selected
// owner admission. It proves routing and retained ambiguous custody only, not
// successful mount/config consumption or engine-crash recovery. The local Link
// guard also keeps a disabled owner-routing negative from creating resources.
func TestServingStartRetainsSelectedOwnerBeforeLocalResources(t *testing.T) {
	root := privateIsolationTestRoot(t, "vor-")
	t.Setenv("TMPDIR", filepath.Join(root, "tmp"))
	t.Setenv("JM_APP_ISOLATION", "1")
	t.Setenv("JM_APP_ISOLATION_ROOT", root)
	t.Setenv("JM_DEBUG_META_ADDR", "")
	t.Setenv("JM_BOOT_PARALLEL", "0")
	ports := map[string]bool{}
	port := func() string {
		for i := 0; i < 20; i++ {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			a := listener.Addr().String()
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			if !ports[a] {
				ports[a] = true
				return a
			}
		}
		t.Fatal("could not allocate distinct private port identities")
		return ""
	}
	cfg := ServerConfig{RedisURL: "redis://127.0.0.1:1/0", FUSEPath: filepath.Join(root, "fuse-internal"),
		MountPoint: filepath.Join(root, "volume"), DBPath: filepath.Join(root, "metadata.db"),
		LogFile: filepath.Join(root, "test.log"), SpoolDir: filepath.Join(root, "spool"),
		ListenAddr: port(), MetricsAddr: port(), FUSEMetricsAddr: port(), LogLevel: "error"}
	t.Setenv("JM_APP_ISOLATION_FUSE_METRICS_ADDR", cfg.FUSEMetricsAddr)
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateIsolatedRaw(string(raw)); err != nil {
		t.Fatalf("fixture failed real isolation preflight: %v", err)
	}
	globalMu.Lock()
	occupied := globalServer != nil || globalFUSE != nil || globalOwner != nil || globalMetrics != nil || globalNFSController != nil || globalStopPending
	globalMu.Unlock()
	if occupied {
		t.Fatal("routing test requires empty serving runtime")
	}
	oldSelect, oldLink := selectServingOwner, startServingLink
	oldMode := servingProcessMode.Load()
	selected := &ownerFacade{}
	const sentinel = "selected owner original claim remains pending"
	claims, localLinks := 0, 0
	selectServingOwner = func(got ServerConfig) (*ownerFacade, error) {
		claims++
		if got != cfg {
			t.Error("selected owner did not receive original requested config")
		}
		return selected, errors.New(sentinel)
	}
	startServingLink = func(*ServerConfig) (*jmnfs.LinkNode, bool, error) {
		localLinks++
		return nil, false, errors.New("local Link path forbidden in selected owner test")
	}
	servingProcessMode.Store(true)
	t.Cleanup(func() {
		selectServingOwner, startServingLink = oldSelect, oldLink
		servingProcessMode.Store(oldMode)
		globalMu.Lock()
		if globalOwner == selected {
			globalOwner = nil
		}
		globalMu.Unlock()
		_ = NFSServerStop()
	})
	resp := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "start", Strings: []string{string(raw)}})
	if claims != 1 || localLinks != 0 || resp.Error != "" || !strings.Contains(resp.Text, sentinel) {
		t.Fatalf("actual Start did not route to retained owner: claims=%d localLinks=%d response=%+v", claims, localLinks, resp)
	}
	globalMu.Lock()
	retained := globalOwner == selected && globalFUSE == nil && globalServer == nil && globalMetrics == nil
	globalMu.Unlock()
	if !retained {
		t.Fatal("ambiguous selected claim lost custody or published local runtime")
	}
}
