//go:build !serviceclient

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// Overlay-only regression: the production dispatcher must durably stop while
// the volatile volume child fails full isolation validation. No listener,
// mount, external helper, or owner operation is started by this test.
func TestControlIsolationDispatchStopAndTerminalShutdown(t *testing.T) {
	root := controlIsolationFixture(t)
	cfg := ServerConfig{DBPath: filepath.Join(root, "metadata.db")}
	revision, err := beginServingDesired(cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope() // selection precedes the bad child
	if err != nil {
		t.Fatal(err)
	}
	intent, created, err := createOwnerIntentUnfenced(scope, cfg)
	if err != nil || !created {
		t.Fatalf("original selection: %+v %v", intent, err)
	}
	if err := updateServingDesired(revision, func(r *servingDesiredRecord) error { r.Owner = intent; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := confirmServingDesired(revision); err != nil {
		t.Fatal(err)
	}
	lifetime, err := acquireOwnerLifetime(scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := lifetime.publishRetired(intent, ownerShutdownID(intent)); err != nil {
		lifetime.close()
		t.Fatal(err)
	}
	lifetime.close()                       // original terminal receipt and lifetime, no live socket
	facade := &ownerFacade{intent: intent} // any new owner IPC is a test failure
	globalMu.Lock()
	if globalOwner != nil || globalFUSE != nil || globalServer != nil || globalNFSController != nil || globalStore != nil || globalMountPath != "" || globalStopPending {
		globalMu.Unlock()
		t.Fatal("dispatcher fixture requires idle resources")
	}
	globalOwner, globalOwnerStopID = facade, ""
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		if globalOwner == facade {
			globalOwner = nil
		}
		globalOwnerStopID = ""
		globalMu.Unlock()
	})
	if err := os.Symlink(filepath.Join(root, "absent"), filepath.Join(root, "volume")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := selectedOwnerScope(); err == nil {
		t.Fatal("full Start/owner admission accepted volatile child")
	}
	stop := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "stop-mount"})
	if stop.Error != "" || stop.Int != 1 {
		t.Fatalf("StopMount blocked by volatile child: %+v", stop)
	}
	afterStop, err := readServingDesired()
	if err != nil || afterStop.Desired != "stopped" || afterStop.Revision != revision+1 || afterStop.Owner != intent || afterStop.Config != cfg {
		t.Fatalf("StopMount changed original control identity: %+v %v", afterStop, err)
	}
	globalMu.Lock()
	retained := globalOwner == facade
	globalMu.Unlock()
	if !retained {
		t.Fatal("StopMount discarded retained owner")
	}
	shutdown := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "shutdown"})
	if shutdown.Error != "" || shutdown.Int != 1 {
		t.Fatalf("terminal Shutdown blocked by volatile child: %+v", shutdown)
	}
	afterShutdown, err := readServingDesired()
	if err != nil || afterShutdown.Desired != "stopped" || afterShutdown.Owner != intent || afterShutdown.Config != cfg || afterShutdown.Revision != revision+2 {
		t.Fatalf("Shutdown changed original control identity: %+v %v", afterShutdown, err)
	}
	if got, err := readOwnerIntent(scope); err != nil || got != intent {
		t.Fatalf("selected generation/claim identity changed: %+v %v", got, err)
	}
	if _, err := readOwnerRetirement(scope, intent); err != nil {
		t.Fatalf("exact terminal receipt lost: %v", err)
	}
	globalMu.Lock()
	retained = globalOwner != nil
	globalMu.Unlock()
	if retained {
		t.Fatal("terminal owner remained published")
	}
}

// An idle replacement still routes through the actual dispatcher, but an
// occupied foreign native path cannot authorize owner retirement or a socket.
func TestControlIsolationDispatchHoldsForeignNativeMount(t *testing.T) {
	root := controlIsolationFixture(t)
	volume := filepath.Join(root, "volume")
	if err := os.Mkdir(filepath.Join(root, "mount-transactions"), 0700); err != nil {
		t.Fatal(err)
	}
	cfg := ServerConfig{DBPath: filepath.Join(root, "metadata.db"), MountPoint: volume}
	revision, err := beginServingDesired(cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	intent, created, err := createOwnerIntentUnfenced(scope, cfg)
	if err != nil || !created {
		t.Fatalf("original selection: %+v %v", intent, err)
	}
	if err := updateServingDesired(revision, func(r *servingDesiredRecord) error { r.Owner = intent; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := confirmServingDesired(revision); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "absent"), volume); err != nil {
		t.Fatal(err)
	}
	if _, _, err := selectedOwnerScope(); err == nil {
		t.Fatal("full owner admission accepted volatile child")
	}
	priorObserve := observeOwnedNFS
	observations := 0
	observeOwnedNFS = func(context.Context) (mounttable.Snapshot, error) {
		observations++
		return mounttable.Snapshot{Version: 1, BootID: "current-boot", Entries: []mounttable.MountEntry{{Path: volume, Source: "foreign:/", Type: "nfs", FSID: [2]int32{6, 7}}}}, nil
	}
	t.Cleanup(func() { observeOwnedNFS = priorObserve })
	priorSocket := shutdownOwnerSocketPath
	shutdownOwnerSocketPath = func() (string, error) { t.Error("foreign mount reached owner IPC"); return "", os.ErrPermission }
	t.Cleanup(func() { shutdownOwnerSocketPath = priorSocket })
	globalMu.Lock()
	if globalOwner != nil || globalFUSE != nil || globalServer != nil || globalNFSController != nil || globalMountPath != "" || globalStopPending {
		globalMu.Unlock()
		t.Fatal("replacement fixture requires idle engine")
	}
	globalMu.Unlock()
	result := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "shutdown"})
	if result.Error != "" || result.Int != 0 {
		t.Fatalf("foreign native mount admitted: %+v", result)
	}
	if observations != 1 {
		t.Fatalf("foreign native observation count: got %d, want 1", observations)
	}
	desired, err := readServingDesired()
	if err != nil || desired.Desired != "stopped" || desired.Owner != intent || desired.Config != cfg {
		t.Fatalf("HOLD lost original intent: %+v %v", desired, err)
	}
	if got, err := readOwnerIntent(scope); err != nil || got != intent {
		t.Fatalf("HOLD lost selected generation/claim: %+v %v", got, err)
	}
	if _, err := readOwnerRetirement(scope, intent); !os.IsNotExist(err) {
		t.Fatalf("foreign mount published retirement: %v", err)
	}
}
