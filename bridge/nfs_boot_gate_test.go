package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBootNFSMountAllowedRequiresVerifiedFUSE(t *testing.T) {
	tests := []struct {
		name       string
		fusePath   string
		fuseReady  bool
		cachedOnly bool
		want       bool
	}{
		{name: "desktop partial FUSE is withheld", fusePath: "/Users/test/.juicemount/fuse-internal", want: false},
		{name: "desktop verified FUSE is published", fusePath: "/Users/test/.juicemount/fuse-internal", fuseReady: true, want: true},
		{name: "desktop cached-only namespace is published while FUSE recovers", fusePath: "/Users/test/.juicemount/fuse-internal", cachedOnly: true, want: true},
		{name: "explicit NFS-only setup is preserved", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bootNFSMountAllowed(tt.fusePath, tt.fuseReady, tt.cachedOnly); got != tt.want {
				t.Fatalf("bootNFSMountAllowed(%q, %v, %v) = %v, want %v", tt.fusePath, tt.fuseReady, tt.cachedOnly, got, tt.want)
			}
		})
	}
}

func TestNFSListenerRequiresVerifiedFUSE(t *testing.T) {
	for _, tt := range []struct {
		name       string
		fusePath   string
		fuseReady  bool
		cachedOnly bool
		want       bool
	}{
		{name: "desktop deferred FUSE keeps listener closed", fusePath: "/fuse", want: false},
		{name: "desktop verified FUSE opens listener", fusePath: "/fuse", fuseReady: true, want: true},
		{name: "desktop cached-only mode opens listener", fusePath: "/fuse", cachedOnly: true, want: true},
		{name: "NFS-only server opens listener", want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := nfsListenerMayStart(tt.fusePath, tt.fuseReady, tt.cachedOnly); got != tt.want {
				t.Fatalf("nfsListenerMayStart(%q, %v, %v) = %v, want %v", tt.fusePath, tt.fuseReady, tt.cachedOnly, got, tt.want)
			}
		})
	}
}

func TestBootNFSMountCallSiteUsesVerifiedFUSEGate(t *testing.T) {
	source, err := os.ReadFile("cbridge.go")
	if err != nil {
		t.Fatalf("read cbridge.go: %v", err)
	}
	text := string(source)
	start := strings.Index(text, `if cfg.MountPoint != "" && !bootNFSMountAllowed(cfg.FUSEPath, bootFUSEReady, startedOffline)`)
	if start < 0 {
		t.Fatal("could not locate the boot NFS mount source window")
	}
	record := strings.Index(text, `bootMounted := recordExistingBootNFSMount(cfg.MountPoint, inspectConfiguredNFSMount)`)
	if record < 0 || record > start {
		t.Fatal("existing kernel mount is not recorded before the deferred-FUSE branch")
	}
	end := strings.Index(text[start:], "// Health monitor.")
	if end < 0 {
		t.Fatal("could not locate the boot NFS mount source window")
	}
	window := text[start : start+end]
	if !strings.Contains(window, `if cfg.MountPoint != "" && !bootNFSMountAllowed(cfg.FUSEPath, bootFUSEReady, startedOffline)`) {
		t.Fatal("the actual boot NFS mount call site is not guarded by verified FUSE readiness")
	}
}

func TestDeferredFUSEStartupRetainsExistingNFSMountForShutdown(t *testing.T) {
	spool, drainer := shutdownTestDrainer(t)
	publishShutdownTestDrainer(t, spool, drainer, nil)
	globalMu.Lock()
	previousPath, previousFlights := globalMountPath, globalMountFlights
	globalMountPath = ""
	globalMountFlights = newNFSMountFlights()
	if bootNFSMountAllowed("/unverified/fuse", false, false) {
		globalMu.Unlock()
		t.Fatal("test requires deferred FUSE startup")
	}
	mounted := recordExistingBootNFSMount("/Volumes/deferred-existing", func(string) nfsMountOwnership { return nfsMountOurs })
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalMountPath, globalMountFlights = previousPath, previousFlights
		globalMu.Unlock()
	})
	if !mounted {
		t.Fatal("existing kernel mount was not detected")
	}
	if stopServerSerializedWithDeadline(20 * time.Millisecond) {
		t.Fatal("shutdown closed stores beneath a deferred existing NFS mount")
	}
	globalMu.Lock()
	retained := globalMountPath == "/Volumes/deferred-existing" && globalSpool == spool
	globalMu.Unlock()
	if !retained {
		t.Fatal("deferred mount or spool ownership was lost")
	}
	if unmountAndClearNFS("/Volumes/deferred-existing", func(string) bool { return false }) {
		t.Fatal("failed unmount reported success")
	}
	if stopServerSerializedWithDeadline(20 * time.Millisecond) {
		t.Fatal("failed unmount allowed stores to close")
	}
	if !unmountAndClearNFS("/Volumes/deferred-existing", func(string) bool { return true }) {
		t.Fatal("successful unmount reported failure")
	}
	if !stopServerSerializedWithDeadline(time.Second) {
		t.Fatal("shutdown did not complete after confirmed unmount")
	}
}

func TestSwiftHealthProbeHasIndependentWaitDeadline(t *testing.T) {
	path := filepath.Join("..", "app", "JuiceMount", "Sources", "JuiceMount", "Core", "NFSBridge.swift")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read NFSBridge.swift: %v", err)
	}
	text := string(source)
	start := strings.Index(text, "public static func healthProbe")
	if start < 0 {
		t.Fatal("could not locate the healthProbe source window")
	}
	end := strings.Index(text[start:], "public static func offlineState")
	if end < 0 {
		t.Fatal("could not locate the healthProbe source window")
	}
	window := text[start : start+end]
	if !strings.Contains(window, "sem.wait(timeout:") || !strings.Contains(window, "session.invalidateAndCancel()") {
		t.Fatal("healthProbe must bound its semaphore wait and cancel the session on timeout")
	}
	if strings.Contains(window, "sem.wait()") {
		t.Fatal("healthProbe regressed to an unbounded semaphore wait")
	}
}
