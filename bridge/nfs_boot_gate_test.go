package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBootNFSMountAllowedRequiresVerifiedFUSE(t *testing.T) {
	tests := []struct {
		name      string
		fusePath  string
		fuseReady bool
		want      bool
	}{
		{name: "desktop partial FUSE is withheld", fusePath: "/Users/test/.juicemount/fuse-internal", want: false},
		{name: "desktop verified FUSE is published", fusePath: "/Users/test/.juicemount/fuse-internal", fuseReady: true, want: true},
		{name: "explicit NFS-only setup is preserved", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bootNFSMountAllowed(tt.fusePath, tt.fuseReady); got != tt.want {
				t.Fatalf("bootNFSMountAllowed(%q, %v) = %v, want %v", tt.fusePath, tt.fuseReady, got, tt.want)
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
	start := strings.Index(text, "nfsMountReused := false")
	if start < 0 {
		t.Fatal("could not locate the boot NFS mount source window")
	}
	end := strings.Index(text[start:], "// Health monitor.")
	if end < 0 {
		t.Fatal("could not locate the boot NFS mount source window")
	}
	window := text[start : start+end]
	if !strings.Contains(window, `if cfg.MountPoint != "" && !bootNFSMountAllowed(cfg.FUSEPath, bootFUSEReady)`) {
		t.Fatal("the actual boot NFS mount call site is not guarded by verified FUSE readiness")
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
