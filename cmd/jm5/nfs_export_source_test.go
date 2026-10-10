package main

import "testing"

func TestCLINFSMountSourceUsesServerExportPath(t *testing.T) {
	const path = "/jm-0123456789abcdef0123456789abcdef"
	source, port, err := cliNFSMountSource("127.0.0.1:12049", path)
	if err != nil {
		t.Fatal(err)
	}
	if source != "127.0.0.1:"+path || port != "12049" {
		t.Fatalf("source=%q port=%q, want exact export at listener port", source, port)
	}
}

func TestCLINFSMountRejectsUnavailableExportBeforePrivilegedCommand(t *testing.T) {
	for _, path := range []string{"", "/", "/jm-00000000000000000000000000000000", "/jm-0123456789abcdef0123456789abcdeF"} {
		if source, port, err := cliNFSMountSource("127.0.0.1:12049", path); err == nil || source != "" || port != "" {
			t.Errorf("path %q: source=%q port=%q err=%v, want refusal", path, source, port, err)
		}
		if err := mountNFS("127.0.0.1:12049", path, "/Volumes/unused", "/unused/meta.db"); err == nil {
			t.Errorf("CLI mount accepted invalid path %q", path)
		}
	}
}

func TestCLINFSMountSourceRejectsInvalidListener(t *testing.T) {
	const path = "/jm-0123456789abcdef0123456789abcdef"
	for _, addr := range []string{"", "127.0.0.1", "127.0.0.1:0", "127.0.0.1:bad", ":12049"} {
		if source, port, err := cliNFSMountSource(addr, path); err == nil || source != "" || port != "" {
			t.Errorf("addr %q: source=%q port=%q err=%v, want refusal", addr, source, port, err)
		}
	}
}
