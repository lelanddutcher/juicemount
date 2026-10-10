package main

import (
	"strings"
	"testing"
)

func TestNFSMountArgsUseServerExportPath(t *testing.T) {
	const path = "/jm-0123456789abcdef0123456789abcdef"
	source, opts, err := nfsMountArgs("127.0.0.1:12049", path)
	if err != nil {
		t.Fatal(err)
	}
	if source != "127.0.0.1:"+path || strings.HasSuffix(source, ":/") {
		t.Fatalf("mount source = %q, want exact server export", source)
	}
	if !hasOpt(opts, "port=12049") || !hasOpt(opts, "mountport=12049") {
		t.Fatalf("mount options lost listener port: %q", opts)
	}
}

func TestNFSMountArgsRejectLegacyAndMalformedExports(t *testing.T) {
	for _, path := range []string{
		"", "/", "/jm-", "/jm-00000000000000000000000000000000", "/jm-0123456789abcdef0123456789abcde",
		"/jm-0123456789abcdef0123456789abcdef/", "/jm-0123456789abcdef0123456789abcdeF",
		"/jm-0123456789abcdef0123456789abcdeg",
	} {
		if source, opts, err := nfsMountArgs("127.0.0.1:12049", path); err == nil || source != "" || opts != "" {
			t.Errorf("path %q: source=%q opts=%q err=%v, want refusal", path, source, opts, err)
		}
		if err := mountNFSWithPrompt("127.0.0.1:12049", path, "/Volumes/unused"); err == nil {
			t.Errorf("interactive mount accepted invalid path %q", path)
		}
		if err := mountNFSNonInteractive("127.0.0.1:12049", path, "/Volumes/unused"); err == nil {
			t.Errorf("unattended mount accepted invalid path %q", path)
		}
	}
}

func TestNFSMountArgsRejectInvalidListenerBeforeMount(t *testing.T) {
	const path = "/jm-0123456789abcdef0123456789abcdef"
	for _, addr := range []string{"", "127.0.0.1", "127.0.0.1:0", "127.0.0.1:bad", ":12049"} {
		if source, opts, err := nfsMountArgs(addr, path); err == nil || source != "" || opts != "" {
			t.Errorf("addr %q: source=%q opts=%q err=%v, want refusal", addr, source, opts, err)
		}
	}
}
