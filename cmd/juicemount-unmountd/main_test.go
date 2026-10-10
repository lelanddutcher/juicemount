package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestRefusesToRunWithoutRoot(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"-root", t.TempDir() + "/r"}, 501, &out); code != 1 || !strings.Contains(out.String(), "must be root") {
		t.Fatalf("non-root run: code %d output %q", code, out.String())
	}
}

func TestPrintRequirementsNeedsNoRootAndIsRelease(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"-print-requirements"}, 501, &out); code != 0 {
		t.Fatalf("code %d: %s", code, out.String())
	}
	s := out.String()
	if !strings.Contains(s, "channel=release\n") ||
		!strings.Contains(s, "service=anchor apple generic") || !strings.Contains(s, `certificate leaf[subject.OU] = "PKUE74YS72"`) ||
		strings.Count(s, " and notarized and ") != 2 {
		t.Fatalf("a default build must print the release requirements:\n%s", s)
	}
}

func TestCheckPeerRejectsAdHocCode(t *testing.T) {
	if checkPeer == nil {
		t.Skip("no transport compiled in on this platform")
	}
	self, err := os.Executable() // linker-signed ad-hoc
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"service", "status"} {
		var out bytes.Buffer
		if code := run([]string{"-check-peer", self, "-check-peer-role", role}, 501, &out); code != 1 ||
			!strings.Contains(out.String(), "does not satisfy") {
			t.Fatalf("%s: ad-hoc code passed -check-peer: code %d %q", role, code, out.String())
		}
	}
	var out bytes.Buffer
	if code := run([]string{"-check-peer", self, "-check-peer-role", "root"}, 501, &out); code != 1 {
		t.Fatalf("unknown role accepted: %d %q", code, out.String())
	}
}
