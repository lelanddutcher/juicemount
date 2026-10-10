package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The farm walk must NEVER descend into .juicemount.
//
// This is a safety property for the streaming-drain design (task #4): a file
// that is mid-drain lives under .juicemount/ and is INCOMPLETE by construction.
// If collectTargets could see it, the farm would enqueue a partial file as a
// job target and generate derivatives from truncated bytes — the black-frame
// class of failure, arriving through the back door.
//
// It also protects what is there today: the derivative blobs themselves must
// never become inputs to derivative generation.
func TestCollectTargetsNeverDescendsIntoJuicemount(t *testing.T) {
	root := t.TempDir()

	// A real target the walk SHOULD find, so a pass cannot be vacuous.
	real := filepath.Join(root, "Project", "clip.mov")
	mustWrite(t, real, 32<<20)

	// Everything the derivative namespace can contain, including the shapes the
	// streaming design would add.
	for _, p := range []string{
		filepath.Join(root, ".juicemount", "derivatives", "12345", "proxy.mp4"),
		filepath.Join(root, ".juicemount", "derivatives", "12345", ".stage-999-1-proxy.mp4"),
		filepath.Join(root, ".juicemount", "inflight", "7-abc"),
		filepath.Join(root, ".juicemount", "derivatives", "12345", "manifest.json"),
	} {
		mustWrite(t, p, 32<<20) // big enough to clear any min-size rule
	}

	got, err := collectTargets(root, "", 0, 20<<20)
	if err != nil {
		t.Fatalf("collectTargets: %v", err)
	}

	var leaked []string
	var sawReal bool
	for _, g := range got {
		if strings.Contains(g, ".juicemount") {
			leaked = append(leaked, g)
		}
		if g == real {
			sawReal = true
		}
	}
	if len(leaked) > 0 {
		t.Errorf("farm walk descended into .juicemount and would enqueue %v — a mid-drain "+
			"or derivative file must never become a job target", leaked)
	}
	if !sawReal {
		t.Fatalf("walk did not find the ordinary target %q — the test proves nothing if the "+
			"walk found nothing at all (got %v)", real, got)
	}
}

func mustWrite(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
}
