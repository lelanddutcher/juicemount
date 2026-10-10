package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestQueuedJobScopeNeverEscapesWorkerMount(t *testing.T) {
	base := t.TempDir()
	mount := filepath.Join(base, "jfs")
	nested := filepath.Join(mount, "media", "nested")
	outside := filepath.Join(base, "outside")
	for _, dir := range []string{nested, outside} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(mount, "escape")); err != nil {
		t.Fatal(err)
	}
	cfg := queueConfig{mount: mount}
	cases := []struct {
		name, path string
		reject     bool
	}{
		{"nested", nested, false},
		{"outside", outside, true},
		{"prefix sibling", mount + "-other", true},
		{"traversal", mount + "/media/../../../outside", true},
		{"symlink escape", filepath.Join(mount, "escape"), true},
		{"relative", "media/nested", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := runJob(context.Background(), nil, nil, cfg, farmqueue.Worker{}, farmqueue.NewJob(tc.path, []string{farmqueue.KindDerivatives}, "manager"), nil)
			if (err != nil) != tc.reject {
				t.Fatalf("path=%q err=%v reject=%v", tc.path, err, tc.reject)
			}
		})
	}
}

func TestRetryScopeRejectsSymlinkEscape(t *testing.T) {
	base := t.TempDir()
	mount := filepath.Join(base, "jfs")
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(mount, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	media := filepath.Join(outside, "clip.mov")
	if err := os.WriteFile(media, []byte("media"), 0o644); err != nil {
		t.Fatal(err)
	}
	scope := filepath.Join(mount, "escape")
	if err := os.Symlink(outside, scope); err != nil {
		t.Fatal(err)
	}
	if got, err := collectRetryTargets(mount, scope, []string{filepath.Join(scope, "clip.mov")}, 0); err == nil {
		t.Fatalf("symlinked retry scope reached outside mount: %v", got)
	}
}

func TestQueuedCollectionSkipsSymlinkMediaLeaf(t *testing.T) {
	base := t.TempDir()
	mount := filepath.Join(base, "jfs")
	inside := filepath.Join(mount, "media", "nested")
	outside := filepath.Join(base, "outside")
	for _, dir := range []string{inside, outside} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	good := filepath.Join(inside, "good.mov")
	foreign := filepath.Join(outside, "foreign.mov")
	for _, path := range []string{good, foreign} {
		if err := os.WriteFile(path, []byte("media-123"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(inside, "tiny.mov"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inside, "proxy.mov"), []byte("media-123"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, filepath.Join(inside, "linked.mov")); err != nil {
		t.Fatal(err)
	}
	scope, err := validateQueuePath(mount, inside)
	if err != nil {
		t.Fatal(err)
	}
	got, err := collectTargetsWithPolicy(scope, "", 0, 4, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != good {
		t.Fatalf("queued targets=%v, want only %q", got, good)
	}
}

func TestQueuedMountAliasKeepsNestedMedia(t *testing.T) {
	base := t.TempDir()
	realMount := filepath.Join(base, "real-jfs")
	alias := filepath.Join(base, "alias-jfs")
	nested := filepath.Join(alias, "media", "nested")
	if err := os.MkdirAll(filepath.Join(realMount, "media", "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realMount, alias); err != nil {
		t.Fatal(err)
	}
	if got, err := validateQueuePath(alias, nested); err != nil || got != nested {
		t.Fatalf("mount alias nested path = %q, %v", got, err)
	}
}
