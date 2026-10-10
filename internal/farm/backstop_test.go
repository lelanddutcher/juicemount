package farm

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDiscoverModifiedRecursesAndExcludesFarmOutputs(t *testing.T) {
	root := t.TempDir()
	cursor := time.Now().Add(-time.Minute)
	old := cursor.Add(-time.Minute)
	newer := cursor.Add(time.Second)
	for _, dir := range []string{"old/nested", "new/reel", ".juicemount/derivatives"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	rootClip := filepath.Join(root, "landed.mov")
	if err := os.WriteFile(rootClip, []byte("media"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(filepath.Join(root, "old"), old, old)
	_ = os.Chtimes(filepath.Join(root, "old", "nested"), newer, newer)
	_ = os.Chtimes(filepath.Join(root, "new"), newer, newer)
	_ = os.Chtimes(filepath.Join(root, ".juicemount"), newer, newer)
	_ = os.Chtimes(rootClip, newer, newer)

	got, saturated, err := DiscoverModified(context.Background(), root, cursor, 20)
	if err != nil || saturated {
		t.Fatalf("discover: saturated=%v err=%v", saturated, err)
	}
	want := map[string]bool{"old/nested": true, "new": true, "landed.mov": true}
	for _, path := range got {
		if !want[path] {
			t.Errorf("unexpected path %q in %v", path, got)
		}
		delete(want, path)
	}
	if len(want) != 0 {
		t.Fatalf("missing modified paths %v from %v", want, got)
	}
}

func TestDiscoverModifiedFindsEditedNestedFileWithUnchangedDirectoryMtime(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "shoot", "card")
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	clip := filepath.Join(parent, "take.mov")
	if err := os.WriteFile(clip, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	cursor := time.Now().Add(-time.Hour)
	old := cursor.Add(-time.Hour)
	newer := cursor.Add(time.Minute)
	for _, dir := range []string{filepath.Join(root, "shoot"), parent} {
		if err := os.Chtimes(dir, old, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(clip, newer, newer); err != nil {
		t.Fatal(err)
	}

	got, saturated, err := DiscoverModified(context.Background(), root, cursor, 20)
	if err != nil || saturated {
		t.Fatalf("discover: saturated=%v err=%v", saturated, err)
	}
	if len(got) != 1 || got[0] != "shoot/card" {
		t.Fatalf("edited nested file should enqueue its parent, got %v", got)
	}
}

func TestDiscoverModifiedDeduplicatesNestedJobsBeforeCandidateCap(t *testing.T) {
	root := t.TempDir()
	cursor := time.Now().Add(-time.Hour)
	old := cursor.Add(-time.Hour)
	newer := cursor.Add(time.Minute)
	for _, dir := range []string{"a", "b", ".juicemount/derivatives"} {
		path := filepath.Join(root, dir)
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, rel := range []string{"a/one.mov", "a/two.mov", "b/three.mov", ".juicemount/derivatives/own.mov"} {
		path := filepath.Join(root, rel)
		if err := os.WriteFile(path, []byte("media"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, newer, newer); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"a", "b", ".juicemount", ".juicemount/derivatives"} {
		path := filepath.Join(root, dir)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	got, saturated, err := DiscoverModified(context.Background(), root, cursor, 2)
	if err != nil || !saturated {
		t.Fatalf("discover: saturated=%v err=%v", saturated, err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expected one candidate per changed parent, got %v", got)
	}
}
