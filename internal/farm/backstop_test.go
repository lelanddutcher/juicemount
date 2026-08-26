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
