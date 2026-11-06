package farm

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// topLevelFixture makes a mount with folders that settled before the cursor
// it returns. Their ctimes are real, so the cursor is taken after them.
func topLevelFixture(t *testing.T) (mount string, cursor, old time.Time) {
	t.Helper()
	mount = t.TempDir()
	for _, dir := range []string{"Existing", "Retired"} {
		if err := os.MkdirAll(filepath.Join(mount, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	cursor = time.Now()
	time.Sleep(20 * time.Millisecond)
	return mount, cursor, cursor.Add(-30 * 24 * time.Hour)
}

// A folder copied to the volume root with preserved dates (rsync -a, cp -p,
// Finder) carries mtimes older than the cursor all the way down, so the
// backstop found nothing and advanced its idle cursor past it. A copy cannot
// preserve ctime.
func TestDiscoverModifiedFindsTopLevelFolderWithPreservedMtimes(t *testing.T) {
	mount, cursor, old := topLevelFixture(t)
	dir := filepath.Join(mount, "Shoot_2026_10_01")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	clip := filepath.Join(dir, "A001.mov")
	if err := os.WriteFile(clip, []byte("media"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{clip, dir} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	paths, saturated, err := DiscoverModified(context.Background(), mount, cursor, 2000)
	if err != nil || saturated {
		t.Fatalf("discover: saturated=%v err=%v", saturated, err)
	}
	if !slices.Equal(paths, []string{"Shoot_2026_10_01"}) {
		t.Fatalf("paths = %v; want only the copied folder", paths)
	}
}

// Hidden files and deletes at the root change the root's mtime but say nothing
// about any folder. Treating a root mtime change as a rescan made a Finder
// .DS_Store or a removed folder start a full-volume root epoch.
func TestDiscoverModifiedIgnoresHiddenFilesAndDeletesAtTheRoot(t *testing.T) {
	mount, cursor, _ := topLevelFixture(t)
	if err := os.WriteFile(filepath.Join(mount, ".DS_Store"), []byte("finder"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(mount, "Retired")); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(mount); err != nil || !info.ModTime().After(cursor) {
		t.Fatalf("fixture: root mtime should follow the changes: %v %v", info, err)
	}
	paths, saturated, err := DiscoverModified(context.Background(), mount, cursor, 2000)
	if err != nil || saturated || len(paths) != 0 {
		t.Fatalf("paths=%v saturated=%v err=%v; want none", paths, saturated, err)
	}
}
