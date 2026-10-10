package appisolation

import (
	"os"
	"os/user"
	"path/filepath"
	"testing"
)

func TestResolvePathsKeepsAccountHomeAndPrivateState(t *testing.T) {
	root, _ := isolationFixture(t)
	setIsolationEnv(t, root)
	t.Setenv("JM_CACHE_DIR", "")
	t.Setenv("JM_INFLIGHT_DUMP_DIR", "")
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	paths, isolated, err := ResolvePaths()
	if err != nil || !isolated || os.Getenv("HOME") != account.HomeDir ||
		paths.CacheBase != filepath.Join(root, ".juicefs", "cache") ||
		paths.FUSELog != filepath.Join(root, ".juicefs", "juicefs.log") ||
		paths.OfflineIntent != filepath.Join(root, ".juicemount", "offline-intent") ||
		paths.StallDumps != filepath.Join(root, "Library", "Logs", "JuiceMount", "stall-dumps") {
		t.Fatalf("explicit private paths or real HOME changed: %+v isolated=%t err=%v", paths, isolated, err)
	}
	volume := filepath.Join(paths.CacheBase, "12345678-1234-abcd-5678-123456789abc")
	t.Setenv("JM_CACHE_DIR", volume)
	if _, _, err := ResolvePaths(); err != nil {
		t.Fatalf("private volume override refused: %v", err)
	}
	t.Setenv("JM_CACHE_DIR", filepath.Join(account.HomeDir, ".juicefs", "cache", filepath.Base(volume)))
	if _, _, err := ResolvePaths(); err == nil {
		t.Fatal("ordinary-home cache override accepted")
	}
	t.Setenv("JM_CACHE_DIR", "")
	t.Setenv("JM_INFLIGHT_DUMP_DIR", filepath.Join(account.HomeDir, "Library", "Logs", "JuiceMount", "stall-dumps"))
	if _, _, err := ResolvePaths(); err == nil {
		t.Fatal("ordinary-home dump override accepted")
	}
}

func TestResolvePathsRejectsLinkedAndInsecureChildren(t *testing.T) {
	root, _ := isolationFixture(t)
	setIsolationEnv(t, root)
	t.Setenv("JM_CACHE_DIR", "")
	t.Setenv("JM_INFLIGHT_DUMP_DIR", "")
	link := filepath.Join(root, ".juicefs")
	if err := os.Symlink(filepath.Join(root, "tmp"), link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ResolvePaths(); err == nil {
		t.Fatal("linked cache/log parent accepted")
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(link, 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(link, 0777); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ResolvePaths(); err == nil {
		t.Fatal("insecure cache/log parent accepted")
	}
	if err := os.Chmod(link, 0755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ResolvePaths(); err != nil {
		t.Fatalf("repaired private parent refused: %v", err)
	}
}
