package metadata

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreFileLockContentionAndClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	first, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dbPath); !errors.Is(err, ErrStoreAlreadyOpen) {
		_ = first.Close()
		t.Fatalf("second Open error = %v, want ErrStoreAlreadyOpen", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreFileLockNormalizesPathAliases(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "metadata.db")
	first, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	fileAlias := filepath.Join(root, "metadata-alias.db")
	if err := os.Symlink(dbPath, fileAlias); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(fileAlias); !errors.Is(err, ErrStoreAlreadyOpen) {
		t.Errorf("symlinked database path Open error = %v, want ErrStoreAlreadyOpen", err)
	}

	parentAlias := filepath.Join(root, "parent-alias")
	if err := os.Symlink(root, parentAlias); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(filepath.Join(parentAlias, "metadata.db")); !errors.Is(err, ErrStoreAlreadyOpen) {
		t.Errorf("symlinked parent Open error = %v, want ErrStoreAlreadyOpen", err)
	}

	previousDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previousDir) })
	if _, err := Open("metadata.db"); !errors.Is(err, ErrStoreAlreadyOpen) {
		t.Errorf("relative database path Open error = %v, want ErrStoreAlreadyOpen", err)
	}
}

func TestStoreFileLockRejectsSQLiteURI(t *testing.T) {
	for _, path := range []string{
		"file:" + filepath.Join(t.TempDir(), "metadata.db") + "?mode=rwc",
		filepath.Join(t.TempDir(), "metadata?query.db"),
		filepath.Join(t.TempDir(), "metadata#fragment.db"),
	} {
		if _, err := Open(path); err == nil {
			t.Errorf("Open(%q) unexpectedly succeeded", path)
		}
	}
}

func TestStoreFileLockRejectsDanglingDatabaseSymlink(t *testing.T) {
	root := t.TempDir()
	link := filepath.Join(root, "metadata.db")
	if err := os.Symlink(filepath.Join(root, "future.db"), link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := acquireStoreFileLock(link); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("dangling symlink lock error = %v, want explicit symlink error", err)
	}
}

func TestStoreFileLockRejectsHardLinkAliases(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "metadata.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "metadata-hardlink.db")
	if err := os.Link(path, alias); err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	if _, err := Open(alias); err == nil || !strings.Contains(err.Error(), "hard links") {
		_ = first.Close()
		t.Fatalf("hard-linked alias Open error = %v, want hard-link refusal", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "hard links") {
		t.Fatalf("hard-linked database Open error = %v, want hard-link refusal", err)
	}
}

func TestStoreFileLockReleasedAfterOpenFailure(t *testing.T) {
	directoryDB := filepath.Join(t.TempDir(), "not-a-database")
	if err := os.Mkdir(directoryDB, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(directoryDB); err == nil {
		t.Fatal("Open on a directory unexpectedly succeeded")
	}
	lock, _, err := acquireStoreFileLock(directoryDB)
	if err != nil {
		t.Fatalf("lock remained held after failed Open: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreFileLockReleasedAfterProcessExit(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "child-open")
	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestStoreFileLockHelperProcess$")
	cmd.Env = append(os.Environ(), "JM_STORE_LOCK_HELPER=1", "JM_STORE_LOCK_DB="+dbPath, "JM_STORE_LOCK_MARKER="+marker)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if cmd.ProcessState != nil {
			t.Fatalf("lock helper exited before publishing marker: %v", cmd.ProcessState)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("helper did not acquire Store lock: %v", err)
	}
	if _, err := Open(dbPath); !errors.Is(err, ErrStoreAlreadyOpen) {
		t.Fatalf("concurrent parent Open error = %v, want ErrStoreAlreadyOpen", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("lock helper exited successfully before forced process termination")
	}
	cmd.Process = nil

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open after helper process exit: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreFileLockHelperProcess(t *testing.T) {
	if os.Getenv("JM_STORE_LOCK_HELPER") != "1" {
		return
	}
	store, err := Open(os.Getenv("JM_STORE_LOCK_DB"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	_ = store
	if err := os.WriteFile(os.Getenv("JM_STORE_LOCK_MARKER"), []byte("ready"), 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	select {}
}
