package appisolation

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"syscall"
	"testing"
)

func TestControlScopeSurvivesMountedChildEIOButFullCurrentRefuses(t *testing.T) {
	root, _ := isolationFixture(t)
	setIsolationEnv(t, root)
	volatile := filepath.Join(root, "volume")
	probes := 0
	lstat := func(path string) (os.FileInfo, error) {
		if path == volatile {
			probes++
			return nil, syscall.EIO
		}
		return os.Lstat(path)
	}
	if _, active, err := currentWith(true, lstat); !errors.Is(err, syscall.EIO) || active {
		t.Fatalf("full scope accepted unreadable mounted child: active=%v err=%v", active, err)
	}
	if probes != 1 {
		t.Fatalf("full scope skipped the data-plane probe: %d", probes)
	}
	if scope, active, err := currentWith(false, lstat); err != nil || !active || scope.Root != root || probes != 1 {
		t.Fatalf("control scope probed volatile child: scope=%+v active=%v probes=%d err=%v", scope, active, probes, err)
	}
	if scope, active, err := CurrentControl(); err != nil || !active || scope.Root != root {
		t.Fatalf("exported control scope rejected valid root: %+v %v %v", scope, active, err)
	}
}

func TestControlScopeRejectsRootMarkerAndControlPathErrors(t *testing.T) {
	root, _ := isolationFixture(t)
	setIsolationEnv(t, root)
	marker := filepath.Join(root, Marker)
	if _, _, err := currentWith(false, func(path string) (os.FileInfo, error) {
		if path == marker {
			return nil, syscall.EIO
		}
		return os.Lstat(path)
	}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("marker I/O error accepted: %v", err)
	}
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CurrentControl(); err == nil {
		t.Fatal("insecure root accepted")
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte("wrong marker\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CurrentControl(); err == nil {
		t.Fatal("invalid marker body accepted")
	}
	if err := os.WriteFile(marker, []byte(markerBody), 0600); err != nil {
		t.Fatal(err)
	}
	scope, _, err := CurrentControl()
	if err != nil {
		t.Fatal(err)
	}
	control := filepath.Join(root, "mount-transactions")
	if err := scope.requireChildWith(control, func(path string) (os.FileInfo, error) {
		if path == control {
			return nil, syscall.EIO
		}
		return os.Lstat(path)
	}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("control-path I/O error accepted: %v", err)
	}
	if err := os.Symlink(root, control); err != nil {
		t.Fatal(err)
	}
	if err := scope.RequireChild(control); err == nil {
		t.Fatal("linked control directory accepted")
	}
}

// These cases exercise Current itself. The root is a new short directory in
// the user's private state area; no app, service, socket, or mount is started.
func isolationFixture(t *testing.T) (string, string) {
	t.Helper()
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(account.HomeDir, "Developer", ".state")
	if st, err := os.Stat(base); err != nil || !st.IsDir() {
		t.Skip("private Developer/.state fixture base unavailable")
	}
	root, err := os.MkdirTemp(base, "jm-i-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Mkdir(filepath.Join(root, "tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, Marker), []byte("JuiceMount isolated state v1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return root, account.HomeDir
}

func setIsolationEnv(t *testing.T, root string) {
	t.Helper()
	t.Setenv("JM_APP_ISOLATION", "1")
	t.Setenv("JM_APP_ISOLATION_ROOT", root)
	t.Setenv("TMPDIR", filepath.Join(root, "tmp"))
}

func unsetIsolationEnv(t *testing.T, key string) {
	t.Helper()
	old, present := os.LookupEnv(key)
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	})
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
}

func TestCurrentPrivateRootBelowAccountHome(t *testing.T) {
	root, _ := isolationFixture(t)
	setIsolationEnv(t, root)
	scope, active, err := Current()
	if err != nil || !active || scope.Root != root {
		t.Fatalf("private root rejected: active=%v root=%q err=%v", active, scope.Root, err)
	}
	if err := scope.RequireChild(filepath.Join(root, "data", "metadata.db")); err != nil {
		t.Fatalf("private DB path rejected: %v", err)
	}
}

func TestCurrentNormalAndIncompleteScopes(t *testing.T) {
	root, _ := isolationFixture(t)
	t.Run("absent is ordinary mode", func(t *testing.T) {
		unsetIsolationEnv(t, "JM_APP_ISOLATION")
		unsetIsolationEnv(t, "JM_APP_ISOLATION_ROOT")
		_, active, err := Current()
		if err != nil || active {
			t.Fatalf("ordinary mode changed: active=%v err=%v", active, err)
		}
	})
	t.Run("flag only", func(t *testing.T) {
		setIsolationEnv(t, root)
		unsetIsolationEnv(t, "JM_APP_ISOLATION_ROOT")
		if _, _, err := Current(); err == nil {
			t.Fatal("partial scope accepted")
		}
	})
	t.Run("root only", func(t *testing.T) {
		setIsolationEnv(t, root)
		unsetIsolationEnv(t, "JM_APP_ISOLATION")
		if _, _, err := Current(); err == nil {
			t.Fatal("partial scope accepted")
		}
	})
}

func TestCurrentRejectsAccountHomeProtectedDescendantAndSymlink(t *testing.T) {
	root, home := isolationFixture(t)
	t.Run("real home", func(t *testing.T) {
		setIsolationEnv(t, home)
		if _, _, err := Current(); err == nil {
			t.Fatal("account home accepted as private root")
		}
	})
	t.Run("protected descendant", func(t *testing.T) {
		setIsolationEnv(t, filepath.Join(home, "Library", "JuiceMount"))
		if _, _, err := Current(); err == nil {
			t.Fatal("protected Library descendant accepted")
		}
	})
	t.Run("symlink alias", func(t *testing.T) {
		alias := root + "-alias"
		if err := os.Symlink(root, alias); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Remove(alias) })
		setIsolationEnv(t, alias)
		if _, _, err := Current(); err == nil {
			t.Fatal("symlinked root accepted")
		}
	})
}
