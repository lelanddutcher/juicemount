// Package appisolation validates the opt-in private desktop runtime namespace.
// The acceptance harness provisions the root and starts both the GUI and the
// bundled service with the same environment. Normal installs do not opt in.
package appisolation

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
)

const Marker = ".juicemount-isolated-v1"
const markerBody = "JuiceMount isolated state v1\n"

type Scope struct{ Root string }

// Current does not create or repair a namespace. A partial or invalid opt-in
// is an error, never a request to use the ordinary user's state.
func Current() (Scope, bool, error) {
	return currentWith(true, os.Lstat)
}

// CurrentControl validates the private control namespace without probing
// unrelated data-plane children. Every control path still requires its own
// RequireChild check before use.
func CurrentControl() (Scope, bool, error) {
	return currentWith(false, os.Lstat)
}

// lstat is per-call so a filesystem error can be tested without mutable
// process-wide hooks. Production always supplies os.Lstat.
func currentWith(full bool, lstat func(string) (os.FileInfo, error)) (Scope, bool, error) {
	enabled, haveEnabled := os.LookupEnv("JM_APP_ISOLATION")
	root, haveRoot := os.LookupEnv("JM_APP_ISOLATION_ROOT")
	if !haveEnabled && !haveRoot {
		return Scope{}, false, nil
	}
	if !haveEnabled || enabled != "1" || !haveRoot || root == "" {
		return Scope{}, false, errors.New("incomplete app isolation opt-in")
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || strings.IndexByte(root, 0) >= 0 || root == "/" {
		return Scope{}, false, errors.New("app isolation root must be a canonical absolute directory")
	}
	if root == "/tmp" || strings.HasPrefix(root, "/tmp/") || root == "/private/tmp" || strings.HasPrefix(root, "/private/tmp/") || root == "/var/tmp" || strings.HasPrefix(root, "/var/tmp/") {
		return Scope{}, false, errors.New("app isolation root may not be temporary storage")
	}
	current, err := user.Current()
	if err != nil || current.HomeDir == "" {
		return Scope{}, false, errors.New("app isolation account home is unavailable")
	}
	if os.Getenv("HOME") != current.HomeDir {
		return Scope{}, false, errors.New("app isolation HOME must remain the account home")
	}
	if root == current.HomeDir ||
		strings.HasPrefix(root, filepath.Join(current.HomeDir, "Library")+string(filepath.Separator)) ||
		root == filepath.Join(current.HomeDir, "Library") ||
		strings.HasPrefix(root, filepath.Join(current.HomeDir, ".juicemount")+string(filepath.Separator)) ||
		root == filepath.Join(current.HomeDir, ".juicemount") ||
		strings.HasPrefix(root, filepath.Join(current.HomeDir, ".juicefs")+string(filepath.Separator)) ||
		root == filepath.Join(current.HomeDir, ".juicefs") ||
		root == "/Volumes" || strings.HasPrefix(root, "/Volumes/") {
		return Scope{}, false, errors.New("app isolation root overlaps an ordinary state or mount directory")
	}
	tmp := filepath.Join(root, "tmp")
	if os.Getenv("TMPDIR") != tmp {
		return Scope{}, false, errors.New("app isolation TMPDIR must be the private tmp directory")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return Scope{}, false, errors.New("app isolation root must exist without symlink components")
	}
	if err := privateOwnedWith(root, true, lstat); err != nil {
		return Scope{}, false, err
	}
	if err := privateOwnedWith(tmp, true, lstat); err != nil {
		return Scope{}, false, err
	}
	marker := filepath.Join(root, Marker)
	if err := privateOwnedWith(marker, false, lstat); err != nil {
		return Scope{}, false, err
	}
	body, err := os.ReadFile(marker)
	if err != nil || string(body) != markerBody {
		return Scope{}, false, errors.New("app isolation marker is missing or invalid")
	}
	if len(filepath.Join(root, "s.sock")) > 103 {
		return Scope{}, false, errors.New("app isolation root is too long for a Unix socket")
	}
	scope := Scope{Root: root}
	if !full {
		return scope, true, nil
	}
	for _, child := range []string{
		"s.sock", "mount-transactions", "preferences.plist", "data", "logs", "spool",
		"fuse-internal", "volume", ".juicefs", ".juicefs/cache", ".juicefs/juicefs.log",
		".juicemount", ".juicemount/offline-intent", "Library", "Library/Logs",
		"Library/Logs/JuiceMount", "Library/Logs/JuiceMount/stall-dumps",
	} {
		if err := scope.requireChildWith(filepath.Join(root, child), lstat); err != nil {
			return Scope{}, false, err
		}
	}
	return scope, true, nil
}

func privateOwned(path string, directory bool) error {
	return privateOwnedWith(path, directory, os.Lstat)
}

func privateOwnedWith(path string, directory bool, lstat func(string) (os.FileInfo, error)) error {
	st, err := lstat(path)
	if err != nil {
		return fmt.Errorf("app isolation private path unavailable: %w", err)
	}
	if st.Mode()&os.ModeSymlink != 0 || directory && !st.IsDir() || !directory && !st.Mode().IsRegular() {
		return errors.New("app isolation path has unexpected file type")
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return errors.New("app isolation path is not owned by this user")
	}
	want := os.FileMode(0600)
	if directory {
		want = 0700
	}
	if st.Mode().Perm() != want {
		return errors.New("app isolation path has wrong permissions")
	}
	return nil
}

// RequireChild rejects paths outside the private root and existing symlink
// components, including a link planted below that root before Start.
func (s Scope) RequireChild(path string) error {
	return s.requireChildWith(path, os.Lstat)
}

func (s Scope) requireChildWith(path string, lstat func(string) (os.FileInfo, error)) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("isolated path must be canonical and absolute")
	}
	rel, err := filepath.Rel(s.Root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("isolated path escapes the private root")
	}
	for parent := path; parent != s.Root; parent = filepath.Dir(parent) {
		st, err := lstat(parent)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return errors.New("isolated path contains a symlink")
		}
	}
	return nil
}
