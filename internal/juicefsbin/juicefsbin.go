// Package juicefsbin names the only JuiceFS executable JuiceMount runs: the
// pinned, patched build that scripts/build-app.sh places beside the app's
// executables (Contents/MacOS/juicefs). Its signal handlers never unmount by
// path (third_party/juicefs patch 0005). Any other juicefs (Homebrew, PATH,
// /usr) answers SIGTERM with `umount -f <path>`, which can remove a mount this
// process does not own, so nothing here searches for one.
package juicefsbin

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Name is the bundled executable's file name.
const Name = "juicefs"

// NotBundledError refuses a JuiceFS executable that is not the bundled one.
type NotBundledError struct {
	Path    string // the refused executable
	Bundled string // the bundled executable, or "" when this build has none
	Source  string // where the refused path came from
}

func (e *NotBundledError) Error() string {
	if e.Bundled == "" {
		return fmt.Sprintf("JuiceFS executable %q from %s refused: this build has no bundled juicefs beside its executable", e.Path, e.Source)
	}
	return fmt.Sprintf("JuiceFS executable %q from %s refused: only the bundled %q may run", e.Path, e.Source, e.Bundled)
}

// host is the resolver's view of the machine; tests replace it.
type host struct {
	executable func() (string, error)
	stat       func(string) (fs.FileInfo, error)
}

var realHost = host{executable: os.Executable, stat: os.Stat}

var override struct {
	sync.Mutex
	path string
}

// SetForTest makes Resolve return path until restore is called. Only _test.go
// files may call it (TestSetForTestIsTestOnly scans the repository); Go
// compiles those files into test binaries alone, so a release build never
// reaches it, and no environment variable or flag leads to it.
func SetForTest(path string) (restore func()) {
	override.Lock()
	previous := override.path
	override.path = path
	override.Unlock()
	return func() {
		override.Lock()
		override.path = previous
		override.Unlock()
	}
}

// Resolve returns the bundled juicefs beside the running executable, or "".
func Resolve() string {
	override.Lock()
	path := override.path
	override.Unlock()
	if path != "" {
		return path
	}
	return resolveOn(realHost)
}

func resolveOn(h host) string {
	executable, err := h.executable()
	if err != nil {
		return ""
	}
	return besideOn(h, executable)
}

// Beside returns the regular, executable juicefs in executable's directory,
// or "".
func Beside(executable string) string { return besideOn(realHost, executable) }

func besideOn(h host, executable string) string {
	candidate := filepath.Join(filepath.Dir(executable), Name)
	info, err := h.stat(candidate)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return ""
	}
	return candidate
}

// Validate admits an explicitly configured JuiceFS executable only when it is
// the bundled one. An empty path is returned unchanged (callers resolve it).
// Builds tagged jmdevjuicefs admit any explicit path; build-app.sh never sets
// that tag and scripts/verify-build.sh refuses a service built with it.
func Validate(path, source string) (string, error) {
	if path == "" || devAnyExplicitBinary {
		return path, nil
	}
	bundled := Resolve()
	if bundled != "" && filepath.Clean(path) == filepath.Clean(bundled) {
		return bundled, nil
	}
	return "", &NotBundledError{Path: path, Bundled: bundled, Source: source}
}
