package juicefsbin

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeInfo struct{ mode fs.FileMode }

func (f fakeInfo) Name() string       { return "juicefs" }
func (f fakeInfo) Size() int64        { return 1 }
func (f fakeInfo) Mode() fs.FileMode  { return f.mode }
func (f fakeInfo) ModTime() time.Time { return time.Time{} }
func (f fakeInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeInfo) Sys() any           { return nil }

// fakeHost is a machine where juicefs exists in every place an installer or
// package manager puts it, and on PATH.
func fakeHost(executable string, files map[string]fs.FileMode) (host, *[]string) {
	var consulted []string
	return host{
		executable: func() (string, error) { return executable, nil },
		stat: func(path string) (fs.FileInfo, error) {
			consulted = append(consulted, path)
			if mode, ok := files[path]; ok {
				return fakeInfo{mode}, nil
			}
			return nil, fs.ErrNotExist
		},
	}, &consulted
}

var elsewhere = map[string]fs.FileMode{
	"/opt/homebrew/bin/juicefs": 0o755,
	"/usr/local/bin/juicefs":    0o755,
	"/usr/bin/juicefs":          0o755,
	"/custom/path/juicefs":      0o755,
}

// Machine-independent: juicefs at /opt/homebrew, /usr/local, /usr/bin and on
// PATH is never chosen, nor even looked at, when the bundle lacks one.
func TestResolveNeverFallsBack(t *testing.T) {
	t.Setenv("PATH", "/custom/path:/opt/homebrew/bin:/usr/local/bin:/usr/bin")
	h, consulted := fakeHost("/Applications/JuiceMount.app/Contents/MacOS/juicemount-service", elsewhere)
	if got := resolveOn(h); got != "" {
		t.Fatalf("resolved %q with no bundled juicefs", got)
	}
	if len(*consulted) != 1 || (*consulted)[0] != "/Applications/JuiceMount.app/Contents/MacOS/juicefs" {
		t.Fatalf("resolver consulted %v; want only the bundle sibling", *consulted)
	}
}

func TestResolveBundledSibling(t *testing.T) {
	files := map[string]fs.FileMode{"/Applications/JuiceMount.app/Contents/MacOS/juicefs": 0o755}
	for k, v := range elsewhere {
		files[k] = v
	}
	for _, exe := range []string{"/Applications/JuiceMount.app/Contents/MacOS/juicemount-service", "/Applications/JuiceMount.app/Contents/MacOS/JuiceMount"} {
		h, _ := fakeHost(exe, files)
		if got := resolveOn(h); got != "/Applications/JuiceMount.app/Contents/MacOS/juicefs" {
			t.Fatalf("%s resolved %q", exe, got)
		}
	}
	for name, mode := range map[string]fs.FileMode{"not executable": 0o644, "directory": fs.ModeDir | 0o755} {
		h, _ := fakeHost("/A.app/Contents/MacOS/x", map[string]fs.FileMode{"/A.app/Contents/MacOS/juicefs": mode})
		if got := resolveOn(h); got != "" {
			t.Fatalf("%s sibling resolved %q", name, got)
		}
	}
}

func TestValidateRefusesUnbundled(t *testing.T) {
	restore := SetForTest("/Applications/JuiceMount.app/Contents/MacOS/juicefs")
	defer restore()
	if got, err := Validate("/Applications/JuiceMount.app/Contents/MacOS/juicefs", "test"); err != nil || got != "/Applications/JuiceMount.app/Contents/MacOS/juicefs" {
		t.Fatalf("bundled path refused: %q %v", got, err)
	}
	if got, err := Validate("", "test"); err != nil || got != "" {
		t.Fatalf("empty path = %q %v", got, err)
	}
	for _, path := range []string{"/opt/homebrew/bin/juicefs", "/usr/local/bin/juicefs", "/usr/bin/juicefs", "juicefs"} {
		_, err := Validate(path, "owner receipt")
		var refused *NotBundledError
		if !errors.As(err, &refused) || refused.Path != path || refused.Source != "owner receipt" {
			t.Fatalf("%s: err = %v, want *NotBundledError", path, err)
		}
	}
}

func TestReleaseBuildHasNoDevOverride(t *testing.T) {
	if devAnyExplicitBinary {
		t.Skip("built with -tags jmdevjuicefs")
	}
	restore := SetForTest("")
	defer restore()
	if _, err := Validate("/opt/homebrew/bin/juicefs", "test"); err == nil {
		t.Fatal("release build admitted an unbundled juicefs")
	}
}

// SetForTest must stay test-only: no non-test file in the repository may call
// it. A release binary therefore has no path to a substituted juicefs.
func TestSetForTestIsTestOnly(t *testing.T) {
	root := filepath.Join("..", "..")
	checked := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "third_party", "build", ".build", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil // not our concern here
		}
		checked++
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if ok && sel.Sel.Name == "SetForTest" {
				if x, ok := sel.X.(*ast.Ident); ok && x.Name == "juicefsbin" {
					t.Errorf("%s: production code references juicefsbin.SetForTest", fset.Position(n.Pos()))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 100 {
		t.Fatalf("checked only %d files", checked)
	}
}
