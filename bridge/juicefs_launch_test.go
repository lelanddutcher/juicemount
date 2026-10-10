package main

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/juicefsbin"
)

const testBundledJuiceFS = "/Applications/JuiceMount.app/Contents/MacOS/juicefs"

func TestEngineLaunchJuiceFSRefusesUnbundled(t *testing.T) {
	defer juicefsbin.SetForTest(testBundledJuiceFS)()
	var refused *juicefsbin.NotBundledError
	if _, err := engineLaunchJuiceFS(false, "", "/opt/homebrew/bin/juicefs"); !errors.As(err, &refused) {
		t.Fatalf("engine start admitted an unbundled juicefs: %v", err)
	}
	for _, configured := range []string{"", testBundledJuiceFS} {
		if got, err := engineLaunchJuiceFS(false, "", configured); err != nil || got != testBundledJuiceFS {
			t.Fatalf("engine start with %q = %q, %v", configured, got, err)
		}
	}
	// A retained manager keeps the executable it was started with.
	if got, err := engineLaunchJuiceFS(true, testBundledJuiceFS, "/opt/homebrew/bin/juicefs"); err != nil || got != testBundledJuiceFS {
		t.Fatalf("retained manager = %q, %v", got, err)
	}
}

func TestOwnerLaunchJuiceFSRefusesUnbundled(t *testing.T) {
	defer juicefsbin.SetForTest(testBundledJuiceFS)()
	var refused *juicefsbin.NotBundledError
	for _, path := range []string{"/opt/homebrew/bin/juicefs", "/usr/local/bin/juicefs", "juicefs"} {
		if _, err := ownerLaunchJuiceFS(path); !errors.As(err, &refused) {
			t.Fatalf("owner claim admitted %s: %v", path, err)
		}
	}
	if got, err := ownerLaunchJuiceFS(""); err != nil || got != testBundledJuiceFS {
		t.Fatalf("owner claim default = %q, %v", got, err)
	}
}

// The helpers only protect the launch paths that call them. Each launch path
// must take cfg.JuiceFSBin from its helper and hand exactly that to the FUSE
// manager; any other write to cfg.JuiceFSBin is a bypass.
func TestLaunchPathsUseTheBundledJuiceFSCheck(t *testing.T) {
	for _, tc := range []struct {
		file, fn, helper string
		allowed          map[string]bool // permitted right-hand sides of cfg.JuiceFSBin =
	}{
		{"cbridge.go", "nfsServerStartSerialized", "engineLaunchJuiceFS", map[string]bool{"bin": true, "ownerReceipt.JuiceFSBin": true}},
		{"volume_owner.go", "claim", "ownerLaunchJuiceFS", map[string]bool{"bin": true}},
	} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, tc.file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var body *ast.BlockStmt
		for _, d := range f.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == tc.fn {
				body = fn.Body
			}
		}
		if body == nil {
			t.Fatalf("%s: func %s not found", tc.file, tc.fn)
		}
		calls, binAssign := 0, 0
		ast.Inspect(body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CallExpr:
				if id, ok := n.Fun.(*ast.Ident); ok && id.Name == tc.helper {
					calls++
				}
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					if src(fset, lhs) != "cfg.JuiceFSBin" || i >= len(n.Rhs) {
						continue
					}
					rhs := src(fset, n.Rhs[i])
					if !tc.allowed[rhs] {
						t.Errorf("%s:%s assigns cfg.JuiceFSBin = %s outside %s", tc.file, tc.fn, rhs, tc.helper)
					}
					if rhs == "bin" {
						binAssign++
					}
				}
			}
			return true
		})
		if calls != 1 || binAssign != 1 {
			t.Errorf("%s:%s calls %s %d times and assigns its result %d times; want 1 and 1", tc.file, tc.fn, tc.helper, calls, binAssign)
		}
	}
}

func src(fset *token.FileSet, n ast.Node) string {
	var b strings.Builder
	_ = printer.Fprint(&b, fset, n)
	return b.String()
}
