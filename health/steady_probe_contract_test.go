package health

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// TestSteadyFUSEHealthDoesNotTouchFUSEPaths is a release contract: periodic
// health may inspect a complete mount snapshot and the retained child, but it must not
// stat/read/readdir the FUSE path. Each of those operations can become a Redis
// round trip and used to generate permanent cellular traffic while idle.
func TestSteadyFUSEHealthDoesNotTouchFUSEPaths(t *testing.T) {
	tests := []struct {
		file string
		fn   string
	}{
		{"fuse.go", "isMountedSteadyLocked"},
		{"monitor.go", "checkFUSE"},
		{"monitor.go", "checkFUSEWith"},
	}
	for _, tt := range tests {
		t.Run(tt.fn, func(t *testing.T) {
			calls := functionCalls(t, tt.file, tt.fn)
			for _, forbidden := range []string{"os.Stat", "os.Lstat", "os.ReadDir", "os.ReadFile"} {
				if calls[forbidden] {
					t.Fatalf("periodic FUSE health calls %s; this can touch Redis over cellular", forbidden)
				}
			}
			if tt.fn == "isMountedSteadyLocked" && (!calls["fm.snapshotFUSE"] || !calls["waitFUSEChannel"]) {
				t.Fatal("steady FUSE health must join the exact child waiter and complete mount snapshot")
			}
			if tt.fn == "isMountedSteadyLocked" && calls["isJuiceFSProcessAliveFn"] {
				t.Fatal("steady FUSE health reverted to a truncated ps command-title probe")
			}
			if tt.fn == "checkFUSE" && (!calls["m.checkFUSEWith"] || !calls["productionHealthMountDeps"]) {
				t.Fatal("unowned monitor wrapper bypassed its native observer and process-guard implementation")
			}
			if tt.fn == "checkFUSEWith" && !calls["isJuiceFSProcessAliveFn"] {
				t.Fatal("unowned monitor health lost its existing process observation")
			}
		})
	}
}

func TestHealthMonitorNativeObservationWiring(t *testing.T) {
	for _, tc := range []struct {
		file, fn string
		want     []string
	}{
		{"monitor.go", "checkNFS", []string{"m.checkNFSWithProbe", "mountTableHas"}},
		{"monitor.go", "mountTableHas", []string{"healthMountPresent"}},
		{"monitor.go", "checkFUSE", []string{"m.checkFUSEWith", "productionHealthMountDeps"}},
		{"mount_observation.go", "healthMountPresent", []string{"healthMountPresentWith", "productionHealthMountDeps"}},
		{"mount_observation.go", "productionHealthMountDeps", []string{
			"runtime.GOOS", "os.Executable", "mounttable.SnapshotFromHelper", "mounttable.Output"}},
	} {
		refs := functionReferences(t, tc.file, tc.fn)
		for _, name := range tc.want {
			if !refs[name] {
				t.Errorf("%s.%s lost production native observation reference %s", tc.file, tc.fn, name)
			}
		}
	}
}

func functionReferences(t *testing.T, filename, function string) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	refs := make(map[string]bool)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != function {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.Ident:
				refs[n.Name] = true
			case *ast.SelectorExpr:
				if base, ok := n.X.(*ast.Ident); ok {
					refs[base.Name+"."+n.Sel.Name] = true
				}
			}
			return true
		})
		return refs
	}
	t.Fatalf("function %s not found in %s", function, filename)
	return nil
}

// Launch verification has the same no-backend-I/O contract as periodic health.
// A root ReadDir here used to sit behind a legitimate cellular object read until
// the verification budget expired; Mount then killed the healthy JuiceFS
// service and left Finder on a zombie FUSE session.
func TestMountLaunchVerificationUsesSteadyProbe(t *testing.T) {
	calls := functionCalls(t, "fuse.go", "waitForMountProcess")
	for _, forbidden := range []string{"fm.isMountedLocked", "os.Stat", "os.Lstat", "os.ReadDir", "os.ReadFile"} {
		if calls[forbidden] {
			t.Fatalf("mount launch verification calls %s; this can block on backend I/O during a cellular handoff", forbidden)
		}
	}
	if !calls["fm.isMountedSteadyLocked"] {
		t.Fatal("mount launch verification no longer requires exact mount snapshot plus retained child")
	}
}

func functionCalls(t *testing.T, filename, function string) map[string]bool {
	t.Helper()
	path := filepath.Join(".", filename)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	calls := make(map[string]bool)
	found := false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != function {
			continue
		}
		found = true
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch target := call.Fun.(type) {
			case *ast.Ident:
				calls[target.Name] = true
			case *ast.SelectorExpr:
				if pkg, ok := target.X.(*ast.Ident); ok {
					calls[pkg.Name+"."+target.Sel.Name] = true
				}
			}
			return true
		})
	}
	if !found {
		t.Fatalf("function %s not found in %s", function, filename)
	}
	return calls
}
