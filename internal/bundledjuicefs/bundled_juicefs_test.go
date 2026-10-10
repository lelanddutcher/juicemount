// Package bundledjuicefs holds the gates for the JuiceFS executable that
// scripts/build-app.sh builds from third_party/juicefs and ships in the app.
// That binary's signal handlers and exit paths must never unmount by path:
// the kernel mount belongs to JuiceMount, which removes it by exact identity.
//
// The upstream cmd tree cannot be compiled from the root module, so the gates
// on it read source. The signal policy itself is stdlib-only and is exercised
// with real signals.
package bundledjuicefs

import (
	"encoding/json"
	"go/ast"
	"go/build"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const sdkRoot = "../../third_party/juicefs"

func parseFile(t *testing.T, name string) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(sdkRoot, name), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return fset, f
}

func funcDecl(t *testing.T, f *ast.File, name string) *ast.FuncDecl {
	t.Helper()
	for _, decl := range f.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return fn
		}
	}
	t.Fatalf("func %s not found", name)
	return nil
}

func callName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		if x, ok := fun.X.(*ast.Ident); ok {
			return x.Name + "." + fun.Sel.Name
		}
		return "." + fun.Sel.Name
	}
	return ""
}

// The patched mount process signal handler hands every signal to the owner
// policy and calls nothing that unmounts or hands the FUSE session over.
func TestInstallHandlerDelegatesToOwnerPolicy(t *testing.T) {
	fset, f := parseFile(t, "cmd/mount_unix.go")
	fn := funcDecl(t, f, "installHandler")
	allowed := map[string]bool{
		"signal.Ignore": true, "make": true, "signal.Notify": true,
		"jmsignal.Handle": true, "v.FlushAll": true, "object.Shutdown": true,
	}
	delegated := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if g, ok := n.(*ast.GoStmt); ok && callName(g.Call) == "jmsignal.Handle" {
			delegated = true
		}
		if call, ok := n.(*ast.CallExpr); ok && !allowed[callName(call)] {
			t.Errorf("%s: installHandler calls %s", fset.Position(call.Pos()), callName(call))
		}
		return true
	})
	if !delegated {
		t.Error("installHandler does not delegate signals to jmsignal.Handle")
	}
}

// The policy is only as good as the effects it is given. The real wiring must
// stop FUSE with go-fuse's Shutdown, flush every buffered write, close the
// real session and exit the process; a no-op in any slot fails here.
func TestInstallHandlerWiresRealEffects(t *testing.T) {
	fset, f := parseFile(t, "cmd/mount_unix.go")
	fn := funcDecl(t, f, "installHandler")
	want := map[string]string{
		"Stop":         "fuse.Shutdown",
		"Flush":        `func() error { return v.FlushAll("") }`,
		"CloseSession": "m.CloseSession",
		"Shutdown":     "func() { object.Shutdown(blob) }",
		"Exit":         "os.Exit",
		"Logf":         "logger.Warnf",
	}
	got := map[string]string{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if sel, ok := lit.Type.(*ast.SelectorExpr); !ok || sel.Sel.Name != "Actions" {
			return true
		}
		for _, elt := range lit.Elts {
			kv := elt.(*ast.KeyValueExpr)
			var b strings.Builder
			if err := printer.Fprint(&b, fset, kv.Value); err != nil {
				t.Fatal(err)
			}
			got[kv.Key.(*ast.Ident).Name] = strings.Join(strings.Fields(b.String()), " ")
		}
		return false
	})
	for field, expr := range want {
		if got[field] != expr {
			t.Errorf("jmsignal.Actions.%s = %q, want %q", field, got[field], expr)
		}
	}
	for field := range got {
		if _, ok := want[field]; !ok {
			t.Errorf("unexpected jmsignal.Actions.%s = %q (Deadline/After are test seams)", field, got[field])
		}
	}
}

// doUmount is reached by every upstream unmount caller; it must refuse.
func TestDoUmountRefuses(t *testing.T) {
	fset, f := parseFile(t, "cmd/umount.go")
	fn := funcDecl(t, f, "doUmount")
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && callName(call) != "fmt.Errorf" {
			t.Errorf("%s: doUmount calls %s", fset.Position(call.Pos()), callName(call))
		}
		return true
	})
}

// versionQuery reports whether args are exactly the literal "-V", as in
// upstream's `fusermount -V` probe, which unmounts nothing.
func versionQuery(args []ast.Expr) bool {
	if len(args) != 1 {
		return false
	}
	lit, ok := args[0].(*ast.BasicLit)
	return ok && lit.Value == `"-V"`
}

type binaryPin struct {
	Binary struct {
		Tags string `json:"tags"`
	} `json:"binary"`
}

// Grep gate: no file compiled into the bundled darwin/arm64 binary may unmount
// by path, through exec (umount, diskutil, fusermount), any diskutil string, or a call named
// Unmount/Umount (go-fuse's Server.Unmount, syscall.Unmount, unix.Unmount).
func TestNoPathUnmountInBundledJuiceFSTree(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(sdkRoot, "SOURCE.json"))
	if err != nil {
		t.Fatal(err)
	}
	var pin binaryPin
	if err := json.Unmarshal(raw, &pin); err != nil || pin.Binary.Tags == "" {
		t.Fatalf("SOURCE.json binary tags: %v", err)
	}
	ctx := build.Default
	ctx.GOOS, ctx.GOARCH, ctx.CgoEnabled = "darwin", "arm64", true
	ctx.BuildTags = strings.Split(pin.Binary.Tags, ",")
	unmountTools := map[string]bool{"umount": true, "diskutil": true, "fusermount": true, "fusermount3": true}
	scanned := 0
	err = filepath.WalkDir(sdkRoot, func(name string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		if ok, err := ctx.MatchFile(filepath.Dir(name), filepath.Base(name)); err != nil || !ok {
			return err
		}
		scanned++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.SelectorExpr:
				// Calls and method values alike: fssrv.Unmount(),
				// stop := fssrv.Unmount, syscall.Unmount, unix.Unmount.
				if strings.EqualFold(n.Sel.Name, "unmount") || strings.EqualFold(n.Sel.Name, "umount") {
					t.Errorf("%s: path unmount reference .%s", fset.Position(n.Pos()), n.Sel.Name)
				}
			case *ast.Ident:
				// go-fuse's package-level unmount(dir, opts). The `umount`
				// CLI action is allowed: it only reaches doUmount's refusal.
				if strings.EqualFold(n.Name, "unmount") {
					t.Errorf("%s: path unmount identifier %s", fset.Position(n.Pos()), n.Name)
				}
			case *ast.CallExpr:
				callee := callName(n)
				if strings.HasPrefix(callee, "exec.") && len(n.Args) > 0 {
					if lit, ok := n.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if tool, err := strconv.Unquote(lit.Value); err == nil && unmountTools[path.Base(tool)] && !versionQuery(n.Args[1:]) {
							t.Errorf("%s: %s(%s)", fset.Position(n.Pos()), callee, lit.Value)
						}
					}
				}
			case *ast.BasicLit:
				if n.Kind == token.STRING {
					if s, err := strconv.Unquote(n.Value); err == nil && path.Base(s) == "diskutil" {
						t.Errorf("%s: diskutil literal %s", fset.Position(n.Pos()), n.Value)
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 120 {
		t.Fatalf("scanned only %d files; the gate did not cover the bundled tree", scanned)
	}
}

// The source the bundled binary is built from must match SOURCE.json and its
// recorded patches. With the upstream module ZIP in the module cache, the
// patches are also applied to upstream and must reproduce the tree.
func TestSDKSourceMatchesManifest(t *testing.T) {
	python := "/usr/bin/python3"
	if _, err := os.Stat(python); err != nil {
		t.Skip("python3 unavailable")
	}
	args := []string{"-I", "-B", filepath.Join("..", "..", "scripts", "verify-juicefs-sdk.py")}
	modcache := os.Getenv("GOMODCACHE")
	if modcache == "" {
		if home, err := os.UserHomeDir(); err == nil {
			modcache = filepath.Join(home, "go", "pkg", "mod")
		}
	}
	archive := filepath.Join(modcache, "cache", "download", "github.com", "juicedata", "juicefs", "@v", "v1.3.1.zip")
	if _, err := os.Stat(archive); err == nil {
		args = append(args, "--archive", archive)
	} else {
		t.Logf("upstream archive absent (%s); verifying recorded hashes only", archive)
	}
	out, err := exec.Command(python, args...).CombinedOutput()
	if err != nil || !strings.Contains(string(out), `"result": "PASS"`) {
		t.Fatalf("verify-juicefs-sdk.py: %v\n%s", err, out)
	}
}
