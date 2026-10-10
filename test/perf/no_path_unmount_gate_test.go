package perf

// H-3 L6 grep gate: JuiceMount never unmounts by pathname.
//
// A pathname cannot prove which kernel mount it reaches. A replaced or
// foreign mount at the same path, and every mount nested under it, would
// be detached by `umount -f <path>`, `diskutil unmount force <path>`, or
// unmount(2). The only permitted detach primitives are the custody
// funmount(fd) calls (the open descriptor binds the call to one mount
// instance) and, once it ships, the privileged JuiceMount unmount helper
// (fsid-targeted, internal/unmountd). Privilege escalation through sudo or
// a sudoers rule is how the deleted path unmount used to run, so `sudo` is
// gated too.
//
// The gate scans production code only:
//   - Go: every non-test .go file outside test/ and third_party/. Only
//     string literals, cgo preambles, and syscall-level calls are
//     inspected; ordinary comments cannot execute or reach a user.
//   - Swift/C/ObjC under app/*/Sources and scripts/: code and string
//     text with // and /* */ comments removed.
//   - Shell/Python/plist under scripts/, server/, and the repo root:
//     every line that is not a whole-line # comment.
// Each remaining hit must match an allowlist entry below, and every
// allowlist entry must still match a hit (no stale entries).
//
// Split literals are folded before matching: Go `"u" + "mount"` chains are
// constant-folded from the AST, and in Swift/scripts any `" + "` or
// `' + '` joint between two literals on one line is removed.
//
// Known evasions this text gate does NOT catch (deliberately; they need a
// real parser per language and are not worth it while every production
// detach goes through custody or the helper review):
//   - aliased imports or wrappers (`import sc "syscall"; sc.Unmount(p, 0)`,
//     a helper func that returns "umount"), and values built at runtime
//     (string(bytes), fmt.Sprintf("%smount", "u"), env vars, files);
//   - Swift Process arguments or concatenations split across lines
//     (executableURL on one line, ["-f", p] on another), and string
//     interpolation that assembles the word;
//   - shell indirection (`cmd=um; ${cmd}ount`, eval, base64) and
//     concatenation spanning lines.
// Code review and the custody/helper architecture remain the primary
// control; this gate catches the direct, copy-pasted shapes.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// gatePatterns are the forbidden shapes. Each is applied per line of the
// extracted production text.
var gatePatterns = []struct {
	name string
	re   *regexp.Regexp
}{
	{"umount", regexp.MustCompile(`\bumount`)},
	{"diskutil unmount/eject", regexp.MustCompile(`(?i)\bdiskutil\W+(unmount|unmountdisk|eject)\b`)},
	{"sudo", regexp.MustCompile(`(?i)\bsudo`)},
	{"unmount( call", regexp.MustCompile(`\b(f?unmount|unmount2)\s*\(`)},
	{"path unmount API", regexp.MustCompile(`\b(unmountVolume|unmountAndEjectDevice|DADiskUnmount|DADiskEject)\b`)},
	{"hdiutil detach/eject", regexp.MustCompile(`(?i)\bhdiutil\W+(detach|eject|unmount)\b`)},
	{"AppleScript eject", regexp.MustCompile(`(?i)\bto\s+eject\b|\beject\s+(disk|every\s+disk|alias|item|POSIX\s+file)\b`)},
}

// gateLiteralJoint removes the joint between two adjacent quoted literals,
// so "u" + "mount" reads as "umount".
var gateLiteralJoint = regexp.MustCompile(`"\s*\+\s*"|'\s*\+\s*'`)

// gateAllow is the complete allowlist. path is repo-relative; line, when
// set, must match the offending source line. pending marks debt in files
// this lane does not own: the named lane must remove the hit and the entry.
type gateAllow struct {
	path    string
	line    *regexp.Regexp
	pattern string
	why     string
	pending string
}

var gateAllowlist = []gateAllow{
	{
		path: "internal/nfscustody/native_darwin.go", pattern: "unmount( call",
		line: regexp.MustCompile(`funmount\(fd, 0\)`),
		why:  "custody funmount(fd): the descriptor was opened on the owned NFS mount, so the detach is bound to that one kernel mount instance, not to a path",
	},
	{
		path: "internal/fusecustody/native_darwin.go", pattern: "unmount( call",
		line: regexp.MustCompile(`funmount\(fd, 0\)`),
		why:  "custody funmount(fd) for the owned FUSE mount; same descriptor-bound identity as the NFS custodian",
	},
	{
		path: "internal/manager/farm_enrollment.go", pattern: "sudo",
		line: regexp.MustCompile(`"sudo", "(install|tailscale)"|sudo tee /etc/systemd/system/|sudo systemctl (daemon-reload|enable --now)`),
		why:  "text of Linux render-node enrollment commands that an administrator runs on the farm host; it creates directories and a systemd unit and never touches a Mac mount",
	},
	{
		path: "server/juicefarm/entrypoint.sh", pattern: "umount",
		line: regexp.MustCompile(`juicefs umount "\$MNT"`),
		why:  "Linux container entrypoint unmounting the juicefs mount it created itself at $MNT inside its own mount namespace during container shutdown; no Mac mount is reachable",
	},
	{
		path: "scripts/verify-juicefs-sdk.py", pattern: "umount",
		line: regexp.MustCompile(`"cmd/umount\.go"`),
		why:  "a list of vendored juicefs source FILE NAMES the provenance verifier hashes; cmd/umount.go is the patched file whose doUmount always refuses, and nothing here executes an unmount",
	},
	{
		path: "scripts/uninstall.sh", pattern: "sudo",
		line: regexp.MustCompile(`SUDOERS_FILE|/etc/sudoers\.d/juicemount-mount|Legacy sudoers rule`),
		why:  "removes the legacy /etc/sudoers.d/juicemount-mount rule that older versions offered; that rule granted passwordless umount, so removing it is the hardening step",
	},
	{
		path: "cmd/jmctl/main.go", pattern: "umount",
		line:    regexp.MustCompile(`force-eject\s+POST /force-eject`),
		why:     "jmctl usage text still describes /force-eject as a privileged umount; the route already refuses with 409",
		pending: "H-3 L4 (cmd/ owner, design §5 `jmctl retire`); outside lane L6 file ownership",
	},
	{
		path: "app/JuiceMount/Sources/JuiceMount/UI/PreferencesWindowView.swift", pattern: "sudo",
		line:    regexp.MustCompile("scoped sudoers rule is installed"),
		why:     "Preferences help text still mentions the removed sudoers rule",
		pending: "H-3 L3 (app UX); PreferencesWindowView.swift is outside lane L6 file ownership",
	},
}

// isGateTestOrVendored reports files that are not production code. Each
// class is excluded for a stated reason.
func isGateTestOrVendored(rel string) bool {
	switch {
	case strings.HasSuffix(rel, "_test.go"):
		return true // Go tests run only under `go test`.
	case strings.HasPrefix(rel, "test/"):
		return true // test harnesses, probes and fixtures; never built into the app or server.
	case strings.Contains(rel, "/Tests/"):
		return true // Swift package test targets.
	case strings.Contains(rel, "/testdata/"):
		return true
	case strings.HasPrefix(rel, "third_party/"):
		return true // vendored upstream JuiceFS; changes there go through upstream review.
	case strings.HasPrefix(rel, "scripts/wedge-tests/"), strings.HasPrefix(rel, "scripts/qa-suite/"):
		return true // dev-only QA/wedge harnesses run by a developer against dev mounts.
	case strings.HasPrefix(rel, "scripts/test-"), rel == "scripts/netshape-dev.sh":
		return true // dev-only acceptance/network-shaping harnesses (pfctl/dnctl, disposable fixture mounts).
	}
	return false
}

type gateLine struct {
	line int
	text string
}

func gateRepoFiles(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "ls-files", "--cached", "--others", "--exclude-standard")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	var files []string
	for _, f := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if f != "" && !strings.HasPrefix(f, ".claude/") {
			files = append(files, f)
		}
	}
	if len(files) < 100 {
		// Validity gate: refuse to pass on a scan that saw nothing.
		t.Fatalf("gate saw only %d repo files; refusing to report", len(files))
	}
	return files
}

// gateGoText returns the inspectable text of one Go file: string literals,
// syscall-level unmount calls, and cgo preambles. Ordinary comments are
// skipped.
func gateGoText(t *testing.T, path string, src []byte) []gateLine {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var lines []gateLine
	add := func(pos token.Pos, text string) {
		lines = append(lines, gateLine{fset.Position(pos).Line, text})
	}
	// cgo preamble: the doc comment of `import "C"` is compiled C.
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.IMPORT {
			continue
		}
		for _, spec := range gd.Specs {
			is := spec.(*ast.ImportSpec)
			if is.Path.Value != `"C"` {
				continue
			}
			doc := is.Doc
			if doc == nil {
				doc = gd.Doc
			}
			if doc == nil {
				continue
			}
			for _, c := range doc.List {
				base := fset.Position(c.Pos()).Line
				for i, l := range strings.Split(c.Text, "\n") {
					lines = append(lines, gateLine{base + i, l})
				}
			}
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if x.Kind == token.STRING {
				if v, err := strconv.Unquote(x.Value); err == nil {
					for i, l := range strings.Split(v, "\n") {
						lines = append(lines, gateLine{fset.Position(x.Pos()).Line + i, l})
					}
				}
			}
		case *ast.BinaryExpr:
			// Constant-fold "u" + "mount" chains of string literals.
			if v, ok := gateFoldConcat(x); ok {
				add(x.Pos(), v)
			}
		case *ast.CallExpr:
			// exec.Command("diskutil", "unmount", p): join literal args so
			// split argv still reads as one command line.
			var parts []string
			for _, a := range x.Args {
				if bl, ok := a.(*ast.BasicLit); ok && bl.Kind == token.STRING {
					if v, err := strconv.Unquote(bl.Value); err == nil {
						parts = append(parts, v)
					}
				}
			}
			if len(parts) > 1 {
				add(x.Pos(), strings.Join(parts, " "))
			}
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok {
					name := sel.Sel.Name
					switch {
					case (id.Name == "syscall" || id.Name == "unix") && (name == "Unmount" || name == "Unmount2"):
						add(x.Pos(), id.Name+".unmount(")
					case id.Name == "C" && strings.Contains(strings.ToLower(name), "unmount"):
						add(x.Pos(), "C."+name+"(")
					}
				}
			}
		case *ast.SelectorExpr:
			if gateSysUnmount.MatchString(x.Sel.Name) {
				add(x.Pos(), "unmount( via "+x.Sel.Name)
			}
		}
		return true
	})
	return lines
}

// gateFoldConcat folds a + chain whose leaves are all string literals.
func gateFoldConcat(e ast.Expr) (string, bool) {
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind != token.STRING {
			return "", false
		}
		v, err := strconv.Unquote(x.Value)
		return v, err == nil
	case *ast.ParenExpr:
		return gateFoldConcat(x.X)
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return "", false
		}
		l, ok1 := gateFoldConcat(x.X)
		r, ok2 := gateFoldConcat(x.Y)
		return l + r, ok1 && ok2
	}
	return "", false
}

var (
	gateBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	gateSysUnmount   = regexp.MustCompile(`^SYS_F?UNMOUNT$`)
)

// gateCLikeText strips // and /* */ comments from Swift/C/ObjC source and
// keeps everything else (code and string literals).
func gateCLikeText(src string) []gateLine {
	src = gateBlockComment.ReplaceAllStringFunc(src, func(m string) string {
		return strings.Repeat("\n", strings.Count(m, "\n"))
	})
	var out []gateLine
	for i, l := range strings.Split(src, "\n") {
		inStr, esc := false, false
		cut := len(l)
		for j := 0; j < len(l); j++ {
			c := l[j]
			if esc {
				esc = false
				continue
			}
			if c == '\\' && inStr {
				esc = true
				continue
			}
			if c == '"' {
				inStr = !inStr
				continue
			}
			if !inStr && c == '/' && j+1 < len(l) && l[j+1] == '/' {
				cut = j
				break
			}
		}
		out = append(out, gateLine{i + 1, l[:cut]})
	}
	return out
}

// gateScriptText keeps every line that is not a whole-line # comment.
func gateScriptText(src string) []gateLine {
	var out []gateLine
	for i, l := range strings.Split(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}
		out = append(out, gateLine{i + 1, l})
	}
	return out
}

type gateHit struct {
	path, pattern, text string
	line                int
}

// gateScanFile returns the hits in one file and whether the file was
// actually inspected as production code of a scanned language.
func gateScanFile(t *testing.T, rel string, src []byte) ([]gateHit, bool) {
	t.Helper()
	ext := filepath.Ext(rel)
	var lines []gateLine
	switch {
	case ext == ".go":
		lines = gateGoText(t, rel, src)
	case ext == ".swift" || ext == ".c" || ext == ".h" || ext == ".m" || ext == ".mm":
		if !strings.Contains(rel, "/Sources/") && !strings.HasPrefix(rel, "scripts/") {
			return nil, false
		}
		lines = gateCLikeText(string(src))
	case ext == ".sh" || ext == ".py" || ext == ".bash" || ext == ".zsh" || ext == ".command" || ext == ".plist":
		if !strings.HasPrefix(rel, "scripts/") && !strings.HasPrefix(rel, "server/") && strings.Contains(rel, "/") {
			return nil, false
		}
		lines = gateScriptText(string(src))
	default:
		return nil, false
	}
	var hits []gateHit
	for _, l := range lines {
		joined := gateLiteralJoint.ReplaceAllString(l.text, "")
		for _, p := range gatePatterns {
			if p.re.MatchString(l.text) || p.re.MatchString(joined) {
				hits = append(hits, gateHit{rel, p.name, strings.TrimSpace(l.text), l.line})
			}
		}
	}
	return hits, true
}

func gateSourceLine(root, rel string, line int) string {
	b, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		return ""
	}
	ls := strings.Split(string(b), "\n")
	if line < 1 || line > len(ls) {
		return ""
	}
	return ls[line-1]
}

func TestNoPathBasedUnmountInProduction(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	used := make([]int, len(gateAllowlist))
	scanned := 0
	var violations []string
	for _, rel := range gateRepoFiles(t, root) {
		if isGateTestOrVendored(rel) {
			continue
		}
		src, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			continue // deleted in the working tree but still in the index
		}
		hits, inspected := gateScanFile(t, rel, src)
		if inspected {
			scanned++
		}
	hit:
		for _, h := range hits {
			srcLine := gateSourceLine(root, rel, h.line)
			for i, a := range gateAllowlist {
				if a.path != h.path || a.pattern != h.pattern {
					continue
				}
				if a.line == nil || a.line.MatchString(srcLine) || a.line.MatchString(h.text) {
					used[i]++
					continue hit
				}
			}
			violations = append(violations, rel+":"+strconv.Itoa(h.line)+": ["+h.pattern+"] "+h.text)
		}
	}
	if scanned < 50 {
		t.Fatalf("gate inspected only %d production files; refusing to report", scanned)
	}
	for _, v := range violations {
		t.Errorf("path-based unmount / privilege escalation in production code: %s", v)
	}
	for i, a := range gateAllowlist {
		if used[i] == 0 {
			t.Errorf("stale allowlist entry (no longer matches anything; delete it): %s [%s]", a.path, a.pattern)
		}
		if a.pending != "" && used[i] > 0 {
			t.Logf("PENDING (allowlisted debt): %s [%s] x%d — %s", a.path, a.pattern, used[i], a.pending)
		}
	}
	t.Logf("inspected %d production files", scanned)
}

// TestNoPathBasedUnmountGateDetects proves each detector fires on the
// shapes it exists to catch, so a passing gate is not a blind gate.
func TestNoPathBasedUnmountGateDetects(t *testing.T) {
	goSrc := []byte(`package x

/*
static int f(const char *p) { return unmount(p, 0); }
*/
import "C"

import (
	"os/exec"
	"syscall"
)

func a(p string) {
	_ = exec.Command("sudo", "umount", "-f", p).Run()
	_ = exec.Command("/usr/sbin/diskutil", "unmount", "force", p).Run()
	_ = syscall.Unmount(p, 0)
	// umount in a comment is ignored
}

func b(p string) {
	_ = exec.Command("/sbin/" + "u" + "mount", p).Run()
}
`)
	got := map[string]bool{}
	goHits, _ := gateScanFile(t, "pkg/x.go", goSrc)
	for _, h := range goHits {
		got[h.pattern] = true
	}
	for _, want := range []string{"sudo", "umount", "diskutil unmount/eject", "unmount( call"} {
		if !got[want] {
			t.Errorf("Go detector missed %q (got %v)", want, got)
		}
	}
	for _, h := range goHits {
		if strings.Contains(h.text, "in a comment") {
			t.Errorf("Go detector flagged an ordinary comment: %+v", h)
		}
	}

	swiftSrc := []byte(`let s = "do shell script \"umount -f /Volumes/x\" with administrator privileges"
let u = URL(string: "http://127.0.0.1/health") // sudo in a comment is ignored
p.executableURL = URL(fileURLWithPath: "/sbin/u" + "mount")
try FileManager.default.unmountVolume(at: url)
`)
	sw, _ := gateScanFile(t, "app/X/Sources/X/A.swift", swiftSrc)
	swGot := map[string]bool{}
	for _, h := range sw {
		swGot[h.pattern] = true
		if strings.Contains(h.text, "in a comment") {
			t.Errorf("Swift detector flagged a comment: %+v", h)
		}
	}
	for _, want := range []string{"umount", "path unmount API"} {
		if !swGot[want] {
			t.Errorf("Swift detector missed %q (got %v)", want, swGot)
		}
	}

	// Split literals alone, with no whole forbidden word anywhere.
	for _, tc := range []struct{ rel, src string }{
		{"pkg/y.go", "package y\n\nimport \"os/exec\"\n\nfunc c(p string) { _ = exec.Command(\"/sbin/\" + \"u\" + \"mount\", p).Run() }\n"},
		{"app/X/Sources/X/B.swift", "p.executableURL = URL(fileURLWithPath: \"/sbin/u\" + \"mount\")\n"},
		{"scripts/y.sh", "cmd='su'+'do'\n"},
	} {
		hits, inspected := gateScanFile(t, tc.rel, []byte(tc.src))
		if !inspected || len(hits) == 0 {
			t.Errorf("concatenation detector missed %s: inspected=%v hits=%v", tc.rel, inspected, hits)
		}
	}
	if _, inspected := gateScanFile(t, "docs/x.md", []byte("sudo umount")); inspected {
		t.Error("non-production file counted as inspected")
	}

	sh, _ := gateScanFile(t, "scripts/x.sh", []byte("# sudo umount in a comment\ndiskutil unmount force \"$mp\"\nsudo -n true\nhdiutil detach -force \"$mp\"\nosascript -e 'tell application \"Finder\" to eject disk \"zpool\"'\n"))
	shGot := map[string]bool{}
	for _, h := range sh {
		shGot[h.pattern] = true
		if h.line == 1 {
			t.Errorf("script detector flagged a comment line: %+v", h)
		}
	}
	for _, want := range []string{"diskutil unmount/eject", "sudo", "hdiutil detach/eject", "AppleScript eject"} {
		if !shGot[want] {
			t.Errorf("script detector missed %q (got %v)", want, shGot)
		}
	}
}
