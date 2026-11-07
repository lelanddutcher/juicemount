package manager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestPathAllowedResolvesLikeTheKernel checks pathAllowed against where
// opening the path would really land. Layout (base is the real,
// symlink-free temp folder):
//
//	<base>/root/                  permitted root
//	<base>/root/sub/
//	<base>/root/link     ->       <base>/outside/inner
//	<base>/root/inlink   ->       <base>/root/sub
//	<base>/root/dangling ->       <base>/outside/missing
//	<base>/outside/inner/, <base>/outside/secret/
//	<base>/rootlink      ->       <base>/root   (a root reached via a symlink)
func TestPathAllowedResolvesLikeTheKernel(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "root")
	outside := filepath.Join(base, "outside")
	for _, d := range []string{filepath.Join(root, "sub"), filepath.Join(outside, "inner"), filepath.Join(outside, "secret")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{
		filepath.Join(root, "link"):     filepath.Join(outside, "inner"),
		filepath.Join(root, "inlink"):   filepath.Join(root, "sub"),
		filepath.Join(root, "dangling"): filepath.Join(outside, "missing"),
		filepath.Join(base, "rootlink"): root,
	} {
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		name string
		p    string
		want bool
	}{
		{"root itself", root, true},
		{"existing child", root + "/sub", true},
		{"missing child", root + "/new/deeper", true},
		{"symlink inside root", root + "/inlink", true},
		{"missing under in-root symlink", root + "/inlink/new", true},
		{"cleaned text of the escape", root + "/secret", true},
		{"dotdot after escaping symlink (exists)", root + "/link/../secret", false},
		{"dotdot after escaping symlink (missing leaf)", root + "/link/../new", false},
		{"escaping symlink", root + "/link", false},
		{"missing under escaping symlink", root + "/link/new/deeper", false},
		{"dangling symlink", root + "/dangling", false},
		{"under dangling symlink", root + "/dangling/x", false},
		{"dotdot out of a missing folder", root + "/missing/../link/x", false},
		{"textual climb out", root + "/../outside", false},
		{"unrelated host path", "/etc", false},
		{"empty", "", false},
	}
	check := func(label string, roots []string) {
		a := &API{sourceRoots: roots}
		for _, tc := range cases {
			if got := a.pathAllowed(tc.p); got != tc.want {
				t.Errorf("%s: %s: pathAllowed(%q) = %v, want %v", label, tc.name, tc.p, got, tc.want)
			}
		}
	}
	check("real root", []string{root})
	check("symlinked root", []string{filepath.Join(base, "rootlink")})

	// The same paths spelled through the symlinked root, as a macOS
	// /var -> /private/var temp folder would produce.
	a := &API{sourceRoots: []string{root}}
	via := filepath.Join(base, "rootlink")
	for p, want := range map[string]bool{
		via + "/new/deeper":     true,
		via + "/sub":            true,
		via + "/link/../secret": false,
		via + "/link/new":       false,
	} {
		if got := a.pathAllowed(p); got != want {
			t.Errorf("via symlinked root: pathAllowed(%q) = %v, want %v", p, got, want)
		}
	}
}

// TestBrowseRefusesDotDotAfterEscapingSymlink: handleBrowse reads the
// raw ?path= text, so its check must judge that text the way the
// kernel resolves it.
func TestBrowseRefusesDotDotAfterEscapingSymlink(t *testing.T) {
	f := newScheduleSourceFixture(t)
	w := destRequest(t, f.mux, "GET", "/api/browse?path="+f.root+"/link/../secret", "")
	if w.Code != 403 {
		t.Errorf("browse %s/link/../secret: %d, want 403 (%s)", f.root, w.Code, w.Body.String())
	}
	w = destRequest(t, f.mux, "GET", "/api/browse?path="+f.root, "")
	if w.Code != 200 {
		t.Errorf("browse root: %d, want 200 (%s)", w.Code, w.Body.String())
	}
}

// TestPathAllowedRefusesOverlongPathQuickly: the resolver walks back one
// component at a time, so an input past PATH_MAX (the kernel refuses it
// with ENAMETOOLONG, which is not "missing") must be refused up front, not
// walked: 100,000 components took about 45 s before.
func TestPathAllowedRefusesOverlongPathQuickly(t *testing.T) {
	root := t.TempDir()
	a := &API{sourceRoots: []string{root}, destMount: "/jfs"}
	long := root + "/x" + strings.Repeat("/a", 100000)
	start := time.Now()
	if a.pathAllowed(long) {
		t.Fatal("an overlong path was allowed")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("refusing a %d-byte path took %v", len(long), d)
	}
	// A long but legal missing path under the root is still allowed (kept
	// under macOS's 1024-byte PATH_MAX, since CI also runs there).
	ok := root + "/x" + strings.Repeat("/a", (900-len(root))/2)
	if !a.pathAllowed(ok) {
		t.Fatalf("a %d-byte missing path under the root was refused", len(ok))
	}
}

// TestResolveFileDestPathRefusesOverlongPathQuickly: the file-destination
// resolver walks back the same way, so it has the same bound.
func TestResolveFileDestPathRefusesOverlongPathQuickly(t *testing.T) {
	long := t.TempDir() + "/x" + strings.Repeat("/a", 100000)
	start := time.Now()
	if _, err := resolveFileDestPath(long); err == nil {
		t.Fatal("an overlong file destination path resolved")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("refusing a %d-byte path took %v", len(long), d)
	}
}
