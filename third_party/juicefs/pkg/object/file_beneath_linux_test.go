//go:build linux

/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// beneathFixture:
//
//	base/root/            the storage root (file://base/root/)
//	base/root/sub/in.txt
//	base/root/inlink  -> sub            (relative, stays beneath)
//	base/root/abslink -> base/root/sub  (absolute: refused, see below)
//	base/root/updown  -> ../root/sub    (climbs above the root: refused)
//	base/root/esc     -> base/outside   (absolute, leaves)
//	base/root/x       -> other          (first hop of a chain, stays)
//	base/root/other/y -> base/victim    (second hop, absolute, leaves)
//	base/root/other/z -> ../../victim   (second hop, relative, leaves)
//
// RESOLVE_BENEATH refuses every absolute symlink and every ".." above the
// root, even when the path would come back beneath it. Such links are
// still stored and read as links; they are just not followed.
//
//	base/outside/secret.txt
//	base/victim/
func beneathFixture(t *testing.T) (base, root string, store *filestore) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root = filepath.Join(base, "root")
	for _, d := range []string{"root/sub", "root/other", "outside", "victim"} {
		if err := os.MkdirAll(filepath.Join(base, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for p, body := range map[string]string{"root/sub/in.txt": "in", "outside/secret.txt": "secret"} {
		if err := os.WriteFile(filepath.Join(base, p), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{
		"root/inlink":  "sub",
		"root/abslink": filepath.Join(root, "sub"),
		"root/updown":  "../root/sub",
		"root/esc":     filepath.Join(base, "outside"),
		"root/x":       "other",
		"root/other/y": filepath.Join(base, "victim"),
		"root/other/z": "../../victim",
	} {
		if err := os.Symlink(target, filepath.Join(base, link)); err != nil {
			t.Fatal(err)
		}
	}
	s, err := newDisk(root+"/", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	return base, root, s.(*filestore)
}

func isOutsideRoot(err error) bool {
	return err != nil && errors.Is(err, syscall.EXDEV) && strings.Contains(err.Error(), "outside the file storage root")
}

func TestFileBeneathRefusesEscapes(t *testing.T) {
	base, _, s := beneathFixture(t)
	for _, key := range []string{"esc/secret.txt", "x/y/anything", "x/z/anything", "abslink/in.txt", "updown/in.txt", "../outside/secret.txt", "sub/../../outside/secret.txt"} {
		if _, err := s.Get(key, 0, -1); !isOutsideRoot(err) {
			t.Errorf("Get(%q): err = %v, want an outside-root refusal", key, err)
		}
		if _, err := s.Head(key); !isOutsideRoot(err) {
			t.Errorf("Head(%q): err = %v, want an outside-root refusal", key, err)
		}
	}
	for _, key := range []string{"esc/new.txt", "x/y/new.txt", "x/z/new.txt", "x/y/deep/new.txt", "../victim/new.txt"} {
		if err := s.Put(key, bytes.NewReader([]byte("payload"))); !isOutsideRoot(err) {
			t.Errorf("Put(%q): err = %v, want an outside-root refusal", key, err)
		}
		if err := s.PutIfAbsent(key, bytes.NewReader([]byte("payload"))); err == nil {
			t.Errorf("PutIfAbsent(%q): no error", key)
		}
	}
	for _, op := range []func() error{
		func() error { return s.Chmod("esc/secret.txt", 0o777) },
		func() error { return s.Chown("esc/secret.txt", "root", "root") },
		func() error { return s.Delete("esc/secret.txt") },
		func() error { return s.Symlink("/etc", "esc/planted") },
		func() error { return s.Symlink("/etc", "x/y/planted") },
		func() error { return s.Symlink("/etc", "x/z/planted") },
	} {
		if err := op(); err == nil {
			t.Errorf("an operation through an escaping symlink succeeded")
		}
	}
	entries, _ := os.ReadDir(filepath.Join(base, "victim"))
	if len(entries) != 0 {
		t.Errorf("victim folder written: %v", entries)
	}
	if b, err := os.ReadFile(filepath.Join(base, "outside", "secret.txt")); err != nil || string(b) != "secret" {
		t.Errorf("outside file changed or removed: %q %v", b, err)
	}
	if fi, _ := os.Stat(filepath.Join(base, "outside", "secret.txt")); fi.Mode().Perm() != 0o644 {
		t.Errorf("outside file mode changed: %v", fi.Mode())
	}
}

func TestFileBeneathFollowsLinksThatStayInside(t *testing.T) {
	_, root, s := beneathFixture(t)
	for _, key := range []string{"inlink/in.txt", "sub/in.txt", "x/../sub/in.txt"} {
		r, err := s.Get(key, 0, -1)
		if err != nil {
			t.Errorf("Get(%q): %v", key, err)
			continue
		}
		b, _ := io.ReadAll(r)
		r.Close()
		if string(b) != "in" {
			t.Errorf("Get(%q) = %q", key, b)
		}
	}
	if err := s.Put("inlink/new/deeper.txt", bytes.NewReader([]byte("n"))); err != nil {
		t.Fatalf("Put through an in-root link: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "sub", "new", "deeper.txt")); err != nil {
		t.Errorf("Put did not land beneath the link target: %v", err)
	}
	if err := s.Put("fresh/dir/", nil); err != nil {
		t.Errorf("Put of a folder key: %v", err)
	}
	if err := s.PutIfAbsent("fresh/once.txt", bytes.NewReader([]byte("1"))); err != nil {
		t.Errorf("PutIfAbsent: %v", err)
	}
	if err := s.PutIfAbsent("fresh/once.txt", bytes.NewReader([]byte("2"))); !errors.Is(err, os.ErrExist) {
		t.Errorf("second PutIfAbsent: %v, want ErrExist", err)
	}
	// Links can still be stored and read as links, whatever they point at.
	if err := s.Symlink("/etc", "stored/abs"); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	if got, err := s.Readlink("stored/abs"); err != nil || got != "/etc" {
		t.Errorf("Readlink = %q, %v", got, err)
	}
	if got, err := s.Readlink("esc"); err != nil || !strings.HasSuffix(got, "/outside") {
		t.Errorf("Readlink(esc) = %q, %v", got, err)
	}
	if err := s.Delete("esc"); err != nil {
		t.Errorf("Delete of the link itself: %v", err)
	}
	if err := s.Delete("fresh/once.txt"); err != nil {
		t.Errorf("Delete: %v", err)
	}
}

func TestFileBeneathListDoesNotFollowOutside(t *testing.T) {
	_, _, s := beneathFixture(t)
	objs, _, _, err := s.List("", "", "", "/", 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Object{}
	for _, o := range objs {
		got[o.Key()] = o
	}
	if o, ok := got["inlink/"]; !ok || !o.IsDir() {
		t.Errorf("in-root link to a folder not listed as a folder: %v", got)
	}
	if _, ok := got["esc/"]; ok {
		t.Errorf("escaping link listed as a folder: it was followed")
	}
	if o, ok := got["esc"]; !ok || !o.IsSymlink() {
		t.Errorf("escaping link not listed as a broken link: %v", got)
	}
	if _, ok := got["x/"]; !ok {
		t.Errorf("in-root relative link x not listed as a folder: %v", got)
	}
	// Listing the chain's folder through the first hop: y and z lead outside.
	objs, _, _, err = s.List("x/", "", "", "/", 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range objs {
		if o.Key() == "x/y/" || o.Key() == "x/z/" {
			t.Errorf("second hop listed as a folder: it was followed")
		}
	}
}

func TestFileBeneathRefusesSwapBetweenCalls(t *testing.T) {
	base, root, s := beneathFixture(t)
	if err := s.Put("work/a.txt", bytes.NewReader([]byte("a"))); err != nil {
		t.Fatal(err)
	}
	// Swap the folder for a symlink to a folder outside the root.
	if err := os.Rename(filepath.Join(root, "work"), filepath.Join(root, "work.old")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(base, "victim"), filepath.Join(root, "work")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("work/b.txt", bytes.NewReader([]byte("b"))); !isOutsideRoot(err) {
		t.Errorf("Put after the swap: %v, want an outside-root refusal", err)
	}
	if entries, _ := os.ReadDir(filepath.Join(base, "victim")); len(entries) != 0 {
		t.Errorf("victim folder written after the swap: %v", entries)
	}
}

func TestFileBeneathCreatesMissingRoot(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "new", "dest")
	s, _ := newDisk(root+"/", "", "", "")
	if err := s.Put("a/b.txt", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Put into a missing root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "a", "b.txt")); err != nil {
		t.Error(err)
	}
	// A root without a trailing slash is a name prefix in its parent.
	p, _ := newDisk(filepath.Join(base, "new", "de"), "", "", "")
	if err := p.Put("st/a/c.txt", bytes.NewReader([]byte("y"))); err != nil {
		t.Fatalf("Put under a prefix root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "a", "c.txt")); err != nil {
		t.Error(err)
	}
}

func TestFileBeneathFailsClosedWithoutOpenat2(t *testing.T) {
	prev := openat2Func
	openat2Func = func(int, string, *unix.OpenHow) (int, error) { return -1, unix.ENOSYS }
	t.Cleanup(func() { openat2Func = prev })
	_, _, s := beneathFixture(t)
	if _, err := s.Get("sub/in.txt", 0, -1); !errors.Is(err, errNoOpenat2) {
		t.Errorf("Get without openat2: %v, want errNoOpenat2", err)
	}
	if err := s.Put("sub/new.txt", bytes.NewReader([]byte("x"))); !errors.Is(err, errNoOpenat2) {
		t.Errorf("Put without openat2: %v, want errNoOpenat2", err)
	}
	if _, _, _, err := s.List("", "", "", "/", 10, true); !errors.Is(err, errNoOpenat2) {
		t.Errorf("List without openat2: %v, want errNoOpenat2", err)
	}
}
