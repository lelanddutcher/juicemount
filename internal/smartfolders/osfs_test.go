//go:build unix

package smartfolders_test

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/sftest"
)

type osFixture struct {
	dir string
	fs  *smartfolders.OSFS
}

func newOSFixture(t *testing.T) sftest.Fixture {
	t.Helper()
	dir := t.TempDir()
	fsys, err := smartfolders.OpenOSFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fsys.Close() })
	return &osFixture{dir: dir, fs: fsys}
}

func (f *osFixture) host(p string) string      { return filepath.Join(f.dir, filepath.FromSlash(p)) }
func (f *osFixture) FS() smartfolders.FS       { return f.fs }
func (f *osFixture) TrashCount(*testing.T) int { return -1 }
func (f *osFixture) Rmdir(p string) error      { return os.Remove(f.host(p)) }

func (f *osFixture) MkdirAll(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(f.host(p), 0o755); err != nil {
		t.Fatal(err)
	}
}

func (f *osFixture) WriteFile(t *testing.T, p string) {
	t.Helper()
	f.MkdirAll(t, filepath.ToSlash(filepath.Dir(p)))
	if err := os.WriteFile(f.host(p), []byte(p), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (f *osFixture) Symlink(t *testing.T, text, p string) {
	t.Helper()
	if err := os.Symlink(text, f.host(p)); err != nil {
		t.Fatal(err)
	}
}

func (f *osFixture) Remove(t *testing.T, p string) {
	t.Helper()
	if err := os.Remove(f.host(p)); err != nil {
		t.Fatal(err)
	}
}

func (f *osFixture) ReadDir(t *testing.T, p string) []string {
	t.Helper()
	ents, err := os.ReadDir(f.host(p))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func TestReconcilerOnOSFS(t *testing.T) {
	sftest.Run(t, newOSFixture)
}

// Through the real kernel: every link resolves with os.Stat to its target.
func TestOSFSLinksResolveThroughKernel(t *testing.T) {
	fx := newOSFixture(t).(*osFixture)
	fx.WriteFile(t, "Proj/deep/er/a.mov")
	rec := &smartfolders.Reconciler{FS: fx.fs, Root: "Recents", Own: &smartfolders.Ownership{}}
	if _, err := rec.Reconcile("Proj", []smartfolders.Link{{Name: "a.mov", Target: "Proj/deep/er/a.mov"}}); err != nil {
		t.Fatal(err)
	}
	through, err := os.Stat(fx.host("Recents/Proj/a.mov"))
	if err != nil {
		t.Fatal(err)
	}
	direct, _ := os.Stat(fx.host("Proj/deep/er/a.mov"))
	if !os.SameFile(through, direct) {
		t.Fatal("link resolves elsewhere")
	}
}

// OSFS never leaves its root, whatever the path says.
func TestOSFSStaysInsideRoot(t *testing.T) {
	outside := t.TempDir()
	fx := newOSFixture(t).(*osFixture)
	if err := os.Symlink(outside, fx.host("out")); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"out/x", "../x", "/x", "a/../../x"} {
		if _, err := fx.fs.Mkdir(p, 0o755); err == nil {
			t.Errorf("Mkdir(%q) succeeded", p)
		}
		if _, err := fx.fs.Symlink("t", p); err == nil {
			t.Errorf("Symlink(%q) succeeded", p)
		}
	}
	if ents, _ := os.ReadDir(outside); len(ents) != 0 {
		t.Fatal("wrote outside the root")
	}
	if _, err := fx.fs.Lstat("out/x"); !errors.Is(err, smartfolders.ErrSymlinkInPath) {
		t.Fatalf("lstat through symlink: %v", err)
	}
	if err := fx.fs.Remove("out"); err != nil {
		t.Fatalf("removing the symlink itself: %v", err)
	}
	fx.MkdirAll(t, "dir")
	if err := fx.fs.Remove("dir"); err == nil {
		t.Fatal("Remove deleted a directory")
	}
}

// chmodCrash dies inside Chmod: the process stops between mkdir and the
// mode fix.
type chmodCrash struct{ smartfolders.FS }

func (c chmodCrash) Chmod(string, uint64, fs.FileMode) error { panic("crash") }

// A umask that strips OwnedDirPerm's bits must not leave the Manager's
// directory unrecognised: its inode is recorded before the mode is fixed,
// and a restart after a crash in between still owns it and fixes the mode.
func TestOSFSUmaskedMkdirRecognisedByInode(t *testing.T) {
	old := syscall.Umask(0o077) // mkdir 0705 lands as 0700
	defer syscall.Umask(old)
	fx := newOSFixture(t).(*osFixture)
	fx.WriteFile(t, "Proj/a.mov")
	var saved []byte
	rec := &smartfolders.Reconciler{FS: chmodCrash{fx.fs}, Root: "Recents", Own: &smartfolders.Ownership{}}
	rec.Persist = func() error { saved, _ = json.Marshal(rec.Own); return nil }
	func() {
		defer func() { _ = recover() }()
		rec.Reconcile("Proj", []smartfolders.Link{{Name: "a.mov", Target: "Proj/a.mov"}})
		t.Fatal("did not reach the crash point")
	}()
	if fi, _ := os.Stat(fx.host("Recents")); fi.Mode().Perm() != 0o700 {
		t.Fatalf("setup: Recents is %v", fi.Mode().Perm())
	}
	own := &smartfolders.Ownership{}
	if err := json.Unmarshal(saved, own); err != nil {
		t.Fatal(err)
	}
	rec2 := &smartfolders.Reconciler{FS: fx.fs, Root: "Recents", Own: own, Persist: func() error { return nil }}
	res, err := rec2.Reconcile("Proj", []smartfolders.Link{{Name: "a.mov", Target: "Proj/a.mov"}})
	if err != nil || !reflect.DeepEqual(res.Created, []string{"a.mov"}) {
		t.Fatalf("restart = %+v, %v", res, err)
	}
	for _, d := range []string{"Recents", "Recents/Proj"} {
		if fi, _ := os.Stat(fx.host(d)); fi.Mode().Perm() != smartfolders.OwnedDirPerm {
			t.Fatalf("%s is %v, want %v", d, fi.Mode().Perm(), smartfolders.OwnedDirPerm)
		}
	}
}

// Chmod works on a handle and refuses a directory that is not the inode
// the Manager recorded.
func TestOSFSChmodChecksIdentity(t *testing.T) {
	fx := newOSFixture(t).(*osFixture)
	info, err := fx.fs.Mkdir("D", 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if err := fx.fs.Chmod("D", info.Ino+1, 0o705); err == nil {
		t.Fatal("chmod of a directory with another inode succeeded")
	}
	if err := fx.fs.Chmod("D", info.Ino, 0o705); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(fx.host("D")); fi.Mode().Perm() != 0o705 {
		t.Fatalf("mode = %v", fi.Mode().Perm())
	}
	if err := os.Symlink("D", fx.host("L")); err != nil {
		t.Fatal(err)
	}
	if err := fx.fs.Chmod("L", info.Ino, 0o700); err == nil {
		t.Fatal("chmod followed a symlink")
	}
}
