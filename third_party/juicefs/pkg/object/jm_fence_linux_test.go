//go:build linux

/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// The project fence (patch 0017) on the file:// backend: root/A and root/B
// are projects (depth 1).
//
//	root/A/sub/in.txt
//	root/A/inlink -> sub          (same project: followed)
//	root/A/toB    -> ../B         (into project B: refused)
//	root/A/updown -> ../A/sub     (climbs above A: refused)
//	root/B/b.txt
//	root/Alink    -> A            (a project-level symlink: refused)
func TestJMProjectFenceFile(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "root")
	for _, d := range []string{"A/sub", "B"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for p, body := range map[string]string{"A/sub/in.txt": "in", "B/b.txt": "b"} {
		if err := os.WriteFile(filepath.Join(root, p), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{"A/inlink": "sub", "A/toB": "../B", "A/updown": "../A/sub", "Alink": "A"} {
		if err := os.Symlink(target, filepath.Join(root, link)); err != nil {
			t.Fatal(err)
		}
	}
	s, err := newDisk(root+"/", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	store := s.(*filestore)
	// Without the fence, a link that stays beneath the root is followed.
	if err := store.Put("A/toB/before.txt", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("unfenced write: %v", err)
	}
	os.Remove(filepath.Join(root, "B/before.txt"))
	if err := store.JMSetProjectDepth(1); err != nil {
		t.Fatal(err)
	}
	outside := func(err error) bool {
		return err != nil && errors.Is(err, syscall.EXDEV) && errors.Is(err, ErrOutsideProject)
	}
	for _, key := range []string{"A/toB/p.txt", "A/updown/p.txt", "Alink/p.txt", "Alink/"} {
		if err := store.Put(key, bytes.NewReader([]byte("x"))); !outside(err) {
			t.Errorf("Put %s: %v, want a project refusal", key, err)
		}
	}
	if err := store.Chmod("A/toB/../B/b.txt", 0o600); err == nil {
		t.Errorf("chmod through a key with .. was not refused")
	}
	if err := store.Symlink("x", "A/inlink/new"); !outside(err) {
		t.Errorf("symlink under a linked folder: %v, want a project refusal", err)
	}
	if got, _ := os.ReadDir(filepath.Join(root, "B")); len(got) != 1 {
		t.Errorf("project B was written: %v", got)
	}
	// Same-project links keep working.
	if err := store.Put("A/inlink/ok.txt", bytes.NewReader([]byte("ok"))); err != nil {
		t.Errorf("write through a same-project link: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(root, "A/sub/ok.txt")); err != nil || string(b) != "ok" {
		t.Errorf("same-project write landed wrong: %q %v", b, err)
	}
	if err := store.Symlink("sub/in.txt", "A/rel"); err != nil {
		t.Errorf("store a link in a real folder: %v", err)
	}
	if err := store.Put("C/new/file.txt", bytes.NewReader([]byte("c"))); err != nil {
		t.Errorf("write into a new project: %v", err)
	}
	// Reading a project-level link itself still works.
	if got, err := store.Readlink("Alink"); err != nil || got != "A" {
		t.Errorf("readlink of a project-level link: %q %v", got, err)
	}
}
