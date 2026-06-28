package nfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

// newSymlinkTestFS builds a juiceFS backed by a real temp dir standing in for
// the JuiceFS FUSE mount root (a POSIX fs that supports os.Symlink/Readlink/
// Lstat), with NO spool wired — symlinks carry no data and must not route
// through the write spool. Returns the juiceFS and the FUSE root path so a
// test can verify the on-disk symlink directly.
func newSymlinkTestFS(t *testing.T) (*juiceFS, string) {
	t.Helper()
	fuseRoot := t.TempDir()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	handler := NewHandler(store, fuseRoot)
	t.Cleanup(func() {
		handler.StopHandler()
		_ = store.Close()
	})
	return &juiceFS{handler: handler}, fuseRoot
}

// TestSymlinkCreateLstatReadlink covers the JM symlink fix end to end at the
// billy.Filesystem layer the go-nfs fork's onSymlink/onReadLink call into:
//   - Symlink creates a real symlink on FUSE,
//   - a cache-hit Lstat reports os.ModeSymlink (so NFS emits type=NF3LNK and
//     the client then issues READLINK),
//   - Readlink returns the target verbatim,
//   - ReadDir surfaces the entry with ModeSymlink,
//   - a RELATIVE target (e.g. "A", "Versions/Current/Lib") round-trips,
//   - the on-disk FUSE symlink matches.
func TestSymlinkCreateLstatReadlink(t *testing.T) {
	jfs, fuseRoot := newSymlinkTestFS(t)

	// Absolute-looking and relative targets are both stored verbatim.
	cases := []struct {
		name   string
		link   string
		target string
	}{
		{"relative-sibling", "Current", "A"},
		{"relative-nested", "Lib.dylib", "Versions/Current/Lib"},
		{"absolute", "absLink", "/usr/lib/libSystem.B.dylib"},
		{"dot-relative", "dotLink", "./peer"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := jfs.Symlink(tc.target, tc.link); err != nil {
				t.Fatalf("Symlink(%q, %q): %v", tc.target, tc.link, err)
			}

			// Cache-hit Lstat must report ModeSymlink — this is what makes
			// the NFS GETATTR/LOOKUP report type=symlink so the client
			// follows up with READLINK instead of treating it as a file.
			fi, err := jfs.Lstat(tc.link)
			if err != nil {
				t.Fatalf("Lstat(%q): %v", tc.link, err)
			}
			if fi.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("Lstat(%q) mode = %v, want ModeSymlink set", tc.link, fi.Mode())
			}
			if fi.IsDir() {
				t.Fatalf("Lstat(%q) reports IsDir, want symlink", tc.link)
			}

			// Readlink returns the target VERBATIM — never resolved.
			got, err := jfs.Readlink(tc.link)
			if err != nil {
				t.Fatalf("Readlink(%q): %v", tc.link, err)
			}
			if got != tc.target {
				t.Fatalf("Readlink(%q) = %q, want verbatim %q", tc.link, got, tc.target)
			}

			// On-disk FUSE symlink must match (proves we created it on FUSE,
			// not just in the metadata mirror).
			onDisk, err := os.Readlink(filepath.Join(fuseRoot, tc.link))
			if err != nil {
				t.Fatalf("os.Readlink on FUSE %q: %v", tc.link, err)
			}
			if onDisk != tc.target {
				t.Fatalf("on-disk symlink target = %q, want %q", onDisk, tc.target)
			}
		})
	}

	// ReadDir must surface the symlinks with ModeSymlink so a directory
	// enumeration (Finder bundle copy) classifies them correctly.
	infos, err := jfs.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	seen := map[string]os.FileMode{}
	for _, fi := range infos {
		seen[fi.Name()] = fi.Mode()
	}
	for _, tc := range cases {
		mode, ok := seen[tc.link]
		if !ok {
			t.Fatalf("ReadDir missing symlink %q", tc.link)
		}
		if mode&os.ModeSymlink == 0 {
			t.Fatalf("ReadDir %q mode = %v, want ModeSymlink", tc.link, mode)
		}
	}
}

// TestSymlinkAlreadyExists verifies a SYMLINK over an existing path maps to an
// EEXIST-class error (onSymlink surfaces this as NFSStatusExist), not a silent
// success or a generic access error.
func TestSymlinkAlreadyExists(t *testing.T) {
	jfs, _ := newSymlinkTestFS(t)

	if err := jfs.Symlink("target-a", "dup"); err != nil {
		t.Fatalf("first Symlink: %v", err)
	}
	err := jfs.Symlink("target-b", "dup")
	if err == nil {
		t.Fatal("second Symlink over existing path: want error, got nil")
	}
	if !os.IsExist(err) {
		t.Fatalf("second Symlink error = %v, want os.IsExist", err)
	}

	// The original target must be untouched.
	got, err := jfs.Readlink("dup")
	if err != nil {
		t.Fatalf("Readlink after dup: %v", err)
	}
	if got != "target-a" {
		t.Fatalf("Readlink after dup = %q, want unchanged %q", got, "target-a")
	}
}

// TestSymlinkDanglingTarget confirms a symlink whose target does NOT exist is
// still created, Lstat'd as a symlink, and Readlink'd — Lstat must not follow
// the link (which would ENOENT) and the phantom-purge gate must not delete it.
func TestSymlinkDanglingTarget(t *testing.T) {
	jfs, _ := newSymlinkTestFS(t)

	if err := jfs.Symlink("does/not/exist", "dangling"); err != nil {
		t.Fatalf("Symlink dangling: %v", err)
	}

	fi, err := jfs.Lstat("dangling")
	if err != nil {
		t.Fatalf("Lstat dangling: %v", err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("Lstat dangling mode = %v, want ModeSymlink", fi.Mode())
	}

	got, err := jfs.Readlink("dangling")
	if err != nil {
		t.Fatalf("Readlink dangling: %v", err)
	}
	if got != "does/not/exist" {
		t.Fatalf("Readlink dangling = %q, want %q", got, "does/not/exist")
	}
}
