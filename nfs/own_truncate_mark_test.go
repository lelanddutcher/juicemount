package nfs

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

// Review 99e M2: the Mac's own truncate marks its own bytes, with the
// interval of that truncate, at each entry point: an O_TRUNC open, a CREATE
// UNCHECKED over an existing file, a SETATTR of the size, and a truncate on
// a handle that WROTE before (whose WRITE interval must not be reused).
func TestOwnTruncateMarksItsOwnBytes(t *testing.T) {
	for _, entry := range []string{"open_trunc", "create_unchecked", "setattr_size", "after_write"} {
		t.Run(entry, func(t *testing.T) {
			jfs, store, fuseRoot := newIntegrityHarness(t)
			const path = "reel.mov"
			seedFile(t, jfs, store, fuseRoot, path, patternedPayload(8192, 113))
			want := int64(0)
			// JuiceFS stamps the mtime inside the truncate on this host's
			// fine clock. The test's local disk stamps with the kernel's
			// coarse clock, up to a tick earlier, so the test samples the
			// clock inside the operation instead: just after the syscall.
			var inside time.Time
			jfs.handler.testAfterInPlaceMutation = func(_ string, op string) {
				if op == "truncate" {
					inside = time.Now()
				}
			}
			switch entry {
			case "open_trunc":
				f, err := jfs.OpenFile(path, os.O_RDWR|os.O_TRUNC, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			case "create_unchecked":
				createModeOnce(t, jfs, path, nfslib.CreateHow{Mode: nfslib.CreateUnchecked}, false)
			case "setattr_size":
				size := uint64(4096)
				want = 4096
				attrs := &nfslib.SetFileAttributes{SetSize: &size}
				if err := attrs.Apply(jfs.handler.Change(jfs), jfs, path); err != nil {
					t.Fatal(err)
				}
			case "after_write":
				f, err := jfs.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.(io.WriterAt).WriteAt(patternedPayload(4096, 7), 0); err != nil {
					t.Fatal(err)
				}
				// The WRITE's interval ends before the truncate starts.
				time.Sleep(20 * time.Millisecond)
				want = 2048
				if err := f.Truncate(want); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			fi, err := os.Stat(filepath.Join(fuseRoot, path))
			if err != nil {
				t.Fatal(err)
			}
			if fi.Size() != want {
				t.Fatalf("backing size %d, want %d", fi.Size(), want)
			}
			if inside.IsZero() {
				t.Fatal("the truncate did not run")
			}
			if !store.OwnBytesMarked(path, want, inside) {
				t.Errorf("the truncate's own bytes (size %d, a stamp at %d inside it) are not marked: the push would move them", want, inside.UnixNano())
			}
		})
	}
}
