package derivatives

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestOpenRegularUnderRefusesAncestorSymlink is the round-3 HIGH: O_NOFOLLOW
// binds only the FINAL component, so guarding just the blob name left the
// per-inode DIRECTORY swappable for a symlink pointing anywhere. The reviewer
// drove this end to end with no forged DB row at all — a real directory long
// enough for the on-miss reconcile to persist a row, then swapped for a link.
func TestOpenRegularUnderRefusesAncestorSymlink(t *testing.T) {
	mount := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "poster.jpg"), []byte("PRIVATE-BYTES"), 0o600); err != nil {
		t.Fatal(err)
	}

	rel := DerivBlobRel(700001, "poster.jpg")
	dir := filepath.Dir(filepath.Join(mount, rel))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mount, rel), []byte("REAL-POSTER"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Sanity: the legitimate arrangement reads normally.
	f, err := OpenRegularUnder(mount, rel)
	if err != nil {
		t.Fatalf("legitimate blob refused: %v", err)
	}
	buf := make([]byte, 32)
	n, _ := f.Read(buf)
	f.Close()
	if string(buf[:n]) != "REAL-POSTER" {
		t.Fatalf("read %q, want REAL-POSTER", buf[:n])
	}

	// THE ATTACK: swap the per-inode DIRECTORY for a symlink out of the tree.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, dir); err != nil {
		t.Fatal(err)
	}
	if f, err := OpenRegularUnder(mount, rel); err == nil {
		got := make([]byte, 32)
		n, _ := f.Read(got)
		f.Close()
		t.Fatalf("ancestor symlink followed — streamed %q from outside the mount", got[:n])
	}
}

// TestOpenRegularUnderRefusesFinalSymlink keeps the original round-1 case
// covered by the new primitive.
func TestOpenRegularUnderRefusesFinalSymlink(t *testing.T) {
	mount := t.TempDir()
	secret := filepath.Join(t.TempDir(), "id_rsa")
	if err := os.WriteFile(secret, []byte("PRIVATE-KEY"), 0o600); err != nil {
		t.Fatal(err)
	}
	rel := DerivBlobRel(700002, "proxy.mp4")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(mount, rel)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(mount, rel)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRegularUnder(mount, rel); err == nil {
		t.Fatal("final-component symlink followed")
	}
}

// TestOpenRegularUnderRefusesNonRegular covers what O_NOFOLLOW does NOT: a
// directory or a FIFO planted at a reserved blob name. The FIFO is the one that
// bites — without O_NONBLOCK the open blocks forever waiting for a writer, and
// a bounded-open helper that gives up on its timer still leaks its concurrency
// slot because the underlying syscall never returns.
func TestOpenRegularUnderRefusesNonRegular(t *testing.T) {
	mount := t.TempDir()

	t.Run("directory", func(t *testing.T) {
		rel := DerivBlobRel(700003, "poster.jpg")
		if err := os.MkdirAll(filepath.Join(mount, rel), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenRegularUnder(mount, rel); err == nil {
			t.Fatal("opened a DIRECTORY as a derivative blob")
		}
	})

	t.Run("fifo returns instead of hanging", func(t *testing.T) {
		rel := DerivBlobRel(700004, "waveform.json")
		if err := os.MkdirAll(filepath.Dir(filepath.Join(mount, rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mkfifo(filepath.Join(mount, rel), 0o644); err != nil {
			t.Skipf("mkfifo unavailable: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := OpenRegularUnder(mount, rel)
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("opened a FIFO as a derivative blob")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("open on a FIFO BLOCKED — this is the gate-slot leak: " +
				"a bounded-open wrapper gives up on its timer but the syscall never returns")
		}
	})
}

// TestOpenRegularUnderRejectsEscape: the relative path may not climb out.
func TestOpenRegularUnderRejectsEscape(t *testing.T) {
	mount := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{
		"../" + filepath.Base(outside) + "/secret",
		".juicemount/derivatives/../../../etc/passwd",
		"",
	} {
		if _, err := OpenRegularUnder(mount, rel); err == nil {
			t.Errorf("escape %q was permitted", rel)
		}
	}
}
