package privatefile

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func managedTestDirectory(t *testing.T, parent, name string) string {
	t.Helper()
	path := filepath.Join(parent, name)
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func managedTestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
}

func managedTestRoot(t *testing.T) (string, *ManagedRoot) {
	t.Helper()
	rootPath := managedTestDirectory(t, t.TempDir(), "managed")
	managedTestDirectory(t, rootPath, "attempts")
	managedTestDirectory(t, rootPath, "ready")
	root, err := OpenManagedRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	return rootPath, root
}

func managedTestID(n uint64) string { return fmt.Sprintf("dmat_%032x", n) }

func managedTestPrepared(t *testing.T, root *ManagedRoot, id string, content []byte) *PreparedFile {
	t.Helper()
	attempt, err := root.OpenAttempt(id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := attempt.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := attempt.Writer().Write(content); err != nil {
		t.Fatal(err)
	}
	prepared, err := attempt.Finish()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	return prepared
}

func TestManagedFileMultipleIndependentExposures(t *testing.T) {
	_, root := managedTestRoot(t)
	for i, payload := range [][]byte{[]byte("first exact payload"), bytes.Repeat([]byte("second"), 257)} {
		id := managedTestID(uint64(i + 1))
		prepared := managedTestPrepared(t, root, id, payload)
		if prepared.FileIdentity().Size != int64(len(payload)) {
			t.Fatalf("prepared size = %d, want %d", prepared.FileIdentity().Size, len(payload))
		}
		if err := prepared.Revalidate(); err != nil {
			t.Fatalf("revalidate attempt %d: %v", i, err)
		}
		exposed, err := prepared.ExposeNoReplace()
		if err != nil || !exposed {
			t.Fatalf("expose attempt %d: exposed=%t, err=%v", i, exposed, err)
		}
		ready, err := root.ReadyPath(id)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(ready)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("ready payload %d: got=%q, err=%v", i, got, err)
		}
		if err := root.Revalidate(); err != nil {
			t.Fatalf("root after exposure %d: %v", i, err)
		}
	}
}

func TestManagedFileExistingReadyDestinationUntouched(t *testing.T) {
	rootPath, root := managedTestRoot(t)
	id := managedTestID(3)
	destination := filepath.Join(rootPath, "ready", id+".payload")
	original := []byte("existing ready bytes")
	managedTestFile(t, destination, original)
	prepared := managedTestPrepared(t, root, id, []byte("different new bytes"))
	if exposed, err := prepared.ExposeNoReplace(); err == nil || exposed {
		t.Fatalf("existing destination accepted: exposed=%t, err=%v", exposed, err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("existing destination changed: got=%q, err=%v", got, err)
	}
	partial := filepath.Join(rootPath, "attempts", id, "payload.partial")
	got, err = os.ReadFile(partial)
	if err != nil || !bytes.Equal(got, []byte("different new bytes")) {
		t.Fatalf("failed exposure lost partial: got=%q, err=%v", got, err)
	}
}

func TestManagedFileRejectsPathSubstitution(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		path, root := managedTestRoot(t)
		prepared := managedTestPrepared(t, root, managedTestID(4), []byte("root swap"))
		if err := os.Rename(path, path+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := prepared.Revalidate(); err == nil {
			t.Fatal("replacement root accepted")
		}
	})
	t.Run("attempt", func(t *testing.T) {
		path, root := managedTestRoot(t)
		id := managedTestID(5)
		prepared := managedTestPrepared(t, root, id, []byte("attempt swap"))
		attemptPath := filepath.Join(path, "attempts", id)
		if err := os.Rename(attemptPath, attemptPath+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(attemptPath, 0700); err != nil {
			t.Fatal(err)
		}
		if err := prepared.Revalidate(); err == nil {
			t.Fatal("replacement attempt accepted")
		}
	})
	t.Run("partial", func(t *testing.T) {
		path, root := managedTestRoot(t)
		id := managedTestID(6)
		prepared := managedTestPrepared(t, root, id, []byte("partial swap"))
		partial := filepath.Join(path, "attempts", id, "payload.partial")
		if err := os.Rename(partial, partial+".old"); err != nil {
			t.Fatal(err)
		}
		managedTestFile(t, partial, []byte("other"))
		if err := prepared.Revalidate(); err == nil {
			t.Fatal("replacement partial accepted")
		}
	})
}

func TestManagedFileRejectsSymlinks(t *testing.T) {
	t.Run("partial", func(t *testing.T) {
		path, root := managedTestRoot(t)
		id := managedTestID(7)
		prepared := managedTestPrepared(t, root, id, []byte("original"))
		partial := filepath.Join(path, "attempts", id, "payload.partial")
		if err := os.Rename(partial, partial+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("payload.partial.old", partial); err != nil {
			t.Fatal(err)
		}
		if exposed, err := prepared.ExposeNoReplace(); err == nil || exposed {
			t.Fatalf("symlink partial accepted: exposed=%t, err=%v", exposed, err)
		}
	})
	t.Run("ready directory", func(t *testing.T) {
		path, root := managedTestRoot(t)
		prepared := managedTestPrepared(t, root, managedTestID(8), []byte("original"))
		ready := filepath.Join(path, "ready")
		if err := os.Rename(ready, ready+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("ready.old", ready); err != nil {
			t.Fatal(err)
		}
		if exposed, err := prepared.ExposeNoReplace(); err == nil || exposed {
			t.Fatalf("symlink ready directory accepted: exposed=%t, err=%v", exposed, err)
		}
	})
}

func TestManagedFileRejectsHardlinkedRegularFiles(t *testing.T) {
	t.Run("private input", func(t *testing.T) {
		parent := managedTestDirectory(t, t.TempDir(), "secrets")
		input := filepath.Join(parent, "reader.pem")
		managedTestFile(t, input, []byte("private"))
		if b, _, err := ReadPrivate(input, 1024); err != nil || !bytes.Equal(b, []byte("private")) {
			t.Fatalf("private input before hardlink: got=%q, err=%v", b, err)
		}
		if err := os.Link(input, filepath.Join(parent, "alias.pem")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := ReadPrivate(input, 1024); err == nil {
			t.Fatal("hardlinked private input accepted")
		}
	})
	t.Run("partial before finish", func(t *testing.T) {
		path, root := managedTestRoot(t)
		id := managedTestID(9)
		attempt, err := root.OpenAttempt(id)
		if err != nil {
			t.Fatal(err)
		}
		defer attempt.Close()
		if _, err := attempt.Writer().Write([]byte("private partial")); err != nil {
			t.Fatal(err)
		}
		partial := filepath.Join(path, "attempts", id, "payload.partial")
		if err := os.Link(partial, partial+".link"); err != nil {
			t.Fatal(err)
		}
		prepared, err := attempt.Finish()
		if prepared != nil {
			defer prepared.Close()
		}
		if err == nil {
			t.Fatal("hardlinked writable partial accepted")
		}
	})
	t.Run("prepared partial", func(t *testing.T) {
		path, root := managedTestRoot(t)
		id := managedTestID(10)
		prepared := managedTestPrepared(t, root, id, []byte("prepared partial"))
		partial := filepath.Join(path, "attempts", id, "payload.partial")
		if err := os.Link(partial, partial+".link"); err != nil {
			t.Fatal(err)
		}
		if exposed, err := prepared.ExposeNoReplace(); err == nil || exposed {
			t.Fatalf("hardlinked prepared partial accepted: exposed=%t, err=%v", exposed, err)
		}
	})
}
