package catalog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The catalog holds library metadata; group/other access to its directory or
// files would let another local account read it or plant a WAL beside it.
func TestCatalogRefusesSharedDirectoryAndNonPrivateFiles(t *testing.T) {
	source, _ := newTestStore(t)
	applyTest(t, source, initialOperation())
	var stream bytes.Buffer
	if err := source.Export(context.Background(), testLibrary, &stream, portabilityLimits()); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []os.FileMode{0770, 0707, 0777} {
		dir := t.TempDir()
		if err := os.Chmod(dir, mode); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "catalog.sqlite")
		if s, err := Create(path, testLibrary); !errors.Is(err, ErrInvalid) {
			if s != nil {
				s.Close()
			}
			t.Fatalf("Create in %v directory: %v", mode, err)
		}
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Fatalf("refused Create left files in %v directory: %v, %v", mode, entries, err)
		}
		if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream.Bytes()), portabilityLimits()); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Restore into %v directory: %v", mode, err)
		}
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Fatalf("refused Restore left files in %v directory: %v, %v", mode, entries, err)
		}
	}

	s, path := newTestStore(t)
	applyTest(t, s, initialOperation())
	if err := os.Chmod(path, 0640); err != nil {
		t.Fatal(err)
	}
	// A permission problem (for example a copy without -p) is not corruption.
	if _, err := s.GetAsset(context.Background(), testLibrary, testAsset); !errors.Is(err, ErrInvalid) || errors.Is(err, ErrFormat) {
		t.Fatalf("open store served a group-readable catalog: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(path); !errors.Is(err, ErrInvalid) || errors.Is(err, ErrFormat) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("Open admitted a group-readable catalog: %v", err)
	}
	if err := Upgrade(context.Background(), path, testLibrary); !errors.Is(err, ErrInvalid) || errors.Is(err, ErrFormat) {
		t.Fatalf("Upgrade admitted a group-readable catalog: %v", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0730); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(path); !errors.Is(err, ErrInvalid) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("Open admitted a group-writable directory: %v", err)
	}
	if err := Upgrade(context.Background(), path, testLibrary); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Upgrade admitted a group-writable directory: %v", err)
	}
	// A readable but not writable directory (0755) is acceptable.
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
}

// Another account owning the directory could plant WAL sidecars even when the
// mode bits look private; owning the catalog file lets it edit the database.
func TestCatalogRefusesForeignOwnedDirectoryAndFiles(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing ownership to another uid requires root")
	}
	const foreign = 4242
	source, _ := newTestStore(t)
	applyTest(t, source, initialOperation())
	var stream bytes.Buffer
	if err := source.Export(context.Background(), testLibrary, &stream, portabilityLimits()); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(dir, foreign, -1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "catalog.sqlite")
	if s, err := Create(path, testLibrary); !errors.Is(err, ErrInvalid) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("Create in a foreign-owned directory: %v", err)
	}
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream.Bytes()), portabilityLimits()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Restore into a foreign-owned directory: %v", err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatalf("refused calls left files in a foreign-owned directory: %v, %v", entries, err)
	}

	s, path := newTestStore(t)
	applyTest(t, s, initialOperation())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(filepath.Dir(path), foreign, -1); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(path); !errors.Is(err, ErrInvalid) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("Open in a foreign-owned directory: %v", err)
	}
	if err := Upgrade(context.Background(), path, testLibrary); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Upgrade in a foreign-owned directory: %v", err)
	}
	if err := os.Chown(filepath.Dir(path), os.Geteuid(), -1); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(path, foreign, -1); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(path); !errors.Is(err, ErrInvalid) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("Open of a foreign-owned catalog file: %v", err)
	}
	if err := os.Chown(path, os.Geteuid(), -1); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
}
