package verify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalTargetIdentifier(t *testing.T) {
	tmp := t.TempDir()
	tg, err := NewLocalTarget(tmp)
	if err != nil {
		t.Fatal(err)
	}
	id := tg.Identifier()
	if !strings.HasPrefix(id, "local:") {
		t.Errorf("Identifier %q should start with 'local:'", id)
	}
}

func TestLocalTargetWalkAndHash(t *testing.T) {
	tmp := t.TempDir()
	// Three files in a small tree
	mustWrite(t, filepath.Join(tmp, "a.txt"), "hello")
	mustWrite(t, filepath.Join(tmp, "sub", "b.txt"), "world")
	mustWrite(t, filepath.Join(tmp, "sub", "._b.txt"), "apple double") // skip

	tg, err := NewLocalTarget(tmp)
	if err != nil {
		t.Fatal(err)
	}
	tg.SkipFunc(DefaultMacSkipFunc)

	ctx := context.Background()
	entries, errs := tg.Walk(ctx)

	seen := map[string]Entry{}
	done := false
	for !done {
		select {
		case e, ok := <-entries:
			if !ok {
				done = true
				continue
			}
			seen[e.RelPath] = e
		case err, ok := <-errs:
			if ok && err != nil {
				t.Errorf("walk error: %v", err)
			}
		}
	}

	if len(seen) != 2 {
		t.Errorf("expected 2 entries (skipping ._b.txt), got %d: %v", len(seen), keysOf(seen))
	}
	if _, ok := seen["a.txt"]; !ok {
		t.Errorf("a.txt missing from walk")
	}
	if _, ok := seen["sub/b.txt"]; !ok {
		t.Errorf("sub/b.txt missing from walk")
	}

	// Hash a known file
	h, err := tg.Hash(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 64 { // sha256 hex
		t.Errorf("hash length = %d, want 64", len(h))
	}
	// "hello" -> 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
	expected := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if h != expected {
		t.Errorf("hash(a.txt) = %s, want %s", h, expected)
	}
}

func TestLocalTargetAvailable(t *testing.T) {
	tmp := t.TempDir()
	tg, _ := NewLocalTarget(tmp)
	if !tg.Available(context.Background()) {
		t.Error("Available should be true for an extant directory")
	}
}

func TestNewLocalTargetRejectsNonDir(t *testing.T) {
	tmp := t.TempDir()
	file := filepath.Join(tmp, "file.txt")
	mustWrite(t, file, "x")
	_, err := NewLocalTarget(file)
	if err == nil {
		t.Error("expected error when root is a file, got nil")
	}
}

func TestDefaultMacSkipFunc(t *testing.T) {
	cases := map[string]bool{
		"file.mov":              false,
		".DS_Store":             true,
		"._file":                true,
		"sub/._resource":        true,
		".Spotlight-V100":       true,
		".Trashes":              true,
		"normal/folder/file.txt": false,
	}
	for path, want := range cases {
		got := DefaultMacSkipFunc(path)
		if got != want {
			t.Errorf("DefaultMacSkipFunc(%q) = %v, want %v", path, got, want)
		}
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func keysOf(m map[string]Entry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
