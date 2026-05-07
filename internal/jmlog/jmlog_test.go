package jmlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPackageLevelLogging(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Config{Stderr: &buf, Level: LevelDebug}); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = Init(Config{Level: LevelInfo}) })

	Info("hello", "key", "value", "n", 42)
	Warn("careful", "reason", "drift")
	Error("boom", "err", "explode")

	out := buf.String()
	for _, want := range []string{`"msg":"hello"`, `"key":"value"`, `"n":42`, `"msg":"careful"`, `"msg":"boom"`} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q. got: %s", want, out)
		}
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]Level{
		"debug":   LevelDebug,
		"INFO":    LevelInfo,
		" Warn ":  LevelWarn,
		"warning": LevelWarn,
		"error":   LevelError,
		"err":     LevelError,
		"weird":   LevelInfo,
	}
	for in, want := range cases {
		got := ParseLevel(in)
		if got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestRotation writes enough bytes to trigger a rotation and verifies that
// (a) the active log keeps growing afterward, (b) a .1 backup is created,
// and (c) we never exceed (rotateMaxBackup+1) generations on disk.
func TestRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	rf, err := newRotatingFile(path)
	if err != nil {
		t.Fatalf("newRotatingFile: %v", err)
	}
	t.Cleanup(func() { _ = rf.Close() })

	// Force the threshold low so the test runs in milliseconds.
	// We can't change the const, so we drive enough bytes through to cross it.
	chunk := bytes.Repeat([]byte("a"), 1<<20) // 1 MiB
	for i := 0; i < 20; i++ {                 // 20 MiB total — past the 16 MiB threshold
		if _, err := rf.Write(chunk); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Active log should exist and be smaller than the total written.
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat active log: %v", err)
	}
	if st.Size() == 0 {
		t.Fatalf("active log is empty after writes")
	}
	if st.Size() >= 20<<20 {
		t.Fatalf("active log = %d bytes, expected rotation to have shrunk it", st.Size())
	}

	// At least one backup must exist.
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("expected %s.1 to exist after rotation: %v", path, err)
	}

	// Now hammer enough rotations to ensure we cap at rotateMaxBackup.
	for i := 0; i < 200; i++ {
		_, _ = rf.Write(chunk)
	}
	// Beyond rotateMaxBackup — last should not exist
	overflow := fmt.Sprintf("%s.%d", path, rotateMaxBackup+1)
	if _, err := os.Stat(overflow); !os.IsNotExist(err) {
		t.Errorf("expected %s to not exist (over the cap), err=%v", overflow, err)
	}
}

func TestUnpairedKey(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Config{Stderr: &buf, Level: LevelInfo}); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = Init(Config{Level: LevelInfo}) })

	Info("oops", "lonely")

	var rec map[string]any
	if err := json.NewDecoder(&buf).Decode(&rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, ok := rec["lonely"]; !ok || got != "MISSING" {
		t.Errorf("expected lonely=MISSING, got %v", rec)
	}
}
