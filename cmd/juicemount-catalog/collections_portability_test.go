package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// Exercises the production CLI dispatcher and exclusive named file path in
// process. Actual compiled CLI/Manager process acceptance remains root-owned.
func TestCLICollectionPortabilityIndependentFiles(t *testing.T) {
	for _, name := range []string{"empty", "shared"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "..", "internal", "continuity", "catalog", "testdata", "continuity-c04b-"+name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var f struct {
				LibraryID continuity.LibraryID `json:"library_id"`
				StreamHex string               `json:"stream_hex"`
				Combined  int64                `json:"combined_event_count"`
				Rows      int64                `json:"projection_rows"`
			}
			if err := json.Unmarshal(data, &f); err != nil {
				t.Fatal(err)
			}
			wire, err := hex.DecodeString(f.StreamHex)
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			input := filepath.Join(dir, "input.jmc")
			target := filepath.Join(dir, "target.sqlite")
			output := filepath.Join(dir, "output.jmc")
			if err := os.WriteFile(input, wire, 0600); err != nil {
				t.Fatal(err)
			}
			args := func(command string, extra ...string) []string {
				a := []string{"--catalog", target, "--library", string(f.LibraryID), "--max-bytes", fmt.Sprint(len(wire)), "--max-events", fmt.Sprint(max(int64(1), f.Combined)), "--max-rows", fmt.Sprint(f.Rows)}
				a = append(a, extra...)
				return append(a, command)
			}
			var out, diagnostic bytes.Buffer
			if code := run(context.Background(), args("restore", "--input", input), bytes.NewReader(nil), &out, &diagnostic); code != 0 {
				t.Fatalf("production CLI restore code%d diagnostics%d", code, diagnostic.Len())
			}
			s, err := catalog.Open(target)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := s.LocalOwner(f.LibraryID)
			if err != nil {
				s.Close()
				t.Fatal(err)
			}
			status, err := owner.Status(context.Background())
			if err != nil || status.Incarnation != "" {
				s.Close()
				t.Fatalf("CLI imported authority active: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			out.Reset()
			diagnostic.Reset()
			if code := run(context.Background(), args("export", "--output", output), bytes.NewReader(nil), &out, &diagnostic); code != 0 || out.Len() != 0 || diagnostic.Len() != 0 {
				t.Fatalf("named production CLI export code%d out%d diagnostic%d", code, out.Len(), diagnostic.Len())
			}
			actual, err := os.ReadFile(output)
			if err != nil || !bytes.Equal(actual, wire) {
				t.Fatalf("CLI exact independent stream changed: %v", err)
			}
			info, err := os.Stat(output)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("CLI exclusive output permissions: %v", err)
			}
			out.Reset()
			diagnostic.Reset()
			if code := run(context.Background(), args("export", "--output", output), bytes.NewReader(nil), &out, &diagnostic); code == 0 {
				t.Fatal("CLI replaced existing portable output")
			}
			after, err := os.ReadFile(output)
			if err != nil || !bytes.Equal(after, wire) {
				t.Fatal("refused export changed existing output")
			}
			out.Reset()
			diagnostic.Reset()
			if code := run(context.Background(), args("restore", "--input", input), bytes.NewReader(nil), &out, &diagnostic); code == 0 {
				t.Fatal("CLI adopted existing target")
			}
			s, err = catalog.Open(target)
			if err != nil {
				t.Fatalf("refused restore damaged ready target: %v", err)
			}
			var check bytes.Buffer
			if err := s.Export(context.Background(), f.LibraryID, &check, catalog.Limits{MaxBytes: int64(len(wire)), MaxEvents: max(int64(1), f.Combined), MaxProjectionRows: f.Rows}); err != nil || !bytes.Equal(check.Bytes(), wire) {
				s.Close()
				t.Fatalf("existing target changed: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
