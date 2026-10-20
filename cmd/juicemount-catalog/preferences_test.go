package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func cliPreferenceExport(t *testing.T, path string) []byte {
	t.Helper()
	code, stream, diagnostic := invokeCLI(path, []string{"--max-bytes", "1048576", "--max-events", "100", "--max-rows", "100"}, nil, "export")
	if code != 0 || len(diagnostic) != 0 {
		t.Fatalf("export failed code=%d diagnostics=%d", code, len(diagnostic))
	}
	return stream
}

func TestCLIPreferencesExplicitUpgradePreservesPortableBytes(t *testing.T) {
	path, authority := cliSetup(t)
	before := cliPreferenceExport(t, path)
	if code, _, _ := invokeCLI(path, nil, nil, "owner", "preference", "prune"); code != 3 {
		t.Fatal("schema3 prune implicitly enabled preferences")
	}
	for i := 0; i < 2; i++ {
		if code, out, diagnostic := invokeCLI(path, nil, nil, "upgrade-preferences"); code != 0 || len(out) != 0 || len(diagnostic) != 0 {
			t.Fatal("explicit/repeated preference upgrade failed")
		}
		if after := cliPreferenceExport(t, path); !bytes.Equal(before, after) {
			t.Fatal("schema4 changed portable schema3 data")
		}
	}
	// Existing owner/credential administration remains usable after upgrade.
	body := cliIssueBody(t, "post-upgrade-token", authority, cliOwner)
	bearer, _ := cliIssue(t, path, body)
	defer clear(bearer)
	code, out, diagnostic := invokeCLI(path, nil, bearer, "auth", "whoami")
	if code != 0 || len(out) == 0 || len(diagnostic) != 0 {
		t.Fatal("qualified credential CLI regressed after upgrade")
	}
	code, out, diagnostic = invokeCLI(path, nil, nil, "owner", "preference", "prune")
	if code != 0 || string(out) != "{\"version\":1,\"status\":\"complete\"}\n" || len(diagnostic) != 0 {
		t.Fatal("owner prune completion was not fixed")
	}
	if after := cliPreferenceExport(t, path); !bytes.Equal(before, after) {
		t.Fatal("prune/credential activity entered portable stream")
	}
	// Import remains schema3/inactive until separate explicit upgrade/binding.
	restored := filepath.Join(t.TempDir(), "restored.sqlite")
	code, _, _ = invokeCLI(restored, []string{"--max-bytes", "1048576", "--max-events", "100", "--max-rows", "100"}, before, "restore")
	if code != 0 {
		t.Fatal("restore failed")
	}
	if code, _, _ := invokeCLI(restored, nil, nil, "owner", "preference", "prune"); code != 3 {
		t.Fatal("restore transferred preference capability")
	}
	if code, _, _ := invokeCLI(restored, nil, bearer, "auth", "whoami"); code != 2 {
		t.Fatal("restore retained live credential")
	}
	if code, _, _ := invokeCLI(restored, nil, nil, "upgrade-preferences"); code != 0 {
		t.Fatal("restored schema3 requires explicit upgrade")
	}
	if after := cliPreferenceExport(t, restored); !bytes.Equal(before, after) {
		t.Fatal("restored upgrade changed portable bytes")
	}
}

func TestCLIPreferencesCreateRequiresNewPrivateCatalog(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "preferences.sqlite")
	if code, out, diagnostic := invokeCLI(path, nil, nil, "init-preferences"); code != 0 || len(out) != 0 || len(diagnostic) != 0 {
		t.Fatal("preference initialization failed")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("new catalog is not private")
	}
	before := cliPreferenceExport(t, path)
	if code, _, _ := invokeCLI(path, nil, nil, "init-preferences"); code != 3 {
		t.Fatal("init adopted existing catalog")
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(info, after) || !bytes.Equal(before, cliPreferenceExport(t, path)) {
		t.Fatal("failed init changed existing catalog")
	}
	label := "synthetic preference owner"
	body := cliRequest(t, "bootstrap-preferences", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: cliOwner, CreateLabel: &label})
	if code, _, _ := invokeCLI(path, nil, body, "owner", "bootstrap"); code != 0 {
		t.Fatal("new schema4 catalog cannot be explicitly bound")
	}
	if code, out, _ := invokeCLI(path, nil, nil, "owner", "preference", "prune"); code != 0 || string(out) != "{\"version\":1,\"status\":\"complete\"}\n" {
		t.Fatal("new catalog was not preference-capable")
	}
}

func TestCLIPreferencesRejectMissingWrongUnsafeAndOldTargets(t *testing.T) {
	for _, command := range [][]string{{"init-preferences"}, {"upgrade-preferences"}, {"owner", "preference", "prune"}} {
		for _, flags := range [][]string{nil, {"--catalog", filepath.Join(t.TempDir(), "missing.sqlite")}, {"--library", string(cliLibrary)}} {
			var out, diagnostic bytes.Buffer
			args := append(append([]string(nil), flags...), command...)
			if code := run(context.Background(), args, strings.NewReader("private-input"), &out, &diagnostic); code != 3 || out.Len() != 0 || strings.Contains(diagnostic.String(), "private-input") {
				t.Fatal("command admitted incomplete identity or echoed input")
			}
		}
	}
	path, _ := cliSetup(t)
	before := cliPreferenceExport(t, path)
	var out, diagnostic bytes.Buffer
	wrong := continuity.LibraryID("lib_99999999999999999999999999999999")
	for _, command := range [][]string{{"upgrade-preferences"}, {"owner", "preference", "prune"}} {
		args := append([]string{"--catalog", path, "--library", string(wrong)}, command...)
		if code := run(context.Background(), args, strings.NewReader(""), &out, &diagnostic); code != 3 {
			t.Fatal("wrong library was accepted")
		}
	}
	if !bytes.Equal(before, cliPreferenceExport(t, path)) {
		t.Fatal("wrong library changed portable state")
	}
	missing := filepath.Join(t.TempDir(), "must-not-create.sqlite")
	if code, _, _ := invokeCLI(missing, nil, nil, "upgrade-preferences"); code != 3 {
		t.Fatal("upgrade created missing target")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("upgrade reserved a missing file")
	}
	link := filepath.Join(t.TempDir(), "linked.sqlite")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := invokeCLI(link, nil, nil, "upgrade-preferences"); code != 3 {
		t.Fatal("upgrade followed symlink")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0600)
	if code, _, _ := invokeCLI(path, nil, nil, "upgrade-preferences"); code != 3 {
		t.Fatal("upgrade admitted nonprivate file")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(t.TempDir(), "schema2.sqlite")
	s, err := catalog.Create(old, cliLibrary)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := invokeCLI(old, nil, nil, "upgrade-preferences"); code != 3 {
		t.Fatal("preference upgrade skipped required schema3 transition")
	}
}

type cliPreferenceFaultWriter struct {
	writes int
	short  bool
}

func (w *cliPreferenceFaultWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.short {
		return len(data) / 2, nil
	}
	return len(data), errors.New("private-preference-output-error")
}

func TestCLIPreferencePrunePropagatesOutputFailureAndCancellation(t *testing.T) {
	path, _ := cliSetup(t)
	if code, _, _ := invokeCLI(path, nil, nil, "upgrade-preferences"); code != 0 {
		t.Fatal("upgrade failed")
	}
	for _, short := range []bool{false, true} {
		writer := &cliPreferenceFaultWriter{short: short}
		var diagnostic bytes.Buffer
		code := run(context.Background(), cliArgs(path, nil, "owner", "preference", "prune"), strings.NewReader(""), writer, &diagnostic)
		if code != 3 || writer.writes != 1 || !strings.Contains(diagnostic.String(), "catalog preference maintenance outcome uncertain") || strings.Contains(diagnostic.String(), "private-preference-output-error") {
			t.Fatal("prune output failure lost uncertainty/redaction")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, diagnostic bytes.Buffer
	if code := run(ctx, cliArgs(path, nil, "owner", "preference", "prune"), strings.NewReader(""), &out, &diagnostic); code != 3 || out.Len() != 0 {
		t.Fatal("canceled prune claimed completion")
	}
	// A failed/canceled invocation must release owners so the next explicit
	// operation can recognize, acquire and close the catalog immediately.
	if code, _, _ := invokeCLI(path, []string{"--timeout", "2s"}, nil, "owner", "preference", "prune"); code != 0 {
		t.Fatal("failed prune retained catalog ownership")
	}
}

func TestCLIPreferencePruneOwnedStdioAndNoMutationCLI(t *testing.T) {
	path, _ := cliSetup(t)
	if code, _, _ := invokeCLI(path, nil, nil, "upgrade-preferences"); code != 0 {
		t.Fatal("upgrade failed")
	}
	files := make([]*os.File, 3)
	for i, name := range []string{"input", "output", "diagnostic"} {
		f, err := os.OpenFile(filepath.Join(t.TempDir(), name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		files[i] = f
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if code := runOwned(ctx, cliArgs(path, nil, "owner", "preference", "prune"), files[0], files[1], files[2]); code != 0 {
		t.Fatal("owned-stdio prune failed")
	}
	output, err := os.ReadFile(files[1].Name())
	if err != nil || string(output) != "{\"version\":1,\"status\":\"complete\"}\n" {
		t.Fatal("owned completion output mismatch")
	}
	for _, command := range [][]string{{"preference", "select"}, {"preference", "clear"}, {"owner", "preference", "select"}} {
		code, out, diagnostic := invokeCLI(path, nil, []byte("synthetic-private-bearer-and-json"), command...)
		if code != 3 || len(out) != 0 || bytes.Contains(diagnostic, []byte("synthetic-private")) {
			t.Fatal("unexpected second mutation CLI")
		}
	}
}
