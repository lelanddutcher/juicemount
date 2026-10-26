package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity/nativeclient"
)

func TestNativeProfileRecoveryCLIExistingOnlyAndOffline(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	root := filepath.Join(t.TempDir(), "profile")
	code, out, _ := nativeTestRun(root, server.URL, nil, nil, "native", "profile", "open")
	if code != 3 || len(out) != 0 {
		t.Fatal("profile open created missing root")
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("profile open left a new root")
	}
	if code, _, _ := nativeTestRun(root, server.URL, nil, nil, "native", "profile", "create"); code != 0 {
		t.Fatal("profile create failed")
	}
	marker, err := os.ReadFile(filepath.Join(root, "initialized.json"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		code, out, diagnostic := nativeTestRun(root, server.URL, nil, nil, "native", "profile", "open")
		if code != 0 || string(out) != "{\"version\":1,\"opened\":true}\n" || len(diagnostic) != 0 {
			t.Fatalf("exact offline profile reopen failed: code=%d out=%s err=%s", code, out, diagnostic)
		}
	}
	after, err := os.ReadFile(filepath.Join(root, "initialized.json"))
	if err != nil || !bytes.Equal(after, marker) {
		t.Fatal("profile open replaced initialization marker")
	}
	for _, flags := range [][]string{{"--catalog="}, {"--copy-id=foreign"}, {"--basename=foreign"}, {"--library=library_foreign"}, {"--endpoint=http://127.0.0.1:1"}} {
		code, out, _ := nativeTestRun(root, server.URL, nil, flags, "native", "profile", "open")
		if code != 3 || len(out) != 0 {
			t.Fatalf("profile open accepted foreign authority: %v", flags)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("profile open reached network")
	}
	if err := os.WriteFile(filepath.Join(root, "initialized.json"), []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	code, out, _ = nativeTestRun(root, server.URL, nil, nil, "native", "profile", "open")
	if code != 3 || len(out) != 0 {
		t.Fatal("profile open accepted malformed initialization")
	}
}

func TestNativeProfileRecoveryCLICloseAndOutputUncertainty(t *testing.T) {
	root := filepath.Join(t.TempDir(), "profile")
	const endpoint = "https://manager.example"
	j, err := nativeclient.CreateJournal(root, endpoint, cliLibrary, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	args := []string{"--native-root", root, "--endpoint", endpoint, "--library", string(cliLibrary), "native", "profile", "open"}
	files := defaultFiles()
	closes := 0
	files.closeNativeJournal = func(j *nativeclient.Journal) error {
		closes++
		return errors.Join(j.Close(), errors.New("private close fault"))
	}
	var output, diagnostic bytes.Buffer
	code := runConfigured(context.Background(), args, bytes.NewReader(nil), &output, &diagnostic, false, files)
	if code != 3 || output.Len() != 0 || closes != 1 {
		t.Fatal("profile open acknowledged failed original close")
	}
	nativeTestDiagnostic(t, diagnostic.Bytes(), "uncertain", true)
	diagnostic.Reset()
	var short shortDeliveryWriter
	code = run(context.Background(), args, bytes.NewReader(nil), &short, &diagnostic)
	if code != 3 {
		t.Fatal("profile open claimed success after partial receipt")
	}
	nativeTestDiagnostic(t, diagnostic.Bytes(), "uncertain", true)
}
