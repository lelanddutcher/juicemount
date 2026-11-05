package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
)

const (
	tokenID     = "0123456789abcdef"
	tokenSecret = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	testToken   = "jma_" + tokenID + "_" + tokenSecret
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// The real binary path off darwin: a token imported into the private config
// file is what serve uses, stdout carries only protocol messages, and no
// output holds the token.
func TestImportThenServe(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("the token is kept in the Keychain on darwin")
	}
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	os.Unsetenv("JM_APP_ISOLATION")
	os.Unsetenv("JM_APP_ISOLATION_ROOT")
	t.Setenv("JUICEMOUNT_MOUNT", "/Volumes/ignored")
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			w.WriteHeader(401)
			_, _ = io.WriteString(w, `{"error":"unknown or revoked agent token"}`)
			return
		}
		switch r.URL.Path {
		case "/api/collections":
			_, _ = io.WriteString(w, `{"collections":[]}`)
		case "/api/continuity/find":
			_, _ = io.WriteString(w, `{"files":[{"asset_id":"asset_00000000000000000000000000000001","name":"x.mov","path":"/A/x.mov","collections":[]}],"scanned":1,"resolved":1}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer mgr.Close()

	var out, errb bytes.Buffer
	if code := run([]string{"token", "import", "--manager", mgr.URL}, strings.NewReader(testToken+"\n"), &out, &errb); code != 0 {
		t.Fatalf("import: %d %s", code, errb.String())
	}
	if strings.Contains(out.String()+errb.String(), tokenSecret) {
		t.Fatal("import printed the token")
	}

	in := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"find_files","arguments":{}}}`,
	}, "\n") + "\n"
	var sout bytes.Buffer
	serr := &lockedBuffer{}
	if code := run([]string{"serve", "--mount", "/Volumes/media"}, strings.NewReader(in), &sout, serr); code != 0 {
		t.Fatalf("serve: %d %s", code, serr)
	}
	sc := bufio.NewScanner(&sout)
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if len(lines) != 2 {
		t.Fatalf("stdout carries %d lines: %q", len(lines), sout.String())
	}
	var resp struct {
		Result struct {
			Content []struct{ Text string } `json:"content"`
			IsError bool                    `json:"isError"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &resp); err != nil || resp.Result.IsError || !strings.Contains(resp.Result.Content[0].Text, `"mount_path":"/Volumes/media/A/x.mov"`) {
		t.Fatalf("find: %s", lines[1])
	}
	if strings.Contains(sout.String()+serr.String(), tokenSecret) {
		t.Fatal("serve output holds the token")
	}
	if !strings.Contains(serr.String(), "serving with agent token "+tokenID) {
		t.Fatalf("stderr: %s", serr)
	}

	// Revoked locally: serve still answers, and tools say how to get a token.
	if code := run([]string{"token", "revoke", tokenID, "--local-only"}, nil, &out, &errb); code != 0 {
		t.Fatalf("revoke: %s", errb.String())
	}
	sout.Reset()
	if code := run([]string{"serve"}, strings.NewReader(in), &sout, serr); code != 0 {
		t.Fatalf("serve without a token: %d", code)
	}
	if !strings.Contains(sout.String(), `"isError":true`) || !strings.Contains(sout.String(), "token create") {
		t.Fatalf("no token: %s", sout.String())
	}

	// Bad invocations.
	for _, args := range [][]string{{"serve", "--mount", "relative"}, {"token", "import"}, {"token", "revoke"}, {"token", "create", "extra"}} {
		if code := run(args, strings.NewReader(""), io.Discard, io.Discard); code == 0 {
			t.Errorf("%v succeeded", args)
		}
	}
}
