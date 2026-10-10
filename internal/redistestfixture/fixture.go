//go:build redisoptionsacceptance

package redistestfixture

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Start owns an isolated, password-protected Redis subprocess for tagged acceptance.
func Start(t *testing.T) string {
	t.Helper()
	binary, want := os.Getenv("JM_REDIS_OPTIONS_SERVER"), os.Getenv("JM_REDIS_OPTIONS_SERVER_SHA256")
	if !filepath.IsAbs(binary) || len(want) != 64 {
		t.Fatal("pinned Redis binary and SHA256 required")
	}
	b, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != want {
		t.Fatal("owned Redis binary fingerprint mismatch")
	}
	base := filepath.Clean(os.Getenv("TMPDIR"))
	if !filepath.IsAbs(base) || base == "/tmp" || strings.HasPrefix(base+"/", "/tmp/") || strings.HasPrefix(base+"/", "/private/tmp/") || strings.HasPrefix(base+"/", "/var/tmp/") {
		t.Fatal("private TMPDIR required")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	dir := t.TempDir()
	conf := fmt.Sprintf("bind 127.0.0.1\nport %d\nprotected-mode yes\nsave \"\"\nappendonly no\ndir %q\nuser default off\nuser probe on >synthetic-secret ~* +@all\nloglevel warning\n", port, dir)
	confPath := filepath.Join(dir, "redis.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, confPath)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("owned Redis did not exit")
		}
	})
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(3 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 30*time.Millisecond)
		if err == nil {
			_ = c.Close()
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("owned Redis did not listen")
	}
	u := &url.URL{Scheme: "redis", Host: addr, User: url.UserPassword("probe", "synthetic-secret"), Path: "/2", RawQuery: "db=3"}
	return u.String()
}
