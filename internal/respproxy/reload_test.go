package respproxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func reloadTestBody(t *testing.T, name string) []byte {
	t.Helper()
	hash, err := HashPassword("synthetic-password", 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(fmt.Sprintf(`{"principals":[{"name":%q,"kind":"mac","host":"test","argon2id":%q}]}`+"\n", name, hash))
}

func reloadTestLine(body []byte, nonce string) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]) + " 1700000000000 " + nonce
}

func reloadTestFixture(t *testing.T) (string, string, *Authenticator, *PrincipalReloader) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "principals.json")
	base := reloadTestBody(t, "old")
	if err := os.WriteFile(path, base, 0o600); err != nil {
		t.Fatal(err)
	}
	ps, err := ParsePrincipals(base)
	if err != nil {
		t.Fatal(err)
	}
	auth := NewAuthenticator(ps)
	r, err := NewPrincipalReloader(path, auth)
	if err != nil {
		t.Fatal(err)
	}
	return dir, path, auth, r
}

func reloadTestRequest(t *testing.T, dir, line string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "reload.request"), []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func reloadTestReceipt(t *testing.T, dir, name, line string) {
	t.Helper()
	path := filepath.Join(dir, name)
	b, err := os.ReadFile(path)
	if err != nil || string(b) != line+"\n" {
		t.Fatalf("%s = %q, %v; want captured request", name, b, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("%s mode: %v %v", name, info, err)
	}
}

func reloadTestHeartbeat(t *testing.T, dir string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "reload.heartbeat"))
	if err != nil || len(bytes.TrimSpace(b)) == 0 {
		t.Fatalf("heartbeat absent: %v", err)
	}
}

func TestPrincipalReloaderInvalidatesStaleReceiptBeforeHeartbeat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "principals.json")
	if err := os.WriteFile(path, reloadTestBody(t, "old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "reload.applied"), []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "reload.ack"), []byte("stale legacy\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := NewPrincipalReloader(path, NewAuthenticator(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "reload.applied")); !os.IsNotExist(err) {
		t.Fatalf("stale applied receipt survived constructor: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "reload.ack")); !os.IsNotExist(err) {
		t.Fatalf("stale legacy ack survived constructor: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "reload.heartbeat")); !os.IsNotExist(err) {
		t.Fatalf("constructor wrote heartbeat before invalidation: %v", err)
	}
	if err := r.Tick(); err != nil {
		t.Fatal(err)
	}
	reloadTestHeartbeat(t, dir)
	if err := os.Mkdir(filepath.Join(dir, "reload.applied"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "reload.applied", "obstruction"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPrincipalReloader(path, NewAuthenticator(nil)); err == nil {
		t.Fatal("constructor accepted an unremovable prior applied receipt")
	}
	if err := os.Remove(filepath.Join(dir, "reload.applied", "obstruction")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "reload.applied")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "reload.ack"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "reload.ack", "obstruction"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "reload.applied"), []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPrincipalReloader(path, NewAuthenticator(nil)); err == nil {
		t.Fatal("constructor accepted an unremovable prior legacy ack")
	}
	if b, err := os.ReadFile(filepath.Join(dir, "reload.applied")); err != nil || string(b) != "stale\n" {
		t.Fatalf("constructor touched applied before failed legacy-ack removal: %q %v", b, err)
	}
}

func TestPrincipalReloaderRejectsUnappliedInputs(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(t *testing.T, path string) (body []byte, request string)
	}{
		{"exact-bytes-mismatch", func(t *testing.T, path string) ([]byte, string) {
			body := reloadTestBody(t, "new")
			return body, reloadTestLine(bytes.TrimSuffix(body, []byte("\n")), "0123456789abcdef")
		}},
		{"trailing-json", func(t *testing.T, path string) ([]byte, string) {
			body := append(reloadTestBody(t, "new"), []byte(`{}`)...)
			return body, reloadTestLine(body, "0123456789abcdef")
		}},
		{"oversize-principals", func(t *testing.T, path string) ([]byte, string) {
			body := bytes.Repeat([]byte(" "), (16<<20)+1)
			return body, reloadTestLine(body, "0123456789abcdef")
		}},
		{"loose-permissions", func(t *testing.T, path string) ([]byte, string) {
			body := reloadTestBody(t, "new")
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
			return body, reloadTestLine(body, "0123456789abcdef")
		}},
		{"invalid-nonce", func(t *testing.T, path string) ([]byte, string) {
			body := reloadTestBody(t, "new")
			return body, reloadTestLine(body, "A123456789abcdef")
		}},
		{"zero-time", func(t *testing.T, path string) ([]byte, string) {
			body := reloadTestBody(t, "new")
			return body, strings.Replace(reloadTestLine(body, "0123456789abcdef"), "1700000000000", "0", 1)
		}},
		{"oversize-request", func(t *testing.T, path string) ([]byte, string) {
			return reloadTestBody(t, "new"), strings.Repeat("x", 4097)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, path, auth, r := reloadTestFixture(t)
			before := auth.table.Load()
			body, line := tc.make(t, path)
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatal(err)
			}
			reloadTestRequest(t, dir, line)
			if err := r.Tick(); err == nil {
				t.Fatal("invalid request or principal file was applied")
			}
			if auth.table.Load() != before {
				t.Fatal("rejected principal file changed active authenticator")
			}
			for _, name := range []string{"reload.applied", "reload.ack"} {
				if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
					t.Fatalf("rejected input advanced %s: %v", name, err)
				}
			}
			reloadTestHeartbeat(t, dir)
		})
	}
}

func TestPrincipalReloaderRetriesAndLegacyLine(t *testing.T) {
	dir, path, auth, r := reloadTestFixture(t)
	body := reloadTestBody(t, "new")
	line := strings.Join(strings.Fields(reloadTestLine(body, "0123456789abcdef"))[:2], " ")
	reloadTestRequest(t, dir, line)
	if err := r.Tick(); err == nil {
		t.Fatal("mismatched bytes were accepted")
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	var swaps int
	r.swap = func(ps *Principals) { swaps++; auth.Swap(ps) }
	if err := r.Tick(); err != nil {
		t.Fatal(err)
	}
	reloadTestReceipt(t, dir, "reload.applied", line)
	reloadTestReceipt(t, dir, "reload.ack", line)
	if err := r.Tick(); err != nil || swaps != 1 {
		t.Fatalf("repeat request swapped again: swaps=%d err=%v", swaps, err)
	}
	next := reloadTestLine(body, "fedcba9876543210")
	reloadTestRequest(t, dir, next)
	if err := r.Tick(); err != nil || swaps != 2 {
		t.Fatalf("same digest with fresh nonce was not reapplied: swaps=%d err=%v", swaps, err)
	}
	reloadTestReceipt(t, dir, "reload.applied", next)
	reloadTestReceipt(t, dir, "reload.ack", next)
}

func TestPrincipalReloaderPublishesOnlyAfterSwapAndCapturedRequest(t *testing.T) {
	dir, path, auth, r := reloadTestFixture(t)
	firstBody := reloadTestBody(t, "first")
	if err := os.WriteFile(path, firstBody, 0o600); err != nil {
		t.Fatal(err)
	}
	first := reloadTestLine(firstBody, "0123456789abcdef")
	reloadTestRequest(t, dir, first)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	joined := false
	defer func() {
		once.Do(func() { close(release) })
		if !joined {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("held Tick did not join during cleanup")
			}
		}
	}()
	r.swap = func(ps *Principals) {
		close(entered)
		<-release
		auth.Swap(ps)
	}
	go func() { done <- r.Tick() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Swap was never entered")
	}
	for _, name := range []string{"reload.applied", "reload.ack"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s appeared before Swap returned: %v", name, err)
		}
	}
	secondBody := reloadTestBody(t, "second")
	if err := os.WriteFile(path, secondBody, 0o600); err != nil {
		t.Fatal(err)
	}
	second := reloadTestLine(secondBody, "fedcba9876543210")
	reloadTestRequest(t, dir, second)
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("held Swap did not finish")
	}
	reloadTestReceipt(t, dir, "reload.applied", first)
	reloadTestReceipt(t, dir, "reload.ack", first)
	if _, ok := auth.table.Load().byName["first"]; !ok || auth.table.Load().Len() != 1 {
		t.Fatal("superseding path replaced the captured table before its receipt")
	}
	r.swap = auth.Swap
	if err := r.Tick(); err != nil {
		t.Fatal(err)
	}
	reloadTestReceipt(t, dir, "reload.applied", second)
	reloadTestReceipt(t, dir, "reload.ack", second)
}

func TestPrincipalReloaderRetriesAfterReceiptFailure(t *testing.T) {
	dir, path, auth, r := reloadTestFixture(t)
	body := reloadTestBody(t, "new")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	line := reloadTestLine(body, "0123456789abcdef")
	reloadTestRequest(t, dir, line)
	var swaps int
	r.swap = func(ps *Principals) { swaps++; auth.Swap(ps) }
	if err := os.Mkdir(filepath.Join(dir, "reload.applied"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := r.Tick(); err == nil || swaps != 1 {
		t.Fatalf("blocked applied receipt: swaps=%d err=%v", swaps, err)
	}
	if err := os.Remove(filepath.Join(dir, "reload.applied")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "reload.ack"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := r.Tick(); err == nil || swaps != 2 {
		t.Fatalf("failed applied receipt did not retry Swap and reach legacy ack: swaps=%d err=%v", swaps, err)
	}
	reloadTestReceipt(t, dir, "reload.applied", line)
	if err := os.Remove(filepath.Join(dir, "reload.ack")); err != nil {
		t.Fatal(err)
	}
	if err := r.Tick(); err != nil || swaps != 2 {
		t.Fatalf("legacy ack retry: swaps=%d err=%v", swaps, err)
	}
	reloadTestReceipt(t, dir, "reload.ack", line)
}
