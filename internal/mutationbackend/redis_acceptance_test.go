//go:build backendacceptance

package mutationbackend

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/redis/go-redis/v9"
)

// Every test owns a fresh server and real process. No external endpoint is
// accepted. Irreversible journal pins are never undone by a testing backdoor.
func TestBackendProcessBoundaries(t *testing.T) {
	for _, name := range []string{"worker", "failed-init", "changed-baseline", "lost-control", "unknown-user", "socket-parent"} {
		t.Run(name, func(t *testing.T) {
			// The parent owns Redis so killing a timed-out helper cannot bypass
			// server cleanup. No process group or process-name lookup is needed.
			f := newBackendFixture(t)
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "-test.run=^TestBackendProcessChild$", "-test.v")
			cmd.Env = append(os.Environ(), "JM_BACKEND_CASE="+name, "JM_BACKEND_DIR="+f.dir)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("isolated %s: %v\n%s", name, err, output)
			}
			t.Logf("owned child pid=%d exited successfully; %s", cmd.Process.Pid, output)
		})
	}
}

func TestBackendProcessChild(t *testing.T) {
	mode := os.Getenv("JM_BACKEND_CASE")
	if mode == "" {
		return
	}
	dir := os.Getenv("JM_BACKEND_DIR")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := &backendFixture{t: t, ctx: ctx, dir: dir, cfg: RedisConfig{
		Socket: filepath.Join(dir, "r.sock"), ACLFile: filepath.Join(dir, "users.acl"), SecretFile: filepath.Join(dir, "control.secret"),
	}}
	secret, err := os.ReadFile(f.cfg.SecretFile)
	if err != nil {
		t.Fatal("fixture secret unavailable")
	}
	f.secret = string(secret)
	j, err := mutationauthority.Open(filepath.Join(f.dir, "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	if mode == "socket-parent" {
		alias := filepath.Join(f.dir, "alias")
		if err := os.Symlink(f.dir, alias); err != nil {
			t.Fatal(err)
		}
		cfg := f.cfg
		cfg.Socket = filepath.Join(alias, "r.sock")
		if fence, _, err := Open(f.ctx, j, "volume", cfg); !errors.Is(err, ErrUnavailable) || fence != nil {
			t.Fatal("symlinked socket parent accepted")
		}
		return
	}
	if mode == "failed-init" {
		if err := os.WriteFile(f.cfg.ACLFile, []byte("changed\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if fence, _, err := Open(f.ctx, j, "volume", f.cfg); !errors.Is(err, ErrUnavailable) || fence != nil {
			t.Fatal("changed startup ACL accepted")
		}
		f.restoreFiles()
		if fence, _, err := Open(f.ctx, j, "volume", f.cfg); !errors.Is(err, mutationauthority.ErrProcessLifetime) || fence != nil {
			t.Fatal("failed backend initialization retried")
		}
		if err := j.Close(); !errors.Is(err, mutationauthority.ErrProcessLifetime) {
			t.Fatal("failed initialization released authority")
		}
		return
	}
	if mode == "unknown-user" {
		admin := f.admin()
		if err := admin.Do(f.ctx, "ACL", "SETUSER", "unknown", "reset", "on", ">"+backendSecret(t), "~*", "+set").Err(); err != nil {
			t.Fatal("fixture unknown user setup failed")
		}
		_ = admin.Close()
		if fence, _, err := Open(f.ctx, j, "volume", f.cfg); !errors.Is(err, ErrUnavailable) || fence != nil {
			t.Fatal("unknown writer adopted")
		}
		return
	}
	fence, credentials, err := Open(f.ctx, j, "volume", f.cfg)
	if err != nil {
		t.Fatalf("baseline Open: %v", err)
	}
	defer fence.Freeze()
	if err := fence.Check(f.ctx); err != nil {
		t.Fatalf("initial Check: %v", err)
	}
	if mode == "seed" {
		// Test-only transfer to the acceptance owner models server-side old
		// connections. Production never exports worker or control credentials.
		if err := os.WriteFile(filepath.Join(f.dir, "worker.url"), []byte(credentials.MetadataURL()), 0600); err != nil {
			t.Fatal("fixture credential capture failed")
		}
		return
	}
	if mode == "changed-baseline" {
		rotated := backendSecret(t)
		acl, _ := StaticACL(rotated)
		if err := os.WriteFile(f.cfg.SecretFile, []byte(rotated), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f.cfg.ACLFile, []byte(acl), 0600); err != nil {
			t.Fatal(err)
		}
		if err := fence.Check(f.ctx); !errors.Is(err, ErrUnavailable) {
			t.Fatal("changed credential baseline accepted")
		}
		f.restoreFiles()
		if err := fence.Check(f.ctx); !errors.Is(err, ErrUnavailable) {
			t.Fatal("failed baseline resumed admission after restoration")
		}
		return
	}
	if mode == "lost-control" {
		admin := f.admin()
		if err := admin.Do(f.ctx, "CLIENT", "KILL", "USER", ControlUser, "SKIPME", "yes").Err(); err != nil {
			t.Fatal("fixture control kill failed")
		}
		_ = admin.Close()
		for range 2 {
			if err := fence.Check(f.ctx); !errors.Is(err, ErrUnavailable) {
				t.Fatal("lost control connection recovered admission")
			}
		}
		return
	}
	options, err := redis.ParseURL(credentials.MetadataURL())
	if err != nil {
		t.Fatal("invalid SDK metadata URL")
	}
	options.MaxRetries, options.DisableIdentity = -1, true
	worker := redis.NewClient(options)
	defer worker.Close()
	if err := worker.Set(f.ctx, "fixture", "exact bytes", 0).Err(); err != nil {
		t.Fatal("worker SET denied")
	}
	if value, err := worker.Get(f.ctx, "fixture").Result(); err != nil || value != "exact bytes" {
		t.Fatal("worker roundtrip differs")
	}
	for _, command := range [][]any{{"ACL", "USERS"}, {"CONFIG", "GET", "aclfile"}, {"MODULE", "LIST"}} {
		if err := worker.Do(f.ctx, command...).Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
			t.Fatalf("worker administrative command %s not denied", command[0])
		}
	}
	if err := fence.Check(f.ctx); err != nil {
		t.Fatalf("worker connection invalidated authority: %v", err)
	}
	rows, err := j.BackendProcesses(f.ctx, "volume")
	wantRows := 1
	if mode == "takeover" {
		wantRows = 2
	}
	if err != nil || len(rows) != wantRows || rows[len(rows)-1].Username != credentials.username || rows[len(rows)-1].RedisRunID != fence.runID {
		t.Fatal("worker lacks its durable exact identity")
	}
	fence.Freeze()
	if err := fence.Check(f.ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatal("frozen controller admitted work")
	}
	if err := j.Close(); !errors.Is(err, mutationauthority.ErrProcessLifetime) {
		t.Fatal("controller close released process authority")
	}
	other, err := mutationauthority.Open(filepath.Join(f.dir, "authority.db"))
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(err, mutationauthority.ErrLocked) {
		t.Fatal("frozen controller allowed premature takeover")
	}
}

func TestBackendTakeoverRevokesRetainedControlAndWorker(t *testing.T) {
	f := newBackendFixture(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func(mode string) {
		t.Helper()
		cmd := exec.CommandContext(f.ctx, binary, "-test.run=^TestBackendProcessChild$", "-test.v")
		cmd.Env = append(os.Environ(), "JM_BACKEND_CASE="+mode, "JM_BACKEND_DIR="+f.dir)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("owned %s process: %v\n%s", mode, err, output)
		}
		t.Logf("owned %s process pid=%d reaped", mode, cmd.Process.Pid)
	}
	run("seed")
	uri, err := os.ReadFile(filepath.Join(f.dir, "worker.url"))
	if err != nil {
		t.Fatal("fixture worker credential unavailable")
	}
	options, err := redis.ParseURL(string(uri))
	if err != nil {
		t.Fatal("invalid worker metadata URL")
	}
	options.MaxRetries, options.DisableIdentity = -1, true
	worker := redis.NewClient(options)
	defer worker.Close()
	workerConn := worker.Conn()
	defer workerConn.Close()
	admin := f.admin()
	defer admin.Close()
	controlConn := admin.Conn()
	defer controlConn.Close()
	command := func(c *redis.Conn, args ...any) (any, error) {
		cmd := redis.NewCmd(f.ctx, args...)
		err := c.Process(f.ctx, cmd)
		return cmd.Val(), err
	}
	ids := map[int64]bool{}
	for _, c := range []*redis.Conn{workerConn, controlConn} {
		value, err := command(c, "CLIENT", "ID")
		id, ok := value.(int64)
		if err != nil || !ok || id <= 0 || ids[id] {
			t.Fatal("fixture retained connection identity invalid")
		}
		ids[id] = true
		if value, err := command(c, "MULTI"); err != nil || value != "OK" {
			t.Fatal("fixture transaction setup failed")
		}
		if value, err := command(c, "SET", "fixture", "stale overwrite"); err != nil || value != "QUEUED" {
			t.Fatal("fixture stale write was not queued")
		}
	}
	run("takeover")
	for _, c := range []*redis.Conn{workerConn, controlConn} {
		if _, err := command(c, "EXEC"); err == nil {
			t.Fatal("retained queued transaction survived takeover")
		}
	}
	// A fresh pool must also fail to authenticate with the previous identity.
	stale := redis.NewClient(options)
	defer stale.Close()
	if err := stale.Set(f.ctx, "fixture", "stale reconnect", 0).Err(); err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatal("old worker identity reauthenticated")
	}
	// Only the test owner reconnects administratively, after the successor has
	// exited. Such external holders are forbidden in the deployment contract.
	inspect := f.admin()
	defer inspect.Close()
	if got, err := inspect.Get(f.ctx, "fixture").Result(); err != nil || got != "exact bytes" {
		t.Fatal("stale queued or reconnected writer changed successor bytes")
	}
	list, err := inspect.ClientList(f.ctx).Result()
	if err != nil {
		t.Fatal("fixture client inspection failed")
	}
	for id := range ids {
		for _, row := range strings.Split(list, "\n") {
			if strings.HasPrefix(row, fmt.Sprintf("id=%d ", id)) {
				t.Fatal("old connection remains after takeover")
			}
		}
	}
}

type backendFixture struct {
	t      *testing.T
	ctx    context.Context
	dir    string
	cfg    RedisConfig
	secret string
}

func backendSecret(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func (f *backendFixture) restoreFiles() {
	f.t.Helper()
	acl, err := StaticACL(f.secret)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(f.cfg.SecretFile, []byte(f.secret), 0600); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(f.cfg.ACLFile, []byte(acl), 0600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *backendFixture) admin() *redis.Client {
	return redis.NewClient(&redis.Options{Network: "unix", Addr: f.cfg.Socket, Username: ControlUser, Password: f.secret,
		MaxRetries: -1, Protocol: 2, DisableIdentity: true, ContextTimeoutEnabled: true,
		ReadTimeout: time.Second, WriteTimeout: time.Second})
}

func newBackendFixture(t *testing.T) *backendFixture {
	t.Helper()
	binary, want := os.Getenv("JM_REDIS_FENCE_SERVER"), os.Getenv("JM_REDIS_FENCE_SERVER_SHA256")
	if !filepath.IsAbs(binary) || len(want) != 64 {
		t.Fatal("tagged gate requires pinned absolute JM_REDIS_FENCE_SERVER and SHA256")
	}
	b, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	if hex.EncodeToString(h[:]) != want {
		t.Fatal("Redis binary does not match pinned digest")
	}
	base := os.Getenv("TMPDIR")
	if !filepath.IsAbs(base) || strings.HasPrefix(filepath.Clean(base)+"/", "/tmp/") || strings.HasPrefix(filepath.Clean(base)+"/", "/private/tmp/") || strings.HasPrefix(filepath.Clean(base)+"/", "/var/tmp/") {
		t.Fatal("set TMPDIR to persistent private scratch")
	}
	dir, err := os.MkdirTemp(base, "rb-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	f := &backendFixture{t: t, ctx: ctx, dir: dir, secret: backendSecret(t), cfg: RedisConfig{
		Socket: filepath.Join(dir, "r.sock"), ACLFile: filepath.Join(dir, "users.acl"), SecretFile: filepath.Join(dir, "control.secret"),
	}}
	if len(f.cfg.Socket) >= 104 {
		t.Fatal("Unix socket path too long")
	}
	f.restoreFiles()
	config := fmt.Sprintf("port 0\nunixsocket %q\nunixsocketperm 700\ndir %q\naclfile %q\nsave \"\"\nappendonly yes\nappendfsync always\nloglevel warning\n", f.cfg.Socket, dir, f.cfg.ACLFile)
	configPath := filepath.Join(dir, "redis.conf")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, configPath)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
			t.Logf("owned Redis pid=%d reaped", cmd.Process.Pid)
		case <-time.After(3 * time.Second):
			t.Error("owned Redis did not terminate")
		}
	})
	for {
		connection, err := net.DialTimeout("unix", f.cfg.Socket, 50*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("fixture Redis startup timed out")
		case <-time.After(5 * time.Millisecond):
		}
	}
	return f
}
