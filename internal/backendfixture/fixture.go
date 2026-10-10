//go:build sdkacceptance

// Package backendfixture owns private Redis processes for SDK acceptance only.
// It never accepts an existing endpoint or joins the production executable.
package backendfixture

import (
	"bytes"
	"context"
	"crypto/rand"
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

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/redis/go-redis/v9"
)

type Fixture struct {
	Dir, Socket, ACLFile, SecretFile string
	Context                          context.Context
	secret                           string
}

func (f *Fixture) MetadataURL() string {
	u := url.URL{Scheme: "unix", Path: f.Socket, User: url.UserPassword("jmgw_control", f.secret)}
	return u.String()
}

func (f *Fixture) Admin() *redis.Client {
	return redis.NewClient(&redis.Options{Network: "unix", Addr: f.Socket, Username: "jmgw_control", Password: f.secret,
		MaxRetries: -1, Protocol: 2, DisableIdentity: true, ContextTimeoutEnabled: true,
		ReadTimeout: time.Second, WriteTimeout: time.Second})
}

func New(t *testing.T, acl func(string) (string, error)) *Fixture {
	t.Helper()
	binary, want := os.Getenv("JM_REDIS_FENCE_SERVER"), os.Getenv("JM_REDIS_FENCE_SERVER_SHA256")
	b, err := os.ReadFile(binary)
	if err != nil || !filepath.IsAbs(binary) || len(want) != 64 {
		t.Fatal("owned SDK acceptance requires a pinned absolute Redis binary")
	}
	h := sha256.Sum256(b)
	if hex.EncodeToString(h[:]) != want {
		t.Fatal("Redis binary fingerprint mismatch")
	}
	base := os.Getenv("TMPDIR")
	if !filepath.IsAbs(base) || strings.HasPrefix(filepath.Clean(base)+"/", "/tmp/") || strings.HasPrefix(filepath.Clean(base)+"/", "/private/tmp/") || strings.HasPrefix(filepath.Clean(base)+"/", "/var/tmp/") {
		t.Fatal("set TMPDIR to private persistent scratch")
	}
	dir, err := os.MkdirTemp(base, "sdk-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	f := &Fixture{Dir: dir, Context: ctx, secret: hex.EncodeToString(random[:]), Socket: filepath.Join(dir, "r.sock"), ACLFile: filepath.Join(dir, "users.acl"), SecretFile: filepath.Join(dir, "control.secret")}
	if len(f.Socket) >= 104 {
		t.Fatal("owned Unix socket path too long")
	}
	baseline, err := acl(f.secret)
	if err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf("port 0\nunixsocket %q\nunixsocketperm 700\ndir %q\naclfile %q\nsave \"\"\nappendonly yes\nappendfsync always\nloglevel warning\n", f.Socket, dir, f.ACLFile)
	configPath := filepath.Join(dir, "redis.conf")
	for path, data := range map[string]string{f.ACLFile: baseline, f.SecretFile: f.secret, configPath: config} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
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
			t.Error("owned Redis failed to terminate")
		}
	})
	for {
		conn, err := net.DialTimeout("unix", f.Socket, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("owned Redis startup timeout")
		case <-time.After(5 * time.Millisecond):
		}
	}
	return f
}

// SeedExisting uses only the freshly owned fixture. It closes every privileged
// connection before the candidate's production fence takes control.
func (f *Fixture) SeedExisting(t *testing.T, format *meta.Format) []byte {
	t.Helper()
	conf := meta.DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := meta.NewClient(f.MetadataURL(), conf)
	if err := m.Init(format, false); err != nil {
		t.Fatal("owned format provisioning failed")
	}
	if err := m.Shutdown(); err != nil {
		t.Fatal("owned format client shutdown failed")
	}
	admin := f.Admin()
	data, err := admin.Get(f.Context, "setting").Bytes()
	_ = admin.Close()
	if err != nil {
		t.Fatal("owned format read failed")
	}
	return data
}
