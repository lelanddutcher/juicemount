//go:build redisoptionsacceptance

package respproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// This fixture owns its server and DB; it never flushes an inherited Redis.
func birthActivationRedis(t *testing.T) (string, *redis.Client) {
	t.Helper()
	binary, want := os.Getenv("JM_REDIS_OPTIONS_SERVER"), os.Getenv("JM_REDIS_OPTIONS_SERVER_SHA256")
	if !filepath.IsAbs(binary) || len(want) != 64 {
		t.Fatal("owned Redis requires pinned absolute binary and SHA256")
	}
	f, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(h.Sum(nil)) != want {
		t.Fatal("owned Redis binary fingerprint mismatch")
	}
	base := os.Getenv("TMPDIR")
	clean := filepath.Clean(base) + "/"
	if !filepath.IsAbs(base) || strings.HasPrefix(clean, "/tmp/") || strings.HasPrefix(clean, "/private/tmp/") || strings.HasPrefix(clean, "/var/tmp/") {
		t.Fatal("private TMPDIR required")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf("bind 127.0.0.1\nport %s\nprotected-mode yes\nsave \"\"\nappendonly no\ndir %q\nloglevel warning\n", port, dir)
	configPath := filepath.Join(dir, "redis.conf")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, configPath)
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
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: testDB, Protocol: 3})
	t.Cleanup(func() { _ = rdb.Close() })
	deadline := time.Now().Add(3 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := rdb.Ping(ctx).Err()
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("owned Redis not ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return addr, rdb
}

func TestStageBirthCoreStaticAdmission(t *testing.T) {
	for _, tc := range []struct {
		name             string
		mode             Mode
		singleton, valid bool
	}{
		{"normalized-default-mode", "", true, true},
		{"explicit-enforce", ModeEnforce, true, true},
		{"observe", ModeObserve, true, false},
		{"passthrough", ModePassthrough, true, false},
		{"no-singleton", ModeEnforce, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(Config{Upstream: "127.0.0.1:1", Mode: tc.mode, Singleton: tc.singleton, StageBirth: true})
			if (err == nil) != tc.valid {
				t.Fatalf("admission=%v, want valid=%t", err, tc.valid)
			}
			if p != nil {
				if p.Mode() != ModeEnforce {
					t.Fatalf("normalized mode=%s", p.Mode())
				}
				_ = p.Close()
			}
		})
	}
}

func TestStageBirthStartupRefusesEffectiveModeOverride(t *testing.T) {
	addr, rdb := birthActivationRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, mode := range []Mode{ModeObserve, ModePassthrough} {
		t.Run(string(mode), func(t *testing.T) {
			if err := rdb.HSet(ctx, ConfigKey, "mode", string(mode)).Err(); err != nil {
				t.Fatal(err)
			}
			p, err := New(Config{Upstream: addr, Mode: ModeEnforce, VolumeDB: testDB,
				Singleton: true, StageBirth: true, ModePoll: time.Hour, Logf: t.Logf})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.Start(); err == nil {
				_ = p.Close()
				t.Fatal("effective mode override admitted birth proxy")
			}
			if !p.closed.Load() {
				t.Fatal("refused proxy remained open")
			}
			if n, err := rdb.Exists(ctx, SingletonKey).Result(); err != nil || n != 0 {
				t.Fatalf("refused proxy retained singleton: count=%d error=%v", n, err)
			}
		})
	}
	if err := rdb.Del(ctx, ConfigKey).Err(); err != nil {
		t.Fatal(err)
	}
	p, err := New(Config{Upstream: addr, Mode: ModeEnforce, VolumeDB: testDB,
		Singleton: true, StageBirth: true, ModePoll: time.Hour, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(); err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if got, err := rdb.Get(ctx, SingletonKey).Result(); err != nil || got != p.Incarnation() {
		_ = p.Close()
		t.Fatalf("valid start ownership=%q error=%v", got, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := rdb.Exists(ctx, SingletonKey).Result(); err != nil || n != 0 {
		t.Fatalf("valid close retained singleton: count=%d error=%v", n, err)
	}
}

func TestStageBirthExpiredAcquisitionCloses(t *testing.T) {
	p := singletonScripted(t, 20*time.Millisecond, time.Hour, func(a [][]byte) string {
		if string(a[0]) == "SET" {
			time.Sleep(50 * time.Millisecond)
			return "+OK\r\n"
		}
		return ":0\r\n"
	})
	// The fixture constructs the production pool before starting any workers.
	p.cfg.StageBirth = true
	p.stageBirth.Store(true)
	if err := p.Start(); err == nil {
		t.Fatal("expired acquisition admitted stage birth")
	}
	p.side.mu.Lock()
	closed := p.side.closed
	p.side.mu.Unlock()
	if !p.closed.Load() || !closed {
		t.Fatal("refused acquisition retained the proxy or pool")
	}
}
