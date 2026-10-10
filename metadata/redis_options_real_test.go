//go:build redisoptionsacceptance

package metadata

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func startOwnedAuthRedis(t *testing.T) string {
	t.Helper()
	binary, want := os.Getenv("JM_REDIS_OPTIONS_SERVER"), os.Getenv("JM_REDIS_OPTIONS_SERVER_SHA256")
	if !filepath.IsAbs(binary) || len(want) != 64 {
		t.Fatal("owned Redis fixture requires pinned absolute binary and SHA256")
	}
	b, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	if hex.EncodeToString(h[:]) != want {
		t.Fatal("owned Redis binary fingerprint mismatch")
	}
	base := os.Getenv("TMPDIR")
	if !filepath.IsAbs(base) || strings.HasPrefix(filepath.Clean(base)+"/", "/tmp/") || strings.HasPrefix(filepath.Clean(base)+"/", "/private/tmp/") || strings.HasPrefix(filepath.Clean(base)+"/", "/var/tmp/") {
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
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 30*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	u := &url.URL{Scheme: "redis", Host: fmt.Sprintf("127.0.0.1:%d", port), User: url.UserPassword("probe", "synthetic-secret"), Path: "/2", RawQuery: "db=3"}
	return u.String()
}

func TestSavedRedisURLRealOwnedConnection(t *testing.T) {
	u := startOwnedAuthRedis(t)
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	connected, err := NewRedisClient(u, store)
	if err != nil {
		t.Fatalf("connected: %v", err)
	}
	defer connected.Stop()
	deferred, err := NewRedisClientDeferred(u, store)
	if err != nil {
		t.Fatalf("deferred: %v", err)
	}
	defer deferred.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := deferred.RawDB().Ping(ctx).Err(); err != nil {
		t.Fatalf("deferred first use: %v", err)
	}
	if err := connected.Reconnect(); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	for name, client := range map[string]*RedisClient{"connected after reconnect": connected, "deferred": deferred} {
		opts := client.RawDB().Options()
		if opts.DB != 3 || opts.Username != "probe" || opts.Password != "synthetic-secret" {
			t.Errorf("%s lost saved URL options", name)
		}
		if err := client.RawDB().Ping(ctx).Err(); err != nil {
			t.Errorf("%s ping: %v", name, err)
		}
	}
	key := "redis-options-contract-db"
	if err := connected.RawDB().Set(ctx, key, "db3-value", 0).Err(); err != nil {
		t.Fatal(err)
	}
	for _, db := range []int{0, 2} {
		isolated := redis.NewClient(&redis.Options{Addr: connected.RawDB().Options().Addr, Username: "probe", Password: "synthetic-secret", DB: db})
		value, err := isolated.Get(ctx, key).Result()
		_ = isolated.Close()
		if err != redis.Nil {
			t.Fatalf("DB %d unexpectedly reads DB3 key %q: %q, %v", db, key, value, err)
		}
	}
	if value, err := deferred.RawDB().Get(ctx, key).Result(); err != nil || value != "db3-value" {
		t.Fatalf("deferred DB3 read: %q, %v", value, err)
	}
}

func startTLSFront(t *testing.T, target string) string {
	t.Helper()
	t.Setenv("GODEBUG", "x509usefallbackroots=1")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "owned-redis-front"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	rootPool := x509.NewCertPool()
	root, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatal(err)
	}
	rootPool.AddCert(root)
	x509.SetFallbackRoots(rootPool)
	cert := tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: key}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			incoming, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer incoming.Close()
				outgoing, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer outgoing.Close()
				go func() { _, _ = io.Copy(outgoing, incoming); _ = outgoing.Close() }()
				_, _ = io.Copy(incoming, outgoing)
			}()
		}
	}()
	return ln.Addr().String()
}

func TestSavedRedisURLRealTLSFront(t *testing.T) {
	if os.Getenv("JM_REDIS_OPTIONS_TLS_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSavedRedisURLRealTLSFront$")
		cmd.Env = append(os.Environ(), "JM_REDIS_OPTIONS_TLS_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated TLS certificate verification: %v: %s", err, out)
		}
		return
	}
	raw := startOwnedAuthRedis(t)
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	u.Scheme = "rediss"
	u.Host = startTLSFront(t, u.Host)
	u.RawQuery = "db=3"
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rc, err := NewRedisClient(u.String(), store)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Stop()
	if rc.RawDB().Options().TLSConfig == nil {
		t.Fatal("rediss TLS config lost")
	}
	if err := rc.Reconnect(); err != nil {
		t.Fatal(err)
	}
	deferred, err := NewRedisClientDeferred(u.String(), store)
	if err != nil {
		t.Fatal(err)
	}
	defer deferred.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := deferred.RawDB().Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
}
