package manager

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func newTestHostSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("host signer: %v", err)
	}
	return signer
}

func authorizedLine(pub ssh.PublicKey) string {
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub)))
}

// testHostKeyLine returns a fresh ed25519 public key in authorized_keys form.
func testHostKeyLine(t *testing.T) string {
	return authorizedLine(newTestHostSigner(t).PublicKey())
}

// startTestSSHServer accepts SSH handshakes with the given host key and
// password, counting every password attempt it receives.
func startTestSSHServer(t *testing.T, host ssh.Signer, password string) (string, *int32) {
	t.Helper()
	var attempts int32
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, pw []byte) (*ssh.Permissions, error) {
			atomic.AddInt32(&attempts, 1)
			if string(pw) == password {
				return nil, nil
			}
			return nil, ssh.ErrNoAuth
		},
	}
	cfg.AddHostKey(host)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				sc, chans, reqs, err := ssh.NewServerConn(c, cfg)
				if err != nil {
					return
				}
				defer sc.Close()
				go ssh.DiscardRequests(reqs)
				for ch := range chans {
					_ = ch.Reject(ssh.Prohibited, "test server")
				}
			}(c)
		}
	}()
	return ln.Addr().String(), &attempts
}

func sftpTestConfig(t *testing.T, addr, password, hostKey string) map[string]string {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split %q: %v", addr, err)
	}
	return map[string]string{
		"host": host, "port": port, "user": "u",
		"password": password, "host_key": hostKey,
	}
}

func TestSFTPTestConnectionPinsHostKeyOnFirstUse(t *testing.T) {
	host := newTestHostSigner(t)
	addr, _ := startTestSSHServer(t, host, "pw")
	ctx := context.Background()

	got, err := testConnectionSFTP(ctx, sftpTestConfig(t, addr, "pw", ""))
	if err != nil {
		t.Fatalf("first use: %v", err)
	}
	if want := authorizedLine(host.PublicKey()); got != want {
		t.Fatalf("pinned key = %q, want %q", got, want)
	}

	again, err := testConnectionSFTP(ctx, sftpTestConfig(t, addr, "pw", got))
	if err != nil {
		t.Fatalf("pinned, same server: %v", err)
	}
	if again != "" {
		t.Errorf("already pinned: got new key %q, want none", again)
	}
}

func TestSFTPTestConnectionRefusesChangedHostKeyBeforeAuth(t *testing.T) {
	impostor := newTestHostSigner(t)
	addr, attempts := startTestSSHServer(t, impostor, "pw")

	_, err := testConnectionSFTP(context.Background(), sftpTestConfig(t, addr, "pw", testHostKeyLine(t)))
	if err == nil || !strings.Contains(err.Error(), "host key mismatch") {
		t.Fatalf("err = %v, want host key mismatch", err)
	}
	if n := atomic.LoadInt32(attempts); n != 0 {
		t.Errorf("impostor received %d password attempts, want 0", n)
	}
}

func TestSFTPSyncRequiresPinnedHostKey(t *testing.T) {
	d := Destination{Name: "n", Kind: "sftp", Config: map[string]string{
		"host": "h.example.com", "port": "2222", "user": "u", "password": "pw",
	}}
	if _, _, err := d.ToSyncURI(false); err == nil || !strings.Contains(err.Error(), "Test Connection") {
		t.Fatalf("unpinned sync: err = %v, want pin instruction", err)
	}

	host := newTestHostSigner(t)
	d.Config["host_key"] = authorizedLine(host.PublicKey())
	_, env, err := d.ToSyncURI(false)
	if err != nil {
		t.Fatalf("pinned sync: %v", err)
	}
	var path string
	for _, e := range env {
		if strings.HasPrefix(e, "SSH_KNOWN_HOSTS=") {
			path = strings.TrimPrefix(e, "SSH_KNOWN_HOSTS=")
		}
	}
	if path == "" {
		t.Fatalf("env has no SSH_KNOWN_HOSTS: %v", env)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("known_hosts file %q: mode %v err %v", path, fi, err)
	}
	cb, err := knownhosts.New(path)
	if err != nil {
		t.Fatalf("parse known_hosts: %v", err)
	}
	remote := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 2222}
	if err := cb("h.example.com:2222", remote, host.PublicKey()); err != nil {
		t.Errorf("pinned key rejected: %v", err)
	}
	if err := cb("h.example.com:2222", remote, newTestHostSigner(t).PublicKey()); err == nil {
		t.Errorf("different key accepted by known_hosts")
	}
}

func TestValidateConfigRejectsBadSFTPHostKey(t *testing.T) {
	err := validateConfig("sftp", map[string]string{
		"host": "h", "user": "u", "password": "p", "host_key": "not a key",
	})
	if err == nil {
		t.Fatal("invalid host_key accepted")
	}
}
