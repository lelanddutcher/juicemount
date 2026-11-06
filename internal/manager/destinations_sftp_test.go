package manager

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
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

// testSSHServer records what a client offered during authentication.
type testSSHServer struct {
	addr     string
	attempts int32 // password attempts
	mu       sync.Mutex
	methods  []string // every auth method the client tried, "none" included
}

func (s *testSSHServer) offered() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.methods...)
}

// startTestSSHServer accepts SSH handshakes with the given host key and
// password, recording every authentication attempt it receives.
func startTestSSHServer(t *testing.T, host ssh.Signer, password string) (string, *int32) {
	srv := startRecordingSSHServer(t, host, password)
	return srv.addr, &srv.attempts
}

func startRecordingSSHServer(t *testing.T, host ssh.Signer, password string) *testSSHServer {
	t.Helper()
	srv := &testSSHServer{}
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, pw []byte) (*ssh.Permissions, error) {
			atomic.AddInt32(&srv.attempts, 1)
			if string(pw) == password {
				return nil, nil
			}
			return nil, ssh.ErrNoAuth
		},
		AuthLogCallback: func(_ ssh.ConnMetadata, method string, _ error) {
			srv.mu.Lock()
			srv.methods = append(srv.methods, method)
			srv.mu.Unlock()
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
	srv.addr = ln.Addr().String()
	return srv
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

// An unpinned destination must not authenticate to whatever answers:
// Test Connection only reads the server key and hands its fingerprint
// to the operator, sending the server no credentials at all. The pin
// is stored only when the operator confirms that fingerprint.
func TestSFTPUnpinnedTestOffersNoAuthAndConfirmPins(t *testing.T) {
	host := newTestHostSigner(t)
	srv := startRecordingSSHServer(t, host, "pw")
	mux, store := registerDestTest(t, t.TempDir(), "")
	cfg := sftpTestConfig(t, srv.addr, "pw", "")
	delete(cfg, "host_key")
	body, _ := json.Marshal(map[string]any{"name": "s", "kind": "sftp", "config": cfg})
	if w := destRequest(t, mux, http.MethodPost, "/api/destinations", string(body)); w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}

	w := destRequest(t, mux, http.MethodPost, "/api/destinations/s/test", "")
	if w.Code != http.StatusOK {
		t.Fatalf("unpinned test: %d %s", w.Code, w.Body.String())
	}
	var got struct {
		OK          bool   `json:"ok"`
		NeedConfirm bool   `json:"host_key_confirmation_required"`
		HostKey     string `json:"host_key"`
		Fingerprint string `json:"host_key_fingerprint"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode %s: %v", w.Body.String(), err)
	}
	wantFP := ssh.FingerprintSHA256(host.PublicKey())
	if got.OK || !got.NeedConfirm || got.Fingerprint != wantFP || got.HostKey != authorizedLine(host.PublicKey()) {
		t.Fatalf("unpinned test response = %+v, want confirmation request for %s", got, wantFP)
	}
	if m := srv.offered(); len(m) != 0 {
		t.Errorf("unpinned test offered auth methods %v to the server, want none", m)
	}
	if d, _ := store.getPlaintext("s"); d.Config["host_key"] != "" {
		t.Fatalf("unconfirmed key was pinned")
	}

	confirm := func(fp string) *httptest.ResponseRecorder {
		b, _ := json.Marshal(map[string]string{"host_key": got.HostKey, "fingerprint": fp})
		return destRequest(t, mux, http.MethodPost, "/api/destinations/s/host-key", string(b))
	}
	if w := confirm(ssh.FingerprintSHA256(newTestHostSigner(t).PublicKey())); w.Code != http.StatusBadRequest {
		t.Errorf("confirm with wrong fingerprint: %d, want 400", w.Code)
	}
	if d, _ := store.getPlaintext("s"); d.Config["host_key"] != "" {
		t.Fatalf("key pinned despite fingerprint mismatch")
	}
	if w := confirm(wantFP); w.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", w.Code, w.Body.String())
	}
	if d, _ := store.getPlaintext("s"); d.Config["host_key"] != got.HostKey {
		t.Fatalf("confirmed key not pinned: %q", d.Config["host_key"])
	}

	w = destRequest(t, mux, http.MethodPost, "/api/destinations/s/test", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ok": true`) {
		t.Fatalf("pinned test: %d %s", w.Code, w.Body.String())
	}
	if n := atomic.LoadInt32(&srv.attempts); n != 1 {
		t.Errorf("pinned test sent %d password attempts, want 1", n)
	}
}

func TestSFTPFetchHostKeySendsNoCredentials(t *testing.T) {
	host := newTestHostSigner(t)
	srv := startRecordingSSHServer(t, host, "pw")
	ctx := context.Background()

	key, err := fetchSFTPHostKey(ctx, sftpTestConfig(t, srv.addr, "pw", ""))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if authorizedLine(key) != authorizedLine(host.PublicKey()) {
		t.Fatalf("fetched key %q, want %q", authorizedLine(key), authorizedLine(host.PublicKey()))
	}
	if err := testConnectionSFTP(ctx, sftpTestConfig(t, srv.addr, "pw", "")); err == nil {
		t.Fatal("authenticated test ran without a pinned key")
	}
	if m := srv.offered(); len(m) != 0 {
		t.Errorf("server saw auth methods %v, want none", m)
	}
	if err := testConnectionSFTP(ctx, sftpTestConfig(t, srv.addr, "pw", authorizedLine(key))); err != nil {
		t.Fatalf("pinned test: %v", err)
	}
}

func TestSFTPTestConnectionRefusesChangedHostKeyBeforeAuth(t *testing.T) {
	impostor := newTestHostSigner(t)
	addr, attempts := startTestSSHServer(t, impostor, "pw")

	err := testConnectionSFTP(context.Background(), sftpTestConfig(t, addr, "pw", testHostKeyLine(t)))
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

// A PUT for the same server must not swap the pinned key; replacing a
// pin goes through the confirmation endpoint (which refuses a
// different key) or a change of host/port.
func TestSFTPUpdateRefusesDifferentPinForSameServer(t *testing.T) {
	mux, store := registerDestTest(t, t.TempDir(), "")
	pinA, pinB := testHostKeyLine(t), testHostKeyLine(t)
	body := func(port, hk string) string {
		b, _ := json.Marshal(map[string]any{"name": "s", "kind": "sftp", "config": map[string]string{
			"host": "h.example.com", "port": port, "user": "u", "password": "pw", "host_key": hk,
		}})
		return string(b)
	}
	if w := destRequest(t, mux, http.MethodPost, "/api/destinations", body("22", pinA)); w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if w := destRequest(t, mux, http.MethodPut, "/api/destinations/s", body("22", pinB)); w.Code != http.StatusConflict {
		t.Errorf("PUT with a different pin for the same server: %d, want 409", w.Code)
	}
	if d, _ := store.getPlaintext("s"); d.Config["host_key"] != pinA {
		t.Fatalf("pin replaced by PUT")
	}
	if w := destRequest(t, mux, http.MethodPut, "/api/destinations/s", body("22", pinA)); w.Code != http.StatusOK {
		t.Errorf("PUT with the same pin: %d", w.Code)
	}
	if w := destRequest(t, mux, http.MethodPut, "/api/destinations/s", body("2222", pinB)); w.Code != http.StatusOK {
		t.Errorf("PUT with a new port and its own pin: %d", w.Code)
	}
}
