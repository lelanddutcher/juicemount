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
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// testSSHServer is a minimal SSH server for the SFTP connection probe.
// It counts how many times a client offered a password so tests can
// assert that a refused host key stops the client before any credential
// leaves the process.
type testSSHServer struct {
	addr          string
	host, port    string
	hostKey       ssh.PublicKey
	passwordTries atomic.Int32
}

func newTestHostSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	s, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	return s
}

func startTestSSHServer(t *testing.T, signer ssh.Signer, wantPassword string) *testSSHServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	srv := &testSSHServer{addr: ln.Addr().String(), hostKey: signer.PublicKey()}
	srv.host, srv.port, _ = net.SplitHostPort(srv.addr)
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, pw []byte) (*ssh.Permissions, error) {
			srv.passwordTries.Add(1)
			if string(pw) == wantPassword {
				return &ssh.Permissions{}, nil
			}
			return nil, errBadTestPassword
		},
	}
	cfg.AddHostKey(signer)
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
				go ssh.DiscardRequests(reqs)
				for ch := range chans {
					_ = ch.Reject(ssh.Prohibited, "test server")
				}
				_ = sc.Close()
			}(c)
		}
	}()
	return srv
}

type testPasswordError struct{}

func (testPasswordError) Error() string { return "bad password" }

var errBadTestPassword = testPasswordError{}

func sftpCfg(srv *testSSHServer, pinned string) map[string]string {
	cfg := map[string]string{
		"host":     srv.host,
		"port":     srv.port,
		"user":     "u",
		"password": "pw",
	}
	if pinned != "" {
		cfg["host_key"] = pinned
	}
	return cfg
}

// A pinned host key that differs from the one the server presents must
// be refused during key exchange, before any credential is offered.
func TestSFTPTestRefusesChangedHostKeyBeforeAuth(t *testing.T) {
	srv := startTestSSHServer(t, newTestHostSigner(t), "pw")
	other := newTestHostSigner(t).PublicKey()

	_, err := testConnectionSFTP(context.Background(), sftpCfg(srv, formatHostKey(other)))
	if err == nil {
		t.Fatal("connection with a mismatched pinned host key succeeded")
	}
	if !strings.Contains(err.Error(), "host key") {
		t.Errorf("error does not mention the host key: %v", err)
	}
	if n := srv.passwordTries.Load(); n != 0 {
		t.Errorf("client offered a password %d time(s) to a server with an unexpected host key", n)
	}
}

// An unchanged pinned key passes.
func TestSFTPTestAcceptsPinnedHostKey(t *testing.T) {
	srv := startTestSSHServer(t, newTestHostSigner(t), "pw")
	seen, err := testConnectionSFTP(context.Background(), sftpCfg(srv, formatHostKey(srv.hostKey)))
	if err != nil {
		t.Fatalf("pinned key matching the server was refused: %v", err)
	}
	if seen == nil || ssh.FingerprintSHA256(seen) != ssh.FingerprintSHA256(srv.hostKey) {
		t.Errorf("probe did not report the server's host key")
	}
}

// A malformed pin is an error, not a silent fallback to accepting any key.
func TestSFTPTestRejectsMalformedPin(t *testing.T) {
	srv := startTestSSHServer(t, newTestHostSigner(t), "pw")
	if _, err := testConnectionSFTP(context.Background(), sftpCfg(srv, "not a key")); err == nil {
		t.Fatal("malformed host_key was accepted")
	}
	if n := srv.passwordTries.Load(); n != 0 {
		t.Errorf("password offered despite malformed pin")
	}
}

func newSFTPTestAPI(t *testing.T) (*http.ServeMux, *JobManager) {
	t.Helper()
	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    "the-admin-key-for-tests",
		StateFile:   filepath.Join(t.TempDir(), "state.json"),
	})
	t.Cleanup(mgr.StopAll)
	return mux, mgr
}

func sftpDoJSON(t *testing.T, mux *http.ServeMux, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *strings.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = strings.NewReader(string(b))
	} else {
		rdr = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("X-JuiceMount-Admin-Key", "the-admin-key-for-tests")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

// The first successful test records the server's host key with the
// destination and reports its fingerprint; a later server with a
// different key is then refused before authentication.
func TestSFTPTestRecordsHostKeyOnFirstUse(t *testing.T) {
	mux, mgr := newSFTPTestAPI(t)
	signer := newTestHostSigner(t)
	srv := startTestSSHServer(t, signer, "pw")

	w := sftpDoJSON(t, mux, http.MethodPost, "/api/destinations", Destination{Name: "box", Kind: "sftp", Config: sftpCfg(srv, "")})
	if w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}

	w = sftpDoJSON(t, mux, http.MethodPost, "/api/destinations/box/test", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("first test: %d %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	wantFP := ssh.FingerprintSHA256(signer.PublicKey())
	if resp["host_key_fingerprint"] != wantFP {
		t.Errorf("fingerprint in response = %v, want %s", resp["host_key_fingerprint"], wantFP)
	}
	if resp["host_key_recorded"] != true {
		t.Errorf("response does not say the key was recorded: %v", resp)
	}

	d, err := storeOf(mgr).getPlaintext("box")
	if err != nil {
		t.Fatal(err)
	}
	if d.Config["host_key"] != formatHostKey(signer.PublicKey()) {
		t.Fatalf("host key not stored with destination: %q", d.Config["host_key"])
	}

	// Second test against the same server: passes, nothing new recorded.
	w = sftpDoJSON(t, mux, http.MethodPost, "/api/destinations/box/test", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("second test: %d %s", w.Code, w.Body.String())
	}
	resp = nil
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["host_key_recorded"] != false {
		t.Errorf("second test re-recorded the key: %v", resp)
	}

	// The destination now reaches a server presenting a different key
	// (the stored pin is kept): refused before any password is offered.
	srv2 := startTestSSHServer(t, newTestHostSigner(t), "pw")
	d.Config["port"] = srv2.port
	if err := storeOf(mgr).upsert(d, true); err != nil {
		t.Fatal(err)
	}
	w = sftpDoJSON(t, mux, http.MethodPost, "/api/destinations/box/test", nil)
	if w.Code == http.StatusOK {
		t.Fatalf("test against a changed host key succeeded")
	}
	if n := srv2.passwordTries.Load(); n != 0 {
		t.Errorf("password offered to a server with a changed host key")
	}
	d, _ = storeOf(mgr).getPlaintext("box")
	if d.Config["host_key"] != formatHostKey(signer.PublicKey()) {
		t.Errorf("pinned key was replaced after a mismatch")
	}
}

func storeOf(mgr *JobManager) *destinationStoreImpl {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.destinations.(*destinationStoreImpl)
}

// Editing a destination without supplying host_key keeps the pin when
// host and port are unchanged; changing the host clears it.
func TestSFTPEditKeepsPinUnlessHostChanges(t *testing.T) {
	mux, mgr := newSFTPTestAPI(t)
	key := formatHostKey(newTestHostSigner(t).PublicKey())
	cfg := map[string]string{"host": "h.example.com", "port": "2222", "user": "u", "password": "pw", "host_key": key}
	if w := sftpDoJSON(t, mux, http.MethodPost, "/api/destinations", Destination{Name: "box", Kind: "sftp", Config: cfg}); w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	edit := map[string]string{"host": "h.example.com", "port": "2222", "user": "u2", "password": "pw2"}
	if w := sftpDoJSON(t, mux, http.MethodPut, "/api/destinations/box", Destination{Kind: "sftp", Config: edit}); w.Code != http.StatusOK {
		t.Fatalf("edit: %d %s", w.Code, w.Body.String())
	}
	d, _ := storeOf(mgr).getPlaintext("box")
	if d.Config["host_key"] != key {
		t.Fatalf("edit without host_key dropped the pin")
	}
	moved := map[string]string{"host": "other.example.com", "port": "2222", "user": "u2", "password": "pw2"}
	if w := sftpDoJSON(t, mux, http.MethodPut, "/api/destinations/box", Destination{Kind: "sftp", Config: moved}); w.Code != http.StatusOK {
		t.Fatalf("move: %d %s", w.Code, w.Body.String())
	}
	d, _ = storeOf(mgr).getPlaintext("box")
	if d.Config["host_key"] != "" {
		t.Fatalf("pin carried over to a different host")
	}
}

// Transfers: an SFTP destination without a recorded host key is refused,
// and a pinned key is handed to juicefs sync as a known_hosts file.
func TestSFTPTransferUsesPinnedHostKey(t *testing.T) {
	d := Destination{Name: "n", Kind: "sftp", Config: map[string]string{
		"host": "h.example.com", "port": "2222", "user": "u", "password": "pw",
	}}
	if _, _, err := d.ToSyncURI(false); err == nil {
		t.Fatal("transfer to an sftp destination with no recorded host key was allowed")
	}

	pub := newTestHostSigner(t).PublicKey()
	d.Config["host_key"] = formatHostKey(pub)
	_, env, err := d.ToSyncURI(false)
	if err != nil {
		t.Fatalf("ToSyncURI: %v", err)
	}
	env2, cleanup, err := prepareKnownHostsEnv(env)
	if err != nil {
		t.Fatalf("prepareKnownHostsEnv: %v", err)
	}
	var khPath string
	for _, e := range env2 {
		if strings.HasPrefix(e, sftpHostKeyEnv+"=") {
			t.Errorf("internal host-key marker leaked to subprocess env")
		}
		if v, ok := strings.CutPrefix(e, "SSH_KNOWN_HOSTS="); ok {
			khPath = v
		}
	}
	if khPath == "" {
		t.Fatalf("SSH_KNOWN_HOSTS not set: %v", env2)
	}
	cb, err := knownhosts.New(khPath)
	if err != nil {
		t.Fatalf("known_hosts unreadable: %v", err)
	}
	addr := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 2222}
	if err := cb("h.example.com:2222", addr, pub); err != nil {
		t.Errorf("pinned key rejected by generated known_hosts: %v", err)
	}
	if err := cb("h.example.com:2222", addr, newTestHostSigner(t).PublicKey()); err == nil {
		t.Errorf("generated known_hosts accepted a different key")
	}
	cleanup()
	if _, err := os.Stat(khPath); !os.IsNotExist(err) {
		t.Errorf("known_hosts file not removed on cleanup")
	}
}
