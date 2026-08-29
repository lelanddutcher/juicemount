package main

// Headscale supervisor + pairing API (Tier-2 T2.1, JuiceMount Link).
//
// When JM_NET_HEADSCALE=on, the manager runs an embedded Headscale control
// server as a supervised child process:
//
//	headscale serve --config /data/headscale/config.yaml   (listen :8091)
//
// The compose layer maps host 30193 → 8091 so remote Macs reach it. Pairing
// is the consumer UX: the manager mints a Tailscale preauth key and hands it
// to the Mac app, which embeds tsnet and joins silently. No accounts, no
// client installs.
//
// State layout under the manager-state bind mount:
//	/data/headscale/config.yaml   generated once, then user-owned
//	/data/headscale/*.key, db.sqlite   headscale's own state
//
// All /api/net/* routes are admin-key protected by the same middleware as
// the rest of the manager.

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	hsListen         = "0.0.0.0:8091" // in-container; compose maps 30193 → 8091
	hsBinary         = "/usr/local/bin/headscale"
	hsStartupTimeout = 45 * time.Second
)

// hsDataDir is a var (not const) solely so tests can redirect config
// generation into a temp directory.
var hsDataDir = "/data/headscale"

// headscaleSupervisor owns the child process lifecycle. Restart-on-exit with
// bounded backoff; Stop on manager shutdown so the container exits cleanly.
type headscaleSupervisor struct {
	mu     sync.Mutex
	cmd    *exec.Cmd
	stop   chan struct{}
	stopMu sync.Once
}

func headscaleEnabled() bool { return os.Getenv("JM_NET_HEADSCALE") == "on" }

// externalURL is the address Mac clients must reach for coordination. The
// container only knows its internal listen port, so the operator tells us the
// externally-visible form once: JM_NET_SERVER_URL (e.g.
// http://192.168.0.197:30193). Without it, pairing is refused with a clear
// error rather than minting keys that point at an unreachable address.
func externalURL() string { return strings.TrimRight(os.Getenv("JM_NET_SERVER_URL"), "/") }

// tlsConfigBlock renders the headscale TLS stanza for operator-provided
// certificates (headscale v0.29 "bring your own certificate" mode —
// tls_cert_path/tls_key_path in config-example.yaml). ACME is deliberately
// NOT used: it needs port 80 + a public name, which a home NAS rarely has.
// An empty result means "no TLS" (plain HTTP on listen_addr, today's
// behavior).
//
// DEPLOYMENT (T2.4): mount the PEM cert (full chain) and key into the
// manager container, then set before FIRST generation:
//
//	JM_NET_TLS_CERT=/data/tls/fullchain.pem
//	JM_NET_TLS_KEY=/data/tls/privkey.pem
//	JM_NET_SERVER_URL=https://<public-name-or-ip>:30193   # must match the cert
//
// config.yaml is generated once and then user-owned, so changing these env
// vars later requires deleting /data/headscale/config.yaml to regenerate.
// With TLS on, headscale serves HTTPS on the same listen_addr; Mac clients
// pair against the https:// server_url, and tsnet verifies the certificate
// against the system trust store.
func tlsConfigBlock(certPath, keyPath string) string {
	if certPath == "" || keyPath == "" {
		return ""
	}
	return fmt.Sprintf("tls_cert_path: %s\ntls_key_path: %s\n", certPath, keyPath)
}

func ensureConfig() (string, error) {
	cfgPath := filepath.Join(hsDataDir, "config.yaml")
	publicURL := externalURL()
	certPath, keyPath := os.Getenv("JM_NET_TLS_CERT"), os.Getenv("JM_NET_TLS_KEY")
	if err := validateLinkTransport(publicURL, certPath, keyPath, os.Getenv("JM_NET_ALLOW_INSECURE") == "1"); err != nil {
		return "", err
	}
	if _, err := os.Stat(cfgPath); err == nil {
		return cfgPath, nil
	}
	if err := os.MkdirAll(hsDataDir, 0o755); err != nil {
		return "", err
	}
	tlsBlock := tlsConfigBlock(certPath, keyPath)
	cfg := fmt.Sprintf(`server_url: %s
listen_addr: %s
metrics_listen_addr: 127.0.0.1:9091
grpc_listen_addr: 127.0.0.1:50443
private_key_path: %[3]s/node.key
noise:
  private_key_path: %[3]s/noise_private.key
prefixes:
  v4: 100.64.0.0/10
  v6: fd7a:115c:a1e0::/48
  allocation: sequential
dns:
  magic_dns: false
  base_domain: jum.local
  override_local_dns: false
  nameservers:
    global: []
    split: {}
derp:
  server:
    enabled: false
  urls:
    - https://controlplane.tailscale.com/derpmap/default
  auto_update_enabled: true
database:
  type: sqlite
  sqlite:
    path: %[3]s/db.sqlite
log:
  level: info
%[4]sunix_socket: %[3]s/headscale.sock
unix_socket_permission: "0770"
`, publicURL, hsListen, hsDataDir, tlsBlock)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		return "", err
	}
	return cfgPath, nil
}

func validateLinkTransport(raw, certPath, keyPath string, allowInsecure bool) error {
	if raw == "" {
		return fmt.Errorf("JM_NET_SERVER_URL not set (the address Mac clients will use, e.g. https://<nas-name>:30193)")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("JM_NET_SERVER_URL must be an absolute http or https URL")
	}
	if (certPath == "") != (keyPath == "") {
		return fmt.Errorf("JM_NET_TLS_CERT and JM_NET_TLS_KEY must be configured together")
	}
	if certPath != "" && u.Scheme != "https" {
		return fmt.Errorf("JM_NET_SERVER_URL must use https when Link TLS certificates are configured")
	}
	if u.Scheme == "http" && !allowInsecure {
		return fmt.Errorf("refusing plaintext JuiceMount Link control traffic; use https or set JM_NET_ALLOW_INSECURE=1 for an explicitly trusted test LAN")
	}
	return nil
}

// Start launches the headscale child, waits for its coordination listener,
// creates the Link user, then supervises it in the background. A live PID is
// not sufficient: the old behavior declared success while Headscale was
// still booting (or while user creation had failed), so the UI issued pairing
// codes that could never register a Mac.
func (h *headscaleSupervisor) Start() error {
	cfg, err := ensureConfig()
	if err != nil {
		return err
	}
	h.mu.Lock()
	if h.stop == nil {
		h.stop = make(chan struct{})
	}
	h.cmd = exec.Command(hsBinary, "serve", "--config", cfg)
	h.cmd.Stdout = os.Stdout
	h.cmd.Stderr = os.Stderr
	if err := h.cmd.Start(); err != nil {
		h.mu.Unlock()
		return fmt.Errorf("headscale start: %w", err)
	}
	h.mu.Unlock()
	go h.supervise(cfg)
	// A cold Headscale start opens and validates its durable SQLite state
	// before binding the coordination listener. The release NAS measured just
	// over the old ten-second budget while its ZFS pool was busy, so a healthy
	// child was killed and Link stayed disabled until the whole Manager was
	// restarted. Keep startup bounded, but leave enough room for real storage
	// latency rather than treating it as a control-plane failure.
	if err := waitForHeadscaleListener(hsStartupTimeout); err != nil {
		h.Stop()
		return err
	}
	if err := ensureHeadscaleUser(cfg); err != nil {
		h.Stop()
		return err
	}
	return nil
}

func waitForHeadscaleListener(timeout time.Duration) error {
	_, port, err := net.SplitHostPort(hsListen)
	if err != nil {
		return fmt.Errorf("headscale listener config %q: %w", hsListen, err)
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", port), 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("headscale did not listen within %s: %w", timeout, lastErr)
}

func ensureHeadscaleUser(cfg string) error {
	var lastErr error
	for tries := 0; tries < 10; tries++ {
		out, err := exec.Command(hsBinary, "--config", cfg, "users", "create", "jm").CombinedOutput()
		if err == nil || headscaleUserAlreadyExists(string(out)) {
			return nil
		}
		lastErr = fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("headscale user setup failed: %w", lastErr)
}

// headscaleUserAlreadyExists recognises the two forms emitted by supported
// Headscale versions when `users create jm` is repeated. Older releases say
// "already exists"; newer gRPC-backed releases surface SQLite's unique-key
// violation instead. Both mean the desired durable Link user is ready.
func headscaleUserAlreadyExists(out string) bool {
	s := strings.ToLower(out)
	return strings.Contains(s, "already exists") ||
		(strings.Contains(s, "unique constraint failed") && strings.Contains(s, "users.name"))
}

func (h *headscaleSupervisor) supervise(cfg string) {
	for {
		h.mu.Lock()
		cmd := h.cmd
		h.mu.Unlock()
		if cmd == nil {
			return
		}
		_ = cmd.Wait()
		select {
		case <-h.stop:
			return
		default:
		}
		time.Sleep(2 * time.Second) // bounded restart backoff
		h.mu.Lock()
		h.cmd = exec.Command(hsBinary, "serve", "--config", cfg)
		h.cmd.Stdout = os.Stdout
		h.cmd.Stderr = os.Stderr
		err := h.cmd.Start()
		h.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (h *headscaleSupervisor) Stop() {
	h.stopMu.Do(func() {
		if h.stop == nil {
			h.stop = make(chan struct{})
		}
		close(h.stop)
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cmd != nil && h.cmd.Process != nil {
		_ = h.cmd.Process.Signal(syscall.SIGTERM)
	}
}
