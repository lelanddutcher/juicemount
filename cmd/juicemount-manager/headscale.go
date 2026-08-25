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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	hsListen = "0.0.0.0:8091" // in-container; compose maps 30193 → 8091
	hsBinary = "/usr/local/bin/headscale"
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
	if _, err := os.Stat(cfgPath); err == nil {
		return cfgPath, nil
	}
	if err := os.MkdirAll(hsDataDir, 0o755); err != nil {
		return "", err
	}
	url := externalURL()
	if url == "" {
		return "", fmt.Errorf("JM_NET_SERVER_URL not set (the address Mac clients will use, e.g. http://<nas-ip>:30193)")
	}
	tlsBlock := tlsConfigBlock(os.Getenv("JM_NET_TLS_CERT"), os.Getenv("JM_NET_TLS_KEY"))
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
`, url, hsListen, hsDataDir, tlsBlock)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		return "", err
	}
	return cfgPath, nil
}

// Start launches (or adopts) the headscale child. Blocks until the process
// is up or the first failure, then supervises in the background.
func (h *headscaleSupervisor) Start() error {
	cfg, err := ensureConfig()
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.cmd = exec.Command(hsBinary, "serve", "--config", cfg)
	h.cmd.Stdout = os.Stdout
	h.cmd.Stderr = os.Stderr
	if err := h.cmd.Start(); err != nil {
		h.mu.Unlock()
		return fmt.Errorf("headscale start: %w", err)
	}
	h.mu.Unlock()
	go h.supervise(cfg)
	// Wait briefly for the coordination port before declaring success.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if processAlive(h.cmd.Process.Pid) {
			// User creation must happen AFTER serve is accepting gRPC;
			// before that it fails and pairing would mint against an empty
			// directory ("user not found"). Idempotent: duplicate = fine.
			for tries := 0; tries < 10; tries++ {
				if err := exec.Command(hsBinary, "--config", cfg, "users", "create", "jm").Run(); err == nil {
					break
				}
				time.Sleep(500 * time.Millisecond) // already-exists also lands here; harmless
			}
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("headscale exited during startup")
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

func processAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return p.Signal(syscall.Signal(0)) == nil
}

// pairResponse is the JSON shape of POST /api/net/pair.
type pairResponse struct {
	OK          bool   `json:"ok"`
	Code        string `json:"code,omitempty"`         // the preauth key itself (the "code")
	ServerURL   string `json:"server_url,omitempty"`   // where the Mac's tsnet dials
	Error       string `json:"error,omitempty"`
}

// pairMint runs the headscale CLI to mint a reusable preauth key. CLI over
// the unix socket keeps us off headscale's gRPC API surface (which changes
// between versions); the CLI is the stable contract.
func pairMint() (string, error) {
	url := externalURL()
	if url == "" {
		return "", fmt.Errorf("JM_NET_SERVER_URL not set")
	}
	out, err := exec.Command(hsBinary, "--config", hsDataDir+"/config.yaml",
		"preauthkeys", "--user", "1", "create",
		"--reusable", "--expiration", "1h").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	key := strings.TrimSpace(string(out))
	// CLI prints a log line + the key; take the last non-empty token.
	for _, line := range strings.Split(key, "\n") {
		if strings.HasPrefix(line, "20") { // timestamped log lines
			continue
		}
		if line != "" {
			return line, nil
		}
	}
	return "", fmt.Errorf("could not parse preauth key from output")
}

// handlePair mints one pairing code. Registered under /api/net/pair.
func handlePair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	key, err := pairMint()
	resp := pairResponse{OK: err == nil, Error: errString(err), Code: key, ServerURL: externalURL()}
	writeJSON(w, resp)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
