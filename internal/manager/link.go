package manager

// JuiceMount Link management (Tier-2 T2.1/T2.3): pairing API + node list +
// revoke. These endpoints are registered by Register() alongside the existing
// /api/ routes and protected by the same admin-key middleware.
//
// The actual headscale process is supervised either:
//   - inside this container (JM_NET_HEADSCALE=on), or
//   - on the NAS host directly (spike deployment)
//
// In both cases, the CLI is the stable contract. We shell out rather than
// using headscale's gRPC API (which changes between versions).

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
)

// LinkStatus is returned by GET /api/net/link.
type LinkStatus struct {
	Enabled  bool   `json:"enabled"`
	ServerURL string `json:"server_url,omitempty"`
	Error    string `json:"error,omitempty"`
}

// PairRequest is accepted by POST /api/net/pair (empty body is fine).
type PairRequest struct{}

// handleLinkStatus reports whether the Link subsystem is available.
func (a *API) handleLinkStatus(w http.ResponseWriter, r *http.Request) {
	enabled := os.Getenv("JM_NET_HEADSCALE") == "on" ||
		headscaleHostBinaryExists()
	url := strings.TrimRight(os.Getenv("JM_NET_SERVER_URL"), "/")
	writeJSON(w, http.StatusOK, LinkStatus{Enabled: enabled && url != "", ServerURL: url})
}

// handlePair mints a preauth key for a new Mac.
func (a *API) handlePair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	out, err := exec.Command(headscaleBin(), "--config", headscaleConfig(),
		"preauthkeys", "--user", "1", "create",
		"--reusable", "--expiration", "1h").CombinedOutput()
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": fmt.Sprintf("%v: %s", err, strings.TrimSpace(string(out)))})
		return
	}
	key := lastNonEmptyLine(string(out))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         key != "",
		"code":       key,
		"server_url": strings.TrimRight(os.Getenv("JM_NET_SERVER_URL"), "/"),
	})
}

// handlePairedNodes lists all registered tailnet nodes.
func (a *API) handlePairedNodes(w http.ResponseWriter, r *http.Request) {
	out, err := exec.Command(headscaleBin(), "--config", headscaleConfig(),
		"nodes", "list").CombinedOutput()
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "raw": string(out)})
}

// handleRevokeNode expires a node's registration.
func (a *API) handleRevokeNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	nodeID := r.URL.Query().Get("id")
	if nodeID == "" {
		http.Error(w, "missing ?id=", http.StatusBadRequest)
		return
	}
	out, err := exec.Command(headscaleBin(), "--config", headscaleConfig(),
		"nodes", "expire", nodeID).CombinedOutput()
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": fmt.Sprintf("%v: %s", err, strings.TrimSpace(string(out)))})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// --- helpers ---

func headscaleBin() string {
	if b := os.Getenv("JM_HEADSCALE_BIN"); b != "" {
		return b
	}
	// Try container path first, then host path
	for _, p := range []string{"/usr/local/bin/headscale", "/root/headscale/hs"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "headscale"
}

func headscaleConfig() string {
	if c := os.Getenv("JM_HEADSCALE_CONFIG"); c != "" {
		return c
	}
	// Container or host default
	for _, p := range []string{"/data/headscale/config.yaml", "/root/headscale/config.yaml"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "/data/headscale/config.yaml"
}

func headscaleHostBinaryExists() bool {
	_, err := os.Stat(headscaleBin())
	return err == nil
}

func lastNonEmptyLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if l != "" && !strings.HasPrefix(l, "20") { // skip timestamped log lines
			return l
		}
	}
	return ""
}


