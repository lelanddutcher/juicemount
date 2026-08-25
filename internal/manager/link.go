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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// LinkStatus is returned by GET /api/net/link.
type LinkStatus struct {
	Enabled   bool   `json:"enabled"`
	ServerURL string `json:"server_url,omitempty"`
	Error     string `json:"error,omitempty"`
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

// PreauthKeyArgs builds the headscale CLI argument vector for minting a
// preauth key. reusable=false produces a ONE-OFF key (T2.3 hardening): it
// registers exactly one node and is then dead, so a leaked pairing code can
// never be replayed to add a second machine.
func PreauthKeyArgs(reusable bool) []string {
	args := []string{"preauthkeys", "--user", "1", "create"}
	if reusable {
		args = append(args, "--reusable")
	}
	return args
}

// sanitizeDeviceName reduces a client-supplied ?device= hint to a safe,
// display-only hostname fragment. Anything unexpected becomes "" so a hostile
// query string can never inject markup or CLI-shaped content into responses
// or logs.
func sanitizeDeviceName(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 63 {
		s = s[:63]
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
			b.WriteRune(r)
		default:
			return "" // any other byte class rejects the whole hint
		}
	}
	return b.String()
}

// handlePair mints a preauth key for a new Mac.
//
// T2.3 hardening: by default each request mints a NON-reusable (one-off)
// key — one code registers exactly one device and then expires unused
// capability. The optional ?device=<hostname> parameter is stored in the
// response for display/audit only; headscale's v1 API has no per-key
// hostname binding, so enforcement stays one-code-one-use. Legacy callers
// that genuinely need many registrations per code pass ?multi=1 to get the
// old reusable behavior back.
//
// The response always carries "reusable" so the UI can adapt its copy
// without parsing CLI semantics.
func (a *API) handlePair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	reusable := r.URL.Query().Get("multi") == "1"
	device := sanitizeDeviceName(r.URL.Query().Get("device"))

	args := append([]string{"--config", headscaleConfig()}, PreauthKeyArgs(reusable)...)
	args = append(args, "--expiration", "1h")
	out, err := exec.Command(headscaleBin(), args...).CombinedOutput()
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "reusable": reusable, "error": fmt.Sprintf("%v: %s", err, strings.TrimSpace(string(out)))})
		return
	}
	key := lastNonEmptyLine(string(out))
	resp := map[string]any{
		"ok":         key != "",
		"code":       key,
		"server_url": strings.TrimRight(os.Getenv("JM_NET_SERVER_URL"), "/"),
		"reusable":   reusable,
	}
	if device != "" {
		resp["device"] = device
	}
	writeJSON(w, http.StatusOK, resp)
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
		"nodes", "expire", "--identifier", nodeID).CombinedOutput()
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

// --- subnet-route approval (T2.2) ---
//
// Headscale v0.27+ folded route management into the nodes commands: a node
// ADVERTISES prefixes (available_routes), and an operator APPROVES them
// (`nodes approve-routes`). The CLI is the stable contract here too —
// `nodes list -o json` emits the protobuf structs' encoding/json shape
// (snake_case), which HSNode mirrors.

// NasLanSubnet is the NAS-side default advertisement: the home LAN that
// holds Redis/MinIO/manager, so a linked Mac can reach them remotely.
const NasLanSubnet = "192.168.0.0/24"

// DefaultNasHostname is the tailnet hostname of the embedded NAS node
// (cmd/juicemount-manager nasnode.go registers under this name).
const DefaultNasHostname = "juicemount-nas"

// HSNode is the subset of headscale's `nodes list -o json` output this
// package consumes.
type HSNode struct {
	ID              uint64   `json:"id"`
	Name            string   `json:"name"`
	GivenName       string   `json:"given_name"`
	AvailableRoutes []string `json:"available_routes"`
	ApprovedRoutes  []string `json:"approved_routes"`
}

// parseHSNodes decodes `headscale nodes list -o json` output. An empty JSON
// array (no nodes yet) is valid, not an error.
func parseHSNodes(data []byte) ([]HSNode, error) {
	var nodes []HSNode
	if err := json.Unmarshal(data, &nodes); err != nil {
		return nil, fmt.Errorf("parse nodes list: %w", err)
	}
	return nodes, nil
}

// nodeForHost finds the node registered under host (matching given_name or
// name; headscale's given_name is the deduplicated DNS label form).
func nodeForHost(nodes []HSNode, host string) *HSNode {
	if host == "" {
		return nil
	}
	for i := range nodes {
		if nodes[i].GivenName == host || nodes[i].Name == host || strings.HasPrefix(nodes[i].Name, host+".") {
			return &nodes[i]
		}
	}
	return nil
}

// pendingApproval returns the wanted prefixes the node advertises but that
// are not approved yet.
func pendingApproval(n *HSNode, want []string) []string {
	if n == nil {
		return nil
	}
	approved := make(map[string]bool, len(n.ApprovedRoutes))
	for _, r := range n.ApprovedRoutes {
		approved[r] = true
	}
	advertised := make(map[string]bool, len(n.AvailableRoutes))
	for _, r := range n.AvailableRoutes {
		advertised[r] = true
	}
	var out []string
	for _, w := range want {
		if w != "" && advertised[w] && !approved[w] {
			out = append(out, w)
		}
	}
	return out
}

// mergeApproved unions existing approved routes with add, preserving order
// and de-duplicating. approve-routes REPLACES the approved set, so callers
// must always pass the merged list or silently drop operator-approved extras.
func mergeApproved(existing, add []string) []string {
	seen := make(map[string]bool, len(existing)+len(add))
	out := make([]string, 0, len(existing)+len(add))
	for _, r := range append(append([]string{}, existing...), add...) {
		if r == "" || seen[r] {
			continue
		}
		seen[r] = true
		out = append(out, r)
	}
	return out
}

// ApproveSubnetRoutes approves prefixes on the node named host. It is
// idempotent: an already-approved prefix is a no-op. Returns an error when
// the node has not registered yet (caller retries) or when the CLI fails.
func ApproveSubnetRoutes(bin, config, host string, prefixes []string) error {
	out, err := exec.Command(bin, "--config", config, "nodes", "list", "-o", "json").Output()
	if err != nil {
		return fmt.Errorf("nodes list: %v: %s", err, strings.TrimSpace(string(out)))
	}
	nodes, err := parseHSNodes(out)
	if err != nil {
		return err
	}
	n := nodeForHost(nodes, host)
	if n == nil {
		return fmt.Errorf("node %q not registered yet", host)
	}
	pending := pendingApproval(n, prefixes)
	if len(pending) == 0 {
		return nil // nothing to do
	}
	merged := mergeApproved(n.ApprovedRoutes, pending)
	setOut, err := exec.Command(bin, "--config", config,
		"nodes", "approve-routes",
		"--identifier", strconv.FormatUint(n.ID, 10),
		"--routes", strings.Join(merged, ",")).CombinedOutput()
	if err != nil {
		return fmt.Errorf("approve-routes: %v: %s", err, strings.TrimSpace(string(setOut)))
	}
	return nil
}

// handleNetRouteApprove approves the NAS LAN subnet route on demand:
//
//	POST /api/net/routes/approve[?host=juicemount-nas&prefix=192.168.0.0/24]
//
// Defaults target the embedded NAS node and JM_NET_NAS_ROUTE (falling back
// to the home-LAN default). GET returns the raw route listing instead, so
// operators can see advertisement/approval state from the UI later.
func (a *API) handleNetRouteApprove(w http.ResponseWriter, r *http.Request) {
	bin, config := headscaleBin(), headscaleConfig()
	switch r.Method {
	case http.MethodGet:
		out, err := exec.Command(bin, "--config", config, "nodes", "list-routes", "-o", "json").Output()
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	case http.MethodPost:
		host := r.URL.Query().Get("host")
		if host == "" {
			host = os.Getenv("JM_NET_NAS_HOSTNAME")
		}
		if host == "" {
			host = DefaultNasHostname
		}
		prefix := r.URL.Query().Get("prefix")
		if prefix == "" {
			prefix = envString("JM_NET_NAS_ROUTE", NasLanSubnet)
		}
		err := ApproveSubnetRoutes(bin, config, host, []string{prefix})
		writeJSON(w, http.StatusOK, map[string]any{"ok": err == nil, "host": host, "prefix": prefix, "error": errString(err)})
	default:
		http.Error(w, "GET or POST only", http.StatusMethodNotAllowed)
	}
}

// envString returns value if set+non-empty, else fallback.
func envString(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
