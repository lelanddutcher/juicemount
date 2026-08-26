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
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// LinkStatus is returned by GET /api/net/link.
type LinkStatus struct {
	Enabled          bool   `json:"enabled"`
	ControlReady     bool   `json:"control_ready"`
	RouteReady       bool   `json:"route_ready"`
	RedisReady       bool   `json:"redis_ready"`
	ObjectStoreReady bool   `json:"object_store_ready"`
	DataPlaneReady   bool   `json:"data_plane_ready"`
	ServerURL        string `json:"server_url,omitempty"`
	Error            string `json:"error,omitempty"`
}

const embeddedHeadscaleHealthAddr = "127.0.0.1:8091"

// PairRequest is accepted by POST /api/net/pair (empty body is fine).
type PairRequest struct{}

// handleLinkStatus reports whether the Link subsystem is available.
func (a *API) handleLinkStatus(w http.ResponseWriter, r *http.Request) {
	url := strings.TrimRight(os.Getenv("JM_NET_SERVER_URL"), "/")
	if url == "" {
		writeJSON(w, http.StatusOK, LinkStatus{
			Error: "Link is not configured: set JM_NET_SERVER_URL to the address Macs can reach",
		})
		return
	}
	if !linkConfigured() {
		writeJSON(w, http.StatusOK, LinkStatus{
			ServerURL: url,
			Error:     "Link is disabled: set JM_NET_HEADSCALE=on (or configure an external Headscale instance)",
		})
		return
	}
	status := LinkStatus{Enabled: true, ServerURL: url}
	if os.Getenv("JM_NET_HEADSCALE") == "on" {
		conn, err := net.DialTimeout("tcp", embeddedHeadscaleHealthAddr, 250*time.Millisecond)
		if err != nil {
			status.Error = "Headscale is configured but not ready: " + err.Error()
			writeJSON(w, http.StatusOK, status)
			return
		}
		_ = conn.Close()
	}
	status.ControlReady = true
	status.RouteReady = true
	if os.Getenv("JM_NET_NAS_NODE") == "on" || strings.TrimSpace(os.Getenv("JM_NET_NAS_ROUTE")) != "" {
		ready, err := linkRouteReady(headscaleBin(), headscaleConfig(), envString("JM_NET_NAS_HOSTNAME", DefaultNasHostname), envString("JM_NET_NAS_ROUTE", NasLanSubnet))
		status.RouteReady = ready
		if err != nil {
			status.Error = err.Error()
		}
	}
	status.RedisReady = probeLinkRedis(a.linkMetaURL)
	status.ObjectStoreReady = probeLinkObjectStore(a.linkMinIOURL)
	status.DataPlaneReady = status.ControlReady && status.RouteReady && status.RedisReady && status.ObjectStoreReady
	if status.Error == "" && !status.DataPlaneReady {
		switch {
		case a.linkMetaURL == "":
			status.Error = "Link data plane is incomplete: Redis endpoint is not configured"
		case a.linkMinIOURL == "":
			status.Error = "Link data plane is incomplete: object-storage endpoint is not configured"
		case !status.RedisReady:
			status.Error = "Link data plane is degraded: Redis is not ready"
		case !status.ObjectStoreReady:
			status.Error = "Link data plane is degraded: object storage is not ready"
		}
	}
	writeJSON(w, http.StatusOK, status)
}

func probeLinkRedis(raw string) bool {
	if strings.TrimSpace(raw) == "" {
		return false
	}
	opts, err := redis.ParseURL(raw)
	if err != nil {
		return false
	}
	opts.DialTimeout = 500 * time.Millisecond
	opts.ReadTimeout = 500 * time.Millisecond
	opts.WriteTimeout = 500 * time.Millisecond
	client := redis.NewClient(opts)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return client.Ping(ctx).Err() == nil
}

func probeLinkObjectStore(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	u.Path = "/minio/health/live"
	u.RawPath, u.RawQuery, u.Fragment = "", "", ""
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get(u.String())
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// linkConfigured accepts both the managed container deployment and a
// deliberately configured external Headscale instance. The mere presence of
// the bundled headscale binary is not configuration and must not make the UI
// claim that pairing works.
func linkConfigured() bool {
	return os.Getenv("JM_NET_HEADSCALE") == "on" ||
		strings.TrimSpace(os.Getenv("JM_HEADSCALE_CONFIG")) != ""
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

var ansiEscape = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// parseRouteTable reads the stable human table emitted by `nodes list-routes`.
// Headscale 0.29's general node JSON includes available_routes but omits the
// approved/serving route state, so that JSON cannot prove approval. The
// dedicated table is the CLI's authoritative approval surface.
func parseRouteTable(data []byte, nodeID uint64) (approved, available []string, err error) {
	lines := strings.Split(ansiEscape.ReplaceAllString(string(data), ""), "\n")
	approvedCol, availableCol, idCol := -1, -1, -1
	headerLine := -1
	for i, line := range lines {
		if !strings.Contains(line, "|") {
			continue
		}
		cols := strings.Split(line, "|")
		for j, col := range cols {
			switch strings.ToLower(strings.TrimSpace(col)) {
			case "id":
				idCol = j
			case "approved":
				approvedCol = j
			case "available":
				availableCol = j
			}
		}
		if idCol >= 0 && approvedCol >= 0 && availableCol >= 0 {
			headerLine = i
			break
		}
	}
	if headerLine < 0 {
		return nil, nil, fmt.Errorf("parse route table: required columns not found")
	}
	wantID := strconv.FormatUint(nodeID, 10)
	for _, line := range lines[headerLine+1:] {
		if !strings.Contains(line, "|") {
			continue
		}
		cols := strings.Split(line, "|")
		if idCol >= len(cols) || approvedCol >= len(cols) || availableCol >= len(cols) || strings.TrimSpace(cols[idCol]) != wantID {
			continue
		}
		return splitRouteList(cols[approvedCol]), splitRouteList(cols[availableCol]), nil
	}
	return nil, nil, fmt.Errorf("parse route table: node %d not found", nodeID)
}

func splitRouteList(cell string) []string {
	var out []string
	for _, route := range strings.Split(cell, ",") {
		route = strings.TrimSpace(route)
		if route != "" {
			out = append(out, route)
		}
	}
	return out
}

func routeTableForNode(bin, config string, nodeID uint64) (approved, available []string, err error) {
	out, err := exec.Command(bin, "--config", config, "nodes", "list-routes",
		"--identifier", strconv.FormatUint(nodeID, 10)).CombinedOutput()
	if err != nil {
		return nil, nil, fmt.Errorf("nodes list-routes: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return parseRouteTable(out, nodeID)
}

func linkRouteReady(bin, config, host, prefix string) (bool, error) {
	out, err := exec.Command(bin, "--config", config, "nodes", "list", "-o", "json").CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("Link route check failed: %v: %s", err, strings.TrimSpace(string(out)))
	}
	nodes, err := parseHSNodes(out)
	if err != nil {
		return false, err
	}
	n := nodeForHost(nodes, host)
	if n == nil {
		return false, fmt.Errorf("Link NAS node %q is not registered", host)
	}
	approved, available, err := routeTableForNode(bin, config, n.ID)
	if err != nil {
		return false, err
	}
	if !containsRoute(available, prefix) {
		return false, fmt.Errorf("Link NAS node %q is not advertising %s", host, prefix)
	}
	if !containsRoute(approved, prefix) {
		return false, fmt.Errorf("Link NAS route %s is advertised but not approved", prefix)
	}
	return true, nil
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
	approved, available, err := routeTableForNode(bin, config, n.ID)
	if err != nil {
		return err
	}
	authoritative := *n
	authoritative.ApprovedRoutes = approved
	authoritative.AvailableRoutes = available
	pending := pendingApproval(&authoritative, prefixes)
	if len(pending) == 0 {
		for _, prefix := range prefixes {
			if prefix != "" && !containsRoute(available, prefix) && !containsRoute(approved, prefix) {
				return fmt.Errorf("node %q is not advertising route %s", host, prefix)
			}
		}
		return nil
	}
	merged := mergeApproved(approved, pending)
	setOut, err := exec.Command(bin, "--config", config,
		"nodes", "approve-routes",
		"--identifier", strconv.FormatUint(n.ID, 10),
		"--routes", strings.Join(merged, ",")).CombinedOutput()
	if err != nil {
		return fmt.Errorf("approve-routes: %v: %s", err, strings.TrimSpace(string(setOut)))
	}
	verified, _, err := routeTableForNode(bin, config, n.ID)
	if err != nil {
		return fmt.Errorf("verify route approval: %w", err)
	}
	for _, route := range merged {
		if !containsRoute(verified, route) {
			return fmt.Errorf("route approval did not persist for node %q: %s is still absent", host, route)
		}
	}
	return nil
}

func containsRoute(routes []string, want string) bool {
	for _, route := range routes {
		if route == want {
			return true
		}
	}
	return false
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
