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
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/linkconfig"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
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
	RemoteReady      bool   `json:"remote_ready"`
	LANOnly          bool   `json:"lan_only"`
	ServerURL        string `json:"server_url,omitempty"`
	// Exposure is how the embedded NAS node serves JuiceMount: "services"
	// (tailnet listeners for the JuiceMount ports only) or "subnet" (the
	// legacy LAN route, selected by setting JM_NET_NAS_ROUTE).
	Exposure      string               `json:"exposure,omitempty"`
	PolicyManaged bool                 `json:"policy_managed"`
	Headscale     *LinkComponentStatus `json:"headscale,omitempty"`
	NASNode       *LinkComponentStatus `json:"nas_node,omitempty"`
	Error         string               `json:"error,omitempty"`
}

// LinkComponentStatus is a supervised Link component's live state as
// reported by the process that runs it (cmd/juicemount-manager).
type LinkComponentStatus struct {
	// State is starting, running, backoff or stopped.
	State     string    `json:"state"`
	Detail    string    `json:"detail,omitempty"`
	LastError string    `json:"last_error,omitempty"`
	Restarts  int       `json:"restarts"`
	Since     time.Time `json:"since"`
	NextRetry time.Time `json:"next_retry,omitempty"`
	// NodeKey is the NAS node's own node key (nodekey:...), reported once
	// the node is up. The control server lists it as node_key; it is how
	// the Manager finds the NAS node, whose name any node could also take.
	NodeKey string `json:"node_key,omitempty"`
}

// LinkRuntimeStatus is the supervisors' snapshot. A nil component means
// this process does not run it.
type LinkRuntimeStatus struct {
	Headscale *LinkComponentStatus
	NASNode   *LinkComponentStatus
}

var (
	linkRuntimeMu sync.RWMutex
	linkRuntime   func() LinkRuntimeStatus
)

// SetLinkRuntimeStatus installs the supervisors' status source for
// GET /api/net/link. Passing nil removes it.
func SetLinkRuntimeStatus(fn func() LinkRuntimeStatus) {
	linkRuntimeMu.Lock()
	linkRuntime = fn
	linkRuntimeMu.Unlock()
}

func currentLinkRuntime() (LinkRuntimeStatus, bool) {
	linkRuntimeMu.RLock()
	fn := linkRuntime
	linkRuntimeMu.RUnlock()
	if fn == nil {
		return LinkRuntimeStatus{}, false
	}
	return fn(), true
}

// LinkSubnetRoute returns JM_NET_NAS_ROUTE, the LAN prefix an operator
// asked the NAS node to advertise.
func LinkSubnetRoute() string { return strings.TrimSpace(os.Getenv("JM_NET_NAS_ROUTE")) }

// How the embedded NAS node exposes JuiceMount to the tailnet.
const (
	// LinkExposureServices serves only the JuiceMount ports on the NAS
	// node's own tailnet address, and the node carries tag:jm-nas, which is
	// how a Mac app finds it. Mac apps from before this exposure, and remote
	// farm nodes, follow subnet routes only and cannot reach it.
	LinkExposureServices = "services"
	// LinkExposureSubnet advertises a LAN prefix through the NAS node, the
	// only path Link had before service exposure. The node is not tagged
	// tag:jm-nas, so a Mac waits for the route instead of dialing the
	// node's tailnet address.
	LinkExposureSubnet = "subnet"
)

// linkExposureFile records, next to Headscale's config.yaml, the exposure a
// NAS node first started with when no environment chose one, so the default
// does not change once the node's own state exists.
const linkExposureFile = "nas-exposure"

// NAS node placements (JM_NET_NAS_NODE).
const (
	// NASNodeInManager runs the NAS node inside the Manager's process, on
	// the Manager's container network.
	NASNodeInManager = "on"
	// NASNodeInContainer runs the NAS node in its own container
	// (juicemount-manager --link-node, host networking). The Manager mints
	// its keys, tags it and reports its state over a Unix socket.
	NASNodeInContainer = "container"
)

// NASNodeEmbedded reports whether this Manager manages the NAS tailnet node:
// it runs it in-process (JM_NET_NAS_NODE=on) or serves the separate node
// container (JM_NET_NAS_NODE=container). Either way the node, its keys, its
// tags and its exposure belong to this Manager's Headscale.
func NASNodeEmbedded() bool {
	if os.Getenv("JM_NET_HEADSCALE") != "on" {
		return false
	}
	switch os.Getenv("JM_NET_NAS_NODE") {
	case NASNodeInManager, NASNodeInContainer:
		return true
	}
	return false
}

// LinkExposure resolves the NAS node's exposure and, for subnet exposure,
// the prefix it advertises. cfgPath is Headscale's config.yaml; the NAS node
// state and the recorded default live in its directory. In order:
//
//  1. JM_NET_NAS_EXPOSURE (services or subnet);
//  2. JM_NET_NAS_ROUTE set: subnet;
//  3. the default recorded by RecordLinkExposureDefault;
//  4. a NAS node state directory exists (an install from before service
//     exposure, whose Mac apps follow the subnet route): subnet;
//  5. services.
//
// Steps 3-5 apply only to an embedded NAS node; without one the result is
// "" unless 1 or 2 chose. Subnet exposure without JM_NET_NAS_ROUTE
// advertises NasLanSubnet, the default before service exposure.
func LinkExposure(cfgPath string) (mode, subnet string, err error) {
	route := LinkSubnetRoute()
	switch explicit := strings.TrimSpace(os.Getenv("JM_NET_NAS_EXPOSURE")); explicit {
	case LinkExposureServices:
		if route != "" {
			return "", "", fmt.Errorf("JM_NET_NAS_EXPOSURE=services but JM_NET_NAS_ROUTE=%s is set; unset one", route)
		}
		if _, err := LinkNASLANAddr(); err != nil {
			return "", "", err
		}
		return LinkExposureServices, "", nil
	case LinkExposureSubnet:
		mode = LinkExposureSubnet
	case "":
		switch {
		case route != "":
			mode = LinkExposureSubnet
		case !NASNodeEmbedded():
			return "", "", nil
		default:
			mode = linkExposureDefault(filepath.Dir(cfgPath))
		}
	default:
		return "", "", fmt.Errorf("JM_NET_NAS_EXPOSURE must be %s or %s, not %q", LinkExposureServices, LinkExposureSubnet, explicit)
	}
	if mode == LinkExposureServices {
		if _, err := LinkNASLANAddr(); err != nil {
			return "", "", err
		}
		return mode, "", nil
	}
	if route == "" {
		route = NasLanSubnet
	}
	if _, err := LinkSubnetNASAddr(route); err != nil {
		return "", "", err
	}
	return LinkExposureSubnet, route, nil
}

// LinkSubnetNASAddr returns JM_NET_NAS_LAN_ADDR, the NAS's own address
// inside the subnet route, as a host prefix (a /32 or /128), or "" when it is
// unset. The Manager's policy then lets Link clients reach the JuiceMount
// ports on that address only, instead of on every host in the route.
func LinkSubnetNASAddr(subnet string) (string, error) {
	host, err := LinkNASLANAddr()
	if err != nil || host == "" {
		return "", err
	}
	addr := netip.MustParsePrefix(host).Addr()
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil {
		return "", fmt.Errorf("bad JM_NET_NAS_ROUTE %q: %w", subnet, err)
	}
	if !prefix.Contains(addr) {
		return "", fmt.Errorf("JM_NET_NAS_LAN_ADDR %s is outside the NAS route %s", addr, prefix)
	}
	return host, nil
}

// LinkNASLANAddr returns JM_NET_NAS_LAN_ADDR as a host prefix (a /32 or
// /128), or "" when it is unset. In service exposure it tells status which
// approved routes could carry traffic meant for the NAS (linkNASConflicts).
func LinkNASLANAddr() (string, error) {
	raw := strings.TrimSpace(os.Getenv("JM_NET_NAS_LAN_ADDR"))
	if raw == "" {
		return "", nil
	}
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		return "", fmt.Errorf("JM_NET_NAS_LAN_ADDR %q is not an IP address", raw)
	}
	return netip.PrefixFrom(addr, addr.BitLen()).String(), nil
}

func linkExposureDefault(dir string) string {
	if raw, err := os.ReadFile(filepath.Join(dir, linkExposureFile)); err == nil {
		switch mode := strings.TrimSpace(string(raw)); mode {
		case LinkExposureServices, LinkExposureSubnet:
			return mode
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "nas-node")); err == nil {
		return LinkExposureSubnet
	}
	return LinkExposureServices
}

// RecordLinkExposureDefault records next to cfgPath the exposure the NAS
// node first runs with (LinkExposure: an explicit JM_NET_NAS_EXPOSURE or
// JM_NET_NAS_ROUTE included), unless one is recorded already, and returns
// the recorded exposure. That record is the default once neither variable
// is set. The NAS node must not start until it succeeds: without the
// record, the state directory a fresh install's node creates would make its
// next start look like an upgrade. The record is synced, with its
// directory, before it counts.
func RecordLinkExposureDefault(cfgPath string) (string, error) {
	dir := filepath.Dir(cfgPath)
	path := filepath.Join(dir, linkExposureFile)
	if _, err := os.Stat(path); err == nil {
		return linkExposureDefault(dir), nil
	}
	mode, _, err := LinkExposure(cfgPath)
	if err != nil || mode == "" {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	_, err = f.WriteString(mode + "\n")
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("record Link NAS exposure default in %s: %w", path, err)
	}
	return mode, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// embeddedHeadscaleHealthAddr is a var only so tests can point it at their own
// listener.
var embeddedHeadscaleHealthAddr = "127.0.0.1:8091"

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
	status := LinkStatus{
		Enabled:       true,
		ServerURL:     url,
		RemoteReady:   linkconfig.RemoteReady(url),
		PolicyManaged: LinkPolicyManaged(headscaleConfig()),
	}
	runtime, haveRuntime := currentLinkRuntime()
	status.Headscale, status.NASNode = runtime.Headscale, runtime.NASNode
	if parsed, err := linkconfig.ParseServerURL(url); err == nil {
		status.LANOnly = linkconfig.HostIsLANOnly(parsed.Hostname())
	}
	if err := linkconfig.ValidateServerURL(
		url,
		os.Getenv("JM_NET_ALLOW_INSECURE") == "1",
		os.Getenv("JM_NET_ALLOW_LAN_ONLY") == "1",
	); err != nil {
		status.Error = "Link configuration is not remote-ready: " + err.Error()
		writeJSON(w, http.StatusOK, status)
		return
	}
	if os.Getenv("JM_NET_HEADSCALE") == "on" {
		conn, err := net.DialTimeout("tcp", embeddedHeadscaleHealthAddr, 250*time.Millisecond)
		if err != nil {
			status.Error = "Headscale is configured but not ready: " + err.Error() + componentErrorSuffix(status.Headscale)
			writeJSON(w, http.StatusOK, status)
			return
		}
		_ = conn.Close()
	}
	status.ControlReady = true
	status.RouteReady = true
	if os.Getenv("JM_NET_HEADSCALE") == "on" && status.Headscale != nil && status.Headscale.State != "running" {
		status.ControlReady = false
		status.Error = "Headscale is " + status.Headscale.State + componentErrorSuffix(status.Headscale)
	}
	exposure, subnet, exposureErr := LinkExposure(headscaleConfig())
	status.Exposure = exposure
	routeErr := func(err error) {
		status.RouteReady = false
		if err != nil && status.Error == "" {
			status.Error = err.Error()
		}
	}
	switch {
	case exposureErr != nil:
		routeErr(fmt.Errorf("Link NAS exposure is misconfigured: %w", exposureErr))
	case exposure == "":
		// No embedded NAS node and no route named: whatever carries the NAS
		// is the operator's own, and the Manager neither checks it nor
		// applies a policy to it (LinkPolicyManaged), as before Link
		// exposure existed.
	case !NASNodeEmbedded() && exposure == LinkExposureServices:
		routeErr(errors.New("Link service exposure is served by the NAS node: set JM_NET_NAS_NODE=container (or on)"))
	case !NASNodeEmbedded():
		// The operator's own subnet router carries JM_NET_NAS_ROUTE. The
		// Manager approves nothing for it, so the route is ready when the
		// control server serves the approved prefix from any node.
		if ready, err := linkRouteReady(headscaleBin(), headscaleConfig(), "", subnet); !ready {
			routeErr(err)
		}
	case !haveRuntime || status.NASNode == nil:
		routeErr(errors.New("Link NAS node status is unavailable in this process"))
	case status.NASNode.State != "running":
		routeErr(errors.New("Link NAS node is " + status.NASNode.State + componentErrorSuffix(status.NASNode)))
	case status.NASNode.NodeKey == "":
		routeErr(errors.New("Link NAS node has not reported its node key"))
	case exposure == LinkExposureSubnet:
		if ready, err := linkRouteReady(headscaleBin(), headscaleConfig(), status.NASNode.NodeKey, subnet); !ready {
			routeErr(err)
		}
	default:
		// Service exposure: the NAS node is the route. A Mac finds it only
		// by tag:jm-nas, so it is ready once the node runs its service
		// listeners and the control server lists it online with that tag
		// (and no other node with it).
		if err := linkNASServing(headscaleBin(), headscaleConfig(), status.NASNode.NodeKey); err != nil {
			routeErr(err)
		}
	}
	status.RedisReady = probeLinkRedis(a.linkMetaURL)
	status.ObjectStoreReady = probeLinkObjectStore(a.linkMinIOURL)
	status.DataPlaneReady = status.RemoteReady && status.ControlReady && status.RouteReady && status.RedisReady && status.ObjectStoreReady
	if status.Error == "" && !status.RemoteReady {
		status.Error = "Link is explicitly LAN-only; remote cold start is unavailable until JM_NET_SERVER_URL is a public HTTPS address"
	}
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

func componentErrorSuffix(c *LinkComponentStatus) string {
	if c == nil || c.LastError == "" {
		return ""
	}
	return ": " + c.LastError
}

func probeLinkRedis(raw string) bool {
	if strings.TrimSpace(raw) == "" {
		return false
	}
	opts, err := redisurl.Parse(raw)
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

// Link tags. Mac and farm keys are tagged when the Manager owns the
// Headscale policy (LinkPolicyManaged), so the policy can say exactly which
// devices reach which NAS services. The NAS node's key carries tag:jm-nas
// whenever it uses service exposure, under any policy: that tag is how a Mac
// finds it. A tag is control-assigned identity. Headscale 0.29 applies a
// tagged key's tags under any policy, and the generated policy lists no tag
// owners, so no node can give itself a tag (tailscale --advertise-tags);
// only keys and `headscale nodes tag` assign them. The Manager tags only
// the node holding its NAS node's key (EnsureNodeTags). An operator's own
// policy may still let a node claim tag:jm-nas, so status reports any other
// node that carries it (linkNASConflicts).
const (
	LinkTagMac  = "tag:jm-mac"
	LinkTagNAS  = "tag:jm-nas"
	LinkTagFarm = "tag:jm-farm"
)

// LinkUserName is the Headscale user every JuiceMount node registers under.
// The embedded control server creates it; an external Headscale
// (JM_HEADSCALE_CONFIG) may name it with JM_HEADSCALE_USER.
func LinkUserName() string { return envString("JM_HEADSCALE_USER", "jm") }

// LinkPolicyManaged reports whether the Manager generates and applies the
// Headscale ACL policy: only for the embedded control server with the
// embedded NAS node, unless the operator opted out (JM_NET_POLICY=off) or
// the persisted config.yaml names its own policy. An external Headscale's
// policy belongs to its operator, so keys for it stay untagged as before.
// Without the embedded NAS node the NAS is reached through a router the
// Manager does not run, whose services the policy cannot know, so the
// control server keeps allowing all traffic, as it did before the policy.
func LinkPolicyManaged(cfgPath string) bool {
	if !NASNodeEmbedded() || os.Getenv("JM_NET_POLICY") == "off" {
		return false
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return true // not generated yet: the supervisor will manage it
	}
	return !HeadscaleConfigHasPolicy(string(raw))
}

// HeadscaleConfigHasPolicy reports whether config.yaml carries a top-level
// policy stanza of its own.
func HeadscaleConfigHasPolicy(cfg string) bool {
	for _, line := range strings.Split(cfg, "\n") {
		if strings.HasPrefix(line, "policy:") {
			return true
		}
	}
	return false
}

// PreauthKeyArgs builds the headscale CLI argument vector for minting a
// preauth key for the user with ID userID (Headscale 0.26+ takes the numeric
// ID, not the name; look it up with HeadscaleUserID). reusable=false produces
// a ONE-OFF key (T2.3 hardening): it registers exactly one node and is then
// dead, so a leaked pairing code can never be replayed to add a second
// machine. tags, when non-empty, are assigned to the node at registration.
func PreauthKeyArgs(userID uint64, reusable bool, tags []string) []string {
	args := []string{"preauthkeys", "create", "--user", strconv.FormatUint(userID, 10)}
	if reusable {
		args = append(args, "--reusable")
	}
	if len(tags) > 0 {
		args = append(args, "--tags", strings.Join(tags, ","))
	}
	return args
}

// HeadscaleUserID resolves a Headscale user name to its numeric ID. The ID
// of the Link user is 1 only on a control server that never had another
// user created first, which an operator's own Headscale need not be.
func HeadscaleUserID(bin, config, name string) (uint64, error) {
	out, err := hsOutput(context.Background(), false, bin, "--config", config, "users", "list", "-o", "json")
	if err != nil {
		return 0, fmt.Errorf("headscale users list: %v", err)
	}
	return parseHeadscaleUserID(out, name)
}

func parseHeadscaleUserID(data []byte, name string) (uint64, error) {
	var users []struct {
		ID   json.Number `json:"id"`
		Name string      `json:"name"`
	}
	if err := json.Unmarshal(data, &users); err != nil {
		return 0, fmt.Errorf("parse headscale users list: %w", err)
	}
	for _, u := range users {
		if u.Name != name {
			continue
		}
		id, err := strconv.ParseUint(u.ID.String(), 10, 64)
		if err != nil || id == 0 {
			return 0, fmt.Errorf("headscale user %q has unusable id %q", name, u.ID.String())
		}
		return id, nil
	}
	return 0, fmt.Errorf("headscale user %q does not exist", name)
}

// MintPreauthKey mints one preauth key for the Link user. It never logs or
// persists the returned credential.
func MintPreauthKey(bin, config string, reusable bool, tags []string) (string, error) {
	userID, err := HeadscaleUserID(bin, config, LinkUserName())
	if err != nil {
		return "", err
	}
	args := append([]string{"--config", config}, PreauthKeyArgs(userID, reusable, tags)...)
	args = append(args, "--expiration", "1h")
	out, err := hsOutput(context.Background(), true, bin, args...)
	if err != nil {
		return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	key := lastNonEmptyLine(string(out))
	if key == "" {
		return "", fmt.Errorf("headscale returned no pairing code")
	}
	return key, nil
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
	serverURL := strings.TrimRight(os.Getenv("JM_NET_SERVER_URL"), "/")
	if err := linkconfig.ValidateServerURL(
		serverURL,
		os.Getenv("JM_NET_ALLOW_INSECURE") == "1",
		os.Getenv("JM_NET_ALLOW_LAN_ONLY") == "1",
	); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": false, "reusable": reusable,
			"remote_ready": false,
			"error":        "refusing to mint a Link pairing code: " + err.Error(),
		})
		return
	}

	key, err := mintLinkPreauthKey(reusable, LinkTagMac)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "reusable": reusable, "error": err.Error()})
		return
	}
	resp := map[string]any{
		"ok":           key != "",
		"code":         key,
		"server_url":   serverURL,
		"remote_ready": linkconfig.RemoteReady(serverURL),
		"reusable":     reusable,
	}
	if device != "" {
		resp["device"] = device
	}
	writeJSON(w, http.StatusOK, resp)
}

// mintLinkPreauthKey is shared by Mac pairing and Farm node enrollment. It
// never logs or persists the returned one-time credential; callers must mark
// their HTTP response no-store and show it only to an authenticated operator.
// tag (LinkTagMac or LinkTagFarm) is applied when the Manager owns the
// Headscale policy.
func mintLinkPreauthKey(reusable bool, tag string) (string, error) {
	var tags []string
	if LinkPolicyManaged(headscaleConfig()) {
		tags = []string{tag}
	}
	return MintPreauthKey(headscaleBin(), headscaleConfig(), reusable, tags)
}

// handlePairedNodes lists all registered tailnet nodes.
func (a *API) handlePairedNodes(w http.ResponseWriter, r *http.Request) {
	out, err := hsOutput(r.Context(), true, headscaleBin(), "--config", headscaleConfig(),
		"nodes", "list")
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
	out, err := hsOutput(r.Context(), true, headscaleBin(), "--config", headscaleConfig(),
		"nodes", "expire", "--identifier", nodeID)
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

// NasLanSubnet is the prefix subnet exposure advertises when JM_NET_NAS_ROUTE
// is unset (LinkExposure), the default before service exposure, and the
// fallback for POST /api/net/routes/approve.
const NasLanSubnet = "192.168.0.0/24"

// DefaultNasHostname is the tailnet hostname of the embedded NAS node
// (cmd/juicemount-manager nasnode.go registers under this name).
const DefaultNasHostname = "juicemount-nas"

// HSNode is the subset of headscale's `nodes list -o json` output this
// package consumes.
type HSNode struct {
	ID              uint64   `json:"id"`
	NodeKey         string   `json:"node_key"`
	IPAddresses     []string `json:"ip_addresses"`
	Name            string   `json:"name"`
	GivenName       string   `json:"given_name"`
	AvailableRoutes []string `json:"available_routes"`
	ApprovedRoutes  []string `json:"approved_routes"`
	Tags            []string `json:"tags"`
	Online          bool     `json:"online"`
	// PreAuthKey is the pre-auth key the node registered with, if any.
	PreAuthKey *HSPreAuthKey `json:"pre_auth_key,omitempty"`
}

// HSPreAuthKey is a pre-auth key as the control server lists it (the key
// itself masked).
type HSPreAuthKey struct {
	ID   uint64 `json:"id"`
	Used bool   `json:"used"`
}

// LookupNode returns the control server's record of the node with nodeKey,
// or nil when there is none.
func LookupNode(ctx context.Context, bin, config, nodeKey string) (*HSNode, error) {
	nodes, err := listHSNodes(ctx, bin, config)
	if err != nil {
		return nil, err
	}
	return nodeByKey(nodes, nodeKey), nil
}

// MintPreauthKeyID mints one pre-auth key for the Link user, as
// MintPreauthKey does, and also returns its ID on the control server.
func MintPreauthKeyID(bin, config string, reusable bool, tags []string) (string, uint64, error) {
	userID, err := HeadscaleUserID(bin, config, LinkUserName())
	if err != nil {
		return "", 0, err
	}
	args := append([]string{"--config", config, "-o", "json"}, PreauthKeyArgs(userID, reusable, tags)...)
	args = append(args, "--expiration", "1h")
	out, err := hsOutput(context.Background(), false, bin, args...)
	if err != nil {
		return "", 0, fmt.Errorf("preauthkeys create: %v", err)
	}
	var created struct {
		ID  uint64 `json:"id"`
		Key string `json:"key"`
	}
	if err := json.Unmarshal(out, &created); err != nil || created.Key == "" || created.ID == 0 {
		return "", 0, fmt.Errorf("headscale returned no pre-auth key")
	}
	return created.Key, created.ID, nil
}

// ExpireUnusedPreauthKey expires the pre-auth key with id unless a node
// registered with it (or it is gone). A used key stays as it is.
func ExpireUnusedPreauthKey(bin, config string, id uint64) error {
	out, err := hsOutput(context.Background(), false, bin, "--config", config, "preauthkeys", "list", "-o", "json")
	if err != nil {
		return fmt.Errorf("preauthkeys list: %v", err)
	}
	var keys []HSPreAuthKey
	if err := json.Unmarshal(out, &keys); err != nil {
		return fmt.Errorf("preauthkeys list: %w", err)
	}
	for _, k := range keys {
		if k.ID != id {
			continue
		}
		if k.Used {
			return nil
		}
		if out, err := hsOutput(context.Background(), true, bin, "--config", config, "preauthkeys", "expire", "--id", strconv.FormatUint(id, 10), "--force"); err != nil {
			return fmt.Errorf("preauthkeys expire %d: %v: %s", id, err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	return nil
}

// The NAS node is found by its node key: the WireGuard public key the
// embedded tsnet node holds, reported by runNASNode once it is up and listed
// by the control server as node_key. A node's name is its own choice (a
// paired Mac, or anyone holding a pairing code, may register as
// juicemount-nas), so a name never selects the node the Manager tags,
// approves routes for, or reports on.

// hsCommandTimeout bounds each headscale CLI call the Link code makes
// (lookups, tagging, route approval, key minting), so a wedged control
// server never holds a caller (the NAS node's start or watch loop, the route
// approval loop, a status or pairing request) for long.
var hsCommandTimeout = 10 * time.Second // a var for tests

// hsOutput runs one headscale CLI lookup under ctx and hsCommandTimeout.
func hsOutput(ctx context.Context, combined bool, bin string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, hsCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.WaitDelay = 2 * time.Second
	if combined {
		return cmd.CombinedOutput()
	}
	return cmd.Output()
}

func listHSNodes(ctx context.Context, bin, config string) ([]HSNode, error) {
	out, err := hsOutput(ctx, false, bin, "--config", config, "nodes", "list", "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("nodes list: %v", err)
	}
	return parseHSNodes(out)
}

// nodeByKey finds the node holding nodeKey.
func nodeByKey(nodes []HSNode, nodeKey string) *HSNode {
	if nodeKey == "" {
		return nil
	}
	for i := range nodes {
		if nodes[i].NodeKey == nodeKey {
			return &nodes[i]
		}
	}
	return nil
}

func nodeByID(nodes []HSNode, id uint64) *HSNode {
	for i := range nodes {
		if nodes[i].ID == id {
			return &nodes[i]
		}
	}
	return nil
}

// linkNASServing checks the control server's view of a service-exposure NAS
// node (nodeKey): registered, online, tagged tag:jm-nas (the tag a Mac looks
// for), and the only node a Mac could take for it.
func linkNASServing(bin, config, nodeKey string) error {
	nodes, err := listHSNodes(context.Background(), bin, config)
	if err != nil {
		return fmt.Errorf("Link NAS node: %w", err)
	}
	n := nodeByKey(nodes, nodeKey)
	if n == nil {
		return fmt.Errorf("Link NAS node %s is not registered on the control server", nodeKey)
	}
	if !containsRoute(n.Tags, LinkTagNAS) {
		return fmt.Errorf("Link NAS node %d is not tagged %s, so Macs cannot find it", n.ID, LinkTagNAS)
	}
	if !n.Online {
		return fmt.Errorf("Link NAS node %d is offline on the control server", n.ID)
	}
	rows, err := allRouteRows(context.Background(), bin, config)
	if err != nil {
		return fmt.Errorf("Link NAS node: %w", err)
	}
	return linkNASConflicts(nodes, rows, n.ID, "")
}

// LinkNASConflicts reports any node other than the NAS node (nodeKey) that a
// Mac could take for it (linkNASConflicts). subnet is the subnet exposure's
// prefix, or "" for service exposure. ctx and hsCommandTimeout bound it.
func LinkNASConflicts(ctx context.Context, bin, config, nodeKey, subnet string) error {
	nodes, err := listHSNodes(ctx, bin, config)
	if err != nil {
		return err
	}
	n := nodeByKey(nodes, nodeKey)
	if n == nil {
		return fmt.Errorf("Link NAS node %s is not registered on the control server", nodeKey)
	}
	rows, err := allRouteRows(ctx, bin, config)
	if err != nil {
		return err
	}
	return linkNASConflicts(nodes, rows, n.ID, subnet)
}

// linkNASConflicts names the nodes other than the NAS node (nasID) that a
// Mac could take for it, or whose route could carry its traffic:
//   - any that carries tag:jm-nas, which a Mac dials for the NAS's ports
//     (nfs linkNASServicePeer);
//   - with a subnet (subnet exposure), any that advertises or holds a route
//     overlapping it;
//   - without one (service exposure), any holding an approved route over
//     the NAS's LAN address (JM_NET_NAS_LAN_ADDR), or, with that address
//     unknown, any approved route at all. A current Mac dials the online
//     tag:jm-nas peer ahead of routes, but while the NAS node is offline it
//     follows a route over the NAS's LAN address, so such a route can
//     receive the volume's traffic. The Manager cannot tell which routes
//     cover that address without JM_NET_NAS_LAN_ADDR, so an unknown address
//     blocks "ready" rather than warning.
//
// Default (exit) routes are ignored: a Link node never uses one. rows, from
// `nodes list-routes`, adds to the routes nodes lists.
//
// The Manager never tags such a node or approves its route, but an
// operator's own policy may let a node claim the tag, an operator may
// approve a route, and a record of this NAS's earlier identity (its node
// state was reset) keeps the tag. Status refuses to call the route ready
// and names each node; it does not delete or expire it. Headscale 0.29
// cannot remove a tagged node's last tag, expiring logs a node out but
// leaves its record and tag in place, and deleting is irreversible: the
// operator, who knows whether it is a device of theirs, removes it.
func linkNASConflicts(nodes []HSNode, rows map[uint64]routeRow, nasID uint64, subnet string) error {
	var nasPrefix, lanAddr netip.Prefix
	if subnet != "" {
		p, err := netip.ParsePrefix(subnet)
		if err != nil {
			return fmt.Errorf("bad NAS route %q: %w", subnet, err)
		}
		nasPrefix = p.Masked()
	} else if raw, err := LinkNASLANAddr(); err != nil {
		return err
	} else if raw != "" {
		lanAddr = netip.MustParsePrefix(raw)
	}
	nas := nodeByID(nodes, nasID)
	var found []string
	stale, unknownAddr := false, false
	for i := range nodes {
		n := &nodes[i]
		if n.ID == nasID {
			continue
		}
		row := rows[n.ID]
		var why []string
		if containsRoute(n.Tags, LinkTagNAS) {
			why = append(why, "carries "+LinkTagNAS)
		}
		switch {
		case nasPrefix.IsValid():
			for _, list := range [][]string{n.AvailableRoutes, n.ApprovedRoutes, row.available, row.approved} {
				if r := overlappingRoute(list, nasPrefix); r != "" {
					why = append(why, "advertises "+r)
					break
				}
			}
		default:
			for _, list := range [][]string{n.ApprovedRoutes, row.approved} {
				r := ""
				if lanAddr.IsValid() {
					r = overlappingRoute(list, lanAddr)
				} else {
					r = firstNonDefaultRoute(list)
				}
				if r != "" {
					why = append(why, "holds the approved route "+r)
					unknownAddr = unknownAddr || !lanAddr.IsValid()
					break
				}
			}
		}
		if len(why) == 0 {
			continue
		}
		entry := fmt.Sprintf("node %d (%q) %s", n.ID, n.Name, strings.Join(why, " and "))
		if nas != nil && n.Name == nas.Name && !n.Online && n.ID < nasID {
			// Same name, offline, registered before the NAS node: the
			// usual leftover when the NAS node's state was reset.
			entry += " (offline and older than the NAS node under its name: most likely this NAS's own earlier registration, left when its node state was reset)"
			stale = true
		}
		found = append(found, entry)
	}
	if len(found) == 0 {
		return nil
	}
	advice := "Remove each one that is not a device of yours"
	if stale {
		advice = "Remove this NAS's earlier registration, and any other node that is not a device of yours"
	}
	msg := fmt.Sprintf("Link NAS node %d is not the only node Macs could take for the NAS: %s. %s: `docker exec <Manager container> headscale nodes delete --identifier <id>`", nasID, strings.Join(found, "; "), advice)
	if unknownAddr {
		msg += ", or set JM_NET_NAS_LAN_ADDR to the NAS's LAN address so that only routes over it count"
	}
	return errors.New(msg)
}

// firstNonDefaultRoute returns the first route in routes that is not a
// default (exit) route.
func firstNonDefaultRoute(routes []string) string {
	for _, raw := range routes {
		if p, err := netip.ParsePrefix(raw); err == nil && p.Bits() > 0 {
			return raw
		}
	}
	return ""
}

// overlappingRoute returns the first route in routes that overlaps want,
// ignoring default (exit) routes, which a Link node never uses.
func overlappingRoute(routes []string, want netip.Prefix) string {
	for _, raw := range routes {
		p, err := netip.ParsePrefix(raw)
		if err == nil && p.Bits() > 0 && p.Overlaps(want) {
			return raw
		}
	}
	return ""
}

// EnsureNodeTags makes the node holding nodeKey carry every tag in want,
// keeping tags it already has, and reports whether it changed them. A node
// that registered before JuiceMount tagged its keys keeps its identity (and
// tailnet address) this way instead of being re-enrolled.
func EnsureNodeTags(bin, config, nodeKey string, want []string) (bool, error) {
	nodes, err := listHSNodes(context.Background(), bin, config)
	if err != nil {
		return false, err
	}
	node := nodeByKey(nodes, nodeKey)
	if node == nil {
		return false, fmt.Errorf("node %s not registered yet", nodeKey)
	}
	merged := mergeApproved(node.Tags, want)
	if len(merged) == len(node.Tags) {
		return false, nil
	}
	id := node.ID
	out, err := hsOutput(context.Background(), true, bin, "--config", config, "nodes", "tag",
		"--identifier", strconv.FormatUint(id, 10),
		"--tags", strings.Join(merged, ","))
	if err != nil {
		return false, fmt.Errorf("nodes tag: %v: %s", err, strings.TrimSpace(string(out)))
	}
	if nodes, err = listHSNodes(context.Background(), bin, config); err != nil {
		return false, err
	}
	node = nodeByID(nodes, id)
	if node == nil || node.NodeKey != nodeKey {
		return false, fmt.Errorf("node %d changed while it was tagged", id)
	}
	for _, tag := range want {
		if !containsRoute(node.Tags, tag) {
			return false, fmt.Errorf("node %d is still missing tag %s after tagging", id, tag)
		}
	}
	return true, nil
}

var ansiEscape = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// routeRow is one node's row of `nodes list-routes`.
type routeRow struct {
	approved, available, serving []string
}

// parseRouteTable reads the stable human table emitted by `nodes list-routes`.
// Headscale 0.29's general node JSON includes available_routes but omits the
// approved/serving route state, so that JSON cannot prove approval. The
// dedicated table is the CLI's authoritative approval surface.
func parseRouteTable(data []byte, nodeID uint64) (approved, available, serving []string, err error) {
	rows, err := parseRouteRows(data)
	if err != nil {
		return nil, nil, nil, err
	}
	row, ok := rows[nodeID]
	if !ok {
		return nil, nil, nil, fmt.Errorf("parse route table: node %d not found", nodeID)
	}
	return row.approved, row.available, row.serving, nil
}

// parseRouteRows reads every node's row of the `nodes list-routes` table.
func parseRouteRows(data []byte) (map[uint64]routeRow, error) {
	lines := strings.Split(ansiEscape.ReplaceAllString(string(data), ""), "\n")
	approvedCol, availableCol, servingCol, idCol := -1, -1, -1, -1
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
			case "serving (primary)":
				servingCol = j
			}
		}
		if idCol >= 0 && approvedCol >= 0 && availableCol >= 0 && servingCol >= 0 {
			headerLine = i
			break
		}
	}
	if headerLine < 0 {
		return nil, fmt.Errorf("parse route table: required columns not found")
	}
	rows := map[uint64]routeRow{}
	for _, line := range lines[headerLine+1:] {
		if !strings.Contains(line, "|") {
			continue
		}
		cols := strings.Split(line, "|")
		if idCol >= len(cols) || approvedCol >= len(cols) || availableCol >= len(cols) || servingCol >= len(cols) {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSpace(cols[idCol]), 10, 64)
		if err != nil {
			continue
		}
		rows[id] = routeRow{
			approved:  splitRouteList(cols[approvedCol]),
			available: splitRouteList(cols[availableCol]),
			serving:   splitRouteList(cols[servingCol]),
		}
	}
	return rows, nil
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

func routeTableForNode(bin, config string, nodeID uint64) (approved, available, serving []string, err error) {
	out, err := hsOutput(context.Background(), true, bin, "--config", config, "nodes", "list-routes",
		"--identifier", strconv.FormatUint(nodeID, 10))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("nodes list-routes: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return parseRouteTable(out, nodeID)
}

func allRouteRows(ctx context.Context, bin, config string) (map[uint64]routeRow, error) {
	out, err := hsOutput(ctx, true, bin, "--config", config, "nodes", "list-routes")
	if err != nil {
		return nil, fmt.Errorf("nodes list-routes: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return parseRouteRows(out)
}

// linkRouteReady reports whether the NAS node (nodeKey) serves prefix as an
// approved route and no other node could be taken for it. With nodeKey ""
// (no embedded NAS node: the operator runs the subnet router) it reports
// whether any node serves the approved prefix.
func linkRouteReady(bin, config, nodeKey, prefix string) (bool, error) {
	nodes, err := listHSNodes(context.Background(), bin, config)
	if err != nil {
		return false, fmt.Errorf("Link route check failed: %w", err)
	}
	rows, err := allRouteRows(context.Background(), bin, config)
	if err != nil {
		return false, fmt.Errorf("Link route check failed: %w", err)
	}
	if nodeKey == "" {
		for _, row := range rows {
			if containsRoute(row.approved, prefix) && containsRoute(row.serving, prefix) {
				return true, nil
			}
		}
		return false, fmt.Errorf("Link NAS route %s is not approved and served by any node", prefix)
	}
	n := nodeByKey(nodes, nodeKey)
	if n == nil {
		return false, fmt.Errorf("Link NAS node %s is not registered", nodeKey)
	}
	row, ok := rows[n.ID]
	if !ok {
		return false, fmt.Errorf("Link NAS node %d has no row in the route table", n.ID)
	}
	if !containsRoute(row.available, prefix) {
		return false, fmt.Errorf("Link NAS node %d is not advertising %s", n.ID, prefix)
	}
	if !containsRoute(row.approved, prefix) {
		return false, fmt.Errorf("Link NAS route %s is advertised but not approved", prefix)
	}
	if !containsRoute(row.serving, prefix) {
		return false, fmt.Errorf("Link NAS route %s is approved but the NAS node is not serving it", prefix)
	}
	if err := linkNASConflicts(nodes, rows, n.ID, prefix); err != nil {
		return false, err
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

// ApproveSubnetRoutes approves prefixes on the node holding nodeKey (the
// NAS node). It is idempotent: an already-approved prefix is a no-op.
// Returns an error when the node has not registered yet (caller retries) or
// when the CLI fails.
func ApproveSubnetRoutes(bin, config, nodeKey string, prefixes []string) error {
	return approveSubnetRoutes(bin, config, func(nodes []HSNode) *HSNode { return nodeByKey(nodes, nodeKey) }, "node "+nodeKey, prefixes)
}

// approveSubnetRoutes approves prefixes on the node pick selects; label
// names it in errors.
func approveSubnetRoutes(bin, config string, pick func([]HSNode) *HSNode, label string, prefixes []string) error {
	nodes, err := listHSNodes(context.Background(), bin, config)
	if err != nil {
		return err
	}
	n := pick(nodes)
	if n == nil {
		return fmt.Errorf("%s not registered yet", label)
	}
	approved, available, _, err := routeTableForNode(bin, config, n.ID)
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
				return fmt.Errorf("node %d is not advertising route %s", n.ID, prefix)
			}
		}
		return nil
	}
	merged := mergeApproved(approved, pending)
	setOut, err := hsOutput(context.Background(), true, bin, "--config", config,
		"nodes", "approve-routes",
		"--identifier", strconv.FormatUint(n.ID, 10),
		"--routes", strings.Join(merged, ","))
	if err != nil {
		return fmt.Errorf("approve-routes: %v: %s", err, strings.TrimSpace(string(setOut)))
	}
	verified, _, _, err := routeTableForNode(bin, config, n.ID)
	if err != nil {
		return fmt.Errorf("verify route approval: %w", err)
	}
	for _, route := range merged {
		if !containsRoute(verified, route) {
			return fmt.Errorf("route approval did not persist for node %d: %s is still absent", n.ID, route)
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
//	POST /api/net/routes/approve[?id=<node id>&prefix=192.168.0.0/24]
//
// Defaults target the embedded NAS node, by its node key, and
// JM_NET_NAS_ROUTE (falling back to the home-LAN default). ?id= names
// another node by its control-server ID (GET /api/net/paired); a node's
// hostname is its own choice, so ?host= is refused. GET returns the raw
// route listing instead, so operators can see advertisement/approval state
// from the UI later.
func (a *API) handleNetRouteApprove(w http.ResponseWriter, r *http.Request) {
	bin, config := headscaleBin(), headscaleConfig()
	switch r.Method {
	case http.MethodGet:
		out, err := hsOutput(r.Context(), false, bin, "--config", config, "nodes", "list-routes", "-o", "json")
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	case http.MethodPost:
		prefix := r.URL.Query().Get("prefix")
		if prefix == "" {
			prefix = envString("JM_NET_NAS_ROUTE", NasLanSubnet)
		}
		if r.URL.Query().Get("host") != "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "prefix": prefix,
				"error": "host is not accepted: any node can register under any name; pass ?id=<node id> from GET /api/net/paired, or nothing for the embedded NAS node"})
			return
		}
		var err error
		node := ""
		if raw := r.URL.Query().Get("id"); raw != "" {
			id, perr := strconv.ParseUint(raw, 10, 64)
			if perr != nil || id == 0 {
				writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "prefix": prefix, "error": "id must be a node ID"})
				return
			}
			node = "node " + raw
			err = approveSubnetRoutes(bin, config, func(nodes []HSNode) *HSNode { return nodeByID(nodes, id) }, node, []string{prefix})
		} else {
			runtime, _ := currentLinkRuntime()
			if runtime.NASNode == nil || runtime.NASNode.NodeKey == "" {
				err = errors.New("the embedded NAS node has not reported its node key (it is not running in this Manager); pass ?id=<node id>")
			} else {
				node = runtime.NASNode.NodeKey
				err = ApproveSubnetRoutes(bin, config, node, []string{prefix})
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": err == nil, "node": node, "prefix": prefix, "error": errString(err)})
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
