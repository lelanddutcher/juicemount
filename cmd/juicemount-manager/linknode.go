package main

// The NAS node in its own container (JM_NET_NAS_NODE=container).
//
// Why: inside the Manager's container the NAS node sits behind Docker's NAT
// as well as the home router's. It cannot ask the router for a port
// (NAT-PMP, UPnP, PCP), usually has no IPv6, and tells peers only its
// container address, which a Mac on the home LAN cannot reach. Run with host
// networking, the node sits on the NAS's LAN: it maps a port on the router,
// uses IPv6, and advertises the NAS's LAN address. Only the node moves; the
// Manager, Headscale and the data services stay on the bridge network.
//
// The node container runs this binary as `juicemount-manager --link-node`
// (linknode_run.go): the same tsnet node and the same supervisor (backoff
// 1 s to 60 s) as the in-Manager node. It reaches the NAS services through
// the host's published ports (127.0.0.1:30179, :30151, :30193). Everything
// that needs the Manager goes over two Unix sockets in a volume that only the
// two containers mount (JM_NET_NODE_DIR, default /run/juicemount-link):
//
// control.sock (linkNodeBroker.controlHandler) is the node's own control
// path. Only the node container calls it; nothing a tailnet peer sends ever
// reaches it, and it refuses any request that names a tailnet peer:
//
//	GET  /link-node/v1/config        hostname, exposure (subnet), tags, TLS
//	                                 (503 while Headscale is not running)
//	POST /link-node/v1/key           a one-off pre-auth key with those tags;
//	                                 the previous one is expired if unused
//	POST /link-node/v1/ensure-tags   tag the NAS node registered before
//	                                 tagged keys
//	POST /link-node/v1/approve-route approve the NAS route (subnet exposure)
//	POST /link-node/v1/conflicts     other nodes a Mac could take for the NAS
//	POST /link-node/v1/status        the node's supervisor state, every 5 s
//	GET  /link-node/v1/identity      the Manager's copy of the node's identity
//	                                 (the in-Manager node's), taken once, so an
//	                                 upgrade keeps the same node
//	POST /link-node/v1/identity      the node's identity, whenever it changes,
//	                                 so the Manager's copy stays current and a
//	                                 rollback to JM_NET_NAS_NODE=on keeps the
//	                                 same node too
//
// ensure-tags, approve-route, conflicts and identity act only on the NAS
// node's own key (trustNodeKey): the key in the Manager's copy of the
// identity, or a node named like the NAS node that registered with a key
// this broker minted.
//
// relay.sock (linkNodeBroker.relayHandler) carries the Manager's own API for
// tailnet peers (port 30190 on the tailnet). The node proxies each request
// with the peer's tailnet address in X-JuiceMount-Link-Peer, so the Manager
// still sees each Mac's own address (per-address enrollment limits). It never
// serves /link-node/ paths, and the header is honoured on this socket only,
// never on the Manager's TCP listener.
//
// With JM_NET_NAS_NODE=on (a rollback) the Manager answers control.sock with
// 409 only, so a node container left running stops its node and the Manager
// does not start its own while one is around (linkNodeGuard): the two would
// share one identity.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
	"tailscale.com/ipn"
	"tailscale.com/types/key"
)

const (
	defaultLinkNodeDir    = "/run/juicemount-link"
	linkNodeControlSocket = "control.sock"
	linkNodeRelaySocket   = "relay.sock"
	linkNodeAPIPrefix     = "/link-node/v1/"
	// linkNodePeerHeader carries a proxied request's tailnet peer address.
	linkNodePeerHeader = "X-JuiceMount-Link-Peer"
	// linkNodeReportEvery is how often the node container reports its
	// state; after linkNodeStaleAfter without a report the Manager shows
	// the node as down.
	linkNodeReportEvery = 5 * time.Second
	linkNodeStaleAfter  = 30 * time.Second
	// linkNodeOrphanAfter: a node container that has not reached the
	// Manager's control socket this long stops its node, so a container
	// orphaned by a rollback never runs beside the Manager's own node.
	linkNodeOrphanAfter = 2 * time.Minute
	// linkNodeLeaseFile, in the Manager's identity directory, is touched
	// every linkNodeLeaseEvery while a node container reports, and removed
	// when it reports that it stopped. JM_NET_NAS_NODE=on waits until it is
	// older than linkNodeOrphanAfter + linkNodeLeaseEvery.
	linkNodeLeaseFile  = "node-container.lease"
	linkNodeLeaseEvery = 30 * time.Second
	// linkNodeMintedFile, next to Headscale's config, lists the IDs of the
	// last linkNodeMintedKeep pre-auth keys the broker minted for the node.
	linkNodeMintedFile = "nas-node-keys.json"
	linkNodeMintedKeep = 16
	// linkNodeStateFile is tsnet's state file: the node's machine and node
	// keys, its whole identity.
	linkNodeStateFile = "tailscaled.state"
	// linkNodeMaxState bounds an identity sent over the socket.
	linkNodeMaxState = 1 << 20
	// linkNodeOwnedByManager is the 409 answer's code under
	// JM_NET_NAS_NODE=on.
	linkNodeOwnedByManager = "nas-node-in-manager"
)

// nasNodeMode is JM_NET_NAS_NODE: manager.NASNodeInManager,
// manager.NASNodeInContainer, or "" (no NAS node).
func nasNodeMode() string {
	switch m := os.Getenv("JM_NET_NAS_NODE"); m {
	case manager.NASNodeInManager, manager.NASNodeInContainer:
		return m
	}
	return ""
}

func linkNodeDir() string { return envOr("JM_NET_NODE_DIR", defaultLinkNodeDir) }

// linkNodeSettings is what the Manager tells the node container before each
// start (GET /link-node/v1/config).
type linkNodeSettings struct {
	Hostname string `json:"hostname"`
	// Subnet is the subnet exposure's LAN route; "" in service exposure.
	Subnet string `json:"subnet,omitempty"`
	// Tags are the tags the node's key carries; non-empty means the node
	// must also be tagged server-side (ensure-tags).
	Tags []string `json:"tags,omitempty"`
	// ServerURL and TLS tell the node how to reach control when it has no
	// JM_NET_NAS_CONTROL_URL: with Headscale's own TLS the certificate
	// names the public host, so loopback cannot verify it.
	ServerURL string `json:"server_url"`
	TLS       bool   `json:"tls"`
}

type linkNodeKeyRequest struct {
	NodeKey string `json:"node_key"`
	Prefix  string `json:"prefix,omitempty"`
}

type linkNodeKeyResponse struct {
	Key string `json:"key"`
}

type linkNodeTagsResponse struct {
	Changed bool `json:"changed"`
}

type linkNodeConflictResponse struct {
	// Conflict is the message for another node a Mac could take for this
	// one; "" when there is none.
	Conflict string `json:"conflict"`
}

type linkNodeIdentity struct {
	State []byte `json:"tailscaled_state"`
}

type linkNodeError struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

// linkNodeBroker is the Manager's end of the node container's sockets.
type linkNodeBroker struct {
	// handler is the Manager's API, served to proxied tailnet requests.
	handler http.Handler
	// controlReady reports whether Headscale is running.
	controlReady func() bool
	settings     func() (linkNodeSettings, error)
	mintKey      func(tags []string) (key string, id uint64, err error)
	expireKey    func(id uint64) error
	lookupNode   func(ctx context.Context, nodeKey string) (*manager.HSNode, error)
	ensureTags   func(nodeKey string, tags []string) (bool, error)
	approve      func(nodeKey, prefix string) error
	conflicts    func(ctx context.Context, nodeKey, subnet string) error
	// identityDir is the in-Manager node's tsnet directory, which holds
	// the Manager's copy of the node's identity and the lease.
	identityDir string
	// mintedPath lists the pre-auth keys minted for the node.
	mintedPath string
	now        func() time.Time

	mintMu sync.Mutex

	mu      sync.Mutex
	started time.Time
	last    *manager.LinkComponentStatus
	lastAt  time.Time
	leaseAt time.Time
}

// newLinkNodeBroker serves the node container for the Manager whose
// Headscale config is cfgPath.
func newLinkNodeBroker(cfgPath string, control *headscaleSupervisor, handler http.Handler) *linkNodeBroker {
	b := &linkNodeBroker{
		handler: handler,
		controlReady: func() bool {
			return control == nil || control.status.running()
		},
		settings: func() (linkNodeSettings, error) { return linkNodeSettingsFromEnv(cfgPath) },
		mintKey: func(tags []string) (string, uint64, error) {
			return manager.MintPreauthKeyID(hsBinary, cfgPath, false, tags)
		},
		expireKey: func(id uint64) error { return manager.ExpireUnusedPreauthKey(hsBinary, cfgPath, id) },
		lookupNode: func(ctx context.Context, nodeKey string) (*manager.HSNode, error) {
			return manager.LookupNode(ctx, hsBinary, cfgPath, nodeKey)
		},
		ensureTags: func(nodeKey string, tags []string) (bool, error) {
			return ensureNASNodeTags(cfgPath, nodeKey, tags)
		},
		approve: func(nodeKey, prefix string) error {
			return manager.ApproveSubnetRoutes(hsBinary, cfgPath, nodeKey, []string{prefix})
		},
		conflicts: func(ctx context.Context, nodeKey, subnet string) error {
			return manager.LinkNASConflicts(ctx, hsBinary, cfgPath, nodeKey, subnet)
		},
		identityDir: filepath.Join(filepath.Dir(cfgPath), "nas-node"),
		mintedPath:  filepath.Join(filepath.Dir(cfgPath), linkNodeMintedFile),
		now:         time.Now,
	}
	b.started = b.now()
	return b
}

// linkNodeSettingsFromEnv resolves what the node container needs, as
// nasNodeConfigFromEnv does for the in-Manager node. It records the exposure
// default first, for the same reason.
func linkNodeSettingsFromEnv(cfgPath string) (linkNodeSettings, error) {
	if _, err := manager.RecordLinkExposureDefault(cfgPath); err != nil {
		return linkNodeSettings{}, err
	}
	exposure, subnet, err := manager.LinkExposure(cfgPath)
	if err != nil {
		return linkNodeSettings{}, err
	}
	if subnet != "" {
		if _, err := netip.ParsePrefix(subnet); err != nil {
			return linkNodeSettings{}, fmt.Errorf("bad JM_NET_NAS_ROUTE %q: %w", subnet, err)
		}
	}
	s := linkNodeSettings{
		Hostname:  nasNodeHostname(),
		Subnet:    subnet,
		ServerURL: externalURL(),
		TLS:       os.Getenv("JM_NET_TLS_CERT") != "" || os.Getenv("JM_NET_TLS_KEY") != "",
	}
	if exposure == manager.LinkExposureServices {
		s.Tags = []string{manager.LinkTagNAS}
	}
	return s, nil
}

// snapshot is the node container's state for GET /api/net/link.
func (b *linkNodeBroker) snapshot() *manager.LinkComponentStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.last == nil {
		return &manager.LinkComponentStatus{
			State:  "starting",
			Detail: "waiting for the Link node container (juicemount-link) to report",
			Since:  b.started,
		}
	}
	st := *b.last
	if age := b.now().Sub(b.lastAt); age > linkNodeStaleAfter {
		st.LastError = fmt.Sprintf("no report from the Link node container (juicemount-link) for %s; it last reported %s", age.Round(time.Second), st.State)
		st.State = "backoff"
		st.Detail = ""
		st.NextRetry = time.Time{}
	}
	return &st
}

// record keeps a status report and the lease that tells a later
// JM_NET_NAS_NODE=on Manager a node container may still run.
func (b *linkNodeBroker) record(st manager.LinkComponentStatus) {
	b.mu.Lock()
	b.last = &st
	b.lastAt = b.now()
	lease := filepath.Join(b.identityDir, linkNodeLeaseFile)
	var err error
	switch {
	case st.State == "stopped":
		err = os.Remove(lease)
		if errors.Is(err, fs.ErrNotExist) {
			err = nil
		}
		b.leaseAt = time.Time{}
	case b.lastAt.Sub(b.leaseAt) >= linkNodeLeaseEvery:
		if err = os.MkdirAll(b.identityDir, 0o700); err == nil {
			err = os.WriteFile(lease, []byte(b.lastAt.UTC().Format(time.RFC3339)+"\n"), 0o600)
		}
		if err == nil {
			_ = os.Chtimes(lease, b.lastAt, b.lastAt)
			b.leaseAt = b.lastAt
		}
	}
	b.mu.Unlock()
	if err != nil {
		log.Printf("JuiceMount Link: WARNING could not update the Link node container's lease: %v", err)
	}
}

// controlHandler serves control.sock: the node container's calls only.
func (b *linkNodeBroker) controlHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, named := r.Header[http.CanonicalHeaderKey(linkNodePeerHeader)]; named {
			linkNodeReply(w, http.StatusForbidden, linkNodeError{Error: "a tailnet peer's request never reaches the Link node control socket"})
			return
		}
		if !strings.HasPrefix(r.URL.Path, linkNodeAPIPrefix) {
			linkNodeReply(w, http.StatusNotFound, linkNodeError{Error: "unknown Link node call"})
			return
		}
		b.serveAPI(w, r)
	})
}

// isLinkNodePath reports whether p (a request path) names the node's
// control API, after cleaning.
func isLinkNodePath(p string) bool {
	c := path.Clean("/" + p)
	return c == "/link-node" || strings.HasPrefix(c, "/link-node/")
}

// relayHandler serves relay.sock: the Manager's own API for tailnet peers,
// never the node's control API.
func (b *linkNodeBroker) relayHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isLinkNodePath(r.URL.Path) || isLinkNodePath(r.URL.RawPath) {
			http.NotFound(w, r)
			return
		}
		peer, err := netip.ParseAddrPort(r.Header.Get(linkNodePeerHeader))
		if err != nil || !peer.Addr().IsValid() {
			http.Error(w, "the Link node did not name the tailnet peer", http.StatusBadRequest)
			return
		}
		r2 := r.Clone(r.Context())
		r2.Header.Del(linkNodePeerHeader)
		r2.RemoteAddr = peer.String()
		b.handler.ServeHTTP(w, r2)
	})
}

// trustNodeKey accepts nodeKey only as the NAS node's own: the key in the
// Manager's copy of the node's identity (an upgrade, or the identity the
// node sent back), or a node with the NAS node's name that registered with a
// pre-auth key this broker minted.
func (b *linkNodeBroker) trustNodeKey(ctx context.Context, nodeKey, hostname string) error {
	if nodeKey == "" {
		return errors.New("no node key")
	}
	if data, err := readNodeIdentity(b.identityDir); err == nil {
		if pub, err := identityNodeKey(data); err == nil && pub == nodeKey {
			return nil
		}
	}
	rec, err := b.lookupNode(ctx, nodeKey)
	if err != nil {
		return fmt.Errorf("look up node %s: %w", nodeKey, err)
	}
	if rec == nil {
		return fmt.Errorf("the control server has no node %s", nodeKey)
	}
	if rec.Name != hostname {
		return fmt.Errorf("node %d is named %q, not the NAS node's name %q", rec.ID, rec.Name, hostname)
	}
	if rec.PreAuthKey == nil || !b.minted(rec.PreAuthKey.ID) {
		return fmt.Errorf("node %d did not register with a key this Manager minted for the NAS node", rec.ID)
	}
	return nil
}

func (b *linkNodeBroker) loadMinted() []uint64 {
	data, err := os.ReadFile(b.mintedPath)
	if err != nil {
		return nil
	}
	var ids []uint64
	if json.Unmarshal(data, &ids) != nil {
		return nil
	}
	return ids
}

func (b *linkNodeBroker) minted(id uint64) bool {
	b.mintMu.Lock()
	defer b.mintMu.Unlock()
	for _, m := range b.loadMinted() {
		if m == id {
			return true
		}
	}
	return false
}

// mint mints the node's next key. At most one minted key is outstanding:
// the previous one is expired unless a node registered with it. The key's ID
// is recorded before the key is handed out.
func (b *linkNodeBroker) mint(tags []string) (string, error) {
	b.mintMu.Lock()
	defer b.mintMu.Unlock()
	ids := b.loadMinted()
	if n := len(ids); n > 0 {
		if err := b.expireKey(ids[n-1]); err != nil {
			return "", fmt.Errorf("expire the previous NAS node key: %w", err)
		}
	}
	k, id, err := b.mintKey(tags)
	if err != nil {
		return "", err
	}
	ids = append(ids, id)
	if len(ids) > linkNodeMintedKeep {
		ids = ids[len(ids)-linkNodeMintedKeep:]
	}
	data, _ := json.Marshal(ids)
	if err := writeFileAtomic(b.mintedPath, data); err != nil {
		_ = b.expireKey(id)
		return "", fmt.Errorf("record the NAS node key: %w", err)
	}
	return k, nil
}

func (b *linkNodeBroker) serveAPI(w http.ResponseWriter, r *http.Request) {
	op := strings.TrimPrefix(r.URL.Path, linkNodeAPIPrefix)
	want := http.MethodPost
	if op == "config" || (op == "identity" && r.Method == http.MethodGet) {
		want = http.MethodGet
	}
	if r.Method != want {
		w.Header().Set("Allow", want)
		linkNodeReply(w, http.StatusMethodNotAllowed, linkNodeError{Error: "method not allowed"})
		return
	}
	var req linkNodeKeyRequest
	switch op {
	case "ensure-tags", "approve-route", "conflicts":
		if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil || req.NodeKey == "" {
			linkNodeReply(w, http.StatusBadRequest, linkNodeError{Error: "a node_key is required"})
			return
		}
	}
	switch op {
	case "status":
		var st manager.LinkComponentStatus
		if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&st); err != nil || st.State == "" {
			linkNodeReply(w, http.StatusBadRequest, linkNodeError{Error: "a component status is required"})
			return
		}
		b.record(st)
		w.WriteHeader(http.StatusNoContent)
		return
	case "identity":
		if r.Method == http.MethodGet {
			data, err := readNodeIdentity(b.identityDir)
			if err == nil {
				_, err = identityNodeKey(data)
			}
			switch {
			case errors.Is(err, fs.ErrNotExist):
				linkNodeReply(w, http.StatusNotFound, linkNodeError{Error: "the Manager has no NAS node identity"})
			case err != nil:
				linkNodeReply(w, http.StatusInternalServerError, linkNodeError{Error: "the Manager's copy of the NAS node's identity is unusable: " + err.Error()})
			default:
				linkNodeReply(w, http.StatusOK, linkNodeIdentity{State: data})
			}
			return
		}
	}
	if !b.controlReady() {
		linkNodeReply(w, http.StatusServiceUnavailable, linkNodeError{Error: "waiting for headscale"})
		return
	}
	s, err := b.settings()
	if err != nil {
		linkNodeReply(w, http.StatusInternalServerError, linkNodeError{Error: err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), nasNodeConflictTimeout)
	defer cancel()
	switch op {
	case "ensure-tags", "approve-route", "conflicts":
		if err := b.trustNodeKey(ctx, req.NodeKey, s.Hostname); err != nil {
			linkNodeReply(w, http.StatusForbidden, linkNodeError{Error: "not the NAS node: " + err.Error()})
			return
		}
	}
	switch op {
	case "config":
		linkNodeReply(w, http.StatusOK, s)
	case "key":
		k, err := b.mint(s.Tags)
		if err != nil {
			linkNodeReply(w, http.StatusInternalServerError, linkNodeError{Error: err.Error()})
			return
		}
		linkNodeReply(w, http.StatusOK, linkNodeKeyResponse{Key: k})
	case "identity":
		var id linkNodeIdentity
		if err := json.NewDecoder(io.LimitReader(r.Body, 4*linkNodeMaxState)).Decode(&id); err != nil || len(id.State) == 0 || len(id.State) > linkNodeMaxState {
			linkNodeReply(w, http.StatusBadRequest, linkNodeError{Error: "an identity is required"})
			return
		}
		pub, err := identityNodeKey(id.State)
		if err != nil {
			linkNodeReply(w, http.StatusBadRequest, linkNodeError{Error: "not a NAS node identity: " + err.Error()})
			return
		}
		if err := b.trustNodeKey(ctx, pub, s.Hostname); err != nil {
			linkNodeReply(w, http.StatusForbidden, linkNodeError{Error: "not the NAS node: " + err.Error()})
			return
		}
		if err := writeNodeIdentity(b.identityDir, id.State); err != nil {
			linkNodeReply(w, http.StatusInternalServerError, linkNodeError{Error: err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case "ensure-tags":
		if len(s.Tags) == 0 {
			linkNodeReply(w, http.StatusOK, linkNodeTagsResponse{})
			return
		}
		changed, err := b.ensureTags(req.NodeKey, s.Tags)
		if err != nil {
			linkNodeReply(w, http.StatusInternalServerError, linkNodeError{Error: err.Error()})
			return
		}
		linkNodeReply(w, http.StatusOK, linkNodeTagsResponse{Changed: changed})
	case "approve-route":
		if s.Subnet == "" || req.Prefix != s.Subnet {
			linkNodeReply(w, http.StatusBadRequest, linkNodeError{Error: fmt.Sprintf("route %q is not the NAS route %q", req.Prefix, s.Subnet)})
			return
		}
		if err := b.approve(req.NodeKey, req.Prefix); err != nil {
			linkNodeReply(w, http.StatusInternalServerError, linkNodeError{Error: err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case "conflicts":
		resp := linkNodeConflictResponse{}
		if err := b.conflicts(ctx, req.NodeKey, s.Subnet); err != nil {
			resp.Conflict = err.Error()
		}
		linkNodeReply(w, http.StatusOK, resp)
	default:
		linkNodeReply(w, http.StatusNotFound, linkNodeError{Error: "unknown Link node call"})
	}
}

func linkNodeReply(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// identityNodeKey checks that data is a tsnet state file (a machine key and
// a current profile whose prefs hold a node key) and returns its public node
// key ("nodekey:...").
func identityNodeKey(data []byte) (string, error) {
	var st map[string][]byte
	if err := json.Unmarshal(data, &st); err != nil {
		return "", errors.New("not tsnet state JSON")
	}
	var mk key.MachinePrivate
	if raw, ok := st["_machinekey"]; !ok || mk.UnmarshalText(raw) != nil || mk.IsZero() {
		return "", errors.New("no valid _machinekey")
	}
	profile := string(st["_current-profile"])
	if !strings.HasPrefix(profile, "profile-") {
		return "", errors.New("no current profile")
	}
	raw, ok := st[profile]
	if !ok {
		return "", fmt.Errorf("current profile %q missing", profile)
	}
	var prefs ipn.Prefs
	if err := ipn.PrefsFromBytes(raw, &prefs); err != nil {
		return "", fmt.Errorf("profile %q: %w", profile, err)
	}
	if prefs.Persist == nil || prefs.Persist.PrivateNodeKey.IsZero() {
		return "", fmt.Errorf("profile %q holds no node key", profile)
	}
	return prefs.Persist.PrivateNodeKey.Public().String(), nil
}

// readNodeIdentity reads the tsnet state file in dir. It returns
// fs.ErrNotExist when there is none (a fresh install).
func readNodeIdentity(dir string) ([]byte, error) {
	f, err := os.Open(filepath.Join(dir, linkNodeStateFile))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, linkNodeMaxState+1))
	if err != nil {
		return nil, err
	}
	if len(data) > linkNodeMaxState {
		return nil, fmt.Errorf("%s is larger than %d bytes", linkNodeStateFile, linkNodeMaxState)
	}
	if len(data) == 0 {
		return nil, fs.ErrNotExist
	}
	return data, nil
}

// writeNodeIdentity replaces the tsnet state file in dir.
func writeNodeIdentity(dir string, data []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(dir, linkNodeStateFile), data); err != nil {
		return fmt.Errorf("store the NAS node's identity in %s: %w", dir, err)
	}
	return nil
}

// writeFileAtomic replaces path with data, readable by the owner only.
func writeFileAtomic(p string, data []byte) error {
	dir := filepath.Dir(p)
	tmp, err := os.CreateTemp(dir, filepath.Base(p)+".*")
	if err != nil {
		return err
	}
	_, err = tmp.Write(data)
	if err == nil {
		err = tmp.Chmod(0o600)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), p)
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
	}
	return err
}

// listenLinkNodeSocket replaces a stale socket at p and listens there,
// readable and writable by the owner and group only.
func listenLinkNodeSocket(p string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return nil, err
	}
	if fi, err := os.Lstat(p); err == nil {
		if fi.Mode()&fs.ModeSocket == 0 {
			return nil, fmt.Errorf("%s exists and is not a socket", p)
		}
		if err := os.Remove(p); err != nil {
			return nil, err
		}
	}
	ln, err := net.Listen("unix", p)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(p, 0o660); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

// serveLinkNodeSocket serves h on the Unix socket at p until stop closes,
// listening again (every 5 s) while it cannot.
func serveLinkNodeSocket(p string, h http.Handler, stop <-chan struct{}) {
	for {
		ln, err := listenLinkNodeSocket(p)
		if err != nil {
			log.Printf("JuiceMount Link: WARNING cannot listen for the Link node container on %s: %v; retrying in 5s", p, err)
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}
		log.Printf("JuiceMount Link: serving the Link node container on %s", p)
		srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
		done := make(chan error, 1)
		go func() { done <- srv.Serve(ln) }()
		select {
		case <-stop:
			_ = srv.Close()
			<-done
			_ = os.Remove(p)
			return
		case err := <-done:
			log.Printf("JuiceMount Link: WARNING Link node socket %s stopped: %v; retrying in 5s", p, err)
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// serveLinkNodeSockets serves the broker's control and relay sockets in dir
// until stop closes.
func serveLinkNodeSockets(dir string, b *linkNodeBroker, stop <-chan struct{}) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		serveLinkNodeSocket(filepath.Join(dir, linkNodeControlSocket), b.controlHandler(), stop)
	}()
	go func() {
		defer wg.Done()
		serveLinkNodeSocket(filepath.Join(dir, linkNodeRelaySocket), b.relayHandler(), stop)
	}()
	wg.Wait()
}

// linkNodeGuard keeps the in-Manager node (JM_NET_NAS_NODE=on) from running
// beside a node container, which holds the same identity. It answers the
// control socket, when its volume is mounted, with 409 only, so the node
// container stops its node, and it reports a node container that reached
// it lately, or whose lease is fresh, as blocking.
type linkNodeGuard struct {
	identityDir string
	now         func() time.Time

	mu          sync.Mutex
	lastContact time.Time
}

func (g *linkNodeGuard) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	g.mu.Lock()
	g.lastContact = g.now()
	g.mu.Unlock()
	linkNodeReply(w, http.StatusConflict, linkNodeError{
		Error: "this Manager runs the NAS node itself (JM_NET_NAS_NODE=on); stop and remove the Link node container (juicemount-link)",
		Code:  linkNodeOwnedByManager,
	})
}

// blocked returns why the in-Manager node must not start now, or "".
func (g *linkNodeGuard) blocked() string {
	g.mu.Lock()
	contact := g.lastContact
	g.mu.Unlock()
	now := g.now()
	if !contact.IsZero() && now.Sub(contact) < linkNodeStaleAfter {
		return "a Link node container (juicemount-link) is still running and holds the NAS node's identity; stop and remove it"
	}
	if fi, err := os.Stat(filepath.Join(g.identityDir, linkNodeLeaseFile)); err == nil {
		if age := now.Sub(fi.ModTime()); age < linkNodeOrphanAfter+linkNodeLeaseEvery {
			return fmt.Sprintf("a Link node container (juicemount-link) reported %s ago and may still hold the NAS node's identity; waiting until it has stopped its node (remove juicemount-link if it still runs)", age.Round(time.Second))
		}
	}
	return ""
}
