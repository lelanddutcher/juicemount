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
// that needs the Manager goes over a Unix socket in a volume that only the
// two containers mount (linkNodeBroker):
//
//	GET  /link-node/v1/config        hostname, exposure (subnet), tags, TLS
//	                                 (503 while Headscale is not running)
//	POST /link-node/v1/key           a one-off pre-auth key with those tags
//	POST /link-node/v1/ensure-tags   tag a node registered before tagged keys
//	POST /link-node/v1/approve-route approve the subnet route (subnet exposure)
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
// Any other path is the Manager's own API (port 30190 on the tailnet). The
// node proxies it with the peer's tailnet address in X-JuiceMount-Link-Peer,
// so the Manager still sees each Mac's own address (per-address enrollment
// limits). The header is honoured on this socket only, never on the
// Manager's TCP listener.
//
// The socket is the only trust boundary: whoever can open it can mint
// tailnet keys. Mount its volume into the Manager and the node container,
// nothing else.

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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

const (
	defaultLinkNodeSocket = "/run/juicemount-link/manager.sock"
	linkNodeAPIPrefix     = "/link-node/v1/"
	// linkNodePeerHeader carries a proxied request's tailnet peer address.
	linkNodePeerHeader = "X-JuiceMount-Link-Peer"
	// linkNodeReportEvery is how often the node container reports its
	// state; after linkNodeStaleAfter without a report the Manager shows
	// the node as down.
	linkNodeReportEvery = 5 * time.Second
	linkNodeStaleAfter  = 30 * time.Second
	// linkNodeStateFile is tsnet's state file: the node's machine and node
	// keys, its whole identity.
	linkNodeStateFile = "tailscaled.state"
	// linkNodeMaxState bounds an identity sent over the socket.
	linkNodeMaxState = 1 << 20
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

func linkNodeSocketPath() string { return envOr("JM_NET_NODE_SOCKET", defaultLinkNodeSocket) }

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
}

// linkNodeBroker is the Manager's end of the node container's socket.
type linkNodeBroker struct {
	// handler is the Manager's API, served to proxied tailnet requests.
	handler http.Handler
	// controlReady reports whether Headscale is running.
	controlReady func() bool
	settings     func() (linkNodeSettings, error)
	mintKey      func(tags []string) (string, error)
	ensureTags   func(nodeKey string, tags []string) (bool, error)
	approve      func(nodeKey, prefix string) error
	conflicts    func(ctx context.Context, nodeKey, subnet string) error
	// identityDir is the in-Manager node's tsnet directory, which holds
	// the Manager's copy of the node's identity.
	identityDir string
	now       func() time.Time

	mu      sync.Mutex
	started time.Time
	last    *manager.LinkComponentStatus
	lastAt  time.Time
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
		mintKey: func(tags []string) (string, error) {
			return manager.MintPreauthKey(hsBinary, cfgPath, false, tags)
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

func (b *linkNodeBroker) record(st manager.LinkComponentStatus) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.last = &st
	b.lastAt = b.now()
}

func (b *linkNodeBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, linkNodeAPIPrefix) {
		b.serveAPI(w, r)
		return
	}
	// The Manager's API for a tailnet peer, proxied by the node container.
	peer, err := netip.ParseAddrPort(r.Header.Get(linkNodePeerHeader))
	if err != nil || !peer.Addr().IsValid() {
		http.Error(w, "the Link node did not name the tailnet peer", http.StatusBadRequest)
		return
	}
	r2 := r.Clone(r.Context())
	r2.Header.Del(linkNodePeerHeader)
	r2.RemoteAddr = peer.String()
	b.handler.ServeHTTP(w, r2)
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
	if want == http.MethodPost && op != "status" && op != "key" && op != "identity" {
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
		if r.Method == http.MethodPost {
			var id linkNodeIdentity
			if err := json.NewDecoder(io.LimitReader(r.Body, 4*linkNodeMaxState)).Decode(&id); err != nil || len(id.State) == 0 || len(id.State) > linkNodeMaxState {
				linkNodeReply(w, http.StatusBadRequest, linkNodeError{Error: "an identity is required"})
				return
			}
			if err := writeNodeIdentity(b.identityDir, id.State); err != nil {
				linkNodeReply(w, http.StatusInternalServerError, linkNodeError{Error: err.Error()})
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		data, err := readNodeIdentity(b.identityDir)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			linkNodeReply(w, http.StatusNotFound, linkNodeError{Error: "the Manager has no NAS node identity"})
		case err != nil:
			linkNodeReply(w, http.StatusInternalServerError, linkNodeError{Error: err.Error()})
		default:
			linkNodeReply(w, http.StatusOK, linkNodeIdentity{State: data})
		}
		return
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
	switch op {
	case "config":
		linkNodeReply(w, http.StatusOK, s)
	case "key":
		key, err := b.mintKey(s.Tags)
		if err != nil {
			linkNodeReply(w, http.StatusInternalServerError, linkNodeError{Error: err.Error()})
			return
		}
		linkNodeReply(w, http.StatusOK, linkNodeKeyResponse{Key: key})
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
		ctx, cancel := context.WithTimeout(r.Context(), nasNodeConflictTimeout)
		defer cancel()
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

// listenLinkNodeSocket replaces a stale socket at path and listens there,
// readable and writable by the owner and group only.
func listenLinkNodeSocket(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode()&fs.ModeSocket == 0 {
			return nil, fmt.Errorf("%s exists and is not a socket", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o660); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

// serveLinkNodeSocket serves b on the Unix socket at path until stop
// closes, listening again (every 5 s) while it cannot.
func serveLinkNodeSocket(path string, b *linkNodeBroker, stop <-chan struct{}) {
	for {
		ln, err := listenLinkNodeSocket(path)
		if err != nil {
			log.Printf("JuiceMount Link: WARNING cannot listen for the Link node container on %s: %v; retrying in 5s", path, err)
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}
		log.Printf("JuiceMount Link: serving the Link node container on %s", path)
		srv := &http.Server{Handler: b, ReadHeaderTimeout: 5 * time.Second}
		done := make(chan error, 1)
		go func() { done <- srv.Serve(ln) }()
		select {
		case <-stop:
			_ = srv.Close()
			<-done
			_ = os.Remove(path)
			return
		case err := <-done:
			log.Printf("JuiceMount Link: WARNING Link node socket stopped: %v; retrying in 5s", err)
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// writeNodeIdentity replaces the tsnet state file in dir, atomically and
// readable by the owner only.
func writeNodeIdentity(dir string, data []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, linkNodeStateFile+".*")
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
		err = os.Rename(tmp.Name(), filepath.Join(dir, linkNodeStateFile))
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("store the NAS node's identity in %s: %w", dir, err)
	}
	return nil
}
