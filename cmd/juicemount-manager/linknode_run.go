package main

// The node container's side (juicemount-manager --link-node). See linknode.go
// for the layout and the socket protocol.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

const (
	defaultLinkNodeStateDir = "/var/lib/juicemount-link"
	// defaultLinkNodeHealthFile is on the node container's tmpfs; the
	// container's health check reads it (--link-node-health).
	defaultLinkNodeHealthFile = "/tmp/juicemount-link-node.json"
	// defaultLinkNodeServices are the NAS host's published ports: with host
	// networking the node reaches the Redis proxy and the object store
	// there, and proxies the Manager's API over the socket.
	defaultLinkNodeServices = "30179=127.0.0.1:30179,30151=127.0.0.1:30151,30190=" + nasManagerTarget
	// defaultLinkNodeControl is Headscale's published port on the NAS host.
	defaultLinkNodeControl = "http://127.0.0.1:30193"
)

// errLinkNodeWait is a Manager answer meaning "not yet": Headscale is not
// running.
var errLinkNodeWait = errors.New("waiting for headscale")

// linkNodeHTTPError is a non-success answer from the Manager.
type linkNodeHTTPError struct {
	Code int
	Msg  string
}

func (e *linkNodeHTTPError) Error() string {
	return fmt.Sprintf("Manager answered %d: %s", e.Code, e.Msg)
}

// linkNodeClient calls the Manager over the node socket.
type linkNodeClient struct {
	sock string
	http *http.Client
}

func unixTransport(sock string) *http.Transport {
	return &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
		MaxIdleConns:    4,
		IdleConnTimeout: 30 * time.Second,
	}
}

func newLinkNodeClient(sock string) *linkNodeClient {
	return &linkNodeClient{sock: sock, http: &http.Client{Transport: unixTransport(sock), Timeout: 30 * time.Second}}
}

func (c *linkNodeClient) call(ctx context.Context, method, op string, in, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://juicemount-manager"+linkNodeAPIPrefix+op, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("Manager unreachable on %s: %w", c.sock, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, linkNodeMaxState*2))
	if resp.StatusCode == http.StatusServiceUnavailable {
		return errLinkNodeWait
	}
	if resp.StatusCode/100 != 2 {
		var e linkNodeError
		if json.Unmarshal(data, &e) != nil || e.Error == "" {
			e.Error = strings.TrimSpace(string(data))
		}
		return &linkNodeHTTPError{Code: resp.StatusCode, Msg: e.Error}
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.Unmarshal(data, out)
}

func callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// linkNodeControlURL is where the node container dials Headscale.
// JM_NET_NAS_CONTROL_URL wins. Without it: the published port on loopback,
// or, with Headscale's own TLS (whose certificate names the public host),
// the public URL, which then needs hairpin NAT, a local DNS entry or a
// hosts entry for that name pointing at 127.0.0.1.
func linkNodeControlURL(s linkNodeSettings) (string, error) {
	if override := strings.TrimRight(strings.TrimSpace(os.Getenv("JM_NET_NAS_CONTROL_URL")), "/"); override != "" {
		return override, nil
	}
	if !s.TLS {
		return defaultLinkNodeControl, nil
	}
	if s.ServerURL == "" {
		return "", errors.New("Headscale has its own TLS but JM_NET_SERVER_URL is unset; set JM_NET_NAS_CONTROL_URL on the Link node container")
	}
	return strings.TrimRight(s.ServerURL, "/"), nil
}

// linkNodeManagerProxy serves the Manager's API to tailnet peers through
// the socket, naming each peer in linkNodePeerHeader. runNASNode has already
// set RemoteAddr to the peer's canonical tailnet address
// (nasPeerAddrHandler). No X-Forwarded-For is added.
func linkNodeManagerProxy(sock string) http.Handler {
	target := &url.URL{Scheme: "http", Host: "juicemount-manager"}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host
			pr.Out.Header.Del(linkNodePeerHeader)
			pr.Out.Header.Set(linkNodePeerHeader, pr.In.RemoteAddr)
		},
		Transport:     unixTransport(sock),
		FlushInterval: -1, // server-sent events (job progress)
		ErrorLog:      log.New(io.Discard, "", 0),
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, "the JuiceMount Manager is not reachable from the Link node", http.StatusBadGateway)
		},
	}
}

// migrateLegacyNodeState gives a node container with no identity yet the
// Manager's copy (the in-Manager node's identity), so an upgrade keeps the
// same tailnet node (same node key, same tailnet address). A fresh install
// has none, and the node registers as new.
func migrateLegacyNodeState(c *linkNodeClient, dir string) error {
	if _, err := os.Stat(filepath.Join(dir, linkNodeStateFile)); err == nil {
		return nil
	}
	ctx, cancel := callCtx()
	defer cancel()
	var id linkNodeIdentity
	err := c.call(ctx, http.MethodGet, "identity", nil, &id)
	var he *linkNodeHTTPError
	if errors.As(err, &he) && he.Code == http.StatusNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the Manager's copy of the NAS node's identity: %w", err)
	}
	if len(id.State) == 0 {
		return nil
	}
	if err := writeNodeIdentity(dir, id.State); err != nil {
		return err
	}
	log.Printf("JuiceMount Link: took the NAS node's identity from the Manager's data volume into %s", dir)
	return nil
}

// identitySync sends the node's identity to the Manager whenever it
// changes, so the Manager's copy stays current: going back to
// JM_NET_NAS_NODE=on then resumes the same node, even on an install that
// started with the node container.
type identitySync struct {
	c    *linkNodeClient
	dir  string
	sent [sha256.Size]byte
}

func (s *identitySync) sync(ctx context.Context) error {
	data, err := readNodeIdentity(s.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	sum := sha256.Sum256(data)
	if sum == s.sent {
		return nil
	}
	if err := s.c.call(ctx, http.MethodPost, "identity", linkNodeIdentity{State: data}, nil); err != nil {
		return err
	}
	s.sent = sum
	return nil
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

// linkNodeConfig builds one node start from the Manager's settings.
func linkNodeConfig(c *linkNodeClient, s linkNodeSettings, dir string) (nasNodeConfig, error) {
	control, err := linkNodeControlURL(s)
	if err != nil {
		return nasNodeConfig{}, err
	}
	udpPort, err := nasNodeUDPPort()
	if err != nil {
		return nasNodeConfig{}, err
	}
	services, err := parseNASServices(envOr("JM_NET_NAS_SERVICES", defaultLinkNodeServices))
	if err != nil {
		return nasNodeConfig{}, err
	}
	cfg := nasNodeConfig{
		ControlURL: control,
		Hostname:   s.Hostname,
		Dir:        dir,
		UDPPort:    udpPort,
		Services:   services,
		Subnet:     s.Subnet,
		Handler:    linkNodeManagerProxy(c.sock),
		Tags:       s.Tags,
		MintKey: func() (string, error) {
			ctx, cancel := callCtx()
			defer cancel()
			var resp linkNodeKeyResponse
			if err := c.call(ctx, http.MethodPost, "key", struct{}{}, &resp); err != nil {
				return "", err
			}
			if resp.Key == "" {
				return "", errors.New("the Manager returned no key")
			}
			return resp.Key, nil
		},
		ApproveRoute: func(nodeKey, prefix string) error {
			ctx, cancel := callCtx()
			defer cancel()
			return c.call(ctx, http.MethodPost, "approve-route", linkNodeKeyRequest{NodeKey: nodeKey, Prefix: prefix}, nil)
		},
		Conflicts: func(ctx context.Context, nodeKey string) error {
			var resp linkNodeConflictResponse
			if err := c.call(ctx, http.MethodPost, "conflicts", linkNodeKeyRequest{NodeKey: nodeKey}, &resp); err != nil {
				return fmt.Errorf("could not look for other nodes claiming the NAS: %w", err)
			}
			if resp.Conflict != "" {
				return errors.New(resp.Conflict)
			}
			return nil
		},
		Dial:        (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		UpTimeout:   nasNodeUpTimeout,
		HealthEvery: 10 * time.Second,
		Logf:        func(string, ...any) {},
	}
	if len(s.Tags) > 0 {
		cfg.EnsureTags = func(nodeKey string) (bool, error) {
			ctx, cancel := callCtx()
			defer cancel()
			var resp linkNodeTagsResponse
			if err := c.call(ctx, http.MethodPost, "ensure-tags", linkNodeKeyRequest{NodeKey: nodeKey}, &resp); err != nil {
				return false, err
			}
			return resp.Changed, nil
		}
	}
	return cfg, nil
}

// waitLinkNodeSettings asks the Manager for this start's settings, polling
// while Headscale is not running (not a failure, as for the in-Manager
// node). An unreachable Manager is a failure: the supervisor backs off.
func waitLinkNodeSettings(c *linkNodeClient, status *componentStatus, stop <-chan struct{}) (linkNodeSettings, error) {
	for {
		ctx, cancel := callCtx()
		var s linkNodeSettings
		err := c.call(ctx, http.MethodGet, "config", nil, &s)
		cancel()
		if !errors.Is(err, errLinkNodeWait) {
			return s, err
		}
		status.set("starting", "waiting for headscale")
		select {
		case <-stop:
			return s, errStopped
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// newLinkNodeSupervisor supervises the node in the node container, with
// the same backoff as the in-Manager node.
func newLinkNodeSupervisor(c *linkNodeClient, dir string) *supervisor {
	var sup *supervisor
	sup = newSupervisor("nas node", func(stop <-chan struct{}, ready func(string)) error {
		s, err := waitLinkNodeSettings(c, sup.status, stop)
		if err != nil {
			return err
		}
		if err := migrateLegacyNodeState(c, dir); err != nil {
			return err
		}
		cfg, err := linkNodeConfig(c, s, dir)
		if err != nil {
			return err
		}
		cfg.Identity = sup.status.setNodeKey
		log.Printf("JuiceMount Link: nas node dials control at %s", cfg.ControlURL)
		return runNASNode(cfg, stop, ready)
	})
	return sup
}

// reportLinkNode sends the node's state (and its identity, when it
// changed) to the Manager and writes the state for the health check, until
// stop closes. dir is the node's state directory ("" skips the identity).
func reportLinkNode(c *linkNodeClient, status *componentStatus, dir, healthFile string, stop <-chan struct{}) {
	t := time.NewTicker(linkNodeReportEvery)
	defer t.Stop()
	ids := &identitySync{c: c, dir: dir}
	for {
		st := status.snapshot()
		ctx, cancel := context.WithTimeout(context.Background(), linkNodeReportEvery)
		_ = c.call(ctx, http.MethodPost, "status", st, nil)
		if dir != "" && st.State == "running" {
			if err := ids.sync(ctx); err != nil {
				log.Printf("JuiceMount Link: could not give the Manager a copy of the NAS node's identity: %v", err)
			}
		}
		cancel()
		writeLinkNodeHealth(healthFile, st)
		select {
		case <-stop:
			return
		case <-t.C:
		}
	}
}

type linkNodeHealth struct {
	manager.LinkComponentStatus
	At time.Time `json:"at"`
}

func writeLinkNodeHealth(path string, st *manager.LinkComponentStatus) {
	if path == "" {
		return
	}
	data, err := json.Marshal(linkNodeHealth{LinkComponentStatus: *st, At: time.Now()})
	if err != nil {
		return
	}
	tmp := path + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil {
		_ = os.Rename(tmp, path)
	}
}

// linkNodeHealthy is the node container's health check: the node runs, and
// the report is fresh (the process is not stuck).
func linkNodeHealthy(path string, now time.Time) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var h linkNodeHealth
	if err := json.Unmarshal(data, &h); err != nil {
		return err
	}
	if age := now.Sub(h.At); age > linkNodeStaleAfter {
		return fmt.Errorf("no state report for %s", age.Round(time.Second))
	}
	if h.State != "running" {
		msg := "nas node is " + h.State
		if h.LastError != "" {
			msg += ": " + h.LastError
		}
		return errors.New(msg)
	}
	return nil
}

// runLinkNodeMain is juicemount-manager --link-node: the NAS node alone.
func runLinkNodeMain() {
	sock := linkNodeSocketPath()
	dir := envOr("JM_NET_NODE_STATE_DIR", defaultLinkNodeStateDir)
	health := envOr("JM_NET_NODE_HEALTH_FILE", defaultLinkNodeHealthFile)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Fatalf("JuiceMount Link node: state directory %s: %v", dir, err)
	}
	log.Printf("JuiceMount Link node starting: Manager socket %s, state %s", sock, dir)
	c := newLinkNodeClient(sock)
	sup := newLinkNodeSupervisor(c, dir)
	sup.Start()
	stopReport := make(chan struct{})
	reported := make(chan struct{})
	go func() {
		defer close(reported)
		reportLinkNode(c, sup.status, dir, health, stopReport)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("JuiceMount Link node: shutting down")
	sup.Stop()
	close(stopReport)
	<-reported
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = c.call(ctx, http.MethodPost, "status", sup.status.snapshot(), nil)
	cancel()
}

// runLinkNodeHealth is juicemount-manager --link-node-health.
func runLinkNodeHealth() int {
	if err := linkNodeHealthy(envOr("JM_NET_NODE_HEALTH_FILE", defaultLinkNodeHealthFile), time.Now()); err != nil {
		fmt.Fprintln(os.Stderr, "JuiceMount Link node unhealthy:", err)
		return 1
	}
	return 0
}
