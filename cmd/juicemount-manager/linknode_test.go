package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

// fakeBroker is a broker with every Headscale call replaced by a recorder.
type fakeBroker struct {
	*linkNodeBroker
	mu       sync.Mutex
	ready    bool
	settings linkNodeSettings
	minted   [][]string
	tagged   []string
	approved []string
	conflict string
	now      time.Time
}

func newFakeBroker(t *testing.T, handler http.Handler) *fakeBroker {
	t.Helper()
	f := &fakeBroker{
		ready:    true,
		settings: linkNodeSettings{Hostname: "juicemount-nas", Tags: []string{manager.LinkTagNAS}, ServerURL: "https://link.example.com:30193"},
		now:      time.Unix(1_800_000_000, 0),
	}
	f.linkNodeBroker = &linkNodeBroker{
		handler:      handler,
		controlReady: func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.ready },
		settings:     func() (linkNodeSettings, error) { f.mu.Lock(); defer f.mu.Unlock(); return f.settings, nil },
		mintKey: func(tags []string) (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.minted = append(f.minted, tags)
			return "hskey-test", nil
		},
		ensureTags: func(nodeKey string, tags []string) (bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.tagged = append(f.tagged, nodeKey+"="+strings.Join(tags, ","))
			return true, nil
		},
		approve: func(nodeKey, prefix string) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.approved = append(f.approved, nodeKey+"="+prefix)
			return nil
		},
		conflicts: func(_ context.Context, nodeKey, subnet string) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.conflict != "" {
				return errors.New(f.conflict)
			}
			return nil
		},
		identityDir: filepath.Join(t.TempDir(), "nas-node"),
		now:       func() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.now },
	}
	f.started = f.now
	return f
}

// serveOnSocket serves h on a Unix socket and returns its path.
func serveOnSocket(t *testing.T, h http.Handler) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "jmln")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "m.sock")
	ln, err := listenLinkNodeSocket(sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sock
}

func TestLinkNodeBrokerProtocol(t *testing.T) {
	f := newFakeBroker(t, http.NotFoundHandler())
	c := newLinkNodeClient(serveOnSocket(t, f))
	ctx := context.Background()

	// Headscale not running: the node waits instead of failing.
	f.mu.Lock()
	f.ready = false
	f.mu.Unlock()
	if err := c.call(ctx, http.MethodGet, "config", nil, &linkNodeSettings{}); !errors.Is(err, errLinkNodeWait) {
		t.Fatalf("config before headscale runs: %v, want errLinkNodeWait", err)
	}
	f.mu.Lock()
	f.ready = true
	f.mu.Unlock()

	var s linkNodeSettings
	if err := c.call(ctx, http.MethodGet, "config", nil, &s); err != nil || !reflect.DeepEqual(s, f.settings) {
		t.Fatalf("config = %+v, %v; want %+v", s, err, f.settings)
	}
	var key linkNodeKeyResponse
	if err := c.call(ctx, http.MethodPost, "key", struct{}{}, &key); err != nil || key.Key != "hskey-test" {
		t.Fatalf("key = %+v, %v", key, err)
	}
	if !reflect.DeepEqual(f.minted, [][]string{{manager.LinkTagNAS}}) {
		t.Fatalf("minted %v, want one key tagged %s", f.minted, manager.LinkTagNAS)
	}
	var tags linkNodeTagsResponse
	if err := c.call(ctx, http.MethodPost, "ensure-tags", linkNodeKeyRequest{NodeKey: "nodekey:1"}, &tags); err != nil || !tags.Changed {
		t.Fatalf("ensure-tags = %+v, %v", tags, err)
	}
	if err := c.call(ctx, http.MethodPost, "ensure-tags", linkNodeKeyRequest{}, &tags); err == nil {
		t.Fatal("ensure-tags without a node key was accepted")
	}

	// Service exposure has no route to approve; only the NAS route is
	// approved in subnet exposure.
	if err := c.call(ctx, http.MethodPost, "approve-route", linkNodeKeyRequest{NodeKey: "nodekey:1", Prefix: "0.0.0.0/0"}, nil); err == nil {
		t.Fatal("approve-route approved a route that is not the NAS route")
	}
	f.mu.Lock()
	f.settings = linkNodeSettings{Hostname: "juicemount-nas", Subnet: "192.0.2.0/24"}
	f.mu.Unlock()
	if err := c.call(ctx, http.MethodPost, "approve-route", linkNodeKeyRequest{NodeKey: "nodekey:1", Prefix: "0.0.0.0/0"}, nil); err == nil {
		t.Fatal("approve-route approved a default route")
	}
	if err := c.call(ctx, http.MethodPost, "approve-route", linkNodeKeyRequest{NodeKey: "nodekey:1", Prefix: "192.0.2.0/24"}, nil); err != nil {
		t.Fatal(err)
	}
	// Subnet exposure: no tags, so ensure-tags tags nothing.
	if err := c.call(ctx, http.MethodPost, "ensure-tags", linkNodeKeyRequest{NodeKey: "nodekey:2"}, &tags); err != nil || tags.Changed {
		t.Fatalf("ensure-tags in subnet exposure = %+v, %v", tags, err)
	}
	if want := []string{"nodekey:1=" + manager.LinkTagNAS}; !reflect.DeepEqual(f.tagged, want) {
		t.Fatalf("tagged %v, want %v", f.tagged, want)
	}
	if want := []string{"nodekey:1=192.0.2.0/24"}; !reflect.DeepEqual(f.approved, want) {
		t.Fatalf("approved %v, want %v", f.approved, want)
	}

	var conf linkNodeConflictResponse
	if err := c.call(ctx, http.MethodPost, "conflicts", linkNodeKeyRequest{NodeKey: "nodekey:1"}, &conf); err != nil || conf.Conflict != "" {
		t.Fatalf("conflicts = %+v, %v", conf, err)
	}
	f.mu.Lock()
	f.conflict = "node 7 (\"rogue\") advertises 192.0.2.0/24"
	f.mu.Unlock()
	if err := c.call(ctx, http.MethodPost, "conflicts", linkNodeKeyRequest{NodeKey: "nodekey:1"}, &conf); err != nil || conf.Conflict != f.conflict {
		t.Fatalf("conflicts = %+v, %v", conf, err)
	}

	if err := c.call(ctx, http.MethodGet, "key", nil, nil); err == nil {
		t.Fatal("GET key was accepted")
	}
	if err := c.call(ctx, http.MethodPost, "nope", linkNodeKeyRequest{NodeKey: "k"}, nil); err == nil {
		t.Fatal("an unknown call was accepted")
	}
}

func TestLinkNodeBrokerStatus(t *testing.T) {
	f := newFakeBroker(t, http.NotFoundHandler())
	c := newLinkNodeClient(serveOnSocket(t, f))

	if st := f.snapshot(); st.State != "starting" || !strings.Contains(st.Detail, "juicemount-link") {
		t.Fatalf("before any report: %+v", st)
	}
	report := manager.LinkComponentStatus{State: "running", Detail: "up", NodeKey: "nodekey:1", Restarts: 2}
	if err := c.call(context.Background(), http.MethodPost, "status", report, nil); err != nil {
		t.Fatal(err)
	}
	if st := f.snapshot(); st.State != "running" || st.NodeKey != "nodekey:1" || st.Restarts != 2 {
		t.Fatalf("after a report: %+v", st)
	}
	// No report for longer than linkNodeStaleAfter: the node is down, and
	// GET /api/net/link stops calling the route ready.
	f.mu.Lock()
	f.now = f.now.Add(linkNodeStaleAfter + time.Second)
	f.mu.Unlock()
	st := f.snapshot()
	if st.State != "backoff" || !strings.Contains(st.LastError, "no report") || st.NodeKey != "nodekey:1" {
		t.Fatalf("stale report: %+v", st)
	}
	if err := c.call(context.Background(), http.MethodPost, "status", struct{}{}, nil); err == nil {
		t.Fatal("an empty status was accepted")
	}
}

// The Manager's API through the node: the Manager sees each peer's
// tailnet address, and only through the socket.
func TestLinkNodeManagerProxyPeerAddress(t *testing.T) {
	seen := make(chan *http.Request, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r
		_, _ = io.WriteString(w, "ok")
	})
	f := newFakeBroker(t, handler)
	sock := serveOnSocket(t, f)

	proxy := linkNodeManagerProxy(sock)
	req := httptest.NewRequest(http.MethodGet, "http://192.0.2.10:30190/api/sources", nil)
	req.RemoteAddr = "100.64.0.7:51000"
	req.Header.Set(linkNodePeerHeader, "100.64.0.99:1") // a peer cannot name itself
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("proxied request: %d %q", rec.Code, rec.Body.String())
	}
	got := <-seen
	if got.RemoteAddr != "100.64.0.7:51000" {
		t.Fatalf("Manager saw RemoteAddr %q, want the peer's tailnet address", got.RemoteAddr)
	}
	if got.Header.Get(linkNodePeerHeader) != "" || got.Header.Get("X-Forwarded-For") != "" {
		t.Fatalf("headers leaked to the Manager: %v", got.Header)
	}
	if got.Host != "192.0.2.10:30190" || got.URL.Path != "/api/sources" {
		t.Fatalf("Manager saw host %q path %q", got.Host, got.URL.Path)
	}

	// Straight to the socket without a peer: refused.
	c := &http.Client{Transport: unixTransport(sock)}
	resp, err := c.Get("http://juicemount-manager/api/sources")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("request without a peer: %d, want 400", resp.StatusCode)
	}

	// The Manager is down: a clear 502, not a hang.
	down := linkNodeManagerProxy(filepath.Join(t.TempDir(), "gone.sock"))
	rec = httptest.NewRecorder()
	down.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://x/", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("Manager down: %d, want 502", rec.Code)
	}
}

func TestLinkNodeMigratesLegacyState(t *testing.T) {
	f := newFakeBroker(t, http.NotFoundHandler())
	c := newLinkNodeClient(serveOnSocket(t, f))
	dir := filepath.Join(t.TempDir(), "state")

	// A fresh install: nothing to move.
	if err := migrateLegacyNodeState(c, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, linkNodeStateFile)); !os.IsNotExist(err) {
		t.Fatalf("fresh install got a state file: %v", err)
	}

	// An upgrade: the in-Manager node's identity moves, and the Manager
	// keeps its copy for a rollback.
	if err := os.MkdirAll(f.identityDir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := []byte(`{"_machinekey":"old"}`)
	if err := os.WriteFile(filepath.Join(f.identityDir, linkNodeStateFile), legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyNodeState(c, dir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, linkNodeStateFile))
	if err != nil || string(got) != string(legacy) {
		t.Fatalf("moved state = %q, %v", got, err)
	}
	if fi, err := os.Stat(filepath.Join(dir, linkNodeStateFile)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode: %v, %v", fi.Mode(), err)
	}
	if _, err := os.Stat(filepath.Join(f.identityDir, linkNodeStateFile)); err != nil {
		t.Fatalf("the Manager's copy is gone: %v", err)
	}

	// The node's own identity is never replaced.
	if err := os.WriteFile(filepath.Join(f.identityDir, linkNodeStateFile), []byte(`{"other":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyNodeState(c, dir); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, linkNodeStateFile)); string(got) != string(legacy) {
		t.Fatalf("existing state replaced: %q", got)
	}

	// The Manager unreachable with no identity yet: fail, so the supervisor
	// retries, rather than register a second node.
	gone := newLinkNodeClient(filepath.Join(t.TempDir(), "gone.sock"))
	if err := migrateLegacyNodeState(gone, filepath.Join(t.TempDir(), "s2")); err == nil {
		t.Fatal("migration without the Manager succeeded")
	}
}

// The node gives its identity back whenever it changes, so a rollback to
// the in-Manager node resumes the same node, also on an install that started
// with the node container (the Manager had no copy).
func TestLinkNodeIdentitySync(t *testing.T) {
	f := newFakeBroker(t, http.NotFoundHandler())
	c := newLinkNodeClient(serveOnSocket(t, f))
	dir := t.TempDir()
	ids := &identitySync{c: c, dir: dir}
	ctx := context.Background()
	manager := filepath.Join(f.identityDir, linkNodeStateFile)

	if err := ids.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(manager); !os.IsNotExist(err) {
		t.Fatalf("a copy without an identity: %v", err)
	}
	if err := writeNodeIdentity(dir, []byte(`{"k":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := ids.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(manager); err != nil || string(got) != `{"k":1}` {
		t.Fatalf("Manager's copy = %q, %v", got, err)
	}
	if fi, _ := os.Stat(manager); fi.Mode().Perm() != 0o600 {
		t.Fatalf("Manager's copy mode %v", fi.Mode())
	}
	// Unchanged: not sent again (the Manager's copy is left alone).
	if err := os.WriteFile(manager, []byte("touched"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ids.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(manager); string(got) != "touched" {
		t.Fatalf("unchanged identity sent again: %q", got)
	}
	if err := writeNodeIdentity(dir, []byte(`{"k":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := ids.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(manager); string(got) != `{"k":2}` {
		t.Fatalf("changed identity not sent: %q", got)
	}
	// An empty identity is refused.
	if err := c.call(ctx, http.MethodPost, "identity", linkNodeIdentity{}, nil); err == nil {
		t.Fatal("an empty identity was accepted")
	}
}

func TestLinkNodeControlURL(t *testing.T) {
	t.Setenv("JM_NET_NAS_CONTROL_URL", "")
	if got, err := linkNodeControlURL(linkNodeSettings{}); err != nil || got != "http://127.0.0.1:30193" {
		t.Fatalf("no TLS: %q, %v", got, err)
	}
	if got, err := linkNodeControlURL(linkNodeSettings{TLS: true, ServerURL: "https://link.example.com:30193/"}); err != nil || got != "https://link.example.com:30193" {
		t.Fatalf("TLS: %q, %v", got, err)
	}
	if _, err := linkNodeControlURL(linkNodeSettings{TLS: true}); err == nil {
		t.Fatal("TLS without a server URL accepted")
	}
	t.Setenv("JM_NET_NAS_CONTROL_URL", "https://link.example.com:30193/")
	if got, err := linkNodeControlURL(linkNodeSettings{}); err != nil || got != "https://link.example.com:30193" {
		t.Fatalf("override: %q, %v", got, err)
	}
}

func TestLinkNodeConfigDefaults(t *testing.T) {
	t.Setenv("JM_NET_NAS_CONTROL_URL", "")
	t.Setenv("JM_NET_NAS_SERVICES", "")
	t.Setenv("JM_NET_NAS_UDP_PORT", "")
	cfg, err := linkNodeConfig(newLinkNodeClient("/nonexistent"), linkNodeSettings{Hostname: "nas", Tags: []string{manager.LinkTagNAS}}, "/state")
	if err != nil {
		t.Fatal(err)
	}
	want := []nasService{{30151, "127.0.0.1:30151"}, {30179, "127.0.0.1:30179"}, {30190, nasManagerTarget}}
	if !reflect.DeepEqual(cfg.Services, want) {
		t.Fatalf("services = %+v, want the host's published ports %+v", cfg.Services, want)
	}
	if cfg.UDPPort != defaultNASUDPPort || cfg.ControlURL != defaultLinkNodeControl || cfg.Dir != "/state" || cfg.Hostname != "nas" {
		t.Fatalf("config = %+v", cfg)
	}
	if cfg.EnsureTags == nil || cfg.Handler == nil || cfg.MintKey == nil {
		t.Fatal("tagged node without EnsureTags, Handler or MintKey")
	}
	cfg, err = linkNodeConfig(newLinkNodeClient("/nonexistent"), linkNodeSettings{Hostname: "nas", Subnet: "192.0.2.0/24"}, "/state")
	if err != nil || cfg.EnsureTags != nil || cfg.Subnet != "192.0.2.0/24" {
		t.Fatalf("subnet config = %+v, %v", cfg, err)
	}
}

// The node container's supervisor waits for Headscale without counting a
// failure, and backs off while the Manager is unreachable.
func TestLinkNodeSupervisorWaitsAndBacksOff(t *testing.T) {
	gone := newLinkNodeClient(filepath.Join(t.TempDir(), "gone.sock"))
	sup := newLinkNodeSupervisor(gone, t.TempDir())
	sup.Start()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := sup.status.snapshot()
		if st.State == "backoff" && strings.Contains(st.LastError, "Manager unreachable") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no backoff while the Manager is unreachable: %+v", st)
		}
		time.Sleep(20 * time.Millisecond)
	}
	sup.Stop()

	f := newFakeBroker(t, http.NotFoundHandler())
	f.ready = false
	sup = newLinkNodeSupervisor(newLinkNodeClient(serveOnSocket(t, f)), t.TempDir())
	sup.Start()
	defer sup.Stop()
	deadline = time.Now().Add(5 * time.Second)
	for {
		st := sup.status.snapshot()
		if st.State == "starting" && st.Detail == "waiting for headscale" {
			if st.Restarts != 0 {
				t.Fatalf("waiting for headscale counted as a failure: %+v", st)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not waiting for headscale: %+v", st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLinkNodeHealth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "health.json")
	now := time.Now()
	if err := linkNodeHealthy(path, now); err == nil {
		t.Fatal("healthy without a report")
	}
	writeLinkNodeHealth(path, &manager.LinkComponentStatus{State: "backoff", LastError: "boom"})
	if err := linkNodeHealthy(path, now); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("backoff: %v", err)
	}
	writeLinkNodeHealth(path, &manager.LinkComponentStatus{State: "running"})
	if err := linkNodeHealthy(path, time.Now()); err != nil {
		t.Fatalf("running: %v", err)
	}
	if err := linkNodeHealthy(path, time.Now().Add(linkNodeStaleAfter+time.Second)); err == nil {
		t.Fatal("a stale report is healthy")
	}
}

func TestListenLinkNodeSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "jmls")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "sub", "m.sock")
	ln, err := listenLinkNodeSocket(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o660 {
		t.Fatalf("socket mode %v, %v", fi.Mode(), err)
	}
	// A stale socket from a previous Manager is replaced (the listener
	// does not unlink it on this simulated crash).
	if ul, ok := ln.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}
	_ = ln.Close()
	ln, err = listenLinkNodeSocket(path)
	if err != nil {
		t.Fatalf("stale socket not replaced: %v", err)
	}
	_ = ln.Close()
	// Anything else at the path is never removed.
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := listenLinkNodeSocket(file); err == nil {
		t.Fatal("a regular file was replaced by the socket")
	}
}
