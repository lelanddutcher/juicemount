package main

import (
	"context"
	"encoding/json"
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
	"tailscale.com/ipn"
	"tailscale.com/types/key"
	"tailscale.com/types/persist"
)

// testIdentity returns a tsnet state file as tsnet writes it, and its
// public node key.
func testIdentity(t *testing.T) ([]byte, string) {
	t.Helper()
	mk, err := key.NewMachine().MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	nk := key.NewNode()
	prefs := ipn.NewPrefs()
	prefs.Persist = &persist.Persist{PrivateNodeKey: nk}
	data, err := json.Marshal(map[string][]byte{
		"_machinekey":      mk,
		"_current-profile": []byte("profile-abcd"),
		"profile-abcd":     prefs.ToBytes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return data, nk.Public().String()
}

// fakeBroker is a broker with every Headscale call replaced by a recorder.
type fakeBroker struct {
	*linkNodeBroker
	mu       sync.Mutex
	ready    bool
	settings linkNodeSettings
	nextID   uint64
	minted   [][]string
	expired  []uint64
	tagged   []string
	approved []string
	conflict string
	nodes    map[string]*manager.HSNode
	now      time.Time
}

func newFakeBroker(t *testing.T, handler http.Handler) *fakeBroker {
	t.Helper()
	dir := t.TempDir()
	f := &fakeBroker{
		ready:    true,
		settings: linkNodeSettings{Hostname: "juicemount-nas", Tags: []string{manager.LinkTagNAS}, ServerURL: "https://link.example.com:30193"},
		nodes:    map[string]*manager.HSNode{},
		now:      time.Unix(1_800_000_000, 0),
	}
	f.linkNodeBroker = &linkNodeBroker{
		handler:      handler,
		controlReady: func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.ready },
		settings:     func() (linkNodeSettings, error) { f.mu.Lock(); defer f.mu.Unlock(); return f.settings, nil },
		mintKey: func(tags []string) (string, uint64, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.nextID++
			f.minted = append(f.minted, tags)
			return "hskey-test", f.nextID, nil
		},
		expireKey: func(id uint64) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.expired = append(f.expired, id)
			return nil
		},
		lookupNode: func(_ context.Context, nodeKey string) (*manager.HSNode, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.nodes[nodeKey], nil
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
		identityDir: filepath.Join(dir, "nas-node"),
		mintedPath:  filepath.Join(dir, linkNodeMintedFile),
		now:         func() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.now },
	}
	f.started = f.now
	return f
}

// registered records a node the fake control server lists.
func (f *fakeBroker) registered(nodeKey, name string, preauthID uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := &manager.HSNode{ID: uint64(len(f.nodes) + 1), NodeKey: nodeKey, Name: name}
	if preauthID != 0 {
		n.PreAuthKey = &manager.HSPreAuthKey{ID: preauthID, Used: true}
	}
	f.nodes[nodeKey] = n
}

// socketDir returns a short directory for Unix sockets.
func socketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "jmln")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// serveBroker serves the broker's two sockets and returns their directory.
func serveBroker(t *testing.T, b *linkNodeBroker) string {
	t.Helper()
	dir := socketDir(t)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); serveLinkNodeSockets(dir, b, stop) }()
	t.Cleanup(func() { close(stop); <-done })
	for _, s := range []string{linkNodeControlSocket, linkNodeRelaySocket} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if c, err := net.Dial("unix", filepath.Join(dir, s)); err == nil {
				c.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("socket %s never listened", s)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return dir
}

func TestLinkNodeBrokerProtocol(t *testing.T) {
	f := newFakeBroker(t, http.NotFoundHandler())
	c := newLinkNodeClient(serveBroker(t, f.linkNodeBroker))
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
	var k linkNodeKeyResponse
	if err := c.call(ctx, http.MethodPost, "key", struct{}{}, &k); err != nil || k.Key != "hskey-test" {
		t.Fatalf("key = %+v, %v", k, err)
	}
	if !reflect.DeepEqual(f.minted, [][]string{{manager.LinkTagNAS}}) || len(f.expired) != 0 {
		t.Fatalf("minted %v, expired %v; want one key tagged %s, nothing expired", f.minted, f.expired, manager.LinkTagNAS)
	}
	// One outstanding key: the next mint expires the previous (if unused).
	if err := c.call(ctx, http.MethodPost, "key", struct{}{}, &k); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.expired, []uint64{1}) {
		t.Fatalf("expired %v, want [1]", f.expired)
	}

	// The NAS node registered with minted key 2.
	f.registered("nodekey:nas", "juicemount-nas", 2)
	var tags linkNodeTagsResponse
	if err := c.call(ctx, http.MethodPost, "ensure-tags", linkNodeKeyRequest{NodeKey: "nodekey:nas"}, &tags); err != nil || !tags.Changed {
		t.Fatalf("ensure-tags = %+v, %v", tags, err)
	}
	if err := c.call(ctx, http.MethodPost, "ensure-tags", linkNodeKeyRequest{}, &tags); err == nil {
		t.Fatal("ensure-tags without a node key was accepted")
	}

	var conf linkNodeConflictResponse
	if err := c.call(ctx, http.MethodPost, "conflicts", linkNodeKeyRequest{NodeKey: "nodekey:nas"}, &conf); err != nil || conf.Conflict != "" {
		t.Fatalf("conflicts = %+v, %v", conf, err)
	}
	f.mu.Lock()
	f.conflict = "node 7 (\"rogue\") advertises 192.0.2.0/24"
	f.mu.Unlock()
	if err := c.call(ctx, http.MethodPost, "conflicts", linkNodeKeyRequest{NodeKey: "nodekey:nas"}, &conf); err != nil || conf.Conflict != f.conflict {
		t.Fatalf("conflicts = %+v, %v", conf, err)
	}

	// Subnet exposure: only the NAS route is approved; no tags to apply.
	if err := c.call(ctx, http.MethodPost, "approve-route", linkNodeKeyRequest{NodeKey: "nodekey:nas", Prefix: "0.0.0.0/0"}, nil); err == nil {
		t.Fatal("approve-route approved a route in service exposure")
	}
	f.mu.Lock()
	f.settings = linkNodeSettings{Hostname: "juicemount-nas", Subnet: "192.0.2.0/24"}
	f.mu.Unlock()
	if err := c.call(ctx, http.MethodPost, "approve-route", linkNodeKeyRequest{NodeKey: "nodekey:nas", Prefix: "0.0.0.0/0"}, nil); err == nil {
		t.Fatal("approve-route approved a default route")
	}
	if err := c.call(ctx, http.MethodPost, "approve-route", linkNodeKeyRequest{NodeKey: "nodekey:nas", Prefix: "192.0.2.0/24"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.call(ctx, http.MethodPost, "ensure-tags", linkNodeKeyRequest{NodeKey: "nodekey:nas"}, &tags); err != nil || tags.Changed {
		t.Fatalf("ensure-tags in subnet exposure = %+v, %v", tags, err)
	}
	if want := []string{"nodekey:nas=" + manager.LinkTagNAS}; !reflect.DeepEqual(f.tagged, want) {
		t.Fatalf("tagged %v, want %v", f.tagged, want)
	}
	if want := []string{"nodekey:nas=192.0.2.0/24"}; !reflect.DeepEqual(f.approved, want) {
		t.Fatalf("approved %v, want %v", f.approved, want)
	}

	if err := c.call(ctx, http.MethodGet, "key", nil, nil); err == nil {
		t.Fatal("GET key was accepted")
	}
	if err := c.call(ctx, http.MethodPost, "nope", linkNodeKeyRequest{NodeKey: "k"}, nil); err == nil {
		t.Fatal("an unknown call was accepted")
	}
}

// Control calls act only on the NAS node's own key.
func TestLinkNodeBrokerTrustsOnlyTheNASNode(t *testing.T) {
	f := newFakeBroker(t, http.NotFoundHandler())
	f.settings.Subnet = "192.0.2.0/24"
	c := newLinkNodeClient(serveBroker(t, f.linkNodeBroker))
	ctx := context.Background()
	if err := c.call(ctx, http.MethodPost, "key", struct{}{}, &linkNodeKeyResponse{}); err != nil {
		t.Fatal(err)
	}
	f.registered("nodekey:mac", "some-mac", 99)        // a Mac's own key
	f.registered("nodekey:imp", "juicemount-nas", 99)  // the NAS name, a Mac's key
	f.registered("nodekey:nokey", "juicemount-nas", 0) // no pre-auth key
	f.registered("nodekey:other", "not-the-nas", 1)    // minted key, other name
	for _, k := range []string{"nodekey:mac", "nodekey:imp", "nodekey:nokey", "nodekey:other", "nodekey:unknown"} {
		for _, op := range []string{"ensure-tags", "approve-route", "conflicts"} {
			err := c.call(ctx, http.MethodPost, op, linkNodeKeyRequest{NodeKey: k, Prefix: "192.0.2.0/24"}, &json.RawMessage{})
			var he *linkNodeHTTPError
			if !errors.As(err, &he) || he.Code != http.StatusForbidden {
				t.Errorf("%s for %s: %v, want 403", op, k, err)
			}
		}
	}
	if len(f.tagged) != 0 || len(f.approved) != 0 {
		t.Fatalf("acted on a foreign key: tagged %v, approved %v", f.tagged, f.approved)
	}
	// The key in the Manager's copy of the identity is the NAS node's
	// (an upgrade: registered by an older Manager with an unrecorded key).
	data, pub := testIdentity(t)
	if err := writeNodeIdentity(f.identityDir, data); err != nil {
		t.Fatal(err)
	}
	f.registered(pub, "juicemount-nas", 0)
	if err := c.call(ctx, http.MethodPost, "approve-route", linkNodeKeyRequest{NodeKey: pub, Prefix: "192.0.2.0/24"}, nil); err != nil {
		t.Fatalf("the identity copy's key refused: %v", err)
	}
	// Minted IDs survive a Manager restart (they are in a file).
	f.registered("nodekey:fresh", "juicemount-nas", 1)
	f2 := newFakeBroker(t, http.NotFoundHandler())
	f2.mintedPath = f.mintedPath
	f2.nodes = f.nodes
	f2.settings.Subnet = "192.0.2.0/24"
	c2 := newLinkNodeClient(serveBroker(t, f2.linkNodeBroker))
	if err := c2.call(ctx, http.MethodPost, "approve-route", linkNodeKeyRequest{NodeKey: "nodekey:fresh", Prefix: "192.0.2.0/24"}, nil); err != nil {
		t.Fatalf("a node registered with a minted key refused after a restart: %v", err)
	}
}

func TestLinkNodeBrokerStatusAndLease(t *testing.T) {
	f := newFakeBroker(t, http.NotFoundHandler())
	c := newLinkNodeClient(serveBroker(t, f.linkNodeBroker))
	lease := filepath.Join(f.identityDir, linkNodeLeaseFile)

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
	if _, err := os.Stat(lease); err != nil {
		t.Fatalf("no lease after a report: %v", err)
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
	// A clean stop releases the lease.
	if err := c.call(context.Background(), http.MethodPost, "status", manager.LinkComponentStatus{State: "stopped"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(lease); !os.IsNotExist(err) {
		t.Fatalf("lease kept after a clean stop: %v", err)
	}
}

// The node's control API is never reachable from the tailnet: the relay
// socket and the node's proxy refuse it, and the control socket refuses any
// request that names a tailnet peer.
func TestLinkNodeControlNotReachableFromTailnet(t *testing.T) {
	handled := make(chan string, 64)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handled <- r.URL.Path
		http.NotFound(w, r)
	})
	f := newFakeBroker(t, handler)
	data, _ := testIdentity(t)
	if err := writeNodeIdentity(f.identityDir, data); err != nil {
		t.Fatal(err)
	}
	dir := serveBroker(t, f.linkNodeBroker)
	paths := []string{
		"/link-node/v1/identity", "/link-node/v1/key", "/link-node/v1/config", "/link-node/v1/ensure-tags",
		"/link-node/v1/approve-route", "/link-node/v1/conflicts", "/link-node/v1/status", "/link-node/",
		"/link-node", "/x/../link-node/v1/identity", "//link-node/v1/identity", "/link-node/v1/../v1/key",
		"/%6cink-node/v1/identity", "/link-node%2Fv1%2Fidentity",
	}
	proxy := linkNodeManagerProxy(filepath.Join(dir, linkNodeRelaySocket))
	relay := &http.Client{Transport: unixTransport(filepath.Join(dir, linkNodeRelaySocket))}
	for _, p := range paths {
		for _, m := range []string{http.MethodGet, http.MethodPost} {
			req := httptest.NewRequest(m, "http://192.0.2.10:30190"+p, strings.NewReader("{}"))
			req.RemoteAddr = "100.64.0.7:51000"
			rec := httptest.NewRecorder()
			proxy.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Errorf("%s %s through the node's proxy: %d, want 404", m, p, rec.Code)
			}
			// Straight to the relay socket, as if the proxy let it through.
			rreq, _ := http.NewRequest(m, "http://juicemount-manager"+p, strings.NewReader("{}"))
			rreq.Header.Set(linkNodePeerHeader, "100.64.0.7:51000")
			resp, err := relay.Do(rreq)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound || strings.Contains(string(body), "tailscaled_state") || strings.Contains(string(body), "hskey") {
				t.Errorf("%s %s on the relay socket: %d %q, want 404", m, p, resp.StatusCode, body)
			}
		}
	}
	f.mu.Lock()
	minted := len(f.minted)
	f.mu.Unlock()
	if minted != 0 {
		t.Fatalf("a tailnet request minted %d keys", minted)
	}

	// The control socket refuses anything that names a tailnet peer.
	ctl := &http.Client{Transport: unixTransport(filepath.Join(dir, linkNodeControlSocket))}
	req, _ := http.NewRequest(http.MethodGet, "http://juicemount-manager/link-node/v1/identity", nil)
	req.Header.Set(linkNodePeerHeader, "100.64.0.7:51000")
	resp, err := ctl.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("control socket with a peer header: %d, want 403", resp.StatusCode)
	}
	// And never serves the Manager's API.
	resp, err = ctl.Get("http://juicemount-manager/api/sources")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("control socket served the Manager's API: %d", resp.StatusCode)
	}
	select {
	case p := <-handled:
		t.Fatalf("the Manager handler saw %q", p)
	default:
	}
}

// The Manager's API through the node: the Manager sees each peer's
// tailnet address, and only through the relay socket.
func TestLinkNodeManagerProxyPeerAddress(t *testing.T) {
	seen := make(chan *http.Request, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r
		_, _ = io.WriteString(w, "ok")
	})
	f := newFakeBroker(t, handler)
	dir := serveBroker(t, f.linkNodeBroker)
	relaySock := filepath.Join(dir, linkNodeRelaySocket)

	proxy := linkNodeManagerProxy(relaySock)
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

	// Straight to the relay socket without a peer: refused.
	c := &http.Client{Transport: unixTransport(relaySock)}
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

func TestLinkNodeIdentityValidation(t *testing.T) {
	data, pub := testIdentity(t)
	if got, err := identityNodeKey(data); err != nil || got != pub {
		t.Fatalf("valid identity: %q, %v; want %q", got, err, pub)
	}
	var st map[string][]byte
	_ = json.Unmarshal(data, &st)
	bad := func(edit func(map[string][]byte)) []byte {
		m := map[string][]byte{}
		for k, v := range st {
			m[k] = v
		}
		edit(m)
		out, _ := json.Marshal(m)
		return out
	}
	for name, b := range map[string][]byte{
		"not JSON":        []byte("hello"),
		"empty":           []byte("{}"),
		"no machine key":  bad(func(m map[string][]byte) { delete(m, "_machinekey") }),
		"bad machine key": bad(func(m map[string][]byte) { m["_machinekey"] = []byte("privkey:zz") }),
		"no profile":      bad(func(m map[string][]byte) { delete(m, "_current-profile") }),
		"missing profile": bad(func(m map[string][]byte) { delete(m, "profile-abcd") }),
		"no node key":     bad(func(m map[string][]byte) { m["profile-abcd"] = ipn.NewPrefs().ToBytes() }),
	} {
		if _, err := identityNodeKey(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestLinkNodeMigratesLegacyState(t *testing.T) {
	f := newFakeBroker(t, http.NotFoundHandler())
	c := newLinkNodeClient(serveBroker(t, f.linkNodeBroker))
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
	legacy, _ := testIdentity(t)
	if err := writeNodeIdentity(f.identityDir, legacy); err != nil {
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
	other, _ := testIdentity(t)
	if err := writeNodeIdentity(f.identityDir, other); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyNodeState(c, dir); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, linkNodeStateFile)); string(got) != string(legacy) {
		t.Fatalf("existing state replaced: %q", got)
	}

	// An unusable copy is refused, not installed.
	if err := os.WriteFile(filepath.Join(f.identityDir, linkNodeStateFile), []byte(`{"x":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyNodeState(c, filepath.Join(t.TempDir(), "s3")); err == nil {
		t.Fatal("an unusable identity was installed")
	}

	// The Manager unreachable with no identity yet: fail, so the supervisor
	// retries, rather than register a second node.
	gone := newLinkNodeClient(socketDir(t))
	if err := migrateLegacyNodeState(gone, filepath.Join(t.TempDir(), "s2")); err == nil {
		t.Fatal("migration without the Manager succeeded")
	}
}

// The node gives its identity back whenever it changes, so a rollback to
// the in-Manager node resumes the same node, also on an install that started
// with the node container (the Manager had no copy). The Manager stores only
// a valid identity of the NAS node.
func TestLinkNodeIdentitySync(t *testing.T) {
	f := newFakeBroker(t, http.NotFoundHandler())
	c := newLinkNodeClient(serveBroker(t, f.linkNodeBroker))
	dir := t.TempDir()
	ids := &identitySync{c: c, dir: dir}
	ctx := context.Background()
	managerCopy := filepath.Join(f.identityDir, linkNodeStateFile)
	if err := c.call(ctx, http.MethodPost, "key", struct{}{}, &linkNodeKeyResponse{}); err != nil {
		t.Fatal(err)
	}

	if err := ids.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(managerCopy); !os.IsNotExist(err) {
		t.Fatalf("a copy without an identity: %v", err)
	}
	id1, pub1 := testIdentity(t)
	f.registered(pub1, "juicemount-nas", 1) // registered with the minted key
	if err := writeNodeIdentity(dir, id1); err != nil {
		t.Fatal(err)
	}
	if err := ids.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(managerCopy); err != nil || string(got) != string(id1) {
		t.Fatalf("Manager's copy = %q, %v", got, err)
	}
	if fi, _ := os.Stat(managerCopy); fi.Mode().Perm() != 0o600 {
		t.Fatalf("Manager's copy mode %v", fi.Mode())
	}
	// Unchanged: not sent again.
	if err := os.WriteFile(managerCopy, []byte("touched"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ids.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(managerCopy); string(got) != "touched" {
		t.Fatalf("unchanged identity sent again: %q", got)
	}
	_ = writeNodeIdentity(f.identityDir, id1)

	// Refused: not tsnet state, and the identity of a node that is not the
	// NAS node. The Manager's copy stays.
	for name, blob := range map[string][]byte{"garbage": []byte(`{"_machinekey":"x"}`), "empty": nil} {
		if err := c.call(ctx, http.MethodPost, "identity", linkNodeIdentity{State: blob}, nil); err == nil {
			t.Errorf("%s identity accepted", name)
		}
	}
	foreign, foreignPub := testIdentity(t)
	f.registered(foreignPub, "juicemount-nas", 99)
	err := c.call(ctx, http.MethodPost, "identity", linkNodeIdentity{State: foreign}, nil)
	var he *linkNodeHTTPError
	if !errors.As(err, &he) || he.Code != http.StatusForbidden {
		t.Fatalf("a foreign identity: %v, want 403", err)
	}
	if got, _ := os.ReadFile(managerCopy); string(got) != string(id1) {
		t.Fatal("the Manager's copy was replaced by a refused identity")
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
	gone := newLinkNodeClient(socketDir(t))
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
	sup = newLinkNodeSupervisor(newLinkNodeClient(serveBroker(t, f.linkNodeBroker)), t.TempDir())
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

// After a rollback to JM_NET_NAS_NODE=on, a node container left running
// is told so (409) and stops; the in-Manager node waits while one reaches
// the Manager or its lease is fresh.
func TestLinkNodeRollbackGuard(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	g := &linkNodeGuard{identityDir: t.TempDir(), now: func() time.Time { return now }}
	if why := g.blocked(); why != "" {
		t.Fatalf("blocked with no node container: %s", why)
	}
	dir := socketDir(t)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveLinkNodeSocket(filepath.Join(dir, linkNodeControlSocket), g, stop)
	}()
	defer func() { close(stop); <-done }()
	c := newLinkNodeClient(dir)
	var err error
	for i := 0; i < 200; i++ {
		if err = c.call(context.Background(), http.MethodGet, "config", nil, &linkNodeSettings{}); !strings.Contains(err.Error(), "unreachable") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !errors.Is(err, errLinkNodeOwnedByManager) {
		t.Fatalf("node container under JM_NET_NAS_NODE=on: %v", err)
	}
	if err := c.orphaned(time.Now()); !errors.Is(err, errLinkNodeOwnedByManager) {
		t.Fatalf("orphaned = %v", err)
	}
	if why := g.blocked(); !strings.Contains(why, "juicemount-link") {
		t.Fatalf("not blocked while a node container calls: %q", why)
	}
	now = now.Add(linkNodeStaleAfter + time.Second)
	if why := g.blocked(); why != "" {
		t.Fatalf("still blocked after the node container went quiet: %s", why)
	}

	// The old overlay (no shared socket volume): a fresh lease blocks
	// until the node container must have stopped its node.
	lease := filepath.Join(g.identityDir, linkNodeLeaseFile)
	if err := os.WriteFile(lease, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(lease, now, now)
	if why := g.blocked(); !strings.Contains(why, "waiting") {
		t.Fatalf("not blocked by a fresh lease: %q", why)
	}
	now = now.Add(linkNodeOrphanAfter + linkNodeLeaseEvery)
	if why := g.blocked(); why == "" {
		t.Fatal("unblocked before the rollback wait")
	}
	now = now.Add(linkNodeRollbackWait - linkNodeOrphanAfter - linkNodeLeaseEvery)
	if why := g.blocked(); why != "" {
		t.Fatalf("blocked by an old lease: %s", why)
	}
}

// A node container that has lost the Manager for linkNodeOrphanAfter stops
// its node; the in-Manager supervisor waits while blocked.
func TestLinkNodeOrphanedAndBlockedSupervisor(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	c := newLinkNodeClient(socketDir(t))
	c.now = func() time.Time { return now }
	if err := c.orphaned(now); err != nil {
		t.Fatalf("orphaned at once: %v", err)
	}
	now = now.Add(linkNodeOrphanAfter)
	if err := c.orphaned(now.Add(-linkNodeOrphanAfter)); err == nil || !strings.Contains(err.Error(), "has not answered") {
		t.Fatalf("not orphaned after %s: %v", linkNodeOrphanAfter, err)
	}

	why := "a Link node container is still running"
	var mu sync.Mutex
	sup := newNASNodeSupervisor(filepath.Join(t.TempDir(), "config.yaml"), nil, http.NotFoundHandler(), func() string {
		mu.Lock()
		defer mu.Unlock()
		return why
	})
	sup.Start()
	defer sup.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := sup.status.snapshot()
		if st.State == "starting" && st.Detail == why && st.Restarts == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("in-Manager node not held back: %+v", st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLinkNodeHealth(t *testing.T) {
	p := filepath.Join(t.TempDir(), "health.json")
	now := time.Now()
	if err := linkNodeHealthy(p, now); err == nil {
		t.Fatal("healthy without a report")
	}
	writeLinkNodeHealth(p, &manager.LinkComponentStatus{State: "backoff", LastError: "boom"})
	if err := linkNodeHealthy(p, now); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("backoff: %v", err)
	}
	writeLinkNodeHealth(p, &manager.LinkComponentStatus{State: "running"})
	if err := linkNodeHealthy(p, time.Now()); err != nil {
		t.Fatalf("running: %v", err)
	}
	if err := linkNodeHealthy(p, time.Now().Add(linkNodeStaleAfter+time.Second)); err == nil {
		t.Fatal("a stale report is healthy")
	}
}

func TestListenLinkNodeSocket(t *testing.T) {
	dir := socketDir(t)
	p := filepath.Join(dir, "sub", "m.sock")
	ln, err := listenLinkNodeSocket(p)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != 0o660 {
		t.Fatalf("socket mode %v, %v", fi.Mode(), err)
	}
	// A stale socket from a previous Manager is replaced.
	if ul, ok := ln.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}
	_ = ln.Close()
	ln, err = listenLinkNodeSocket(p)
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
