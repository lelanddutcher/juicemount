package manager

// Link stages 0-2: user lookup by name, tagged keys, and supervisor state in
// GET /api/net/link.

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseHeadscaleUserIDFindsUserByName(t *testing.T) {
	users := []byte(`[{"id": 1, "name": "someone-else"}, {"id": "7", "name": "jm"}]`)
	if id, err := parseHeadscaleUserID(users, "jm"); err != nil || id != 7 {
		t.Fatalf("parseHeadscaleUserID = (%d, %v), want 7", id, err)
	}
	if _, err := parseHeadscaleUserID(users, "missing"); err == nil {
		t.Fatal("missing user resolved to an ID")
	}
	if _, err := parseHeadscaleUserID([]byte(`[{"id": 0, "name": "jm"}]`), "jm"); err == nil {
		t.Fatal("zero user ID accepted")
	}
	if _, err := parseHeadscaleUserID([]byte(`not json`), "jm"); err == nil {
		t.Fatal("malformed listing accepted")
	}
}

func TestPreauthKeyArgs(t *testing.T) {
	got := PreauthKeyArgs(7, false, []string{LinkTagMac})
	want := []string{"preauthkeys", "create", "--user", "7", "--tags", "tag:jm-mac"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PreauthKeyArgs = %q, want %q", got, want)
	}
	got = PreauthKeyArgs(3, true, nil)
	want = []string{"preauthkeys", "create", "--user", "3", "--reusable"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PreauthKeyArgs untagged = %q, want %q", got, want)
	}
}

// TestPairUsesLinkUserIDAndMacTag: the Link user is not ID 1 on a control
// server where another user was created first, and keys from the embedded
// control server carry tag:jm-mac so the ACL policy applies to them.
func TestPairUsesLinkUserIDAndMacTag(t *testing.T) {
	dir := t.TempDir()
	bin := fakeHeadscale(t, dir, "tskey-auth-tagged")
	setLinkEnv(t, bin, filepath.Join(dir, "config.yaml"))
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_NAS_NODE", "on")
	t.Setenv("JM_NET_POLICY", "")
	t.Setenv("JM_HEADSCALE_USER", "")
	if err := os.WriteFile(filepath.Join(dir, "users.json"), []byte(`[{"id": 1, "name": "admin"}, {"id": 4, "name": "jm"}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	if resp := postPair(t, ""); resp["ok"] != true {
		t.Fatalf("pair = %v", resp)
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv.log"))
	if !strings.Contains(string(argv), "preauthkeys create --user 4 --tags tag:jm-mac") {
		t.Fatalf("pair did not mint a tag:jm-mac key for user 4: argv %q", argv)
	}
}

// TestPairLeavesExternalHeadscaleKeysUntagged: an operator's own Headscale
// owns its policy, which need not define JuiceMount's tags.
func TestPairLeavesExternalHeadscaleKeysUntagged(t *testing.T) {
	dir := t.TempDir()
	bin := fakeHeadscale(t, dir, "tskey-auth-plain")
	setLinkEnv(t, bin, filepath.Join(dir, "config.yaml"))
	t.Setenv("JM_NET_HEADSCALE", "off")
	if resp := postPair(t, ""); resp["ok"] != true {
		t.Fatalf("pair = %v", resp)
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv.log"))
	if strings.Contains(string(argv), "--tags") || !strings.Contains(string(argv), "preauthkeys create --user 1") {
		t.Fatalf("external pair argv %q, want untagged key for user 1", argv)
	}
}

func TestLinkPolicyManaged(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_NAS_NODE", "on")
	t.Setenv("JM_NET_POLICY", "")
	if !LinkPolicyManaged(cfg) {
		t.Fatal("embedded Headscale without a config must be managed")
	}
	if err := os.WriteFile(cfg, []byte("server_url: https://link.example.com\n# policy: comments do not count\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !LinkPolicyManaged(cfg) {
		t.Fatal("generated config without a policy stanza must be managed")
	}
	if err := os.WriteFile(cfg, []byte("server_url: https://link.example.com\npolicy:\n  path: /etc/acl.hujson\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if LinkPolicyManaged(cfg) {
		t.Fatal("operator policy stanza must not be overridden")
	}
	_ = os.Remove(cfg)
	t.Setenv("JM_NET_POLICY", "off")
	if LinkPolicyManaged(cfg) {
		t.Fatal("JM_NET_POLICY=off must disable the managed policy")
	}
	t.Setenv("JM_NET_POLICY", "")
	// Without the embedded NAS node an operator's own router carries the
	// NAS (JM_NET_NAS_ROUTE set or not). Its services are unknown to the
	// Manager, so a policy it wrote would deny Link traffic that every
	// earlier release allowed.
	t.Setenv("JM_NET_NAS_NODE", "off")
	for _, route := range []string{"", "192.0.2.0/24"} {
		t.Setenv("JM_NET_NAS_ROUTE", route)
		if LinkPolicyManaged(cfg) {
			t.Fatalf("policy managed without the embedded NAS node (route %q)", route)
		}
	}
	t.Setenv("JM_NET_NAS_ROUTE", "")
	t.Setenv("JM_NET_NAS_NODE", "on")
	t.Setenv("JM_NET_HEADSCALE", "off")
	if LinkPolicyManaged(cfg) {
		t.Fatal("external Headscale policy must not be managed")
	}
}

// TestLinkStatusReportsSupervisedComponents: in service exposure the NAS
// node is the route, so route readiness follows its supervisor, and both
// components' states reach the API.
func TestLinkStatusReportsSupervisedComponents(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	oldAddr := embeddedHeadscaleHealthAddr
	embeddedHeadscaleHealthAddr = ln.Addr().String()
	t.Cleanup(func() { embeddedHeadscaleHealthAddr = oldAddr })
	t.Cleanup(func() { SetLinkRuntimeStatus(nil) })

	dir := t.TempDir()
	setLinkEnv(t, fakeHeadscale(t, dir, "unused"), filepath.Join(dir, "config.yaml"))
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_NAS_NODE", "on")
	t.Setenv("JM_NET_NAS_ROUTE", "")
	t.Setenv("JM_NET_NAS_EXPOSURE", "")
	t.Setenv("JM_NET_POLICY", "")
	if err := os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(`[{"id": 1, "node_key": "`+nodeKeyNAS+`", "name": "juicemount-nas", "given_name": "juicemount-nas", "tags": ["tag:jm-nas"], "online": true}]`), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "routes.txt"), []byte("ID | Hostname | Approved | Available | Serving (Primary)\n1 | juicemount-nas |  |  | \n"), 0o644); err != nil {
		t.Fatal(err)
	}

	status := func() LinkStatus {
		t.Helper()
		rec := httptest.NewRecorder()
		(&API{}).handleLinkStatus(rec, httptest.NewRequest(http.MethodGet, "/api/net/link", nil))
		var got LinkStatus
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	if got := status(); got.RouteReady || !strings.Contains(got.Error, "unavailable") {
		t.Fatalf("status without supervisors = %+v, want route not ready", got)
	}

	runtime := LinkRuntimeStatus{
		Headscale: &LinkComponentStatus{State: "running"},
		NASNode:   &LinkComponentStatus{State: "backoff", LastError: "nas node up: control unreachable", Restarts: 3},
	}
	SetLinkRuntimeStatus(func() LinkRuntimeStatus { return runtime })
	got := status()
	if got.Exposure != "services" || !got.PolicyManaged || got.RouteReady || !got.ControlReady {
		t.Fatalf("backoff status = %+v", got)
	}
	if got.NASNode == nil || got.NASNode.Restarts != 3 || !strings.Contains(got.Error, "backoff: nas node up: control unreachable") {
		t.Fatalf("backoff status lost the NAS node state: %+v (nas %+v)", got, got.NASNode)
	}

	runtime.NASNode = &LinkComponentStatus{State: "running"}
	if got := status(); got.RouteReady || !strings.Contains(got.Error, "node key") {
		t.Fatalf("running status without the node's key = %+v", got)
	}
	runtime.NASNode = &LinkComponentStatus{State: "running", NodeKey: nodeKeyNAS}
	if got := status(); !got.RouteReady || got.Headscale == nil || got.Headscale.State != "running" || got.NASNode.NodeKey != nodeKeyNAS {
		t.Fatalf("running status = %+v", got)
	}

	runtime.Headscale = &LinkComponentStatus{State: "backoff", LastError: "headscale exited: exit status 1"}
	if got := status(); got.ControlReady || got.DataPlaneReady || !strings.Contains(got.Error, "headscale exited") {
		t.Fatalf("control backoff status = %+v", got)
	}
}

// TestEnsureNodeTagsTagsLegacyNode: a NAS node registered before keys were
// tagged is tagged server-side (keeping its identity), and an already
// tagged node is left alone.
func TestEnsureNodeTagsTagsLegacyNode(t *testing.T) {
	dir := t.TempDir()
	bin := fakeHeadscale(t, dir, "unused")
	cfg := filepath.Join(dir, "config.yaml")
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Node 2 registered first under the NAS node's name; only the key
	// selects the NAS node.
	impostor := `{"id": 2, "node_key": "` + nodeKeyMac + `", "name": "juicemount-nas", "given_name": "juicemount-nas", "tags": ["tag:jm-mac"]}`
	write("nodes.json", `[`+impostor+`, {"id": 3, "node_key": "`+nodeKeyNAS+`", "name": "juicemount-nas", "given_name": "juicemount-nas-1", "tags": ["tag:other"]}]`)
	write("nodes-tagged.json", `[`+impostor+`, {"id": 3, "node_key": "`+nodeKeyNAS+`", "name": "juicemount-nas", "given_name": "juicemount-nas-1", "tags": ["tag:other", "tag:jm-nas"]}]`)
	changed, err := EnsureNodeTags(bin, cfg, nodeKeyNAS, []string{LinkTagNAS})
	if err != nil || !changed {
		t.Fatalf("EnsureNodeTags = (%v, %v), want tagged", changed, err)
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv.log"))
	if !strings.Contains(string(argv), "nodes tag --identifier 3 --tags tag:other,tag:jm-nas") {
		t.Fatalf("tag command dropped existing tags or targeted the wrong node: %q", argv)
	}
	if err := os.Remove(filepath.Join(dir, "argv.log")); err != nil {
		t.Fatal(err)
	}
	changed, err = EnsureNodeTags(bin, cfg, nodeKeyNAS, []string{LinkTagNAS})
	if err != nil || changed {
		t.Fatalf("already tagged: EnsureNodeTags = (%v, %v)", changed, err)
	}
	if argv, _ := os.ReadFile(filepath.Join(dir, "argv.log")); strings.Contains(string(argv), "nodes tag") {
		t.Fatalf("already tagged node was retagged: %q", argv)
	}

	// The control server ignoring the request must not pass silently.
	write("nodes.json", `[{"id": 3, "node_key": "`+nodeKeyNAS+`", "name": "juicemount-nas", "given_name": "juicemount-nas"}]`)
	_ = os.Remove(filepath.Join(dir, "nodes-tagged.json"))
	if _, err := EnsureNodeTags(bin, cfg, nodeKeyNAS, []string{LinkTagNAS}); err == nil || !strings.Contains(err.Error(), "still missing") {
		t.Fatalf("unverified tagging error = %v", err)
	}
	_ = os.Remove(filepath.Join(dir, "argv.log"))
	write("nodes.json", `[`+impostor+`]`)
	if _, err := EnsureNodeTags(bin, cfg, nodeKeyNAS, []string{LinkTagNAS}); err == nil {
		t.Fatal("unregistered node tagged")
	}
	if argv, _ := os.ReadFile(filepath.Join(dir, "argv.log")); strings.Contains(string(argv), "nodes tag") {
		t.Fatalf("a node with only the NAS node's name was tagged: %q", argv)
	}
}
