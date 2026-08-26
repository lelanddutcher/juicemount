// JuiceMount Link unit tests (T2.2/T2.3): one-off pairing keys and the
// subnet-route approval helpers. The CLI is faked with a shell script so
// these exercise the real handler + command-construction paths without a
// live headscale.
package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// fakeHeadscale writes an executable shell script that logs its argv to a
// file and prints a plausible preauth key / node listing, letting tests
// assert on the exact CLI invocations handlePair + ApproveSubnetRoutes make.
func fakeHeadscale(t *testing.T, dir string, stdout string) string {
	t.Helper()
	bin := filepath.Join(dir, "fake-headscale.sh")
	script := "#!/bin/sh\necho \"$@\" >> " + dir + "/argv.log\n" +
		"case \" $* \" in\n" +
		"  *\" nodes list-routes \"*) cat " + dir + "/routes.txt; exit 0 ;;\n" +
		"  *\" nodes list \"*) cat " + dir + "/nodes.json; exit 0 ;;\n" +
		"  *\" nodes approve-routes \"*) test ! -f " + dir + "/routes-approved.txt || cp " + dir + "/routes-approved.txt " + dir + "/routes.txt; echo node updated; exit 0 ;;\n" +
		"esac\n" +
		"echo " + stdout + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func setLinkEnv(t *testing.T, bin, cfgPath string) {
	t.Helper()
	oldBin, oldCfg := os.Getenv("JM_HEADSCALE_BIN"), os.Getenv("JM_HEADSCALE_CONFIG")
	os.Setenv("JM_HEADSCALE_BIN", bin)
	os.Setenv("JM_HEADSCALE_CONFIG", cfgPath)
	t.Cleanup(func() {
		os.Setenv("JM_HEADSCALE_BIN", oldBin)
		os.Setenv("JM_HEADSCALE_CONFIG", oldCfg)
	})
}

func postPair(t *testing.T, query string) map[string]any {
	t.Helper()
	a := &API{} // adminKey empty → auth disabled in tests
	req := httptest.NewRequest(http.MethodPost, "/api/net/pair"+query, nil)
	rec := httptest.NewRecorder()
	a.handlePair(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handlePair status = %d, want 200", rec.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body %q)", err, rec.Body.String())
	}
	return resp
}

func TestLinkStatusRequiresRealConfiguration(t *testing.T) {
	status := func() LinkStatus {
		t.Helper()
		a := &API{}
		rec := httptest.NewRecorder()
		a.handleLinkStatus(rec, httptest.NewRequest(http.MethodGet, "/api/net/link", nil))
		var got LinkStatus
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Setenv("JM_NET_HEADSCALE", "off")
	t.Setenv("JM_HEADSCALE_CONFIG", "")
	t.Setenv("JM_NET_SERVER_URL", "")
	if got := status(); got.Enabled || got.Error == "" {
		t.Errorf("unconfigured status = %+v, want disabled with an explanation", got)
	}

	t.Setenv("JM_NET_SERVER_URL", "https://link.example.test")
	if got := status(); got.Enabled || !strings.Contains(got.Error, "disabled") {
		t.Errorf("binary-only status = %+v, want disabled", got)
	}

	t.Setenv("JM_HEADSCALE_CONFIG", "/operator/headscale/config.yaml")
	if got := status(); !got.Enabled || got.ServerURL != "https://link.example.test" {
		t.Errorf("external configured status = %+v, want enabled external Link", got)
	}
}

// TestPairOneOffByDefault: T2.3 — a plain POST must mint a NON-reusable key.
// The fake bin's argv log is the proof: no "--reusable" flag on the wire.
func TestPairOneOffByDefault(t *testing.T) {
	dir := t.TempDir()
	bin := fakeHeadscale(t, dir, "tskey-auth-abc123")
	setLinkEnv(t, bin, dir+"/config.yaml")

	resp := postPair(t, "?device=Lelands-MacBook-Pro.local")
	if resp["ok"] != true {
		t.Fatalf("ok = %v, want true (%v)", resp["ok"], resp)
	}
	if resp["reusable"] != false {
		t.Errorf("reusable = %v, want false by default", resp["reusable"])
	}
	if resp["code"] != "tskey-auth-abc123" {
		t.Errorf("code = %v, want minted key", resp["code"])
	}
	if resp["device"] != "Lelands-MacBook-Pro.local" {
		t.Errorf("device = %v, want echoed hint", resp["device"])
	}
	argv, err := os.ReadFile(filepath.Join(dir, "argv.log"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(argv), "--reusable") {
		t.Errorf("default pair minted a REUSABLE key: argv %q", string(argv))
	}
}

// TestPairMultiFallback: ?multi=1 restores the legacy reusable behavior.
func TestPairMultiFallback(t *testing.T) {
	dir := t.TempDir()
	bin := fakeHeadscale(t, dir, "tskey-auth-multi")
	setLinkEnv(t, bin, dir+"/config.yaml")

	resp := postPair(t, "?multi=1")
	if resp["reusable"] != true {
		t.Errorf("reusable = %v, want true with multi=1", resp["reusable"])
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv.log"))
	if !strings.Contains(string(argv), "--reusable") {
		t.Errorf("multi=1 did not pass --reusable: argv %q", string(argv))
	}
}

func TestSanitizeDeviceName(t *testing.T) {
	cases := map[string]string{
		"Mac-Book.Pro_1": "", // underscore → reject whole hint
		"  nas  ":        "nas",
		"":               "",
		"a.b-c9":         "a.b-c9",
	}
	for in, want := range cases {
		if got := sanitizeDeviceName(in); got != want {
			t.Errorf("sanitizeDeviceName(%q) = %q, want %q", in, got, want)
		}
	}
	long := strings.Repeat("a", 100)
	if got := sanitizeDeviceName(long); len(got) != 63 {
		t.Errorf("sanitizeDeviceName(100×a) len = %d, want capped at 63", len(got))
	}
}

const twoNodesJSON = `[
  {"id": 3, "name": "juicemount-nas", "given_name": "juicemount-nas",
   "available_routes": ["192.168.0.0/24"], "approved_routes": []},
  {"id": 4, "name": "mac.fqdn.example.com", "given_name": "mac",
   "available_routes": [], "approved_routes": ["10.0.0.0/8"]}
]`

func TestParseHSNodes(t *testing.T) {
	nodes, err := parseHSNodes([]byte(twoNodesJSON))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(nodes) != 2 || nodes[0].ID != 3 || nodes[0].AvailableRoutes[0] != "192.168.0.0/24" {
		t.Errorf("unexpected parse result: %+v", nodes)
	}
	if _, err := parseHSNodes([]byte("not json")); err == nil {
		t.Error("expected error for non-JSON input")
	}
	if nodes, err := parseHSNodes([]byte("[]")); err != nil || len(nodes) != 0 {
		t.Errorf("empty array should parse to zero nodes, got %v %v", nodes, err)
	}
}

func TestNodeForHost(t *testing.T) {
	nodes, _ := parseHSNodes([]byte(twoNodesJSON))
	if n := nodeForHost(nodes, "juicemount-nas"); n == nil || n.ID != 3 {
		t.Errorf("given_name match failed: %+v", n)
	}
	if n := nodeForHost(nodes, "mac"); n == nil || n.ID != 4 {
		t.Errorf("bare-given match failed: %+v", n)
	}
	// headscale's name field carries the fqdn form; prefix match must find it
	if n := nodeForHost(nodes, "mac.fqdn.example.com"); n == nil || n.ID != 4 {
		t.Errorf("fqdn name match failed: %+v", n)
	}
	if nodeForHost(nodes, "nope") != nil {
		t.Error("unknown host should return nil")
	}
	if nodeForHost(nodes, "") != nil {
		t.Error("empty host should return nil")
	}
}

func TestPendingApprovalAndMerge(t *testing.T) {
	nodes, _ := parseHSNodes([]byte(twoNodesJSON))
	nas := nodeForHost(nodes, "juicemount-nas")

	got := pendingApproval(nas, []string{"192.168.0.0/24"})
	if len(got) != 1 || got[0] != "192.168.0.0/24" {
		t.Errorf("pendingApproval = %v, want the advertised-but-unapproved prefix", got)
	}
	// Not advertised → nothing pending (approval would fail server-side).
	if got := pendingApproval(nas, []string{"10.1.0.0/16"}); len(got) != 0 {
		t.Errorf("pendingApproval for un-advertised prefix = %v, want empty", got)
	}
	// Already approved → idempotent no-op.
	mac := nodeForHost(nodes, "mac")
	if got := pendingApproval(mac, []string{"10.0.0.0/8"}); len(got) != 0 {
		t.Errorf("pendingApproval for approved prefix = %v, want empty", got)
	}
	if pendingApproval(nil, []string{"10.0.0.0/8"}) != nil {
		t.Error("nil node should yield nil")
	}

	merged := mergeApproved([]string{"10.0.0.0/8"}, []string{"10.0.0.0/8", "192.168.0.0/24"})
	want := []string{"10.0.0.0/8", "192.168.0.0/24"}
	if !reflect.DeepEqual(merged, want) {
		t.Errorf("mergeApproved = %v, want %v (approve-routes REPLACES the set)", merged, want)
	}
}

func TestParseRouteTable(t *testing.T) {
	raw := "\x1b[96mID\x1b[0m | Hostname | Approved | Available | Serving (Primary)\n" +
		"10 | juicemount-nas | 172.16.5.0/24 | 192.168.0.0/24, 172.16.5.0/24 | 192.168.0.0/24\n"
	approved, available, err := parseRouteTable([]byte(raw), 10)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(approved, []string{"172.16.5.0/24"}) {
		t.Fatalf("approved = %v", approved)
	}
	if !reflect.DeepEqual(available, []string{"192.168.0.0/24", "172.16.5.0/24"}) {
		t.Fatalf("available = %v", available)
	}
	if _, _, err := parseRouteTable([]byte("not a table"), 10); err == nil {
		t.Fatal("malformed route table was accepted")
	}
}

// TestApproveSubnetRoutesEndToEnd drives ApproveSubnetRoutes against the
// fake CLI and asserts BOTH commands it must issue: the json listing and the
// approve-routes call carrying the MERGED route set.
func TestApproveSubnetRoutesEndToEnd(t *testing.T) {
	dir := t.TempDir()
	bin := fakeHeadscale(t, dir, "node updated")
	setLinkEnv(t, bin, dir+"/config.yaml")
	// operator had already approved one extra route; merge must preserve it
	if err := os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(`[
      {"id": 3, "name": "juicemount-nas", "given_name": "juicemount-nas",
       "available_routes": ["192.168.0.0/24"], "approved_routes": ["172.16.5.0/24"]}
    ]`), 0o644); err != nil {
		t.Fatal(err)
	}
	before := "ID | Hostname | Approved | Available | Serving (Primary)\n" +
		"3 | juicemount-nas | 172.16.5.0/24 | 192.168.0.0/24, 172.16.5.0/24 |\n"
	after := "ID | Hostname | Approved | Available | Serving (Primary)\n" +
		"3 | juicemount-nas | 172.16.5.0/24, 192.168.0.0/24 | 192.168.0.0/24, 172.16.5.0/24 | 192.168.0.0/24\n"
	if err := os.WriteFile(filepath.Join(dir, "routes.txt"), []byte(before), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "routes-approved.txt"), []byte(after), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := ApproveSubnetRoutes(bin, dir+"/config.yaml", "juicemount-nas", []string{"192.168.0.0/24"}); err != nil {
		t.Fatalf("ApproveSubnetRoutes: %v", err)
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv.log"))
	s := string(argv)
	if !strings.Contains(s, "approve-routes") || !strings.Contains(s, "--identifier 3") ||
		!strings.Contains(s, "192.168.0.0/24") {
		t.Errorf("approve command missing pieces, argv log:\n%s", s)
	}
	if !strings.Contains(s, "172.16.5.0/24") {
		t.Errorf("merge dropped pre-existing approved route, argv log:\n%s", s)
	}

	// Unknown node → retriable error, not a silent success.
	if err := ApproveSubnetRoutes(bin, dir+"/config.yaml", "ghost", []string{"192.168.0.0/24"}); err == nil {
		t.Error("expected error for unregistered node")
	}
}
