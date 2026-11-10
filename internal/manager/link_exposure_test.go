package manager

// Link NAS exposure: default selection (fresh install, upgrade, explicit),
// route readiness that matches what a Mac can reach, and remote farm
// enrollment plans in each exposure.

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setExposureEnv(t *testing.T) string {
	t.Helper()
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_NAS_NODE", "on")
	t.Setenv("JM_NET_NAS_ROUTE", "")
	t.Setenv("JM_NET_NAS_EXPOSURE", "")
	t.Setenv("JM_NET_NAS_LAN_ADDR", "")
	return filepath.Join(t.TempDir(), "config.yaml")
}

func TestLinkExposureDefaultSelection(t *testing.T) {
	check := func(cfgPath, wantMode, wantSubnet string) {
		t.Helper()
		mode, subnet, err := LinkExposure(cfgPath)
		if err != nil || mode != wantMode || subnet != wantSubnet {
			t.Fatalf("LinkExposure = (%q, %q, %v), want (%q, %q)", mode, subnet, err, wantMode, wantSubnet)
		}
	}
	mkState := func(cfgPath string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(filepath.Dir(cfgPath), "nas-node"), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	// Fresh install: services, recorded before the node creates its state.
	fresh := setExposureEnv(t)
	check(fresh, LinkExposureServices, "")
	if mode, err := RecordLinkExposureDefault(fresh); err != nil || mode != LinkExposureServices {
		t.Fatalf("record fresh = %q, %v", mode, err)
	}
	mkState(fresh)
	check(fresh, LinkExposureServices, "")

	// Upgrade: a NAS node from before service exposure (its state exists,
	// nothing recorded) keeps the subnet route its Mac apps follow, at the
	// default prefix it always advertised, and keeps it once recorded.
	upgrade := filepath.Join(t.TempDir(), "config.yaml")
	mkState(upgrade)
	check(upgrade, LinkExposureSubnet, NasLanSubnet)
	if mode, err := RecordLinkExposureDefault(upgrade); err != nil || mode != LinkExposureSubnet {
		t.Fatalf("record upgrade = %q, %v", mode, err)
	}
	check(upgrade, LinkExposureSubnet, NasLanSubnet)
	t.Setenv("JM_NET_NAS_ROUTE", "192.0.2.0/24")
	check(upgrade, LinkExposureSubnet, "192.0.2.0/24")

	// Explicit choices win over the recorded default.
	check(fresh, LinkExposureSubnet, "192.0.2.0/24")
	t.Setenv("JM_NET_NAS_ROUTE", "")
	t.Setenv("JM_NET_NAS_EXPOSURE", "subnet")
	check(fresh, LinkExposureSubnet, NasLanSubnet)
	t.Setenv("JM_NET_NAS_EXPOSURE", "services")
	check(upgrade, LinkExposureServices, "")
	t.Setenv("JM_NET_NAS_ROUTE", "192.0.2.0/24")
	if _, _, err := LinkExposure(upgrade); err == nil {
		t.Fatal("services exposure with a route accepted")
	}
	t.Setenv("JM_NET_NAS_ROUTE", "")
	t.Setenv("JM_NET_NAS_EXPOSURE", "lan")
	if _, _, err := LinkExposure(upgrade); err == nil {
		t.Fatal("unknown exposure accepted")
	}

	// No embedded NAS node: only an explicit route selects anything.
	t.Setenv("JM_NET_NAS_EXPOSURE", "")
	t.Setenv("JM_NET_NAS_NODE", "off")
	check(upgrade, "", "")
	t.Setenv("JM_NET_NAS_ROUTE", "192.0.2.0/24")
	check(upgrade, LinkExposureSubnet, "192.0.2.0/24")
}

func linkStatusFor(t *testing.T) LinkStatus {
	t.Helper()
	rec := httptest.NewRecorder()
	(&API{}).handleLinkStatus(rec, httptest.NewRequest(http.MethodGet, "/api/net/link", nil))
	var got LinkStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// TestLinkStatusServiceRouteNeedsTaggedOnlineNode: in service exposure a
// Mac finds the NAS node only by tag:jm-nas, so a running NAS node is not a
// ready route until the control server lists it online with that tag. This
// holds with the Manager's policy and with JM_NET_POLICY=off.
func TestLinkStatusServiceRouteNeedsTaggedOnlineNode(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	oldAddr := embeddedHeadscaleHealthAddr
	embeddedHeadscaleHealthAddr = ln.Addr().String()
	t.Cleanup(func() { embeddedHeadscaleHealthAddr = oldAddr })
	t.Cleanup(func() { SetLinkRuntimeStatus(nil) })
	SetLinkRuntimeStatus(func() LinkRuntimeStatus {
		return LinkRuntimeStatus{Headscale: &LinkComponentStatus{State: "running"}, NASNode: &LinkComponentStatus{State: "running", NodeKey: nodeKeyNAS}}
	})

	for _, policy := range []string{"", "off"} {
		dir := t.TempDir()
		setLinkEnv(t, fakeHeadscale(t, dir, "unused"), filepath.Join(dir, "config.yaml"))
		setExposureEnv(t)
		t.Setenv("JM_NET_POLICY", policy)
		nodes := func(body string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		routes := func(rows string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(dir, "routes.txt"), []byte("ID | Hostname | Approved | Available | Serving (Primary)\n"+rows), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		routes("")
		nodes(`[{"id": 1, "node_key": "` + nodeKeyNAS + `", "name": "juicemount-nas", "given_name": "juicemount-nas", "online": true}]`)
		if got := linkStatusFor(t); got.Exposure != LinkExposureServices || got.RouteReady || !strings.Contains(got.Error, "not tagged tag:jm-nas") {
			t.Fatalf("policy %q, untagged NAS node: status = %+v", policy, got)
		}
		nodes(`[{"id": 1, "node_key": "` + nodeKeyNAS + `", "name": "juicemount-nas", "given_name": "juicemount-nas", "tags": ["tag:jm-nas"], "online": false}]`)
		if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, "offline") {
			t.Fatalf("policy %q, offline NAS node: status = %+v", policy, got)
		}
		// Registered under the NAS node's name, but not holding its key.
		nodes(`[{"id": 1, "node_key": "` + nodeKeyMac + `", "name": "juicemount-nas", "given_name": "juicemount-nas", "tags": ["tag:jm-nas"], "online": true}]`)
		if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, "not registered") {
			t.Fatalf("policy %q, unregistered NAS node: status = %+v", policy, got)
		}
		nas := `{"id": 2, "node_key": "` + nodeKeyNAS + `", "name": "juicemount-nas", "given_name": "juicemount-nas-1", "tags": ["tag:jm-nas"], "online": true}`
		nodes(`[` + nas + `]`)
		if got := linkStatusFor(t); !got.RouteReady || !got.ControlReady {
			t.Fatalf("policy %q, tagged online NAS node: status = %+v", policy, got)
		}
		// Another node's approved route over the NAS's LAN address could
		// carry a client's traffic: reported. With the address known, a
		// route elsewhere is not.
		other := `{"id": 4, "node_key": "` + nodeKeyMac + `", "name": "router", "online": true}`
		nodes(`[` + other + `, ` + nas + `]`)
		routes("4 | router | 192.0.2.0/24 | 192.0.2.0/24 | 192.0.2.0/24\n")
		if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, `node 4 ("router") holds the approved route 192.0.2.0/24`) {
			t.Fatalf("policy %q, approved route, NAS address unknown: status = %+v", policy, got)
		}
		t.Setenv("JM_NET_NAS_LAN_ADDR", "192.0.2.10")
		if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, "node 4") {
			t.Fatalf("policy %q, approved route over the NAS address: status = %+v", policy, got)
		}
		t.Setenv("JM_NET_NAS_LAN_ADDR", "198.51.100.10")
		if got := linkStatusFor(t); !got.RouteReady {
			t.Fatalf("policy %q, approved route elsewhere: status = %+v", policy, got)
		}
		t.Setenv("JM_NET_NAS_LAN_ADDR", "")
		routes("")
		// Another node with tag:jm-nas, online or a stale record, could
		// receive a Mac's traffic: not ready, and the node is named.
		for _, online := range []string{"true", "false"} {
			nodes(`[{"id": 1, "node_key": "` + nodeKeyMac + `", "name": "juicemount-nas", "tags": ["tag:jm-mac", "tag:jm-nas"], "online": ` + online + `}, ` + nas + `]`)
			if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, `node 1 ("juicemount-nas") carries tag:jm-nas`) {
				t.Fatalf("policy %q, second tag:jm-nas node (online %s): status = %+v", policy, online, got)
			}
		}
	}
}

// TestLinkStatusUpgradeChecksTheSubnetRoute: an upgraded install reports
// subnet exposure and its readiness is the default route's.
func TestLinkStatusUpgradeChecksTheSubnetRoute(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	oldAddr := embeddedHeadscaleHealthAddr
	embeddedHeadscaleHealthAddr = ln.Addr().String()
	t.Cleanup(func() { embeddedHeadscaleHealthAddr = oldAddr })
	t.Cleanup(func() { SetLinkRuntimeStatus(nil) })
	SetLinkRuntimeStatus(func() LinkRuntimeStatus {
		return LinkRuntimeStatus{Headscale: &LinkComponentStatus{State: "running"}, NASNode: &LinkComponentStatus{State: "running", NodeKey: nodeKeyNAS}}
	})
	dir := t.TempDir()
	setLinkEnv(t, fakeHeadscale(t, dir, "unused"), filepath.Join(dir, "config.yaml"))
	setExposureEnv(t)
	if err := os.MkdirAll(filepath.Join(dir, "nas-node"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(`[{"id": 1, "node_key": "`+nodeKeyNAS+`", "name": "juicemount-nas", "given_name": "juicemount-nas"}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	routes := "ID | Hostname | Approved | Available | Serving (Primary)\n1 | juicemount-nas | " + NasLanSubnet + " | " + NasLanSubnet + " | " + NasLanSubnet + "\n"
	if err := os.WriteFile(filepath.Join(dir, "routes.txt"), []byte(routes), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := linkStatusFor(t); got.Exposure != LinkExposureSubnet || !got.RouteReady {
		t.Fatalf("upgrade status = %+v", got)
	}
}

// TestLinkStatusWithoutEmbeddedNASNode: with an operator's own subnet router
// the Manager applies no policy (LinkPolicyManaged), so with no route named
// the route is not checked, as before Link exposure; a named route is ready
// when some node serves it approved. A misconfigured exposure is reported.
func TestLinkStatusWithoutEmbeddedNASNode(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	oldAddr := embeddedHeadscaleHealthAddr
	embeddedHeadscaleHealthAddr = ln.Addr().String()
	t.Cleanup(func() { embeddedHeadscaleHealthAddr = oldAddr })
	t.Cleanup(func() { SetLinkRuntimeStatus(nil) })
	SetLinkRuntimeStatus(func() LinkRuntimeStatus {
		return LinkRuntimeStatus{Headscale: &LinkComponentStatus{State: "running"}}
	})
	dir := t.TempDir()
	setLinkEnv(t, fakeHeadscale(t, dir, "unused"), filepath.Join(dir, "config.yaml"))
	setExposureEnv(t)
	t.Setenv("JM_NET_NAS_NODE", "off")
	t.Setenv("JM_NET_POLICY", "")
	if err := os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(`[{"id": 5, "node_key": "`+nodeKeyMac+`", "name": "router"}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "routes.txt"), []byte("ID | Hostname | Approved | Available | Serving (Primary)\n5 | router | 192.0.2.0/24 | 192.0.2.0/24 | 192.0.2.0/24\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := linkStatusFor(t); got.PolicyManaged || got.Exposure != "" || !got.RouteReady || !got.ControlReady {
		t.Fatalf("no NAS node, no route: status = %+v", got)
	}
	t.Setenv("JM_NET_NAS_ROUTE", "192.0.2.0/24")
	if got := linkStatusFor(t); got.PolicyManaged || got.Exposure != LinkExposureSubnet || !got.RouteReady {
		t.Fatalf("operator router serving the route: status = %+v", got)
	}
	t.Setenv("JM_NET_NAS_ROUTE", "198.51.100.0/24")
	if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, "not approved and served") {
		t.Fatalf("operator route unserved: status = %+v", got)
	}
	t.Setenv("JM_NET_NAS_ROUTE", "")
	t.Setenv("JM_NET_NAS_EXPOSURE", "services")
	if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, "JM_NET_NAS_NODE=container") {
		t.Fatalf("service exposure without the NAS node: status = %+v", got)
	}
	t.Setenv("JM_NET_NAS_NODE", "on")
	t.Setenv("JM_NET_NAS_ROUTE", "192.0.2.0/24")
	if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, "misconfigured") {
		t.Fatalf("services with a route: status = %+v", got)
	}
}

func remoteFarmEnrollment(t *testing.T) (*httptest.ResponseRecorder, string) {
	t.Helper()
	dir := t.TempDir()
	setLinkEnv(t, fakeHeadscale(t, dir, "hskey-auth-farm"), filepath.Join(dir, "config.yaml"))
	t.Setenv("JM_NET_ALLOW_INSECURE", "0")
	t.Setenv("JM_NET_ALLOW_LAN_ONLY", "0")
	t.Setenv("JM_NET_POLICY", "")
	return runFarmEnrollment(t, &API{farmQ: &fakeFarmQ{}, farmNodeMetaURL: "redis://farm@192.0.2.10:30179/1"},
		`{"name":"render-a","role":"render","remote":true}`), dir
}

// TestRemoteFarmEnrollmentFollowsExposure: a remote farm node runs the system
// tailscale with --accept-routes, and its juicefs mount dials the object
// store at the LAN address stored in the volume, so it reaches the NAS only
// through a subnet route. Service exposure refuses the plan, before minting
// a key, with the remedy; subnet exposure (explicit or the upgrade default)
// plans the route.
func TestRemoteFarmEnrollmentFollowsExposure(t *testing.T) {
	setExposureEnv(t)
	rec, dir := remoteFarmEnrollment(t)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "JM_NET_NAS_EXPOSURE=subnet") {
		t.Fatalf("service exposure: status = %d body = %s", rec.Code, rec.Body.String())
	}
	if argv, _ := os.ReadFile(filepath.Join(dir, "argv.log")); strings.Contains(string(argv), "preauthkeys") {
		t.Fatalf("service exposure minted a key: %q", argv)
	}

	for _, tc := range []struct {
		name, route, wantSubnet string
		upgrade                 bool
	}{
		{name: "explicit route", route: "192.0.2.0/24", wantSubnet: "192.0.2.0/24"},
		{name: "upgrade default", upgrade: true, wantSubnet: NasLanSubnet},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setExposureEnv(t)
			t.Setenv("JM_NET_NAS_ROUTE", tc.route)
			dir := t.TempDir()
			setLinkEnv(t, fakeHeadscale(t, dir, "hskey-auth-farm"), filepath.Join(dir, "config.yaml"))
			if tc.upgrade {
				if err := os.MkdirAll(filepath.Join(dir, "nas-node"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("JM_NET_POLICY", "")
			rec := runFarmEnrollment(t, &API{farmQ: &fakeFarmQ{}, farmNodeMetaURL: "redis://farm@192.0.2.10:30179/1"},
				`{"name":"render-a","role":"render","remote":true}`)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
			}
			var resp struct {
				Plan farmEnrollmentPlan `json:"plan"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(resp.Plan.LinkCommand, "--accept-routes") || !strings.Contains(resp.Plan.LinkCommand, "--login-server https://link.example.com:30193") {
				t.Fatalf("link command = %q", resp.Plan.LinkCommand)
			}
			if !strings.Contains(resp.Plan.DockerCommand, "JM_META=redis://farm@192.0.2.10:30179/1") {
				t.Fatalf("docker command lost the LAN Redis endpoint: %q", resp.Plan.DockerCommand)
			}
			if len(resp.Plan.Checks) == 0 || !strings.Contains(resp.Plan.Checks[0], "NAS route "+tc.wantSubnet) {
				t.Fatalf("first check = %q, want the route %s", resp.Plan.Checks, tc.wantSubnet)
			}
			argv, _ := os.ReadFile(filepath.Join(dir, "argv.log"))
			if !strings.Contains(string(argv), "preauthkeys create --user 1 --tags tag:jm-farm") {
				t.Fatalf("farm key argv = %q", argv)
			}
		})
	}
}

// TestLinkSubnetNASAddr: JM_NET_NAS_LAN_ADDR narrows the policy to the NAS's
// own address; one outside the route is a misconfigured exposure.
func TestLinkSubnetNASAddr(t *testing.T) {
	cfg := setExposureEnv(t)
	t.Setenv("JM_NET_NAS_EXPOSURE", "subnet")
	t.Setenv("JM_NET_NAS_ROUTE", "192.0.2.0/24")
	for raw, want := range map[string]string{"": "", "192.0.2.10": "192.0.2.10/32"} {
		t.Setenv("JM_NET_NAS_LAN_ADDR", raw)
		if got, err := LinkSubnetNASAddr("192.0.2.0/24"); err != nil || got != want {
			t.Fatalf("LinkSubnetNASAddr(%q) = %q, %v; want %q", raw, got, err, want)
		}
		if _, _, err := LinkExposure(cfg); err != nil {
			t.Fatalf("LinkExposure with %q: %v", raw, err)
		}
	}
	t.Setenv("JM_NET_NAS_LAN_ADDR", "192.0.2.10")
	if got, err := LinkSubnetNASAddr("2001:db8::/64"); err == nil {
		t.Fatalf("IPv4 address inside an IPv6 route: %q", got)
	}
	t.Setenv("JM_NET_NAS_LAN_ADDR", "2001:db8::5")
	if got, err := LinkSubnetNASAddr("2001:db8::/64"); err != nil || got != "2001:db8::5/128" {
		t.Fatalf("IPv6 NAS address = %q, %v", got, err)
	}
	for _, bad := range []string{"198.51.100.4", "nas.example.com"} {
		t.Setenv("JM_NET_NAS_LAN_ADDR", bad)
		if _, _, err := LinkExposure(cfg); err == nil || !strings.Contains(err.Error(), "JM_NET_NAS_LAN_ADDR") {
			t.Fatalf("LinkExposure with JM_NET_NAS_LAN_ADDR=%s: %v", bad, err)
		}
	}
}

// TestRouteApproveTargetsNASNodeByKey: POST /api/net/routes/approve without
// parameters approves on the node holding the NAS node's key, never on a
// node that only took its name, and refuses ?host=.
func TestRouteApproveTargetsNASNodeByKey(t *testing.T) {
	t.Cleanup(func() { SetLinkRuntimeStatus(nil) })
	dir := t.TempDir()
	bin := fakeHeadscale(t, dir, "node updated")
	setLinkEnv(t, bin, filepath.Join(dir, "config.yaml"))
	t.Setenv("JM_NET_NAS_ROUTE", "192.0.2.0/24")
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("nodes.json", `[{"id": 1, "node_key": "`+nodeKeyMac+`", "name": "juicemount-nas"}, {"id": 2, "node_key": "`+nodeKeyNAS+`", "name": "juicemount-nas"}]`)
	const header = "ID | Hostname | Approved | Available | Serving (Primary)\n"
	write("routes.txt", header+"2 | juicemount-nas |  | 192.0.2.0/24 | \n")
	write("routes-approved.txt", header+"2 | juicemount-nas | 192.0.2.0/24 | 192.0.2.0/24 | 192.0.2.0/24\n")
	post := func(query string) (int, map[string]any) {
		t.Helper()
		rec := httptest.NewRecorder()
		(&API{}).handleNetRouteApprove(rec, httptest.NewRequest(http.MethodPost, "/api/net/routes/approve"+query, nil))
		var got map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &got)
		return rec.Code, got
	}
	if code, got := post("?host=juicemount-nas"); code != http.StatusBadRequest || got["ok"] != false {
		t.Fatalf("?host= = %d %v", code, got)
	}
	if _, got := post(""); got["ok"] != false || !strings.Contains(got["error"].(string), "node key") {
		t.Fatalf("no NAS node runtime: %v", got)
	}
	SetLinkRuntimeStatus(func() LinkRuntimeStatus {
		return LinkRuntimeStatus{NASNode: &LinkComponentStatus{State: "running", NodeKey: nodeKeyNAS}}
	})
	if _, got := post(""); got["ok"] != true {
		t.Fatalf("approve on the NAS node: %v", got)
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv.log"))
	if !strings.Contains(string(argv), "approve-routes --identifier 2 --routes 192.0.2.0/24") || strings.Contains(string(argv), "--identifier 1") {
		t.Fatalf("approve argv = %q, want node 2 only", argv)
	}
	write("routes.txt", header+"2 | juicemount-nas |  | 192.0.2.0/24 | \n")
	if _, got := post("?id=1"); got["ok"] != false {
		t.Fatalf("?id=1 (not advertising, no row) approved: %v", got)
	}
}

// The NAS node in its own container belongs to this Manager as much as the
// in-process one: exposure defaults, the policy and status apply to both.
func TestNASNodeEmbeddedPlacements(t *testing.T) {
	for _, tc := range []struct {
		headscale, node string
		want            bool
	}{
		{"on", NASNodeInManager, true},
		{"on", NASNodeInContainer, true},
		{"on", "off", false},
		{"on", "", false},
		{"on", "yes", false},
		{"off", NASNodeInContainer, false},
	} {
		t.Setenv("JM_NET_HEADSCALE", tc.headscale)
		t.Setenv("JM_NET_NAS_NODE", tc.node)
		if got := NASNodeEmbedded(); got != tc.want {
			t.Errorf("JM_NET_HEADSCALE=%s JM_NET_NAS_NODE=%s: NASNodeEmbedded = %v, want %v", tc.headscale, tc.node, got, tc.want)
		}
	}
}
