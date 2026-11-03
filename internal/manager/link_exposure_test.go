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
		return LinkRuntimeStatus{Headscale: &LinkComponentStatus{State: "running"}, NASNode: &LinkComponentStatus{State: "running"}}
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
		nodes(`[{"id": 1, "name": "juicemount-nas", "given_name": "juicemount-nas", "online": true}]`)
		if got := linkStatusFor(t); got.Exposure != LinkExposureServices || got.RouteReady || !strings.Contains(got.Error, "not tagged tag:jm-nas") {
			t.Fatalf("policy %q, untagged NAS node: status = %+v", policy, got)
		}
		nodes(`[{"id": 1, "name": "juicemount-nas", "given_name": "juicemount-nas", "tags": ["tag:jm-nas"], "online": false}]`)
		if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, "offline") {
			t.Fatalf("policy %q, offline NAS node: status = %+v", policy, got)
		}
		nodes(`[]`)
		if got := linkStatusFor(t); got.RouteReady || !strings.Contains(got.Error, "not registered") {
			t.Fatalf("policy %q, unregistered NAS node: status = %+v", policy, got)
		}
		nodes(`[{"id": 1, "name": "juicemount-nas", "given_name": "juicemount-nas", "tags": ["tag:jm-nas"], "online": true}]`)
		if got := linkStatusFor(t); !got.RouteReady || !got.ControlReady {
			t.Fatalf("policy %q, tagged online NAS node: status = %+v", policy, got)
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
	dir := t.TempDir()
	setLinkEnv(t, fakeHeadscale(t, dir, "unused"), filepath.Join(dir, "config.yaml"))
	setExposureEnv(t)
	if err := os.MkdirAll(filepath.Join(dir, "nas-node"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(`[{"id": 1, "name": "juicemount-nas", "given_name": "juicemount-nas"}]`), 0o644); err != nil {
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
