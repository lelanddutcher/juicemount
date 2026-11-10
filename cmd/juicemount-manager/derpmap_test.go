package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// labMap is a one-region map, as a self-hosted relay would publish.
const labMapJSON = `{"Regions":{"900":{"RegionID":900,"RegionCode":"lab","RegionName":"Lab relay",
 "Nodes":[{"Name":"900a","RegionID":900,"HostName":"derp.lab.example","IPv4":"10.232.3.10","STUNPort":3478,"DERPPort":443}]}}}`

// offlineClient fails every request, as a NAS without internet does.
func offlineClient(t *testing.T) {
	t.Helper()
	old := derpMapFetchClient
	t.Cleanup(func() { derpMapFetchClient = old })
	derpMapFetchClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: connect: network is unreachable")
	})}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// mapServer serves labMapJSON while up is true.
func mapServer(t *testing.T, up *atomic.Bool) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(labMapJSON))
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/derpmap.json"
}

func regionIDs(t *testing.T, path string) []int {
	t.Helper()
	m, err := readDERPMapFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int
	for id := range m.Regions {
		ids = append(ids, id)
	}
	return ids
}

// The generated config must name no URL Headscale fetches at start, and must
// list the Manager's map file.
func TestEnsureConfigDERPIsLocalFile(t *testing.T) {
	oldDir := hsDataDir
	t.Cleanup(func() { hsDataDir = oldDir })
	hsDataDir = t.TempDir()
	t.Setenv("JM_NET_SERVER_URL", "https://nas.example:30193")
	t.Setenv("JM_NET_TLS_CERT", "")
	t.Setenv("JM_NET_TLS_KEY", "")
	cfgPath, err := ensureConfig()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := string(raw)
	want := "derp:\n  server:\n    enabled: false\n  urls: []\n  paths:\n    - " +
		filepath.Join(hsDataDir, "derpmap.yaml") + "\n  auto_update_enabled: true\n  update_frequency: 5m\n"
	if !strings.Contains(cfg, want) {
		t.Fatalf("config.yaml derp stanza:\n%s\nwant:\n%s", cfg, want)
	}
	var parsed struct {
		DERP struct {
			URLs            []string `yaml:"urls"`
			Paths           []string `yaml:"paths"`
			UpdateFrequency string   `yaml:"update_frequency"`
		} `yaml:"derp"`
	}
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("config.yaml is not valid YAML: %v", err)
	}
	if len(parsed.DERP.URLs) != 0 || len(parsed.DERP.Paths) != 1 || parsed.DERP.UpdateFrequency != "5m" {
		t.Fatalf("parsed derp = %+v", parsed.DERP)
	}
	if !derpPathsConfigured(cfg, derpMapPath()) {
		t.Fatal("derpPathsConfigured = false for the generated config")
	}
}

// The file must read back, through the same yaml.v3 decoding into
// tailcfg.DERPMap that Headscale's derp.paths loader uses, as the same map.
func TestDERPMapFileRoundTrip(t *testing.T) {
	want, err := parseDERPMapJSON(publicDERPMapSnapshot)
	if err != nil {
		t.Fatalf("built-in snapshot: %v", err)
	}
	if len(want.Regions) < 10 {
		t.Fatalf("built-in snapshot has %d regions", len(want.Regions))
	}
	path := filepath.Join(t.TempDir(), "derpmap.yaml")
	changed, err := writeDERPMapFile(path, want)
	if err != nil || !changed {
		t.Fatalf("write: changed=%v err=%v", changed, err)
	}
	got, err := readDERPMapFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("DERP map changed through the file")
	}
	if changed, err := writeDERPMapFile(path, want); err != nil || changed {
		t.Fatalf("rewrite of the same map: changed=%v err=%v", changed, err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o644 {
		t.Fatalf("mode = %v, %v", fi.Mode(), err)
	}
}

func TestPrepareDERPMap(t *testing.T) {
	t.Run("online: fetched map", func(t *testing.T) {
		var up atomic.Bool
		up.Store(true)
		t.Setenv("JM_NET_DERP_MAP_URL", mapServer(t, &up))
		path := filepath.Join(t.TempDir(), "derpmap.yaml")
		fresh, err := prepareDERPMap(context.Background(), path)
		if err != nil || !fresh {
			t.Fatalf("fresh=%v err=%v", fresh, err)
		}
		if ids := regionIDs(t, path); !reflect.DeepEqual(ids, []int{900}) {
			t.Fatalf("regions = %v", ids)
		}
	})
	t.Run("offline first boot, default URL: built-in public map", func(t *testing.T) {
		t.Setenv("JM_NET_DERP_MAP_URL", "")
		offlineClient(t)
		path := filepath.Join(t.TempDir(), "derpmap.yaml")
		fresh, err := prepareDERPMap(context.Background(), path)
		if err != nil || fresh {
			t.Fatalf("fresh=%v err=%v", fresh, err)
		}
		m, err := readDERPMapFile(path)
		if err != nil {
			t.Fatal(err)
		}
		snap, _ := parseDERPMapJSON(publicDERPMapSnapshot)
		if !reflect.DeepEqual(m, snap) {
			t.Fatal("offline first boot did not write the built-in public map")
		}
	})
	t.Run("offline with a copy: keeps the copy", func(t *testing.T) {
		var up atomic.Bool
		up.Store(true)
		t.Setenv("JM_NET_DERP_MAP_URL", mapServer(t, &up))
		path := filepath.Join(t.TempDir(), "derpmap.yaml")
		if _, err := prepareDERPMap(context.Background(), path); err != nil {
			t.Fatal(err)
		}
		up.Store(false)
		fresh, err := prepareDERPMap(context.Background(), path)
		if err != nil || fresh {
			t.Fatalf("fresh=%v err=%v", fresh, err)
		}
		if ids := regionIDs(t, path); !reflect.DeepEqual(ids, []int{900}) {
			t.Fatalf("regions = %v", ids)
		}
	})
	t.Run("default URL offline with a copy: keeps the copy, not the snapshot", func(t *testing.T) {
		t.Setenv("JM_NET_DERP_MAP_URL", "")
		offlineClient(t)
		path := filepath.Join(t.TempDir(), "derpmap.yaml")
		lab, _ := parseDERPMapJSON([]byte(labMapJSON))
		if _, err := writeDERPMapFile(path, lab); err != nil {
			t.Fatal(err)
		}
		if _, err := prepareDERPMap(context.Background(), path); err != nil {
			t.Fatal(err)
		}
		if ids := regionIDs(t, path); !reflect.DeepEqual(ids, []int{900}) {
			t.Fatalf("regions = %v", ids)
		}
	})
	t.Run("self-hosted URL offline, no copy: empty map, no public relays", func(t *testing.T) {
		t.Setenv("JM_NET_DERP_MAP_URL", "https://relay.example/derpmap.json")
		offlineClient(t)
		path := filepath.Join(t.TempDir(), "derpmap.yaml")
		if _, err := prepareDERPMap(context.Background(), path); err != nil {
			t.Fatal(err)
		}
		if ids := regionIDs(t, path); len(ids) != 0 {
			t.Fatalf("regions = %v, want none", ids)
		}
	})
	t.Run("off: empty map, no fetch", func(t *testing.T) {
		t.Setenv("JM_NET_DERP_MAP_URL", "off")
		old := derpMapFetchClient
		t.Cleanup(func() { derpMapFetchClient = old })
		derpMapFetchClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Error("fetched a DERP map with JM_NET_DERP_MAP_URL=off")
			return nil, errors.New("no")
		})}
		path := filepath.Join(t.TempDir(), "derpmap.yaml")
		if _, err := prepareDERPMap(context.Background(), path); err != nil {
			t.Fatal(err)
		}
		if ids := regionIDs(t, path); len(ids) != 0 {
			t.Fatalf("regions = %v, want none", ids)
		}
	})
}

func TestDERPMapSource(t *testing.T) {
	for in, want := range map[string]string{
		"":                       defaultDERPMapURL,
		"off":                    "",
		"OFF":                    "",
		"https://r.example/m.js": "https://r.example/m.js",
	} {
		t.Setenv("JM_NET_DERP_MAP_URL", in)
		if got := derpMapSource(); got != want {
			t.Errorf("JM_NET_DERP_MAP_URL=%q: source %q, want %q", in, got, want)
		}
	}
}

// After an offline boot the refresher writes the fetched map once the
// internet is back.
func TestRefreshDERPMapAfterWANReturns(t *testing.T) {
	var up atomic.Bool
	t.Setenv("JM_NET_DERP_MAP_URL", mapServer(t, &up))
	path := filepath.Join(t.TempDir(), "derpmap.yaml")
	if fresh, err := prepareDERPMap(context.Background(), path); err != nil || fresh {
		t.Fatalf("offline prepare: fresh=%v err=%v", fresh, err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { refreshDERPMap(stop, path, false, 20*time.Millisecond, time.Hour); close(done) }()
	defer func() { close(stop); <-done }()
	time.Sleep(60 * time.Millisecond)
	if ids := regionIDs(t, path); len(ids) != 0 {
		t.Fatalf("regions while offline = %v", ids)
	}
	up.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ids := regionIDs(t, path); reflect.DeepEqual(ids, []int{900}) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("refresher did not write the map after the WAN returned")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// legacyGeneratedDERP is the derp stanza Managers up to 0.5.1 wrote.
const legacyGeneratedDERP = `derp:
  server:
    enabled: false
  urls:
    - https://controlplane.tailscale.com/derpmap/default
  auto_update_enabled: true
`

func TestMigrateDERPConfig(t *testing.T) {
	if legacyDERPStanza != legacyGeneratedDERP {
		t.Fatalf("legacyDERPStanza drifted from what 0.5.x generated:\n%s", legacyDERPStanza)
	}
	const mapPath = "/data/headscale/derpmap.yaml"
	legacy := "server_url: https://nas.example:30193\n" + legacyGeneratedDERP + "database:\n  type: sqlite\n"
	got, changed := migrateDERPConfig(legacy, mapPath)
	if !changed {
		t.Fatal("legacy stanza not migrated")
	}
	want := "server_url: https://nas.example:30193\n" + managedDERPStanza(mapPath) + "database:\n  type: sqlite\n"
	if got != want {
		t.Fatalf("migrated:\n%s\nwant:\n%s", got, want)
	}
	if _, again := migrateDERPConfig(got, mapPath); again {
		t.Fatal("migration is not idempotent")
	}
	edited := strings.Replace(legacy, "  auto_update_enabled: true\n", "    - https://relay.example/map.json\n  auto_update_enabled: true\n", 1)
	if out, changed := migrateDERPConfig(edited, mapPath); changed || out != edited {
		t.Fatal("migration changed an operator-edited derp stanza")
	}
}

// An install whose config.yaml an older Manager generated is migrated in
// place on start; the rest of the file is untouched.
func TestEnsureConfigMigratesLegacyDERP(t *testing.T) {
	oldDir := hsDataDir
	t.Cleanup(func() { hsDataDir = oldDir })
	hsDataDir = t.TempDir()
	t.Setenv("JM_NET_SERVER_URL", "https://nas.example:30193")
	t.Setenv("JM_NET_TLS_CERT", "")
	t.Setenv("JM_NET_TLS_KEY", "")
	cfgPath, err := ensureConfig()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(cfgPath)
	legacy := strings.Replace(string(raw), managedDERPStanza(derpMapPath()), legacyGeneratedDERP, 1) + "# operator note\n"
	if !strings.Contains(legacy, "derpmap/default") {
		t.Fatal("test setup: legacy stanza not in place")
	}
	if err := os.WriteFile(cfgPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureConfig(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(cfgPath)
	if strings.Contains(string(after), "derpmap/default") || !strings.Contains(string(after), "# operator note\n") {
		t.Fatalf("after migration:\n%s", after)
	}
	if string(after) != string(raw)+"# operator note\n" {
		t.Fatalf("migration changed more than the derp stanza:\n%s", after)
	}
	if fi, err := os.Stat(cfgPath); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %v, %v", fi.Mode(), err)
	}
}

// TestLinkIntegrationHeadscaleStartsOffline runs the pinned Headscale with
// the config the Manager generates and no internet (every DERP map fetch
// fails, no copy on disk). Before the fix Headscale exited fetching the
// public map URL; now it serves with the built-in copy of that map.
func TestLinkIntegrationHeadscaleStartsOffline(t *testing.T) {
	bin := os.Getenv("JM_TEST_HEADSCALE_BIN")
	if bin == "" {
		t.Skip("JM_TEST_HEADSCALE_BIN is not set; see link_integration_test.go")
	}
	dir := t.TempDir()
	oldBin, oldDir := hsBinary, hsDataDir
	t.Cleanup(func() { hsBinary, hsDataDir = oldBin, oldDir })
	hsBinary, hsDataDir = bin, dir
	for _, k := range []string{"HEADSCALE_DERP_URLS", "HEADSCALE_DERP_PATHS", "JM_NET_DERP_MAP_URL",
		"JM_NET_POLICY", "JM_NET_NAS_EXPOSURE", "JM_NET_NAS_ROUTE", "JM_NET_NAS_LAN_ADDR", "JM_HEADSCALE_USER"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
	t.Setenv("JM_NET_SERVER_URL", "https://link.example.com:30193")
	t.Setenv("JM_NET_TLS_CERT", "")
	t.Setenv("JM_NET_TLS_KEY", "")
	offlineClient(t)
	cfgPath := filepath.Join(dir, "config.yaml")
	hs, _ := startLoopbackHeadscale(t, func(listen string) {
		cfg := renderHeadscaleConfig(headscaleConfigParams{
			ServerURL:     "https://link.example.com:30193",
			Listen:        listen,
			MetricsListen: loopbackAnyPort,
			GRPCListen:    loopbackAnyPort,
			DataDir:       dir,
			DERPPaths:     []string{derpMapPath()},
		})
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
			t.Fatal(err)
		}
	})
	defer hs.Stop()
	snap, _ := parseDERPMapJSON(publicDERPMapSnapshot)
	if ids := regionIDs(t, derpMapPath()); len(ids) != len(snap.Regions) {
		t.Fatalf("derpmap.yaml has %d regions, want the built-in %d", len(ids), len(snap.Regions))
	}
}
