package main

import (
	"bytes"
	"context"
	"errors"
	"log"
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
	t.Setenv("JM_NET_DERP_MAP_ALLOW_HTTP", "1") // httptest serves plain http
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
	m, _, err := readDERPMapFile(path)
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
	changed, err := writeDERPMapFile(path, want, defaultDERPMapURL)
	if err != nil || !changed {
		t.Fatalf("write: changed=%v err=%v", changed, err)
	}
	got, source, err := readDERPMapFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("DERP map changed through the file")
	}
	if source != defaultDERPMapURL {
		t.Fatalf("source = %q", source)
	}
	if changed, err := writeDERPMapFile(path, want, defaultDERPMapURL); err != nil || changed {
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
		m, source, err := readDERPMapFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if source != derpMapSourceBuiltin {
			t.Fatalf("source = %q", source)
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
		if _, err := writeDERPMapFile(path, lab, defaultDERPMapURL); err != nil {
			t.Fatal(err)
		}
		if _, err := prepareDERPMap(context.Background(), path); err != nil {
			t.Fatal(err)
		}
		if ids := regionIDs(t, path); !reflect.DeepEqual(ids, []int{900}) {
			t.Fatalf("regions = %v", ids)
		}
	})
	t.Run("public copy, then a self-hosted URL offline: no public relays", func(t *testing.T) {
		t.Setenv("JM_NET_DERP_MAP_URL", "https://relay.example/derpmap.json")
		offlineClient(t)
		path := filepath.Join(t.TempDir(), "derpmap.yaml")
		snap, _ := parseDERPMapJSON(publicDERPMapSnapshot)
		if _, err := writeDERPMapFile(path, snap, defaultDERPMapURL); err != nil {
			t.Fatal(err)
		}
		if _, err := prepareDERPMap(context.Background(), path); err != nil {
			t.Fatal(err)
		}
		if ids := regionIDs(t, path); len(ids) != 0 {
			t.Fatalf("regions = %v, want none: the public copy was reused for a self-hosted URL", ids)
		}
	})
	t.Run("self-hosted copy, then the default URL offline: built-in public map", func(t *testing.T) {
		t.Setenv("JM_NET_DERP_MAP_URL", "")
		offlineClient(t)
		path := filepath.Join(t.TempDir(), "derpmap.yaml")
		lab, _ := parseDERPMapJSON([]byte(labMapJSON))
		if _, err := writeDERPMapFile(path, lab, "https://relay.example/derpmap.json"); err != nil {
			t.Fatal(err)
		}
		if _, err := prepareDERPMap(context.Background(), path); err != nil {
			t.Fatal(err)
		}
		snap, _ := parseDERPMapJSON(publicDERPMapSnapshot)
		if ids := regionIDs(t, path); len(ids) != len(snap.Regions) {
			t.Fatalf("regions = %v, want the built-in public map", ids)
		}
	})
	t.Run("leftover temp files are removed", func(t *testing.T) {
		t.Setenv("JM_NET_DERP_MAP_URL", "")
		offlineClient(t)
		dir := t.TempDir()
		left := filepath.Join(dir, ".derpmap-123.yaml")
		if err := os.WriteFile(left, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := prepareDERPMap(context.Background(), filepath.Join(dir, "derpmap.yaml")); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(left); !os.IsNotExist(err) {
			t.Fatalf("leftover temp file still there: %v", err)
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
	for _, tc := range []struct {
		in, allowHTTP, want string
		wantErr             bool
	}{
		{"", "", defaultDERPMapURL, false},
		{"off", "", "", false},
		{"OFF", "", "", false},
		{"https://r.example/m.json", "", "https://r.example/m.json", false},
		{"http://r.example/m.json", "", "", true},
		{"http://r.example/m.json", "1", "http://r.example/m.json", false},
		{"ftp://r.example/m.json", "1", "", true},
		{"r.example/m.json", "", "", true},
		{"https://", "", "", true},
		{"::not a url", "", "", true},
	} {
		t.Setenv("JM_NET_DERP_MAP_URL", tc.in)
		t.Setenv("JM_NET_DERP_MAP_ALLOW_HTTP", tc.allowHTTP)
		got, err := derpMapSource()
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("JM_NET_DERP_MAP_URL=%q allow_http=%q: (%q, %v), want (%q, err=%v)", tc.in, tc.allowHTTP, got, err, tc.want, tc.wantErr)
		}
	}
}

// An invalid URL is a start error (shown in the Link status), not a
// silent fallback.
func TestPrepareDERPMapRejectsInvalidURL(t *testing.T) {
	t.Setenv("JM_NET_DERP_MAP_URL", "http://relay.example/derpmap.json")
	t.Setenv("JM_NET_DERP_MAP_ALLOW_HTTP", "")
	if _, err := prepareDERPMap(context.Background(), filepath.Join(t.TempDir(), "derpmap.yaml")); err == nil ||
		!strings.Contains(err.Error(), "https") {
		t.Fatalf("err = %v, want an https error", err)
	}
}

func TestCheckDERPOff(t *testing.T) {
	const mapPath = "/data/headscale/derpmap.yaml"
	own := "derp:\n  server:\n    enabled: false\n  urls: []\n  paths:\n    - " + mapPath + "\n"
	for _, k := range []string{"HEADSCALE_DERP_PATHS", "HEADSCALE_DERP_URLS", "HEADSCALE_DERP_SERVER_ENABLED"} {
		t.Setenv(k, "")
	}
	t.Setenv("JM_NET_DERP_MAP_URL", "")
	if err := checkDERPOff(own, mapPath); err != nil {
		t.Fatalf("default source: %v", err)
	}
	t.Setenv("JM_NET_DERP_MAP_URL", "off")
	if err := checkDERPOff(own, mapPath); err == nil || !strings.Contains(err.Error(), "no other DERP map") {
		t.Fatalf("off with only the Manager's file: err = %v", err)
	}
	withOwn := strings.Replace(own, "    - "+mapPath+"\n", "    - "+mapPath+"\n    - /data/relay.yaml\n", 1)
	if err := checkDERPOff(withOwn, mapPath); err != nil {
		t.Fatalf("off with another derp.paths entry: %v", err)
	}
	embedded := strings.Replace(own, "enabled: false", "enabled: true", 1)
	if err := checkDERPOff(embedded, mapPath); err != nil {
		t.Fatalf("off with the embedded DERP server: %v", err)
	}
	t.Setenv("HEADSCALE_DERP_PATHS", "/data/relay.yaml")
	if err := checkDERPOff(own, mapPath); err != nil {
		t.Fatalf("off with HEADSCALE_DERP_PATHS: %v", err)
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

// A derp stanza the operator edited is not migrated; if it still lists a
// URL, the Manager says why Headscale may not start offline and how to fix it.
func TestMigrateLogsEditedDERPURLs(t *testing.T) {
	oldDir := hsDataDir
	t.Cleanup(func() { hsDataDir = oldDir })
	hsDataDir = t.TempDir()
	cfgPath := filepath.Join(hsDataDir, "config.yaml")
	edited := "server_url: https://nas.example:30193\nderp:\n  server:\n    enabled: false\n  urls:\n    - https://relay.example/map.json\n  auto_update_enabled: true\n"
	if err := os.WriteFile(cfgPath, []byte(edited), 0o600); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	if err := migratePersistedDERPConfig(cfgPath); err != nil {
		t.Fatal(err)
	}
	if after, _ := os.ReadFile(cfgPath); string(after) != edited {
		t.Fatal("edited config changed")
	}
	out := buf.String()
	if !strings.Contains(out, "will not start without internet") || !strings.Contains(out, "- "+derpMapPath()) {
		t.Fatalf("log = %q", out)
	}
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("want one log line, got %q", out)
	}
}
