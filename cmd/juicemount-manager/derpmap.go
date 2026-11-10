package main

// The DERP map Headscale serves to Link nodes, kept by the Manager as a
// local file.
//
// A DERP map lists the relays (DERP servers) and STUN servers that Link
// nodes use when no direct path forms. Headscale v0.29 builds its map at
// start from derp.urls (fetched over HTTP) and derp.paths (local files). A
// URL that cannot be fetched is fatal: `headscale serve` exits. The config
// the Manager generated before listed Tailscale's public map as a URL, so a
// NAS that booted before its router had internet (after a power cut) had no
// Link, and no LAN access through Link, until the internet came back.
//
// Now the Manager fetches the map itself and writes it to
// /data/headscale/derpmap.yaml, which the generated config lists under
// derp.paths. Headscale reads only that file, so it starts without
// internet. The file holds, in order of preference:
//
//  1. the map just fetched from JM_NET_DERP_MAP_URL (default: Tailscale's
//     public map);
//  2. the copy kept from the last successful fetch from that same URL (the
//     file's "# source:" line names the URL it came from);
//  3. for the default URL only, the copy built into the Manager
//     (derpmap_public_snapshot.json); for any other URL, an empty map.
//
// While Headscale runs, the Manager fetches the map again (every minute
// while the fetch fails, every hour once it works) and rewrites the file
// when it changes. Headscale reads the file again every
// derp.update_frequency.
//
// JM_NET_DERP_MAP_URL selects the map:
//
//	(unset)                         Tailscale's public relays (default)
//	https://relay.example/map.json  only the map at that URL, such as a
//	                                self-hosted relay's; no public relays
//	off                             no fetched map: the relays come only
//	                                from derp.paths in config.yaml,
//	                                HEADSCALE_DERP_PATHS, or Headscale's
//	                                embedded DERP server
//
// The URL must be https. Anyone on the path of a plain-http fetch could
// list their own relays, so http needs JM_NET_DERP_MAP_ALLOW_HTTP=1 (for a
// test lab) and logs a warning.
//
// Refresh the built-in copy of the public map before a release:
//
//go:generate curl -fsSL -o derpmap_public_snapshot.json https://controlplane.tailscale.com/derpmap/default

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"tailscale.com/tailcfg"
)

const (
	derpMapFileName      = "derpmap.yaml"
	derpMapFetchTimeout  = 10 * time.Second
	derpMapRetryInterval = time.Minute
	derpMapRefreshEvery  = time.Hour
	derpMapMaxBytes      = 4 << 20
	// derpUpdateFrequency is how often Headscale re-reads derpmap.yaml.
	// Reading a local file is cheap; this bounds how long a refreshed map
	// takes to reach the nodes.
	derpUpdateFrequency = "5m"
)

//go:embed derpmap_public_snapshot.json
var publicDERPMapSnapshot []byte

// derpMapFetchClient is a var so tests can point fetches at a test server.
var derpMapFetchClient = &http.Client{Timeout: derpMapFetchTimeout}

// derpMapSourceOff is what the file's source line says when
// JM_NET_DERP_MAP_URL=off.
const derpMapSourceOff = "off"

// derpMapSource returns the URL the Manager fetches the DERP map from, or ""
// when JM_NET_DERP_MAP_URL=off. An unusable value is an error, which the
// Link status shows.
func derpMapSource() (string, error) {
	v := strings.TrimSpace(os.Getenv("JM_NET_DERP_MAP_URL"))
	switch strings.ToLower(v) {
	case "":
		return defaultDERPMapURL, nil
	case "off", "none":
		return "", nil
	}
	u, err := url.Parse(v)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("JM_NET_DERP_MAP_URL %q is not a URL (use an https URL or off)", v)
	}
	switch u.Scheme {
	case "https":
		return v, nil
	case "http":
		if os.Getenv("JM_NET_DERP_MAP_ALLOW_HTTP") != "1" {
			return "", fmt.Errorf("JM_NET_DERP_MAP_URL %q is plain http: anyone on the path could add relays; use https (JM_NET_DERP_MAP_ALLOW_HTTP=1 allows http for a test lab)", v)
		}
		return v, nil
	}
	return "", fmt.Errorf("JM_NET_DERP_MAP_URL %q must be an https URL or off", v)
}

// sourceLabel is what the file's source line records for src.
func sourceLabel(src string) string {
	if src == "" {
		return derpMapSourceOff
	}
	return src
}

func derpMapPath() string { return filepath.Join(hsDataDir, derpMapFileName) }

// fetchDERPMap downloads a DERP map in Tailscale's JSON form, as Headscale's
// derp.urls loader does.
func fetchDERPMap(ctx context.Context, url string) (*tailcfg.DERPMap, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := derpMapFetchClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, derpMapMaxBytes))
	if err != nil {
		return nil, err
	}
	return parseDERPMapJSON(body)
}

func parseDERPMapJSON(b []byte) (*tailcfg.DERPMap, error) {
	var m tailcfg.DERPMap
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse DERP map: %w", err)
	}
	if len(m.Regions) == 0 {
		return nil, errors.New("DERP map has no regions")
	}
	return &m, nil
}

const derpMapSourcePrefix = "# source: "

// marshalDERPMapYAML renders m in the form Headscale's derp.paths loader
// reads: yaml.v3 with its default (lower-case field name) keys. The first
// line records where the map came from (a URL, "off", or
// derpMapSourceBuiltin), so a copy is reused only for the same source.
func marshalDERPMapYAML(m *tailcfg.DERPMap, source string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(derpMapSourcePrefix + source + "\n")
	buf.WriteString("# Written by juicemount-manager; rewritten when the DERP map changes.\n")
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(m); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// derpMapSourceBuiltin marks the built-in copy of the public map. It is
// not a fetched copy, so it is never reused in place of a fetch: each start
// without internet writes it again from the binary.
const derpMapSourceBuiltin = "built-in copy of " + defaultDERPMapURL

// readDERPMapFile loads a map written by writeDERPMapFile and the source
// its first line records ("" when the file has no source line).
func readDERPMapFile(path string) (*tailcfg.DERPMap, string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	var m tailcfg.DERPMap
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, "", fmt.Errorf("parse %s: %w", path, err)
	}
	first, _, _ := strings.Cut(string(b), "\n")
	source, _ := strings.CutPrefix(first, derpMapSourcePrefix)
	if source == first {
		source = ""
	}
	return &m, source, nil
}

// writeDERPMapFile writes m from source atomically. It reports whether the
// file changed.
func writeDERPMapFile(path string, m *tailcfg.DERPMap, source string) (bool, error) {
	b, err := marshalDERPMapYAML(m, source)
	if err != nil {
		return false, err
	}
	if old, err := os.ReadFile(path); err == nil && bytes.Equal(old, b) {
		return false, nil
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".derpmap-*.yaml")
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	return true, os.Rename(tmp.Name(), path)
}

// removeDERPMapTemps removes temporary files a crash left behind.
func removeDERPMapTemps(dir string) {
	left, _ := filepath.Glob(filepath.Join(dir, ".derpmap-*.yaml"))
	for _, f := range left {
		_ = os.Remove(f)
	}
}

// prepareDERPMap makes sure derpmap.yaml exists before Headscale starts. It
// never fails for lack of internet. It reports whether the file holds a
// freshly fetched map, so the caller knows how soon to fetch again.
func prepareDERPMap(ctx context.Context, path string) (fresh bool, err error) {
	removeDERPMapTemps(filepath.Dir(path))
	src, err := derpMapSource()
	if err != nil {
		return false, err
	}
	empty := &tailcfg.DERPMap{Regions: map[int]*tailcfg.DERPRegion{}}
	if src == "" {
		_, err := writeDERPMapFile(path, empty, derpMapSourceOff)
		return false, err
	}
	if strings.HasPrefix(src, "http://") {
		log.Printf("JuiceMount Link: WARNING: the DERP map comes from plain http (%s); anyone on that path can add relays (JM_NET_DERP_MAP_ALLOW_HTTP=1)", src)
	}
	fctx, cancel := context.WithTimeout(ctx, derpMapFetchTimeout)
	m, fetchErr := fetchDERPMap(fctx, src)
	cancel()
	if fetchErr == nil {
		_, err := writeDERPMapFile(path, m, src)
		return err == nil, err
	}
	// A saved copy counts only if it came from this same URL: after a
	// switch between the public map and a self-hosted one, the old copy
	// would list the relays the operator turned away from.
	if cached, from, err := readDERPMapFile(path); err == nil && from == src && len(cached.Regions) > 0 {
		log.Printf("JuiceMount Link: DERP map fetch failed (%v); using the copy from the last fetch", fetchErr)
		return false, nil
	}
	if src == defaultDERPMapURL {
		snap, err := parseDERPMapJSON(publicDERPMapSnapshot)
		if err != nil {
			return false, fmt.Errorf("built-in DERP map: %w", err)
		}
		log.Printf("JuiceMount Link: DERP map fetch failed (%v); using the built-in copy of the public map", fetchErr)
		_, err = writeDERPMapFile(path, snap, derpMapSourceBuiltin)
		return false, err
	}
	// A custom map with no copy from it yet: Headscale still starts if
	// config.yaml lists other relays; otherwise it reports an empty DERP
	// map and the supervisor retries, with this fetch, until the URL answers.
	log.Printf("JuiceMount Link: DERP map fetch from %s failed (%v) and no copy from it exists yet", src, fetchErr)
	_, err = writeDERPMapFile(path, empty, src)
	return false, err
}

// refreshDERPMap fetches the map again until stop closes: every
// derpMapRetryInterval while the last fetch failed, every
// derpMapRefreshEvery after one worked.
func refreshDERPMap(stop <-chan struct{}, path string, fresh bool, retry, every time.Duration) {
	src, err := derpMapSource()
	if err != nil || src == "" {
		return
	}
	for {
		wait := every
		if !fresh {
			wait = retry
		}
		select {
		case <-stop:
			return
		case <-time.After(wait):
		}
		ctx, cancel := context.WithTimeout(context.Background(), derpMapFetchTimeout)
		m, err := fetchDERPMap(ctx, src)
		cancel()
		if err != nil {
			if fresh {
				log.Printf("JuiceMount Link: DERP map refresh failed (%v); keeping the current map", err)
			}
			fresh = false
			continue
		}
		changed, err := writeDERPMapFile(path, m, src)
		if err != nil {
			log.Printf("JuiceMount Link: write DERP map: %v", err)
			continue
		}
		if changed {
			log.Printf("JuiceMount Link: DERP map updated from %s", src)
		}
		fresh = true
	}
}

// Older Managers generated this derp stanza (a URL Headscale must fetch
// before it serves). migrateDERPConfig replaces exactly that stanza with the
// local-file one; a stanza the operator edited is left alone.
const legacyDERPStanza = `derp:
  server:
    enabled: false
  urls:
    - ` + defaultDERPMapURL + `
  auto_update_enabled: true
`

func managedDERPStanza(path string) string {
	return renderDERPStanza(nil, []string{path})
}

func migrateDERPConfig(cfg, mapPath string) (string, bool) {
	if strings.Count(cfg, legacyDERPStanza) != 1 {
		return cfg, false
	}
	return strings.Replace(cfg, legacyDERPStanza, managedDERPStanza(mapPath), 1), true
}

// derpPathsConfigured reports whether config.yaml lists mapPath under
// derp.paths. Without it, the operator chose their own DERP sources.
func derpPathsConfigured(cfg, mapPath string) bool {
	return hasExactConfigLine(cfg, "- "+mapPath)
}

// headscaleDERPConfig is the part of config.yaml that names DERP sources.
type headscaleDERPConfig struct {
	DERP struct {
		Server struct {
			Enabled bool `yaml:"enabled"`
		} `yaml:"server"`
		URLs  []string `yaml:"urls"`
		Paths []string `yaml:"paths"`
	} `yaml:"derp"`
}

func parseHeadscaleDERPConfig(cfg string) (headscaleDERPConfig, error) {
	var c headscaleDERPConfig
	err := yaml.Unmarshal([]byte(cfg), &c)
	return c, err
}

// otherDERPSource reports whether Headscale has a DERP source besides the
// Manager's map file: another derp.paths entry, a derp.urls entry, its
// embedded DERP server, or the same through HEADSCALE_DERP_* variables.
func otherDERPSource(cfg, mapPath string) bool {
	for _, k := range []string{"HEADSCALE_DERP_PATHS", "HEADSCALE_DERP_URLS"} {
		if strings.TrimSpace(os.Getenv(k)) != "" {
			return true
		}
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("HEADSCALE_DERP_SERVER_ENABLED")), "true") {
		return true
	}
	c, err := parseHeadscaleDERPConfig(cfg)
	if err != nil {
		return false
	}
	if c.DERP.Server.Enabled || len(c.DERP.URLs) > 0 {
		return true
	}
	for _, p := range c.DERP.Paths {
		if p != mapPath {
			return true
		}
	}
	return false
}

// checkDERPOff refuses JM_NET_DERP_MAP_URL=off when nothing else gives
// Headscale a relay: Headscale would exit "initial DERPMap is empty" and
// restart forever with no hint why.
func checkDERPOff(cfg, mapPath string) error {
	if src, err := derpMapSource(); err != nil || src != "" {
		return err
	}
	if otherDERPSource(cfg, mapPath) {
		return nil
	}
	return errors.New("JM_NET_DERP_MAP_URL=off but no other DERP map is configured: add your relay's map file under derp.paths in config.yaml (or HEADSCALE_DERP_PATHS), or unset JM_NET_DERP_MAP_URL to use the public relays")
}
