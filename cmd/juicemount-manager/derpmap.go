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
//  2. the copy kept from the last successful fetch;
//  3. for the default URL only, the copy built into the Manager
//     (derpmap_public_snapshot.json).
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
//	                                from derp.paths in config.yaml or
//	                                HEADSCALE_DERP_PATHS

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

// derpMapSource returns the URL the Manager fetches the DERP map from, or ""
// when JM_NET_DERP_MAP_URL=off.
func derpMapSource() string {
	v := strings.TrimSpace(os.Getenv("JM_NET_DERP_MAP_URL"))
	switch strings.ToLower(v) {
	case "":
		return defaultDERPMapURL
	case "off", "none":
		return ""
	}
	return v
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

// marshalDERPMapYAML renders m in the form Headscale's derp.paths loader
// reads: yaml.v3 with its default (lower-case field name) keys.
func marshalDERPMapYAML(m *tailcfg.DERPMap) ([]byte, error) {
	var buf bytes.Buffer
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

// readDERPMapFile loads a map written by writeDERPMapFile.
func readDERPMapFile(path string) (*tailcfg.DERPMap, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m tailcfg.DERPMap
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &m, nil
}

// writeDERPMapFile writes m atomically. It reports whether the file changed.
func writeDERPMapFile(path string, m *tailcfg.DERPMap) (bool, error) {
	b, err := marshalDERPMapYAML(m)
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
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	return true, os.Rename(tmp.Name(), path)
}

// prepareDERPMap makes sure derpmap.yaml exists before Headscale starts. It
// never fails for lack of internet. It reports whether the file holds a
// freshly fetched map, so the caller knows how soon to fetch again.
func prepareDERPMap(ctx context.Context, path string) (fresh bool, err error) {
	src := derpMapSource()
	if src == "" {
		_, err := writeDERPMapFile(path, &tailcfg.DERPMap{Regions: map[int]*tailcfg.DERPRegion{}})
		return false, err
	}
	fctx, cancel := context.WithTimeout(ctx, derpMapFetchTimeout)
	m, fetchErr := fetchDERPMap(fctx, src)
	cancel()
	if fetchErr == nil {
		_, err := writeDERPMapFile(path, m)
		return err == nil, err
	}
	if cached, err := readDERPMapFile(path); err == nil && len(cached.Regions) > 0 {
		log.Printf("JuiceMount Link: DERP map fetch failed (%v); using the copy from the last fetch", fetchErr)
		return false, nil
	}
	if src == defaultDERPMapURL {
		snap, err := parseDERPMapJSON(publicDERPMapSnapshot)
		if err != nil {
			return false, fmt.Errorf("built-in DERP map: %w", err)
		}
		log.Printf("JuiceMount Link: DERP map fetch failed (%v); using the built-in copy of the public map", fetchErr)
		_, err = writeDERPMapFile(path, snap)
		return false, err
	}
	// A custom map with no copy yet: Headscale still starts if config.yaml
	// lists other relays; otherwise it reports an empty DERP map and the
	// supervisor retries, with this fetch, until the URL answers.
	log.Printf("JuiceMount Link: DERP map fetch from %s failed (%v) and no copy exists yet", src, fetchErr)
	_, err = writeDERPMapFile(path, &tailcfg.DERPMap{Regions: map[int]*tailcfg.DERPRegion{}})
	return false, err
}

// refreshDERPMap fetches the map again until stop closes: every
// derpMapRetryInterval while the last fetch failed, every
// derpMapRefreshEvery after one worked.
func refreshDERPMap(stop <-chan struct{}, path string, fresh bool, retry, every time.Duration) {
	src := derpMapSource()
	if src == "" {
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
		changed, err := writeDERPMapFile(path, m)
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
