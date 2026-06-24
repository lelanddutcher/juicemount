package main

// Contract conformance test (juicemount-contract v1).
//
// The contract is only real if the live control plane serves what the vendored
// fixtures + schemas say it does. This test is the provider's proof: it boots
// the REAL handlers against seeded stores and asserts each response (a)
// validates against contract/spec/schema/<x>.schema.json and (b) structurally
// matches contract/fixtures/<x>.json (modulo runtime-volatile fields). When it
// goes green, that is the signal back to OpenLoupe that the contract converged.
//
// Coverage in this round: the additive endpoints implemented on feat/contract-v1
// — /whoami (gui), /residency (resident/streaming/warming/absent), /lookup. Plus
// schema-validation of EVERY fixture (cheap drift guard). Live coverage of
// /cache-status (JM-3, deferred — still capitalized on the wire), the jm5
// /whoami (cli, separate binary), /residency uploading (needs spool seed), and
// the POST/stateful endpoints (/offline,/spool,/pin,/activity) is intentionally
// out of scope here and tracked in contract/BACKLOG.md.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/cplane"
	"github.com/lelanddutcher/juicemount/metadata"
)

const contractDir = "../contract"

// volatileFields are compared by presence/type, never by value — they are
// host/runtime-specific. Mirrors contract/conformance/README.md "Volatile fields".
var volatileFields = map[string]bool{
	"checked_at": true, "since": true, "since_sec": true,
	"instance_id": true, "current_file": true,
	"control_plane": true, "metadata_db_path": true,
}

type fixtureRow struct {
	File       string `json:"file"`
	Endpoint   string `json:"endpoint"`
	Method     string `json:"method"`
	Schema     string `json:"schema"`
	HTTPStatus int    `json:"http_status"`
}

func loadFixtureIndex(t *testing.T) []fixtureRow {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(contractDir, "fixtures", "index.json"))
	if err != nil {
		t.Fatalf("read fixtures/index.json: %v", err)
	}
	var idx struct {
		Fixtures []fixtureRow `json:"fixtures"`
	}
	if err := json.Unmarshal(b, &idx); err != nil {
		t.Fatalf("parse index.json: %v", err)
	}
	return idx.Fixtures
}

func loadJSONValue(t *testing.T, rel string) any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(contractDir, "fixtures", rel))
	if err != nil {
		t.Fatalf("read fixture %s: %v", rel, err)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("parse fixture %s: %v", rel, err)
	}
	return v
}

// compileSchema compiles a schema file by its canonical $id so internal $ref /
// $defs resolve correctly.
func compileSchema(t *testing.T, schemaFile string) *jsonschema.Schema {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(contractDir, "spec", "schema", schemaFile))
	if err != nil {
		t.Fatalf("read schema %s: %v", schemaFile, err)
	}
	var meta struct {
		ID string `json:"$id"`
	}
	_ = json.Unmarshal(b, &meta)
	id := meta.ID
	if id == "" {
		id = schemaFile
	}
	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft2020
	if err := c.AddResource(id, bytes.NewReader(b)); err != nil {
		t.Fatalf("add schema %s: %v", schemaFile, err)
	}
	sch, err := c.Compile(id)
	if err != nil {
		t.Fatalf("compile schema %s: %v", schemaFile, err)
	}
	return sch
}

// TestContractFixturesValidateAgainstSchemas guards the vendored contract's
// internal consistency: every golden fixture must validate against its schema.
func TestContractFixturesValidateAgainstSchemas(t *testing.T) {
	for _, fx := range loadFixtureIndex(t) {
		if strings.Contains(fx.Schema, "#") {
			continue // fragment-ref schema (offline toggle) — covered elsewhere
		}
		fx := fx
		t.Run(fx.File, func(t *testing.T) {
			sch := compileSchema(t, fx.Schema)
			if err := sch.Validate(loadJSONValue(t, fx.File)); err != nil {
				t.Errorf("fixture %s does not validate against %s:\n%v", fx.File, fx.Schema, err)
			}
		})
	}
}

const (
	mp        = "/Volumes/zpool"
	clip031   = "/Volumes/zpool/Project_Foo/clip_031.mov"
	clip204   = "/Volumes/zpool/Project_Foo/clip_204.mov"
	clip205   = "/Volumes/zpool/Project_Foo/clip_205.mov"
	deleted   = "/Volumes/zpool/Project_Foo/deleted_take.mov"
	notHere   = "/Volumes/zpool/Project_Foo/not_here.mov"
	seedMtime = 1700000000
)

// seedControlPlane wires the cbridge globals to seeded in-memory stores matching
// the contract's fixture seed (contract/fake/README.md §Seed). Returns a restore
// func that nils them so the package's other state is untouched.
func seedControlPlane(t *testing.T) func() {
	t.Helper()
	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	mt := time.Unix(seedMtime, 0)
	// REGRESSION GUARD (path-anchoring bug): the real `entries` mirror is keyed
	// VOLUME-RELATIVE ("Project_Foo/clip.mov"), while the queries + pin store use
	// the full "/Volumes/zpool/..." path. Seed entries relative so the handler is
	// forced to translate the full query path; if it ever stops, exists→false and
	// these cases fail.
	for _, e := range []*metadata.Entry{
		{Path: "Project_Foo/clip_031.mov", Name: "clip_031.mov", ParentPath: "Project_Foo", Size: 880000000, Inode: 1180417, Mtime: mt},
		{Path: "Project_Foo/clip_204.mov", Name: "clip_204.mov", ParentPath: "Project_Foo", Size: 1240000000, Inode: 1180620, Mtime: mt},
		{Path: "Project_Foo/clip_205.mov", Name: "clip_205.mov", ParentPath: "Project_Foo", Size: 1240000000, Inode: 1180621, Mtime: mt},
	} {
		mstore.InsertToCache(e)
	}

	pstore, err := pin.Open(":memory:")
	if err != nil {
		t.Fatalf("pin.Open: %v", err)
	}
	// clip_031: fully resident (status ready, bytes_cached == size).
	_ = pstore.Pin(clip031, 880000000, "/Volumes/zpool/Project_Foo")
	_ = pstore.UpdateStatus(clip031, pin.StatusReady, 880000000, "")
	// clip_205: warming (prefetching, bytes_cached < size).
	_ = pstore.Pin(clip205, 1240000000, "/Volumes/zpool/Project_Foo")
	_ = pstore.UpdateStatus(clip205, pin.StatusPrefetching, 620000000, "")

	guiRoutes := []string{
		"/health", "/metrics", "/pin", "/unpin", "/cache-status", "/offline",
		"/whoami", "/residency", "/lookup", "/reclaim", "/cache-clear",
		"/verify-pins", "/force-eject", "/stop", "/self-test", "/spool",
		"/activity", "/spool-recover", "/mount-now", "/debug/pprof/",
	}

	globalMu.Lock()
	old := struct {
		store                             *metadata.Store
		pin                               *pin.Store
		mount, want, vol, dbp, addr, inst string
		caps                              []string
	}{globalStore, globalPinStore, globalMountPath, globalWantMountPoint, globalVolumeName, globalDBPath, globalMetricsAddr, globalInstanceID, globalCapabilities}
	globalStore = mstore
	globalPinStore = pstore
	globalMountPath = mp
	globalWantMountPoint = mp
	globalVolumeName = "zpool"
	globalDBPath = "/seed/Library/Application Support/JuiceMount/metadata.db"
	globalMetricsAddr = "127.0.0.1:11050"
	globalInstanceID = "TEST-0000-0000-0000-000000000000"
	globalCapabilities = cplane.DeriveCapabilities(guiRoutes)
	globalMu.Unlock()

	return func() {
		globalMu.Lock()
		globalStore = old.store
		globalPinStore = old.pin
		globalMountPath = old.mount
		globalWantMountPoint = old.want
		globalVolumeName = old.vol
		globalDBPath = old.dbp
		globalMetricsAddr = old.addr
		globalInstanceID = old.inst
		globalCapabilities = old.caps
		globalMu.Unlock()
		mstore.Close()
		pstore.Close()
	}
}

// TestContractLiveConformance boots the real handlers against the seed and
// asserts each response validates against its schema AND structurally matches
// the golden fixture (modulo volatile fields).
func TestContractLiveConformance(t *testing.T) {
	restore := seedControlPlane(t)
	defer restore()

	cases := []struct {
		name     string
		endpoint string
		handler  func(w http.ResponseWriter, r *http.Request)
		path     string // ?path= (empty = none)
		fixture  string
		schema   string
	}{
		{"whoami_gui", "/whoami", handleWhoamiHTTP, "", "whoami/gui.json", "whoami.schema.json"},
		{"residency_resident", "/residency", handleResidencyHTTP, clip031, "residency/resident.json", "residency.schema.json"},
		{"residency_streaming", "/residency", handleResidencyHTTP, clip204, "residency/streaming.json", "residency.schema.json"},
		{"residency_warming", "/residency", handleResidencyHTTP, clip205, "residency/warming.json", "residency.schema.json"},
		{"residency_absent", "/residency", handleResidencyHTTP, deleted, "residency/absent.json", "residency.schema.json"},
		{"lookup_file", "/lookup", handleLookupHTTP, clip031, "lookup/file.json", "lookup.schema.json"},
		{"lookup_missing", "/lookup", handleLookupHTTP, notHere, "lookup/missing.json", "lookup.schema.json"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			u := tc.endpoint
			if tc.path != "" {
				u += "?" + url.Values{"path": {tc.path}}.Encode()
			}
			req := httptest.NewRequest("GET", u, nil)
			rr := httptest.NewRecorder()
			tc.handler(rr, req)

			if rr.Code != 200 {
				t.Fatalf("%s: status = %d, body = %s", tc.name, rr.Code, rr.Body.String())
			}

			var got any
			if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
				t.Fatalf("%s: response is not JSON: %v\nbody=%s", tc.name, err, rr.Body.String())
			}
			if err := compileSchema(t, tc.schema).Validate(got); err != nil {
				t.Errorf("%s: response does not validate against %s:\n%v\nbody=%s", tc.name, tc.schema, err, rr.Body.String())
			}
			assertStructEqual(t, tc.name, got, loadJSONValue(t, tc.fixture))
		})
	}
}

// TestCacheStatusSnakeCaseTags is the JM-3 proof: the inner cache-status structs
// (pin.AggregateStats / RootSummary / LiveStats) now marshal with snake_case wire
// keys and no longer leak capitalized Go field names. Deterministic — avoids the
// runtime disk/capacity fields a full /cache-status seed would carry.
func TestCacheStatusSnakeCaseTags(t *testing.T) {
	cases := []struct {
		name        string
		v           any
		wantKeys    []string
		bannedCaped []string
	}{
		{"aggregate",
			pin.AggregateStats{TotalFiles: 142, ReadyFiles: 130, PendingFiles: 12, TotalBytes: 3400000000, CachedBytes: 2800000000},
			[]string{"total_files", "ready_files", "pending_files", "failed_files", "total_bytes", "cached_bytes"},
			[]string{"TotalFiles", "CachedBytes"}},
		{"root",
			pin.RootSummary{Root: "/Volumes/zpool/Project_Foo", TotalFiles: 142, CachedBytes: 2800000000},
			[]string{"root", "total_files", "ready_files", "pending_files", "failed_files", "total_bytes", "cached_bytes"},
			[]string{"Root", "TotalFiles", "CachedBytes"}},
		{"live",
			pin.LiveStats{BytesPrefetched: 1500000000, FilesPrefetched: 30, CurrentFile: "x.mov", Workers: 4},
			[]string{"bytes_prefetched", "files_prefetched", "current_file", "workers"},
			[]string{"BytesPrefetched", "CurrentFile", "Workers"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.v)
			if err != nil {
				t.Fatal(err)
			}
			var m map[string]any
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
			for _, k := range tc.wantKeys {
				if _, ok := m[k]; !ok {
					t.Errorf("%s: missing snake_case key %q in %s", tc.name, k, b)
				}
			}
			for _, k := range tc.bannedCaped {
				if _, ok := m[k]; ok {
					t.Errorf("%s: still emits capitalized key %q in %s", tc.name, k, b)
				}
			}
		})
	}
}

// assertStructEqual compares two decoded JSON objects, ignoring volatileFields
// and comparing the capabilities array as an unordered set.
func assertStructEqual(t *testing.T, name string, got, want any) {
	t.Helper()
	gm, gok := got.(map[string]any)
	wm, wok := want.(map[string]any)
	if !gok || !wok {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: not equal:\n got=%v\nwant=%v", name, got, want)
		}
		return
	}
	// Clone so we don't mutate the caller's maps.
	gm, wm = cloneMap(gm), cloneMap(wm)

	if gc, ok := gm["capabilities"]; ok {
		if !sameStringSet(gc, wm["capabilities"]) {
			t.Errorf("%s: capabilities mismatch:\n got=%v\nwant=%v", name, gc, wm["capabilities"])
		}
		delete(gm, "capabilities")
		delete(wm, "capabilities")
	}
	for k := range volatileFields {
		delete(gm, k)
		delete(wm, k)
	}
	if !reflect.DeepEqual(gm, wm) {
		t.Errorf("%s: structural mismatch:\n got=%#v\nwant=%#v", name, gm, wm)
	}
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func sameStringSet(a, b any) bool {
	as, aok := toStringSlice(a)
	bs, bok := toStringSlice(b)
	if !aok || !bok {
		return false
	}
	sort.Strings(as)
	sort.Strings(bs)
	return reflect.DeepEqual(as, bs)
}

func toStringSlice(v any) ([]string, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		s, ok := e.(string)
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}
