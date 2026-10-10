package main

// Contract conformance test (juicemount-contract v3).
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
// /cache-status beyond the empty-roots response, the jm5
// /whoami (cli, separate binary), /residency uploading (needs spool seed), and
// the POST/stateful endpoints (/offline,/pin,/activity) is intentionally
// out of scope here and tracked in contract/BACKLOG.md.

import (
	"bytes"
	"context"
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
	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

const contractDir = "../contract"

// volatileFields are compared by presence/type, never by value — they are
// host/runtime-specific. Mirrors contract/conformance/README.md "Volatile fields".
var volatileFields = map[string]bool{
	"checked_at": true, "since": true, "since_sec": true,
	"instance_id": true, "device_digest": true, "current_file": true,
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
	filePart, fragment, hasFragment := strings.Cut(schemaFile, "#")
	b, err := os.ReadFile(filepath.Join(contractDir, "spec", "schema", filePart))
	if err != nil {
		t.Fatalf("read schema %s: %v", schemaFile, err)
	}
	var meta struct {
		ID string `json:"$id"`
	}
	_ = json.Unmarshal(b, &meta)
	id := meta.ID
	if id == "" {
		id = filePart
	}
	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft2020
	if err := c.AddResource(id, bytes.NewReader(b)); err != nil {
		t.Fatalf("add schema %s: %v", schemaFile, err)
	}
	ref := id
	if hasFragment {
		ref += "#" + fragment
	}
	sch, err := c.Compile(ref)
	if err != nil {
		t.Fatalf("compile schema %s: %v", schemaFile, err)
	}
	return sch
}

// TestContractFixturesValidateAgainstSchemas guards the vendored contract's
// internal consistency: every golden fixture must validate against its schema.
func TestContractFixturesValidateAgainstSchemas(t *testing.T) {
	for _, fx := range loadFixtureIndex(t) {
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

// Seed a raw durable Ready status. Callers that report local availability must
// separately install a validator and admit the exact test generation.
func seedStatusReady(t *testing.T, store *pin.Store, path string) {
	t.Helper()
	e, ok := store.Get(path)
	if !ok {
		t.Fatal("missing seeded pin")
	}
	claimed, ok, err := store.ClaimPrefetch(*e)
	if err != nil || !ok {
		t.Fatalf("claim seeded pin: claimed=%v err=%v", ok, err)
	}
	if changed, err := store.CompleteReady(claimed, []byte("status-only-test-witness")); err != nil || !changed {
		t.Fatalf("complete seeded status: changed=%v err=%v", changed, err)
	}
}

// seedControlPlane wires the cbridge globals to seeded in-memory stores matching
// the contract's fixture seed (contract/fake/README.md §Seed). Returns a restore
// func that nils them so the package's other state is untouched.
// productionGUIServedRoutes mirrors the startup derivation in cbridge.go: the
// metrics server's built-ins plus every route in controlPlaneRoutes.
func productionGUIServedRoutes() []string {
	served := []string{"/health", "/metrics"}
	for route := range controlPlaneRoutes() {
		served = append(served, route)
	}
	sort.Strings(served)
	return served
}

// TestProductionCapabilitiesAreInVendoredWhoamiEnum is the direct B-7 guard:
// every token the production GUI route inventory derives must be accepted by
// the vendored whoami schema's capability enum, and the GUI fixture must list
// exactly that set.
func TestProductionCapabilitiesAreInVendoredWhoamiEnum(t *testing.T) {
	caps := cplane.DeriveCapabilities(productionGUIServedRoutes())

	var schema struct {
		Properties struct {
			Capabilities struct {
				Items struct {
					Enum []string `json:"enum"`
				} `json:"items"`
			} `json:"capabilities"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(readContractFile(t, "spec/schema/whoami.schema.json"), &schema); err != nil {
		t.Fatal(err)
	}
	enum := map[string]bool{}
	for _, tok := range schema.Properties.Capabilities.Items.Enum {
		enum[tok] = true
	}
	if len(enum) == 0 {
		t.Fatal("vendored whoami schema has no capability enum; refusing to report")
	}
	for _, tok := range caps {
		if !enum[tok] {
			t.Errorf("production /whoami advertises %q, which the vendored whoami schema enum rejects", tok)
		}
	}

	var fixture struct {
		Capabilities []string `json:"capabilities"`
	}
	if err := json.Unmarshal(readContractFile(t, "fixtures/whoami/gui.json"), &fixture); err != nil {
		t.Fatal(err)
	}
	want := append([]string(nil), fixture.Capabilities...)
	sort.Strings(want)
	if !reflect.DeepEqual(caps, want) {
		t.Errorf("production GUI capabilities drift from fixtures/whoami/gui.json:\n got  %v\n want %v", caps, want)
	}
}

func readContractFile(t *testing.T, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "contract", filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func seedControlPlane(t *testing.T, validator ...func(pin.Entry) bool) func() {
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
		e.MtimeNsecKnown = true
		if err := mstore.Insert(e); err != nil {
			t.Fatal(err)
		}
	}

	pstore, err := pin.Open(":memory:")
	if err != nil {
		t.Fatalf("pin.Open: %v", err)
	}
	// clip_031: durable Ready plus explicit mock admission for JSON contracts.
	if err := pstore.Pin(clip031, 880000000, "/Volumes/zpool/Project_Foo"); err != nil {
		t.Fatal(err)
	}
	seedStatusReady(t, pstore, clip031)
	checkReady := func(e pin.Entry) bool { return len(e.Authority) != 0 }
	if len(validator) == 1 {
		checkReady = validator[0]
	}
	if err := pstore.SetReadyValidator(checkReady); err != nil {
		t.Fatal(err)
	}
	seeded, ok := pstore.Get(clip031)
	if !ok {
		t.Fatal("missing seeded Ready pin")
	}
	if admitted, err := pstore.AdmitReady(*seeded); err != nil || !admitted {
		t.Fatalf("admit JSON fixture: admitted=%v err=%v", admitted, err)
	}
	// clip_205: warming (prefetching, bytes_cached < size).
	_ = pstore.Pin(clip205, 1240000000, "/Volumes/zpool/Project_Foo")
	_ = pstore.UpdateStatus(clip205, pin.StatusPrefetching, 620000000, "")

	// Tier-B derivative index (JM-14). Seed inode 1180417 (clip_031) to match
	// contract/fixtures/derivatives/ready.json + metadata/tech.json exactly. The
	// absent fixtures use inode 999999, which is intentionally NOT seeded so the
	// handler's fail-closed exists:false path is exercised.
	dstore, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatalf("derivatives.Open: %v", err)
	}
	seedDerivatives(t, dstore)
	// Keep the absent-derivative fixture hermetic. The live handler performs a
	// one-sidecar reconcile on an unknown inode and prefers globalFUSEPath; if a
	// test leaves it empty, it falls back to the contract's display path
	// (/Volumes/zpool) and can read whichever unrelated share the host mounted
	// there. A local empty root proves the same absent-sidecar contract without
	// touching user data or poisoning the process-wide sidecar admission gate.
	fuseRoot := t.TempDir()

	// B-7: derive the served set from the PRODUCTION route inventory, exactly
	// as startup does (metrics built-ins + controlPlaneRoutes). A hand-kept
	// list here once omitted /derivatives/batch, so live /whoami emitted a
	// token the vendored schema rejected while this suite stayed green.
	guiRoutes := productionGUIServedRoutes()

	globalMu.Lock()
	old := struct {
		store                                   *metadata.Store
		pin                                     *pin.Store
		deriv                                   *derivatives.Store
		mount, fuse, want, vol, dbp, addr, inst string
		caps                                    []string
	}{globalStore, globalPinStore, globalDerivStore, globalMountPath, globalFUSEPath, globalWantMountPoint, globalVolumeName, globalDBPath, globalMetricsAddr, globalInstanceID, globalCapabilities}
	globalStore = mstore
	globalPinStore = pstore
	globalDerivStore = dstore
	globalMountPath = mp
	globalFUSEPath = fuseRoot
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
		globalDerivStore = old.deriv
		globalMountPath = old.mount
		globalFUSEPath = old.fuse
		globalWantMountPoint = old.want
		globalVolumeName = old.vol
		globalDBPath = old.dbp
		globalMetricsAddr = old.addr
		globalInstanceID = old.inst
		globalCapabilities = old.caps
		globalMu.Unlock()
		mstore.Close()
		pstore.Close()
		dstore.Close()
	}
}

// seedDerivatives loads inode 1180417 to match contract/fixtures/derivatives/
// ready.json + metadata/tech.json byte-for-byte (updated_at pinned to the
// fixture's 1750000000 so the live response compares equal). The 6 manifest
// rows + the tech payload are the canonical JM-14 seed.
func seedDerivatives(t *testing.T, ds *derivatives.Store) {
	t.Helper()
	const inode = 1180417
	const srcHash = "9f2b1c0a4e7d8b30"
	const updated = 1750000000
	if err := ds.PutSource(inode, sp(srcHash)); err != nil {
		t.Fatalf("PutSource: %v", err)
	}
	rows := []derivatives.DerivRow{
		{Kind: "tech", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp(srcHash), UpdatedAt: updated},
		{Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp(srcHash), BlobRelPath: sp("poster.jpg"), MediaType: sp("image/jpeg"), UpdatedAt: updated},
		{Kind: "filmstrip", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp(srcHash), BlobRelPath: sp("strip.jpg"), MediaType: sp("image/jpeg"), UpdatedAt: updated,
			Filmstrip: &derivatives.FilmstripGeo{FrameCount: 120, Cols: 12, Rows: 10, CellW: 160, CellH: 90, IntervalMS: 1000, DurationMS: 120000}},
		{Kind: "waveform", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp(srcHash), BlobRelPath: sp("waveform.json"), MediaType: sp("application/json"), UpdatedAt: updated},
		{Kind: "proxy", Status: "pending", Producer: "linux-farm", Version: 1, MediaType: sp("video/mp4"), UpdatedAt: updated},
		{Kind: "embedding", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp(srcHash), Model: sp("openclip-vit-b32-v1"), Dim: ip(512), UpdatedAt: updated},
		{Kind: "ai", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp(srcHash), BlobRelPath: sp("ai.loupe.json"), MediaType: sp("application/json"), Model: sp("openclip-vit-b32-v1"), Dim: ip(512), UpdatedAt: updated},
	}
	for _, r := range rows {
		if err := ds.PutDeriv(inode, r); err != nil {
			t.Fatalf("PutDeriv %s: %v", r.Kind, err)
		}
	}
	techPayload := `{
		"container": "mov",
		"duration_ms": 12480,
		"size_bytes": 880000000,
		"video": { "codec": "hevc", "width": 3840, "height": 2160, "fps": 23.976, "pix_fmt": "yuv422p10le", "bit_depth": 10, "color_primaries": "bt2020", "transfer": "arib-std-b67", "matrix": "bt2020nc", "hdr": "hlg", "log_format": null, "timecode": "01:00:00:00" },
		"audio": [ { "codec": "pcm_s24le", "channels": 2, "sample_rate": 48000, "bit_depth": 24, "language": null } ]
	}`
	if err := ds.PutMetadata(inode, "tech", "linux-farm", 1, sp(srcHash), json.RawMessage(techPayload)); err != nil {
		t.Fatalf("PutMetadata: %v", err)
	}
}

func sp(s string) *string { return &s }
func ip(i int) *int       { return &i }

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
		query    string // raw query string (e.g. "inode=N&kind=tech"); wins over path
		fixture  string
		schema   string
	}{
		{"whoami_gui", "/whoami", handleWhoamiHTTP, "", "", "whoami/gui.json", "whoami.schema.json"},
		{"residency_resident", "/residency", handleResidencyHTTP, clip031, "", "residency/resident.json", "residency.schema.json"},
		{"residency_streaming", "/residency", handleResidencyHTTP, clip204, "", "residency/streaming.json", "residency.schema.json"},
		{"residency_warming", "/residency", handleResidencyHTTP, clip205, "", "residency/warming.json", "residency.schema.json"},
		{"residency_absent", "/residency", handleResidencyHTTP, deleted, "", "residency/absent.json", "residency.schema.json"},
		{"lookup_file", "/lookup", handleLookupHTTP, clip031, "", "lookup/file.json", "lookup.schema.json"},
		{"lookup_missing", "/lookup", handleLookupHTTP, notHere, "", "lookup/missing.json", "lookup.schema.json"},
		// JM-14: derivative manifest + structured metadata, keyed by inode.
		{"derivatives_ready", "/derivatives", handleDerivativesHTTP, "", "inode=1180417", "derivatives/ready.json", "derivatives.schema.json"},
		{"derivatives_absent", "/derivatives", handleDerivativesHTTP, "", "inode=999999", "derivatives/absent.json", "derivatives.schema.json"},
		{"metadata_tech", "/metadata", handleMetadataHTTP, "", "inode=1180417&kind=tech", "metadata/tech.json", "metadata.schema.json"},
		{"metadata_absent", "/metadata", handleMetadataHTTP, "", "inode=999999&kind=tech", "metadata/absent.json", "metadata.schema.json"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			u := tc.endpoint
			if tc.query != "" {
				u += "?" + tc.query
			} else if tc.path != "" {
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

func TestContractResidencyUploadState(t *testing.T) {
	restore := seedControlPlane(t)
	defer restore()
	globalMu.Lock()
	store := globalStore
	oldSpool := globalSpool
	globalMu.Unlock()
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spoolMeta := metadata.NewSpoolStore(store.DB())
	spool, err := jmnfs.NewSpoolStore(t.TempDir(), 0, spoolMeta)
	if err != nil {
		t.Fatal(err)
	}
	globalMu.Lock()
	globalSpool = spool
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalSpool = oldSpool
		globalMu.Unlock()
		spool.Stop()
	}()

	check := func(path string, want any) {
		t.Helper()
		rr := httptest.NewRecorder()
		handleResidencyHTTP(rr, httptest.NewRequest("GET", "/residency?"+url.Values{"path": {path}}.Encode(), nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
		}
		var got map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if err := compileSchema(t, "residency.schema.json").Validate(got); err != nil {
			t.Fatalf("invalid residency response: %v", err)
		}
		if got["upload_state"] != want {
			t.Errorf("%s upload_state=%v, want %v", path, got["upload_state"], want)
		}
	}

	check(clip204, nil)
	failedID, err := spoolMeta.Insert("Project_Foo/clip_204.mov", "/spool/files/failed")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spoolMeta.MarkFailed(failedID, "upload exhausted"); err != nil {
		t.Fatal(err)
	}
	check(clip204, "failed")

	doneID, err := spoolMeta.Insert("Project_Foo/clip_031.mov", "/spool/files/done")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spoolMeta.MarkDone(doneID); err != nil {
		t.Fatal(err)
	}
	check(clip031, nil)
	if ok, err := spoolMeta.ResetForRetry(failedID); err != nil || !ok {
		t.Fatalf("retry failed upload: ok=%v err=%v", ok, err)
	}
	check(clip204, "ready")
	if _, err := spoolMeta.MarkDone(failedID); err != nil {
		t.Fatal(err)
	}
	check(clip204, nil)
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

func TestContractCacheStatusEmptyRootsLive(t *testing.T) {
	pstore, err := pin.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	globalMu.Lock()
	oldPin, oldPrefetcher := globalPinStore, globalPrefetcher
	globalPinStore, globalPrefetcher = pstore, nil
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalPinStore, globalPrefetcher = oldPin, oldPrefetcher
		globalMu.Unlock()
		pstore.Close()
	})

	// The desktop archive (-tags serviceclient) never owns the pin store: its
	// NFSServerCacheStatus forwards to the launchd serving process, which is
	// built without the tag and answers from engineCacheStatus. Stand in for
	// that process with the same engine snapshot, so the client build checks
	// the document the user's GUI really receives, through the real forward.
	var forwarded []string
	if serviceClientBuild {
		oldCall, oldPath := callServingProcess, servingSocketPath
		callServingProcess = func(_ context.Context, _ string, req servicecontrol.Request) (servicecontrol.Response, error) {
			forwarded = append(forwarded, req.Method)
			b, err := json.Marshal(engineCacheStatus())
			if err != nil {
				return servicecontrol.Response{}, err
			}
			return servicecontrol.Response{Version: 1, Text: string(b)}, nil
		}
		servingSocketPath = func() (string, error) { return "/private/fixture/control.sock", nil }
		t.Cleanup(func() { callServingProcess, servingSocketPath = oldCall, oldPath })
	}

	rec := httptest.NewRecorder()
	handleCacheStatusHTTP(rec, httptest.NewRequest(http.MethodGet, "/cache-status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if serviceClientBuild && !reflect.DeepEqual(forwarded, []string{"cache-status"}) {
		t.Fatalf("desktop cache-status forwards = %v; want exactly one cache-status call", forwarded)
	}
	var doc map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	roots, ok := doc["roots"].([]any)
	if !ok || len(roots) != 0 {
		t.Fatalf("empty pin store roots = %v; want []", doc["roots"])
	}
	if err := compileSchema(t, "cache-status.schema.json").Validate(doc); err != nil {
		t.Fatalf("live cache-status violates contract: %v", err)
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
	// The JM-14 derivative manifest is an unordered set of per-kind rows, and
	// each row's updated_at is runtime-volatile. Normalize both sides (strip
	// updated_at, sort by kind) so the comparison reflects the contract's
	// semantics rather than incidental ordering.
	if _, ok := gm["derivatives"]; ok {
		gm["derivatives"] = normalizeDerivatives(gm["derivatives"])
		wm["derivatives"] = normalizeDerivatives(wm["derivatives"])
	}
	if !reflect.DeepEqual(gm, wm) {
		t.Errorf("%s: structural mismatch:\n got=%#v\nwant=%#v", name, gm, wm)
	}
}

// normalizeDerivatives returns a "derivatives" array with each row's volatile
// updated_at removed and the rows sorted by kind — so the manifest compares as
// an unordered set (mirrors the contract: kinds are unique per asset, order is
// not meaningful, updated_at is volatile).
func normalizeDerivatives(v any) any {
	arr, ok := v.([]any)
	if !ok {
		return v
	}
	out := make([]any, 0, len(arr))
	for _, e := range arr {
		if em, ok := e.(map[string]any); ok {
			cm := cloneMap(em)
			delete(cm, "updated_at")
			out = append(out, cm)
		} else {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return derivKind(out[i]) < derivKind(out[j]) })
	return out
}

func derivKind(v any) string {
	if m, ok := v.(map[string]any); ok {
		if k, ok := m["kind"].(string); ok {
			return k
		}
	}
	return ""
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
