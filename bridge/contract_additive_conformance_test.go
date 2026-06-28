package main

// Additive contract conformance (juicemount-contract a59b87f): the PROXY-CODEC
// (#50) /blob byte-range + proxy-codec derivative fields, and JM-ASSERT (#51)
// /assertions sidecars. These cases need a REAL temp mount (a proxy.mp4 blob to
// range-serve; a media file to hash + sidecar), so they run against their own
// self-contained seed rather than the in-memory seedControlPlane used by
// TestContractLiveConformance. The existing fixtures + the new ones are all
// schema-validated by TestContractFixturesValidateAgainstSchemas.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cplane"
	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/metadata"
)

// seedAdditive builds a self-contained control plane over a real temp mount:
//   - mount/Project_Foo/clip_205.mov : a real (small) media file (inode echoed
//     from the metadata mirror; the on-disk file is what the assertion handler
//     hashes + writes the sidecar next to).
//   - mount/.juicemount/derivatives/1180621/proxy.mp4 : a real proxy blob for the
//     /blob range test + the proxy-ready manifest row.
//
// Returns the mount dir + a restore func. Mirrors seedControlPlane's global
// swap discipline.
func seedAdditive(t *testing.T) (string, func()) {
	t.Helper()
	mount := t.TempDir()

	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	mt := time.Unix(seedMtime, 0)
	for _, e := range []*metadata.Entry{
		{Path: "Project_Foo/clip_205.mov", Name: "clip_205.mov", ParentPath: "Project_Foo", Size: 1240000000, Inode: 1180621, Mtime: mt},
	} {
		mstore.InsertToCache(e)
	}
	// The real source file the assertion handler hashes + sidecars next to.
	srcPath := filepath.Join(mount, "Project_Foo", "clip_205.mov")
	if err := os.MkdirAll(filepath.Dir(srcPath), 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	if err := os.WriteFile(srcPath, []byte("FAKE-MOV-BYTES-for-assert-hash"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	ds, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatalf("derivatives.Open: %v", err)
	}
	// Proxy-ready derivative (inode 1180621) — matches derivatives/proxy-ready.json:
	// codec h264 + its canPlayType token + blob_size (actual produced bytes).
	const inode = uint64(1180621)
	const srcHash = "9f2b1c0a4e7d8b30"
	if err := ds.PutSource(inode, sp(srcHash)); err != nil {
		t.Fatalf("PutSource: %v", err)
	}
	blobDir := farm.DerivBlobDir(mount, inode)
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatalf("mkdir blobdir: %v", err)
	}
	// A real proxy blob to range-serve. Its size is what /blob reports in
	// Content-Range/Content-Length — the conformance test asserts the HEADER
	// CONTRACT structure (status, Accept-Ranges, the bytes 0-1/<N> pattern,
	// Content-Length 2, Content-Type), not the fixture's illustrative byte count.
	proxyBytes := []byte("a-small-but-real-faststart-mp4-proxy-blob-for-range-serving")
	blobPath := filepath.Join(blobDir, "proxy.mp4")
	if err := os.WriteFile(blobPath, proxyBytes, 0o644); err != nil {
		t.Fatalf("write proxy blob: %v", err)
	}
	blobSize := int64(len(proxyBytes))
	codec, codecString := "h264", "avc1.640028, mp4a.40.2"
	rel, mtv := "proxy.mp4", "video/mp4"
	if err := ds.PutDeriv(inode, derivatives.DerivRow{
		Kind: "tech", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp(srcHash),
	}); err != nil {
		t.Fatalf("PutDeriv tech: %v", err)
	}
	if err := ds.PutDeriv(inode, derivatives.DerivRow{
		Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp(srcHash),
		BlobRelPath: &rel, MediaType: &mtv, Codec: &codec, CodecString: &codecString, BlobSize: &blobSize,
	}); err != nil {
		t.Fatalf("PutDeriv proxy: %v", err)
	}

	guiRoutes := []string{
		"/health", "/metrics", "/whoami", "/residency", "/lookup",
		"/derivatives", "/metadata", "/derivatives/register", "/derivatives/changes",
		"/blob", "/assertions",
	}

	globalMu.Lock()
	old := struct {
		store                       *metadata.Store
		deriv                       *derivatives.Store
		mount, fuse, want, vol, dbp string
		caps                        []string
	}{globalStore, globalDerivStore, globalMountPath, globalFUSEPath, globalWantMountPoint, globalVolumeName, globalDBPath, globalCapabilities}
	globalStore = mstore
	globalDerivStore = ds
	globalMountPath = mount
	globalFUSEPath = mount
	globalWantMountPoint = mount
	globalVolumeName = "zpool"
	globalDBPath = ":memory:"
	globalCapabilities = cplane.DeriveCapabilities(guiRoutes)
	globalMu.Unlock()

	return mount, func() {
		globalMu.Lock()
		globalStore = old.store
		globalDerivStore = old.deriv
		globalMountPath = old.mount
		globalFUSEPath = old.fuse
		globalWantMountPoint = old.want
		globalVolumeName = old.vol
		globalDBPath = old.dbp
		globalCapabilities = old.caps
		globalMu.Unlock()
		mstore.Close()
		ds.Close()
	}
}

// TestContractProxyCodecConformance: GET /derivatives?inode=1180621 carries the
// PROXY-CODEC additive fields (codec/codec_string/blob_size) and validates
// against derivatives.schema.json. The proxy row is compared field-by-field;
// blob_size is volatile (the seed's real blob is small), so it's checked for
// presence + type only.
func TestContractProxyCodecConformance(t *testing.T) {
	_, restore := seedAdditive(t)
	defer restore()

	req := httptest.NewRequest("GET", "/derivatives?inode=1180621", nil)
	rr := httptest.NewRecorder()
	handleDerivativesHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if err := compileSchema(t, "derivatives.schema.json").Validate(got); err != nil {
		t.Errorf("response does not validate against derivatives.schema.json:\n%v\nbody=%s", err, rr.Body.String())
	}
	// Find the proxy row and assert the PROXY-CODEC fields.
	derivs, _ := got["derivatives"].([]any)
	var proxy map[string]any
	for _, d := range derivs {
		if m, ok := d.(map[string]any); ok && m["kind"] == "proxy" {
			proxy = m
			break
		}
	}
	if proxy == nil {
		t.Fatalf("no proxy row in response: %s", rr.Body.String())
	}
	if proxy["codec"] != "h264" {
		t.Errorf("codec = %v, want h264", proxy["codec"])
	}
	if proxy["codec_string"] != "avc1.640028, mp4a.40.2" {
		t.Errorf("codec_string = %v, want avc1.640028, mp4a.40.2", proxy["codec_string"])
	}
	if _, ok := proxy["blob_size"].(float64); !ok {
		t.Errorf("blob_size missing/not-a-number: %v", proxy["blob_size"])
	}
	if proxy["status"] != "ready" {
		t.Errorf("status = %v, want ready", proxy["status"])
	}
}

// TestContractBlobRangeConformance: GET /blob?inode=1180621&kind=proxy honors the
// PROXY-CODEC range-serve contract (blob/proxy-range.json). A ranged request →
// 206 + Accept-Ranges:bytes + Content-Range:bytes 0-1/<N> + Content-Length:2 +
// Content-Type:video/mp4; an unranged GET → 200 + Accept-Ranges:bytes +
// Content-Length:<N> + Content-Type:video/mp4. <N> is the live blob size
// (volatile); the fixture's 20447232 is illustrative.
func TestContractBlobRangeConformance(t *testing.T) {
	_, restore := seedAdditive(t)
	defer restore()

	// Ranged.
	rreq := httptest.NewRequest("GET", "/blob?inode=1180621&kind=proxy", nil)
	rreq.Header.Set("Range", "bytes=0-1")
	rrr := httptest.NewRecorder()
	handleBlobHTTP(rrr, rreq)
	if rrr.Code != http.StatusPartialContent {
		t.Fatalf("ranged status = %d, want 206; body=%s", rrr.Code, rrr.Body.String())
	}
	if got := rrr.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("ranged Accept-Ranges = %q, want bytes", got)
	}
	if got := rrr.Header().Get("Content-Type"); got != "video/mp4" {
		t.Errorf("ranged Content-Type = %q, want video/mp4", got)
	}
	if got := rrr.Header().Get("Content-Length"); got != "2" {
		t.Errorf("ranged Content-Length = %q, want 2", got)
	}
	if cr := rrr.Header().Get("Content-Range"); !strings.HasPrefix(cr, "bytes 0-1/") {
		t.Errorf("ranged Content-Range = %q, want prefix 'bytes 0-1/'", cr)
	}
	if n := rrr.Body.Len(); n != 2 {
		t.Errorf("ranged body len = %d, want 2", n)
	}

	// Unranged.
	ureq := httptest.NewRequest("GET", "/blob?inode=1180621&kind=proxy", nil)
	urr := httptest.NewRecorder()
	handleBlobHTTP(urr, ureq)
	if urr.Code != http.StatusOK {
		t.Fatalf("unranged status = %d, want 200; body=%s", urr.Code, urr.Body.String())
	}
	if got := urr.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("unranged Accept-Ranges = %q, want bytes", got)
	}
	if got := urr.Header().Get("Content-Type"); got != "video/mp4" {
		t.Errorf("unranged Content-Type = %q, want video/mp4", got)
	}
	if got := urr.Header().Get("Content-Length"); got == "" || got == "0" {
		t.Errorf("unranged Content-Length = %q, want full size", got)
	}
}

// TestContractAssertionsConformance: the JM-ASSERT POST/GET lifecycle.
//   - POST log_profile=clog3 → accepted:true, sidecar written next to the media.
//   - POST log_profile=slog3 with an OLDER asserted_at → accepted:false,
//     winning_asserted_at = the stored winner (reject-stale; assertions.schema).
//   - GET by asset_key → the resolved set (assertions-get.schema).
//   - GET unknown inode → empty asset_key + empty assertions[] (get-empty).
//   - POST namespace=person → merges, doesn't clobber the log_profile triple.
func TestContractAssertionsConformance(t *testing.T) {
	mount, restore := seedAdditive(t)
	defer restore()

	post := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/assertions", strings.NewReader(body))
		rr := httptest.NewRecorder()
		handleAssertionsHTTP(rr, req)
		return rr
	}
	get := func(qs string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/assertions?"+qs, nil)
		rr := httptest.NewRecorder()
		handleAssertionsHTTP(rr, req)
		return rr
	}

	// 1) Accept a fresh log_profile assertion by inode.
	rr := post(`{"inode":1180621,"namespace":"log_profile","key":"value","value":"clog3","asserted_by":"ol:leland@lelanddutcher.com/MacBook","asserted_at":"2026-06-27T18:40:00Z"}`)
	if rr.Code != 200 {
		t.Fatalf("post accept status=%d body=%s", rr.Code, rr.Body.String())
	}
	var acc map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &acc)
	if err := compileSchema(t, "assertions.schema.json").Validate(acc); err != nil {
		t.Errorf("post-accept does not validate: %v\nbody=%s", err, rr.Body.String())
	}
	if acc["accepted"] != true {
		t.Errorf("accepted = %v, want true", acc["accepted"])
	}
	assetKey, _ := acc["asset_key"].(string)
	if !strings.HasPrefix(assetKey, "xxh3:") {
		t.Errorf("asset_key = %q, want xxh3: content-hash form", assetKey)
	}
	// The sidecar is the source of truth — it must exist next to the media + validate.
	scPath := filepath.Join(mount, "Project_Foo", "clip_205.mov.loupe.json")
	scRaw, err := os.ReadFile(scPath)
	if err != nil {
		t.Fatalf("sidecar not written: %v", err)
	}
	var scAny any
	_ = json.Unmarshal(scRaw, &scAny)
	if err := compileSchema(t, "assertions-sidecar.schema.json").Validate(scAny); err != nil {
		t.Errorf("sidecar does not validate against assertions-sidecar.schema.json:\n%v\n%s", err, scRaw)
	}

	// 2) Reject-stale: an OLDER asserted_at for the same (namespace,key).
	rr = post(`{"asset_key":"` + assetKey + `","namespace":"log_profile","key":"value","value":"slog3","asserted_by":"ol:other@host/Mac","asserted_at":"2026-06-27T10:00:00Z"}`)
	var rej map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &rej)
	if err := compileSchema(t, "assertions.schema.json").Validate(rej); err != nil {
		t.Errorf("post-reject does not validate: %v\nbody=%s", err, rr.Body.String())
	}
	if rej["accepted"] != false {
		t.Errorf("stale accepted = %v, want false", rej["accepted"])
	}
	if rej["winning_asserted_at"] != "2026-06-27T18:40:00Z" {
		t.Errorf("winning_asserted_at = %v, want the stored winner 2026-06-27T18:40:00Z", rej["winning_asserted_at"])
	}

	// 3) GET by asset_key → the resolved set, validates assertions-get.schema.
	rr = get("asset_key=" + url.QueryEscape(assetKey))
	var rd map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &rd)
	if err := compileSchema(t, "assertions-get.schema.json").Validate(rd); err != nil {
		t.Errorf("get does not validate: %v\nbody=%s", err, rr.Body.String())
	}
	got := rd["assertions"].([]any)
	if len(got) != 1 {
		t.Errorf("got %d assertions, want 1 (the clog3 winner)", len(got))
	} else {
		a := got[0].(map[string]any)
		if a["value"] != "clog3" {
			t.Errorf("resolved value = %v, want clog3 (stale slog3 must NOT win)", a["value"])
		}
	}

	// 4) GET unknown inode → empty asset_key + empty assertions[] (fail-closed).
	rr = get("inode=999999")
	var empty map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &empty)
	if err := compileSchema(t, "assertions-get.schema.json").Validate(empty); err != nil {
		t.Errorf("get-empty does not validate: %v\nbody=%s", err, rr.Body.String())
	}
	if empty["asset_key"] != "" {
		t.Errorf("empty asset_key = %v, want \"\"", empty["asset_key"])
	}
	if len(empty["assertions"].([]any)) != 0 {
		t.Errorf("empty assertions = %v, want []", empty["assertions"])
	}

	// 5) Merge-not-clobber: a person assertion leaves the log_profile triple intact.
	rr = post(`{"asset_key":"` + assetKey + `","namespace":"person","key":"c_3f","value":"Bob","asserted_by":"ol:leland@lelanddutcher.com/MacBook","asserted_at":"2026-06-27T18:41:12Z"}`)
	if rr.Code != 200 {
		t.Fatalf("person post status=%d", rr.Code)
	}
	rr = get("asset_key=" + url.QueryEscape(assetKey))
	_ = json.Unmarshal(rr.Body.Bytes(), &rd)
	if n := len(rd["assertions"].([]any)); n != 2 {
		t.Errorf("after person merge: %d assertions, want 2 (log_profile + person)", n)
	}
	// The on-disk sidecar must carry both namespaces too.
	scRaw, _ = os.ReadFile(scPath)
	if !strings.Contains(string(scRaw), "log_profile") || !strings.Contains(string(scRaw), "person") {
		t.Errorf("sidecar lost a namespace after merge:\n%s", scRaw)
	}
}

// TestLogProfileOverlay: OUT-8 LWW-over-detection. With JM_LOG_PROFILE_OVERLAY=1
// an accepted human log_profile assertion overrides the DETECTED
// tech.video.log_format; OFF (default) it does not (privacy opt-in gate).
func TestLogProfileOverlay(t *testing.T) {
	_, restore := seedAdditive(t)
	defer restore()

	// Seed a tech metadata row (detected log_format: null) for inode 1180621.
	globalMu.Lock()
	ds := globalDerivStore
	globalMu.Unlock()
	techPayload := `{"container":"mov","duration_ms":1000,"size_bytes":1240000000,"video":{"codec":"hevc","width":3840,"height":2160,"fps":24,"pix_fmt":"yuv422p10le","bit_depth":10,"color_primaries":"bt2020","transfer":"arib-std-b67","matrix":"bt2020nc","hdr":"hlg","log_format":null,"timecode":"01:00:00:00"},"audio":[]}`
	if err := ds.PutMetadata(1180621, "tech", "linux-farm", 1, sp("9f2b1c0a4e7d8b30"), json.RawMessage(techPayload)); err != nil {
		t.Fatalf("PutMetadata: %v", err)
	}
	// Accept a human log_profile assertion (clog3).
	preq := httptest.NewRequest("POST", "/assertions", strings.NewReader(`{"inode":1180621,"namespace":"log_profile","key":"value","value":"clog3","asserted_by":"ol:leland/Mac","asserted_at":"2026-06-27T18:40:00Z"}`))
	prr := httptest.NewRecorder()
	handleAssertionsHTTP(prr, preq)
	if prr.Code != 200 {
		t.Fatalf("assert status=%d", prr.Code)
	}

	techLogFormat := func() any {
		req := httptest.NewRequest("GET", "/metadata?inode=1180621&kind=tech", nil)
		rr := httptest.NewRecorder()
		handleMetadataHTTP(rr, req)
		var m map[string]any
		_ = json.Unmarshal(rr.Body.Bytes(), &m)
		tech := m["tech"].(map[string]any)
		video := tech["video"].(map[string]any)
		return video["log_format"]
	}

	// Gate OFF (default): detection wins (null).
	t.Setenv("JM_LOG_PROFILE_OVERLAY", "")
	if lf := techLogFormat(); lf != nil {
		t.Errorf("gate OFF: log_format = %v, want null (detection)", lf)
	}
	// Gate ON: the human assertion overrides → clog3.
	t.Setenv("JM_LOG_PROFILE_OVERLAY", "1")
	if lf := techLogFormat(); lf != "clog3" {
		t.Errorf("gate ON: log_format = %v, want clog3 (human override)", lf)
	}
}
