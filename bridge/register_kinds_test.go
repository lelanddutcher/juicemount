package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/metadata"
)

// REGISTER-ROUTE (2026-08-04). The kind table is the security boundary of the
// widened route: it pins the on-disk filename AND assigns the media type
// server-side. These pin both properties against drift.

// The reserved filenames MUST match spec/WRITE_PLACEMENT.md §2 exactly. A drift
// here means a consumer writes one name and we register another — the blob-exists
// check would 409 and contribute-back would silently never work.
func TestContributableKindsMatchTheReservedFilenames(t *testing.T) {
	want := map[string]string{
		"proxy":     "proxy.mp4",
		"thumbnail": "poster.jpg",
		"waveform":  "waveform.json",
	}
	for kind, file := range want {
		got, ok := contributableKinds[kind]
		if !ok {
			t.Errorf("kind %q must be consumer-registrable", kind)
			continue
		}
		if got.file != file {
			t.Errorf("kind %q reserved file = %q, want %q (WRITE_PLACEMENT §2)", kind, got.file, file)
		}
	}
	// `ai` keeps the empty sentinel so its dual-name resolution still runs.
	if ai, ok := contributableKinds["ai"]; !ok || ai.file != "" {
		t.Errorf(`kind "ai" must keep file=="" so ResolveAIBlobName picks logger/loupe; got %+v`, ai)
	}
	// filmstrip stays CLOSED until its reader contract is specified.
	if _, ok := contributableKinds["filmstrip"]; ok {
		t.Error("filmstrip must NOT be consumer-registrable — its reader contract (canvas, gutters, orientation) is unspecified")
	}
}

// media_type is assigned from the kind, never echoed from the request: /blob
// pins its response Content-Type from this row field, so a contributor-supplied
// value would be a contributor-controlled Content-Type on our origin.
func TestContributableKindMediaTypesAreServerAssigned(t *testing.T) {
	want := map[string]string{
		"ai":        "application/json",
		"waveform":  "application/json",
		"proxy":     "video/mp4",
		"thumbnail": "image/jpeg",
	}
	for kind, mt := range want {
		if got := contributableKinds[kind].mediaType; got != mt {
			t.Errorf("kind %q media type = %q, want %q", kind, got, mt)
		}
	}
	// Nothing may declare a scriptable type — the whole point of pinning it.
	for kind, spec := range contributableKinds {
		switch spec.mediaType {
		case "text/html", "application/javascript", "image/svg+xml", "":
			t.Errorf("kind %q has media type %q — scriptable/empty types must never be served from our origin", kind, spec.mediaType)
		}
	}
}

// Every registrable kind must also be a kind the manifest schema knows, or the
// row we write fails the consumer's own validation of /derivatives.
func TestContributableKindsAreValidManifestKinds(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "contract", "spec", "schema", "derivatives.schema.json"))
	if err != nil {
		t.Skipf("vendored contract unavailable: %v", err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	// EXACT path, not a search. The first version of this test walked the whole
	// document for any "kind" with an enum and kept the first hit — but Go map
	// iteration is RANDOM, and this schema has TWO such nodes: the real kind enum
	// and a ["proxy","audio_proxy"] enum nested in an allOf/if/not conditional.
	// So the test's verdict depended on map ordering: same code, either result.
	// A locator that can resolve to the wrong node is not a locator.
	kindEnum := digEnum(t, doc, "properties", "derivatives", "items", "properties", "kind")
	valid := map[string]bool{}
	for _, k := range kindEnum {
		if s, ok := k.(string); ok {
			valid[s] = true
		}
	}
	for kind := range contributableKinds {
		if !valid[kind] {
			t.Errorf("registrable kind %q is not in derivatives.schema.json's kind enum — the row we write would fail the consumer's validation", kind)
		}
	}
}

// End-to-end coverage of the WIDENED route (REGISTER-ROUTE). The reviewer noted
// that nothing posted a non-ai kind to the actual handler — the table tests above
// only inspect a map. These drive handleDerivativesRegisterHTTP for real.
func TestRegisterWidenedKindsEndToEnd(t *testing.T) {
	tmp := t.TempDir()
	const inode = 777001
	const rel = "Shoot/clip.mov"
	src := filepath.Join(tmp, rel)
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("src-"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700000000, 0)
	if err := os.Chtimes(src, mt, mt); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(src)
	liveSize, liveMtime := fi.Size(), fi.ModTime().Unix()

	blobDir := filepath.Join(tmp, ".juicemount", "derivatives", "777001")
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeBlob := func(name string) {
		if err := os.WriteFile(filepath.Join(blobDir, name), []byte("blob-bytes"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer mstore.Close()
	mstore.InsertToCache(&metadata.Entry{Path: rel, Name: "clip.mov", ParentPath: "Shoot", Inode: inode, Size: liveSize, Mtime: mt})
	dstore, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dstore.Close()

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = mstore, dstore, tmp
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	}()

	post := func(body map[string]any) *httptest.ResponseRecorder {
		b, _ := json.Marshal(body)
		rr := httptest.NewRecorder()
		handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(b)))
		return rr
	}
	req := func(kind string, extra map[string]any) map[string]any {
		m := map[string]any{
			"inode": inode, "kind": kind, "producer": "on-device",
			"source_size": liveSize, "source_mtime": liveMtime,
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	t.Run("thumbnail and waveform register", func(t *testing.T) {
		writeBlob("poster.jpg")
		if rr := post(req("thumbnail", nil)); rr.Code != 200 {
			t.Errorf("thumbnail: status %d, body %s", rr.Code, rr.Body.String())
		}
		writeBlob("waveform.json")
		if rr := post(req("waveform", nil)); rr.Code != 200 {
			t.Errorf("waveform: status %d, body %s", rr.Code, rr.Body.String())
		}
	})

	// FOUNDER DECISION 2026-08-04, superseding the H.264 floor this route shipped
	// with hours earlier. A node that cannot do a thing contributes the things it
	// CAN do; the fleet's output is the UNION, not the intersection. A codec floor
	// makes the least capable node set the ceiling for every node — strictly worse
	// on a heterogeneous fleet than letting each node declare what it made.
	t.Run("proxy codec is DECLARED, not mandated", func(t *testing.T) {
		writeBlob("proxy.mp4")
		// Still required: derivatives.schema.json reads an ABSENT codec as h264,
		// so silence would mislabel a richer codec rather than describe it.
		if rr := post(req("proxy", nil)); rr.Code != 400 {
			t.Errorf("proxy with NO codec: status %d, want 400 (absent codec is read as h264 downstream)", rr.Code)
		}
		// HEVC is now ACCEPTED — this is the reversal.
		if rr := post(req("proxy", map[string]any{"codec": "hevc"})); rr.Code != 200 {
			t.Errorf("proxy with hevc: status %d, want 200 — the codec floor was overturned; "+
				"a capable node contributes HEVC and the reader checks the row. body %s",
				rr.Code, rr.Body.String())
		}
		if rr := post(req("proxy", map[string]any{"codec": "h264"})); rr.Code != 200 {
			t.Errorf("proxy with h264: status %d, body %s", rr.Code, rr.Body.String())
		}
		// But a codec no reader can even RECOGNISE is still refused — that is
		// worse than one it cannot decode, because it cannot choose.
		if rr := post(req("proxy", map[string]any{"codec": "prores_raw_xyz"})); rr.Code != 400 {
			t.Errorf("proxy with an unknown codec: status %d, want 400", rr.Code)
		}
	})

	// Artifact descriptors: the server must be able to rank a contributed
	// artifact WITHOUT decoding it, so a cheap one can be earmarked for upgrade.
	t.Run("width/height/bitrate are accepted and echoed", func(t *testing.T) {
		writeBlob("poster.jpg")
		rr := post(req("thumbnail", map[string]any{"width": 1920, "height": 1080}))
		if rr.Code != 200 {
			t.Fatalf("thumbnail with dimensions: status %d, body %s", rr.Code, rr.Body.String())
		}
		var resp map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		d := resp["derivative"].(map[string]any)
		if d["width"] != float64(1920) || d["height"] != float64(1080) {
			t.Errorf("dimensions not echoed on the row: w=%v h=%v — without them a 320px and a "+
				"720px poster differ on the wire only by blob_size, which is a bad quality proxy",
				d["width"], d["height"])
		}
		// Nonsense dimensions are refused rather than stored.
		if rr := post(req("thumbnail", map[string]any{"width": 0, "height": 1080})); rr.Code != 400 {
			t.Errorf("zero width: status %d, want 400", rr.Code)
		}
		// Pixel dimensions are not meaningful for a waveform.
		writeBlob("waveform.json")
		if rr := post(req("waveform", map[string]any{"width": 100, "height": 100})); rr.Code != 400 {
			t.Errorf("dimensions on a waveform: status %d, want 400", rr.Code)
		}
	})

	t.Run("a kind outside the table is refused", func(t *testing.T) {
		if rr := post(req("filmstrip", nil)); rr.Code != 400 {
			t.Errorf("filmstrip: status %d, want 400 — it is deliberately withheld", rr.Code)
		}
		if rr := post(req("tech", nil)); rr.Code != 400 {
			t.Errorf("tech: status %d, want 400 — server-only kind", rr.Code)
		}
	})

	t.Run("a non-reserved blob path is refused", func(t *testing.T) {
		if rr := post(req("thumbnail", map[string]any{"blob_rel_path": "pwn.html"})); rr.Code != 400 {
			t.Errorf("status %d, want 400 — the consumer does not choose paths in our namespace", rr.Code)
		}
	})

	t.Run("a missing blob is refused, so no phantom row is minted", func(t *testing.T) {
		_ = os.Remove(filepath.Join(blobDir, "waveform.json"))
		if rr := post(req("waveform", nil)); rr.Code != 409 {
			t.Errorf("status %d, want 409 — the manifest row is the commit point", rr.Code)
		}
		writeBlob("waveform.json")
	})

	// The reviewer's CRITICAL: a symlink at the reserved name pointed anywhere
	// the bridge could read, and /blob then streamed it.
	t.Run("a symlink at the reserved name is refused", func(t *testing.T) {
		secret := filepath.Join(tmp, "secret.txt")
		if err := os.WriteFile(secret, []byte("PRIVATE KEY"), 0o600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(blobDir, "poster.jpg")
		_ = os.Remove(link)
		if err := os.Symlink(secret, link); err != nil {
			t.Skipf("symlink unsupported here: %v", err)
		}
		defer func() { _ = os.Remove(link); writeBlob("poster.jpg") }()

		if rr := post(req("thumbnail", nil)); rr.Code == 200 {
			t.Error("a symlinked blob registered successfully — /blob would serve the target as its media type")
		}
	})

	// A consumer must not silently replace a farm-produced row.
	t.Run("a farm row is not clobbered by an on-device contribution", func(t *testing.T) {
		writeBlob("poster.jpg")
		farmRow := derivatives.DerivRow{
			Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1,
			UpdatedAt: time.Now().Unix(),
		}
		if err := dstore.PutDeriv(inode, farmRow); err != nil {
			t.Fatal(err)
		}
		if rr := post(req("thumbnail", nil)); rr.Code != 409 {
			t.Errorf("status %d, want 409 — an on-device row must not replace the farm's", rr.Code)
		}
	})
}

// digEnum resolves an exact JSON path and returns the `enum` at its end. It
// FAILS rather than skips when the path is absent: a silent skip here would let
// a schema restructure quietly retire the check that keeps our registrable
// kinds and the manifest schema in agreement.
func digEnum(t *testing.T, doc any, path ...string) []any {
	t.Helper()
	cur := doc
	for _, seg := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("digEnum: %q is not an object while resolving %v", seg, path)
		}
		cur, ok = m[seg]
		if !ok {
			t.Fatalf("digEnum: no %q while resolving %v — did the schema move?", seg, path)
		}
	}
	m, ok := cur.(map[string]any)
	if !ok {
		t.Fatalf("digEnum: %v does not end at an object", path)
	}
	e, ok := m["enum"].([]any)
	if !ok {
		t.Fatalf("digEnum: %v has no enum", path)
	}
	return e
}
