package main

// End-to-end producer→handler integration test (JM-14 + JM-16).
//
// Proves the WHOLE chain on real binaries: ffmpeg synthesizes a tiny clip →
// internal/farm.Process derives tech + a poster + content hash and writes them
// through the real derivatives.Store → the real /metadata + /derivatives HTTP
// handlers serve them → the responses validate against the vendored contract
// schemas. The unit tests cover the mapping in isolation; this catches wiring
// regressions the unit tests can't (store schema, handler globals, the
// exists/hash/blob_rel_path contract through the actual JSON).
//
// Skips cleanly where ffmpeg/ffprobe aren't installed (e.g. minimal CI).

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
)

func TestProducerEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not on PATH — skipping producer e2e")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not on PATH — skipping producer e2e")
	}

	tmp := t.TempDir()
	clip := filepath.Join(tmp, "clip.mov")
	// 1s of testsrc video + a sine tone, HEVC 10-bit so the mapping has real
	// color/bit-depth to extract (mirrors the BMD proxy shape).
	gen := exec.Command("ffmpeg", "-y", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=duration=1:size=320x240:rate=30",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=1",
		"-c:v", "libx265", "-pix_fmt", "yuv420p10le", "-x265-params", "log-level=none",
		"-c:a", "aac", "-shortest", clip)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Skipf("could not synthesize test clip (codec unavailable?): %v: %s", err, out)
	}

	// Produce into an isolated on-disk store (WAL needs a real file).
	dbPath := filepath.Join(tmp, "derivatives.db")
	store, err := derivatives.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	res := farm.Process(store, clip, farm.Options{
		Producer: "macos-node", Version: 1, Mount: tmp, Blobs: true, ThumbMaxDim: 320,
	})
	if res.Err != nil {
		t.Fatalf("farm.Process: %v", res.Err)
	}
	if res.Inode == 0 || res.Hash == "" || !res.HasVideo {
		t.Fatalf("unexpected result: %+v", res)
	}

	// The inode the handler is queried with == the file's stat inode == what
	// the producer keyed on. (Mirrors stat == /lookup inode on the real mount.)
	fi, _ := os.Stat(clip)
	wantInode := uint64(fi.Sys().(*syscall.Stat_t).Ino)
	if res.Inode != wantInode {
		t.Fatalf("producer inode %d != stat inode %d", res.Inode, wantInode)
	}

	// The poster blob landed at the Tier-A location.
	poster := filepath.Join(farm.DerivBlobDir(tmp, wantInode), "poster.jpg")
	if st, err := os.Stat(poster); err != nil || st.Size() == 0 {
		t.Fatalf("poster blob missing/empty at %s: err=%v", poster, err)
	}

	// Wire the handler globals to this store, then boot the REAL handlers.
	globalMu.Lock()
	oldStore := globalDerivStore
	globalDerivStore = store
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalDerivStore = oldStore
		globalMu.Unlock()
	}()

	inodeStr := strconv.FormatUint(wantInode, 10)

	// --- /metadata?inode&kind=tech ---
	metaResp := callJSON(t, handleMetadataHTTP, "/metadata?"+url.Values{
		"inode": {inodeStr}, "kind": {"tech"},
	}.Encode())
	if err := compileSchema(t, "metadata.schema.json").Validate(metaResp); err != nil {
		t.Errorf("/metadata does not validate: %v\n%v", err, metaResp)
	}
	m := metaResp.(map[string]any)
	if m["exists"] != true {
		t.Errorf("/metadata exists = %v, want true", m["exists"])
	}
	tech, ok := m["tech"].(map[string]any)
	if !ok || tech["container"] == nil || tech["video"] == nil {
		t.Errorf("/metadata tech missing/empty: %v", m["tech"])
	}

	// --- /derivatives?inode ---
	derResp := callJSON(t, handleDerivativesHTTP, "/derivatives?"+url.Values{"inode": {inodeStr}}.Encode())
	if err := compileSchema(t, "derivatives.schema.json").Validate(derResp); err != nil {
		t.Errorf("/derivatives does not validate: %v\n%v", err, derResp)
	}
	d := derResp.(map[string]any)
	if d["exists"] != true {
		t.Errorf("/derivatives exists = %v, want true", d["exists"])
	}
	srcHash, _ := d["source_hash"].(string)
	if srcHash != res.Hash {
		t.Errorf("source_hash %q != produced hash %q", srcHash, res.Hash)
	}
	// Every derivative's hash must equal source_hash (the consumer's fail-closed
	// gate), and we must have produced both a tech and a thumbnail row.
	kinds := map[string]bool{}
	for _, raw := range d["derivatives"].([]any) {
		row := raw.(map[string]any)
		kinds[row["kind"].(string)] = true
		if h, _ := row["hash"].(string); h != srcHash {
			t.Errorf("derivative %v hash %q != source_hash %q", row["kind"], h, srcHash)
		}
	}
	if !kinds["tech"] || !kinds["thumbnail"] {
		t.Errorf("expected tech+thumbnail rows, got %v", kinds)
	}
	// The thumbnail row's blob_rel_path must point at the file we found.
	for _, raw := range d["derivatives"].([]any) {
		row := raw.(map[string]any)
		if row["kind"] == "thumbnail" {
			if row["blob_rel_path"] != "poster.jpg" || row["media_type"] != "image/jpeg" {
				t.Errorf("thumbnail row blob/media wrong: %v", row)
			}
		}
	}
}

// callJSON invokes a handler with a GET to rawQuery and returns the decoded body.
func callJSON(t *testing.T, h http.HandlerFunc, target string) any {
	t.Helper()
	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest("GET", target, nil))
	if rr.Code != 200 {
		t.Fatalf("%s → %d: %s", target, rr.Code, rr.Body.String())
	}
	var v any
	if err := json.Unmarshal(rr.Body.Bytes(), &v); err != nil {
		t.Fatalf("%s → bad JSON: %v\n%s", target, err, rr.Body.String())
	}
	return v
}
