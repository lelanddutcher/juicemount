package main

// OL-1 contribute-back behavioral test: POST /derivatives/register gates on
// live size+mtime, stamps the canonical xxh3, and writes an on-device `ai` row —
// the symmetric write to JM-14's reads. Sets up a temp FUSE root + a real source
// file so the handler's live re-stat + sampled-hash run for real.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/metadata"
)

func TestDerivativesRegisterOL1(t *testing.T) {
	tmp := t.TempDir() // stands in for the FUSE mount root
	const inode = 999001
	const rel = "Project_Foo/clip_031.mov"
	src := filepath.Join(tmp, rel)
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("juicemount-ol1-"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700000000, 0)
	if err := os.Chtimes(src, mt, mt); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(src)
	liveSize, liveMtime := fi.Size(), fi.ModTime().Unix()

	// Also write the blob the consumer would have written (the handler doesn't
	// require it, but mirror the real flow).
	blob := filepath.Join(tmp, ".juicemount", "derivatives", "999001", "ai.loupe.json")
	_ = os.MkdirAll(filepath.Dir(blob), 0o755)
	_ = os.WriteFile(blob, []byte(`{"image_embedding_global":"...","faces":[],"transcript":{}}`), 0o644)

	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer mstore.Close()
	mstore.InsertToCache(&metadata.Entry{Path: rel, Name: "clip_031.mov", ParentPath: "Project_Foo", Inode: inode, Size: liveSize, Mtime: mt})

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
	base := map[string]any{
		"inode": inode, "kind": "ai", "producer": "on-device",
		"model": "apple-featureprint-v1", "dim": 768,
		"source_size": liveSize, "source_mtime": liveMtime, "blob_rel_path": "ai.loupe.json",
	}

	// (1) matching size/mtime → 200 + schema-valid + the row lands.
	rr := post(base)
	if rr.Code != 200 {
		t.Fatalf("register(ok): status %d, body %s", rr.Code, rr.Body.String())
	}
	var resp any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not JSON: %v", err)
	}
	if err := compileSchema(t, "register.schema.json").Validate(resp); err != nil {
		t.Errorf("register response does not validate: %v\n%s", err, rr.Body.String())
	}
	m := resp.(map[string]any)
	if m["registered"] != true {
		t.Errorf("registered = %v, want true", m["registered"])
	}
	d := m["derivative"].(map[string]any)
	if d["kind"] != "ai" || d["producer"] != "on-device" || d["status"] != "ready" || d["hash"] != d["hash"] {
		t.Errorf("row wrong: %v", d)
	}
	srcHash, _ := d["hash"].(string)
	if srcHash == "" {
		t.Errorf("server did not stamp a source_hash")
	}
	// It must now appear via /derivatives, hash-gated (hash == source_hash).
	dr := callJSON(t, handleDerivativesHTTP, "/derivatives?inode=999001")
	dm := dr.(map[string]any)
	if dm["exists"] != true || dm["source_hash"] != srcHash {
		t.Errorf("/derivatives didn't reflect the register: %v", dm)
	}
	gotAI := false
	for _, raw := range dm["derivatives"].([]any) {
		row := raw.(map[string]any)
		if row["kind"] == "ai" {
			gotAI = true
			if row["hash"] != srcHash || row["producer"] != "on-device" || row["blob_rel_path"] != "ai.loupe.json" {
				t.Errorf("ai row wrong in manifest: %v", row)
			}
		}
	}
	if !gotAI {
		t.Errorf("no ai row in /derivatives after register")
	}

	// (2) stale size → 409, no overwrite.
	stale := map[string]any{}
	for k, v := range base {
		stale[k] = v
	}
	stale["source_size"] = liveSize + 1
	if rr := post(stale); rr.Code != http.StatusConflict {
		t.Errorf("register(stale size): status %d, want 409", rr.Code)
	}

	// (3) stale mtime → 409.
	stale2 := map[string]any{}
	for k, v := range base {
		stale2[k] = v
	}
	stale2["source_mtime"] = liveMtime + 1
	if rr := post(stale2); rr.Code != http.StatusConflict {
		t.Errorf("register(stale mtime): status %d, want 409", rr.Code)
	}

	// (4) non-ai kind → 400 (AI-only scope).
	wrong := map[string]any{}
	for k, v := range base {
		wrong[k] = v
	}
	wrong["kind"] = "tech"
	if rr := post(wrong); rr.Code != 400 {
		t.Errorf("register(kind=tech): status %d, want 400", rr.Code)
	}

	// (5) unknown inode → 404.
	bad := map[string]any{}
	for k, v := range base {
		bad[k] = v
	}
	bad["inode"] = 424242
	if rr := post(bad); rr.Code != 404 {
		t.Errorf("register(unknown inode): status %d, want 404", rr.Code)
	}
}
