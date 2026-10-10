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

// The read counters must distinguish "a client REUSED a contributed derivative"
// from "a client asked and found nothing, so it will regenerate".
//
// WHY THIS MATTERS. The entire value of contributing derivatives back is that
// the next client reads instead of re-deriving. Before this, /derivatives and
// /blob had no instrumentation at all, so those two outcomes were
// indistinguishable from the server. A regression to always-regenerate would
// have been completely silent: it costs CPU and wall-clock, never correctness,
// so no test and no user report would ever surface it. This is the signal that
// multi-machine sync is actually working rather than merely not erroring.
func TestDerivReadCountersDistinguishReuseFromRegenerate(t *testing.T) {
	tmp := t.TempDir()
	const relPath = "REEL/a.mov"
	const inode = 4_100_001
	const emptyInode = 4_100_002

	src := filepath.Join(tmp, relPath)
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("v"), 2048), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700001000, 0)
	_ = os.Chtimes(src, mt, mt)

	ms, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	ds, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	ms.InsertToCache(&metadata.Entry{
		Path: relPath, Name: "a.mov", ParentPath: "REEL",
		Inode: inode, Size: 2048, Mtime: mt,
	})

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = ms, ds, tmp
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	}()

	read := func() map[string]any {
		rr := httptest.NewRecorder()
		handleDerivReadsHTTP(rr, httptest.NewRequest("GET", "/deriv-reads", nil))
		var m map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
			t.Fatalf("/deriv-reads is not JSON: %v", err)
		}
		return m
	}
	num := func(m map[string]any, k string) int64 {
		f, ok := m[k].(float64)
		if !ok {
			t.Fatalf("%s missing from /deriv-reads: %v", k, m)
		}
		return int64(f)
	}
	before := read()

	// NOTE: these go through the WRAPPED handlers, which is what the mux registers.
	// Calling the bare handler would bypass the counter entirely and prove nothing.
	// (1) A manifest query that finds NOTHING — the caller will regenerate.
	rr := httptest.NewRecorder()
	countingManifestHandler(handleDerivativesHTTP)(rr, httptest.NewRequest("GET", "/derivatives?inode=4100002", nil))
	afterEmpty := read()
	if num(afterEmpty, "manifest_empty") != num(before, "manifest_empty")+1 {
		t.Errorf("an empty manifest read was not counted as empty — that is the "+
			"'client will regenerate' case and it must be visible; before=%v after=%v",
			num(before, "manifest_empty"), num(afterEmpty, "manifest_empty"))
	}
	if num(afterEmpty, "manifest_with_row") != num(before, "manifest_with_row") {
		t.Error("an empty manifest read was counted as a hit")
	}

	// (2) Now a real contributed derivative, read back — the REUSE case.
	writeBlob(t, tmp, inode, "poster.jpg", []byte("JPEGDATA-contributed"))
	body, _ := json.Marshal(regBody(inode, "thumbnail", 2048, mt.Unix()))
	reg := httptest.NewRecorder()
	handleDerivativesRegisterHTTP(reg, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(body)))
	if reg.Code != 200 {
		t.Fatalf("setup register failed: %d %s", reg.Code, reg.Body.String())
	}

	mid := read()
	hr := httptest.NewRecorder()
	countingManifestHandler(handleDerivativesHTTP)(hr, httptest.NewRequest("GET", "/derivatives?inode=4100001", nil))
	afterHit := read()
	if num(afterHit, "manifest_with_row") != num(mid, "manifest_with_row")+1 {
		t.Error("a manifest read that RETURNED a row was not counted as a hit")
	}

	br := httptest.NewRecorder()
	countingBlobHandler(handleBlobHTTP)(br, httptest.NewRequest("GET", "/blob?inode=4100001&kind=thumbnail", nil))
	if br.Code != 200 {
		t.Fatalf("blob serve failed: %d %s", br.Code, br.Body.String())
	}
	afterBlob := read()
	if got := num(afterBlob, "blob_served") - num(afterHit, "blob_served"); got != 1 {
		t.Errorf("blob_served advanced by %d, want 1 — without this the server cannot tell "+
			"reuse from regeneration, which is the whole question multi-machine sync turns on", got)
	}
	if got := num(afterBlob, "blob_bytes") - num(afterHit, "blob_bytes"); got != int64(len("JPEGDATA-contributed")) {
		t.Errorf("blob_bytes advanced by %d, want %d", got, len("JPEGDATA-contributed"))
	}
	byKind, _ := afterBlob["blob_by_kind"].(map[string]any)
	if byKind == nil || byKind["thumbnail"] == nil {
		t.Errorf("blob_by_kind lost the kind breakdown: %v", afterBlob["blob_by_kind"])
	}

	// (3) A blob MISS must be counted separately — asked for, not available.
	mr := httptest.NewRecorder()
	countingBlobHandler(handleBlobHTTP)(mr, httptest.NewRequest("GET", "/blob?inode=4100001&kind=filmstrip", nil))
	afterMiss := read()
	if num(afterMiss, "blob_missing") != num(afterBlob, "blob_missing")+1 {
		t.Error("a blob MISS was not counted — 'asked and not there' is the signal that a " +
			"kind is worth contributing, and it must not be silent")
	}
	if num(afterMiss, "blob_served") != num(afterBlob, "blob_served") {
		t.Error("a blob miss was counted as a serve")
	}
}
