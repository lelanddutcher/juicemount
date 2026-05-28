// SLICE 8 tests: admin-key rotation (verify-before-rewrite),
// settings CRUD round-trip, and v1 → settings-absent compat.
package manager

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRotateAdminKeySuccess verifies the happy path: 3 destinations are
// created under the OLD admin key, rotation succeeds with the right
// old_key, and every persisted blob decrypts under the NEW key. The
// in-memory store still holds the OLD key (operator must restart with
// the new env var); we assert that explicitly to prevent a future
// refactor from silently swapping the in-memory key.
func TestRotateAdminKeySuccess(t *testing.T) {
	const oldKey = "old-admin-key-for-rotation-tests"
	const newKey = "new-admin-key-after-rotation"

	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")

	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    oldKey,
		StateFile:   statePath,
	})
	defer mgr.StopAll()

	// Seed 3 destinations under the OLD key. The bodies carry distinct
	// secret values so the post-rotation decrypt check can assert each
	// row round-trips to its own original plaintext (not a single shared
	// constant that a buggy implementation could match by accident).
	bodies := []string{
		`{"name":"dest-a","kind":"s3","config":{"endpoint":"https://s3.example.com","bucket":"a","access_key":"AK-A","secret_key":"SECRET-A"}}`,
		`{"name":"dest-b","kind":"s3","config":{"endpoint":"https://s3.example.com","bucket":"b","access_key":"AK-B","secret_key":"SECRET-B"}}`,
		`{"name":"dest-c","kind":"file","config":{"path":"/tmp/dest-c"}}`,
	}
	for i, body := range bodies {
		req := httptest.NewRequest(http.MethodPost, "/api/destinations", strings.NewReader(body))
		req.Header.Set("X-JuiceMount-Admin-Key", oldKey)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("seed dest %d: got %d, want 201 — body=%s", i, w.Code, w.Body.String())
		}
	}

	// Rotate the admin key. Both fields supplied, non-empty.
	rotateBody := `{"old_key":"` + oldKey + `","new_key":"` + newKey + `"}`
	rotateReq := httptest.NewRequest(http.MethodPost, "/api/settings/rotate-admin-key", strings.NewReader(rotateBody))
	rotateReq.Header.Set("X-JuiceMount-Admin-Key", oldKey)
	rotateW := httptest.NewRecorder()
	mux.ServeHTTP(rotateW, rotateReq)
	if rotateW.Code != http.StatusOK {
		t.Fatalf("rotate: got %d, want 200 — body=%s", rotateW.Code, rotateW.Body.String())
	}
	var rotResp rotateAdminKeyResponse
	if err := json.Unmarshal(rotateW.Body.Bytes(), &rotResp); err != nil {
		t.Fatalf("rotate response unmarshal: %v", err)
	}
	if !rotResp.OK || rotResp.ReEncryptedRows != 3 {
		t.Errorf("rotate response: ok=%v rows=%d, want ok=true rows=3", rotResp.OK, rotResp.ReEncryptedRows)
	}
	if rotResp.Message == "" || !strings.Contains(strings.ToLower(rotResp.Message), "restart") {
		t.Errorf("rotate response message must instruct operator to restart, got: %q", rotResp.Message)
	}

	// Read the on-disk state file and verify every blob decrypts under
	// the NEW key. This is the load-bearing assertion — the file is the
	// source of truth post-rotation.
	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	var pst persistedState
	if err := json.Unmarshal(stateBytes, &pst); err != nil {
		t.Fatalf("parse state file: %v", err)
	}
	if len(pst.Destinations) != 3 {
		t.Fatalf("expected 3 destinations on disk, got %d", len(pst.Destinations))
	}
	newCredKey, err := deriveCredKey([]byte(newKey))
	if err != nil {
		t.Fatalf("derive new cred-key: %v", err)
	}
	wantSecrets := map[string]string{
		"dest-a": "SECRET-A",
		"dest-b": "SECRET-B",
		// dest-c is a file destination — has no secret_key, but it
		// still must decrypt to a valid config map.
	}
	for _, row := range pst.Destinations {
		blob, err := base64.StdEncoding.DecodeString(row.EncryptedConfig)
		if err != nil {
			t.Fatalf("row %q base64 decode: %v", row.Name, err)
		}
		plain, err := decryptSecret(blob, newCredKey)
		if err != nil {
			t.Fatalf("row %q decrypt under NEW key: %v (rotation re-encrypt failed)", row.Name, err)
		}
		var cfg map[string]string
		if err := json.Unmarshal(plain, &cfg); err != nil {
			t.Fatalf("row %q config unmarshal: %v", row.Name, err)
		}
		if want, ok := wantSecrets[row.Name]; ok {
			if cfg["secret_key"] != want {
				t.Errorf("row %q secret_key = %q, want %q", row.Name, cfg["secret_key"], want)
			}
		}
	}

	// Also confirm the state file no longer carries any plaintext from
	// the seed bodies — if rotation accidentally wrote plaintext, this
	// catches it before deploy.
	stateStr := string(stateBytes)
	for _, leak := range []string{"SECRET-A", "SECRET-B", "AK-A", "AK-B"} {
		if strings.Contains(stateStr, leak) {
			t.Errorf("state file leaks plaintext %q post-rotation", leak)
		}
	}

	// The in-memory cred-key MUST stay on the OLD value per the
	// rotation contract (operator restart picks up the new env). We
	// verify this via a separate unit test against the store directly
	// (TestRotateAdminKeyKeepsInMemoryKey) because there's no HTTP
	// surface that exposes the in-memory key, and walking the mux
	// back to the API instance would require widening Register's
	// return signature.
}

// TestRotateAdminKeyKeepsInMemoryKey is the direct-against-store
// counterpart to TestRotateAdminKeySuccess. It verifies the load-bearing
// "in-memory cred-key stays on OLD value after success" invariant — the
// HTTP-level test above can't observe store.key directly without
// widening Register's signature, so we exercise the store API directly.
func TestRotateAdminKeyKeepsInMemoryKey(t *testing.T) {
	const oldKey = "old-key-for-direct-store-test"
	const newKey = "new-key-for-direct-store-test"
	store, err := newDestinationStore(oldKey)
	if err != nil {
		t.Fatalf("newDestinationStore: %v", err)
	}
	// Seed a destination so rotation takes the AEAD-decrypt verify path.
	d := Destination{Name: "x", Kind: "s3", Config: map[string]string{
		"endpoint": "https://s3.example.com", "bucket": "b",
		"access_key": "ak", "secret_key": "sk",
	}}
	if err := store.upsert(d, false); err != nil {
		t.Fatalf("seed upsert: %v", err)
	}
	if _, err := rotateAdminKeyOnStore(store, []byte(oldKey), []byte(newKey)); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	// Direct field access — same package, so we can.
	wantOldCredKey, _ := deriveCredKey([]byte(oldKey))
	store.mu.RLock()
	gotKey := append([]byte(nil), store.key...)
	store.mu.RUnlock()
	if !bytes.Equal(wantOldCredKey, gotKey) {
		t.Errorf("in-memory store key changed after rotation — restart-contract violated")
	}
	// And re-deriving the NEW key should NOT match store.key, so a future
	// regression that swaps the key gets caught immediately.
	newCredKey, _ := deriveCredKey([]byte(newKey))
	if bytes.Equal(newCredKey, gotKey) {
		t.Errorf("in-memory store key equals NEW derived key — store swapped keys, violating restart contract")
	}
}

// TestRotateAdminKeyWrongOldKey is the load-bearing security test for
// this slice. A wrong old_key MUST result in 401 AND the state file on
// disk must be byte-identical before and after the failed call. If the
// implementation ever decrypts-then-rewrites speculatively, this test
// fires.
func TestRotateAdminKeyWrongOldKey(t *testing.T) {
	const realKey = "the-real-admin-key"
	const wrongKey = "definitely-not-the-real-key"
	const newKey = "would-be-new-key"

	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")

	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    realKey,
		StateFile:   statePath,
	})
	defer mgr.StopAll()

	// Seed one destination so the verify path takes Path A (decrypt the
	// first row's blob). Without a destination present the verify path
	// would compare derived keys instead — both branches need their own
	// 401 coverage; this test exercises the destination-present path.
	seed := `{"name":"only","kind":"s3","config":{"endpoint":"https://s3.example.com","bucket":"only","access_key":"ak","secret_key":"sk"}}`
	seedReq := httptest.NewRequest(http.MethodPost, "/api/destinations", strings.NewReader(seed))
	seedReq.Header.Set("X-JuiceMount-Admin-Key", realKey)
	seedW := httptest.NewRecorder()
	mux.ServeHTTP(seedW, seedReq)
	if seedW.Code != http.StatusCreated {
		t.Fatalf("seed: got %d, want 201 — body=%s", seedW.Code, seedW.Body.String())
	}

	// Snapshot the state file bytes BEFORE the failed rotation attempt.
	// The verify-before-rewrite contract requires these to match the
	// post-call bytes exactly.
	before, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state before: %v", err)
	}

	// Attempt rotation with the WRONG old key.
	body := `{"old_key":"` + wrongKey + `","new_key":"` + newKey + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/settings/rotate-admin-key", strings.NewReader(body))
	req.Header.Set("X-JuiceMount-Admin-Key", realKey)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong-old-key rotation: got %d, want 401 — body=%s", w.Code, w.Body.String())
	}

	// Critical: state file MUST be byte-identical. Any divergence means
	// the implementation touched persistence before verifying — which
	// would be catastrophic in production (a typo'd old_key during
	// rotation could partial-rewrite the state and brick destinations).
	after, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("state file modified after failed rotation (verify-before-rewrite contract broken)")
		t.Logf("before (%d bytes): %s", len(before), string(before))
		t.Logf(" after (%d bytes): %s", len(after), string(after))
	}

	// And the seeded destination must still decrypt under the ORIGINAL
	// admin key (i.e., the wrong-old-key call didn't corrupt the row).
	wantKey, _ := deriveCredKey([]byte(realKey))
	var pst persistedState
	if err := json.Unmarshal(after, &pst); err != nil {
		t.Fatalf("parse state after: %v", err)
	}
	if len(pst.Destinations) != 1 {
		t.Fatalf("destinations slice corrupted: got %d, want 1", len(pst.Destinations))
	}
	blob, err := base64.StdEncoding.DecodeString(pst.Destinations[0].EncryptedConfig)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	if _, err := decryptSecret(blob, wantKey); err != nil {
		t.Fatalf("destination unreadable under original key after failed rotation: %v", err)
	}
}

// TestRotateAdminKeyWrongOldKeyNoDestinations covers Path B: when no
// destinations are stored, verification falls back to constant-time
// comparing HKDF(old_key) to the in-memory derived key. The wrong-key
// branch of that compare still MUST 401 + leave the state file
// byte-identical. Without this test, a regression that always returns
// success in Path B would slip through.
func TestRotateAdminKeyWrongOldKeyNoDestinations(t *testing.T) {
	const realKey = "the-real-admin-key"
	const wrongKey = "definitely-not-the-real-key"
	const newKey = "would-be-new-key"

	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")

	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    realKey,
		StateFile:   statePath,
	})
	defer mgr.StopAll()

	// NO destinations seeded — Path B (subtle.ConstantTimeCompare against
	// store.key) is the verification branch.

	// State file may not exist yet (no writes happened); ensure a
	// baseline by GET'ing settings (forces a save on first read? no —
	// only mutations save). Touch by PUT'ing settings to materialise.
	put := `{"theme":"dark","log_retention_lines":500,"destinations_redacted":true,"job_defaults":{}}`
	putReq := httptest.NewRequest(http.MethodPut, "/api/settings", strings.NewReader(put))
	putReq.Header.Set("X-JuiceMount-Admin-Key", realKey)
	putW := httptest.NewRecorder()
	mux.ServeHTTP(putW, putReq)
	if putW.Code != http.StatusOK {
		t.Fatalf("seed settings: got %d, want 200", putW.Code)
	}

	before, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state before: %v", err)
	}

	body := `{"old_key":"` + wrongKey + `","new_key":"` + newKey + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/settings/rotate-admin-key", strings.NewReader(body))
	req.Header.Set("X-JuiceMount-Admin-Key", realKey)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("Path B wrong-old-key: got %d, want 401 — body=%s", w.Code, w.Body.String())
	}

	after, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("Path B: state file modified after failed rotation (verify-before-rewrite contract broken on no-destinations path)")
		t.Logf("before (%d bytes): %s", len(before), string(before))
		t.Logf(" after (%d bytes): %s", len(after), string(after))
	}
}

// TestSettingsCRUD round-trips Settings: PUT a full payload, GET it back,
// confirm every field matches. Also exercises the bounds-clamp on
// LogRetentionLines (out-of-range values get pulled back into the
// allowed [100, 10000] window) and the theme allow-list.
func TestSettingsCRUD(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")

	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    "key-for-settings-test",
		StateFile:   statePath,
	})
	defer mgr.StopAll()

	// GET before any PUT returns defaults — theme=system, log retention
	// = defaultLogRetention, job defaults match DefaultSyncOptions().
	getReq := httptest.NewRequest(http.MethodGet, "/api/settings", nil)
	getReq.Header.Set("X-JuiceMount-Admin-Key", "key-for-settings-test")
	getW := httptest.NewRecorder()
	mux.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("initial GET: got %d, want 200 — body=%s", getW.Code, getW.Body.String())
	}
	var initial Settings
	if err := json.Unmarshal(getW.Body.Bytes(), &initial); err != nil {
		t.Fatalf("initial GET unmarshal: %v", err)
	}
	if initial.Theme != themeSystem {
		t.Errorf("initial theme = %q, want %q", initial.Theme, themeSystem)
	}
	if initial.LogRetentionLines != defaultLogRetention {
		t.Errorf("initial log retention = %d, want %d", initial.LogRetentionLines, defaultLogRetention)
	}
	if initial.JobDefaults.Threads != DefaultSyncOptions().Threads {
		t.Errorf("initial job defaults differ from DefaultSyncOptions")
	}

	// PUT a fully-populated Settings. Note: LogRetentionLines=50000 is
	// out of range — must clamp to maxLogRetention (10000). BWLimit and
	// Threads are non-default so the round-trip catches a sloppy decoder
	// that drops the JobDefaults sub-struct.
	customDefaults := DefaultSyncOptions()
	customDefaults.BWLimit = 25
	customDefaults.Threads = 4
	customDefaults.DryRun = true
	putBody, _ := json.Marshal(Settings{
		JobDefaults:          customDefaults,
		Theme:                themeDark,
		LogRetentionLines:    50000, // out-of-range
		DestinationsRedacted: false,
	})
	putReq := httptest.NewRequest(http.MethodPut, "/api/settings", bytes.NewReader(putBody))
	putReq.Header.Set("X-JuiceMount-Admin-Key", "key-for-settings-test")
	putW := httptest.NewRecorder()
	mux.ServeHTTP(putW, putReq)
	if putW.Code != http.StatusOK {
		t.Fatalf("PUT: got %d, want 200 — body=%s", putW.Code, putW.Body.String())
	}
	var afterPut Settings
	if err := json.Unmarshal(putW.Body.Bytes(), &afterPut); err != nil {
		t.Fatalf("PUT response unmarshal: %v", err)
	}
	if afterPut.LogRetentionLines != maxLogRetention {
		t.Errorf("PUT clamped retention = %d, want %d (max)", afterPut.LogRetentionLines, maxLogRetention)
	}
	if afterPut.Theme != themeDark {
		t.Errorf("PUT theme = %q, want %q", afterPut.Theme, themeDark)
	}
	if afterPut.JobDefaults.BWLimit != 25 || afterPut.JobDefaults.Threads != 4 || !afterPut.JobDefaults.DryRun {
		t.Errorf("PUT JobDefaults round-trip lost data: %+v", afterPut.JobDefaults)
	}
	if afterPut.DestinationsRedacted != false {
		t.Errorf("PUT DestinationsRedacted = %v, want false", afterPut.DestinationsRedacted)
	}

	// GET again — the persisted row should match the PUT response.
	getReq2 := httptest.NewRequest(http.MethodGet, "/api/settings", nil)
	getReq2.Header.Set("X-JuiceMount-Admin-Key", "key-for-settings-test")
	getW2 := httptest.NewRecorder()
	mux.ServeHTTP(getW2, getReq2)
	if getW2.Code != http.StatusOK {
		t.Fatalf("post-PUT GET: got %d, want 200", getW2.Code)
	}
	var afterGet Settings
	if err := json.Unmarshal(getW2.Body.Bytes(), &afterGet); err != nil {
		t.Fatalf("post-PUT GET unmarshal: %v", err)
	}
	// SyncOptions contains slices (Excludes/Includes) so the parent
	// Settings struct isn't directly == comparable. Compare via JSON
	// marshal — semantic equality, not pointer equality.
	if !settingsEqual(afterGet, afterPut) {
		t.Errorf("GET after PUT diverges from PUT response:\n got  %+v\n want %+v", afterGet, afterPut)
	}

	// Invalid theme must 400.
	badBody := `{"theme":"hot-pink","log_retention_lines":1000}`
	badReq := httptest.NewRequest(http.MethodPut, "/api/settings", strings.NewReader(badBody))
	badReq.Header.Set("X-JuiceMount-Admin-Key", "key-for-settings-test")
	badW := httptest.NewRecorder()
	mux.ServeHTTP(badW, badReq)
	if badW.Code != http.StatusBadRequest {
		t.Errorf("invalid theme: got %d, want 400", badW.Code)
	}

	// Persistence: re-read the state file directly to confirm settings
	// landed on disk (not just in memory). The custom Threads=4 acts as
	// a fingerprint — defaults would have 10.
	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	var pst persistedState
	if err := json.Unmarshal(stateBytes, &pst); err != nil {
		t.Fatalf("parse state file: %v", err)
	}
	if pst.Settings == nil {
		t.Fatalf("settings missing from state file after PUT")
	}
	if pst.Settings.JobDefaults.Threads != 4 || pst.Settings.Theme != themeDark {
		t.Errorf("persisted settings divergent: %+v", *pst.Settings)
	}
}

// TestSettingsV1Compat seeds a v1 state file with no settings key, boots
// the manager, and asserts GET /api/settings returns the code defaults
// (not a zero-valued row). This exercises the persistedState.Settings
// pointer + omitempty contract end-to-end — without the pointer, the
// decoder would materialize a zero settingsState and the GET would
// surface theme="" and retention=0, both wrong.
func TestSettingsV1Compat(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")

	// v1 shape: jobs + order only, NO settings key, NO schema_version.
	v1Bytes := []byte(`{
		"jobs": {},
		"order": []
	}`)
	if err := os.WriteFile(statePath, v1Bytes, 0o600); err != nil {
		t.Fatalf("seed v1 state: %v", err)
	}

	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    "v1-compat-key",
		StateFile:   statePath,
	})
	defer mgr.StopAll()

	req := httptest.NewRequest(http.MethodGet, "/api/settings", nil)
	req.Header.Set("X-JuiceMount-Admin-Key", "v1-compat-key")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET: got %d, want 200 — body=%s", w.Code, w.Body.String())
	}
	var got Settings
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("GET unmarshal: %v", err)
	}
	want := defaultSettings()
	if !settingsEqual(got, want) {
		t.Errorf("v1-file GET returned %+v, want defaults %+v", got, want)
	}
}

// settingsEqual compares two Settings via JSON marshal — SyncOptions
// contains slices so the parent struct isn't directly == comparable.
// Equal output strings mean semantically-equal values for the on-disk /
// API-boundary use cases this test covers.
func settingsEqual(a, b Settings) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ab, bb)
}

