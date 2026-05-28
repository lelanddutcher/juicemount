// SLICE 4 tests: crypto round-trip, wrong-key rejection, CRUD
// redaction, and v1→v2 state-schema upgrade.
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
	"time"
)

// TestCryptoRoundTrip verifies that encrypt then decrypt produces the
// original plaintext across multiple iterations (each with a fresh
// nonce). Also confirms nonce uniqueness — every ciphertext begins with
// the 12-byte nonce and no two should match.
func TestCryptoRoundTrip(t *testing.T) {
	key, err := deriveCredKey([]byte("test-admin-key-please-do-not-use-prod"))
	if err != nil {
		t.Fatalf("deriveCredKey: %v", err)
	}
	if len(key) != credKeyLen {
		t.Fatalf("key length %d, want %d", len(key), credKeyLen)
	}
	plaintext := []byte(`{"endpoint":"https://s3.example.com","access_key":"AKIA…","secret_key":"…"}`)

	seenNonces := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		blob, err := encryptSecret(plaintext, key)
		if err != nil {
			t.Fatalf("iter %d encrypt: %v", i, err)
		}
		// Wire format check: <12B nonce><ciphertext><16B tag>
		if len(blob) != gcmNonceLen+len(plaintext)+gcmTagLen {
			t.Errorf("iter %d wire length %d, want %d (nonce+plaintext+tag)",
				i, len(blob), gcmNonceLen+len(plaintext)+gcmTagLen)
		}
		nonce := string(blob[:gcmNonceLen])
		if _, dup := seenNonces[nonce]; dup {
			t.Errorf("iter %d: nonce reused — AES-GCM is unsafe under nonce reuse", i)
		}
		seenNonces[nonce] = struct{}{}

		recovered, err := decryptSecret(blob, key)
		if err != nil {
			t.Fatalf("iter %d decrypt: %v", i, err)
		}
		if !bytes.Equal(recovered, plaintext) {
			t.Errorf("iter %d: round-trip mismatch — got %q want %q", i, recovered, plaintext)
		}
	}
}

// TestCryptoWrongKey verifies decryption fails when the key differs,
// surfacing the standard AEAD verification error. Critical security
// property: a wrong key must not silently return garbage plaintext.
func TestCryptoWrongKey(t *testing.T) {
	keyA, _ := deriveCredKey([]byte("admin-key-one"))
	keyB, _ := deriveCredKey([]byte("admin-key-two"))
	if bytes.Equal(keyA, keyB) {
		t.Fatalf("HKDF derived identical keys from different inputs — broken")
	}
	blob, err := encryptSecret([]byte("secret"), keyA)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := decryptSecret(blob, keyB); err == nil {
		t.Fatalf("decrypt with wrong key succeeded — AEAD verification not enforced")
	}

	// Bit-flip in the tag also must fail (integrity check).
	tampered := append([]byte(nil), blob...)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := decryptSecret(tampered, keyA); err == nil {
		t.Fatalf("decrypt of tampered ciphertext succeeded — integrity not enforced")
	}

	// Bit-flip in the ciphertext body also must fail.
	tampered2 := append([]byte(nil), blob...)
	tampered2[gcmNonceLen] ^= 0x01
	if _, err := decryptSecret(tampered2, keyA); err == nil {
		t.Fatalf("decrypt of body-flipped ciphertext succeeded — integrity not enforced")
	}

	// Bit-flip in the NONCE bytes must also fail. GCM uses the nonce
	// to derive the keystream, so a flipped nonce produces a different
	// tag — but assert it explicitly so a future refactor (e.g.,
	// switching to a different AEAD) can't silently regress this.
	tamperedNonce := append([]byte(nil), blob...)
	tamperedNonce[0] ^= 0x01
	if _, err := decryptSecret(tamperedNonce, keyA); err == nil {
		t.Fatalf("decrypt of nonce-flipped ciphertext succeeded — AEAD nonce binding broken")
	}

	// Truncated blob (shorter than nonce+tag) must fail with a length
	// error, not a panic.
	if _, err := decryptSecret(blob[:5], keyA); err == nil {
		t.Fatalf("decrypt of truncated blob succeeded — length check missing")
	}
}

// TestDestinationsCRUDRedaction exercises the API: POST a plaintext
// destination, GET it back redacted, then confirm the persisted state
// file holds only base64 ciphertext (no plaintext credentials).
func TestDestinationsCRUDRedaction(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")

	mux := http.NewServeMux()
	mgr := Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    "the-admin-key-for-tests",
		StateFile:   statePath,
	})
	defer mgr.StopAll()

	// POST: create a destination with plaintext credentials.
	body := `{
		"name": "test-s3",
		"kind": "s3",
		"config": {
			"endpoint": "https://s3.example.com",
			"bucket": "my-bucket",
			"access_key": "AKIA-fake-key",
			"secret_key": "very-secret-value-NEVER-LEAK"
		}
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/destinations", strings.NewReader(body))
	req.Header.Set("X-JuiceMount-Admin-Key", "the-admin-key-for-tests")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("POST: got %d, want 201 — body=%s", w.Code, w.Body.String())
	}

	// Response body must NOT contain the plaintext secret.
	if strings.Contains(w.Body.String(), "very-secret-value-NEVER-LEAK") {
		t.Errorf("POST response leaked plaintext secret:\n%s", w.Body.String())
	}
	// And it must report the keys with the "<set>" placeholder. Go's
	// json encoder escapes angle brackets to < / > by default
	// (HTML-safe encoding); accept either form so the test stays
	// robust whether or not the manager later enables SetEscapeHTML(false).
	if !containsRedactionPlaceholder(w.Body.String()) {
		t.Errorf("POST response missing <set> redaction placeholder:\n%s", w.Body.String())
	}

	// GET: list returns the same destination, also redacted.
	req2 := httptest.NewRequest(http.MethodGet, "/api/destinations", nil)
	req2.Header.Set("X-JuiceMount-Admin-Key", "the-admin-key-for-tests")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("GET: got %d, want 200 — body=%s", w2.Code, w2.Body.String())
	}
	if strings.Contains(w2.Body.String(), "very-secret-value-NEVER-LEAK") {
		t.Errorf("GET list leaked plaintext secret:\n%s", w2.Body.String())
	}
	if !containsRedactionPlaceholder(w2.Body.String()) {
		t.Errorf("GET list missing <set> placeholder:\n%s", w2.Body.String())
	}

	// Inspect the on-disk state file — must contain ciphertext only.
	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	if strings.Contains(string(stateBytes), "very-secret-value-NEVER-LEAK") {
		t.Errorf("state file contains plaintext secret:\n%s", string(stateBytes))
	}
	if strings.Contains(string(stateBytes), "AKIA-fake-key") {
		t.Errorf("state file contains plaintext access key:\n%s", string(stateBytes))
	}
	// Verify schema_version got upgraded to v2 (we registered a fresh
	// state file, but the test still validates the writer always emits
	// the current schema number).
	var pst persistedState
	if err := json.Unmarshal(stateBytes, &pst); err != nil {
		t.Fatalf("state JSON: %v", err)
	}
	if pst.SchemaVersion != schemaVersion {
		t.Errorf("state schema_version = %d, want %d", pst.SchemaVersion, schemaVersion)
	}
	if len(pst.Destinations) != 1 {
		t.Fatalf("state destinations count = %d, want 1", len(pst.Destinations))
	}
	// The persisted encrypted_config must be valid base64 and decrypt
	// back to the original plaintext config.
	row := pst.Destinations[0]
	if row.EncryptedConfig == "" {
		t.Fatalf("encrypted_config is empty")
	}
	blob, err := base64.StdEncoding.DecodeString(row.EncryptedConfig)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	key, _ := deriveCredKey([]byte("the-admin-key-for-tests"))
	plain, err := decryptSecret(blob, key)
	if err != nil {
		t.Fatalf("decrypt persisted blob: %v", err)
	}
	var cfg map[string]string
	if err := json.Unmarshal(plain, &cfg); err != nil {
		t.Fatalf("plaintext config json: %v", err)
	}
	if cfg["secret_key"] != "very-secret-value-NEVER-LEAK" {
		t.Errorf("round-tripped secret_key = %q, want plain value", cfg["secret_key"])
	}

	// DELETE: idempotent removal.
	delReq := httptest.NewRequest(http.MethodDelete, "/api/destinations/test-s3", nil)
	delReq.Header.Set("X-JuiceMount-Admin-Key", "the-admin-key-for-tests")
	wDel := httptest.NewRecorder()
	mux.ServeHTTP(wDel, delReq)
	if wDel.Code != http.StatusNoContent {
		t.Errorf("DELETE: got %d, want 204", wDel.Code)
	}
	// Second DELETE should also succeed (idempotent).
	delReq2 := httptest.NewRequest(http.MethodDelete, "/api/destinations/test-s3", nil)
	delReq2.Header.Set("X-JuiceMount-Admin-Key", "the-admin-key-for-tests")
	wDel2 := httptest.NewRecorder()
	mux.ServeHTTP(wDel2, delReq2)
	if wDel2.Code != http.StatusNoContent {
		t.Errorf("second DELETE (idempotent): got %d, want 204", wDel2.Code)
	}
}

// TestStateSchemaV1Upgrade seeds a v1-shaped state file (no
// schema_version, no destinations array) and confirms it loads cleanly
// and gets upgraded to v2 on first save.
func TestStateSchemaV1Upgrade(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")

	// Write the v1 shape: ONLY jobs + order, no schema_version key.
	v1Bytes := []byte(`{
		"jobs": {
			"j-old": {
				"id": "j-old",
				"source": "/sources/foo",
				"destination": "/jfs/foo",
				"state": "done",
				"created_at": 1
			}
		},
		"order": ["j-old"]
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
		AdminKey:    "test-key",
		StateFile:   statePath,
	})
	// Swap in a no-op runner so the background job goroutine doesn't
	// try to exec /dev/null (which would otherwise race with TempDir
	// cleanup at test end and emit a flaky "directory not empty" on
	// some systems).
	mgr.SetRunner(runnerSucceedImmediately)
	defer mgr.StopAll()

	// Confirm v1 jobs loaded cleanly.
	if got := mgr.Get("j-old"); got == nil {
		t.Fatalf("v1 job j-old was not loaded")
	}

	// Trigger a save by performing any state mutation. Submit a fresh
	// job; SaveState fires inside Submit() via saveStateLocked.
	j, err := mgr.Submit("/sources/new", "/jfs/new", DefaultSyncOptions(), 0, DirectionIn)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	// Wait for the job to reach a terminal state so the final
	// saveStateLocked from m.run() has completed before we inspect
	// the file. Without this the file we read could be the
	// in-progress (Running) snapshot, not the final one.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := mgr.Get(j.ID).GetState()
		if s == JobDone || s == JobError || s == JobCanceled {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Re-read the file and confirm schema_version got bumped.
	out, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read upgraded state: %v", err)
	}
	var pst persistedState
	if err := json.Unmarshal(out, &pst); err != nil {
		t.Fatalf("parse upgraded state: %v", err)
	}
	if pst.SchemaVersion != schemaVersion {
		t.Errorf("after upgrade, schema_version = %d, want %d", pst.SchemaVersion, schemaVersion)
	}
	// The original v1 job MUST still be present after the upgrade.
	if _, ok := pst.Jobs["j-old"]; !ok {
		t.Errorf("v1 job j-old lost during upgrade")
	}
	// Destinations defaults to nil when no entries exist; the JSON
	// marshaler should omit the empty key thanks to omitempty.
	if strings.Contains(string(out), `"destinations":[]`) {
		t.Errorf("destinations:[] should be omitted on empty slice, got:\n%s", string(out))
	}
}

// TestDestinationsRejectNoAdminKey verifies that CRUD handlers return
// 503 when the manager was started without JM_ADMIN_KEY — we MUST NOT
// fall back to storing plaintext.
func TestDestinationsRejectNoAdminKey(t *testing.T) {
	mux := http.NewServeMux()
	_ = Register(mux, "", Config{
		JuiceFSBin:  "/dev/null",
		FUSEMount:   "/mnt/juicefs",
		SourceRoots: []string{"/sources"},
		DestMount:   "/jfs",
		AdminKey:    "", // intentionally empty
	})

	req := httptest.NewRequest(http.MethodPost, "/api/destinations", strings.NewReader(`{"name":"x","kind":"file","config":{"path":"/tmp"}}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("POST without JM_ADMIN_KEY: got %d, want 503", w.Code)
	}
}

// TestValidateDestinationName covers the URL-segment constraints.
func TestValidateDestinationName(t *testing.T) {
	cases := []struct {
		name    string
		wantErr bool
	}{
		{"valid", false},
		{"with-dash_and_underscore", false},
		{"with123numbers", false},
		{"", true},
		{"UPPER", true},
		{"has space", true},
		{"has.dot", true},
		{"has/slash", true},
		{strings.Repeat("a", maxDestinationNameLen+1), true},
	}
	for _, c := range cases {
		err := validateDestinationName(c.name)
		if (err != nil) != c.wantErr {
			t.Errorf("validateDestinationName(%q): err=%v wantErr=%v", c.name, err, c.wantErr)
		}
	}
}

// TestValidateConfig per-kind required-key check.
func TestValidateConfig(t *testing.T) {
	// s3 missing secret_key
	if err := validateConfig("s3", map[string]string{
		"endpoint": "x", "bucket": "y", "access_key": "z",
	}); err == nil {
		t.Errorf("s3 missing secret_key: expected error")
	}
	// sftp missing both password and private_key
	if err := validateConfig("sftp", map[string]string{
		"host": "h", "user": "u",
	}); err == nil {
		t.Errorf("sftp missing auth: expected error")
	}
	// sftp with password is ok
	if err := validateConfig("sftp", map[string]string{
		"host": "h", "user": "u", "password": "p",
	}); err != nil {
		t.Errorf("sftp with password: unexpected error %v", err)
	}
	// file requires path
	if err := validateConfig("file", map[string]string{}); err == nil {
		t.Errorf("file missing path: expected error")
	}
}

// TestToSyncURIKeepsCredsOffCommandLine verifies the
// Destination.ToSyncURI helper never returns credentials in the URI;
// they must all be in the env slice.
func TestToSyncURIKeepsCredsOffCommandLine(t *testing.T) {
	d := Destination{
		Name: "n", Kind: "s3", Config: map[string]string{
			"endpoint":   "https://s3.example.com",
			"bucket":     "b",
			"access_key": "VERY-SECRET-AK",
			"secret_key": "VERY-SECRET-SK",
		},
	}
	uri, env, err := d.ToSyncURI(true)
	if err != nil {
		t.Fatalf("ToSyncURI: %v", err)
	}
	if strings.Contains(uri, "VERY-SECRET-AK") || strings.Contains(uri, "VERY-SECRET-SK") {
		t.Errorf("URI leaks credentials: %q", uri)
	}
	if !hasEnv(env, "ACCESS_KEY=VERY-SECRET-AK") || !hasEnv(env, "SECRET_KEY=VERY-SECRET-SK") {
		t.Errorf("env slice missing credentials: %v", env)
	}

	// sftp: password must be in env, not URI.
	d2 := Destination{
		Name: "n", Kind: "sftp", Config: map[string]string{
			"host": "h.example.com", "user": "u",
			"password": "VERY-SECRET-PW",
		},
	}
	uri2, env2, err := d2.ToSyncURI(false)
	if err != nil {
		t.Fatalf("ToSyncURI sftp: %v", err)
	}
	if strings.Contains(uri2, "VERY-SECRET-PW") {
		t.Errorf("sftp URI leaks password: %q", uri2)
	}
	if !hasEnv(env2, "SFTP_PASSWORD=VERY-SECRET-PW") {
		t.Errorf("sftp env missing password: %v", env2)
	}
}

func hasEnv(env []string, want string) bool {
	for _, e := range env {
		if e == want {
			return true
		}
	}
	return false
}

// containsRedactionPlaceholder accepts either the literal "<set>" or
// the JSON-unicode-escaped form Go's stdlib json encoder emits by
// default (SetEscapeHTML on). Both encodings carry the same logical
// value — the test just asserts the redaction landed somewhere in the
// response body.
func containsRedactionPlaceholder(s string) bool {
	// Literal backslash-u sequence as Go's json encoder emits when
	// SetEscapeHTML is on (the default). Written via concatenation so
	// the source line itself doesn't get reinterpreted by Go's
	// double-quoted string escape rules.
	escaped := "\\u003cset\\u003e"
	return strings.Contains(s, "<set>") || strings.Contains(s, escaped)
}
