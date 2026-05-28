// Package manager — SLICE 8 Settings: per-job defaults, admin-key
// rotation, log retention knob, theme.
//
// This file owns the /api/settings handlers (GET, PUT) and the
// /api/settings/rotate-admin-key flow. The Settings struct is the
// API-boundary form; settingsState (state.go) is the on-disk shape.
// Storage lifecycle mirrors destinationStore / scheduleStore (jobs.go):
// a settingsStore interface keeps JobManager free of a concrete type,
// and the persistence callback flows mutations through SaveState.
//
// Admin-key rotation is the high-risk feature in this slice. Discipline:
//
//   1. The CURRENT key (old_key) is verified BEFORE any state-mutating
//      work happens. If there are saved destinations, verification means
//      "decrypt at least one destination's encrypted_config under the
//      old key". If there are no destinations, we fall back to deriving
//      the cred-key from old_key via HKDF and constant-time-comparing it
//      to the in-memory cred-key the store derived at startup.
//
//   2. On verify failure: the handler returns 401 WITHOUT touching the
//      destinations store, WITHOUT writing the state file, and WITHOUT
//      altering the in-memory cred-key. The state file on disk before
//      and after a failed rotation MUST be byte-identical.
//
//   3. On verify success: every destination's blob is decrypted under
//      the old key and re-encrypted under the new key in a single pass
//      over the rows slice (the writeLock-held swap). After the swap,
//      onChange fires OUTSIDE the lock so saveState() can flush the new
//      ciphertext to disk atomically (state.go's writer is rename-based).
//
//   4. The in-memory cred-key STAYS on the OLD value after success.
//      Intentional: the operator now needs to update JM_ADMIN_KEY on
//      the container and restart. Leaving the store on the old key
//      keeps subsequent reads-before-restart consistent — every blob on
//      disk is fresh-new-key-encrypted, but the live process still has
//      the old key, so a stray destination GET would fail to decrypt.
//      The success response surfaces this contract clearly in its
//      message field.
package manager

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
)

// Theme constants — locked at the API boundary so the UI dropdown stays
// in lockstep with the backend's allowed set. "system" follows the OS
// preference (default); "dark" / "light" are explicit overrides.
const (
	themeSystem = "system"
	themeDark   = "dark"
	themeLight  = "light"
)

// Log-retention bounds. The Settings tab surfaces this as a numeric
// input; values outside the range are clamped server-side to keep the
// state file from holding obviously-wrong values. 1000 default matches
// SLICE-5 + SLICE-3 history caps so a fresh install starts with
// consistent retention across stores.
const (
	minLogRetention     = 100
	maxLogRetention     = 10000
	defaultLogRetention = 1000
)

// Settings is the API-boundary form of a settingsState row. JSON-identical;
// kept as a separate type so future fields can diverge between the wire
// shape and the on-disk shape without breaking persisted state files.
type Settings struct {
	JobDefaults          SyncOptions `json:"job_defaults"`
	Theme                string      `json:"theme"`
	LogRetentionLines    int         `json:"log_retention_lines"`
	DestinationsRedacted bool        `json:"destinations_redacted"`
}

// defaultSettings returns the code defaults the GET handler emits when
// no settings row has been persisted yet. Pulled out so tests and the
// settings store's initial load share one source of truth.
func defaultSettings() Settings {
	return Settings{
		JobDefaults:          DefaultSyncOptions(),
		Theme:                themeSystem,
		LogRetentionLines:    defaultLogRetention,
		DestinationsRedacted: true,
	}
}

// settingsStoreImpl owns the settings row in memory and serializes
// mutations. Concrete implementation of the settingsStore interface
// declared in jobs.go.
//
// Lock-order rule (mirrors destinations.go): callers MUST release s.mu
// before invoking notifyChange — the onChange callback flows back into
// SaveState → snapshot() → re-acquires s.mu, so holding the lock across
// the notification would self-deadlock.
type settingsStoreImpl struct {
	// RWMutex so a flood of GETs (the Settings tab polls on focus) can
	// proceed concurrently while PUT/rotate take the write side.
	mu sync.RWMutex
	// row is the persisted form. nil means "no settings configured yet,
	// use code defaults" — matches the persistedState.Settings pointer
	// + omitempty contract. The first PUT materializes a row.
	row *settingsState
	// onChange is the persistence callback the API handlers invoke
	// after a successful mutation. Wired by Register to JobManager
	// SaveState. nil is allowed in unit tests that don't exercise the
	// persistence path.
	onChange func()
}

// newSettingsStore constructs an empty store. The first call to load
// (from JobManager.SetSettings) seeds the row; until then snapshot()
// returns nil so an early SaveState round-trip preserves the
// "no settings yet" state on disk.
func newSettingsStore() *settingsStoreImpl {
	return &settingsStoreImpl{}
}

// SetOnChange wires the persistence callback. Pulled out of the
// constructor so the JobManager (which owns saveState) can be passed in
// after construction; Register handles the ordering.
func (s *settingsStoreImpl) SetOnChange(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onChange = fn
}

// snapshot returns the on-disk row pointer for the JobManager to write
// to the state file. nil means "no settings persisted yet" — the
// persistedState.Settings field is omitempty so a nil snapshot
// translates to no key in the JSON output.
//
// Returns a defensive copy so the JobManager's serializer can't observe
// a half-written row if a concurrent PUT lands mid-snapshot.
func (s *settingsStoreImpl) snapshot() *settingsState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.row == nil {
		return nil
	}
	cp := *s.row
	return &cp
}

// load installs the row read from disk during JobManager startup. nil
// means "no settings key was in the state file" — the store stays in
// the "use code defaults" state. Replaces any in-memory row (none
// expected at this point — load fires exactly once during Register
// before the API is reachable).
func (s *settingsStoreImpl) load(row *settingsState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if row == nil {
		s.row = nil
		return
	}
	cp := *row
	s.row = &cp
}

// get returns the effective Settings — either the persisted row, or
// the code defaults if no row has been persisted yet. Used by the GET
// handler and by callers that want the live defaults for new jobs.
func (s *settingsStoreImpl) get() Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.row == nil {
		return defaultSettings()
	}
	return Settings{
		JobDefaults:          s.row.JobDefaults,
		Theme:                s.row.Theme,
		LogRetentionLines:    s.row.LogRetentionLines,
		DestinationsRedacted: s.row.DestinationsRedacted,
	}
}

// put replaces the settings row with the given Settings. Returns the
// normalized Settings (bounds-clamped, theme-validated). Calls onChange
// AFTER releasing s.mu so the persistence callback can re-enter
// snapshot() without deadlocking.
func (s *settingsStoreImpl) put(in Settings) (Settings, error) {
	normalized, err := normalizeSettings(in)
	if err != nil {
		return Settings{}, err
	}
	s.mu.Lock()
	s.row = &settingsState{
		JobDefaults:          normalized.JobDefaults,
		Theme:                normalized.Theme,
		LogRetentionLines:    normalized.LogRetentionLines,
		DestinationsRedacted: normalized.DestinationsRedacted,
	}
	s.mu.Unlock()
	s.notifyChange()
	return normalized, nil
}

// notifyChange fires the onChange callback. Caller MUST hold neither
// s.mu nor any lock that the callback might re-acquire (in particular
// JobManager.mu, which calls snapshot() which takes s.mu). put and the
// rotation flow call this strictly after releasing s.mu so the
// lock-order rule documented on snapshot() holds.
func (s *settingsStoreImpl) notifyChange() {
	s.mu.RLock()
	cb := s.onChange
	s.mu.RUnlock()
	if cb != nil {
		cb()
	}
}

// normalizeSettings validates and clamps the incoming Settings to the
// server-side bounds. Empty theme defaults to "system" (matches the UI's
// default state); out-of-range LogRetentionLines clamps to the nearest
// bound. JobDefaults is accepted verbatim — its own fields are already
// bounded by SyncOptions parsing.
func normalizeSettings(in Settings) (Settings, error) {
	out := in
	switch out.Theme {
	case "":
		out.Theme = themeSystem
	case themeSystem, themeDark, themeLight:
		// ok
	default:
		return Settings{}, fmt.Errorf("invalid theme %q (allowed: %s, %s, %s)", in.Theme, themeSystem, themeDark, themeLight)
	}
	if out.LogRetentionLines == 0 {
		out.LogRetentionLines = defaultLogRetention
	}
	if out.LogRetentionLines < minLogRetention {
		out.LogRetentionLines = minLogRetention
	}
	if out.LogRetentionLines > maxLogRetention {
		out.LogRetentionLines = maxLogRetention
	}
	return out, nil
}

// ===========================================================================
// Admin-key rotation
// ===========================================================================

// rotateAdminKeyRequest is the body of POST /api/settings/rotate-admin-key.
// Both fields are required and non-empty. The handler verifies oldKey
// BEFORE touching state (see the verify-before-rewrite contract at the
// top of this file).
type rotateAdminKeyRequest struct {
	OldKey string `json:"old_key"`
	NewKey string `json:"new_key"`
}

// rotateAdminKeyResponse surfaces the post-rotation instructions to the
// operator. Returned with 200 on success. The message field explicitly
// states the operator-action contract — update JM_ADMIN_KEY on the
// container, then restart — so the UI can render it verbatim.
type rotateAdminKeyResponse struct {
	OK              bool   `json:"ok"`
	ReEncryptedRows int    `json:"re_encrypted_rows"`
	Message         string `json:"message"`
}

// rotateAdminKeyOnStore re-encrypts every destination row under newKey
// after verifying oldKey. Implements the verify-before-rewrite contract:
//
//   - If the store has any destinations: try to decrypt the FIRST row's
//     blob under oldKey. Decryption success is the verification — AEAD
//     ensures only the right key could have produced a matching tag.
//
//   - If the store has no destinations: derive a key from oldKey via
//     HKDF and constant-time-compare to the in-memory cred-key. This
//     covers the "fresh install, admin wants to rotate proactively"
//     case where there's nothing to decrypt yet.
//
// On verify success, all rows are re-encrypted in-place under the new
// key. On any failure, returns errRotateVerifyFailed without touching
// any state. The lock-order rule (notifyChange outside s.mu) applies —
// onChange fires AFTER the write lock is released so saveState can
// re-enter snapshot().
//
// IMPORTANT: this function does NOT mutate s.key. The store keeps the
// old key in memory after a successful rotation — see the rotation
// contract at the top of this file for the rationale (operator must
// restart with the new JM_ADMIN_KEY to load the new key, ensuring
// process and disk are aligned).
func rotateAdminKeyOnStore(store *destinationStoreImpl, oldKey, newKey []byte) (int, error) {
	if store == nil {
		return 0, errors.New("destinations subsystem not initialized")
	}
	if len(oldKey) == 0 || len(newKey) == 0 {
		return 0, errors.New("old_key and new_key are required and must be non-empty")
	}
	oldCredKey, err := deriveCredKey(oldKey)
	if err != nil {
		return 0, fmt.Errorf("derive old cred-key: %w", err)
	}
	newCredKey, err := deriveCredKey(newKey)
	if err != nil {
		// Scrub the old derived key on the failure path — it's about to
		// go out of scope but the explicit zero limits exposure if a
		// coredump captures this frame.
		zeroBytes(oldCredKey)
		return 0, fmt.Errorf("derive new cred-key: %w", err)
	}

	store.mu.Lock()
	// Defensive — if the store wasn't initialized with a key (no
	// JM_ADMIN_KEY at startup), there's nothing to verify against.
	// Reject the rotation rather than silently installing a fresh key.
	if len(store.key) != credKeyLen {
		store.mu.Unlock()
		zeroBytes(oldCredKey)
		zeroBytes(newCredKey)
		return 0, errors.New("destinations subsystem has no in-memory key; set JM_ADMIN_KEY and restart before rotating")
	}

	// ---- Verify the old key ---------------------------------------
	// Discipline: we MUST NOT mutate any row until verification passes.
	// On failure, every defer/exit path leaves store.rows untouched and
	// the in-memory store.key unchanged.
	if len(store.rows) > 0 {
		// Path A: at least one destination exists. Decrypt the first
		// row's blob under oldCredKey. AEAD verification is the
		// authoritative check — only the correct key produces a
		// matching tag, so success here proves the operator supplied
		// the right old admin key. We do NOT iterate every row at
		// verification time (one suffices); the bulk decrypt happens
		// next, in the re-encrypt pass.
		blob, decErr := base64.StdEncoding.DecodeString(store.rows[0].EncryptedConfig)
		if decErr != nil {
			store.mu.Unlock()
			zeroBytes(oldCredKey)
			zeroBytes(newCredKey)
			return 0, fmt.Errorf("verify: row 0 base64 decode: %w", decErr)
		}
		verifyPlain, decErr := decryptSecret(blob, oldCredKey)
		// Scrub the verify-step plaintext immediately. Even on success
		// we don't need it — verification is purely AEAD-tag based.
		// On failure decryptSecret returns nil, so the zero is a no-op.
		zeroBytes(verifyPlain)
		if decErr != nil {
			// AEAD failure → wrong old key. Surface as the sentinel so
			// the HTTP handler returns 401 without touching state.
			store.mu.Unlock()
			zeroBytes(oldCredKey)
			zeroBytes(newCredKey)
			return 0, errRotateVerifyFailed
		}
	} else {
		// Path B: no destinations to decrypt. Compare the derived old
		// cred-key to the in-memory key via constant-time compare so a
		// timing oracle can't recover the admin key one byte at a time
		// even if the attacker controls the HTTP client.
		if subtle.ConstantTimeCompare(oldCredKey, store.key) != 1 {
			store.mu.Unlock()
			zeroBytes(oldCredKey)
			zeroBytes(newCredKey)
			return 0, errRotateVerifyFailed
		}
	}

	// ---- Re-encrypt every row under newCredKey --------------------
	// At this point oldCredKey is proven valid. Build the new slice
	// atomically — if any row fails to decrypt-then-encrypt, roll back
	// to the original rows by NOT assigning the new slice. The state
	// file is only updated via the onChange callback below; until that
	// fires, on-disk state still has the old ciphertext.
	rebuilt := make([]destinationsState, len(store.rows))
	for i, r := range store.rows {
		blob, decErr := base64.StdEncoding.DecodeString(r.EncryptedConfig)
		if decErr != nil {
			store.mu.Unlock()
			zeroBytes(oldCredKey)
			zeroBytes(newCredKey)
			return 0, fmt.Errorf("rotate: row %d (%q) base64 decode: %w", i, r.Name, decErr)
		}
		plaintext, decErr := decryptSecret(blob, oldCredKey)
		if decErr != nil {
			store.mu.Unlock()
			zeroBytes(oldCredKey)
			zeroBytes(newCredKey)
			return 0, fmt.Errorf("rotate: row %d (%q) decrypt: %w", i, r.Name, decErr)
		}
		newBlob, encErr := encryptSecret(plaintext, newCredKey)
		// Best-effort plaintext scrub the moment we no longer need it.
		zeroBytes(plaintext)
		if encErr != nil {
			store.mu.Unlock()
			zeroBytes(oldCredKey)
			zeroBytes(newCredKey)
			return 0, fmt.Errorf("rotate: row %d (%q) encrypt: %w", i, r.Name, encErr)
		}
		rebuilt[i] = destinationsState{
			Name:            r.Name,
			Kind:            r.Kind,
			EncryptedConfig: base64.StdEncoding.EncodeToString(newBlob),
			CreatedAt:       r.CreatedAt,
			UpdatedAt:       r.UpdatedAt,
		}
	}
	count := len(rebuilt)
	store.rows = rebuilt
	// NOTE: deliberately NOT updating store.key — see contract at top.
	store.mu.Unlock()

	// Scrub the derived keys now that the re-encryption pass is done.
	// The new key only needed to live long enough to seal each row's
	// ciphertext; subsequent reads (until restart) use store.key,
	// which is still the OLD value.
	zeroBytes(oldCredKey)
	zeroBytes(newCredKey)

	// Persist the new ciphertexts. Fires OUTSIDE the store lock — the
	// callback chain re-enters snapshot() which acquires store.mu.
	if store.onChange != nil {
		store.onChange()
	}
	return count, nil
}

// errRotateVerifyFailed is the sentinel returned when the old_key
// supplied to a rotation request fails verification. The HTTP handler
// recognizes it and returns 401 without echoing the underlying error
// message (we don't want an oracle that leaks "no destinations exist"
// vs. "wrong key").
var errRotateVerifyFailed = errors.New("admin key verification failed")

// zeroBytes overwrites a byte slice with zeros. Best-effort scrub — Go
// doesn't promise the underlying memory isn't already in another
// runtime buffer, but the explicit zero limits incidental exposure
// from a coredump.
func zeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// ===========================================================================
// HTTP handlers
// ===========================================================================

// handleSettings dispatches GET (current settings) and PUT (replace
// settings) to /api/settings.
func (a *API) handleSettings(w http.ResponseWriter, r *http.Request) {
	if a.settings == nil {
		http.Error(w, "settings subsystem not initialized", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.settings.get())
	case http.MethodPut:
		var in Settings
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		out, err := a.settings.put(in)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, out)
	default:
		http.Error(w, "GET or PUT only", http.StatusMethodNotAllowed)
	}
}

// handleRotateAdminKey runs the verify-before-rewrite admin-key
// rotation flow. POST only. Body: {"old_key": "...", "new_key": "..."}.
//
// Verify failures return 401. State file is NOT touched on the failure
// path — the verify check happens before any row mutation, and a 401
// short-circuits before saveState could fire.
//
// On success: every destination's encrypted_config is rebuilt under the
// new key, the state file is rewritten atomically (rename-based, see
// state.go's writer), and the response instructs the operator to update
// JM_ADMIN_KEY and restart. The in-memory cred-key STAYS on the old
// value until the restart picks up the new env — keeps process state
// and on-disk state coherent during the rotation window.
func (a *API) handleRotateAdminKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if a.dests == nil {
		http.Error(w, "destinations subsystem not initialized", http.StatusServiceUnavailable)
		return
	}
	var req rotateAdminKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.OldKey == "" || req.NewKey == "" {
		http.Error(w, "old_key and new_key are required and must be non-empty", http.StatusBadRequest)
		return
	}
	count, err := rotateAdminKeyOnStore(a.dests, []byte(req.OldKey), []byte(req.NewKey))
	if err != nil {
		if errors.Is(err, errRotateVerifyFailed) {
			// Generic 401 message — don't distinguish "wrong key" from
			// "no destinations to verify against" by error text alone
			// (state-file inspection trivially answers the latter, but
			// the API shouldn't be the oracle).
			http.Error(w, "admin key verification failed", http.StatusUnauthorized)
			return
		}
		// Don't echo err.Error() — the rotation error strings can
		// contain destination row indices/names and infrastructure
		// hints (e.g. the JM_ADMIN_KEY env-var name). Log internally,
		// return a generic message so a probing client doesn't learn
		// the server's destinations inventory or deployment shape.
		log.Printf("manager: rotate admin key failed: %v", err)
		http.Error(w, "rotation failed; check server logs", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, rotateAdminKeyResponse{
		OK:              true,
		ReEncryptedRows: count,
		Message:         "Admin key rotated. Every destination's encrypted_config has been rewritten under the new key. Update JM_ADMIN_KEY on the container and restart for the in-memory key to follow.",
	})
}
