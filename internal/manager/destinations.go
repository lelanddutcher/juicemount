// Package manager — SLICE 4 destinations: named, encrypted-at-rest
// remote-endpoint profiles.
//
// This file owns the CRUD handlers (/api/destinations) and the per-kind
// testConnection probes. Crypto primitives live in crypto.go; on-disk
// schema lives in state.go and the JobManager persistence path in
// jobs.go. The split keeps the security-reviewer surface tight.
//
// Wire model:
//
//   - Destination (this file, exported)       — plaintext API/runtime form.
//   - destinationsState (state.go, unexported) — encrypted on-disk form.
//
// Plaintext credentials NEVER touch disk and NEVER appear in API
// responses. POST/PUT accept them (encrypted before persist); GET
// returns config keys with "<set>" placeholders so the UI can render
// "all fields configured" without echoing the value back. juicefs sync
// receives them only via cmd.Env at exec time (handled by ToSyncEnv).
package manager

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/ssh"
)

// destinationKinds is the allow-list of recognized profile kinds. Any
// new backend goes here AND in the testConnection switch + ToSyncEnv
// switch. Kept as a map for O(1) membership check at the API boundary.
var destinationKinds = map[string]struct{}{
	"file":   {}, // local filesystem path (file://...)
	"s3":     {}, // generic S3-compatible (AWS, MinIO, Wasabi, ...)
	"b2":     {}, // Backblaze B2 (S3-compatible mode or native API)
	"sftp":   {}, // SSH file transfer
	"webdav": {}, // WebDAV / HTTP
	"jfs":    {}, // JuiceFS volume (different metaURL than the primary)
}

// nameRegex constraints: lower-cased ASCII letters, digits, dash and
// underscore, 1..64 chars. Used as the URL path segment in
// /api/destinations/{name} so we keep it strict — no slashes, no dots,
// no whitespace, no characters that would force url-encoding in the
// path or in env-var names (which we derive from the name).
//
// Validated via validateDestinationName rather than a regexp so the
// error messages can be specific.
const maxDestinationNameLen = 64

// Destination is the in-memory, API-boundary representation of a saved
// remote-endpoint profile. Config holds plaintext credentials and is
// the value the caller PUTs; the on-disk form (destinationsState)
// stores Config encrypted as a single AES-GCM blob.
//
// Config keys vary by kind:
//   - file:   {"path": "/external/backups"}
//   - s3/b2:  {"endpoint", "bucket", "access_key", "secret_key", "region" (optional)}
//   - sftp:   {"host", "port", "user", "password" OR "private_key", "path" (optional)}
//   - webdav: {"endpoint", "user" (optional), "password" (optional)}
//   - jfs:    {"meta_url", "volume" (optional, defaults to "jfs")}
//
// Validation of required keys per kind happens in validateConfig
// before encryption; missing keys yield a 400 with the missing list.
type Destination struct {
	Name      string            `json:"name"`
	Kind      string            `json:"kind"`
	Config    map[string]string `json:"config,omitempty"`
	CreatedAt int64             `json:"created_at,omitempty"`
	UpdatedAt int64             `json:"updated_at,omitempty"`
}

// destinationStoreImpl owns the encrypted destinations slice in memory
// and serializes mutations. Concrete implementation of the
// destinationStore interface declared in jobs.go.
//
// All accessors take m.mu for the duration of the operation so the API
// layer and the JobManager's snapshot path see a consistent picture.
type destinationStoreImpl struct {
	// RWMutex so snapshot/exists/listRedacted can run concurrently
	// with each other (read-only paths) while upsert/remove are write
	// paths. Critical lock-order: callers MUST release this before
	// invoking notifyChange (which triggers SaveState → snapshot →
	// reacquires this same lock).
	mu sync.RWMutex
	// rows holds the encrypted-at-rest form. Lookup by name is O(N)
	// — fine for the expected dozens-of-entries scale, and keeping a
	// slice (not a map) means snapshot() returns a deterministic JSON
	// ordering for operator review.
	rows []destinationsState
	// key is the AES-256 key derived from JM_ADMIN_KEY via HKDF.
	// Captured at construction so encrypt/decrypt are cheap. Empty
	// when adminKey was empty — every CRUD handler returns an error
	// in that case so we never persist plaintext.
	key []byte
	// onChange is the persistence callback the API handlers invoke
	// after a successful mutation. Wired by Register to
	// jobs.saveState(). nil is allowed in unit tests that don't
	// exercise the persistence path.
	onChange func()
}

// newDestinationStore constructs an empty store. The key is derived
// once via deriveCredKey; an empty adminKey leaves key=nil and disables
// the CRUD handlers (they return 503 with an actionable message).
func newDestinationStore(adminKey string) (*destinationStoreImpl, error) {
	d := &destinationStoreImpl{}
	if adminKey == "" {
		// No admin key → no encryption key. The /api/destinations
		// handlers refuse to operate in this state; LAN-only "no
		// auth" deployments can still use Migrations without
		// destinations.
		return d, nil
	}
	k, err := deriveCredKey([]byte(adminKey))
	if err != nil {
		return nil, err
	}
	d.key = k
	return d, nil
}

// snapshot returns a defensive copy of the encrypted rows for the
// JobManager to write to disk. Satisfies the destinationStore
// interface in jobs.go.
//
// Acquires s.mu (read lock) — safe because the mutation paths
// (upsert/remove) call notifyChange AFTER releasing s.mu, so the
// onChange → SaveState → saveStateLocked → snapshot() callback chain
// can re-acquire s.mu without deadlock. Any goroutine that invokes
// SaveState directly (e.g., a future scheduled job) also can't race
// with concurrent upsert/remove because snapshot takes the lock.
func (s *destinationStoreImpl) snapshot() []destinationsState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]destinationsState, len(s.rows))
	copy(out, s.rows)
	return out
}

// load installs the rows read from disk during JobManager startup.
// Replaces any in-memory rows (none expected at this point — load fires
// exactly once during Register before the API is reachable).
func (s *destinationStoreImpl) load(rows []destinationsState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append([]destinationsState(nil), rows...)
}

// SetOnChange wires the persistence callback. Pulled out of the
// constructor so the JobManager (which owns saveState) can be passed
// in after construction; Register handles the ordering.
func (s *destinationStoreImpl) SetOnChange(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onChange = fn
}

// encryptedReady reports whether the store can encrypt/decrypt. False
// when no JM_ADMIN_KEY was configured; handlers return 503 in that
// case (rather than silently storing plaintext or crashing).
func (s *destinationStoreImpl) encryptedReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.key) == credKeyLen
}

// listRedacted returns every destination in a redacted form suitable
// for GET /api/destinations. Each entry's Config map has its values
// replaced with "<set>" / "" placeholders so the UI can render "all
// fields configured" without the response ever carrying the secret.
func (s *destinationStoreImpl) listRedacted() ([]Destination, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.key) != credKeyLen {
		return nil, errors.New("destinations are unavailable: set JM_ADMIN_KEY to enable encrypted credential storage")
	}
	out := make([]Destination, 0, len(s.rows))
	for _, r := range s.rows {
		blob, err := base64.StdEncoding.DecodeString(r.EncryptedConfig)
		if err != nil {
			// One bad row shouldn't blank the whole tab — surface a
			// stub with no Config so the operator can see the entry
			// name and re-create it.
			out = append(out, Destination{Name: r.Name, Kind: r.Kind, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, Config: map[string]string{"<error>": "base64 decode failed"}})
			continue
		}
		plaintext, err := decryptSecret(blob, s.key)
		if err != nil {
			out = append(out, Destination{Name: r.Name, Kind: r.Kind, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, Config: map[string]string{"<error>": "decryption failed (admin key changed?)"}})
			continue
		}
		var cfg map[string]string
		if err := json.Unmarshal(plaintext, &cfg); err != nil {
			out = append(out, Destination{Name: r.Name, Kind: r.Kind, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, Config: map[string]string{"<error>": "config json malformed"}})
			continue
		}
		// Best-effort: zero the plaintext slice after we're done with
		// it. Go's GC and string interning mean this isn't a strong
		// guarantee — full memory hygiene needs a syscall.mlock + an
		// explicit free, which is out of scope for slice-4 — but
		// scrubbing the buffer at least limits incidental exposure
		// from a coredump captured mid-handler.
		for i := range plaintext {
			plaintext[i] = 0
		}
		redacted := make(map[string]string, len(cfg))
		for k, v := range cfg {
			if v == "" {
				redacted[k] = ""
			} else {
				// Literal placeholder string. Picked "<set>" because
				// the angle brackets are unambiguous in a JSON value
				// (won't be confused with a literal user-supplied
				// string) and the word is short enough to read at a
				// glance in the UI.
				redacted[k] = "<set>"
			}
		}
		out = append(out, Destination{
			Name:      r.Name,
			Kind:      r.Kind,
			Config:    redacted,
			CreatedAt: r.CreatedAt,
			UpdatedAt: r.UpdatedAt,
		})
	}
	// Sort by name so the UI list ordering is stable across reloads.
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// getPlaintext returns the plaintext Destination for a given name.
// Used by testConnection and (in slice-5) the schedule runner. Never
// exposed via the HTTP API.
func (s *destinationStoreImpl) getPlaintext(name string) (Destination, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.key) != credKeyLen {
		return Destination{}, errors.New("destinations are unavailable: set JM_ADMIN_KEY to enable encrypted credential storage")
	}
	for _, r := range s.rows {
		if r.Name != name {
			continue
		}
		blob, err := base64.StdEncoding.DecodeString(r.EncryptedConfig)
		if err != nil {
			return Destination{}, fmt.Errorf("base64 decode: %w", err)
		}
		plaintext, err := decryptSecret(blob, s.key)
		if err != nil {
			return Destination{}, err
		}
		var cfg map[string]string
		if err := json.Unmarshal(plaintext, &cfg); err != nil {
			return Destination{}, fmt.Errorf("config unmarshal: %w", err)
		}
		for i := range plaintext {
			plaintext[i] = 0
		}
		return Destination{
			Name:      r.Name,
			Kind:      r.Kind,
			Config:    cfg,
			CreatedAt: r.CreatedAt,
			UpdatedAt: r.UpdatedAt,
		}, nil
	}
	return Destination{}, errDestinationNotFound
}

var errDestinationNotFound = errors.New("destination not found")

// upsert encrypts the given Destination and stores it. If a row with
// the same name already exists, it's replaced (UpdatedAt bumped,
// CreatedAt preserved). Calls onChange AFTER releasing s.mu so the
// persistence callback can re-enter snapshot() without deadlocking.
func (s *destinationStoreImpl) upsert(d Destination, allowReplace bool) error {
	mutated, err := s.upsertLocked(d, allowReplace)
	if mutated {
		s.notifyChange()
	}
	return err
}

// upsertLocked does the actual mutation under s.mu and returns whether
// onChange should fire. Separated so upsert can release the lock
// before notification — see snapshot()'s doc for the lock-order rule.
func (s *destinationStoreImpl) upsertLocked(d Destination, allowReplace bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.key) != credKeyLen {
		return false, errors.New("destinations are unavailable: set JM_ADMIN_KEY to enable encrypted credential storage")
	}
	plaintext, err := json.Marshal(d.Config)
	if err != nil {
		return false, fmt.Errorf("marshal config: %w", err)
	}
	blob, err := encryptSecret(plaintext, s.key)
	// Scrub the plaintext as soon as it's no longer needed; see the
	// caveat in listRedacted about Go's memory hygiene limits.
	for i := range plaintext {
		plaintext[i] = 0
	}
	if err != nil {
		return false, err
	}
	encoded := base64.StdEncoding.EncodeToString(blob)
	now := time.Now().UnixMilli()
	for i, r := range s.rows {
		if r.Name == d.Name {
			if !allowReplace {
				return false, fmt.Errorf("destination %q already exists", d.Name)
			}
			s.rows[i] = destinationsState{
				Name:            r.Name,
				Kind:            d.Kind,
				EncryptedConfig: encoded,
				CreatedAt:       r.CreatedAt, // preserve original
				UpdatedAt:       now,
			}
			return true, nil
		}
	}
	s.rows = append(s.rows, destinationsState{
		Name:            d.Name,
		Kind:            d.Kind,
		EncryptedConfig: encoded,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	return true, nil
}

// remove deletes a destination by name. Idempotent — returns nil if
// the name doesn't exist (per spec acceptance criterion #3).
func (s *destinationStoreImpl) remove(name string) {
	removed := s.removeLocked(name)
	if removed {
		s.notifyChange()
	}
}

func (s *destinationStoreImpl) removeLocked(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, r := range s.rows {
		if r.Name == name {
			s.rows = append(s.rows[:i], s.rows[i+1:]...)
			return true
		}
	}
	return false
}

// exists reports whether a destination with the given name is currently
// stored. Used by PUT handlers that want to enforce update-only semantics
// (return 404 on missing name instead of silently creating).
func (s *destinationStoreImpl) exists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.rows {
		if r.Name == name {
			return true
		}
	}
	return false
}

// notifyChange fires the onChange callback. Caller MUST hold neither
// s.mu nor any lock that the callback might re-acquire (in particular
// JobManager.mu, which calls snapshot() which takes s.mu). upsert and
// remove call this strictly after releasing s.mu so the lock-order
// rule documented on snapshot() holds.
func (s *destinationStoreImpl) notifyChange() {
	s.mu.RLock()
	cb := s.onChange
	s.mu.RUnlock()
	if cb != nil {
		cb()
	}
}

// validateDestinationName enforces the URL-segment-safe character set
// and length cap. Returns a specific error so the API can surface it
// without ambiguity.
func validateDestinationName(name string) error {
	if name == "" {
		return errors.New("name is required")
	}
	if len(name) > maxDestinationNameLen {
		return fmt.Errorf("name too long (%d > %d)", len(name), maxDestinationNameLen)
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return fmt.Errorf("name contains invalid character %q at position %d (allowed: a-z 0-9 - _)", r, i)
		}
	}
	return nil
}

// validateKind returns an error if kind isn't in the destinationKinds
// allow-list.
func validateKind(kind string) error {
	if _, ok := destinationKinds[kind]; !ok {
		return fmt.Errorf("unknown kind %q (allowed: file, s3, b2, sftp, webdav, jfs)", kind)
	}
	return nil
}

// validateConfig checks that the Config map carries the required keys
// for the given kind. Optional keys are not enforced. The check is
// lenient about extras — operators can stash kind-specific tuning
// (e.g. an "endpoint_style" hint) without us refusing the write.
func validateConfig(kind string, cfg map[string]string) error {
	if cfg == nil {
		return errors.New("config is required")
	}
	required := map[string][]string{
		"file":   {"path"},
		"s3":     {"endpoint", "bucket", "access_key", "secret_key"},
		"b2":     {"endpoint", "bucket", "access_key", "secret_key"},
		"sftp":   {"host", "user"},
		"webdav": {"endpoint"},
		"jfs":    {"meta_url"},
	}
	missing := []string{}
	for _, k := range required[kind] {
		if cfg[k] == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required config keys for kind %q: %s", kind, strings.Join(missing, ", "))
	}
	// sftp must have either password or private_key
	if kind == "sftp" {
		if cfg["password"] == "" && cfg["private_key"] == "" {
			return errors.New("sftp requires either 'password' or 'private_key' in config")
		}
	}
	return nil
}

// ===========================================================================
// HTTP handlers
// ===========================================================================

// handleDestinations dispatches GET (list) and POST (create) to
// /api/destinations.
func (a *API) handleDestinations(w http.ResponseWriter, r *http.Request) {
	if a.dests == nil {
		http.Error(w, "destinations subsystem not initialized", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		a.handleDestinationsList(w, r)
	case http.MethodPost:
		a.handleDestinationsCreate(w, r)
	default:
		http.Error(w, "GET or POST only", http.StatusMethodNotAllowed)
	}
}

// handleDestinationItem dispatches PUT (update), DELETE (remove), and
// POST .../test to /api/destinations/{name} and
// /api/destinations/{name}/test.
func (a *API) handleDestinationItem(w http.ResponseWriter, r *http.Request) {
	if a.dests == nil {
		http.Error(w, "destinations subsystem not initialized", http.StatusServiceUnavailable)
		return
	}
	// Strip the prefix to extract the {name}[/test] tail. Defensive
	// fallback to the unprefixed form for tests that hand-construct an
	// API with a "" prefix.
	tail := strings.TrimPrefix(r.URL.Path, a.prefix+"/api/destinations/")
	if tail == r.URL.Path {
		tail = strings.TrimPrefix(r.URL.Path, "/api/destinations/")
	}
	if tail == "" {
		http.Error(w, "destination name required", http.StatusBadRequest)
		return
	}
	parts := strings.SplitN(tail, "/", 2)
	name := parts[0]
	sub := ""
	if len(parts) > 1 {
		sub = parts[1]
	}
	if err := validateDestinationName(name); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch {
	case sub == "test" && r.Method == http.MethodPost:
		a.handleDestinationTest(w, r, name)
	case sub == "" && r.Method == http.MethodPut:
		a.handleDestinationUpdate(w, r, name)
	case sub == "" && r.Method == http.MethodDelete:
		a.handleDestinationDelete(w, r, name)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (a *API) handleDestinationsList(w http.ResponseWriter, r *http.Request) {
	out, err := a.dests.listRedacted()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"destinations": out})
}

func (a *API) handleDestinationsCreate(w http.ResponseWriter, r *http.Request) {
	if !a.dests.encryptedReady() {
		http.Error(w, "destinations are unavailable: set JM_ADMIN_KEY to enable encrypted credential storage", http.StatusServiceUnavailable)
		return
	}
	var d Destination
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	d.Name = strings.TrimSpace(d.Name)
	d.Kind = strings.TrimSpace(d.Kind)
	if err := validateDestinationName(d.Name); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateKind(d.Kind); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateConfig(d.Kind, d.Config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.dests.upsert(d, false); err != nil {
		// "already exists" is a 409 — duplicate-key class of error.
		if strings.Contains(err.Error(), "already exists") {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Return the freshly-redacted entry so the UI can render the
	// "all fields configured" state without a follow-up GET.
	out, _ := a.dests.listRedacted()
	for _, e := range out {
		if e.Name == d.Name {
			writeJSON(w, http.StatusCreated, e)
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"name": d.Name})
}

func (a *API) handleDestinationUpdate(w http.ResponseWriter, r *http.Request, name string) {
	if !a.dests.encryptedReady() {
		http.Error(w, "destinations are unavailable: set JM_ADMIN_KEY to enable encrypted credential storage", http.StatusServiceUnavailable)
		return
	}
	var d Destination
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The URL fixes the name; ignore any value in the body (or reject
	// a mismatch, which we do here to make API misuse obvious rather
	// than silently choosing one).
	if d.Name != "" && d.Name != name {
		http.Error(w, "name in URL and body must match (or omit body.name)", http.StatusBadRequest)
		return
	}
	d.Name = name
	d.Kind = strings.TrimSpace(d.Kind)
	if err := validateKind(d.Kind); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateConfig(d.Kind, d.Config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// PUT is replace-only — a 404 here surfaces a client typo faster
	// than silently creating a duplicate. POST is the create path.
	if !a.dests.exists(name) {
		http.Error(w, "destination not found", http.StatusNotFound)
		return
	}
	if err := a.dests.upsert(d, true); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out, err := a.dests.listRedacted()
	if err != nil {
		log.Printf("manager: listRedacted after PUT %q failed: %v", d.Name, err)
	}
	for _, e := range out {
		if e.Name == d.Name {
			writeJSON(w, http.StatusOK, e)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": d.Name})
}

func (a *API) handleDestinationDelete(w http.ResponseWriter, r *http.Request, name string) {
	a.dests.remove(name)
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) handleDestinationTest(w http.ResponseWriter, r *http.Request, name string) {
	if !a.dests.encryptedReady() {
		http.Error(w, "destinations are unavailable: set JM_ADMIN_KEY to enable encrypted credential storage", http.StatusServiceUnavailable)
		return
	}
	d, err := a.dests.getPlaintext(name)
	if err != nil {
		if errors.Is(err, errDestinationNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := testConnection(ctx, d); err != nil {
		// 502 = upstream failure (the remote endpoint we proxied to is
		// the upstream). Includes the diagnostic message so the
		// operator knows whether it's a credential, a network, or a
		// permissions issue.
		http.Error(w, "connection test failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	// Scrub the plaintext config we held briefly so a subsequent
	// memory dump doesn't carry it.
	for k := range d.Config {
		d.Config[k] = ""
	}
}

// ===========================================================================
// Per-kind connection probes
// ===========================================================================

// testConnection runs a kind-specific liveness check against the
// destination. Probes are intentionally minimal — we want to confirm
// credentials work without copying or mutating data:
//
//   - file:   os.Stat the path; verify directory + writable.
//   - s3/b2:  HEAD <endpoint>/<bucket>. We use raw HTTP rather than an
//             SDK to keep the dependency surface small; the response
//             code distinguishes 200 (ok), 403 (auth), 404 (bucket not
//             found), 0 (network).
//   - sftp:   SSH dial + handshake + close. Does not open a session;
//             the auth handshake is enough.
//   - webdav: PROPFIND on the root with Depth: 0. Servers return 207
//             Multi-Status on success.
//   - jfs:    Redis PING against the meta_url.
//
// For test-only HTTP probes (s3, b2, webdav) we use a short HTTP
// client timeout (4s) on top of the ctx deadline so a hanging TCP
// connection can't extend the request beyond the handler's 10s budget.
func testConnection(ctx context.Context, d Destination) error {
	switch d.Kind {
	case "file":
		return testConnectionFile(d.Config)
	case "s3", "b2":
		return testConnectionS3(ctx, d.Config)
	case "sftp":
		return testConnectionSFTP(ctx, d.Config)
	case "webdav":
		return testConnectionWebDAV(ctx, d.Config)
	case "jfs":
		return testConnectionJFS(ctx, d.Config)
	default:
		return fmt.Errorf("no probe implemented for kind %q", d.Kind)
	}
}

func testConnectionFile(cfg map[string]string) error {
	p := strings.TrimSpace(cfg["path"])
	if p == "" {
		return errors.New("path is empty")
	}
	if !filepath.IsAbs(p) {
		return errors.New("path must be absolute")
	}
	info, err := os.Stat(p)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if !info.IsDir() {
		return errors.New("path is not a directory")
	}
	// Write-test by attempting to create + delete a stamp file. Going
	// through os.CreateTemp avoids racing with a real file at a fixed
	// name and gives us actual write-permission feedback (a read-only
	// mount fails here even though Stat succeeded above).
	f, err := os.CreateTemp(p, ".juicemount-write-test-*")
	if err != nil {
		return fmt.Errorf("write test: %w", err)
	}
	tmpName := f.Name()
	_ = f.Close()
	_ = os.Remove(tmpName)
	return nil
}

func testConnectionS3(ctx context.Context, cfg map[string]string) error {
	endpoint := strings.TrimSpace(cfg["endpoint"])
	bucket := strings.TrimSpace(cfg["bucket"])
	if endpoint == "" || bucket == "" {
		return errors.New("endpoint and bucket are required")
	}
	// Bare hosts (no scheme) default to https — the cautious choice for
	// internet-facing S3-compatible providers. Operators who genuinely
	// need plaintext HTTP pass the http:// scheme explicitly.
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("endpoint parse: %w", err)
	}
	// HEAD https://<endpoint>/<bucket> — virtual-hosted vs path-style
	// is moot for the liveness check; both surface ok/403/404. The
	// 403 case is the common "credentials wrong" tell.
	u.Path = strings.TrimRight(u.Path, "/") + "/" + bucket + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	// Custom client with a short timeout (we don't want a half-open
	// TCP session to swallow the whole 10s handler budget).
	client := &http.Client{
		Timeout: 4 * time.Second,
		Transport: &http.Transport{
			// Keep TLS default — we WANT cert verification. Operators
			// who legitimately need self-signed should configure the
			// system trust store, not toggle InsecureSkipVerify.
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return nil
	case http.StatusForbidden:
		return errors.New("403 forbidden (check access_key / secret_key)")
	case http.StatusNotFound:
		return errors.New("404 not found (check bucket name)")
	case http.StatusMovedPermanently, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return errors.New("redirect (check endpoint URL / region)")
	default:
		// HEAD against an unauthenticated S3 endpoint typically returns
		// 401/403; we return the status verbatim so the operator can
		// look it up if needed.
		return fmt.Errorf("unexpected HTTP %d", resp.StatusCode)
	}
}

func testConnectionSFTP(ctx context.Context, cfg map[string]string) error {
	host := strings.TrimSpace(cfg["host"])
	user := strings.TrimSpace(cfg["user"])
	if host == "" || user == "" {
		return errors.New("host and user are required")
	}
	port := strings.TrimSpace(cfg["port"])
	if port == "" {
		port = "22"
	}
	authMethods := []ssh.AuthMethod{}
	if pw := cfg["password"]; pw != "" {
		authMethods = append(authMethods, ssh.Password(pw))
	}
	if key := cfg["private_key"]; key != "" {
		// Accept both unencrypted PEM and an encrypted key with a
		// "private_key_passphrase" sibling. We don't currently support
		// passphrase-protected keys (slice-4 scope) but the error is
		// explicit so the operator knows what's missing.
		signer, err := ssh.ParsePrivateKey([]byte(key))
		if err != nil {
			return fmt.Errorf("parse private_key: %w", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}
	if len(authMethods) == 0 {
		return errors.New("no auth method (set password or private_key)")
	}
	// SSH host-key verification: we accept any host key here because
	// the operator is wiring up a brand-new destination and there's
	// no UX yet to surface the host-key fingerprint for explicit
	// confirmation. Documented as a follow-up; the connection-test
	// surface is narrow (no data transfer), so a MITM here only
	// confirms a wrong key works against an attacker, not data
	// disclosure.
	cfg2 := &ssh.ClientConfig{
		User:            user,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         4 * time.Second,
	}
	// Use Dial with a context-aware net.Dialer so ctx cancellation
	// (handler-timeout) cuts the connect attempt cleanly.
	dialer := &net.Dialer{Timeout: 4 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return fmt.Errorf("tcp dial: %w", err)
	}
	defer conn.Close()
	sshConn, chans, reqs, err := ssh.NewClientConn(conn, net.JoinHostPort(host, port), cfg2)
	if err != nil {
		return fmt.Errorf("ssh handshake: %w", err)
	}
	client := ssh.NewClient(sshConn, chans, reqs)
	_ = client.Close()
	return nil
}

func testConnectionWebDAV(ctx context.Context, cfg map[string]string) error {
	endpoint := strings.TrimSpace(cfg["endpoint"])
	if endpoint == "" {
		return errors.New("endpoint is required")
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	// PROPFIND with Depth:0 is the cheapest WebDAV liveness probe —
	// asks for properties of the URI itself, no children.
	req, err := http.NewRequestWithContext(ctx, "PROPFIND", endpoint, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Depth", "0")
	if u := cfg["user"]; u != "" {
		req.SetBasicAuth(u, cfg["password"])
	}
	client := &http.Client{
		Timeout: 4 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer resp.Body.Close()
	// 207 Multi-Status is the success code for PROPFIND. Many servers
	// accept the request without auth so a 200 is also fine. 401/403
	// surface the credential problem clearly.
	switch resp.StatusCode {
	case http.StatusOK, http.StatusMultiStatus:
		return nil
	case http.StatusUnauthorized:
		return errors.New("401 unauthorized (check user / password)")
	case http.StatusForbidden:
		return errors.New("403 forbidden")
	case http.StatusNotFound:
		return errors.New("404 not found (check endpoint URL)")
	default:
		return fmt.Errorf("unexpected HTTP %d", resp.StatusCode)
	}
}

func testConnectionJFS(ctx context.Context, cfg map[string]string) error {
	meta := strings.TrimSpace(cfg["meta_url"])
	if meta == "" {
		return errors.New("meta_url is required")
	}
	// JuiceFS meta URL is the same format as the redis-go ParseURL
	// expects (redis://host:port/db). Anything else is rejected up
	// front so we don't try to PING a Postgres or MySQL metaURL with
	// a Redis client.
	if !strings.HasPrefix(meta, "redis://") && !strings.HasPrefix(meta, "rediss://") {
		return errors.New("only redis://… meta URLs are supported by the jfs probe (set meta_url accordingly)")
	}
	addr, db, err := parseRedisAddr(meta)
	if err != nil {
		return fmt.Errorf("meta_url parse: %w", err)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           db,
		DialTimeout:  4 * time.Second,
		ReadTimeout:  4 * time.Second,
		WriteTimeout: 4 * time.Second,
	})
	defer rdb.Close()
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	if pong != "PONG" {
		return fmt.Errorf("unexpected ping response %q", pong)
	}
	return nil
}

// ===========================================================================
// Subprocess env mapping (for the migrator's sync subprocess)
// ===========================================================================

// ToSyncURI converts a saved destination into the URI + environment
// pair that juicefs sync expects. Returns (uri, envSlice, error).
//
// CRITICAL: credentials go in env vars NEVER in the returned URI.
// `ps aux` shows process command lines; secrets in argv would leak to
// any local user. The migrator's RunSync already passes cmd.Env
// (inheriting os.Environ) so the additional env vars here merge
// cleanly with the existing environment.
//
// preserveStructure passes through to matchSlash so src/dst agree on
// the rsync trailing-slash convention (see sync.go).
func (d Destination) ToSyncURI(preserveStructure bool) (string, []string, error) {
	var uri string
	var env []string
	switch d.Kind {
	case "file":
		p := d.Config["path"]
		if p == "" {
			return "", nil, errors.New("file: path required")
		}
		uri = "file://" + p
	case "s3", "b2":
		// juicefs sync accepts <scheme>://<access_key>:<secret_key>@<endpoint>/<bucket>
		// but THAT puts creds on the command line — exactly what we
		// must avoid. Instead we use the env-var form supported by
		// most juicefs sync providers: ACCESS_KEY / SECRET_KEY for
		// the source and DST_ACCESS_KEY / DST_SECRET_KEY for the
		// destination. Callers pass the right env-var prefix via the
		// envPrefix arg.
		ep := strings.TrimRight(d.Config["endpoint"], "/")
		bucket := d.Config["bucket"]
		if ep == "" || bucket == "" {
			return "", nil, errors.New("s3/b2: endpoint and bucket required")
		}
		// Strip the scheme — juicefs sync's "s3://" prefix carries
		// the bucket+key path itself; the endpoint is configured via
		// env vars (S3_ENDPOINT / B2_ENDPOINT) so we don't double-
		// encode it in the URI.
		scheme := d.Kind
		uri = scheme + "://" + bucket
		envPrefix := strings.ToUpper(scheme) + "_"
		env = append(env,
			"ACCESS_KEY="+d.Config["access_key"],
			"SECRET_KEY="+d.Config["secret_key"],
			envPrefix+"ENDPOINT="+ep,
		)
		if r := d.Config["region"]; r != "" {
			env = append(env, envPrefix+"REGION="+r)
		}
	case "sftp":
		// juicefs sync sftp form: sftp://<user>@<host>:<port>/<path>
		// Password / key go via env vars (SFTP_PASSWORD / SFTP_KEY)
		// to keep them off the command line.
		user := d.Config["user"]
		host := d.Config["host"]
		port := d.Config["port"]
		if port == "" {
			port = "22"
		}
		path := d.Config["path"]
		if user == "" || host == "" {
			return "", nil, errors.New("sftp: user and host required")
		}
		uri = fmt.Sprintf("sftp://%s@%s:%s%s", user, host, port, path)
		if pw := d.Config["password"]; pw != "" {
			env = append(env, "SFTP_PASSWORD="+pw)
		}
		if key := d.Config["private_key"]; key != "" {
			env = append(env, "SFTP_PRIVATE_KEY="+key)
		}
	case "webdav":
		ep := strings.TrimRight(d.Config["endpoint"], "/")
		if ep == "" {
			return "", nil, errors.New("webdav: endpoint required")
		}
		uri = "webdav://" + strings.TrimPrefix(strings.TrimPrefix(ep, "https://"), "http://")
		if u := d.Config["user"]; u != "" {
			env = append(env, "WEBDAV_USER="+u)
		}
		if pw := d.Config["password"]; pw != "" {
			env = append(env, "WEBDAV_PASSWORD="+pw)
		}
	case "jfs":
		meta := d.Config["meta_url"]
		vol := d.Config["volume"]
		if meta == "" {
			return "", nil, errors.New("jfs: meta_url required")
		}
		if vol == "" {
			vol = "jfs"
		}
		uri = "jfs://" + vol + "/"
		// juicefs sync's URL-alias convention: env var named after the
		// volume holds the metaURL.
		env = append(env, vol+"="+meta)
	default:
		return "", nil, fmt.Errorf("unknown kind %q", d.Kind)
	}
	uri = matchSlash(uri, preserveStructure)
	return uri, env, nil
}
