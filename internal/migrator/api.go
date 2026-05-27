package migrator

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//go:embed static
var staticFS embed.FS

// API holds the HTTP handlers and their shared state.
type API struct {
	jobs        *JobManager
	sourceRoots []string // allowable host paths under /browse
	destMount   string   // user-facing prefix used in destination paths (e.g. /jfs)
	adminKey    string   // empty = no auth
	prefix      string   // route-mount prefix (e.g. "/migrator"); empty for standalone
	fuseMount   string   // for ModeEmbedded dest-traversal check; empty in standalone
	volName     string   // for ModeStandalone dest-validation
}

// Config bundles the fields needed to construct + register the API.
//
// Destination-write mode: pick ONE of (FUSEMount) or (MetaURL+VolName).
//
//   - FUSEMount set → embedded mode. Writes go through the in-process
//     juicefs FUSE mount at file:///<FUSEMount>/<path>. Used when the
//     migrator runs inside juicemount-server which already has the
//     volume mounted.
//
//   - MetaURL + VolName set → standalone mode. Writes go through
//     jfs://<VolName>/<path> with VolName=MetaURL set as an env var
//     (juicefs sync's URL-alias convention). Used when the migrator
//     runs as its own container without a local FUSE mount.
type Config struct {
	JuiceFSBin  string   // path to juicefs binary (or "juicefs" for PATH lookup)
	FUSEMount   string   // embedded mode: in-process FUSE mount path
	MetaURL     string   // standalone mode: redis://host:port/db
	VolName     string   // standalone mode: JuiceFS volume name (e.g. "zpool")
	SourceRoots []string // host paths the user is allowed to browse from
	DestMount   string   // user-facing destination prefix (e.g. /jfs)
	AdminKey    string   // empty = no auth (LAN-only)
}

// Register wires the migrator's routes onto an existing ServeMux at
// the given prefix (e.g. "/migrator"). Returns the JobManager so
// callers can attach lifecycle hooks (StopAll on shutdown).
//
// Static UI is served from <prefix>/, JSON API from <prefix>/api/...
func Register(mux *http.ServeMux, prefix string, cfg Config) *JobManager {
	prefix = strings.TrimSuffix(prefix, "/")
	// Derive the RunSync spec from the Config's destination-mode fields.
	// Embedded (FUSEMount) takes precedence; falls back to standalone
	// (MetaURL+VolName) if FUSEMount is unset.
	spec := RunSyncSpec{}
	if cfg.FUSEMount != "" {
		spec.Mode = ModeEmbedded
		spec.FUSEMount = cfg.FUSEMount
	} else {
		spec.Mode = ModeStandalone
		spec.MetaURL = cfg.MetaURL
		spec.VolName = cfg.VolName
	}
	mgr := NewJobManager(cfg.JuiceFSBin, spec)
	a := &API{
		jobs:        mgr,
		sourceRoots: cfg.SourceRoots,
		destMount:   cfg.DestMount,
		adminKey:    cfg.AdminKey,
		prefix:      prefix,
		fuseMount:   cfg.FUSEMount,
		volName:     cfg.VolName,
	}
	mux.HandleFunc(prefix+"/api/sources", a.auth(a.handleSources))
	mux.HandleFunc(prefix+"/api/browse", a.auth(a.handleBrowse))
	mux.HandleFunc(prefix+"/api/preview", a.auth(a.handlePreview))
	mux.HandleFunc(prefix+"/api/migrate", a.auth(a.handleMigrate))
	mux.HandleFunc(prefix+"/api/jobs", a.auth(a.handleListJobs))
	mux.HandleFunc(prefix+"/api/jobs/", a.auth(a.handleJobOps))
	// Static UI: serve <prefix>/ and <prefix>/<file>. Strip prefix so
	// the existing handleStatic logic still works.
	staticHandler := http.StripPrefix(prefix, http.HandlerFunc(a.handleStatic))
	mux.Handle(prefix+"/", staticHandler)
	return mgr
}

// auth wraps a handler with X-JuiceMount-Admin-Key check.
// Empty configured key disables auth (LAN-only / dev mode).
//
// Also accepts the key via a `?key=...` query parameter — needed for
// the EventSource API which can't set custom HTTP headers from
// JavaScript. The query param is only consulted when the header is
// absent or empty.
//
// **Known limitation (Rule 4 MEDIUM finding):** the metrics HTTP
// server itself does not log request URIs, but a TLS-terminating
// proxy in front of this server (nginx, Caddy, Traefik) typically
// logs full URIs by default — including the `?key=` value. Operators
// behind a logging proxy must either: (a) disable access-log query-
// string capture for /api/jobs/*/stream, or (b) only expose this
// service on the LAN behind a non-logging proxy. Documented in
// `docs/OPEN_BUGS.md` for a future fix (issue: POST-then-stream
// ticket exchange so the key never traverses a URL).
func (a *API) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.adminKey == "" {
			next(w, r)
			return
		}
		got := r.Header.Get("X-JuiceMount-Admin-Key")
		if got == "" {
			got = r.URL.Query().Get("key")
		}
		if got != a.adminKey {
			http.Error(w, "missing or invalid X-JuiceMount-Admin-Key", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// handleSources returns the configured source roots the user can browse.
func (a *API) handleSources(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"sources":     a.sourceRoots,
		"destination": a.destMount,
	})
}

// handleBrowse lists entries under ?path=... and resolves the path
// against the configured sourceRoots to prevent directory traversal.
func (a *API) handleBrowse(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	if !a.pathAllowed(path) {
		http.Error(w, "path outside permitted source roots", http.StatusForbidden)
		return
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	type entry struct {
		Name  string `json:"name"`
		IsDir bool   `json:"is_dir"`
		Size  int64  `json:"size"`
	}
	out := make([]entry, 0, len(entries))
	for _, e := range entries {
		// Hide dotfiles to reduce noise; user can browse a dotfile
		// dir by typing the path directly.
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, entry{
			Name:  e.Name(),
			IsDir: e.IsDir(),
			Size:  info.Size(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": path, "entries": out})
}

// migrateRequest is the body of POST /api/migrate. Options is optional;
// missing fields fall back to DefaultSyncOptions().
type migrateRequest struct {
	Source      string       `json:"source"`
	Destination string       `json:"destination"`
	Options     *SyncOptions `json:"options,omitempty"`
}

func (a *API) handleMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req migrateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Source == "" || req.Destination == "" {
		http.Error(w, "source and destination are required", http.StatusBadRequest)
		return
	}
	if !a.pathAllowed(req.Source) {
		http.Error(w, "source outside permitted source roots", http.StatusForbidden)
		return
	}
	// Destination security gates (Rule 4 HIGH findings):
	//   1. Reject any user-supplied URL scheme. Only path-style
	//      destinations are accepted; the runner converts them to
	//      file:// or jfs:// internally. Otherwise a client with
	//      the admin key could exfiltrate to s3://attacker-bucket/.
	//   2. Reject any path containing `..` segments. Clean the path
	//      and verify it still starts with destMount. Otherwise
	//      "/jfs/../../../tmp/evil" escapes the JuiceFS volume into
	//      the host filesystem.
	dest := strings.TrimSpace(req.Destination)
	if i := strings.Index(dest, "://"); i > 0 && i < 10 {
		http.Error(w, "destination must be a path under "+a.destMount+", not a URL", http.StatusBadRequest)
		return
	}
	if dest == "" || !strings.HasPrefix(dest, "/") {
		http.Error(w, "destination must be an absolute path", http.StatusBadRequest)
		return
	}
	cleaned := filepath.Clean(dest)
	dmClean := filepath.Clean(a.destMount)
	if cleaned != dmClean && !strings.HasPrefix(cleaned, dmClean+"/") {
		http.Error(w, "destination must be under "+a.destMount+" (no parent-dir traversal)", http.StatusForbidden)
		return
	}
	// At this point `cleaned` is safe: absolute, no `..`, under destMount.
	dest = cleaned
	if dest == dmClean {
		dest = filepath.Join(dmClean, "imported", time.Now().UTC().Format("20060102-150405"))
	}
	opts := DefaultSyncOptions()
	if req.Options != nil {
		opts = *req.Options
	}
	job, err := a.jobs.Submit(req.Source, dest, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (a *API) handleListJobs(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.jobs.List())
}

// handlePreview walks the source path (file or directory) and returns
// aggregate stats: total file count, total size, top file extensions.
// Bounded by maxPreviewEntries to avoid hanging on enormous trees.
const maxPreviewEntries = 50000

func (a *API) handlePreview(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	if !a.pathAllowed(path) {
		http.Error(w, "path outside permitted source roots", http.StatusForbidden)
		return
	}
	type preview struct {
		Files       int64            `json:"files"`
		Directories int64            `json:"directories"`
		Bytes       int64            `json:"bytes"`
		ExtCounts   map[string]int64 `json:"ext_counts"` // .mp4 → 142, .mov → 38, ...
		Truncated   bool             `json:"truncated"`
	}
	out := preview{ExtCounts: map[string]int64{}}

	var walk func(p string) error
	visited := int64(0)
	walk = func(p string) error {
		if visited >= maxPreviewEntries {
			out.Truncated = true
			return filepath.SkipAll
		}
		entries, err := os.ReadDir(p)
		if err != nil {
			return err
		}
		for _, e := range entries {
			// Skip hidden entries entirely — matches the junk-filter
			// the sync runner applies AND keeps the visited counter
			// honest. Previously dotfile-heavy trees (e.g. Time
			// Machine backups full of ._ sidecars) would burn through
			// the 50k limit returning truncated=true alongside
			// files=0, which looked like an empty directory in the UI.
			if strings.HasPrefix(e.Name(), ".") {
				continue
			}
			visited++
			if visited >= maxPreviewEntries {
				out.Truncated = true
				return filepath.SkipAll
			}
			full := filepath.Join(p, e.Name())
			info, err := e.Info()
			if err != nil {
				continue
			}
			if e.IsDir() {
				out.Directories++
				if err := walk(full); err == filepath.SkipAll {
					return err
				}
			} else {
				out.Files++
				out.Bytes += info.Size()
				ext := strings.ToLower(filepath.Ext(e.Name()))
				if ext == "" {
					ext = "(no extension)"
				}
				out.ExtCounts[ext]++
			}
		}
		return nil
	}
	// If the path itself is a file, return single-file stats.
	info, err := os.Stat(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !info.IsDir() {
		out.Files = 1
		out.Bytes = info.Size()
		ext := strings.ToLower(filepath.Ext(path))
		if ext == "" {
			ext = "(no extension)"
		}
		out.ExtCounts[ext] = 1
	} else {
		_ = walk(path)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleJobOps routes /api/jobs/{id} and /api/jobs/{id}/stream.
// Uses a.prefix to strip the mount prefix in embedded mode — without
// this, r.URL.Path is "/migrator/api/jobs/..." and TrimPrefix against
// "/api/jobs/" returns the whole path unchanged, causing every job-
// specific endpoint to return 400. Caught by Rule 4 review.
func (a *API) handleJobOps(w http.ResponseWriter, r *http.Request) {
	suffix := strings.TrimPrefix(r.URL.Path, a.prefix+"/api/jobs/")
	if suffix == r.URL.Path {
		// TrimPrefix is a no-op when prefix doesn't match → bug.
		// Fall back to the unprefixed form for safety in standalone
		// mode (a.prefix == "" already, but defensive).
		suffix = strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	}
	if suffix == "" {
		http.Error(w, "job id required", http.StatusBadRequest)
		return
	}
	parts := strings.SplitN(suffix, "/", 2)
	id := parts[0]
	subpath := ""
	if len(parts) > 1 {
		subpath = parts[1]
	}

	switch {
	case r.Method == http.MethodGet && subpath == "":
		j := a.jobs.Get(id)
		if j == nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, j)
	case r.Method == http.MethodGet && subpath == "stream":
		a.streamJob(w, r, id)
	case r.Method == http.MethodDelete && subpath == "":
		ok := a.jobs.Cancel(id)
		if !ok {
			http.Error(w, "job not found or not cancellable", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// streamJob fans out ProgressEvent over Server-Sent Events.
func (a *API) streamJob(w http.ResponseWriter, r *http.Request, id string) {
	ch, cleanup, ok := a.jobs.Subscribe(id)
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	defer cleanup()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, _ := w.(http.Flusher)

	for {
		select {
		case ev, more := <-ch:
			if !more {
				// Channel closed → job is in a terminal state.
				return
			}
			b, _ := json.Marshal(ev)
			fmt.Fprintf(w, "data: %s\n\n", b)
			if flusher != nil {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

// handleStatic serves the embedded UI assets.
func (a *API) handleStatic(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}
	data, err := staticFS.ReadFile("static/" + path)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	switch filepath.Ext(path) {
	case ".html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case ".js":
		w.Header().Set("Content-Type", "application/javascript")
	case ".css":
		w.Header().Set("Content-Type", "text/css")
	}
	_, _ = w.Write(data)
}

// pathAllowed returns true iff `p` is under one of the configured
// source roots after symlink resolution. Prevents path-traversal
// attacks via "../" segments.
func (a *API) pathAllowed(p string) bool {
	clean, err := filepath.Abs(p)
	if err != nil {
		return false
	}
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		// If the path doesn't exist yet (legal for browse fallback),
		// fall back to the cleaned absolute path.
		resolved = clean
	}
	for _, root := range a.sourceRoots {
		rootAbs, _ := filepath.Abs(root)
		rootResolved, err := filepath.EvalSymlinks(rootAbs)
		if err != nil {
			rootResolved = rootAbs
		}
		if resolved == rootResolved || strings.HasPrefix(resolved, rootResolved+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(body)
}

// Compile-time check that the embedded FS resolved.
var _ fs.FS = staticFS
