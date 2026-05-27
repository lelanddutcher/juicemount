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
}

// Config bundles the fields needed to construct + register the API.
type Config struct {
	JuiceFSBin  string   // path to juicefs binary (or "juicefs" for PATH lookup)
	FUSEMount   string   // in-process JuiceFS FUSE mount path (e.g. /mnt/juicefs)
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
	mgr := NewJobManager(cfg.JuiceFSBin, cfg.FUSEMount)
	a := &API{
		jobs:        mgr,
		sourceRoots: cfg.SourceRoots,
		destMount:   cfg.DestMount,
		adminKey:    cfg.AdminKey,
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
// absent or empty; never logged.
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
	// Destination defaults to /jfs/imported/<timestamp> if user just
	// gave the JuiceFS mount root.
	dest := req.Destination
	if dest == a.destMount || dest == a.destMount+"/" {
		dest = filepath.Join(a.destMount, "imported", time.Now().UTC().Format("20060102-150405"))
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
			visited++
			if visited >= maxPreviewEntries {
				out.Truncated = true
				return filepath.SkipAll
			}
			// Skip hidden entries from the count too — matches the
			// junk-filter the sync runner applies.
			if strings.HasPrefix(e.Name(), ".") {
				continue
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
func (a *API) handleJobOps(w http.ResponseWriter, r *http.Request) {
	suffix := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
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
