package manager

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
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
	prefix      string   // route-mount prefix (e.g. "/manager"); empty for standalone
	fuseMount   string   // for ModeEmbedded dest-traversal check; empty in standalone
	volName     string   // for ModeStandalone dest-validation

	// overview is the SLICE-2 fan-out aggregator. Nil only in unit
	// tests that hand-construct an API without going through Register
	// (the handler defensively returns an "overview not configured"
	// snapshot in that case rather than NPE'ing).
	overview *overviewSource
}

// Config bundles the fields needed to construct + register the API.
//
// Destination-write mode: pick ONE of (FUSEMount) or (MetaURL+VolName).
//
//   - FUSEMount set → embedded mode. Writes go through the in-process
//     juicefs FUSE mount at file:///<FUSEMount>/<path>. Used when the
//     manager runs inside juicemount-server which already has the
//     volume mounted.
//
//   - MetaURL + VolName set → standalone mode. Writes go through
//     jfs://<VolName>/<path> with VolName=MetaURL set as an env var
//     (juicefs sync's URL-alias convention). Used when the manager
//     runs as its own container without a local FUSE mount.
type Config struct {
	JuiceFSBin  string   // path to juicefs binary (or "juicefs" for PATH lookup)
	FUSEMount   string   // embedded mode: in-process FUSE mount path
	MetaURL     string   // standalone mode: redis://host:port/db
	VolName     string   // standalone mode: JuiceFS volume name (e.g. "zpool")
	SourceRoots []string // host paths the user is allowed to browse from
	DestMount   string   // user-facing destination prefix (e.g. /jfs)
	AdminKey    string   // empty = no auth (LAN-only)
	StateFile   string   // optional JSON path for job-history persistence (empty = ephemeral)
	// MinIOURL is the http endpoint the SLICE-2 Overview tab pings via
	// /minio/health/live. Optional — when empty the MinIO card on the
	// dashboard renders an "endpoint not configured" hint rather than a
	// false-green reachable state.
	MinIOURL string
	// OverviewMetaURL is the Redis URL the SLICE-2 Overview tab uses for
	// `juicefs status` + Redis INFO probes when running in embedded
	// mode (where MetaURL above stays unset because the FUSE mount
	// handles writes). When OverviewMetaURL is empty we fall back to
	// MetaURL — useful for standalone mode where the one URL serves
	// both purposes.
	OverviewMetaURL string
}

// Register wires the manager's routes onto an existing ServeMux at
// the given prefix (e.g. "/manager"). Returns the JobManager so
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
	mgr.SetStateFile(cfg.StateFile)
	a := &API{
		jobs:        mgr,
		sourceRoots: cfg.SourceRoots,
		destMount:   cfg.DestMount,
		adminKey:    cfg.AdminKey,
		prefix:      prefix,
		fuseMount:   cfg.FUSEMount,
		volName:     cfg.VolName,
	}
	// SLICE 2: wire the overview aggregator. Picks OverviewMetaURL when
	// set (embedded mode passes it explicitly so the dashboard can probe
	// Redis even though writes go through FUSE), falls back to MetaURL
	// (standalone mode). Either may be empty in which case the per-
	// section probes emit an "metaURL not configured" Error and the
	// dashboard shows an actionable hint instead of bogus data.
	overviewMeta := cfg.OverviewMetaURL
	if overviewMeta == "" {
		overviewMeta = cfg.MetaURL
	}
	a.overview = newOverviewSource(mgr, cfg.JuiceFSBin, overviewMeta, cfg.MinIOURL, cfg.FUSEMount)
	mux.HandleFunc(prefix+"/api/sources", a.auth(a.handleSources))
	mux.HandleFunc(prefix+"/api/browse", a.auth(a.handleBrowse))
	// SLICE 1: /api/browse-jfs walks the JuiceFS FUSE mount tree.
	// Needed when the user picks DirectionOut / DirectionBetween in
	// the Migrations tab — the source picker browses /jfs/ instead of
	// /sources/. Path-traversal guard is the same pathAllowed pattern,
	// only checked against the fuseMount root.
	mux.HandleFunc(prefix+"/api/browse-jfs", a.auth(a.handleBrowseJFS))
	mux.HandleFunc(prefix+"/api/preview", a.auth(a.handlePreview))
	mux.HandleFunc(prefix+"/api/resolve-destination", a.auth(a.handleResolveDestination))
	mux.HandleFunc(prefix+"/api/migrate", a.auth(a.handleMigrate))
	mux.HandleFunc(prefix+"/api/jobs", a.auth(a.handleListJobs))
	mux.HandleFunc(prefix+"/api/jobs/", a.auth(a.handleJobOps))
	// SLICE 2: read-only Overview dashboard aggregator. Always returns
	// 200 — per-backend failures surface in OverviewSnapshot.<section>.Error
	// so a hung Redis doesn't break the entire dashboard. Auth-wrapped
	// like every other endpoint (no auth bypass).
	mux.HandleFunc(prefix+"/api/overview", a.auth(a.handleOverview))
	// SLICE 3: Trash tab — list/restore/delete/empty/config.
	// /api/trash/empty enforces a typed-confirmation header
	// (X-Confirm-Empty: yes) server-side so a typo'd curl can't wipe
	// a week of retention; /api/trash/config GET reads the current
	// --trash-days value and PUT updates it via `juicefs config`.
	// All endpoints are auth-wrapped like every other manager route.
	mux.HandleFunc(prefix+"/api/trash/list", a.auth(a.handleTrashList))
	mux.HandleFunc(prefix+"/api/trash/restore", a.auth(a.handleTrashRestore))
	mux.HandleFunc(prefix+"/api/trash/delete", a.auth(a.handleTrashDelete))
	mux.HandleFunc(prefix+"/api/trash/empty", a.auth(a.handleTrashEmpty))
	mux.HandleFunc(prefix+"/api/trash/config", a.auth(a.handleTrashConfig))
	// Static UI: serve <prefix>/ and <prefix>/<file>. Strip prefix so
	// the existing handleStatic logic still works.
	staticHandler := http.StripPrefix(prefix, http.HandlerFunc(a.handleStatic))
	mux.Handle(prefix+"/", staticHandler)
	return mgr
}

// RedirectMigrator returns an http.HandlerFunc that 301-redirects any
// request under the legacy /migrator/* prefix to the corresponding
// /manager/* path, preserving the request's path tail and query
// string. SLICE 0 of the manager roadmap renamed the HTTP mount
// prefix from /migrator/ to /manager/; this redirect is the one-
// release compatibility shim so bookmarks, scripts, and Mac-side
// shortcuts that still point at /migrator/ keep working.
//
// Permanent (301) so browsers and intermediaries can cache it; the
// redirect target itself is stable across the compat period.
func RedirectMigrator() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Strip the legacy prefix and re-attach the new one. We
		// preserve the path tail verbatim (including a trailing
		// slash) so /migrator/ → /manager/ and /migrator/api/sources
		// → /manager/api/sources without surprises.
		target := "/manager"
		tail := strings.TrimPrefix(r.URL.Path, "/migrator")
		if tail != "" {
			target += tail
		} else {
			target += "/"
		}
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		http.Redirect(w, r, target, http.StatusMovedPermanently)
	}
}

// redirectMigrator is the method form bound to an API receiver. It
// delegates to the package-level RedirectMigrator() — kept as a method
// so future slices can attach instance state (e.g. metrics) without
// changing the call site in jm5/main.go.
func (a *API) redirectMigrator(w http.ResponseWriter, r *http.Request) {
	RedirectMigrator()(w, r)
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

// handleBrowseJFS lists entries inside the JuiceFS FUSE mount tree.
// SLICE 1 introduces this for DirectionOut / DirectionBetween, where
// the source picker browses /jfs/ instead of /sources/.
//
// The user-facing path the UI shows starts with a.destMount (e.g.
// /jfs/...); on the wire to juicefs sync this is rewritten via
// normalizeAnyURI to file:///<FUSEMount>/... so the kernel mount
// handles the actual reads. handleBrowseJFS performs the same
// rewrite for its own os.ReadDir call — the dest-mount prefix is
// virtual, not a real path the manager process can stat. In
// standalone mode (no FUSE mount), browsing the JuiceFS tree from
// inside the manager isn't supported and the handler returns 501.
//
// Path traversal: jfsPathAllowed is the gate; same pattern as
// handleBrowse uses pathAllowed.
//
// Hidden entries are filtered out — matches handleBrowse's policy
// and avoids surfacing the .trash/ subtree here (that's SLICE 3's
// territory, with restore semantics that handleBrowseJFS shouldn't
// duplicate).
func (a *API) handleBrowseJFS(w http.ResponseWriter, r *http.Request) {
	if a.fuseMount == "" {
		http.Error(w, "browsing the JuiceFS tree requires embedded mode (FUSE mount)", http.StatusNotImplemented)
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		// Default to the volume root so the UI can boot the browser
		// without first knowing where /jfs lives. Mirrors the
		// "default to root" behavior /api/sources clients use.
		path = a.destMount
	}
	if !a.jfsPathAllowed(path) {
		http.Error(w, "path outside the JuiceFS volume", http.StatusForbidden)
		return
	}
	// Rewrite the /jfs/... user-facing prefix to the on-disk FUSE
	// mount path before stat'ing. Inverse of what the UI does when
	// the user clicks an entry: client sees /jfs/foo, server stats
	// /<FUSEMount>/foo.
	dmClean := filepath.Clean(a.destMount)
	cleaned := filepath.Clean(path)
	rel := strings.TrimPrefix(cleaned, dmClean)
	rel = strings.TrimPrefix(rel, "/")
	fuse := strings.TrimSuffix(a.fuseMount, "/")
	real := fuse
	if rel != "" {
		real = fuse + "/" + rel
	}
	entries, err := os.ReadDir(real)
	if err != nil {
		// Don't echo err.Error() — it contains the FUSE-resolved
		// absolute path, which leaks the internal mount layout to
		// authenticated clients. Log server-side, return generic.
		log.Printf("manager: browse-jfs ReadDir(%q) failed: %v", real, err)
		http.Error(w, "cannot list directory", http.StatusBadRequest)
		return
	}
	type entry struct {
		Name  string `json:"name"`
		IsDir bool   `json:"is_dir"`
		Size  int64  `json:"size"`
	}
	out := make([]entry, 0, len(entries))
	for _, e := range entries {
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
	// Return the user-facing path (the /jfs-prefixed form) so the UI
	// can build the next browse URL by appending "/<entry.name>"
	// without knowing the FUSE-mount path. The user-facing tree stays
	// the only thing the UI ever sees.
	writeJSON(w, http.StatusOK, map[string]any{"path": cleaned, "entries": out})
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
// missing fields fall back to DefaultSyncOptions(). TotalBytes is the
// pre-computed source size from the UI's preview pane and drives the
// progress bar's % display — 0 means "unknown, show indeterminate."
//
// Direction (SLICE 1) selects In / Out / Between. Empty string is
// treated as DirectionIn for backwards compatibility with pre-SLICE-1
// clients (jmctl scripts, existing UI bundles in the field, etc.) that
// don't know about the field.
type migrateRequest struct {
	Source      string       `json:"source"`
	Destination string       `json:"destination"`
	Direction   Direction    `json:"direction,omitempty"`
	Options     *SyncOptions `json:"options,omitempty"`
	TotalBytes  int64        `json:"total_bytes,omitempty"`
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
	dir := req.Direction
	if dir == "" {
		// Default for pre-SLICE-1 clients. validateDirectionPair also
		// recognizes "" as DirectionIn; setting it explicitly here keeps
		// later logic that switches on dir from needing the same check.
		dir = DirectionIn
	}

	source := strings.TrimSpace(req.Source)
	dest := strings.TrimSpace(req.Destination)

	// Reject scheme-prefixed strings on both sides. SLICE 0 only checked
	// destinations; SLICE 1 adds the same protection on the source side
	// because Out / Between sources are user-provided too (the user
	// picks a /jfs/... path in the browser).
	if i := strings.Index(source, "://"); i > 0 && i < 10 {
		http.Error(w, "source must be a path, not a URL", http.StatusBadRequest)
		return
	}
	if i := strings.Index(dest, "://"); i > 0 && i < 10 {
		http.Error(w, "destination must be a path, not a URL", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(source, "/") {
		http.Error(w, "source must be an absolute path", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(dest, "/") {
		http.Error(w, "destination must be an absolute path", http.StatusBadRequest)
		return
	}

	// Path-traversal guard for both sides. filepath.Clean removes `..`
	// segments; we then verify the cleaned path still starts with the
	// expected root (destMount for /jfs sides, any sourceRoot for host
	// sides). Without this, "/jfs/../../tmp/evil" would escape the
	// volume into the host filesystem. Mirrors SLICE-0's destination
	// check, but applied symmetrically.
	source = filepath.Clean(source)
	dest = filepath.Clean(dest)
	dmClean := filepath.Clean(a.destMount)

	// Direction-shape gate. Calls validateDirectionPair on the cleaned
	// paths so the rules see the canonical form (no /jfs/../foo
	// bypass).
	if err := validateDirectionPair(dir, source, dest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch dir {
	case DirectionIn:
		if !a.pathAllowed(source) {
			http.Error(w, "source outside permitted source roots", http.StatusForbidden)
			return
		}
		if dest != dmClean && !strings.HasPrefix(dest, dmClean+"/") {
			http.Error(w, "destination must be under "+a.destMount+" (no parent-dir traversal)", http.StatusForbidden)
			return
		}
		if dest == dmClean {
			dest = filepath.Join(dmClean, "imported", time.Now().UTC().Format("20060102-150405"))
		}
	case DirectionOut:
		// Source is /jfs/... — validate against the FUSE mount root.
		if !a.jfsPathAllowed(source) {
			http.Error(w, "source outside /jfs (FUSE mount)", http.StatusForbidden)
			return
		}
		// Destination is an external host path. Reuse the existing
		// pathAllowed (which gates against sourceRoots) — the same
		// bind-mounts that are valid sources of an In are valid
		// targets of an Out. Operators can add or restrict mounts by
		// editing the source-roots config.
		if !a.pathAllowed(dest) {
			http.Error(w, "destination outside permitted roots (Out direction writes to a configured source-root mount)", http.StatusForbidden)
			return
		}
	case DirectionBetween:
		// Unreachable: validateDirectionPair returns an error for
		// DirectionBetween in SLICE 1 (the "Configure a second JuiceFS
		// destination first" stub). The switch case is here for the
		// future SLICE-4 wiring; for now the validate call above
		// already returned 400.
		http.Error(w, "JuiceFS-to-JuiceFS migrations land in slice-4", http.StatusNotImplemented)
		return
	}

	opts := DefaultSyncOptions()
	if req.Options != nil {
		opts = *req.Options
	}
	job, err := a.jobs.Submit(source, dest, opts, req.TotalBytes, dir)
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
// 500k chosen so most personal libraries scan to completion (the prior
// 50k cap left root-level previews showing dramatically undercounted
// totals labeled as if they were final).
const maxPreviewEntries = 500000

func (a *API) handlePreview(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	// SLICE 1: previews for DirectionOut start at a /jfs/... path the
	// host filesystem can't stat directly. Rewrite to the FUSE-mount
	// path before the os.Stat call; the user-facing reply is the same
	// (numbers don't change). Path-traversal guard switches helper
	// based on which root the request targets.
	statPath := path
	if a.jfsPathAllowed(path) {
		if a.fuseMount == "" {
			http.Error(w, "previewing /jfs requires embedded mode (FUSE mount)", http.StatusNotImplemented)
			return
		}
		dmClean := filepath.Clean(a.destMount)
		rel := strings.TrimPrefix(filepath.Clean(path), dmClean)
		statPath = strings.TrimSuffix(a.fuseMount, "/") + rel
	} else if !a.pathAllowed(path) {
		http.Error(w, "path outside permitted source roots", http.StatusForbidden)
		return
	}
	path = statPath
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

// resolveDestRequest mirrors the JSON body of POST /api/resolve-destination.
// Direction (SLICE 1) controls which side gets the host-path-vs-/jfs
// check; empty string defaults to DirectionIn for backwards compat.
type resolveDestRequest struct {
	Source            string    `json:"source"`
	Destination       string    `json:"destination"`
	Direction         Direction `json:"direction,omitempty"`
	PreserveStructure bool      `json:"preserve_structure"`
}

// exampleMapping describes one source→destination path pair for the
// destination preview UI. Helps users see exactly where their files
// will land before they hit Start.
type exampleMapping struct {
	SourcePath string `json:"source"`
	DestPath   string `json:"destination"`
}

// resolveDestResponse is the body returned from /api/resolve-destination.
// SourceURL/DestinationURL are the literal arguments that will be
// handed to `juicefs sync` (so users can verify the trailing-slash
// agreement that juicefs FATALs on). ExampleMappings shows up to
// maxExamples source-relative file paths and the corresponding host-side
// destination path the user will see in Finder.
type resolveDestResponse struct {
	SourceURL       string           `json:"source_url"`
	DestinationURL  string           `json:"destination_url"`
	ExampleMappings []exampleMapping `json:"example_mappings"`
	Info            string           `json:"info"`
}

// handleResolveDestination computes the resolved sync URLs + a few
// example file mappings for the dest-preview UI block. Read-only.
func (a *API) handleResolveDestination(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req resolveDestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Source == "" || req.Destination == "" {
		http.Error(w, "source and destination are required", http.StatusBadRequest)
		return
	}
	dir := req.Direction
	if dir == "" {
		dir = DirectionIn
	}
	source := strings.TrimSpace(req.Source)
	dest := strings.TrimSpace(req.Destination)
	// Reject URL-scheme strings on both sides — same protection as
	// handleMigrate, so the preview shows the same constraints.
	if i := strings.Index(source, "://"); i > 0 && i < 10 {
		http.Error(w, "source must be a path, not a URL", http.StatusBadRequest)
		return
	}
	if i := strings.Index(dest, "://"); i > 0 && i < 10 {
		http.Error(w, "destination must be a path, not a URL", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(source, "/") {
		http.Error(w, "source must be an absolute path", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(dest, "/") {
		http.Error(w, "destination must be an absolute path", http.StatusBadRequest)
		return
	}
	source = filepath.Clean(source)
	dest = filepath.Clean(dest)
	dmClean := filepath.Clean(a.destMount)

	if err := validateDirectionPair(dir, source, dest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch dir {
	case DirectionIn:
		if !a.pathAllowed(source) {
			http.Error(w, "source outside permitted source roots", http.StatusForbidden)
			return
		}
		if dest != dmClean && !strings.HasPrefix(dest, dmClean+"/") {
			http.Error(w, "destination must be under "+a.destMount, http.StatusForbidden)
			return
		}
	case DirectionOut:
		if !a.jfsPathAllowed(source) {
			http.Error(w, "source outside /jfs (FUSE mount)", http.StatusForbidden)
			return
		}
		if !a.pathAllowed(dest) {
			http.Error(w, "destination outside permitted roots", http.StatusForbidden)
			return
		}
	case DirectionBetween:
		// Unreachable: validateDirectionPair already returned the
		// "configure a second JuiceFS destination first" stub error.
		http.Error(w, "JuiceFS-to-JuiceFS preview lands in slice-4", http.StatusNotImplemented)
		return
	}

	// Resolve to the exact URL forms RunSync will use, via the same
	// normalize helpers — no JS-side replica that could drift. Source
	// runs through normalizeAnyURI so /jfs/... sources (Out) get the
	// FUSE-mount rewrite; In direction sources fall through to the
	// plain file:// branch identically to normalizeSourceURI.
	srcURL := normalizeAnyURI(source, a.fuseMount, req.PreserveStructure)
	var destURL string
	switch dir {
	case DirectionIn:
		if a.fuseMount != "" {
			destURL = normalizeDestURIEmbedded(dest, a.fuseMount, req.PreserveStructure)
		} else {
			destURL = normalizeDestURIJFS(dest, a.volName, req.PreserveStructure)
		}
	case DirectionOut:
		// Out-direction dest is a host path, not a JuiceFS path.
		destURL = normalizeAnyURI(dest, "", req.PreserveStructure)
	}

	// Sample up to maxExamples files from the source tree. For a single
	// file, the sample is just that file. For a directory, walk shallow-
	// first and stop at maxExamples to keep the preview snappy.
	//
	// For DirectionOut, the source the UI shows is a /jfs/... path but
	// sampleSourceFiles needs to os.ReadDir on the actual FUSE-mount
	// path. We walk the FUSE-mount form and then rewrite the sampled
	// paths back to the /jfs/... shape so the UI's preview block
	// matches the path the user picked. Without this the preview would
	// either fail to stat (no such file in the host filesystem) or
	// leak the FUSE-mount path into the UI.
	walkRoot := source
	if dir == DirectionOut && a.fuseMount != "" {
		rel := strings.TrimPrefix(source, dmClean)
		walkRoot = strings.TrimSuffix(a.fuseMount, "/") + rel
	}
	samples := sampleSourceFiles(walkRoot, maxResolveExamples)
	mappings := make([]exampleMapping, 0, len(samples))
	for _, s := range samples {
		// Map the FUSE-mount-rooted sample back to the user-facing
		// /jfs/... prefix for the UI. For In, walkRoot == source so
		// the substitution is a no-op.
		displayPath := s
		if dir == DirectionOut && a.fuseMount != "" {
			displayPath = source + strings.TrimPrefix(s, walkRoot)
		}
		// rel is source-relative; same string is also the destination
		// suffix when preserve_structure=true (1:1 mapping). For
		// preserve_structure=false, juicefs sync prepends the source
		// basename, so the destination gains an extra path segment.
		rel := strings.TrimPrefix(s, walkRoot)
		rel = strings.TrimPrefix(rel, "/")
		var destPath string
		if req.PreserveStructure {
			destPath = filepath.Join(dest, rel)
		} else {
			// flatten-by-basename: juicefs sync without trailing slash
			// puts contents at <dst>/<basename-of-src>/<rel>.
			base := filepath.Base(source)
			destPath = filepath.Join(dest, base, rel)
		}
		mappings = append(mappings, exampleMapping{
			SourcePath: displayPath,
			DestPath:   destPath,
		})
	}

	info := "Files will be copied 1:1, preserving the folder structure under the source."
	if !req.PreserveStructure {
		info = "Files will be placed under " + dest + "/" + filepath.Base(source) + "/ — juicefs sync adds the source's basename as a parent directory in this mode."
	}

	writeJSON(w, http.StatusOK, resolveDestResponse{
		SourceURL:       srcURL,
		DestinationURL:  destURL,
		ExampleMappings: mappings,
		Info:            info,
	})
}

// maxResolveExamples caps the number of sample file paths returned by
// /api/resolve-destination so the preview stays cheap even on large
// trees. The UI shows ~3, so a few extras absorbs hidden/skipped files.
const maxResolveExamples = 5

// sampleSourceFiles walks `root` shallow-first and returns up to `limit`
// regular file paths, skipping dotfiles (which match the auto-junk
// filter). If `root` is a regular file, returns [root].
//
// Each discovered path is re-validated under the resolved root to
// prevent symlinked subdirectories from causing the preview to leak
// filenames from outside the configured source root. Caller has
// already validated `root` itself via pathAllowed; this layer guards
// against symlinks planted in writable source trees by other users.
func sampleSourceFiles(root string, limit int) []string {
	info, err := os.Stat(root)
	if err != nil {
		return nil
	}
	if !info.IsDir() {
		return []string{root}
	}
	rootResolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		rootResolved = root
	}
	underRoot := func(p string) bool {
		resolved, err := filepath.EvalSymlinks(p)
		if err != nil {
			return false
		}
		return resolved == rootResolved || strings.HasPrefix(resolved, rootResolved+string(filepath.Separator))
	}
	out := make([]string, 0, limit)
	// Breadth-first so the user sees representative files near the top
	// of the tree, not the alphabetically-first deep branch.
	queue := []string{root}
	for len(queue) > 0 && len(out) < limit {
		dir := queue[0]
		queue = queue[1:]
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		// Files first, then dirs, so we return file samples sooner.
		var subdirs []string
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".") {
				continue
			}
			full := filepath.Join(dir, e.Name())
			if !underRoot(full) {
				continue
			}
			if e.IsDir() {
				subdirs = append(subdirs, full)
			} else if len(out) < limit {
				out = append(out, full)
			}
		}
		queue = append(queue, subdirs...)
	}
	return out
}

// handleJobOps routes /api/jobs/{id} and /api/jobs/{id}/stream.
// Uses a.prefix to strip the mount prefix in embedded mode — without
// this, r.URL.Path is "/manager/api/jobs/..." and TrimPrefix against
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

// jfsPathAllowed returns true iff `p` is the JuiceFS user-facing
// prefix (a.destMount, e.g. "/jfs") or under it. Used for the
// SLICE-1 /api/browse-jfs handler and the DirectionOut source check
// — both need to confirm a user-supplied path resolves inside the
// JuiceFS volume rather than escaping to the host filesystem.
//
// Implementation note: we deliberately do NOT EvalSymlinks here.
// The JuiceFS FUSE mount exposes a virtual tree managed by the
// kernel + juicefs daemon; any symlinks inside it are user data,
// not host-filesystem links. The traversal protection comes from
// the filepath.Clean+prefix check, same pattern handleMigrate uses
// for its destination guard. The /jfs prefix itself is a fixed
// virtual path; nothing on the host filesystem owns it, so symlink
// resolution outside the kernel mount can't change what /jfs
// refers to.
func (a *API) jfsPathAllowed(p string) bool {
	cleaned := filepath.Clean(p)
	dm := filepath.Clean(a.destMount)
	return cleaned == dm || strings.HasPrefix(cleaned, dm+"/")
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
