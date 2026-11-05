package manager

// discovery.go: continuity slice 6a, finding files (experimental, behind
// JM_CONTINUITY like collections; with it off every route is 404).
//
//	GET /api/continuity/find         files matching filters, a page at a time
//	GET /api/continuity/asset/{id}   one file by asset ID
//
// Who. The admin key, a person's Mac, or an agent token (agenttokens.go),
// with the budgets and the volume-read slot of collections: both read the
// volume, so both take the actor's slot (shared by a person's Mac and
// agents).
//
// What a caller sees. The admin sees every file. A person (or their agent)
// sees only files whose current path their project grants cover (a person
// with access "all" sees every file; one with no write access sees none).
// This is stricter than saved-query results and collection members, which
// any authenticated caller reads whole (Teams v1, as through the mount):
// finding files is scoped to the grants, as the agent decision asks. A file
// outside the grants is left out of a page, and by ID it is 404, as if it
// did not exist (also when its path cannot be read and its hint is outside
// the grants). For a person limited to grants, path_hint is shown only
// inside them and label only when it is the file's current name: a stale
// hint or a first name may come from outside. Agents read collection
// members and query results under the same rule (the rest is withheld,
// counted without names); Macs read those whole.
//
// Filters (all optional, combined with AND): name (a fragment of the file
// name, ignoring case, compared in Unicode NFC as queries do), folder (one
// or more volume folders, a file anywhere below one; repeat the parameter),
// collection (a collection ID: its active members only), modified_after (the
// file's modification time; RFC 3339 or unix ms), seen_after (a catalog
// sequence: files whose path hint the catalog recorded after it, i.e. bound,
// moved or saved in place since), ext and kind (extensions, with or without
// the dot, and named sets of them: video, audio, image, project, document;
// a file matches any of them). Files under a smart-folder root the Manager
// owns are never listed (they are its links).
//
// How. Candidates are the catalog's path hints (one per bound file), read in
// location ID order from the cursor; the name, folder, extension, sequence,
// collection and grant filters are applied to the hint, and each remaining
// candidate is resolved on the volume by its inode (its live path,
// modification time and size), where every filter is applied again to the
// live path. As for queries, a file whose hint is stale (its folder renamed
// with no event per file) is judged by the hint until its next event. A
// file whose path cannot be read is counted in unresolved and skipped.
//
// Pages. At most limit results (default 50, at most 200), and per request
// at most findScanBudget hints read and findResolveBudget files resolved,
// within the 30 s request deadline. The answer's next is the cursor to
// continue from (the last hint examined); a page cut short by the budget
// says budget_exhausted and may hold fewer results than limit, even none,
// and still have a next. Without next the scan is complete. The order is
// the catalog's and never changes, so paging neither repeats nor skips a
// file that stays put; a file bound or moved while paging may appear on a
// later page or be missed until the next scan from the start.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/unicode/norm"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const (
	findDefaultLimit = 50
	findMaxLimit     = 200
	// findCollectionsPerFile bounds the collections listed with one file.
	findCollectionsPerFile = 50
)

var (
	// Per-request budgets (variables for tests): hints read from the
	// catalog, files resolved on the volume.
	findScanBudget    = 100_000
	findResolveBudget = 1_000

	// findBeforeStat runs before each volume resolution (tests).
	findBeforeStat func()

	findCursorRe = regexp.MustCompile(`^jfs-[0-9A-Za-z-]{1,64}-[0-9]{1,20}(-g[0-9]{1,9})?-hint$`)

	// findKinds: the extensions each kind stands for.
	findKinds = map[string][]string{
		"video":    {"mov", "mp4", "m4v", "mxf", "avi", "mkv", "mts", "m2ts", "r3d", "braw", "ari", "crm", "webm"},
		"audio":    {"wav", "aif", "aiff", "mp3", "m4a", "aac", "flac", "bwf"},
		"image":    {"jpg", "jpeg", "png", "tif", "tiff", "dng", "exr", "dpx", "psd", "heic", "gif", "cr2", "cr3", "nef", "arw"},
		"project":  {"prproj", "drp", "fcpxml", "aep", "aaf", "edl", "otio"},
		"document": {"pdf", "doc", "docx", "txt", "rtf", "md", "pages", "xlsx", "csv"},
	}
)

// FileView is one file as discovery returns it.
type FileView struct {
	AssetID continuity.AssetID `json:"asset_id"`
	// Name: the file's current name. Label: the name it was first
	// catalogued with.
	Name  string `json:"name"`
	Label string `json:"label"`
	// PathHint: volume-relative, as the catalog last recorded it (stale
	// after a folder rename until the file's next event).
	PathHint string `json:"path_hint"`
	// Path: the file's current path under the mount root, with a leading
	// slash (a Mac prefixes its mount point), read from the volume now.
	Path        string           `json:"path"`
	Size        uint64           `json:"size"`
	Modified    time.Time        `json:"modified"`
	Collections []FileCollection `json:"collections"`
	// MoreCollections: it belongs to more than the collections listed.
	MoreCollections bool `json:"more_collections,omitempty"`
}

// FileCollection is a collection a file belongs to.
type FileCollection struct {
	ID   catalog.CollectionID `json:"id"`
	Name string               `json:"name"`
}

// findFilter is a parsed find request.
type findFilter struct {
	name          string // NFC, lower case
	folders       []string
	exts          map[string]bool
	members       map[continuity.AssetID]bool // nil: any file
	modifiedAfter time.Time
	seenAfter     int64
	roots         []string
	covers        func(string) bool
}

// pathMatches applies every path filter to a volume-relative path.
func (f *findFilter) pathMatches(p string) bool {
	top, _, _ := strings.Cut(p, "/")
	for _, r := range f.roots {
		if strings.EqualFold(top, r) {
			return false
		}
	}
	if len(f.folders) > 0 {
		in := false
		for _, d := range f.folders {
			if underFolder(p, d) && p != d {
				in = true
				break
			}
		}
		if !in {
			return false
		}
	}
	base := strings.ToLower(norm.NFC.String(path.Base(p)))
	if f.name != "" && !strings.Contains(base, f.name) {
		return false
	}
	if len(f.exts) > 0 && !f.exts[strings.TrimPrefix(path.Ext(base), ".")] {
		return false
	}
	return f.covers(p)
}

// hintMatches is the catalog-side check of a candidate.
func (f *findFilter) hintMatches(h binder.Hint) bool {
	if h.Sequence <= f.seenAfter || f.members != nil && !f.members[h.AssetID] {
		return false
	}
	return f.pathMatches(h.Path)
}

func parseFindTime(v string) (time.Time, error) {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return time.UnixMilli(n), nil
	}
	return time.Parse(time.RFC3339, v)
}

// findFilterOf parses the request's filters.
func (c *collectionCall) findFilterOf(q url.Values, roots []string) (*findFilter, error) {
	f := &findFilter{exts: map[string]bool{}, roots: roots, covers: c.actor.covers}
	if n := q.Get("name"); n != "" {
		if len(n) > catalog.MaxQueryNameContains || strings.ContainsAny(n, "/\x00") {
			return nil, fmt.Errorf("%w: name is at most %d bytes, without '/'", errCollectionInput, catalog.MaxQueryNameContains)
		}
		f.name = strings.ToLower(norm.NFC.String(n))
	}
	if len(q["folder"]) > catalog.MaxQueryPrefixes {
		return nil, fmt.Errorf("%w: at most %d folders", errCollectionInput, catalog.MaxQueryPrefixes)
	}
	for _, d := range q["folder"] {
		p := strings.TrimSuffix(strings.TrimPrefix(d, "/"), "/")
		if !catalog.ValidQueryPrefix(p) {
			return nil, fmt.Errorf("%w: folder %q is not a volume folder path", errCollectionInput, d)
		}
		f.folders = append(f.folders, p)
	}
	if len(q["ext"]) > catalog.MaxQueryExtensions {
		return nil, fmt.Errorf("%w: at most %d extensions", errCollectionInput, catalog.MaxQueryExtensions)
	}
	for _, e := range q["ext"] {
		e = strings.ToLower(strings.TrimPrefix(e, "."))
		if !catalog.ValidQueryExtension(e) {
			return nil, fmt.Errorf("%w: an extension is 1-%d letters or digits", errCollectionInput, catalog.MaxQueryExtensionBytes)
		}
		f.exts[e] = true
	}
	for _, k := range q["kind"] {
		exts, ok := findKinds[strings.ToLower(k)]
		if !ok {
			return nil, fmt.Errorf("%w: kind is video, audio, image, project or document", errCollectionInput)
		}
		for _, e := range exts {
			f.exts[e] = true
		}
	}
	var err error
	if v := q.Get("modified_after"); v != "" {
		if f.modifiedAfter, err = parseFindTime(v); err != nil {
			return nil, fmt.Errorf("%w: modified_after is RFC 3339 or unix ms", errCollectionInput)
		}
	}
	if v := q.Get("seen_after"); v != "" {
		if f.seenAfter, err = strconv.ParseInt(v, 10, 64); err != nil || f.seenAfter < 0 {
			return nil, fmt.Errorf("%w: seen_after is a catalog sequence (0 or more)", errCollectionInput)
		}
	}
	if v := q.Get("collection"); v != "" {
		id := catalog.CollectionID(v)
		if id.Validate() != nil {
			return nil, failf(http.StatusNotFound, "no such collection")
		}
		if _, err := c.active(id); err != nil {
			return nil, err
		}
		f.members = map[continuity.AssetID]bool{}
		var after continuity.AssetID
		for {
			page, err := c.svc.store.ListCollectionMembers(c.ctx(), c.library, id, after, catalog.MaxPageSize)
			if err != nil {
				return nil, err
			}
			for _, m := range page {
				f.members[m.AssetID] = true
			}
			if len(page) < catalog.MaxPageSize {
				break
			}
			after = page[len(page)-1].AssetID
		}
	}
	return f, nil
}

// Both read the volume, so both take the actor's slot.
func discoveryReads(string, []string) bool { return true }

func (h *collectionsAPI) serveFind(w http.ResponseWriter, r *http.Request) {
	h.serveWith(w, r, "/api/continuity/find", discoveryReads, h.routeFind)
}

func (h *collectionsAPI) serveAsset(w http.ResponseWriter, r *http.Request) {
	h.serveWith(w, r, "/api/continuity/asset", discoveryReads, h.routeAsset)
}

// discoveryCall checks the method and builds the call.
func (h *collectionsAPI) discoveryCall(w http.ResponseWriter, r *http.Request, svc *continuityService, actor collectionActor) (*collectionCall, error) {
	if r.Method != http.MethodGet {
		return nil, failf(http.StatusMethodNotAllowed, "GET only")
	}
	library, err := svc.store.LibraryID()
	if err != nil {
		return nil, err
	}
	return &collectionCall{svc: svc, actor: actor, library: library, w: w, r: r, audit: h.audit}, nil
}

func (h *collectionsAPI) routeFind(w http.ResponseWriter, r *http.Request, svc *continuityService, actor collectionActor, parts []string) error {
	if len(parts) != 0 {
		return failf(http.StatusNotFound, "no such route")
	}
	c, err := h.discoveryCall(w, r, svc, actor)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	limit := findDefaultLimit
	if v := q.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > findMaxLimit {
			return fmt.Errorf("%w: limit is 1-%d", errCollectionInput, findMaxLimit)
		}
	}
	cursor := q.Get("cursor")
	if cursor != "" && !findCursorRe.MatchString(cursor) {
		return fmt.Errorf("%w: bad cursor", errCollectionInput)
	}
	f, err := c.findFilterOf(q, h.roots())
	if err != nil {
		return err
	}
	out := struct {
		Files      []FileView `json:"files"`
		Next       string     `json:"next,omitempty"`
		Scanned    int        `json:"scanned"`
		Resolved   int        `json:"resolved"`
		Unresolved int        `json:"unresolved,omitempty"`
		Budget     bool       `json:"budget_exhausted,omitempty"`
	}{Files: []FileView{}}
	if !actor.writesAnywhere() {
		// No grant covers anything: nothing to find.
		writeJSON(w, http.StatusOK, out)
		return nil
	}
	ctx := c.ctx()
	last, stopped := cursor, false
	var stepErr error
	scanErr := svc.binder.HintsAfter(ctx, cursor, func(hint binder.Hint) bool {
		if out.Scanned >= findScanBudget || ctx.Err() != nil {
			out.Budget, stopped = true, true
			return false
		}
		out.Scanned++
		if !f.hintMatches(hint) {
			last = hint.ID
			return true
		}
		if out.Resolved >= findResolveBudget {
			out.Scanned--
			out.Budget, stopped = true, true
			return false
		}
		out.Resolved++
		if findBeforeStat != nil {
			findBeforeStat()
		}
		st, err := svc.binder.Stat(ctx, hint.Ino, hint.Path)
		switch {
		case errors.Is(err, binder.ErrUnresolved):
			out.Unresolved++
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			out.Resolved--
			out.Scanned--
			out.Budget, stopped = true, true
			return false
		case err != nil:
			stepErr = err
			return false
		case st.Path != "" && f.pathMatches(st.Path) && (f.modifiedAfter.IsZero() || st.Mtime.After(f.modifiedAfter)):
			v, err := c.fileView(hint, st)
			if err != nil {
				stepErr = err
				return false
			}
			out.Files = append(out.Files, v)
		}
		last = hint.ID
		if len(out.Files) >= limit {
			stopped = true
			return false
		}
		return true
	})
	if scanErr == nil {
		scanErr = stepErr
	}
	if errors.Is(scanErr, catalog.ErrInvalid) {
		return fmt.Errorf("%w: bad cursor", errCollectionInput)
	}
	if errors.Is(scanErr, binder.ErrNotReady) {
		return failf(http.StatusServiceUnavailable, "%v", scanErr)
	}
	if scanErr != nil {
		return scanErr
	}
	if stopped {
		out.Next = last
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// fileView completes one result: its label and collections.
func (c *collectionCall) fileView(hint binder.Hint, st binder.FileStat) (FileView, error) {
	asset, err := c.svc.store.GetAsset(c.ctx(), c.library, hint.AssetID)
	if err != nil {
		return FileView{}, err
	}
	cols, err := c.svc.store.ListAssetCollections(c.ctx(), c.library, hint.AssetID, "", findCollectionsPerFile+1)
	if err != nil {
		return FileView{}, err
	}
	v := FileView{AssetID: hint.AssetID, Name: path.Base(st.Path), Label: asset.Label, PathHint: hint.Path,
		Path: "/" + st.Path, Size: st.Size, Modified: st.Mtime.UTC(), Collections: []FileCollection{}}
	if c.actor.person != nil && c.actor.person.Access != personAccessAll {
		// The live path is inside the grants; a stale hint, or the name the
		// file was first catalogued with, may come from outside them.
		if !c.actor.covers(hint.Path) {
			v.PathHint = ""
		}
		if v.Label != v.Name {
			v.Label = ""
		}
	}
	if len(cols) > findCollectionsPerFile {
		cols, v.MoreCollections = cols[:findCollectionsPerFile], true
	}
	for _, col := range cols {
		v.Collections = append(v.Collections, FileCollection{col.ID, col.Name})
	}
	return v, nil
}

func (h *collectionsAPI) routeAsset(w http.ResponseWriter, r *http.Request, svc *continuityService, actor collectionActor, parts []string) error {
	if len(parts) != 1 || continuity.AssetID(parts[0]).Validate() != nil {
		return failf(http.StatusNotFound, "no such file")
	}
	c, err := h.discoveryCall(w, r, svc, actor)
	if err != nil {
		return err
	}
	id := continuity.AssetID(parts[0])
	notFound := failf(http.StatusNotFound, "no such file, or it is no longer on the volume")
	hint, err := svc.binder.AssetHint(c.ctx(), id)
	switch {
	case errors.Is(err, catalog.ErrNotFound):
		return notFound
	case errors.Is(err, binder.ErrNotReady):
		return failf(http.StatusServiceUnavailable, "%v", err)
	case err != nil:
		return err
	}
	st, err := svc.binder.Stat(c.ctx(), hint.Ino, hint.Path)
	switch {
	case errors.Is(err, binder.ErrUnresolved) && !actor.covers(hint.Path):
		return notFound // do not say a file outside the grants exists
	case errors.Is(err, binder.ErrUnresolved):
		return failf(http.StatusServiceUnavailable, "cannot read the file's path on the volume; try again")
	case err != nil:
		return err
	case st.Path == "" || !actor.covers(st.Path):
		return notFound
	}
	v, err := c.fileView(hint, st)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, v)
	return nil
}
