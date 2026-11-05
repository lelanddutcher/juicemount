package agentmcp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

var (
	assetIDRe      = regexp.MustCompile(`^asset_[0-9a-f]{32}$`)
	collectionIDRe = regexp.MustCompile(`^col_[0-9a-f]{32}$`)
	queryIDRe      = regexp.MustCompile(`^qry_[0-9a-f]{32}$`)
	operationIDRe  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

const (
	// maxAssetsPerEdit: the Manager takes at most 255 members a request
	// (internal/manager maxCollectionRequestMembers).
	maxAssetsPerEdit = 255
	// writeAttempts: a write whose answer was lost, or that found the
	// Manager busy (503), is sent again with the same operation_id.
	writeAttempts = 3
)

// Tools runs the MCP tools against the Manager.
type Tools struct {
	// Mount is this Mac's mount point (CleanMount).
	Mount string
	// Client returns the Manager client of the stored agent token, or why
	// there is none (the tool call fails with that text).
	Client func() (*Client, error)
	Log    *log.Logger
	// Sleep paces write retries (tests replace it).
	Sleep func(time.Duration)
}

// Tool is one tool as tools/list describes it.
type Tool struct {
	Name        string          `json:"name"`
	Title       string          `json:"title"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
	Annotations ToolAnnotations `json:"annotations"`
	run         func(t *Tools, ctx context.Context, args json.RawMessage) (any, error)
}

// ToolAnnotations are the MCP behaviour hints.
type ToolAnnotations struct {
	ReadOnly    bool `json:"readOnlyHint"`
	Destructive bool `json:"destructiveHint"`
	Idempotent  bool `json:"idempotentHint"`
	OpenWorld   bool `json:"openWorldHint"`
}

// toolError is a failure the agent sees as the tool's result (isError).
type toolError struct{ msg string }

func (e *toolError) Error() string { return e.msg }

func argErr(format string, a ...any) error { return &toolError{fmt.Sprintf(format, a...)} }

var readOnly = ToolAnnotations{ReadOnly: true, Idempotent: true}

const opIDSchema = `"operation_id": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,64}$",
      "description": "Optional. Makes a retry safe: the same operation_id with the same request returns the first result instead of applying it twice. Leave it out for a new change (one is generated and returned); send the returned one again only to retry a call whose outcome you did not see."}`

// toolList is every tool, in the order tools/list gives them.
var toolList = []*Tool{
	{
		Name:  "find_files",
		Title: "Find files",
		Description: "Find files on the JuiceMount volume that the token's person may see (their project grants), by name, folder, " +
			"collection, kind, extension and time. Returns metadata only, never contents: each file has mount_path, its absolute " +
			"path on this Mac, which you read with your own file tools. Filters combine with AND. Results come a page at a time " +
			"in the catalog's stable order: when next is present, call again with cursor set to it to continue. A page can be " +
			"cut short by the Manager's per-request budget (budget_exhausted true): it may then hold fewer files than limit, even " +
			"none, and still have next. Without next the search is complete.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "name": {"type": "string", "minLength": 1, "maxLength": 256, "description": "A fragment of the file name, ignoring case (no '/')."},
    "folders": {"type": "array", "maxItems": 16, "items": {"type": "string", "minLength": 1, "maxLength": 4096},
      "description": "Volume folders such as \"/Clients/Acme\"; a file anywhere below any of them matches. Volume paths, not paths on this Mac."},
    "collection_id": {"type": "string", "pattern": "^col_[0-9a-f]{32}$", "description": "Only the files in this collection."},
    "modified_after": {"type": "string", "minLength": 1, "maxLength": 64, "description": "Only files modified after this time: RFC 3339 (2026-10-01T00:00:00Z) or unix milliseconds."},
    "seen_after": {"type": "integer", "minimum": 0, "description": "Only files the catalog recorded (bound, moved or saved in place) after this catalog sequence."},
    "extensions": {"type": "array", "maxItems": 32, "items": {"type": "string", "pattern": "^\\.?[A-Za-z0-9]{1,16}$"}, "description": "File extensions, with or without the dot; a file matches any of them."},
    "kinds": {"type": "array", "maxItems": 5, "items": {"type": "string", "enum": ["video", "audio", "image", "project", "document"]}, "description": "Named sets of extensions; a file matches any of them (or any of extensions)."},
    "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50, "description": "Most files on one page."},
    "cursor": {"type": "string", "minLength": 1, "maxLength": 128, "description": "The next value of the previous page, to continue that search with the same filters."}
  },
  "additionalProperties": false
}`),
		Annotations: readOnly,
		run:         (*Tools).findFiles,
	},
	{
		Name:  "get_file",
		Title: "Get a file",
		Description: "One file by asset ID (the asset_id find_files and collections return): its current name, volume path, " +
			"mount_path on this Mac, size, modification time and collections. Metadata only; read the contents at mount_path. " +
			"Not found when the file is gone or outside the person's grants.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "asset_id": {"type": "string", "pattern": "^asset_[0-9a-f]{32}$"}
  },
  "required": ["asset_id"],
  "additionalProperties": false
}`),
		Annotations: readOnly,
		run:         (*Tools).getFile,
	},
	{
		Name:  "list_collections",
		Title: "List collections",
		Description: "The library's collections (named sets of files people curate), with member counts, a page at a time in " +
			"ID order: when next is present, call again with after set to it.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "after": {"type": "string", "pattern": "^col_[0-9a-f]{32}$", "description": "The next value of the previous page."},
    "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}
  },
  "additionalProperties": false
}`),
		Annotations: readOnly,
		run:         (*Tools).listCollections,
	},
	{
		Name:  "get_collection_members",
		Title: "Get collection members",
		Description: "A collection and a page of its members, each with its file's current volume path and mount_path on this Mac. " +
			"state is present (path set), missing (the file is gone or in the trash; it stays a member) or unknown (the volume " +
			"could not be read). When next is present, call again with after set to it.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "collection_id": {"type": "string", "pattern": "^col_[0-9a-f]{32}$"},
    "after": {"type": "string", "pattern": "^asset_[0-9a-f]{32}$", "description": "The next value of the previous page."},
    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 50}
  },
  "required": ["collection_id"],
  "additionalProperties": false
}`),
		Annotations: readOnly,
		run:         (*Tools).collectionMembers,
	},
	{
		Name:  "create_collection",
		Title: "Create a collection",
		Description: "Create a collection, optionally with members (asset IDs). Every member must be a file the person's project " +
			"grants cover. Returns the collection and the operation_id used. Collections cannot be deleted through this server.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "name": {"type": "string", "minLength": 1, "maxLength": 256},
    "asset_ids": {"type": "array", "maxItems": 255, "items": {"type": "string", "pattern": "^asset_[0-9a-f]{32}$"}},
    ` + opIDSchema + `
  },
  "required": ["name"],
  "additionalProperties": false
}`),
		Annotations: ToolAnnotations{},
		run:         (*Tools).createCollection,
	},
	{
		Name:  "rename_collection",
		Title: "Rename a collection",
		Description: "Rename a collection. The person's project grants must cover every member. Fails with a conflict when the " +
			"collection changed while it was being checked: read it again and retry.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "collection_id": {"type": "string", "pattern": "^col_[0-9a-f]{32}$"},
    "name": {"type": "string", "minLength": 1, "maxLength": 256},
    ` + opIDSchema + `
  },
  "required": ["collection_id", "name"],
  "additionalProperties": false
}`),
		Annotations: ToolAnnotations{Idempotent: true},
		run:         (*Tools).renameCollection,
	},
	{
		Name:  "add_to_collection",
		Title: "Add files to a collection",
		Description: "Add files (asset IDs, at most 255 a call) to a collection. The person's project grants must cover each " +
			"file. Adding a member that is already there changes nothing.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "collection_id": {"type": "string", "pattern": "^col_[0-9a-f]{32}$"},
    "asset_ids": {"type": "array", "minItems": 1, "maxItems": 255, "items": {"type": "string", "pattern": "^asset_[0-9a-f]{32}$"}},
    ` + opIDSchema + `
  },
  "required": ["collection_id", "asset_ids"],
  "additionalProperties": false
}`),
		Annotations: ToolAnnotations{Idempotent: true},
		run: func(t *Tools, ctx context.Context, a json.RawMessage) (any, error) {
			return t.editMembers(ctx, a, true)
		},
	},
	{
		Name:  "remove_from_collection",
		Title: "Remove files from a collection",
		Description: "Remove files (asset IDs, at most 255 a call) from a collection. The files themselves are not touched. The " +
			"person's project grants must cover each file.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "collection_id": {"type": "string", "pattern": "^col_[0-9a-f]{32}$"},
    "asset_ids": {"type": "array", "minItems": 1, "maxItems": 255, "items": {"type": "string", "pattern": "^asset_[0-9a-f]{32}$"}},
    ` + opIDSchema + `
  },
  "required": ["collection_id", "asset_ids"],
  "additionalProperties": false
}`),
		Annotations: ToolAnnotations{Destructive: true, Idempotent: true},
		run: func(t *Tools, ctx context.Context, a json.RawMessage) (any, error) {
			return t.editMembers(ctx, a, false)
		},
	},
	{
		Name:  "list_saved_queries",
		Title: "List saved queries",
		Description: "The library's saved queries (named, deterministic file searches over folders), a page at a time: when " +
			"next is present, call again with after set to it. Saved queries are read-only through this server.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "after": {"type": "string", "pattern": "^qry_[0-9a-f]{32}$", "description": "The next value of the previous page."},
    "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}
  },
  "additionalProperties": false
}`),
		Annotations: readOnly,
		run:         (*Tools).listQueries,
	},
	{
		Name:  "get_saved_query_results",
		Title: "Get saved query results",
		Description: "The files a saved query finds, newest first, each with its volume path and mount_path on this Mac. The " +
			"answer may come from an evaluation up to a few minutes old (evaluated_at, age_seconds); capped means the query " +
			"examined only its most recently changed candidates.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "query_id": {"type": "string", "pattern": "^qry_[0-9a-f]{32}$"}
  },
  "required": ["query_id"],
  "additionalProperties": false
}`),
		Annotations: readOnly,
		run:         (*Tools).queryResults,
	},
}

var toolsByName = func() map[string]*Tool {
	m := map[string]*Tool{}
	for _, t := range toolList {
		m[t.Name] = t
	}
	return m
}()

// decodeArgs decodes a tool's arguments strictly (no unknown field).
func decodeArgs(raw json.RawMessage, into any) error {
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return argErr("invalid arguments: %v", err)
	}
	return nil
}

func checkID(re *regexp.Regexp, what, v string, required bool) error {
	if v == "" && !required {
		return nil
	}
	if !re.MatchString(v) {
		return argErr("%s must match %s", what, re.String())
	}
	return nil
}

func checkLimit(n, max int) error {
	if n < 0 || n > max {
		return argErr("limit is 1-%d", max)
	}
	return nil
}

// ---- output shapes ---------------------------------------------------------

// collectionRef is a collection a file belongs to.
type collectionRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// managerFile is a file as the Manager returns it (internal/manager
// FileView).
type managerFile struct {
	AssetID         string          `json:"asset_id"`
	Name            string          `json:"name"`
	Label           string          `json:"label"`
	PathHint        string          `json:"path_hint"`
	Path            string          `json:"path"`
	Size            uint64          `json:"size"`
	Modified        time.Time       `json:"modified"`
	Collections     []collectionRef `json:"collections"`
	MoreCollections bool            `json:"more_collections,omitempty"`
}

// fileOut is a file as the tools return it.
type fileOut struct {
	AssetID string `json:"asset_id"`
	Name    string `json:"name"`
	Label   string `json:"label"`
	// Path is the volume path; MountPath the same file on this Mac.
	Path            string          `json:"path"`
	MountPath       string          `json:"mount_path"`
	PathHint        string          `json:"path_hint,omitempty"`
	Size            uint64          `json:"size"`
	Modified        time.Time       `json:"modified"`
	Collections     []collectionRef `json:"collections"`
	MoreCollections bool            `json:"more_collections,omitempty"`
}

func (t *Tools) fileOut(f managerFile) (fileOut, bool) {
	mp, ok := mountPath(t.Mount, f.Path, rootedPath)
	if !ok {
		return fileOut{}, false
	}
	if f.Collections == nil {
		f.Collections = []collectionRef{}
	}
	return fileOut{AssetID: f.AssetID, Name: f.Name, Label: f.Label, Path: f.Path, MountPath: mp, PathHint: f.PathHint,
		Size: f.Size, Modified: f.Modified, Collections: f.Collections, MoreCollections: f.MoreCollections}, true
}

func (t *Tools) noteRefused(tool string, n int) {
	if n > 0 {
		t.Log.Printf("%s: skipped %d result(s) whose path would leave the mount root", tool, n)
	}
}

// ---- reads -----------------------------------------------------------------

func (t *Tools) get(ctx context.Context, path string, q url.Values, out any) error {
	c, err := t.Client()
	if err != nil {
		return err
	}
	return c.Do(ctx, http.MethodGet, path, q, nil, out)
}

func (t *Tools) findFiles(ctx context.Context, raw json.RawMessage) (any, error) {
	var a struct {
		Name          string   `json:"name"`
		Folders       []string `json:"folders"`
		CollectionID  string   `json:"collection_id"`
		ModifiedAfter string   `json:"modified_after"`
		SeenAfter     *int64   `json:"seen_after"`
		Extensions    []string `json:"extensions"`
		Kinds         []string `json:"kinds"`
		Limit         int      `json:"limit"`
		Cursor        string   `json:"cursor"`
	}
	if err := decodeArgs(raw, &a); err != nil {
		return nil, err
	}
	if err := checkID(collectionIDRe, "collection_id", a.CollectionID, false); err != nil {
		return nil, err
	}
	if err := checkLimit(a.Limit, 200); err != nil {
		return nil, err
	}
	if len(a.Cursor) > 128 {
		return nil, argErr("cursor is the next value of a previous page")
	}
	if a.SeenAfter != nil && *a.SeenAfter < 0 {
		return nil, argErr("seen_after is 0 or more")
	}
	q := url.Values{}
	set := func(k, v string) {
		if v != "" {
			q.Set(k, v)
		}
	}
	set("name", a.Name)
	set("collection", a.CollectionID)
	set("modified_after", a.ModifiedAfter)
	set("cursor", a.Cursor)
	if a.SeenAfter != nil {
		q.Set("seen_after", strconv.FormatInt(*a.SeenAfter, 10))
	}
	if a.Limit > 0 {
		q.Set("limit", strconv.Itoa(a.Limit))
	}
	q["folder"], q["ext"], q["kind"] = a.Folders, a.Extensions, a.Kinds
	for _, k := range []string{"folder", "ext", "kind"} {
		if len(q[k]) == 0 {
			delete(q, k)
		}
	}
	var page struct {
		Files      []managerFile `json:"files"`
		Next       string        `json:"next"`
		Scanned    int           `json:"scanned"`
		Resolved   int           `json:"resolved"`
		Unresolved int           `json:"unresolved"`
		Budget     bool          `json:"budget_exhausted"`
	}
	if err := t.get(ctx, "/api/continuity/find", q, &page); err != nil {
		return nil, err
	}
	out := struct {
		Files           []fileOut `json:"files"`
		Next            string    `json:"next,omitempty"`
		BudgetExhausted bool      `json:"budget_exhausted"`
		Scanned         int       `json:"scanned"`
		Resolved        int       `json:"resolved"`
		Unresolved      int       `json:"unresolved,omitempty"`
		SkippedUnsafe   int       `json:"skipped_unsafe_paths,omitempty"`
	}{Files: []fileOut{}, Next: page.Next, BudgetExhausted: page.Budget, Scanned: page.Scanned, Resolved: page.Resolved, Unresolved: page.Unresolved}
	for _, f := range page.Files {
		v, ok := t.fileOut(f)
		if !ok {
			out.SkippedUnsafe++
			continue
		}
		out.Files = append(out.Files, v)
	}
	t.noteRefused("find_files", out.SkippedUnsafe)
	return out, nil
}

func (t *Tools) getFile(ctx context.Context, raw json.RawMessage) (any, error) {
	var a struct {
		AssetID string `json:"asset_id"`
	}
	if err := decodeArgs(raw, &a); err != nil {
		return nil, err
	}
	if err := checkID(assetIDRe, "asset_id", a.AssetID, true); err != nil {
		return nil, err
	}
	var f managerFile
	if err := t.get(ctx, "/api/continuity/asset/"+a.AssetID, nil, &f); err != nil {
		return nil, err
	}
	v, ok := t.fileOut(f)
	if !ok {
		t.noteRefused("get_file", 1)
		return nil, &toolError{"the Manager returned a path for this file that would leave the mount root; it was refused"}
	}
	return v, nil
}

func pageQuery(after string, limit int) url.Values {
	q := url.Values{}
	if after != "" {
		q.Set("after", after)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	return q
}

func (t *Tools) listCollections(ctx context.Context, raw json.RawMessage) (any, error) {
	var a struct {
		After string `json:"after"`
		Limit int    `json:"limit"`
	}
	if err := decodeArgs(raw, &a); err != nil {
		return nil, err
	}
	if err := checkID(collectionIDRe, "after", a.After, false); err != nil {
		return nil, err
	}
	if err := checkLimit(a.Limit, 1000); err != nil {
		return nil, err
	}
	var out struct {
		Collections []json.RawMessage `json:"collections"`
		Next        string            `json:"next,omitempty"`
	}
	if err := t.get(ctx, "/api/collections", pageQuery(a.After, a.Limit), &out); err != nil {
		return nil, err
	}
	if out.Collections == nil {
		out.Collections = []json.RawMessage{}
	}
	return out, nil
}

func (t *Tools) collectionMembers(ctx context.Context, raw json.RawMessage) (any, error) {
	var a struct {
		CollectionID string `json:"collection_id"`
		After        string `json:"after"`
		Limit        int    `json:"limit"`
	}
	if err := decodeArgs(raw, &a); err != nil {
		return nil, err
	}
	if err := checkID(collectionIDRe, "collection_id", a.CollectionID, true); err != nil {
		return nil, err
	}
	if err := checkID(assetIDRe, "after", a.After, false); err != nil {
		return nil, err
	}
	if err := checkLimit(a.Limit, 100); err != nil {
		return nil, err
	}
	var page map[string]json.RawMessage
	if err := t.get(ctx, "/api/collections/"+a.CollectionID+"/members", pageQuery(a.After, a.Limit), &page); err != nil {
		return nil, err
	}
	return t.withMountPaths("get_collection_members", page, "members")
}

func (t *Tools) listQueries(ctx context.Context, raw json.RawMessage) (any, error) {
	var a struct {
		After string `json:"after"`
		Limit int    `json:"limit"`
	}
	if err := decodeArgs(raw, &a); err != nil {
		return nil, err
	}
	if err := checkID(queryIDRe, "after", a.After, false); err != nil {
		return nil, err
	}
	if err := checkLimit(a.Limit, 1000); err != nil {
		return nil, err
	}
	var out struct {
		Queries []json.RawMessage `json:"queries"`
		Next    string            `json:"next,omitempty"`
	}
	if err := t.get(ctx, "/api/queries", pageQuery(a.After, a.Limit), &out); err != nil {
		return nil, err
	}
	if out.Queries == nil {
		out.Queries = []json.RawMessage{}
	}
	return out, nil
}

func (t *Tools) queryResults(ctx context.Context, raw json.RawMessage) (any, error) {
	var a struct {
		QueryID string `json:"query_id"`
	}
	if err := decodeArgs(raw, &a); err != nil {
		return nil, err
	}
	if err := checkID(queryIDRe, "query_id", a.QueryID, true); err != nil {
		return nil, err
	}
	var page map[string]json.RawMessage
	if err := t.get(ctx, "/api/queries/"+a.QueryID+"/results", nil, &page); err != nil {
		return nil, err
	}
	return t.withMountPaths("get_saved_query_results", page, "results")
}

// withMountPaths gives each item of page[list] (collection members, query
// results: volume paths without a leading slash) its mount_path, and drops
// an item whose path would leave the mount root. Every other field of the
// page and of each item is passed on as the Manager sent it, so a field it
// adds (such as a count of items withheld from the agent) reaches the
// agent.
func (t *Tools) withMountPaths(tool string, page map[string]json.RawMessage, list string) (any, error) {
	if page == nil {
		page = map[string]json.RawMessage{}
	}
	var items []map[string]json.RawMessage
	if raw, ok := page[list]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, fmt.Errorf("the Manager's answer could not be read: %v", err)
		}
	}
	kept := []map[string]json.RawMessage{}
	skipped := 0
	for _, it := range items {
		delete(it, "mount_path")
		var p string
		if raw, ok := it["path"]; ok {
			if err := json.Unmarshal(raw, &p); err != nil {
				skipped++
				continue
			}
		}
		if p != "" {
			mp, ok := mountPath(t.Mount, p, relativePath)
			if !ok {
				skipped++
				continue
			}
			b, _ := json.Marshal(mp)
			it["mount_path"] = b
		}
		kept = append(kept, it)
	}
	b, err := json.Marshal(kept)
	if err != nil {
		return nil, err
	}
	page[list] = b
	delete(page, "skipped_unsafe_paths")
	if skipped > 0 {
		page["skipped_unsafe_paths"] = json.RawMessage(strconv.Itoa(skipped))
	}
	t.noteRefused(tool, skipped)
	return page, nil
}

// ---- writes ----------------------------------------------------------------

// newOperationID is a fresh operation_id for a change the agent sent
// without one.
func newOperationID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return "mcp-" + hex.EncodeToString(b)
}

func operationID(given string) (string, error) {
	if given == "" {
		return newOperationID(), nil
	}
	if !operationIDRe.MatchString(given) {
		return "", argErr("operation_id is 1-64 letters, digits, - or _")
	}
	return given, nil
}

type memberRef struct {
	AssetID string `json:"asset_id"`
}

func assetRefs(ids []string) ([]memberRef, error) {
	if len(ids) > maxAssetsPerEdit {
		return nil, argErr("at most %d asset_ids a call; split the change", maxAssetsPerEdit)
	}
	out := make([]memberRef, 0, len(ids))
	for _, id := range ids {
		if err := checkID(assetIDRe, "each asset_id", id, true); err != nil {
			return nil, err
		}
		out = append(out, memberRef{id})
	}
	return out, nil
}

// write sends a change with its operation_id, again with the same one when
// the answer was lost or the Manager was busy (503): the Manager applies an
// operation_id once and answers a repeat with the first result.
func (t *Tools) write(ctx context.Context, path, opID string, body any) (any, error) {
	c, err := t.Client()
	if err != nil {
		return nil, err
	}
	var reply struct {
		Collection json.RawMessage `json:"collection"`
		Operation  json.RawMessage `json:"operation"`
	}
	for attempt := 1; ; attempt++ {
		err = c.Do(ctx, http.MethodPost, path, nil, body, &reply)
		var ae *APIError
		retry := errors.Is(err, errNoAnswer) || errors.As(err, &ae) && ae.Status == http.StatusServiceUnavailable
		if !retry || attempt == writeAttempts || ctx.Err() != nil {
			break
		}
		t.Log.Printf("%s: no usable answer (attempt %d); sending the same operation again", path, attempt)
		t.Sleep(time.Duration(attempt) * time.Second)
	}
	if errors.Is(err, errNoAnswer) {
		return nil, &toolError{fmt.Sprintf("%v. The change may or may not have been made: call again with operation_id %q to retry it safely", err, opID)}
	}
	if err != nil {
		return nil, err
	}
	return struct {
		Collection  json.RawMessage `json:"collection"`
		Operation   json.RawMessage `json:"operation"`
		OperationID string          `json:"operation_id"`
	}{reply.Collection, reply.Operation, opID}, nil
}

func (t *Tools) createCollection(ctx context.Context, raw json.RawMessage) (any, error) {
	var a struct {
		Name        string   `json:"name"`
		AssetIDs    []string `json:"asset_ids"`
		OperationID string   `json:"operation_id"`
	}
	if err := decodeArgs(raw, &a); err != nil {
		return nil, err
	}
	if a.Name == "" || len(a.Name) > 256 {
		return nil, argErr("name is 1-256 bytes")
	}
	refs, err := assetRefs(a.AssetIDs)
	if err != nil {
		return nil, err
	}
	opID, err := operationID(a.OperationID)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"name": a.Name, "operation_id": opID}
	if len(refs) > 0 {
		body["members"] = refs
	}
	return t.write(ctx, "/api/collections", opID, body)
}

func (t *Tools) renameCollection(ctx context.Context, raw json.RawMessage) (any, error) {
	var a struct {
		CollectionID string `json:"collection_id"`
		Name         string `json:"name"`
		OperationID  string `json:"operation_id"`
	}
	if err := decodeArgs(raw, &a); err != nil {
		return nil, err
	}
	if err := checkID(collectionIDRe, "collection_id", a.CollectionID, true); err != nil {
		return nil, err
	}
	if a.Name == "" || len(a.Name) > 256 {
		return nil, argErr("name is 1-256 bytes")
	}
	opID, err := operationID(a.OperationID)
	if err != nil {
		return nil, err
	}
	return t.write(ctx, "/api/collections/"+a.CollectionID+"/rename", opID, map[string]any{"name": a.Name, "operation_id": opID})
}

func (t *Tools) editMembers(ctx context.Context, raw json.RawMessage, add bool) (any, error) {
	var a struct {
		CollectionID string   `json:"collection_id"`
		AssetIDs     []string `json:"asset_ids"`
		OperationID  string   `json:"operation_id"`
	}
	if err := decodeArgs(raw, &a); err != nil {
		return nil, err
	}
	if err := checkID(collectionIDRe, "collection_id", a.CollectionID, true); err != nil {
		return nil, err
	}
	if len(a.AssetIDs) == 0 {
		return nil, argErr("give at least one asset_id")
	}
	refs, err := assetRefs(a.AssetIDs)
	if err != nil {
		return nil, err
	}
	opID, err := operationID(a.OperationID)
	if err != nil {
		return nil, err
	}
	verb := "remove"
	if add {
		verb = "add"
	}
	return t.write(ctx, "/api/collections/"+a.CollectionID+"/members/"+verb, opID, map[string]any{"members": refs, "operation_id": opID})
}

// ---- errors ----------------------------------------------------------------

// errorMessage is what the agent reads when a tool fails.
func errorMessage(err error) string {
	var te *toolError
	if errors.As(err, &te) {
		return te.msg
	}
	var ae *APIError
	if !errors.As(err, &ae) {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return "the request to the Manager was cancelled or timed out; try again"
		}
		return "the Manager could not be reached: " + err.Error()
	}
	switch ae.Status {
	case http.StatusUnauthorized:
		return "The Manager refused this Mac's agent token (" + ae.Message + "). It was revoked or expired, or its Mac or person " +
			"changed. A person must mint a new one on this Mac: juicemount-mcp token create --name <name>."
	case http.StatusForbidden:
		return "The Manager refused this (" + ae.Message + "). An agent acts as its person: it may find files and edit " +
			"collections only where that person's project grants allow, and may not delete collections or change saved queries."
	case http.StatusNotFound:
		return "Not found, or not visible to this token's person (" + ae.Message + "). A file outside the person's " +
			"project grants is reported as not found."
	case http.StatusConflict:
		return "Conflict (" + ae.Message + ")."
	case http.StatusTooManyRequests:
		return "The Manager is limiting requests (" + ae.Message + "); wait a little and try again."
	case http.StatusServiceUnavailable:
		return "The Manager is busy or not ready (" + ae.Message + "); try again shortly."
	case http.StatusBadRequest:
		return "The Manager rejected the request as invalid (" + ae.Message + ")."
	}
	return fmt.Sprintf("The Manager answered HTTP %d (%s).", ae.Status, ae.Message)
}
