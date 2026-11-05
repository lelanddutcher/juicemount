package manager

// queries.go: continuity slice 5, saved queries (experimental, behind
// JM_CONTINUITY like collections; with it off every route is 404). A saved
// query is a named, deterministic file query (queries_eval.go) kept in the
// continuity catalog, so every change is an event in its ordered history
// with the actor that made it. With smart folders on too, each query is a
// view under /Queries (smartfolders_queries.go).
//
// Who. As for collections (collections.go): the admin key, or a Teams
// person through one of their Macs, with the same request budgets; the
// same handler serves both.
//
// What. Reads are shared (Teams v1: any authenticated caller), results
// included: a query only shows names a reader can already see. A person may
// create, rename or delete a query only when their project write grants
// cover every one of its folders (a grant covers its folder and everything
// below), the rule collections apply to their members: a query is defined
// over folders, so its folders are what it touches. A query over a parent
// of several projects needs a grant on that parent (or the admin). A
// person with no write access writes nothing; with no admin key configured
// while grants are in force, the open admin path may not write. A query's
// folders never change after creation (delete and create a new one), so a
// rename or delete is checked against the folders it was created with and
// needs no sequence guard. Refusals are 403 and logged as for collections.
// A folder inside a smart-folder root the Manager owns is refused; a
// user-made folder of such a name is ordinary data (decision 2).
//
// Limits. A library holds at most catalog.MaxQueries (256) active queries
// and a person at most queryMaxPerPerson (32) created by them (the admin is
// exempt from the per-person limit); beyond, 409. A create takes the
// actor's request slot, so one person's creates are counted one at a time.
//
// Format. A catalog stays at format 3 (readable by the Manager before
// queries, so a rollback stays possible) until the first query is
// created: that create, once authorized, adds the query table in place
// (catalog EnableQueries).
//
// Cost. Writes check grants against the folder names only, never the
// volume. GET .../results answers from the query's last complete
// evaluation when it is under a minute old, or under queryResultsKept (20
// minutes) while the query views run (they rebuild every view at least
// every 10 minutes); with evaluated_at and age_seconds; otherwise it runs the query (a
// catalog scan and at most queryMaxCandidates volume resolutions) under a
// volume-read slot and the 30 s deadline, and keeps the result.
//
// Retries. operation_id as for collections ("qry-" operation IDs).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// queryMaxPerPerson bounds the active queries one person created.
const queryMaxPerPerson = 32

// queryResultsKept: how old a remembered evaluation the results API returns
// while the query views run.
const queryResultsKept = 2 * collectionViewsSweep

// ownedSmartRoots lists the smart-folder roots the Manager owns now (none
// while smart folders are off).
func (a *API) ownedSmartRoots() []string {
	if a.jobs == nil {
		return nil
	}
	a.jobs.mu.RLock()
	sf, _ := a.jobs.smartFolders.(*smartFoldersService)
	a.jobs.mu.RUnlock()
	if sf == nil {
		return nil
	}
	return sf.ownedRoots()
}

// QueryView is one saved query as the API returns it. Folders are
// volume-relative, without a leading slash.
type QueryView struct {
	ID                    catalog.QueryID `json:"id"`
	Name                  string          `json:"name"`
	Folders               []string        `json:"folders"`
	NameContains          string          `json:"name_contains,omitempty"`
	Extensions            []string        `json:"extensions"`
	ModifiedWithinSeconds int64           `json:"modified_within_seconds,omitempty"`
	Limit                 int             `json:"limit"`
	CreatedSequence       int64           `json:"created_sequence"`
	UpdatedSequence       int64           `json:"updated_sequence"`
	Removed               bool            `json:"removed,omitempty"`
}

func queryView(q catalog.Query) QueryView {
	d := q.Definition
	return QueryView{q.ID, q.Name, d.Prefixes, d.NameContains, d.Extensions, d.ModifiedWithinSeconds, d.Limit,
		q.CreatedSequence, q.UpdatedSequence, q.Removed}
}

// QueryResultView is one file a query found.
type QueryResultView struct {
	AssetID  continuity.AssetID `json:"asset_id"`
	Path     string             `json:"path"`
	Modified time.Time          `json:"modified"`
}

type queryBody struct {
	Name                  *string  `json:"name"`
	Folders               []string `json:"folders"`
	NameContains          string   `json:"name_contains"`
	Extensions            []string `json:"extensions"`
	ModifiedWithinSeconds int64    `json:"modified_within_seconds"`
	Limit                 int      `json:"limit"`
	OperationID           string   `json:"operation_id"`
}

// serveQueries is serve for /api/queries.
func (h *collectionsAPI) serveQueries(w http.ResponseWriter, r *http.Request) {
	h.serveWith(w, r, queryReadsVolume(r), h.routeQueries)
}

// queryReadsVolume: a results request may read the volume; a create takes
// the slot too, so a person's creates are counted one at a time.
func queryReadsVolume(r *http.Request) bool {
	p := strings.TrimSuffix(r.URL.Path, "/")
	return r.Method == http.MethodGet && strings.HasSuffix(p, "/results") ||
		r.Method == http.MethodPost && strings.HasSuffix(p, "/api/queries")
}

// routeQueries dispatches
//
//	GET    /api/queries                  list (after, limit)
//	POST   /api/queries                  create {name, folders, name_contains?, extensions?,
//	                                     modified_within_seconds?, limit?, operation_id?}
//	GET    /api/queries/{id}             one query
//	GET    /api/queries/{id}/results     run it now
//	DELETE /api/queries/{id}             delete (body optional: {operation_id})
//	POST   /api/queries/{id}/rename      {name, operation_id?}
//	POST   /api/queries/{id}/delete      {operation_id?} or no body
func (h *collectionsAPI) routeQueries(w http.ResponseWriter, r *http.Request, svc *continuityService, actor collectionActor) error {
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, h.prefix+"/api/queries"), "/")
	parts := []string{}
	if rest != "" {
		parts = strings.Split(rest, "/")
	}
	if r.Method != http.MethodGet && actor.open && h.people != nil && h.people.grantsInForce() {
		return failf(http.StatusForbidden, "no admin key is configured while people's grants are in force; set an admin key to change queries as the admin")
	}
	library, err := svc.store.LibraryID()
	if err != nil {
		return err
	}
	c := &queryCall{collectionCall{svc: svc, actor: actor, library: library, w: w, r: r}, h.roots}
	if len(parts) == 0 {
		switch r.Method {
		case http.MethodGet:
			return c.list()
		case http.MethodPost:
			return c.create()
		}
		return failf(http.StatusMethodNotAllowed, "GET or POST only")
	}
	id := catalog.QueryID(parts[0])
	if id.Validate() != nil {
		return failf(http.StatusNotFound, "no such query")
	}
	want := func(method string) error {
		if r.Method != method {
			return failf(http.StatusMethodNotAllowed, "%s only", method)
		}
		return nil
	}
	switch strings.Join(parts[1:], "/") {
	case "":
		switch r.Method {
		case http.MethodGet:
			return c.get(id)
		case http.MethodDelete:
			return c.remove(id)
		}
		return failf(http.StatusMethodNotAllowed, "GET or DELETE only")
	case "results":
		if err := want(http.MethodGet); err != nil {
			return err
		}
		return c.results(id)
	case "rename":
		if err := want(http.MethodPost); err != nil {
			return err
		}
		return c.rename(id)
	case "delete":
		if err := want(http.MethodPost); err != nil {
			return err
		}
		return c.remove(id)
	}
	return failf(http.StatusNotFound, "no such route")
}

type queryCall struct {
	collectionCall
	roots func() []string // the smart-folder roots the Manager owns
}

func (c *queryCall) list() error {
	after, limit, err := pageArgs(c.r, 100, catalog.MaxPageSize)
	if err != nil {
		return err
	}
	if after != "" && catalog.QueryID(after).Validate() != nil {
		return fmt.Errorf("%w: bad cursor", errCollectionInput)
	}
	page, err := c.svc.store.ListQueries(c.ctx(), c.library, catalog.QueryID(after), limit)
	if err != nil {
		return err
	}
	out := struct {
		Queries []QueryView `json:"queries"`
		Next    string      `json:"next,omitempty"`
	}{Queries: []QueryView{}}
	for _, q := range page {
		out.Queries = append(out.Queries, queryView(q))
	}
	if len(page) == limit {
		out.Next = string(page[len(page)-1].ID)
	}
	writeJSON(c.w, http.StatusOK, out)
	return nil
}

// active reads a query that has not been removed.
func (c *queryCall) active(id catalog.QueryID) (catalog.Query, error) {
	q, err := c.svc.store.GetQuery(c.ctx(), c.library, id)
	if err == nil && q.Removed {
		err = catalog.ErrNotFound
	}
	if errors.Is(err, catalog.ErrNotFound) {
		return q, failf(http.StatusNotFound, "no such query")
	}
	return q, err
}

func (c *queryCall) get(id catalog.QueryID) error {
	q, err := c.active(id)
	if err != nil {
		return err
	}
	writeJSON(c.w, http.StatusOK, queryView(q))
	return nil
}

func (c *queryCall) results(id catalog.QueryID) error {
	q, err := c.active(id)
	if err != nil {
		return err
	}
	now := time.Now()
	cached, ok := c.svc.results.get(id)
	if age := now.Sub(cached.at); !ok || age >= queryResultsFresh && (!c.svc.queryViewsRunning() || age >= queryResultsKept) {
		res, err := c.svc.runQuery(c.ctx(), id, q.Definition, c.roots(), now)
		if errors.Is(err, binder.ErrNotReady) {
			return failf(http.StatusServiceUnavailable, "%v", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return failf(http.StatusServiceUnavailable, "running the query took too long; try again")
		}
		if err != nil {
			return err
		}
		cached = cachedQueryResult{res, now}
		c.svc.rememberQuery(id, res, now)
	}
	res := cached.res
	out := struct {
		Query       QueryView         `json:"query"`
		Results     []QueryResultView `json:"results"`
		EvaluatedAt time.Time         `json:"evaluated_at"`
		AgeSeconds  int64             `json:"age_seconds"`
		Unresolved  int               `json:"unresolved,omitempty"`
		OverLimit   int               `json:"results_over_limit,omitempty"`
		// Capped: more files matched the query's folders and name than it
		// examines (queryMaxCandidates, the most recently changed first).
		Capped     bool `json:"capped,omitempty"`
		Examined   int  `json:"examined"`
		Candidates int  `json:"candidates"`
	}{Query: queryView(q), Results: []QueryResultView{}, EvaluatedAt: cached.at.UTC(), AgeSeconds: int64(now.Sub(cached.at) / time.Second),
		Unresolved: len(res.Unknown), OverLimit: res.Over, Capped: res.Capped > 0, Examined: res.Examined, Candidates: res.Examined + res.Capped}
	for _, h := range res.Hits {
		out.Results = append(out.Results, QueryResultView{h.AssetID, h.Path, h.Mtime.UTC()})
	}
	writeJSON(c.w, http.StatusOK, out)
	return nil
}

// body decodes the request body; an empty body is an empty request.
func (c *queryCall) body() (queryBody, error) {
	var b queryBody
	dec := json.NewDecoder(io.LimitReader(c.r.Body, maxCollectionBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil && err != io.EOF {
		return b, fmt.Errorf("%w: invalid JSON", errCollectionInput)
	}
	if b.OperationID != "" && !collectionKeyRe.MatchString(b.OperationID) {
		return b, fmt.Errorf("%w: operation_id is 1-64 letters, digits, - or _", errCollectionInput)
	}
	return b, nil
}

// definition checks and normalizes a create request: folders with or
// without a leading slash, each once; extensions lowercased, with or without
// the dot; the default limit.
func (b queryBody) definition(roots []string) (catalog.QueryDefinition, error) {
	d := catalog.QueryDefinition{NameContains: b.NameContains, ModifiedWithinSeconds: b.ModifiedWithinSeconds, Limit: b.Limit}
	if d.Limit == 0 {
		d.Limit = queryDefaultLimit
	}
	if len(b.Folders) == 0 || len(b.Folders) > catalog.MaxQueryPrefixes {
		return d, fmt.Errorf("%w: give 1-%d folders", errCollectionInput, catalog.MaxQueryPrefixes)
	}
	seen := map[string]bool{}
	for _, f := range b.Folders {
		p := strings.TrimSuffix(strings.TrimPrefix(f, "/"), "/")
		if !catalog.ValidQueryPrefix(p) {
			return d, fmt.Errorf("%w: folder %q is not a volume folder path (no empty, '.', '..' or hidden components)", errCollectionInput, f)
		}
		top, _, _ := strings.Cut(p, "/")
		for _, r := range roots {
			if strings.EqualFold(top, r) {
				return d, fmt.Errorf("%w: folder %q is in the smart-folder root /%s", errCollectionInput, f, r)
			}
		}
		if !seen[p] {
			seen[p] = true
			d.Prefixes = append(d.Prefixes, p)
		}
	}
	extSeen := map[string]bool{}
	for _, e := range b.Extensions {
		e = strings.ToLower(strings.TrimPrefix(e, "."))
		if !catalog.ValidQueryExtension(e) {
			return d, fmt.Errorf("%w: an extension is 1-%d letters or digits", errCollectionInput, catalog.MaxQueryExtensionBytes)
		}
		if !extSeen[e] {
			extSeen[e] = true
			d.Extensions = append(d.Extensions, e)
		}
	}
	switch {
	case len(d.Extensions) > catalog.MaxQueryExtensions:
		return d, fmt.Errorf("%w: at most %d extensions", errCollectionInput, catalog.MaxQueryExtensions)
	case len(d.NameContains) > catalog.MaxQueryNameContains || strings.ContainsAny(d.NameContains, "/\x00"):
		return d, fmt.Errorf("%w: name_contains is at most %d bytes, without '/'", errCollectionInput, catalog.MaxQueryNameContains)
	case d.ModifiedWithinSeconds < 0 || d.ModifiedWithinSeconds > catalog.MaxQueryWithinSeconds:
		return d, fmt.Errorf("%w: modified_within_seconds is 0 (any time) to %d", errCollectionInput, catalog.MaxQueryWithinSeconds)
	case d.Limit < 1 || d.Limit > catalog.MaxQueryLimit:
		return d, fmt.Errorf("%w: limit is 1-%d (default %d)", errCollectionInput, catalog.MaxQueryLimit, queryDefaultLimit)
	}
	return d, nil
}

// authorize checks that a person's grants cover every folder of the query.
// The admin is always allowed.
func (c *queryCall) authorize(def catalog.QueryDefinition) error {
	if c.actor.person == nil {
		return nil
	}
	if !c.actor.writesAnywhere() {
		return failf(http.StatusForbidden, "%s has no write access, so may not change queries", c.actor.person.Name)
	}
	for _, f := range def.Prefixes {
		if !c.actor.covers(f) {
			return failf(http.StatusForbidden, "%s may not define a query over %q: no project grant covers it", c.actor.person.Name, "/"+f)
		}
	}
	return nil
}

// operationID derives the catalog operation ID from the caller's key (a
// random one when it gave none) and the actor.
func (c *queryCall) operationID(key string) (string, error) {
	id, err := c.collectionCall.operationID(key)
	return "qry-" + strings.TrimPrefix(id, "col-"), err
}

// change records one write. An exact retry (its operation ID is in the
// history) returns the original result without a second authorization, as
// for collections.
func (c *queryCall) change(opID string, id catalog.QueryID, authorize func() error, changes []catalog.Change) error {
	_, err := c.svc.store.GetEvent(c.ctx(), c.library, opID)
	switch {
	case errors.Is(err, catalog.ErrNotFound):
		if err := authorize(); err != nil {
			return err
		}
	case err != nil:
		return err
	}
	receipt, err := c.svc.store.Apply(c.ctx(), catalog.Operation{
		Version: catalog.OperationVersion, LibraryID: c.library, ID: opID, Actor: c.actor.id(), Changes: changes})
	if errors.Is(err, catalog.ErrNotFound) {
		return failf(http.StatusNotFound, "no such query")
	}
	if err != nil {
		return err
	}
	c.svc.queriesChanged()
	q, err := c.svc.store.GetQuery(c.ctx(), c.library, id)
	if err != nil {
		return err
	}
	writeJSON(c.w, http.StatusOK, map[string]any{
		"query":     queryView(q),
		"operation": map[string]any{"id": opID, "sequence": receipt.Sequence, "actor": c.actor.id()},
	})
	return nil
}

func (c *queryCall) create() error {
	b, err := c.body()
	if err != nil {
		return err
	}
	name, err := cleanCollectionName(b.Name)
	if err != nil {
		return err
	}
	def, err := b.definition(c.roots())
	if err != nil {
		return err
	}
	opID, err := c.operationID(b.OperationID)
	if err != nil {
		return err
	}
	// The identity follows from the operation, so a retry names the same one.
	sum := sha256.Sum256([]byte("juicemount.queries.id\x00" + opID))
	id := catalog.QueryID("qry_" + hex.EncodeToString(sum[:16]))
	return c.change(opID, id, func() error {
		if err := c.authorize(def); err != nil {
			return err
		}
		if err := c.underPersonLimit(); err != nil {
			return err
		}
		// The first query: the catalog gains its query table now.
		if c.svc.store.Format() < 4 {
			if err := c.svc.store.EnableQueries(c.ctx()); err != nil {
				return err
			}
			log.Printf("manager: continuity: the catalog is now format 4 (saved queries); a Manager from before saved queries no longer opens it")
		}
		return nil
	}, []catalog.Change{{CreateQuery: &catalog.CreateQuery{QueryID: id, Name: name, Definition: def}}})
}

// underPersonLimit refuses a person's create beyond queryMaxPerPerson
// active queries of theirs (the creator is the actor of the query's
// create event). The admin is exempt; the library limit is the catalog's.
func (c *queryCall) underPersonLimit() error {
	if c.actor.person == nil {
		return nil
	}
	qs, err := c.svc.activeQueries(c.ctx())
	if err != nil {
		return err
	}
	n := 0
	for _, q := range qs {
		evs, err := c.svc.store.Events(c.ctx(), c.library, q.CreatedSequence-1, 1)
		if err != nil {
			return err
		}
		if len(evs) == 1 && evs[0].Operation.Actor == c.actor.id() {
			n++
		}
	}
	if n >= queryMaxPerPerson {
		return failf(http.StatusConflict, "%s already has %d saved queries, the most one person may have; delete one first", c.actor.person.Name, n)
	}
	return nil
}

// existing reads the query a rename or delete names, for its folders. A
// retry of a delete that committed finds it removed, and still authorizes
// nothing (change skips authorize for a recorded operation).
func (c *queryCall) existing(id catalog.QueryID) (catalog.Query, error) {
	q, err := c.svc.store.GetQuery(c.ctx(), c.library, id)
	if errors.Is(err, catalog.ErrNotFound) {
		return q, failf(http.StatusNotFound, "no such query")
	}
	return q, err
}

func (c *queryCall) rename(id catalog.QueryID) error {
	b, err := c.body()
	if err != nil {
		return err
	}
	name, err := cleanCollectionName(b.Name)
	if err != nil {
		return err
	}
	if b.Folders != nil || b.Extensions != nil || b.NameContains != "" || b.ModifiedWithinSeconds != 0 || b.Limit != 0 {
		return fmt.Errorf("%w: rename takes only name (a query's definition never changes; create a new one)", errCollectionInput)
	}
	opID, err := c.operationID(b.OperationID)
	if err != nil {
		return err
	}
	q, err := c.existing(id)
	if err != nil {
		return err
	}
	return c.change(opID, id, func() error { return c.authorize(q.Definition) },
		[]catalog.Change{{RenameQuery: &catalog.RenameQuery{QueryID: id, Name: name}}})
}

func (c *queryCall) remove(id catalog.QueryID) error {
	b, err := c.body()
	if err != nil {
		return err
	}
	if b.Name != nil || b.Folders != nil || b.Extensions != nil || b.NameContains != "" || b.ModifiedWithinSeconds != 0 || b.Limit != 0 {
		return fmt.Errorf("%w: delete takes only operation_id", errCollectionInput)
	}
	opID, err := c.operationID(b.OperationID)
	if err != nil {
		return err
	}
	q, err := c.existing(id)
	if err != nil {
		return err
	}
	return c.change(opID, id, func() error { return c.authorize(q.Definition) },
		[]catalog.Change{{RemoveQuery: &catalog.RemoveQuery{QueryID: id}}})
}
