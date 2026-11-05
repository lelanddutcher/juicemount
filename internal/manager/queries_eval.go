package manager

// queries_eval.go: evaluating a saved query (continuity slice 5) over the
// files the continuity catalog knows.
//
// A query is deterministic and unranked: the files under one of its
// folders, optionally whose name contains a string (ignoring case; both
// compared in Unicode NFC, so a name stored decomposed by macOS matches),
// whose extension is in a set, modified within a duration; newest first by
// modification time, then by path; at most its limit (default 200, at most
// 500).
//
// Candidates come from the catalog: every active path hint (one hint per
// bound file; binder.Hints) under one of the query's folders and passing
// its name and extension filters. A hint is only a candidate: each one is
// resolved on the volume by its inode (binder.Locate: the hint while it
// still names the inode, else its current visible name) and the file's
// modification time read there, and only then are the filters applied, to
// the live path. So a file moved out of the folder, renamed away or deleted
// drops out, and one whose folder was renamed within the query's folder is
// linked at its new path. Residual: a file whose hint is stale (a folder
// renamed without an event per file) and points outside the query's folders
// is not a candidate until its next event, so a folder renamed INTO a
// query's folder is not seen until then. A file whose path cannot be read
// is "unknown", never "gone".
//
// Bounds. Each query keeps at most queryMaxCandidates candidates: the most
// recent by the hint's catalog sequence, which moves when a file is bound,
// moved or saved in place (the binder's recency marker, coalesced to once
// per 10 minutes per file); the rest are counted and the query reported as
// capped (examined N of M). A catalog scan reads every active hint once
// (local SQLite, no volume reads) for all the queries that need
// candidates. Each examined candidate costs a few volume point reads (a
// stat and a lookup; every name only when the hint moved), and a run
// (queryRun) stops at its budget and resumes where it stopped.

import (
	"container/heap"
	"context"
	"errors"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/unicode/norm"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const (
	queryDefaultLimit = 200
	// queryMaxCandidates bounds the files one evaluation resolves on the
	// volume: four times the largest limit.
	queryMaxCandidates = 4 * catalog.MaxQueryLimit
	// queryResultsFresh: the results API answers from an evaluation at
	// most this old (or kept current by the views) instead of running the
	// query again.
	queryResultsFresh = time.Minute
)

// queryCandidateCap is queryMaxCandidates (a variable for tests).
var queryCandidateCap = queryMaxCandidates

// queryCandidates is one query's candidates from a catalog scan: at most
// queryMaxCandidates hints, the most recently recorded kept.
type queryCandidates struct {
	hints  hintHeap
	capped int // matching hints left out
}

// hintHeap is a min-heap by (sequence, asset ID): the root is the first to
// drop when a newer hint arrives.
type hintHeap []binder.Hint

func (h hintHeap) Len() int { return len(h) }
func (h hintHeap) Less(i, j int) bool {
	if h[i].Sequence != h[j].Sequence {
		return h[i].Sequence < h[j].Sequence
	}
	return h[i].AssetID < h[j].AssetID
}
func (h hintHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *hintHeap) Push(x any)   { *h = append(*h, x.(binder.Hint)) }
func (h *hintHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func (c *queryCandidates) add(h binder.Hint, max int) {
	if len(c.hints) < max {
		heap.Push(&c.hints, h)
		return
	}
	c.capped++
	if top := c.hints[0]; top.Sequence < h.Sequence || (top.Sequence == h.Sequence && top.AssetID < h.AssetID) {
		c.hints[0] = h
		heap.Fix(&c.hints, 0)
	}
}

// ordered returns the candidates newest first (by sequence, then asset ID).
func (c *queryCandidates) ordered() []binder.Hint {
	if c == nil {
		return nil
	}
	out := append([]binder.Hint(nil), c.hints...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sequence != out[j].Sequence {
			return out[i].Sequence > out[j].Sequence
		}
		return out[i].AssetID < out[j].AssetID
	})
	return out
}

// queryHit is one file a query found, at its live path.
type queryHit struct {
	AssetID continuity.AssetID
	Path    string
	Mtime   time.Time
}

// queryResult is one evaluation.
type queryResult struct {
	Hits     []queryHit           // stable order, at most the limit
	Unknown  []continuity.AssetID // candidates whose path could not be read
	Over     int                  // matches beyond the limit
	Capped   int                  // candidates left unexamined
	Examined int                  // candidates examined
	Ops      int                  // candidates resolved on the volume
}

// queryRun is one evaluation in progress: its candidates, where it is, and
// what it found so far. now is the time the run started (modified-within
// is measured from it).
type queryRun struct {
	def     catalog.QueryDefinition
	now     time.Time
	cands   []binder.Hint
	capped  int
	next    int
	hits    []queryHit
	unknown []continuity.AssetID
	seen    map[string]bool
	stale   bool // a change arrived while it ran: run again when done
}

func newQueryRun(def catalog.QueryDefinition, cands *queryCandidates, now time.Time) *queryRun {
	r := &queryRun{def: def, now: now, cands: cands.ordered(), seen: map[string]bool{}}
	if cands != nil {
		r.capped = cands.capped
	}
	return r
}

func (r *queryRun) done() bool { return r.next >= len(r.cands) }

// hintOf returns the candidate of an asset (for a re-check).
func (r *queryRun) hintOf(a continuity.AssetID) (binder.Hint, bool) {
	for _, h := range r.cands {
		if h.AssetID == a {
			return h, true
		}
	}
	return binder.Hint{}, false
}

// result sorts and cuts what the run found.
func (r *queryRun) result(ops int) queryResult {
	res := queryResult{Hits: append([]queryHit(nil), r.hits...), Unknown: r.unknown, Capped: r.capped, Examined: len(r.cands), Ops: ops}
	sortHits(res.Hits)
	if len(res.Hits) > r.def.Limit {
		res.Over = len(res.Hits) - r.def.Limit
		res.Hits = res.Hits[:r.def.Limit]
	}
	return res
}

func sortHits(hits []queryHit) {
	sort.Slice(hits, func(i, j int) bool {
		a, b := hits[i], hits[j]
		if !a.Mtime.Equal(b.Mtime) {
			return a.Mtime.After(b.Mtime)
		}
		return a.Path < b.Path
	})
}

// inQueryFolders: p is under one of the query's folders and not under an
// excluded root (the smart-folder roots; compared ignoring case).
func inQueryFolders(def catalog.QueryDefinition, p string, exclude []string) bool {
	p = norm.NFC.String(p) // folders are stored NFC
	top, _, _ := strings.Cut(p, "/")
	for _, r := range exclude {
		if strings.EqualFold(top, r) {
			return false
		}
	}
	for _, f := range def.Prefixes {
		if underFolder(p, f) && p != f {
			return true
		}
	}
	return false
}

// queryNameMatches applies the name and extension filters to p's last
// component (NFC, lower case).
func queryNameMatches(def catalog.QueryDefinition, p string) bool {
	base := strings.ToLower(norm.NFC.String(path.Base(p)))
	if def.NameContains != "" && !strings.Contains(base, strings.ToLower(norm.NFC.String(def.NameContains))) {
		return false
	}
	if len(def.Extensions) == 0 {
		return true
	}
	ext := strings.TrimPrefix(path.Ext(base), ".")
	for _, e := range def.Extensions {
		if ext == e {
			return true
		}
	}
	return false
}

// activeQueries lists every query that is not removed.
func (s *continuityService) activeQueries(ctx context.Context) ([]catalog.Query, error) {
	library, err := s.store.LibraryID()
	if err != nil {
		return nil, err
	}
	var out []catalog.Query
	var after catalog.QueryID
	for {
		page, err := s.store.ListQueries(ctx, library, after, catalog.MaxPageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < catalog.MaxPageSize {
			return out, nil
		}
		after = page[len(page)-1].ID
	}
}

// scanQueries reads the catalog's hints once and keeps each query's
// candidates (catalog reads only).
func (s *continuityService) scanQueries(ctx context.Context, defs map[catalog.QueryID]catalog.QueryDefinition, exclude []string) (map[catalog.QueryID]*queryCandidates, error) {
	out := make(map[catalog.QueryID]*queryCandidates, len(defs))
	for id := range defs {
		out[id] = &queryCandidates{}
	}
	if len(defs) == 0 {
		return out, nil
	}
	err := s.binder.Hints(ctx, func(h binder.Hint) bool {
		for id, def := range defs {
			if inQueryFolders(def, h.Path, exclude) && queryNameMatches(def, h.Path) {
				out[id].add(h, queryCandidateCap)
			}
		}
		return ctx.Err() == nil
	})
	if err == nil {
		err = ctx.Err()
	}
	return out, err
}

// locateHit resolves one candidate on the volume and applies the filters
// to its live path: the hit, or ok=false (gone, or no longer matching).
// binder.ErrUnresolved for an unreadable file.
func (s *continuityService) locateHit(ctx context.Context, def catalog.QueryDefinition, h binder.Hint, exclude []string, now time.Time) (queryHit, bool, error) {
	p, mtime, err := s.binder.Locate(ctx, h.Ino, h.Path)
	if err != nil || p == "" {
		return queryHit{}, false, err
	}
	within := time.Duration(def.ModifiedWithinSeconds) * time.Second
	if !inQueryFolders(def, p, exclude) || !queryNameMatches(def, p) || (within > 0 && now.Sub(mtime) > within) {
		return queryHit{}, false, nil
	}
	return queryHit{AssetID: h.AssetID, Path: p, Mtime: mtime}, true, nil
}

// advanceQuery resolves the run's next candidates, at most maxOps of them
// (0: no bound) and not past until (zero: no bound). It returns how many
// it resolved. binder.ErrNotReady while the volume is not connected; a
// failed read of one file only makes that file unknown.
func (s *continuityService) advanceQuery(ctx context.Context, r *queryRun, exclude []string, maxOps int, until time.Time) (int, error) {
	ops := 0
	for !r.done() {
		if maxOps > 0 && ops >= maxOps || !until.IsZero() && !time.Now().Before(until) {
			return ops, nil
		}
		if err := ctx.Err(); err != nil {
			return ops, err
		}
		h := r.cands[r.next]
		ops++
		hit, ok, err := s.locateHit(ctx, r.def, h, exclude, r.now)
		switch {
		case errors.Is(err, binder.ErrNotReady):
			return ops, err
		case errors.Is(err, binder.ErrUnresolved):
			r.unknown = append(r.unknown, h.AssetID)
		case err != nil:
			return ops, err
		case ok && !r.seen[hit.Path]:
			r.seen[hit.Path] = true
			r.hits = append(r.hits, hit)
		}
		r.next++
	}
	return ops, nil
}

// evaluateQuery runs a query to the end over its candidates.
func (s *continuityService) evaluateQuery(ctx context.Context, def catalog.QueryDefinition, cands *queryCandidates, exclude []string, now time.Time) (queryResult, error) {
	r := newQueryRun(def, cands, now)
	ops, err := s.advanceQuery(ctx, r, exclude, 0, time.Time{})
	if err != nil {
		return queryResult{Ops: ops}, err
	}
	return r.result(ops), nil
}

// runQuery evaluates one query on its own: a catalog scan for it alone,
// then evaluateQuery (the results API, when no recent evaluation exists).
func (s *continuityService) runQuery(ctx context.Context, id catalog.QueryID, def catalog.QueryDefinition, exclude []string, now time.Time) (queryResult, error) {
	cands, err := s.scanQueries(ctx, map[catalog.QueryID]catalog.QueryDefinition{id: def}, exclude)
	if err != nil {
		return queryResult{}, err
	}
	return s.evaluateQuery(ctx, def, cands[id], exclude, now)
}

// queryResultCache holds each query's last complete evaluation (from the
// views or the results API).
type queryResultCache struct {
	mu sync.Mutex
	m  map[catalog.QueryID]cachedQueryResult
}

type cachedQueryResult struct {
	res queryResult
	at  time.Time
	// byViews: written by the query views after linking, so they keep it
	// current while they run.
	byViews bool
}

func (c *queryResultCache) put(id catalog.QueryID, r cachedQueryResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[catalog.QueryID]cachedQueryResult{}
	}
	c.m[id] = r
}

// demote marks an entry as no longer kept current by the views.
func (c *queryResultCache) demote(id catalog.QueryID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.m[id]; ok {
		r.byViews = false
		c.m[id] = r
	}
}

func (c *queryResultCache) get(id catalog.QueryID) (cachedQueryResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.m[id]
	return r, ok
}

// keep drops the entries of queries not in active.
func (c *queryResultCache) keep(active map[catalog.QueryID]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range c.m {
		if !active[id] {
			delete(c.m, id)
		}
	}
}

// rememberQuery stores a complete evaluation for the results API, as the
// views' (once linked) or the API's own.
func (s *continuityService) rememberQuery(id catalog.QueryID, res queryResult, at time.Time, byViews bool) {
	s.results.put(id, cachedQueryResult{res, at, byViews})
}

// demoteQuery: the view of the query failed to rebuild, so its remembered
// evaluation is no longer kept current.
func (s *continuityService) demoteQuery(id catalog.QueryID) { s.results.demote(id) }

// forgetQueries drops the remembered results of queries no longer active.
func (s *continuityService) forgetQueries(active map[catalog.QueryID]bool) { s.results.keep(active) }
