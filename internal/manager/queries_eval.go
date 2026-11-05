package manager

// queries_eval.go: evaluating a saved query (continuity slice 5) over the
// files the continuity catalog knows.
//
// A query is deterministic and unranked: the files under one of its
// folders, optionally whose name contains a string (ignoring case), whose
// extension is in a set, modified within a duration; newest first by
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
// Bounds. The catalog scan reads every active hint once per pass (local
// SQLite, no volume reads), shared by every query rebuilt in that pass.
// Each query keeps at most queryMaxCandidates candidates: the most recently
// recorded hints (a file bound or moved most recently), by catalog sequence;
// the rest are counted (candidates_over_cap) and not examined. Each
// examined candidate costs a few volume point reads (a stat and a lookup;
// every name only when the hint moved), counted against the pass budget.

import (
	"container/heap"
	"context"
	"errors"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const (
	queryDefaultLimit = 200
	// queryMaxCandidates bounds the files one evaluation resolves on the
	// volume: four times the largest limit.
	queryMaxCandidates = 4 * catalog.MaxQueryLimit
)

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
	Hits    []queryHit           // stable order, at most the limit
	Unknown []continuity.AssetID // candidates whose path could not be read
	Over    int                  // matches beyond the limit
	Capped  int                  // candidates left unexamined
	Ops     int                  // candidates resolved on the volume
}

// inQueryFolders: p is under one of the query's folders and not under an
// excluded root (the smart-folder roots; compared ignoring case).
func inQueryFolders(def catalog.QueryDefinition, p string, exclude []string) bool {
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
// component.
func queryNameMatches(def catalog.QueryDefinition, p string) bool {
	base := strings.ToLower(path.Base(p))
	if def.NameContains != "" && !strings.Contains(base, strings.ToLower(def.NameContains)) {
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
				out[id].add(h, queryMaxCandidates)
			}
		}
		return ctx.Err() == nil
	})
	if err == nil {
		err = ctx.Err()
	}
	return out, err
}

// evaluateQuery resolves the candidates on the volume and applies the
// filters to the live paths. binder.ErrNotReady while the volume is not
// connected; a failed read of one file only makes that file unknown.
func (s *continuityService) evaluateQuery(ctx context.Context, def catalog.QueryDefinition, cands *queryCandidates, exclude []string, now time.Time) (queryResult, error) {
	var res queryResult
	if cands != nil {
		res.Capped = cands.capped
	}
	within := time.Duration(def.ModifiedWithinSeconds) * time.Second
	seen := map[string]bool{}
	for _, h := range cands.ordered() {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		res.Ops++
		p, mtime, err := s.binder.Locate(ctx, h.Ino, h.Path)
		switch {
		case errors.Is(err, binder.ErrNotReady):
			return res, err
		case errors.Is(err, binder.ErrUnresolved):
			res.Unknown = append(res.Unknown, h.AssetID)
			continue
		case err != nil:
			return res, err
		case p == "" || seen[p]:
			continue
		}
		if !inQueryFolders(def, p, exclude) || !queryNameMatches(def, p) || (within > 0 && now.Sub(mtime) > within) {
			continue
		}
		seen[p] = true
		res.Hits = append(res.Hits, queryHit{AssetID: h.AssetID, Path: p, Mtime: mtime})
	}
	sort.Slice(res.Hits, func(i, j int) bool {
		a, b := res.Hits[i], res.Hits[j]
		if !a.Mtime.Equal(b.Mtime) {
			return a.Mtime.After(b.Mtime)
		}
		return a.Path < b.Path
	})
	if len(res.Hits) > def.Limit {
		res.Over = len(res.Hits) - def.Limit
		res.Hits = res.Hits[:def.Limit]
	}
	return res, nil
}

// runQuery evaluates one query on its own: a catalog scan for it alone,
// then evaluateQuery (the results API).
func (s *continuityService) runQuery(ctx context.Context, id catalog.QueryID, def catalog.QueryDefinition, exclude []string, now time.Time) (queryResult, error) {
	cands, err := s.scanQueries(ctx, map[catalog.QueryID]catalog.QueryDefinition{id: def}, exclude)
	if err != nil {
		return queryResult{}, err
	}
	return s.evaluateQuery(ctx, def, cands[id], exclude, now)
}
