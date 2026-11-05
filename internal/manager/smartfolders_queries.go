package manager

// smartfolders_queries.go: continuity slice 5, saved queries as smart
// folders (experimental). With JM_SMART_FOLDERS=on AND JM_CONTINUITY=on the
// smart-folder service also keeps one view per saved query (queries.go),
// <JM_SMART_FOLDERS_QUERIES_ROOT>/<query name>/ (default root "Queries"),
// holding one relative symlink per file the query finds (queries_eval.go),
// newest first. Either switch off: no query view is created or changed.
//
// Everything about the views is as for collection views
// (smartfolders_collections.go; the same engine, catalogViews): records in
// smart-folders.json (version 4) before anything is created, a user-made
// root or view left alone, folder names from the query names with clashes
// numbered, at most collectionViewsMaxViews views, removal of an empty view
// by a direct rmdir the RESP proxy admits (the proxy's smart_folder_roots
// includes the query root under the same two switches), views refused
// removal parked, the same pass budget. No view links into another smart
// root: Recents always, Collections while its views run, and query views
// are left out of /Recents and collection views once the Manager owns the
// query root.
//
// What differs. A view holds the query's results (at most its limit), not
// members: the files are found again at every rebuild. A file whose path
// cannot be read keeps the link it had, if any (unknown is not gone). A
// view is rebuilt when its query is renamed, when a change event names a
// path at, above or below one of its folders, when the binder records a
// change to a file's hint there (a file bound, moved or gone), after a
// query change through the API, and every ten minutes (files age out of a
// modified-within window without any event). Events are debounced
// (queryViewsDebounce). The candidate scan is shared by all the queries a
// pass rebuilds.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

const (
	smartFoldersQueriesRoot = "Queries"
	queryViewsDebounce      = 5 * time.Second
)

// querySource is what the query views read (continuityService).
type querySource interface {
	activeQueries(ctx context.Context) ([]catalog.Query, error)
	scanQueries(ctx context.Context, defs map[catalog.QueryID]catalog.QueryDefinition, exclude []string) (map[catalog.QueryID]*queryCandidates, error)
	evaluateQuery(ctx context.Context, def catalog.QueryDefinition, cands *queryCandidates, exclude []string, now time.Time) (queryResult, error)
}

type (
	queryViews      = catalogViews[catalog.QueryID, querySource]
	queryViewsState = catalogViewsState[catalog.QueryID]
)

// queryScan is one pass's catalog scan for every active query.
type queryScan struct {
	cands map[catalog.QueryID]*queryCandidates
	err   error
}

// QueryViewStats is what the query views did in their last pass.
type QueryViewStats struct {
	Root              string    `json:"root"`
	MaxViews          int       `json:"max_views"`
	MaxLinksPerView   int       `json:"max_links_per_view"`
	Queries           int       `json:"queries"`
	Views             int       `json:"views"`
	Links             int       `json:"links"`
	OverflowQueries   int       `json:"overflow_queries"`    // beyond MaxViews: no view
	OverLimit         int       `json:"results_over_limit"`  // matches beyond a query's limit
	CandidatesOverCap int       `json:"candidates_over_cap"` // not examined (queryMaxCandidates)
	GoneResults       int       `json:"gone_results"`        // found gone while linking
	RefusedLinks      int       `json:"refused_links"`
	UnresolvedResults int       `json:"unresolved_results"` // path unreadable: link kept as it was, or none
	Deferred          int       `json:"deferred_queries"`
	MaxRecorded       int       `json:"max_recorded_views"`
	ViewsAtCap        int       `json:"views_waiting_at_cap"`
	ViewsParked       int       `json:"views_parked"`
	LostOverflow      int       `json:"lost_overflow,omitempty"`
	ViewsLeft         []string  `json:"views_left,omitempty"`
	RootForeign       bool      `json:"root_not_created_by_manager,omitempty"`
	Problems          []string  `json:"problems,omitempty"`
	LastPass          time.Time `json:"last_pass"`
}

func queryKind() viewKind[catalog.QueryID, querySource] {
	return viewKind[catalog.QueryID, querySource]{
		noun: "query", plural: "queries", rootVar: "JM_SMART_FOLDERS_QUERIES_ROOT", debounce: queryViewsDebounce,
		root: func(s *smartFoldersService) string { return s.cfg.Settings.QueriesRoot },
		off:  func(s *smartFoldersService) string { return s.cfg.Settings.QueriesOff },
		exclude: func(s *smartFoldersService) []string {
			out := []string{s.cfg.Settings.Root}
			if c := s.cfg.Settings.CollectionsRoot; s.collectionsRootActive(c) {
				out = append(out, c)
			}
			return out
		},
		list: func(ctx context.Context, src querySource) ([]viewItem[catalog.QueryID], error) {
			qs, err := src.activeQueries(ctx)
			out := make([]viewItem[catalog.QueryID], len(qs))
			for i, q := range qs {
				def := q.Definition
				out[i] = viewItem[catalog.QueryID]{ID: q.ID, Name: q.Name, CreatedSequence: q.CreatedSequence, UpdatedSequence: q.UpdatedSequence, query: &def}
			}
			return out, err
		},
		begin: func(s *smartFoldersService) { s.qscan = nil },
		build: (*smartFoldersService).reconcileQuery,
	}
}

// queriesRootActive is rootActive for the query views.
func (s *smartFoldersService) queriesRootActive(top string) bool { return s.qry.rootActive(s, top) }

// attachQueries turns query views on, reading src. It reports whether they
// run (a bad JM_SMART_FOLDERS_QUERIES_ROOT keeps them off).
func (s *smartFoldersService) attachQueries(src querySource) bool {
	return s.qry.attach(s, src, fmt.Sprintf("at most %d views of up to %d links", s.qry.maxViews, catalog.MaxQueryLimit))
}

// queriesChanged is called after a query change commits.
func (s *smartFoldersService) queriesChanged() { s.qry.schedule() }

// noteQueryEvent marks dirty the query views watching a folder an event
// names (or one above or below it).
func (s *smartFoldersService) noteQueryEvent(ev smartfolders.Event) {
	s.qry.notePaths(ev.Path, ev.OldPath)
}

// queriesOnlyCycle is the debounced pass after a change.
func (s *smartFoldersService) queriesOnlyCycle(ctx context.Context) { s.qry.onlyCycle(s, ctx) }

// queryExclude is what a query's results never include: the other smart
// roots and the query root itself.
func queryExclude(rec *smartfolders.Reconciler) []string {
	return append(append([]string(nil), rec.Exclude...), rec.Root)
}

// queryCandidatesFor returns one query's candidates from this pass's scan,
// scanning the catalog once (for every active query) at the first call.
func (s *smartFoldersService) queryCandidatesFor(ctx context.Context, src querySource, id catalog.QueryID, exclude []string) (*queryCandidates, error) {
	if s.qscan == nil {
		qs, err := src.activeQueries(ctx)
		defs := map[catalog.QueryID]catalog.QueryDefinition{}
		for _, q := range qs {
			defs[q.ID] = q.Definition
		}
		var cands map[catalog.QueryID]*queryCandidates
		if err == nil {
			cands, err = src.scanQueries(ctx, defs, exclude)
		}
		s.qscan = &queryScan{cands: cands, err: err}
	}
	return s.qscan.cands[id], s.qscan.err
}

// reconcileQuery makes one query's view hold a link to each file the query
// finds, at its live path. It returns how many candidates it resolved.
// While the volume is not connected the view is left as it is. A file found
// gone at the last moment makes the query run once more.
func (s *smartFoldersService) reconcileQuery(ctx context.Context, rec *smartfolders.Reconciler, src querySource, item viewItem[catalog.QueryID], folder string, prior *collSeen, now time.Time) (*collSeen, int, error) {
	c := &s.qry
	exclude := queryExclude(rec)
	cands, err := s.queryCandidatesFor(ctx, src, item.ID, exclude)
	if errors.Is(err, binder.ErrNotReady) {
		return nil, 0, fmt.Errorf("%s/%s: %w (%v); view left unchanged", rec.Root, folder, errMembersUnresolved, err)
	}
	if err != nil {
		return nil, 0, err
	}
	var had map[continuity.AssetID]string
	if prior != nil && prior.folder == folder {
		had = prior.assets
	}
	seen := &collSeen{seq: item.UpdatedSequence, folder: folder}
	ops := 0
	for pass := 0; pass < 2; pass++ {
		res, err := src.evaluateQuery(ctx, *item.query, cands, exclude, now)
		ops += res.Ops
		if errors.Is(err, binder.ErrNotReady) {
			return nil, ops, fmt.Errorf("%s/%s: %w (%v); view left unchanged", rec.Root, folder, errMembersUnresolved, err)
		}
		if err != nil {
			return nil, ops, err
		}
		var targets []string
		assets := map[continuity.AssetID]string{}
		linked := map[string]bool{}
		for _, h := range res.Hits {
			targets = append(targets, h.Path)
			assets[h.AssetID] = h.Path
			linked[h.Path] = true
		}
		// Unknown is not gone: a file whose path could not be read keeps
		// its link, within the limit.
		for _, a := range res.Unknown {
			if p, ok := had[a]; ok && !linked[p] && len(targets) < item.query.Limit {
				targets = append(targets, p)
				assets[a] = p
				linked[p] = true
			}
		}
		links := smartfolders.LinkNames(targets, rec.PreviousNames(folder))
		out, err := rec.Reconcile(folder, links)
		c.logView(s, folder, out, err)
		if err != nil {
			return nil, ops, err
		}
		gone, refused := 0, 0
		placed := map[string]bool{}
		for _, l := range links {
			placed[l.Target] = true
		}
		for _, r := range out.Refused {
			delete(placed, r.Target)
			if r.TargetGone() {
				gone++
			} else {
				refused++
			}
		}
		if gone > 0 && pass == 0 {
			continue // moved since it was resolved: run the query again
		}
		seen.missing, seen.refused, seen.unknown, seen.over, seen.capped = gone, refused, res.Unknown, res.Over, res.Capped
		seen.assets = map[continuity.AssetID]string{}
		for a, p := range assets {
			if placed[p] {
				seen.assets[a] = p
			}
		}
		for _, l := range links {
			if placed[l.Target] {
				seen.targets = append(seen.targets, l.Target)
			}
		}
		if ov := rec.Own.Views[folder]; ov != nil {
			seen.hasView, seen.links = true, len(ov.Links)
		}
		break
	}
	return seen, ops, nil
}

// queryStats returns the last pass's numbers (nil before the first).
func (s *smartFoldersService) queryStats() *QueryViewStats {
	st := s.qry.stats.Load()
	if st == nil {
		return nil
	}
	return &QueryViewStats{
		Root: st.Root, MaxViews: st.MaxViews, MaxLinksPerView: catalog.MaxQueryLimit,
		Queries: st.Collections, Views: st.Views, Links: st.Links, OverflowQueries: st.OverflowCollections,
		OverLimit: st.OverflowMembers, CandidatesOverCap: int(s.qry.capped.Load()), GoneResults: st.MissingMembers,
		RefusedLinks: st.RefusedLinks, UnresolvedResults: st.UnresolvedMembers, Deferred: st.Deferred,
		MaxRecorded: st.MaxRecorded, ViewsAtCap: st.ViewsAtCap, ViewsParked: st.ViewsParked, LostOverflow: st.LostOverflow,
		ViewsLeft: st.ViewsLeft, RootForeign: st.RootForeign, Problems: st.Problems, LastPass: st.LastPass,
	}
}
