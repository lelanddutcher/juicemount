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
// view is rebuilt when its query is created or renamed, when a change event
// names a path at, above or below one of its folders or the binder records
// a change to a file's hint there (bound, moved, gone, saved in place),
// and every ten minutes (files age out of a modified-within window without
// any event). File changes are debounced (queryViewsDebounce) and rebuild
// a view at most once a minute (queryViewsMinInterval), so an ingest does
// not run passes back to back.
//
// Pacing. A rebuild (a queryRun) resolves candidates within what is left
// of the pass budget and resumes in the next pass where it stopped; the
// view changes only once the run is complete. One catalog scan per pass
// serves the queries that need candidates; a query's candidates are kept
// until a change under its folders or the next sweep. A file found gone
// while linking is looked up again on its own.

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
	// queryViewsMinInterval: a view dirty only from file events is rebuilt
	// at most this often, so an ingest does not run passes back to back.
	queryViewsMinInterval = time.Minute
)

// querySource is what the query views read (continuityService).
type querySource interface {
	scanQueries(ctx context.Context, defs map[catalog.QueryID]catalog.QueryDefinition, exclude []string) (map[catalog.QueryID]*queryCandidates, error)
	advanceQuery(ctx context.Context, r *queryRun, exclude []string, maxOps int, until time.Time) (int, error)
	locateHit(ctx context.Context, def catalog.QueryDefinition, h binder.Hint, exclude []string, now time.Time) (queryHit, bool, error)
	activeQueries(ctx context.Context) ([]catalog.Query, error)
	rememberQuery(id catalog.QueryID, res queryResult, at time.Time, byViews bool)
	demoteQuery(id catalog.QueryID)
	forgetQueries(active map[catalog.QueryID]bool)
}

type (
	queryViews      = catalogViews[catalog.QueryID, querySource]
	queryViewsState = catalogViewsState[catalog.QueryID]
)

// queryState is the query views' working state (reconcile goroutine only):
// each query's candidates from the last catalog scan, kept until a change
// under its folders or the next sweep; the runs under way; and this
// pass's scan error.
type queryState struct {
	cands   map[catalog.QueryID]*queryCandidates
	runs    map[catalog.QueryID]*queryRun
	scanErr error
}

// QueryViewStats is what the query views did in their last pass.
type QueryViewStats struct {
	Root              string       `json:"root"`
	MaxViews          int          `json:"max_views"`
	MaxLinksPerView   int          `json:"max_links_per_view"`
	Queries           int          `json:"queries"`
	Views             int          `json:"views"`
	Links             int          `json:"links"`
	OverflowQueries   int          `json:"overflow_queries"`   // beyond MaxViews: no view
	OverLimit         int          `json:"results_over_limit"` // matches beyond a query's limit
	CappedQueries     []cappedView `json:"capped_queries,omitempty"`
	GoneResults       int          `json:"gone_results"` // found gone while linking
	RefusedLinks      int          `json:"refused_links"`
	UnresolvedResults int          `json:"unresolved_results"` // path unreadable: link kept as it was, or none
	Deferred          int          `json:"deferred_queries"`
	MaxRecorded       int          `json:"max_recorded_views"`
	ViewsAtCap        int          `json:"views_waiting_at_cap"`
	ViewsParked       int          `json:"views_parked"`
	LostOverflow      int          `json:"lost_overflow,omitempty"`
	ViewsLeft         []string     `json:"views_left,omitempty"`
	RootForeign       bool         `json:"root_not_created_by_manager,omitempty"`
	Problems          []string     `json:"problems,omitempty"`
	LastPass          time.Time    `json:"last_pass"`
}

func queryKind() viewKind[catalog.QueryID, querySource] {
	return viewKind[catalog.QueryID, querySource]{
		noun: "query", plural: "queries", rootVar: "JM_SMART_FOLDERS_QUERIES_ROOT",
		debounce: queryViewsDebounce, minInterval: queryViewsMinInterval,
		root: func(s *smartFoldersService) string { return s.cfg.Settings.QueriesRoot },
		off:  func(s *smartFoldersService) string { return s.cfg.Settings.QueriesOff },
		// The other roots the Manager owns: a user-made folder of one of
		// those names is ordinary data here (decision 2).
		exclude: func(s *smartFoldersService) []string {
			var out []string
			if s.recentsOwned.Load() {
				out = append(out, s.cfg.Settings.Root)
			}
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
		prepare: (*smartFoldersService).prepareQueries,
		busy:    func(s *smartFoldersService, id catalog.QueryID) bool { return s.qstate.runs[id] != nil },
		build:   (*smartFoldersService).reconcileQuery,
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
// roots the Manager owns and the query root itself.
func queryExclude(rec *smartfolders.Reconciler) []string {
	return append(append([]string(nil), rec.Exclude...), rec.Root)
}

// prepareQueries runs once a pass: it forgets what belongs to queries
// gone, drops the candidates of queries changed (or all of them at a
// sweep), marks a run under way stale when its query changed, and scans
// the catalog once for the jobs that need candidates.
func (s *smartFoldersService) prepareQueries(ctx context.Context, src querySource, all, jobs []viewItem[catalog.QueryID], dirty map[catalog.QueryID]bool, sweep bool) {
	q := &s.qstate
	q.scanErr = nil
	active := map[catalog.QueryID]bool{}
	for _, it := range all {
		active[it.ID] = true
	}
	for id := range q.runs {
		if !active[id] {
			delete(q.runs, id)
		}
	}
	for id := range q.cands {
		if !active[id] || sweep || dirty[id] {
			delete(q.cands, id)
		}
	}
	for id := range dirty {
		if r := q.runs[id]; r != nil {
			r.stale = true
		}
	}
	src.forgetQueries(active)
	defs := map[catalog.QueryID]catalog.QueryDefinition{}
	for _, it := range jobs {
		if q.runs[it.ID] == nil && q.cands[it.ID] == nil {
			defs[it.ID] = *it.query
		}
	}
	if len(defs) == 0 {
		return
	}
	rec := &smartfolders.Reconciler{Root: s.cfg.Settings.QueriesRoot, Exclude: s.qry.kind.exclude(s)}
	cands, err := src.scanQueries(ctx, defs, queryExclude(rec))
	if err != nil {
		q.scanErr = err
		return
	}
	for id, c := range cands {
		q.cands[id] = c
	}
}

// reconcileQuery runs a query (resuming its run) within the budget, then
// makes its view hold a link to each file found, at its live path. It
// returns how many candidates it resolved; errViewPartial when the budget
// ran out first. While the volume is not connected the view is left as it
// is. A file found gone at the last moment is looked up again, alone.
func (s *smartFoldersService) reconcileQuery(ctx context.Context, rec *smartfolders.Reconciler, src querySource, item viewItem[catalog.QueryID], folder string, prior *collSeen, now time.Time, budget viewBudget) (_ *collSeen, _ int, resultErr error) {
	c, q := &s.qry, &s.qstate
	defer func() {
		if resultErr != nil && !errors.Is(resultErr, errViewPartial) {
			src.demoteQuery(item.ID)
		}
	}()
	exclude := queryExclude(rec)
	notReady := func(err error) error {
		return fmt.Errorf("%s/%s: %w (%v); view left unchanged", rec.Root, folder, errMembersUnresolved, err)
	}
	run := q.runs[item.ID]
	if run == nil {
		if errors.Is(q.scanErr, binder.ErrNotReady) {
			return nil, 0, notReady(q.scanErr)
		}
		if q.scanErr != nil {
			return nil, 0, q.scanErr
		}
		run = newQueryRun(*item.query, q.cands[item.ID], now)
		q.runs[item.ID] = run
	}
	ops, err := src.advanceQuery(ctx, run, exclude, budget.ops, budget.until)
	if errors.Is(err, binder.ErrNotReady) {
		return nil, ops, notReady(err)
	}
	if err != nil {
		delete(q.runs, item.ID)
		return nil, ops, err
	}
	if !run.done() {
		return nil, ops, errViewPartial
	}
	delete(q.runs, item.ID)
	if run.stale {
		c.markDirty(item.ID) // changed while it ran: once more, after the minimum interval
	}

	var had map[continuity.AssetID]string
	if prior != nil && prior.folder == folder {
		had = prior.assets
	}
	seen := &collSeen{seq: item.UpdatedSequence, folder: folder}
	for pass := 0; pass < 2; pass++ {
		res := run.result(ops)
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
		gone, refused := map[string]bool{}, 0
		placed := map[string]bool{}
		for _, l := range links {
			placed[l.Target] = true
		}
		for _, r := range out.Refused {
			delete(placed, r.Target)
			if r.TargetGone() {
				gone[r.Target] = true
			} else {
				refused++
			}
		}
		if len(gone) > 0 && pass == 0 {
			// Moved since it was resolved: look those files up again.
			kept := run.hits[:0]
			for _, h := range run.hits {
				if !gone[h.Path] {
					kept = append(kept, h)
					continue
				}
				delete(run.seen, h.Path)
				cand, ok := run.hintOf(h.AssetID)
				if !ok {
					continue
				}
				ops++
				hit, ok, err := src.locateHit(ctx, run.def, cand, exclude, run.now)
				if err == nil && ok && !run.seen[hit.Path] {
					run.seen[hit.Path] = true
					kept = append(kept, hit)
				}
			}
			run.hits = kept
			continue
		}
		seen.missing, seen.refused, seen.unknown, seen.over, seen.capped, seen.examined = len(gone), refused, res.Unknown, res.Over, res.Capped, res.Examined
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
		src.rememberQuery(item.ID, res, time.Now(), true)
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
	var capped []cappedView
	if p := s.qry.capped.Load(); p != nil {
		capped = *p
	}
	return &QueryViewStats{
		Root: st.Root, MaxViews: st.MaxViews, MaxLinksPerView: catalog.MaxQueryLimit,
		Queries: st.Collections, Views: st.Views, Links: st.Links, OverflowQueries: st.OverflowCollections,
		OverLimit: st.OverflowMembers, CappedQueries: capped, GoneResults: st.MissingMembers,
		RefusedLinks: st.RefusedLinks, UnresolvedResults: st.UnresolvedMembers, Deferred: st.Deferred,
		MaxRecorded: st.MaxRecorded, ViewsAtCap: st.ViewsAtCap, ViewsParked: st.ViewsParked, LostOverflow: st.LostOverflow,
		ViewsLeft: st.ViewsLeft, RootForeign: st.RootForeign, Problems: st.Problems, LastPass: st.LastPass,
	}
}
