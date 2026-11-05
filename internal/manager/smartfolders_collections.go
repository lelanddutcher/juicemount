package manager

// smartfolders_collections.go: continuity slice 4, Collections as smart
// folders (experimental). With JM_SMART_FOLDERS=on AND JM_CONTINUITY=on the
// smart-folder service also keeps one view per collection,
// <JM_SMART_FOLDERS_COLLECTIONS_ROOT>/<collection name>/ (default root
// "Collections"), holding one relative symlink per member whose file is
// present. Either switch off: no collection view is created or changed
// (views already written stay as they are, with their records).
//
// Same rules as /Recents (internal/smartfolders): a second Reconciler with
// its own Ownership records, written to smart-folders.json before anything
// is created; a root or view the Manager did not create is left alone (a
// user-made /Collections stops collection views, never /Recents, and is
// looked at again every ten minutes); nothing a user puts inside a view is
// ever modified or removed; every write checks out the view's project
// lease. Each kind of view refuses targets under the other's root, and
// /Recents no longer lists files under the collections root.
//
// Where a member is. A member is an asset (a JuiceFS inode, via the
// binder). Its link points at the file's current path, resolved live from
// the volume (binder.AssetByID: inode -> name); the binder's path hint is
// never trusted. A member with no visible name (deleted, only in the
// trash, only under hidden names) gets no link; it stays a member, and its
// link comes back when the file does. A member whose path cannot be read
// leaves the whole view as it is until the next try ("unknown is not
// gone").
//
// Names. A view folder is the collection's name with "/", ":" and control
// characters replaced by "-", leading dots dropped and cut to NAME_MAX
// (smartfolders.FolderName); empty is "Untitled". Collections are ranked
// by creation (catalog sequence, then ID); when two names clash
// (case-insensitively) the earlier keeps the plain name and the later gets
// "<name> (2)", "(3)"... A collection keeps its folder while its name
// still yields it, so deleting the first of two clashing collections does
// not rename the second. Links inside a view follow the /Recents rules
// (smartfolders.LinkNames): the member that joined first gets the plain
// file name, a later one with the same name is labelled with its parent
// folder, then numbered; a member keeps its link name while it stays.
//
// Changes. A rename of a collection creates the view under the new name and
// removes the old one; a deleted collection's view is removed; a member
// that leaves loses its link. Removing a view first removes the Manager's
// links, then the directory, only once empty: a view holding a user's
// file is left in place (with that file) and retried later. Paths an
// editing app remembered through a view stop working when the view or
// link goes away; the app sees media offline until it is relinked to the
// real path.
//
// Through the RESP proxy (the default deployment) a view folder is a
// project root (project depth 2), and the proxy lets only an admin rename
// or remove one. There the old view of a renamed or deleted collection is
// emptied but stays, recorded as the Manager's (reused if a collection
// takes that name again), reported under views_left, and retried with
// backoff; an admin can remove the empty folder.
//
// Limits. At most collectionViewsMaxViews views (collections beyond it, by
// creation order, get none) and collectionViewsMaxLinks links per view (the
// members that joined first; later ones get none). Both overflows are
// counted in GET /api/continuity/status (collection_views), as are missing
// members, refused links and views left in use.
//
// When. Every reconcile cycle (JM_SMART_FOLDERS interval) reads the list
// of collections, which is cheap, and rebuilds only a view whose
// collection changed (its catalog sequence moved, or its folder name), is
// marked dirty, or lost its directory. A collection change through the API
// or a juicemount:metadata event naming a member's path (or a folder
// above it) marks it dirty and runs a collections-only pass after a short
// debounce. Every ten minutes all views are rebuilt, which catches what
// events miss (a member file coming back, a change made while the Manager
// was down). All of it runs on the smart-folder goroutine: nothing here is
// on the NFS or Finder path.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

const (
	smartFoldersCollectionsRoot = "Collections"
	collectionViewsMaxViews     = 128
	collectionViewsMaxLinks     = 500
	collectionViewsSweep        = 10 * time.Minute
	collectionViewsDebounce     = 2 * time.Second
)

// errMembersUnresolved: a member's path could not be read (the volume is
// not connected yet, or a read failed). The view is retried at the next
// cycle without backing off.
var errMembersUnresolved = errors.New("members could not be resolved on the volume")

// collectionViewsState is the collection views' part of smart-folders.json
// (version 3).
type collectionViewsState struct {
	Ownership *smartfolders.Ownership `json:"ownership,omitempty"`
	// Folders maps a collection to its view folder name, so the name stays
	// put (FolderNames prev).
	Folders map[catalog.CollectionID]string `json:"folders,omitempty"`
}

// collectionSource is what the views read: the continuity catalog and the
// binder (continuityService).
type collectionSource interface {
	// activeCollections lists every collection that is not removed.
	activeCollections(ctx context.Context) ([]catalog.Collection, error)
	// collectionMembers lists a collection's active members.
	collectionMembers(ctx context.Context, id catalog.CollectionID) ([]catalog.CollectionMember, error)
	// resolveAsset returns the file's current path, resolved from the
	// volume: state "present" (path set), "missing" or "unknown".
	resolveAsset(ctx context.Context, id continuity.AssetID) (string, string, error)
}

// CollectionViewStats is what the views did in their last pass.
type CollectionViewStats struct {
	Root                string `json:"root"`
	MaxViews            int    `json:"max_views"`
	MaxLinksPerView     int    `json:"max_links_per_view"`
	Collections         int    `json:"collections"`
	Views               int    `json:"views"`
	Links               int    `json:"links"`
	MissingMembers      int    `json:"missing_members"`
	OverflowCollections int    `json:"overflow_collections"` // beyond MaxViews: no view
	OverflowMembers     int    `json:"overflow_members"`     // beyond MaxLinksPerView: no link
	RefusedLinks        int    `json:"refused_links"`        // e.g. the name holds a user's entry
	// ViewsLeft: views no collection wants that are still there, with why:
	// a user's entry inside, or the volume refused removing the folder.
	ViewsLeft   []string  `json:"views_left,omitempty"`
	RootForeign bool      `json:"root_not_created_by_manager,omitempty"`
	Problems    []string  `json:"problems,omitempty"`
	LastPass    time.Time `json:"last_pass"`
}

// collSeen is a collection as last reconciled.
type collSeen struct {
	seq     int64
	folder  string
	hasView bool
	targets []string // linked paths (for the event index)
	links   int
	missing int
	over    int
	refused int
}

type collectionViews struct {
	own     *smartfolders.Ownership
	folders map[catalog.CollectionID]string
	src     atomic.Pointer[collectionSourceRef]
	wake    chan struct{}

	maxViews, maxLinks int
	sweep, debounce    time.Duration

	// Reconcile-goroutine state.
	seen        map[catalog.CollectionID]*collSeen
	sweepAt     time.Time
	rootRetryAt time.Time
	backoff     map[string]*folderBackoff // by view folder
	left        map[string]string         // view folder -> why it was not removed

	// Event-path state.
	mu      sync.Mutex
	dirty   map[catalog.CollectionID]bool
	files   map[string][]catalog.CollectionID // linked path -> collections
	dirs    map[string][]catalog.CollectionID // folder above a linked path -> collections
	pending bool                              // a debounced pass is scheduled

	stats atomic.Pointer[CollectionViewStats]
}

type collectionSourceRef struct{ collectionSource }

func (c *collectionViews) init() {
	c.own = &smartfolders.Ownership{}
	c.own.Normalize()
	c.folders = map[catalog.CollectionID]string{}
	c.wake = make(chan struct{}, 1)
	c.maxViews, c.maxLinks = collectionViewsMaxViews, collectionViewsMaxLinks
	c.sweep, c.debounce = collectionViewsSweep, collectionViewsDebounce
	c.seen = map[catalog.CollectionID]*collSeen{}
	c.backoff = map[string]*folderBackoff{}
	c.left = map[string]string{}
	c.dirty = map[catalog.CollectionID]bool{}
	c.files = map[string][]catalog.CollectionID{}
	c.dirs = map[string][]catalog.CollectionID{}
}

func (c *collectionViews) load(st *collectionViewsState) {
	if st == nil {
		return
	}
	if st.Ownership != nil {
		c.own = st.Ownership
		c.own.Normalize()
	}
	for id, f := range st.Folders {
		c.folders[id] = f
	}
}

// state is what persist writes; nil when there is nothing to keep, so the
// file stays version 2.
func (c *collectionViews) state() *collectionViewsState {
	if c.own.Root == nil && len(c.own.Views) == 0 && len(c.folders) == 0 {
		return nil
	}
	return &collectionViewsState{Ownership: c.own, Folders: c.folders}
}

func (c *collectionViews) source() collectionSource {
	if r := c.src.Load(); r != nil {
		return r.collectionSource
	}
	return nil
}

// collectionsRootActive reports whether top is the root of collection
// views that are running (the tracker's Ignore; runs on the event path).
func (s *smartFoldersService) collectionsRootActive(top string) bool {
	return top != "" && top == s.cfg.Settings.CollectionsRoot && s.coll.src.Load() != nil
}

func (s *smartFoldersService) collectionsDue(now time.Time) bool {
	return s.coll.src.Load() != nil && !now.Before(s.coll.rootRetryAt)
}

// attachCollections turns collection views on, reading src. It reports
// whether they run (a bad JM_SMART_FOLDERS_COLLECTIONS_ROOT keeps them off).
func (s *smartFoldersService) attachCollections(src collectionSource) bool {
	if s.cfg.Settings.CollectionsRoot == "" {
		log.Printf("manager: smart folders: collection views OFF: %s", s.cfg.Settings.CollectionsOff)
		return false
	}
	s.coll.src.Store(&collectionSourceRef{src})
	log.Printf("manager: smart folders: collection views ON (experimental): /%s/<collection> (at most %d views of %d links)",
		s.cfg.Settings.CollectionsRoot, s.coll.maxViews, s.coll.maxLinks)
	s.scheduleCollections()
	return true
}

// collectionsChanged is called after a collection change commits.
func (s *smartFoldersService) collectionsChanged() { s.scheduleCollections() }

// scheduleCollections runs a collections-only pass after the debounce; calls
// in between coalesce. It never blocks.
func (s *smartFoldersService) scheduleCollections() {
	c := &s.coll
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending {
		return
	}
	c.pending = true
	time.AfterFunc(c.debounce, func() {
		c.mu.Lock()
		c.pending = false
		c.mu.Unlock()
		select {
		case c.wake <- struct{}{}:
		default:
		}
	})
}

// noteCollectionEvent marks the collections linking a path an event names
// (or a file below a folder it names) dirty. Event path: map lookups only.
func (s *smartFoldersService) noteCollectionEvent(ev smartfolders.Event) {
	if s.coll.src.Load() == nil {
		return
	}
	c := &s.coll
	c.mu.Lock()
	hit := false
	for _, p := range []string{ev.Path, ev.OldPath} {
		p = strings.TrimPrefix(p, "/")
		if p == "" {
			continue
		}
		for _, ids := range [][]catalog.CollectionID{c.files[p], c.dirs[p]} {
			for _, id := range ids {
				c.dirty[id], hit = true, true
			}
		}
	}
	c.mu.Unlock()
	if hit {
		s.scheduleCollections()
	}
}

func (c *collectionViews) takeDirty() map[catalog.CollectionID]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := c.dirty
	c.dirty = map[catalog.CollectionID]bool{}
	return d
}

func (c *collectionViews) markDirty(id catalog.CollectionID) {
	c.mu.Lock()
	c.dirty[id] = true
	c.mu.Unlock()
}

// index replaces a collection's entries in the event index (nil drops it).
func (c *collectionViews) index(id catalog.CollectionID, old, targets []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	drop := func(m map[string][]catalog.CollectionID, k string) {
		ids := slices.DeleteFunc(m[k], func(x catalog.CollectionID) bool { return x == id })
		if len(ids) == 0 {
			delete(m, k)
		} else {
			m[k] = ids
		}
	}
	add := func(m map[string][]catalog.CollectionID, k string) {
		if !slices.Contains(m[k], id) {
			m[k] = append(m[k], id)
		}
	}
	for _, p := range old {
		drop(c.files, p)
		for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
			drop(c.dirs, d)
		}
	}
	for _, p := range targets {
		add(c.files, p)
		for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
			add(c.dirs, d)
		}
	}
}

// collectionsOnlyCycle is the debounced pass after a change.
func (s *smartFoldersService) collectionsOnlyCycle(ctx context.Context) {
	if s.disabled.Load() || !s.collectionsDue(s.cfg.Now()) {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			s.disable("a collection-views pass", p)
		}
	}()
	fsys, release, err := s.cfg.Backend.acquire()
	if err != nil {
		s.logOnce("", fmt.Sprintf("views not updated: %v", err))
		return
	}
	defer release()
	s.collectionsCycle(ctx, fsys)
}

// collectionsCycle brings every collection view up to date: views of
// collections that are gone (or renamed away) are removed, changed ones
// rebuilt.
func (s *smartFoldersService) collectionsCycle(ctx context.Context, fsys smartfolders.FS) {
	src := s.coll.source()
	if src == nil {
		return
	}
	c := &s.coll
	now := s.cfg.Now()
	if now.Before(c.rootRetryAt) {
		return
	}
	root := s.cfg.Settings.CollectionsRoot
	st := &CollectionViewStats{Root: root, MaxViews: c.maxViews, MaxLinksPerView: c.maxLinks, LastPass: now}
	defer func() { c.stats.Store(st) }()
	rec := &smartfolders.Reconciler{
		FS:       fsys,
		Root:     root,
		Own:      c.own,
		Persist:  s.persist,
		Gate:     s.gate(ctx),
		MaxViews: c.maxViews,
		Exclude:  []string{s.cfg.Settings.Root},
	}
	state, err := rec.CheckRoot()
	if err != nil {
		st.Problems = append(st.Problems, err.Error())
		s.logOnce("collections", fmt.Sprintf("checking /%s: %v", root, err))
		return
	}
	if state == smartfolders.RootForeign {
		c.rootRetryAt = now.Add(smartFoldersRootRecheck)
		st.RootForeign = true
		s.logOnce("collections", fmt.Sprintf("/%s exists and was not created by the Manager: collection views leave it alone (checking again every %s; rename it or set JM_SMART_FOLDERS_COLLECTIONS_ROOT)", root, smartFoldersRootRecheck))
		return
	}
	cols, err := src.activeCollections(ctx)
	if err != nil {
		st.Problems = append(st.Problems, err.Error())
		s.logOnce("collections", fmt.Sprintf("listing collections: %v", err))
		return
	}
	s.logOnce("collections", "")
	sort.Slice(cols, func(i, j int) bool {
		if cols[i].CreatedSequence != cols[j].CreatedSequence {
			return cols[i].CreatedSequence < cols[j].CreatedSequence
		}
		return cols[i].ID < cols[j].ID
	})
	st.Collections = len(cols)
	if len(cols) > c.maxViews {
		st.OverflowCollections = len(cols) - c.maxViews
		cols = cols[:c.maxViews]
	}
	want := make([]smartfolders.Folder, len(cols))
	prev := map[string]string{}
	for i, col := range cols {
		want[i] = smartfolders.Folder{Key: string(col.ID), Name: col.Name}
	}
	for id, f := range c.folders {
		prev[string(id)] = f
	}
	names := smartfolders.FolderNames(want, prev)
	wanted := map[string]bool{}
	for _, f := range names {
		wanted[f] = true
	}
	sweep := !now.Before(c.sweepAt)
	if sweep {
		c.sweepAt = now.Add(c.sweep)
	}
	dirty := c.takeDirty()
	failed := func(folder string, err error) {
		if errors.Is(err, errMembersUnresolved) {
			st.Problems = append(st.Problems, err.Error())
			return
		}
		b := c.backoff[folder]
		if b == nil {
			b = &folderBackoff{}
			c.backoff[folder] = b
		}
		b.failures++
		delay := s.cfg.Interval << min(b.failures-1, 16)
		if delay > smartFoldersMaxBackoff || delay <= 0 {
			delay = smartFoldersMaxBackoff
		}
		b.until = now.Add(delay)
		if !errors.Is(err, smartfolders.ErrViewInUse) {
			st.Problems = append(st.Problems, err.Error())
		}
	}
	waiting := func(folder string) bool {
		b := c.backoff[folder]
		return ctx.Err() != nil || (b != nil && now.Before(b.until))
	}

	// 1. Views no collection wants (deleted, renamed away, over the
	// limit) go, unless a user put something in them.
	for _, folder := range sortedFolders(c.own.Views) {
		if wanted[folder] || waiting(folder) {
			continue
		}
		res, err := rec.RemoveView(folder)
		s.logCollectionView(root, folder, res, err)
		if err != nil {
			c.left[folder] = "removal refused: " + err.Error()
			if errors.Is(err, smartfolders.ErrViewInUse) {
				c.left[folder] = "holds entries the Manager did not create"
			}
			failed(folder, err)
			continue
		}
		delete(c.left, folder)
		delete(c.backoff, folder)
		if c.own.Views[folder] == nil {
			delete(s.lastLog, "collections:"+folder)
		}
	}

	// 2. Rebuild what changed.
	changed := false
	active := map[catalog.CollectionID]bool{}
	for _, col := range cols {
		active[col.ID] = true
		folder := names[string(col.ID)]
		prior := c.seen[col.ID]
		need := sweep || dirty[col.ID] || prior == nil || prior.seq != col.UpdatedSequence ||
			prior.folder != folder || (prior.hasView && c.own.Views[folder] == nil)
		if _, ok := c.left[folder]; ok {
			// A view left behind is wanted again: its removal backoff
			// does not apply.
			delete(c.left, folder)
			delete(c.backoff, folder)
			need = true
		}
		if !need {
			continue
		}
		if waiting(folder) {
			c.markDirty(col.ID)
			continue
		}
		seen, err := s.reconcileCollection(ctx, rec, src, col, folder)
		if err != nil {
			failed(folder, err)
			c.markDirty(col.ID)
			continue
		}
		delete(c.backoff, folder)
		delete(c.left, folder)
		var old []string
		if prior != nil {
			old = prior.targets
		}
		c.index(col.ID, old, seen.targets)
		c.seen[col.ID] = seen
		if c.folders[col.ID] != folder {
			c.folders[col.ID] = folder
			changed = true
		}
	}

	// 3. Forget collections that are gone or over the limit.
	for id, prior := range c.seen {
		if !active[id] {
			c.index(id, prior.targets, nil)
			delete(c.seen, id)
		}
	}
	for id := range c.folders {
		if !active[id] {
			delete(c.folders, id)
			changed = true
		}
	}
	for folder := range c.backoff {
		if !wanted[folder] && c.own.Views[folder] == nil {
			delete(c.backoff, folder)
			delete(c.left, folder)
		}
	}
	if changed {
		if err := s.persist(); err != nil {
			s.logOnce("save", fmt.Sprintf("saving state: %v", err))
		}
	}

	st.Views = len(c.own.Views)
	for _, v := range c.own.Views {
		st.Links += len(v.Links)
	}
	for _, seen := range c.seen {
		st.MissingMembers += seen.missing
		st.OverflowMembers += seen.over
		st.RefusedLinks += seen.refused
	}
	for folder, why := range c.left {
		st.ViewsLeft = append(st.ViewsLeft, root+"/"+folder+": "+why)
	}
	sort.Strings(st.ViewsLeft)
}

// reconcileCollection makes one collection's view hold a link to each of
// its first maxLinks members that is present, at its current path. A
// member found gone at the last moment is resolved once more.
func (s *smartFoldersService) reconcileCollection(ctx context.Context, rec *smartfolders.Reconciler, src collectionSource, col catalog.Collection, folder string) (*collSeen, error) {
	members, err := src.collectionMembers(ctx, col.ID)
	if err != nil {
		return nil, err
	}
	// Join order: the members that joined first get links (and plain names).
	sort.Slice(members, func(i, j int) bool {
		if members[i].UpdatedSequence != members[j].UpdatedSequence {
			return members[i].UpdatedSequence < members[j].UpdatedSequence
		}
		return members[i].AssetID < members[j].AssetID
	})
	seen := &collSeen{seq: col.UpdatedSequence, folder: folder}
	if len(members) > s.coll.maxLinks {
		seen.over = len(members) - s.coll.maxLinks
		members = members[:s.coll.maxLinks]
	}
	for pass := 0; pass < 2; pass++ {
		var targets []string
		missing, unknown := 0, 0
		for _, m := range members {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			p, state, err := src.resolveAsset(ctx, m.AssetID)
			switch {
			case err != nil || state == "unknown":
				unknown++
			case state == "present":
				targets = append(targets, p)
			default:
				missing++
			}
		}
		if unknown > 0 {
			// Unknown is not gone: leave the view as it is.
			return nil, fmt.Errorf("%s/%s: %d %w; view left unchanged", rec.Root, folder, unknown, errMembersUnresolved)
		}
		links := smartfolders.LinkNames(targets, rec.PreviousNames(folder))
		res, err := rec.Reconcile(folder, links)
		s.logCollectionView(rec.Root, folder, res, err)
		if err != nil {
			return nil, err
		}
		gone, refused := 0, 0
		placed := map[string]bool{}
		for _, l := range links {
			placed[l.Target] = true
		}
		for _, r := range res.Refused {
			delete(placed, r.Target)
			if r.TargetGone() {
				gone++
			} else {
				refused++
			}
		}
		if gone > 0 && pass == 0 {
			continue // moved since it was resolved: resolve again
		}
		seen.missing, seen.refused = missing+gone, refused
		for _, l := range links {
			if placed[l.Target] {
				seen.targets = append(seen.targets, l.Target)
			}
		}
		if ov := rec.Own.Views[folder]; ov != nil {
			seen.hasView, seen.links = true, len(ov.Links)
		}
		return seen, nil
	}
	return seen, nil
}

func (s *smartFoldersService) logCollectionView(root, folder string, res smartfolders.Result, err error) {
	var problems []string
	if err != nil && !errors.Is(err, smartfolders.ErrViewInUse) {
		problems = append(problems, err.Error())
	}
	if errors.Is(err, smartfolders.ErrViewInUse) {
		problems = append(problems, "no collection wants this view any more, but it holds entries the Manager did not create; it is left in place")
	}
	for _, r := range res.Refused {
		if !r.TargetGone() {
			problems = append(problems, fmt.Sprintf("%s: %v", r.Name, r.Err))
		}
	}
	if len(res.Foreign) > 0 {
		problems = append(problems, "replaced by entries the Manager did not create (left alone): "+strings.Join(res.Foreign, ", "))
	}
	key := "collections:" + folder
	if len(problems) == 0 {
		s.logOnce(key, "")
		return
	}
	s.logOnce(key, fmt.Sprintf("view %s/%s: %s", root, folder, strings.Join(problems, "; ")))
}

func sortedFolders(m map[string]*smartfolders.OwnedView) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// collectionStats returns the last pass's numbers (nil before the first).
func (s *smartFoldersService) collectionStats() *CollectionViewStats { return s.coll.stats.Load() }
