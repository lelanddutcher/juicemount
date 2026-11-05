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
// once the Manager owns the collections root /Recents no longer lists files
// under it (a user-made folder of that name stays ordinary for /Recents).
//
// Where a member is. A member is an asset (a JuiceFS inode, via the
// binder). Its link points at the file's current path, resolved live from
// the volume (binder.AssetByID: inode -> name); the binder's path hint is
// never trusted. A member with no visible name (deleted, only in the
// trash, only under hidden names) gets no link; it stays a member, and its
// link comes back when the file does. A member whose path cannot be read
// keeps the link it had (or gets none), is counted, and is not resolved
// again until its own backoff passes ("unknown is not gone"); the rest of
// the view follows. While the volume is not connected at all, views are
// left as they are.
//
// Names. A view folder is the collection's name with "/", ":" and control
// characters replaced by "-", leading dots dropped and cut to NAME_MAX
// (smartfolders.FolderName); empty is "Untitled". Collections are ranked
// by creation (catalog sequence, then ID); when two names clash
// (case-insensitively) the collection that already holds the name keeps
// it, else the earlier one, and the other gets "<name> (2)", "(3)"... A
// name held by any other entry in the root (a user's folder, a view left
// behind under another case) is skipped the same way. A collection keeps
// its folder while its name still yields it, ignoring case (a rename that
// only changes case keeps the folder, in its old case), so deleting the
// first of two clashing collections does not rename the second. A new
// folder name is recorded before its view is written. Links inside a view follow the /Recents rules
// (smartfolders.LinkNames): the member that joined first gets the plain
// file name, a later one with the same name is labelled with its parent
// folder, then numbered; a member keeps its link name while it stays.
//
// Changes. A rename of a collection creates the view under the new name and
// removes the old one; a deleted collection's view is removed; a member
// that leaves loses its link. Removing a view first removes the Manager's
// links, then the directory, only once empty: a view holding a user's
// file is left in place (with that file) and retried later. The one
// exception is the Finder's own settings file: a view whose only entry is
// a regular file named exactly ".DS_Store" counts as empty, and that file
// is removed with it (Finder writes it into any folder a window opens, and
// writes it again when needed). AppleDouble "._" files and any other
// entry keep the view. Paths an
// editing app remembered through a view stop working when the view or
// link goes away; the app sees media offline until it is relinked to the
// real path.
//
// Through the RESP proxy (the default deployment) a view folder is a
// project root (project depth 2). The proxy lets the Manager remove one
// only by a direct rmdir without the trash, and only a view it recorded the
// Manager creating directly in a smart root (respproxy/smartview.go);
// renaming one stays admin-only. A view it refuses (an older proxy, a view
// made before that record existed) is emptied but stays, recorded as the
// Manager's (reused if a collection takes that name again) and reported
// under views_left. A view the volume refused to remove is parked: tried
// again only after
// collectionViewsPark or a restart. Recorded views, left-behind ones
// included, are capped at twice the view limit: at the cap no new view is
// made and views left behind are not retried (both counted).
//
// Limits. At most collectionViewsMaxViews wanted views (collections
// beyond it, by creation order, get none; views left behind do not count)
// and collectionViewsMaxLinks links per view (the members that joined
// first; later ones get none). Both overflows are counted in GET
// /api/continuity/status (collection_views), as are missing and
// unresolved members, refused links, views left behind (with why, and
// any hidden path one is stuck under) and collections deferred to the
// next pass.
//
// When. Every reconcile cycle (JM_SMART_FOLDERS interval) reads the list
// of collections, which is cheap, and rebuilds only a view whose
// collection changed (its catalog sequence moved, or its folder name), is
// marked dirty, or lost its directory. A collection change through the API
// or a juicemount:metadata event naming a member's path (or a folder
// above it) marks it dirty and runs a collections-only pass after a short
// debounce. Every ten minutes all views are marked for a rebuild, which
// catches what events miss (a member file coming back, a change made while
// the Manager was down). A pass starts no new view once it has resolved
// collectionViewsPassOps members or run collectionViewsPassTime, changed
// collections first (each list resuming after where the last pass
// stopped), and schedules the next pass for the rest, so /Recents
// (same goroutine) waits at most about one pass. Nothing here is on the
// NFS or Finder path.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"golang.org/x/text/unicode/norm"
)

const (
	smartFoldersCollectionsRoot = "Collections"
	collectionViewsMaxViews     = 128
	collectionViewsMaxLinks     = 500
	collectionViewsSweep        = 10 * time.Minute
	collectionViewsDebounce     = 2 * time.Second
	// A pass starts no new view after this many member resolutions or this
	// long; the rest follows in the next pass.
	collectionViewsPassOps  = 1000
	collectionViewsPassTime = 5 * time.Second
	collectionViewsRootList = 4096 // root entries read for name clashes
	// A view the volume refused to remove (not one a user's entry keeps)
	// is parked: tried again only after this long, or after a restart.
	collectionViewsPark = 6 * time.Hour
	// Ownership.Lost entries are reported for this many passes at most.
	collectionViewsLostPasses = 100
)

// errMembersUnresolved: a member's path could not be read (the volume is
// not connected yet, or a read failed). The view is retried at the next
// cycle without backing off.
var errMembersUnresolved = errors.New("members could not be resolved on the volume")

// catalogViewsState is one kind of catalog views' part of
// smart-folders.json: the collection views' (version 3) or the query views'
// (version 4).
type catalogViewsState[K ~string] struct {
	Ownership *smartfolders.Ownership `json:"ownership,omitempty"`
	// Folders maps a collection (or query) to its view folder name, so the
	// name stays put (FolderNames prev).
	Folders map[K]string `json:"folders,omitempty"`
}

type collectionViewsState = catalogViewsState[catalog.CollectionID]

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
	UnresolvedMembers   int    `json:"unresolved_members"`   // path unreadable: link kept as it was, or none
	Deferred            int    `json:"deferred_collections"` // left for the next pass (budget)
	// Recorded views (wanted and left behind) are capped at MaxRecorded:
	// at the cap no new view is made (ViewsAtCap counts the collections
	// waiting) and views left behind are not retried (ViewsParked).
	MaxRecorded  int `json:"max_recorded_views"`
	ViewsAtCap   int `json:"views_waiting_at_cap"`
	ViewsParked  int `json:"views_parked"`
	LostOverflow int `json:"lost_overflow,omitempty"`
	// ViewsLeft: views no collection wants that are still there, with why:
	// a user's entry inside, or the volume refused removing the folder.
	ViewsLeft   []string  `json:"views_left,omitempty"`
	RootForeign bool      `json:"root_not_created_by_manager,omitempty"`
	Problems    []string  `json:"problems,omitempty"`
	LastPass    time.Time `json:"last_pass"`
}

// collSeen is a collection (or query) as last reconciled.
type collSeen struct {
	seq      int64
	folder   string
	hasView  bool
	targets  []string                      // linked paths (for the event index)
	assets   map[continuity.AssetID]string // member -> linked path
	unknown  []continuity.AssetID          // members whose path could not be resolved
	links    int
	missing  int
	over     int
	refused  int
	capped   int       // queries: candidates left unexamined (queryMaxCandidates)
	examined int       // queries: candidates examined
	built    time.Time // when this view was last built (Now)
}

// viewBudget is what is left of a pass's budget for one view: volume
// resolutions and time. A kind that can stop part way (queries) stops when
// it is spent and returns errViewPartial; collections finish their view.
type viewBudget struct {
	ops   int
	until time.Time
}

// errViewPartial: the view's rebuild stopped at the budget and resumes in
// the next pass; nothing is wrong.
var errViewPartial = errors.New("view rebuild continues in the next pass")

// cappedView names a query whose candidates were capped.
type cappedView struct {
	ID         string `json:"id"`
	Examined   int    `json:"examined"`
	Candidates int    `json:"candidates"`
}

// viewItem is one catalog object that wants a view: a collection, or a
// saved query (with its definition).
type viewItem[K ~string] struct {
	ID              K
	Name            string
	CreatedSequence int64
	UpdatedSequence int64
	query           *catalog.QueryDefinition
}

// viewKind is what differs between kinds of catalog views (collections,
// queries): the root, how the objects are listed and how one view's links
// are worked out. The rest (naming, limits, ownership, removal, pacing) is
// catalogViews, shared.
type viewKind[K ~string, S any] struct {
	noun, plural string        // "collection", "collections": messages and log keys
	rootVar      string        // the variable that moves the root
	debounce     time.Duration // from a change to its pass
	// minInterval: a view dirty only from events (its object unchanged) is
	// rebuilt at most this often (0: no minimum).
	minInterval time.Duration
	root        func(s *smartFoldersService) string
	off         func(s *smartFoldersService) string // why the root is unusable
	// exclude: the other smart roots, whose files no view of this kind links.
	exclude func(s *smartFoldersService) []string
	list    func(ctx context.Context, src S) ([]viewItem[K], error)
	// prepare, when set, runs once a pass before its jobs, with every
	// object that has a view, the jobs (some of which the budget may leave
	// for later), the views marked dirty and whether a sweep started
	// (queries: one candidate scan).
	prepare func(s *smartFoldersService, ctx context.Context, src S, all, jobs []viewItem[K], dirty map[K]bool, sweep bool)
	// busy, when set, reports a rebuild in progress (resumed whatever the
	// minimum interval).
	busy  func(s *smartFoldersService, id K) bool
	build func(s *smartFoldersService, ctx context.Context, rec *smartfolders.Reconciler, src S, item viewItem[K], folder string, prior *collSeen, now time.Time, budget viewBudget) (*collSeen, int, error)
}

// catalogViews keeps one kind of catalog views in its own root.
type catalogViews[K ~string, S any] struct {
	kind    viewKind[K, S]
	own     *smartfolders.Ownership
	folders map[K]string
	src     atomic.Pointer[sourceRef[S]]
	wake    chan struct{}

	maxViews, maxLinks int
	sweep, debounce    time.Duration
	passOps            int
	passTime           time.Duration
	rootOwned          atomic.Bool // the last root check found the Manager's root

	// Reconcile-goroutine state.
	seen          map[K]*collSeen
	sweepAt       time.Time
	rootRetryAt   time.Time
	backoff       map[string]*folderBackoff // by view folder
	parked        map[string]time.Time      // view folder -> next removal try
	parkFor       time.Duration
	lostPasses    map[string]int    // Ownership.Lost path -> passes reported
	cursor        K                 // where the last budget-limited pass stopped
	left          map[string]string // view folder -> why it was not removed
	memberBackoff map[continuity.AssetID]*folderBackoff
	sweepPending  map[K]bool

	// Event-path state.
	mu      sync.Mutex
	dirty   map[K]bool
	requeue map[K]bool     // jobs a pass left for the next (budget, backoff), not changed
	files   map[string][]K // linked path -> views
	dirs    map[string][]K // folder above a linked path -> views
	watch   map[K][]string // queries: folders any change in marks the view dirty
	pending bool           // a debounced pass is scheduled

	stats    atomic.Pointer[CollectionViewStats]
	capped   atomic.Pointer[[]cappedView] // queries whose candidates were capped
	deferred bool                         // a wake for a deferred view is scheduled (under mu)
}

type sourceRef[S any] struct{ src S }

type (
	collectionViews     = catalogViews[catalog.CollectionID, collectionSource]
	collectionSourceRef = sourceRef[collectionSource]
)

func (c *catalogViews[K, S]) init(kind viewKind[K, S]) {
	c.kind = kind
	c.own = &smartfolders.Ownership{}
	c.own.Normalize()
	c.folders = map[K]string{}
	c.wake = make(chan struct{}, 1)
	c.maxViews, c.maxLinks = collectionViewsMaxViews, collectionViewsMaxLinks
	c.sweep, c.debounce = collectionViewsSweep, kind.debounce
	c.passOps, c.passTime = collectionViewsPassOps, collectionViewsPassTime
	c.memberBackoff = map[continuity.AssetID]*folderBackoff{}
	c.parked = map[string]time.Time{}
	c.parkFor = collectionViewsPark
	c.lostPasses = map[string]int{}
	c.sweepPending = map[K]bool{}
	c.seen = map[K]*collSeen{}
	c.backoff = map[string]*folderBackoff{}
	c.left = map[string]string{}
	c.dirty = map[K]bool{}
	c.requeue = map[K]bool{}
	c.files = map[string][]K{}
	c.dirs = map[string][]K{}
	c.watch = map[K][]string{}
}

func (c *catalogViews[K, S]) load(st *catalogViewsState[K]) {
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
// file stays at the older version.
func (c *catalogViews[K, S]) state() *catalogViewsState[K] {
	if c.own.Root == nil && len(c.own.Views) == 0 && len(c.folders) == 0 {
		return nil
	}
	return &catalogViewsState[K]{Ownership: c.own, Folders: c.folders}
}

func (c *catalogViews[K, S]) source() S {
	if r := c.src.Load(); r != nil {
		return r.src
	}
	var none S
	return none
}

// rootActive reports whether top is the root of views of this kind that
// are running, in a root the Manager owns (the tracker's Ignore; runs on
// the event path). A user-made folder of that name is listed by /Recents
// like any other.
func (c *catalogViews[K, S]) rootActive(s *smartFoldersService, top string) bool {
	return top != "" && top == c.kind.root(s) && c.src.Load() != nil && c.rootOwned.Load()
}

func (c *catalogViews[K, S]) due(now time.Time) bool {
	return c.src.Load() != nil && !now.Before(c.rootRetryAt)
}

// attach turns the views on, reading src. It reports whether they run (a
// bad root name keeps them off).
func (c *catalogViews[K, S]) attach(s *smartFoldersService, src S, limits string) bool {
	root := c.kind.root(s)
	if root == "" {
		log.Printf("manager: smart folders: %s views OFF: %s", c.kind.noun, c.kind.off(s))
		return false
	}
	c.rootOwned.Store(c.own.Root != nil && !c.own.Root.Pending)
	c.src.Store(&sourceRef[S]{src})
	log.Printf("manager: smart folders: %s views ON (experimental): /%s/<%s> (%s)", c.kind.noun, root, c.kind.noun, limits)
	c.schedule()
	return true
}

// collectionsRootActive is rootActive for the collection views.
func (s *smartFoldersService) collectionsRootActive(top string) bool {
	return s.coll.rootActive(s, top)
}

func (s *smartFoldersService) collectionsDue(now time.Time) bool { return s.coll.due(now) }

// attachCollections turns collection views on, reading src. It reports
// whether they run (a bad JM_SMART_FOLDERS_COLLECTIONS_ROOT keeps them off).
func (s *smartFoldersService) attachCollections(src collectionSource) bool {
	return s.coll.attach(s, src, fmt.Sprintf("at most %d views of %d links", s.coll.maxViews, s.coll.maxLinks))
}

// collectionsChanged is called after a collection change commits.
func (s *smartFoldersService) collectionsChanged() { s.coll.schedule() }

// scheduleCollections runs a collections-only pass after the debounce.
func (s *smartFoldersService) scheduleCollections() { s.coll.schedule() }

// scheduleAt runs a pass of this kind after d (a view deferred by the
// minimum interval); one such wake is pending at a time. It never blocks.
func (c *catalogViews[K, S]) scheduleAt(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deferred {
		return
	}
	c.deferred = true
	time.AfterFunc(max(d, c.debounce), func() {
		c.mu.Lock()
		c.deferred = false
		c.mu.Unlock()
		select {
		case c.wake <- struct{}{}:
		default:
		}
	})
}

// schedule runs a pass of this kind alone after the debounce; calls in
// between coalesce. It never blocks.
func (c *catalogViews[K, S]) schedule() {
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
	s.coll.notePaths(ev.Path, ev.OldPath)
}

// notePaths marks dirty the views linking one of paths (or a file below a
// folder it names) and, for queries, the views watching a folder at, above
// or below one of them; then schedules a pass. Event path: map lookups and
// one prefix check per watched folder.
func (c *catalogViews[K, S]) notePaths(paths ...string) {
	if c.src.Load() == nil {
		return
	}
	c.mu.Lock()
	hit := false
	for _, p := range paths {
		p = strings.TrimPrefix(p, "/")
		if p == "" {
			continue
		}
		for _, ids := range [][]K{c.files[p], c.dirs[p]} {
			for _, id := range ids {
				c.dirty[id], hit = true, true
			}
		}
		composed := norm.NFC.String(p)
		for id, folders := range c.watch {
			for _, f := range folders {
				if underFolder(composed, f) || underFolder(f, composed) {
					c.dirty[id], hit = true, true
					break
				}
			}
		}
	}
	c.mu.Unlock()
	if hit {
		c.schedule()
	}
}

// underFolder: p is folder or below it.
func underFolder(p, folder string) bool {
	return p == folder || strings.HasPrefix(p, folder+"/")
}

func (c *catalogViews[K, S]) takeDirty() (dirty, requeue map[K]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dirty, requeue = c.dirty, c.requeue
	c.dirty, c.requeue = map[K]bool{}, map[K]bool{}
	return dirty, requeue
}

// markRequeue leaves a job for the next pass without marking it changed.
func (c *catalogViews[K, S]) markRequeue(id K) {
	c.mu.Lock()
	c.requeue[id] = true
	c.mu.Unlock()
}

func (c *catalogViews[K, S]) markDirty(id K) {
	c.mu.Lock()
	c.dirty[id] = true
	c.mu.Unlock()
}

// index replaces a view's entries in the event index (nil drops it).
func (c *catalogViews[K, S]) index(id K, old, targets []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	drop := func(m map[string][]K, k string) {
		ids := slices.DeleteFunc(m[k], func(x K) bool { return x == id })
		if len(ids) == 0 {
			delete(m, k)
		} else {
			m[k] = ids
		}
	}
	add := func(m map[string][]K, k string) {
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
func (s *smartFoldersService) collectionsOnlyCycle(ctx context.Context) { s.coll.onlyCycle(s, ctx) }

// onlyCycle is the debounced pass of this kind alone after a change.
func (c *catalogViews[K, S]) onlyCycle(s *smartFoldersService, ctx context.Context) {
	if s.disabled.Load() || !c.due(s.cfg.Now()) {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			s.disable("a "+c.kind.noun+"-views pass", p)
		}
	}()
	fsys, release, err := s.cfg.Backend.acquire()
	if err != nil {
		s.logOnce("", fmt.Sprintf("views not updated: %v", err))
		return
	}
	defer release()
	c.cycle(s, ctx, fsys)
}

// collectionsCycle brings the collection views up to date: views of
// collections that are gone (or renamed away) are removed, changed ones
// rebuilt. A pass stops starting new views once it has used its budget
// (passOps member resolutions or passTime); the rest is left marked and
// another pass is scheduled, so /Recents, which shares the goroutine, is
// never held up for long. Changed collections go before the periodic
// sweep's.
func (s *smartFoldersService) collectionsCycle(ctx context.Context, fsys smartfolders.FS) {
	s.coll.cycle(s, ctx, fsys)
}

// cycle brings this kind's views up to date, as collectionsCycle says.
func (c *catalogViews[K, S]) cycle(s *smartFoldersService, ctx context.Context, fsys smartfolders.FS) {
	ref := c.src.Load()
	if ref == nil {
		return
	}
	src := ref.src
	now := s.cfg.Now()
	if now.Before(c.rootRetryAt) {
		return
	}
	started := time.Now()
	root := c.kind.root(s)
	logKey := c.kind.plural
	st := &CollectionViewStats{Root: root, MaxViews: c.maxViews, MaxLinksPerView: c.maxLinks, LastPass: now}
	capped := []cappedView{}
	defer func() { c.stats.Store(st); c.capped.Store(&capped) }()
	// Wanted views are bounded by the collection cutoff below; all recorded
	// views, left-behind ones included, by twice that.
	rec := &smartfolders.Reconciler{
		FS:       fsys,
		Root:     root,
		Own:      c.own,
		Persist:  s.persist,
		Gate:     s.gate(ctx),
		Exclude:  c.kind.exclude(s),
		MaxViews: 2 * c.maxViews,
	}
	st.MaxRecorded = rec.MaxViews
	defer func() { c.rootOwned.Store(c.own.Root != nil && !c.own.Root.Pending) }()
	state, err := rec.CheckRoot()
	if err != nil {
		st.Problems = append(st.Problems, err.Error())
		s.logOnce(logKey, fmt.Sprintf("checking /%s: %v", root, err))
		return
	}
	if state == smartfolders.RootForeign {
		c.rootRetryAt = now.Add(smartFoldersRootRecheck)
		st.RootForeign = true
		s.logOnce(logKey, fmt.Sprintf("/%s exists and was not created by the Manager: %s views leave it alone (checking again every %s; rename it or set %s)", root, c.kind.noun, smartFoldersRootRecheck, c.kind.rootVar))
		return
	}
	cols, err := c.kind.list(ctx, src)
	if err != nil {
		st.Problems = append(st.Problems, err.Error())
		s.logOnce(logKey, fmt.Sprintf("listing %s: %v", c.kind.plural, err))
		return
	}
	s.logOnce(logKey, "")
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

	// Folder names. A name is not free while the root holds another entry
	// with it in any case (a user's folder, a view left behind under
	// another case), except this exact name's own view.
	var inRoot []string
	if state == smartfolders.RootOwned {
		inRoot, _ = fsys.List(root, collectionViewsRootList)
	}
	blocked := func(name string) bool {
		for _, e := range inRoot {
			if strings.EqualFold(e, name) && !(e == name && c.own.Views[e] != nil) {
				return true
			}
		}
		return false
	}
	want := make([]smartfolders.Folder, len(cols))
	prev := map[string]string{}
	watch := map[K][]string{}
	for i, col := range cols {
		want[i] = smartfolders.Folder{Key: string(col.ID), Name: col.Name}
		if col.query != nil {
			for _, f := range col.query.Prefixes {
				watch[col.ID] = append(watch[col.ID], norm.NFC.String(f)) // as queries match
			}
		}
	}
	c.mu.Lock()
	c.watch = watch
	c.mu.Unlock()
	for id, f := range c.folders {
		prev[string(id)] = f
	}
	names := smartfolders.FolderNames(want, prev, blocked)
	wanted := map[string]bool{}
	for _, f := range names {
		wanted[f] = true
	}
	// Record the names before acting, so a numbered name stays put
	// whatever happens next.
	changed := false
	active := map[K]bool{}
	for _, col := range cols {
		active[col.ID] = true
		if f := names[string(col.ID)]; c.folders[col.ID] != f {
			c.folders[col.ID] = f
			changed = true
		}
	}
	for id := range c.folders {
		if !active[id] {
			delete(c.folders, id)
			changed = true
		}
	}
	if changed {
		if err := s.persist(); err != nil {
			s.logOnce("save", fmt.Sprintf("saving state: %v", err))
		}
	}

	sweeping := false
	if !now.Before(c.sweepAt) {
		sweeping = true
		c.sweepAt = now.Add(c.sweep)
		for _, col := range cols {
			c.sweepPending[col.ID] = true
		}
	}
	dirty, requeue := c.takeDirty()
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
		b.until = now.Add(s.backoffDelay(b.failures))
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
	// A view the volume refused to remove is parked; at the cap of recorded
	// views, every view already left behind is (until a restart).
	atCap := len(c.own.Views) >= rec.MaxViews
	for _, folder := range sortedFolders(c.own.Views) {
		if wanted[folder] || waiting(folder) {
			continue
		}
		if until, ok := c.parked[folder]; ok && (now.Before(until) || atCap) {
			st.ViewsParked++
			continue
		}
		if _, left := c.left[folder]; left && atCap {
			st.ViewsParked++
			continue
		}
		res, err := rec.RemoveView(folder)
		c.logView(s, folder, res, err)
		if err != nil {
			c.left[folder] = leftReason(err)
			// The volume itself refused (the RESP proxy's denial reaches
			// the JuiceFS client as EROFS): park. A busy lease or a
			// user's entry is only backed off.
			if errors.Is(err, syscall.EROFS) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
				c.parked[folder] = now.Add(c.parkFor)
				c.left[folder] += fmt.Sprintf(" (parked: tried again after %s or a restart)", c.parkFor)
				st.Problems = append(st.Problems, err.Error())
				continue
			}
			failed(folder, err)
			continue
		}
		delete(c.parked, folder)
		delete(c.left, folder)
		delete(c.backoff, folder)
		if c.own.Views[folder] == nil {
			delete(s.lastLog, logKey+":"+folder)
		}
	}

	// 2. Rebuild what changed, then what the sweep marked, within the
	// pass budget.
	type job struct {
		col   viewItem[K]
		sweep bool
	}
	var urgent, swept []job
	var deferTo time.Time
	for _, col := range cols {
		folder := names[string(col.ID)]
		prior := c.seen[col.ID]
		changed := prior == nil || prior.seq != col.UpdatedSequence ||
			prior.folder != folder || (prior.hasView && c.own.Views[folder] == nil)
		if _, ok := c.left[folder]; ok {
			// A view left behind is wanted again: its removal backoff
			// does not apply.
			delete(c.left, folder)
			delete(c.backoff, folder)
			delete(c.parked, folder)
			changed = true
		}
		need := changed || dirty[col.ID] || requeue[col.ID]
		// Only events: at most once per minInterval, unless a rebuild is
		// under way or was left for this pass. The view stays dirty and a
		// pass is due at the end of the interval.
		if need && !changed && !requeue[col.ID] && c.kind.minInterval > 0 && (c.kind.busy == nil || !c.kind.busy(s, col.ID)) {
			if next := prior.built.Add(c.kind.minInterval); now.Before(next) {
				c.markDirty(col.ID)
				if deferTo.IsZero() || next.Before(deferTo) {
					deferTo = next
				}
				continue
			}
		}
		switch {
		case need:
			urgent = append(urgent, job{col: col})
		case c.sweepPending[col.ID]:
			swept = append(swept, job{col: col, sweep: true})
		}
	}
	// Each list starts after the collection the last pass stopped at, so
	// early collections that keep changing cannot starve later ones.
	pos := map[K]int{}
	for i, col := range cols {
		pos[col.ID] = i
	}
	rotate := func(js []job) []job {
		at, ok := pos[c.cursor]
		if !ok {
			return js
		}
		var after, before []job
		for _, j := range js {
			if pos[j.col.ID] > at {
				after = append(after, j)
			} else {
				before = append(before, j)
			}
		}
		return append(after, before...)
	}
	jobs := append(rotate(urgent), rotate(swept)...)
	if c.kind.prepare != nil {
		items := make([]viewItem[K], len(jobs))
		for i, j := range jobs {
			items[i] = j.col
		}
		c.kind.prepare(s, ctx, src, cols, items, dirty, sweeping)
	}
	ops, more := 0, false
	for _, j := range jobs {
		col := j.col
		folder := names[string(col.ID)]
		if ops >= c.passOps || time.Since(started) >= c.passTime {
			more = true
			if !j.sweep {
				c.markRequeue(col.ID)
			}
			continue
		}
		if waiting(folder) {
			c.markRequeue(col.ID)
			continue
		}
		delete(c.sweepPending, col.ID)
		c.cursor = col.ID
		prior := c.seen[col.ID]
		seen, n, err := c.kind.build(s, ctx, rec, src, col, folder, prior, now,
			viewBudget{ops: c.passOps - ops, until: started.Add(c.passTime)})
		ops += n
		if errors.Is(err, errViewPartial) {
			more = true
			c.markRequeue(col.ID)
			continue
		}
		if errors.Is(err, smartfolders.ErrTooManyViews) {
			st.ViewsAtCap++
		}
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
		seen.built = now
		c.index(col.ID, old, seen.targets)
		c.seen[col.ID] = seen
	}
	if !deferTo.IsZero() {
		c.scheduleAt(deferTo.Sub(now))
	}

	// 3. Forget collections that are gone or over the limit.
	unknown := map[continuity.AssetID]bool{}
	for id, prior := range c.seen {
		if !active[id] {
			c.index(id, prior.targets, nil)
			delete(c.seen, id)
			continue
		}
		for _, a := range prior.unknown {
			unknown[a] = true
		}
	}
	for id := range c.sweepPending {
		if !active[id] {
			delete(c.sweepPending, id)
		}
	}
	for id, seen := range c.seen {
		if seen.capped > 0 {
			capped = append(capped, cappedView{ID: string(id), Examined: seen.examined, Candidates: seen.examined + seen.capped})
		}
	}
	sort.Slice(capped, func(i, j int) bool { return capped[i].ID < capped[j].ID })
	for a := range c.memberBackoff {
		if !unknown[a] {
			delete(c.memberBackoff, a)
		}
	}
	for folder := range c.backoff {
		if !wanted[folder] && c.own.Views[folder] == nil {
			delete(c.backoff, folder)
			delete(c.left, folder)
		}
	}
	for folder := range c.parked {
		if c.own.Views[folder] == nil {
			delete(c.parked, folder)
			delete(c.left, folder)
		}
	}
	// Hidden paths lost with an old root: dropped once the path is
	// confirmed gone, or after being reported for collectionViewsLostPasses.
	if state == smartfolders.RootOwned || state == smartfolders.RootAbsent {
		kept := c.own.Lost[:0:0]
		for _, p := range c.own.Lost {
			_, err := fsys.Lstat(p)
			c.lostPasses[p]++
			if errors.Is(err, fs.ErrNotExist) || c.lostPasses[p] > collectionViewsLostPasses {
				delete(c.lostPasses, p)
				continue
			}
			kept = append(kept, p)
		}
		if len(kept) != len(c.own.Lost) {
			c.own.Lost = kept
			if err := s.persist(); err != nil {
				s.logOnce("save", fmt.Sprintf("saving state: %v", err))
			}
		}
	}
	if more {
		c.schedule()
	}

	st.Views = len(c.own.Views)
	for _, v := range c.own.Views {
		st.Links += len(v.Links)
	}
	for _, seen := range c.seen {
		st.MissingMembers += seen.missing
		st.OverflowMembers += seen.over
		st.RefusedLinks += seen.refused
		st.UnresolvedMembers += len(seen.unknown)
	}
	for folder, why := range c.left {
		st.ViewsLeft = append(st.ViewsLeft, root+"/"+folder+": "+why)
	}
	for _, p := range c.own.Lost {
		st.ViewsLeft = append(st.ViewsLeft, p+": a view moved aside here before its root was removed, renamed or replaced")
	}
	sort.Strings(st.ViewsLeft)
	st.LostOverflow = c.own.LostOverflow
	c.mu.Lock()
	st.Deferred = len(c.sweepPending) + len(c.dirty) + len(c.requeue)
	c.mu.Unlock()
}

// collectionKind is the collection views' part of catalogViews.
func collectionKind() viewKind[catalog.CollectionID, collectionSource] {
	return viewKind[catalog.CollectionID, collectionSource]{
		noun: "collection", plural: "collections", rootVar: "JM_SMART_FOLDERS_COLLECTIONS_ROOT", debounce: collectionViewsDebounce,
		root: func(s *smartFoldersService) string { return s.cfg.Settings.CollectionsRoot },
		off:  func(s *smartFoldersService) string { return s.cfg.Settings.CollectionsOff },
		exclude: func(s *smartFoldersService) []string {
			out := []string{s.cfg.Settings.Root}
			if q := s.cfg.Settings.QueriesRoot; s.queriesRootActive(q) {
				out = append(out, q)
			}
			return out
		},
		list: func(ctx context.Context, src collectionSource) ([]viewItem[catalog.CollectionID], error) {
			cols, err := src.activeCollections(ctx)
			out := make([]viewItem[catalog.CollectionID], len(cols))
			for i, col := range cols {
				out[i] = viewItem[catalog.CollectionID]{ID: col.ID, Name: col.Name, CreatedSequence: col.CreatedSequence, UpdatedSequence: col.UpdatedSequence}
			}
			return out, err
		},
		build: (*smartFoldersService).reconcileCollection,
	}
}

// leftReason says why a view no collection wants is still there.
func leftReason(err error) string {
	var stuck *smartfolders.StuckError
	switch {
	case errors.Is(err, smartfolders.ErrViewInUse):
		return "holds entries the Manager did not create"
	case errors.As(err, &stuck):
		return "moved aside and cannot go back: it is under the hidden name " + stuck.Hidden
	default:
		return "removal refused: " + err.Error()
	}
}

// backoffDelay is the delay after the n-th failure in a row: the interval,
// doubling, at most smartFoldersMaxBackoff.
func (s *smartFoldersService) backoffDelay(n int) time.Duration {
	delay := s.cfg.Interval << min(n-1, 16)
	if delay > smartFoldersMaxBackoff || delay <= 0 {
		delay = smartFoldersMaxBackoff
	}
	return delay
}

// reconcileCollection makes one collection's view hold a link to each of
// its first maxLinks members that is present, at its current path. It
// returns how many members it resolved.
//
// A member whose path cannot be resolved keeps the link it had (prior), or
// gets none, and is counted; it is not resolved again until its own
// backoff passes, so one unresolvable member never holds up the view.
// Only when the volume is not connected at all is the view left as it is.
// A member found gone at the last moment is resolved once more.
func (s *smartFoldersService) reconcileCollection(ctx context.Context, rec *smartfolders.Reconciler, src collectionSource, col viewItem[catalog.CollectionID], folder string, prior *collSeen, now time.Time, _ viewBudget) (*collSeen, int, error) {
	c := &s.coll
	members, err := src.collectionMembers(ctx, col.ID)
	if err != nil {
		return nil, 0, err
	}
	// Join order: the members that joined first get links (and plain names).
	sort.Slice(members, func(i, j int) bool {
		if members[i].UpdatedSequence != members[j].UpdatedSequence {
			return members[i].UpdatedSequence < members[j].UpdatedSequence
		}
		return members[i].AssetID < members[j].AssetID
	})
	seen := &collSeen{seq: col.UpdatedSequence, folder: folder}
	if len(members) > c.maxLinks {
		seen.over = len(members) - c.maxLinks
		members = members[:c.maxLinks]
	}
	var had map[continuity.AssetID]string
	if prior != nil && prior.folder == folder {
		had = prior.assets
	}
	ops := 0
	for pass := 0; pass < 2; pass++ {
		var targets []string
		assets := map[continuity.AssetID]string{}
		var unknown []continuity.AssetID
		missing := 0
		keep := func(a continuity.AssetID) {
			unknown = append(unknown, a)
			if p, ok := had[a]; ok {
				targets = append(targets, p)
				assets[a] = p
			}
		}
		for _, m := range members {
			if err := ctx.Err(); err != nil {
				return nil, ops, err
			}
			if b := c.memberBackoff[m.AssetID]; b != nil && now.Before(b.until) {
				keep(m.AssetID)
				continue
			}
			ops++
			p, state, err := src.resolveAsset(ctx, m.AssetID)
			switch {
			case errors.Is(err, binder.ErrNotReady):
				return nil, ops, fmt.Errorf("%s/%s: %w (%v); view left unchanged", rec.Root, folder, errMembersUnresolved, err)
			case err != nil || state == "unknown":
				b := c.memberBackoff[m.AssetID]
				if b == nil {
					b = &folderBackoff{}
					c.memberBackoff[m.AssetID] = b
				}
				b.failures++
				b.until = now.Add(s.backoffDelay(b.failures))
				keep(m.AssetID)
			case state == "present":
				delete(c.memberBackoff, m.AssetID)
				targets = append(targets, p)
				assets[m.AssetID] = p
			default:
				delete(c.memberBackoff, m.AssetID)
				missing++
			}
		}
		links := smartfolders.LinkNames(targets, rec.PreviousNames(folder))
		res, err := rec.Reconcile(folder, links)
		c.logView(s, folder, res, err)
		if err != nil {
			return nil, ops, err
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
		seen.missing, seen.refused, seen.unknown = missing+gone, refused, unknown
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
		return seen, ops, nil
	}
	return seen, ops, nil
}

// logView logs a view's problems once (under "<plural>:<folder>").
func (c *catalogViews[K, S]) logView(s *smartFoldersService, folder string, res smartfolders.Result, err error) {
	var problems []string
	if err != nil && !errors.Is(err, smartfolders.ErrViewInUse) {
		problems = append(problems, err.Error())
	}
	if errors.Is(err, smartfolders.ErrViewInUse) {
		problems = append(problems, "no "+c.kind.noun+" wants this view any more, but it holds entries the Manager did not create; it is left in place")
	}
	for _, r := range res.Refused {
		if !r.TargetGone() {
			problems = append(problems, fmt.Sprintf("%s: %v", r.Name, r.Err))
		}
	}
	if len(res.Foreign) > 0 {
		problems = append(problems, "replaced by entries the Manager did not create (left alone): "+strings.Join(res.Foreign, ", "))
	}
	key := c.kind.plural + ":" + folder
	if len(problems) == 0 {
		s.logOnce(key, "")
		return
	}
	s.logOnce(key, fmt.Sprintf("view %s/%s: %s", c.kind.root(s), folder, strings.Join(problems, "; ")))
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
