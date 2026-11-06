package manager

// smartfolders.go: the Manager side of smart folders (experimental, off by
// default). With JM_SMART_FOLDERS=on the Manager subscribes to the clients'
// juicemount:metadata change events, keeps a recency list per top-level
// folder (internal/smartfolders) and writes <JM_SMART_FOLDERS_ROOT>/<folder>/
// views of relative symlinks into the volume, so every client sees them with
// no extra app.
//
// Views are written only in standalone mode (the default deployment,
// JM_META set), through a JuiceFS metadata client as the Manager's own
// principal (internal/smartfolders/jfsmeta): no FUSE mount or object store
// is needed, and a created directory is identified by the create call
// itself. In embedded mode (--fuse-mount) smart folders do not start: a
// directory created through a mount cannot be identified atomically.
//
// What the Manager created (the root, each view, each link, each temp name)
// is recorded in its state file before it is created; nothing else in the
// volume is ever modified, and no /Recents directory is ever removed (a
// collection view goes with its collection: smartfolders_collections.go).
//
// Known gaps (documented in server/README.md):
//   - pub/sub is fire-and-forget: changes while the Manager is down are
//     missed; a rename of a file the client had not mirrored publishes no
//     event; writes by Manager sync, the farm or the WebDAV/S3 gateway
//     publish none.
//   - case folding for name clashes is strings.ToLower (no full Unicode
//     folding or normalisation).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
	"github.com/redis/go-redis/v9"
)

const (
	// metadataEventChannel is metadata.SubscribeChannel (pinned by the
	// smartfolders contract test; the Manager does not import metadata).
	metadataEventChannel = "juicemount:metadata"

	smartFoldersStateName = "smart-folders.json"
	// smartFoldersStateVersion is the format written while the file holds
	// collection-view records (smartfolders_collections.go). Without them
	// it is written as version 2, which a Manager from before collection
	// views still reads. That older Manager refuses version 3 (unsupported
	// version: smart folders stay off and the file is never rewritten), so
	// it cannot drop the collection records it does not know. Version 4
	// adds query-view records (smartfolders_queries.go) the same way: it is
	// written only while there are some, and a Manager from before query
	// views refuses it.
	smartFoldersStateVersion    = 3
	smartFoldersStateVersionV2  = 2
	smartFoldersStateVersionV4  = 4
	defaultSmartFoldersInterval = 30 * time.Second
	smartFoldersLeaseTimeout    = 30 * time.Second
	smartFoldersMaxBackoff      = 30 * time.Minute
	smartFoldersRootRecheck     = 10 * time.Minute
	smartFoldersStopTimeout     = 10 * time.Second
	smartFoldersFolderCacheTTL  = time.Minute
	smartFoldersFolderCacheMax  = 4096
	defaultSmartFoldersMaxViews = smartfolders.DefaultMaxFolders
	maxSmartFoldersMaxViews     = 4096
)

type smartFoldersSettings struct {
	Root     string
	MaxViews int
	// CollectionsRoot is the root of the collection views (they also need
	// JM_CONTINUITY=on); "" when JM_SMART_FOLDERS_COLLECTIONS_ROOT is
	// unusable, with the reason in CollectionsOff. A bad value turns only
	// collection views off, never /Recents.
	CollectionsRoot string
	CollectionsOff  string
	// QueriesRoot is the root of the query views (smartfolders_queries.go),
	// likewise: "" with the reason in QueriesOff.
	QueriesRoot string
	QueriesOff  string
}

// smartFoldersEnv reads JM_SMART_FOLDERS, JM_SMART_FOLDERS_ROOT,
// JM_SMART_FOLDERS_MAX_VIEWS, JM_SMART_FOLDERS_COLLECTIONS_ROOT and
// JM_SMART_FOLDERS_QUERIES_ROOT.
func smartFoldersEnv(getenv func(string) string) (bool, smartFoldersSettings, error) {
	var s smartFoldersSettings
	switch strings.ToLower(strings.TrimSpace(getenv("JM_SMART_FOLDERS"))) {
	case "on", "1", "true", "yes":
	case "", "off", "0", "false", "no":
		return false, s, nil
	default:
		return false, s, fmt.Errorf("JM_SMART_FOLDERS=%q: use on or off", getenv("JM_SMART_FOLDERS"))
	}
	s.Root = strings.TrimSpace(getenv("JM_SMART_FOLDERS_ROOT"))
	if s.Root == "" {
		s.Root = smartfolders.DefaultRoot
	}
	if err := smartfolders.ValidateRootName(s.Root); err != nil {
		return false, s, fmt.Errorf("JM_SMART_FOLDERS_ROOT: %w", err)
	}
	s.MaxViews = defaultSmartFoldersMaxViews
	if v := strings.TrimSpace(getenv("JM_SMART_FOLDERS_MAX_VIEWS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxSmartFoldersMaxViews {
			return false, s, fmt.Errorf("JM_SMART_FOLDERS_MAX_VIEWS=%q: use 1..%d", v, maxSmartFoldersMaxViews)
		}
		s.MaxViews = n
	}
	s.CollectionsRoot = strings.TrimSpace(getenv("JM_SMART_FOLDERS_COLLECTIONS_ROOT"))
	if s.CollectionsRoot == "" {
		s.CollectionsRoot = smartFoldersCollectionsRoot
	}
	if err := smartfolders.ValidateRootName(s.CollectionsRoot); err != nil {
		s.CollectionsRoot, s.CollectionsOff = "", "JM_SMART_FOLDERS_COLLECTIONS_ROOT: "+err.Error()
	} else if strings.EqualFold(s.CollectionsRoot, s.Root) {
		s.CollectionsOff = fmt.Sprintf("the collection views root %q is the /Recents root (JM_SMART_FOLDERS_ROOT); set JM_SMART_FOLDERS_COLLECTIONS_ROOT to another name", s.CollectionsRoot)
		s.CollectionsRoot = ""
	}
	// The query views root is compared with both, under case folding, as
	// the RESP proxy compares the roots it is given.
	s.QueriesRoot = strings.TrimSpace(getenv("JM_SMART_FOLDERS_QUERIES_ROOT"))
	if s.QueriesRoot == "" {
		s.QueriesRoot = smartFoldersQueriesRoot
	}
	if err := smartfolders.ValidateRootName(s.QueriesRoot); err != nil {
		s.QueriesRoot, s.QueriesOff = "", "JM_SMART_FOLDERS_QUERIES_ROOT: "+err.Error()
	} else if strings.EqualFold(s.QueriesRoot, s.Root) || (s.CollectionsRoot != "" && strings.EqualFold(s.QueriesRoot, s.CollectionsRoot)) {
		s.QueriesOff = fmt.Sprintf("the query views root %q is the /Recents or collection views root; set JM_SMART_FOLDERS_QUERIES_ROOT to another name", s.QueriesRoot)
		s.QueriesRoot = ""
	}
	return true, s, nil
}

// smartFoldersBackend hands out the FS. acquire is for the reconcile loop
// and may connect (slowly); lookup is for the event path and never does.
type smartFoldersBackend interface {
	acquire() (smartfolders.FS, func(), error)
	lookup() (smartfolders.FS, func(), error)
	close() error
	describe() string
}

// metaBackend: a JuiceFS metadata client (standalone mode), connected by
// the reconcile loop (bounded by jfsmeta.OpenTimeout) and kept until close.
// The lock is never held while connecting, so the event path (lookup) never
// waits on an unreachable endpoint.
type metaBackend struct {
	url  string
	open func(string) (*jfsmeta.FS, error)
	mu   sync.Mutex
	fs   *jfsmeta.FS
}

func (b *metaBackend) acquire() (smartfolders.FS, func(), error) {
	b.mu.Lock()
	fsys := b.fs
	b.mu.Unlock()
	if fsys == nil {
		opened, err := b.open(b.url)
		if err != nil {
			return nil, nil, err
		}
		b.mu.Lock()
		if b.fs == nil {
			b.fs = opened
		} else {
			_ = opened.Close() // only the reconcile loop opens; defensive
		}
		fsys = b.fs
		b.mu.Unlock()
	}
	// No session flushes the usage counters; do it after each cycle.
	return fsys, fsys.Flush, nil
}

func (b *metaBackend) lookup() (smartfolders.FS, func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fs == nil {
		return nil, nil, errors.New("metadata client not connected yet")
	}
	return b.fs, func() {}, nil
}

func (b *metaBackend) close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fs == nil {
		return nil
	}
	err := b.fs.Close()
	b.fs = nil
	return err
}

func (b *metaBackend) describe() string {
	return "a JuiceFS metadata client (" + redactMetaURL(b.url) + ")"
}

func redactMetaURL(raw string) string {
	opts, err := redisurl.Parse(raw)
	if err != nil {
		return "redis"
	}
	if opts.Username != "" {
		return opts.Username + "@" + opts.Addr
	}
	return opts.Addr
}

// smartFoldersConfig wires one service. Tests build it directly.
type smartFoldersConfig struct {
	Settings  smartFoldersSettings
	Backend   smartFoldersBackend
	Redis     *redis.Client        // subscribed to juicemount:metadata; closed by Stop
	StatePath string               // required: ownership must be persisted
	Leaser    func() ProjectLeaser // nil or returning nil = no checkout
	Interval  time.Duration        // reconcile debounce (default 30s)
	Tracker   smartfolders.TrackerConfig
	Now       func() time.Time
	StopWait  time.Duration
}

type folderBackoff struct {
	failures int
	until    time.Time
}

type folderCacheEntry struct {
	ok bool
	at time.Time
}

type smartFoldersService struct {
	cfg     smartFoldersConfig
	tracker *smartfolders.Tracker
	own     *smartfolders.Ownership
	cancel  context.CancelFunc
	done    sync.WaitGroup
	ready   chan struct{} // closed once the first SUBSCRIBE is acknowledged
	wake    chan struct{} // test hook: run a cycle now

	readyOnce sync.Once
	stopOnce  sync.Once

	// Reconcile-goroutine state.
	savedGen    uint64
	swept       bool
	disabled    atomic.Bool // a panic was caught; nothing more runs in this process
	rootRetryAt time.Time
	backoff     map[string]*folderBackoff
	lastLog     map[string]string

	cacheMu sync.Mutex
	cache   map[string]folderCacheEntry

	coll collectionViews // smartfolders_collections.go
	qry  queryViews      // smartfolders_queries.go
	// qstate is the query views' working state (reconcile goroutine only).
	qstate queryState
	// recentsOwned: the /Recents root is the Manager's (its last check).
	recentsOwned atomic.Bool
}

type smartFoldersState struct {
	Version   int                     `json:"version"`
	Tracker   json.RawMessage         `json:"tracker,omitempty"`
	Ownership *smartfolders.Ownership `json:"ownership,omitempty"`
	// Version 3: the collection views' own records.
	Collections *collectionViewsState `json:"collections,omitempty"`
	// Version 4: the query views' own records.
	Queries *queryViewsState `json:"queries,omitempty"`
}

func newSmartFoldersService(cfg smartFoldersConfig) (*smartFoldersService, error) {
	if cfg.Redis == nil || cfg.Backend == nil {
		return nil, errors.New("smart folders need a Redis client and a backend")
	}
	if cfg.StatePath == "" {
		return nil, errors.New("smart folders need a state file (JM_STATE_FILE) to record what they create")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultSmartFoldersInterval
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.StopWait <= 0 {
		cfg.StopWait = smartFoldersStopTimeout
	}
	s := &smartFoldersService{
		cfg:     cfg,
		own:     &smartfolders.Ownership{},
		ready:   make(chan struct{}),
		wake:    make(chan struct{}, 1),
		backoff: map[string]*folderBackoff{},
		lastLog: map[string]string{},
		cache:   map[string]folderCacheEntry{},
	}
	s.coll.init(collectionKind())
	s.qry.init(queryKind())
	s.qstate = queryState{cands: map[catalog.QueryID]*queryCandidates{}, runs: map[catalog.QueryID]*queryRun{}}
	tc := cfg.Tracker
	tc.Root = cfg.Settings.Root
	tc.MaxFolders = cfg.Settings.MaxViews
	tc.FolderOK = s.folderOK
	tc.Ignore = func(top string) bool { return s.collectionsRootActive(top) || s.queriesRootActive(top) }
	if tc.Now == nil {
		tc.Now = cfg.Now
	}
	tr, err := smartfolders.NewTracker(tc)
	if err != nil {
		return nil, err
	}
	s.tracker = tr
	data, err := os.ReadFile(cfg.StatePath)
	switch {
	case err == nil:
		if err := s.load(data); err != nil {
			// Never guess ownership: without the records nothing already in
			// the volume can be told apart from a user's entries.
			return nil, fmt.Errorf("smart-folder state %s: %w", cfg.StatePath, err)
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf("reading smart-folder state: %w", err)
	}
	s.savedGen = tr.Generation()
	s.recentsOwned.Store(s.own.Root != nil && !s.own.Root.Pending)
	return s, nil
}

func (s *smartFoldersService) load(data []byte) error {
	var st smartFoldersState
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	switch st.Version {
	case 1: // round-1 format: the tracker state alone, no ownership records
		return s.tracker.LoadState(data)
	case smartFoldersStateVersionV2:
		if st.Collections != nil || st.Queries != nil {
			return errors.New("version 2 with collection- or query-view records")
		}
	case smartFoldersStateVersion:
		if st.Queries != nil {
			return errors.New("version 3 with query-view records")
		}
		s.coll.load(st.Collections)
	case smartFoldersStateVersionV4:
		s.coll.load(st.Collections)
		s.qry.load(st.Queries)
	default:
		return fmt.Errorf("unsupported version %d", st.Version)
	}
	if st.Ownership != nil {
		s.own = st.Ownership
		s.own.Normalize() // omitempty drops empty maps
	}
	if len(st.Tracker) > 0 {
		return s.tracker.LoadState(st.Tracker)
	}
	return nil
}

// persist durably writes the tracker state and the ownership records.
func (s *smartFoldersService) persist() error {
	tracker, gen, err := s.tracker.MarshalState()
	if err != nil {
		return err
	}
	st := smartFoldersState{Version: smartFoldersStateVersionV2, Tracker: tracker, Ownership: s.own}
	if c := s.coll.state(); c != nil {
		st.Version, st.Collections = smartFoldersStateVersion, c
	}
	if q := s.qry.state(); q != nil {
		st.Version, st.Queries = smartFoldersStateVersionV4, q
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic0600Durable(s.cfg.StatePath, data); err != nil {
		return err
	}
	s.savedGen = gen
	return nil
}

// folderOK reports whether a top-level folder exists as a real directory
// (cached; bounded), so events naming folders that do not exist never start
// a list. It never connects the backend: it runs on the event path.
func (s *smartFoldersService) folderOK(folder string) bool {
	now := s.cfg.Now()
	s.cacheMu.Lock()
	if e, ok := s.cache[folder]; ok && now.Sub(e.at) < smartFoldersFolderCacheTTL {
		s.cacheMu.Unlock()
		return e.ok
	}
	s.cacheMu.Unlock()
	fsys, release, err := s.cfg.Backend.lookup()
	if err != nil {
		// Not connected yet (or not mounted): accept provisionally, uncached.
		// The folder bound still applies, and the reconcile loop forgets
		// every entry whose target is missing, which empties (and drops) a
		// made-up folder at its first cycle.
		return true
	}
	info, err := fsys.Lstat(folder)
	release()
	ok := err == nil && info.Type == smartfolders.TypeDir
	s.cacheMu.Lock()
	if len(s.cache) >= smartFoldersFolderCacheMax {
		s.cache = map[string]folderCacheEntry{}
	}
	s.cache[folder] = folderCacheEntry{ok: ok, at: now}
	s.cacheMu.Unlock()
	return ok
}

// Start runs the subscriber and the reconcile loop until Stop.
func (s *smartFoldersService) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done.Add(2)
	go func() { defer s.done.Done(); s.subscribeLoop(ctx) }()
	go func() { defer s.done.Done(); s.reconcileLoop(ctx) }()
}

// Stop ends both loops (waiting at most StopWait), saves the state, closes
// the backend (ending its JuiceFS session) and the Redis client.
func (s *smartFoldersService) Stop() {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		finished := make(chan struct{})
		go func() { s.done.Wait(); close(finished) }()
		select {
		case <-finished:
			if s.tracker.Generation() != s.savedGen {
				if err := s.persist(); err != nil {
					log.Printf("manager: smart folders: saving state: %v", err)
				}
			}
			if err := s.cfg.Backend.close(); err != nil {
				log.Printf("manager: smart folders: closing %s: %v", s.cfg.Backend.describe(), err)
			}
		case <-time.After(s.cfg.StopWait):
			// A cycle is stuck (a hung FUSE or lease call). Its records were
			// saved before it acted; leave it rather than block shutdown.
			log.Printf("manager: smart folders: did not stop within %s; leaving it", s.cfg.StopWait)
		}
		_ = s.cfg.Redis.Close()
	})
}

func (s *smartFoldersService) subscribeLoop(ctx context.Context) {
	// Same guard as the cycle: the event path (tracker -> folderOK ->
	// backend lookup) must never take the Manager down.
	defer func() {
		if p := recover(); p != nil {
			s.disable("the change-event reader", p)
		}
	}()
	backoff := time.Second
	for ctx.Err() == nil {
		ps := s.cfg.Redis.Subscribe(ctx, metadataEventChannel)
		if _, err := ps.Receive(ctx); err != nil {
			_ = ps.Close()
			if ctx.Err() == nil {
				log.Printf("manager: smart folders: subscribe %s: %v (retrying in %s)", metadataEventChannel, err, backoff)
				select {
				case <-ctx.Done():
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, time.Minute)
			}
			continue
		}
		backoff = time.Second
		s.readyOnce.Do(func() { close(s.ready) })
		// go-redis re-subscribes on its own after a dropped connection; the
		// channel closes only when ps is closed.
		ch := ps.Channel(redis.WithChannelSize(1024))
	read:
		for {
			select {
			case <-ctx.Done():
				break read
			case msg, ok := <-ch:
				if !ok {
					break read
				}
				if ev, err := smartfolders.ParseEvent([]byte(msg.Payload)); err == nil {
					s.tracker.Apply(ev)
					s.noteCollectionEvent(ev)
					s.noteQueryEvent(ev)
				}
			}
		}
		_ = ps.Close()
	}
}

func (s *smartFoldersService) reconcileLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	s.cycle(ctx) // restore views right after a restart
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-s.wake:
		case <-s.coll.wake:
			s.collectionsOnlyCycle(ctx)
			continue
		case <-s.qry.wake:
			s.queriesOnlyCycle(ctx)
			continue
		}
		s.cycle(ctx)
	}
}

// disable turns smart folders off for this process after a panic in one of
// its goroutines: logged with the stack, never re-raised.
func (s *smartFoldersService) disable(where string, p any) {
	s.disabled.Store(true)
	log.Printf("manager: smart folders DISABLED until restart after an internal error in %s: %v\n%s", where, p, debug.Stack())
}

// kick runs a reconcile cycle now instead of waiting for the ticker (tests).
func (s *smartFoldersService) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// cycle reconciles every dirty view whose backoff has passed, /Recents
// then the collection views. A root the Manager did not create stops its
// own kind of view (no lease is checked out) until it is looked at again
// ten minutes later.
func (s *smartFoldersService) cycle(ctx context.Context) {
	if s.disabled.Load() {
		return
	}
	// A bug in smart folders must never take the Manager down (or loop it
	// through restarts): log the stack once and turn the feature off for
	// this process.
	defer func() {
		if p := recover(); p != nil {
			s.disable("a reconcile cycle", p)
		}
	}()
	now := s.cfg.Now()
	recents := !now.Before(s.rootRetryAt)
	if !recents && !s.collectionsDue(now) && !s.qry.due(now) {
		return
	}
	fsys, release, err := s.cfg.Backend.acquire()
	if err != nil {
		s.logOnce("", fmt.Sprintf("views not updated: %v", err))
		return
	}
	defer release()
	if recents {
		s.recentsCycle(ctx, fsys, now)
	}
	// Collection and query views run in the same cycle, after /Recents
	// (and on their own when a collection, a query or a file under one
	// changes): one goroutine writes the volume.
	s.collectionsCycle(ctx, fsys)
	s.qry.cycle(s, ctx, fsys)
}

// recentsCycle reconciles every dirty /Recents view whose backoff has
// passed.
func (s *smartFoldersService) recentsCycle(ctx context.Context, fsys smartfolders.FS, now time.Time) {
	rec := &smartfolders.Reconciler{
		FS:       fsys,
		Root:     s.cfg.Settings.Root,
		Own:      s.own,
		Persist:  s.persist,
		Gate:     s.gate(ctx),
		MaxViews: s.cfg.Settings.MaxViews,
	}
	if s.collectionsRootActive(s.cfg.Settings.CollectionsRoot) {
		rec.Exclude = []string{s.cfg.Settings.CollectionsRoot}
	}
	if s.queriesRootActive(s.cfg.Settings.QueriesRoot) {
		rec.Exclude = append(rec.Exclude, s.cfg.Settings.QueriesRoot)
	}
	state, err := rec.CheckRoot()
	s.recentsOwned.Store(s.own.Root != nil && !s.own.Root.Pending)
	if err != nil {
		s.logOnce("", fmt.Sprintf("checking /%s: %v", s.cfg.Settings.Root, err))
		return
	}
	if state == smartfolders.RootForeign {
		s.rootRetryAt = now.Add(smartFoldersRootRecheck)
		s.logOnce("", fmt.Sprintf("/%s exists and was not created by the Manager: smart folders leave it alone (checking again every %s; rename it or set JM_SMART_FOLDERS_ROOT)", s.cfg.Settings.Root, smartFoldersRootRecheck))
		return
	}
	s.logOnce("", "")
	if !s.swept {
		views := make([]string, 0, len(s.own.Views))
		for f := range s.own.Views {
			views = append(views, f)
		}
		s.tracker.MarkDirty(views...)
		s.tracker.MarkDirty(s.tracker.Folders()...)
		s.swept = true
	}
	s.tracker.Expire()
	for _, folder := range s.tracker.TakeDirty() {
		if b := s.backoff[folder]; ctx.Err() != nil || (b != nil && now.Before(b.until)) {
			s.tracker.MarkDirty(folder)
			continue
		}
		if err := s.reconcileFolder(rec, folder); err != nil {
			b := s.backoff[folder]
			if b == nil {
				b = &folderBackoff{}
				s.backoff[folder] = b
			}
			b.failures++
			delay := s.cfg.Interval << min(b.failures-1, 16)
			if delay > smartFoldersMaxBackoff || delay <= 0 {
				delay = smartFoldersMaxBackoff
			}
			b.until = now.Add(delay)
			s.tracker.MarkDirty(folder)
			continue
		}
		delete(s.backoff, folder)
	}
	if s.tracker.Generation() != s.savedGen {
		if err := s.persist(); err != nil {
			s.logOnce("save", fmt.Sprintf("saving state: %v", err))
		} else {
			s.logOnce("save", "")
		}
	}
}

// ownedRoots lists the smart-folder roots the Manager owns now (any
// goroutine): a query may not be defined inside one. A user-made folder of
// such a name is left alone (decision 2), so it is not listed.
func (s *smartFoldersService) ownedRoots() []string {
	var out []string
	if s.recentsOwned.Load() {
		out = append(out, s.cfg.Settings.Root)
	}
	if r := s.cfg.Settings.CollectionsRoot; r != "" && s.coll.rootOwned.Load() {
		out = append(out, r)
	}
	if r := s.cfg.Settings.QueriesRoot; r != "" && s.qry.rootOwned.Load() {
		out = append(out, r)
	}
	return out
}

// gate checks out the view's project lease (the same gate trash restore
// uses) before the reconciler's first write into it.
func (s *smartFoldersService) gate(ctx context.Context) func(string) (func(), error) {
	return func(view string) (func(), error) {
		if s.cfg.Leaser == nil {
			return nil, nil
		}
		leaser := s.cfg.Leaser()
		if leaser == nil {
			return nil, nil
		}
		proj := projectOfVolumePath("/" + view)
		if proj == "" {
			return nil, nil
		}
		lctx, cancel := context.WithTimeout(ctx, smartFoldersLeaseTimeout)
		defer cancel()
		release, _, err := leaser.Checkout(lctx, []string{proj})
		if err != nil {
			return nil, fmt.Errorf("lease refused: %w", err)
		}
		return release, nil
	}
}

// reconcileFolder updates one view: names stay bound to their targets, and
// files found gone are forgotten and the view recomputed once.
func (s *smartFoldersService) reconcileFolder(rec *smartfolders.Reconciler, folder string) error {
	view := s.cfg.Settings.Root + "/" + folder
	for pass := 0; pass < 2; pass++ {
		links := smartfolders.Names(s.tracker.Recent(folder), rec.PreviousNames(folder))
		res, err := rec.Reconcile(folder, links)
		var gone, problems []string
		for _, r := range res.Refused {
			// A target under the collection or query views' root (once the
			// Manager owns it, the tracker no longer lists it) is forbidden.
			top, _, _ := strings.Cut(r.Target, "/")
			if r.TargetGone() || (errors.Is(r.Err, smartfolders.ErrTargetForbidden) && (s.collectionsRootActive(top) || s.queriesRootActive(top))) {
				gone = append(gone, r.Target)
			} else {
				problems = append(problems, fmt.Sprintf("%s: %v", r.Name, r.Err))
			}
		}
		if len(res.Foreign) > 0 {
			problems = append(problems, "replaced by entries the Manager did not create (left alone): "+strings.Join(res.Foreign, ", "))
		}
		if err != nil {
			problems = append([]string{err.Error()}, problems...)
		}
		if len(problems) > 0 {
			s.logOnce(folder, fmt.Sprintf("view %s: %s", view, strings.Join(problems, "; ")))
		} else {
			s.logOnce(folder, "")
		}
		if err != nil {
			return err
		}
		if len(gone) == 0 {
			return nil
		}
		s.tracker.Forget(gone...)
	}
	return nil
}

// logOnce logs msg for key unless it repeats the last message for key; an
// empty msg clears it, so a problem that comes back is logged again.
func (s *smartFoldersService) logOnce(key, msg string) {
	if s.lastLog[key] == msg {
		return
	}
	s.lastLog[key] = msg
	if msg != "" {
		log.Printf("manager: smart folders: %s", msg)
	}
}

// startSmartFolders starts the service when JM_SMART_FOLDERS=on. It never
// fails the Manager: every problem is logged and the feature stays off.
func (a *API) startSmartFolders(cfg Config, getenv func(string) string) *smartFoldersService {
	svc := a.buildSmartFolders(cfg, getenv)
	if svc != nil {
		svc.Start()
		log.Printf("manager: smart folders ON (experimental): /%s/<folder> views (at most %d) written through %s",
			svc.cfg.Settings.Root, svc.cfg.Settings.MaxViews, svc.cfg.Backend.describe())
	}
	return svc
}

// buildSmartFolders picks the backend from the Manager's mode: the FUSE
// mount in embedded mode, else a JuiceFS metadata client on the Manager's
// own metadata URL (standalone mode, the default deployment).
func (a *API) buildSmartFolders(cfg Config, getenv func(string) string) *smartFoldersService {
	enabled, settings, err := smartFoldersEnv(getenv)
	if err != nil {
		log.Printf("manager: smart folders disabled: %v", err)
		return nil
	}
	if !enabled {
		return nil
	}
	if cfg.StateFile == "" {
		log.Printf("manager: smart folders disabled: they need JM_STATE_FILE (--state-file) to record what they create")
		return nil
	}
	// Smart folders write only through the JuiceFS metadata client
	// (standalone mode, the default deployment). Through a mount, a newly
	// created directory cannot be identified atomically, so embedded mode
	// is refused rather than half supported.
	if cfg.FUSEMount != "" {
		log.Printf("manager: smart folders NOT started: they require standalone mode (JM_META set, no --fuse-mount); this Manager runs in embedded mode")
		return nil
	}
	if cfg.MetaURL == "" {
		log.Printf("manager: smart folders NOT started: they require the volume's metadata URL (JM_META, standalone mode)")
		return nil
	}
	backend := &metaBackend{url: cfg.MetaURL, open: jfsmeta.Open}
	redisURL := cfg.MetaURL
	opts, err := redisurl.Parse(redisURL)
	if err != nil {
		log.Printf("manager: smart folders disabled: %v", err)
		return nil
	}
	rdb := redis.NewClient(opts)
	svc, err := newSmartFoldersService(smartFoldersConfig{
		Settings:  settings,
		Backend:   backend,
		Redis:     rdb,
		StatePath: filepath.Join(filepath.Dir(cfg.StateFile), smartFoldersStateName),
		Leaser:    a.jobs.projectLeaser,
	})
	if err != nil {
		_ = rdb.Close()
		log.Printf("manager: smart folders disabled: %v", err)
		return nil
	}
	return svc
}
