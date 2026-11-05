package manager

// continuity.go: the continuity volume binder (experimental, off by
// default). With JM_CONTINUITY=on the Manager keeps a continuity catalog in
// a private subdirectory of its state directory (never on the volume) and,
// following the same juicemount:metadata change events smart folders use,
// binds every regular file to a catalog asset keyed by the volume UUID and
// the file's inode; the path is only a hint (internal/continuity/binder).
// It reads the volume's metadata and never writes to the volume.
//
// Known gaps (internal/continuity/binder/README.md): changes made while
// the Manager is down, dropped from the bounded queue, or published by no
// client are missed until a rescan, which this slice does not have;
// GET /api/continuity/status reports what was missed.
//
// Collections (collections.go) and saved queries (queries.go) live in the
// same catalog.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
	"github.com/redis/go-redis/v9"
)

const (
	continuityDirName     = "continuity"
	continuityCatalogName = "catalog.sqlite"
	continuityStopTimeout = 10 * time.Second
)

// continuityEnv reads JM_CONTINUITY.
func continuityEnv(getenv func(string) string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(getenv("JM_CONTINUITY"))) {
	case "on", "1", "true", "yes":
		return true, nil
	case "", "off", "0", "false", "no":
		return false, nil
	default:
		return false, fmt.Errorf("JM_CONTINUITY=%q: use on or off", getenv("JM_CONTINUITY"))
	}
}

type continuityService struct {
	binder   *binder.Binder
	store    *catalog.Store
	rdb      *redis.Client // nil: no subscription (tests)
	cancel   context.CancelFunc
	done     sync.WaitGroup
	stopOnce sync.Once
	stopWait time.Duration
	// views shows the collections and queries as smart folders
	// (smartfolders_collections.go, smartfolders_queries.go); nil while not
	// attached.
	views atomic.Pointer[smartFoldersService]
}

func (s *continuityService) start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done.Add(1)
	go func() { defer s.done.Done(); s.binder.Run(ctx) }()
	if s.rdb != nil {
		s.done.Add(1)
		go func() { defer s.done.Done(); s.binder.Subscribe(ctx, s.rdb, metadataEventChannel) }()
	}
}

// Stop ends both loops (waiting at most stopWait), then closes the volume
// connection, the catalog and the Redis client. A stuck loop is left; the
// catalog is then not closed under it.
func (s *continuityService) Stop() {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		finished := make(chan struct{})
		go func() { s.done.Wait(); close(finished) }()
		wait := s.stopWait
		if wait <= 0 {
			wait = continuityStopTimeout
		}
		select {
		case <-finished:
			if err := errors.Join(s.binder.Close(), s.store.Close()); err != nil {
				log.Printf("manager: continuity: closing: %v", err)
			}
		case <-time.After(wait):
			log.Printf("manager: continuity: did not stop within %s; leaving it", wait)
		}
		if s.rdb != nil {
			_ = s.rdb.Close()
		}
	})
}

// startContinuity starts the binder when JM_CONTINUITY=on. It never fails
// the Manager: every problem is logged and the feature stays off.
func (a *API) startContinuity(cfg Config, getenv func(string) string) *continuityService {
	svc := buildContinuity(cfg, getenv)
	if svc != nil {
		svc.start()
		library, _ := svc.store.LibraryID()
		log.Printf("manager: continuity ON (experimental): binding volume files to catalog %s", library)
		a.attachCollectionViews(svc)
	}
	return svc
}

// attachCollectionViews shows the collections and saved queries as smart
// folders when smart folders run too (JM_SMART_FOLDERS=on; started before
// continuity).
func (a *API) attachCollectionViews(svc *continuityService) {
	if a.jobs == nil {
		return
	}
	a.jobs.mu.RLock()
	sf, _ := a.jobs.smartFolders.(*smartFoldersService)
	a.jobs.mu.RUnlock()
	if sf == nil {
		return
	}
	collections := sf.attachCollections(svc)
	if sf.attachQueries(svc) || collections {
		svc.views.Store(sf)
	}
}

// collectionsChanged tells the collection views a collection changed.
func (s *continuityService) collectionsChanged() {
	if v := s.views.Load(); v != nil {
		v.collectionsChanged()
	}
}

// queriesChanged tells the query views a query changed.
func (s *continuityService) queriesChanged() {
	if v := s.views.Load(); v != nil {
		v.queriesChanged()
	}
}

// applied is the binder's Applied hook: an operation that moved a file's
// path hint (bound, moved, gone) marks the query views watching either
// path dirty. Binder goroutine; map lookups only.
func (s *continuityService) applied(op catalog.Operation) {
	v := s.views.Load()
	if v == nil {
		return
	}
	var paths []string
	hint := func(l catalog.LocationValue) {
		if strings.HasPrefix(l.Source, "jfs-path:") {
			paths = append(paths, l.Locator)
		}
	}
	for _, c := range op.Changes {
		switch {
		case c.PutLocation != nil:
			hint(c.PutLocation.Value)
			if c.PutLocation.Expected != nil {
				hint(c.PutLocation.Expected.Value)
			}
		case c.RemoveLocation != nil && c.RemoveLocation.Expected != nil:
			hint(c.RemoveLocation.Expected.Value)
		}
	}
	if len(paths) > 0 {
		v.qry.notePaths(paths...)
	}
}

// activeCollections lists every collection that is not removed.
func (s *continuityService) activeCollections(ctx context.Context) ([]catalog.Collection, error) {
	library, err := s.store.LibraryID()
	if err != nil {
		return nil, err
	}
	var out []catalog.Collection
	var after catalog.CollectionID
	for {
		page, err := s.store.ListCollections(ctx, library, after, catalog.MaxPageSize)
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

// collectionMembers lists a collection's active members (at most
// catalog.MaxCollectionMembers), ordered by asset ID.
func (s *continuityService) collectionMembers(ctx context.Context, id catalog.CollectionID) ([]catalog.CollectionMember, error) {
	library, err := s.store.LibraryID()
	if err != nil {
		return nil, err
	}
	var out []catalog.CollectionMember
	var after continuity.AssetID
	for {
		page, err := s.store.ListCollectionMembers(ctx, library, id, after, catalog.MaxPageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < catalog.MaxPageSize {
			return out, nil
		}
		after = page[len(page)-1].AssetID
	}
}

// resolveAsset returns the asset's current path, resolved from the volume
// by inode (never the path hint), and its state as the members API reports
// it; binder.ErrNotReady while the volume is not connected.
func (s *continuityService) resolveAsset(ctx context.Context, id continuity.AssetID) (string, string, error) {
	v, err := s.binder.AssetByID(ctx, id)
	if errors.Is(err, catalog.ErrNotFound) {
		return "", "missing", nil
	}
	if err != nil {
		return "", "", err
	}
	if v.ResolveError == binder.ErrNotReady.Error() {
		return "", "unknown", binder.ErrNotReady
	}
	return v.Path, memberState(v), nil
}

func buildContinuity(cfg Config, getenv func(string) string) *continuityService {
	enabled, err := continuityEnv(getenv)
	if err != nil {
		log.Printf("manager: continuity disabled: %v", err)
		return nil
	}
	if !enabled {
		return nil
	}
	if cfg.StateFile == "" {
		log.Printf("manager: continuity disabled: the catalog lives beside JM_STATE_FILE (--state-file), which is not set")
		return nil
	}
	// Reads only, so either mode works: standalone names the volume's
	// metadata URL directly, embedded passes it for the Overview tab.
	metaURL := cfg.MetaURL
	if metaURL == "" {
		metaURL = cfg.OverviewMetaURL
	}
	if metaURL == "" {
		log.Printf("manager: continuity disabled: it needs the volume's metadata URL (JM_META)")
		return nil
	}
	opts, err := redisurl.Parse(metaURL)
	if err != nil {
		log.Printf("manager: continuity disabled: %v", err)
		return nil
	}
	store, err := openContinuityCatalog(filepath.Dir(cfg.StateFile), cfg.FUSEMount, cfg.DestMount)
	if err != nil {
		log.Printf("manager: continuity disabled: %v", err)
		return nil
	}
	svc := &continuityService{store: store}
	b, err := binder.New(binder.Config{Store: store, Applied: svc.applied, Connect: func() (binder.Volume, error) {
		fsys, err := jfsmeta.OpenReadOnly(metaURL)
		if err != nil {
			return nil, err
		}
		return fsys, nil
	}})
	if err != nil {
		_ = store.Close()
		log.Printf("manager: continuity disabled: %v", err)
		return nil
	}
	svc.binder, svc.rdb = b, redis.NewClient(opts)
	return svc
}

// openContinuityCatalog opens (creating on first use) the catalog in
// <stateDir>/continuity: a directory of its own, mode 0700 and owned by this
// process, so no other account can plant or swap the catalog's files, and
// never inside the volume (a mount path, or any FUSE filesystem on Linux).
// The directory is checked and narrowed through one no-follow handle. A
// catalog it cannot open is left untouched: never repaired or replaced.
func openContinuityCatalog(stateDir string, mounts ...string) (*catalog.Store, error) {
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("state directory: %w", err)
	}
	base, err := filepath.EvalSymlinks(stateDir)
	if err != nil {
		return nil, fmt.Errorf("state directory: %w", err)
	}
	if base, err = filepath.Abs(base); err != nil {
		return nil, err
	}
	// Create and Open reach the catalog by path again, so the state
	// directory holding continuity/ must not let another account swap it.
	if err := checkPrivateParent(base); err != nil {
		return nil, err
	}
	dir := filepath.Join(base, continuityDirName)
	for _, m := range mounts {
		if m == "" {
			continue
		}
		mount, err := filepath.EvalSymlinks(m)
		if err != nil {
			mount = m // not mounted (yet): compare the configured path
		}
		if mount, err = filepath.Abs(mount); err == nil && (dir == mount || strings.HasPrefix(dir, mount+string(filepath.Separator)) || mount == "/") {
			return nil, fmt.Errorf("the catalog directory %s is inside the volume mount %s", dir, mount)
		}
	}
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("catalog directory: %w", err)
	}
	fd, err := syscall.Open(dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("catalog directory %s (must be a real directory, not a symlink): %w", dir, err)
	}
	handle := os.NewFile(uintptr(fd), dir)
	defer handle.Close()
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		return nil, fmt.Errorf("catalog directory: %w", err)
	}
	if int(st.Uid) != os.Geteuid() {
		return nil, fmt.Errorf("catalog directory %s is owned by uid %d, not this process (%d)", dir, st.Uid, os.Geteuid())
	}
	// An existing directory keeps its mode through Mkdir: narrow it.
	if err := syscall.Fchmod(fd, 0o700); err != nil {
		return nil, fmt.Errorf("catalog directory: %w", err)
	}
	if err := refuseFUSEDir(fd, dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, continuityCatalogName)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		library, err := continuity.NewLibraryID()
		if err != nil {
			return nil, err
		}
		store, err := catalog.Create(path, library)
		if err != nil {
			return nil, fmt.Errorf("creating the catalog %s: %w", path, err)
		}
		return store, nil
	} else if err != nil {
		return nil, err
	}
	store, err := catalog.Open(path)
	if errors.Is(err, catalog.ErrUpgradeRequired) {
		// The Manager owns this catalog and nothing else uses it yet, so it
		// upgrades it in place (a format 2 catalog from slice 2 gains the
		// collection tables, a format 3 one the query table; nothing
		// existing changes). A binary from
		// before the upgrade refuses the new format; it never damages it.
		err = upgradeContinuityCatalog(path)
		if err == nil {
			store, err = catalog.Open(path)
		}
	}
	switch {
	case err == nil:
		return store, nil
	case errors.Is(err, catalog.ErrUpgradeRequired):
		return nil, fmt.Errorf("the catalog %s needs an explicit schema upgrade: %w", path, err)
	case errors.Is(err, catalog.ErrNewerFormat):
		return nil, fmt.Errorf("the catalog %s was written by a newer JuiceMount (left untouched; update this Manager): %w", path, err)
	case errors.Is(err, catalog.ErrFormat):
		return nil, fmt.Errorf("the catalog %s is incomplete or corrupt (left untouched): %w", path, err)
	default:
		return nil, fmt.Errorf("opening the catalog %s: %w", path, err)
	}
}

func upgradeContinuityCatalog(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	library, version, err := catalog.Inspect(ctx, path)
	if err != nil {
		return err
	}
	if err := catalog.Upgrade(ctx, path, library); err != nil {
		return fmt.Errorf("upgrading the catalog %s from format %d: %w", path, version, err)
	}
	log.Printf("manager: continuity: upgraded the catalog %s from format %d", path, version)
	return nil
}

// checkPrivateParent refuses a state directory another account could write
// to (or that another account owns).
func checkPrivateParent(dir string) error {
	fd, err := syscall.Open(dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("state directory %s: %w", dir, err)
	}
	defer syscall.Close(fd)
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		return fmt.Errorf("state directory %s: %w", dir, err)
	}
	if int(st.Uid) != os.Geteuid() || st.Mode&0o022 != 0 {
		return fmt.Errorf("state directory %s must be owned by this process (uid %d) and not writable by group or others (owner %d, mode %o)", dir, os.Geteuid(), st.Uid, st.Mode&0o7777)
	}
	return nil
}

// handleContinuityAssets: GET ?path=<volume path> or ?id=<asset id> returns
// the asset, its active locations and the file's current path(s).
func (a *API) handleContinuityAssets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	svc := a.continuity
	if svc == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "continuity is off (JM_CONTINUITY)"})
		return
	}
	q := r.URL.Query()
	p, id := q.Get("path"), q.Get("id")
	if (p == "") == (id == "") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "give exactly one of path or id"})
		return
	}
	var view binder.AssetView
	var err error
	if p != "" {
		view, err = svc.binder.AssetByPath(r.Context(), p)
	} else {
		view, err = svc.binder.AssetByID(r.Context(), continuity.AssetID(id))
	}
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, view)
	case errors.Is(err, catalog.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid path or asset id"})
	case errors.Is(err, catalog.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not in the catalog"})
	case errors.Is(err, binder.ErrNotReady):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
	default:
		log.Printf("manager: continuity: asset lookup: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "catalog lookup failed"})
	}
}

// handleContinuityStatus: GET returns the binder's counters, including what
// it missed (the state a later rescan starts from).
func (a *API) handleContinuityStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	svc := a.continuity
	if svc == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}
	out := map[string]any{"enabled": true, "binder": svc.binder.Stats()}
	if v := svc.views.Load(); v != nil {
		out["collection_views"] = v.collectionStats()
		out["query_views"] = v.queryStats()
	}
	writeJSON(w, http.StatusOK, out)
}
