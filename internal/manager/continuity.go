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
	}
	return svc
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
	b, err := binder.New(binder.Config{Store: store, Connect: func() (binder.Volume, error) {
		fsys, err := jfsmeta.Open(metaURL)
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
	return &continuityService{binder: b, store: store, rdb: redis.NewClient(opts)}
}

// openContinuityCatalog opens (creating on first use) the catalog in
// <stateDir>/continuity: a directory of its own, mode 0700 and owned by this
// process, so no other account can plant or swap the catalog's files, and
// never inside the volume (a mount path, or any FUSE filesystem on Linux).
// A catalog it cannot open is left untouched: never repaired or replaced.
func openContinuityCatalog(stateDir string, mounts ...string) (*catalog.Store, error) {
	dir, err := filepath.Abs(filepath.Join(stateDir, continuityDirName))
	if err != nil {
		return nil, err
	}
	for _, m := range mounts {
		if m == "" {
			continue
		}
		if mount, err := filepath.Abs(m); err == nil && (dir == mount || strings.HasPrefix(dir, mount+string(filepath.Separator))) {
			return nil, fmt.Errorf("the catalog directory %s is inside the volume mount %s", dir, mount)
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("catalog directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("catalog directory: %w", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || !ok {
		return nil, fmt.Errorf("catalog directory %s is not a directory", dir)
	}
	if int(st.Uid) != os.Geteuid() {
		return nil, fmt.Errorf("catalog directory %s is owned by uid %d, not this process (%d)", dir, st.Uid, os.Geteuid())
	}
	// MkdirAll leaves an existing directory's mode as it was.
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("catalog directory: %w", err)
	}
	if err := refuseFUSEDir(dir); err != nil {
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
	switch {
	case err == nil:
		return store, nil
	case errors.Is(err, catalog.ErrUpgradeRequired):
		return nil, fmt.Errorf("the catalog %s needs an explicit schema upgrade: %w", path, err)
	case errors.Is(err, catalog.ErrFormat):
		return nil, fmt.Errorf("the catalog %s is incomplete or corrupt (left untouched): %w", path, err)
	default:
		return nil, fmt.Errorf("opening the catalog %s: %w", path, err)
	}
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
	writeJSON(w, http.StatusOK, map[string]any{"enabled": true, "binder": svc.binder.Stats()})
}
