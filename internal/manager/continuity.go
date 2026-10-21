package manager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// ContinuityConfig names an existing, locally administered catalog. The
// deployment key protects the legacy host; it is never a discovery identity.
type ContinuityConfig struct {
	CatalogPath     string
	LibraryID       string
	ManagerAdminKey string
}

type Continuity struct {
	store      *catalog.Store
	service    *catalog.DiscoveryService
	ctx        context.Context
	cancel     context.CancelFunc
	slots      chan struct{}
	mu         sync.Mutex
	closeGate  chan struct{}
	registered bool
	closing    bool
	closed     bool
	active     int
	drained    chan struct{}
	closeErr   error
}

// PrepareContinuity recognizes existing authority only. In particular, an
// imported catalog must be explicitly rebound by local owner administration.
func PrepareContinuity(ctx context.Context, cfg ContinuityConfig) (*Continuity, error) {
	if cfg.CatalogPath == "" && cfg.LibraryID == "" {
		return nil, nil
	}
	if strings.TrimSpace(cfg.CatalogPath) == "" || cfg.LibraryID == "" {
		return nil, errors.New("continuity requires both catalog path and library")
	}
	key := strings.TrimSpace(cfg.ManagerAdminKey)
	if len(key) < 32 {
		return nil, errors.New("continuity requires a Manager deployment key of at least 32 characters")
	}
	for _, prefix := range []string{"CHANGE", "CHANGEME", "REPLACE", "REPLACEME"} {
		if strings.HasPrefix(strings.ToUpper(key), prefix) {
			return nil, errors.New("continuity refuses a placeholder Manager deployment key")
		}
	}
	library := continuity.LibraryID(cfg.LibraryID)
	if err := library.Validate(); err != nil {
		return nil, errors.New("invalid continuity library")
	}
	store, err := catalog.OpenContext(ctx, cfg.CatalogPath)
	if err != nil {
		return nil, fmt.Errorf("open continuity catalog: %w", err)
	}
	service, err := catalog.NewDiscoveryService(ctx, store, library)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("prepare continuity discovery: %w", err), store.Close())
	}
	lifetime, cancel := context.WithCancel(context.Background())
	return &Continuity{
		store: store, service: service, ctx: lifetime, cancel: cancel,
		slots: make(chan struct{}, 16), drained: make(chan struct{}), closeGate: make(chan struct{}, 1),
	}, nil
}

// RegisterRoutes is called once, on an unserved caller-owned mux. Failure must
// abort startup; the caller must not serve a partially configured mux.
func (c *Continuity) RegisterRoutes(mux *http.ServeMux, prefix string) (err error) {
	if c == nil {
		return nil
	}
	if mux == nil || (prefix != "" && (prefix == "/" || !strings.HasPrefix(prefix, "/") ||
		path.Clean(prefix) != prefix || strings.ContainsAny(prefix, "{}%?#\\ \t\r\n"))) {
		return errors.New("invalid continuity route prefix")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.registered || c.closing {
		return errors.New("continuity routes already registered or closing")
	}
	defer func() {
		if recover() != nil {
			err = errors.New("continuity route registration conflict")
		}
	}()
	base := prefix + "/api/continuity"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, base+"/v1/collections/") {
			c.serveContinuityHTTP(w, r, base+"/v1/collections/", 1, true)
			return
		}
		if strings.HasPrefix(r.URL.Path, base+"/v2/") {
			c.serveHTTPVersion(w, r, base+"/v2/", 2)
			return
		}
		c.serveHTTP(w, r, base+"/v1/")
	})
	mux.Handle(base, handler)
	mux.Handle(base+"/", handler)
	c.registered = true
	return nil
}

func (c *Continuity) admit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return false
	}
	select {
	case c.slots <- struct{}{}:
		c.active++
		return true
	default:
		return false
	}
}

func (c *Continuity) leave() {
	c.mu.Lock()
	defer c.mu.Unlock()
	<-c.slots
	c.active--
	if c.closing && c.active == 0 {
		close(c.drained)
	}
}

// Close retains the catalog on incomplete drain. A terminal error after all
// owners close is cached and reported, rather than confused with live work.
func (c *Continuity) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	select {
	case c.closeGate <- struct{}{}:
		defer func() { <-c.closeGate }()
	case <-ctx.Done():
		return errors.Join(catalog.ErrDiscoveryDraining, ctx.Err())
	}
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		return err
	}
	if !c.closing {
		c.closing = true
		c.cancel()
		if c.active == 0 {
			close(c.drained)
		}
	}
	c.mu.Unlock()
	serviceErr := c.service.Close(ctx)
	if errors.Is(serviceErr, catalog.ErrDiscoveryDraining) {
		return serviceErr
	}
	select {
	case <-c.drained:
	case <-ctx.Done():
		return errors.Join(catalog.ErrDiscoveryDraining, ctx.Err(), serviceErr)
	}
	err := errors.Join(serviceErr, c.store.Close())
	c.mu.Lock()
	c.closed, c.closeErr = true, err
	c.mu.Unlock()
	return err
}
