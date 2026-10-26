package manager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
)

type ContinuityDeliveryConfig struct {
	RegistryPath string
	Mode         string // loopback_http or https_edge
	EdgeOrigin   string // exact public HTTPS origin in https_edge mode
}

// ContinuityConfig names an existing, locally administered catalog. The
// deployment key protects the legacy host; it is never a discovery identity.
type ContinuityConfig struct {
	CatalogPath     string
	LibraryID       string
	ManagerAdminKey string
	Delivery        ContinuityDeliveryConfig
}

type Continuity struct {
	store    *catalog.Store
	library  continuity.LibraryID
	service  *catalog.DiscoveryService
	delivery *delivery.Service
	// closeDelivery owns exactly one original delivery.Service.Close call.
	closeDelivery func() error
	deliveryMode  string
	deliveryHosts map[string]bool
	deliveryDone  chan struct{}
	deliveryErr   error
	ctx           context.Context
	cancel        context.CancelFunc
	slots         chan struct{}
	heavySlots    chan struct{}
	mu            sync.Mutex
	closeGate     chan struct{}
	registered    bool
	closing       bool
	closed        bool
	active        int
	drained       chan struct{}
	closeErr      error
}

// PrepareContinuity recognizes existing authority only. In particular, an
// imported catalog must be explicitly rebound by local owner administration.
func PrepareContinuity(ctx context.Context, cfg ContinuityConfig) (*Continuity, error) {
	if err := validateContinuityDeliveryConfig(cfg.Delivery); err != nil {
		return nil, err
	}
	if cfg.CatalogPath == "" && cfg.LibraryID == "" {
		if cfg.Delivery.RegistryPath != "" {
			return nil, errors.New("delivery requires an existing continuity catalog")
		}
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
	var deliveryService *delivery.Service
	var closeDelivery func() error
	if cfg.Delivery.RegistryPath != "" {
		if err := store.RequireDeliveryLifecycle(ctx); err != nil {
			return nil, errors.Join(fmt.Errorf("delivery requires schema 8: %w", err), service.Close(ctx), store.Close())
		}
		deliveryService, err = delivery.NewService(ctx, store, library, cfg.Delivery.RegistryPath)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("prepare continuity delivery: %w", err), service.Close(ctx), store.Close())
		}
		closeDelivery = deliveryService.Close
	}
	lifetime, cancel := context.WithCancel(context.Background())
	return &Continuity{
		store: store, library: library, service: service, delivery: deliveryService, closeDelivery: closeDelivery, deliveryMode: cfg.Delivery.Mode, ctx: lifetime, cancel: cancel,
		slots: make(chan struct{}, 16), heavySlots: make(chan struct{}, 2), drained: make(chan struct{}), closeGate: make(chan struct{}, 1),
	}, nil
}

func validateContinuityDeliveryConfig(cfg ContinuityDeliveryConfig) error {
	if cfg.RegistryPath == "" && cfg.Mode == "" && cfg.EdgeOrigin == "" {
		return nil
	}
	if cfg.RegistryPath == "" {
		return errors.New("delivery requires a private registry")
	}
	switch cfg.Mode {
	case "loopback_http":
		if cfg.EdgeOrigin != "" {
			return errors.New("loopback delivery cannot set an edge origin")
		}
	case "https_edge":
		u, err := url.Parse(cfg.EdgeOrigin)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || (u.Path != "" && u.Path != "/manager") || u.RawPath != "" || strings.HasSuffix(u.Hostname(), ".") || len(cfg.EdgeOrigin) > 275 || u.String() != cfg.EdgeOrigin {
			return errors.New("invalid delivery HTTPS edge origin")
		}
	default:
		return errors.New("delivery requires loopback_http or https_edge mode")
	}
	return nil
}

// BindDeliveryListeners freezes the exact, already-bound local authorities.
// The caller invokes this after all listener binds and before any Serve call.
func (c *Continuity) BindDeliveryListeners(listeners ...net.Listener) error {
	if c == nil || c.delivery == nil {
		return nil
	}
	if len(listeners) == 0 {
		return errors.New("delivery requires a bound listener")
	}
	hosts := make(map[string]bool, len(listeners))
	for _, listener := range listeners {
		if listener == nil {
			return errors.New("delivery listener missing")
		}
		addr, ok := listener.Addr().(*net.TCPAddr)
		if !ok || addr.Port < 1 || addr.Zone != "" {
			return errors.New("delivery requires a numeric TCP listener")
		}
		ip, ok := netip.AddrFromSlice(addr.IP)
		if !ok || ip.Is4In6() || !ip.IsLoopback() {
			return errors.New("delivery listener must bind numeric loopback")
		}
		hosts[net.JoinHostPort(ip.String(), fmt.Sprint(addr.Port))] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deliveryHosts != nil || c.closing {
		return errors.New("delivery listener already bound or closing")
	}
	c.deliveryHosts = hosts
	return nil
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
		if strings.HasPrefix(r.URL.Path, base+"/v1/delivery/") {
			c.serveDeliveryHTTP(w, r, base+"/v1/delivery/")
			return
		}
		if strings.HasPrefix(r.URL.Path, base+"/v2/collections/") {
			c.serveContinuityFamilyHTTP(w, r, base+"/v2/collections/", 2, "saved_queries")
			return
		}
		if strings.HasPrefix(r.URL.Path, base+"/v2/recents/") {
			c.serveContinuityFamilyHTTP(w, r, base+"/v2/recents/", 2, "recents")
			return
		}
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

func (c *Continuity) admitDelivery(heavy bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.deliveryHosts == nil {
		return false
	}
	if heavy {
		select {
		case c.heavySlots <- struct{}{}:
		default:
			return false
		}
	} else {
		select {
		case c.slots <- struct{}{}:
		default:
			return false
		}
	}
	c.active++
	return true
}

func (c *Continuity) leaveDelivery(heavy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if heavy {
		<-c.heavySlots
	} else {
		<-c.slots
	}
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
		if c.delivery != nil {
			c.deliveryDone = make(chan struct{})
			go func() {
				c.deliveryErr = c.closeDelivery()
				close(c.deliveryDone)
			}()
		}
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
	if c.deliveryDone != nil {
		select {
		case <-c.deliveryDone:
		case <-ctx.Done():
			return errors.Join(catalog.ErrDiscoveryDraining, ctx.Err(), serviceErr)
		}
	}
	err := errors.Join(serviceErr, c.deliveryErr, c.store.Close())
	c.mu.Lock()
	c.closed, c.closeErr = true, err
	c.mu.Unlock()
	return err
}
