// Package health — network interface change detection.
//
// NetWatcher polls the system's network interfaces every second and fires a
// callback whenever the "active" interface changes (e.g. WiFi → 10GbE →
// Tailscale). "Active" is determined by scanning non-loopback, up interfaces
// that have at least one unicast IP address.
//
// G8 (task #81): when constructed WithBackendTarget, "active" instead means
// "the interface the kernel ROUTES TO THE BACKEND", not the default-route
// interface. Proven live 2026-07-02: on iPhone-hotspot + Tailscale the NAS
// route was utun6 but the default-route heuristic reported en0 (WiFi), so
// every class-gated consumer (G7 scan budget, G6 tunnel deferral, backstop
// cadence, coalescer tuning) ran with WiFi timings on a metered tunnel — the
// SCAN could never finish inside the 120s WiFi budget and the retry loop
// burned the metered link. Classification must follow the path the bytes
// actually take.
package health

import (
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// NetChangeCallback is invoked when the active network interface changes.
// oldIface/newIface are the interface names (e.g. "en0", "en7", "utun4").
type NetChangeCallback func(oldIface, newIface string)

// NetWatcher monitors for network interface changes and fires callbacks.
type NetWatcher struct {
	pollInterval time.Duration
	callbacks    []NetChangeCallback

	// G8 backend-route classification (see WithBackendTarget). Empty
	// targetAddr keeps the historical default-route behavior exactly.
	targetAddr    string
	routeResolver func(target string) (string, error) // injectable for tests
	routeCacheFor time.Duration

	mu           sync.RWMutex
	activeIface  string
	lastChangeAt time.Time
	changeCount  int
	// Backend-route resolution cache. Resolution is cheap (a connected UDP
	// socket — no packet, no exec) but may involve DNS when the Redis URL
	// carries a hostname, so it is NOT run on every 1s poll: the result
	// (or failure — routeIface=="") is reused for routeCacheFor. Route
	// changes are therefore detected within ~routeCacheFor, which is fine —
	// interface/route changes are rare events.
	routeIface     string
	routeCheckedAt time.Time
	routeFailing   bool // last resolve failed (for transition-only logging)

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NetWatcherOption customizes a NetWatcher at construction.
type NetWatcherOption func(*NetWatcher)

// WithBackendTarget switches the watcher from default-route detection to
// BACKEND-ROUTE detection: ActiveInterface reports the interface the kernel
// routes to target ("host:port", e.g. the Redis addr from
// metadata.ParseRedisURL), so link classification follows the path traffic
// to the backend actually takes (G8, task #81 — a Tailscale'd NAS is utun6
// even while the default route is en0).
//
// Chosen resolver: a connected UDP socket (net.Dial "udp") — connect(2) on a
// datagram socket sends NO packet; the kernel just consults its routing
// table, picks the route, and binds the local source IP. LocalAddr then maps
// back to the owning interface via net.Interfaces. No exec, no privileges,
// no probe traffic on a possibly-metered link. Verified by reasoning for
// both bands: a LAN backend (10.x NAS over en21) binds en21's address; a
// Tailscale backend (100.64/10, or a subnet-router route) binds the Mac's
// OWN utunN address (the utun has its own local IP), mapping to utunN.
// Live-validated on the hotspot+Tailscale rig 2026-07-02.
//
// Failure modes (all degrade to the historical default-route behavior, never
// worse): DNS-named backend with DNS down → bounded 2s dial timeout, failure
// cached routeCacheFor so a dead resolver costs one bounded dial per window;
// fully offline (no route) → connect fails → fallback; loopback backend (dev
// localhost Redis) → resolves lo0, which downstream class-maps to the
// conservative WiFi band; link-local IPv6 zone mismatch → no interface match
// → fallback. Note the connected socket proves ROUTING only, not
// reachability — liveness stays with health.Reachability.
func WithBackendTarget(target string) NetWatcherOption {
	return func(w *NetWatcher) {
		w.targetAddr = target
	}
}

// NewNetWatcher creates a network watcher that polls every pollInterval.
func NewNetWatcher(pollInterval time.Duration, opts ...NetWatcherOption) *NetWatcher {
	w := &NetWatcher{
		pollInterval:  pollInterval,
		routeResolver: resolveRouteInterface,
		routeCacheFor: 10 * time.Second,
		stopCh:        make(chan struct{}),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// OnChange registers a callback for network interface changes.
// Must be called before Start().
func (w *NetWatcher) OnChange(cb NetChangeCallback) {
	w.callbacks = append(w.callbacks, cb)
}

// Start begins polling for network changes in a background goroutine.
func (w *NetWatcher) Start() {
	// Capture initial state
	iface := w.detect()
	w.mu.Lock()
	w.activeIface = iface
	w.mu.Unlock()
	if w.targetAddr != "" {
		log.Printf("[netwatch] initial interface: %s (backend-route mode, target %s)", iface, w.targetAddr)
	} else {
		log.Printf("[netwatch] initial interface: %s", iface)
	}

	go w.pollLoop()
}

// Stop halts the network watcher.
func (w *NetWatcher) Stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

// ActiveInterface returns the currently detected active interface name.
func (w *NetWatcher) ActiveInterface() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.activeIface
}

// LastChangeAt returns when the last network change was detected.
func (w *NetWatcher) LastChangeAt() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastChangeAt
}

// ChangeCount returns how many network changes have been detected.
func (w *NetWatcher) ChangeCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.changeCount
}

// InGracePeriod returns true if a network change happened within the given duration.
func (w *NetWatcher) InGracePeriod(d time.Duration) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.lastChangeAt.IsZero() {
		return false
	}
	return time.Since(w.lastChangeAt) < d
}

func (w *NetWatcher) pollLoop() {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.check()
		}
	}
}

func (w *NetWatcher) check() {
	current := w.detect()

	w.mu.RLock()
	prev := w.activeIface
	w.mu.RUnlock()

	if current != prev && current != "" {
		w.mu.Lock()
		w.activeIface = current
		w.lastChangeAt = time.Now()
		w.changeCount++
		w.mu.Unlock()

		log.Printf("[netwatch] network change detected: %s → %s", prev, current)

		for _, cb := range w.callbacks {
			cb(prev, current)
		}
	}
}

// detect returns the interface name to report as "active". Backend-route
// mode (targetAddr set) resolves the interface the kernel routes to the
// backend, with the resolution cached for routeCacheFor (success AND failure
// — a dead resolver must not add a bounded-but-real dial to every 1s poll).
// Any resolver failure falls back to the historical default-route detection
// for that window, so behavior is never worse than pre-G8.
func (w *NetWatcher) detect() string {
	if w.targetAddr == "" {
		return detectActiveInterface()
	}

	now := time.Now()
	w.mu.RLock()
	cached, checkedAt := w.routeIface, w.routeCheckedAt
	w.mu.RUnlock()

	if !checkedAt.IsZero() && now.Sub(checkedAt) < w.routeCacheFor {
		if cached != "" {
			return cached
		}
		return detectActiveInterface() // cached failure → default-route fallback
	}

	iface, err := w.routeResolver(w.targetAddr)
	if err != nil || iface == "" {
		w.mu.Lock()
		w.routeIface = ""
		w.routeCheckedAt = now
		logTransition := !w.routeFailing
		w.routeFailing = true
		w.mu.Unlock()
		if logTransition {
			log.Printf("[netwatch] backend-route resolve failed for %s (falling back to default-route detection): %v",
				w.targetAddr, err)
		}
		return detectActiveInterface()
	}

	w.mu.Lock()
	w.routeIface = iface
	w.routeCheckedAt = now
	logRecovery := w.routeFailing
	w.routeFailing = false
	w.mu.Unlock()
	if logRecovery {
		log.Printf("[netwatch] backend-route resolve recovered for %s: %s", w.targetAddr, iface)
	}
	return iface
}

// resolveRouteInterface returns the name of the interface the kernel would
// use to reach target ("host:port"). Implementation: connect(2) a UDP socket
// to the target — this transmits NOTHING; it only performs the kernel
// routing-table lookup and binds the local source IP the chosen route uses.
// That local IP is then mapped back to its owning interface. Cheap,
// unprivileged, and probe-free (see WithBackendTarget for the full
// rationale + failure modes). The 2s dial timeout bounds a DNS lookup when
// the target is a hostname; for a literal IP the connect is purely local
// and effectively instant.
func resolveRouteInterface(target string) (string, error) {
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.Dial("udp", target)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	la, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || la == nil || la.IP == nil || la.IP.IsUnspecified() {
		return "", fmt.Errorf("no usable local address for route to %s", target)
	}
	return interfaceForIP(la.IP)
}

// interfaceForIP returns the name of the first up interface that owns ip.
// This is the local-IP → interface half of the backend-route resolver: the
// kernel told us which SOURCE address the route to the backend binds; the
// interface holding that address is the one carrying backend traffic (a
// Tailscale utunN owns the Mac's own 100.64/10 address, so a tunneled
// backend maps to utunN — live-validated 2026-07-02; loopback and en* are
// covered by unit tests, utun cannot be fabricated in CI).
func interfaceForIP(ip net.IP) (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var candidate net.IP
			switch a := addr.(type) {
			case *net.IPNet:
				candidate = a.IP
			case *net.IPAddr:
				candidate = a.IP
			}
			if candidate != nil && candidate.Equal(ip) {
				return iface.Name, nil
			}
		}
	}
	return "", fmt.Errorf("no up interface owns local IP %s", ip)
}

// detectActiveInterface returns the name of the "best" active non-loopback
// network interface. Priority order:
//  1. Ethernet-like interfaces (en1-en9 on macOS, eth* on Linux) — typically 10GbE
//  2. en0 (built-in WiFi on macOS)
//  3. Tailscale (utun* on macOS, tailscale0 on Linux)
//  4. Any other interface with an IP
func detectActiveInterface() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	type candidate struct {
		name     string
		priority int
	}
	var candidates []candidate

	for _, iface := range ifaces {
		// Skip down, loopback, or pointopoint-only interfaces
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		// Must have at least one unicast address
		addrs, err := iface.Addrs()
		if err != nil || len(addrs) == 0 {
			continue
		}

		// Check for non-link-local addresses
		hasRoutable := false
		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				continue
			}
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
				continue
			}
			hasRoutable = true
			break
		}
		if !hasRoutable {
			continue
		}

		name := iface.Name
		pri := 100 // default

		switch {
		// High-priority: Ethernet interfaces (not en0 which is WiFi on macOS)
		case (strings.HasPrefix(name, "en") && name != "en0") ||
			strings.HasPrefix(name, "eth"):
			pri = 10
		// Medium: WiFi (en0 on macOS)
		case name == "en0":
			pri = 20
		// Tailscale
		case strings.HasPrefix(name, "utun") || name == "tailscale0":
			pri = 30
		// Bridge interfaces
		case strings.HasPrefix(name, "bridge"):
			pri = 50
		}

		candidates = append(candidates, candidate{name: name, priority: pri})
	}

	if len(candidates) == 0 {
		return ""
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].priority < candidates[j].priority
	})

	return candidates[0].name
}
