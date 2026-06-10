// Package health — network interface change detection.
//
// NetWatcher polls the system's network interfaces every second and fires a
// callback whenever the "active" interface changes (e.g. WiFi → 10GbE →
// Tailscale). "Active" is determined by scanning non-loopback, up interfaces
// that have at least one unicast IP address.
package health

import (
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

	mu           sync.RWMutex
	activeIface  string
	lastChangeAt time.Time
	changeCount  int

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewNetWatcher creates a network watcher that polls every pollInterval.
func NewNetWatcher(pollInterval time.Duration) *NetWatcher {
	return &NetWatcher{
		pollInterval: pollInterval,
		stopCh:       make(chan struct{}),
	}
}

// OnChange registers a callback for network interface changes.
// Must be called before Start().
func (w *NetWatcher) OnChange(cb NetChangeCallback) {
	w.callbacks = append(w.callbacks, cb)
}

// Start begins polling for network changes in a background goroutine.
func (w *NetWatcher) Start() {
	// Capture initial state
	iface := detectActiveInterface()
	w.mu.Lock()
	w.activeIface = iface
	w.mu.Unlock()
	log.Printf("[netwatch] initial interface: %s", iface)

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
	current := detectActiveInterface()

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
