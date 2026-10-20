package catalog

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

type discoveryWindow struct {
	binding discoveryBinding
	request []byte
	created time.Time
	ordinal uint64
	tokens  []string
	bodies  [][]byte
	charge  int64
	leases  int
	retired bool
}
type discoveryPage struct {
	window *discoveryWindow
	index  int
}
type discoveryCollection struct {
	Version      int               `json:"version"`
	Kind         string            `json:"kind"`
	Items        []json.RawMessage `json:"items"`
	MatchedCount int               `json:"matched_count"`
	CountScope   string            `json:"count_scope"`
	Complete     bool              `json:"window_complete"`
	Next         *string           `json:"next_page_token"`
	Sort         string            `json:"sort_version"`
}

func encodeDiscoveryValue(value any, max int) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(b) > max {
		return nil, errDiscoveryWork
	}
	return b, nil
}

func (d *DiscoveryService) retireLocked(w *discoveryWindow) {
	if w.retired {
		return
	}
	w.retired = true
	for _, token := range w.tokens {
		delete(d.tokens, token)
	}
	if w.leases == 0 {
		delete(d.windows, w.ordinal)
		d.bytes -= w.charge
	}
}
func (d *DiscoveryService) releaseLease(w *discoveryWindow) {
	d.mu.Lock()
	w.leases--
	if w.retired && w.leases == 0 {
		delete(d.windows, w.ordinal)
		d.bytes -= w.charge
	}
	d.mu.Unlock()
}
func (d *DiscoveryService) releasePage(w *discoveryWindow) {
	d.mu.Lock()
	d.bytes -= discoveryCopyBytes
	d.mu.Unlock()
	d.releaseLease(w)
}
func sameDiscoveryOwner(a, b discoveryBinding) bool {
	return a.handle.store == b.handle.store && a.handle.selector == b.handle.selector && a.handle.subject == b.handle.subject
}

func (d *DiscoveryService) leasePage(token string, b discoveryBinding) ([]byte, *discoveryWindow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p := d.tokens[token]
	// No expiration sweep, allocation reservation or eviction before ownership.
	if p == nil || !sameDiscoveryOwner(p.window.binding, b) {
		return nil, nil, errDiscoveryRestart
	}
	w := p.window
	if w.retired || time.Since(w.created) >= 5*time.Minute || w.binding.generation != b.generation || w.binding.handle.incarnation != b.handle.incarnation || w.binding.handle.generation != b.handle.generation {
		d.retireLocked(w)
		return nil, nil, errDiscoveryRestart
	}
	if d.bytes+discoveryCopyBytes > discoveryQuota {
		return nil, nil, errDiscoveryBusy
	}
	d.bytes += discoveryCopyBytes
	w.leases++
	return append([]byte(nil), w.bodies[p.index]...), w, nil
}
func (d *DiscoveryService) oldestLocked(subject SubjectID) *discoveryWindow {
	var oldest *discoveryWindow
	for _, w := range d.windows {
		if w.retired || w.leases != 0 || subject != "" && w.binding.handle.subject != subject {
			continue
		}
		if oldest == nil || w.ordinal < oldest.ordinal {
			oldest = w
		}
	}
	return oldest
}
func (d *DiscoveryService) reserveNew(b discoveryBinding) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, w := range d.windows {
		if time.Since(w.created) >= 5*time.Minute {
			d.retireLocked(w)
		}
	}
	subject := b.handle.subject
	count := d.building[subject]
	for _, w := range d.windows {
		if !w.retired && w.binding.handle.subject == subject {
			count++
		}
	}
	for count >= 16 {
		w := d.oldestLocked(subject)
		if w == nil {
			return errDiscoveryBusy
		}
		d.retireLocked(w)
		count--
	}
	for d.bytes+discoveryNewBytes > discoveryQuota {
		w := d.oldestLocked("")
		if w == nil {
			return errDiscoveryBusy
		}
		d.retireLocked(w)
	}
	d.bytes += discoveryNewBytes
	d.building[subject]++
	return nil
}

func (d *DiscoveryService) makeWindow(request DiscoveryRequest, b discoveryBinding, items []json.RawMessage, complete bool) (*discoveryWindow, error) {
	// Group by page-size AND a conservative envelope allowance. Every item was
	// separately bounded to64KiB before its construction reservation is consumed.
	var groups [][]json.RawMessage
	for start := 0; start < len(items); {
		end := start
		size := 1024
		for end < len(items) && end-start < request.PageSize && size+len(items[end])+1 <= 1<<20 {
			size += len(items[end]) + 1
			end++
		}
		if end == start {
			return nil, errDiscoveryWork
		}
		groups = append(groups, items[start:end])
		start = end
	}
	if len(groups) == 0 {
		groups = append(groups, make([]json.RawMessage, 0))
	}
	w := &discoveryWindow{binding: b, created: time.Now(), tokens: make([]string, len(groups))}
	used := make(map[string]bool)
	for i := range groups {
		for attempt := 0; attempt < 8; attempt++ {
			var raw [32]byte
			if _, err := rand.Read(raw[:]); err != nil {
				return nil, err
			}
			token := "page1_" + hex.EncodeToString(raw[:])
			d.mu.Lock()
			exists := d.tokens[token] != nil
			d.mu.Unlock()
			if !exists && !used[token] {
				w.tokens[i] = token
				used[token] = true
				break
			}
		}
		if w.tokens[i] == "" {
			return nil, errDiscoveryBusy
		}
	}
	normalized, _ := json.Marshal(request)
	w.request = normalized
	w.charge = 4096 + int64(cap(normalized)) + 256*int64(len(items)) + 256*int64(len(groups))
	var rawBytes int64
	for _, item := range items {
		rawBytes += int64(cap(item))
	}
	for i, group := range groups {
		var next *string
		if i+1 < len(groups) {
			next = &w.tokens[i+1]
		}
		estimate := int64(1024)
		for _, item := range group {
			estimate += int64(len(item)) + 1
		}
		if rawBytes+w.charge+2*estimate > discoveryBuildBytes {
			return nil, errDiscoveryWork
		}
		body, err := encodeDiscoveryValue(discoveryCollection{1, request.Kind, group, len(items), "frozen_window", complete, next, "discovery1"}, 1<<20)
		if err != nil {
			return nil, err
		}
		w.bodies = append(w.bodies, body)
		w.charge += int64(cap(body))
		if rawBytes+w.charge > discoveryBuildBytes {
			return nil, errDiscoveryWork
		}
	}
	d.mu.Lock()
	d.ordinal++
	w.ordinal = d.ordinal
	d.mu.Unlock()
	return w, nil
}
