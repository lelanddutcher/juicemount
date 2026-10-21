package catalog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

const collectionReadFamily = "collections1"

type collectionReadPage struct {
	start, end        int
	body              []byte
	assets            []collectionAssetBinding
	building, evicted bool
	leases            int
}
type collectionReadWindow struct {
	binding   collectionReadBinding
	summary   CollectionSummary
	members   []collectionMemberIdentity
	pages     []*collectionReadPage
	count     int
	complete  bool
	bodyBytes int64
}

func (d *DiscoveryService) collectionToken() (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		var raw [32]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return "", err
		}
		token := "page1_" + hex.EncodeToString(raw[:])
		d.mu.Lock()
		exists := d.tokens[token] != nil
		d.mu.Unlock()
		if !exists {
			return token, nil
		}
	}
	return "", errDiscoveryBusy
}
func collectionEnvelope(c *collectionReadWindow, items []json.RawMessage, next *string) any {
	if items == nil {
		items = make([]json.RawMessage, 0)
	}
	r := c.binding.request
	switch r.Kind {
	case "directory":
		return struct {
			Version      int                `json:"version"`
			Kind         string             `json:"kind"`
			Collection   *CollectionSummary `json:"collection"`
			CanCreate    bool               `json:"can_create"`
			Items        []json.RawMessage  `json:"items"`
			MatchedCount int                `json:"matched_count"`
			CountScope   string             `json:"count_scope"`
			Complete     bool               `json:"window_complete"`
			Next         *string            `json:"next_page_token"`
			Sort         string             `json:"sort_version"`
			Snapshot     string             `json:"snapshot_scope"`
		}{1, "directory", nil, c.binding.canCreate, items, c.count, "frozen_window", c.complete, next, "collections1", "frozen_snapshot"}
	case "members":
		// Raw items are already typed/validated; the common internal envelope below
		// avoids re-decoding full labels just to serialize this same bounded page.
		return struct {
			Version      int               `json:"version"`
			Kind         string            `json:"kind"`
			Collection   CollectionSummary `json:"collection"`
			State        string            `json:"state"`
			Items        []json.RawMessage `json:"items"`
			MatchedCount int               `json:"matched_count"`
			CountScope   string            `json:"count_scope"`
			Complete     bool              `json:"window_complete"`
			Next         *string           `json:"next_page_token"`
			Sort         string            `json:"sort_version"`
			Snapshot     string            `json:"snapshot_scope"`
		}{1, "members", c.summary, r.State, items, c.count, "frozen_visible_membership", true, next, "collections1", "frozen_membership_current_page"}
	default:
		return CollectionListResponse[json.RawMessage]{1, r.Kind, c.summary, items, c.count, "frozen_window", c.complete, next, "collections1", "frozen_snapshot"}
	}
}
func encodeCollectionPage(c *collectionReadWindow, items []json.RawMessage, next *string) ([]byte, error) {
	// Check the real empty envelope too; the4KiB estimate is never the only bound.
	envelope, err := encodeDiscoveryValue(collectionEnvelope(c, make([]json.RawMessage, 0), next), collectionEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	_ = envelope
	return encodeDiscoveryValue(collectionEnvelope(c, items, next), 1<<20)
}
func collectionWindowCharge(w *discoveryWindow) int64 {
	c := w.collection
	charge := int64(16384) + 128*int64(cap(c.members)) + 384*int64(cap(c.pages)) + 128*int64(cap(c.binding.directory))
	for _, p := range c.pages {
		charge += int64(cap(p.body)) + 128*int64(cap(p.assets))
	}
	return charge
}
func (d *DiscoveryService) makeCollectionWindow(b collectionReadBinding, summary CollectionSummary, ids []collectionMemberIdentity, items []json.RawMessage, assets []collectionAssetBinding, end int, complete bool) (*discoveryWindow, error) {
	c := &collectionReadWindow{binding: b, summary: summary, members: ids, count: len(items), complete: complete}
	w := &discoveryWindow{binding: cloneDiscoveryBinding(b.auth), version: 1, family: collectionReadFamily, created: time.Now(), collection: c}
	if b.request.Kind == "members" {
		c.count = len(ids)
		c.complete = true
		c.pages = make([]*collectionReadPage, 0, len(ids)+1)
		w.tokens = make([]string, 0, len(ids)+1)
		p := &collectionReadPage{start: 0, end: end, assets: assets}
		c.pages = append(c.pages, p)
		token, err := d.collectionToken()
		if err != nil {
			return nil, err
		}
		w.tokens = append(w.tokens, token)
		var next *string
		if end < len(ids) {
			token, err := d.collectionToken()
			if err != nil {
				return nil, err
			}
			if token == w.tokens[0] {
				return nil, errDiscoveryBusy
			}
			w.tokens = append(w.tokens, token)
			c.pages = append(c.pages, &collectionReadPage{start: end})
			next = &w.tokens[1]
		}
		body, err := encodeCollectionPage(c, items, next)
		if err != nil {
			return nil, err
		}
		p.body = body
		c.bodyBytes = int64(cap(body))
	} else {
		groups := make([][]json.RawMessage, 0)
		for start := 0; start < len(items); {
			end := start
			size := collectionEnvelopeBytes
			for end < len(items) && end-start < b.request.PageSize && size+len(items[end])+1 <= 1<<20 {
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
		c.pages = make([]*collectionReadPage, len(groups))
		w.tokens = make([]string, len(groups))
		used := make(map[string]bool, len(groups))
		for i := range groups {
			token, err := d.collectionToken()
			if err != nil {
				return nil, err
			}
			if used[token] {
				return nil, errDiscoveryBusy
			}
			used[token] = true
			w.tokens[i] = token
		}
		var rawCharge int64
		for _, item := range items {
			rawCharge += int64(cap(item))
		}
		for i, group := range groups {
			if rawCharge+16384+256*int64(cap(items))+384*int64(len(groups))+c.bodyBytes+2*discoveryCopyBytes > discoveryBuildBytes {
				return nil, errDiscoveryWork
			}
			var next *string
			if i+1 < len(groups) {
				next = &w.tokens[i+1]
			}
			body, err := encodeCollectionPage(c, group, next)
			if err != nil {
				return nil, err
			}
			c.pages[i] = &collectionReadPage{body: body}
			c.bodyBytes += int64(cap(body))
			if rawCharge+16384+256*int64(cap(items))+384*int64(len(groups))+c.bodyBytes+2*int64(cap(body)) > discoveryBuildBytes {
				return nil, errDiscoveryWork
			}
		}
	}
	w.charge = collectionWindowCharge(w)
	if w.charge > discoveryBuildBytes {
		return nil, errDiscoveryWork
	}
	return w, nil
}

// The caller still owns a cold21MiB reservation. Publication transfers only the
// actual retained window charge and pins its first body until disclosure ends.
func (d *DiscoveryService) publishCollectionWindow(ctx context.Context, w *discoveryWindow) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, token := range w.tokens {
		if d.tokens[token] != nil {
			return errDiscoveryBusy
		}
	}
	d.ordinal++
	w.ordinal = d.ordinal
	w.leases = 1
	w.collection.pages[0].leases = 1
	d.bytes += w.charge - discoveryBuildBytes
	d.windows[w.ordinal] = w
	for i, token := range w.tokens {
		d.tokens[token] = &discoveryPage{w, i}
	}
	d.releaseBuildingLocked(w.binding.handle.subject)
	return nil
}
func (d *DiscoveryService) releaseCollectionBody(w *discoveryWindow, p *collectionReadPage, cost int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bytes -= cost
	if p != nil {
		p.leases--
	}
	w.leases--
	if w.retired && w.leases == 0 {
		delete(d.windows, w.ordinal)
		d.bytes -= w.charge
	}
}
func (d *DiscoveryService) retireCollectionWindow(w *discoveryWindow) {
	if w != nil {
		d.mu.Lock()
		d.retireLocked(w)
		d.mu.Unlock()
	}
}

// Ownership/family/version precede ALL expiry sweeps, eviction or reservation.
// An unmaterialized page pins its window BEFORE reserving construction space.
func (d *DiscoveryService) beginCollectionPage(ctx context.Context, token string, b discoveryBinding) (*discoveryWindow, *collectionReadPage, int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry := d.tokens[token]
	if entry == nil || entry.window.family != collectionReadFamily || entry.window.version != 1 || !sameDiscoveryOwner(entry.window.binding, b) {
		return nil, nil, 0, errDiscoveryRestart
	}
	w := entry.window
	p := w.collection.pages[entry.index]
	if w.retired || time.Since(w.created) >= 5*time.Minute || w.binding.generation != b.generation || w.binding.handle.incarnation != b.handle.incarnation || w.binding.handle.generation != b.handle.generation {
		d.retireLocked(w)
		return nil, nil, 0, errDiscoveryRestart
	}
	if p.evicted {
		return nil, nil, 0, errDiscoveryRestart
	}
	if d.closing {
		return nil, nil, 0, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, 0, err
	}
	if p.building {
		return nil, nil, 0, errDiscoveryBusy
	}
	w.leases++
	cost := int64(collectionReplayBytes)
	if p.body == nil {
		cost = discoveryNewBytes
		p.building = true
		for d.bytes+cost > discoveryQuota {
			old := d.oldestLocked("")
			if old == nil {
				p.building = false
				w.leases--
				return nil, nil, 0, errDiscoveryBusy
			}
			d.retireLocked(old)
		}
	} else if d.bytes+cost > discoveryQuota {
		w.leases--
		return nil, nil, 0, errDiscoveryBusy
	}
	d.bytes += cost
	if p.body != nil {
		p.leases++
	}
	return w, p, cost, nil
}
func (d *DiscoveryService) abandonCollectionPage(w *discoveryWindow, p *collectionReadPage, cost int64) {
	d.mu.Lock()
	p.building = false
	d.mu.Unlock()
	d.releaseCollectionBody(w, nil, cost)
}

// Only unleased member bodies can be discarded. Retained token tombstones make
// an old body permanently unrebuildable without destroying forward traversal.
func (d *DiscoveryService) publishCollectionPage(ctx context.Context, w *discoveryWindow, p *collectionReadPage, body []byte, assets []collectionAssetBinding, end int, nextToken string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := w.collection
	if d.closing {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.retired || time.Since(w.created) >= 5*time.Minute || !p.building || p.body != nil || p.evicted {
		return errDiscoveryRestart
	}
	if nextToken != "" && d.tokens[nextToken] != nil {
		return errDiscoveryBusy
	}
	bodyCost := int64(cap(body))
	if bodyCost > 2<<20 {
		return errDiscoveryWork
	}
	for c.bodyBytes+bodyCost > 2<<20 {
		var victim *collectionReadPage
		for _, candidate := range c.pages {
			if candidate != p && candidate.body != nil && candidate.leases == 0 && !candidate.building {
				victim = candidate
				break
			}
		}
		if victim == nil {
			return errDiscoveryBusy
		}
		freed := int64(cap(victim.body)) + 128*int64(cap(victim.assets))
		c.bodyBytes -= int64(cap(victim.body))
		victim.body = nil
		victim.assets = nil
		victim.evicted = true
		w.charge -= freed
		d.bytes -= freed
	}
	extra := bodyCost + 128*int64(cap(assets))
	if w.charge+extra > discoveryBuildBytes {
		return errDiscoveryWork
	}
	if end < len(c.members) {
		if nextToken == "" || len(c.pages) >= cap(c.pages) {
			return errDiscoveryWork
		}
		index := len(c.pages)
		c.pages = append(c.pages, &collectionReadPage{start: end})
		w.tokens = append(w.tokens, nextToken)
		d.tokens[nextToken] = &discoveryPage{w, index}
	}
	p.end = end
	p.body = body
	p.assets = assets
	p.building = false
	p.leases = 1
	c.bodyBytes += bodyCost
	w.charge += extra
	d.bytes += extra - discoveryBuildBytes
	return nil
}
