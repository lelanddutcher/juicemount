package catalog

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

const readPlanningToken = "page1_0000000000000000000000000000000000000000000000000000000000000000"
const recentsActiveBytes = discoveryWorkBytes + 2*discoveryCopyBytes
const savedReadActiveBytes = discoveryWorkBytes + discoveryCopyBytes

func savedReadEnvelope(w *savedQueryReadWindow, count int, complete bool, next *string) any {
	r := w.binding.request
	items := make([]json.RawMessage, 0)
	if r.Kind == "directory" {
		return struct {
			Version      int                `json:"version"`
			Kind         string             `json:"kind"`
			Collection   *SavedQuerySummary `json:"collection"`
			CanCreate    bool               `json:"can_create"`
			Items        []json.RawMessage  `json:"items"`
			MatchedCount int                `json:"matched_count"`
			CountScope   string             `json:"count_scope"`
			Complete     bool               `json:"window_complete"`
			Next         *string            `json:"next_page_token"`
			Sort         string             `json:"sort_version"`
			Snapshot     string             `json:"snapshot_scope"`
		}{2, "directory", nil, w.binding.canCreate, items, count, "frozen_window", complete, next, "saved_queries2", "frozen_snapshot"}
	}
	if r.Kind == "execute" {
		return struct {
			Version      int               `json:"version"`
			Kind         string            `json:"kind"`
			Collection   SavedQuerySummary `json:"collection"`
			QueryVersion CollectionVersion `json:"query_version"`
			Items        []json.RawMessage `json:"items"`
			MatchedCount int               `json:"matched_count"`
			CountScope   string            `json:"count_scope"`
			Complete     bool              `json:"window_complete"`
			Next         *string           `json:"next_page_token"`
			Sort         string            `json:"sort_version"`
			Snapshot     string            `json:"snapshot_scope"`
		}{2, "execute", w.summary, w.summary.DefinitionVersion, items, count, "frozen_window", complete, next, "discovery2", "frozen_snapshot"}
	}
	return SavedQueryListResponse[json.RawMessage]{2, r.Kind, w.summary, items, count, "frozen_window", complete, next, "saved_queries2", "frozen_snapshot"}
}

// Only the small empty envelope uses encoding/json. Items are already canonical
// and copied into one exact-sized destination; no full-page encoder corpus.
func readEnvelope(v any) ([]byte, int, error) {
	b, err := encodeDiscoveryValue(v, 4096)
	if err != nil {
		return nil, 0, err
	}
	i := bytes.Index(b, []byte(`"items":[]`))
	if i < 0 {
		return nil, 0, corrupt("read envelope items")
	}
	return b, i + len(`"items":[`), nil
}
func assembleReadPage(envelope []byte, split int, items []json.RawMessage) ([]byte, error) {
	size := len(envelope)
	for _, item := range items {
		size += len(item)
	}
	if len(items) > 0 {
		size += len(items) - 1
	}
	if size > 1<<20 {
		return nil, errDiscoveryWork
	}
	b := make([]byte, size)
	at := copy(b, envelope[:split])
	for i, item := range items {
		if i != 0 {
			b[at] = ','
			at++
		}
		at += copy(b[at:], item)
	}
	at += copy(b[at:], envelope[split:])
	if at != size {
		return nil, corrupt("read page length")
	}
	return b, nil
}

type savedReadPlanner struct {
	base, raw, bodies                              int64
	envelope, lastSize, lastCount, pages, pageSize int
}

func newSavedReadPlanner(w *savedQueryReadWindow, count int, complete bool, requestBytes, capacity int) (savedReadPlanner, error) {
	token := readPlanningToken
	envelope, _, err := readEnvelope(savedReadEnvelope(w, count, complete, &token))
	return savedReadPlanner{base: 16384 + int64(cap(w.definition)+requestBytes) + 256*int64(capacity), envelope: len(envelope), pageSize: w.binding.request.PageSize}, err
}
func (p *savedReadPlanner) add(length, capacity int) bool {
	if length > 64<<10 || length+p.envelope > 1<<20 {
		return false
	}
	if p.pages == 0 || p.lastCount == p.pageSize || p.lastSize+1+length > 1<<20 {
		p.pages++
		p.lastSize = p.envelope
		p.lastCount = 0
		p.bodies += int64(p.envelope)
	}
	comma := 0
	if p.lastCount > 0 {
		comma = 1
	}
	p.lastSize += length + comma
	p.bodies += int64(length + comma)
	p.lastCount++
	p.raw += int64(capacity)
	return p.cost() <= discoveryBuildBytes
}
func (p savedReadPlanner) cost() int64 {
	return p.base + p.raw + p.bodies + 384*int64(p.pages) + 2*discoveryCopyBytes
}

func readRanges(lengths []int, pageSize, envelope int) ([][2]int, error) {
	count := 0
	for start := 0; start < len(lengths); {
		end, size := start, envelope
		for end < len(lengths) && end-start < pageSize {
			n := lengths[end]
			if end > start {
				n++
			}
			if size+n > 1<<20 {
				break
			}
			size += n
			end++
		}
		if end == start {
			return nil, errDiscoveryWork
		}
		count++
		start = end
	}
	if count == 0 {
		return make([][2]int, 1), nil
	}
	ranges := make([][2]int, count)
	for i, start := 0, 0; i < count; i++ {
		end, size := start, envelope
		for end < len(lengths) && end-start < pageSize {
			n := lengths[end]
			if end > start {
				n++
			}
			if size+n > 1<<20 {
				break
			}
			size += n
			end++
		}
		ranges[i] = [2]int{start, end}
		start = end
	}
	return ranges, nil
}
func (d *DiscoveryService) readTokens(pages int) ([]string, error) {
	return d.readTokensWithDraw(pages, rand.Read)
}

// The draw callback is private and call-owned; production always supplies
// crypto/rand.Read. One total attempt bound covers global and local collisions.
func (d *DiscoveryService) readTokensWithDraw(pages int, draw func([]byte) (int, error)) ([]string, error) {
	tokens := make([]string, pages-1)
	used := make(map[string]bool, pages-1)
	for i := range tokens {
		for attempt := 0; attempt < 8; attempt++ {
			var raw [32]byte
			if _, err := draw(raw[:]); err != nil {
				return nil, err
			}
			token := "page1_" + hex.EncodeToString(raw[:])
			d.mu.Lock()
			exists := d.tokens[token] != nil
			d.mu.Unlock()
			if !exists && !used[token] {
				tokens[i] = token
				used[token] = true
				break
			}
		}
		if tokens[i] == "" {
			return nil, errDiscoveryBusy
		}
	}
	return tokens, nil
}
func (d *DiscoveryService) makeSavedReadWindow(w *savedQueryReadWindow, request []byte) (*discoveryWindow, error) {
	lengths := make([]int, len(w.items))
	for i, item := range w.items {
		lengths[i] = len(item)
	}
	token := readPlanningToken
	envelope, _, err := readEnvelope(savedReadEnvelope(w, len(w.items), w.complete, &token))
	if err != nil {
		return nil, err
	}
	ranges, err := readRanges(lengths, w.binding.request.PageSize, len(envelope))
	if err != nil {
		return nil, err
	}
	tokens, err := d.readTokens(len(ranges))
	if err != nil {
		return nil, err
	}
	out := &discoveryWindow{family: savedQueryReadFamily, saved: w, binding: cloneDiscoveryBinding(w.binding.auth), version: 2, request: request, created: time.Now(), tokens: tokens, bodies: make([][]byte, len(ranges))}
	charge := int64(16384+cap(request)+cap(w.definition)) + 256*int64(cap(w.items)) + 384*int64(len(ranges))
	for _, item := range w.items {
		charge += int64(cap(item))
	}
	for i, r := range ranges {
		var next *string
		if i < len(tokens) {
			next = &tokens[i]
		}
		envelope, split, err := readEnvelope(savedReadEnvelope(w, len(w.items), w.complete, next))
		if err != nil {
			return nil, err
		}
		body, err := assembleReadPage(envelope, split, w.items[r[0]:r[1]])
		if err != nil {
			return nil, err
		}
		out.bodies[i] = body
		charge += int64(cap(body))
		if charge+2*discoveryCopyBytes > discoveryBuildBytes {
			return nil, errDiscoveryWork
		}
	}
	out.charge = charge
	return out, nil
}

// Owner/family/query checks precede all expiry, retirement and reservations.
func (d *DiscoveryService) beginSavedReadPage(ctx context.Context, r SavedQueryReadRequest, family string, auth discoveryBinding) (*discoveryWindow, int, int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p := d.tokens[r.Token]
	if p == nil || p.window.family != family || !sameDiscoveryOwner(p.window.binding, auth) {
		return nil, 0, 0, errDiscoveryRestart
	}
	w := p.window
	var initial SavedQueryReadRequest
	if family == savedQueryReadFamily {
		initial = w.saved.binding.request
	} else {
		initial = w.recent.binding.request
	}
	if initial.Kind != r.QueryKind || initial.CollectionID != r.CollectionID {
		return nil, 0, 0, errDiscoveryRestart
	}
	if w.retired || time.Since(w.created) >= 5*time.Minute || w.binding.handle.generation != auth.handle.generation || w.binding.handle.incarnation != auth.handle.incarnation {
		d.retireLocked(w)
		return nil, 0, 0, errDiscoveryRestart
	}
	if d.closing {
		return nil, 0, 0, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, 0, err
	}
	reservation := savedReadActiveBytes
	if family == recentsReadFamily {
		reservation = recentsActiveBytes
	}
	if d.bytes+reservation > discoveryQuota {
		return nil, 0, 0, errDiscoveryBusy
	}
	d.bytes += reservation
	w.leases++
	return w, p.index, reservation, nil
}
func (d *DiscoveryService) publishSavedReadWindow(ctx context.Context, w *discoveryWindow) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return 0, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	for _, token := range w.tokens {
		if d.tokens[token] != nil {
			return 0, errDiscoveryBusy
		}
	}
	active := savedReadActiveBytes
	if w.family == recentsReadFamily {
		active = recentsActiveBytes
	}
	if d.bytes-discoveryNewBytes+w.charge+active > discoveryQuota {
		return 0, errDiscoveryBusy
	}
	d.bytes += w.charge + active - discoveryNewBytes
	d.releaseBuildingLocked(w.binding.handle.subject)
	d.ordinal++
	w.ordinal = d.ordinal
	w.leases = 1
	d.windows[w.ordinal] = w
	for i, token := range w.tokens {
		d.tokens[token] = &discoveryPage{w, i + 1}
	}
	return active, nil
}

func (d *DiscoveryService) retireSavedReadWindow(w *discoveryWindow) {
	if w != nil {
		d.mu.Lock()
		d.retireLocked(w)
		d.mu.Unlock()
	}
}

// New read families share the pool and per-person limit, but never evict a
// foreign person's window to force this caller's admission.
func (d *DiscoveryService) reserveSavedReadNew(b discoveryBinding) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return ErrClosed
	}
	subject := b.handle.subject
	for _, w := range d.windows {
		if w.binding.handle.subject == subject && time.Since(w.created) >= 5*time.Minute {
			d.retireLocked(w)
		}
	}
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
		w := d.oldestLocked(subject)
		if w == nil {
			return errDiscoveryBusy
		}
		d.retireLocked(w)
	}
	d.bytes += discoveryNewBytes
	d.building[subject]++
	return nil
}
