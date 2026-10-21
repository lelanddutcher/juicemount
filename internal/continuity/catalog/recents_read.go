package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"
)

// All variable payload owners are pointer-free columns of exact N capacity.
// Labels have one raw UTF-8 owner; pages are reconstructed and never cached.
type recentsWindow struct {
	binding     savedQueryReadBinding
	assets      [][38]byte
	heads       [][69]byte
	headFlags   []uint8
	labels      []byte
	labelBounds [][2]uint32
	times       [][2]int64
	lengths     []uint32
	ranges      [][2]uint16
	pageLengths []uint32
}
type recentsSource struct {
	asset Asset
	entry preferenceEntry
}

func (d *DiscoveryService) readRecentsWindow(ctx context.Context, q queryer, c credential, b savedQueryReadBinding, pageSize int, request []byte) (*discoveryWindow, error) {
	view, pref, err := d.readPreferenceView(ctx, q, c.subject)
	if err != nil {
		return nil, err
	}
	b.auth.preference = pref
	rights, err := readDiscoveryRights(ctx, q, d.library, c)
	if err != nil {
		return nil, err
	}
	entries := make([]preferenceEntry, 0, len(view))
	for _, e := range view {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].order != entries[j].order {
			return entries[i].order > entries[j].order
		}
		return entries[i].asset < entries[j].asset
	})
	source := make([]recentsSource, 0, 256)
	labelBytes := 0
	for _, e := range entries {
		if !rights.allows(e.asset, "assets.discover") || !rights.allows(e.asset, "activity.record") {
			continue
		}
		a, err := scanAsset(q.QueryRowContext(ctx, `SELECT library_id,asset_id,CASE WHEN length(CAST(label AS BLOB))<=4096 THEN label END,COALESCE(head,''),created_sequence,updated_sequence FROM assets WHERE asset_id=?`, e.asset), d.library)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !rights.allows(a.ID, "revisions.read") {
			a.Head = ""
		}
		source = append(source, recentsSource{a, e})
		labelBytes += len(a.Label)
		if pref.expiry == 0 || e.expires < pref.expiry {
			pref.expiry = e.expires
		}
	}
	if labelBytes > 1<<20 {
		return nil, errDiscoveryWork
	}
	n := len(source)
	r := &recentsWindow{binding: b, assets: make([][38]byte, n), heads: make([][69]byte, n), headFlags: make([]uint8, n), labels: make([]byte, labelBytes), labelBounds: make([][2]uint32, n), times: make([][2]int64, n), lengths: make([]uint32, n)}
	at := 0
	for i, s := range source {
		if len(s.asset.ID) != 38 || s.asset.Head != "" && len(s.asset.Head) != 69 {
			return nil, corrupt("recents identity width")
		}
		copy(r.assets[i][:], s.asset.ID)
		if s.asset.Head != "" {
			copy(r.heads[i][:], s.asset.Head)
			r.headFlags[i] = 1
		}
		r.labelBounds[i] = [2]uint32{uint32(at), uint32(len(s.asset.Label))}
		at += copy(r.labels[at:], s.asset.Label)
		r.times[i] = [2]int64{s.entry.selected, s.entry.expires}
		// One bounded item scratch, no retained encoded-row corpus.
		item := appendRecentItem(make([]byte, 0, 64<<10), r, i)
		if len(item) > 64<<10 {
			return nil, corrupt("recents item length")
		}
		r.lengths[i] = uint32(len(item))
	}
	// Drop all temporary owners (including source labels and preference tokens)
	// before the compact object can transfer into the shared retained pool.
	source = nil
	entries = nil
	view = nil
	token := readPlanningToken
	envelope, _, err := recentsEnvelope(n, &token)
	if err != nil {
		return nil, err
	}
	lengths := make([]int, n)
	for i, length := range r.lengths {
		lengths[i] = int(length)
	}
	planned, err := readRanges(lengths, pageSize, len(envelope))
	if err != nil {
		return nil, err
	}
	r.ranges = make([][2]uint16, len(planned))
	r.pageLengths = make([]uint32, len(planned))
	for i, p := range planned {
		r.ranges[i] = [2]uint16{uint16(p[0]), uint16(p[1])}
		size := len(envelope)
		for j := p[0]; j < p[1]; j++ {
			size += lengths[j]
			if j > p[0] {
				size++
			}
		}
		if i == len(planned)-1 {
			size -= 68
		}
		r.pageLengths[i] = uint32(size)
	}
	tokens, err := d.readTokens(len(planned))
	if err != nil {
		return nil, err
	}
	w := &discoveryWindow{family: recentsReadFamily, recent: r, binding: cloneDiscoveryBinding(b.auth), version: 2, request: request, created: time.Now(), tokens: tokens}
	w.charge = recentsCharge(w)
	if w.charge > 2<<20 || w.charge+2*discoveryCopyBytes > discoveryBuildBytes {
		return nil, corrupt("recents retained charge")
	}
	return w, nil
}

func recentsCharge(w *discoveryWindow) int64 {
	r := w.recent
	charge := int64(16384+cap(w.request)+cap(r.labels)) + 38*int64(cap(r.assets)) + 69*int64(cap(r.heads)) + int64(cap(r.headFlags)) + 8*int64(cap(r.labelBounds)) + 16*int64(cap(r.times)) + 4*int64(cap(r.lengths)) + 4*int64(cap(r.ranges)) + 4*int64(cap(r.pageLengths)) + 32*int64(cap(w.tokens)) + 384*int64(len(w.tokens))
	for _, token := range w.tokens {
		charge += int64(len(token))
	}
	return charge
}
func recentsEnvelope(count int, next *string) ([]byte, int, error) {
	return readEnvelope(RecentsResponse{2, "recents", make([]RecentsItem, 0), count, "frozen_authorized_recent", true, next, "recents1", "frozen_snapshot"})
}

// Go-compatible string escaping, directly from the sole raw-label slab. The
// preallocated page is exact-sized, so appends cannot retain geometric surplus.
func appendReadString(out []byte, text []byte) []byte {
	out = append(out, '"')
	const hex = "0123456789abcdef"
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch c {
		case '"', '\\':
			out = append(out, '\\', c)
		case '\b':
			out = append(out, '\\', 'b')
		case '\f':
			out = append(out, '\\', 'f')
		case '\n':
			out = append(out, '\\', 'n')
		case '\r':
			out = append(out, '\\', 'r')
		case '\t':
			out = append(out, '\\', 't')
		case '<', '>', '&':
			out = append(out, '\\', 'u', '0', '0', hex[c>>4], hex[c&15])
		default:
			if c < 0x20 {
				out = append(out, '\\', 'u', '0', '0', hex[c>>4], hex[c&15])
			} else if c == 0xe2 && i+2 < len(text) && text[i+1] == 0x80 && (text[i+2] == 0xa8 || text[i+2] == 0xa9) {
				out = append(out, '\\', 'u', '2', '0', '2', hex[text[i+2]&15])
				i += 2
			} else {
				out = append(out, c)
			}
		}
	}
	return append(out, '"')
}
func appendRecentItem(out []byte, r *recentsWindow, i int) []byte {
	out = append(out, `{"asset_id":`...)
	out = appendReadString(out, r.assets[i][:])
	out = append(out, `,"label":`...)
	bound := r.labelBounds[i]
	out = appendReadString(out, r.labels[int(bound[0]):int(bound[0]+bound[1])])
	out = append(out, `,"head_revision_id":`...)
	if r.headFlags[i] != 0 {
		out = appendReadString(out, r.heads[i][:])
	} else {
		out = append(out, "null"...)
	}
	out = append(out, `,"reasons":["declared_selection"],"selected_unix":"`...)
	out = strconv.AppendInt(out, r.times[i][0], 10)
	out = append(out, `","expires_unix":"`...)
	out = strconv.AppendInt(out, r.times[i][1], 10)
	return append(out, `"}`...)
}
func serializeRecentsPage(w *discoveryWindow, index int) ([]byte, error) {
	r := w.recent
	if index < 0 || index >= len(r.ranges) {
		return nil, corrupt("recents page index")
	}
	var next *string
	if index < len(w.tokens) {
		next = &w.tokens[index]
	}
	envelope, split, err := recentsEnvelope(len(r.assets), next)
	if err != nil {
		return nil, err
	}
	size := int(r.pageLengths[index])
	if size > 1<<20 || size < len(envelope) {
		return nil, corrupt("recents page plan")
	}
	out := make([]byte, 0, size)
	out = append(out, envelope[:split]...)
	span := r.ranges[index]
	for i := int(span[0]); i < int(span[1]); i++ {
		if i > int(span[0]) {
			out = append(out, ',')
		}
		before := len(out)
		out = appendRecentItem(out, r, i)
		if len(out)-before != int(r.lengths[i]) {
			return nil, corrupt("recents encoded row plan")
		}
	}
	out = append(out, envelope[split:]...)
	if len(out) != size || cap(out) != size {
		return nil, corrupt("recents encoded page plan")
	}
	return out, nil
}

func recentsReadBytes(r RecentsReadRequest) ([]byte, error) {
	b, err := json.Marshal(RecentsCLIListRequest{r.Version, r.Kind, r.LibraryID, r.PageSize})
	if len(b) > 512 {
		return nil, invalid("recents request capacity")
	}
	return exactReadBytes(b), err
}
