package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// History is a bounded authorized recent window, not an operation-log export.
func readCollectionReadHistory(ctx context.Context, q queryer, library continuity.LibraryID, c credential, r CollectionQueryRequest, rights discoveryRights) (items []json.RawMessage, complete bool, resultErr error) {
	rows, err := q.QueryContext(ctx, `SELECT actor_subject_id,CASE WHEN length(request) BETWEEN 1 AND 32768 THEN request END,recorded_unix_nano FROM collection_events INDEXED BY collection_events_history WHERE collection_id=? ORDER BY sequence DESC LIMIT 8193`, r.CollectionID)
	if err != nil {
		return nil, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	items = make([]json.RawMessage, 0)
	count := 0
	var rawBytes int64
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		count++
		if count > 8192 {
			return nil, false, errDiscoveryWork
		}
		var actor SubjectID
		var raw []byte
		var timestamp int64
		if err := rows.Scan(&actor, &raw, &timestamp); err != nil {
			return nil, false, err
		}
		rawBytes += int64(len(raw))
		if rawBytes > 32<<20 {
			return nil, false, errDiscoveryWork
		}
		op, err := DecodeCollectionRequest(raw)
		if err != nil || op.LibraryID != library || op.CollectionID != r.CollectionID || actor.Validate() != nil || timestamp <= 0 {
			return nil, false, corrupt("collection history record")
		}
		value, err := collectionValue(op)
		if err != nil {
			return nil, false, err
		}
		var disclosed any
		visible := true
		switch v := value.(type) {
		case *CreateCollection:
			for _, e := range v.Entries {
				if !rights.allows(e.AssetID, "assets.discover") {
					visible = false
					break
				}
			}
			disclosed = CollectionHistoryCreate{v.Name, v.Entries}
		case *EditCollectionEntries:
			for _, e := range v.Changes {
				if !rights.allows(e.AssetID, "assets.discover") {
					visible = false
					break
				}
			}
			disclosed = v
		case *RenameCollection:
			disclosed = v
		case *SetCollectionArchived:
			disclosed = v
		default:
			continue // grant administration is available only in current access.
		}
		if !visible {
			continue
		}
		v, err := json.Marshal(disclosed)
		if err != nil {
			return nil, false, err
		}
		category := "another_authorized_person"
		if actor == c.subject {
			category = "you"
		}
		item, err := encodeCollectionItem(CollectionHistoryItem{op.Kind, strconv.FormatInt(timestamp, 10), category, v})
		if err != nil {
			return nil, false, err
		}
		candidate := append(items, item)
		if collectionFrozenCost(candidate, r.PageSize) > discoveryBuildBytes {
			if len(items) == 0 {
				return nil, false, errDiscoveryWork
			}
			return items, false, nil
		}
		items = candidate
		if len(items) >= 1000 {
			return items, false, nil
		}
	}
	return items, true, rows.Err()
}

// Include retained raw items, final bodies, page/token state and TWO transient
// full1MiB serialization buffers before accepting a visible item. This fixed
// transient reserve also dominates every constructor prefix, including a short
// final page; estimated largest-page size alone would not imply constructibility.
func collectionFrozenCost(items []json.RawMessage, pageSize int) int64 {
	raw := int64(0)
	pages := 0
	var encoded int64
	for start := 0; start < len(items); {
		size := int64(collectionEnvelopeBytes)
		end := start
		for end < len(items) && end-start < pageSize && size+int64(len(items[end]))+1 <= 1<<20 {
			raw += int64(cap(items[end]))
			size += int64(len(items[end])) + 1
			end++
		}
		if end == start {
			return discoveryBuildBytes + 1
		}
		pages++
		encoded += size + 8192 // bounded page-envelope and allocation allowance
		start = end
	}
	if pages == 0 {
		pages = 1
		encoded = collectionEnvelopeBytes + 8192
	}
	return 16384 + raw + encoded + 2*discoveryCopyBytes + 256*int64(cap(items)) + 384*int64(pages)
}
