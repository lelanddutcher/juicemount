package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sort"
	"strconv"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const savedQueryReadFamily = "saved_queries2"
const recentsReadFamily = "recents2"

type savedQueryReadBinding struct {
	auth      discoveryBinding
	request   SavedQueryReadRequest
	metadata  int64
	version   CollectionVersion
	policy    int64
	directory [32]byte
	canCreate bool
}
type savedQueryReadWindow struct {
	binding    savedQueryReadBinding
	summary    SavedQuerySummary
	definition []byte
	items      []json.RawMessage
	complete   bool
}

func readMetadataEpoch(ctx context.Context, q queryer) (int64, error) {
	var n int64
	err := q.QueryRowContext(ctx, `SELECT COALESCE(max(sequence),0) FROM events`).Scan(&n)
	if err == nil && n < 0 {
		err = corrupt("metadata epoch")
	}
	return n, err
}

// Independent probes deliberately do not populate discoveryReader.assets:
// that cache strips labels, which must remain available to the ranker.
func authorizeSavedQueryAnchors(ctx context.Context, q queryer, library continuity.LibraryID, def SavedQueryDefinition, rights discoveryRights) error {
	for _, id := range def.ContextAssetIDs {
		if !rights.allows(id, "assets.discover") {
			return ErrNotFound
		}
		if _, err := scanAsset(q.QueryRowContext(ctx, `SELECT library_id,asset_id,CASE WHEN length(CAST(label AS BLOB))<=4096 THEN label END,COALESCE(head,''),created_sequence,updated_sequence FROM assets WHERE asset_id=?`, id), library); err != nil {
			return err
		}
	}
	return nil
}
func savedReadSummary(def savedQueryDefinition, c credential, bits uint8) SavedQuerySummary {
	s := collectionSummary(def.collectionDefinition, c, bits)
	s.Kind = "saved_query"
	return s
}

// The digest binds the complete ordered canonical summary set without retaining
// a second name/summary corpus. Length delimiters are implicit JSON object ends;
// a NUL separator cannot appear in a canonical JSON object.
func readSavedQueryDirectory(ctx context.Context, q queryer, c credential, r SavedQueryReadRequest, rights collectionReadRights, retain bool) ([]json.RawMessage, [32]byte, bool, error) {
	var zero [32]byte
	ids := make([]CollectionID, 0, len(rights.collections))
	for id, bits := range rights.collections {
		if bits&collectionReadBit != 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var items []json.RawMessage
	if retain {
		items = make([]json.RawMessage, 0, 1024)
	}
	h := sha256.New()
	for _, id := range ids {
		kind, err := collectionStoredKind(ctx, q, id)
		if err != nil {
			return nil, zero, false, err
		}
		if kind != "saved_query" {
			continue
		}
		def, err := readSavedQueryDefinition(ctx, q, id)
		if err != nil {
			return nil, zero, false, err
		}
		if r.State == "active" && def.archived || r.State == "archived" && !def.archived {
			continue
		}
		raw, err := readExactItem(savedReadSummary(def, c, rights.collections[id]))
		if err != nil {
			return nil, zero, false, err
		}
		_, _ = h.Write(raw)
		_, _ = h.Write([]byte{0})
		if retain {
			if len(items) == cap(items) {
				return nil, zero, false, errDiscoveryWork
			}
			items = append(items, raw)
		}
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return items, sum, collectionCanCreate(c, rights), nil
}

func readExactItem(v any) (json.RawMessage, error) {
	b, err := encodeDiscoveryValue(v, 64<<10)
	if err != nil {
		return nil, err
	}
	return exactReadBytes(b), nil
}

func readSavedQueryExecution(ctx context.Context, q queryer, library continuity.LibraryID, def savedQueryDefinition, rights discoveryRights, view map[continuity.AssetID]preferenceEntry, pref *preferenceBinding, pageSize int) ([]json.RawMessage, bool, error) {
	r, err := freezeDiscoveryRequest(DiscoveryRequest{Version: 2, Kind: "search", LibraryID: library, Text: def.definition.Text, ContextAssetIDs: def.definition.ContextAssetIDs, PageSize: pageSize}, library)
	if err != nil {
		return nil, false, err
	}
	reader := discoveryReader{ctx: ctx, q: q, library: library, rights: rights, assets: make(map[continuity.AssetID]*Asset), preferences: view, preferenceFence: pref}
	old, complete, err := reader.search(r)
	if err != nil {
		return nil, false, err
	}
	items := make([]json.RawMessage, len(old), 1000)
	for i, raw := range old {
		var a discoveryAsset
		if decodeCanonicalValue(raw, &a) != nil {
			return nil, false, corrupt("ranked query result")
		}
		items[i], err = readExactItem(SavedQueryResult{SavedQueryResultID{def.id, a.AssetID}, a.AssetID, a.Label, a.Head, a.Reasons})
		if err != nil {
			return nil, false, err
		}
		old[i] = nil // intermediate discovery DTOs are construction work, not retained
	}
	return items, complete, nil
}

func readSavedQueryHistory(ctx context.Context, q queryer, library continuity.LibraryID, c credential, w *savedQueryReadWindow, rights discoveryRights, requestBytes int) (items []json.RawMessage, complete bool, resultErr error) {
	rows, err := q.QueryContext(ctx, `SELECT actor_subject_id,CASE WHEN length(request) BETWEEN 1 AND 32768 THEN request END,recorded_unix_nano FROM collection_events INDEXED BY collection_events_history WHERE collection_id=? ORDER BY sequence DESC LIMIT 8193`, w.binding.request.CollectionID)
	if err != nil {
		return nil, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	items = make([]json.RawMessage, 0, 1000)
	planner, err := newSavedReadPlanner(w, 1000, false, requestBytes, 1000)
	if err != nil {
		return nil, false, err
	}
	var scan savedQueryHistoryScan
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if err := scan.next(); err != nil {
			return nil, false, err
		}
		var actor SubjectID
		var raw []byte
		var timestamp int64
		if err := rows.Scan(&actor, &raw, &timestamp); err != nil {
			return nil, false, err
		}
		if err := scan.charge(len(raw)); err != nil {
			return nil, false, err
		}
		op, err := DecodeSavedQueryRequest(raw)
		if err != nil || op.LibraryID != library || op.CollectionID != w.binding.request.CollectionID || actor.Validate() != nil || timestamp <= 0 {
			return nil, false, corrupt("saved query history record")
		}
		value, err := savedQueryValue(op)
		if err != nil {
			return nil, false, corrupt("saved query history value")
		}
		var disclosed any
		var anchors *SavedQueryDefinition
		switch v := value.(type) {
		case *CreateSavedQuery:
			disclosed = SavedQueryHistoryCreate{v.Name, v.Definition}
			anchors = &v.Definition
		case *ReplaceSavedQueryDefinition:
			disclosed = v
			anchors = &v.Definition
		case *RenameCollection:
			disclosed = v
		case *SetCollectionArchived:
			disclosed = v
		default:
			continue // canonical grants still consume source scan and byte budgets
		}
		v, err := json.Marshal(disclosed)
		if err != nil {
			return nil, false, err
		}
		who := "another_authorized_person"
		if actor == c.subject {
			who = "you"
		}
		item, err := readExactItem(CollectionHistoryItem{op.Kind, strconv.FormatInt(timestamp, 10), who, v})
		if err != nil {
			return nil, false, err
		}
		next := planner
		if !next.add(len(item), cap(item)) {
			if len(items) == 0 {
				return nil, false, errDiscoveryWork
			}
			return items, false, nil
		}
		// Only the prospective INCLUDED record needs disclosure authorization.
		// All included later pages are checked before any window is published.
		if anchors != nil {
			if err := authorizeSavedQueryAnchors(ctx, q, library, *anchors, rights); err != nil {
				return nil, false, err
			}
		}
		planner = next
		items = append(items, item)
		if len(items) == 1000 {
			return items, false, nil
		}
	}
	return items, true, rows.Err()
}

type savedQueryHistoryScan struct {
	records int
	bytes   int64
}

func (s *savedQueryHistoryScan) next() error {
	s.records++
	if s.records > 8192 {
		return errDiscoveryWork
	}
	return nil
}
func (s *savedQueryHistoryScan) charge(n int) error {
	s.bytes += int64(n)
	if s.bytes > 32<<20 {
		return errDiscoveryWork
	}
	return nil
}

func recentsReadEligible(c credential, sub Subject) error {
	if sub.Value.Kind != "person" || c.scope.Kind != "library" || !containsAction(c.actions, "assets.discover") || !containsAction(c.actions, "activity.record") {
		return ErrNotFound
	}
	return nil
}

// savedReadFence uses one fresh snapshot. Unchanged metadata and subject auth
// bind every previously checked history anchor, including undisclosed pages.
func (d *DiscoveryService) savedReadFence(ctx context.Context, b *savedQueryReadBinding, family string) (resultErr error) {
	tx, done, err := d.store.readSnapshot(ctx)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, d.finishSnapshot(done)) }()
	c, sub, err := d.store.currentHandle(ctx, tx, b.auth.handle)
	if err != nil {
		return err
	}
	if family == recentsReadFamily {
		err = recentsReadEligible(c, sub)
	} else {
		err = collectionReadEligible(c, sub)
	}
	if err != nil {
		return err
	}
	if family == savedQueryReadFamily {
		rights, err := readCollectionReadRights(ctx, tx, c, b.request.CollectionID)
		if err != nil {
			return err
		}
		if b.request.Kind == "directory" {
			_, sum, create, err := readSavedQueryDirectory(ctx, tx, c, b.request, rights, false)
			if err != nil {
				return err
			}
			if sum != b.directory || create != b.canCreate {
				return errDiscoveryRestart
			}
		} else {
			bits := rights.collections[b.request.CollectionID]
			required := collectionReadBit
			if b.request.Kind == "access" {
				required |= collectionEditBit | collectionAdminBit
			}
			if bits&required != required {
				return ErrNotFound
			}
			def, err := readSavedQueryDefinition(ctx, tx, b.request.CollectionID)
			if err != nil {
				return err
			}
			if b.request.Kind == "definition" || b.request.Kind == "execute" || b.request.Kind == "history" {
				assetRights, err := readDiscoveryRights(ctx, tx, d.library, c)
				if err != nil {
					return err
				}
				if err := authorizeSavedQueryAnchors(ctx, tx, d.library, def.definition, assetRights); err != nil {
					return err
				}
			}
			if def.version != b.version || def.generation != b.policy || b.request.Kind == "execute" && def.archived {
				return errDiscoveryRestart
			}
		}
	}
	if sub.AuthorizationGeneration != b.auth.generation {
		return errDiscoveryRestart
	}
	epoch, err := readMetadataEpoch(ctx, tx)
	if err != nil {
		return err
	}
	if epoch != b.metadata {
		return errDiscoveryRestart
	}
	if b.auth.preference != nil {
		control, err := readPreferenceControl(ctx, tx, d.library)
		if err != nil {
			return err
		}
		current, err := readPreferenceSubject(ctx, tx, d.library, c.subject)
		if err != nil {
			return err
		}
		if current.generation != b.auth.preference.generation {
			return errDiscoveryRestart
		}
		b.auth.preference.floor = control.floor // lower bound, never an equality token
		if err := b.auth.preference.checkTime(d.store.now().Unix()); err != nil {
			return err
		}
	}
	return nil
}
