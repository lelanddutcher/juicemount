package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const (
	collectionReadBit uint8 = 1 << iota
	collectionEditBit
	collectionAdminBit
	collectionCreateBit
)

type collectionReadRights struct {
	collections map[CollectionID]uint8
	create      bool
	rows        int
}

func collectionActionBit(s string) uint8 {
	switch s {
	case "collections.read":
		return collectionReadBit
	case "collections.edit":
		return collectionEditBit
	case "collections.admin":
		return collectionAdminBit
	case "collections.create":
		return collectionCreateBit
	}
	return 0
}
func collectionCeiling(c credential) uint8 {
	var out uint8
	for _, s := range c.actions {
		out |= collectionActionBit(s)
	}
	return out
}

// Stream grants into small rights unions; examined work is not retained memory.
// A targeted fresh fence visits at most128 grants, independently of construction.
func readCollectionReadRights(ctx context.Context, q queryer, c credential, target CollectionID) (out collectionReadRights, resultErr error) {
	out = collectionReadRights{collections: make(map[CollectionID]uint8)}
	statement := `SELECT scope_kind,COALESCE(collection_id,''),CASE WHEN length(actions) BETWEEN 1 AND 128 THEN actions END FROM collection_grants INDEXED BY collection_grants_access WHERE subject_id=? AND removed=0`
	args := []any{c.subject}
	limit := 16384
	if target != "" {
		statement += ` AND scope_kind='collection' AND collection_id=?`
		args = append(args, target)
		limit = 128
	}
	statement += ` LIMIT ?`
	args = append(args, limit+1)
	rows, err := q.QueryContext(ctx, statement, args...)
	if err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		out.rows++
		if out.rows > limit {
			return out, errDiscoveryWork
		}
		var scope CollectionGrantScope
		var raw []byte
		var actions []string
		if err := rows.Scan(&scope.Kind, &scope.CollectionID, &raw); err != nil {
			return out, err
		}
		if decodeCanonicalValue(raw, &actions) != nil || !validCollectionGrantValue(CollectionGrantValue{c.subject, scope, actions}) {
			return out, corrupt("collection read rights")
		}
		var bits uint8
		for _, action := range actions {
			bits |= collectionActionBit(action)
		}
		bits &= collectionCeiling(c)
		if scope.Kind == "library_create" {
			out.create = out.create || bits&collectionCreateBit != 0
			continue
		}
		if _, ok := out.collections[scope.CollectionID]; !ok && len(out.collections) >= 1024 {
			return out, errDiscoveryWork
		}
		out.collections[scope.CollectionID] |= bits
	}
	return out, rows.Err()
}
func collectionReadEligible(c credential, subject Subject) error {
	if subject.Value.Kind != "person" || c.scope.Kind != "library" || collectionCeiling(c)&collectionReadBit == 0 {
		return ErrNotFound
	}
	return nil
}
func collectionReadPermissions(bits uint8) CollectionPermissions {
	return CollectionPermissions{bits&collectionReadBit != 0, bits&collectionEditBit != 0, bits&collectionAdminBit != 0}
}
func collectionSummary(def collectionDefinition, c credential, bits uint8) CollectionSummary {
	creator := "another_authorized_person"
	if def.creator == c.subject {
		creator = "you"
	}
	return CollectionSummary{def.id, "manual", def.name, def.archived, def.version, creator, collectionReadPermissions(bits)}
}

type collectionDirectoryBinding struct {
	id          CollectionID
	version     CollectionVersion
	generation  int64
	permissions uint8
}
type collectionAssetBinding struct {
	id       continuity.AssetID
	sequence int64
}
type collectionMemberIdentity struct {
	entry   CollectionEntryID
	asset   continuity.AssetID
	removed bool
}
type collectionReadBinding struct {
	auth       discoveryBinding
	request    CollectionQueryRequest
	definition collectionDefinition
	directory  []collectionDirectoryBinding
	canCreate  bool
}

func readCollectionDirectory(ctx context.Context, q queryer, c credential, r CollectionQueryRequest, rights collectionReadRights) ([]json.RawMessage, []collectionDirectoryBinding, bool, error) {
	ids := make([]CollectionID, 0, len(rights.collections))
	for id, bits := range rights.collections {
		if bits&collectionReadBit != 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	items := make([]json.RawMessage, 0, len(ids))
	bindings := make([]collectionDirectoryBinding, 0, len(ids))
	for _, id := range ids {
		def, err := readCollectionReadDefinition(ctx, q, id)
		if err != nil {
			return nil, nil, false, err
		}
		if r.State == "active" && def.archived || r.State == "archived" && !def.archived {
			continue
		}
		item, err := encodeCollectionItem(collectionSummary(def, c, rights.collections[id]))
		if err != nil {
			return nil, nil, false, err
		}
		items = append(items, item)
		bindings = append(bindings, collectionDirectoryBinding{id, def.version, def.generation, rights.collections[id]})
	}
	canCreate := rights.create && collectionCeiling(c)&(collectionCreateBit|collectionReadBit|collectionEditBit|collectionAdminBit) == collectionCreateBit|collectionReadBit|collectionEditBit|collectionAdminBit
	return items, bindings, canCreate, nil
}
func sameCollectionDirectory(a, b []collectionDirectoryBinding) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func readCollectionMembers(ctx context.Context, q queryer, r CollectionQueryRequest, rights discoveryRights) (result []collectionMemberIdentity, resultErr error) {
	removed := r.State == "removed"
	rows, err := q.QueryContext(ctx, `SELECT entry_id,asset_id,removed FROM collection_entries INDEXED BY collection_entries_page WHERE collection_id=? AND removed=? ORDER BY entry_id LIMIT 8193`, r.CollectionID, removed)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	// Fixed capacity avoids unaccounted geometric slice growth at8192 identities.
	result = make([]collectionMemberIdentity, 0, 8192)
	count := 0
	for rows.Next() {
		count++
		if count > 8192 {
			return nil, errDiscoveryWork
		}
		var id collectionMemberIdentity
		if err := rows.Scan(&id.entry, &id.asset, &id.removed); err != nil {
			return nil, err
		}
		if id.entry.Validate() != nil || id.asset.Validate() != nil || id.removed != removed {
			return nil, corrupt("collection member identity")
		}
		if rights.allows(id.asset, "assets.discover") {
			result = append(result, id)
		}
	}
	if !removed && len(result) > 4096 {
		return nil, errDiscoveryWork
	}
	return result, rows.Err()
}
func readCollectionAccess(ctx context.Context, q queryer, id CollectionID) (items []json.RawMessage, resultErr error) {
	rows, err := q.QueryContext(ctx, `SELECT grant_id,subject_id,scope_kind,COALESCE(collection_id,''),CASE WHEN length(actions) BETWEEN 1 AND 128 THEN actions END,grant_version,removed FROM collection_grants WHERE collection_id=? ORDER BY grant_id LIMIT 129`, id)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	items = make([]json.RawMessage, 0, 128)
	for rows.Next() {
		if len(items) >= 128 {
			return nil, errDiscoveryWork
		}
		var item CollectionAccessItem
		var raw []byte
		v := &item.Expected
		if err := rows.Scan(&item.GrantID, &v.Value.SubjectID, &v.Value.Scope.Kind, &v.Value.Scope.CollectionID, &raw, &v.Version, &v.Removed); err != nil {
			return nil, err
		}
		if item.GrantID.Validate() != nil || v.Version.Validate() != nil || decodeCanonicalValue(raw, &v.Value.Actions) != nil || !validCollectionGrantValue(v.Value) || v.Value.Scope.Kind != "collection" || v.Value.Scope.CollectionID != id {
			return nil, corrupt("collection access grant")
		}
		encoded, err := encodeCollectionItem(item)
		if err != nil {
			return nil, err
		}
		items = append(items, encoded)
	}
	return items, rows.Err()
}

// Only this page's metadata is loaded. No labels are retained in membership.
func readCollectionMemberPage(ctx context.Context, q queryer, library continuity.LibraryID, ids []collectionMemberIdentity, start, pageSize int, rights discoveryRights) ([]json.RawMessage, []collectionAssetBinding, int, error) {
	items := make([]json.RawMessage, 0, pageSize)
	bindings := make([]collectionAssetBinding, 0, pageSize)
	size := collectionEnvelopeBytes
	end := start
	for end < len(ids) && len(items) < pageSize {
		id := ids[end]
		if !rights.allows(id.asset, "assets.discover") {
			return nil, nil, start, errDiscoveryRestart
		}
		// SQL guards the raw label even if a damaged database bypassed its DDL.
		a, err := scanAsset(q.QueryRowContext(ctx, `SELECT library_id,asset_id,CASE WHEN length(CAST(label AS BLOB))<=4096 THEN label END,COALESCE(head,''),created_sequence,updated_sequence FROM assets WHERE asset_id=?`, id.asset), library)
		if err != nil {
			return nil, nil, start, err
		}
		raw, err := encodeCollectionItem(CollectionMember{id.entry, id.asset, a.Label, disclosedRevision(a.Head, rights.allows(a.ID, "revisions.read")), id.removed})
		if err != nil {
			return nil, nil, start, err
		}
		if size+len(raw)+1 > 1<<20 {
			if len(items) == 0 {
				return nil, nil, start, errDiscoveryWork
			}
			break
		}
		size += len(raw) + 1
		items = append(items, raw)
		bindings = append(bindings, collectionAssetBinding{a.ID, a.UpdatedSequence})
		end++
	}
	return items, bindings, end, nil
}
func (d *DiscoveryService) collectionReadFence(ctx context.Context, b collectionReadBinding, assets []collectionAssetBinding) error {
	tx, done, err := d.store.readSnapshot(ctx)
	if err != nil {
		return err
	}
	c, subject, err := d.store.currentHandle(ctx, tx, b.auth.handle)
	if err == nil && subject.AuthorizationGeneration != b.auth.generation {
		err = errDiscoveryRestart
	}
	if err == nil {
		err = collectionReadEligible(c, subject)
	}
	if err == nil {
		rights, e := readCollectionReadRights(ctx, tx, c, b.request.CollectionID)
		err = e
		if err == nil && b.request.Kind == "directory" {
			// Fence retains only scalar bindings; it never encodes/copies directory items.
			current, e := collectionDirectoryFence(ctx, tx, c, b.request, rights)
			err = e
			if err == nil && (!sameCollectionDirectory(current, b.directory) || collectionCanCreate(c, rights) != b.canCreate) {
				err = errDiscoveryRestart
			}
		} else if err == nil {
			bits := rights.collections[b.request.CollectionID]
			required := collectionReadBit
			if b.request.Kind == "access" {
				required |= collectionEditBit | collectionAdminBit
			}
			if bits&required != required {
				err = errDiscoveryRestart
			}
			if err == nil {
				def, e := readCollectionReadDefinition(ctx, tx, b.request.CollectionID)
				err = e
				if err == nil && (def.version != b.definition.version || def.generation != b.definition.generation) {
					err = errDiscoveryRestart
				}
			}
		}
	}
	if err == nil {
		if len(assets) > 100 {
			err = errDiscoveryWork
		} else {
			for _, a := range assets {
				var sequence int64
				e := tx.QueryRowContext(ctx, `SELECT updated_sequence FROM assets WHERE asset_id=?`, a.id).Scan(&sequence)
				if e != nil {
					err = e
					break
				}
				if sequence != a.sequence {
					err = errDiscoveryRestart
					break
				}
			}
		}
	}
	return errors.Join(err, d.finishSnapshot(done))
}
func collectionCanCreate(c credential, r collectionReadRights) bool {
	return r.create && collectionCeiling(c)&15 == 15
}
func collectionDirectoryFence(ctx context.Context, q queryer, c credential, r CollectionQueryRequest, rights collectionReadRights) ([]collectionDirectoryBinding, error) {
	ids := make([]CollectionID, 0, len(rights.collections))
	for id, bits := range rights.collections {
		if bits&collectionReadBit != 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]collectionDirectoryBinding, 0, len(ids))
	for _, id := range ids {
		def, err := readCollectionReadDefinition(ctx, q, id)
		if err != nil {
			return nil, err
		}
		if r.State == "active" && def.archived || r.State == "archived" && !def.archived {
			continue
		}
		out = append(out, collectionDirectoryBinding{id, def.version, def.generation, rights.collections[id]})
	}
	return out, nil
}

func readCollectionReadDefinition(ctx context.Context, q queryer, id CollectionID) (collectionDefinition, error) {
	var def collectionDefinition
	def.id = id
	err := q.QueryRowContext(ctx, `SELECT i.creator_subject_id,CASE WHEN length(CAST(d.name AS BLOB)) BETWEEN 1 AND 256 THEN d.name END,d.archived,d.definition_version,i.policy_generation FROM collection_definitions d JOIN collection_identities i USING(collection_id) WHERE d.collection_id=?`, id).Scan(&def.creator, &def.name, &def.archived, &def.version, &def.generation)
	if errors.Is(err, sql.ErrNoRows) {
		return def, ErrNotFound
	}
	if err != nil {
		return def, err
	}
	if id.Validate() != nil || def.creator.Validate() != nil || !validText(def.name, 256, true) || def.version.Validate() != nil || def.generation < 1 {
		return def, corrupt("collection read definition")
	}
	return def, nil
}
