package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Schema6 admission streams one collection and one historical record at a time.
// Only a manual collection's <=8192 entry identities are retained while folding
// its history. No current credential, authority incarnation or grant is used to
// authorize a historical definition or actor.
func checkSavedQueryState(ctx context.Context, q queryer, library continuity.LibraryID) error {
	usage, err := readCollectionQuota(ctx, q, "")
	if err != nil {
		return err
	}
	var invalidRows int
	err = q.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM collection_library_quota WHERE library_id<>?)+
 (SELECT count(*) FROM catalog_credentials c WHERE NOT EXISTS(SELECT 1 FROM credential_codecs d WHERE d.selector=c.selector))+
 (SELECT count(*) FROM catalog_admin_events a WHERE NOT EXISTS(SELECT 1 FROM catalog_admin_codecs d WHERE d.operation_id=a.operation_id))+
 (SELECT count(*) FROM collection_identities i WHERE i.library_id<>? OR (EXISTS(SELECT 1 FROM collection_definitions m WHERE m.collection_id=i.collection_id)+EXISTS(SELECT 1 FROM saved_query_definitions s WHERE s.collection_id=i.collection_id))<>1)+
 (SELECT count(*) FROM collection_entries e JOIN saved_query_definitions s USING(collection_id))`, library, library).Scan(&invalidRows)
	if err != nil {
		return err
	}
	if invalidRows != 0 {
		return corrupt("saved query family references")
	}
	totals, err := collectionPortableTotals(ctx, q)
	if err != nil {
		return err
	}
	if totals.CollectionCount > 1024 || totals.CollectionEventCount > 131072 || totals.CollectionPolicyEventCount > 131072 {
		return corrupt("saved query history bounds")
	}
	if err := checkCollectionOrder(ctx, q); err != nil {
		return err
	}
	if err := validateCollectionQuotaIdentity(ctx, q, ""); err != nil {
		return err
	}
	if usage.Library.Collections != totals.CollectionCount {
		return corrupt("saved query identity count")
	}
	var last CollectionID
	for count := int64(0); count < totals.CollectionCount; count++ {
		var id CollectionID
		if err := q.QueryRowContext(ctx, `SELECT collection_id FROM collection_identities WHERE collection_id>? ORDER BY collection_id LIMIT 1`, last).Scan(&id); err != nil {
			return err
		}
		if id.Validate() != nil {
			return corrupt("saved query identity")
		}
		if err := validateCollectionQuotaIdentity(ctx, q, id); err != nil {
			return err
		}
		if err := validateSavedQueryCollection(ctx, q, library, id); err != nil {
			return err
		}
		last = id
	}
	for sequence := int64(1); sequence <= totals.CollectionPolicyEventCount; sequence++ {
		if _, _, err := readCollectionPolicyRecord(ctx, q, library, sequence); err != nil {
			return corrupt("saved query policy history")
		}
	}
	return validateSavedQueryGrants(ctx, q, library)
}

type savedQueryEntryState struct {
	asset            continuity.AssetID
	removed          bool
	created, updated int64
}

func validateSavedQueryCollection(ctx context.Context, q queryer, library continuity.LibraryID, id CollectionID) error {
	kind, err := collectionStoredKind(ctx, q, id)
	if err != nil {
		return err
	}
	var current collectionDefinition
	var currentData []byte
	var created, updated int64
	if kind == "saved_query" {
		d, err := readSavedQueryDefinition(ctx, q, id)
		if err != nil {
			return err
		}
		current, currentData, created, updated = d.collectionDefinition, d.canonical, d.created, d.updated
	} else {
		current, err = readCollectionDefinition(ctx, q, id)
		if err != nil {
			return err
		}
		if err := q.QueryRowContext(ctx, `SELECT created_sequence,updated_sequence FROM collection_definitions WHERE collection_id=?`, id).Scan(&created, &updated); err != nil {
			return err
		}
	}
	var declarationLibrary continuity.LibraryID
	var creation CollectionOperationID
	var creator SubjectID
	var creationDigest string
	if err := q.QueryRowContext(ctx, `SELECT library_id,creator_subject_id,creation_operation_id,creation_payload_sha256 FROM collection_identities WHERE collection_id=?`, id).Scan(&declarationLibrary, &creator, &creation, &creationDigest); err != nil {
		return err
	}
	if declarationLibrary != library || creator != current.creator {
		return corrupt("saved query declaration")
	}
	var previous CollectionVersion
	var sequence int64
	var name string
	var archived bool
	var definition []byte
	entries := make(map[CollectionEntryID]savedQueryEntryState)
	for count := 0; ; count++ {
		var next int64
		err := q.QueryRowContext(ctx, `SELECT sequence FROM collection_events WHERE collection_id=? AND sequence>? ORDER BY sequence LIMIT 1`, id, sequence).Scan(&next)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if count >= 8192 {
			return corrupt("saved query local history bound")
		}
		var op CollectionOperationID
		var actor SubjectID
		var expected, result CollectionVersion
		var operation, digest string
		var value any
		var child int64
		var binding func(*CollectionPolicyRecord) error
		if kind == "saved_query" {
			r, _, c, err := readSavedQueryContentRecord(ctx, q, library, next)
			if err != nil {
				return corrupt("saved query typed history")
			}
			if r.Request.CollectionID != id {
				return corrupt("saved query history identity")
			}
			op, actor, expected, result, operation = r.Request.ID, r.ActorSubjectID, r.Request.ExpectedVersion, r.ResultVersion, r.Request.Kind
			_, digest, _ = CanonicalSavedQueryRequest(r.Request)
			value, err = savedQueryValue(r.Request)
			if err != nil {
				return err
			}
			child = c
			binding = func(p *CollectionPolicyRecord) error { return bindSavedQueryChild(r, p) }
		} else {
			r, _, c, err := readCollectionContentRecord(ctx, q, library, next)
			if err != nil {
				return corrupt("manual typed history")
			}
			if r.Request.CollectionID != id {
				return corrupt("manual history identity")
			}
			op, actor, expected, result, operation = r.Request.ID, r.ActorSubjectID, r.Request.ExpectedVersion, r.ResultVersion, r.Request.Kind
			_, digest, _ = CanonicalCollectionRequest(r.Request)
			value, err = collectionValue(r.Request)
			if err != nil {
				return err
			}
			child = c
			binding = func(p *CollectionPolicyRecord) error { return bindCollectionChild(r, p) }
		}
		if expected != previous || (count == 0) != (operation == "create") || count == 0 && (created != next || creation != op || creator != actor || creationDigest != digest) {
			return corrupt("collection historical CAS or create")
		}
		var p *CollectionPolicyRecord
		if child != 0 {
			record, _, err := readCollectionPolicyRecord(ctx, q, library, child)
			if err != nil {
				return err
			}
			p = &record
		}
		if err := binding(p); err != nil {
			return err
		}
		var proposed *SavedQueryDefinition
		switch v := value.(type) {
		case *CreateSavedQuery:
			name = v.Name
			proposed = &v.Definition
		case *ReplaceSavedQueryDefinition:
			proposed = &v.Definition
		case *CreateCollection:
			name = v.Name
			for _, entry := range v.Entries {
				if _, exists := entries[entry.ID]; exists {
					return corrupt("manual duplicate entry")
				}
				entries[entry.ID] = savedQueryEntryState{entry.AssetID, false, next, next}
			}
		case *RenameCollection:
			if name == v.Name {
				return corrupt("collection historical rename no-op")
			}
			name = v.Name
		case *SetCollectionArchived:
			if archived == v.Archived {
				return corrupt("collection historical archive no-op")
			}
			archived = v.Archived
		case *EditCollectionEntries:
			for _, change := range v.Changes {
				prior, exists := entries[change.ID]
				if change.Kind == "add" {
					if exists || len(entries) >= 8192 {
						return corrupt("manual entry lifetime")
					}
					entries[change.ID] = savedQueryEntryState{change.AssetID, false, next, next}
				} else {
					removed := change.Kind == "remove"
					if !exists || prior.asset != change.AssetID || prior.removed == removed {
						return corrupt("manual entry history")
					}
					prior.removed, prior.updated = removed, next
					entries[change.ID] = prior
				}
			}
		}
		if proposed != nil {
			data, _, err := CanonicalSavedQueryDefinition(*proposed)
			if err != nil {
				return corrupt("saved query historical definition")
			}
			if err := savedQueryAnchors(ctx, q, library, *proposed); err != nil {
				return corrupt("saved query historical anchors")
			}
			if operation == "replace_definition" && bytes.Equal(definition, data) {
				return corrupt("saved query historical replacement no-op")
			}
			definition = data
		}
		sequence, previous = next, result
	}
	if sequence == 0 || updated != sequence || current.version != previous || current.name != name || current.archived != archived || !bytes.Equal(currentData, definition) {
		return corrupt("collection current history projection")
	}
	var generation int64
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM collection_policy_events WHERE collection_id=?`, id).Scan(&generation); err != nil {
		return err
	}
	if current.generation != generation {
		return corrupt("collection policy generation")
	}
	var entryCount int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM collection_entries WHERE collection_id=?`, id).Scan(&entryCount); err != nil {
		return err
	}
	if entryCount != len(entries) {
		return corrupt("collection entry coverage")
	}
	for entry, want := range entries {
		var got savedQueryEntryState
		var entryLibrary continuity.LibraryID
		if err := q.QueryRowContext(ctx, `SELECT library_id,asset_id,removed,created_sequence,updated_sequence FROM collection_entries WHERE collection_id=? AND entry_id=?`, id, entry).Scan(&entryLibrary, &got.asset, &got.removed, &got.created, &got.updated); err != nil {
			return err
		}
		if got != want || entryLibrary != library {
			return corrupt("collection entry projection")
		}
	}
	return nil
}

// Grant history is folded one identity at a time. Each record is bounded and
// released before the next; final disabled subjects do not revoke past writes.
func validateSavedQueryGrants(ctx context.Context, q queryer, library continuity.LibraryID) error {
	var last CollectionGrantID
	for count := 0; ; count++ {
		var id CollectionGrantID
		err := q.QueryRowContext(ctx, `SELECT grant_id FROM collection_grants WHERE grant_id>? ORDER BY grant_id LIMIT 1`, last).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if count >= 16384 {
			return corrupt("collection grant lifetime bound")
		}
		current, err := readCollectionGrant(ctx, q, id)
		if err != nil {
			return err
		}
		var expected *ExpectedCollectionGrant
		var first, sequence int64
		for {
			var next int64
			err := q.QueryRowContext(ctx, `SELECT policy_sequence FROM collection_policy_results WHERE grant_id=? AND policy_sequence>? ORDER BY policy_sequence LIMIT 1`, id, sequence).Scan(&next)
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			if err != nil {
				return err
			}
			p, _, err := readCollectionPolicyRecord(ctx, q, library, next)
			if err != nil {
				return err
			}
			found := false
			for index, change := range p.Request.Changes {
				if change.ID != id {
					continue
				}
				found = true
				if !reflect.DeepEqual(change.Expected, expected) {
					return corrupt("collection grant historical CAS")
				}
				value := CollectionGrantValue{}
				if change.Kind == "remove" {
					if expected == nil || expected.Removed {
						return corrupt("collection grant historical remove")
					}
					value = expected.Value
				} else {
					value = *change.Value
					if expected != nil && !expected.Removed && reflect.DeepEqual(expected.Value, value) {
						return corrupt("collection grant historical put no-op")
					}
					if expected != nil && (expected.Value.SubjectID != value.SubjectID || !reflect.DeepEqual(expected.Value.Scope, value.Scope)) {
						return corrupt("collection grant immutable scope")
					}
				}
				e := ExpectedCollectionGrant{value, p.Results[index].Version, p.Results[index].Removed}
				expected = &e
			}
			if !found {
				return corrupt("collection grant unowned result")
			}
			if first == 0 {
				first = next
			}
			sequence = next
		}
		if expected == nil || !reflect.DeepEqual(*expected, current.expected) {
			return corrupt("collection grant current history")
		}
		var created, updated int64
		var lib continuity.LibraryID
		if err := q.QueryRowContext(ctx, `SELECT library_id,created_sequence,updated_sequence FROM collection_grants WHERE grant_id=?`, id).Scan(&lib, &created, &updated); err != nil {
			return err
		}
		if lib != library || first != created || sequence != updated {
			return corrupt("collection grant history bounds")
		}
		last = id
	}
}
