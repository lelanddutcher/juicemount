package catalog

import (
	"context"
	"database/sql"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type collectionIdentityProjection struct {
	LibraryID             continuity.LibraryID  `json:"library_id"`
	CollectionID          CollectionID          `json:"collection_id"`
	CreatorSubjectID      SubjectID             `json:"creator_subject_id"`
	CreationOperationID   CollectionOperationID `json:"creation_operation_id"`
	CreationPayloadSHA256 string                `json:"creation_payload_sha256"`
	PolicyGeneration      int64                 `json:"policy_generation"`
}
type collectionDefinitionProjection struct {
	LibraryID         continuity.LibraryID `json:"library_id"`
	CollectionID      CollectionID         `json:"collection_id"`
	Kind              string               `json:"kind"`
	Name              string               `json:"name"`
	Archived          bool                 `json:"archived"`
	DefinitionVersion CollectionVersion    `json:"definition_version"`
	CreatedSequence   int64                `json:"created_sequence"`
	UpdatedSequence   int64                `json:"updated_sequence"`
}
type collectionEntryProjection struct {
	LibraryID       continuity.LibraryID `json:"library_id"`
	EntryID         CollectionEntryID    `json:"entry_id"`
	CollectionID    CollectionID         `json:"collection_id"`
	AssetID         continuity.AssetID   `json:"asset_id"`
	Removed         bool                 `json:"removed"`
	CreatedSequence int64                `json:"created_sequence"`
	UpdatedSequence int64                `json:"updated_sequence"`
}
type collectionGrantProjection struct {
	LibraryID       continuity.LibraryID   `json:"library_id"`
	GrantID         CollectionGrantID      `json:"grant_id"`
	Value           CollectionGrantValue   `json:"value"`
	Version         CollectionGrantVersion `json:"version"`
	Removed         bool                   `json:"removed"`
	CreatedSequence int64                  `json:"created_sequence"`
	UpdatedSequence int64                  `json:"updated_sequence"`
}
type collectionResultProjection struct {
	PolicySequence int64                  `json:"policy_sequence"`
	GrantID        CollectionGrantID      `json:"grant_id"`
	ResultVersion  CollectionGrantVersion `json:"result_version"`
	Removed        bool                   `json:"removed"`
}
type collectionOrderProjection struct {
	Ordinal                 int64  `json:"ordinal"`
	Kind                    string `json:"kind"`
	Sequence                int64  `json:"sequence"`
	GeneratedPolicySequence int64  `json:"generated_policy_sequence"`
}

// These tags and their declaration order are frozen format3 bytes. Do not embed
// CollectionQuotaCounters: its untagged public Go names are not the wire format.
type collectionQuotaProjectionCounters struct {
	Collections            int64 `json:"collections"`
	ActiveEntries          int64 `json:"active_entries"`
	LifetimeEntries        int64 `json:"lifetime_entries"`
	LifetimeGrants         int64 `json:"lifetime_grants"`
	ContentEvents          int64 `json:"content_events"`
	ContentReceipts        int64 `json:"content_receipts"`
	PolicyEvents           int64 `json:"policy_events"`
	PolicyReceipts         int64 `json:"policy_receipts"`
	CanonicalBytes         int64 `json:"canonical_bytes"`
	ReservedPolicyEvents   int64 `json:"reserved_policy_events"`
	ReservedPolicyReceipts int64 `json:"reserved_policy_receipts"`
	ReservedBytes          int64 `json:"reserved_bytes"`
}
type collectionLibraryQuotaProjection struct {
	LibraryID continuity.LibraryID `json:"library_id"`
	collectionQuotaProjectionCounters
}
type collectionLocalQuotaProjection struct {
	LibraryID    continuity.LibraryID `json:"library_id"`
	CollectionID CollectionID         `json:"collection_id"`
	collectionQuotaProjectionCounters
}

func (p *collectionQuotaProjectionCounters) destinations() []any {
	return []any{&p.Collections, &p.ActiveEntries, &p.LifetimeEntries, &p.LifetimeGrants, &p.ContentEvents, &p.ContentReceipts, &p.PolicyEvents, &p.PolicyReceipts, &p.CanonicalBytes, &p.ReservedPolicyEvents, &p.ReservedPolicyReceipts, &p.ReservedBytes}
}
func (p collectionQuotaProjectionCounters) counters() CollectionQuotaCounters {
	return CollectionQuotaCounters{p.Collections, p.ActiveEntries, p.LifetimeEntries, p.LifetimeGrants, p.ContentEvents, p.ContentReceipts, p.PolicyEvents, p.PolicyReceipts, p.CanonicalBytes, p.ReservedPolicyEvents, p.ReservedPolicyReceipts, p.ReservedBytes}
}
func addCollectionProjectionRows(a, b int64) (int64, error) {
	n, ok := checkedQuotaAdd(a, b)
	if !ok || a < 0 || b < 0 {
		return 0, corrupt("collection projection count arithmetic")
	}
	return n, nil
}
func collectionProjectionTotal(ctx context.Context, q queryer) (int64, error) {
	var counts [8]int64
	args := make([]any, len(counts))
	for i := range counts {
		args[i] = &counts[i]
	}
	if err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM collection_identities),(SELECT count(*) FROM collection_definitions),(SELECT count(*) FROM collection_entries),(SELECT count(*) FROM collection_grants),(SELECT count(*) FROM collection_policy_results),(SELECT count(*) FROM collection_order),(SELECT count(*) FROM collection_library_quota),(SELECT count(*) FROM collection_quota)`).Scan(args...); err != nil {
		return 0, err
	}
	var total int64
	for _, n := range counts {
		var err error
		total, err = addCollectionProjectionRows(total, n)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// Walk all format3-only logical rows in the caller's snapshot, including every
// tombstone. Each query scans one bounded row at a time, with no nested query or
// whole-history cache. The caller's emit owns JSON/frame size and row admission.
func walkCollectionProjections(ctx context.Context, q queryer, library continuity.LibraryID, emit func(byte, any) error) error {
	if emit == nil {
		return invalid("collection projection emitter")
	}
	if err := walkRows(ctx, q, `SELECT library_id,collection_id,creator_subject_id,creation_operation_id,creation_payload_sha256,policy_generation FROM collection_identities ORDER BY collection_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var p collectionIdentityProjection
		if err := rows.Scan(&p.LibraryID, &p.CollectionID, &p.CreatorSubjectID, &p.CreationOperationID, &p.CreationPayloadSHA256, &p.PolicyGeneration); err != nil {
			return formatError(err)
		}
		if p.LibraryID != library || p.CollectionID.Validate() != nil || p.CreatorSubjectID.Validate() != nil || p.CreationOperationID.Validate() != nil || !validCollectionDigest(p.CreationPayloadSHA256) || p.PolicyGeneration < 0 {
			return corrupt("identity projection")
		}
		return emit(0x19, p)
	}); err != nil {
		return err
	}
	if err := walkRows(ctx, q, `SELECT i.library_id,d.collection_id,d.kind,CASE WHEN length(CAST(d.name AS BLOB)) BETWEEN 1 AND 256 THEN d.name END,d.archived,d.definition_version,d.created_sequence,d.updated_sequence FROM collection_definitions d LEFT JOIN collection_identities i USING(collection_id) ORDER BY d.collection_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var p collectionDefinitionProjection
		if err := rows.Scan(&p.LibraryID, &p.CollectionID, &p.Kind, &p.Name, &p.Archived, &p.DefinitionVersion, &p.CreatedSequence, &p.UpdatedSequence); err != nil {
			return formatError(err)
		}
		if p.LibraryID != library || p.CollectionID.Validate() != nil || p.Kind != "manual" || !validText(p.Name, 256, true) || p.DefinitionVersion.Validate() != nil || p.CreatedSequence <= 0 || p.UpdatedSequence < p.CreatedSequence {
			return corrupt("definition projection")
		}
		return emit(0x1a, p)
	}); err != nil {
		return err
	}
	if err := walkRows(ctx, q, `SELECT e.library_id,e.entry_id,e.collection_id,e.asset_id,e.removed,e.created_sequence,e.updated_sequence,i.library_id,a.library_id FROM collection_entries e LEFT JOIN collection_identities i USING(collection_id) LEFT JOIN assets a USING(asset_id) ORDER BY e.entry_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var p collectionEntryProjection
		var collectionLibrary, assetLibrary continuity.LibraryID
		if err := rows.Scan(&p.LibraryID, &p.EntryID, &p.CollectionID, &p.AssetID, &p.Removed, &p.CreatedSequence, &p.UpdatedSequence, &collectionLibrary, &assetLibrary); err != nil {
			return formatError(err)
		}
		if p.LibraryID != library || collectionLibrary != library || assetLibrary != library || p.EntryID.Validate() != nil || p.CollectionID.Validate() != nil || p.AssetID.Validate() != nil || p.CreatedSequence <= 0 || p.UpdatedSequence < p.CreatedSequence {
			return corrupt("entry projection")
		}
		return emit(0x1b, p)
	}); err != nil {
		return err
	}
	if err := walkRows(ctx, q, `SELECT g.library_id,g.grant_id,g.subject_id,g.scope_kind,COALESCE(g.collection_id,''),CASE WHEN length(g.actions) BETWEEN 1 AND 128 THEN g.actions END,g.grant_version,g.removed,g.created_sequence,g.updated_sequence,s.library_id,i.library_id FROM collection_grants g LEFT JOIN policy_subjects s USING(subject_id) LEFT JOIN collection_identities i USING(collection_id) ORDER BY g.grant_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var p collectionGrantProjection
		var actions []byte
		var subjectLibrary continuity.LibraryID
		var collectionLibrary sql.NullString
		if err := rows.Scan(&p.LibraryID, &p.GrantID, &p.Value.SubjectID, &p.Value.Scope.Kind, &p.Value.Scope.CollectionID, &actions, &p.Version, &p.Removed, &p.CreatedSequence, &p.UpdatedSequence, &subjectLibrary, &collectionLibrary); err != nil {
			return formatError(err)
		}
		if decodeCanonicalValue(actions, &p.Value.Actions) != nil || p.LibraryID != library || subjectLibrary != library || p.GrantID.Validate() != nil || !validCollectionGrantValue(p.Value) || p.Version.Validate() != nil || p.CreatedSequence <= 0 || p.UpdatedSequence < p.CreatedSequence {
			return corrupt("grant projection")
		}
		if p.Value.Scope.Kind == "collection" && (!collectionLibrary.Valid || collectionLibrary.String != string(library)) || p.Value.Scope.Kind == "library_create" && collectionLibrary.Valid {
			return corrupt("grant projection scope membership")
		}
		return emit(0x1c, p)
	}); err != nil {
		return err
	}
	if err := walkRows(ctx, q, `SELECT r.policy_sequence,r.grant_id,r.result_version,r.removed,p.library_id,g.library_id FROM collection_policy_results r LEFT JOIN collection_policy_events p ON p.sequence=r.policy_sequence LEFT JOIN collection_grants g USING(grant_id) ORDER BY r.policy_sequence,r.grant_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var p collectionResultProjection
		var eventLibrary, grantLibrary continuity.LibraryID
		if err := rows.Scan(&p.PolicySequence, &p.GrantID, &p.ResultVersion, &p.Removed, &eventLibrary, &grantLibrary); err != nil {
			return formatError(err)
		}
		if p.PolicySequence <= 0 || p.GrantID.Validate() != nil || p.ResultVersion.Validate() != nil || eventLibrary != library || grantLibrary != library {
			return corrupt("result projection membership")
		}
		return emit(0x1d, p)
	}); err != nil {
		return err
	}
	var ordinal, legacy, policy, content int64
	if err := walkRows(ctx, q, `SELECT o.ordinal,o.legacy_sequence,o.owner_policy_sequence,o.content_sequence,c.generated_policy_sequence,l.library_id,p.library_id,p.administration,c.library_id,g.library_id,g.administration,c.actor_subject_id,g.operator_subject_id,g.namespace_subject_id FROM collection_order o LEFT JOIN policy_events l ON l.sequence=o.legacy_sequence LEFT JOIN collection_policy_events p ON p.sequence=o.owner_policy_sequence LEFT JOIN collection_events c ON c.sequence=o.content_sequence LEFT JOIN collection_policy_events g ON g.sequence=c.generated_policy_sequence ORDER BY o.ordinal`, func(rows *sql.Rows) error {
		var out collectionOrderProjection
		var l, p, c, g sql.NullInt64
		var ll, pl, admin, cl, gl, childAdmin, actor, operator, namespace sql.NullString
		if err := rows.Scan(&out.Ordinal, &l, &p, &c, &g, &ll, &pl, &admin, &cl, &gl, &childAdmin, &actor, &operator, &namespace); err != nil {
			return formatError(err)
		}
		ordinal++
		if out.Ordinal != ordinal {
			return corrupt("order projection ordinal")
		}
		references := 0
		for _, n := range []sql.NullInt64{l, p, c} {
			if n.Valid {
				references++
				if n.Int64 <= 0 {
					return corrupt("order projection reference")
				}
			}
		}
		if references != 1 {
			return corrupt("order projection family")
		}
		switch {
		case l.Valid:
			legacy++
			if !ll.Valid || ll.String != string(library) || l.Int64 != legacy || g.Valid {
				return corrupt("order legacy membership")
			}
			out.Kind = "legacy_policy"
			out.Sequence = l.Int64
		case p.Valid:
			policy++
			if !pl.Valid || pl.String != string(library) || !admin.Valid || admin.String != "local_owner" || p.Int64 != policy || g.Valid {
				return corrupt("order owner membership")
			}
			out.Kind = "owner_policy"
			out.Sequence = p.Int64
		case c.Valid:
			content++
			if !cl.Valid || cl.String != string(library) || c.Int64 != content {
				return corrupt("order content membership")
			}
			out.Kind = "content"
			out.Sequence = c.Int64
			if g.Valid {
				policy++
				if g.Int64 != policy || !gl.Valid || gl.String != string(library) || !childAdmin.Valid || childAdmin.String != "collection_service" || !actor.Valid || !operator.Valid || !namespace.Valid || actor.String != operator.String || actor.String != namespace.String {
					return corrupt("order generated membership")
				}
				out.GeneratedPolicySequence = g.Int64
			}
		}
		return emit(0x1e, out)
	}); err != nil {
		return err
	}
	var libraryQuota collectionQuotaProjectionCounters
	seen := 0
	if err := walkRows(ctx, q, `SELECT singleton,library_id,`+collectionQuotaColumns+` FROM collection_library_quota ORDER BY singleton`, func(rows *sql.Rows) error {
		var p collectionLibraryQuotaProjection
		var singleton int
		args := []any{&singleton, &p.LibraryID}
		args = append(args, p.destinations()...)
		if err := rows.Scan(args...); err != nil {
			return formatError(err)
		}
		seen++
		if seen != 1 || singleton != 1 || p.LibraryID != library || !validQuotaCounters(p.counters(), false) || !quotaWithinCaps(p.counters(), false) {
			return corrupt("library quota projection")
		}
		libraryQuota = p.collectionQuotaProjectionCounters
		return emit(0x1f, p)
	}); err != nil {
		return err
	}
	if seen != 1 {
		return corrupt("missing library quota projection")
	}
	return walkRows(ctx, q, `SELECT i.library_id,q.collection_id,q.`+qualifiedCollectionQuotaColumns()+` FROM collection_quota q LEFT JOIN collection_identities i USING(collection_id) ORDER BY q.collection_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var p collectionLocalQuotaProjection
		args := []any{&p.LibraryID, &p.CollectionID}
		args = append(args, p.destinations()...)
		if err := rows.Scan(args...); err != nil {
			return formatError(err)
		}
		counters := p.counters()
		if p.LibraryID != library || p.CollectionID.Validate() != nil || !validQuotaCounters(counters, true) || !quotaWithinCaps(counters, true) || !quotaContained(counters, libraryQuota.counters()) {
			return corrupt("local quota projection")
		}
		return emit(0x20, p)
	})
}
func qualifiedCollectionQuotaColumns() string {
	return "collections,q.active_entries,q.lifetime_entries,q.lifetime_grants,q.content_events,q.content_receipts,q.policy_events,q.policy_receipts,q.canonical_bytes,q.reserved_policy_events,q.reserved_policy_receipts,q.reserved_bytes"
}
