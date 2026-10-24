package catalog

import (
	"context"
	"database/sql"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type byteGrantProjection struct {
	LibraryID       continuity.LibraryID `json:"library_id"`
	GrantID         string               `json:"grant_id"`
	Value           ByteGrantValue       `json:"value"`
	Version         string               `json:"version"`
	Removed         bool                 `json:"removed"`
	CreatedSequence int64                `json:"created_sequence"`
	UpdatedSequence int64                `json:"updated_sequence"`
}
type bytePolicyResultProjection struct {
	PolicySequence int64  `json:"policy_sequence"`
	GrantID        string `json:"grant_id"`
	ResultVersion  string `json:"result_version"`
	Removed        bool   `json:"removed"`
}
type bytePolicyQuotaProjection struct {
	LibraryID        continuity.LibraryID `json:"library_id"`
	LifetimeGrants   int64                `json:"lifetime_grants"`
	Events           int64                `json:"events"`
	CanonicalBytes   int64                `json:"canonical_bytes"`
	ReservedRemovals int64                `json:"reserved_removals"`
}

func deliveryProjectionTotal(ctx context.Context, q queryer) (int64, error) {
	var count int64
	err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM byte_grants)+(SELECT count(*) FROM byte_policy_results)+(SELECT count(*) FROM byte_policy_quota)`).Scan(&count)
	return count, err
}
func readBytePolicyQuotaProjection(ctx context.Context, q queryer, library continuity.LibraryID) (bytePolicyQuotaProjection, error) {
	var p bytePolicyQuotaProjection
	var count int64
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM byte_policy_quota`).Scan(&count); err != nil {
		return p, err
	}
	if count != 1 {
		return p, corrupt("byte policy quota singleton")
	}
	if err := q.QueryRowContext(ctx, `SELECT library_id,lifetime_grants,events,canonical_bytes,reserved_removals FROM byte_policy_quota WHERE singleton=1`).Scan(&p.LibraryID, &p.LifetimeGrants, &p.Events, &p.CanonicalBytes, &p.ReservedRemovals); err != nil {
		return p, err
	}
	if p.LibraryID != library || p.LifetimeGrants < 0 || p.LifetimeGrants > 16384 || p.Events < 0 || p.Events > 131072 || p.CanonicalBytes < 0 || p.CanonicalBytes > 64<<20 || p.ReservedRemovals < 0 || p.ReservedRemovals > p.LifetimeGrants || p.Events+p.ReservedRemovals > 131072 || p.CanonicalBytes+3072*p.ReservedRemovals > 64<<20 {
		return p, corrupt("byte policy quota bounds")
	}
	return p, nil
}
func validateBytePolicyPortableState(ctx context.Context, q queryer, library continuity.LibraryID) error {
	p, err := readBytePolicyQuotaProjection(ctx, q, library)
	if err != nil {
		return err
	}
	var grants, events, canonical, reserved int64
	err = q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM byte_grants),(SELECT count(*) FROM byte_policy_events),(SELECT COALESCE(sum(length(request)+length(receipt)),0) FROM byte_policy_events),(SELECT count(*) FROM byte_grants WHERE removed=0)`).Scan(&grants, &events, &canonical, &reserved)
	if err != nil {
		return err
	}
	if p.LifetimeGrants != grants || p.Events != events || p.CanonicalBytes != canonical || p.ReservedRemovals != reserved {
		return corrupt("byte policy quota does not match durable identities")
	}
	return checkDeliveryOrder(ctx, q)
}

// Format5 adds only portable policy to the complete domain4 projection. Source
// routes, credentials, operational clocks, selections and output never enter it.
func walkDeliveryProjections(ctx context.Context, q queryer, library continuity.LibraryID, emit func(byte, any) error) error {
	if emit == nil {
		return invalid("delivery projection emitter")
	}
	if err := walkRows(ctx, q, `SELECT g.library_id,g.grant_id,g.subject_id,g.scope_kind,COALESCE(g.asset_id,''),g.action,g.grant_version,g.removed,g.created_sequence,g.updated_sequence,s.library_id,c.library_id,u.library_id,r.result_version,r.removed FROM byte_grants g LEFT JOIN policy_subjects s USING(subject_id) LEFT JOIN byte_policy_events c ON c.sequence=g.created_sequence LEFT JOIN byte_policy_events u ON u.sequence=g.updated_sequence LEFT JOIN byte_policy_results r ON r.policy_sequence=g.updated_sequence AND r.grant_id=g.grant_id ORDER BY g.grant_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var p byteGrantProjection
		var subjectLibrary, createdLibrary, updatedLibrary continuity.LibraryID
		var lastVersion string
		var lastRemoved bool
		if err := rows.Scan(&p.LibraryID, &p.GrantID, &p.Value.SubjectID, &p.Value.Scope.Kind, &p.Value.Scope.AssetID, &p.Value.Action, &p.Version, &p.Removed, &p.CreatedSequence, &p.UpdatedSequence, &subjectLibrary, &createdLibrary, &updatedLibrary, &lastVersion, &lastRemoved); err != nil {
			return formatError(err)
		}
		if p.LibraryID != library || subjectLibrary != library || createdLibrary != library || updatedLibrary != library || !validRandomID(p.GrantID, "bgr_") || !validByteGrantValue(p.Value) || !validRandomID(p.Version, "bgv_") || p.CreatedSequence <= 0 || p.UpdatedSequence < p.CreatedSequence || p.Version != lastVersion || p.Removed != lastRemoved {
			return corrupt("byte grant projection")
		}
		return emit(0x22, p)
	}); err != nil {
		return err
	}
	if err := walkRows(ctx, q, `SELECT r.policy_sequence,r.grant_id,r.result_version,r.removed,e.library_id,g.library_id FROM byte_policy_results r LEFT JOIN byte_policy_events e ON e.sequence=r.policy_sequence LEFT JOIN byte_grants g USING(grant_id) ORDER BY r.policy_sequence,r.grant_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var p bytePolicyResultProjection
		var eventLibrary, grantLibrary continuity.LibraryID
		if err := rows.Scan(&p.PolicySequence, &p.GrantID, &p.ResultVersion, &p.Removed, &eventLibrary, &grantLibrary); err != nil {
			return formatError(err)
		}
		if p.PolicySequence <= 0 || !validRandomID(p.GrantID, "bgr_") || !validRandomID(p.ResultVersion, "bgv_") || eventLibrary != library || grantLibrary != library {
			return corrupt("byte policy result projection")
		}
		return emit(0x23, p)
	}); err != nil {
		return err
	}
	p, err := readBytePolicyQuotaProjection(ctx, q, library)
	if err != nil {
		return err
	}
	return emit(0x24, p)
}
