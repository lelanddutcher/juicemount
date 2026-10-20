package catalog

import (
	"database/sql"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func (r *discoveryReader) history(asset continuity.AssetID, add func(any) (bool, error)) (bool, error) {
	var before int64
	for {
		if err := r.ctx.Err(); err != nil {
			return false, err
		}
		if r.budget.events >= 8192 {
			return false, errDiscoveryWork
		}
		var sequence, requestBytes, receiptBytes int64
		query := `SELECT sequence,length(request),length(receipt) FROM events ORDER BY sequence DESC LIMIT 1`
		var args []any
		if before != 0 {
			query = `SELECT sequence,length(request),length(receipt) FROM events WHERE sequence<? ORDER BY sequence DESC LIMIT 1`
			args = append(args, before)
		}
		err := r.q.QueryRowContext(r.ctx, query, args...).Scan(&sequence, &requestBytes, &receiptBytes)
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if requestBytes < 1 || requestBytes > MaxRequestBytes || receiptBytes < 1 || receiptBytes > 1024 {
			return false, corrupt("discovery event lengths")
		}
		if requestBytes+receiptBytes > (32<<20)-r.budget.raw {
			return false, errDiscoveryWork
		}
		r.budget.raw += requestBytes + receiptBytes
		r.budget.events++
		e, err := scanEvent(r.q.QueryRowContext(r.ctx, `SELECT `+boundedEventColumns+` FROM events WHERE sequence=?`, sequence))
		if err != nil {
			return false, err
		}
		event, err := e.decode(r.library)
		if err != nil {
			return false, err
		}
		// Preserve original CHANGE ORDER within a newest-first event; do not emit
		// its ordinal, event identity, sequence, payload hash or other batch data.
		for _, change := range event.Operation.Changes {
			if err := r.ctx.Err(); err != nil {
				return false, err
			}
			item, visible, err := r.historyChange(asset, change)
			if err != nil {
				return false, err
			}
			if !visible {
				continue
			}
			item.Timestamp = e.timestamp
			item.Actor = event.Operation.Actor
			stop, err := add(item)
			if err != nil {
				return false, err
			}
			if stop {
				return false, nil
			}
		}
		before = sequence
	}
}

func (r *discoveryReader) historyChange(asset continuity.AssetID, c Change) (discoveryHistory, bool, error) {
	item := discoveryHistory{}
	discover := func(id continuity.AssetID) bool { return r.rights.allows(id, "assets.discover") }
	revisions := func(id continuity.AssetID) bool { return discover(id) && r.rights.allows(id, "revisions.read") }
	locationAllowed := func(v LocationValue) bool { return discover(v.AssetID) && r.rights.allows(v.AssetID, "locations.read") }
	touchesEdge := func(v RelationshipValue) bool { return v.SourceAssetID == asset || v.TargetAssetID == asset }
	switch {
	case c.CreateAsset != nil:
		v := c.CreateAsset
		if v.AssetID != asset || !discover(v.AssetID) {
			return item, false, nil
		}
		item.Kind = "create_asset"
		item.After = historyAsset{v.AssetID, v.Label}
	case c.RenameAsset != nil:
		v := c.RenameAsset
		if v.AssetID != asset || !discover(v.AssetID) {
			return item, false, nil
		}
		item.Kind = "rename_asset"
		item.Before = historyAsset{v.AssetID, v.ExpectedLabel}
		item.After = historyAsset{v.AssetID, v.Label}
	case c.RecordRevision != nil:
		m := *c.RecordRevision
		if m.AssetID != asset || !revisions(m.AssetID) {
			return item, false, nil
		}
		id, err := m.RevisionID()
		if err != nil {
			return item, false, err
		}
		item.Kind = "record_revision"
		item.After = revisionDTO(m, id)
	case c.AdvanceHead != nil:
		v := c.AdvanceHead
		if v.AssetID != asset || !revisions(v.AssetID) {
			return item, false, nil
		}
		item.Kind = "advance_head"
		item.Before = historyHead{v.AssetID, disclosedRevision(v.ExpectedRevision, true)}
		item.After = historyHead{v.AssetID, disclosedRevision(v.Revision, true)}
	case c.PutRelationship != nil:
		v := c.PutRelationship
		if !touchesEdge(v.Value) && (v.Expected == nil || !touchesEdge(v.Expected.Value)) {
			return item, false, nil
		}
		if !r.edgeAllowed(v.Value) {
			return item, false, nil
		}
		// The old endpoints are a separate authorization obligation. Removing
		// just this predicate must expose the otherwise-valid negative fixture.
		if v.Expected != nil && !r.edgeAllowed(v.Expected.Value) {
			return item, false, nil
		}
		if err := r.edgeMembership(v.Value); err != nil {
			return item, false, err
		}
		if v.Expected != nil {
			if err := r.edgeMembership(v.Expected.Value); err != nil {
				return item, false, err
			}
			item.Before = historyRelationship{r.relationshipDTO(v.ID, v.Expected.Value), v.Expected.Removed}
		}
		item.Kind = "put_relationship"
		item.After = historyRelationship{r.relationshipDTO(v.ID, v.Value), false}
	case c.RemoveRelationship != nil:
		v := c.RemoveRelationship
		if !touchesEdge(v.Expected.Value) || !r.edgeAllowed(v.Expected.Value) {
			return item, false, nil
		}
		if err := r.edgeMembership(v.Expected.Value); err != nil {
			return item, false, err
		}
		item.Kind = "remove_relationship"
		item.Before = historyRelationship{r.relationshipDTO(v.ID, v.Expected.Value), v.Expected.Removed}
		item.After = historyRelationship{r.relationshipDTO(v.ID, v.Expected.Value), true}
	case c.PutLocation != nil:
		v := c.PutLocation
		if v.Value.AssetID != asset && (v.Expected == nil || v.Expected.Value.AssetID != asset) {
			return item, false, nil
		}
		if !locationAllowed(v.Value) || v.Expected != nil && !locationAllowed(v.Expected.Value) {
			return item, false, nil
		}
		if _, err := r.asset(v.Value.AssetID); err != nil {
			return item, false, err
		}
		if v.Expected != nil {
			if _, err := r.asset(v.Expected.Value.AssetID); err != nil {
				return item, false, err
			}
			item.Before = historyLocation{r.locationDTO(v.ID, v.Expected.Value), v.Expected.Removed}
		}
		item.Kind = "put_location"
		item.After = historyLocation{r.locationDTO(v.ID, v.Value), false}
	case c.RemoveLocation != nil:
		v := c.RemoveLocation
		if v.Expected.Value.AssetID != asset || !locationAllowed(v.Expected.Value) {
			return item, false, nil
		}
		if _, err := r.asset(v.Expected.Value.AssetID); err != nil {
			return item, false, err
		}
		item.Kind = "remove_location"
		item.Before = historyLocation{r.locationDTO(v.ID, v.Expected.Value), v.Expected.Removed}
		item.After = historyLocation{r.locationDTO(v.ID, v.Expected.Value), true}
	default:
		return item, false, corrupt("discovery history variant")
	}
	return item, true, nil
}
