package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type discoveryRights struct {
	credential credential
	library    map[string]bool
	assets     map[string]map[continuity.AssetID]bool
}

func (r discoveryRights) allows(id continuity.AssetID, action string) bool {
	if !r.credential.scope.matches(id) || !containsAction(r.credential.actions, action) {
		return false
	}
	return r.library[action] || r.assets[action][id]
}
func readDiscoveryRights(ctx context.Context, q queryer, library continuity.LibraryID, c credential) (discoveryRights, error) {
	r := discoveryRights{c, make(map[string]bool), make(map[string]map[continuity.AssetID]bool)}
	count := 0
	err := walkRows(ctx, q, `SELECT `+grantColumns+` FROM policy_grants INDEXED BY policy_grants_access WHERE subject_id=? AND removed=0 LIMIT 4097`, func(rows *sql.Rows) error {
		if count >= 4096 {
			return errDiscoveryWork
		}
		count++
		g, err := scanGrant(rows, library)
		if err != nil {
			return err
		}
		for _, action := range g.Value.Actions {
			if g.Value.Scope.Kind == "library" {
				r.library[action] = true
			} else {
				if r.assets[action] == nil {
					r.assets[action] = make(map[continuity.AssetID]bool)
				}
				r.assets[action][g.Value.Scope.AssetID] = true
			}
		}
		return nil
	}, c.subject)
	return r, err
}

type discoveryBudget struct {
	assets, relationships, revisions, locations, endpoints, events int
	raw                                                            int64
}
type discoveryReader struct {
	ctx       context.Context
	q         queryer
	library   continuity.LibraryID
	rights    discoveryRights
	budget    discoveryBudget
	assets    map[continuity.AssetID]*Asset
	itemBytes int64
}

func (r *discoveryReader) asset(id continuity.AssetID) (Asset, error) {
	if a, ok := r.assets[id]; ok {
		if a == nil {
			return Asset{}, ErrNotFound
		}
		return *a, nil
	}
	if r.budget.endpoints >= 8192 {
		return Asset{}, errDiscoveryWork
	}
	r.budget.endpoints++
	a, err := readAsset(r.ctx, r.q, r.library, id)
	if errors.Is(err, ErrNotFound) {
		r.assets[id] = nil
		return Asset{}, err
	}
	if err != nil {
		return Asset{}, err
	}
	cached := a
	cached.Label = ""
	r.assets[id] = &cached
	return a, nil
}
func (r *discoveryReader) requireAsset(id continuity.AssetID, action string) (Asset, error) {
	if !r.rights.allows(id, "assets.discover") || action != "" && !r.rights.allows(id, action) {
		return Asset{}, ErrNotFound
	}
	return r.asset(id)
}
func (r *discoveryReader) assetDTO(a Asset, reasons []string) discoveryAsset {
	return discoveryAsset{a.ID, a.Label, disclosedRevision(a.Head, r.rights.allows(a.ID, "revisions.read")), reasons}
}
func (r *discoveryReader) relationshipDTO(id string, v RelationshipValue) discoveryRelationship {
	return discoveryRelationship{id, v.SourceAssetID, disclosedRevision(v.SourceRevision, r.rights.allows(v.SourceAssetID, "revisions.read")), v.TargetAssetID, disclosedRevision(v.TargetRevision, r.rights.allows(v.TargetAssetID, "revisions.read")), v.Predicate, v.Provenance, v.Source}
}
func (r *discoveryReader) locationDTO(id string, v LocationValue) discoveryLocation {
	return discoveryLocation{id, v.AssetID, disclosedRevision(v.Revision, r.rights.allows(v.AssetID, "revisions.read")), v.Source, v.Locator}
}
func (r *discoveryReader) edgeAllowed(v RelationshipValue) bool {
	return r.rights.allows(v.SourceAssetID, "assets.discover") && r.rights.allows(v.SourceAssetID, "relationships.read") && r.rights.allows(v.TargetAssetID, "assets.discover") && r.rights.allows(v.TargetAssetID, "relationships.read")
}
func (r *discoveryReader) edgeMembership(v RelationshipValue) error {
	if _, err := r.asset(v.SourceAssetID); err != nil {
		return err
	}
	_, err := r.asset(v.TargetAssetID)
	return err
}
func (r *discoveryReader) add(items *[]json.RawMessage, value any) (bool, error) {
	data, err := encodeDiscoveryValue(value, 64<<10)
	if err != nil {
		return false, err
	}
	cost := int64(cap(data)) + 256
	if r.itemBytes+cost > discoveryBuildBytes {
		return false, errDiscoveryWork
	}
	r.itemBytes += cost
	*items = append(*items, data)
	return len(*items) >= 1000, nil
}

func (r *discoveryReader) query(request DiscoveryRequest) (items []json.RawMessage, single any, complete bool, resultErr error) {
	if request.Kind == "search" {
		items, complete, resultErr = r.search(request)
		return
	}
	action := map[string]string{"asset": "", "counts": "", "revisions": "revisions.read", "history": "", "relationships": "relationships.read", "locations": "locations.read"}[request.Kind]
	a, err := r.requireAsset(request.AssetID, action)
	if err != nil {
		return nil, nil, false, err
	}
	if request.Kind == "asset" {
		return nil, struct {
			Version int            `json:"version"`
			Asset   discoveryAsset `json:"asset"`
		}{1, r.assetDTO(a, []string{"stable_id"})}, true, nil
	}
	if request.Kind == "counts" {
		value, err := r.counts(a.ID)
		return nil, value, true, err
	}
	items = make([]json.RawMessage, 0)
	add := func(value any) (bool, error) { return r.add(&items, value) }
	switch request.Kind {
	case "revisions":
		complete, resultErr = r.revisions(a.ID, func(v discoveryRevision) (bool, error) { return add(v) })
	case "relationships":
		complete, resultErr = r.relationships(a.ID, func(v Relationship) (bool, error) { return add(r.relationshipDTO(v.ID, v.Value)) })
	case "locations":
		complete, resultErr = r.locations(a.ID, func(v Location) (bool, error) { return add(r.locationDTO(v.ID, v.Value)) })
	case "history":
		complete, resultErr = r.history(a.ID, add)
	}
	return
}

func (r *discoveryReader) search(request DiscoveryRequest) ([]json.RawMessage, bool, error) {
	type candidate struct {
		asset   Asset
		context bool
		tier    int
	}
	found := make([]candidate, 0)
	seen := make(map[continuity.AssetID]bool)
	contextIDs := make(map[continuity.AssetID]bool)
	for _, id := range request.ContextAssetIDs {
		contextIDs[id] = true
	}
	consider := func(a Asset) bool {
		if seen[a.ID] {
			return false
		}
		seen[a.ID] = true
		label := foldDiscoveryText(a.Label)
		tier := 0
		if request.Text != "" {
			switch {
			case label == request.Text:
				tier = 3
			case strings.HasPrefix(label, request.Text):
				tier = 2
			case strings.Contains(label, request.Text):
				tier = 1
			default:
				return false
			}
		}
		found = append(found, candidate{a, contextIDs[a.ID], tier})
		return len(found) >= 1000
	}
	contexts := append([]continuity.AssetID(nil), request.ContextAssetIDs...)
	sort.Slice(contexts, func(i, j int) bool { return contexts[i] < contexts[j] })
	for _, id := range contexts {
		if !r.rights.allows(id, "assets.discover") {
			continue
		}
		if r.budget.assets >= 4096 {
			return nil, false, errDiscoveryWork
		}
		r.budget.assets++
		a, err := r.asset(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		consider(a)
	}
	complete := true
	if r.rights.credential.scope.Kind == "library" && r.rights.library["assets.discover"] && containsAction(r.rights.credential.actions, "assets.discover") {
		remaining := 4096 - r.budget.assets
		if remaining <= 0 {
			return nil, false, errDiscoveryWork
		}
		examined := 0
		stop := false
		err := walkRows(r.ctx, r.q, `SELECT library_id,asset_id,CASE WHEN length(CAST(label AS BLOB))<=4096 THEN label END,COALESCE(head,''),created_sequence,updated_sequence FROM assets ORDER BY asset_id COLLATE BINARY LIMIT ?`, func(rows *sql.Rows) error {
			examined++
			r.budget.assets++
			a, err := scanAsset(rows, r.library)
			if err != nil {
				return err
			}
			if consider(a) {
				stop = true
				complete = false
				return errDiscoveryStop
			}
			return nil
		}, remaining)
		if err != nil && !errors.Is(err, errDiscoveryStop) {
			return nil, false, err
		}
		if !stop && examined >= remaining {
			return nil, false, errDiscoveryWork
		}
	} else {
		ids := make([]continuity.AssetID, 0)
		if r.rights.credential.scope.Kind == "asset" {
			ids = append(ids, r.rights.credential.scope.AssetID)
		} else {
			for id := range r.rights.assets["assets.discover"] {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			if seen[id] || !r.rights.allows(id, "assets.discover") {
				continue
			}
			if r.budget.assets >= 4096 {
				return nil, false, errDiscoveryWork
			}
			r.budget.assets++
			a, err := r.asset(id)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, false, err
			}
			if consider(a) {
				complete = false
				break
			}
		}
	}
	sort.Slice(found, func(i, j int) bool {
		a, b := found[i], found[j]
		if a.context != b.context {
			return a.context
		}
		if a.tier != b.tier {
			return a.tier > b.tier
		}
		return a.asset.ID < b.asset.ID
	})
	items := make([]json.RawMessage, 0, len(found))
	for _, c := range found {
		reasons := make([]string, 0, 2)
		if c.context {
			reasons = append(reasons, "context")
		}
		switch c.tier {
		case 3:
			reasons = append(reasons, "label_exact")
		case 2:
			reasons = append(reasons, "label_prefix")
		case 1:
			reasons = append(reasons, "label_contains")
		default:
			reasons = append(reasons, "stable_id")
		}
		if _, err := r.add(&items, r.assetDTO(c.asset, reasons)); err != nil {
			return nil, false, err
		}
	}
	return items, complete, nil
}

func (r *discoveryReader) revisions(asset continuity.AssetID, fn func(discoveryRevision) (bool, error)) (bool, error) {
	var after continuity.RevisionID
	for {
		if err := r.ctx.Err(); err != nil {
			return false, err
		}
		if r.budget.revisions >= 4096 {
			return false, errDiscoveryWork
		}
		var id continuity.RevisionID
		var owner continuity.LibraryID
		var data, canonical []byte
		err := r.q.QueryRowContext(r.ctx, `SELECT revision_id,library_id,CASE WHEN length(manifest_json)<=8192 THEN manifest_json END,CASE WHEN length(canonical)<=2143 THEN canonical END FROM revisions WHERE asset_id=? AND revision_id>? ORDER BY revision_id LIMIT 1`, asset, after).Scan(&id, &owner, &data, &canonical)
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		r.budget.revisions++
		m, err := decodeManifest(data)
		if err != nil {
			return false, err
		}
		computed, err := m.RevisionID()
		if err != nil {
			return false, err
		}
		expected, _ := m.Canonical()
		if owner != r.library || m.LibraryID != r.library || m.AssetID != asset || computed != id || !bytes.Equal(expected, canonical) {
			return false, corrupt("discovery revision identity")
		}
		stop, err := fn(revisionDTO(m, id))
		if err != nil {
			return false, err
		}
		if stop {
			return false, nil
		}
		after = id
	}
}

type relationshipCursor struct {
	source bool
	after  string
	value  *Relationship
	done   bool
}

func (r *discoveryReader) nextRelationship(asset continuity.AssetID, c *relationshipCursor) error {
	if c.done {
		return nil
	}
	if r.budget.relationships >= 4096 {
		return errDiscoveryWork
	}
	// The shared validated selector owns four joined PK membership probes.
	// Reserve all four, including nullable anchors and a possible EOF lookup.
	if r.budget.endpoints+4 > 8192 {
		return errDiscoveryWork
	}
	r.budget.endpoints += 4
	column, index := "r.target_asset_id", "relationships_target_active"
	if c.source {
		column, index = "r.source_asset_id", "relationships_source_active"
	}
	// Each side is an existing exact index range; never an unbounded OR sort.
	query := strings.Replace(relationshipSelect, "FROM relationships r ", "FROM relationships r INDEXED BY "+index+" ", 1) + ` WHERE ` + column + `=? AND r.removed=0 AND r.relationship_id>? ORDER BY r.relationship_id LIMIT 1`
	v, err := scanRelationship(r.q.QueryRowContext(r.ctx, query, asset, c.after), r.library)
	if errors.Is(err, ErrNotFound) {
		c.done = true
		c.value = nil
		return nil
	}
	if err != nil {
		return err
	}
	r.budget.relationships++
	c.after = v.ID
	c.value = &v
	return nil
}
func (r *discoveryReader) relationships(asset continuity.AssetID, fn func(Relationship) (bool, error)) (bool, error) {
	left, right := relationshipCursor{source: true}, relationshipCursor{}
	if err := r.nextRelationship(asset, &left); err != nil {
		return false, err
	}
	if err := r.nextRelationship(asset, &right); err != nil {
		return false, err
	}
	for left.value != nil || right.value != nil {
		if err := r.ctx.Err(); err != nil {
			return false, err
		}
		var v Relationship
		takeLeft, takeRight := false, false
		if right.value == nil || left.value != nil && left.value.ID < right.value.ID {
			v = *left.value
			takeLeft = true
		} else if left.value == nil || right.value.ID < left.value.ID {
			v = *right.value
			takeRight = true
		} else {
			v = *left.value
			takeLeft, takeRight = true, true
		}
		if r.edgeAllowed(v.Value) {
			if err := r.edgeMembership(v.Value); err != nil {
				return false, err
			}
			stop, err := fn(v)
			if err != nil {
				return false, err
			}
			if stop {
				return false, nil
			}
		}
		if takeLeft {
			if err := r.nextRelationship(asset, &left); err != nil {
				return false, err
			}
		}
		if takeRight {
			if err := r.nextRelationship(asset, &right); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}
func (r *discoveryReader) locations(asset continuity.AssetID, fn func(Location) (bool, error)) (bool, error) {
	after := ""
	for {
		if err := r.ctx.Err(); err != nil {
			return false, err
		}
		if r.budget.locations >= 4096 {
			return false, errDiscoveryWork
		}
		if r.budget.endpoints+2 > 8192 {
			return false, errDiscoveryWork
		}
		r.budget.endpoints += 2
		query := strings.Replace(locationSelect, "FROM locations l ", "FROM locations l INDEXED BY locations_asset_active ", 1)
		v, err := scanLocation(r.q.QueryRowContext(r.ctx, query+` WHERE l.asset_id=? AND l.removed=0 AND l.location_id>? ORDER BY l.location_id LIMIT 1`, asset, after), r.library)
		if errors.Is(err, ErrNotFound) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		r.budget.locations++
		stop, err := fn(v)
		if err != nil {
			return false, err
		}
		if stop {
			return false, nil
		}
		after = v.ID
	}
}
func (r *discoveryReader) counts(asset continuity.AssetID) (discoveryCounts, error) {
	counts := discoveryCounts{Version: 1, AssetID: asset}
	if r.rights.allows(asset, "revisions.read") {
		n := 0
		_, err := r.revisions(asset, func(discoveryRevision) (bool, error) { n++; return false, nil })
		if err != nil {
			return counts, err
		}
		counts.Revisions = &n
	}
	if r.rights.allows(asset, "relationships.read") {
		n := 0
		_, err := r.relationships(asset, func(Relationship) (bool, error) { n++; return false, nil })
		if err != nil {
			return counts, err
		}
		counts.Relationships = &n
	}
	if r.rights.allows(asset, "locations.read") {
		n := 0
		_, err := r.locations(asset, func(Location) (bool, error) { n++; return false, nil })
		if err != nil {
			return counts, err
		}
		counts.Locations = &n
	}
	return counts, nil
}
