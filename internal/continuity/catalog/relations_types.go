package catalog

import "github.com/lelanddutcher/juicemount/internal/continuity"

// Provenance labels a caller's attribution, not authentication or authority.
type Provenance string

const (
	Observed Provenance = "observed"
	Claimed  Provenance = "claimed"
	Inferred Provenance = "inferred"
)

type RelationshipValue struct {
	SourceAssetID  continuity.AssetID    `json:"source_asset_id"`
	SourceRevision continuity.RevisionID `json:"source_revision"`
	TargetAssetID  continuity.AssetID    `json:"target_asset_id"`
	TargetRevision continuity.RevisionID `json:"target_revision"`
	Predicate      string                `json:"predicate"`
	Provenance     Provenance            `json:"provenance"`
	Source         string                `json:"source"`
}

type LocationValue struct {
	AssetID  continuity.AssetID    `json:"asset_id"`
	Revision continuity.RevisionID `json:"revision"`
	Source   string                `json:"source"`
	Locator  string                `json:"locator"`
}

type RelationshipExpected struct {
	Value           RelationshipValue `json:"value"`
	UpdatedSequence int64             `json:"updated_sequence"`
	Removed         bool              `json:"removed"`
}

type LocationExpected struct {
	Value           LocationValue `json:"value"`
	UpdatedSequence int64         `json:"updated_sequence"`
	Removed         bool          `json:"removed"`
}

type PutRelationship struct {
	ID       string                `json:"id"`
	Expected *RelationshipExpected `json:"expected"`
	Value    RelationshipValue     `json:"value"`
}

type RemoveRelationship struct {
	ID       string                `json:"id"`
	Expected *RelationshipExpected `json:"expected"`
}

type PutLocation struct {
	ID       string            `json:"id"`
	Expected *LocationExpected `json:"expected"`
	Value    LocationValue     `json:"value"`
}

type RemoveLocation struct {
	ID       string            `json:"id"`
	Expected *LocationExpected `json:"expected"`
}

// Tombstones retain their last value and lifetime identity.
type Relationship struct {
	LibraryID       continuity.LibraryID
	ID              string
	Value           RelationshipValue
	CreatedSequence int64
	UpdatedSequence int64
	Removed         bool
}

type Location struct {
	LibraryID       continuity.LibraryID
	ID              string
	Value           LocationValue
	CreatedSequence int64
	UpdatedSequence int64
	Removed         bool
}

func validOptionalRevision(id continuity.RevisionID) bool { return id == "" || id.Validate() == nil }

func (v RelationshipValue) valid() bool {
	return v.SourceAssetID.Validate() == nil && v.TargetAssetID.Validate() == nil &&
		validOptionalRevision(v.SourceRevision) && validOptionalRevision(v.TargetRevision) &&
		validText(v.Predicate, 128, true) && validText(v.Source, 256, true) &&
		(v.Provenance == Observed || v.Provenance == Claimed || v.Provenance == Inferred)
}

func (v LocationValue) valid() bool {
	return v.AssetID.Validate() == nil && validOptionalRevision(v.Revision) &&
		validText(v.Source, 256, true) && validText(v.Locator, 8192, true)
}

func freezeRelationshipExpected(v *RelationshipExpected) (*RelationshipExpected, error) {
	if v == nil {
		return nil, nil
	}
	copy := *v
	if copy.UpdatedSequence <= 0 || !copy.Value.valid() {
		return nil, invalid("expected relationship state")
	}
	return &copy, nil
}

func freezeLocationExpected(v *LocationExpected) (*LocationExpected, error) {
	if v == nil {
		return nil, nil
	}
	copy := *v
	if copy.UpdatedSequence <= 0 || !copy.Value.valid() {
		return nil, invalid("expected location state")
	}
	return &copy, nil
}
