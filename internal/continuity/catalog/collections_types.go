package catalog

import (
	"encoding/json"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const (
	MaxCollectionRequestBytes = 32 << 10
	MaxCollectionRecordBytes  = 33792
	MaxCollectionReceiptBytes = 1024
)

type CollectionID string
type CollectionEntryID string
type CollectionVersion string
type CollectionGrantID string
type CollectionGrantVersion string
type CollectionOperationID string
type CollectionPolicyOperationID string

func collectionIDError(ok bool) error {
	if !ok {
		return invalid("collection identity")
	}
	return nil
}
func (id CollectionID) Validate() error { return collectionIDError(validRandomID(string(id), "col_")) }
func (id CollectionEntryID) Validate() error {
	return collectionIDError(validRandomID(string(id), "cent_"))
}
func (id CollectionVersion) Validate() error {
	return collectionIDError(validRandomID(string(id), "cv_"))
}
func (id CollectionGrantID) Validate() error {
	return collectionIDError(validRandomID(string(id), "cgr_"))
}
func (id CollectionGrantVersion) Validate() error {
	return collectionIDError(validRandomID(string(id), "cgv_"))
}
func (id CollectionOperationID) Validate() error {
	return collectionIDError(validRandomID(string(id), "cop_"))
}
func (id CollectionPolicyOperationID) Validate() error {
	return collectionIDError(validRandomID(string(id), "cpo_"))
}

// CollectionRequest contains no actor: a later authenticated admission derives it.
type CollectionRequest struct {
	Version         int                   `json:"version"`
	LibraryID       continuity.LibraryID  `json:"library_id"`
	ID              CollectionOperationID `json:"operation_id"`
	CollectionID    CollectionID          `json:"collection_id"`
	ExpectedVersion CollectionVersion     `json:"expected_version"`
	Kind            string                `json:"kind"`
	Value           json.RawMessage       `json:"value"`
}
type CreateCollection struct {
	Name           string               `json:"name"`
	CreatorGrantID CollectionGrantID    `json:"creator_grant_id"`
	Entries        []NewCollectionEntry `json:"entries"`
}
type NewCollectionEntry struct {
	ID      CollectionEntryID  `json:"entry_id"`
	AssetID continuity.AssetID `json:"asset_id"`
}
type RenameCollection struct {
	Name string `json:"name"`
}
type SetCollectionArchived struct {
	Archived bool `json:"archived"`
}
type EditCollectionEntries struct {
	Changes []CollectionEntryChange `json:"changes"`
}
type CollectionEntryChange struct {
	Kind    string             `json:"kind"`
	ID      CollectionEntryID  `json:"entry_id"`
	AssetID continuity.AssetID `json:"asset_id"`
}
type EditCollectionGrants struct {
	Changes []CollectionGrantChange `json:"changes"`
}

// CollectionRecord preserves source-recorded attribution, not a new authentication
// claim on import. Cross-record authority, identity and historical ABA checks
// belong to transactional admission and replay, not these pure codecs.
type CollectionRecord struct {
	Version                    int                         `json:"version"`
	Request                    CollectionRequest           `json:"request"`
	ActorSubjectID             SubjectID                   `json:"actor_subject_id"`
	ResultVersion              CollectionVersion           `json:"result_version"`
	GeneratedPolicyOperationID CollectionPolicyOperationID `json:"generated_policy_operation_id"`
	RecordedUnixNano           int64                       `json:"recorded_unix_nano"`
}

// CollectionReceipt confirms only a historical operation, never current state.
type CollectionReceipt struct {
	Version       int                   `json:"version"`
	OperationID   CollectionOperationID `json:"operation_id"`
	CollectionID  CollectionID          `json:"collection_id"`
	ResultVersion CollectionVersion     `json:"result_version"`
}
