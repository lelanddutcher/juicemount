package catalog

import (
	"encoding/json"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const (
	MaxSavedQueryDefinitionBytes = 8192
	MaxSavedQueryRequestBytes    = MaxCollectionRequestBytes
	MaxSavedQueryRecordBytes     = MaxCollectionRecordBytes
	MaxSavedQueryReceiptBytes    = MaxCollectionReceiptBytes
)

// SavedQueryDefinition describes a ranked asset query. Its anchors are syntax
// here; their existence, library membership and visibility require admission.
type SavedQueryDefinition struct {
	Version         int                  `json:"version"`
	Kind            string               `json:"kind"`
	Text            string               `json:"text"`
	ContextAssetIDs []continuity.AssetID `json:"context_asset_ids"`
}

// SavedQueryRequest is the version2 content family and carries no actor.
// It does not share a decoder with the version1 manual collection family.
type SavedQueryRequest struct {
	Version         int                   `json:"version"`
	LibraryID       continuity.LibraryID  `json:"library_id"`
	ID              CollectionOperationID `json:"operation_id"`
	CollectionID    CollectionID          `json:"collection_id"`
	ExpectedVersion CollectionVersion     `json:"expected_version"`
	Kind            string                `json:"kind"`
	Value           json.RawMessage       `json:"value"`
}

type CreateSavedQuery struct {
	Name           string               `json:"name"`
	CreatorGrantID CollectionGrantID    `json:"creator_grant_id"`
	Definition     SavedQueryDefinition `json:"definition"`
}

type ReplaceSavedQueryDefinition struct {
	Definition SavedQueryDefinition `json:"definition"`
}

// SavedQueryRecord retains source-recorded attribution. The derived child ID
// establishes syntax only; cross-record ownership and authority need admission.
type SavedQueryRecord struct {
	Version                    int                         `json:"version"`
	Request                    SavedQueryRequest           `json:"request"`
	ActorSubjectID             SubjectID                   `json:"actor_subject_id"`
	ResultVersion              CollectionVersion           `json:"result_version"`
	GeneratedPolicyOperationID CollectionPolicyOperationID `json:"generated_policy_operation_id"`
	RecordedUnixNano           int64                       `json:"recorded_unix_nano"`
}

// SavedQueryReceipt has no definition or current-state claim. Pairing it with
// a request or record is separate from decoding this minimal historical value.
type SavedQueryReceipt struct {
	Version       int                   `json:"version"`
	OperationID   CollectionOperationID `json:"operation_id"`
	CollectionID  CollectionID          `json:"collection_id"`
	ResultVersion CollectionVersion     `json:"result_version"`
}
