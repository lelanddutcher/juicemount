// Package catalog persists continuity declarations and metadata operations.
// Its records do not prove source stability, authorization, or media publication.
package catalog

import (
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

var (
	ErrInvalid         = errors.New("invalid catalog input")
	ErrNotFound        = errors.New("catalog record not found")
	ErrConflict        = errors.New("catalog optimistic conflict")
	ErrChangedRequest  = errors.New("catalog operation ID reused with changed request")
	ErrClosed          = errors.New("catalog closed")
	ErrFormat          = errors.New("invalid or corrupt catalog format")
	ErrUpgradeRequired = errors.New("catalog requires explicit schema upgrade")
)

const (
	OperationVersion = 1
	MaxChanges       = 256
	MaxRequestBytes  = 1 << 20
	MaxPageSize      = 1000
)

// Operation is a trusted-service request; Actor is attribution, not authentication.
type Operation struct {
	Version   int
	LibraryID continuity.LibraryID
	ID        string
	Actor     string
	Changes   []Change
}

// Change has exactly one non-nil variant. The ordered batch is atomic.
type Change struct {
	CreateAsset        *CreateAsset
	RenameAsset        *RenameAsset
	RecordRevision     *continuity.Manifest
	AdvanceHead        *AdvanceHead
	PutRelationship    *PutRelationship
	RemoveRelationship *RemoveRelationship
	PutLocation        *PutLocation
	RemoveLocation     *RemoveLocation
}

type CreateAsset struct {
	AssetID continuity.AssetID `json:"asset_id"`
	Label   string             `json:"label"`
}

type RenameAsset struct {
	AssetID       continuity.AssetID `json:"asset_id"`
	ExpectedLabel string             `json:"expected_label"`
	Label         string             `json:"label"`
}

type AdvanceHead struct {
	AssetID          continuity.AssetID    `json:"asset_id"`
	ExpectedRevision continuity.RevisionID `json:"expected_revision"`
	Revision         continuity.RevisionID `json:"revision"`
}

// Receipt is stable across an identical operation retry. Sequence orders events;
// the informational wall clock neither orders operations nor proves human activity.
type Receipt struct {
	Version          int                  `json:"version"`
	LibraryID        continuity.LibraryID `json:"library_id"`
	OperationID      string               `json:"operation_id"`
	Sequence         int64                `json:"sequence"`
	PayloadSHA256    string               `json:"payload_sha256"`
	RecordedUnixNano int64                `json:"recorded_unix_nano"`
}

type Asset struct {
	LibraryID       continuity.LibraryID
	ID              continuity.AssetID
	Label           string
	Head            continuity.RevisionID
	CreatedSequence int64
	UpdatedSequence int64
}

type Revision struct {
	ID              continuity.RevisionID
	Manifest        continuity.Manifest
	Canonical       []byte
	CreatedSequence int64
}

type Event struct {
	Receipt   Receipt
	Operation Operation
}
