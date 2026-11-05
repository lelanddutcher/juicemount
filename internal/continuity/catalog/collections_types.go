package catalog

import (
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// MaxCollectionMembers bounds a collection's active members, so a host can
// check every member of one collection within a request.
const MaxCollectionMembers = 10000

// ErrCollectionFull: the operation would leave a collection with more than
// MaxCollectionMembers active members. Nothing was applied.
var ErrCollectionFull = errors.New("catalog collection member limit reached")

// CollectionID is `col_` and 32 lowercase hex digits, not all zero.
type CollectionID string

func (id CollectionID) Validate() error {
	s := string(id)
	if len(s) != 36 || s[:4] != "col_" {
		return invalid("collection identity")
	}
	zero := true
	for _, c := range s[4:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return invalid("collection identity")
		}
		zero = zero && c == '0'
	}
	if zero {
		return invalid("collection identity")
	}
	return nil
}

func validCollectionName(name string) bool { return validText(name, 256, true) }

// CreateCollection reserves a new identity; it conflicts with any existing
// collection, including a removed one.
type CreateCollection struct {
	CollectionID CollectionID `json:"collection_id"`
	Name         string       `json:"name"`
}

// RenameCollection sets the name of an active collection.
type RenameCollection struct {
	CollectionID CollectionID `json:"collection_id"`
	Name         string       `json:"name"`
}

// RemoveCollection tombstones an active collection. Its member rows stay as
// they are; a removed collection accepts no further change.
type RemoveCollection struct {
	CollectionID CollectionID `json:"collection_id"`
}

// AddCollectionMember makes an existing asset an active member of an active
// collection: it inserts or reactivates the row, and leaves an already active
// member unchanged. Membership is a set, so the request alone decides the
// result, whatever the state it meets.
type AddCollectionMember struct {
	CollectionID CollectionID       `json:"collection_id"`
	AssetID      continuity.AssetID `json:"asset_id"`
}

// RemoveCollectionMember tombstones an active member of an active collection
// and leaves a removed or never-added one unchanged.
type RemoveCollectionMember struct {
	CollectionID CollectionID       `json:"collection_id"`
	AssetID      continuity.AssetID `json:"asset_id"`
}

// Collection includes tombstones; Members counts active members.
type Collection struct {
	LibraryID       continuity.LibraryID
	ID              CollectionID
	Name            string
	Members         int64
	CreatedSequence int64
	UpdatedSequence int64
	Removed         bool
}

// CollectionMember is one membership row. A member is an asset identity: it
// says nothing about whether the asset's file still exists.
type CollectionMember struct {
	CollectionID    CollectionID
	AssetID         continuity.AssetID
	CreatedSequence int64
	UpdatedSequence int64
	Removed         bool
}
