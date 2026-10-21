package catalog

import (
	"encoding/json"
	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type CollectionQueryRequest struct {
	Version      int
	Kind         string
	LibraryID    continuity.LibraryID
	CollectionID CollectionID
	State        string
	PageSize     int
	Token        string
}

type CollectionDirectoryBody struct {
	Version   int                  `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	State     string               `json:"state"`
	PageSize  int                  `json:"page_size"`
}
type CollectionOpenBody struct {
	Version      int                  `json:"version"`
	LibraryID    continuity.LibraryID `json:"library_id"`
	CollectionID CollectionID         `json:"collection_id"`
}
type CollectionMembersBody struct {
	Version      int                  `json:"version"`
	LibraryID    continuity.LibraryID `json:"library_id"`
	CollectionID CollectionID         `json:"collection_id"`
	State        string               `json:"state"`
	PageSize     int                  `json:"page_size"`
}
type CollectionListBody struct { // history and access
	Version      int                  `json:"version"`
	LibraryID    continuity.LibraryID `json:"library_id"`
	CollectionID CollectionID         `json:"collection_id"`
	PageSize     int                  `json:"page_size"`
}
type CollectionPageBody struct { // HTTP only; internal CLI continuation uses domain request
	Version   int                  `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	Token     string               `json:"token"`
}

type CollectionCLIReadEnvelope struct {
	Version int             `json:"version"`
	Bearer  string          `json:"bearer"`
	Request json.RawMessage `json:"request"`
}
type CollectionCLIDirectoryRequest struct {
	Version   int                  `json:"version"`
	Kind      string               `json:"kind"`
	LibraryID continuity.LibraryID `json:"library_id"`
	State     string               `json:"state"`
	PageSize  int                  `json:"page_size"`
}
type CollectionCLIOpenRequest struct {
	Version      int                  `json:"version"`
	Kind         string               `json:"kind"`
	LibraryID    continuity.LibraryID `json:"library_id"`
	CollectionID CollectionID         `json:"collection_id"`
}
type CollectionCLIMembersRequest struct {
	Version      int                  `json:"version"`
	Kind         string               `json:"kind"`
	LibraryID    continuity.LibraryID `json:"library_id"`
	CollectionID CollectionID         `json:"collection_id"`
	State        string               `json:"state"`
	PageSize     int                  `json:"page_size"`
}
type CollectionCLIListRequest struct { // history or access, checked exact literal Kind
	Version      int                  `json:"version"`
	Kind         string               `json:"kind"`
	LibraryID    continuity.LibraryID `json:"library_id"`
	CollectionID CollectionID         `json:"collection_id"`
	PageSize     int                  `json:"page_size"`
}

type CollectionPermissions struct {
	Read  bool `json:"read"`
	Edit  bool `json:"edit"`
	Admin bool `json:"admin"`
}
type CollectionSummary struct {
	CollectionID      CollectionID          `json:"collection_id"`
	Kind              string                `json:"kind"` // exactly "manual"
	Name              string                `json:"name"` // full current source-validated name
	Archived          bool                  `json:"archived"`
	DefinitionVersion CollectionVersion     `json:"definition_version"`
	Creator           string                `json:"creator"` // "you" | "another_authorized_person"
	Permissions       CollectionPermissions `json:"permissions"`
}
type CollectionOpenResponse struct {
	Version    int               `json:"version"` // 1
	Kind       string            `json:"kind"`    // "collection" (not "open")
	Collection CollectionSummary `json:"collection"`
}
type CollectionDirectoryResponse struct {
	Version        int                 `json:"version"`    // 1
	Kind           string              `json:"kind"`       // "directory"
	Collection     *CollectionSummary  `json:"collection"` // MUST be null, never omitted
	CanCreate      bool                `json:"can_create"`
	Items          []CollectionSummary `json:"items"` // never null
	MatchedCount   int                 `json:"matched_count"`
	CountScope     string              `json:"count_scope"`     // "frozen_window"
	WindowComplete bool                `json:"window_complete"` // true after complete authorized scan
	NextPageToken  *string             `json:"next_page_token"` // null at end, never empty string
	SortVersion    string              `json:"sort_version"`    // "collections1"
	SnapshotScope  string              `json:"snapshot_scope"`  // "frozen_snapshot"
}
type CollectionMember struct {
	EntryID        CollectionEntryID      `json:"entry_id"`
	AssetID        continuity.AssetID     `json:"asset_id"`
	Label          string                 `json:"label"`            // full current valid UTF-8, 0..4096 bytes, no NUL
	HeadRevisionID *continuity.RevisionID `json:"head_revision_id"` // null without revision permission/no head
	Removed        bool                   `json:"removed"`
}
type CollectionMembersResponse struct {
	Version        int                `json:"version"`    // 1
	Kind           string             `json:"kind"`       // "members"
	Collection     CollectionSummary  `json:"collection"` // nonnull
	State          string             `json:"state"`      // exact original "active" | "removed"
	Items          []CollectionMember `json:"items"`      // never null
	MatchedCount   int                `json:"matched_count"`
	CountScope     string             `json:"count_scope"`     // "frozen_visible_membership"
	WindowComplete bool               `json:"window_complete"` // true, not a claim of all hidden members
	NextPageToken  *string            `json:"next_page_token"`
	SortVersion    string             `json:"sort_version"`   // "collections1"
	SnapshotScope  string             `json:"snapshot_scope"` // "frozen_membership_current_page"
}

type CollectionAccessItem struct {
	GrantID  CollectionGrantID       `json:"grant_id"`
	Expected ExpectedCollectionGrant `json:"expected"`
}

type CollectionHistoryItem struct {
	Kind             string          `json:"kind"`               // create | rename | entries | archive
	RecordedUnixNano string          `json:"recorded_unix_nano"` // positive base-10 int64, no +/leading zero
	Actor            string          `json:"actor"`              // you | another_authorized_person
	Value            json.RawMessage `json:"value"`              // exactly one nonnull typed object below
}
type CollectionHistoryCreate struct {
	Name    string               `json:"name"`
	Entries []NewCollectionEntry `json:"entries"` // [] for empty create; never null
}

type CollectionListResponse[T any] struct {
	Version        int               `json:"version"` // 1
	Kind           string            `json:"kind"`
	Collection     CollectionSummary `json:"collection"`
	Items          []T               `json:"items"`
	MatchedCount   int               `json:"matched_count"`
	CountScope     string            `json:"count_scope"` // "frozen_window"
	WindowComplete bool              `json:"window_complete"`
	NextPageToken  *string           `json:"next_page_token"`
	SortVersion    string            `json:"sort_version"`   // "collections1"
	SnapshotScope  string            `json:"snapshot_scope"` // "frozen_snapshot"
}
