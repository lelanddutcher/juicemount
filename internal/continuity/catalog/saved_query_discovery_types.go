package catalog

import (
	"encoding/json"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// SavedQueryReadRequest is the saved-query-only v2 family. Page requests bind
// QueryKind and CollectionID before looking up their opaque token.
type SavedQueryReadRequest struct {
	Version      int
	Kind         string
	LibraryID    continuity.LibraryID
	CollectionID CollectionID
	State        string
	PageSize     int
	QueryKind    string
	Token        string
}

type SavedQueryPageBody struct {
	Version      int                  `json:"version"`
	LibraryID    continuity.LibraryID `json:"library_id"`
	QueryKind    string               `json:"query_kind"`
	CollectionID CollectionID         `json:"collection_id"`
	Token        string               `json:"token"`
}

// SavedQuerySummary has the frozen summary shape, with Kind="saved_query".
type SavedQuerySummary = CollectionSummary
type SavedQueryOpenResponse struct {
	Version    int               `json:"version"`
	Kind       string            `json:"kind"`
	Collection SavedQuerySummary `json:"collection"`
}
type SavedQueryDefinitionResponse struct {
	Version    int                  `json:"version"`
	Kind       string               `json:"kind"`
	Collection SavedQuerySummary    `json:"collection"`
	Definition SavedQueryDefinition `json:"definition"`
}
type SavedQueryResultID struct {
	CollectionID CollectionID       `json:"collection_id"`
	AssetID      continuity.AssetID `json:"asset_id"`
}
type SavedQueryResult struct {
	ResultID       SavedQueryResultID     `json:"result_id"`
	AssetID        continuity.AssetID     `json:"asset_id"`
	Label          string                 `json:"label"`
	HeadRevisionID *continuity.RevisionID `json:"head_revision_id"`
	Reasons        []string               `json:"reasons"`
}
type SavedQueryDirectoryResponse struct {
	Version        int                 `json:"version"`
	Kind           string              `json:"kind"`
	Collection     *SavedQuerySummary  `json:"collection"`
	CanCreate      bool                `json:"can_create"`
	Items          []SavedQuerySummary `json:"items"`
	MatchedCount   int                 `json:"matched_count"`
	CountScope     string              `json:"count_scope"`
	WindowComplete bool                `json:"window_complete"`
	NextPageToken  *string             `json:"next_page_token"`
	SortVersion    string              `json:"sort_version"`
	SnapshotScope  string              `json:"snapshot_scope"`
}
type SavedQueryExecuteResponse struct {
	Version        int                `json:"version"`
	Kind           string             `json:"kind"`
	Collection     SavedQuerySummary  `json:"collection"`
	QueryVersion   CollectionVersion  `json:"query_version"`
	Items          []SavedQueryResult `json:"items"`
	MatchedCount   int                `json:"matched_count"`
	CountScope     string             `json:"count_scope"`
	WindowComplete bool               `json:"window_complete"`
	NextPageToken  *string            `json:"next_page_token"`
	SortVersion    string             `json:"sort_version"`
	SnapshotScope  string             `json:"snapshot_scope"`
}
type SavedQueryListResponse[T any] struct {
	Version        int               `json:"version"`
	Kind           string            `json:"kind"`
	Collection     SavedQuerySummary `json:"collection"`
	Items          []T               `json:"items"`
	MatchedCount   int               `json:"matched_count"`
	CountScope     string            `json:"count_scope"`
	WindowComplete bool              `json:"window_complete"`
	NextPageToken  *string           `json:"next_page_token"`
	SortVersion    string            `json:"sort_version"`
	SnapshotScope  string            `json:"snapshot_scope"`
}
type SavedQueryHistoryCreate struct {
	Name       string               `json:"name"`
	Definition SavedQueryDefinition `json:"definition"`
}

func ValidateSavedQueryReadRequest(r SavedQueryReadRequest, library continuity.LibraryID) (SavedQueryReadRequest, error) {
	bad := func() (SavedQueryReadRequest, error) {
		return SavedQueryReadRequest{}, invalid("saved query read request")
	}
	if r.Version != 2 || library.Validate() != nil || r.LibraryID != library {
		return bad()
	}
	if r.Kind == "page" {
		if r.State != "" || r.PageSize != 0 || !validPageToken(r.Token) {
			return bad()
		}
		switch r.QueryKind {
		case "directory":
			if r.CollectionID != "" {
				return bad()
			}
		case "execute", "history", "access":
			if r.CollectionID.Validate() != nil {
				return bad()
			}
		default:
			return bad()
		}
		return r, nil
	}
	if r.Token != "" || r.QueryKind != "" {
		return bad()
	}
	if r.Kind == "directory" {
		if r.CollectionID != "" || r.State != "active" && r.State != "archived" && r.State != "all" {
			return bad()
		}
	} else if r.CollectionID.Validate() != nil || r.State != "" {
		return bad()
	}
	switch r.Kind {
	case "open", "definition":
		if r.PageSize != 0 {
			return bad()
		}
	case "directory", "execute", "history", "access":
		if r.PageSize < 1 || r.PageSize > 100 {
			return bad()
		}
	default:
		return bad()
	}
	return r, nil
}

// DecodeSavedQueryCLIReadRequest accepts the closed canonical initial inner
// CLI shape; transport-owned continuation is deliberately not decoded here.
func DecodeSavedQueryCLIReadRequest(data []byte, library continuity.LibraryID) (SavedQueryReadRequest, error) {
	var r SavedQueryReadRequest
	bad := func() (SavedQueryReadRequest, error) {
		return SavedQueryReadRequest{}, invalid("saved query read JSON")
	}
	if len(data) == 0 || len(data) > MaxSavedQueryRequestBytes {
		return bad()
	}
	var tag struct {
		Kind string `json:"kind"`
	}
	if json.Unmarshal(data, &tag) != nil {
		return bad()
	}
	switch tag.Kind {
	case "directory":
		var v CollectionCLIDirectoryRequest
		if decodeCanonicalValue(data, &v) != nil {
			return bad()
		}
		r = SavedQueryReadRequest{Version: v.Version, Kind: v.Kind, LibraryID: v.LibraryID, State: v.State, PageSize: v.PageSize}
	case "open", "definition":
		var v CollectionCLIOpenRequest
		if decodeCanonicalValue(data, &v) != nil {
			return bad()
		}
		r = SavedQueryReadRequest{Version: v.Version, Kind: v.Kind, LibraryID: v.LibraryID, CollectionID: v.CollectionID}
	case "execute", "history", "access":
		var v CollectionCLIListRequest
		if decodeCanonicalValue(data, &v) != nil {
			return bad()
		}
		r = SavedQueryReadRequest{Version: v.Version, Kind: v.Kind, LibraryID: v.LibraryID, CollectionID: v.CollectionID, PageSize: v.PageSize}
	default:
		return bad()
	}
	return ValidateSavedQueryReadRequest(r, library)
}

func savedQueryReadBytes(r SavedQueryReadRequest) ([]byte, error) {
	var v any
	switch r.Kind {
	case "directory":
		v = CollectionCLIDirectoryRequest{r.Version, r.Kind, r.LibraryID, r.State, r.PageSize}
	case "open", "definition":
		v = CollectionCLIOpenRequest{r.Version, r.Kind, r.LibraryID, r.CollectionID}
	default:
		v = CollectionCLIListRequest{r.Version, r.Kind, r.LibraryID, r.CollectionID, r.PageSize}
	}
	b, err := json.Marshal(v)
	return exactReadBytes(b), err
}

func exactReadBytes(b []byte) []byte { out := make([]byte, len(b)); copy(out, b); return out }
