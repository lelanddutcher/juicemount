package catalog

import (
	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type RecentsReadRequest struct {
	Version   int
	Kind      string
	LibraryID continuity.LibraryID
	PageSize  int
	Token     string
}
type RecentsListBody struct {
	Version   int                  `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	PageSize  int                  `json:"page_size"`
}
type RecentsCLIListRequest struct {
	Version   int                  `json:"version"`
	Kind      string               `json:"kind"`
	LibraryID continuity.LibraryID `json:"library_id"`
	PageSize  int                  `json:"page_size"`
}
type RecentsItem struct {
	AssetID        continuity.AssetID     `json:"asset_id"`
	Label          string                 `json:"label"`
	HeadRevisionID *continuity.RevisionID `json:"head_revision_id"`
	Reasons        []string               `json:"reasons"`
	SelectedUnix   string                 `json:"selected_unix"`
	ExpiresUnix    string                 `json:"expires_unix"`
}
type RecentsResponse struct {
	Version        int           `json:"version"`
	Kind           string        `json:"kind"`
	Items          []RecentsItem `json:"items"`
	MatchedCount   int           `json:"matched_count"`
	CountScope     string        `json:"count_scope"`
	WindowComplete bool          `json:"window_complete"`
	NextPageToken  *string       `json:"next_page_token"`
	SortVersion    string        `json:"sort_version"`
	SnapshotScope  string        `json:"snapshot_scope"`
}

func ValidateRecentsReadRequest(r RecentsReadRequest, library continuity.LibraryID) (RecentsReadRequest, error) {
	if r.Version != 2 || library.Validate() != nil || r.LibraryID != library {
		return RecentsReadRequest{}, invalid("recents read request")
	}
	if r.Kind == "list" && r.PageSize >= 1 && r.PageSize <= 100 && r.Token == "" || r.Kind == "page" && r.PageSize == 0 && validPageToken(r.Token) {
		return r, nil
	}
	return RecentsReadRequest{}, invalid("recents read request")
}
func DecodeRecentsCLIReadRequest(data []byte, library continuity.LibraryID) (RecentsReadRequest, error) {
	var v RecentsCLIListRequest
	if len(data) == 0 || len(data) > MaxSavedQueryRequestBytes || decodeCanonicalValue(data, &v) != nil || v.Kind != "list" {
		return RecentsReadRequest{}, invalid("recents read JSON")
	}
	return ValidateRecentsReadRequest(RecentsReadRequest{Version: v.Version, Kind: v.Kind, LibraryID: v.LibraryID, PageSize: v.PageSize}, library)
}
