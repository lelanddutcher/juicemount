package catalog

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

var (
	ErrDiscoveryDraining = errors.New("discovery drain incomplete")
	errDiscoveryRestart  = errors.New("restart_query")
	errDiscoveryWork     = errors.New("work_budget")
	errDiscoveryBusy     = errors.New("busy")
	errDiscoveryStop     = errors.New("discovery window full")
)

func DiscoveryErrorCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrUnauthenticated):
		return "unauthenticated"
	case errors.Is(err, ErrInvalid):
		return "invalid"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrChangedRequest), errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, errPreferenceExpired):
		return "expired_request"
	case errors.Is(err, errDiscoveryRestart):
		return "restart_query"
	case errors.Is(err, errDiscoveryWork):
		return "work_budget"
	case errors.Is(err, errDiscoveryBusy):
		return "busy"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "deadline"
	default:
		return "unavailable"
	}
}

type DiscoveryRequest struct {
	Version         int
	Kind            string
	LibraryID       continuity.LibraryID
	Text            string
	ContextAssetIDs []continuity.AssetID
	AssetID         continuity.AssetID
	PageSize        int
	Token           string
}
type DisclosureResult struct {
	Started   bool
	SubjectID SubjectID
	Selector  string
}

func normalizeDiscoveryText(s string) string {
	return foldDiscoveryText(strings.Trim(s, " \t\r\n\v\f"))
}
func foldDiscoveryText(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
func validPageToken(token string) bool {
	if len(token) != 70 || !strings.HasPrefix(token, "page1_") || strings.ToLower(token) != token {
		return false
	}
	_, err := hex.DecodeString(token[6:])
	return err == nil
}
func freezeDiscoveryRequest(r DiscoveryRequest, library continuity.LibraryID) (DiscoveryRequest, error) {
	if (r.Version != 1 && r.Version != 2) || r.LibraryID != library || r.Version == 2 && r.Kind != "search" && r.Kind != "page" {
		return r, invalid("discovery identity")
	}
	if r.Kind == "page" {
		if !validPageToken(r.Token) || r.Text != "" || len(r.ContextAssetIDs) != 0 || r.AssetID != "" || r.PageSize != 0 {
			return r, invalid("page request")
		}
		return r, nil
	}
	if r.Token != "" {
		return r, invalid("unexpected page token")
	}
	if r.Kind == "search" {
		if r.AssetID != "" || !validText(r.Text, 256, false) || len(r.ContextAssetIDs) > 64 || r.PageSize < 1 || r.PageSize > 100 {
			return r, invalid("search request")
		}
		seen := make(map[continuity.AssetID]bool)
		r.ContextAssetIDs = append([]continuity.AssetID(nil), r.ContextAssetIDs...)
		for _, id := range r.ContextAssetIDs {
			if id.Validate() != nil || seen[id] {
				return r, invalid("context identities")
			}
			seen[id] = true
		}
		r.Text = normalizeDiscoveryText(r.Text)
		return r, nil
	}
	if r.AssetID.Validate() != nil || r.Text != "" || len(r.ContextAssetIDs) != 0 {
		return r, invalid("asset request")
	}
	switch r.Kind {
	case "asset", "counts":
		if r.PageSize != 0 {
			return r, invalid("unexpected page size")
		}
	case "revisions", "history", "relationships", "locations":
		if r.PageSize < 1 || r.PageSize > 100 {
			return r, invalid("page size")
		}
	default:
		return r, invalid("discovery kind")
	}
	return r, nil
}

type discoveryAsset struct {
	AssetID continuity.AssetID     `json:"asset_id"`
	Label   string                 `json:"label"`
	Head    *continuity.RevisionID `json:"head_revision_id"`
	Reasons []string               `json:"reasons"`
}
type discoveryRevision struct {
	RevisionID continuity.RevisionID   `json:"revision_id"`
	AssetID    continuity.AssetID      `json:"asset_id"`
	Content    contentJSON             `json:"content"`
	Parents    []continuity.RevisionID `json:"parents"`
}
type discoveryRelationship struct {
	ID             string                 `json:"relationship_id"`
	SourceAssetID  continuity.AssetID     `json:"source_asset_id"`
	SourceRevision *continuity.RevisionID `json:"source_revision_id"`
	TargetAssetID  continuity.AssetID     `json:"target_asset_id"`
	TargetRevision *continuity.RevisionID `json:"target_revision_id"`
	Predicate      string                 `json:"predicate"`
	Provenance     Provenance             `json:"provenance"`
	Source         string                 `json:"source"`
}
type discoveryLocation struct {
	ID       string                 `json:"location_id"`
	AssetID  continuity.AssetID     `json:"asset_id"`
	Revision *continuity.RevisionID `json:"revision_id"`
	Source   string                 `json:"source"`
	Locator  string                 `json:"locator"`
}
type discoveryCounts struct {
	Version       int                `json:"version"`
	AssetID       continuity.AssetID `json:"asset_id"`
	Revisions     *int               `json:"revision_count"`
	Relationships *int               `json:"relationship_count"`
	Locations     *int               `json:"location_count"`
}
type discoveryHistory struct {
	Kind      string `json:"kind"`
	Timestamp int64  `json:"recorded_unix_nano"`
	Actor     string `json:"actor_claim"`
	Before    any    `json:"before"`
	After     any    `json:"after"`
}
type historyAsset struct {
	AssetID continuity.AssetID `json:"asset_id"`
	Label   string             `json:"label"`
}
type historyHead struct {
	AssetID  continuity.AssetID     `json:"asset_id"`
	Revision *continuity.RevisionID `json:"revision_id"`
}
type historyRelationship struct {
	discoveryRelationship
	Removed bool `json:"removed"`
}
type historyLocation struct {
	discoveryLocation
	Removed bool `json:"removed"`
}

func disclosedRevision(id continuity.RevisionID, allowed bool) *continuity.RevisionID {
	if !allowed || id == "" {
		return nil
	}
	copy := id
	return &copy
}
func revisionDTO(m continuity.Manifest, id continuity.RevisionID) discoveryRevision {
	parents := append(make([]continuity.RevisionID, 0, len(m.Parents)), m.Parents...)
	return discoveryRevision{id, m.AssetID, contentJSON{m.Content.SHA256, m.Content.Size}, parents}
}
