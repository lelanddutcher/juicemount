package catalog

import (
	"bytes"
	"encoding/json"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// DiscoveryAsset is the server's existing search projection. It is metadata,
// not a source locator, delivery selection, or proof of byte access.
type DiscoveryAsset = discoveryAsset

// DiscoverySearchPage is the typed form of the existing v2 search/page body.
// A page request returns the original search kind and frozen-window count.
type DiscoverySearchPage struct {
	Version      int              `json:"version"`
	Kind         string           `json:"kind"`
	Items        []DiscoveryAsset `json:"items"`
	MatchedCount int              `json:"matched_count"`
	CountScope   string           `json:"count_scope"`
	Complete     bool             `json:"window_complete"`
	Next         *string          `json:"next_page_token"`
	Sort         string           `json:"sort_version"`
}

type discoverySearchWire struct {
	Version         int                  `json:"version"`
	LibraryID       continuity.LibraryID `json:"library_id"`
	Text            string               `json:"text"`
	ContextAssetIDs []continuity.AssetID `json:"context_asset_ids"`
	PageSize        int                  `json:"page_size"`
}
type discoveryPageWire struct {
	Version   int                  `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	Token     string               `json:"token"`
}

// CanonicalNativeDiscoveryRequest encodes only the existing v2 search/page
// route fields. DiscoveryRequest itself intentionally has no JSON wire tags.
func CanonicalNativeDiscoveryRequest(r DiscoveryRequest, library continuity.LibraryID) ([]byte, error) {
	frozen, err := freezeDiscoveryRequest(r, library)
	if err != nil || frozen.Version != 2 || frozen.Kind != "search" && frozen.Kind != "page" {
		return nil, ErrInvalid
	}
	var value any
	if frozen.Kind == "search" {
		contextIDs := frozen.ContextAssetIDs
		if contextIDs == nil {
			contextIDs = []continuity.AssetID{}
		}
		value = discoverySearchWire{2, library, frozen.Text, contextIDs, frozen.PageSize}
	} else {
		value = discoveryPageWire{2, library, frozen.Token}
	}
	b, err := json.Marshal(value)
	if err != nil || len(b) > 32<<10 {
		return nil, ErrInvalid
	}
	return b, nil
}

// DecodeNativeDiscoveryRequest is for the helper's bounded stdin envelope.
// It rejects alternate, missing and unknown fields before any HTTP request.
func DecodeNativeDiscoveryRequest(kind string, b []byte, library continuity.LibraryID) (DiscoveryRequest, error) {
	if len(b) == 0 || len(b) > 32<<10 {
		return DiscoveryRequest{}, ErrInvalid
	}
	var r DiscoveryRequest
	var canonical []byte
	var err error
	switch kind {
	case "search":
		var wire discoverySearchWire
		if strictJSON(b, &wire) != nil || wire.ContextAssetIDs == nil {
			return r, ErrInvalid
		}
		canonical, err = json.Marshal(wire)
		r = DiscoveryRequest{Version: wire.Version, Kind: kind, LibraryID: wire.LibraryID, Text: wire.Text, ContextAssetIDs: wire.ContextAssetIDs, PageSize: wire.PageSize}
	case "page":
		var wire discoveryPageWire
		if strictJSON(b, &wire) != nil {
			return r, ErrInvalid
		}
		canonical, err = json.Marshal(wire)
		r = DiscoveryRequest{Version: wire.Version, Kind: kind, LibraryID: wire.LibraryID, Token: wire.Token}
	default:
		return r, ErrInvalid
	}
	if err != nil || !bytes.Equal(canonical, b) {
		return DiscoveryRequest{}, ErrInvalid
	}
	frozen, err := freezeDiscoveryRequest(r, library)
	if err != nil || frozen.Version != 2 {
		return DiscoveryRequest{}, ErrInvalid
	}
	return frozen, nil
}

// DecodeDiscoverySearchPage accepts the server's exact bounded search page.
// It checks each nested asset as well as the outer canonical JSON. The result
// is a disclosure projection; callers must explicitly select to pin bytes.
func DecodeDiscoverySearchPage(b []byte) (DiscoverySearchPage, error) {
	var page DiscoverySearchPage
	if len(b) == 0 || len(b) > 1<<20 || strictJSON(b, &page) != nil {
		return page, ErrInvalid
	}
	canonical, err := json.Marshal(page)
	if err != nil || !bytes.Equal(canonical, b) || page.Version != 2 || page.Kind != "search" || page.Items == nil || len(page.Items) > 100 || page.MatchedCount < len(page.Items) || page.MatchedCount > 4096 || page.CountScope != "frozen_window" || page.Sort != "discovery2" || page.Next != nil && (!validPageToken(*page.Next) || len(page.Items) == 0 || page.MatchedCount <= len(page.Items)) {
		return DiscoverySearchPage{}, ErrInvalid
	}
	seenAssets := map[continuity.AssetID]bool{}
	for _, item := range page.Items {
		if item.AssetID.Validate() != nil || !validText(item.Label, 4096, false) || item.Head != nil && item.Head.Validate() != nil || len(item.Reasons) == 0 || len(item.Reasons) > 3 {
			return DiscoverySearchPage{}, ErrInvalid
		}
		if seenAssets[item.AssetID] {
			return DiscoverySearchPage{}, ErrInvalid
		}
		seenAssets[item.AssetID] = true
		i := 0
		if item.Reasons[i] == "context" {
			i++
		}
		if i < len(item.Reasons) && item.Reasons[i] == "declared_selection" {
			i++
		}
		if i != len(item.Reasons)-1 {
			return DiscoverySearchPage{}, ErrInvalid
		}
		switch item.Reasons[i] {
		case "label_exact", "label_prefix", "label_contains", "stable_id":
		default:
			return DiscoverySearchPage{}, ErrInvalid
		}
	}
	return page, nil
}
