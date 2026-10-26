package catalog

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func TestNativeDiscoveryCanonicalRequestsAndTypedPage(t *testing.T) {
	library := continuity.LibraryID("lib_11111111111111111111111111111111")
	asset := continuity.AssetID("asset_11111111111111111111111111111111")
	search := DiscoveryRequest{Version: 2, Kind: "search", LibraryID: library, Text: "A", ContextAssetIDs: []continuity.AssetID{asset}, PageSize: 20}
	encoded, err := CanonicalNativeDiscoveryRequest(search, library)
	if err != nil || string(encoded) != `{"version":2,"library_id":"lib_11111111111111111111111111111111","text":"a","context_asset_ids":["asset_11111111111111111111111111111111"],"page_size":20}` {
		t.Fatalf("search wire changed: %s %v", encoded, err)
	}
	decoded, err := DecodeNativeDiscoveryRequest("search", encoded, library)
	if err != nil || decoded.Text != "a" || decoded.PageSize != 20 || len(decoded.ContextAssetIDs) != 1 || decoded.ContextAssetIDs[0] != asset {
		t.Fatalf("canonical search refused: %+v %v", decoded, err)
	}
	mixedCase := bytes.Replace(encoded, []byte(`"text":"a"`), []byte(`"text":" My Project "`), 1)
	mixed, err := DecodeNativeDiscoveryRequest("search", mixedCase, library)
	if err != nil || mixed.Text != "my project" {
		t.Fatalf("valid user search normalization refused: %+v %v", mixed, err)
	}
	token := "page1_" + strings.Repeat("a", 64)
	pageRequest := DiscoveryRequest{Version: 2, Kind: "page", LibraryID: library, Token: token}
	pageWire, err := CanonicalNativeDiscoveryRequest(pageRequest, library)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeNativeDiscoveryRequest("page", pageWire, library); err != nil {
		t.Fatal("canonical page refused:", err)
	}
	result := DiscoverySearchPage{Version: 2, Kind: "search", Items: []DiscoveryAsset{{AssetID: asset, Label: "", Head: nil, Reasons: []string{"stable_id"}}}, MatchedCount: 7, CountScope: "frozen_window", Complete: false, Next: &token, Sort: "discovery2"}
	resultWire, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDiscoverySearchPage(resultWire)
	if err != nil || got.Kind != "search" || got.MatchedCount != 7 || got.Complete || got.Next == nil || *got.Next != token || len(got.Items) != 1 || got.Items[0].Label != "" {
		t.Fatalf("legal frozen page refused: %+v %v", got, err)
	}
	for _, malformed := range [][]byte{
		bytes.Replace(encoded, []byte(`"version":2`), []byte(`"version":2,"version":2`), 1),
		bytes.Replace(encoded, []byte(`"page_size":20`), []byte(`"page_size":20,"unknown":1`), 1),
		bytes.Replace(encoded, []byte(`"context_asset_ids":["asset_11111111111111111111111111111111"]`), []byte(`"context_asset_ids":null`), 1),
		bytes.Replace(encoded, []byte(`"library_id":"lib_11111111111111111111111111111111"`), []byte(`"library_id":"lib_22222222222222222222222222222222"`), 1),
	} {
		if _, err := DecodeNativeDiscoveryRequest("search", malformed, library); err == nil {
			t.Fatalf("malformed search admitted: %s", malformed)
		}
	}
	for _, malformed := range [][]byte{
		bytes.Replace(resultWire, []byte(`"kind":"search"`), []byte(`"kind":"page"`), 1),
		bytes.Replace(resultWire, []byte(`"matched_count":7`), []byte(`"matched_count":0`), 1),
		bytes.Replace(resultWire, []byte(`"label":""`), []byte(`"label":"","label":""`), 1),
		bytes.Replace(resultWire, []byte(`"sort_version":"discovery2"`), []byte(`"sort_version":"discovery1"`), 1),
	} {
		if _, err := DecodeDiscoverySearchPage(malformed); err == nil {
			t.Fatalf("malformed response admitted: %s", malformed)
		}
	}
	duplicate := result
	duplicate.Items = []DiscoveryAsset{result.Items[0], result.Items[0]}
	duplicateWire, _ := json.Marshal(duplicate)
	if _, err := DecodeDiscoverySearchPage(duplicateWire); err == nil {
		t.Fatal("duplicate asset ID admitted")
	}
	badReasons := result
	badReasons.Items = []DiscoveryAsset{{AssetID: asset, Reasons: []string{"label_exact", "label_prefix"}}}
	badReasonsWire, _ := json.Marshal(badReasons)
	if _, err := DecodeDiscoverySearchPage(badReasonsWire); err == nil {
		t.Fatal("contradictory search reasons admitted")
	}
}
