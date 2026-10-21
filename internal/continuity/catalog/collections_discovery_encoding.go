package catalog

import (
	"encoding/json"
	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// ValidateCollectionQueryRequest does not default or normalize caller fields.
func ValidateCollectionQueryRequest(r CollectionQueryRequest, library continuity.LibraryID) (CollectionQueryRequest, error) {
	bad := func() (CollectionQueryRequest, error) {
		return CollectionQueryRequest{}, invalid("collection read request")
	}
	if r.Version != 1 || library.Validate() != nil || r.LibraryID != library {
		return bad()
	}
	if r.Kind == "page" {
		if r.CollectionID != "" || r.State != "" || r.PageSize != 0 || !validPageToken(r.Token) {
			return bad()
		}
		return r, nil
	}
	if r.Token != "" {
		return bad()
	}
	if r.Kind == "directory" {
		if r.CollectionID != "" || (r.State != "active" && r.State != "archived" && r.State != "all") {
			return bad()
		}
	} else {
		if r.CollectionID.Validate() != nil {
			return bad()
		}
		if r.Kind == "members" {
			if r.State != "active" && r.State != "removed" {
				return bad()
			}
		} else if r.State != "" {
			return bad()
		}
	}
	switch r.Kind {
	case "open":
		if r.PageSize != 0 {
			return bad()
		}
	case "directory", "members", "history", "access":
		if r.PageSize < 1 || r.PageSize > 100 {
			return bad()
		}
	default:
		return bad()
	}
	return r, nil
}

// DecodeCollectionCLIReadRequest accepts only canonical initial CLI requests.
// Typed re-encoding rejects duplicates, omitted fields and null zero-values.
func DecodeCollectionCLIReadRequest(data []byte, library continuity.LibraryID) (CollectionQueryRequest, error) {
	bad := func() (CollectionQueryRequest, error) {
		return CollectionQueryRequest{}, invalid("collection read JSON")
	}
	if len(data) == 0 || len(data) > MaxCollectionRequestBytes {
		return bad()
	}
	var discriminator struct {
		Kind string `json:"kind"`
	}
	if json.Unmarshal(data, &discriminator) != nil {
		return bad()
	}
	var r CollectionQueryRequest
	switch discriminator.Kind {
	case "directory":
		var v CollectionCLIDirectoryRequest
		if decodeCanonicalValue(data, &v) != nil {
			return bad()
		}
		r = CollectionQueryRequest{Version: v.Version, Kind: v.Kind, LibraryID: v.LibraryID, State: v.State, PageSize: v.PageSize}
	case "open":
		var v CollectionCLIOpenRequest
		if decodeCanonicalValue(data, &v) != nil {
			return bad()
		}
		r = CollectionQueryRequest{Version: v.Version, Kind: v.Kind, LibraryID: v.LibraryID, CollectionID: v.CollectionID}
	case "members":
		var v CollectionCLIMembersRequest
		if decodeCanonicalValue(data, &v) != nil {
			return bad()
		}
		r = CollectionQueryRequest{Version: v.Version, Kind: v.Kind, LibraryID: v.LibraryID, CollectionID: v.CollectionID, State: v.State, PageSize: v.PageSize}
	case "history", "access":
		var v CollectionCLIListRequest
		if decodeCanonicalValue(data, &v) != nil {
			return bad()
		}
		r = CollectionQueryRequest{Version: v.Version, Kind: v.Kind, LibraryID: v.LibraryID, CollectionID: v.CollectionID, PageSize: v.PageSize}
	default:
		return bad()
	}
	return ValidateCollectionQueryRequest(r, library)
}

const collectionEnvelopeBytes = 4096
const collectionItemBytes = 32 << 10
const collectionReplayBytes = 2 * discoveryCopyBytes

// A fully escaped 256-byte name costs at most1536 bytes; all other summary,
// fixed envelope and opaque-token fields fit below the remaining2560 bytes.
// Actual encoded envelopes and complete bodies are also checked independently.
func encodeCollectionItem(v any) (json.RawMessage, error) {
	return encodeDiscoveryValue(v, collectionItemBytes)
}
