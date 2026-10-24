package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"hash"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const streamMagic4 = "juicemount.catalog.export\x00\x04"

// Callers validate the format before choosing its domain. All subsequent
// framing, counts and lifecycle ownership are shared with format3.
func collectionFormatHash(format int) (hash.Hash, string) {
	if format == 5 {
		return deliveryStreamHash(), streamMagic5
	}
	if format == 4 {
		h := sha256.New()
		h.Write([]byte("juicemount.catalog.export.stream\x00\x04"))
		return h, streamMagic4
	}
	return collectionStreamHash(), streamMagic3
}

func decodeSavedQueryContentPair(data []byte, library continuity.LibraryID) (SavedQueryRecord, []byte, *CollectionPolicyRecord, []byte, error) {
	var zero SavedQueryRecord
	a, b, rest, err := takeCollectionPair(data, MaxCollectionRecordBytes, false)
	if err != nil {
		return zero, nil, nil, nil, err
	}
	r, err := DecodeSavedQueryRecord(a)
	if err != nil {
		return zero, nil, nil, nil, err
	}
	request, _, err := CanonicalSavedQueryRequest(r.Request)
	if err != nil {
		return zero, nil, nil, nil, err
	}
	if _, err := validateRecordedSavedQueryContent(r, request, b, library); err != nil {
		return zero, nil, nil, nil, err
	}
	x, y, tail, err := takeCollectionPair(rest, MaxCollectionPolicyRecordBytes, true)
	if err != nil {
		return zero, nil, nil, nil, err
	}
	if len(tail) != 0 {
		return zero, nil, nil, nil, corrupt("trailing query content pair")
	}
	var child *CollectionPolicyRecord
	var childReceipt []byte
	if len(x) != 0 {
		p, receipt, err := decodeCollectionPolicyPair(appendCollectionPair(nil, x, y), library)
		if err != nil {
			return zero, nil, nil, nil, err
		}
		child, childReceipt = &p, receipt
	}
	if err := bindSavedQueryChild(r, child); err != nil {
		return zero, nil, nil, nil, err
	}
	return r, b, child, childReceipt, nil
}

type portableCollectionReplay func(context.Context, *Store, int64, int64) (int64, error)

// Inspect only the discriminator before the selected strict canonical decoder.
// A format3 stream can never opt into the query codec.
func decodePortableCollectionContent(data []byte, library continuity.LibraryID, format int) (portableCollectionReplay, bool, error) {
	if format != 3 && format != 4 && format != 5 {
		return nil, false, corrupt("content stream format")
	}
	a, _, _, err := takeCollectionPair(data, MaxCollectionRecordBytes, false)
	if err != nil {
		return nil, false, err
	}
	var envelope struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(a, &envelope); err != nil {
		return nil, false, corrupt("content record discriminator")
	}
	switch envelope.Version {
	case 1:
		r, receipt, child, childReceipt, err := decodeCollectionContentPair(data, library)
		if err != nil {
			return nil, false, err
		}
		return func(ctx context.Context, s *Store, rows, max int64) (int64, error) {
			return s.replayCollectionContent(ctx, r, receipt, child, childReceipt, rows, max)
		}, child != nil, nil
	case 2:
		if format != 4 && format != 5 {
			return nil, false, corrupt("query content in older format")
		}
		r, receipt, child, childReceipt, err := decodeSavedQueryContentPair(data, library)
		if err != nil {
			return nil, false, err
		}
		return func(ctx context.Context, s *Store, rows, max int64) (int64, error) {
			return s.replaySavedQueryContent(ctx, r, receipt, child, childReceipt, rows, max)
		}, child != nil, nil
	default:
		return nil, false, corrupt("unknown content record version")
	}
}

func readPortableCollectionContent(ctx context.Context, q queryer, library continuity.LibraryID, sequence int64) ([]byte, []byte, int64, func(*CollectionPolicyRecord) error, error) {
	version, err := catalogVersion(ctx, q)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	kind := "manual"
	if supportsSavedQueries(version) {
		var id CollectionID
		if err := q.QueryRowContext(ctx, `SELECT collection_id FROM collection_events WHERE sequence=?`, sequence).Scan(&id); err != nil {
			return nil, nil, 0, nil, err
		}
		kind, err = collectionStoredKind(ctx, q, id)
		if err != nil {
			return nil, nil, 0, nil, err
		}
	} else if version != 5 {
		return nil, nil, 0, nil, corrupt("content schema version")
	}
	if kind == "saved_query" {
		r, receipt, child, err := readSavedQueryContentRecord(ctx, q, library, sequence)
		if err != nil {
			return nil, nil, 0, nil, err
		}
		data, _, err := CanonicalSavedQueryRecord(r)
		return data, receipt, child, func(p *CollectionPolicyRecord) error { return bindSavedQueryChild(r, p) }, err
	}
	r, receipt, child, err := readCollectionContentRecord(ctx, q, library, sequence)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	data, _, err := CanonicalCollectionRecord(r)
	return data, receipt, child, func(p *CollectionPolicyRecord) error { return bindCollectionChild(r, p) }, err
}
