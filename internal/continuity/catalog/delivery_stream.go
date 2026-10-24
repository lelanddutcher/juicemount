package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const (
	streamMagic5             = "juicemount.catalog.export\x00\x05"
	maxBytePolicyRecordBytes = 36864
)

type deliveryStreamCounts struct {
	collectionStreamCounts
	BytePolicyEventCount int64 `json:"byte_policy_event_count"`
}
type deliveryStreamHeader struct {
	Version       int                  `json:"version"`
	SchemaVersion int                  `json:"schema_version"`
	LibraryID     continuity.LibraryID `json:"library_id"`
	deliveryStreamCounts
}
type deliveryStreamFooter struct {
	deliveryStreamCounts
	ProjectionRows   int64  `json:"projection_rows"`
	ProjectionSHA256 string `json:"projection_sha256"`
	StreamSHA256     string `json:"stream_sha256"`
}

func validatePortableCollectionCounts(c collectionStreamCounts, byteCount int64, delivery bool, limits Limits) error {
	if !delivery {
		if byteCount != 0 {
			return corrupt("byte policy in older stream")
		}
		return c.validate(limits)
	}
	if byteCount < 0 || byteCount > 131072 || c.TransactionCount < byteCount {
		return corrupt("byte policy stream counts")
	}
	if byteCount > limits.MaxEvents || limits.MaxProjectionRows < 3 {
		return ErrLimit
	}
	// Apply the frozen older-family arithmetic to precisely its own ordinals
	// and remaining combined event budget. Byte events are independent owners.
	c.TransactionCount -= byteCount
	limits.MaxEvents -= byteCount
	return c.validate(limits)
}
func encodePortableCollectionHeader(h collectionStreamHeader, byteCount int64) ([]byte, error) {
	if h.Version != 5 {
		return json.Marshal(h)
	}
	return json.Marshal(deliveryStreamHeader{5, 7, h.LibraryID, deliveryStreamCounts{h.collectionStreamCounts, byteCount}})
}
func decodePortableCollectionHeader(data []byte, library continuity.LibraryID, limits Limits, format int) (collectionStreamHeader, int64, error) {
	if format != 5 {
		h, err := decodeCollectionFormatHeader(data, library, limits, format)
		return h, 0, err
	}
	var h deliveryStreamHeader
	if len(data) == 0 || len(data) > 1024 {
		return collectionStreamHeader{}, 0, corrupt("delivery header length")
	}
	if err := canonicalJSON(data, &h); err != nil {
		return collectionStreamHeader{}, 0, err
	}
	if h.Version != 5 || h.SchemaVersion != 7 || h.LibraryID != library {
		return collectionStreamHeader{}, 0, corrupt("delivery stream identity or version")
	}
	c := collectionStreamHeader{5, 7, library, h.collectionStreamCounts}
	return c, h.BytePolicyEventCount, validatePortableCollectionCounts(h.collectionStreamCounts, h.BytePolicyEventCount, true, limits)
}
func encodePortableCollectionFooter(format int, f collectionStreamFooter, byteCount int64) ([]byte, error) {
	if format != 5 {
		return json.Marshal(f)
	}
	return json.Marshal(deliveryStreamFooter{deliveryStreamCounts{f.collectionStreamCounts, byteCount}, f.ProjectionRows, f.ProjectionSHA256, f.StreamSHA256})
}
func decodePortableCollectionFooter(data []byte, counts collectionStreamCounts, byteCount int64, limits Limits, format int) (collectionStreamFooter, error) {
	if format != 5 {
		return decodeCollectionFooter(data, counts, limits)
	}
	var f deliveryStreamFooter
	if len(data) == 0 || len(data) > 1024 {
		return collectionStreamFooter{}, corrupt("delivery footer length")
	}
	if err := canonicalJSON(data, &f); err != nil {
		return collectionStreamFooter{}, err
	}
	if f.collectionStreamCounts != counts || f.BytePolicyEventCount != byteCount || f.ProjectionRows < 3 || !validCollectionDigest(f.ProjectionSHA256) || !validCollectionDigest(f.StreamSHA256) {
		return collectionStreamFooter{}, corrupt("delivery footer")
	}
	if f.ProjectionRows > limits.MaxProjectionRows {
		return collectionStreamFooter{}, ErrLimit
	}
	return collectionStreamFooter{counts, f.ProjectionRows, f.ProjectionSHA256, f.StreamSHA256}, nil
}
func deliveryStreamHash() hash.Hash {
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.export.stream\x00\x05"))
	return h
}
func finishPortableCollectionStreamHash(format int, h hash.Hash, c collectionStreamCounts, byteCount, rows int64, projection string) (string, error) {
	if format != 5 {
		return finishCollectionStreamHash(h, c, rows, projection)
	}
	if rows < 3 || byteCount < 0 || !validCollectionDigest(projection) {
		return "", corrupt("delivery projection digest")
	}
	for _, n := range []int64{c.EventCount, c.PolicyEventCount, c.CollectionCount, c.CollectionEventCount, c.CollectionPolicyEventCount, c.TransactionCount, byteCount, rows} {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		h.Write(b[:])
	}
	raw, _ := hex.DecodeString(projection)
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}
func bytePolicyTotal(ctx context.Context, q queryer) (int64, error) {
	var count, last int64
	if err := q.QueryRowContext(ctx, `SELECT count(*),COALESCE(max(sequence),0) FROM byte_policy_events`).Scan(&count, &last); err != nil {
		return 0, err
	}
	if count < 0 || count > 131072 || count != last {
		return 0, corrupt("byte policy sequence count")
	}
	return count, nil
}

func readBytePolicyRecord(ctx context.Context, q queryer, library continuity.LibraryID, sequence int64) (BytePolicyRecord, error) {
	var r BytePolicyRecord
	var lib continuity.LibraryID
	var id, digest string
	var request, receipt []byte
	var timestamp int64
	err := q.QueryRowContext(ctx, `SELECT library_id,operator_subject_id,operation_id,CASE WHEN length(request) BETWEEN 1 AND 32768 THEN request END,payload_sha256,CASE WHEN length(receipt) BETWEEN 1 AND 2048 THEN receipt END,recorded_unix_nano FROM byte_policy_events WHERE sequence=?`, sequence).Scan(&lib, &r.OperatorSubjectID, &id, &request, &digest, &receipt, &timestamp)
	if err != nil {
		return r, err
	}
	r.Version = 1
	r.Request, err = DecodeBytePolicyRequest(request)
	if err != nil {
		return r, err
	}
	r.Receipt, err = DecodeBytePolicyReceipt(receipt)
	if err != nil {
		return r, err
	}
	if lib != library || r.Request.LibraryID != library || r.Request.ID != id || r.Receipt.Sequence != sequence || r.Receipt.PayloadSHA256 != digest || r.Receipt.RecordedUnixNano != timestamp {
		return r, corrupt("stored byte policy binding")
	}
	if _, _, err := CanonicalBytePolicyRecord(r); err != nil {
		return r, err
	}
	var index int
	err = walkRows(ctx, q, `SELECT grant_id,result_version,removed FROM byte_policy_results WHERE policy_sequence=? ORDER BY grant_id COLLATE BINARY LIMIT 17`, func(rows *sql.Rows) error {
		var result ByteGrantResult
		if err := rows.Scan(&result.ID, &result.Version, &result.Removed); err != nil {
			return err
		}
		if index >= len(r.Receipt.Results) || result != r.Receipt.Results[index] {
			return corrupt("byte policy result binding")
		}
		index++
		return nil
	}, sequence)
	if err != nil {
		return r, err
	}
	if index != len(r.Receipt.Results) {
		return r, corrupt("missing byte policy results")
	}
	return r, nil
}

// Only owner identities are retained, as one bit each, with an explicit bound.
// Attribution follows historical subject mutations; final enabled state and
// operational authority/credentials are deliberately irrelevant to this check.
type deliveryOwnerPrefix map[SubjectID]bool

func newDeliveryOwnerPrefix(ctx context.Context, q queryer) (deliveryOwnerPrefix, error) {
	owners := make(deliveryOwnerPrefix)
	err := walkRows(ctx, q, `SELECT operator_subject_id FROM collection_policy_events WHERE administration='local_owner' UNION SELECT operator_subject_id FROM byte_policy_events`, func(rows *sql.Rows) error {
		var id SubjectID
		if err := rows.Scan(&id); err != nil {
			return err
		}
		if id.Validate() != nil || len(owners) >= 262144 {
			return corrupt("delivery owner identity bound")
		}
		owners[id] = false
		return nil
	})
	return owners, err
}
func (p deliveryOwnerPrefix) apply(op PolicyOperation) error {
	if p == nil {
		return nil
	}
	for _, change := range op.Changes {
		v, err := policyChangeValue(change)
		if err != nil {
			return err
		}
		var id SubjectID
		var value SubjectValue
		switch x := v.(type) {
		case *CreateSubject:
			id, value = x.SubjectID, x.Value
		case *UpdateSubject:
			id, value = x.SubjectID, x.Value
		default:
			continue
		}
		if _, tracked := p[id]; tracked {
			p[id] = value.Kind == "person" && value.Enabled
		}
	}
	return nil
}
func (p deliveryOwnerPrefix) require(id SubjectID) error {
	if p != nil && !p[id] {
		return corrupt("delivery owner not enabled at historical prefix")
	}
	return nil
}
