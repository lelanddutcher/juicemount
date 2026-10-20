package catalog

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const streamMagic2 = "juicemount.catalog.export\x00\x02"

type streamHeader2 struct {
	Version          int                  `json:"version"`
	SchemaVersion    int                  `json:"schema_version"`
	LibraryID        continuity.LibraryID `json:"library_id"`
	EventCount       int64                `json:"event_count"`
	PolicyEventCount int64                `json:"policy_event_count"`
}
type streamFooter2 struct {
	EventCount       int64  `json:"event_count"`
	PolicyEventCount int64  `json:"policy_event_count"`
	ProjectionRows   int64  `json:"projection_rows"`
	ProjectionSHA256 string `json:"projection_sha256"`
	StreamSHA256     string `json:"stream_sha256"`
}

func catalogVersion(ctx context.Context, q queryer) (int, error) {
	var version int
	err := q.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}
func streamHashFor(version int) hash.Hash {
	if version == 1 {
		return streamHash()
	}
	h := streamHash()
	h.Reset()
	h.Write([]byte("juicemount.catalog.export.stream\x00\x02"))
	return h
}
func finishStreamHashFor(version int, h hash.Hash, events, policy, rows int64, projection string) (string, error) {
	if version == 1 {
		return finishStreamHash(h, events, rows, projection)
	}
	if (continuity.Content{SHA256: projection}).Validate() != nil {
		return "", corrupt("projection digest spelling")
	}
	var counts [24]byte
	binary.BigEndian.PutUint64(counts[:8], uint64(events))
	binary.BigEndian.PutUint64(counts[8:16], uint64(policy))
	binary.BigEndian.PutUint64(counts[16:], uint64(rows))
	h.Write(counts[:])
	raw, _ := hex.DecodeString(projection)
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}
func encodeHeader(version int, library continuity.LibraryID, events, policy int64) []byte {
	if version == 1 {
		b, _ := json.Marshal(streamHeader{1, 2, library, events})
		return b
	}
	b, _ := json.Marshal(streamHeader2{2, 3, library, events, policy})
	return b
}
func decodeHeader(data []byte, version int, library continuity.LibraryID) (streamHeader2, error) {
	var h streamHeader2
	if version == 1 {
		var v streamHeader
		if err := canonicalJSON(data, &v); err != nil {
			return h, err
		}
		h = streamHeader2{v.Version, v.SchemaVersion, v.LibraryID, v.EventCount, 0}
	} else {
		if err := canonicalJSON(data, &h); err != nil {
			return h, err
		}
	}
	if h.Version != version || h.SchemaVersion != version+1 || h.LibraryID != library || h.EventCount < 0 || h.PolicyEventCount < 0 {
		return h, corrupt("export header identity or version")
	}
	return h, nil
}
func encodeFooter(version int, f streamFooter2) []byte {
	if version == 1 {
		b, _ := json.Marshal(streamFooter{f.EventCount, f.ProjectionRows, f.ProjectionSHA256, f.StreamSHA256})
		return b
	}
	b, _ := json.Marshal(f)
	return b
}
func decodeFooter(data []byte, version int) (streamFooter2, error) {
	var f streamFooter2
	if version == 1 {
		var v streamFooter
		err := canonicalJSON(data, &v)
		return streamFooter2{v.EventCount, 0, v.ProjectionRows, v.ProjectionSHA256, v.StreamSHA256}, err
	}
	err := canonicalJSON(data, &f)
	return f, err
}
func decodeStreamPolicyEvent(payload []byte, library continuity.LibraryID, sequence int64) (eventRow, PolicyOperation, error) {
	if len(payload) < 8 {
		return eventRow{}, PolicyOperation{}, corrupt("short policy frame")
	}
	n := int64(binary.BigEndian.Uint32(payload[:4]))
	if n == 0 || n > MaxRequestBytes || n > int64(len(payload))-8 {
		return eventRow{}, PolicyOperation{}, corrupt("policy request length")
	}
	end := 4 + int(n)
	m := int64(binary.BigEndian.Uint32(payload[end : end+4]))
	if m == 0 || m > 1024 || int64(end)+4+m != int64(len(payload)) {
		return eventRow{}, PolicyOperation{}, corrupt("policy receipt length")
	}
	var receipt Receipt
	if err := canonicalJSON(payload[end+4:], &receipt); err != nil {
		return eventRow{}, PolicyOperation{}, err
	}
	e := eventRow{sequence, library, receipt.OperationID, payload[4:end], receipt.PayloadSHA256, receipt.RecordedUnixNano, payload[end+4:]}
	op, _, err := e.decodePolicy(library)
	return e, op, err
}
