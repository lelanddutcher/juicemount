package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const (
	streamMagic3                = "juicemount.catalog.export\x00\x03"
	maxCollectionOwnerPayload   = 8 + MaxCollectionPolicyRecordBytes + MaxCollectionReceiptBytes
	maxCollectionContentPayload = 16 + MaxCollectionRecordBytes + MaxCollectionReceiptBytes + MaxCollectionPolicyRecordBytes + MaxCollectionReceiptBytes
)

type collectionStreamCounts struct {
	EventCount                 int64 `json:"event_count"`
	PolicyEventCount           int64 `json:"policy_event_count"`
	CollectionCount            int64 `json:"collection_count"`
	CollectionEventCount       int64 `json:"collection_event_count"`
	CollectionPolicyEventCount int64 `json:"collection_policy_event_count"`
	TransactionCount           int64 `json:"transaction_count"`
}
type collectionStreamHeader struct {
	Version       int                  `json:"version"`
	SchemaVersion int                  `json:"schema_version"`
	LibraryID     continuity.LibraryID `json:"library_id"`
	collectionStreamCounts
}
type collectionStreamFooter struct {
	collectionStreamCounts
	ProjectionRows   int64  `json:"projection_rows"`
	ProjectionSHA256 string `json:"projection_sha256"`
	StreamSHA256     string `json:"stream_sha256"`
}
type collectionDeclaration struct {
	Version               int                   `json:"version"`
	LibraryID             continuity.LibraryID  `json:"library_id"`
	CollectionID          CollectionID          `json:"collection_id"`
	CreatorSubjectID      SubjectID             `json:"creator_subject_id"`
	CreationOperationID   CollectionOperationID `json:"creation_operation_id"`
	CreationPayloadSHA256 string                `json:"creation_payload_sha256"`
}

func (d collectionDeclaration) validate(library continuity.LibraryID) error {
	if d.Version != 1 || d.LibraryID != library || d.CollectionID.Validate() != nil || d.CreatorSubjectID.Validate() != nil || d.CreationOperationID.Validate() != nil || !validCollectionDigest(d.CreationPayloadSHA256) {
		return corrupt("collection declaration")
	}
	return nil
}
func (c collectionStreamCounts) validate(limits Limits) error {
	if c.EventCount < 0 || c.PolicyEventCount < 0 || c.CollectionCount < 0 || c.CollectionCount > 1024 || c.CollectionEventCount < 0 || c.CollectionEventCount > 131072 || c.CollectionPolicyEventCount < 0 || c.CollectionPolicyEventCount > 131072 || c.TransactionCount < 0 {
		return corrupt("collection stream counts")
	}
	// Budget subtractions bound every addition before target reservation.
	remaining := limits.MaxEvents
	for _, n := range []int64{c.EventCount, c.PolicyEventCount, c.CollectionEventCount, c.CollectionPolicyEventCount} {
		if n > remaining {
			return ErrLimit
		}
		remaining -= n
	}
	if c.TransactionCount < c.PolicyEventCount || c.TransactionCount-c.PolicyEventCount < c.CollectionEventCount {
		return corrupt("collection transaction count")
	}
	owner := c.TransactionCount - c.PolicyEventCount - c.CollectionEventCount
	if owner > c.CollectionPolicyEventCount || c.CollectionPolicyEventCount-owner > c.CollectionEventCount {
		return corrupt("collection generated count")
	}
	return nil
}
func decodeCollectionHeader(data []byte, library continuity.LibraryID, limits Limits) (collectionStreamHeader, error) {
	return decodeCollectionFormatHeader(data, library, limits, 3)
}
func decodeCollectionFormatHeader(data []byte, library continuity.LibraryID, limits Limits, format int) (collectionStreamHeader, error) {
	var h collectionStreamHeader
	if format != 3 && format != 4 {
		return h, corrupt("collection stream version")
	}
	if len(data) == 0 || len(data) > 1024 {
		return h, corrupt("collection header length")
	}
	if limits.MaxProjectionRows < 2 {
		return h, ErrLimit
	}
	if err := canonicalJSON(data, &h); err != nil {
		return h, err
	}
	if h.Version != format || h.SchemaVersion != format+2 || h.LibraryID != library {
		return h, corrupt("collection stream identity or version")
	}
	return h, h.collectionStreamCounts.validate(limits)
}
func decodeCollectionFooter(data []byte, c collectionStreamCounts, limits Limits) (collectionStreamFooter, error) {
	var f collectionStreamFooter
	if len(data) == 0 || len(data) > 1024 {
		return f, corrupt("collection footer length")
	}
	if err := canonicalJSON(data, &f); err != nil {
		return f, err
	}
	if f.collectionStreamCounts != c || f.ProjectionRows < 2 || !validCollectionDigest(f.ProjectionSHA256) || !validCollectionDigest(f.StreamSHA256) {
		return f, corrupt("collection footer")
	}
	if f.ProjectionRows > limits.MaxProjectionRows {
		return f, ErrLimit
	}
	return f, nil
}
func collectionStreamHash() hash.Hash {
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.export.stream\x00\x03"))
	return h
}
func finishCollectionStreamHash(h hash.Hash, c collectionStreamCounts, rows int64, projection string) (string, error) {
	if !validCollectionDigest(projection) || rows < 2 {
		return "", corrupt("collection projection digest")
	}
	for _, n := range []int64{c.EventCount, c.PolicyEventCount, c.CollectionCount, c.CollectionEventCount, c.CollectionPolicyEventCount, c.TransactionCount, rows} {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		h.Write(b[:])
	}
	raw, _ := hex.DecodeString(projection)
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// The caller supplies the expected phase. Transaction kinds are selected only
// within the mixed chronology; declarations and footer cannot appear there.
func (s *streamInput) collectionFrame(transaction bool, expected byte, h hash.Hash) (byte, []byte, error) {
	return s.collectionFormatFrame(transaction, expected, h, 3)
}
func (s *streamInput) collectionFormatFrame(transaction bool, expected byte, h hash.Hash, format int) (byte, []byte, error) {
	if format != 3 && format != 4 && format != 5 {
		return 0, nil, corrupt("collection frame format")
	}
	var header [5]byte
	if err := s.exact(header[:]); err != nil {
		return 0, nil, err
	}
	kind := header[0]
	max := uint32(1024)
	if transaction {
		switch kind {
		case 4:
			max = maxEventPayload
		case 6:
			max = maxCollectionOwnerPayload
		case 7:
			max = maxCollectionContentPayload
		case 8:
			if format != 5 {
				return 0, nil, corrupt("byte policy in older format")
			}
			max = maxBytePolicyRecordBytes
		default:
			return 0, nil, corrupt("collection transaction frame kind")
		}
	} else if kind != expected || kind != 1 && kind != 2 && kind != 3 && kind != 5 {
		return 0, nil, corrupt("collection stream frame kind")
	} else if kind == 2 {
		max = maxEventPayload
	}
	size := binary.BigEndian.Uint32(header[1:])
	if size == 0 || size > max {
		return 0, nil, corrupt("collection frame length")
	}
	if int64(size) > s.max-s.used {
		return 0, nil, ErrLimit
	}
	payload := make([]byte, int(size))
	if err := s.exact(payload); err != nil {
		return 0, nil, err
	}
	if h != nil {
		h.Write(header[:])
		h.Write(payload)
	}
	return kind, payload, nil
}
func appendCollectionPair(dst, a, b []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(a)))
	dst = append(dst, a...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(b)))
	return append(dst, b...)
}
func takeCollectionPair(payload []byte, maxRecord int, optional bool) ([]byte, []byte, []byte, error) {
	if len(payload) < 8 {
		return nil, nil, nil, corrupt("short collection pair")
	}
	n := int64(binary.BigEndian.Uint32(payload[:4]))
	if n > int64(maxRecord) || n > int64(len(payload))-8 {
		return nil, nil, nil, corrupt("collection record length")
	}
	end := 4 + int(n)
	m := int64(binary.BigEndian.Uint32(payload[end : end+4]))
	if m > 1024 || m > int64(len(payload))-int64(end)-4 {
		return nil, nil, nil, corrupt("collection receipt length")
	}
	if n == 0 || m == 0 {
		if !optional || n != 0 || m != 0 {
			return nil, nil, nil, corrupt("empty collection pair")
		}
	}
	return payload[4:end], payload[end+4 : end+4+int(m)], payload[end+4+int(m):], nil
}
func decodeCollectionPolicyPair(data []byte, library continuity.LibraryID) (CollectionPolicyRecord, []byte, error) {
	var zero CollectionPolicyRecord
	a, b, rest, err := takeCollectionPair(data, MaxCollectionPolicyRecordBytes, false)
	if err != nil {
		return zero, nil, err
	}
	if len(rest) != 0 {
		return zero, nil, corrupt("trailing policy pair")
	}
	p, err := DecodeCollectionPolicyRecord(a)
	if err != nil {
		return zero, nil, err
	}
	r, err := DecodeCollectionPolicyReceipt(b)
	if err != nil {
		return zero, nil, err
	}
	_, digest, _ := CanonicalCollectionPolicyRequest(p.Request)
	if p.Request.LibraryID != library || r.LibraryID != library || r.OperationID != p.Request.ID || r.PayloadSHA256 != digest || r.RecordedUnixNano != p.RecordedUnixNano {
		return zero, nil, corrupt("policy receipt binding")
	}
	return p, b, nil
}
func bindCollectionChild(r CollectionRecord, p *CollectionPolicyRecord) error {
	generated := r.Request.Kind == "create" || r.Request.Kind == "grants"
	if !generated {
		if p != nil {
			return corrupt("unexpected generated policy")
		}
		return nil
	}
	if p == nil {
		return corrupt("missing generated policy")
	}
	_, digest, _ := CanonicalCollectionRequest(r.Request)
	if p.Request.LibraryID != r.Request.LibraryID || p.Request.CollectionID != r.Request.CollectionID || p.Administration != "collection_service" || p.OperatorSubjectID != r.ActorSubjectID || p.Request.ID != r.GeneratedPolicyOperationID || p.Request.BindingOperationID != r.Request.ID || p.Request.BindingPayloadSHA256 != digest || p.RecordedUnixNano != r.RecordedUnixNano {
		return corrupt("generated policy binding")
	}
	v, err := collectionValue(r.Request)
	if err != nil {
		return err
	}
	var changes []CollectionGrantChange
	switch x := v.(type) {
	case *CreateCollection:
		grant := CollectionGrantValue{r.ActorSubjectID, CollectionGrantScope{"collection", r.Request.CollectionID}, []string{"collections.admin", "collections.edit", "collections.read"}}
		changes = []CollectionGrantChange{{"put", x.CreatorGrantID, nil, &grant}}
	case *EditCollectionGrants:
		changes = x.Changes
	default:
		return corrupt("generated policy kind")
	}
	a, err := json.Marshal(changes)
	if err != nil {
		return err
	}
	b, err := json.Marshal(p.Request.Changes)
	if err != nil {
		return err
	}
	if string(a) != string(b) {
		return corrupt("generated policy changes")
	}
	return nil
}
func decodeCollectionContentPair(data []byte, library continuity.LibraryID) (CollectionRecord, []byte, *CollectionPolicyRecord, []byte, error) {
	var zero CollectionRecord
	a, b, rest, err := takeCollectionPair(data, MaxCollectionRecordBytes, false)
	if err != nil {
		return zero, nil, nil, nil, err
	}
	r, err := DecodeCollectionRecord(a)
	if err != nil {
		return zero, nil, nil, nil, err
	}
	receipt, err := DecodeCollectionReceipt(b)
	if err != nil {
		return zero, nil, nil, nil, err
	}
	if r.Request.LibraryID != library || receipt.OperationID != r.Request.ID || receipt.CollectionID != r.Request.CollectionID || receipt.ResultVersion != r.ResultVersion {
		return zero, nil, nil, nil, corrupt("content receipt binding")
	}
	x, y, tail, err := takeCollectionPair(rest, MaxCollectionPolicyRecordBytes, true)
	if err != nil {
		return zero, nil, nil, nil, err
	}
	if len(tail) != 0 {
		return zero, nil, nil, nil, corrupt("trailing content pair")
	}
	var child *CollectionPolicyRecord
	var childReceipt []byte
	if len(x) > 0 {
		p, receipt, err := decodeCollectionPolicyPair(appendCollectionPair(nil, x, y), library)
		if err != nil {
			return zero, nil, nil, nil, err
		}
		child = &p
		childReceipt = receipt
	}
	if err := bindCollectionChild(r, child); err != nil {
		return zero, nil, nil, nil, err
	}
	return r, b, child, childReceipt, nil
}

// Record JSON is reconstructed from bounded durable scalars, requests and
// results. It is streamed once and never counted as another quota byte copy.
func readCollectionPolicyRecord(ctx context.Context, q queryer, library continuity.LibraryID, sequence int64) (CollectionPolicyRecord, []byte, error) {
	var p CollectionPolicyRecord
	var lib continuity.LibraryID
	var namespace, category, digest string
	var collection CollectionID
	var request, receipt []byte
	var id CollectionPolicyOperationID
	err := q.QueryRowContext(ctx, `SELECT library_id,administration,namespace_subject_id,operator_subject_id,operation_id,category,COALESCE(collection_id,''),CASE WHEN length(request) BETWEEN 1 AND 32768 THEN request END,payload_sha256,CASE WHEN length(receipt) BETWEEN 1 AND 1024 THEN receipt END,recorded_unix_nano FROM collection_policy_events WHERE sequence=?`, sequence).Scan(&lib, &p.Administration, &namespace, &p.OperatorSubjectID, &id, &category, &collection, &request, &digest, &receipt, &p.RecordedUnixNano)
	if err != nil {
		return p, nil, err
	}
	p.Request, err = DecodeCollectionPolicyRequest(request)
	if err != nil {
		return p, nil, err
	}
	p.Version = 2
	p.Results = make([]CollectionGrantResult, 0, 16)
	err = walkRows(ctx, q, `SELECT grant_id,result_version,removed FROM collection_policy_results WHERE policy_sequence=? ORDER BY grant_id COLLATE BINARY LIMIT 17`, func(rows *sql.Rows) error {
		var r CollectionGrantResult
		if err := rows.Scan(&r.ID, &r.Version, &r.Removed); err != nil {
			return err
		}
		p.Results = append(p.Results, r)
		return nil
	}, sequence)
	if err != nil {
		return p, nil, err
	}
	data, _, err := CanonicalCollectionPolicyRecord(p)
	if err != nil {
		return p, nil, err
	}
	ns, err := p.Namespace()
	if err != nil {
		return p, nil, err
	}
	_, expected, _ := CanonicalCollectionPolicyRequest(p.Request)
	if lib != library || p.Request.LibraryID != library || p.Request.ID != id || p.Request.Category != category || p.Request.CollectionID != collection || ns.SubjectID != SubjectID(namespace) || digest != expected {
		return p, nil, corrupt("stored collection policy binding")
	}
	if _, _, err := decodeCollectionPolicyPair(appendCollectionPair(nil, data, receipt), library); err != nil {
		return p, nil, err
	}
	return p, receipt, nil
}
func readCollectionContentRecord(ctx context.Context, q queryer, library continuity.LibraryID, sequence int64) (CollectionRecord, []byte, int64, error) {
	var r CollectionRecord
	var lib continuity.LibraryID
	var id CollectionOperationID
	var collection CollectionID
	var request, receipt []byte
	var digest string
	var child int64
	err := q.QueryRowContext(ctx, `SELECT library_id,actor_subject_id,operation_id,collection_id,CASE WHEN length(request) BETWEEN 1 AND 32768 THEN request END,payload_sha256,CASE WHEN length(receipt) BETWEEN 1 AND 1024 THEN receipt END,result_version,COALESCE(generated_policy_sequence,0),recorded_unix_nano FROM collection_events WHERE sequence=?`, sequence).Scan(&lib, &r.ActorSubjectID, &id, &collection, &request, &digest, &receipt, &r.ResultVersion, &child, &r.RecordedUnixNano)
	if err != nil {
		return r, nil, 0, err
	}
	r.Request, err = DecodeCollectionRequest(request)
	if err != nil {
		return r, nil, 0, err
	}
	r.Version = 1
	if child != 0 {
		r.GeneratedPolicyOperationID = generatedCollectionPolicyID(r.Request.ID)
	}
	_, expected, _ := CanonicalCollectionRequest(r.Request)
	if lib != library || r.Request.LibraryID != library || r.Request.ID != id || r.Request.CollectionID != collection || digest != expected || child < 0 {
		return r, nil, 0, corrupt("stored collection content binding")
	}
	if _, _, err := CanonicalCollectionRecord(r); err != nil {
		return r, nil, 0, err
	}
	rc, err := DecodeCollectionReceipt(receipt)
	if err != nil || rc.OperationID != id || rc.CollectionID != collection || rc.ResultVersion != r.ResultVersion {
		return r, nil, 0, corrupt("stored collection content receipt")
	}
	return r, receipt, child, nil
}
func collectionPortableTotals(ctx context.Context, q queryer) (collectionStreamCounts, error) {
	var c collectionStreamCounts
	var err error
	c.EventCount, err = eventTotal(ctx, q)
	if err != nil {
		return c, err
	}
	c.PolicyEventCount, err = policyTotal(ctx, q)
	if err != nil {
		return c, err
	}
	var lastContent, lastPolicy, lastOrdinal int64
	err = q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM collection_identities),(SELECT count(*) FROM collection_events),(SELECT count(*) FROM collection_policy_events),(SELECT count(*) FROM collection_order),(SELECT COALESCE(max(sequence),0) FROM collection_events),(SELECT COALESCE(max(sequence),0) FROM collection_policy_events),(SELECT COALESCE(max(ordinal),0) FROM collection_order)`).Scan(&c.CollectionCount, &c.CollectionEventCount, &c.CollectionPolicyEventCount, &c.TransactionCount, &lastContent, &lastPolicy, &lastOrdinal)
	if err == nil && (lastContent != c.CollectionEventCount || lastPolicy != c.CollectionPolicyEventCount || lastOrdinal != c.TransactionCount) {
		err = corrupt("collection portable sequence count")
	}
	return c, err
}
func walkCollectionDeclarations(ctx context.Context, q queryer, library continuity.LibraryID, expected int64, fn func(collectionDeclaration) error) error {
	var count int64
	var last CollectionID
	err := walkRows(ctx, q, `SELECT library_id,collection_id,creator_subject_id,creation_operation_id,creation_payload_sha256 FROM collection_identities ORDER BY collection_id COLLATE BINARY`, func(rows *sql.Rows) error {
		d := collectionDeclaration{Version: 1}
		if err := rows.Scan(&d.LibraryID, &d.CollectionID, &d.CreatorSubjectID, &d.CreationOperationID, &d.CreationPayloadSHA256); err != nil {
			return err
		}
		if err := d.validate(library); err != nil {
			return err
		}
		if count >= expected || d.CollectionID <= last {
			return corrupt("collection declaration coverage")
		}
		if fn != nil {
			if err := fn(d); err != nil {
				return err
			}
		}
		last = d.CollectionID
		count++
		return nil
	})
	if err != nil {
		return err
	}
	if count != expected {
		return corrupt("missing collection declarations")
	}
	return nil
}
func walkCollectionTransactions(ctx context.Context, q queryer, library continuity.LibraryID, c collectionStreamCounts, fn func(byte, []byte) error) error {
	version, err := catalogVersion(ctx, q)
	if err != nil {
		return err
	}
	var byteCount int64
	var owners deliveryOwnerPrefix
	orderSQL := `SELECT COALESCE(legacy_sequence,0),COALESCE(owner_policy_sequence,0),COALESCE(content_sequence,0),0 FROM collection_order WHERE ordinal=?`
	if supportsDelivery(version) {
		byteCount, err = bytePolicyTotal(ctx, q)
		if err != nil {
			return err
		}
		owners, err = newDeliveryOwnerPrefix(ctx, q)
		if err != nil {
			return err
		}
		orderSQL = `SELECT COALESCE(legacy_sequence,0),COALESCE(owner_policy_sequence,0),COALESCE(content_sequence,0),COALESCE(byte_policy_sequence,0) FROM collection_order WHERE ordinal=?`
	}
	var legacy, content, policy, bytePolicy int64
	emit := func(kind byte, payload []byte) error {
		if fn != nil {
			return fn(kind, payload)
		}
		return nil
	}
	for ordinal := int64(1); ordinal <= c.TransactionCount; ordinal++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var l, p, n, b int64
		if err := q.QueryRowContext(ctx, orderSQL, ordinal).Scan(&l, &p, &n, &b); err != nil {
			return err
		}
		references := 0
		for _, v := range []int64{l, p, n, b} {
			if v < 0 {
				return corrupt("collection order negative reference")
			}
			if v != 0 {
				references++
			}
		}
		if references != 1 {
			return corrupt("collection order family reference")
		}
		switch {
		case l > 0:
			legacy++
			if l != legacy || legacy > c.PolicyEventCount {
				return corrupt("legacy collection order")
			}
			e, err := scanEvent(q.QueryRowContext(ctx, `SELECT `+boundedEventColumns+` FROM policy_events WHERE sequence=?`, l))
			if err != nil {
				return err
			}
			op, _, err := e.decodePolicy(library)
			if err != nil {
				return err
			}
			if err := owners.apply(op); err != nil {
				return err
			}
			if err := emit(4, encodeEvent(e)); err != nil {
				return err
			}
		case p > 0:
			policy++
			if p != policy || policy > c.CollectionPolicyEventCount {
				return corrupt("owner collection order")
			}
			r, receipt, err := readCollectionPolicyRecord(ctx, q, library, p)
			if err != nil {
				return err
			}
			if r.Administration != "local_owner" {
				return corrupt("independent generated policy")
			}
			if err := owners.require(r.OperatorSubjectID); err != nil {
				return err
			}
			data, _, _ := CanonicalCollectionPolicyRecord(r)
			if err := emit(6, appendCollectionPair(nil, data, receipt)); err != nil {
				return err
			}
		case b > 0:
			bytePolicy++
			if b != bytePolicy || bytePolicy > byteCount {
				return corrupt("byte policy collection order")
			}
			r, err := readBytePolicyRecord(ctx, q, library, b)
			if err != nil {
				return err
			}
			if err := owners.require(r.OperatorSubjectID); err != nil {
				return err
			}
			data, _, err := CanonicalBytePolicyRecord(r)
			if err != nil {
				return err
			}
			if err := emit(8, data); err != nil {
				return err
			}
		case n > 0:
			content++
			if n != content || content > c.CollectionEventCount {
				return corrupt("content collection order")
			}
			data, receipt, child, bindChild, err := readPortableCollectionContent(ctx, q, library, n)
			if err != nil {
				return err
			}
			payload := appendCollectionPair(nil, data, receipt)
			var generated *CollectionPolicyRecord
			if child != 0 {
				policy++
				if child != policy || policy > c.CollectionPolicyEventCount {
					return corrupt("generated collection order")
				}
				p, b, err := readCollectionPolicyRecord(ctx, q, library, child)
				if err != nil {
					return err
				}
				generated = &p
				data, _, _ := CanonicalCollectionPolicyRecord(p)
				payload = appendCollectionPair(payload, data, b)
			} else {
				payload = appendCollectionPair(payload, nil, nil)
			}
			if err := bindChild(generated); err != nil {
				return err
			}
			if err := emit(7, payload); err != nil {
				return err
			}
		}
	}
	if legacy != c.PolicyEventCount || content != c.CollectionEventCount || policy != c.CollectionPolicyEventCount || bytePolicy != byteCount {
		return corrupt("collection family coverage")
	}
	return nil
}

// These are identities of the current bounded projection, not a replay proof.
// They include attached reserves and exact request+receipt lengths, never the
// reconstructed record JSON. Restore recomputes these values at every ordinal.
func validateCollectionQuotaIdentity(ctx context.Context, q queryer, id CollectionID) error {
	usage, err := readCollectionQuota(ctx, q, id)
	if err != nil {
		return err
	}
	var expected CollectionQuotaCounters
	var collections, active, entries, grants, content, policy, contentBytes, policyBytes, reserves int64
	collectionsExpr := "(SELECT count(*) FROM collection_identities)"
	predicate := ""
	activePredicate := " WHERE removed=0"
	var args []any
	if id != "" {
		collectionsExpr = "0"
		predicate = " WHERE collection_id=?"
		activePredicate = " WHERE removed=0 AND collection_id=?"
		for i := 0; i < 8; i++ {
			args = append(args, id)
		}
	}
	query := "SELECT " + collectionsExpr + ",(SELECT count(*) FROM collection_entries" + activePredicate + "),(SELECT count(*) FROM collection_entries" + predicate + "),(SELECT count(*) FROM collection_grants" + predicate + "),(SELECT count(*) FROM collection_events" + predicate + "),(SELECT count(*) FROM collection_policy_events" + predicate + "),(SELECT COALESCE(sum(length(request)+length(receipt)),0) FROM collection_events" + predicate + "),(SELECT COALESCE(sum(length(request)+length(receipt)),0) FROM collection_policy_events" + predicate + "),(SELECT count(*) FROM collection_grants" + activePredicate + ")"
	err = q.QueryRowContext(ctx, query, args...).Scan(&collections, &active, &entries, &grants, &content, &policy, &contentBytes, &policyBytes, &reserves)
	if err != nil {
		return err
	}
	canonical, ok := checkedQuotaAdd(contentBytes, policyBytes)
	if !ok || reserves < 0 || reserves > 16384 {
		return corrupt("collection quota identity arithmetic")
	}
	expected = CollectionQuotaCounters{collections, active, entries, grants, content, content, policy, policy, canonical, reserves, reserves, reserves * CollectionRemovalReserveBytes}
	actual := usage.Library
	if id != "" {
		if usage.Collection == nil {
			return corrupt("missing portable local quota")
		}
		actual = *usage.Collection
	}
	if actual != expected {
		return corrupt("collection quota does not match durable identities")
	}
	return nil
}
func validateCollectionPortableState(ctx context.Context, q queryer, library continuity.LibraryID) error {
	if err := checkCollectionState(ctx, q, library); err != nil {
		return err
	}
	if err := validateCollectionQuotaIdentity(ctx, q, ""); err != nil {
		return err
	}
	var last CollectionID
	for count := 0; ; count++ {
		var id CollectionID
		err := q.QueryRowContext(ctx, `SELECT collection_id FROM collection_identities WHERE collection_id>? COLLATE BINARY ORDER BY collection_id COLLATE BINARY LIMIT 1`, last).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if count >= 1024 {
			return corrupt("portable collection identity bound")
		}
		if err := validateCollectionQuotaIdentity(ctx, q, id); err != nil {
			return err
		}
		last = id
	}
	var duplicates bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM collection_events GROUP BY collection_id,result_version HAVING count(*)>1) OR EXISTS(SELECT 1 FROM collection_policy_results GROUP BY grant_id,result_version HAVING count(*)>1)`).Scan(&duplicates); err != nil {
		return err
	}
	if duplicates {
		return corrupt("portable historical token reuse")
	}
	return nil
}

// Return only a pending footer. Public Export closes its owned read snapshot
// successfully before publishing this footer; a failed earlier step has none.
func exportCollectionSnapshot(ctx context.Context, q queryer, library continuity.LibraryID, output *streamOutput, limits Limits) ([]byte, error) {
	c, err := collectionPortableTotals(ctx, q)
	if err != nil {
		return nil, err
	}
	version, err := catalogVersion(ctx, q)
	if err != nil {
		return nil, err
	}
	var byteCount int64
	if supportsDelivery(version) {
		byteCount, err = bytePolicyTotal(ctx, q)
		if err != nil {
			return nil, err
		}
	}
	if err := validatePortableCollectionCounts(c, byteCount, supportsDelivery(version), limits); err != nil {
		return nil, err
	}
	rows, err := projectionTotal(ctx, q)
	if err != nil {
		return nil, err
	}
	if rows > limits.MaxProjectionRows {
		return nil, ErrLimit
	}
	if err := validateCollectionPortableState(ctx, q, library); err != nil {
		return nil, err
	}
	if supportsDelivery(version) {
		if err := validateBytePolicyPortableState(ctx, q, library); err != nil {
			return nil, err
		}
	}
	format := 3
	if supportsDelivery(version) {
		format = 5
	} else if version == 6 {
		format = 4
	} else if version != 5 {
		return nil, corrupt("collection export schema")
	}
	h, magic := collectionFormatHash(format)
	if err := output.write([]byte(magic), h); err != nil {
		return nil, err
	}
	header, err := encodePortableCollectionHeader(collectionStreamHeader{format, portableSchemaVersion(version), library, c}, byteCount)
	if err != nil {
		return nil, err
	}
	if len(header) > 1024 {
		return nil, corrupt("collection header length")
	}
	if err := output.frame(1, header, h); err != nil {
		return nil, err
	}
	if err := walkEvents(ctx, q, library, c.EventCount, func(e eventRow) error { return output.frame(2, encodeEvent(e), h) }); err != nil {
		return nil, err
	}
	if err := walkCollectionDeclarations(ctx, q, library, c.CollectionCount, func(d collectionDeclaration) error {
		b, err := json.Marshal(d)
		if err != nil {
			return err
		}
		if len(b) > 1024 {
			return corrupt("collection declaration length")
		}
		return output.frame(5, b, h)
	}); err != nil {
		return nil, err
	}
	if err := walkCollectionTransactions(ctx, q, library, c, func(kind byte, payload []byte) error { return output.frame(kind, payload, h) }); err != nil {
		return nil, err
	}
	projection, observed, err := projectionDigest(ctx, q, library, limits.MaxProjectionRows)
	if err != nil {
		return nil, err
	}
	if observed != rows {
		return nil, corrupt("collection projection row count")
	}
	digest, err := finishPortableCollectionStreamHash(format, h, c, byteCount, rows, projection)
	if err != nil {
		return nil, err
	}
	footer, err := encodePortableCollectionFooter(format, collectionStreamFooter{c, rows, projection, digest}, byteCount)
	if err != nil {
		return nil, err
	}
	if len(footer) > 1024 {
		return nil, corrupt("collection footer length")
	}
	return footer, nil
}
