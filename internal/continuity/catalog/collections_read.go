package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type collectionDefinition struct {
	id         CollectionID
	creator    SubjectID
	name       string
	archived   bool
	version    CollectionVersion
	generation int64
}

func readCollectionDefinition(ctx context.Context, q queryer, id CollectionID) (collectionDefinition, error) {
	var d collectionDefinition
	d.id = id
	err := q.QueryRowContext(ctx, `SELECT i.creator_subject_id,d.name,d.archived,d.definition_version,i.policy_generation FROM collection_definitions d JOIN collection_identities i USING(collection_id) WHERE d.collection_id=?`, id).Scan(&d.creator, &d.name, &d.archived, &d.version, &d.generation)
	if errors.Is(err, sql.ErrNoRows) {
		return d, ErrNotFound
	}
	if err != nil {
		return d, err
	}
	if id.Validate() != nil || d.creator.Validate() != nil || !validText(d.name, 256, true) || d.version.Validate() != nil || d.generation < 1 {
		return d, corrupt("collection definition")
	}
	return d, nil
}

type storedCollectionGrant struct {
	id               CollectionGrantID
	expected         ExpectedCollectionGrant
	created, updated int64
}

func readCollectionGrant(ctx context.Context, q queryer, id CollectionGrantID) (storedCollectionGrant, error) {
	var g storedCollectionGrant
	var actions []byte
	g.id = id
	err := q.QueryRowContext(ctx, `SELECT subject_id,scope_kind,COALESCE(collection_id,''),CASE WHEN length(actions)<=128 THEN actions END,grant_version,removed,created_sequence,updated_sequence FROM collection_grants WHERE grant_id=?`, id).Scan(&g.expected.Value.SubjectID, &g.expected.Value.Scope.Kind, &g.expected.Value.Scope.CollectionID, &actions, &g.expected.Version, &g.expected.Removed, &g.created, &g.updated)
	if errors.Is(err, sql.ErrNoRows) {
		return g, ErrNotFound
	}
	if err != nil {
		return g, err
	}
	if decodeCanonicalValue(actions, &g.expected.Value.Actions) != nil || !validCollectionGrantValue(g.expected.Value) || g.expected.Version.Validate() != nil || g.created <= 0 || g.updated < g.created {
		return g, corrupt("collection grant")
	}
	return g, nil
}
func collectionRights(ctx context.Context, q queryer, subject SubjectID, id CollectionID) (map[string]bool, error) {
	rights := make(map[string]bool)
	var rows *sql.Rows
	var err error
	if id == "" {
		rows, err = q.QueryContext(ctx, `SELECT actions FROM collection_grants WHERE subject_id=? AND removed=0 AND scope_kind='library_create'`, subject)
	} else {
		rows, err = q.QueryContext(ctx, `SELECT actions FROM collection_grants WHERE subject_id=? AND removed=0 AND scope_kind='collection' AND collection_id=?`, subject, id)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		if count > 16384 {
			return nil, corrupt("collection grant bound")
		}
		var b []byte
		var actions []string
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		v := CollectionGrantValue{SubjectID: subject, Scope: CollectionGrantScope{"collection", id}}
		if id == "" {
			v.Scope.Kind = "library_create"
		}
		if len(b) > 128 || decodeCanonicalValue(b, &actions) != nil {
			return nil, corrupt("collection rights")
		}
		v.Actions = actions
		if !validCollectionGrantValue(v) {
			return nil, corrupt("collection rights")
		}
		for _, a := range actions {
			rights[a] = true
		}
	}
	return rights, rows.Err()
}
func requireCollectionRights(ctx context.Context, q queryer, c credential, id CollectionID, actions ...string) error {
	if c.scope.Kind != "library" {
		return ErrNotFound
	}
	rights, err := collectionRights(ctx, q, c.subject, id)
	if err != nil {
		return err
	}
	for _, a := range actions {
		if !containsAction(c.actions, a) || !rights[a] {
			return ErrNotFound
		}
	}
	return nil
}
func collectionAssetAllowed(ctx context.Context, q queryer, library continuity.LibraryID, c credential, asset continuity.AssetID) error {
	rights, err := readDiscoveryRights(ctx, q, library, c)
	if err != nil {
		return err
	}
	if !rights.allows(asset, "assets.discover") {
		return ErrNotFound
	}
	_, err = readAsset(ctx, q, library, asset)
	return err
}
func readCollectionQuota(ctx context.Context, q queryer, id CollectionID) (CollectionQuotaUsage, error) {
	scan := func(row *sql.Row) (CollectionQuotaCounters, error) {
		var v CollectionQuotaCounters
		f := make([]int64, 12)
		args := make([]any, 12)
		for i := range f {
			args[i] = &f[i]
		}
		if err := row.Scan(args...); err != nil {
			return v, err
		}
		var a [12]int64
		copy(a[:], f)
		return quotaFromFields(a), nil
	}
	library, err := scan(q.QueryRowContext(ctx, `SELECT `+collectionQuotaColumns+` FROM collection_library_quota WHERE singleton=1`))
	if err != nil {
		return CollectionQuotaUsage{}, err
	}
	out := CollectionQuotaUsage{Library: library}
	if id != "" {
		c, err := scan(q.QueryRowContext(ctx, `SELECT `+collectionQuotaColumns+` FROM collection_quota WHERE collection_id=?`, id))
		if err != nil {
			return out, err
		}
		out.Collection = &c
	}
	if _, err := planCollectionQuota(out, CollectionQuotaDelta{}); err != nil {
		return out, corrupt("collection quota projection")
	}
	return out, nil
}
func saveCollectionQuota(ctx context.Context, tx *sql.Tx, id CollectionID, usage CollectionQuotaUsage) error {
	set := strings.ReplaceAll(collectionQuotaColumns, ",", "=?,") + "=?"
	update := func(table, where string, c CollectionQuotaCounters, arg any) error {
		values := quotaFields(c)
		args := make([]any, 0, 13)
		for _, n := range values {
			args = append(args, n)
		}
		args = append(args, arg)
		return changedOne(tx.ExecContext(ctx, `UPDATE `+table+` SET `+set+` WHERE `+where+`=?`, args...))
	}
	if err := update("collection_library_quota", "singleton", usage.Library, 1); err != nil {
		return err
	}
	if id != "" {
		if usage.Collection == nil {
			return corrupt("missing collection quota")
		}
		return update("collection_quota", "collection_id", *usage.Collection, id)
	}
	return nil
}
func appendCollectionOrder(ctx context.Context, tx *sql.Tx, column string, sequence int64) error {
	if column != "legacy_sequence" && column != "owner_policy_sequence" && column != "content_sequence" {
		return invalid("collection order reference")
	}
	var ordinal int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(max(ordinal),0) FROM collection_order`).Scan(&ordinal); err != nil {
		return err
	}
	next, ok := checkedQuotaAdd(ordinal, 1)
	if !ok {
		return ErrConflict
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO collection_order(ordinal,`+column+`) VALUES(?,?)`, next, sequence)
	return err
}
func checkCollectionOrder(ctx context.Context, q queryer) error {
	var missing int
	err := q.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM policy_events p WHERE NOT EXISTS(SELECT 1 FROM collection_order o WHERE o.legacy_sequence=p.sequence))+
 (SELECT count(*) FROM collection_events c WHERE NOT EXISTS(SELECT 1 FROM collection_order o WHERE o.content_sequence=c.sequence))+
 (SELECT count(*) FROM collection_policy_events p WHERE (p.administration='local_owner' AND NOT EXISTS(SELECT 1 FROM collection_order o WHERE o.owner_policy_sequence=p.sequence)) OR (p.administration='collection_service' AND (EXISTS(SELECT 1 FROM collection_order o WHERE o.owner_policy_sequence=p.sequence) OR NOT EXISTS(SELECT 1 FROM collection_events c WHERE c.generated_policy_sequence=p.sequence))))`).Scan(&missing)
	if err != nil {
		return err
	}
	if missing != 0 {
		return corrupt("collection order coverage")
	}
	rows, err := q.QueryContext(ctx, `SELECT ordinal,COALESCE(legacy_sequence,0),COALESCE(owner_policy_sequence,0),COALESCE(content_sequence,0),COALESCE((SELECT generated_policy_sequence FROM collection_events WHERE sequence=content_sequence),0) FROM collection_order ORDER BY ordinal`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ordinal, legacy, policy, content int64
	for rows.Next() {
		var o, l, p, c, g int64
		if err := rows.Scan(&o, &l, &p, &c, &g); err != nil {
			return err
		}
		ordinal++
		if o != ordinal {
			return corrupt("collection order discontinuity")
		}
		if l != 0 {
			legacy++
			if l != legacy {
				return corrupt("legacy policy order")
			}
		}
		if c != 0 {
			content++
			if c != content {
				return corrupt("content order")
			}
		}
		if g != 0 {
			p = g
		}
		if p != 0 {
			policy++
			if p != policy {
				return corrupt("collection policy order")
			}
		}
	}
	return rows.Err()
}
func requireAdminCodec(ctx context.Context, q queryer, id, expected string) error {
	version, err := catalogVersion(ctx, q)
	if err != nil {
		return err
	}
	if !supportsCollections(version) {
		if expected != "admin1" {
			return ErrUpgradeRequired
		}
		return nil
	}
	var codec string
	if err := q.QueryRowContext(ctx, `SELECT codec FROM catalog_admin_codecs WHERE operation_id=?`, id).Scan(&codec); err != nil {
		return corrupt("administrative codec")
	}
	if codec != expected {
		return ErrChangedRequest
	}
	return nil
}

func checkCollectionState(ctx context.Context, q queryer, library continuity.LibraryID) error {
	if _, err := readCollectionQuota(ctx, q, ""); err != nil {
		return err
	}
	var quotaLibrary continuity.LibraryID
	if err := q.QueryRowContext(ctx, `SELECT library_id FROM collection_library_quota WHERE singleton=1`).Scan(&quotaLibrary); err != nil {
		return err
	}
	if quotaLibrary != library {
		return corrupt("collection quota library")
	}
	var last CollectionID
	count := 0
	for {
		var id CollectionID
		err := q.QueryRowContext(ctx, `SELECT collection_id FROM collection_identities WHERE collection_id>? ORDER BY collection_id LIMIT 1`, last).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		count++
		if count > 1024 {
			return corrupt("collection identity bound")
		}
		if _, err := readCollectionQuota(ctx, q, id); err != nil {
			return corrupt("collection local quota")
		}
		if err := validateCollectionDeclaration(ctx, q, library, id); err != nil {
			return err
		}
		last = id
	}

	var invalidRows int
	err := q.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM catalog_credentials c WHERE NOT EXISTS(SELECT 1 FROM credential_codecs d WHERE d.selector=c.selector))+
 (SELECT count(*) FROM catalog_admin_events a WHERE NOT EXISTS(SELECT 1 FROM catalog_admin_codecs d WHERE d.operation_id=a.operation_id))+
 (SELECT count(*) FROM collection_identities i WHERE i.library_id<>? OR NOT EXISTS(SELECT 1 FROM collection_definitions d WHERE d.collection_id=i.collection_id) OR NOT EXISTS(SELECT 1 FROM policy_subjects s WHERE s.subject_id=i.creator_subject_id))`, library).Scan(&invalidRows)
	if err != nil {
		return err
	}
	if invalidRows != 0 {
		return corrupt("collection projection references")
	}
	if err := checkCollectionOrder(ctx, q); err != nil {
		return err
	}
	return validateCollectionHistory(ctx, q, library)
}

// Every ready declaration must be consumed by its definition's exact create.
// The caller visits at most1024 identities; this reads one bounded request and
// releases its row before decoding. Forward event/child checks remain separate.
func validateCollectionDeclaration(ctx context.Context, q queryer, library continuity.LibraryID, id CollectionID) error {
	var declarationLibrary, eventLibrary continuity.LibraryID
	var creator, actor SubjectID
	var creation, eventOperation CollectionOperationID
	var eventCollection CollectionID
	var declarationDigest, eventDigest string
	var created, eventSequence int64
	var request []byte
	err := q.QueryRowContext(ctx, `SELECT i.library_id,i.creator_subject_id,i.creation_operation_id,i.creation_payload_sha256,d.created_sequence,COALESCE(e.sequence,0),COALESCE(e.library_id,''),COALESCE(e.collection_id,''),COALESCE(e.actor_subject_id,''),COALESCE(e.operation_id,''),COALESCE(e.payload_sha256,''),CASE WHEN length(e.request) BETWEEN 1 AND 32768 THEN e.request END FROM collection_identities i JOIN collection_definitions d ON d.collection_id=i.collection_id LEFT JOIN collection_events e ON e.sequence=d.created_sequence WHERE i.collection_id=?`, id).Scan(&declarationLibrary, &creator, &creation, &declarationDigest, &created, &eventSequence, &eventLibrary, &eventCollection, &actor, &eventOperation, &eventDigest, &request)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return corrupt("collection declaration create binding")
	}
	op, err := DecodeCollectionRequest(request)
	if err != nil {
		return corrupt("collection declaration create binding")
	}
	_, digest, err := CanonicalCollectionRequest(op)
	if err != nil || created <= 0 || eventSequence != created || declarationLibrary != library || eventLibrary != library || op.LibraryID != library || eventCollection != id || op.CollectionID != id || creator != actor || creation != eventOperation || op.ID != creation || op.Kind != "create" || declarationDigest != eventDigest || eventDigest != digest {
		return corrupt("collection declaration create binding")
	}
	return nil
}

// Bounded canonical cross-record validation; neither a declaration nor a linked
// FK alone proves that the generated grant belongs to this actor's content.
func validateCollectionHistory(ctx context.Context, q queryer, library continuity.LibraryID) error {
	count := int64(0)
	total := int64(0)
	for {
		var seq, child, timestamp int64
		var lib continuity.LibraryID
		var actor SubjectID
		var id CollectionOperationID
		var collection CollectionID
		var request, receiptBytes []byte
		var digest string
		var version CollectionVersion
		err := q.QueryRowContext(ctx, `SELECT sequence,library_id,actor_subject_id,operation_id,collection_id,CASE WHEN length(request)<=32768 THEN request END,payload_sha256,CASE WHEN length(receipt)<=1024 THEN receipt END,result_version,COALESCE(generated_policy_sequence,0),recorded_unix_nano FROM collection_events WHERE sequence>? ORDER BY sequence LIMIT 1`, count).Scan(&seq, &lib, &actor, &id, &collection, &request, &digest, &receiptBytes, &version, &child, &timestamp)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		count++
		if count > 131072 {
			return corrupt("collection history bound")
		}
		total += int64(len(request) + len(receiptBytes))
		if total > 256<<20 {
			return corrupt("collection history bytes")
		}
		op, err := DecodeCollectionRequest(request)
		if err != nil {
			return corrupt("collection history request")
		}
		receipt, err := DecodeCollectionReceipt(receiptBytes)
		if err != nil {
			return corrupt("collection history receipt")
		}
		if seq != count || lib != library || op.LibraryID != library || op.ID != id || op.CollectionID != collection || receipt.OperationID != id || receipt.CollectionID != collection || receipt.ResultVersion != version {
			return corrupt("collection history identity")
		}
		_, expectedDigest, _ := CanonicalCollectionRequest(op)
		if digest != expectedDigest {
			return corrupt("collection history digest")
		}
		generated := CollectionPolicyOperationID("")
		if child != 0 {
			generated = generatedCollectionPolicyID(op.ID)
		}
		if _, _, err := CanonicalCollectionRecord(CollectionRecord{1, op, actor, version, generated, timestamp}); err != nil {
			return corrupt("collection history record")
		}
		if op.Kind == "create" {
			var creator SubjectID
			var creation CollectionOperationID
			var hash string
			var declarationLibrary continuity.LibraryID
			if err := q.QueryRowContext(ctx, `SELECT creator_subject_id,creation_operation_id,creation_payload_sha256,library_id FROM collection_identities WHERE collection_id=?`, collection).Scan(&creator, &creation, &hash, &declarationLibrary); err != nil {
				return err
			}
			if creator != actor || creation != op.ID || hash != digest || declarationLibrary != library {
				return corrupt("collection creation declaration")
			}
		}
		if child == 0 {
			continue
		}
		var childRequest, childReceipt []byte
		var childLibrary continuity.LibraryID
		var childActor SubjectID
		var administration, namespace, childDigest string
		var childID CollectionPolicyOperationID
		var childCollection sql.NullString
		var childTime int64
		err = q.QueryRowContext(ctx, `SELECT library_id,operator_subject_id,administration,namespace_subject_id,operation_id,collection_id,CASE WHEN length(request)<=32768 THEN request END,payload_sha256,CASE WHEN length(receipt)<=1024 THEN receipt END,recorded_unix_nano FROM collection_policy_events WHERE sequence=?`, child).Scan(&childLibrary, &childActor, &administration, &namespace, &childID, &childCollection, &childRequest, &childDigest, &childReceipt, &childTime)
		if err != nil {
			return err
		}
		p, err := DecodeCollectionPolicyRequest(childRequest)
		if err != nil {
			return corrupt("collection child request")
		}
		r, err := DecodeCollectionPolicyReceipt(childReceipt)
		if err != nil {
			return corrupt("collection child receipt")
		}
		_, hash, _ := CanonicalCollectionPolicyRequest(p)
		if childLibrary != library || childActor != actor || administration != "collection_service" || namespace != string(actor) || childID != generated || !childCollection.Valid || childCollection.String != string(collection) || p.LibraryID != library || p.CollectionID != collection || p.ID != generated || p.BindingOperationID != op.ID || p.BindingPayloadSHA256 != digest || childDigest != hash || r.LibraryID != library || r.OperationID != generated || r.PayloadSHA256 != hash || r.RecordedUnixNano != childTime || childTime != timestamp {
			return corrupt("collection child binding")
		}
		value, err := collectionValue(op)
		if err != nil {
			return err
		}
		var expected []CollectionGrantChange
		switch v := value.(type) {
		case *CreateCollection:
			grant := CollectionGrantValue{actor, CollectionGrantScope{"collection", collection}, []string{"collections.admin", "collections.edit", "collections.read"}}
			expected = []CollectionGrantChange{{"put", v.CreatorGrantID, nil, &grant}}
		case *EditCollectionGrants:
			expected = v.Changes
		default:
			return corrupt("unexpected collection child")
		}
		a, _ := json.Marshal(expected)
		b, _ := json.Marshal(p.Changes)
		if !bytes.Equal(a, b) {
			return corrupt("collection child changes")
		}
		var results []CollectionGrantResult
		resultRows, err := q.QueryContext(ctx, `SELECT grant_id,result_version,removed FROM collection_policy_results WHERE policy_sequence=? ORDER BY grant_id LIMIT 17`, child)
		if err != nil {
			return err
		}
		for resultRows.Next() {
			var result CollectionGrantResult
			if err := resultRows.Scan(&result.ID, &result.Version, &result.Removed); err != nil {
				resultRows.Close()
				return err
			}
			results = append(results, result)
		}
		if err := errors.Join(resultRows.Err(), resultRows.Close()); err != nil {
			return err
		}
		if _, _, err := CanonicalCollectionPolicyRecord(CollectionPolicyRecord{2, p, administration, childActor, results, childTime}); err != nil {
			return corrupt("collection child results")
		}

	}
}
