package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

type DeliverySelectRequest struct {
	Version     int                   `json:"version"`
	LibraryID   continuity.LibraryID  `json:"library_id"`
	ID          string                `json:"operation_id"`
	AssetID     continuity.AssetID    `json:"asset_id"`
	Mode        string                `json:"mode"`
	RevisionID  continuity.RevisionID `json:"revision_id,omitempty"`
	ExpiresUnix int64                 `json:"expires_unix"`
	MaxBytes    int64                 `json:"max_bytes"`
}
type DeliverySelection struct {
	Version       int                   `json:"version"`
	LibraryID     continuity.LibraryID  `json:"library_id"`
	SelectionID   string                `json:"selection_id"`
	OperationID   string                `json:"operation_id"`
	AssetID       continuity.AssetID    `json:"asset_id"`
	RevisionID    continuity.RevisionID `json:"revision_id"`
	SourceID      string                `json:"source_id"`
	SourceVersion int64                 `json:"source_version"`
	Content       continuity.Content    `json:"content"`
	ExpiresUnix   int64                 `json:"expires_unix"`
	Canceled      bool                  `json:"canceled"`
}
type DeliveryChunkBinding struct {
	// AuthorityIncarnation is the authenticated original pin, never an owner-status snapshot.
	AuthorityIncarnation string
	Selection        DeliverySelection
	RouteID          string
	RouteDigest      string
	ReaderLeafSHA256 string
	View             mutationauthority.ImageView
}

func CanonicalDeliverySelectRequest(r DeliverySelectRequest) ([]byte, string, error) {
	if r.Version != 1 || r.LibraryID.Validate() != nil || !validOperationID(r.ID) || r.AssetID.Validate() != nil || r.ExpiresUnix <= 0 || r.MaxBytes < 0 || r.MaxBytes > mutationauthority.MaxImageBytes {
		return nil, "", invalid("delivery selection envelope")
	}
	if r.Mode == "latest" {
		if r.RevisionID != "" {
			return nil, "", invalid("latest selection revision")
		}
	} else if r.Mode == "exact" {
		if r.RevisionID.Validate() != nil {
			return nil, "", invalid("exact selection revision")
		}
	} else {
		return nil, "", invalid("delivery selection mode")
	}
	b, err := json.Marshal(r)
	if err != nil || len(b) > 32768 {
		return nil, "", invalid("delivery selection bytes")
	}
	return b, domainDigest("juicemount.catalog.delivery.select\x00\x01", b), nil
}
func DecodeDeliverySelectRequest(b []byte) (DeliverySelectRequest, error) {
	var r DeliverySelectRequest
	if len(b) == 0 || len(b) > 32768 || decodeCanonicalValue(b, &r) != nil {
		return r, invalid("delivery selection JSON")
	}
	encoded, _, err := CanonicalDeliverySelectRequest(r)
	if err != nil || !bytes.Equal(encoded, b) {
		return DeliverySelectRequest{}, invalid("delivery selection request")
	}
	return r, nil
}
func deliveryRights(ctx context.Context, tx *sql.Tx, s *Store, c credential, asset continuity.AssetID) error {
	if c.codec != 3 || !c.scope.matches(asset) || !containsAction(c.actions, "assets.discover") || !containsAction(c.actions, "revisions.read") || !containsAction(c.actions, "bytes.read") {
		return ErrNotFound
	}
	rights, err := readDiscoveryRights(ctx, tx, s.library, c)
	if err != nil {
		return err
	}
	if !rights.allows(asset, "assets.discover") || !rights.allows(asset, "revisions.read") {
		return ErrNotFound
	}
	granted := false
	err = walkRows(ctx, tx, `SELECT grant_id,library_id,subject_id,scope_kind,COALESCE(asset_id,''),action,grant_version FROM byte_grants INDEXED BY byte_grants_access WHERE subject_id=? AND removed=0 AND (scope_kind='library' OR (scope_kind='asset' AND asset_id=?)) LIMIT 16385`, func(rows *sql.Rows) error {
		var id, library, subject, kind, assetID, action, version string
		if err := rows.Scan(&id, &library, &subject, &kind, &assetID, &action, &version); err != nil {
			return err
		}
		v := ByteGrantValue{SubjectID: SubjectID(subject), Scope: Scope{Kind: kind, AssetID: continuity.AssetID(assetID)}, Action: action}
		if library != string(s.library) || !validRandomID(id, "bgr_") || !validRandomID(version, "bgv_") || !validByteGrantValue(v) || subject != string(c.subject) {
			return corrupt("byte grant access")
		}
		if v.Scope.matches(asset) {
			granted = true
		}
		return nil
	}, c.subject, asset)
	if err != nil {
		return err
	}
	if !granted {
		return ErrNotFound
	}
	return nil
}
func readCurrentDeliverySource(ctx context.Context, q queryer, s *Store, asset continuity.AssetID, revision continuity.RevisionID) (DeliveryChunkBinding, error) {
	var binding DeliveryChunkBinding
	var image []byte
	var library continuity.LibraryID
	var enabled bool
	var size int64
	var digest, incarnation string
	err := q.QueryRowContext(ctx, `SELECT d.library_id,d.source_id,d.source_version,d.content_size,d.content_sha256,d.authority_incarnation,d.route_id,d.route_digest,d.reader_leaf_sha256,d.image_view,d.enabled FROM delivery_source_heads h JOIN delivery_sources d ON d.source_id=h.source_id AND d.source_version=h.source_version WHERE h.library_id=? AND h.asset_id=? AND h.revision_id=?`, s.library, asset, revision).Scan(&library, &binding.Selection.SourceID, &binding.Selection.SourceVersion, &size, &digest, &incarnation, &binding.RouteID, &binding.RouteDigest, &binding.ReaderLeafSHA256, &image, &enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return binding, ErrNotFound
	}
	if err != nil {
		return binding, err
	}
	if library != s.library || !validRandomID(binding.Selection.SourceID, "dsrc_") || binding.Selection.SourceVersion < 1 || mutationauthority.DecodeImageCanonical(image, &binding.View) != nil || binding.View.Validate() != nil || binding.View.Content.Size != size || binding.View.Content.SHA256 != digest || binding.View.LibraryID != string(s.library) || !validCollectionDigest(binding.RouteDigest) || !mutationauthority.ImageHashValid(binding.ReaderLeafSHA256) {
		return DeliveryChunkBinding{}, corrupt("delivery source")
	}
	if supportsDeliveryLifecycle(s.version) {
		slot, err := readSourceTerminal(ctx, q, s.library, binding.Selection.SourceID, binding.Selection.SourceVersion)
		if err != nil {
			return DeliveryChunkBinding{}, err
		}
		if slot.legacy == enabled {
			return DeliveryChunkBinding{}, corrupt("source terminal coupling")
		}
		if slot.bit {
			return DeliveryChunkBinding{}, ErrNotFound
		}
	}
	if !enabled {
		return DeliveryChunkBinding{}, ErrNotFound
	}
	authority, err := readAuthority(ctx, q, s.library)
	if err != nil {
		return DeliveryChunkBinding{}, err
	}
	if incarnation != authority.Incarnation {
		return DeliveryChunkBinding{}, ErrNotFound
	}
	return binding, nil
}
func loadSelection(ctx context.Context, q queryer, s *Store, id string) (DeliverySelection, []byte, string, string, int64, int64, int64, error) {
	var selection DeliverySelection
	var request []byte
	var requestDigest, selector, incarnation string
	var credentialGeneration, subjectGeneration, budget int64
	var library continuity.LibraryID
	err := q.QueryRowContext(ctx, `SELECT selection_id,library_id,operation_id,request,payload_sha256,asset_id,revision_id,source_id,source_version,credential_selector,credential_generation,authority_incarnation,subject_generation,content_size,content_sha256,expires_unix,byte_budget,canceled FROM delivery_selections WHERE selection_id=?`, id).Scan(&selection.SelectionID, &library, &selection.OperationID, &request, &requestDigest, &selection.AssetID, &selection.RevisionID, &selection.SourceID, &selection.SourceVersion, &selector, &credentialGeneration, &incarnation, &subjectGeneration, &selection.Content.Size, &selection.Content.SHA256, &selection.ExpiresUnix, &budget, &selection.Canceled)
	if errors.Is(err, sql.ErrNoRows) {
		return selection, nil, "", "", 0, 0, 0, ErrNotFound
	}
	if err != nil {
		return selection, nil, "", "", 0, 0, 0, err
	}
	selection.Version = 1
	selection.LibraryID = library
	if supportsDeliveryLifecycle(s.version) {
		if _, err := readSelectionTerminal(ctx, q, s.library, id, selection.Canceled); err != nil {
			return DeliverySelection{}, nil, "", "", 0, 0, 0, err
		}
	}
	op, err := DecodeDeliverySelectRequest(request)
	if err != nil || library != s.library || op.ID != selection.OperationID || op.AssetID != selection.AssetID || requestDigest != domainDigest("juicemount.catalog.delivery.select\x00\x01", request) || !validRandomID(selection.SelectionID, "dsel_") || selection.Content.Validate() != nil {
		return DeliverySelection{}, nil, "", "", 0, 0, 0, corrupt("delivery selection")
	}
	return selection, request, selector, incarnation, credentialGeneration, subjectGeneration, budget, nil
}
func (s *Store) selectionFence(ctx context.Context, f *deliveryClockTxn, bearer []byte, id string) (DeliveryChunkBinding, error) {
	var binding DeliveryChunkBinding
	tx := f.tx
	now, err := f.observe(ctx)
	if err != nil {
		return binding, err
	}
	handle, c, sub, err := s.authenticateSnapshotAt(ctx, tx, s.library, bearer, now)
	if err != nil {
		return binding, err
	}
	selection, _, selector, incarnation, credentialGeneration, subjectGeneration, budget, err := loadSelection(ctx, tx, s, id)
	if err != nil {
		return binding, err
	}
	if handle.selector != selector || c.generation != credentialGeneration || c.incarnation != incarnation || sub.AuthorizationGeneration != subjectGeneration || selection.Canceled || now >= selection.ExpiresUnix || now >= c.expires || selection.Content.Size > budget {
		return binding, ErrNotFound
	}
	if err := deliveryRights(ctx, tx, s, c, selection.AssetID); err != nil {
		return binding, err
	}
	revision, err := readRevision(ctx, tx, s.library, selection.RevisionID)
	if err != nil {
		return binding, err
	}
	if revision.Manifest.AssetID != selection.AssetID || revision.Manifest.Content != selection.Content {
		return binding, corrupt("selection revision")
	}
	binding, err = readCurrentDeliverySource(ctx, tx, s, selection.AssetID, selection.RevisionID)
	if err != nil {
		return DeliveryChunkBinding{}, err
	}
	if binding.Selection.SourceID != selection.SourceID || binding.Selection.SourceVersion != selection.SourceVersion || binding.View.Content.Size != selection.Content.Size || binding.View.Content.SHA256 != selection.Content.SHA256 {
		return DeliveryChunkBinding{}, ErrNotFound
	}
	binding.Selection = selection
	binding.AuthorityIncarnation = incarnation
	return binding, nil
}
func (s *Store) SelectDelivery(ctx context.Context, bearer []byte, input DeliverySelectRequest) (result DeliverySelection, resultErr error) {
	if err := s.enter(); err != nil {
		return DeliverySelection{}, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return DeliverySelection{}, ErrUpgradeRequired
	}
	data, digest, err := CanonicalDeliverySelectRequest(input)
	if err != nil {
		return DeliverySelection{}, err
	}
	op, err := DecodeDeliverySelectRequest(data)
	if err != nil {
		return DeliverySelection{}, err
	}
	if op.LibraryID != s.library {
		return DeliverySelection{}, ErrNotFound
	}
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return DeliverySelection{}, err
	}
	defer f.finish(&resultErr)
	tx := f.tx
	if err := s.checkVersion(ctx, tx); err != nil {
		return DeliverySelection{}, err
	}
	now, err := f.observe(ctx)
	if err != nil {
		return DeliverySelection{}, err
	}
	handle, c, sub, err := s.authenticateSnapshotAt(ctx, tx, s.library, bearer, now)
	if err != nil {
		return DeliverySelection{}, err
	}
	var oldID string
	err = tx.QueryRowContext(ctx, `SELECT selection_id FROM delivery_selections WHERE library_id=? AND subject_id=? AND operation_id=?`, s.library, c.subject, op.ID).Scan(&oldID)
	if err == nil {
		priorData := []byte(nil)
		var savedDigest string
		if err := tx.QueryRowContext(ctx, `SELECT request,payload_sha256 FROM delivery_selections WHERE selection_id=?`, oldID).Scan(&priorData, &savedDigest); err != nil {
			return DeliverySelection{}, err
		}
		if !bytes.Equal(priorData, data) || savedDigest != digest {
			return DeliverySelection{}, ErrChangedRequest
		}
		binding, err := s.selectionFence(ctx, f, bearer, oldID)
		if err != nil {
			return DeliverySelection{}, err
		}
		if err := ctx.Err(); err != nil {
			return DeliverySelection{}, err
		}
		if err := s.checkPaths(); err != nil {
			return DeliverySelection{}, err
		}
		if err := f.commit(); err != nil {
			return DeliverySelection{}, fmt.Errorf("delivery select retry commit uncertain (operation %s): %w", op.ID, err)
		}
		return binding.Selection, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return DeliverySelection{}, err
	}
	if op.ExpiresUnix <= now || op.ExpiresUnix > now+900 || op.ExpiresUnix > c.expires {
		return DeliverySelection{}, invalid("delivery expiry")
	}
	if err := deliveryRights(ctx, tx, s, c, op.AssetID); err != nil {
		return DeliverySelection{}, err
	}
	asset, err := readAsset(ctx, tx, s.library, op.AssetID)
	if err != nil {
		return DeliverySelection{}, err
	}
	revisionID := op.RevisionID
	if op.Mode == "latest" {
		revisionID = asset.Head
	}
	if revisionID == "" {
		return DeliverySelection{}, ErrNotFound
	}
	revision, err := readRevision(ctx, tx, s.library, revisionID)
	if err != nil {
		return DeliverySelection{}, err
	}
	if revision.Manifest.AssetID != op.AssetID || revision.Manifest.Content.Size > op.MaxBytes {
		return DeliverySelection{}, ErrNotFound
	}
	binding, err := readCurrentDeliverySource(ctx, tx, s, op.AssetID, revisionID)
	if err != nil {
		return DeliverySelection{}, err
	}
	if binding.View.Content.Size != revision.Manifest.Content.Size || binding.View.Content.SHA256 != revision.Manifest.Content.SHA256 {
		return DeliverySelection{}, corrupt("source revision content")
	}
	var count, canonical int64
	if err := tx.QueryRowContext(ctx, `SELECT selections,canonical_bytes FROM delivery_quota WHERE singleton=1`).Scan(&count, &canonical); err != nil {
		return DeliverySelection{}, corrupt("delivery quota")
	}
	if count >= 4096 || canonical+int64(len(data)) > 64<<20 {
		return DeliverySelection{}, ErrLimit
	}
	id, err := randomIdentity("dsel_")
	if err != nil {
		return DeliverySelection{}, err
	}
	timestamp := s.now().UnixNano()
	selection := DeliverySelection{1, s.library, id, op.ID, op.AssetID, revisionID, binding.Selection.SourceID, binding.Selection.SourceVersion, revision.Manifest.Content, op.ExpiresUnix, false}
	if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_selections(selection_id,library_id,subject_id,operation_id,request,payload_sha256,asset_id,revision_id,source_id,source_version,credential_selector,credential_generation,authority_incarnation,subject_generation,content_size,content_sha256,expires_unix,byte_budget,canceled,created_unix_nano) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)`, id, s.library, c.subject, op.ID, data, digest, op.AssetID, revisionID, selection.SourceID, selection.SourceVersion, handle.selector, c.generation, c.incarnation, sub.AuthorizationGeneration, selection.Content.Size, selection.Content.SHA256, selection.ExpiresUnix, op.MaxBytes, timestamp); err != nil {
		return DeliverySelection{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_quota SET selections=selections+1,canonical_bytes=canonical_bytes+? WHERE singleton=1`, len(data)); err != nil {
		return DeliverySelection{}, err
	}
	if err := ctx.Err(); err != nil {
		return DeliverySelection{}, err
	}
	if err := s.checkPaths(); err != nil {
		return DeliverySelection{}, err
	}
	if err := f.commit(); err != nil {
		return DeliverySelection{}, fmt.Errorf("delivery select commit uncertain (operation %s): %w", op.ID, err)
	}
	return selection, nil
}
func (s *Store) AdmitDeliveryChunk(ctx context.Context, bearer []byte, id string, index int64) (result DeliveryChunkBinding, resultErr error) {
	if err := s.enter(); err != nil {
		return DeliveryChunkBinding{}, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return DeliveryChunkBinding{}, ErrUpgradeRequired
	}
	if !validRandomID(id, "dsel_") {
		return DeliveryChunkBinding{}, invalid("selection identity")
	}
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return DeliveryChunkBinding{}, err
	}
	defer f.finish(&resultErr)
	tx := f.tx
	if err := s.checkVersion(ctx, tx); err != nil {
		return DeliveryChunkBinding{}, err
	}
	binding, err := s.selectionFence(ctx, f, bearer, id)
	if err != nil {
		return DeliveryChunkBinding{}, err
	}
	if index < 0 || index >= int64(len(binding.View.Chunks)) {
		return DeliveryChunkBinding{}, invalid("chunk index")
	}
	if err := ctx.Err(); err != nil {
		return DeliveryChunkBinding{}, err
	}
	if err := s.checkPaths(); err != nil {
		return DeliveryChunkBinding{}, err
	}
	if err := f.commit(); err != nil {
		return DeliveryChunkBinding{}, err
	}
	return binding, nil
}
func (s *Store) AdmitDelivery(ctx context.Context, bearer []byte, id string) (result DeliveryChunkBinding, resultErr error) {
	if err := s.enter(); err != nil {
		return DeliveryChunkBinding{}, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return DeliveryChunkBinding{}, ErrUpgradeRequired
	}
	if !validRandomID(id, "dsel_") {
		return DeliveryChunkBinding{}, invalid("selection identity")
	}
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return DeliveryChunkBinding{}, err
	}
	defer f.finish(&resultErr)
	tx := f.tx
	if err := s.checkVersion(ctx, tx); err != nil {
		return DeliveryChunkBinding{}, err
	}
	binding, err := s.selectionFence(ctx, f, bearer, id)
	if err != nil {
		return DeliveryChunkBinding{}, err
	}
	if err := ctx.Err(); err != nil {
		return DeliveryChunkBinding{}, err
	}
	if err := s.checkPaths(); err != nil {
		return DeliveryChunkBinding{}, err
	}
	if err := f.commit(); err != nil {
		return DeliveryChunkBinding{}, err
	}
	return binding, nil
}
func (s *Store) DeliveryStatus(ctx context.Context, bearer []byte, id string) (result DeliverySelection, resultErr error) {
	if err := s.enter(); err != nil {
		return DeliverySelection{}, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return DeliverySelection{}, ErrUpgradeRequired
	}
	if !validRandomID(id, "dsel_") {
		return DeliverySelection{}, invalid("selection identity")
	}
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return DeliverySelection{}, err
	}
	defer f.finish(&resultErr)
	tx := f.tx
	if err := s.checkVersion(ctx, tx); err != nil {
		return DeliverySelection{}, err
	}
	binding, err := s.selectionFence(ctx, f, bearer, id)
	if err != nil {
		return DeliverySelection{}, err
	}
	if err := ctx.Err(); err != nil {
		return DeliverySelection{}, err
	}
	if err := s.checkPaths(); err != nil {
		return DeliverySelection{}, err
	}
	if err := f.commit(); err != nil {
		return DeliverySelection{}, err
	}
	return binding.Selection, nil
}
