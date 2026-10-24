package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationclient"
)

type SourceBindRequest struct {
	Version               int                        `json:"version"`
	LibraryID             continuity.LibraryID       `json:"library_id"`
	ID                    string                     `json:"operation_id"`
	ExpectedIncarnation   string                     `json:"expected_incarnation"`
	AssetID               continuity.AssetID         `json:"asset_id"`
	RevisionID            continuity.RevisionID      `json:"revision_id"`
	SourceID              string                     `json:"source_id"`
	ExpectedSourceVersion int64                      `json:"expected_source_version"`
	RouteID               string                     `json:"route_id"`
	RouteDigest           string                     `json:"route_digest"`
	ReaderLeafSHA256      string                     `json:"reader_leaf_sha256"`
	Image                 mutationauthority.ImageRef `json:"image"`
}
type SourceReceipt struct {
	Version          int                   `json:"version"`
	LibraryID        continuity.LibraryID  `json:"library_id"`
	OperationID      string                `json:"operation_id"`
	PayloadSHA256    string                `json:"payload_sha256"`
	SourceID         string                `json:"source_id"`
	SourceVersion    int64                 `json:"source_version"`
	AssetID          continuity.AssetID    `json:"asset_id"`
	RevisionID       continuity.RevisionID `json:"revision_id"`
	Content          continuity.Content    `json:"content"`
	Enabled          bool                  `json:"enabled"`
	RecordedUnixNano int64                 `json:"recorded_unix_nano"`
}
type DeliverySource struct {
	Receipt              SourceReceipt
	RouteID              string
	RouteDigest          string
	ReaderLeafSHA256     string
	View                 mutationauthority.ImageView
	AuthorityIncarnation string
}

func CanonicalSourceBindRequest(r SourceBindRequest) ([]byte, string, error) {
	if r.Version != 1 || r.LibraryID.Validate() != nil || !validOperationID(r.ID) || !validRandomID(r.ExpectedIncarnation, "auth_") || r.AssetID.Validate() != nil || r.RevisionID.Validate() != nil || !validText(r.RouteID, 128, false) || !validCollectionDigest(r.RouteDigest) || !mutationauthority.ImageHashValid(r.ReaderLeafSHA256) || r.Image.Validate() != nil || r.Image.LibraryID != string(r.LibraryID) {
		return nil, "", invalid("source bind envelope")
	}
	if r.ExpectedSourceVersion == 0 {
		if r.SourceID != "" {
			return nil, "", invalid("new source identity")
		}
	} else if r.ExpectedSourceVersion < 0 || !validRandomID(r.SourceID, "dsrc_") {
		return nil, "", invalid("source version")
	}
	b, err := json.Marshal(r)
	if err != nil || len(b) > 32768 {
		return nil, "", invalid("source bind bytes")
	}
	return b, domainDigest("juicemount.catalog.delivery.source.operation\x00\x01", b), nil
}
func DecodeSourceBindRequest(b []byte) (SourceBindRequest, error) {
	var r SourceBindRequest
	if len(b) == 0 || len(b) > 32768 || decodeCanonicalValue(b, &r) != nil {
		return r, invalid("source bind JSON")
	}
	encoded, _, err := CanonicalSourceBindRequest(r)
	if err != nil || !bytes.Equal(encoded, b) {
		return SourceBindRequest{}, invalid("source bind request")
	}
	return r, nil
}
func decodeSourceReceipt(b []byte) (SourceReceipt, error) {
	var r SourceReceipt
	if len(b) == 0 || len(b) > 2048 || decodeCanonicalValue(b, &r) != nil || r.Version != 1 || r.LibraryID.Validate() != nil || !validOperationID(r.OperationID) || !validCollectionDigest(r.PayloadSHA256) || !validRandomID(r.SourceID, "dsrc_") || r.SourceVersion < 1 || r.AssetID.Validate() != nil || r.RevisionID.Validate() != nil || r.Content.Validate() != nil || r.RecordedUnixNano <= 0 {
		return SourceReceipt{}, corrupt("source receipt")
	}
	return r, nil
}
func sourcePrior(ctx context.Context, q queryer, op SourceBindRequest, data []byte, digest string) (SourceReceipt, bool, error) {
	var saved, receipt []byte
	var savedDigest string
	err := q.QueryRowContext(ctx, `SELECT request,payload_sha256,receipt FROM delivery_source_events WHERE operation_id=?`, op.ID).Scan(&saved, &savedDigest, &receipt)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceReceipt{}, false, nil
	}
	if err != nil {
		return SourceReceipt{}, false, err
	}
	if !bytes.Equal(saved, data) {
		return SourceReceipt{}, false, ErrChangedRequest
	}
	r, err := decodeSourceReceipt(receipt)
	if err != nil || savedDigest != digest || r.PayloadSHA256 != digest || r.OperationID != op.ID || r.LibraryID != op.LibraryID || r.AssetID != op.AssetID || r.RevisionID != op.RevisionID {
		return SourceReceipt{}, false, corrupt("source receipt link")
	}
	return r, true, nil
}

type sourceImageStream struct {
	ctx     context.Context
	client  *mutationclient.Client
	view    mutationauthority.ImageView
	index   int64
	current *bytes.Reader
}

func (s *sourceImageStream) Read(p []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	for {
		if s.current != nil {
			n, err := s.current.Read(p)
			if err == io.EOF {
				s.current = nil
				if n > 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}
		if s.index == int64(len(s.view.Chunks)) {
			return 0, io.EOF
		}
		b, err := s.client.ImageReadback(s.ctx, s.view, s.index)
		if err != nil {
			return 0, err
		}
		s.index++
		s.current = bytes.NewReader(b)
	}
}

// BindDeliverySource verifies the complete actual image before committing a
// metadata-to-custody binding. The supplied client must come from the owner's
// pinned private route registry; only its configured route identity is stored.
func (o *LocalOwnerService) BindDeliverySource(ctx context.Context, input SourceBindRequest, client *mutationclient.Client) (SourceBindResult, error) {
	if err := o.enter(); err != nil {
		return SourceBindResult{}, err
	}
	defer o.store.leave()
	s := o.store
	if !supportsDelivery(s.version) {
		return SourceBindResult{}, ErrUpgradeRequired
	}
	data, digest, err := CanonicalSourceBindRequest(input)
	if err != nil {
		return SourceBindResult{}, err
	}
	op, err := DecodeSourceBindRequest(data)
	if err != nil {
		return SourceBindResult{}, err
	}
	if op.LibraryID != o.library {
		return SourceBindResult{}, invalid("source library")
	}
	snap, done, err := s.readSnapshot(ctx)
	if err != nil {
		return SourceBindResult{}, err
	}
	if _, authErr := o.currentDeliveryOwner(ctx, snap); authErr != nil {
		return SourceBindResult{}, errors.Join(authErr, done())
	}
	prior, found, priorErr := sourcePrior(ctx, snap, op, data, digest)
	var priorStatus SourceStatus
	if priorErr == nil && found {
		priorStatus, priorErr = sourceStatusSnapshot(ctx, snap, s, prior.SourceID, prior.SourceVersion)
	}
	closeErr := done()
	if priorErr != nil || closeErr != nil {
		return SourceBindResult{}, errors.Join(priorErr, closeErr)
	}
	if found {
		return SourceBindResult{prior, priorStatus}, nil
	}
	if client == nil {
		return SourceBindResult{}, invalid("delivery reader")
	}
	view, err := client.ImageStatus(ctx, op.Image)
	if err != nil {
		return SourceBindResult{}, err
	}
	if view.Validate() != nil || view.LibraryID != string(s.library) || view.Content.Size > mutationauthority.MaxImageBytes {
		return SourceBindResult{}, ErrConflict
	}
	if err := continuity.VerifyContent(&sourceImageStream{ctx: ctx, client: client, view: view}, continuity.Content{Size: view.Content.Size, SHA256: view.Content.SHA256}); err != nil {
		return SourceBindResult{}, err
	}
	current, err := client.ImageStatus(ctx, op.Image)
	if err != nil {
		return SourceBindResult{}, err
	}
	imageBytes, _ := json.Marshal(view)
	currentBytes, _ := json.Marshal(current)
	if !bytes.Equal(currentBytes, imageBytes) {
		return SourceBindResult{}, ErrConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceBindResult{}, err
	}
	defer tx.Rollback()
	if err := s.checkVersion(ctx, tx); err != nil {
		return SourceBindResult{}, err
	}
	authority, err := o.currentDeliveryOwner(ctx, tx)
	if err != nil {
		return SourceBindResult{}, err
	}
	prior, found, err = sourcePrior(ctx, tx, op, data, digest)
	if err != nil {
		return SourceBindResult{}, err
	}
	if found {
		status, err := sourceStatusSnapshot(ctx, tx, s, prior.SourceID, prior.SourceVersion)
		if err != nil {
			return SourceBindResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return SourceBindResult{}, err
		}
		if err := s.checkPaths(); err != nil {
			return SourceBindResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return SourceBindResult{}, terminalCommitError("source bind retry", err)
		}
		return SourceBindResult{prior, status}, nil
	}
	if err != nil {
		return SourceBindResult{}, err
	}
	if authority.Incarnation == "" || authority.Incarnation != op.ExpectedIncarnation {
		return SourceBindResult{}, ErrConflict
	}
	owner, err := readSubject(ctx, tx, s.library, authority.OwnerSubjectID)
	if err != nil {
		return SourceBindResult{}, err
	}
	if !owner.Value.Enabled || owner.Value.Kind != "person" {
		return SourceBindResult{}, ErrConflict
	}
	revision, err := readRevision(ctx, tx, s.library, op.RevisionID)
	if err != nil {
		return SourceBindResult{}, err
	}
	if revision.Manifest.AssetID != op.AssetID || revision.Manifest.Content.Size != view.Content.Size || revision.Manifest.Content.SHA256 != view.Content.SHA256 {
		return SourceBindResult{}, ErrConflict
	}
	id := op.SourceID
	version := op.ExpectedSourceVersion + 1
	if op.ExpectedSourceVersion == 0 {
		var existing int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM delivery_source_heads WHERE library_id=? AND asset_id=? AND revision_id=?`, s.library, op.AssetID, op.RevisionID).Scan(&existing); err != nil {
			return SourceBindResult{}, err
		}
		if existing != 0 {
			return SourceBindResult{}, ErrConflict
		}
		id, err = randomIdentity("dsrc_")
		if err != nil {
			return SourceBindResult{}, err
		}
	} else {
		var priorVersion int64
		var priorAsset continuity.AssetID
		var priorRevision continuity.RevisionID
		if err := tx.QueryRowContext(ctx, `SELECT source_version,asset_id,revision_id FROM delivery_source_heads WHERE source_id=? AND library_id=?`, id, s.library).Scan(&priorVersion, &priorAsset, &priorRevision); err != nil {
			return SourceBindResult{}, ErrConflict
		}
		if priorVersion != op.ExpectedSourceVersion || priorAsset != op.AssetID || priorRevision != op.RevisionID {
			return SourceBindResult{}, ErrConflict
		}
	}
	var sourceCount, canonical int64
	if err := tx.QueryRowContext(ctx, `SELECT source_versions,canonical_bytes FROM delivery_quota WHERE singleton=1`).Scan(&sourceCount, &canonical); err != nil {
		return SourceBindResult{}, corrupt("delivery quota")
	}
	timestamp := s.now().UnixNano()
	receipt := SourceReceipt{1, s.library, op.ID, digest, id, version, op.AssetID, op.RevisionID, revision.Manifest.Content, true, timestamp}
	receiptBytes, err := json.Marshal(receipt)
	if err != nil || len(receiptBytes) > 2048 {
		return SourceBindResult{}, invalid("source receipt bytes")
	}
	if sourceCount >= 1024 || canonical+int64(len(data)+len(receiptBytes)+len(imageBytes)) > 64<<20 {
		return SourceBindResult{}, ErrLimit
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_sources(source_id,source_version,library_id,asset_id,revision_id,content_size,content_sha256,authority_incarnation,route_id,route_digest,reader_leaf_sha256,image_view,enabled,created_unix_nano) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1,?)`, id, version, s.library, op.AssetID, op.RevisionID, view.Content.Size, view.Content.SHA256, authority.Incarnation, op.RouteID, op.RouteDigest, op.ReaderLeafSHA256, imageBytes, timestamp); err != nil {
		return SourceBindResult{}, err
	}
	if op.ExpectedSourceVersion == 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_source_heads(library_id,asset_id,revision_id,source_id,source_version) VALUES(?,?,?,?,?)`, s.library, op.AssetID, op.RevisionID, id, version); err != nil {
			return SourceBindResult{}, err
		}
	} else {
		if err := changedOne(tx.ExecContext(ctx, `UPDATE delivery_source_heads SET source_version=? WHERE library_id=? AND asset_id=? AND revision_id=? AND source_id=? AND source_version=?`, version, s.library, op.AssetID, op.RevisionID, id, op.ExpectedSourceVersion)); err != nil {
			return SourceBindResult{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_source_events(operation_id,library_id,request,payload_sha256,receipt,source_id,source_version,recorded_unix_nano) VALUES(?,?,?,?,?,?,?,?)`, op.ID, s.library, data, digest, receiptBytes, id, version, timestamp); err != nil {
		return SourceBindResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_quota SET source_versions=source_versions+1,canonical_bytes=canonical_bytes+? WHERE singleton=1`, len(data)+len(receiptBytes)+len(imageBytes)); err != nil {
		return SourceBindResult{}, err
	}
	status, err := sourceStatusSnapshot(ctx, tx, s, id, version)
	if err != nil {
		return SourceBindResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SourceBindResult{}, err
	}
	if err := s.checkPaths(); err != nil {
		return SourceBindResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourceBindResult{}, fmt.Errorf("source bind commit uncertain (operation %s): %w", op.ID, err)
	}
	return SourceBindResult{receipt, status}, nil
}
