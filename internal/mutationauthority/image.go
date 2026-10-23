package mutationauthority

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type imageStore struct {
	config                                ImageConfig
	volume                                string
	images, logical, metadata, activation int64
	active                                bool
}

func readImageStore(q schemaReader) (imageStore, error) {
	var s imageStore
	s.config.Version = 1
	err := q.QueryRow(`SELECT store_id,volume,max_images,max_logical,max_metadata,images,logical,metadata,active,activation FROM image_store WHERE id=1`).Scan(
		&s.config.StoreID, &s.volume, &s.config.MaxImages, &s.config.MaxLogicalBytes, &s.config.MaxMetadataBytes, &s.images, &s.logical, &s.metadata, &s.active, &s.activation)
	return s, err
}
func readImage(q schemaReader, id string, activation int64) (ImageRecord, error) {
	var r ImageRecord
	var b, receipt []byte
	var producer, operation, incarnation, library string
	var logical, metadata int64
	err := q.QueryRow(`SELECT CASE WHEN length(envelope)<=524288 THEN envelope ELSE NULL END,digest,state,CASE WHEN length(receipt)<=8192 THEN receipt ELSE NULL END,producer,operation_id,incarnation,library_id,logical,metadata FROM image_seals WHERE image_id=?`, id).Scan(&b, &r.Digest, &r.State, &receipt, &producer, &operation, &incarnation, &library, &logical, &metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNoSuchOperation
	}
	if err != nil {
		return r, err
	}
	if DecodeImageCanonical(b, &r.Seal) != nil {
		return r, ErrInvalid
	}
	canonical, digest, err := r.Seal.Canonical()
	if err != nil || !bytes.Equal(canonical, b) || digest != r.Digest || r.Seal.ImageID != id || r.Seal.Producer != producer || r.Seal.OperationID != operation || r.Seal.Incarnation != incarnation || r.Seal.LibraryID != library || r.Seal.Content.Size != logical || metadata != int64(len(b))+8192*int64(len(r.Seal.Receipts)+2) {
		return r, ErrInvalid
	}
	r.Activation = activation
	if r.State == "SEALED" {
		var sr ImageSealReceipt
		if DecodeImageCanonical(receipt, &sr) != nil || sr != (ImageSealReceipt{1, r.Seal.StoreID, id, r.Seal.OperationID, r.Digest, activation, 1}) {
			return r, ErrInvalid
		}
		r.Receipt = &sr
		var reader sql.NullString
		if err = q.QueryRow(`SELECT reader,generation FROM image_readers WHERE image_id=?`, id).Scan(&reader, &r.Generation); err != nil {
			return r, ErrInvalid
		}
		if reader.Valid {
			value := reader.String
			r.Reader = &value
			if !ImageHashValid(value) {
				return r, ErrInvalid
			}
		}
	} else if r.State != "RESERVED" || receipt != nil {
		return r, ErrInvalid
	}
	return r, nil
}

// ConfigureImages runs while the original journal lock is held and before a
// listener or SDK is started. Omitting config refuses any initialized authority.
func (j *Journal) ConfigureImages(ctx context.Context, volume string, cfg *ImageConfig) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}
	var version int
	if err := j.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if cfg == nil {
		if version == 4 {
			return nil
		}
		return ErrImageInactive
	}
	if cfg.Validate() != nil || !imageIdentifier(volume) {
		return ErrInvalid
	}
	if version == 4 {
		if !cfg.Initialize {
			return ErrImageInactive
		}
		tx, err := j.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, name := range []string{"image_store", "image_seals", "image_parts", "image_readers", "image_reader_events"} {
			if _, err = tx.ExecContext(ctx, imageSchema[name]); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO image_store(id,store_id,volume,max_images,max_logical,max_metadata,images,logical,metadata,active,activation) VALUES(1,?,?,?,?,?,0,0,0,1,1)`, cfg.StoreID, volume, cfg.MaxImages, cfg.MaxLogicalBytes, cfg.MaxMetadataBytes); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `PRAGMA user_version=5`); err != nil {
			return err
		}
		return tx.Commit()
	}
	s, err := readImageStore(j.db)
	if err != nil {
		return err
	}
	want := *cfg
	want.Initialize = false
	if s.config != want || s.volume != volume {
		return ErrInvalid
	}
	if !s.active {
		return ErrImageInactive
	}
	return nil
}
func imageActive(q schemaReader, store string) (imageStore, error) {
	var version int
	if err := q.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return imageStore{}, err
	}
	if version != 5 {
		return imageStore{}, ErrImageInactive
	}
	s, err := readImageStore(q)
	if errors.Is(err, sql.ErrNoRows) {
		return s, ErrImageInactive
	}
	if err != nil {
		return s, err
	}
	if s.config.StoreID != store {
		return s, ErrInvalid
	}
	if !s.active {
		return s, ErrImageInactive
	}
	return s, nil
}

// ImageForProducer reveals an immutable binding only to its original producer.
// Callers map absent and wrong-principal results to the same network refusal.
func (j *Journal) ImageForProducer(ctx context.Context, store, id, producer string) (ImageRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ImageRecord{}, ErrClosed
	}
	s, err := imageActive(j.db, store)
	if err != nil {
		return ImageRecord{}, err
	}
	r, err := readImage(j.db, id, s.activation)
	if err != nil {
		return ImageRecord{}, err
	}
	if r.Seal.Producer != producer {
		return ImageRecord{}, ErrInvalid
	}
	return r, nil
}
func (j *Journal) InspectImage(ctx context.Context, ref ImageRef) (ImageRecord, error) {
	if ref.Validate() != nil {
		return ImageRecord{}, ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ImageRecord{}, ErrClosed
	}
	s, err := imageActive(j.db, ref.StoreID)
	if err != nil {
		return ImageRecord{}, err
	}
	r, err := readImage(j.db, ref.ImageID, s.activation)
	if err == nil && r.Seal.LibraryID != ref.LibraryID {
		return ImageRecord{}, ErrNoSuchOperation
	}
	return r, err
}
func verifyImageCustody(q schemaReader, s ImageSeal) error {
	for _, p := range s.Receipts {
		c := p.Chunk
		h, _ := json.Marshal(struct {
			Kind   string     `json:"kind"`
			Volume string     `json:"volume"`
			Chunk  ImageChunk `json:"chunk"`
		}{"custody-v1", s.Volume, c})
		if p.RequestSHA256 != DigestBytes(h) || p.ArtifactSize != 4+int64(len(h))+c.Size {
			return ErrInvalid
		}
		var r Receipt
		var state string
		err := q.QueryRow(`SELECT volume,id,session,epoch,digest,expected_revision,revision,evidence,committed_ns,kind,file_id,state FROM operations WHERE volume=? AND id=?`, s.Volume, "custody-v1:"+c.ID).Scan(&r.Volume, &r.ID, &r.Session, &r.Epoch, &r.Digest, &r.ExpectedRevision, &r.Revision, &r.Evidence, &r.CommittedAtUnixNS, &r.Kind, &r.FileID, &state)
		if err != nil {
			return ErrInvalid
		}
		if state != "COMMITTED" || r.Session != c.Session || r.Epoch != c.Epoch || r.Digest != p.RequestSHA256 || r.ExpectedRevision != "custody-v1" || r.Kind != CustodyOperation || r.FileID != "" || r.Revision != "custody-v1:"+p.ArtifactSHA256 || r.Evidence != "private-stage-fsync-v1:"+p.ArtifactSHA256 || r.CommittedAtUnixNS != p.CommittedAtUnixNS {
			return ErrInvalid
		}
		original, _ := json.Marshal(struct {
			Volume            string
			ID                string
			Session           string
			Epoch             int64
			Digest            string
			ExpectedRevision  string
			Revision          string
			Evidence          string
			CommittedAtUnixNS int64
		}{r.Volume, r.ID, r.Session, r.Epoch, r.Digest, r.ExpectedRevision, r.Revision, r.Evidence, r.CommittedAtUnixNS})
		if DigestBytes(original) != p.ReceiptID {
			return ErrInvalid
		}
	}
	return nil
}
func (j *Journal) ReserveImage(ctx context.Context, seal ImageSeal) (ImageRecord, error) {
	b, digest, err := seal.Canonical()
	if err != nil {
		return ImageRecord{}, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ImageRecord{}, ErrClosed
	}
	s, err := imageActive(j.db, seal.StoreID)
	if err != nil {
		return ImageRecord{}, err
	}
	if s.volume != seal.Volume {
		return ImageRecord{}, ErrInvalid
	}
	if old, err := readImage(j.db, seal.ImageID, s.activation); err == nil {
		if old.Digest != digest {
			return ImageRecord{}, ErrChangedRequest
		}
		return old, nil
	} else if !errors.Is(err, ErrNoSuchOperation) {
		return ImageRecord{}, err
	}
	var collision bool
	if err = j.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM image_seals WHERE producer=? AND (operation_id=? OR incarnation=?))`, seal.Producer, seal.OperationID, seal.Incarnation).Scan(&collision); err != nil {
		return ImageRecord{}, err
	}
	if collision {
		return ImageRecord{}, ErrChangedRequest
	}
	if err = verifyImageCustody(j.db, seal); err != nil {
		return ImageRecord{}, err
	}
	charge := int64(len(b)) + 8192*int64(len(seal.Receipts)+2)
	if s.images >= s.config.MaxImages || seal.Content.Size > s.config.MaxLogicalBytes-s.logical || charge > s.config.MaxMetadataBytes-s.metadata {
		return ImageRecord{}, ErrImageQuota
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return ImageRecord{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO image_seals(image_id,producer,operation_id,incarnation,library_id,envelope,digest,state,logical,metadata) VALUES(?,?,?,?,?,?,?,'RESERVED',?,?)`, seal.ImageID, seal.Producer, seal.OperationID, seal.Incarnation, seal.LibraryID, b, digest, seal.Content.Size, charge); err != nil {
		return ImageRecord{}, err
	}
	for i, p := range seal.Receipts {
		pb, _ := json.Marshal(p)
		if _, err = tx.ExecContext(ctx, `INSERT INTO image_parts(image_id,ordinal,volume,custody_id,receipt) VALUES(?,?,?,?,?)`, seal.ImageID, i, seal.Volume, "custody-v1:"+p.Chunk.ID, pb); err != nil {
			return ImageRecord{}, err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE image_store SET images=images+1,logical=logical+?,metadata=metadata+? WHERE id=1`, seal.Content.Size, charge); err != nil {
		return ImageRecord{}, err
	}
	if err = tx.Commit(); err != nil {
		return ImageRecord{}, err
	}
	return ImageRecord{Seal: seal, Digest: digest, State: "RESERVED", Activation: s.activation}, nil
}

// SealImage is a trusted capability: the gateway must have re-read every
// original artifact and verified all logical bytes through EOF before calling.
func (j *Journal) SealImage(ctx context.Context, ref ImageRef, digest string) (ImageSealReceipt, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ImageSealReceipt{}, ErrClosed
	}
	s, err := imageActive(j.db, ref.StoreID)
	if err != nil {
		return ImageSealReceipt{}, err
	}
	r, err := readImage(j.db, ref.ImageID, s.activation)
	if err != nil {
		return ImageSealReceipt{}, err
	}
	if r.Seal.LibraryID != ref.LibraryID || r.Digest != digest {
		return ImageSealReceipt{}, ErrChangedRequest
	}
	if r.Receipt != nil {
		return *r.Receipt, nil
	}
	if err = verifyImageCustody(j.db, r.Seal); err != nil {
		return ImageSealReceipt{}, err
	}
	receipt := ImageSealReceipt{1, ref.StoreID, ref.ImageID, r.Seal.OperationID, digest, s.activation, 1}
	b, _ := json.Marshal(receipt)
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return ImageSealReceipt{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE image_seals SET state='SEALED',receipt=? WHERE image_id=? AND state='RESERVED'`, b, ref.ImageID); err != nil {
		return ImageSealReceipt{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO image_readers(image_id,reader,generation) VALUES(?,?,1)`, ref.ImageID, r.Seal.InitialReader); err != nil {
		return ImageSealReceipt{}, err
	}
	return receipt, tx.Commit()
}
func (j *Journal) RebindImageReader(ctx context.Context, producer string, change ImageReaderChange) (ImageReaderReceipt, error) {
	b, digest, err := change.Canonical()
	if err != nil {
		return ImageReaderReceipt{}, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ImageReaderReceipt{}, ErrClosed
	}
	s, err := imageActive(j.db, change.StoreID)
	if err != nil {
		return ImageReaderReceipt{}, err
	}
	r, err := readImage(j.db, change.ImageID, s.activation)
	if err != nil {
		return ImageReaderReceipt{}, err
	}
	if r.Seal.Producer != producer {
		return ImageReaderReceipt{}, ErrInvalid
	}
	if r.State != "SEALED" {
		return ImageReaderReceipt{}, ErrUncertain
	}
	var old, d []byte
	var oldDigest string
	err = j.db.QueryRowContext(ctx, `SELECT CASE WHEN length(request)<=8192 THEN request ELSE NULL END,digest,CASE WHEN length(receipt)<=8192 THEN receipt ELSE NULL END FROM image_reader_events WHERE image_id=? AND operation_id=?`, change.ImageID, change.OperationID).Scan(&old, &oldDigest, &d)
	if err == nil {
		var receipt ImageReaderReceipt
		if !bytes.Equal(b, old) || oldDigest != digest {
			return receipt, ErrChangedRequest
		}
		expected := ImageReaderReceipt{1, change.StoreID, change.ImageID, change.OperationID, digest, s.activation, change.ExpectedGeneration + 1, change.Reader}
		if DecodeImageCanonical(d, &receipt) != nil || !bytes.Equal(d, mustImageReceiptBytes(expected)) {
			return receipt, ErrInvalid
		}
		return receipt, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ImageReaderReceipt{}, err
	}
	if change.ExpectedGeneration != r.Generation {
		return ImageReaderReceipt{}, ErrImageConflict
	}
	charge := int64(len(b)) + 8192
	if charge > s.config.MaxMetadataBytes-s.metadata {
		return ImageReaderReceipt{}, ErrImageQuota
	}
	receipt := ImageReaderReceipt{1, change.StoreID, change.ImageID, change.OperationID, digest, s.activation, r.Generation + 1, change.Reader}
	rb, _ := json.Marshal(receipt)
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return ImageReaderReceipt{}, err
	}
	defer tx.Rollback()
	var reader any
	if change.Reader != nil {
		reader = *change.Reader
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO image_reader_events(image_id,operation_id,request,digest,generation,receipt,charge) VALUES(?,?,?,?,?,?,?)`, change.ImageID, change.OperationID, b, digest, receipt.Generation, rb, charge); err != nil {
		return ImageReaderReceipt{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE image_readers SET reader=?,generation=? WHERE image_id=?`, reader, receipt.Generation, change.ImageID); err != nil {
		return ImageReaderReceipt{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE image_store SET metadata=metadata+? WHERE id=1`, charge); err != nil {
		return ImageReaderReceipt{}, err
	}
	return receipt, tx.Commit()
}

// validateImages independently reconstructs quota counters, immutable bindings,
// exact parts and the complete contiguous grant history before admission.
func validateImages(q schemaReader) error {
	s, err := readImageStore(q)
	if errors.Is(err, sql.ErrNoRows) {
		for _, table := range []string{"image_seals", "image_parts", "image_readers", "image_reader_events"} {
			var n int
			if err = q.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
				return err
			}
			if n != 0 {
				return ErrInvalid
			}
		}
		return nil
	}
	if err != nil {
		return err
	}
	if s.config.Validate() != nil || !imageIdentifier(s.volume) || s.activation != 1 {
		return ErrInvalid
	}
	rows, err := q.Query(`SELECT image_id FROM image_seals ORDER BY image_id`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
		if len(ids) > int(MaxImages) {
			rows.Close()
			return ErrInvalid
		}
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	var logical, metadata int64
	for _, id := range ids {
		r, err := readImage(q, id, s.activation)
		if err != nil {
			return err
		}
		if r.Seal.StoreID != s.config.StoreID || r.Seal.Volume != s.volume || verifyImageCustody(q, r.Seal) != nil {
			return ErrInvalid
		}
		b, _, _ := r.Seal.Canonical()
		charge := int64(len(b)) + 8192*int64(len(r.Seal.Receipts)+2)
		if r.Seal.Content.Size > s.config.MaxLogicalBytes-logical || charge > s.config.MaxMetadataBytes-metadata {
			return ErrInvalid
		}
		logical += r.Seal.Content.Size
		metadata += charge
		parts, err := q.Query(`SELECT ordinal,volume,custody_id,CASE WHEN length(receipt)<=8192 THEN receipt ELSE NULL END FROM image_parts WHERE image_id=? ORDER BY ordinal`, id)
		if err != nil {
			return err
		}
		count := 0
		for parts.Next() {
			var ordinal int
			var volume, custody string
			var pb []byte
			if err = parts.Scan(&ordinal, &volume, &custody, &pb); err != nil {
				parts.Close()
				return err
			}
			if count >= len(r.Seal.Receipts) {
				parts.Close()
				return ErrInvalid
			}
			want, _ := json.Marshal(r.Seal.Receipts[count])
			if ordinal != count || volume != s.volume || custody != "custody-v1:"+r.Seal.Receipts[count].Chunk.ID || !bytes.Equal(pb, want) {
				parts.Close()
				return ErrInvalid
			}
			count++
		}
		err = errors.Join(parts.Err(), parts.Close())
		if err != nil {
			return err
		}
		if count != len(r.Seal.Receipts) {
			return ErrInvalid
		}
		events, err := q.Query(`SELECT operation_id,CASE WHEN length(request)<=8192 THEN request ELSE NULL END,digest,generation,CASE WHEN length(receipt)<=8192 THEN receipt ELSE NULL END,charge FROM image_reader_events WHERE image_id=? ORDER BY generation`, id)
		if err != nil {
			return err
		}
		generation := int64(1)
		reader := r.Seal.InitialReader
		current := &reader
		for events.Next() {
			var op, digest string
			var request, receipt []byte
			var gen, charge int64
			if err = events.Scan(&op, &request, &digest, &gen, &receipt, &charge); err != nil {
				events.Close()
				return err
			}
			var change ImageReaderChange
			var received ImageReaderReceipt
			if DecodeImageCanonical(request, &change) != nil || DecodeImageCanonical(receipt, &received) != nil {
				events.Close()
				return ErrInvalid
			}
			cb, cd, ce := change.Canonical()
			expected := ImageReaderReceipt{1, s.config.StoreID, id, op, digest, s.activation, gen, change.Reader}
			eb, _ := json.Marshal(expected)
			if ce != nil || !bytes.Equal(cb, request) || cd != digest || change.StoreID != s.config.StoreID || change.ImageID != id || change.OperationID != op || change.ExpectedGeneration != generation || gen != generation+1 || charge != int64(len(request))+8192 || !bytes.Equal(eb, receipt) {
				events.Close()
				return ErrInvalid
			}
			if charge > s.config.MaxMetadataBytes-metadata {
				events.Close()
				return ErrInvalid
			}
			generation = gen
			current = change.Reader
			metadata += charge
		}
		err = errors.Join(events.Err(), events.Close())
		if err != nil {
			return err
		}
		var readerRows int
		if err = q.QueryRow(`SELECT count(*) FROM image_readers WHERE image_id=?`, id).Scan(&readerRows); err != nil {
			return err
		}
		if r.State == "RESERVED" {
			if generation != 1 || readerRows != 0 {
				return ErrInvalid
			}
		} else if readerRows != 1 || r.Generation != generation || !sameImageReader(r.Reader, current) {
			return ErrInvalid
		}
	}
	var bad bool
	if err = q.QueryRow(`SELECT EXISTS(SELECT 1 FROM image_parts p LEFT JOIN image_seals s ON s.image_id=p.image_id WHERE s.image_id IS NULL UNION ALL SELECT 1 FROM image_readers r LEFT JOIN image_seals s ON s.image_id=r.image_id WHERE s.image_id IS NULL UNION ALL SELECT 1 FROM image_reader_events e LEFT JOIN image_seals s ON s.image_id=e.image_id WHERE s.image_id IS NULL)`).Scan(&bad); err != nil {
		return err
	}
	if bad || s.images != int64(len(ids)) || s.logical != logical || s.metadata != metadata || s.images > s.config.MaxImages || logical > s.config.MaxLogicalBytes || metadata > s.config.MaxMetadataBytes {
		return ErrInvalid
	}
	return nil
}
func sameImageReader(a, b *string) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
func mustImageReceiptBytes(v ImageReaderReceipt) []byte { b, _ := json.Marshal(v); return b }
