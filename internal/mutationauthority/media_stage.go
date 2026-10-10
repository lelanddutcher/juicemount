package mutationauthority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
)

var stageSchema = map[string]string{
	"media_stages": `CREATE TABLE media_stages (
 volume TEXT NOT NULL, id TEXT NOT NULL, body BLOB NOT NULL, path TEXT NOT NULL,
 backend TEXT NOT NULL, size INTEGER NOT NULL CHECK(size>=0), sha256 TEXT NOT NULL,
 inode INTEGER NOT NULL DEFAULT 0, marker TEXT NOT NULL DEFAULT '', closed INTEGER NOT NULL DEFAULT 0 CHECK(closed IN (0,1)),
 manifest TEXT NOT NULL DEFAULT '', PRIMARY KEY(volume,id), UNIQUE(volume,path),
 FOREIGN KEY(volume,id) REFERENCES operations(volume,id))`,
	"media_stage_inode": `CREATE UNIQUE INDEX media_stage_inode ON media_stages(volume,inode) WHERE inode>0`,
	"media_slices": `CREATE TABLE media_slices (
 volume TEXT NOT NULL, id TEXT NOT NULL, inode INTEGER NOT NULL, slice INTEGER NOT NULL CHECK(slice>0),
 written INTEGER NOT NULL DEFAULT 0 CHECK(written IN (0,1)), chunk_index INTEGER NOT NULL DEFAULT 0,
 offset INTEGER NOT NULL DEFAULT 0, size INTEGER NOT NULL DEFAULT 0, slice_offset INTEGER NOT NULL DEFAULT 0, length INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(volume,slice), FOREIGN KEY(volume,id) REFERENCES media_stages(volume,id))`,
	"media_object_attempts": `CREATE TABLE media_object_attempts (
 attempt INTEGER PRIMARY KEY AUTOINCREMENT, volume TEXT NOT NULL, id TEXT NOT NULL, slice INTEGER NOT NULL,
 provider TEXT NOT NULL, bucket TEXT NOT NULL, shard INTEGER NOT NULL, object_key TEXT NOT NULL,
 size INTEGER NOT NULL CHECK(size>=0), sha256 TEXT NOT NULL, verified INTEGER NOT NULL DEFAULT 0 CHECK(verified IN (0,1)),
 FOREIGN KEY(volume,id) REFERENCES media_stages(volume,id), FOREIGN KEY(volume,slice) REFERENCES media_slices(volume,slice))`,
}

// StageIntent is an immutable preparation record, not a publication request or
// durability proof. Backend binds the exact SDK/provider configuration digest.
type StageIntent struct {
	Body          []byte
	Path, Backend string
	Size          int64
	SHA256        string
}

type StageSlice struct {
	ID, Inode                                     uint64
	Written                                       bool
	ChunkIndex, Offset, Size, SliceOffset, Length uint32
}

type ObjectAttempt struct {
	Attempt          int64
	Slice            uint64
	Provider, Bucket string
	Shard            int
	Key              string
	Size             int64
	SHA256           string
	Verified         bool
}

type MediaStage struct {
	Request  Request
	Intent   StageIntent
	Inode    uint64
	Marker   string
	Closed   bool
	Manifest string
	Slices   []StageSlice
	Objects  []ObjectAttempt
}

func sameStageIntent(a, b StageIntent) bool {
	return bytes.Equal(a.Body, b.Body) && a.Path == b.Path && a.Backend == b.Backend && a.Size == b.Size && a.SHA256 == b.SHA256
}

// ExecuteStage atomically persists the canonical intent and PREPARED operation
// before invoking a backend. Neither a successful callback nor a retry can
// produce a Receipt here. Uncertain effects must never be replayed or adopted.
func (j *Journal) ExecuteStage(ctx context.Context, r Request, intent StageIntent, backend Backend) error {
	intent.Body = append([]byte(nil), intent.Body...)
	if r.Kind != MediaPublishOperation || len(intent.Body) == 0 || len(intent.Body) > 64<<10 || !json.Valid(intent.Body) || DigestBytes(intent.Body) != r.Digest ||
		intent.Path != "/.juicemount-stage-"+DigestBytes([]byte(r.Volume+"\x00"+r.ID)) || len(intent.Backend) != 64 || !validStageHash(intent.Backend) ||
		intent.Size < 0 || intent.Size > 1<<40 || !validStageHash(intent.SHA256) {
		return ErrInvalid
	}
	_, err := j.execute(ctx, r, backend, &intent)
	return err
}

func validStageHash(s string) bool { return len(s) == 64 && strings.ToLower(s) == s && allHex(s) }
func allHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (j *Journal) readStage(ctx context.Context, volume, id string) (MediaStage, error) {
	var s MediaStage
	op, err := j.readOperation(ctx, volume, id)
	if err != nil {
		return s, err
	}
	s.Request = op.Request
	err = j.db.QueryRowContext(ctx, `SELECT body,path,backend,size,sha256,inode,marker,closed,manifest FROM media_stages WHERE volume=? AND id=?`, volume, id).
		Scan(&s.Intent.Body, &s.Intent.Path, &s.Intent.Backend, &s.Intent.Size, &s.Intent.SHA256, &s.Inode, &s.Marker, &s.Closed, &s.Manifest)
	return s, err
}

func (j *Journal) stageWritable(ctx context.Context, r Request) (MediaStage, error) {
	if j.closed {
		return MediaStage{}, ErrClosed
	}
	if _, ok := j.active[keyForOperation(r.Volume, r.ID)]; !ok {
		return MediaStage{}, ErrUncertain
	}
	s, err := j.readStage(ctx, r.Volume, r.ID)
	if err != nil {
		return s, err
	}
	if !sameRequest(s.Request, r) {
		return s, ErrChangedRequest
	}
	if s.Closed {
		return s, ErrUncertain
	}
	return s, nil
}

func (j *Journal) BindStageInode(ctx context.Context, r Request, inode uint64, marker string) error {
	if inode <= 1 || inode > math.MaxInt64 || !validStageHash(marker) {
		return ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	s, err := j.stageWritable(ctx, r)
	if err != nil {
		return err
	}
	if s.Inode != 0 {
		return ErrChangedRequest
	}
	_, err = j.db.ExecContext(ctx, `UPDATE media_stages SET inode=?,marker=? WHERE volume=? AND id=? AND inode=0`, inode, marker, r.Volume, r.ID)
	return err
}

func (j *Journal) StageForInode(ctx context.Context, volume string, inode uint64) (MediaStage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return MediaStage{}, ErrClosed
	}
	var id string
	if err := j.db.QueryRowContext(ctx, `SELECT id FROM media_stages WHERE volume=? AND inode=?`, volume, inode).Scan(&id); err != nil {
		return MediaStage{}, err
	}
	return j.readStage(ctx, volume, id)
}

func (j *Journal) RecordStageSlice(ctx context.Context, r Request, inode, id uint64) error {
	if id == 0 || id > math.MaxInt64 {
		return ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	s, err := j.stageWritable(ctx, r)
	if err != nil {
		return err
	}
	if s.Inode != inode || inode == 0 {
		return ErrChangedRequest
	}
	_, err = j.db.ExecContext(ctx, `INSERT INTO media_slices(volume,id,inode,slice) VALUES(?,?,?,?)`, r.Volume, r.ID, inode, id)
	return err
}

func (j *Journal) StageForSlice(ctx context.Context, volume string, slice uint64) (MediaStage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return MediaStage{}, ErrClosed
	}
	var id string
	if err := j.db.QueryRowContext(ctx, `SELECT id FROM media_slices WHERE volume=? AND slice=?`, volume, slice).Scan(&id); err != nil {
		return MediaStage{}, err
	}
	return j.readStage(ctx, volume, id)
}

func (j *Journal) RecordSliceWritten(ctx context.Context, r Request, w StageSlice) error {
	if w.ID == 0 || w.Inode == 0 || w.Size == 0 || w.Length == 0 || uint64(w.SliceOffset)+uint64(w.Length) > uint64(w.Size) {
		return ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	s, err := j.stageWritable(ctx, r)
	if err != nil {
		return err
	}
	if s.Inode != w.Inode {
		return ErrChangedRequest
	}
	result, err := j.db.ExecContext(ctx, `UPDATE media_slices SET written=1,chunk_index=?,offset=?,size=?,slice_offset=?,length=? WHERE volume=? AND id=? AND inode=? AND slice=? AND written=0`,
		w.ChunkIndex, w.Offset, w.Size, w.SliceOffset, w.Length, r.Volume, r.ID, w.Inode, w.ID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrChangedRequest
	}
	return nil
}

// BeginObjectAttempt commits before the raw provider call. Closing a stage and
// admitting an attempt serialize under the same lock; a late worker cannot pass
// this gate after closure. Each call gets its own row, including identical retries.
func (j *Journal) BeginObjectAttempt(ctx context.Context, r Request, a ObjectAttempt) (int64, error) {
	if a.Slice == 0 || a.Size < 0 || a.Size > 32<<20 || !validStageHash(a.SHA256) || a.Provider == "" || a.Bucket == "" || a.Shard < 0 || a.Shard > 255 || a.Key == "" || len(a.Key) > 1024 || len(a.Bucket) > 4096 {
		return 0, ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	_, err := j.stageWritable(ctx, r)
	if err != nil {
		return 0, err
	}
	var owner string
	if err = j.db.QueryRowContext(ctx, `SELECT id FROM media_slices WHERE volume=? AND slice=?`, r.Volume, a.Slice).Scan(&owner); err != nil {
		return 0, err
	}
	if owner != r.ID {
		return 0, ErrChangedRequest
	}
	result, err := j.db.ExecContext(ctx, `INSERT INTO media_object_attempts(volume,id,slice,provider,bucket,shard,object_key,size,sha256) VALUES(?,?,?,?,?,?,?,?,?)`, r.Volume, r.ID, a.Slice, a.Provider, a.Bucket, a.Shard, a.Key, a.Size, a.SHA256)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (j *Journal) VerifyObjectAttempt(ctx context.Context, r Request, attempt int64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	_, err := j.stageWritable(ctx, r)
	if err != nil {
		return err
	}
	result, err := j.db.ExecContext(ctx, `UPDATE media_object_attempts SET verified=1 WHERE attempt=? AND volume=? AND id=? AND verified=0`, attempt, r.Volume, r.ID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrChangedRequest
	}
	return nil
}

func (j *Journal) InspectStage(ctx context.Context, volume, id string) (MediaStage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return MediaStage{}, ErrClosed
	}
	return j.inspectStage(ctx, volume, id)
}

func (j *Journal) inspectStage(ctx context.Context, volume, id string) (MediaStage, error) {
	s, err := j.readStage(ctx, volume, id)
	if err != nil {
		return s, err
	}
	rows, err := j.db.QueryContext(ctx, `SELECT slice,inode,written,chunk_index,offset,size,slice_offset,length FROM media_slices WHERE volume=? AND id=? ORDER BY chunk_index,offset,slice LIMIT 1048577`, volume, id)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var v StageSlice
		if err = rows.Scan(&v.ID, &v.Inode, &v.Written, &v.ChunkIndex, &v.Offset, &v.Size, &v.SliceOffset, &v.Length); err != nil {
			rows.Close()
			return s, err
		}
		s.Slices = append(s.Slices, v)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return s, err
	}
	rows, err = j.db.QueryContext(ctx, `SELECT attempt,slice,provider,bucket,shard,object_key,size,sha256,verified FROM media_object_attempts WHERE volume=? AND id=? ORDER BY attempt LIMIT 1048577`, volume, id)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var v ObjectAttempt
		if err = rows.Scan(&v.Attempt, &v.Slice, &v.Provider, &v.Bucket, &v.Shard, &v.Key, &v.Size, &v.SHA256, &v.Verified); err != nil {
			rows.Close()
			return s, err
		}
		s.Objects = append(s.Objects, v)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return s, err
	}
	if len(s.Slices) > 1<<20 || len(s.Objects) > 1<<20 {
		return s, ErrInvalid
	}
	return s, nil
}

// CloseStage freezes the observed manifest and raw PUT admission together. The
// trusted verifier must check exact slice coverage/key layout; it must not call
// the journal. This is still PREPARED, never a media DurabilityProof or receipt.
func (j *Journal) CloseStage(ctx context.Context, r Request, verify func(MediaStage) error) (MediaStage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if verify == nil {
		return MediaStage{}, ErrInvalid
	}
	if _, err := j.stageWritable(ctx, r); err != nil {
		return MediaStage{}, err
	}
	s, err := j.inspectStage(ctx, r.Volume, r.ID)
	if err != nil {
		return s, err
	}
	if s.Inode == 0 || s.Marker == "" {
		return s, ErrUncertain
	}
	for _, a := range s.Objects {
		if !a.Verified {
			return s, ErrUncertain
		}
	}
	for _, sl := range s.Slices {
		if !sl.Written {
			return s, ErrUncertain
		}
	}
	if err = verify(s); err != nil {
		return s, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return s, err
	}
	manifest := DigestBytes(b)
	_, err = j.db.ExecContext(ctx, `UPDATE media_stages SET closed=1,manifest=? WHERE volume=? AND id=? AND closed=0`, manifest, r.Volume, r.ID)
	if err != nil {
		return s, err
	}
	s.Closed = true
	s.Manifest = manifest
	return s, nil
}

// Require stages to remain attached to real operations on schema reopen.
func validateMediaStages(q schemaReader) error {
	rows, err := q.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return ErrInvalid
	}
	return rows.Err()
}
