package mutationauthority

import (
	"context"
	"database/sql"
	"errors"
)

var publicationSchema = map[string]string{
	"media_publications": `CREATE TABLE media_publications (
 volume TEXT NOT NULL, id TEXT NOT NULL, snapshot TEXT NOT NULL,
 observed INTEGER NOT NULL DEFAULT 0 CHECK(observed IN (0,1)),
 PRIMARY KEY(volume,id), FOREIGN KEY(volume,id) REFERENCES media_stages(volume,id))`,
	"media_publication_retention": `CREATE TABLE media_publication_retention (
 volume TEXT NOT NULL, id TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation>0),
 redis_run_id TEXT NOT NULL, backend TEXT NOT NULL, snapshot TEXT NOT NULL,
 PRIMARY KEY(volume,id,generation),
 FOREIGN KEY(volume,id) REFERENCES media_publications(volume,id),
 FOREIGN KEY(generation) REFERENCES backend_processes(generation))`,
}

// StagePublication records intent and an observed namespace outcome. Neither
// observation nor recovery is a durability receipt or permission to retire data.
type StagePublication struct {
	Snapshot string
	Observed bool
}

func (j *Journal) publication(ctx context.Context, volume, id string) (*StagePublication, error) {
	var p StagePublication
	err := j.db.QueryRowContext(ctx, `SELECT snapshot,observed FROM media_publications WHERE volume=? AND id=?`, volume, id).Scan(&p.Snapshot, &p.Observed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (j *Journal) InspectPublication(ctx context.Context, volume, id string) (*StagePublication, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, ErrClosed
	}
	return j.publication(ctx, volume, id)
}

// BeginPublication must precede any namespace effect. Existing intent is not
// replayed in this process, including after an uncertain response. A current
// lease may explicitly publish an older retained stage without retargeting it.
func (j *Journal) BeginPublication(ctx context.Context, lease Lease, r Request) (MediaStage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var empty MediaStage
	if j.closed || !j.processPinned || !j.backendRecorded || len(j.active) != 0 || lease.Volume != r.Volume {
		return empty, ErrInFlight
	}
	if err := j.checkLease(ctx, lease, true); err != nil {
		return empty, err
	}
	s, err := j.inspectStage(ctx, r.Volume, r.ID)
	if err != nil {
		return empty, err
	}
	if !sameRequest(s.Request, r) {
		return empty, ErrChangedRequest
	}
	op, err := j.readOperation(ctx, r.Volume, r.ID)
	if err != nil {
		return empty, err
	}
	if r.Kind != MediaPublishOperation || op.State != Prepared || op.Receipt != nil || !s.Closed || s.Inode == 0 || !validStageHash(s.Manifest) {
		return empty, ErrInvalid
	}
	if p, err := j.publication(ctx, r.Volume, r.ID); err != nil {
		return empty, err
	} else if p != nil {
		return empty, ErrUncertain
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO media_publications(volume,id,snapshot) VALUES(?,?,?)`, r.Volume, r.ID, StageSnapshotDigest(s)); err != nil {
		return empty, err
	}
	// A private-confinement certificate must stop exempting this operation
	// from admission before its inode can acquire a public link.
	for _, table := range []string{"media_retention", "media_publication_retention"} {
		if _, err = tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE volume=? AND id=?`, r.Volume, r.ID); err != nil {
			return empty, err
		}
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	return s, nil
}

// ObservePublication is called only after the trusted guarded namespace method
// succeeds. It leaves the operation PREPARED and does not grant a claim exemption.
func (j *Journal) ObservePublication(ctx context.Context, s MediaStage) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed || !j.processPinned || !j.backendRecorded {
		return ErrInvalid
	}
	current, err := j.inspectStage(ctx, s.Request.Volume, s.Request.ID)
	if err != nil {
		return err
	}
	snapshot := StageSnapshotDigest(s)
	if StageSnapshotDigest(current) != snapshot {
		return ErrChangedRequest
	}
	p, err := j.publication(ctx, s.Request.Volume, s.Request.ID)
	if err != nil {
		return err
	}
	if p == nil || p.Snapshot != snapshot {
		return ErrChangedRequest
	}
	_, err = j.db.ExecContext(ctx, `UPDATE media_publications SET observed=1 WHERE volume=? AND id=?`, s.Request.Volume, s.Request.ID)
	return err
}

// RequireRecoveredStage accepts either typed private confinement or a separately
// certified public namespace observation from THIS fenced process generation.
func (j *Journal) RequireRecoveredStage(ctx context.Context, r Request) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed || j.retainedGeneration == 0 || r.Kind != MediaPublishOperation {
		return ErrInvalid
	}
	s, err := j.inspectStage(ctx, r.Volume, r.ID)
	if err != nil {
		return err
	}
	if !sameRequest(s.Request, r) {
		return ErrChangedRequest
	}
	var snapshot string
	err = j.db.QueryRowContext(ctx, `SELECT snapshot FROM media_retention WHERE volume=? AND id=? AND generation=?
 UNION ALL SELECT c.snapshot FROM media_publication_retention c JOIN media_publications p ON p.volume=c.volume AND p.id=c.id
 WHERE c.volume=? AND c.id=? AND c.generation=? AND p.observed=1 AND p.snapshot=c.snapshot`, r.Volume, r.ID, j.retainedGeneration, r.Volume, r.ID, j.retainedGeneration).Scan(&snapshot)
	if err != nil {
		return err
	}
	if snapshot != StageSnapshotDigest(s) {
		return ErrChangedRequest
	}
	return nil
}
