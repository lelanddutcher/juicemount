package mutationauthority

import (
	"context"
	"encoding/json"
)

// Retention records confinement, not completion. Old provider requests may
// still commit; the operation remains PREPARED and its custody must be kept.
const retentionDDL = `CREATE TABLE media_retention (
 volume TEXT NOT NULL, id TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation>0),
 redis_run_id TEXT NOT NULL, backend TEXT NOT NULL, snapshot TEXT NOT NULL,
 PRIMARY KEY(volume,id,generation),
 FOREIGN KEY(volume,id) REFERENCES media_stages(volume,id),
 FOREIGN KEY(generation) REFERENCES backend_processes(generation))`

func StageSnapshotDigest(s MediaStage) string {
	b, _ := json.Marshal(s)
	return DigestBytes(b)
}

// StageRecoveryCandidates enumerates ALL media operations, including terminal
// or unsupported ones, and refuses anything other than preexisting PREPARED
// stages. It grants no admission and never replays an effect.
func (j *Journal) StageRecoveryCandidates(ctx context.Context, volume string) ([]MediaStage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.stageRecoveryCandidates(ctx, volume)
}

func (j *Journal) stageRecoveryCandidates(ctx context.Context, volume string) ([]MediaStage, error) {
	if j.closed {
		return nil, ErrClosed
	}
	if len(j.active) != 0 || len(j.deadlines) != 0 {
		return nil, ErrInFlight
	}
	var admitted bool
	if err := j.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM owners WHERE admission_closed=0)`).Scan(&admitted); err != nil {
		return nil, err
	}
	if admitted {
		return nil, ErrInFlight
	}
	var wrongKind bool
	if err := j.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM media_stages s JOIN operations o ON o.volume=s.volume AND o.id=s.id WHERE o.kind<>?)`, MediaPublishOperation).Scan(&wrongKind); err != nil {
		return nil, err
	}
	if wrongKind {
		return nil, ErrInvalid
	}
	rows, err := j.db.QueryContext(ctx, `SELECT rowid,volume,id,state,revision,evidence,committed_ns FROM operations WHERE kind=? ORDER BY volume,id LIMIT 65537`, MediaPublishOperation)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var rowID, committed int64
		var vol, id, state, revision, evidence string
		if err = rows.Scan(&rowID, &vol, &id, &state, &revision, &evidence, &committed); err != nil {
			rows.Close()
			return nil, err
		}
		if vol != volume || rowID > j.initialOperationRowID || state != string(Prepared) || revision != "" || evidence != "" || committed != 0 {
			rows.Close()
			return nil, ErrInvalid
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(ids) > 65536 {
		return nil, ErrInvalid
	}
	var out []MediaStage
	for _, id := range ids {
		s, err := j.inspectStage(ctx, volume, id)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// CertifyRetainedStages is a trusted SDK startup primitive, never a client
// endpoint. The SDK must validate canonical stage identities and their backend
// confinement first. checkFence must check the actual completed Redis takeover
// and must not call the journal. Recording a worker identity is NOT a fence.
// All certificates commit together. A fresh Open always starts with no latch,
// so persisted certificates alone cannot unblock a successor claim.
func (j *Journal) CertifyRetainedStages(ctx context.Context, process BackendProcess, binding string, stages []MediaStage, checkFence func() error) error {
	return j.CertifyRecoveredStages(ctx, process, binding, stages, nil, checkFence)
}

// CertifyRecoveredStages commits private and published classifications together.
// published is supplied only by the trusted post-fence namespace verifier.
func (j *Journal) CertifyRecoveredStages(ctx context.Context, process BackendProcess, binding string, stages []MediaStage, published map[string]bool, checkFence func() error) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.processPinned || !j.backendStarted || !j.backendRecorded || process.Generation <= 0 || process != j.backendProcess || checkFence == nil || j.retainedGeneration != 0 {
		return ErrInvalid
	}
	current, err := j.stageRecoveryCandidates(ctx, process.Volume)
	if err != nil {
		return err
	}
	if len(current) != len(stages) {
		return ErrChangedRequest
	}
	publicCount := 0
	for i, s := range current {
		if StageSnapshotDigest(s) != StageSnapshotDigest(stages[i]) || s.Intent.Backend != binding {
			return ErrChangedRequest
		}
		p, err := j.publication(ctx, s.Request.Volume, s.Request.ID)
		if err != nil {
			return err
		}
		if p != nil && p.Snapshot != StageSnapshotDigest(s) {
			return ErrChangedRequest
		}
		if published[s.Request.ID] {
			publicCount++
			if p == nil || !s.Closed {
				return ErrInvalid
			}
		} else if p != nil && p.Observed {
			return ErrUncertain
		}
	}
	if publicCount != len(published) {
		return ErrInvalid
	}
	var volume, persistedBinding string
	if err := j.db.QueryRowContext(ctx, `SELECT volume,digest FROM backend_configuration WHERE id=1`).Scan(&volume, &persistedBinding); err != nil {
		return err
	}
	if volume != process.Volume || binding != persistedBinding || !validStageHash(binding) {
		return ErrChangedRequest
	}
	if len(stages) != 0 {
		var count, incompatible int
		if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(volume<>? OR redis_run_id<>?),0) FROM backend_processes WHERE generation<?`, process.Volume, process.RedisRunID, process.Generation).Scan(&count, &incompatible); err != nil {
			return err
		}
		if count == 0 || incompatible != 0 {
			return ErrInvalid
		}
	}
	if err := checkFence(); err != nil {
		return err
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, s := range stages {
		table := "media_retention"
		if published[s.Request.ID] {
			table = "media_publication_retention"
			if _, err := tx.ExecContext(ctx, `UPDATE media_publications SET observed=1 WHERE volume=? AND id=?`, s.Request.Volume, s.Request.ID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+table+`(volume,id,generation,redis_run_id,backend,snapshot) VALUES(?,?,?,?,?,?)`, s.Request.Volume, s.Request.ID, process.Generation, process.RedisRunID, binding, StageSnapshotDigest(s)); err != nil {
			return err
		}
	}
	if err := checkFence(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	j.retainedGeneration = process.Generation
	return nil
}

// RequireRetainedStage is used by mixed custody/media reconciliation. Unknown
// media rows must not disappear through a filtered custody query.
func (j *Journal) RequireRetainedStage(ctx context.Context, r Request) error {
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
	if err := j.db.QueryRowContext(ctx, `SELECT snapshot FROM media_retention WHERE volume=? AND id=? AND generation=?`, r.Volume, r.ID, j.retainedGeneration).Scan(&snapshot); err != nil {
		return err
	}
	if snapshot != StageSnapshotDigest(s) {
		return ErrChangedRequest
	}
	return nil
}
