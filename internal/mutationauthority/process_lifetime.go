package mutationauthority

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
)

// ErrProcessLifetime means only actual process exit may release this journal.
// A backend with unjoinable background writers must never release its authority
// just because HTTP handlers drained or a library Close method returned.
var ErrProcessLifetime = errors.New("backend authority is retained until process exit")

var processJournals = struct {
	sync.Mutex
	kept map[*Journal]struct{}
}{kept: make(map[*Journal]struct{})}

// PinProcessLifetime irreversibly retains this journal, database and flock.
// Call BEFORE acquiring any backend/control credential or writable capability.
// The strong process-global reference prevents os.File finalizers from releasing
// the lock after a failed constructor or caller dropping its last reference.
// There is intentionally no unpin operation. A cooperating successor can open
// this journal only after the OS has terminated this entire process.
func (j *Journal) PinProcessLifetime() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.pinProcessLifetimeLocked()
}

func (j *Journal) pinProcessLifetimeLocked() error {
	if j.closed {
		return ErrClosed
	}
	if j.processPinned {
		return nil
	}
	if len(j.active) != 0 {
		return ErrInFlight
	}
	j.processPinned = true
	processJournals.Lock()
	processJournals.kept[j] = struct{}{}
	processJournals.Unlock()
	return nil
}

// BeginBackendProcess consumes the one backend initialization attempt before
// reading a control secret or opening a backend connection. Failure later in
// initialization cannot be retried in this process, even before any identity
// was recorded. The irreversible pin is installed as part of the same lock.
func (j *Journal) BeginBackendProcess() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.pinProcessLifetimeLocked(); err != nil {
		return err
	}
	if j.backendStarted {
		return ErrProcessLifetime
	}
	j.backendStarted = true
	return nil
}

// BackendProcess records a never-reused worker identity before it is provisioned.
// It grants no media ownership, mutation permission or durability receipt.
type BackendProcess struct {
	Generation int64
	Volume     string
	Username   string
	RedisRunID string
}

func validBackendIdentity(username, runID string) bool {
	if !strings.HasPrefix(username, "jmgw_") || len(username) != 69 || len(runID) != 40 {
		return false
	}
	for _, s := range []string{username[5:], runID} {
		if _, err := hex.DecodeString(s); err != nil || s != strings.ToLower(s) {
			return false
		}
	}
	return true
}

// RecordBackendProcess persists the exact runtime ACL identity before creating
// it in Redis. One attempt is allowed per pinned process; an ambiguous failure
// requires process exit and startup reconciliation, not a second identity.
func (j *Journal) RecordBackendProcess(ctx context.Context, volume, username, runID string) (BackendProcess, error) {
	if volume == "" || len(volume) > 128 || !validBackendIdentity(username, runID) {
		return BackendProcess{}, ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return BackendProcess{}, ErrClosed
	}
	if !j.processPinned || !j.backendStarted || j.backendRecorded {
		return BackendProcess{}, ErrInvalid
	}
	j.backendRecorded = true
	var foreign bool
	if err := j.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM owners WHERE volume<>?
		UNION ALL SELECT 1 FROM backend_processes WHERE volume<>?)`, volume, volume).Scan(&foreign); err != nil {
		return BackendProcess{}, err
	}
	if foreign {
		return BackendProcess{}, ErrInvalid
	}
	result, err := j.db.ExecContext(ctx, `INSERT INTO backend_processes(volume,username,redis_run_id) VALUES(?,?,?)`, volume, username, runID)
	if err != nil {
		return BackendProcess{}, err
	}
	generation, err := result.LastInsertId()
	if err != nil {
		return BackendProcess{}, err
	}
	j.backendProcess = BackendProcess{generation, volume, username, runID}
	return j.backendProcess, nil
}

// BackendProcesses enumerates every identity this authority might have created.
// Absence in Redis is permitted after a crash before provisioning. An unknown
// Redis writer is never adopted merely because its name has the same prefix.
func (j *Journal) BackendProcesses(ctx context.Context, volume string) ([]BackendProcess, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, ErrClosed
	}
	rows, err := j.db.QueryContext(ctx, `SELECT generation,volume,username,redis_run_id FROM backend_processes ORDER BY generation`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BackendProcess
	for rows.Next() {
		var p BackendProcess
		if err := rows.Scan(&p.Generation, &p.Volume, &p.Username, &p.RedisRunID); err != nil {
			return nil, err
		}
		if p.Volume != volume || p.Generation <= 0 || !validBackendIdentity(p.Username, p.RedisRunID) {
			return nil, ErrInvalid
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
