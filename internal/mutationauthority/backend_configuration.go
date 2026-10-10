package mutationauthority

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
)

// CheckBackendConfiguration refuses changing a previously bound SDK/provider
// identity. Absence is permitted before the first validated attachment.
func (j *Journal) CheckBackendConfiguration(ctx context.Context, volume, digest string) error {
	return j.backendConfiguration(ctx, volume, digest, false)
}

// BindBackendConfiguration durably fixes the validated SDK/provider identity in
// this SAME whole-volume journal before constructing a provider or session.
// There is no overwrite: a different configuration requires explicit migration.
func (j *Journal) BindBackendConfiguration(ctx context.Context, volume, digest string) error {
	return j.backendConfiguration(ctx, volume, digest, true)
}

func (j *Journal) backendConfiguration(ctx context.Context, volume, digest string, bind bool) error {
	if volume == "" || len(volume) > 128 || len(digest) != 64 || strings.ToLower(digest) != digest {
		return ErrInvalid
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}
	if bind && (!j.processPinned || !j.backendStarted || !j.backendRecorded || len(j.active) != 0) {
		return ErrInvalid
	}
	var previousVolume, previousDigest string
	err := j.db.QueryRowContext(ctx, `SELECT volume,digest FROM backend_configuration WHERE id=1`).Scan(&previousVolume, &previousDigest)
	if errors.Is(err, sql.ErrNoRows) {
		if !bind {
			return nil
		}
		var foreign bool
		if err := j.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM owners WHERE volume<>?
			UNION ALL SELECT 1 FROM backend_processes WHERE volume<>?)`, volume, volume).Scan(&foreign); err != nil {
			return err
		}
		if foreign {
			return ErrInvalid
		}
		_, err = j.db.ExecContext(ctx, `INSERT INTO backend_configuration(id,volume,digest) VALUES(1,?,?)`, volume, digest)
		return err
	}
	if err != nil {
		return err
	}
	if previousVolume != volume || previousDigest != digest {
		return ErrChangedRequest
	}
	return nil
}

// RequireNoBackend prevents a fresh process from omitting SDK configuration
// and reopening previously attached state as a custody-only fallback.
func (j *Journal) RequireNoBackend(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}
	var exists bool
	if err := j.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM backend_configuration
		UNION ALL SELECT 1 FROM backend_processes)`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return ErrInvalid
	}
	return nil
}
