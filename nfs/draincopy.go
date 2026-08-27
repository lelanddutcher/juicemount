package nfs

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"time"
)

// durableSyncWriter is the subset of *os.File used by the spool drainer.
// Keeping the copy loop against this small interface makes the durability
// boundary directly testable without pretending a temp directory is JuiceFS.
type durableSyncWriter interface {
	io.Writer
	Sync() error
}

type durableCopyResult struct {
	Bytes        int64
	SHA256       [sha256.Size]byte
	SyncCalls    int
	SyncDuration time.Duration
	MaxSync      time.Duration
}

// copyWithDurableCheckpoints copies src to dst and durably flushes dst after at
// most syncInterval bytes. The final partial interval is always flushed; an
// empty file is flushed once as well.
//
// This is deliberately not JuiceFS --writeback. Each successful Sync means the
// corresponding bytes have reached the real backend before the drainer may
// advance. The spool file remains untouched until the caller completes every
// checkpoint, verifies the byte count/SHA, closes dst, and commits the row.
// Bounding the dirty interval prevents one giant whole-file Fsync from freezing
// a multi-gigabyte writer inside macFUSE for JuiceFS's full flush deadline.
func copyWithDurableCheckpoints(dst durableSyncWriter, src io.Reader, syncInterval int64, buf []byte) (durableCopyResult, error) {
	var result durableCopyResult
	if dst == nil {
		return result, errors.New("durable copy: destination is required")
	}
	if src == nil {
		return result, errors.New("durable copy: source is required")
	}
	if syncInterval <= 0 {
		return result, errors.New("durable copy: sync interval must be positive")
	}
	if len(buf) == 0 {
		return result, errors.New("durable copy: buffer must not be empty")
	}

	h := sha256.New()
	sinceSync := int64(0)
	syncNow := func() error {
		started := time.Now()
		err := dst.Sync()
		elapsed := time.Since(started)
		result.SyncCalls++
		result.SyncDuration += elapsed
		if elapsed > result.MaxSync {
			result.MaxSync = elapsed
		}
		if err == nil {
			sinceSync = 0
		}
		return err
	}

	for {
		nr, readErr := src.Read(buf)
		if nr > 0 {
			chunk := buf[:nr]
			for len(chunk) > 0 {
				remaining := syncInterval - sinceSync
				part := chunk
				if int64(len(part)) > remaining {
					part = part[:remaining]
				}

				nw, writeErr := dst.Write(part)
				if nw > 0 {
					_, _ = h.Write(part[:nw]) // hash.Hash.Write never returns an error
					result.Bytes += int64(nw)
					sinceSync += int64(nw)
				}
				if writeErr != nil {
					return result, writeErr
				}
				if nw != len(part) {
					return result, io.ErrShortWrite
				}
				chunk = chunk[nw:]

				if sinceSync == syncInterval {
					if err := syncNow(); err != nil {
						return result, fmt.Errorf("sync destination after byte %d: %w", result.Bytes, err)
					}
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return result, readErr
		}
		if nr == 0 {
			return result, io.ErrNoProgress
		}
	}

	// An exact interval ended with a successful checkpoint already. Otherwise
	// flush the final partial interval (or the zero-byte file) before returning.
	if sinceSync > 0 || result.SyncCalls == 0 {
		if err := syncNow(); err != nil {
			return result, fmt.Errorf("sync destination after byte %d: %w", result.Bytes, err)
		}
	}
	copy(result.SHA256[:], h.Sum(nil))
	return result, nil
}
