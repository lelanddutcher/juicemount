// Package verify provides content-hash backup verification across multiple
// independent storage targets (local FS, S3-compatible, NAS, etc.).
//
// The package answers two practical questions for the editor:
//   1. "Is this file actually backed up, or is the backup script lying to me?"
//   2. "Can I delete this file from my main library without losing it forever?"
//
// Both questions reduce to: walk every configured Target, compute SHA-256
// content hashes, and aggregate the results into a per-file traffic-light
// status (green / yellow / red).
package verify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"strings"
	"time"
)

// Entry is a single file as observed on a target.
// Hash is empty until ComputeHash is called.
type Entry struct {
	RelPath string    // path relative to the target's root
	Size    int64
	ModTime time.Time
	Hash    string // hex-encoded sha256, empty until computed
}

// Target abstracts a storage location that contains files we want to verify.
// Implementations: LocalTarget (filesystem), and (future) S3Target, SMBTarget.
type Target interface {
	// Identifier is a stable, human-readable string like "local:/Volumes/zpool"
	// or "s3:b2://my-bucket". Used as the manifest key and in UI listings.
	Identifier() string

	// Walk emits every file the target knows about. The channel closes when
	// the walk finishes or ctx cancels. Sends are non-blocking — if the
	// consumer falls behind, the walker pauses (no buffering past channel cap).
	Walk(ctx context.Context) (<-chan Entry, <-chan error)

	// Hash returns the SHA-256 of the file at the given relative path.
	// Streams from disk/network; never loads the full file into memory.
	Hash(ctx context.Context, relPath string) (string, error)

	// Available returns true if the target is currently reachable. Used by
	// the manager to skip dead targets without aborting the whole verification.
	Available(ctx context.Context) bool
}

// Status is the traffic-light verdict for a single file.
type Status int

const (
	StatusUnknown   Status = iota
	StatusGreen            // ≥ MinGreenCopies verified copies
	StatusYellow           // 1 ≤ count < MinGreenCopies
	StatusRed              // 0 verified copies OR a hash mismatch detected
)

func (s Status) String() string {
	switch s {
	case StatusGreen:
		return "green"
	case StatusYellow:
		return "yellow"
	case StatusRed:
		return "red"
	default:
		return "unknown"
	}
}

// MinGreenCopies is the threshold for "green" status. The folk wisdom
// "if a file doesn't exist in three places, it doesn't exist" gives 3.
// Configurable per-instance later.
const MinGreenCopies = 3

// hashReader streams sha256 of an io.Reader. Used by every Target
// implementation so the hashing rule is uniform across backends.
func hashReader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hashWriter wraps a hash.Hash to satisfy io.Writer for use in tee patterns.
// Useful when the caller wants both the file bytes (e.g. to upload) AND
// the hash without two reads.
type hashWriter struct{ h hash.Hash }

// NewHashWriter returns an io.Writer that computes sha256 of everything
// written through it. Call Hex() at the end.
func NewHashWriter() *hashWriter { return &hashWriter{h: sha256.New()} }

func (hw *hashWriter) Write(p []byte) (int, error) { return hw.h.Write(p) }
func (hw *hashWriter) Hex() string                  { return hex.EncodeToString(hw.h.Sum(nil)) }

// commonPrefix is a small helper used by some targets to make Identifier
// strings stable across machines that mount the same NAS at different paths.
func commonPrefix(scheme, path string) string {
	return scheme + ":" + strings.TrimRight(path, "/")
}

// ErrTargetUnavailable is returned by operations against an unreachable target.
var ErrTargetUnavailable = errors.New("target unavailable")
