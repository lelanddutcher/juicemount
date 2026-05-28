package nfs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ManifestFile is the basename of the spool's append-only audit log,
// written under <spool_root>. One JSONL record per drain disposition.
const ManifestFile = "manifest.log"

// ManifestRecord is one line of the JSONL audit log. New fields may
// be appended over time — JSONL with field tags is forward-compatible
// for parsers that ignore unknown keys.
type ManifestRecord struct {
	TimestampRFC3339Nano string `json:"ts"`
	Event                string `json:"event"`
	Path                 string `json:"path"`
	SpoolFile            string `json:"spool_file,omitempty"`
	Size                 int64  `json:"size,omitempty"`
	SHA256Hex            string `json:"sha256,omitempty"`
	SHA256ExpectedHex    string `json:"sha256_expected,omitempty"`
	SHA256ActualHex      string `json:"sha256_actual,omitempty"`
	// SHA256Unavailable distinguishes "we have no SHA for this record"
	// (out-of-order writes invalidated the streaming hash, or the row
	// row.SHA256 lookup failed) from "we forgot to populate the field."
	// Without this, an auditor reading a quarantine record with no sha
	// can't tell whether the SHA never existed or was suppressed.
	SHA256Unavailable bool `json:"sha256_unavailable,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

// Manifest event identifiers.
const (
	ManifestEventDrainDone   = "drain_done"
	ManifestEventQuarantine  = "quarantine"
	ManifestEventDrainFailed = "drain_failed"
)

// manifestWriter serializes JSONL appends to <root>/manifest.log.
//
// Each call takes a per-writer mutex and a synchronous write+fsync, so
// concurrent appends from multiple drainer workers produce a strictly
// linearized log. The mutex contention is negligible at drain rates
// (≤1 record per file drained, throughput is bounded by MinIO upload
// speed, not by log append).
type manifestWriter struct {
	path string

	mu     sync.Mutex
	closed bool
}

// newManifestWriter creates the manifest in the spool root if it doesn't
// already exist. Failure here is non-fatal for the spool itself; the
// SpoolStore constructor uses a nil manifest writer when this errors
// and the audit path becomes a no-op (logged once at boot).
func newManifestWriter(root string) (*manifestWriter, error) {
	path := filepath.Join(root, ManifestFile)
	// Touch the file so a parser that opens-for-read on first boot
	// doesn't get an ENOENT. Append-mode open creates it if missing.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("manifest open: %w", err)
	}
	_ = f.Close()
	return &manifestWriter{path: path}, nil
}

// Append writes one JSONL record + newline and fsyncs. Errors are
// returned to the caller; the drainer logs them but does not abort the
// drain on a manifest write failure (the SQL row is the durable
// truth).
func (m *manifestWriter) Append(rec ManifestRecord) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("manifest: writer closed")
	}
	if rec.TimestampRFC3339Nano == "" {
		rec.TimestampRFC3339Nano = time.Now().UTC().Format(time.RFC3339Nano)
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("manifest marshal: %w", err)
	}
	data = append(data, '\n')

	f, err := os.OpenFile(m.path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("manifest open: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("manifest write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("manifest fsync: %w", err)
	}
	return f.Close()
}

// Close marks the writer closed. Subsequent Append calls return an
// error. Idempotent.
func (m *manifestWriter) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// Path returns the manifest file path.
func (m *manifestWriter) Path() string {
	if m == nil {
		return ""
	}
	return m.path
}
