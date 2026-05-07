package verify

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// TargetVerification records one observation of a file on one target.
type TargetVerification struct {
	Target     string    `json:"target"`      // target Identifier()
	Hash       string    `json:"hash"`        // sha256 hex; "" if walked but not yet hashed
	Size       int64     `json:"size"`        // bytes (always present after walk)
	ModTime    time.Time `json:"mtime"`       // mtime as observed
	VerifiedAt time.Time `json:"verified_at"` // when this record was last updated
	OK         bool      `json:"ok"`          // hash matched the canonical hash
}

// FileRecord is the manifest entry for a single file (keyed by the canonical
// path — typically the path on the source library). Holds one TargetVerification
// per target the file has been observed on.
type FileRecord struct {
	Path          string                          `json:"path"`
	Verifications map[string]TargetVerification   `json:"verifications"`
	// CanonicalHash is the hash agreed on by the majority of targets.
	// If targets disagree, this records the most-common value; mismatched
	// targets are marked OK=false in their TargetVerification.
	CanonicalHash string `json:"canonical_hash"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Manifest is the on-disk record of all verifications. Thread-safe.
type Manifest struct {
	path string

	mu    sync.Mutex
	files map[string]*FileRecord // canonical path → record
}

// NewManifest creates an empty manifest at the given path. If a file already
// exists there, it is loaded.
func NewManifest(path string) (*Manifest, error) {
	m := &Manifest{path: path, files: make(map[string]*FileRecord)}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if data, err := os.ReadFile(path); err == nil {
		var disk struct {
			Files map[string]*FileRecord `json:"files"`
		}
		if err := json.Unmarshal(data, &disk); err == nil && disk.Files != nil {
			m.files = disk.Files
		}
	}
	return m, nil
}

// DefaultManifestPath returns the canonical on-disk location:
// ~/Library/Application Support/JuiceMount/manifest.json on macOS.
func DefaultManifestPath() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, "Library", "Application Support",
			"JuiceMount", "manifest.json")
	}
	return "./juicemount-manifest.json"
}

// Save writes the manifest atomically (.tmp + rename).
func (m *Manifest) Save() error {
	m.mu.Lock()
	disk := struct {
		Files     map[string]*FileRecord `json:"files"`
		SavedAt   time.Time              `json:"saved_at"`
		FileCount int                    `json:"file_count"`
	}{
		Files:     m.files,
		SavedAt:   time.Now(),
		FileCount: len(m.files),
	}
	m.mu.Unlock()

	data, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		return err
	}

	tmp := fmt.Sprintf("%s.tmp.%d", m.path, os.Getpid())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, m.path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// Record updates the manifest with a single observation. Recomputes the
// canonical hash (majority vote across all known verifications for this path).
func (m *Manifest) Record(canonicalPath string, tv TargetVerification) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.files[canonicalPath]
	if !ok {
		rec = &FileRecord{
			Path:          canonicalPath,
			Verifications: make(map[string]TargetVerification),
		}
		m.files[canonicalPath] = rec
	}

	// Store the verification keyed by target identifier (replaces any prior obs)
	rec.Verifications[tv.Target] = tv

	// Recompute canonical hash: simple majority vote
	rec.CanonicalHash = majorityHash(rec.Verifications)
	rec.UpdatedAt = time.Now()

	// Update OK flag on every target verification based on agreement
	for k, v := range rec.Verifications {
		v.OK = v.Hash != "" && v.Hash == rec.CanonicalHash
		rec.Verifications[k] = v
	}
}

// Get returns a snapshot copy of a file record (or nil if unknown).
func (m *Manifest) Get(canonicalPath string) *FileRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.files[canonicalPath]
	if !ok {
		return nil
	}
	// Return a copy so callers can't mutate our internal state
	cp := *rec
	cp.Verifications = make(map[string]TargetVerification, len(rec.Verifications))
	for k, v := range rec.Verifications {
		cp.Verifications[k] = v
	}
	return &cp
}

// AllPaths returns every path tracked in the manifest, sorted.
func (m *Manifest) AllPaths() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.files))
	for p := range m.files {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// Stats returns aggregate counts by status.
type ManifestStats struct {
	TotalFiles  int
	GreenCount  int
	YellowCount int
	RedCount    int
	UnknownCount int
}

func (m *Manifest) Stats() ManifestStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := ManifestStats{TotalFiles: len(m.files)}
	for _, rec := range m.files {
		switch statusOf(rec) {
		case StatusGreen:
			s.GreenCount++
		case StatusYellow:
			s.YellowCount++
		case StatusRed:
			s.RedCount++
		default:
			s.UnknownCount++
		}
	}
	return s
}

// majorityHash returns the most-common non-empty hash among verifications.
// On a tie, returns the lexicographically smallest. Returns "" if no
// verification has a hash yet.
func majorityHash(verifs map[string]TargetVerification) string {
	counts := make(map[string]int)
	for _, v := range verifs {
		if v.Hash != "" {
			counts[v.Hash]++
		}
	}
	if len(counts) == 0 {
		return ""
	}
	var best string
	bestCount := 0
	for h, c := range counts {
		if c > bestCount || (c == bestCount && h < best) {
			best = h
			bestCount = c
		}
	}
	return best
}

// statusOf computes the traffic light for a single file record.
// Counts only verifications where Hash != "" AND OK == true.
func statusOf(rec *FileRecord) Status {
	if rec == nil || rec.CanonicalHash == "" {
		return StatusUnknown
	}
	verified := 0
	for _, v := range rec.Verifications {
		if v.OK {
			verified++
		}
	}
	if verified == 0 {
		return StatusRed
	}
	if verified >= MinGreenCopies {
		return StatusGreen
	}
	// Also: if any target reported a hash mismatch, that's red — silent corruption
	for _, v := range rec.Verifications {
		if v.Hash != "" && !v.OK {
			return StatusRed
		}
	}
	return StatusYellow
}
