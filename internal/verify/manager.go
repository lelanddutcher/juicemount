package verify

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Manager runs verification across a set of Targets, persists results to a
// Manifest, and answers status queries.
type Manager struct {
	manifest *Manifest
	targets  []Target
}

// NewManager creates a manager with the given manifest + targets.
// The first target in the list is treated as the canonical source — its
// paths drive the iteration order. Subsequent targets are checked for
// matching files.
func NewManager(manifest *Manifest, targets ...Target) *Manager {
	return &Manager{manifest: manifest, targets: targets}
}

// AddTarget adds a target to the manager. Useful for incremental setup
// (add a USB drive after construction).
func (m *Manager) AddTarget(t Target) { m.targets = append(m.targets, t) }

// VerifyAll walks every target and updates the manifest. Each target runs
// in its own goroutine; within a target, files are walked + hashed serially
// to avoid hammering spinning disks or rate-limited cloud storage.
//
// Returns a per-target summary of how many files were observed/hashed/failed.
func (m *Manager) VerifyAll(ctx context.Context) (map[string]TargetSummary, error) {
	if len(m.targets) == 0 {
		return nil, fmt.Errorf("no targets configured")
	}

	results := make(map[string]TargetSummary)
	var resMu sync.Mutex

	var wg sync.WaitGroup
	for _, t := range m.targets {
		t := t // capture
		if !t.Available(ctx) {
			resMu.Lock()
			results[t.Identifier()] = TargetSummary{
				Target:    t.Identifier(),
				Available: false,
			}
			resMu.Unlock()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			summary := m.verifyTarget(ctx, t)
			resMu.Lock()
			results[t.Identifier()] = summary
			resMu.Unlock()
		}()
	}
	wg.Wait()

	if err := m.manifest.Save(); err != nil {
		return results, fmt.Errorf("save manifest: %w", err)
	}
	return results, nil
}

// TargetSummary reports the outcome of one VerifyAll call for one target.
type TargetSummary struct {
	Target     string
	Available  bool
	FilesSeen  int
	FilesHashed int
	FilesFailed int
	Duration    time.Duration
}

func (m *Manager) verifyTarget(ctx context.Context, t Target) TargetSummary {
	start := time.Now()
	summary := TargetSummary{
		Target:    t.Identifier(),
		Available: true,
	}

	entries, errs := t.Walk(ctx)
	for {
		select {
		case <-ctx.Done():
			summary.Duration = time.Since(start)
			return summary
		case err := <-errs:
			if err != nil {
				summary.FilesFailed++
			}
		case entry, ok := <-entries:
			if !ok {
				summary.Duration = time.Since(start)
				return summary
			}
			summary.FilesSeen++

			// Skip recently-modified files (likely an in-progress write).
			// 60s is generous enough to dodge most active rsyncs without
			// being so long that we miss a fresh backup.
			if time.Since(entry.ModTime) < 60*time.Second {
				continue
			}

			hash, err := t.Hash(ctx, entry.RelPath)
			tv := TargetVerification{
				Target:     t.Identifier(),
				Size:       entry.Size,
				ModTime:    entry.ModTime,
				VerifiedAt: time.Now(),
			}
			if err != nil {
				summary.FilesFailed++
				// Record the observation without a hash; canonical hash
				// resolution will mark this OK=false
			} else {
				tv.Hash = hash
				summary.FilesHashed++
			}
			m.manifest.Record(entry.RelPath, tv)
		}
	}
}

// Status returns the traffic-light verdict for a single canonical path.
func (m *Manager) Status(canonicalPath string) Status {
	rec := m.manifest.Get(canonicalPath)
	return statusOf(rec)
}

// SafeToDelete reports whether a file can be deleted from one location
// without risk of permanent loss. The rule is: at least 2 OTHER targets
// must have a verified copy with the agreed canonical hash.
//
// Returns (safe bool, explanation string).
type DeleteVerdict struct {
	Safe         bool
	Explanation  string
	VerifiedCopies int
	OtherTargets []string // identifiers of targets holding a verified copy
}

func (m *Manager) SafeToDelete(canonicalPath, deletingFrom string) DeleteVerdict {
	rec := m.manifest.Get(canonicalPath)
	if rec == nil {
		return DeleteVerdict{
			Safe:        false,
			Explanation: "file not in manifest — has never been verified",
		}
	}

	others := []string{}
	for tID, v := range rec.Verifications {
		if tID == deletingFrom {
			continue
		}
		if v.OK {
			others = append(others, tID)
		}
	}
	if len(others) >= 2 {
		return DeleteVerdict{
			Safe:           true,
			Explanation:    fmt.Sprintf("%d verified copies on %d other targets", len(others), len(others)),
			VerifiedCopies: len(others),
			OtherTargets:   others,
		}
	}
	return DeleteVerdict{
		Safe:           false,
		Explanation:    fmt.Sprintf("only %d verified copies exist outside the deletion target — refusing", len(others)),
		VerifiedCopies: len(others),
		OtherTargets:   others,
	}
}

// Stats returns aggregate manifest statistics.
func (m *Manager) Stats() ManifestStats {
	return m.manifest.Stats()
}

// AllPaths returns every path the manager knows about, sorted.
func (m *Manager) AllPaths() []string {
	return m.manifest.AllPaths()
}
