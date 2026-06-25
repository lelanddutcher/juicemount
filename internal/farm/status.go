package farm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// SweepInfo describes the sweep that just finished.
type SweepInfo struct {
	Mode       string `json:"mode"`
	Producer   string `json:"producer"`
	Target     string `json:"target"`
	Processed  int    `json:"processed"`
	Failed     int    `json:"failed"`
	StartedAt  int64  `json:"started_at"`
	DurationMS int64  `json:"duration_ms"`
}

// FarmStatus is the operator rollup the farm writes after each sweep. The manager
// Farm tab reads it as a PLAIN JSON FILE (the manager is CGO-free + can't open the
// farm's sqlite, and there's no farm control plane) — so the farm pre-aggregates
// here. Reflects what the farm's own index has done (Mac-generated derivatives
// aren't in this db; full-volume coverage is the Mac's control plane post-JM-15).
type FarmStatus struct {
	Index     derivatives.Stats `json:"index"`
	LastSweep SweepInfo         `json:"last_sweep"`
	WrittenAt int64             `json:"written_at"`
}

// WriteFarmStatus rolls up the index + the just-finished sweep to a JSON file
// (e.g. /state/farm-status.json) for the manager to display.
func WriteFarmStatus(store *derivatives.Store, path string, sweep SweepInfo) error {
	st, err := store.Stats()
	if err != nil {
		return err
	}
	fs := FarmStatus{Index: st, LastSweep: sweep, WrittenAt: time.Now().Unix()}
	b, err := json.MarshalIndent(fs, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
