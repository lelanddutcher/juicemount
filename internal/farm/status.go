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

// Governor is the RESOLVED run configuration the farm was launched with — the
// CPU/IO knobs the manager's governor set plus the encode/transcript settings.
// It's stamped verbatim so the manager's read-only knob inspector shows REAL
// values (not whatever defaults it guesses). Any field may be zero/"" if the
// run didn't supply it; the UI degrades gracefully. The shape is a fixed wire
// contract — do not rename JSON keys.
type Governor struct {
	Model        string `json:"model"`         // contract provider/model id, e.g. "whisper.cpp/medium.en"
	VCodec       string `json:"vcodec"`        // proxy video encoder
	CRF          int    `json:"crf"`           // proxy CRF quality
	Preset       string `json:"preset"`        // proxy x264 preset
	Workers      int    `json:"workers"`       // effective worker count (-concurrency)
	ProxyWorkers int    `json:"proxy_workers"` // proxy-mode worker count (-proxy-concurrency)
	Nice         int    `json:"nice"`          // CPU niceness the entrypoint applied (display-only)
	IONice       int    `json:"ionice"`        // best-effort IO class the entrypoint applied (display-only)
	Mode         string `json:"mode"`          // resolved run mode (derivatives|transcript(AI)|proxy|all)
	IntervalSec  int    `json:"interval_sec"`  // seconds between sweeps (display-only)
}

// NewGovernor builds the governor stamp from a run's resolved settings. modelPath
// is the raw -whisper-model value; it's normalized to the contract's provider/model
// id via whisperModelID (e.g. "/models/ggml-medium.en.bin" → "whisper.cpp/medium.en").
// An empty modelPath yields an empty model so the UI can degrade gracefully rather
// than display a bogus "whisper.cpp/unknown".
func NewGovernor(modelPath, vcodec, preset, mode string, crf, workers, proxyWorkers, nice, ionice, intervalSec int) Governor {
	model := ""
	if modelPath != "" {
		model = whisperModelID(modelPath)
	}
	return Governor{
		Model:        model,
		VCodec:       vcodec,
		CRF:          crf,
		Preset:       preset,
		Workers:      workers,
		ProxyWorkers: proxyWorkers,
		Nice:         nice,
		IONice:       ionice,
		Mode:         mode,
		IntervalSec:  intervalSec,
	}
}

// FarmStatus is the operator rollup the farm writes after each sweep. The manager
// Farm tab reads it as a PLAIN JSON FILE (the manager is CGO-free + can't open the
// farm's sqlite, and there's no farm control plane) — so the farm pre-aggregates
// here. Reflects what the farm's own index has done (Mac-generated derivatives
// aren't in this db; full-volume coverage is the Mac's control plane post-JM-15).
type FarmStatus struct {
	Index     derivatives.Stats `json:"index"`
	LastSweep SweepInfo         `json:"last_sweep"`
	Governor  Governor          `json:"governor"`
	WrittenAt int64             `json:"written_at"`
}

// WriteFarmStatus rolls up the index + the just-finished sweep + the resolved
// governor settings to a JSON file (e.g. /state/farm-status.json) for the
// manager to display.
func WriteFarmStatus(store *derivatives.Store, path string, sweep SweepInfo, gov Governor) error {
	st, err := store.Stats()
	if err != nil {
		return err
	}
	fs := FarmStatus{Index: st, LastSweep: sweep, Governor: gov, WrittenAt: time.Now().Unix()}
	b, err := json.MarshalIndent(fs, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
