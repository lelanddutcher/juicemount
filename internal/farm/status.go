package farm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
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

// InProgress is the live snapshot of the sweep currently running. The farm writes
// it on a ~3s ticker DURING a sweep so the manager Farm tab can show a progress bar
// + ETA without a farm control plane. It is present ONLY mid-sweep; the FINAL
// WriteFarmStatus (after wg.Wait) clears it (nil → omitted on the wire) so an idle
// status file never carries a stale in-progress block. ETA = elapsed/done * (total-done).
type InProgress struct {
	Pass      string `json:"pass"`       // current jmfarm pass label (the resolved mode)
	Total     int    `json:"total"`      // total files this sweep targets
	Done      int    `json:"done"`       // ok+failed so far (atomic snapshot)
	Failed    int    `json:"failed"`     // failed so far (atomic snapshot)
	StartedAt int64  `json:"started_at"` // unix seconds; sweep start
}

// BloatRow is one underperforming proxy in the economics rollup: a proxy that
// saved LESS than the threshold (the iPhone/h264-already-small case). Name is the
// source basename when cheaply resolvable, else omitted (inode identifies it).
type BloatRow struct {
	Name        string `json:"name,omitempty"`
	Inode       uint64 `json:"inode"`
	SourceBytes int64  `json:"source_bytes"`
	ProxyBytes  int64  `json:"proxy_bytes"`
	SavingPct   int    `json:"saving_pct"`
}

// ProxyEconomics is a read-only rollup over the proxy derivatives whose blob
// exists on disk: how many were measured, the aggregate source vs proxy bytes, the
// average saving %, and the worst offenders (proxies that barely shrank — the
// iPhone-proxy-bloat worry). Computed by stat-walking the blobs; NO schema change.
type ProxyEconomics struct {
	Measured         int        `json:"measured"`
	TotalSourceBytes int64      `json:"total_source_bytes"`
	TotalProxyBytes  int64      `json:"total_proxy_bytes"`
	AvgSavingPct     int        `json:"avg_saving_pct"`
	Bloat            []BloatRow `json:"bloat"`
}

// FarmStatus is the operator rollup the farm writes after each sweep. The manager
// Farm tab reads it as a PLAIN JSON FILE (the manager is CGO-free + can't open the
// farm's sqlite, and there's no farm control plane) — so the farm pre-aggregates
// here. Reflects what the farm's own index has done (Mac-generated derivatives
// aren't in this db; full-volume coverage is the Mac's control plane post-JM-15).
//
// InProgress + ProxyEconomics are ADDITIVE: InProgress is present only mid-sweep
// (pointer → omitted when idle); ProxyEconomics is the proxy size/saving rollup
// (pointer → omitted when not measured this write).
type FarmStatus struct {
	Index          derivatives.Stats `json:"index"`
	LastSweep      SweepInfo         `json:"last_sweep"`
	Governor       Governor          `json:"governor"`
	InProgress     *InProgress       `json:"in_progress,omitempty"`
	ProxyEconomics *ProxyEconomics   `json:"proxy_economics,omitempty"`
	WrittenAt      int64             `json:"written_at"`
}

// bloatThresholdPct is the saving-% floor below which a proxy is flagged as a
// bloat offender (saved less than this much). The iPhone/h264 case typically lands
// in single digits because the source is already an efficient delivery codec.
const bloatThresholdPct = 25

// maxBloatRows caps the bloat list so the status file stays small even on a volume
// full of already-compressed clips.
const maxBloatRows = 20

// ComputeProxyEconomics rolls up the proxy derivatives whose blob exists on disk.
// For each ready proxy row it os.Stats <mount>/.juicemount/derivatives/<inode>/proxy.mp4
// for the actual proxy bytes and compares against the row's recorded source_size.
// Rows with no source_size, a missing/zero blob, or a non-positive source are
// skipped (not measurable). Returns the measured count, aggregate bytes, the
// average saving % (1 - totalProxy/totalSource), and up to maxBloatRows worst
// offenders saving < bloatThresholdPct, sorted ascending by saving %. Returns nil
// when nothing is measurable (caller omits the field). Read-only; no DB writes.
func ComputeProxyEconomics(store *derivatives.Store, mount string) (*ProxyEconomics, error) {
	if store == nil || mount == "" {
		return nil, nil
	}
	rows, err := store.ListProxyRows()
	if err != nil {
		return nil, err
	}
	var measured int
	var totalSrc, totalProxy int64
	var bloat []BloatRow
	for _, r := range rows {
		if r.SourceSize == nil || *r.SourceSize <= 0 {
			continue
		}
		src := *r.SourceSize
		blob := filepath.Join(DerivBlobDir(mount, r.Inode), "proxy.mp4")
		fi, err := os.Stat(blob)
		if err != nil || fi.Size() <= 0 {
			continue // blob gone/empty → not measurable
		}
		pb := fi.Size()
		measured++
		totalSrc += src
		totalProxy += pb
		saving := int((1 - float64(pb)/float64(src)) * 100)
		if saving < bloatThresholdPct {
			bloat = append(bloat, BloatRow{
				Inode: r.Inode, SourceBytes: src, ProxyBytes: pb, SavingPct: saving,
			})
		}
	}
	if measured == 0 {
		return nil, nil
	}
	avg := 0
	if totalSrc > 0 {
		avg = int((1 - float64(totalProxy)/float64(totalSrc)) * 100)
	}
	// Worst (smallest saving, incl. negative bloat) first; cap the list.
	sort.Slice(bloat, func(i, j int) bool { return bloat[i].SavingPct < bloat[j].SavingPct })
	if len(bloat) > maxBloatRows {
		bloat = bloat[:maxBloatRows]
	}
	return &ProxyEconomics{
		Measured:         measured,
		TotalSourceBytes: totalSrc,
		TotalProxyBytes:  totalProxy,
		AvgSavingPct:     avg,
		Bloat:            bloat,
	}, nil
}

// WriteFarmStatus rolls up the index + the just-finished sweep + the resolved
// governor settings to a JSON file (e.g. /state/farm-status.json) for the manager
// to display. This is the FINAL (idle) write: it clears in_progress (absent) and
// measures proxy_economics by stat-walking the proxy blobs under mount. A "" mount
// (or a measure error) just omits proxy_economics — the rest of the status is still
// written so a measure hiccup never blanks the dashboard.
func WriteFarmStatus(store *derivatives.Store, path, mount string, sweep SweepInfo, gov Governor) error {
	st, err := store.Stats()
	if err != nil {
		return err
	}
	fs := FarmStatus{Index: st, LastSweep: sweep, Governor: gov, WrittenAt: time.Now().Unix()}
	// Best-effort: a stat-walk error or a "" mount just leaves economics absent.
	if econ, err := ComputeProxyEconomics(store, mount); err == nil {
		fs.ProxyEconomics = econ
	}
	return writeStatusFile(path, fs)
}

// WriteFarmProgress writes a mid-sweep status file carrying the live in_progress
// block. It is cheap by design: it runs the same fast Stats GROUP BY but SKIPS the
// proxy_economics stat-walk (that's measured on the final WriteFarmStatus) so the
// ~3s ticker never stalls on a slow volume. The final WriteFarmStatus overwrites
// this and clears in_progress.
func WriteFarmProgress(store *derivatives.Store, path, mount string, sweep SweepInfo, gov Governor, ip InProgress) error {
	st, err := store.Stats()
	if err != nil {
		return err
	}
	fs := FarmStatus{Index: st, LastSweep: sweep, Governor: gov, InProgress: &ip, WrittenAt: time.Now().Unix()}
	return writeStatusFile(path, fs)
}

// writeStatusFile marshals + atomically-ish writes the status JSON, creating the
// parent dir. Shared by the final + progress writers.
func writeStatusFile(path string, fs FarmStatus) error {
	b, err := json.MarshalIndent(fs, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return atomicWriteFile(path, b, 0o644)
}
