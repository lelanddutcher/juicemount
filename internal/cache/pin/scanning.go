package pin

import (
	"sync"
	"time"
)

// scanning.go — tracks pin roots whose file tree is currently being enumerated
// (R-2, release punch list).
//
// Pinning a large folder walks its whole subtree (pin.CountFilesUnder), which
// can take tens of seconds for a multi-thousand-file video reel. The old
// NFSServerPin did that walk synchronously before returning, so the menu-bar
// app showed NOTHING — no new pinned root, no spinner — until the walk and the
// SQL insert both finished. The user clicked Pin and the UI sat dead for tens
// of seconds, which reads as "pinning is broken / didn't work."
//
// NFSServerPin now marks the root as "scanning" here, returns immediately, and
// does the walk + insert on a background goroutine. /cache-status surfaces the
// scanning set so the UI can show "Scanning <folder>…" the instant the click
// lands, then swap to the real pinned-root row once the rows exist.

// ScanningRoot is one root mid-enumeration, surfaced in /cache-status.
type ScanningRoot struct {
	Root       string `json:"root"`
	FilesFound int    `json:"files_found"`
	BytesFound int64  `json:"bytes_found"`
	SinceSec   int64  `json:"since_sec"`
}

type scanState struct {
	files   int
	bytes   int64
	started time.Time
}

var (
	scanningMu    sync.RWMutex
	scanningRoots = map[string]*scanState{}
)

// MarkScanning records that root's subtree is being enumerated. Call before
// kicking off the async walk so the UI shows the spinner immediately.
func MarkScanning(root string) {
	scanningMu.Lock()
	scanningRoots[root] = &scanState{started: time.Now()}
	scanningMu.Unlock()
}

// UpdateScanProgress updates the running file/byte tally for a scanning root.
// No-op if the root isn't currently marked scanning.
func UpdateScanProgress(root string, files int, bytes int64) {
	scanningMu.Lock()
	if s, ok := scanningRoots[root]; ok {
		s.files = files
		s.bytes = bytes
	}
	scanningMu.Unlock()
}

// ClearScanning removes root from the scanning set — call once its rows are
// inserted (the pinned-root row takes over in the UI) or the walk failed.
func ClearScanning(root string) {
	scanningMu.Lock()
	delete(scanningRoots, root)
	scanningMu.Unlock()
}

// ScanningRoots snapshots the roots currently being enumerated. Cheap RLock;
// called on the /cache-status poll cadence, not a hot path.
func ScanningRoots() []ScanningRoot {
	scanningMu.RLock()
	defer scanningMu.RUnlock()
	out := make([]ScanningRoot, 0, len(scanningRoots))
	for root, s := range scanningRoots {
		out = append(out, ScanningRoot{
			Root:       root,
			FilesFound: s.files,
			BytesFound: s.bytes,
			SinceSec:   int64(time.Since(s.started).Seconds()),
		})
	}
	return out
}
