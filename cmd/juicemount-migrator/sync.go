//go:build migrator_wip
// +build migrator_wip

package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// RunSync invokes `juicefs sync` against the given source and
// destination. The destination is interpreted as a path inside the
// JuiceFS volume identified by metaURL; the source is any URL/path
// juicefs sync accepts (file://, s3://, sftp://, etc.).
//
// Emits ProgressEvent on the `progress` channel each time juicefs
// writes a recognized stderr progress line. Blocks until juicefs
// exits or ctx is canceled.
//
// Returns nil on clean exit (`juicefs sync` returned 0), or a
// non-nil error describing why it failed. Cancelation via ctx
// returns context.Canceled.
func RunSync(ctx context.Context, juicefsBin, metaURL, source, destination string, progress chan<- ProgressEvent) error {
	// juicefs sync syntax:
	//   juicefs sync [options] SRC DST
	// Destinations expressed via a meta-url-style scheme:
	//   jfs://<volume-name>/path  (talks via metaURL)
	// For a local source we pass the file:// scheme or a raw path.
	src := normalizeSyncURI(source)
	dst := normalizeSyncURI(destination)

	args := []string{
		"sync",
		"--list-threads", "10", // parallel directory walk
		"--threads", "10", // parallel file copy
		"--no-https",         // local LAN, no need for TLS overhead
		"--update",           // only copy newer / missing — idempotent re-run
		"--check-change",     // verify size/mtime to skip already-synced files
		"--manager-addr", "127.0.0.1:6710",
		src,
		dst,
	}
	cmd := exec.CommandContext(ctx, juicefsBin, args...)
	cmd.Env = append(cmd.Env, "JFS_META_URL="+metaURL)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("pipe stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start juicefs sync: %w", err)
	}

	// Parse stderr in the foreground; juicefs writes progress to stderr.
	parseSyncProgress(stderr, progress)

	if err := cmd.Wait(); err != nil {
		if ctx.Err() == context.Canceled {
			return context.Canceled
		}
		return fmt.Errorf("juicefs sync exited: %w", err)
	}
	return nil
}

// progressRegex matches a juicefs sync progress line. Example formats
// the parser handles (juicefs's output has evolved across versions):
//   Scanned: 1234, copied: 567 (12.3 MiB/s), skipped: 0, failed: 0, eta: 12s
//   2026/05/26 14:33:21 <INFO> Scanned 100 entries, copied 50 (1.5 GiB), failed 0
//
// Any unmatched line is ignored. Robust to format drift — emits a
// partial event with whatever fields parsed.
var progressRegex = regexp.MustCompile(
	`(?i)\b(?:scanned|copied|skipped|failed|bytes|eta)[:\s]+([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]i?B)?(/s)?`,
)

// parseSyncProgress reads stderr line-by-line, extracts progress
// counters, and emits ProgressEvent. Closes nothing — caller owns
// the `progress` channel lifecycle.
func parseSyncProgress(r io.Reader, progress chan<- ProgressEvent) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024) // 1 MiB max line
	lastEmit := time.Now()
	var current ProgressEvent
	for scanner.Scan() {
		line := scanner.Text()
		updated := false
		matches := progressRegex.FindAllStringSubmatch(line, -1)
		for _, m := range matches {
			label := strings.ToLower(extractLabel(m[0]))
			valStr := m[1]
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				continue
			}
			unit := strings.ToUpper(m[2])
			// Convert to bytes if a size unit was present.
			if unit != "" {
				val = applyUnit(val, unit)
			}
			switch label {
			case "scanned":
				// scanned ≠ copied; we don't surface scanned count
				// directly but it could be useful for progress %.
				updated = true
			case "copied":
				if unit == "" {
					current.Files = int64(val)
				} else {
					current.Bytes = int64(val)
				}
				updated = true
			case "skipped":
				// not currently surfaced
				updated = true
			case "failed":
				current.Errors = int64(val)
				updated = true
			case "bytes":
				current.Bytes = int64(val)
				updated = true
			case "eta":
				current.ETASec = int64(val)
				updated = true
			}
		}
		// Look for a "current file" marker — juicefs prints
		// "Copying <path>" or "Processed <path>" lines.
		if strings.Contains(line, "Copying ") {
			if idx := strings.Index(line, "Copying "); idx >= 0 {
				current.Current = strings.TrimSpace(line[idx+len("Copying "):])
				updated = true
			}
		}

		if updated {
			now := time.Now()
			// Throttle event emission to ~5 Hz so a flood of
			// progress lines doesn't overwhelm SSE consumers.
			if now.Sub(lastEmit) >= 200*time.Millisecond {
				current.UpdatedAt = now.UnixMilli()
				// Best-effort send; drop if buffer is full.
				select {
				case progress <- current:
				default:
				}
				lastEmit = now
			}
		}
	}
	// Final flush — emit whatever we have so the listener sees the
	// last state even if the throttle window was active.
	current.UpdatedAt = time.Now().UnixMilli()
	select {
	case progress <- current:
	default:
	}
}

// extractLabel returns the label part of a match like "copied: 50"
// → "copied". The regex captures the value separately so we re-parse
// the first word here.
func extractLabel(match string) string {
	for i, r := range match {
		if r == ':' || r == ' ' || r == '\t' {
			return match[:i]
		}
	}
	return match
}

// applyUnit converts a value + IEC/SI unit string to bytes.
// Examples: ("12.3", "MIB") → 12.3 * 1024 * 1024.
func applyUnit(v float64, unit string) float64 {
	switch unit {
	case "KB", "KIB":
		return v * 1024
	case "MB", "MIB":
		return v * 1024 * 1024
	case "GB", "GIB":
		return v * 1024 * 1024 * 1024
	case "TB", "TIB":
		return v * 1024 * 1024 * 1024 * 1024
	case "PB", "PIB":
		return v * 1024 * 1024 * 1024 * 1024 * 1024
	case "EB", "EIB":
		return v * 1024 * 1024 * 1024 * 1024 * 1024 * 1024
	}
	return v
}

// normalizeSyncURI converts a user-provided source/destination string
// into the form juicefs sync expects. Heuristics:
//   - bare path (starts with /): becomes file://<abspath>
//   - jfs://... : kept as-is (talks to metaURL)
//   - scheme://... : kept as-is
func normalizeSyncURI(s string) string {
	if strings.HasPrefix(s, "/") {
		return "file://" + s
	}
	return s
}

// randHex returns n random bytes as a hex string. Used for job IDs.
func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
