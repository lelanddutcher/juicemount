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
// destination. Source is any URL/path juicefs sync accepts
// (file://, s3://, sftp://, etc.). Destination is interpreted as
// a path inside the JuiceFS volume identified by metaURL.
//
// Folder-structure preservation: source is unconditionally given
// a trailing slash so juicefs sync uses the rsync "copy contents,
// not the directory itself" semantic. The UI's destination already
// includes a basename; without the trailing slash we'd produce
// `<dest>/<basename>/<basename>/...` double-nested output.
//
// Destination URL form: jfs://<volName>/<path-within-volume>. This
// uses juicefs's native meta+S3 write path so the migrator container
// does NOT need a FUSE mount of the volume — chunks are written
// directly to MinIO with metadata going through Redis via JFS_META_URL.
//
// Emits ProgressEvent on the `progress` channel each time juicefs
// writes a recognized stderr progress line. Blocks until juicefs
// exits or ctx is canceled.
//
// Returns nil on clean exit (`juicefs sync` returned 0), or a
// non-nil error describing why it failed. Cancelation via ctx
// returns context.Canceled.
func RunSync(ctx context.Context, juicefsBin, metaURL, volName, destMount, source, destination string, progress chan<- ProgressEvent) error {
	src := normalizeSourceURI(source)
	dst := normalizeDestURI(destination, destMount, volName)

	args := []string{
		"sync",
		"--list-threads", "10", // parallel directory walk
		"--threads", "10", // parallel file copy
		"--update",        // only copy newer / missing — idempotent re-run
		"--check-change",  // verify size/mtime to skip already-synced files
		// Note (2026-05-27): --no-https and --manager-addr removed.
		// --no-https isn't a real juicefs flag (was rclone confusion).
		// --manager-addr binds 6710 for distributed-sync coordination;
		// for single-node migration it can fail and is unnecessary.
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

// normalizeSourceURI converts a user-provided source string into the
// form juicefs sync expects.
//
//   - bare absolute path (starts with /): becomes file://<abspath>/
//   - already-scheme'd URL (scheme://...): trailing slash appended if absent
//
// Trailing slash is ALWAYS appended (idempotent if already present)
// so juicefs sync uses rsync "copy contents" semantics instead of
// "copy this directory itself". See the RunSync doc comment for why.
func normalizeSourceURI(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "/") {
		s = "file://" + s
	}
	if !strings.HasSuffix(s, "/") {
		s = s + "/"
	}
	return s
}

// normalizeDestURI translates a UI-supplied destination path into a
// juicefs-sync-compatible URL.
//
// The UI shows the destination to users as a filesystem path like
// `/jfs/imported/2026-05-27-foo` because that maps to their mental
// model of "a place inside the JuiceFS volume." Internally we
// translate that to `jfs://<volName>/imported/2026-05-27-foo` so
// juicefs sync writes directly through JFS_META_URL — no FUSE mount
// required in the migrator container.
//
// Path-resolution rules:
//   - If dest already has a scheme: kept as-is + trailing slash
//   - If dest starts with destMount (e.g. "/jfs/foo"): strip the
//     destMount prefix and emit "jfs://<volName>/foo/"
//   - Otherwise (bare absolute path not under destMount): assume the
//     user means a path within the volume rooted at the path itself.
//     Emit "jfs://<volName>/<the-whole-path>/"
//
// The trailing slash is always appended for consistency, though
// destinations are less sensitive to it than sources.
func normalizeDestURI(dest, destMount, volName string) string {
	dest = strings.TrimSpace(dest)
	// Already-scheme'd → pass through unchanged (+ trailing slash).
	if i := strings.Index(dest, "://"); i > 0 && i < 10 {
		if !strings.HasSuffix(dest, "/") {
			dest = dest + "/"
		}
		return dest
	}
	// Strip optional destMount prefix to get the path inside the volume.
	rel := dest
	mp := strings.TrimSuffix(destMount, "/")
	if mp != "" && strings.HasPrefix(rel, mp+"/") {
		rel = rel[len(mp):] // keep leading slash for the path
	} else if rel == mp {
		rel = "/"
	}
	// Ensure rel starts with /.
	if !strings.HasPrefix(rel, "/") {
		rel = "/" + rel
	}
	if !strings.HasSuffix(rel, "/") {
		rel = rel + "/"
	}
	return "jfs://" + volName + rel
}

// randHex returns n random bytes as a hex string. Used for job IDs.
func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
