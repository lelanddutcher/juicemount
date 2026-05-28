package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SLICE 3 — Trash tab.
//
// JuiceFS ships a built-in retention feature: deleted files are
// moved into a hidden subtree at <fuseMount>/.trash/ instead of
// being purged immediately. The trash is laid out as
//
//	<fuseMount>/.trash/<yyyy-mm-dd-hh>/<inode-and-original-path>
//
// The bucket directories rotate hourly. juicefs auto-purges anything
// older than the configured --trash-days value; setting trash-days
// to 0 disables retention entirely (current default for installs
// shipped before SLICE 3 — those installs need the one-time
// `juicefs config <metaURL> --trash-days 7` documented in
// INSTALL-TrueNAS.md).
//
// IMPORTANT for capacity planning: the .trash/ tree counts against
// the JuiceFS volume's used space. A 1 TB volume with --trash-days
// 7 and active churn will see a real chunk of "used" be retained
// trash. The Trash tab makes this LOUD in its header.

// trashDir is the leaf directory name where JuiceFS keeps deleted
// items. Constant rather than configurable — juicefs hardcodes the
// name and there is no upstream knob to rename it.
const trashDir = ".trash"

// trashListDefaultLimit and trashListMaxLimit bound /api/trash/list.
// A volume with 100k+ deletions over a long retention window would
// otherwise OOM the response. Default 100 matches the page-size
// the static UI uses; max 1000 caps power-user pagination requests.
const (
	trashListDefaultLimit = 100
	trashListMaxLimit     = 1000
)

// trashConfigRetentionDays is the value SLICE 3 flips the docker-
// compose juicefs-init to. Surfaced here as a constant so the
// /api/trash/config GET handler can advertise the recommended value
// alongside the current one — the UI's retention drop-down marks
// this entry "(recommended)".
const trashConfigRetentionDays = 7

// TrashEntry is one item under the JuiceFS .trash/ tree. Returned
// from /api/trash/list. Path is the FUSE-mount-relative path the
// other handlers (restore/delete) use as the canonical identifier;
// OriginalPath is the user-facing path the file lived at before
// deletion (best-effort reconstruction from the trash filename
// encoding); DeletedAt is the unix-ms timestamp inferred from the
// bucket directory name; Size is the trash file's current size in
// bytes (which can be 0 for tombstones of zero-byte files).
type TrashEntry struct {
	Path         string `json:"path"`
	OriginalPath string `json:"original_path"`
	DeletedAt    int64  `json:"deleted_at"`
	Size         int64  `json:"size"`
}

// trashListResponse is the JSON body of /api/trash/list. Total is
// the count of all entries discovered (post-filter, pre-pagination)
// so the UI can show "X of N" and decide whether a "load more"
// button is needed.
type trashListResponse struct {
	Entries []TrashEntry `json:"entries"`
	Total   int          `json:"total"`
	Offset  int          `json:"offset"`
	Limit   int          `json:"limit"`
	// Truncated is true when the scan stopped early (e.g. exceeded
	// trashScanMaxEntries). When set, Total is a lower bound, not an
	// exact count, and the UI prefixes it with "≥".
	Truncated bool `json:"truncated,omitempty"`
}

// trashScanMaxEntries caps how many entries the listTrash walker
// will collect before giving up. 50k is well above any realistic
// retention window for a creator-scale install (handfuls of GB of
// edits per week × 7-day retention) and well under what would OOM
// the response JSON. Matches the "bounded so a 100k-entry trash
// doesn't OOM" requirement.
const trashScanMaxEntries = 50000

// trashBucketRE matches the date-hour bucket dirnames JuiceFS uses
// under .trash/. Example: "2026-05-26-14". We use this both to
// classify entries (skip stray non-bucket dirs) and to parse the
// DeletedAt timestamp.
var trashBucketRE = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})-(\d{2})$`)

// listTrash walks the .trash/ tree under fuseMount and returns every
// entry it finds (up to trashScanMaxEntries), newest first.
//
// Implementation notes:
//   - JuiceFS lays out trash as hourly buckets. We sort the bucket
//     dirs descending so the newest entries come first without a
//     full sort of every leaf — the page-1 response stays cheap.
//   - We tolerate (skip) leaf names that don't fit the inode-encoded
//     scheme; juicefs has evolved this twice in the 1.x line and a
//     mis-parse here shouldn't break the list.
//   - We log + skip any subdir we cannot read. A single permission-
//     denied entry shouldn't blank the whole list.
func listTrash(ctx context.Context, fuseMount string) ([]TrashEntry, bool, error) {
	if fuseMount == "" {
		return nil, false, errors.New("trash requires the FUSE mount (embedded mode)")
	}
	root := filepath.Join(fuseMount, trashDir)
	info, err := os.Stat(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// No .trash/ yet — JuiceFS lazily creates it on first
			// deletion under a non-zero retention. An empty list is
			// the correct answer, not a 5xx.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("stat trash root: %w", err)
	}
	if !info.IsDir() {
		return nil, false, fmt.Errorf("trash path is not a directory: %s", root)
	}
	buckets, err := os.ReadDir(root)
	if err != nil {
		return nil, false, fmt.Errorf("read trash root: %w", err)
	}
	// Filter + sort buckets newest first by name. The naming scheme
	// is yyyy-mm-dd-hh which sorts lexicographically the same as
	// chronologically, so plain string sort works.
	type bucketDir struct {
		name      string
		deletedAt int64
	}
	bds := make([]bucketDir, 0, len(buckets))
	for _, b := range buckets {
		if !b.IsDir() {
			continue
		}
		ts, ok := parseTrashBucketTime(b.Name())
		if !ok {
			continue
		}
		bds = append(bds, bucketDir{name: b.Name(), deletedAt: ts})
	}
	sort.Slice(bds, func(i, j int) bool { return bds[i].name > bds[j].name })

	out := make([]TrashEntry, 0, 256)
	truncated := false
	for _, b := range bds {
		if ctx.Err() != nil {
			return out, truncated, ctx.Err()
		}
		if len(out) >= trashScanMaxEntries {
			truncated = true
			break
		}
		bucketPath := filepath.Join(root, b.name)
		entries, err := os.ReadDir(bucketPath)
		if err != nil {
			log.Printf("manager: trash list: skip bucket %q: %v", b.name, err)
			continue
		}
		// Within a bucket the leaf order is arbitrary; sort by name
		// descending so the list is at least deterministic between
		// pages (otherwise an offset+limit pagination could skip or
		// double-count entries).
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() > entries[j].Name() })
		for _, e := range entries {
			if len(out) >= trashScanMaxEntries {
				truncated = true
				break
			}
			full := filepath.Join(bucketPath, e.Name())
			info, err := e.Info()
			if err != nil {
				continue
			}
			out = append(out, TrashEntry{
				Path:         full,
				OriginalPath: decodeTrashOriginalPath(e.Name()),
				DeletedAt:    b.deletedAt,
				Size:         info.Size(),
			})
		}
	}
	return out, truncated, nil
}

// parseTrashBucketTime extracts a unix-ms timestamp from a JuiceFS
// trash bucket directory name like "2026-05-26-14". The hour is
// interpreted as UTC because juicefs's mount writes the buckets in
// the container's local TZ which the container image pins to UTC.
// Returns (0, false) on any parse failure so the caller can skip
// non-bucket entries.
func parseTrashBucketTime(name string) (int64, bool) {
	m := trashBucketRE.FindStringSubmatch(name)
	if m == nil {
		return 0, false
	}
	year, _ := strconv.Atoi(m[1])
	month, _ := strconv.Atoi(m[2])
	day, _ := strconv.Atoi(m[3])
	hour, _ := strconv.Atoi(m[4])
	t := time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC)
	return t.UnixMilli(), true
}

// decodeTrashOriginalPath best-effort reconstructs a user-facing path
// from a juicefs .trash/ leaf filename. JuiceFS encodes the original
// path with slashes replaced by '|' and prefixes the inode number
// followed by a '-' (e.g. "12345-Film Projects|Edit 1|clip.mp4").
// Older versions (1.0.x) prefixed with the inode followed by '-'
// only; newer versions wrap with '%' around the inode. We tolerate
// both. Unrecoverable encodings fall back to the raw filename.
func decodeTrashOriginalPath(leaf string) string {
	s := leaf
	// Strip inode prefix forms: "<digits>-" and "%<digits>%-".
	if i := strings.IndexByte(s, '-'); i > 0 {
		head := s[:i]
		head = strings.Trim(head, "%")
		if _, err := strconv.ParseUint(head, 10, 64); err == nil {
			s = s[i+1:]
		}
	}
	// Restore '/' from '|' separators. juicefs uses '|' because '/'
	// can't appear in a single dirent name. Filenames containing a
	// literal '|' are vanishingly rare in creator workflows; we
	// accept the false-positive here (the original-path display is
	// best-effort, the real identity is .Path).
	s = strings.ReplaceAll(s, "|", "/")
	if !strings.HasPrefix(s, "/") {
		s = "/" + s
	}
	return s
}

// restoreTrash moves a trash entry back into the JuiceFS tree at
// targetPath. Collision handling: if targetPath already exists, the
// basename is suffixed with " (restored YYYY-MM-DD HH-MM-SS)" so the
// restore never clobbers a file the user created in the meantime.
//
// targetPath is the user-facing path inside the JuiceFS volume (e.g.
// "/jfs/Film Projects/clip.mp4"). The caller (the HTTP handler)
// rewrites this to the FUSE-mount form before calling.
//
// The function returns the final on-disk path the entry landed at,
// so the audit log + UI can show the user where it actually went
// when collision-rename triggered.
func restoreTrash(entryPath, targetPath string) (string, error) {
	if entryPath == "" || targetPath == "" {
		return "", errors.New("entry path and target path are required")
	}
	// Ensure the parent directory exists. juicefs sync recreates
	// missing dirs as part of restore; os.Rename is more strict.
	parent := filepath.Dir(targetPath)
	if parent != "" {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return "", fmt.Errorf("mkdir parent: %w", err)
		}
	}
	final := targetPath
	if _, err := os.Lstat(targetPath); err == nil {
		// Collision — pick a non-existing renamed target. Retry up to
		// 5 times with increasing second-precision offsets so two
		// rapid restores in the same wall-clock second can't silently
		// clobber each other (POSIX rename is destructive). After 5
		// attempts give up and return an error rather than overwrite.
		now := time.Now()
		picked := ""
		for attempt := 0; attempt < 5; attempt++ {
			candidate := collisionRenameTarget(targetPath, now.Add(time.Duration(attempt)*time.Second))
			if _, statErr := os.Lstat(candidate); errors.Is(statErr, fs.ErrNotExist) {
				picked = candidate
				break
			} else if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
				return "", fmt.Errorf("stat candidate: %w", statErr)
			}
		}
		if picked == "" {
			return "", fmt.Errorf("restore: %d candidate names all exist; refusing to overwrite", 5)
		}
		final = picked
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("stat target: %w", err)
	}
	if err := os.Rename(entryPath, final); err != nil {
		return "", fmt.Errorf("rename: %w", err)
	}
	return final, nil
}

// collisionRenameTarget appends " (restored YYYY-MM-DD HH-MM-SS)" to
// the basename of `target` (before the extension, if any). Exposed
// (lowercased) for the SLICE-3 unit test.
//
// We use HH-MM-SS rather than HH:MM:SS because filesystems are
// inconsistent about colons in filenames — APFS/HFS+ disallow them,
// the JuiceFS FUSE mount allows them but Mac clients viewing the
// volume over WebDAV/NFS get confused. Hyphens are universally safe.
func collisionRenameTarget(target string, now time.Time) string {
	ext := filepath.Ext(target)
	base := strings.TrimSuffix(target, ext)
	stamp := now.Format("2006-01-02 15-04-05")
	return fmt.Sprintf("%s (restored %s)%s", base, stamp, ext)
}

// deleteTrash permanently removes one trash entry. JuiceFS does NOT
// have an undelete-from-trash escape hatch — once we os.Remove the
// .trash/ leaf, the underlying chunks become eligible for GC at the
// next compaction. Caller must be sure (typed-confirm gate lives at
// the HTTP layer for Empty Trash; per-row delete is a single click
// that the UI confirms with a plain prompt).
func deleteTrash(entryPath string) error {
	if entryPath == "" {
		return errors.New("entry path is required")
	}
	info, err := os.Lstat(entryPath)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if info.IsDir() {
		return os.RemoveAll(entryPath)
	}
	return os.Remove(entryPath)
}

// emptyTrash purges every entry under <fuseMount>/.trash/. Returns
// the count of removed entries and the total bytes reclaimed. The
// caller's typed-confirm gate (X-Confirm-Empty: yes) MUST be checked
// before this is invoked — there is no second-chance prompt here.
//
// We walk the tree ourselves rather than os.RemoveAll(<root>) so the
// return values are accurate (RemoveAll would also remove the .trash
// dir itself, which juicefs re-creates on the next deletion anyway —
// removing it doesn't break anything but the count would be off by
// one).
func emptyTrash(ctx context.Context, fuseMount string) (int, int64, error) {
	if fuseMount == "" {
		return 0, 0, errors.New("trash requires the FUSE mount (embedded mode)")
	}
	root := filepath.Join(fuseMount, trashDir)
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("stat trash root: %w", err)
	}
	buckets, err := os.ReadDir(root)
	if err != nil {
		return 0, 0, fmt.Errorf("read trash root: %w", err)
	}
	count := 0
	var bytes int64
	for _, b := range buckets {
		if ctx.Err() != nil {
			return count, bytes, ctx.Err()
		}
		if !b.IsDir() {
			continue
		}
		bucketPath := filepath.Join(root, b.Name())
		entries, err := os.ReadDir(bucketPath)
		if err != nil {
			log.Printf("manager: empty trash: skip bucket %q: %v", b.Name(), err)
			continue
		}
		for _, e := range entries {
			full := filepath.Join(bucketPath, e.Name())
			info, ierr := e.Info()
			if ierr == nil {
				bytes += info.Size()
			}
			if err := os.RemoveAll(full); err != nil {
				log.Printf("manager: empty trash: remove %q failed: %v", full, err)
				continue
			}
			count++
		}
		// Remove the now-empty bucket dir. Best-effort — if a stray
		// entry survived the per-leaf removal, the bucket dir
		// removal will fail and we just leave it for the next pass.
		_ = os.Remove(bucketPath)
	}
	return count, bytes, nil
}

// getTrashConfig runs `juicefs config <metaURL>` and parses the
// trash-days value out of the output. juicefs config without a flag
// prints the current volume settings in a human-friendly two-column
// table; we scan for the trash-days row.
//
// Returns days=-1 when juicefs config doesn't expose the field (very
// old 1.0 builds). Caller treats -1 as "unknown — surface the
// recommended value as a hint instead".
func getTrashConfig(ctx context.Context, juicefsBin, metaURL string) (int, error) {
	if metaURL == "" {
		return 0, errors.New("metaURL not configured")
	}
	bin := juicefsBin
	if bin == "" {
		bin = "juicefs"
	}
	cmd := exec.CommandContext(ctx, bin, "config", metaURL)
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("juicefs config: %v", trimErr(err))
	}
	return parseTrashConfig(out)
}

// parseTrashConfig extracts the trash-days value from the output of
// `juicefs config <metaURL>`. Exposed (lowercased) for the SLICE-3
// unit test. juicefs's output format has shifted across the 1.x
// line; we accept the two known shapes:
//
//   - 1.3.x: "  TrashDays: 7" or "  trash-days: 7"
//   - 1.0.x: "TrashDays               7" (column-aligned)
//
// Returns (days, nil) on success, (-1, nil) when no trash-days row
// was found (juicefs build without retention support), (0, err) on
// any other parse failure.
func parseTrashConfig(raw []byte) (int, error) {
	lines := strings.Split(string(raw), "\n")
	for _, line := range lines {
		ll := strings.ToLower(strings.TrimSpace(line))
		// Match either "trashdays" or "trash-days" prefix. juicefs
		// has used both — the YAML shape uses snake_case, the CLI
		// status output uses PascalCase.
		if !strings.HasPrefix(ll, "trashdays") && !strings.HasPrefix(ll, "trash-days") && !strings.HasPrefix(ll, "trash_days") {
			continue
		}
		// Strip the leading key, any colon, and whitespace.
		rest := strings.TrimSpace(line)
		// Drop everything before the first colon OR run of whitespace.
		if i := strings.IndexByte(rest, ':'); i >= 0 {
			rest = strings.TrimSpace(rest[i+1:])
		} else {
			fields := strings.Fields(rest)
			if len(fields) >= 2 {
				rest = fields[1]
			}
		}
		// rest now should be the digit string ("7"). Tolerate a
		// trailing unit if juicefs ever decides to add one.
		rest = strings.Fields(rest)[0]
		n, err := strconv.Atoi(rest)
		if err != nil {
			return 0, fmt.Errorf("parse trash-days value %q: %w", rest, err)
		}
		return n, nil
	}
	return -1, nil
}

// setTrashConfig calls `juicefs config <metaURL> --trash-days N`.
// Allowed values are 0 (disabled) and any positive integer up to a
// reasonable cap (we enforce 365 — five years of retention is more
// than enough for any creator workflow and prevents accidental
// fat-finger of "3650" that would silently retain everything for a
// decade and quietly fill the volume).
func setTrashConfig(ctx context.Context, juicefsBin, metaURL string, days int) error {
	if metaURL == "" {
		return errors.New("metaURL not configured")
	}
	if days < 0 || days > 365 {
		return fmt.Errorf("trash-days must be between 0 and 365, got %d", days)
	}
	bin := juicefsBin
	if bin == "" {
		bin = "juicefs"
	}
	cmd := exec.CommandContext(ctx, bin, "config", metaURL, "--trash-days", strconv.Itoa(days))
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Trim and length-cap the upstream message so we don't echo
		// a multi-line juicefs error including the resolved metaURL
		// (which may carry creds in the password field).
		msg := strings.TrimSpace(string(out))
		if i := strings.IndexByte(msg, '\n'); i >= 0 {
			msg = msg[:i]
		}
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("juicefs config --trash-days %d: %s", days, msg)
	}
	return nil
}

// ─── HTTP handlers ─────────────────────────────────────────────────

// trashConfigResponse is the JSON body of /api/trash/config GET. Days
// is the current retention setting (-1 means juicefs didn't expose
// it); Recommended is trashConfigRetentionDays so the UI can mark
// the matching drop-down entry.
type trashConfigResponse struct {
	Days        int   `json:"days"`
	Recommended int   `json:"recommended"`
	Choices     []int `json:"choices"`
	Error       string `json:"error,omitempty"`
}

// trashConfigUpdateRequest is the body of PUT /api/trash/config.
type trashConfigUpdateRequest struct {
	Days int `json:"days"`
}

// trashRestoreRequest is the body of POST /api/trash/restore. Both
// Path (the .trash/<bucket>/<leaf> identifier the listTrash response
// surfaces) and TargetPath (where to put the file — defaults to the
// decoded OriginalPath when empty) are accepted.
type trashRestoreRequest struct {
	Path       string `json:"path"`
	TargetPath string `json:"target_path,omitempty"`
}

// trashRestoreResponse echoes back the final on-disk path the entry
// landed at, so the UI can render the rename-on-collision suffix
// without a second round-trip.
type trashRestoreResponse struct {
	RestoredAt string `json:"restored_at"`
}

// trashDeleteRequest is the body of POST /api/trash/delete (per-row
// permanent delete). The X-Confirm-Empty header is NOT required
// here — Empty Trash is the dangerous bulk operation; a single-row
// delete is closer to "I picked the wrong file to restore".
type trashDeleteRequest struct {
	Path string `json:"path"`
}

// trashEmptyResponse summarizes the result of POST /api/trash/empty.
type trashEmptyResponse struct {
	Count int   `json:"count"`
	Bytes int64 `json:"bytes"`
}

// handleTrashList is GET /api/trash/list?offset=&limit=. Always 200
// with a JSON body (an unconfigured FUSE mount returns an empty list
// plus an explanatory note in entries[0].original_path? — no, we
// return 501 in that case; the UI surfaces a clearer "standalone
// mode" message).
func (a *API) handleTrashList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if a.fuseMount == "" {
		http.Error(w, "trash requires embedded mode (FUSE mount)", http.StatusNotImplemented)
		return
	}
	offset, limit := parseTrashPagination(r.URL.Query().Get("offset"), r.URL.Query().Get("limit"))
	all, truncated, err := listTrash(r.Context(), a.fuseMount)
	if err != nil {
		log.Printf("manager: trash list failed: %v", err)
		http.Error(w, "list trash failed", http.StatusInternalServerError)
		return
	}
	page := paginateTrash(all, offset, limit)
	// Rewrite paths from the FUSE-mount form back to a user-facing
	// /jfs prefix so the UI never sees the internal mount path. The
	// .Path is still usable as an opaque identifier for subsequent
	// restore/delete calls — the handlers reverse the rewrite.
	for i := range page {
		page[i].Path = fuseToUserPath(page[i].Path, a.fuseMount, a.destMount)
	}
	writeJSON(w, http.StatusOK, trashListResponse{
		Entries:   page,
		Total:     len(all),
		Offset:    offset,
		Limit:     limit,
		Truncated: truncated,
	})
}

// parseTrashPagination clamps offset/limit to safe ranges. Exposed
// (lowercased) for the SLICE-3 pagination unit test. Defaults:
// offset=0, limit=trashListDefaultLimit. Caps: limit ≤ trashListMaxLimit.
// Negative values fold to zero / default.
func parseTrashPagination(offsetStr, limitStr string) (offset, limit int) {
	offset = 0
	limit = trashListDefaultLimit
	if offsetStr != "" {
		if n, err := strconv.Atoi(offsetStr); err == nil && n > 0 {
			offset = n
		}
	}
	if limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil {
			if n < 0 {
				limit = trashListDefaultLimit
			} else if n == 0 {
				limit = trashListDefaultLimit
			} else if n > trashListMaxLimit {
				limit = trashListMaxLimit
			} else {
				limit = n
			}
		}
	}
	return offset, limit
}

// paginateTrash slices `all` to [offset, offset+limit). Exposed
// (lowercased) for the pagination unit test. Returns nil rather than
// a zero-length slice when offset is past the end so the JSON body
// emits `"entries":null` and the UI doesn't show a confusing empty
// list under a non-empty total.
func paginateTrash(all []TrashEntry, offset, limit int) []TrashEntry {
	if offset >= len(all) {
		return []TrashEntry{}
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	out := make([]TrashEntry, end-offset)
	copy(out, all[offset:end])
	return out
}

// fuseToUserPath rewrites a FUSE-mount-rooted path back to the user-
// facing destMount-prefixed form. Mirrors the reverse rewrite the
// browse handler does for /jfs/... paths.
func fuseToUserPath(p, fuseMount, destMount string) string {
	if fuseMount == "" || destMount == "" {
		return p
	}
	fuse := strings.TrimSuffix(fuseMount, "/")
	// Strict prefix check — must be exactly fuse OR fuse + "/". Bare
	// HasPrefix would match a sibling like "/jfs2" when fuse is "/jfs"
	// and rewrite it incorrectly.
	if p != fuse && !strings.HasPrefix(p, fuse+"/") {
		return p
	}
	tail := strings.TrimPrefix(p, fuse)
	if tail == "" {
		return destMount
	}
	return strings.TrimSuffix(destMount, "/") + tail
}

// userToFusePath is the reverse rewrite. Used by the restore/delete
// handlers to translate the path the UI hands back into the .trash/
// leaf the on-disk operation needs.
func userToFusePath(p, fuseMount, destMount string) string {
	if fuseMount == "" || destMount == "" {
		return p
	}
	dm := strings.TrimSuffix(destMount, "/")
	// Strict prefix check — must be exactly dm OR dm + "/". Bare
	// HasPrefix would match a sibling like "/jfsxx" when dm is "/jfs".
	if p != dm && !strings.HasPrefix(p, dm+"/") {
		return p
	}
	tail := strings.TrimPrefix(p, dm)
	if tail == "" {
		return strings.TrimSuffix(fuseMount, "/")
	}
	return strings.TrimSuffix(fuseMount, "/") + tail
}

// handleTrashRestore is POST /api/trash/restore. Body shape:
// {"path": "<the listTrash entry's .path>", "target_path": "<optional override>"}.
// On success returns 200 with the final restored-to path so the UI
// can show "Restored to <path>" without a follow-up read.
func (a *API) handleTrashRestore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if a.fuseMount == "" {
		http.Error(w, "trash requires embedded mode (FUSE mount)", http.StatusNotImplemented)
		return
	}
	var req trashRestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Path == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	// Translate the user-facing identifier into the FUSE-mount form
	// AND verify it points inside the .trash subtree — without this
	// gate a malicious caller could POST {"path":"/jfs/important.mp4"}
	// and rename a live file via the restore handler.
	entryPath := userToFusePath(req.Path, a.fuseMount, a.destMount)
	if !isInsideTrash(entryPath, a.fuseMount) {
		http.Error(w, "path is not under the trash subtree", http.StatusBadRequest)
		return
	}
	// Default target = the decoded original path under the volume,
	// rewritten back to the FUSE-mount form for the actual rename.
	target := req.TargetPath
	if target == "" {
		// Pull the .trash/<bucket>/<leaf> leafname and decode.
		leaf := filepath.Base(entryPath)
		orig := decodeTrashOriginalPath(leaf)
		// orig starts with "/" but is relative to the JuiceFS volume
		// root, NOT to /jfs. Prepend the destMount so it matches the
		// user-facing path the UI showed.
		target = strings.TrimSuffix(a.destMount, "/") + orig
	}
	fuseTarget := userToFusePath(target, a.fuseMount, a.destMount)
	// Refuse to restore over the .trash subtree itself.
	if isInsideTrash(fuseTarget, a.fuseMount) {
		http.Error(w, "target cannot be inside .trash/", http.StatusBadRequest)
		return
	}
	final, err := restoreTrash(entryPath, fuseTarget)
	if err != nil {
		log.Printf("manager: trash restore failed: %v", err)
		http.Error(w, "restore failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, trashRestoreResponse{
		RestoredAt: fuseToUserPath(final, a.fuseMount, a.destMount),
	})
}

// handleTrashDelete is POST /api/trash/delete. Permanently removes a
// single trash entry. The X-Confirm-Empty header is NOT required —
// that's reserved for the bulk Empty operation; per-row delete is a
// focused click that the UI confirms inline.
func (a *API) handleTrashDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if a.fuseMount == "" {
		http.Error(w, "trash requires embedded mode (FUSE mount)", http.StatusNotImplemented)
		return
	}
	var req trashDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Path == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	entryPath := userToFusePath(req.Path, a.fuseMount, a.destMount)
	if !isInsideTrash(entryPath, a.fuseMount) {
		http.Error(w, "path is not under the trash subtree", http.StatusBadRequest)
		return
	}
	if err := deleteTrash(entryPath); err != nil {
		log.Printf("manager: trash delete failed: %v", err)
		http.Error(w, "delete failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleTrashEmpty is POST /api/trash/empty. Purges every entry.
// Requires the X-Confirm-Empty: yes header — server-side gate, NOT
// just a UI confirm — so a typo'd curl can't accidentally wipe a
// week of retention.
func (a *API) handleTrashEmpty(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if a.fuseMount == "" {
		http.Error(w, "trash requires embedded mode (FUSE mount)", http.StatusNotImplemented)
		return
	}
	if strings.ToLower(r.Header.Get("X-Confirm-Empty")) != "yes" {
		http.Error(w, "missing X-Confirm-Empty: yes header (typed confirmation required)", http.StatusPreconditionRequired)
		return
	}
	count, bytes, err := emptyTrash(r.Context(), a.fuseMount)
	if err != nil {
		log.Printf("manager: empty trash failed: %v", err)
		http.Error(w, "empty trash failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, trashEmptyResponse{Count: count, Bytes: bytes})
}

// handleTrashConfig is GET/PUT /api/trash/config. GET returns the
// current retention setting; PUT updates it via `juicefs config
// --trash-days`. Always 200 from GET — a juicefs-config failure
// surfaces in .error so the UI shows an actionable message rather
// than a blank knob.
func (a *API) handleTrashConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// Pick the metaURL the overview slice plumbs through — same
		// source so the manager doesn't grow a second metaURL config
		// surface for SLICE 3. Fall back to (no probe) when neither
		// is set; the UI then surfaces the configured-elsewhere hint.
		metaURL := ""
		if a.overview != nil {
			metaURL = a.overview.metaURL
		}
		resp := trashConfigResponse{
			Days:        -1,
			Recommended: trashConfigRetentionDays,
			Choices:     []int{0, 1, 7, 30, 90, 365},
		}
		if metaURL == "" {
			resp.Error = "metaURL not configured — retention can be queried but not changed from this UI"
			writeJSON(w, http.StatusOK, resp)
			return
		}
		days, err := getTrashConfig(r.Context(), a.jobs.juicefsBin, metaURL)
		if err != nil {
			resp.Error = err.Error()
			writeJSON(w, http.StatusOK, resp)
			return
		}
		resp.Days = days
		writeJSON(w, http.StatusOK, resp)
	case http.MethodPut:
		var req trashConfigUpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		metaURL := ""
		if a.overview != nil {
			metaURL = a.overview.metaURL
		}
		if metaURL == "" {
			http.Error(w, "metaURL not configured — cannot set trash-days from this UI", http.StatusBadRequest)
			return
		}
		if err := setTrashConfig(r.Context(), a.jobs.juicefsBin, metaURL, req.Days); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"days": req.Days})
	default:
		http.Error(w, "GET or PUT", http.StatusMethodNotAllowed)
	}
}

// isInsideTrash returns true iff `p` is under <fuseMount>/.trash/ —
// the gate that keeps the restore/delete handlers from being used as
// rename/delete primitives against live files.
func isInsideTrash(p, fuseMount string) bool {
	if fuseMount == "" {
		return false
	}
	trashRoot := filepath.Join(strings.TrimSuffix(fuseMount, "/"), trashDir)
	cleaned := filepath.Clean(p)
	return cleaned == trashRoot || strings.HasPrefix(cleaned, trashRoot+string(filepath.Separator))
}
