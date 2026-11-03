package smartfolders

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultMaxPerFolder bounds each top-level folder's recency list.
	DefaultMaxPerFolder = 100
	// DefaultMaxAge drops entries that have not changed for this long.
	DefaultMaxAge = 30 * 24 * time.Hour
	// DefaultMaxFolders bounds the number of top-level folders tracked
	// (and so the number of views).
	DefaultMaxFolders = 256
	// maxTrackedPathBytes bounds one tracked path, which with the two
	// limits above bounds the state: at most MaxFolders x MaxPerFolder
	// entries of at most 1 KiB of path each.
	maxTrackedPathBytes = 1024
	// maxEventBytes bounds one pub/sub payload. Real events are a few
	// hundred bytes; anything far larger is not a metadata event.
	maxEventBytes = 64 << 10
	stateVersion  = 1
)

// Event is the subset of a juicemount:metadata pub/sub message the tracker
// uses. The JSON field names match metadata.MetadataEvent, which the Mac
// clients publish (op is "create", "delete", "rename" or "reconcile"; paths
// are volume-relative without a leading slash).
type Event struct {
	Op        string `json:"op"`
	Path      string `json:"path"`
	OldPath   string `json:"old_path,omitempty"`
	IsDir     bool   `json:"is_dir,omitempty"`
	IsSymlink bool   `json:"is_symlink,omitempty"`
}

// ParseEvent decodes one juicemount:metadata payload.
func ParseEvent(payload []byte) (Event, error) {
	var ev Event
	if len(payload) > maxEventBytes {
		return ev, errors.New("event payload too large")
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return ev, err
	}
	return ev, nil
}

// Entry is one recently changed file.
type Entry struct {
	Path    string    `json:"path"`    // volume-relative, e.g. "Projects/A/clip.mov"
	Changed time.Time `json:"changed"` // when the Manager saw the change
}

// TrackerConfig configures a Tracker. Zero values take the defaults.
type TrackerConfig struct {
	Root         string           // smart-folder root name (default "Recents"); its subtree is ignored
	MaxPerFolder int              // per top-level folder bound (default 100)
	MaxAge       time.Duration    // age cutoff (default 30 days)
	MaxFolders   int              // top-level folders tracked (default 256)
	Now          func() time.Time // clock (default time.Now)
	// FolderOK, when set, is asked before a top-level folder is tracked for
	// the first time: events can come from any client, so only a folder that
	// really exists at the volume root (a cached lookup) gets a list.
	FolderOK func(folder string) bool
}

// Tracker keeps, per top-level folder of the volume, a bounded list of the
// most recently changed regular files. It is safe for concurrent use.
//
// Recency is the time the tracker saw the change event, not the file's
// mtime: copies (Finder, cp -p, rsync) preserve the source mtime, but the
// user still just put the file there.
//
// Pub/sub is fire-and-forget: changes made while the Manager is not
// subscribed are never seen. That is acceptable for a recency view; the
// persisted state keeps what was already known across a restart.
type Tracker struct {
	mu      sync.Mutex
	root    string
	max     int
	maxDirs int
	maxAge  time.Duration
	now     func() time.Time
	ok      func(string) bool
	folders map[string]map[string]time.Time // top-level folder -> path -> changed
	dirty   map[string]struct{}
	gen     uint64 // bumped on every state change (persistence)
}

// NewTracker returns an empty tracker.
func NewTracker(cfg TrackerConfig) (*Tracker, error) {
	if cfg.Root == "" {
		cfg.Root = DefaultRoot
	}
	if err := ValidateRootName(cfg.Root); err != nil {
		return nil, err
	}
	if cfg.MaxPerFolder <= 0 {
		cfg.MaxPerFolder = DefaultMaxPerFolder
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = DefaultMaxAge
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxFolders <= 0 {
		cfg.MaxFolders = DefaultMaxFolders
	}
	return &Tracker{
		root:    cfg.Root,
		max:     cfg.MaxPerFolder,
		maxDirs: cfg.MaxFolders,
		ok:      cfg.FolderOK,
		maxAge:  cfg.MaxAge,
		now:     cfg.Now,
		folders: map[string]map[string]time.Time{},
		dirty:   map[string]struct{}{},
	}, nil
}

// trackable validates p and reports its top-level folder when p is a path
// the tracker may list: inside a top-level folder (not at the volume root),
// with no hidden component (so never under .trash or .juicemount, never an
// AppleDouble or .DS_Store file), and not under the smart-folder root.
func (t *Tracker) trackable(p string) (clean, folder string, ok bool) {
	parts, ok := cleanVolumePath(p)
	if !ok || len(parts) < 2 || parts[0] == t.root || len(p) > maxTrackedPathBytes {
		return "", "", false
	}
	for _, c := range parts {
		if isHiddenComponent(c) {
			return "", "", false
		}
	}
	return strings.Join(parts, "/"), parts[0], true
}

// Apply feeds one change event to the tracker. It reports whether the
// tracked state changed.
func (t *Tracker) Apply(ev Event) bool {
	// Ask about a new destination folder before taking the lock: FolderOK
	// may do I/O.
	allow := true
	switch ev.Op {
	case "create", "update", "reconcile", "rename":
		// The top-level component, even for a bare folder rename.
		if parts, ok := cleanVolumePath(ev.Path); ok && t.ok != nil && !t.hasFolder(parts[0]) {
			allow = t.ok(parts[0])
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	before := t.gen
	switch ev.Op {
	case "create", "update", "reconcile":
		// "reconcile" is the path-only hint a client publishes when a spooled
		// write lands; it may name a directory (no is_dir), which reconcile
		// later drops as "not a regular file".
		if ev.IsDir || ev.IsSymlink {
			return false
		}
		if p, folder, ok := t.trackable(ev.Path); ok {
			t.setLocked(folder, p, t.now(), allow)
		}
	case "delete":
		t.dropTreeLocked(ev.Path)
	case "rename":
		t.renameLocked(ev, allow)
	}
	return t.gen != before
}

func (t *Tracker) hasFolder(folder string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.folders[folder] != nil
}

func (t *Tracker) renameLocked(ev Event, allow bool) {
	oldParts, oldOK := cleanVolumePath(ev.OldPath)
	newParts, newOK := cleanVolumePath(ev.Path)
	if !oldOK {
		// Unknown source: treat the destination as a fresh change.
		if newOK && !ev.IsDir && !ev.IsSymlink {
			if p, folder, ok := t.trackable(ev.Path); ok {
				t.setLocked(folder, p, t.now(), allow)
			}
		}
		return
	}
	oldPath := strings.Join(oldParts, "/")
	newPath := ""
	if newOK {
		newPath = strings.Join(newParts, "/")
	}
	// A directory rename moves every tracked file under it, keeping each
	// file's time. Run it for every rename: older publishers may omit is_dir.
	prefix := oldPath + "/"
	type moved struct {
		path    string
		changed time.Time
	}
	var moves []moved
	for p, at := range t.folders[oldParts[0]] {
		if strings.HasPrefix(p, prefix) {
			t.deleteLocked(oldParts[0], p)
			if newPath != "" {
				moves = append(moves, moved{newPath + "/" + strings.TrimPrefix(p, prefix), at})
			}
		}
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].path < moves[j].path })
	for _, m := range moves {
		if p, folder, ok := t.trackable(m.path); ok {
			t.setLocked(folder, p, m.changed, allow)
		}
	}

	// The renamed file itself keeps its time when it was tracked; a file
	// renamed from an untracked name (an app's hidden temp file saved into
	// place) is a fresh change.
	changed := t.now()
	if files := t.folders[oldParts[0]]; files != nil {
		if at, ok := files[oldPath]; ok {
			changed = at
			t.deleteLocked(oldParts[0], oldPath)
		}
	}
	if newPath == "" || ev.IsDir {
		return
	}
	p, folder, ok := t.trackable(newPath)
	if !ok {
		return
	}
	if ev.IsSymlink {
		// A symlink renamed over a tracked file replaces it.
		t.deleteLocked(folder, p)
		return
	}
	t.setLocked(folder, p, changed, allow)
}

// setLocked records p as changed at `at` and enforces the bounds. A folder
// not tracked yet is only started when allowNew is set and the folder limit
// is not reached.
func (t *Tracker) setLocked(folder, p string, at time.Time, allowNew bool) {
	files := t.folders[folder]
	if files == nil {
		if !allowNew || len(t.folders) >= t.maxDirs {
			return
		}
		files = map[string]time.Time{}
		t.folders[folder] = files
	}
	if old, ok := files[p]; ok && old.Equal(at) {
		return
	}
	files[p] = at
	for len(files) > t.max {
		var oldest string
		var oldestAt time.Time
		for q, qt := range files {
			if oldest == "" || qt.Before(oldestAt) || (qt.Equal(oldestAt) && q > oldest) {
				oldest, oldestAt = q, qt
			}
		}
		delete(files, oldest)
	}
	t.touchLocked(folder)
}

func (t *Tracker) deleteLocked(folder, p string) {
	files := t.folders[folder]
	if _, ok := files[p]; !ok {
		return
	}
	delete(files, p)
	if len(files) == 0 {
		delete(t.folders, folder)
	}
	t.touchLocked(folder)
}

// dropTreeLocked drops p and everything tracked under it. Deletes carry no
// is_dir, so a path is always treated as a possible directory too.
func (t *Tracker) dropTreeLocked(p string) {
	parts, ok := cleanVolumePath(p)
	if !ok {
		return
	}
	clean := strings.Join(parts, "/")
	if len(parts) == 1 {
		// A whole top-level folder went away.
		for q := range t.folders[clean] {
			t.deleteLocked(clean, q)
		}
		return
	}
	prefix := clean + "/"
	for q := range t.folders[parts[0]] {
		if q == clean || strings.HasPrefix(q, prefix) {
			t.deleteLocked(parts[0], q)
		}
	}
}

func (t *Tracker) touchLocked(folder string) {
	t.dirty[folder] = struct{}{}
	t.gen++
}

// Expire drops entries older than the age cutoff and marks their folders
// dirty. Recent never returns expired entries; Expire makes the views
// follow.
func (t *Tracker) Expire() {
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := t.now().Add(-t.maxAge)
	for folder, files := range t.folders {
		for p, at := range files {
			if at.Before(cutoff) {
				t.deleteLocked(folder, p)
			}
		}
	}
}

// Forget drops the given paths (files the reconciler found gone or no
// longer regular). It does not mark folders dirty: the caller is
// reconciling them already.
func (t *Tracker) Forget(paths ...string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range paths {
		parts, ok := cleanVolumePath(p)
		if !ok {
			continue
		}
		files := t.folders[parts[0]]
		clean := strings.Join(parts, "/")
		if _, ok := files[clean]; ok {
			delete(files, clean)
			if len(files) == 0 {
				delete(t.folders, parts[0])
			}
			t.gen++
		}
	}
}

// Recent returns a folder's entries, newest first (ties by path), within the
// age cutoff.
func (t *Tracker) Recent(folder string) []Entry {
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := t.now().Add(-t.maxAge)
	out := make([]Entry, 0, len(t.folders[folder]))
	for p, at := range t.folders[folder] {
		if !at.Before(cutoff) {
			out = append(out, Entry{Path: p, Changed: at})
		}
	}
	sortEntries(out)
	return out
}

func sortEntries(es []Entry) {
	sort.Slice(es, func(i, j int) bool {
		if !es[i].Changed.Equal(es[j].Changed) {
			return es[i].Changed.After(es[j].Changed)
		}
		return es[i].Path < es[j].Path
	})
}

// Folders lists the top-level folders that have entries, sorted.
func (t *Tracker) Folders() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.folders))
	for f := range t.folders {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// TakeDirty returns, sorted, the folders whose list changed since the last
// call, and clears the set.
func (t *Tracker) TakeDirty() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.dirty))
	for f := range t.dirty {
		out = append(out, f)
	}
	t.dirty = map[string]struct{}{}
	sort.Strings(out)
	return out
}

// MarkDirty queues folders for reconciliation (a failed or deferred cycle,
// or a view found on disk at startup).
func (t *Tracker) MarkDirty(folders ...string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, f := range folders {
		t.dirty[f] = struct{}{}
	}
}

// Generation changes whenever the tracked state changes.
func (t *Tracker) Generation() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.gen
}

type persistedState struct {
	Version int     `json:"version"`
	Entries []Entry `json:"entries"`
}

// MarshalState serialises the tracked entries (sorted, so the output is
// stable) and returns the generation they reflect.
func (t *Tracker) MarshalState() ([]byte, uint64, error) {
	t.mu.Lock()
	st := persistedState{Version: stateVersion}
	for _, files := range t.folders {
		for p, at := range files {
			st.Entries = append(st.Entries, Entry{Path: p, Changed: at.UTC()})
		}
	}
	gen := t.gen
	t.mu.Unlock()
	sort.Slice(st.Entries, func(i, j int) bool { return st.Entries[i].Path < st.Entries[j].Path })
	data, err := json.MarshalIndent(st, "", "  ")
	return data, gen, err
}

// LoadState replaces the tracked entries with a MarshalState snapshot. Every
// entry is re-validated (the file is outside the volume but still input),
// times in the future are clamped to now, and the bound and age cutoff are
// applied. Every loaded folder is marked dirty.
func (t *Tracker) LoadState(data []byte) error {
	var st persistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("smart-folder state: %w", err)
	}
	if st.Version != stateVersion {
		return fmt.Errorf("smart-folder state: unsupported version %d", st.Version)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.folders = map[string]map[string]time.Time{}
	now := t.now()
	cutoff := now.Add(-t.maxAge)
	sortEntries(st.Entries) // newest first, so the bound keeps the newest
	for _, e := range st.Entries {
		p, folder, ok := t.trackable(e.Path)
		if !ok || e.Changed.Before(cutoff) {
			continue
		}
		at := e.Changed
		if at.After(now) {
			at = now
		}
		if _, dup := t.folders[folder][p]; dup {
			continue
		}
		if len(t.folders[folder]) >= t.max {
			continue
		}
		t.setLocked(folder, p, at, true)
	}
	return nil
}
