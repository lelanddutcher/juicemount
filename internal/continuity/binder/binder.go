// Package binder binds the regular files of one JuiceFS volume to continuity
// catalog assets. It follows the clients' juicemount:metadata change events;
// each file is identified by its inode, and its path is only a hint. It never
// writes to the volume and never reads file contents. See README.md.
package binder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
)

const (
	// DefaultQueueSize bounds the events waiting for the worker. A full
	// queue drops (and counts) instead of blocking the subscriber.
	DefaultQueueSize = 4096
	// MaxEventBytes bounds one pub/sub payload (real ones are a few hundred bytes).
	MaxEventBytes = 64 << 10
	// Actor attributes the binder's catalog operations. It is not an identity.
	Actor = "manager:volume-binder"
	// MaxLinks: an inode with more names is not enumerated (each name
	// costs whole directory listings); its state stays unknown.
	MaxLinks = 64

	maxHintBytes     = 4096 // longer paths are not catalogued
	maxAttempts      = 3    // plan/apply rounds before a conflict counts as a failure
	defaultPathCache = 1 << 16
	defaultRetryMax  = time.Minute
	logEvery         = time.Minute
)

// eventRetries are the pauses before re-running an event whose volume reads
// failed (state unknown); after the last one it counts as failed.
var eventRetries = []time.Duration{200 * time.Millisecond, time.Second}

// errUnknown marks a volume read that failed: the file's state is unknown,
// so nothing is changed and the event is retried, then counted as failed.
var errUnknown = errors.New("continuity: volume state unknown")

func unknown(err error) error { return fmt.Errorf("%w: %w", errUnknown, err) }

// Event is the part of a juicemount:metadata payload the binder uses. Field
// names match metadata.MetadataEvent (pinned by the contract test).
type Event struct {
	Op        string `json:"op"`
	Path      string `json:"path"`
	OldPath   string `json:"old_path,omitempty"`
	Inode     uint64 `json:"inode,omitempty"`
	IsDir     bool   `json:"is_dir,omitempty"`
	IsSymlink bool   `json:"is_symlink,omitempty"`
}

// ParseEvent decodes one bounded payload.
func ParseEvent(payload []byte) (Event, error) {
	var ev Event
	if len(payload) > MaxEventBytes {
		return ev, errors.New("event payload too large")
	}
	err := json.Unmarshal(payload, &ev)
	return ev, err
}

// Volume is the read-only view of the volume the binder needs
// (jfsmeta.FS opened with OpenReadOnly in production).
type Volume interface {
	VolumeUUID() string
	LookupIno(p string) (jfsmeta.Node, error)
	StatIno(ino uint64) (jfsmeta.Node, error)
	// GetPaths lists the names outside the trash; an incomplete answer
	// is an error, never a short list.
	GetPaths(ino uint64) ([]string, error)
	Close() error
}

// Config wires a Binder. Store and Connect are required.
type Config struct {
	Store *catalog.Store
	// Connect opens the volume. Run calls it, with backoff, until it
	// succeeds; events queue (or drop) meanwhile.
	Connect       func() (Volume, error)
	QueueSize     int
	PathCacheSize int           // recent path -> inode entries (default 65,536)
	RetryMax      time.Duration // connect backoff cap (default 1 min)
	Logf          func(format string, args ...any)
	Now           func() time.Time
}

type volume struct {
	Volume
	uuid string
}

// Binder is safe for concurrent use: Offer from the subscriber, Run on its
// own goroutine, and the read-only queries from HTTP handlers.
type Binder struct {
	cfg     Config
	library continuity.LibraryID
	queue   chan Event
	vol     atomic.Pointer[volume]
	ready   chan struct{} // closed when the first SUBSCRIBE is acknowledged

	readyOnce sync.Once
	disabled  atomic.Bool
	stopped   atomic.Bool

	// Worker-only: recently observed path -> inode, to notice a file that
	// a rename (or a create) replaced, which publishes no delete event.
	paths *pathCache

	received, handled, dropped, discarded, applied, ignored, failed, retried, unresolved, resubscribes atomic.Uint64

	observedSince atomic.Int64 // unix nanoseconds

	logMu   sync.Mutex
	lastLog map[string]time.Time
}

// New builds a Binder over an open catalog.
func New(cfg Config) (*Binder, error) {
	if cfg.Store == nil || cfg.Connect == nil {
		return nil, errors.New("continuity binder needs a catalog and a volume")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.PathCacheSize <= 0 {
		cfg.PathCacheSize = defaultPathCache
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = defaultRetryMax
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	library, err := cfg.Store.LibraryID()
	if err != nil {
		return nil, err
	}
	return &Binder{
		cfg:     cfg,
		library: library,
		queue:   make(chan Event, cfg.QueueSize),
		ready:   make(chan struct{}),
		paths:   newPathCache(cfg.PathCacheSize),
		lastLog: map[string]time.Time{},
	}, nil
}

// Ready is closed once the change-event subscription is acknowledged.
func (b *Binder) Ready() <-chan struct{} { return b.ready }

// Offer queues one event without blocking. A full queue (or a stopped or
// disabled binder) drops it and counts the drop: the catalog then misses
// that change until a rescan, which this slice does not have.
func (b *Binder) Offer(ev Event) bool {
	b.received.Add(1)
	if !b.disabled.Load() && !b.stopped.Load() {
		select {
		case b.queue <- ev:
			return true
		default:
		}
	}
	n := b.dropped.Add(1)
	b.logEvery("drop", "continuity: dropped %d change events in total (queue full or binder stopped); the catalog misses those changes until a rescan", n)
	return false
}

// Run connects the volume and applies queued events until ctx ends; events
// still queued then are discarded (counted). A panic disables the binder
// for this process; it never takes the caller down.
func (b *Binder) Run(ctx context.Context) {
	defer b.drain()
	defer func() {
		if p := recover(); p != nil {
			b.disable("the event worker", p)
		}
	}()
	v := b.connect(ctx)
	if v == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-b.queue:
			b.handle(ctx, v, ev)
		}
	}
}

// drain stops queueing and counts what was still waiting.
func (b *Binder) drain() {
	b.stopped.Store(true)
	for {
		select {
		case <-b.queue:
			b.discarded.Add(1)
		default:
			if n := b.discarded.Load(); n > 0 {
				b.cfg.Logf("continuity: %d queued change events discarded at stop; the catalog misses those changes until a rescan", n)
			}
			return
		}
	}
}

func (b *Binder) disable(where string, p any) {
	b.disabled.Store(true)
	b.cfg.Logf("continuity: binder DISABLED until restart after an internal error in %s: %v\n%s", where, p, debug.Stack())
}

// Close closes the volume connection, if any. Call it after Run returned.
func (b *Binder) Close() error {
	if v := b.vol.Swap(nil); v != nil {
		return v.Close()
	}
	return nil
}

func (b *Binder) connect(ctx context.Context) *volume {
	backoff := time.Second
	for ctx.Err() == nil {
		raw, err := b.cfg.Connect()
		if err == nil {
			uuid := raw.VolumeUUID()
			if !validUUID(uuid) {
				_ = raw.Close()
				b.disabled.Store(true)
				b.cfg.Logf("continuity: binder disabled: the volume UUID %q cannot name catalog records", uuid)
				return nil
			}
			if ctx.Err() != nil {
				_ = raw.Close()
				return nil
			}
			v := &volume{Volume: raw, uuid: uuid}
			b.vol.Store(v)
			b.logEvery("connect", "")
			return v
		}
		b.logEvery("connect", "continuity: connecting to the volume: %v (retrying)", err)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, b.cfg.RetryMax)
	}
	return nil
}

// validUUID keeps the UUID usable inside operation and location IDs, which
// allow ASCII letters, digits, dashes and at most 128 characters (40 leave
// room for the inode, kind and sequences). JuiceFS formats a 36-character UUID.
func validUUID(s string) bool {
	if s == "" || len(s) > 40 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// AssetID is the asset an inode of a volume is bound to. It is derived, not
// random, so replaying a bind recreates the identical operation.
func AssetID(volumeUUID string, ino uint64) continuity.AssetID {
	sum := sha256.Sum256([]byte("juicemount.continuity.jfs-asset\x00" + volumeUUID + "\x00" + strconv.FormatUint(ino, 10)))
	return continuity.AssetID("asset_" + hex.EncodeToString(sum[:16]))
}

// LocationID names the inode location record of an inode of a volume.
func LocationID(volumeUUID string, ino uint64) string {
	return "jfs-" + volumeUUID + "-" + strconv.FormatUint(ino, 10)
}

// HintID names the path-hint location record of an inode of a volume.
func HintID(volumeUUID string, ino uint64) string { return LocationID(volumeUUID, ino) + "-hint" }

// Source is the inode locations' source; HintSource the path hints'.
func Source(volumeUUID string) string     { return "jfs:" + volumeUUID }
func HintSource(volumeUUID string) string { return "jfs-path:" + volumeUUID }

// Locator is the inode location's locator.
func Locator(ino uint64) string { return "ino:" + strconv.FormatUint(ino, 10) }

// operationID names one transition of an inode's records: the kind plus the
// sequences of the two records it starts from (0 when absent), so a replay
// or retry from the same state is the identical request, and every new
// state gets a new ID.
func operationID(v *volume, ino uint64, kind string, inoSeq, hintSeq int64) string {
	return fmt.Sprintf("jfs-%s-%d-%s-%d-%d", v.uuid, ino, kind, inoSeq, hintSeq)
}

func (b *Binder) handle(ctx context.Context, v *volume, ev Event) {
	defer b.handled.Add(1)
	var err error
	for attempt := 0; ; attempt++ {
		if err = b.apply(ctx, v, ev); !errors.Is(err, errUnknown) || attempt == len(eventRetries) {
			break
		}
		b.retried.Add(1)
		select {
		case <-ctx.Done():
			return
		case <-time.After(eventRetries[attempt]):
		}
	}
	if err != nil {
		n := b.failed.Add(1)
		b.logEvery("failed", "continuity: %d change events not recorded in total; latest (%s %q): %v", n, ev.Op, ev.Path, err)
	}
}

func (b *Binder) apply(ctx context.Context, v *volume, ev Event) error {
	switch ev.Op {
	case "create", "update", "reconcile", "rename":
		p, ok := cleanPath(ev.Path)
		if ev.IsDir || ev.IsSymlink || !ok {
			// A directory rename moves its files without an event each;
			// their hints stay stale until their next event.
			b.ignored.Add(1)
			return nil
		}
		if old, ok := cleanPath(ev.OldPath); ok && ev.Op == "rename" {
			b.paths.remove(old)
		}
		return b.observe(ctx, v, p)
	case "delete":
		return b.unlinked(ctx, v, ev)
	default:
		b.ignored.Add(1)
		return nil
	}
}

// observe syncs the regular file now at p: binding it when p is visible,
// and updating (or retiring) an already bound file that moved to a hidden
// name, such as a Finder .Trashes folder.
func (b *Binder) observe(ctx context.Context, v *volume, p string) error {
	n, err := v.LookupIno(p)
	if gone(err) {
		// Changed again since the event: a later event names it.
		b.ignored.Add(1)
		return nil
	}
	if err != nil {
		return unknown(err)
	}
	if n.Type != smartfolders.TypeRegular {
		b.ignored.Add(1)
		return nil
	}
	if hidden(p) {
		found, err := b.sync(ctx, v, n.Ino, "")
		if err == nil && !found {
			b.ignored.Add(1)
		}
		return err
	}
	if prev, ok := b.paths.get(p); ok && prev != n.Ino {
		if _, err := b.sync(ctx, v, prev, ""); err != nil {
			return err
		}
	}
	b.paths.put(p, n.Ino)
	_, err = b.sync(ctx, v, n.Ino, p)
	return err
}

// unlinked handles a delete event. Its inode may be a client's synthetic
// one or stale, so every candidate is synced against the volume: the
// event's inode and the one last seen at the path.
func (b *Binder) unlinked(ctx context.Context, v *volume, ev Event) error {
	p, ok := cleanPath(ev.Path)
	var candidates []uint64
	if ev.Inode != 0 {
		candidates = append(candidates, ev.Inode)
	}
	cached := false
	if ok {
		if ino, hit := b.paths.get(p); hit {
			cached = true
			if ino != ev.Inode {
				candidates = append(candidates, ino)
			}
		}
	}
	found := false
	for _, ino := range candidates {
		f, err := b.sync(ctx, v, ino, "")
		if err != nil {
			return err
		}
		found = found || f
	}
	if cached {
		b.paths.remove(p)
	}
	if !found {
		b.unresolved.Add(1)
	}
	return nil
}

// sync makes an inode's records match the volume. With observed == "" it
// only updates an inode that already has a location (found reports that);
// with observed (a visible path just resolved to it) it may also bind.
func (b *Binder) sync(ctx context.Context, v *volume, ino uint64, observed string) (found bool, err error) {
	inoLoc, hintLoc, err := b.records(ctx, v, ino)
	if err != nil {
		return false, err
	}
	found = inoLoc != nil
	if !found && observed == "" {
		return false, nil
	}
	hint := ""
	if hintLoc != nil && !hintLoc.Removed {
		hint = hintLoc.Value.Locator
	}
	live, name, err := b.where(v, ino, hint, observed)
	if err != nil {
		return found, err
	}
	return found, b.converge(ctx, func() (*catalog.Operation, error) { return b.plan(ctx, v, ino, live, name) })
}

// where reports whether the inode still has a visible name outside the
// trash, and the name to hint: the current hint while it still names the
// inode, else (one link) the observed path, else the bytewise-smallest
// visible name. Hidden-only files are not catalogued. A failed read is
// errUnknown, never "gone".
func (b *Binder) where(v *volume, ino uint64, hint, observed string) (bool, string, error) {
	n, err := v.StatIno(ino)
	if gone(err) || errors.Is(err, syscall.EINVAL) {
		return false, "", nil
	}
	if err != nil {
		return false, "", unknown(err)
	}
	if n.InTrash || n.Type != smartfolders.TypeRegular {
		return false, "", nil
	}
	if hint != "" && !hidden(hint) {
		if ok, err := names(v, hint, ino); err != nil || ok {
			return ok, hint, err
		}
	}
	if n.Nlink == 1 && observed != "" && !hidden(observed) {
		if ok, err := names(v, observed, ino); err != nil || ok {
			return ok, observed, err
		}
	}
	if n.Nlink > MaxLinks {
		return false, "", unknown(fmt.Errorf("inode %d has %d links (over %d)", ino, n.Nlink, MaxLinks))
	}
	paths, err := v.GetPaths(ino)
	if gone(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", unknown(err)
	}
	c := canonical(paths)
	return c != "", c, nil
}

// names reports whether p currently resolves to ino.
func names(v *volume, p string, ino uint64) (bool, error) {
	n, err := v.LookupIno(p)
	if gone(err) {
		return false, nil
	}
	if err != nil {
		return false, unknown(err)
	}
	return n.Ino == ino, nil
}

func (b *Binder) records(ctx context.Context, v *volume, ino uint64) (inoLoc, hintLoc *catalog.Location, err error) {
	get := func(id string) (*catalog.Location, error) {
		l, err := b.cfg.Store.GetLocation(ctx, b.library, id)
		if errors.Is(err, catalog.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &l, nil
	}
	if inoLoc, err = get(LocationID(v.uuid, ino)); err != nil {
		return nil, nil, err
	}
	hintLoc, err = get(HintID(v.uuid, ino))
	return inoLoc, hintLoc, err
}

// converge plans from the current catalog state and applies, re-planning
// when the state moved under it. Any other Apply error may be an uncertain
// commit, so the identical request is sent once more: if it had committed,
// the catalog answers with the original receipt. A nil plan means nothing
// to do.
func (b *Binder) converge(ctx context.Context, plan func() (*catalog.Operation, error)) error {
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		var op *catalog.Operation
		if op, err = plan(); err != nil || op == nil {
			return err
		}
		if _, err = b.cfg.Store.Apply(ctx, *op); err != nil && !conflict(err) {
			_, err = b.cfg.Store.Apply(ctx, *op)
		}
		if err == nil {
			b.applied.Add(1)
			return nil
		}
		if !conflict(err) {
			return err
		}
	}
	return err
}

func conflict(err error) bool {
	return errors.Is(err, catalog.ErrConflict) || errors.Is(err, catalog.ErrChangedRequest)
}

func seq(l *catalog.Location) int64 {
	if l == nil {
		return 0
	}
	return l.UpdatedSequence
}

func expected(l *catalog.Location) *catalog.LocationExpected {
	return &catalog.LocationExpected{Value: l.Value, UpdatedSequence: l.UpdatedSequence, Removed: l.Removed}
}

// plan is the one operation that moves the inode's two records to the
// target: both active, hinting name, when live; both tombstoned when not.
// The asset is created once (labelled with the file name) and never
// renamed: its label belongs to people.
func (b *Binder) plan(ctx context.Context, v *volume, ino uint64, live bool, name string) (*catalog.Operation, error) {
	inoLoc, hintLoc, err := b.records(ctx, v, ino)
	if err != nil {
		return nil, err
	}
	var changes []catalog.Change
	kind := "unlink"
	if !live {
		for _, l := range []*catalog.Location{inoLoc, hintLoc} {
			if l != nil && !l.Removed {
				changes = append(changes, catalog.Change{RemoveLocation: &catalog.RemoveLocation{ID: l.ID, Expected: expected(l)}})
			}
		}
	} else {
		asset := AssetID(v.uuid, ino)
		if inoLoc != nil {
			asset = inoLoc.Value.AssetID
		}
		value := catalog.LocationValue{AssetID: asset, Source: Source(v.uuid), Locator: Locator(ino)}
		hintValue := catalog.LocationValue{AssetID: asset, Source: HintSource(v.uuid), Locator: name}
		switch {
		case inoLoc == nil:
			kind = "bind"
			if _, err := b.cfg.Store.GetAsset(ctx, b.library, asset); errors.Is(err, catalog.ErrNotFound) {
				changes = append(changes, catalog.Change{CreateAsset: &catalog.CreateAsset{AssetID: asset, Label: path.Base(name)}})
			} else if err != nil {
				return nil, err
			}
			changes = append(changes, catalog.Change{PutLocation: &catalog.PutLocation{ID: LocationID(v.uuid, ino), Value: value}})
		case inoLoc.Removed:
			kind = "revive"
			changes = append(changes, catalog.Change{PutLocation: &catalog.PutLocation{ID: inoLoc.ID, Expected: expected(inoLoc), Value: inoLoc.Value}})
		default:
			kind = "hint"
		}
		switch {
		case hintLoc == nil:
			changes = append(changes, catalog.Change{PutLocation: &catalog.PutLocation{ID: HintID(v.uuid, ino), Value: hintValue}})
		case hintLoc.Removed || hintLoc.Value != hintValue:
			changes = append(changes, catalog.Change{PutLocation: &catalog.PutLocation{ID: hintLoc.ID, Expected: expected(hintLoc), Value: hintValue}})
		}
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return &catalog.Operation{
		Version:   catalog.OperationVersion,
		LibraryID: b.library,
		ID:        operationID(v, ino, kind, seq(inoLoc), seq(hintLoc)),
		Actor:     Actor,
		Changes:   changes,
	}, nil
}

// gone: the path or inode no longer resolves (or crosses a symlink, which
// the binder never follows).
func gone(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) || errors.Is(err, smartfolders.ErrSymlinkInPath)
}

// cleanPath validates a volume-relative event path ("A/clip.mov"; one
// leading slash tolerated). Paths that are not canonical, or too long to be
// a hint, are refused rather than repaired: events come from any client.
func cleanPath(p string) (string, bool) {
	p = strings.TrimPrefix(p, "/")
	if !validHint(p) || path.Clean(p) != p {
		return "", false
	}
	for _, c := range strings.Split(p, "/") {
		if c == "" || c == "." || c == ".." {
			return "", false
		}
	}
	return p, true
}

func validHint(p string) bool {
	return p != "" && len(p) <= maxHintBytes && utf8.ValidString(p) && !strings.ContainsRune(p, 0)
}

// hidden: a dot component (.trash, .Trashes, .juicemount, .DS_Store,
// AppleDouble and editor temp files) or Finder's custom-icon file. A file
// with only hidden names is not catalogued.
func hidden(p string) bool {
	for _, c := range strings.Split(p, "/") {
		if strings.HasPrefix(c, ".") || c == "Icon\r" {
			return true
		}
	}
	return false
}

// canonical picks the hint when one must be chosen among several names:
// the bytewise-smallest visible one ("" when none is visible).
func canonical(paths []string) string {
	best := ""
	for _, p := range paths {
		if validHint(p) && !hidden(p) && (best == "" || p < best) {
			best = p
		}
	}
	return best
}

// logEvery logs at most once per minute per key; an empty format resets key.
func (b *Binder) logEvery(key, format string, args ...any) {
	b.logMu.Lock()
	if format == "" {
		delete(b.lastLog, key)
		b.logMu.Unlock()
		return
	}
	now := b.cfg.Now()
	if last, ok := b.lastLog[key]; ok && now.Sub(last) < logEvery {
		b.logMu.Unlock()
		return
	}
	b.lastLog[key] = now
	b.logMu.Unlock()
	b.cfg.Logf(format, args...)
}
