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

	maxHintBytes    = 4096 // the catalog's label bound; longer paths are not hinted
	maxAttempts     = 3    // plan/apply rounds before a conflict counts as a failure
	pathCacheMax    = 1 << 16
	defaultRetryMax = time.Minute
	logEvery        = time.Minute
)

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
// (jfsmeta.FS in production).
type Volume interface {
	VolumeUUID() string
	LookupIno(p string) (jfsmeta.Node, error)
	StatIno(ino uint64) (jfsmeta.Node, error)
	GetPaths(ino uint64) ([]string, error)
	Close() error
}

// Config wires a Binder. Store and Connect are required.
type Config struct {
	Store *catalog.Store
	// Connect opens the volume. Run calls it, with backoff, until it
	// succeeds; events queue (or drop) meanwhile.
	Connect   func() (Volume, error)
	QueueSize int
	RetryMax  time.Duration // connect backoff cap (default 1 min)
	Logf      func(format string, args ...any)
	Now       func() time.Time
}

type volume struct {
	Volume
	uuid   string
	source string
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

	// Worker-only: recently observed path -> inode, to notice a file that
	// a rename (or a create) replaced, which publishes no delete event.
	paths map[string]uint64

	received, handled, dropped, applied, ignored, failed, unresolved, resubscribes atomic.Uint64

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
		paths:   map[string]uint64{},
		lastLog: map[string]time.Time{},
	}, nil
}

// Ready is closed once the change-event subscription is acknowledged.
func (b *Binder) Ready() <-chan struct{} { return b.ready }

// Offer queues one event without blocking. A full queue (or a disabled
// binder) drops it and counts the drop: the catalog then misses that change
// until a rescan, which this slice does not have.
func (b *Binder) Offer(ev Event) bool {
	b.received.Add(1)
	if !b.disabled.Load() {
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

// Run connects the volume and applies queued events until ctx ends. A panic
// disables the binder for this process; it never takes the caller down.
func (b *Binder) Run(ctx context.Context) {
	defer func() {
		if p := recover(); p != nil {
			b.disabled.Store(true)
			b.cfg.Logf("continuity: binder DISABLED until restart after an internal error: %v\n%s", p, debug.Stack())
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
			v := &volume{Volume: raw, uuid: uuid, source: "jfs:" + uuid}
			if ctx.Err() != nil {
				_ = raw.Close()
				return nil
			}
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

// LocationID names the one location record of an inode of a volume.
func LocationID(volumeUUID string, ino uint64) string {
	return "jfs-" + volumeUUID + "-" + strconv.FormatUint(ino, 10)
}

// Locator is the location's locator for an inode.
func Locator(ino uint64) string { return "ino:" + strconv.FormatUint(ino, 10) }

// operationID names one transition of an inode's records: the kind plus the
// sequences of the state it starts from, so a replay or retry from the same
// state produces the identical operation, and every new state a new ID.
func operationID(v *volume, ino uint64, kind string, locSeq, assetSeq int64) string {
	return fmt.Sprintf("jfs-%s-%d-%s-%d-%d", v.uuid, ino, kind, locSeq, assetSeq)
}

func (b *Binder) handle(ctx context.Context, v *volume, ev Event) {
	defer b.handled.Add(1)
	var err error
	switch ev.Op {
	case "create", "update", "reconcile", "rename":
		p, ok := cleanPath(ev.Path)
		if ev.IsDir || ev.IsSymlink || !ok || hidden(p) {
			// A directory rename moves its files without an event each;
			// their hints stay stale until their next event (paths are
			// resolved live on read).
			b.ignored.Add(1)
			return
		}
		if ev.Op == "rename" {
			if old, ok := cleanPath(ev.OldPath); ok {
				delete(b.paths, old)
			}
		}
		err = b.observe(ctx, v, p)
	case "delete":
		err = b.unlinked(ctx, v, ev)
	default:
		b.ignored.Add(1)
	}
	if err != nil {
		n := b.failed.Add(1)
		b.logEvery("failed", "continuity: %d change events not recorded in total; latest (%s %q): %v", n, ev.Op, ev.Path, err)
	}
}

// observe binds the regular file now at p.
func (b *Binder) observe(ctx context.Context, v *volume, p string) error {
	n, err := v.LookupIno(p)
	if gone(err) {
		// Changed again since the event: a later event names it.
		b.ignored.Add(1)
		return nil
	}
	if err != nil {
		return err
	}
	if n.Type != smartfolders.TypeRegular {
		b.ignored.Add(1)
		return nil
	}
	if prev, ok := b.paths[p]; ok && prev != n.Ino {
		if err := b.retire(ctx, v, prev, p); err != nil {
			return err
		}
	}
	b.remember(p, n.Ino)
	hint := p
	if n.Nlink > 1 {
		paths, err := v.GetPaths(n.Ino)
		if err != nil && !gone(err) {
			return err
		}
		if c := canonical(paths); c != "" {
			hint = c
		}
	}
	return b.ensure(ctx, v, n.Ino, hint)
}

func (b *Binder) remember(p string, ino uint64) {
	if len(b.paths) >= pathCacheMax {
		b.paths = map[string]uint64{}
	}
	b.paths[p] = ino
}

// unlinked handles a delete event: the inode is the event's, or the one last
// seen at the path.
func (b *Binder) unlinked(ctx context.Context, v *volume, ev Event) error {
	p, ok := cleanPath(ev.Path)
	ino := ev.Inode
	if ino == 0 && ok {
		ino = b.paths[p]
	}
	if ino == 0 {
		b.unresolved.Add(1)
		return nil
	}
	if ok && b.paths[p] == ino {
		delete(b.paths, p)
	}
	return b.retire(ctx, v, ino, p)
}

// retire is called when a bound inode lost the name gonePath. It tombstones
// the location if the file is gone (removed, or only in the trash), else it
// moves the hint off the lost name.
func (b *Binder) retire(ctx context.Context, v *volume, ino uint64, gonePath string) error {
	loc, err := b.cfg.Store.GetLocation(ctx, b.library, LocationID(v.uuid, ino))
	if errors.Is(err, catalog.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if loc.Removed {
		return nil
	}
	n, err := v.StatIno(ino)
	live := err == nil && !n.InTrash
	if err != nil && !gone(err) {
		return err
	}
	var paths []string
	if live && n.Parent == 0 {
		// Hard-linked: live only while a name outside the trash remains.
		if paths, err = v.GetPaths(ino); err != nil && !gone(err) {
			return err
		}
		live = len(paths) > 0
	}
	if !live {
		return b.converge(ctx, func() (*catalog.Operation, error) { return b.planRemove(ctx, v, ino) })
	}
	asset, err := b.cfg.Store.GetAsset(ctx, b.library, loc.Value.AssetID)
	if err != nil || asset.Label != gonePath {
		return err
	}
	if paths == nil {
		if paths, err = v.GetPaths(ino); err != nil && !gone(err) {
			return err
		}
	}
	return b.ensure(ctx, v, ino, canonical(paths))
}

// ensure makes the inode's location active and its asset's hint hint.
func (b *Binder) ensure(ctx context.Context, v *volume, ino uint64, hint string) error {
	return b.converge(ctx, func() (*catalog.Operation, error) { return b.planBind(ctx, v, ino, hint) })
}

// converge plans from the current catalog state and applies, re-planning
// when the state moved under it. A nil plan means nothing to do.
func (b *Binder) converge(ctx context.Context, plan func() (*catalog.Operation, error)) error {
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		var op *catalog.Operation
		if op, err = plan(); err != nil || op == nil {
			return err
		}
		if _, err = b.cfg.Store.Apply(ctx, *op); err == nil {
			b.applied.Add(1)
			return nil
		}
		if !errors.Is(err, catalog.ErrConflict) && !errors.Is(err, catalog.ErrChangedRequest) {
			return err
		}
	}
	return err
}

func (b *Binder) operation(id string, changes ...catalog.Change) *catalog.Operation {
	return &catalog.Operation{Version: catalog.OperationVersion, LibraryID: b.library, ID: id, Actor: Actor, Changes: changes}
}

func (b *Binder) planBind(ctx context.Context, v *volume, ino uint64, hint string) (*catalog.Operation, error) {
	locID := LocationID(v.uuid, ino)
	value := catalog.LocationValue{AssetID: AssetID(v.uuid, ino), Source: v.source, Locator: Locator(ino)}
	loc, err := b.cfg.Store.GetLocation(ctx, b.library, locID)
	if errors.Is(err, catalog.ErrNotFound) {
		var changes []catalog.Change
		var assetSeq int64
		asset, err := b.cfg.Store.GetAsset(ctx, b.library, value.AssetID)
		switch {
		case errors.Is(err, catalog.ErrNotFound):
			changes = append(changes, catalog.Change{CreateAsset: &catalog.CreateAsset{AssetID: value.AssetID, Label: hint}})
		case err != nil:
			return nil, err
		default: // never left so by this binder: adopt it
			assetSeq = asset.UpdatedSequence
			if asset.Label != hint {
				changes = append(changes, catalog.Change{RenameAsset: &catalog.RenameAsset{AssetID: value.AssetID, ExpectedLabel: asset.Label, Label: hint}})
			}
		}
		changes = append(changes, catalog.Change{PutLocation: &catalog.PutLocation{ID: locID, Value: value}})
		return b.operation(operationID(v, ino, "bind", 0, assetSeq), changes...), nil
	}
	if err != nil {
		return nil, err
	}
	asset, err := b.cfg.Store.GetAsset(ctx, b.library, loc.Value.AssetID)
	if err != nil {
		return nil, err
	}
	var changes []catalog.Change
	kind := "hint"
	if loc.Removed {
		kind = "revive"
		changes = append(changes, catalog.Change{PutLocation: &catalog.PutLocation{
			ID:       locID,
			Expected: &catalog.LocationExpected{Value: loc.Value, UpdatedSequence: loc.UpdatedSequence, Removed: true},
			Value:    loc.Value,
		}})
	}
	if hint != "" && asset.Label != hint {
		changes = append(changes, catalog.Change{RenameAsset: &catalog.RenameAsset{AssetID: asset.ID, ExpectedLabel: asset.Label, Label: hint}})
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return b.operation(operationID(v, ino, kind, loc.UpdatedSequence, asset.UpdatedSequence), changes...), nil
}

func (b *Binder) planRemove(ctx context.Context, v *volume, ino uint64) (*catalog.Operation, error) {
	locID := LocationID(v.uuid, ino)
	loc, err := b.cfg.Store.GetLocation(ctx, b.library, locID)
	if errors.Is(err, catalog.ErrNotFound) {
		return nil, nil
	}
	if err != nil || loc.Removed {
		return nil, err
	}
	return b.operation(operationID(v, ino, "unlink", loc.UpdatedSequence, 0), catalog.Change{RemoveLocation: &catalog.RemoveLocation{
		ID:       locID,
		Expected: &catalog.LocationExpected{Value: loc.Value, UpdatedSequence: loc.UpdatedSequence},
	}}), nil
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

// hidden: a dot component (.trash, .juicemount, .DS_Store, AppleDouble and
// editor temp files) or Finder's custom-icon file. Not catalogued.
func hidden(p string) bool {
	for _, c := range strings.Split(p, "/") {
		if strings.HasPrefix(c, ".") || c == "Icon\r" {
			return true
		}
	}
	return false
}

// canonical picks the hint of a hard-linked file: the bytewise-smallest
// visible path, else the smallest hidden one. It depends only on the set of
// names, never on which event arrived first.
func canonical(paths []string) string {
	best, bestHidden := "", ""
	for _, p := range paths {
		if !validHint(p) {
			continue
		}
		if hidden(p) {
			if bestHidden == "" || p < bestHidden {
				bestHidden = p
			}
		} else if best == "" || p < best {
			best = p
		}
	}
	if best == "" {
		return bestHidden
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
