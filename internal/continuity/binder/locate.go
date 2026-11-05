package binder

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// Hint is one bound file as the catalog last recorded it: its asset, its
// inode, and its path hint (stale after a folder rename) with the catalog
// sequence that recorded the hint. ID is the hint's location ID, the order
// Hints and HintsAfter follow.
type Hint struct {
	ID       string
	AssetID  continuity.AssetID
	Ino      uint64
	Path     string
	Sequence int64
}

// Hints calls fn for every active path hint of the connected volume's
// files, in location ID order, until fn returns false. It reads only the
// catalog (one page of catalog.MaxPageSize at a time), never the volume:
// a hint is a candidate path, to be resolved with Locate before use.
func (b *Binder) Hints(ctx context.Context, fn func(Hint) bool) error {
	return b.HintsAfter(ctx, "", fn)
}

// HintsAfter is Hints starting after the hint whose ID is after (""
// starts at the beginning), so a caller can resume a scan where it
// stopped.
func (b *Binder) HintsAfter(ctx context.Context, after string, fn func(Hint) bool) error {
	v := b.vol.Load()
	if v == nil {
		return ErrNotReady
	}
	prefix := "jfs-" + v.uuid + "-"
	for {
		page, err := b.cfg.Store.ListSourceLocations(ctx, b.library, HintSource(v.uuid), after, catalog.MaxPageSize)
		if err != nil {
			return err
		}
		for _, l := range page {
			digits, ok := strings.CutPrefix(l.ID, prefix)
			digits, hint := strings.CutSuffix(digits, "-hint")
			digits, _, _ = strings.Cut(digits, "-") // a later pair's "-g<n>"
			ino, err := strconv.ParseUint(digits, 10, 64)
			if !ok || !hint || err != nil {
				continue
			}
			if !fn(Hint{ID: l.ID, AssetID: l.Value.AssetID, Ino: ino, Path: l.Value.Locator, Sequence: l.UpdatedSequence}) {
				return nil
			}
		}
		if len(page) < catalog.MaxPageSize {
			return nil
		}
		after = page[len(page)-1].ID
	}
}

// AssetHint returns the active path hint of an asset's file on the
// connected volume (catalog reads only). catalog.ErrNotFound when the asset
// does not exist or no file of this volume is bound to it now.
func (b *Binder) AssetHint(ctx context.Context, id continuity.AssetID) (Hint, error) {
	v := b.vol.Load()
	if v == nil {
		return Hint{}, ErrNotReady
	}
	if id.Validate() != nil {
		return Hint{}, fmt.Errorf("%w: asset id", catalog.ErrInvalid)
	}
	if _, err := b.cfg.Store.GetAsset(ctx, b.library, id); err != nil {
		return Hint{}, err
	}
	// The binder records two locations per pair; a page of 100 is ample.
	locs, err := b.cfg.Store.ListLocations(ctx, b.library, id, "", 100)
	if err != nil {
		return Hint{}, err
	}
	var ino uint64
	var hint *catalog.Location
	for i, l := range locs {
		switch l.Value.Source {
		case Source(v.uuid):
			digits, _ := strings.CutPrefix(l.Value.Locator, "ino:")
			if n, err := strconv.ParseUint(digits, 10, 64); err == nil {
				ino = n
			}
		case HintSource(v.uuid):
			hint = &locs[i]
		}
	}
	if ino == 0 || hint == nil {
		return Hint{}, catalog.ErrNotFound
	}
	return Hint{ID: hint.ID, AssetID: id, Ino: ino, Path: hint.Value.Locator, Sequence: hint.UpdatedSequence}, nil
}

// FileStat is a bound file as the volume shows it now: its current visible
// path (volume-relative), modification time and length.
type FileStat struct {
	Path  string
	Mtime time.Time
	Size  uint64
}

// Locate resolves an inode's current visible path, as the binder hints it
// (the hint while it still names the inode, else the bytewise-smallest
// visible name), and the file's modification time, both read from the
// volume now. The path is "" when the file has no visible name outside the
// trash or is no longer a regular file. A failed volume read is
// ErrUnresolved (unknown, not gone); ErrNotReady while the volume is not
// connected.
func (b *Binder) Locate(ctx context.Context, ino uint64, hint string) (string, time.Time, error) {
	st, err := b.Stat(ctx, ino, hint)
	return st.Path, st.Mtime, err
}

// Stat is Locate with the file's length.
func (b *Binder) Stat(ctx context.Context, ino uint64, hint string) (FileStat, error) {
	v := b.vol.Load()
	if v == nil {
		return FileStat{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return FileStat{}, err
	}
	n, err := v.StatIno(ino)
	if gone(err) || errors.Is(err, syscall.EINVAL) {
		return FileStat{}, nil
	}
	if err != nil {
		return FileStat{}, fmt.Errorf("%w: %v", ErrUnresolved, err)
	}
	live, name, err := b.whereNode(v, ino, n, hint, "")
	if err != nil {
		return FileStat{}, fmt.Errorf("%w: %v", ErrUnresolved, err)
	}
	if !live {
		return FileStat{}, nil
	}
	return FileStat{Path: name, Mtime: n.Mtime, Size: n.Size}, nil
}
