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
// sequence that recorded the hint.
type Hint struct {
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
	v := b.vol.Load()
	if v == nil {
		return ErrNotReady
	}
	prefix := "jfs-" + v.uuid + "-"
	for after := ""; ; {
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
			if !fn(Hint{AssetID: l.Value.AssetID, Ino: ino, Path: l.Value.Locator, Sequence: l.UpdatedSequence}) {
				return nil
			}
		}
		if len(page) < catalog.MaxPageSize {
			return nil
		}
		after = page[len(page)-1].ID
	}
}

// Locate resolves an inode's current visible path, as the binder hints it
// (the hint while it still names the inode, else the bytewise-smallest
// visible name), and the file's modification time, both read from the
// volume now. The path is "" when the file has no visible name outside the
// trash or is no longer a regular file. A failed volume read is
// ErrUnresolved (unknown, not gone); ErrNotReady while the volume is not
// connected.
func (b *Binder) Locate(ctx context.Context, ino uint64, hint string) (string, time.Time, error) {
	v := b.vol.Load()
	if v == nil {
		return "", time.Time{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return "", time.Time{}, err
	}
	n, err := v.StatIno(ino)
	if gone(err) || errors.Is(err, syscall.EINVAL) {
		return "", time.Time{}, nil
	}
	if err != nil {
		return "", time.Time{}, fmt.Errorf("%w: %v", ErrUnresolved, err)
	}
	live, name, err := b.whereNode(v, ino, n, hint, "")
	if err != nil {
		return "", time.Time{}, fmt.Errorf("%w: %v", ErrUnresolved, err)
	}
	if !live {
		return "", time.Time{}, nil
	}
	return name, n.Mtime, nil
}
