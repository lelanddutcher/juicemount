package binder

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// ErrUnresolved: the volume could not say where an asset's file is (a
// failed read, or too many links to list). Unknown is not gone.
var ErrUnresolved = errors.New("continuity: cannot resolve the file's names")

// Access is the outcome of CheckAccess.
type Access struct {
	// Missing: the file is gone (its inode no longer exists, or it lives
	// only in the trash). No name of it is in any folder.
	Missing bool `json:"missing"`
	// Allowed: some name of the live file satisfied the check (Name).
	// Otherwise Name is a name that did not.
	Allowed bool   `json:"allowed"`
	Name    string `json:"name,omitempty"`
}

// CheckAccess decides whether a caller may act on an asset's file through
// one of its names: allow is asked about the file's names outside the
// trash, hidden ones included (a file moved into a dot folder is still in
// that folder). It is Allowed if any name passes, which matches writing the
// file through that name. The inode comes from the asset's inode location,
// including one the binder retired, so a file that left the catalog by a
// rename to a hidden name is still found; only a file whose inode is gone,
// or that is only in the trash, is Missing. ctx is checked between volume
// reads (they take no context of their own).
func (b *Binder) CheckAccess(ctx context.Context, id continuity.AssetID, allow func(volumePath string) bool) (Access, error) {
	v := b.vol.Load()
	if v == nil {
		return Access{}, ErrNotReady
	}
	if id.Validate() != nil {
		return Access{}, fmt.Errorf("%w: asset id", catalog.ErrInvalid)
	}
	// The binder keeps two location records per asset; 100 is ample.
	locs, err := b.cfg.Store.ListLocationRecords(ctx, b.library, id, "", 100)
	if err != nil {
		return Access{}, err
	}
	var ino uint64
	hint := ""
	for _, l := range locs {
		switch l.Value.Source {
		case Source(v.uuid):
			digits, _ := strings.CutPrefix(l.Value.Locator, "ino:")
			if n, err := strconv.ParseUint(digits, 10, 64); err == nil {
				ino = n
			}
		case HintSource(v.uuid):
			hint = l.Value.Locator
		}
	}
	if ino == 0 {
		return Access{Missing: true}, nil // never bound to a file of this volume
	}
	if err := ctx.Err(); err != nil {
		return Access{}, err
	}
	n, err := v.StatIno(ino)
	if gone(err) || errors.Is(err, syscall.EINVAL) {
		return Access{Missing: true}, nil
	}
	if err != nil {
		return Access{}, fmt.Errorf("%w: %v", ErrUnresolved, err)
	}
	if n.InTrash && n.Nlink <= 1 {
		return Access{Missing: true}, nil
	}
	// The hint first (one lookup): it decides alone when it passes, or when
	// it is the file's only name.
	if hint != "" {
		if err := ctx.Err(); err != nil {
			return Access{}, err
		}
		ok, err := names(v, hint, ino)
		if err != nil {
			return Access{}, fmt.Errorf("%w: %v", ErrUnresolved, err)
		}
		if ok && allow(hint) {
			return Access{Allowed: true, Name: hint}, nil
		}
		if ok && n.Nlink <= 1 {
			return Access{Name: hint}, nil
		}
	}
	if n.Nlink > MaxLinks {
		return Access{}, fmt.Errorf("%w: it has %d hard links (over %d), too many to check every name", ErrUnresolved, n.Nlink, MaxLinks)
	}
	if err := ctx.Err(); err != nil {
		return Access{}, err
	}
	paths, err := v.GetPaths(ino)
	if gone(err) {
		return Access{Missing: true}, nil
	}
	if err != nil {
		return Access{}, fmt.Errorf("%w: %v", ErrUnresolved, err)
	}
	if len(paths) == 0 {
		return Access{Missing: true}, nil // only in the trash
	}
	sort.Strings(paths)
	for _, p := range paths {
		if allow(p) {
			return Access{Allowed: true, Name: p}, nil
		}
	}
	return Access{Name: paths[0]}, nil
}

// AssetAt returns the asset bound to the regular file now at p (a volume
// lookup, never a path hint): one volume read and one catalog read.
func (b *Binder) AssetAt(ctx context.Context, p string) (continuity.AssetID, error) {
	v := b.vol.Load()
	if v == nil {
		return "", ErrNotReady
	}
	clean, ok := cleanPath(p)
	if !ok {
		return "", fmt.Errorf("%w: path", catalog.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	n, err := v.LookupIno(clean)
	if gone(err) {
		return "", catalog.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnresolved, err)
	}
	loc, err := b.cfg.Store.GetLocation(ctx, b.library, LocationID(v.uuid, n.Ino))
	if err != nil {
		return "", err
	}
	if loc.Removed {
		return "", catalog.ErrNotFound
	}
	return loc.Value.AssetID, nil
}
