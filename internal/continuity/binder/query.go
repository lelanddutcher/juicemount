package binder

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// ErrNotReady: the volume is not connected yet, so a path cannot be resolved.
var ErrNotReady = errors.New("continuity: volume not connected yet")

// AssetView is one asset as the read-only API returns it.
type AssetView struct {
	AssetID      continuity.AssetID `json:"asset_id"`
	Label        string             `json:"label"`               // set at creation; never changed by the binder
	PathHint     string             `json:"path_hint,omitempty"` // last path the binder recorded
	Path         string             `json:"path,omitempty"`      // current path, resolved from the volume
	Removed      bool               `json:"removed"`             // no live visible name when last observed
	ResolveError string             `json:"resolve_error,omitempty"`
	Locations    []LocationView     `json:"locations"` // active only
}

type LocationView struct {
	ID              string `json:"id"`
	Source          string `json:"source"`
	Locator         string `json:"locator"`
	UpdatedSequence int64  `json:"updated_sequence"`
}

// AssetByPath returns the asset bound to the regular file now at p.
func (b *Binder) AssetByPath(ctx context.Context, p string) (AssetView, error) {
	v := b.vol.Load()
	if v == nil {
		return AssetView{}, ErrNotReady
	}
	clean, ok := cleanPath(p)
	if !ok {
		return AssetView{}, fmt.Errorf("%w: path", catalog.ErrInvalid)
	}
	n, err := v.LookupIno(clean)
	if gone(err) {
		return AssetView{}, catalog.ErrNotFound
	}
	if err != nil {
		b.cfg.Logf("continuity: resolving %q: %v", clean, err)
		return AssetView{}, errors.New("continuity: could not read the volume")
	}
	loc, err := b.cfg.Store.GetLocation(ctx, b.library, LocationID(v.uuid, n.Ino))
	if err != nil {
		return AssetView{}, err
	}
	return b.view(ctx, v, loc.Value.AssetID)
}

// AssetByID returns one asset; paths resolve only once the volume is open.
func (b *Binder) AssetByID(ctx context.Context, id continuity.AssetID) (AssetView, error) {
	if id.Validate() != nil {
		return AssetView{}, fmt.Errorf("%w: asset id", catalog.ErrInvalid)
	}
	return b.view(ctx, b.vol.Load(), id)
}

func (b *Binder) view(ctx context.Context, v *volume, id continuity.AssetID) (AssetView, error) {
	asset, err := b.cfg.Store.GetAsset(ctx, b.library, id)
	if err != nil {
		return AssetView{}, err
	}
	// The binder records two locations per asset; a page of 100 is ample.
	locs, err := b.cfg.Store.ListLocations(ctx, b.library, id, "", 100)
	if err != nil {
		return AssetView{}, err
	}
	out := AssetView{AssetID: asset.ID, Label: asset.Label, Removed: true, Locations: []LocationView{}}
	var ino uint64
	for _, l := range locs {
		out.Locations = append(out.Locations, LocationView{ID: l.ID, Source: l.Value.Source, Locator: l.Value.Locator, UpdatedSequence: l.UpdatedSequence})
		if v == nil {
			continue
		}
		switch l.Value.Source {
		case HintSource(v.uuid):
			out.PathHint = l.Value.Locator
		case Source(v.uuid):
			digits, _ := strings.CutPrefix(l.Value.Locator, "ino:")
			if n, err := strconv.ParseUint(digits, 10, 64); err == nil {
				ino = n
				out.Removed = false
			}
		}
	}
	if v == nil {
		out.ResolveError = ErrNotReady.Error()
		return out, nil
	}
	if ino != 0 {
		// The hint first (two point reads); every name only if it moved.
		live, name, err := b.where(v, ino, out.PathHint, "")
		if err != nil {
			b.cfg.Logf("continuity: resolving inode %d: %v", ino, err)
			out.ResolveError = "could not resolve the current path"
		} else if live {
			out.Path = name
		}
	}
	return out, nil
}
