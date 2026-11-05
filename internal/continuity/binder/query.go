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
	PathHint     string             `json:"path_hint"` // last path the binder recorded
	Path         string             `json:"path,omitempty"`
	Paths        []string           `json:"paths,omitempty"` // every current name (hard links)
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
		return AssetView{}, err
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
	// The binder records one location per asset; a page of 100 is ample.
	locs, err := b.cfg.Store.ListLocations(ctx, b.library, id, "", 100)
	if err != nil {
		return AssetView{}, err
	}
	out := AssetView{AssetID: asset.ID, PathHint: asset.Label, Locations: []LocationView{}}
	for _, l := range locs {
		out.Locations = append(out.Locations, LocationView{ID: l.ID, Source: l.Value.Source, Locator: l.Value.Locator, UpdatedSequence: l.UpdatedSequence})
		if v == nil {
			out.ResolveError = ErrNotReady.Error()
			continue
		}
		digits, ok := strings.CutPrefix(l.Value.Locator, "ino:")
		ino, err := strconv.ParseUint(digits, 10, 64)
		if l.Value.Source != v.source || !ok || err != nil {
			continue
		}
		paths, err := v.GetPaths(ino)
		if err != nil && !gone(err) {
			out.ResolveError = err.Error()
			continue
		}
		out.Paths = append(out.Paths, paths...)
	}
	out.Path = canonical(out.Paths)
	return out, nil
}
