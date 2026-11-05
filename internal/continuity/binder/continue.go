package binder

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

// Saves that replace the file (README.md). An editor that writes a new file
// and renames it over the document, or Finder's replace, gives the document
// a new inode. The new inode continues the replaced inode's asset, so the
// document keeps its identity and its collections.

// move is a rename of a known file to a sibling name: maybe a safe save's
// backup, which the save deletes once the new file holds the path.
type move struct {
	from string
	at   time.Time
}

// noteMove remembers that ino left from for the sibling name to. It also
// keeps to -> ino in the path cache when the volume no longer shows ino
// there (hidden names are not cached; the backup may be gone already), so
// the backup's delete event finds ino even with a client's synthetic inode.
func (b *Binder) noteMove(ino uint64, from, to string) {
	now := b.cfg.Now()
	if len(b.moves) >= maxMoves {
		for k, m := range b.moves {
			if now.Sub(m.at) > b.cfg.SafeSaveWindow {
				delete(b.moves, k)
			}
		}
		if len(b.moves) >= maxMoves {
			return
		}
	}
	b.moves[ino] = move{from: from, at: now}
	if _, ok := b.paths.get(to); !ok {
		b.paths.put(to, ino)
	}
}

// safeSave runs on the delete of inode x. If x was renamed to a sibling
// name within SafeSaveWindow and another regular file now holds the path x
// left (write a temp file, rename the original to a backup name, rename the
// temp file to the original name, delete the backup), that file continues
// x's asset. Reports whether it did.
func (b *Binder) safeSave(ctx context.Context, v *volume, x uint64) (bool, error) {
	m, ok := b.moves[x]
	if !ok {
		return false, nil
	}
	if b.cfg.Now().Sub(m.at) > b.cfg.SafeSaveWindow || hidden(m.from) {
		delete(b.moves, x)
		return false, nil
	}
	n, err := v.LookupIno(m.from)
	if err != nil && !gone(err) {
		return false, unknown(err) // kept for the retry
	}
	delete(b.moves, x)
	if err != nil || n.Type != smartfolders.TypeRegular || n.Ino == x {
		return false, nil
	}
	cont, err := b.replaced(ctx, v, x, n.Ino, m.from)
	if err != nil || !cont {
		return false, err
	}
	b.paths.put(m.from, n.Ino)
	_, err = b.sync(ctx, v, n.Ino, m.from)
	return true, err
}

// replaced handles path p, last seen naming inode x and now naming y with
// no event saying x left. If x is bound and gone (no visible name; a rename
// over p sends it to the trash), y continues x's asset. Reports whether y
// now carries it; false leaves both to the usual handling: x's records
// are retired and y is bound on its own. A replacement that cannot be
// continued is counted (not_continued).
func (b *Binder) replaced(ctx context.Context, v *volume, x, y uint64, p string) (bool, error) {
	xs, err := b.records(ctx, v, x)
	if err != nil || xs.ino == nil {
		return false, err
	}
	live, _, err := b.where(v, x, xs.activeHint(), "")
	if err != nil || live {
		return false, err // x lives on under another name: not a replacement
	}
	n, err := v.StatIno(y)
	if gone(err) || errors.Is(err, syscall.EINVAL) {
		return false, nil // changed again: a later event names it
	}
	if err != nil {
		return false, unknown(err)
	}
	if n.Type != smartfolders.TypeRegular || n.InTrash {
		return false, nil
	}
	if n.Nlink > 1 {
		b.notContinue(p, "the new file has other names")
		return false, nil
	}
	var kind, blocked string
	err = b.converge(ctx, func() (*catalog.Operation, error) {
		op, k, why, err := b.planContinue(ctx, v, x, y, p)
		kind, blocked = k, why
		return op, err
	})
	switch {
	case err != nil:
		return false, err
	case blocked != "":
		b.notContinue(p, blocked)
		return false, nil
	case kind == "":
		return false, nil // x's records went meanwhile: nothing to continue
	case kind == "merge":
		b.merged.Add(1)
		fallthrough
	case kind == "continue":
		b.continued.Add(1)
	}
	return true, nil
}

func (b *Binder) notContinue(p, why string) {
	n := b.notContinued.Add(1)
	b.logEvery("continue", "continuity: %d replaced files not continued in total (their new file got an asset of its own); latest %q: %s", n, p, why)
}

// planContinue is the one operation that moves x's asset to y at p:
// x's records retired, y's records put with x's asset. kind is "continue"
// (y was unbound), "merge" (y's provisional asset is retired: its records
// are tombstoned, y's continuation pair carries x's asset, and a MergedInto
// relationship leads from the retired asset to x's), "done" (a replay), or
// "" (x is not bound). blocked says why y cannot continue x's asset.
func (b *Binder) planContinue(ctx context.Context, v *volume, x, y uint64, p string) (op *catalog.Operation, kind, blocked string, err error) {
	xs, err := b.records(ctx, v, x)
	if err != nil || xs.ino == nil {
		return nil, "", "", err
	}
	asset := xs.ino.Value.AssetID
	ys, err := b.records(ctx, v, y)
	if err != nil {
		return nil, "", "", err
	}
	if ys.ino != nil && ys.ino.Value.AssetID == asset {
		return nil, "done", "", nil
	}
	// Two live files never share an asset.
	active, err := b.cfg.Store.ListLocations(ctx, b.library, asset, "", 100)
	if err != nil {
		return nil, "", "", err
	}
	for _, l := range active {
		if l.Value.Source == Source(v.uuid) && l.ID != xs.inoID {
			return nil, "", "the replaced file's asset is bound to another file", nil
		}
	}
	var changes []catalog.Change
	retire := func(ls ...*catalog.Location) {
		for _, l := range ls {
			if l != nil && !l.Removed {
				changes = append(changes, catalog.Change{RemoveLocation: &catalog.RemoveLocation{ID: l.ID, Expected: expected(l)}})
			}
		}
	}
	retire(xs.ino, xs.hint)
	kind = "continue"
	inoID, hintID := LocationID(v.uuid, y), HintID(v.uuid, y)
	switch {
	case ys.ino == nil && ys.hint == nil:
	case ys.cont:
		return nil, "", "the new file already continues a merged asset", nil
	case ys.ino == nil:
		return nil, "", "the new file has a hint without an inode location", nil
	default:
		own := ys.ino.Value.AssetID
		ok, err := b.provisional(ctx, v, y, own)
		if err != nil {
			return nil, "", "", err
		}
		if !ok {
			return nil, "", "the new file has an established asset of its own", nil
		}
		kind = "merge"
		retire(ys.ino, ys.hint)
		inoID, hintID = ContinuationID(v.uuid, y), ContinuationHintID(v.uuid, y)
		changes = append(changes, catalog.Change{PutRelationship: &catalog.PutRelationship{ID: mergeID(v.uuid, y), Value: catalog.RelationshipValue{
			SourceAssetID: own, TargetAssetID: asset, Predicate: MergedInto, Provenance: catalog.Observed, Source: Source(v.uuid),
		}}})
	}
	changes = append(changes,
		catalog.Change{PutLocation: &catalog.PutLocation{ID: inoID, Value: catalog.LocationValue{AssetID: asset, Source: Source(v.uuid), Locator: Locator(y)}}},
		catalog.Change{PutLocation: &catalog.PutLocation{ID: hintID, Value: catalog.LocationValue{AssetID: asset, Source: HintSource(v.uuid), Locator: p}}},
	)
	return &catalog.Operation{
		Version:   catalog.OperationVersion,
		LibraryID: b.library,
		ID:        operationID(v, y, kind, seq(xs.ino), seq(ys.ino)),
		Actor:     Actor,
		Changes:   changes,
	}, kind, "", nil
}

// provisional reports whether y's own records may be retired into another
// asset: y was bound under its own derived asset (it never continued one)
// within ProvisionalAge, by the receipt time of its bind. That is the temp
// file of a save whose create event came first. An older file is someone's
// document; a heuristic never ends its identity.
func (b *Binder) provisional(ctx context.Context, v *volume, y uint64, own continuity.AssetID) (bool, error) {
	if own != AssetID(v.uuid, y) {
		return false, nil
	}
	ev, err := b.cfg.Store.GetEvent(ctx, b.library, operationID(v, y, "bind", 0, 0))
	if errors.Is(err, catalog.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return b.cfg.Now().Sub(time.Unix(0, ev.Receipt.RecordedUnixNano)) <= b.cfg.ProvisionalAge, nil
}

// warm seeds the path cache from the catalog's active hints, which a
// restart would otherwise forget, so a file replaced after a restart is
// still continued. Hints can be stale: a stale entry costs only the check
// that its inode is gone, and a file deleted while the Manager was down is
// continued by the next file created at its path. Best effort; it stops
// before the cache would evict.
func (b *Binder) warm(ctx context.Context, v *volume) {
	prefix := "jfs-" + v.uuid + "-"
	after := ""
	for ctx.Err() == nil && b.paths.len() < b.paths.max {
		page, err := b.cfg.Store.ListSourceLocations(ctx, b.library, HintSource(v.uuid), after, catalog.MaxPageSize)
		if err != nil {
			b.cfg.Logf("continuity: seeding recent paths from the catalog: %v (a file replaced before its next event gets an asset of its own)", err)
			return
		}
		for _, l := range page {
			if b.paths.len() >= b.paths.max {
				return
			}
			digits, ok := strings.CutPrefix(l.ID, prefix)
			digits, hint := strings.CutSuffix(digits, "-hint")
			digits, _ = strings.CutSuffix(digits, "-c")
			ino, err := strconv.ParseUint(digits, 10, 64)
			if p, valid := cleanPath(l.Value.Locator); ok && hint && err == nil && valid && !hidden(p) {
				b.paths.put(p, ino)
			}
		}
		if len(page) < catalog.MaxPageSize {
			return
		}
		after = page[len(page)-1].ID
	}
}

// lastInode is the inode an asset was last bound to, and its hint: the
// asset's active inode location, else its most recently updated retired one
// (an asset continued across saves keeps one retired pair per save). 0 when
// it was never bound to a file of this volume.
func (b *Binder) lastInode(ctx context.Context, v *volume, id continuity.AssetID) (uint64, string, error) {
	var best *catalog.Location
	pick := func(ls []catalog.Location) {
		for i := range ls {
			l := &ls[i]
			if l.Value.Source != Source(v.uuid) {
				continue
			}
			if best == nil || (best.Removed && !l.Removed) || (best.Removed == l.Removed && l.UpdatedSequence > best.UpdatedSequence) {
				best = l
			}
		}
	}
	active, err := b.cfg.Store.ListLocations(ctx, b.library, id, "", 100)
	if err != nil {
		return 0, "", err
	}
	pick(active)
	for after := ""; best == nil || best.Removed; {
		page, err := b.cfg.Store.ListLocationRecords(ctx, b.library, id, after, catalog.MaxPageSize)
		if err != nil {
			return 0, "", err
		}
		pick(page)
		if len(page) < catalog.MaxPageSize {
			break
		}
		after = page[len(page)-1].ID
	}
	if best == nil {
		return 0, "", nil
	}
	digits, _ := strings.CutPrefix(best.Value.Locator, "ino:")
	ino, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, "", nil
	}
	hint := ""
	h, err := b.cfg.Store.GetLocation(ctx, b.library, best.ID+"-hint")
	if err == nil {
		hint = h.Value.Locator
	} else if !errors.Is(err, catalog.ErrNotFound) {
		return 0, "", err
	}
	return ino, hint, nil
}
