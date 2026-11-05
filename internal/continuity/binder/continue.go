package binder

import (
	"context"
	"errors"
	"fmt"
	"path"
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
// no event saying x left. If x is bound, has no visible name left, and was
// replaced by a save, y continues x's asset. The evidence of a save is
// timing: a rename over p, or the delete of a safe save's backup, sets x's
// change time (JuiceFS keeps x's attributes in the trash), and the rename
// that brings y in sets y's, so the two are within SaveWindow. A file
// deleted unseen and a new one created at its path later (camera clip
// names repeat) has no such evidence; nor has an x whose attributes are
// gone (deleted for good, or a volume without a trash). Reports whether y
// now carries x's asset; false leaves both to the usual handling: x's
// records are retired and y is bound on its own. A replacement that is
// not continued is counted (not_continued).
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
	old, err := v.StatIno(x)
	if gone(err) || errors.Is(err, syscall.EINVAL) {
		b.notContinue(p, "the replaced file was deleted for good: no evidence of a save")
		return false, nil
	}
	if err != nil {
		return false, unknown(err)
	}
	if d := n.Ctime.Sub(old.Ctime).Abs(); d > b.cfg.SaveWindow {
		b.notContinue(p, fmt.Sprintf("the replaced and the new file changed %s apart: not a save", d.Round(time.Second)))
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
// are tombstoned, y's next pair carries x's asset, and a MergedInto
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
	if elsewhere, err := b.liveElsewhere(ctx, v, asset, xs.inoID); err != nil || elsewhere {
		if elsewhere {
			blocked = "the replaced file's asset is bound to another file"
		}
		return nil, "", blocked, err
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
	case ys.gen >= MaxGenerations:
		return nil, "", "the new file has no record pair left", nil
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
		inoID, hintID = GenerationID(v.uuid, y, ys.gen+1), GenerationHintID(v.uuid, y, ys.gen+1)
		changes = append(changes, catalog.Change{PutRelationship: &catalog.PutRelationship{ID: inoID + "-merge", Value: catalog.RelationshipValue{
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
// asset: y was bound under its own derived asset by its first pair (it never
// continued one) within ProvisionalAge, by the receipt time of its bind. That is the temp
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
			digits, _, _ = strings.Cut(digits, "-") // a later pair's "-g<n>"
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

// liveElsewhere reports whether asset has an active inode location other
// than except. Two live files never share an asset.
func (b *Binder) liveElsewhere(ctx context.Context, v *volume, asset continuity.AssetID, except string) (bool, error) {
	for after := ""; ; {
		page, err := b.cfg.Store.ListLocations(ctx, b.library, asset, after, catalog.MaxPageSize)
		if err != nil {
			return false, err
		}
		for _, l := range page {
			if l.Value.Source == Source(v.uuid) && l.ID != except {
				return true, nil
			}
		}
		if len(page) < catalog.MaxPageSize {
			return false, nil
		}
		after = page[len(page)-1].ID
	}
}

// planRestore binds an inode that is visible again (restored from the
// trash, or moved back from a hidden name) while its asset lives on in
// another file (a save replaced it): reviving it would give two files one
// asset. It gets the next record pair, under an asset of its own, and a
// RestoredFrom relationship to the asset it had.
func (b *Binder) planRestore(ctx context.Context, v *volume, ino uint64, s slot, name string) (*catalog.Operation, error) {
	gen := s.gen + 1
	if gen > MaxGenerations {
		return nil, fmt.Errorf("continuity: inode %d has used all %d record pairs", ino, MaxGenerations)
	}
	asset := generationAsset(v.uuid, ino, gen)
	var changes []catalog.Change
	if _, err := b.cfg.Store.GetAsset(ctx, b.library, asset); errors.Is(err, catalog.ErrNotFound) {
		changes = append(changes, catalog.Change{CreateAsset: &catalog.CreateAsset{AssetID: asset, Label: path.Base(name)}})
	} else if err != nil {
		return nil, err
	}
	id := GenerationID(v.uuid, ino, gen)
	changes = append(changes,
		catalog.Change{PutRelationship: &catalog.PutRelationship{ID: id + "-restored", Value: catalog.RelationshipValue{
			SourceAssetID: asset, TargetAssetID: s.ino.Value.AssetID, Predicate: RestoredFrom, Provenance: catalog.Observed, Source: Source(v.uuid),
		}}},
		catalog.Change{PutLocation: &catalog.PutLocation{ID: id, Value: catalog.LocationValue{AssetID: asset, Source: Source(v.uuid), Locator: Locator(ino)}}},
		catalog.Change{PutLocation: &catalog.PutLocation{ID: GenerationHintID(v.uuid, ino, gen), Value: catalog.LocationValue{AssetID: asset, Source: HintSource(v.uuid), Locator: name}}},
	)
	return &catalog.Operation{
		Version:   catalog.OperationVersion,
		LibraryID: b.library,
		ID:        operationID(v, ino, s.kind("restore"), seq(s.ino), seq(s.hint)),
		Actor:     Actor,
		Changes:   changes,
	}, nil
}

// inodeRef is one inode an asset is bound to, with its hint.
type inodeRef struct {
	ino  uint64
	hint string
}

// assetInodes lists the inodes an asset is bound to: every active inode
// location (normally one), else its most recently updated retired one (an
// asset continued across saves keeps one retired pair per save). None when
// it was never bound to a file of this volume.
func (b *Binder) assetInodes(ctx context.Context, v *volume, id continuity.AssetID) ([]inodeRef, error) {
	var live []catalog.Location
	var latest *catalog.Location
	for _, active := range []bool{true, false} {
		for after := ""; ; {
			var page []catalog.Location
			var err error
			if active {
				page, err = b.cfg.Store.ListLocations(ctx, b.library, id, after, catalog.MaxPageSize)
			} else {
				page, err = b.cfg.Store.ListLocationRecords(ctx, b.library, id, after, catalog.MaxPageSize)
			}
			if err != nil {
				return nil, err
			}
			for i := range page {
				l := &page[i]
				switch {
				case l.Value.Source != Source(v.uuid):
				case active:
					live = append(live, *l)
				case latest == nil || l.UpdatedSequence > latest.UpdatedSequence:
					latest = l
				}
			}
			if len(page) < catalog.MaxPageSize {
				break
			}
			after = page[len(page)-1].ID
		}
		if len(live) > 0 {
			break
		}
	}
	if len(live) == 0 && latest != nil {
		live = append(live, *latest)
	}
	var out []inodeRef
	for _, l := range live {
		digits, _ := strings.CutPrefix(l.Value.Locator, "ino:")
		ino, err := strconv.ParseUint(digits, 10, 64)
		if err != nil {
			continue
		}
		ref := inodeRef{ino: ino}
		h, err := b.cfg.Store.GetLocation(ctx, b.library, l.ID+"-hint")
		if err == nil {
			ref.hint = h.Value.Locator
		} else if !errors.Is(err, catalog.ErrNotFound) {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, nil
}
