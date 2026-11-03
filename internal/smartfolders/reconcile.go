package smartfolders

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"sort"
	"strings"
	"syscall"
)

var (
	// ErrNotManaged: the smart-folder root or a view exists but was not
	// created by the Manager (or no longer is the directory it created).
	// Nothing is written into it; it is never adopted.
	ErrNotManaged = errors.New("directory was not created by the Manager")
	// ErrTooManyViews: creating the view would exceed Reconciler.MaxViews.
	ErrTooManyViews = errors.New("smart-folder view limit reached")
	// ErrBadView: the folder name cannot be a view.
	ErrBadView = errors.New("invalid smart-folder view")

	// Per-link refusals (Result.Refused).
	ErrBadName          = errors.New("invalid link name")
	ErrDuplicateName    = errors.New("duplicate link name")
	ErrNameTaken        = errors.New("name is taken by an entry the Manager did not create")
	ErrTargetForbidden  = errors.New("target is outside the linkable part of the volume")
	ErrTargetMissing    = errors.New("target does not exist")
	ErrTargetNotRegular = errors.New("target is not a regular file")
)

// OwnedDirPerm is the mode of every directory the Manager creates. It is
// set atomically by mkdir and is unusual (group bits clear), so a directory
// recorded as pending (its creation unconfirmed after a crash or a lost
// reply) is recognised as the Manager's only if it carries exactly this
// mode; anything else under that name is someone else's and is never
// adopted. Others keep read and traverse, which is all clients need.
const OwnedDirPerm fs.FileMode = 0o705

// Ownership is everything the Manager created in the volume for smart
// folders. It lives in the Manager's own state file, not in the volume.
// Every creation is recorded and persisted BEFORE it happens: a directory
// as a pending record (resolved by OwnedDirPerm), a link as a temp record
// under a random hidden name that only the Manager knows, renamed into place
// with no-replace semantics. Nothing outside these records is ever modified.
type Ownership struct {
	Root  *OwnedDir             `json:"root,omitempty"`
	Views map[string]*OwnedView `json:"views,omitempty"` // by top-level folder name
}

// OwnedDir is a directory the Manager created, identified by inode. JuiceFS
// allocates inode numbers from a counter and never reuses one, so a
// directory someone recreated under the same name has a different inode.
type OwnedDir struct {
	Ino     uint64 `json:"ino,omitempty"`
	Pending bool   `json:"pending,omitempty"` // mkdir sent, result not confirmed
}

// OwnedView is one view directory and the links in it the Manager created.
type OwnedView struct {
	OwnedDir
	Links map[string]*OwnedLink `json:"links,omitempty"` // by link name
	Temps map[string]*OwnedTemp `json:"temps,omitempty"` // by temp name
}

// OwnedLink is one symlink the Manager created and placed.
type OwnedLink struct {
	Target string `json:"target,omitempty"` // volume path of the target file
	Text   string `json:"text"`             // the symlink text written
	Ino    uint64 `json:"ino"`
}

// OwnedTemp is a link being created under a temporary name.
type OwnedTemp struct {
	For    string `json:"for"`    // the link name it becomes
	Target string `json:"target"` // volume path of the target file
	Text   string `json:"text"`
	Ino    uint64 `json:"ino,omitempty"`     // once the temp link exists
	OldIno uint64 `json:"old_ino,omitempty"` // retarget: the link it replaces
}

// RootState is the smart-folder root as the reconciler found it.
type RootState int

const (
	RootAbsent  RootState = iota // not there; created with the first view
	RootOwned                    // the directory the Manager created
	RootForeign                  // someone else's: never touched
)

// Refusal is a wanted link the reconciler did not create.
type Refusal struct {
	Name   string
	Target string
	Err    error
}

// TargetGone reports whether the target is no longer a regular file (the
// caller should stop tracking it).
func (f Refusal) TargetGone() bool {
	return errors.Is(f.Err, ErrTargetMissing) || errors.Is(f.Err, ErrTargetNotRegular)
}

// Result reports what one reconcile did. Names are link names in the view.
type Result struct {
	CreatedRoot bool
	CreatedView bool
	Created     []string
	Retargeted  []string
	Removed     []string
	Unchanged   []string
	// Foreign lists recorded names found replaced by something the Manager
	// did not create; the record is dropped and the entry left alone.
	Foreign []string
	Refused []Refusal
}

// Changed reports whether the volume was modified.
func (r Result) Changed() bool {
	return r.CreatedRoot || r.CreatedView || len(r.Created)+len(r.Retargeted)+len(r.Removed) > 0
}

// Reconciler keeps views of relative symlinks in a volume. It is not safe
// for concurrent use; the Manager drives it from one goroutine.
type Reconciler struct {
	FS   FS
	Root string // smart-folder root name, e.g. "Recents"
	Own  *Ownership
	// Persist durably saves Own. It is called before every write that
	// creates something and after each view is reconciled; when it fails,
	// nothing further is written.
	Persist func() error
	// Gate runs before the first write into a view (the project lease
	// checkout) and returns its release. Nil means no gate. It is never
	// called when a view needs no change.
	Gate func(view string) (release func(), err error)
	// MaxViews bounds the number of view directories (0 = unbounded).
	MaxViews int

	rootPerm fs.FileMode // mode of the owned root, from the last CheckRoot
	release  []func()    // gate releases held by the running Reconcile
}

func (r *Reconciler) persist() error {
	if r.Persist == nil {
		return nil
	}
	if err := r.Persist(); err != nil {
		return fmt.Errorf("persisting smart-folder ownership: %w", err)
	}
	return nil
}

// Normalize gives every view non-nil maps. JSON drops empty maps
// (omitempty), so a state saved mid-run reloads with nil ones.
func (o *Ownership) Normalize() {
	if o.Views == nil {
		o.Views = map[string]*OwnedView{}
	}
	for name, v := range o.Views {
		if v == nil {
			delete(o.Views, name)
			continue
		}
		v.normalize()
	}
}

func (v *OwnedView) normalize() {
	if v.Links == nil {
		v.Links = map[string]*OwnedLink{}
	}
	if v.Temps == nil {
		v.Temps = map[string]*OwnedTemp{}
	}
}

// settleDir compares a recorded directory with the volume. A pending
// record is confirmed by the inode recorded right after mkdir returned or,
// when the reply never came, only by OwnedDirPerm. It reports whether the
// directory is the Manager's, and whether the record was changed.
func settleDir(info Info, rec *OwnedDir) (owned, changed bool) {
	if info.Type != TypeDir {
		return false, false
	}
	if rec.Pending {
		switch {
		case rec.Ino != 0 && info.Ino != rec.Ino:
			return false, false
		case rec.Ino == 0 && info.Perm != OwnedDirPerm:
			return false, false
		}
		rec.Ino, rec.Pending = info.Ino, false
		return true, true
	}
	return info.Ino == rec.Ino, false
}

// CheckRoot inspects the smart-folder root and settles its record: a root
// that vanished (or was never created) drops every record under it; a
// pending root is confirmed by inode or mode; a root that is not the
// recorded directory is RootForeign (and the stale records are dropped).
func (r *Reconciler) CheckRoot() (RootState, error) {
	r.Own.Normalize()
	r.rootPerm = 0
	if err := ValidateRootName(r.Root); err != nil {
		return RootForeign, err
	}
	info, err := r.FS.Lstat(r.Root)
	if errors.Is(err, fs.ErrNotExist) {
		if r.Own.Root != nil {
			lost := r.dropAll()
			return RootAbsent, errors.Join(r.persist(), lost)
		}
		return RootAbsent, nil
	}
	if err != nil {
		return RootForeign, err
	}
	if r.Own.Root == nil {
		return RootForeign, nil
	}
	owned, changed := settleDir(info, r.Own.Root)
	if !owned {
		lost := r.dropAll()
		return RootForeign, errors.Join(r.persist(), lost)
	}
	r.rootPerm = info.Perm
	if changed {
		return RootOwned, r.persist()
	}
	return RootOwned, nil
}

// DroppedTempsError reports temp records dropped because the directory
// holding them was removed, renamed or replaced by someone else: those
// hidden names may still hold an entry (possibly a user's) that the
// Manager can no longer reach. It names each one so it can be recovered.
type DroppedTempsError struct {
	Names []string // "<Root>/<view>/<temp name>", as they were
}

func (e *DroppedTempsError) Error() string {
	return "smart-folder directory removed, renamed or replaced; these hidden names under it may still hold entries: " + strings.Join(e.Names, ", ")
}

func droppedTemps(view string, ov *OwnedView) *DroppedTempsError {
	if ov == nil || len(ov.Temps) == 0 {
		return nil
	}
	var names []string
	for _, tmp := range sortedKeys(ov.Temps) {
		names = append(names, view+"/"+tmp)
	}
	return &DroppedTempsError{Names: names}
}

// dropAll forgets the root and every view, reporting their temp names.
func (r *Reconciler) dropAll() error {
	var names []string
	for _, folder := range sortedKeys(r.Own.Views) {
		if d := droppedTemps(r.Root+"/"+folder, r.Own.Views[folder]); d != nil {
			names = append(names, d.Names...)
		}
	}
	r.Own.Root, r.Own.Views = nil, map[string]*OwnedView{}
	if names == nil {
		return nil
	}
	return &DroppedTempsError{Names: names}
}

// mkdirOwned creates p for a record already persisted as pending. On any
// error but "exists" the record stays pending, to be settled next time (the
// mkdir may have happened and only its reply been lost). When a umask left
// the directory without OwnedDirPerm, its inode is recorded before the mode
// is fixed, so a crash in between still recognises it.
func (r *Reconciler) mkdirOwned(p string, rec *OwnedDir) error {
	info, err := r.FS.Mkdir(p, OwnedDirPerm)
	if err == nil {
		if info.Perm != OwnedDirPerm {
			rec.Ino = info.Ino
			if err := r.persist(); err != nil {
				return err
			}
			if err := r.FS.Chmod(p, info.Ino, OwnedDirPerm); err != nil {
				return err
			}
		}
		rec.Ino, rec.Pending = info.Ino, false
		return nil
	}
	if !errors.Is(err, fs.ErrExist) {
		return err
	}
	if info, lerr := r.FS.Lstat(p); lerr == nil {
		if owned, _ := settleDir(info, rec); owned {
			return nil
		}
	}
	return fmt.Errorf("%s: %w", p, ErrNotManaged)
}

// PreviousNames maps each target to the name its link has in the view now,
// for Names.
func (r *Reconciler) PreviousNames(folder string) map[string]string {
	out := map[string]string{}
	if v := r.Own.Views[folder]; v != nil {
		for name, l := range v.Links {
			if l.Target != "" {
				out[l.Target] = name
			}
		}
	}
	return out
}

// Reconcile makes the view <Root>/<folder> hold exactly the wanted links
// among the entries the Manager created there:
//
//   - targets are validated first (a regular file reachable without
//     symlinks, outside hidden names, .trash, .juicemount and the smart
//     root); invalid ones are refused, and a view that would hold no valid
//     link is not created;
//   - interrupted operations are settled first from their temp records, by
//     inspecting both the temp name and the link name (finishTemp);
//   - a missing link is created under a recorded random temp name and
//     renamed into place without replacing anything; a changed one is
//     exchanged atomically with a recorded temp link after re-checking the
//     name; a link no longer wanted is moved aside to a recorded temp name
//     and only unlinked there if it is the Manager's; an unchanged one is
//     left alone (same inode);
//   - an emptied view stays, empty: the Manager never removes a directory;
//   - anything the Manager did not create is never modified or removed:
//     an existing root or view it did not create is ErrNotManaged, a
//     recorded name now holding something else is dropped from the records
//     (Result.Foreign), a wanted name taken by such an entry is refused
//     (ErrNameTaken), and an entry that ends up under a temp name is moved
//     back (or, if that is impossible, kept recorded and reported).
func (r *Reconciler) Reconcile(folder string, want []Link) (Result, error) {
	var res Result
	if !validName(folder) || folder == r.Root {
		return res, fmt.Errorf("%w: %q", ErrBadView, folder)
	}
	r.Own.Normalize()
	view := r.Root + "/" + folder
	dirty := false // records changed without a write

	// 1. Validate every wanted link before touching anything.
	type plan struct{ target, text string }
	valid := map[string]plan{}
	seen := map[string]bool{}
	for _, l := range want {
		switch {
		case !validName(l.Name) || strings.HasPrefix(l.Name, tempPrefix):
			res.Refused = append(res.Refused, Refusal{l.Name, l.Target, ErrBadName})
		case seen[foldName(l.Name)]:
			res.Refused = append(res.Refused, Refusal{l.Name, l.Target, ErrDuplicateName})
		default:
			seen[foldName(l.Name)] = true
			if err := r.checkTarget(l.Target); err != nil {
				res.Refused = append(res.Refused, Refusal{l.Name, l.Target, err})
				continue
			}
			valid[l.Name] = plan{target: l.Target, text: relTarget(2, l.Target)}
		}
	}

	// 2. The root and the view must be absent or the Manager's own.
	var errs []error
	rootState, err := r.CheckRoot()
	var lost *DroppedTempsError
	switch {
	case errors.As(err, &lost):
		errs = append(errs, lost) // reported; carry on with the new state
	case err != nil:
		return res, err
	}
	if rootState == RootForeign {
		return res, errors.Join(append(errs, fmt.Errorf("%s: %w", r.Root, ErrNotManaged))...)
	}
	var fixPerm []string // owned directories a umask left without OwnedDirPerm
	if rootState == RootOwned && r.rootPerm != OwnedDirPerm {
		fixPerm = append(fixPerm, r.Root)
	}
	ov := r.Own.Views[folder]
	viewExists := false
	if rootState == RootOwned {
		info, err := r.FS.Lstat(view)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			if ov != nil {
				// Gone or renamed away: whatever its temp names held went
				// with it. Report them; nothing here can be settled.
				if d := droppedTemps(view, ov); d != nil {
					errs = append(errs, d)
				}
				delete(r.Own.Views, folder)
				ov, dirty = nil, true
			}
		case err != nil:
			return res, err
		case ov == nil:
			return res, fmt.Errorf("%s: %w", view, ErrNotManaged)
		default:
			owned, changed := settleDir(info, &ov.OwnedDir)
			if !owned {
				d := droppedTemps(view, ov)
				delete(r.Own.Views, folder)
				if perr := r.persist(); perr != nil {
					return res, perr
				}
				err := fmt.Errorf("%s: %w", view, ErrNotManaged)
				if d != nil {
					err = errors.Join(err, d)
				}
				return res, errors.Join(append(errs, err)...)
			}
			viewExists, dirty = true, dirty || changed
			if info.Perm != OwnedDirPerm {
				fixPerm = append(fixPerm, view)
			}
		}
	}
	finish := func(err error) (Result, error) {
		if dirty {
			if perr := r.persist(); perr != nil {
				err = errors.Join(err, perr)
			}
		}
		sort.Strings(res.Foreign)
		return res, err
	}
	gated := false
	gate := func() error {
		if gated || r.Gate == nil {
			gated = true
			return nil
		}
		release, err := r.Gate(view)
		if err != nil {
			return err
		}
		gated = true
		if release != nil {
			r.release = append(r.release, release)
		}
		return nil
	}
	defer func() {
		for _, rel := range r.release {
			rel()
		}
		r.release = nil
	}()

	// 3. Settle interrupted operations, then the recorded links.
	if viewExists {
		var open []string
		for _, tmp := range sortedKeys(ov.Temps) {
			t := ov.Temps[tmp]
			if t.Ino != 0 {
				if info, err := r.FS.Lstat(view + "/" + t.For); err == nil && info.Type == TypeSymlink && info.Ino == t.Ino {
					// The temp link reached its name before the run stopped.
					ov.Links[t.For] = &OwnedLink{Target: t.Target, Text: t.Text, Ino: t.Ino}
					dirty = true
				}
			}
			if _, err := r.FS.Lstat(view + "/" + tmp); errors.Is(err, fs.ErrNotExist) {
				delete(ov.Temps, tmp) // nothing under the temp name
				dirty = true
				continue
			}
			open = append(open, tmp)
		}
		if len(open)+len(fixPerm) > 0 {
			if err := gate(); err != nil {
				return finish(err)
			}
			dirty = true
			for _, tmp := range open {
				if err := r.finishTemp(view, tmp, ov, &res); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	current := map[string]string{} // owned link name -> text on disk
	if ov != nil {
		for name, l := range ov.Links {
			text, ok := r.ownedLink(view+"/"+name, l)
			if !ok {
				delete(ov.Links, name)
				dirty = true
				if text != "" && !slices.Contains(res.Foreign, name) {
					res.Foreign = append(res.Foreign, name)
				}
				continue
			}
			current[name] = text
		}
	}

	// 4. Plan.
	var creates, retargets, removes []string
	for _, name := range sortedKeys(valid) {
		text, owned := current[name]
		switch {
		case !owned && viewExists && r.taken(view+"/"+name):
			res.Refused = append(res.Refused, Refusal{name, valid[name].target, ErrNameTaken})
		case !owned:
			creates = append(creates, name)
		case text == valid[name].text:
			res.Unchanged = append(res.Unchanged, name)
			if l := ov.Links[name]; l.Target != valid[name].target {
				l.Target, dirty = valid[name].target, true
			}
		default:
			retargets = append(retargets, name)
		}
	}
	for _, name := range sortedKeys(current) {
		if _, ok := valid[name]; !ok {
			removes = append(removes, name)
		}
	}
	if !viewExists && len(creates) == 0 {
		return finish(nil)
	}
	if !viewExists && r.MaxViews > 0 && len(r.Own.Views) >= r.MaxViews {
		return finish(fmt.Errorf("%s: %w (%d)", view, ErrTooManyViews, r.MaxViews))
	}
	if len(creates)+len(retargets)+len(removes)+len(fixPerm) == 0 {
		return finish(errors.Join(errs...))
	}

	// 5. Act, under the gate.
	if err := gate(); err != nil {
		return finish(err)
	}
	for _, p := range fixPerm {
		rec := r.Own.Root
		if p == view {
			rec = &ov.OwnedDir
		}
		if err := r.FS.Chmod(p, rec.Ino, OwnedDirPerm); err != nil {
			errs = append(errs, fmt.Errorf("mode of %s: %w", p, err))
		}
	}
	if rootState == RootAbsent {
		r.Own.Root = &OwnedDir{Pending: true}
		r.Own.Views = map[string]*OwnedView{}
		if err := r.persist(); err != nil {
			return res, err
		}
		if err := r.mkdirOwned(r.Root, r.Own.Root); err != nil {
			if errors.Is(err, ErrNotManaged) {
				r.Own.Root = nil
			}
			dirty = true
			return finish(err)
		}
		res.CreatedRoot, dirty = true, true
	}
	if !viewExists {
		ov = &OwnedView{OwnedDir: OwnedDir{Pending: true}}
		ov.normalize()
		r.Own.Views[folder] = ov
		if err := r.persist(); err != nil {
			return res, err
		}
		if err := r.mkdirOwned(view, &ov.OwnedDir); err != nil {
			if errors.Is(err, ErrNotManaged) {
				delete(r.Own.Views, folder)
			}
			dirty = true
			return finish(err)
		}
		res.CreatedView, dirty = true, true
	}
	dirty = true

	// Record intent: every temp name this run creates, then create them and
	// record their inodes, before anything is renamed into place.
	tempFor := map[string]string{}
	for _, name := range append(append([]string(nil), creates...), retargets...) {
		tmp := tempPrefix + randomSuffix()
		tempFor[name] = tmp
		t := &OwnedTemp{For: name, Target: valid[name].target, Text: valid[name].text}
		if l := ov.Links[name]; l != nil {
			t.OldIno = l.Ino
		}
		ov.Temps[tmp] = t
	}
	if len(tempFor) > 0 {
		if err := r.persist(); err != nil {
			return res, err
		}
		for _, name := range sortedKeys(tempFor) {
			tmp := tempFor[name]
			info, err := r.FS.Symlink(valid[name].text, view+"/"+tmp)
			switch {
			case err == nil:
				ov.Temps[tmp].Ino = info.Ino
			case errors.Is(err, fs.ErrExist):
				// A retried request whose first attempt committed reports
				// EEXIST: the random name then holds the Manager's own link
				// (same text). Anything else keeps its record, to be settled
				// like any temp that holds an entry the Manager did not make.
				if r.ownTemp(view+"/"+tmp, ov.Temps[tmp]) {
					if in, lerr := r.FS.Lstat(view + "/" + tmp); lerr == nil {
						ov.Temps[tmp].Ino = in.Ino
						continue
					}
				}
				errs = append(errs, fmt.Errorf("create %s: temp name taken: %w", name, err))
				delete(tempFor, name)
			default:
				errs = append(errs, fmt.Errorf("create %s: %w", name, err))
				delete(tempFor, name) // stays recorded; settled next run
			}
		}
		if err := r.persist(); err != nil {
			return res, err
		}
	}

	for _, name := range creates {
		tmp, ok := tempFor[name]
		if !ok {
			continue
		}
		t := ov.Temps[tmp]
		err := r.FS.Place(view+"/"+tmp, view+"/"+name)
		switch {
		case err == nil:
			ov.Links[name] = &OwnedLink{Target: t.Target, Text: t.Text, Ino: t.Ino}
			delete(ov.Temps, tmp)
			res.Created = append(res.Created, name)
		case errors.Is(err, fs.ErrExist):
			res.Refused = append(res.Refused, Refusal{name, t.Target, ErrNameTaken})
			if ferr := r.finishTemp(view, tmp, ov, &res); ferr != nil {
				errs = append(errs, ferr)
			}
		default:
			// The record stays; the next run inspects both names.
			errs = append(errs, fmt.Errorf("place %s: %w", name, err))
		}
	}
	for _, name := range retargets {
		tmp, ok := tempFor[name]
		if !ok {
			continue
		}
		if err := r.retarget(view, name, tmp, ov, &res); err != nil {
			errs = append(errs, fmt.Errorf("retarget %s: %w", name, err))
		}
	}
	for _, name := range removes {
		if err := r.removeLink(view, name, ov, &res); err != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", name, err))
		}
	}
	return finish(errors.Join(errs...))
}

// finishTemp settles one temp record by looking at what is under the temp
// name and the link name. The record is dropped only once the temp name is
// empty or the Manager's own entry there has been removed:
//
//   - the temp name holds the Manager's link (the temp link, the link it
//     replaced, or the link being removed): it is removed;
//   - it holds something else (a user's entry moved there by a rename or
//     exchange that raced them): it goes back to the link name, by
//     no-replace rename if the name is free, or by exchange if the name
//     holds the Manager's new link (which then lands under the temp name
//     and is removed). If neither is possible the record is kept and the
//     error names the hidden temp name.
func (r *Reconciler) finishTemp(view, tmp string, ov *OwnedView, res *Result) error {
	t := ov.Temps[tmp]
	tp, np := view+"/"+tmp, view+"/"+t.For
	if _, err := r.FS.Lstat(tp); errors.Is(err, fs.ErrNotExist) {
		delete(ov.Temps, tmp)
		return nil
	} else if err != nil {
		return err
	}
	if r.ownTemp(tp, t) {
		if err := r.FS.Remove(tp); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove temp %s: %w", tmp, err)
		}
		delete(ov.Temps, tmp)
		return nil
	}
	stuck := func(err error) error {
		return fmt.Errorf("an entry the Manager did not create is under the hidden name %s/%s (it belongs at %s): %w", view, tmp, t.For, err)
	}
	if !slices.Contains(res.Foreign, t.For) {
		res.Foreign = append(res.Foreign, t.For)
	}
	ninfo, nerr := r.FS.Lstat(np)
	switch {
	case errors.Is(nerr, fs.ErrNotExist):
		if err := r.FS.Place(tp, np); err != nil {
			return stuck(err)
		}
		delete(ov.Temps, tmp)
		delete(ov.Links, t.For)
		return nil
	case nerr == nil && ninfo.Type == TypeSymlink && t.Ino != 0 && ninfo.Ino == t.Ino:
		if err := r.FS.Swap(tp, np); err != nil {
			return stuck(err)
		}
		delete(ov.Links, t.For)
		// The temp name now holds the Manager's new link: remove it.
		if r.ownTemp(tp, t) {
			if err := r.FS.Remove(tp); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("remove temp %s: %w", tmp, err)
			}
		}
		if _, err := r.FS.Lstat(tp); errors.Is(err, fs.ErrNotExist) {
			delete(ov.Temps, tmp)
		}
		return nil
	case nerr != nil:
		return stuck(nerr)
	default:
		return stuck(fmt.Errorf("%s is taken", t.For))
	}
}

// removeLink removes the owned link `name` without ever unlinking anything
// else: the name is moved aside, without replacing, to a recorded random
// temp name; what landed there is then settled by finishTemp, which unlinks
// it only if it is the Manager's link and otherwise moves it back. On any
// error the record stays for the next run.
func (r *Reconciler) removeLink(view, name string, ov *OwnedView, res *Result) error {
	l := ov.Links[name]
	if _, ok := r.ownedLink(view+"/"+name, l); !ok {
		delete(ov.Links, name)
		res.Foreign = append(res.Foreign, name)
		return nil
	}
	tmp := tempPrefix + randomSuffix()
	// Recorded as the link itself, so the temp name holding it is the
	// Manager's entry whatever happens next.
	ov.Temps[tmp] = &OwnedTemp{For: name, Target: l.Target, Text: l.Text, Ino: l.Ino}
	if err := r.persist(); err != nil {
		delete(ov.Temps, tmp)
		return err
	}
	if err := r.FS.Place(view+"/"+name, view+"/"+tmp); err != nil {
		return err // the record stays; the next run inspects both names
	}
	if _, ok := r.ownedLink(view+"/"+tmp, l); ok {
		delete(ov.Links, name)
		if err := r.finishTemp(view, tmp, ov, res); err != nil {
			return err
		}
		res.Removed = append(res.Removed, name)
		return nil
	}
	delete(ov.Links, name)
	return r.finishTemp(view, tmp, ov, res)
}

// retarget replaces the owned link `name` with the temp link `tmp` by an
// atomic exchange, after re-checking the name right before it. Whatever
// the exchange left under the temp name is settled by finishTemp (the old
// link is removed; anything else is exchanged back). On any error the temp
// record stays for the next run.
func (r *Reconciler) retarget(view, name, tmp string, ov *OwnedView, res *Result) error {
	t := ov.Temps[tmp]
	old := ov.Links[name]
	if _, ok := r.ownedLink(view+"/"+name, old); !ok {
		delete(ov.Links, name)
		res.Foreign = append(res.Foreign, name)
		return r.finishTemp(view, tmp, ov, res)
	}
	if err := r.FS.Swap(view+"/"+tmp, view+"/"+name); err != nil {
		return err
	}
	// Look at both names: a retried exchange may have been applied twice.
	ninfo, nerr := r.FS.Lstat(view + "/" + name)
	switch {
	case nerr == nil && ninfo.Type == TypeSymlink && ninfo.Ino == t.Ino:
		// The new link holds the name. If the temp name got the old link,
		// done; otherwise finishTemp exchanges the other entry back.
		if info, err := r.FS.Lstat(view + "/" + tmp); err == nil && info.Type == TypeSymlink && info.Ino == old.Ino {
			ov.Links[name] = &OwnedLink{Target: t.Target, Text: t.Text, Ino: t.Ino}
			res.Retargeted = append(res.Retargeted, name)
		} else {
			delete(ov.Links, name)
		}
	case nerr == nil && ninfo.Type == TypeSymlink && ninfo.Ino == old.Ino:
		// Applied twice: nothing moved. The old link stays recorded; the
		// temp link is removed below and the next run retargets again.
	default:
		delete(ov.Links, name)
	}
	return r.finishTemp(view, tmp, ov, res)
}

// ownTemp reports whether the temp name holds a link the Manager made:
// the recorded temp inode, the replaced link's inode, or (before the inode
// was recorded) the recorded text under the random name only it knows.
func (r *Reconciler) ownTemp(p string, t *OwnedTemp) bool {
	info, err := r.FS.Lstat(p)
	if err != nil || info.Type != TypeSymlink {
		return false
	}
	if t.Ino != 0 {
		return info.Ino == t.Ino || (t.OldIno != 0 && info.Ino == t.OldIno)
	}
	text, err := r.FS.Readlink(p)
	return err == nil && text == t.Text
}

// taken reports whether something already holds p.
func (r *Reconciler) taken(p string) bool {
	_, err := r.FS.Lstat(p)
	return !errors.Is(err, fs.ErrNotExist)
}

// ownedLink checks that p is still the link the record describes: a symlink
// with the recorded inode and text. It returns the text; ok=false with a
// non-empty text means something else now holds the name.
func (r *Reconciler) ownedLink(p string, l *OwnedLink) (string, bool) {
	info, err := r.FS.Lstat(p)
	if err != nil {
		return "", false
	}
	if info.Type != TypeSymlink {
		return "?", false
	}
	text, err := r.FS.Readlink(p)
	if err != nil {
		return "?", false
	}
	if l == nil || info.Ino != l.Ino || text != l.Text {
		return text, false
	}
	return text, true
}

// checkTarget enforces the link-target rules: a canonical volume path with
// no hidden component (so never .trash or .juicemount), not under the smart
// root, reachable without crossing a symlink, and a regular file.
func (r *Reconciler) checkTarget(target string) error {
	parts, ok := cleanVolumePath(target)
	if !ok || strings.HasPrefix(target, "/") || len(parts) < 2 {
		return fmt.Errorf("%w: %q", ErrTargetForbidden, target)
	}
	if parts[0] == r.Root || reservedTopLevel[parts[0]] {
		return fmt.Errorf("%w: %q", ErrTargetForbidden, target)
	}
	for _, c := range parts {
		if isHiddenComponent(c) {
			return fmt.Errorf("%w: %q", ErrTargetForbidden, target)
		}
	}
	info, err := r.FS.Lstat(target)
	switch {
	case errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR):
		return fmt.Errorf("%w: %q", ErrTargetMissing, target)
	case errors.Is(err, ErrSymlinkInPath):
		return fmt.Errorf("%w: %q crosses a symlink", ErrTargetNotRegular, target)
	case err != nil:
		return err
	case info.Type != TypeRegular:
		return fmt.Errorf("%w: %q", ErrTargetNotRegular, target)
	}
	return nil
}

// relTarget is the symlink text for a link `depth` directories below the
// volume root: climb to the root, then descend to the target.
func relTarget(depth int, target string) string {
	return strings.Repeat("../", depth) + target
}

func randomSuffix() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
