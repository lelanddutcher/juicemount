package respproxy

// txnunit.go turns one decision unit (H8 plan §1.4 step 3) into the
// namespaces it writes.
//
// analyzeUnit reads only the unit: which dentry fields it sets or removes,
// which inodes' own keys it writes, which Attr values it stores.
// placeUnit then checks that against Redis (side reads, cached):
//
//   - every inode the unit writes is anchored to EVERY parent it has now
//     (Attr.Parent, or all p<n> fields for a hard link), never to a parent
//     the unit merely claims (review H8B-L2 #1/#2);
//   - an inode that does not exist yet must be created by this unit's own
//     dentry write, and is anchored there;
//   - a change of Attr.Parent must be matched by this unit's dentry writes:
//     the inode is placed in the new parent and its entry removed from the
//     stored one (rename, unlink into trash, restore);
//   - an existing inode placed in a second directory without leaving the
//     first is a hard link: files only, and all its parents must be in one
//     project;
//   - a trash entry must name an inode whose non-trash entry this unit
//     removes (same-inode trash pairing).
//
// Edges are learned only from committed dentry writes (ancestry.go).

import (
	"bytes"
	"strconv"
)

// unitEdge is a dentry placement (parent -> child) a unit writes. It is
// learned only after the unit commits.
type unitEdge struct {
	parent, child uint64
	isDir         bool
	name          string
}

// dentryOp is one field write on a d<dir> hash.
type dentryOp struct {
	dir   uint64
	name  string
	del   bool
	val   []byte // HSET value; nil for HDEL
	ino   uint64 // HSET: the inode the entry names
	isDir bool   // HSET: the entry's type
	trash bool   // dir is a trash directory
}

type unitFacts struct {
	dentries []dentryOp
	edges    []unitEdge
	// inodes whose own keys the unit writes (i, c, x, s, p, lockf, lockp),
	// plus inodes named by cleanup records (delfiles/detachedNodes members,
	// Unit.RefInodes). why[n] names the first such write (logs).
	inodes  []uint64
	why     map[uint64]string
	setAttr map[uint64]attrInfo // SET i<n> values in this unit
	links   map[uint64][]uint64 // p<n> fields (link parents) written
	// trashKey: the unit writes a trash inode's own key (not a dentry).
	trashKey string
	// dirStat: directory inodes named by dir-stat / quota hash fields.
	dirStat []uint64
	// topology: the unit moves a directory between two parents (rename,
	// exchange, restore from trash). It takes the proxy-wide topology lock
	// and bumps the project-cache generation when it commits.
	topology bool
	// unresolvable: writes the analysis cannot anchor at all.
	unresolvable []string
	// touched: every inode the unit names (invalidated after it ends).
	touched []uint64
}

// dirStatOnly: the unit writes nothing but dir-stat fields (the juicefs
// 1 s stat flush, doUpdateDirStat / doSyncDirStat). These never grant.
func (f *unitFacts) dirStatOnly() bool {
	return len(f.dentries) == 0 && len(f.inodes) == 0 && len(f.dirStat) > 0 &&
		len(f.unresolvable) == 0 && f.trashKey == ""
}

// attrInfo is what a marshalled JuiceFS Attr says about placement.
type attrInfo struct {
	typ    byte
	parent uint64
}

// parseAttr decodes the type and Attr.Parent of a JuiceFS v1.3.1 attr
// value (pkg/meta/interface.go Attr.Marshal): byte 1 holds the type in its
// top four bits; Parent is the big-endian uint64 at offset 63.
func parseAttr(v []byte) (attrInfo, bool) {
	p, ok := attrParent(v)
	if !ok {
		return attrInfo{}, false
	}
	return attrInfo{typ: v[1] >> 4, parent: p}, true
}

const (
	typeFile = 1 // meta.TypeFile
	typeDir  = 2 // meta.TypeDirectory
)

func isTrashIno(n uint64) bool { return n >= TrashInode }

// A completed sustained-file unlink removes the Attr before BG deletes its
// old chunk list. Only that deletion-only unit can name a confirmed-absent
// inode; failed placement reads still refuse. The later delfiles ZREM is a
// background key and does not enter this project-placement path.
func absentChunkCleanup(u *Unit, n uint64) bool {
	sawDelete := false
	for _, c := range u.Cmds {
		if c.Info.Class == ClassRead {
			continue // Read preconditions do not mutate state.
		}
		switch c.Info.Name {
		case "DEL":
			if len(c.Args) < 2 {
				return false
			}
			for _, raw := range c.Args[1:] {
				key := ParseKey(string(raw))
				if key.Shape != ShapeChunk || key.Ino != n {
					return false
				}
				sawDelete = true
			}
		case "HINCRBY":
			if len(c.Args) != 4 || string(c.Args[1]) != "sliceRef" {
				return false
			}
			delta, err := strconv.ParseInt(string(c.Args[3]), 10, 64)
			if err != nil || delta >= 0 {
				return false
			}
		default:
			return false
		}
	}
	return sawDelete
}

// analyzeUnit builds the facts of u from u.Project and the argv of their
// commands, without side reads.
func analyzeUnit(u *Unit) *unitFacts {
	f := &unitFacts{why: map[uint64]string{}, setAttr: map[uint64]attrInfo{}, links: map[uint64][]uint64{}}
	touched := map[uint64]bool{}
	touch := func(n uint64) {
		if !touched[n] {
			touched[n] = true
			f.touched = append(f.touched, n)
		}
	}
	addInode := func(n uint64, why string) {
		touch(n)
		if _, ok := f.why[n]; !ok {
			f.why[n] = why
			f.inodes = append(f.inodes, n)
		}
	}
	allDentry := map[uint64]bool{}
	dirHSetNonTrash := false
	for _, w := range u.Project {
		c := u.Cmds[w.Cmd]
		why := c.Info.Name + " " + w.Raw
		switch w.Key.Group {
		case "dentry":
			dir := w.Key.Ino
			allDentry[dir] = true
			touch(dir)
			switch c.Info.Name {
			case "HSET", "HMSET", "HSETNX":
				for i := 2; i+1 < len(c.Args); i += 2 {
					v := c.Args[i+1]
					ino, ok := entryInode(v)
					typ, _ := entryType(v)
					if !ok {
						f.unresolvable = append(f.unresolvable, why+": unparsable entry")
						continue
					}
					op := dentryOp{dir: dir, name: string(c.Args[i]), val: v, ino: ino, isDir: typ == typeDir, trash: w.Key.Trash}
					f.dentries = append(f.dentries, op)
					f.edges = append(f.edges, unitEdge{parent: dir, child: ino, isDir: op.isDir, name: op.name})
					touch(ino)
					if op.isDir && !w.Key.Trash {
						dirHSetNonTrash = true
					}
				}
			case "HDEL":
				for _, fld := range c.Args[2:] {
					f.dentries = append(f.dentries, dentryOp{dir: dir, name: string(fld), del: true, trash: w.Key.Trash})
				}
			default:
				f.unresolvable = append(f.unresolvable, why)
			}
		case "inode":
			n := w.Key.Ino
			if w.Key.Trash {
				if f.trashKey == "" {
					f.trashKey = why
				}
				continue
			}
			addInode(n, why)
			if w.Key.Shape == ShapeInode && (c.Info.Name == "SET" || c.Info.Name == "SETNX") && len(c.Args) >= 3 {
				if a, ok := parseAttr(c.Args[2]); ok {
					f.setAttr[n] = a
				} else {
					f.unresolvable = append(f.unresolvable, why+": unparsable attr")
				}
			}
			if w.Key.Shape == ShapeParent && len(c.Args) >= 3 {
				if p, ok := digits(string(c.Args[2])); ok {
					f.links[n] = append(f.links[n], p)
					touch(p)
				}
			}
		case "dirstat", "quota":
			f.dirStat = append(f.dirStat, unitFieldInodes(c.Info.Name, w.Key, c.Args)...)
		case "gc":
			// delfiles / detachedNodes members name an inode, which is
			// anchored like a written one; slice records are covered by
			// Unit.RefInodes below.
			for _, n := range unitFieldInodes(c.Info.Name, w.Key, c.Args) {
				if isTrashIno(n) {
					if f.trashKey == "" {
						f.trashKey = why
					}
					continue
				}
				addInode(n, why)
			}
		default:
			f.unresolvable = append(f.unresolvable, why)
		}
	}
	for _, n := range refInodes(u) {
		if isTrashIno(n) {
			if f.trashKey == "" {
				f.trashKey = "cleanup record of trash inode " + inoStr(n)
			}
			continue
		}
		addInode(n, "cleanup record references inode "+inoStr(n))
	}
	// A directory moved between two parents: a dir entry set on a non-trash
	// dentry while another dentry hash is written in the same unit.
	f.topology = dirHSetNonTrash && len(allDentry) >= 2

	// Dir-stat rows of an hourly trash directory created by this same txn
	// ride with the allow-listed creation (checkTrash's mknod, base.go:2488).
	if len(f.dirStat) > 0 {
		created := map[uint64]bool{}
		for _, w := range u.Writes {
			c := u.Cmds[w.Cmd]
			if w.Class != KCTrashCreate || w.Key.Shape != ShapeDentry || !isHSet(c.Info.Name) {
				continue
			}
			for i := 3; i < len(c.Args); i += 2 {
				if n, ok := entryInode(c.Args[i]); ok {
					created[n] = true
				}
			}
		}
		kept := f.dirStat[:0]
		for _, d := range f.dirStat {
			if !(isTrashIno(d) && created[d]) {
				kept = append(kept, d)
			}
		}
		f.dirStat = kept
	}
	// In a unit that also writes entries or inodes, dir-stat fields ride
	// along (mknod initialises the new dir's rows; rmdir drops them).
	if len(f.dentries) > 0 || len(f.inodes) > 0 {
		f.dirStat = nil
	}
	return f
}

// placed is one directory a unit places an inode in.
type placedIn struct {
	dir   uint64
	isDir bool
	trash bool
}

// placeUnit checks the unit against the stored tree and returns the
// location of every anchor. Admin/bg bypass policy restrictions, but an
// unknown location or failed authority read must still refuse.
func (e *LeaseEngine) placeUnit(u *Unit, f *unitFacts, bypass bool) ([]location, *Denial) {
	a := e.anc
	var locs []location
	refuse := func(d *Denial) *Denial {
		if bypass && d.Reason != ReasonUnknownKey {
			return nil
		}
		return d
	}
	unknown := func(detail string) *Denial { return refuse(&Denial{Reason: ReasonUnknownKey, Detail: detail}) }

	if f.trashKey != "" {
		if d := refuse(&Denial{Reason: ReasonBGOnly, Detail: "write to a trash inode's own key: " + f.trashKey}); d != nil {
			return nil, d
		}
	}

	removed := map[uint64][]uint64{} // inode -> dirs whose entry for it the unit removes or replaces
	placed := map[uint64][]placedIn{}
	var order []uint64
	seen := map[uint64]bool{}
	note := func(n uint64) {
		if !seen[n] {
			seen[n] = true
			order = append(order, n)
		}
	}
	for _, op := range f.dentries {
		if !op.trash {
			l, err := a.resolveDir(op.dir)
			if err != nil {
				if d := unknown("unresolved directory " + inoStr(op.dir) + ": " + err.Error()); d != nil {
					return nil, d
				}
				continue
			}
			locs = append(locs, l)
		}
		if op.del {
			ino, _, ok, err := a.entryAt(op.dir, op.name)
			if err != nil {
				if d := unknown("cannot read entry " + strconv.Quote(op.name)); d != nil {
					return nil, d
				}
				continue
			}
			if ok {
				removed[ino] = append(removed[ino], op.dir)
				note(ino)
			}
			continue
		}
		placed[op.ino] = append(placed[op.ino], placedIn{dir: op.dir, isDir: op.isDir, trash: op.trash})
		note(op.ino)
	}
	for _, n := range f.inodes {
		note(n)
	}

	stored := map[uint64]placement{}
	for _, n := range order {
		if isTrashIno(n) {
			continue
		}
		pl, err := a.placementOf(n)
		if err != nil {
			if d := unknown("cannot read inode " + inoStr(n) + ": " + err.Error()); d != nil {
				return nil, d
			}
			continue
		}
		stored[n] = pl
	}
	// An existing inode set over another entry replaces that entry (rename
	// onto an existing name, RENAME_EXCHANGE): the replaced inode leaves
	// that directory.
	for _, op := range f.dentries {
		if op.del || isTrashIno(op.ino) || !stored[op.ino].exists {
			continue
		}
		cur, _, ok, err := a.entryAt(op.dir, op.name)
		if err != nil {
			if d := unknown("cannot read entry " + strconv.Quote(op.name)); d != nil {
				return nil, d
			}
			continue
		}
		if ok && cur != op.ino {
			removed[cur] = append(removed[cur], op.dir)
			if !seen[cur] {
				seen[cur] = true
				order = append(order, cur)
				pl, err := a.placementOf(cur)
				if err != nil {
					return nil, unknown("cannot read replaced inode " + inoStr(cur) + ": " + err.Error())
				}
				stored[cur] = pl
			}
		}
	}

	nonTrash := func(ps []placedIn) []placedIn {
		var out []placedIn
		for _, p := range ps {
			if !p.trash {
				out = append(out, p)
			}
		}
		return out
	}
	hasDir := func(ds []uint64, d uint64) bool {
		for _, x := range ds {
			if x == d {
				return true
			}
		}
		return false
	}
	placedAt := func(ps []placedIn, d uint64) bool {
		for _, p := range ps {
			if p.dir == d {
				return true
			}
		}
		return false
	}
	removedFromNonTrash := func(n uint64) bool {
		for _, d := range removed[n] {
			if !isTrashIno(d) {
				return true
			}
		}
		return false
	}

	for _, n := range order {
		if isTrashIno(n) {
			continue
		}
		pl := stored[n]
		why := f.why[n]
		if why == "" {
			why = "entry for inode " + inoStr(n)
		}
		newHere := nonTrash(placed[n])
		if !pl.exists {
			if bypass && len(f.dentries) == 0 && len(f.setAttr) == 0 && len(placed[n]) == 0 &&
				f.trashKey == "" && absentChunkCleanup(u, n) {
				continue
			}
			if len(newHere) == 0 {
				if len(placed[n]) == 0 && len(f.why[n]) == 0 {
					continue // a removed entry that points nowhere
				}
				if d := unknown("write to inode " + inoStr(n) + " that does not exist and is not created here (" + why + ")"); d != nil {
					return nil, d
				}
				continue
			}
			for _, p := range newHere {
				l, err := a.childLocP(p.dir, n, p.isDir, true)
				if err != nil {
					if d := unknown("unresolved directory " + inoStr(p.dir)); d != nil {
						return nil, d
					}
					continue
				}
				locs = append(locs, l)
			}
			if at, ok := f.setAttr[n]; ok && at.parent != 0 && !placedAt(newHere, at.parent) {
				if d := unknown("new inode " + inoStr(n) + " claims a parent it is not created in"); d != nil {
					return nil, d
				}
			}
			continue
		}

		// Every parent it has now.
		sl, err := a.locsOf(n, pl)
		if err != nil {
			if d := unknown("unresolved placement of inode " + inoStr(n) + " (" + why + "): " + err.Error()); d != nil {
				return nil, d
			}
			continue
		}
		for i, l := range sl {
			if l.kind == nsTrash {
				// In the trash: only a restore (this unit removes it from
				// that trash directory and places it outside) may touch it.
				if !(hasDir(removed[n], pl.parents[i]) && len(newHere) > 0) {
					if d := refuse(&Denial{Reason: ReasonBGOnly, Detail: "write to inode " + inoStr(n) + " in ns:trash (" + why + ")"}); d != nil {
						return nil, d
					}
				}
				continue
			}
			locs = append(locs, l)
		}
		linkSet := append([]location(nil), sl...)

		// Placed somewhere new.
		if len(newHere) > 0 {
			moved := len(removed[n]) > 0
			if !moved {
				if pl.isDir {
					if d := unknown("directory " + inoStr(n) + " placed in a second directory without leaving the first"); d != nil {
						return nil, d
					}
				}
			}
			for _, p := range newHere {
				l, err := a.childLocP(p.dir, n, pl.isDir, true)
				if err != nil {
					if d := unknown("unresolved directory " + inoStr(p.dir)); d != nil {
						return nil, d
					}
					continue
				}
				locs = append(locs, l)
				if !moved {
					linkSet = append(linkSet, l)
				}
			}
			if !moved || pl.parent == 0 {
				if d := oneProject(linkSet); d != nil && !bypass {
					return nil, d
				}
			}
		} else if pl.parent == 0 {
			if d := oneProject(linkSet); d != nil && !bypass {
				return nil, d
			}
		}
		for _, lp := range f.links[n] {
			if isTrashIno(lp) {
				continue
			}
			l, err := a.childLoc(lp, n, false)
			if err != nil {
				if d := unknown("unresolved link parent " + inoStr(lp)); d != nil {
					return nil, d
				}
				continue
			}
			locs = append(locs, l)
		}

		// Attr.Parent changes must be matched by this unit's dentries.
		if at, ok := f.setAttr[n]; ok && at.parent != pl.parent {
			okChange := false
			switch {
			case at.parent == 0:
				okChange = !pl.isDir && (len(newHere) > 0 || len(f.links[n]) > 0)
			default:
				okChange = placedAt(placed[n], at.parent) && (pl.parent == 0 || hasDir(removed[n], pl.parent))
			}
			if !okChange {
				if d := unknown("Attr.Parent of inode " + inoStr(n) + " changed from " + inoStr(pl.parent) + " to " + inoStr(at.parent) + " without matching entries in this unit"); d != nil {
					return nil, d
				}
			}
		}
	}

	// Same-inode trash pairing: a trash entry must name an inode whose
	// non-trash entry this unit removes or replaces.
	for _, op := range f.dentries {
		if !op.trash || op.del {
			continue
		}
		if !removedFromNonTrash(op.ino) {
			if d := refuse(&Denial{Reason: ReasonBGOnly, Detail: "trash entry for inode " + inoStr(op.ino) + " that this unit does not remove"}); d != nil {
				return nil, d
			}
		}
	}
	// A trash entry removed without placing the inode elsewhere is a purge
	// (trash cleanup): bg only.
	for _, op := range f.dentries {
		if !op.trash || !op.del {
			continue
		}
		restore := false
		for n, ds := range removed {
			if hasDir(ds, op.dir) && len(nonTrash(placed[n])) > 0 {
				restore = true
			}
		}
		if !restore {
			if d := refuse(&Denial{Reason: ReasonBGOnly, Detail: "removing a trash entry without restoring it"}); d != nil {
				return nil, d
			}
		}
	}
	return locs, nil
}

// oneProject refuses a hard link whose parents span projects (plan §1.4
// step 3: "If the parents are in different projects, deny").
func oneProject(ls []location) *Denial {
	var first uint64
	for _, l := range ls {
		if l.kind != nsProject {
			continue
		}
		if first == 0 {
			first = l.proj
		} else if l.proj != first {
			return &Denial{Reason: ReasonXProject, Proj: l.proj, Detail: "hard link with parents in more than one project"}
		}
	}
	return nil
}

func inoStr(n uint64) string { return strconv.FormatUint(n, 10) }

// refInodes returns the inodes named by the unit's shared cleanup records
// (delfiles/detachedNodes members, and the chunk inode owning every slice a
// sliceRef/delSlices record names), as resolved by L1's guard. Each one is
// anchored like a written inode, so its project's lease is required.
func refInodes(u *Unit) []uint64 { return u.RefInodes }

// unitFieldInodes (lane L2's copy of L1's former fieldInodes) extracts the inodes named in the fields/members of a write on
// a global hash or set. dirstat hashes are keyed by the directory inode in
// the field; delfiles members are "<ino>:<length>"; detachedNodes members
// and locked<sid> set members are the inode.
func unitFieldInodes(name string, key KeyRef, args [][]byte) []uint64 {
	var out []uint64
	add := func(b []byte) {
		if i := bytes.IndexByte(b, ':'); i > 0 {
			b = b[:i]
		}
		if n, ok := digits(string(b)); ok {
			out = append(out, n)
		}
	}
	switch {
	case (key.Group == "dirstat" || key.Group == "quota") &&
		(name == "HINCRBY" || name == "HINCRBYFLOAT" || name == "HSET" || name == "HMSET" || name == "HDEL" || name == "HSETNX"):
		step := 1
		if name == "HSET" || name == "HMSET" {
			step = 2
		}
		if name == "HINCRBY" || name == "HINCRBYFLOAT" || name == "HSETNX" {
			if len(args) > 2 {
				add(args[2])
			}
			return out
		}
		for i := 2; i < len(args); i += step {
			add(args[i])
		}
	case (key.Shape == "delfiles" || key.Shape == "detachedNodes") && name == "ZADD":
		for i := 3; i < len(args); i += 2 {
			add(args[i])
		}
	case (key.Shape == "delfiles" || key.Shape == "detachedNodes") && name == "ZREM":
		for _, a := range args[2:] {
			add(a)
		}
	case key.Shape == ShapeLocked && (name == "SADD" || name == "SREM"):
		for _, a := range args[2:] {
			add(a)
		}
	}
	return out
}
