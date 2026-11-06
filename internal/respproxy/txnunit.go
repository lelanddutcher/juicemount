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
//   - an inode that does not exist yet must be created by this unit: one
//     dentry plus its SET i<n> of the same type with Attr.Parent = that
//     directory, numbered from the writer's own INCRBY nextinode batch, and
//     is anchored there;
//   - a change of Attr.Parent must be matched by this unit's dentry writes:
//     the inode is placed in the new parent and its entry removed from the
//     stored one (rename, unlink into trash, restore);
//   - an existing inode placed in a second directory without leaving the
//     first is a hard link: files only, and all its parents must be in one
//     project;
//   - a trash entry must name an inode whose non-trash entry this unit
//     removes (same-inode trash pairing), and never replaces an entry
//     naming another inode; entries outside the trash never name a trash
//     inode, and the trash root is never written here.
//
// Edges are learned only from committed dentry writes (ancestry.go).

import (
	"bytes"
	"strconv"
	"strings"
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
	typ   byte   // HSET: the entry's type
	isDir bool   // HSET: typ == typeDir
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
	// quota: directory inodes whose quota definition (dirQuota) the unit
	// writes. Unlike dirStat it is kept when the unit writes more.
	quota []uint64
	// statRows: every dir-stat, quota and deferred-delete field the unit
	// writes, kept (unlike dirStat) when the unit writes more: the grant
	// check places each one and the audit log lists them.
	statRows []statRow
	// topology: the unit moves a directory between two parents (rename,
	// exchange, restore from trash). It takes the proxy-wide topology lock
	// and bumps the project-cache generation when it commits.
	topology bool
	// unresolvable: writes the analysis cannot anchor at all.
	unresolvable []string
	// touched: every inode the unit names (invalidated after it ends).
	touched []uint64
	// unlinked (placeUnit): inodes whose entry the unit removes or
	// replaces (smartview.go forgets them as views once it committed).
	unlinked []uint64
}

// statRow is one field of a volume-wide accounting hash: a dir-stat or
// quota-usage counter, a quota definition (dirQuota), or a deferred-delete
// record (delfiles, detachedNodes; dir is then the inode it names).
type statRow struct {
	hash string // the key: dirUsedSpace, dirQuotaUsedInodes, dirQuota, delfiles, ...
	dir  uint64
	op   string // set, incr or del; add or remove for delfiles / detachedNodes
	val  string // set: the value; incr: the delta
}

// quotaUsage: the hash holds the usage of a quota (juicefs checks a write
// against it, so an inflated value refuses writes with EDQUOT).
func (r statRow) quotaUsage() bool {
	return r.hash == "dirQuotaUsedSpace" || r.hash == "dirQuotaUsedInodes"
}

// gc: a deferred-delete record (its inode is placed like a written one).
func (r statRow) gc() bool { return r.hash == "delfiles" || r.hash == "detachedNodes" }

// statRowsOf lists the fields one dir-stat, quota or gc command writes
// (the fields unitFieldInodes names, with what is written).
func statRowsOf(name string, key KeyRef, args [][]byte) []statRow {
	field := func(b []byte) (uint64, bool) {
		if i := bytes.IndexByte(b, ':'); i > 0 {
			b = b[:i]
		}
		return digits(string(b))
	}
	val := func(b []byte) string {
		if len(b) > 64 {
			return string(b[:64]) + "..."
		}
		return string(b)
	}
	var out []statRow
	add := func(b []byte, op, v string) {
		if n, ok := field(b); ok {
			out = append(out, statRow{hash: key.Shape, dir: n, op: op, val: v})
		}
	}
	switch {
	case key.Group == "dirstat" || key.Group == "quota":
		switch name {
		case "HINCRBY", "HINCRBYFLOAT", "HSETNX":
			op := "incr"
			if name == "HSETNX" {
				op = "set"
			}
			if len(args) > 3 {
				add(args[2], op, val(args[3]))
			} else if len(args) > 2 {
				add(args[2], op, "")
			}
		case "HSET", "HMSET":
			for i := 2; i < len(args); i += 2 {
				v := ""
				if i+1 < len(args) {
					v = val(args[i+1])
				}
				add(args[i], "set", v)
			}
		case "HDEL":
			for _, a := range args[2:] {
				add(a, "del", "")
			}
		}
	case (key.Shape == "delfiles" || key.Shape == "detachedNodes") && name == "ZADD":
		for i := 3; i < len(args); i += 2 {
			add(args[i], "add", "")
		}
	case (key.Shape == "delfiles" || key.Shape == "detachedNodes") && name == "ZREM":
		for _, a := range args[2:] {
			add(a, "remove", "")
		}
	}
	return out
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
					op := dentryOp{dir: dir, name: string(c.Args[i]), val: v, ino: ino, typ: typ, isDir: typ == typeDir, trash: w.Key.Trash}
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
			ns := unitFieldInodes(c.Info.Name, w.Key, c.Args)
			f.dirStat = append(f.dirStat, ns...)
			f.statRows = append(f.statRows, statRowsOf(c.Info.Name, w.Key, c.Args)...)
			if w.Key.Group == "quota" {
				f.quota = append(f.quota, ns...)
			}
		case "gc":
			// delfiles / detachedNodes members name an inode, which is
			// anchored like a written one; slice records are covered by
			// Unit.RefInodes below.
			f.statRows = append(f.statRows, statRowsOf(c.Info.Name, w.Key, c.Args)...)
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
	if len(f.statRows) > 0 {
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
		rows := f.statRows[:0]
		for _, r := range f.statRows {
			if r.gc() || !(isTrashIno(r.dir) && created[r.dir]) {
				rows = append(rows, r)
			}
		}
		f.statRows = rows
	}
	// In a unit that also writes entries or inodes, dir-stat fields ride
	// along for the lease rules (mknod initialises the new dir's rows;
	// rmdir drops them). The grant check still places every one (statRows).
	if len(f.dentries) > 0 || len(f.inodes) > 0 {
		f.dirStat = nil
	}
	return f
}

// placed is one directory a unit places an inode in.
type placedIn struct {
	dir   uint64
	typ   byte
	isDir bool
	trash bool
}

// placeMode is how placeUnit treats the refusals of the lease rules.
type placeMode int

const (
	// placeStrict: every refusal (a Mac, a manager or farm unit in
	// enforce and observe).
	placeStrict placeMode = iota
	// placeBypass: admin, bg and passthrough. Only an unknown location or
	// a failed authority read refuses.
	placeBypass
	// placeGrant: the person-grant check (grants.go), in every mode. It
	// keeps every refusal except XPROJECT for a hard link across projects,
	// which is a lease rule: the trash rules (no write to a trashed inode,
	// trash pairing, no purge) hold even in passthrough.
	placeGrant
)

// placeUnit checks the unit of principal p against the stored tree and
// returns the location of every anchor (see placeMode for which refusals
// apply). Admin/bg bypass policy restrictions, but an unknown location or
// failed authority read must still refuse.
func (e *LeaseEngine) placeUnit(p Principal, u *Unit, f *unitFacts, mode placeMode) ([]location, *Denial) {
	a := e.anc
	bypass := mode == placeBypass
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
	delIno := map[int]uint64{}       // f.dentries index of an HDEL -> the inode its entry named
	placed := map[uint64][]placedIn{}
	var order []uint64
	seen := map[uint64]bool{}
	note := func(n uint64) {
		if !seen[n] {
			seen[n] = true
			order = append(order, n)
		}
	}
	for i, op := range f.dentries {
		// bg's trash cleanup deletes everything below an expired entry of
		// the trash root, so the root holds only the hourly directories
		// checkTrash creates (the guard's KCTrashCreate, never a project
		// write), and a trash inode is reachable only through the trash.
		if op.dir == TrashInode {
			if d := refuse(&Denial{Reason: ReasonBGOnly, Detail: "trash root entry " + strconv.Quote(op.name) + " outside an hourly directory create"}); d != nil {
				return nil, d
			}
		}
		if !op.del && !op.trash && isTrashIno(op.ino) {
			if d := refuse(&Denial{Reason: ReasonBGOnly, Detail: "entry " + strconv.Quote(op.name) + " outside the trash names trash inode " + inoStr(op.ino)}); d != nil {
				return nil, d
			}
		}
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
				delIno[i] = ino
				note(ino)
				f.unlinked = append(f.unlinked, ino)
			}
			continue
		}
		placed[op.ino] = append(placed[op.ino], placedIn{dir: op.dir, typ: op.typ, isDir: op.isDir, trash: op.trash})
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
	// A trash entry is only ever added: juicefs names it
	// <parent>-<inode>-<name> (trashEntry), so the one field it can find
	// set already names the same inode (a hard link trashed again under the
	// same name). Every trashed inode keeps its entry until bg's cleanup or
	// a restore removes it.
	for _, op := range f.dentries {
		if !op.trash || op.del {
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
			if d := refuse(&Denial{Reason: ReasonBGOnly, Detail: "trash entry " + strconv.Quote(op.name) + " replaces the entry for inode " + inoStr(cur)}); d != nil {
				return nil, d
			}
		}
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
			f.unlinked = append(f.unlinked, cur)
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
			if !bypass {
				if d := newInodeGate(p, u, n, newHere, f.setAttr); d != nil {
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
				if d := oneProject(linkSet); d != nil && mode == placeStrict {
					return nil, d
				}
			}
		} else if pl.parent == 0 {
			if d := oneProject(linkSet); d != nil && mode == placeStrict {
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
	// non-trash entry this unit removes or replaces, under juicefs's name
	// for it, <directory>-<inode>-<name> (trashEntry), whose directory is
	// one it is removed from. That name is what a restore reads back.
	for _, op := range f.dentries {
		if !op.trash || op.del {
			continue
		}
		named := false
		if dir, ino, ok := trashEntryOrigin(op.name); ok && ino == op.ino && !isTrashIno(dir) {
			named = hasDir(removed[op.ino], dir)
		}
		if !removedFromNonTrash(op.ino) || !named {
			if d := refuse(&Denial{Reason: ReasonBGOnly, Detail: "trash entry " + strconv.Quote(op.name) + " for inode " + inoStr(op.ino) + " is not named for an entry this unit removes"}); d != nil {
				return nil, d
			}
		}
	}
	// A trash entry removed without placing the inode elsewhere is a purge
	// (trash cleanup): bg only. A restore places it outside the trash, and
	// is anchored in the directory the entry was trashed from as well as
	// its destination: the inode still belongs to that directory's project.
	for i, op := range f.dentries {
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
			continue
		}
		if bypass {
			continue
		}
		n, gone := delIno[i]
		dir, ino, ok := trashEntryOrigin(op.name)
		if !gone || !ok || ino != n {
			return nil, &Denial{Reason: ReasonBGOnly, Detail: "trash entry " + strconv.Quote(op.name) + " does not name the directory it was trashed from"}
		}
		l, err := a.resolveDir(dir)
		if err != nil {
			return nil, &Denial{Reason: ReasonUnknownKey, Detail: "unresolved original directory " + inoStr(dir) + " of a trash entry: " + err.Error()}
		}
		locs = append(locs, l)
	}
	return locs, nil
}

// trashEntryOrigin parses juicefs's trash entry name
// "<directory>-<inode>-<name>" (base.go trashEntry).
func trashEntryOrigin(name string) (dir, ino uint64, ok bool) {
	parts := strings.SplitN(name, "-", 3)
	if len(parts) != 3 {
		return 0, 0, false
	}
	dir, ok1 := digits(parts[0])
	ino, ok2 := digits(parts[1])
	return dir, ino, ok1 && ok2
}

// newInodeGate: an entry naming an inode that does not exist yet creates it,
// as juicefs's mknod / create / mkdir / symlink and clone do (redis.go
// doMknod, doCloneEntry): exactly one entry, the inode's SET i<n> in the same
// unit with the entry's type and Attr.Parent = that directory, and a number
// from p's own INCRBY nextinode batch (Unit.inodeOwner). So an entry always
// names an inode its writer created, never a number that may be handed to
// another principal, and what bg's trash cleanup deletes below a directory
// is what that directory's project created or linked.
//
// A number at or below Unit.inodeMark with no recorded owner was handed out
// before the proxy recorded batches; a mount still holding such a batch
// creates from it with the structural rules alone. Each such number names
// at most one inode, created by whoever creates it first, and every batch
// taken after recording began is recorded.
func newInodeGate(p Principal, u *Unit, n uint64, newHere []placedIn, attrs map[uint64]attrInfo) *Denial {
	refuse := func(r Reason, detail string) *Denial {
		return &Denial{Reason: r, Detail: "new inode " + inoStr(n) + ": " + detail}
	}
	at, ok := attrs[n]
	switch {
	case !ok:
		return refuse(ReasonUnknownKey, "an entry names it but this unit does not create it")
	case len(newHere) != 1 || at.parent != newHere[0].dir:
		return refuse(ReasonUnknownKey, "not created in exactly one directory, its Attr.Parent")
	case at.typ != newHere[0].typ:
		return refuse(ReasonUnknownKey, "its attr and its entry differ in type")
	}
	if u.inodeOwner == nil {
		return refuse(ReasonAdminOnly, "no inode allocation record")
	}
	owner, known, err := u.inodeOwner(n)
	switch {
	case err != nil:
		return refuse(ReasonBGOnly, "inode allocation record: "+err.Error())
	case known && owner != p.Name:
		return refuse(ReasonAdminOnly, "from another principal's INCRBY nextinode")
	case known:
		return nil
	}
	if u.inodeMark == nil {
		return refuse(ReasonAdminOnly, "no inode allocation record")
	}
	mark, err := u.inodeMark()
	if err != nil {
		return refuse(ReasonBGOnly, "inode allocation mark: "+err.Error())
	}
	if n > mark {
		return refuse(ReasonAdminOnly, "not from this principal's INCRBY nextinode")
	}
	return nil
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
