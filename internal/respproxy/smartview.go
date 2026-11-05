package respproxy

// smartview.go is the one exception to the root-entry gate (decide.go
// rootEntryGate) that is not the admin: the Manager's server principal may
// remove an EMPTY view directory it created directly under one of its
// smart-folder roots (/Recents/<view>, /Collections/<view>;
// LeaseConfig.SmartRoots). Such a view is a project root, so without this
// only an admin could remove it, and the Manager would leave every emptied
// view behind. Every other removal or rename of a directory above the
// project depth stays admin-only.
//
// Smart roots. P is a smart root while a configured name names it in the
// volume root and its inode came from the manager principal's own INCRBY
// nextinode batch (Guard.inodes): a top-level folder of that name made by
// anyone else never qualifies. d1 is not WATCHed by these units, so the
// name is read as it is at decision time; that is sound because renaming
// or removing an entry of the volume root is itself admin-only here.
//
// The view record. The inode-batch owner alone is not enough to tell a
// view: everything the Manager's principal creates counts (migration sync
// jobs run under it too), and a lease holder can move an empty folder into
// a smart root and give it any mode. So the proxy keeps its own record,
// SmartViewsKey (a set of inodes, under jmlease:, which only the admin may
// write), and adds N to it when a unit of the manager principal that
// created N, a new directory placed directly in a smart root P owned by
// that principal, committed (smartViewCreation). A directory moved into a
// smart root, or created there before this record existed, is never in it
// and is never removable this way. A view leaves the record when any unit
// (of any principal) that removes or replaces its entry, or deletes its
// inode, committed or may have (forgetSmartViews): a removal, a rename out
// of the root or within it, a move to the trash. A view renamed out and
// moved back is not a view again. At start the proxy drops records whose
// inode is gone or no longer a directory directly in a smart root
// (loadSmartViews, at most smartPruneMax checks).
//
// The removal unit must be exactly juicefs's rmdir without the trash
// (redis.go doRmdir with skipCheckTrash, which the Manager's metadata
// client uses), and nothing else:
//
//	WATCH i<P> d<P> ... WATCH i<N> d<N> ... MULTI
//	HDEL d<P> <name>; SET i<P> <attr>; DEL i<N>; DEL x<N>;
//	INCRBY usedSpace -4096; DECR totalInodes;
//	HDEL dirDataLength|dirUsedSpace|dirUsedInodes|dirQuota|
//	     dirQuotaUsedSpace|dirQuotaUsedInodes <N>
//	EXEC
//
// and, read through the side connection once the connection's WATCH has
// reached Redis (Unit.beforeSide), so any change before EXEC aborts it:
//
//   - P is a smart root (above);
//   - <name> in P names the directory N, whose Attr.Parent is P;
//   - d<N> is empty (juicefs's own ENOTEMPTY check, HLEN, repeated here);
//   - the new attr of P is the stored one with only its link count one
//     lower and its change and modify times new, within smartViewSkew of
//     Redis TIME;
//   - N came from the manager principal's own inode batch and is in the
//     view record.
//
// A removal that committed is logged to jmlease:v1:log (event
// smart_view_remove) and, like every admitted namespace unit, to the
// per-person audit log. Like any write in a free project, the removal
// grants the manager principal the view's lease (implicitly); that lease
// on a directory that no longer exists stays until it expires idle.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

// SmartViewsKey is the proxy's record of the smart-folder view directories
// the manager principal created (a set of inode numbers).
const SmartViewsKey = "jmlease:v1:smartviews"

// smartPruneMax bounds the records loadSmartViews checks at start.
const smartPruneMax = 4096

// smartViewSkew bounds how far the times rmdir writes into a smart root
// may be from Redis TIME (the writer's clock names them).
const smartViewSkew = time.Hour

// CleanSmartRoots returns the valid smart-folder root names among names,
// checked as the Manager checks them (one visible path component, not a
// reserved namespace), and an error for each invalid one. An invalid name
// is left out: it only narrows the exception. A name equal to an earlier
// one under case folding (strings.EqualFold, as the Manager compares the
// collection views root with the Recents root, turning the later one off)
// is left out too.
func CleanSmartRoots(names []string) ([]string, []error) {
	var ok []string
	var bad []error
	for _, n := range names {
		if err := smartfolders.ValidateRootName(n); err != nil {
			bad = append(bad, err)
			continue
		}
		dup := false
		for _, o := range ok {
			dup = dup || strings.EqualFold(n, o)
		}
		if !dup {
			ok = append(ok, n)
		}
	}
	return ok, bad
}

// smartViewDirStatHashes are the per-directory rows juicefs's rmdir drops.
var smartViewDirStatHashes = []string{"dirDataLength", "dirUsedSpace", "dirUsedInodes",
	"dirQuota", "dirQuotaUsedSpace", "dirQuotaUsedInodes"}

// Attr layout (pkg/meta/interface.go Attr.Marshal): the modify and change
// times are bytes [23,47), the link count [47,51); the rest of a 72-byte
// attr (80 with ACLs) is flags, mode, owner, atime, length, rdev, parent.
const (
	attrTimesFrom = 23
	attrNlinkAt   = 47
	attrNlinkEnd  = 51
	attrMinLen    = 72
)

// smartViewRemoval checks that the unit of p removing entry op (stored
// value stored, a directory) of a directory above the project depth is the
// Manager's rmdir of an empty smart-folder view (see the file comment). It
// returns what to run once the removal committed, or why not.
func (e *LeaseEngine) smartViewRemoval(p Principal, u *Unit, op dentryOp, stored []byte) (func(), string) {
	if p.Kind != KindManager {
		return nil, "not the manager principal"
	}
	if len(e.cfg.SmartRoots) == 0 {
		return nil, "no smart-folder roots configured"
	}
	if u.Kind != UnitTxn || u.DB != e.cfg.VolumeDB {
		return nil, "not a transaction on the volume"
	}
	n, ok := entryInode(stored)
	if !ok || !op.del || op.trash {
		return nil, "not the removal of a directory entry"
	}
	pd, nd := inoStr(op.dir), inoStr(n)

	// The commands: exactly juicefs's rmdir of <name> in P, and nothing else.
	want := map[string]int{
		rmdirCmdKey("HDEL", "d"+pd, op.name):        1,
		rmdirCmdKey("DEL", "i"+nd):                  1,
		rmdirCmdKey("DEL", "x"+nd):                  1,
		rmdirCmdKey("INCRBY", "usedSpace", "-4096"): 1,
		rmdirCmdKey("DECR", "totalInodes"):          1,
	}
	for _, h := range smartViewDirStatHashes {
		want[rmdirCmdKey("HDEL", h, nd)] = 1
	}
	var newAttr []byte
	for _, c := range u.Cmds {
		if c.Info.Name == "SET" && len(c.Args) == 3 && string(c.Args[1]) == "i"+pd && newAttr == nil {
			newAttr = c.Args[2]
			continue
		}
		args := make([]string, 0, len(c.Args))
		for _, a := range c.Args[min(1, len(c.Args)):] {
			args = append(args, string(a))
		}
		k := rmdirCmdKey(c.Info.Name, args...)
		if want[k] == 0 {
			return nil, "the unit is not exactly an rmdir: " + sanitizeToken(c.Info.Name)
		}
		want[k]--
	}
	for _, left := range want {
		if left != 0 {
			return nil, "the unit is not exactly an rmdir"
		}
	}
	if newAttr == nil {
		return nil, "the unit is not exactly an rmdir: no SET i" + pd
	}

	// The reads below are covered by the WATCH juicefs's rmdir takes.
	watched := map[string]bool{}
	for _, k := range u.Watched {
		watched[k] = true
	}
	for _, k := range []string{"i" + pd, "d" + pd, "i" + nd, "d" + nd} {
		if !watched[k] {
			return nil, k + " is not WATCHed"
		}
	}
	if u.beforeSide == nil {
		return nil, "no WATCH on the connection"
	}
	if err := u.beforeSide(); err != nil {
		return nil, "WATCH not confirmed before side read: " + err.Error()
	}
	read := func(args ...string) (Value, error) {
		e.anc.sideReads.Add(1)
		return e.side(args...)
	}

	root, why := e.smartRootOf(p, u, op.dir)
	if why != "" {
		return nil, why
	}

	// <name> names the directory N, placed in P.
	v, err := read("HGET", "d"+pd, op.name)
	if err != nil || v.Null || !bytes.Equal(v.Str, stored) {
		return nil, "the entry changed"
	}
	if v, err = read("GET", "i"+nd); err != nil || v.Null {
		return nil, "cannot read inode " + nd
	}
	if a, ok := parseAttr(v.Str); !ok || a.typ != typeDir || a.parent != op.dir {
		return nil, "inode " + nd + " is not a directory in " + pd
	}
	// Empty.
	if v, err = read("HLEN", "d"+nd); err != nil || v.Type != ':' {
		return nil, "cannot read directory " + nd
	}
	if v.Int != 0 {
		return nil, "directory " + nd + " is not empty"
	}
	// P's attr changes only as rmdir changes it.
	if v, err = read("GET", "i"+pd); err != nil || v.Null {
		return nil, "cannot read inode " + pd
	}
	if !rmdirParentAttr(v.Str, newAttr) {
		return nil, "the new attr of " + pd + " changes more than its link count and times"
	}
	if why := e.attrTimesNow(newAttr); why != "" {
		return nil, "the new attr of " + pd + ": " + why
	}

	// Created by this principal, as a view.
	if why := ownedBy(p, u, n); why != "" {
		return nil, why
	}
	if v, err = read("SISMEMBER", SmartViewsKey, nd); err != nil || v.Type != ':' {
		return nil, "cannot read the view record"
	}
	if v.Int != 1 {
		return nil, "directory " + nd + " is not a recorded smart-folder view"
	}
	name := op.name
	return func() {
		e.dropSmartView(n)
		e.queueLog(logEntry{"event", "smart_view_remove", "proj", nd, "actor", p.Name,
			"kind", string(p.Kind), "root", sanitizeToken(root), "view", sanitizeToken(name),
			"ts_ms", strconv.FormatInt(ms(e.now()), 10)})
	}, ""
}

// smartViewCreation recognises a unit of the manager principal that
// creates a view: a new directory N (an inode from p's own batch that does
// not exist yet, its SET i<N> a directory under P) placed by the unit's
// only entry write directly in a smart root P (smartRootOf). It returns
// what to run once the unit committed: add N to the view record. Any other
// unit returns nil.
func (e *LeaseEngine) smartViewCreation(p Principal, u *Unit, f *unitFacts) func() {
	if len(e.cfg.SmartRoots) == 0 || u.DB != e.cfg.VolumeDB || len(f.dentries) != 1 {
		return nil
	}
	op := f.dentries[0]
	if op.del || op.trash || !op.isDir {
		return nil
	}
	if a, ok := f.setAttr[op.ino]; !ok || a.typ != typeDir || a.parent != op.dir {
		return nil
	}
	if loc, err := e.anc.resolveDir(op.dir); err != nil || loc.kind != nsRoot || loc.depth != 1 {
		return nil
	}
	root, why := e.smartRootOf(p, u, op.dir)
	if why != "" || ownedBy(p, u, op.ino) != "" {
		return nil
	}
	nd := inoStr(op.ino)
	e.anc.sideReads.Add(1)
	if v, err := e.side("EXISTS", "i"+nd); err != nil || v.Int != 0 {
		return nil // not a new directory
	}
	name := op.name
	return func() {
		if _, err := e.side("SADD", SmartViewsKey, nd); err != nil {
			e.logf("respproxy: smart-folder view record: SADD %s: %v (the view cannot be removed by the Manager)", nd, err)
			return
		}
		e.smartMu.Lock()
		e.smartViews[op.ino] = true
		e.smartMu.Unlock()
		e.queueLog(logEntry{"event", "smart_view_create", "proj", nd, "actor", p.Name,
			"kind", string(p.Kind), "root", sanitizeToken(root), "view", sanitizeToken(name),
			"ts_ms", strconv.FormatInt(ms(e.now()), 10)})
	}
}

// forgetSmartViews drops from the record every view whose entry the unit
// removes or replaces, or whose inode it deletes. Called once the unit
// committed or may have.
func (e *LeaseEngine) forgetSmartViews(u *Unit, f *unitFacts) {
	cand := f.unlinked
	for _, w := range u.Project {
		if w.Key.Shape == ShapeInode && w.Key.HasIno {
			switch u.Cmds[w.Cmd].Info.Name {
			case "DEL", "UNLINK", "GETDEL":
				cand = append(cand, w.Key.Ino)
			}
		}
	}
	if len(cand) == 0 {
		return
	}
	var drop []uint64
	e.smartMu.Lock()
	for _, n := range cand {
		if e.smartViews[n] {
			drop = append(drop, n)
		}
	}
	e.smartMu.Unlock()
	for _, n := range drop {
		e.dropSmartView(n)
	}
}

// dropSmartView removes n from the record. If Redis refuses, the entry
// stays in the mirror and the next start's check drops it once the view
// is gone or out of its root.
func (e *LeaseEngine) dropSmartView(n uint64) {
	if _, err := e.side("SREM", SmartViewsKey, inoStr(n)); err != nil {
		e.logf("respproxy: smart-folder view record: SREM %d: %v", n, err)
		return
	}
	e.smartMu.Lock()
	delete(e.smartViews, n)
	e.smartMu.Unlock()
}

// loadSmartViews reads the record into the mirror and drops (at most
// smartPruneMax checks) every entry whose inode is gone, is not a
// directory, or, while smart roots are configured, does not sit directly
// in one of them.
func (e *LeaseEngine) loadSmartViews() error {
	views := map[uint64]bool{}
	cursor := "0"
	for {
		v, err := e.side("SSCAN", SmartViewsKey, cursor, "COUNT", "1000")
		if err != nil || len(v.Elems) != 2 {
			return fmt.Errorf("SSCAN %s: %v", SmartViewsKey, err)
		}
		for _, m := range v.Elems[1].Elems {
			if n, ok := digits(string(m.Str)); ok {
				views[n] = true
			}
		}
		if cursor = string(v.Elems[0].Str); cursor == "0" {
			break
		}
	}
	roots := map[uint64]bool{}
	if len(e.cfg.SmartRoots) > 0 {
		v, err := e.side(append([]string{"HMGET", "d" + inoStr(rootIno)}, e.cfg.SmartRoots...)...)
		if err != nil {
			return fmt.Errorf("read the smart-folder roots: %v", err)
		}
		for _, el := range v.Elems {
			if t, ok := entryType(el.Str); ok && t == typeDir && !el.Null {
				if ino, ok := entryInode(el.Str); ok {
					roots[ino] = true
				}
			}
		}
	}
	checked, dropped := 0, 0
	for n := range views {
		if checked >= smartPruneMax {
			e.logf("respproxy: smart-folder view record: %d entries, checked %d at start", len(views), checked)
			break
		}
		checked++
		v, err := e.side("GET", "i"+inoStr(n))
		if err != nil {
			return fmt.Errorf("GET i%d: %v", n, err)
		}
		a, ok := parseAttr(v.Str)
		if !v.Null && ok && a.typ == typeDir && (len(e.cfg.SmartRoots) == 0 || roots[a.parent]) {
			continue
		}
		if _, err := e.side("SREM", SmartViewsKey, inoStr(n)); err != nil {
			return fmt.Errorf("SREM %s %d: %v", SmartViewsKey, n, err)
		}
		delete(views, n)
		dropped++
	}
	if dropped > 0 {
		e.logf("respproxy: smart-folder view record: dropped %d entries of views gone or out of their root", dropped)
	}
	e.smartMu.Lock()
	e.smartViews = views
	e.smartMu.Unlock()
	return nil
}

// smartRootOf reports which configured smart root the directory dir is:
// the one a configured name names in the volume root now, with an inode
// from p's own batch. d1 is not WATCHed; renaming or removing a root entry
// is admin-only, so only the admin can change what a name names.
func (e *LeaseEngine) smartRootOf(p Principal, u *Unit, dir uint64) (string, string) {
	e.anc.sideReads.Add(1)
	v, err := e.side(append([]string{"HMGET", "d" + inoStr(rootIno)}, e.cfg.SmartRoots...)...)
	if err != nil || len(v.Elems) != len(e.cfg.SmartRoots) {
		return "", "cannot read the smart-folder roots"
	}
	root := ""
	for i, el := range v.Elems {
		if el.Null {
			continue
		}
		t, tok := entryType(el.Str)
		ino, iok := entryInode(el.Str)
		if tok && iok && t == typeDir && ino == dir {
			root = e.cfg.SmartRoots[i]
			break
		}
	}
	if root == "" {
		return "", "directory " + inoStr(dir) + " is not a smart-folder root"
	}
	if why := ownedBy(p, u, dir); why != "" {
		return "", "smart-folder root " + root + ": " + why
	}
	return root, ""
}

// ownedBy: inode n came from p's own INCRBY nextinode batch. A number
// handed out before the proxy recorded batches has no recorded owner.
func ownedBy(p Principal, u *Unit, n uint64) string {
	if u.inodeOwner == nil {
		return "no inode allocation record"
	}
	owner, known, err := u.inodeOwner(n)
	switch {
	case err != nil:
		return "inode allocation record: " + err.Error()
	case !known:
		return "inode " + inoStr(n) + " has no recorded creator"
	case owner != p.Name:
		return "inode " + inoStr(n) + " was not created by " + p.Name
	}
	return ""
}

// attrTimesNow: the modify and change times of attr a are within
// smartViewSkew of Redis TIME.
func (e *LeaseEngine) attrTimesNow(a []byte) string {
	v, err := e.side("TIME")
	if err != nil || len(v.Elems) != 2 {
		return "cannot read Redis TIME"
	}
	now, err := strconv.ParseInt(string(v.Elems[0].Str), 10, 64)
	if err != nil {
		return "cannot read Redis TIME"
	}
	srv := time.Unix(now, 0)
	for _, at := range []int{attrTimesFrom, attrTimesFrom + 12} { // mtime, ctime
		t := time.Unix(int64(binary.BigEndian.Uint64(a[at:at+8])), 0)
		if t.Before(srv.Add(-smartViewSkew)) || t.After(srv.Add(smartViewSkew)) {
			return "its times are not the server's current time"
		}
	}
	return ""
}

// rmdirParentAttr: next is old with the link count one lower and new modify
// and change times, as juicefs's rmdir writes the parent.
func rmdirParentAttr(old, next []byte) bool {
	if len(old) < attrMinLen || len(next) != len(old) {
		return false
	}
	if !bytes.Equal(old[:attrTimesFrom], next[:attrTimesFrom]) || !bytes.Equal(old[attrNlinkEnd:], next[attrNlinkEnd:]) {
		return false
	}
	was := binary.BigEndian.Uint32(old[attrNlinkAt:attrNlinkEnd])
	return was > 2 && binary.BigEndian.Uint32(next[attrNlinkAt:attrNlinkEnd]) == was-1
}

// rmdirCmdKey is a command and its arguments as one comparable string.
func rmdirCmdKey(name string, args ...string) string {
	k := name
	for _, a := range args {
		k += "\x00" + strconv.Quote(a)
	}
	return k
}
