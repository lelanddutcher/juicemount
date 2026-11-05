package respproxy

// smartview.go is the one exception to the root-entry gate (decide.go
// rootEntryGate) that is not the admin: the Manager's server principal may
// remove an EMPTY view directory it created directly under one of its
// smart-folder roots (/Recents/<view>, /Collections/<view>,
// /Queries/<view>; LeaseConfig.SmartRoots). Such a view is a project root,
// so without this only an admin could remove it, and the Manager would
// leave every emptied view behind. Every other removal or rename of a
// directory above the project depth stays admin-only.
//
// The unit must be exactly juicefs's rmdir without the trash (redis.go
// doRmdir with skipCheckTrash, which the Manager's metadata client uses),
// and nothing else:
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
//   - P is the directory a configured smart-root name names in the volume
//     root now (a root renamed away no longer qualifies);
//   - <name> in P names the directory N, whose Attr.Parent is P;
//   - d<N> is empty (juicefs's own ENOTEMPTY check, HLEN, repeated here);
//   - the new attr of P is the stored one with only its link count one
//     lower and its change and modify times new;
//   - N came from the Manager principal's own INCRBY nextinode batch
//     (Guard.inodes): a directory it created. A number handed out before
//     the proxy recorded batches has no recorded owner and is refused.
//
// The admitted removal is logged to jmlease:v1:log (event
// smart_view_remove) and, like every admitted namespace unit, to the
// per-person audit log.

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

// DefaultSmartRoots are the smart-folder root names the proxy uses when its
// config names none: the Manager's defaults for JM_SMART_FOLDERS_ROOT,
// JM_SMART_FOLDERS_COLLECTIONS_ROOT and JM_SMART_FOLDERS_QUERIES_ROOT.
var DefaultSmartRoots = []string{"Recents", "Collections", "Queries"}

// CleanSmartRoots returns the valid smart-folder root names among names,
// checked as the Manager checks them (one visible path component, not a
// reserved namespace), each once, and an error for each invalid one. An
// invalid name is left out: it only narrows the exception.
func CleanSmartRoots(names []string) ([]string, []error) {
	var ok []string
	var bad []error
	seen := map[string]bool{}
	for _, n := range names {
		if err := smartfolders.ValidateRootName(n); err != nil {
			bad = append(bad, err)
			continue
		}
		if !seen[n] {
			seen[n] = true
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
// returns the smart root's name, or why not.
func (e *LeaseEngine) smartViewRemoval(p Principal, u *Unit, op dentryOp, stored []byte) (string, string) {
	if p.Kind != KindManager {
		return "", "not the manager principal"
	}
	if len(e.cfg.SmartRoots) == 0 {
		return "", "no smart-folder roots configured"
	}
	if u.Kind != UnitTxn || u.DB != e.cfg.VolumeDB {
		return "", "not a transaction on the volume"
	}
	n, ok := entryInode(stored)
	if !ok || !op.del || op.trash {
		return "", "not the removal of a directory entry"
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
			return "", "the unit is not exactly an rmdir: " + sanitizeToken(c.Info.Name)
		}
		want[k]--
	}
	for _, left := range want {
		if left != 0 {
			return "", "the unit is not exactly an rmdir"
		}
	}
	if newAttr == nil {
		return "", "the unit is not exactly an rmdir: no SET i" + pd
	}

	// The reads below are covered by the WATCH juicefs's rmdir takes.
	watched := map[string]bool{}
	for _, k := range u.Watched {
		watched[k] = true
	}
	for _, k := range []string{"i" + pd, "d" + pd, "i" + nd, "d" + nd} {
		if !watched[k] {
			return "", k + " is not WATCHed"
		}
	}
	if u.beforeSide == nil {
		return "", "no WATCH on the connection"
	}
	if err := u.beforeSide(); err != nil {
		return "", "WATCH not confirmed before side read: " + err.Error()
	}
	read := func(args ...string) (Value, error) {
		e.anc.sideReads.Add(1)
		return e.side(args...)
	}

	// P is what a configured smart-root name names in the volume root now.
	hm := append([]string{"HMGET", "d" + inoStr(rootIno)}, e.cfg.SmartRoots...)
	v, err := read(hm...)
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
		if tok && iok && t == typeDir && ino == op.dir {
			root = e.cfg.SmartRoots[i]
			break
		}
	}
	if root == "" {
		return "", "directory " + pd + " is not a smart-folder root"
	}

	// <name> names the directory N, placed in P.
	if v, err = read("HGET", "d"+pd, op.name); err != nil || v.Null || !bytes.Equal(v.Str, stored) {
		return "", "the entry changed"
	}
	if v, err = read("GET", "i"+nd); err != nil || v.Null {
		return "", "cannot read inode " + nd
	}
	if a, ok := parseAttr(v.Str); !ok || a.typ != typeDir || a.parent != op.dir {
		return "", "inode " + nd + " is not a directory in " + pd
	}
	// Empty.
	if v, err = read("HLEN", "d"+nd); err != nil || v.Type != ':' {
		return "", "cannot read directory " + nd
	}
	if v.Int != 0 {
		return "", "directory " + nd + " is not empty"
	}
	// P's attr changes only as rmdir changes it.
	if v, err = read("GET", "i"+pd); err != nil || v.Null {
		return "", "cannot read inode " + pd
	}
	if !rmdirParentAttr(v.Str, newAttr) {
		return "", "the new attr of " + pd + " changes more than its link count and times"
	}

	// Created by this principal.
	if u.inodeOwner == nil {
		return "", "no inode allocation record"
	}
	owner, known, err := u.inodeOwner(n)
	switch {
	case err != nil:
		return "", "inode allocation record: " + err.Error()
	case !known:
		return "", "inode " + nd + " has no recorded creator"
	case owner != p.Name:
		return "", "inode " + nd + " was not created by " + p.Name
	}
	return root, ""
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
