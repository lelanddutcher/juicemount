package metadata

import (
	"sync"
	"time"
)

// ============================================================================
// A save's parts that commit after its final mtime (the mtime-span finding,
// remote lab and laptop, 2026-11-14).
//
// THE PROBLEM. juicefs gives every slice of an open file the time of its
// last write; a setattr of the mtime (an app's or the kernel's) sets that
// time on every slice still buffered (pkg/vfs/vfs_unix.go, UpdateMtime).
// Each commit (pkg/meta/redis.go doWrite) then writes the inode's mtime from
// its slice and its ctime as the commit time. So a save's final mtime can
// reach Redis with its FIRST commit after the setattr, while the other parts
// commit over the next seconds (minutes on a far link). The attr push sees
// the new mtime once; the later commits change the bytes and the ctime only.
// The NFS client on the Mac keeps a file's pages until it sees another mtime
// (a ctime-only change does not drop them: laptop run, 4 of 4), so the parts
// it read between the first and the last commit stay old under the final
// mtime for good. JuiceMount reported ctime = mtime anyway (go-nfs).
//
// THE FIX. The push keeps the ctime it last saw for each file (attrCtimes,
// keyed by inode, with the mtime it came with). A ctime that moves on while
// the mtime stays the same is a late commit: the push upserts the row with
// MtimeBump one higher, which fires the content hooks (the byte caches, the
// juicefs refresh and the pooled descriptor, as for any edit) and makes the
// NFS layer report mtime + MtimeBump nanoseconds (Entry.NFSMtime). The Mac
// sees another mtime after every late commit and reads the file again.
//
// WHY NANOSECONDS, AND WHY NEVER BACK. The reported mtime only ever moves
// forward while the row has this mtime, and by nanoseconds, so:
//   - it never returns to a value under which the Mac may have cached older
//     parts (a design that reports max(mtime, ctime) during the save and the
//     true mtime after it returns to the final mtime; a Mac that did not
//     look during the save would then keep the parts it read early);
//   - the date apps and Finder show (seconds) does not change;
//   - the true mtime stays in Entry.Mtime, so every identity check against
//     juicefs (stat of the FUSE file, the cache proofs) is unchanged.
//
// APPS. Only another client's save moves it: the push skips files with a
// pending local write, and this client's own drained file comes back with an
// mtime the last observation did not have, so its first observation is only
// recorded. A teammate's save already moved the mtime at its first commit,
// which an app with the file open (Premiere's "modified since last save"
// check of a project, a media relink) already sees; the bumps are more moves
// of the same save. The bump lives in RAM: a row evicted from the cache and
// read back from SQL reports its true mtime, which is again another mtime.
// ============================================================================

// NFSMtime is the mtime the NFS layer reports for e: the true mtime plus one
// nanosecond per late commit seen under it (MtimeBump).
func (e *Entry) NFSMtime() time.Time {
	if e.MtimeBump == 0 {
		return e.Mtime
	}
	return e.Mtime.Add(time.Duration(e.MtimeBump))
}

// carryMtimeBump keeps a row's bump when it is published again under the
// same inode and mtime by a path that did not observe the late commits (a
// folder reconcile, a SCAN): the reported mtime never goes back.
func carryMtimeBump(old, e *Entry) {
	if old == nil || e == nil || old.Inode != e.Inode || !old.Mtime.Equal(e.Mtime) {
		return
	}
	if e.MtimeBump < old.MtimeBump {
		e.MtimeBump = old.MtimeBump
	}
}

// ctimeObservation is the last ctime the push read for a file, and the
// mtime it read with it.
type ctimeObservation struct {
	mtimeNS, ctimeNS int64
}

// attrCtimeMax caps the observations; past it they start over (a file's next
// late commit is then only recorded).
const attrCtimeMax = 1 << 16

// attrCtimes is the push's memory of each file's last ctime.
type attrCtimes struct {
	mu   sync.Mutex
	seen map[uint64]ctimeObservation
}

// lateCommit reports whether ctime is a late commit for inode: the same
// mtime as the last observation and a newer ctime. It records nothing; note
// does, once the row is up to date.
func (a *attrCtimes) lateCommit(inode uint64, mtime, ctime time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	prev, ok := a.seen[inode]
	return ok && prev.mtimeNS == mtime.UnixNano() && ctime.UnixNano() > prev.ctimeNS
}

// note records the ctime the row now reflects. It reports whether that
// changed the observation.
func (a *attrCtimes) note(inode uint64, mtime, ctime time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.seen == nil || len(a.seen) >= attrCtimeMax {
		a.seen = make(map[uint64]ctimeObservation)
	}
	o := ctimeObservation{mtimeNS: mtime.UnixNano(), ctimeNS: ctime.UnixNano()}
	if prev, ok := a.seen[inode]; ok && prev.mtimeNS == o.mtimeNS && prev.ctimeNS >= o.ctimeNS {
		return false
	}
	a.seen[inode] = o
	return true
}
