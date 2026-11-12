/*
 * JuiceMount-owned source (patch 0022). Not part of upstream JuiceFS.
 *
 * Folder creation with an inode number reserved ahead of time. The JuiceMount
 * NFS layer answers MKDIR at local speed and creates the folder on the volume
 * later, in the background. The NFS client keeps the folder's file id for as
 * long as it holds the folder, and macOS treats a changed file id as a stale
 * handle. So the id it hands out must be the inode the folder will get. These
 * two calls make that possible:
 *
 *   - JMReserveInodes takes numbers from the volume's own inode counter (the
 *     counter nextInode uses), so no client of the volume will allocate them.
 *   - JMMkdirInode creates one directory with one of those numbers, through
 *     the same checks and the same engine transaction as Mkdir.
 *
 * A number is usable once, and only by the process that reserved it: the
 * reservation lives in memory and ends with the process. A caller whose
 * number is unknown here (ESRCH) falls back to an ordinary Mkdir.
 */

package meta

import (
	"sync"
	"syscall"
)

const (
	// jmReserveMax bounds one reservation call.
	jmReserveMax = 4096
	// jmReservedMax bounds the numbers held by one process.
	jmReservedMax = 1 << 16
)

type jmReservation struct {
	mu  sync.Mutex
	ids map[Ino]struct{}
	// inflight: numbers a JMCreateFilesIn is creating with (patch 0025).
	inflight map[Ino]struct{}
}

var jmReservations sync.Map // *baseMeta -> *jmReservation

func (m *baseMeta) jmReserved() *jmReservation {
	v, _ := jmReservations.LoadOrStore(m, &jmReservation{ids: make(map[Ino]struct{})})
	return v.(*jmReservation)
}

// JMReserveInodes reserves up to n inode numbers. It returns the first number
// and how many consecutive numbers from it are reserved.
func (m *baseMeta) JMReserveInodes(n uint32) (Ino, uint32, syscall.Errno) {
	if n == 0 || n > jmReserveMax {
		return 0, 0, syscall.EINVAL
	}
	if m.conf.ReadOnly {
		return 0, 0, syscall.EROFS
	}
	r := m.jmReserved()
	r.mu.Lock()
	full := len(r.ids)+int(n) > jmReservedMax
	r.mu.Unlock()
	if full {
		return 0, 0, syscall.EAGAIN
	}
	v, err := m.en.incrCounter("nextInode", int64(n))
	if err != nil {
		return 0, 0, errno(err)
	}
	first := Ino(uint64(v) - uint64(n))
	count := n
	for first <= RootInode && count > 0 { // a brand-new volume: skip 0 and 1, as nextInode does
		first++
		count--
	}
	r.mu.Lock()
	for i := Ino(0); i < Ino(count); i++ {
		r.ids[first+i] = struct{}{}
	}
	r.mu.Unlock()
	return first, count, 0
}

// JMReleaseInodes gives back reserved numbers from first to first+n-1 that
// this process still holds and has not used. It returns how many it released:
// 0 for numbers it does not hold (a client from before this process
// restarted). Released numbers are not reused; the volume counter only grows.
func (m *baseMeta) JMReleaseInodes(first Ino, n uint32) (uint32, syscall.Errno) {
	if n > jmReservedMax {
		return 0, syscall.EINVAL
	}
	r := m.jmReserved()
	r.mu.Lock()
	defer r.mu.Unlock()
	var released uint32
	for i := Ino(0); i < Ino(n); i++ {
		if _, ok := r.ids[first+i]; ok {
			delete(r.ids, first+i)
			released++
		}
	}
	return released, 0
}

// jmDefiniteRefusal lists results after which the engine wrote nothing, so the
// number stays reserved for another attempt. Any other failure may have
// committed (an ambiguous EXEC reply), and the number is never offered again.
func jmDefiniteRefusal(st syscall.Errno) bool {
	switch st {
	case syscall.EEXIST, syscall.ENOENT, syscall.ENOTDIR, syscall.EACCES, syscall.EPERM,
		syscall.EROFS, syscall.EINVAL, syscall.ENAMETOOLONG, syscall.EDQUOT, syscall.ENOSPC:
		return true
	}
	return false
}

// JMMkdirInode creates directory name in parent with inode number ino, which
// JMReserveInodes reserved in this process. It returns ESRCH, and writes
// nothing, for a number this process does not hold.
func (m *baseMeta) JMMkdirInode(ctx Context, parent Ino, name string, mode, cumask uint16, ino Ino, attr *Attr) syscall.Errno {
	r := m.jmReserved()
	r.mu.Lock()
	_, ok := r.ids[ino]
	delete(r.ids, ino)
	r.mu.Unlock()
	if !ok {
		return syscall.ESRCH
	}
	var inode Ino
	st := m.mknod(ctx, parent, name, TypeDirectory, mode, cumask, 0, "", ino, &inode, attr)
	if st == 0 {
		m.parentMu.Lock()
		m.dirParents[inode] = parent
		m.parentMu.Unlock()
		return 0
	}
	if jmDefiniteRefusal(st) {
		r.mu.Lock()
		r.ids[ino] = struct{}{}
		r.mu.Unlock()
	}
	return st
}
