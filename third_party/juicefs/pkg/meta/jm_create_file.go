/*
 * JuiceMount-owned (patch 0025). Not part of upstream JuiceFS.
 *
 * JMCreateFile (and JMCreateFilesIn, for several files in one folder)
 * creates a regular file that is complete when its name appears: its data slice (already uploaded by the caller), length, mode and
 * mtime are written by the same transaction that adds the directory entry.
 * The JuiceMount drainer uses it (through the JMCreateFiles control message,
 * pkg/vfs/jm_create_files.go) instead of create + write + fsync + utimes +
 * rename of a private stage. On a distant link that sequence costs about 16
 * metadata round trips, 7 of them under the kernel's lock on the folder; this
 * costs 2, none under a kernel lock, and JMCreateFilesIn creates several
 * files of one folder with those same 2 (JuiceFS runs one transaction per
 * folder at a time, so separate transactions would queue).
 *
 * Same checks as Mknod: name, trash, read-only, quota (space and one inode),
 * the parent's write and search permission for the caller, immutable parent.
 * A parent with a default ACL, a case-insensitive volume and the reserved
 * birth-stage names are refused with ENOTSUP or EINVAL: the caller then uses
 * the ordinary create path. An existing name is never replaced or adopted
 * (EEXIST, nothing written).
 */

package meta

import (
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/replaceguard"
)

// JMNewFile is one file of JMCreateFilesIn: the request (Name to Slice) and
// its result (Inode, Attr, Errno).
type JMNewFile struct {
	Name         string
	Mode, Cumask uint16
	Mtime        time.Time
	Slice        Slice // Off 0, Len = Size = the file length; zero for an empty file
	Inode        Ino
	Attr         Attr
	Errno        syscall.Errno
}

// jmCreateFileEngine is implemented by the engines that support
// JMCreateFilesIn (Redis only). It writes every file whose Errno is 0 and
// whose name is free in one transaction, sets EEXIST on the others, and
// returns an error that applies to all of them.
type jmCreateFileEngine interface {
	doJMCreateFiles(ctx Context, parent Ino, files []*JMNewFile) syscall.Errno
}

// JMCreateFileMaxBytes bounds the data of one file: one slice in the first
// chunk.
const JMCreateFileMaxBytes = 4 << 20

// JMCreateFileSupported reports whether this engine implements JMCreateFile.
func (m *baseMeta) JMCreateFileSupported() bool {
	_, ok := m.en.(jmCreateFileEngine)
	return ok
}

// JMCreateFile creates name in parent holding slice (Off 0, Len = Size = the
// file length; Id 0 and Len 0 for an empty file) with mode &^ cumask and
// atime = mtime = mtime. On success *inode and *attr describe the new file.
func (m *baseMeta) JMCreateFile(ctx Context, parent Ino, name string, mode, cumask uint16, mtime time.Time, slice Slice, inode *Ino, attr *Attr) syscall.Errno {
	f := &JMNewFile{Name: name, Mode: mode, Cumask: cumask, Mtime: mtime, Slice: slice}
	if st := m.JMCreateFilesIn(ctx, parent, []*JMNewFile{f}); st != 0 {
		return st
	}
	if f.Errno == 0 {
		*inode, *attr = f.Inode, f.Attr
	}
	return f.Errno
}

// JMCreateFilesIn creates files in parent, all in one transaction (one
// txLock of the parent, 2 round trips). Each file gets its own result in
// Errno; a non-zero return applies to every file and nothing was written.
func (m *baseMeta) JMCreateFilesIn(ctx Context, parent Ino, files []*JMNewFile) syscall.Errno {
	en, ok := m.en.(jmCreateFileEngine)
	if !ok {
		return syscall.ENOTSUP
	}
	if isTrash(parent) {
		return syscall.EPERM
	}
	if m.conf.ReadOnly {
		return syscall.EROFS
	}
	defer m.timeit("JMCreateFile", time.Now())
	parent = m.checkRoot(parent)
	var todo []*JMNewFile
	var space, inodes int64
	for _, f := range files {
		f.Inode, f.Attr, f.Errno = 0, Attr{}, 0
		switch {
		case parent == RootInode && f.Name == TrashName:
			f.Errno = syscall.EPERM
		case f.Name == "." || f.Name == "..":
			f.Errno = syscall.EEXIST
		default:
			f.Errno = checkInodeName(f.Name)
		}
		if f.Errno != 0 {
			continue
		}
		if _, birth := replaceguard.ParseBirthStageName(f.Name); birth {
			f.Errno = syscall.EINVAL
			continue
		}
		s := f.Slice
		if s.Off != 0 || s.Len != s.Size || s.Len > JMCreateFileMaxBytes || (s.Len == 0) != (s.Id == 0) {
			f.Errno = syscall.EINVAL
			continue
		}
		todo = append(todo, f)
		space += align4K(uint64(s.Len))
		inodes++
	}
	if len(todo) == 0 {
		return 0
	}
	if st := m.checkQuota(ctx, space, inodes, parent); st != 0 {
		if len(todo) == 1 {
			todo[0].Errno = st
			return 0
		}
		// One file may be what exceeds the quota: try each alone.
		for _, f := range todo {
			if st := m.JMCreateFilesIn(ctx, parent, []*JMNewFile{f}); st != 0 {
				f.Errno = st
			}
		}
		return 0
	}
	for _, f := range todo {
		ino, err := m.nextInode()
		if err != nil {
			return errno(err)
		}
		f.Inode = ino
		f.Attr = Attr{Typ: TypeFile, Uid: ctx.Uid(), Gid: ctx.Gid(), Nlink: 1, Length: uint64(f.Slice.Len), Parent: parent, Full: true}
	}
	if st := en.doJMCreateFiles(ctx, parent, todo); st != 0 {
		for _, f := range todo {
			f.Inode, f.Attr = 0, Attr{}
		}
		return st
	}
	var length int64
	space, inodes = 0, 0
	for _, f := range todo {
		if f.Errno != 0 {
			f.Inode, f.Attr = 0, Attr{}
			continue
		}
		length += int64(f.Slice.Len)
		space += align4K(uint64(f.Slice.Len))
		inodes++
	}
	if inodes > 0 {
		m.en.updateStats(space, inodes)
		m.updateDirStat(ctx, parent, length, space, inodes)
		m.updateDirQuota(ctx, parent, space, inodes)
	}
	return 0
}
