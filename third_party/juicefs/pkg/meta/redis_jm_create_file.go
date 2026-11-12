/*
 * JuiceMount-owned (patch 0025). Not part of upstream JuiceFS.
 *
 * The Redis transaction of JMCreateFilesIn (jm_create_file.go): doMknod for
 * each regular file plus doWrite of its one slice, in one MULTI/EXEC. It WATCHes
 * the parent attribute and entries (as doMknod), reads the parent and every
 * name in one pipeline, and writes the inodes, the entries, the parent's
 * mtime, the chunks and the counters together: 2 round trips with patch 0019,
 * whatever the number of files.
 */

package meta

import (
	"runtime"
	"syscall"
	"time"

	aclAPI "github.com/juicedata/juicefs/pkg/acl"
	"github.com/redis/go-redis/v9"
)

func (m *redisMeta) doJMCreateFiles(ctx Context, parent Ino, files []*JMNewFile) syscall.Errno {
	if m.conf.CaseInsensi {
		return syscall.ENOTSUP // resolveCase reads outside the txn; keep it on the ordinary path
	}
	base := make([]Attr, len(files))
	for i, f := range files {
		base[i] = f.Attr
	}
	return errno(m.txn(ctx, func(tx *redis.Tx) error {
		var pcmd *redis.StringCmd
		ecmds := make([]*redis.StringCmd, len(files))
		if _, err := tx.Pipelined(ctx, func(p redis.Pipeliner) error {
			pcmd = p.Get(ctx, m.inodeKey(parent))
			for i, f := range files {
				ecmds[i] = p.HGet(ctx, m.entryKey(parent), f.Name)
			}
			return nil
		}); err != nil && !isReplyError(err) {
			return err
		}
		var pattr Attr
		a, err := pcmd.Bytes()
		if err != nil {
			return err
		}
		m.parseAttr(a, &pattr)
		if pattr.Typ != TypeDirectory {
			return syscall.ENOTDIR
		}
		if pattr.Parent > TrashInode {
			return syscall.ENOENT
		}
		if st := m.Access(ctx, parent, MODE_MASK_W|MODE_MASK_X, &pattr); st != 0 {
			return st
		}
		if (pattr.Flags & FlagImmutable) != 0 {
			return syscall.EPERM
		}
		if pattr.DefaultACL != aclAPI.None {
			return syscall.ENOTSUP // ACL inheritance writes more keys; ordinary path
		}
		now := time.Now()
		seen := make(map[string]bool, len(files))
		write := 0
		for i, f := range files {
			f.Attr, f.Errno, f.recognized = base[i], 0, false
			if buf, err := ecmds[i].Bytes(); err == nil {
				if _, ino := m.parseEntry(buf); ino == f.Inode && !seen[f.Name] {
					// The name already holds this file's own inode: an
					// earlier send (or attempt of this txn) committed it.
					f.recognized = true
					seen[f.Name] = true
					continue
				}
				f.Errno = syscall.EEXIST
				continue
			} else if err != redis.Nil {
				return err
			}
			if seen[f.Name] {
				f.Errno = syscall.EEXIST
				continue
			}
			if f.recognize {
				f.Errno = syscall.ESRCH // a number we do not hold, and not at the name
				continue
			}
			seen[f.Name] = true
			write++
			attr := &f.Attr
			attr.Mode = f.Mode & 07777 &^ f.Cumask
			if ctx.Value(CtxKey("behavior")) == "Hadoop" || runtime.GOOS == "darwin" {
				attr.Gid = pattr.Gid
			} else if runtime.GOOS == "linux" && pattr.Mode&02000 != 0 {
				attr.Gid = pattr.Gid
				if attr.Mode&02010 == 02010 && ctx.Uid() != 0 {
					var found bool
					for _, gid := range ctx.Gids() {
						if gid == pattr.Gid {
							found = true
						}
					}
					if !found {
						attr.Mode &= ^uint16(02000)
					}
				}
			}
			attr.Atime, attr.Atimensec = f.Mtime.Unix(), uint32(f.Mtime.Nanosecond())
			attr.Mtime, attr.Mtimensec = f.Mtime.Unix(), uint32(f.Mtime.Nanosecond())
			attr.Ctime, attr.Ctimensec = now.Unix(), uint32(now.Nanosecond())
		}
		if write == 0 {
			return nil
		}
		updateParent := false
		if now.Sub(time.Unix(pattr.Mtime, int64(pattr.Mtimensec))) >= m.conf.SkipDirMtime {
			pattr.Mtime = now.Unix()
			pattr.Mtimensec = uint32(now.Nanosecond())
			pattr.Ctime = now.Unix()
			pattr.Ctimensec = uint32(now.Nanosecond())
			updateParent = true
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			if updateParent {
				pipe.Set(ctx, m.inodeKey(parent), m.marshal(&pattr), 0)
			}
			// Per file the commands of a create plus its first write, so
			// the counters move as for separate creates (the RESP proxy
			// bounds totalInodes to one per INCR).
			for _, f := range files {
				if f.Errno != 0 || f.recognized {
					continue
				}
				pipe.Set(ctx, m.inodeKey(f.Inode), m.marshal(&f.Attr), 0)
				pipe.HSet(ctx, m.entryKey(parent), f.Name, m.packEntry(TypeFile, f.Inode))
				if f.Slice.Len > 0 {
					pipe.RPush(ctx, m.chunkKey(f.Inode, 0), marshalSlice(0, f.Slice.Id, f.Slice.Size, 0, f.Slice.Len))
				}
				pipe.IncrBy(ctx, m.usedSpaceKey(), align4K(uint64(f.Slice.Len)))
				pipe.Incr(ctx, m.totalInodesKey())
			}
			return nil
		})
		return err
	}, m.inodeKey(parent), m.entryKey(parent)))
}
