/*
 * JuiceMount: the JMRefreshInode control message (patch 0031).
 *
 * The app sends it when its metadata mirror learns that another client
 * changed a file (a teammate's save). It makes this mount serve the new
 * bytes on the next read of a descriptor that is already open, with no
 * reopen. A reopen is not enough on macOS: macFUSE shares one FUSE handle
 * between all open descriptors of a file, so a second open() never reaches
 * juicefs while the first one is open (laptop run 2026-11-14, bug P1).
 *
 * Three caches hold the old version, and the message drops all three:
 *  1. the open file's slice lists (meta openfiles), and its attributes, read
 *     again from the engine (one metadata round trip);
 *  2. the vfs read buffers of every open reader of the inode, with the
 *     reader's length set from the fresh attributes (an append becomes
 *     readable);
 *  3. the kernel's pages and attributes of the inode (FUSE_NOTIFY_INVAL_INODE).
 *     This runs on the message's goroutine, outside any FUSE request, so it
 *     is synchronous: the reply comes after the kernel took the notify. On
 *     macOS juicefs's own open sends this notify asynchronously, and in the
 *     laptop pin run a read beat it (48 MiB served from stale pages).
 *
 * Body: version(1) = 1, inode(8).
 *
 * Reply: errno(1); on errno 0 also flags(1), length(8), mtime_ns(8): 18
 * bytes. flags: bit 1 the kernel dropped its pages; bit 2 no kernel notify
 * (no FUSE server, or the kernel lacks the notify); bit 4 the notify did not
 * answer within jmRefreshNotifyWait (it may still land); bit 8 the notify
 * failed. EINVAL: an unknown version or a short body. ENOTSUP: the metadata
 * engine has no refresh. Engine errors (ENOENT for a deleted inode) as is.
 */

package vfs

import (
	"io"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	jmRefreshVersion = 1
	jmRefreshReplyOK = 1 + 1 + 8 + 8

	JMRefreshKernelDropped = 1
	JMRefreshKernelNone    = 2
	JMRefreshKernelTimeout = 4
	JMRefreshKernelFailed  = 8
)

// jmRefreshNotifyWait bounds the wait for the kernel notify. The notify
// takes the inode's vnode lock, so it waits for a read of that file that is
// already in the kernel.
var jmRefreshNotifyWait = 2 * time.Second

// jmOpenFileRefresher is the metadata side (pkg/meta/jm_refresh.go).
type jmOpenFileRefresher interface {
	JMRefreshOpenFile(ctx meta.Context, inode Ino, attr *Attr) syscall.Errno
}

var jmInodeInvalidator atomic.Pointer[func(Ino) syscall.Errno]

// JMSetInodeInvalidator sets how the refresh drops the kernel's pages and
// attributes of an inode (pkg/fuse sets FUSE_NOTIFY_INVAL_INODE). It returns
// ENOSYS when the kernel has no such notify.
func JMSetInodeInvalidator(f func(Ino) syscall.Errno) {
	if f == nil {
		jmInodeInvalidator.Store(nil)
		return
	}
	jmInodeInvalidator.Store(&f)
}

// jmRefresh drops every read buffer of the inode's open readers and sets
// their length. A buffer being filled is marked to fill again (REFRESH); a
// buffer a read holds is filled again; an idle one is deleted.
func (r *dataReader) jmRefresh(inode Ino, length uint64) (buffers int) {
	r.visit(inode, func(f *fileReader) {
		f.length = length
		f.visit(func(s *sliceReader) bool {
			s.invalidate()
			buffers++
			return true
		})
	})
	return buffers
}

func (v *VFS) jmRefreshMsg(ctx meta.Context, r *utils.Buffer, out io.Writer) {
	fail := func(eno syscall.Errno) { _, _ = out.Write([]byte{uint8(eno)}) }
	if r.Left() != 1+8 || r.Get8() != jmRefreshVersion {
		fail(syscall.EINVAL)
		return
	}
	inode := Ino(r.Get64())
	m, ok := v.Meta.(jmOpenFileRefresher)
	if !ok {
		fail(syscall.ENOTSUP)
		return
	}
	var attr Attr
	if eno := m.JMRefreshOpenFile(ctx, inode, &attr); eno != 0 {
		fail(eno)
		return
	}
	length := attr.Length
	if attr.Typ == meta.TypeFile {
		if v.writer != nil {
			if w := v.writer.GetLength(inode); w > length {
				length = w // our own write in flight
			}
		}
		if dr, ok := v.reader.(*dataReader); ok {
			dr.jmRefresh(inode, length)
		}
	}
	flags := v.jmRefreshKernel(inode)
	w := utils.NewBuffer(jmRefreshReplyOK)
	w.Put8(0)
	w.Put8(flags)
	w.Put64(length)
	w.Put64(uint64(attr.Mtime)*1e9 + uint64(attr.Mtimensec))
	_, _ = out.Write(w.Bytes())
}

func (v *VFS) jmRefreshKernel(inode Ino) uint8 {
	fp := jmInodeInvalidator.Load()
	if fp == nil {
		return JMRefreshKernelNone
	}
	done := make(chan syscall.Errno, 1)
	go func() { done <- (*fp)(inode) }()
	select {
	case eno := <-done:
		switch eno {
		case 0:
			return JMRefreshKernelDropped
		case syscall.ENOSYS, syscall.ENOTSUP:
			return JMRefreshKernelNone
		default:
			logger.Warnf("refresh inode %d: kernel notify: %s", inode, eno)
			return JMRefreshKernelFailed
		}
	case <-time.After(jmRefreshNotifyWait):
		logger.Warnf("refresh inode %d: kernel notify did not answer in %s", inode, jmRefreshNotifyWait)
		return JMRefreshKernelTimeout
	}
}
