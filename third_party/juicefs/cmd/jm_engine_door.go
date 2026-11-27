/*
 * JuiceMount patch 0037, step 2 ("reads"): the engine socket inside
 * `juicefs mount` ("both doors").
 *
 * At the app's engine levels `reads` to `writes` the socket is served by the
 * mount process itself, beside the kernel mount: two doors into one engine,
 * one block cache, one set of open files, one upload queue, one pin table.
 * A read through the socket and a read through the kernel see the same
 * engine state, and a refresh message sent through the control file
 * (patches 0031/0032) refreshes every open reader of the inode, the socket's
 * included.
 *
 * The app passes two inherited descriptors and two flags:
 *
 *	--jm-door-start-fd N   the read end of a private pipe; the first message
 *	                       is the start message (only the launch token is
 *	                       used: the mount gets its secrets as today)
 *	--jm-door-listen-fd M  the listening Unix socket, in a folder only the
 *	                       app's user can open
 *
 * Without the flags (the default) nothing here runs. The door never stops
 * the mount: when the app closes the pipe, the door stops serving and the
 * mount carries on as it does today.
 */

package cmd

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine"
	"github.com/juicedata/juicefs/pkg/jmengine/wire"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
)

func jmDoorFlags() []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{Name: "jm-door-start-fd", Usage: "JuiceMount: pipe with the engine socket's start message (0 = no socket)"},
		&cli.IntFlag{Name: "jm-door-listen-fd", Usage: "JuiceMount: inherited listening Unix socket for the engine (0 = no socket)"},
	}
}

// jmDoorVFS is the engine of a mount, served through the socket. It adds
// the read path of step 2 to the step-1 adapter.
type jmDoorVFS struct {
	jmEngineVFS
}

func jmDoorContext(ctx context.Context) (vfs.LogContext, func() bool) {
	mctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	stop := context.AfterFunc(ctx, mctx.Cancel)
	return vfs.NewLogContext(mctx), stop
}

func jmWireAttr(ino meta.Ino, a *meta.Attr) wire.Attr {
	return wire.Attr{
		Inode: uint64(ino), Type: a.Typ, Mode: a.Mode, Uid: a.Uid, Gid: a.Gid,
		Nlink: a.Nlink, Size: a.Length,
		Atime: a.Atime*1e9 + int64(a.Atimensec),
		Mtime: a.Mtime*1e9 + int64(a.Mtimensec),
		Ctime: a.Ctime*1e9 + int64(a.Ctimensec),
	}
}

// Open opens one regular file read-only. The control and other internal
// files are not reachable through the socket.
func (e *jmDoorVFS) Open(ctx context.Context, ino uint64) (uint64, wire.Attr, syscall.Errno) {
	if vfs.IsSpecialNode(meta.Ino(ino)) {
		return 0, wire.Attr{}, syscall.EPERM
	}
	lctx, stop := jmDoorContext(ctx)
	defer stop()
	entry, fh, st := e.v.Open(lctx, meta.Ino(ino), syscall.O_RDONLY)
	if st != 0 {
		if ctx.Err() != nil {
			return 0, wire.Attr{}, syscall.ETIMEDOUT
		}
		return 0, wire.Attr{}, st
	}
	if entry == nil || entry.Attr == nil || entry.Attr.Typ != meta.TypeFile {
		e.v.Release(lctx, meta.Ino(ino), fh)
		return 0, wire.Attr{}, syscall.EISDIR
	}
	return fh, jmWireAttr(entry.Inode, entry.Attr), 0
}

// Read fills buf from off until it is full or the file ends.
func (e *jmDoorVFS) Read(ctx context.Context, ino, fh uint64, buf []byte, off uint64) (int, syscall.Errno) {
	lctx, stop := jmDoorContext(ctx)
	defer stop()
	got := 0
	for got < len(buf) {
		n, st := e.v.Read(lctx, meta.Ino(ino), buf[got:], off+uint64(got), fh)
		if st != 0 {
			if ctx.Err() != nil {
				return 0, syscall.ETIMEDOUT
			}
			if got > 0 && st == syscall.EIO {
				break // the bytes so far are good; the app asks again for the rest
			}
			return 0, st
		}
		if n == 0 {
			break // end of file
		}
		got += n
	}
	return got, 0
}

// Release closes a handle from Open. The engine flushes nothing for a
// read-only handle; it never blocks on the NAS.
func (e *jmDoorVFS) Release(ino, fh uint64) {
	lctx, stop := jmDoorContext(context.Background())
	defer stop()
	e.v.Release(lctx, meta.Ino(ino), fh)
}

// jmStartEngineDoor starts the engine socket of a mount when the app asked
// for one. It returns at once; the socket serves on its own goroutines.
func jmStartEngineDoor(c *cli.Context, v *vfs.VFS, format *meta.Format, registry *prometheus.Registry) {
	sfd, lfd := c.Int("jm-door-start-fd"), c.Int("jm-door-listen-fd")
	if sfd <= 0 || lfd <= 0 {
		return
	}
	start := os.NewFile(uintptr(sfd), "jm-door-start")
	lfile := os.NewFile(uintptr(lfd), "jm-door-listen")
	if start == nil || lfile == nil {
		logger.Errorf("jm-door: the start pipe and the socket must be inherited; no engine socket")
		return
	}
	ln, err := net.FileListener(lfile)
	_ = lfile.Close()
	if err != nil {
		logger.Errorf("jm-door: inherited socket: %s; no engine socket", err)
		_ = start.Close()
		return
	}
	ul, ok := ln.(*net.UnixListener)
	if !ok {
		logger.Errorf("jm-door: inherited socket is not a Unix socket; no engine socket")
		_ = ln.Close()
		_ = start.Close()
		return
	}
	ul.SetUnlinkOnClose(false) // the app owns the path
	go func() {
		defer start.Close()
		msg, err := wire.ReadStartMessage(start)
		if err != nil {
			logger.Errorf("jm-door: read start message: %s; no engine socket", err)
			_ = ul.Close()
			return
		}
		token, err := hex.DecodeString(msg.Token)
		if err != nil || len(token) < 16 {
			logger.Errorf("jm-door: the start message has no valid launch token; no engine socket")
			_ = ul.Close()
			return
		}
		var eb [8]byte
		if _, err := rand.Read(eb[:]); err != nil {
			_ = ul.Close()
			return
		}
		srv := jmengine.New(jmengine.Config{
			Listener: ul, Token: token, Version: version.Version(),
			Epoch:  binary.BigEndian.Uint64(eb[:]),
			Engine: &jmDoorVFS{jmEngineVFS{v: v, format: format, reg: registry}},
			Logf:   logger.Infof,
		})
		srv.NoteMetaOK()
		logger.Infof("jm-door ready: engine socket beside the mount, volume %s, version %s", format.Name, version.Version())
		go func() {
			if err := srv.Serve(); err != nil {
				logger.Errorf("jm-door: socket failed: %s", err)
			}
		}()
		// The app holds the write end for its whole life. End of file means
		// it is gone: stop serving; the mount itself carries on as today.
		_, _ = io.Copy(io.Discard, start)
		logger.Infof("jm-door: the app closed the start pipe; the engine socket stops")
		srv.Close(5 * time.Second)
	}()
}
