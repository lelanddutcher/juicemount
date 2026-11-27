/*
 * JuiceMount patch 0037: the engine as a process with no mount.
 *
 * `juicefs jm-engine META-URL [mount flags]` builds the engine exactly as
 * `juicefs mount` does (getMetaConf, getChunkConf, getVfsConf,
 * NewReloadableStorage, registerMetaMsg, NewSession, vfs.NewVFS) and then
 * serves it to the JuiceMount app over an inherited Unix socket instead of
 * a kernel mount. It never creates a mount point and never opens a FUSE
 * device.
 *
 * Step 1 ("shadow"): read-only, no block cache, and only hello, ping,
 * stats, getattr and shutdown. Nothing serves user data through it.
 *
 * File descriptors from the app:
 *
 *	3  the read end of a private pipe. The first message is the start
 *	   message (launch token, Redis password, object key). The app keeps
 *	   the write end open; end of file means the app is gone, and the
 *	   helper flushes and exits.
 *	4  the listening Unix socket, in a folder only the app's user can open.
 */

package cmd

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/jmengine"
	"github.com/juicedata/juicefs/pkg/jmengine/wire"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/expfmt"
	"github.com/urfave/cli/v2"
)

// jmEngineStopBound bounds the flush and session close at exit (the app's
// quit policy is the same 30 s).
const jmEngineStopBound = 30 * time.Second

func cmdJMEngine() *cli.Command {
	flags := expandFlags(mountFlags(), clientFlags(1.0), shareInfoFlags())
	flags = append(flags,
		&cli.IntFlag{Name: "jm-start-fd", Value: 3, Usage: "pipe with the start message from the app (JuiceMount)"},
		&cli.IntFlag{Name: "jm-listen-fd", Value: 4, Usage: "inherited listening Unix socket (JuiceMount)"},
	)
	return &cli.Command{
		Name:      "jm-engine",
		Action:    jmEngine,
		Category:  "SERVICE",
		Usage:     "Serve the engine to the JuiceMount app over a private socket (no mount)",
		ArgsUsage: "META-URL",
		Description: `
Started by the JuiceMount app only. It takes the mount flags, ignores the
ones that steer a kernel mount, and reads its secrets from a private pipe.`,
		Flags: flags,
	}
}

// jmEngineVFS adapts the VFS to jmengine.Engine.
type jmEngineVFS struct {
	v      *vfs.VFS
	format *meta.Format
	reg    *prometheus.Registry
	ro     bool
}

func (e *jmEngineVFS) VolumeID() string   { return e.format.UUID }
func (e *jmEngineVFS) VolumeName() string { return e.format.Name }

// SessionID is 0: a read-only engine records no session (step 1).
func (e *jmEngineVFS) SessionID() uint64 { return 0 }
func (e *jmEngineVFS) ReadOnly() bool    { return e.ro }

func (e *jmEngineVFS) GetAttr(ctx context.Context, ino uint64) (wire.Attr, syscall.Errno) {
	mctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	stop := context.AfterFunc(ctx, mctx.Cancel)
	defer stop()
	entry, st := e.v.GetAttr(vfs.NewLogContext(mctx), meta.Ino(ino), 0)
	if st != 0 {
		if ctx.Err() != nil {
			return wire.Attr{}, syscall.ETIMEDOUT
		}
		return wire.Attr{}, st
	}
	a := entry.Attr
	return wire.Attr{
		Inode: uint64(entry.Inode), Type: a.Typ, Mode: a.Mode, Uid: a.Uid, Gid: a.Gid,
		Nlink: a.Nlink, Size: a.Length,
		Atime: a.Atime*1e9 + int64(a.Atimensec),
		Mtime: a.Mtime*1e9 + int64(a.Mtimensec),
		Ctime: a.Ctime*1e9 + int64(a.Ctimensec),
	}, 0
}

func (e *jmEngineVFS) Metrics() string {
	mfs, err := e.reg.Gather()
	if err != nil {
		return ""
	}
	var b strings.Builder
	for _, mf := range mfs {
		_, _ = expfmt.MetricFamilyToText(&b, mf)
	}
	return b.String()
}

func jmEngine(c *cli.Context) error {
	setup(c, 1)
	addr := c.Args().Get(0)
	start := os.NewFile(uintptr(c.Int("jm-start-fd")), "jm-start")
	lfile := os.NewFile(uintptr(c.Int("jm-listen-fd")), "jm-listen")
	if start == nil || lfile == nil {
		return fmt.Errorf("jm-engine: the start pipe and the socket must be inherited")
	}
	msg, err := wire.ReadStartMessage(start)
	if err != nil {
		return fmt.Errorf("jm-engine: read start message: %w", err)
	}
	token, err := hex.DecodeString(msg.Token)
	if err != nil || len(token) < 16 {
		return fmt.Errorf("jm-engine: the start message has no valid launch token")
	}
	// Secrets stay inside this process: the Redis client reads
	// META_PASSWORD and the minio backend MINIO_* (as for juicefs mount),
	// but they never were in the environment this process started with.
	for k, v := range map[string]string{
		"META_PASSWORD": msg.MetaPassword, "MINIO_ACCESS_KEY": msg.AccessKey, "MINIO_SECRET_KEY": msg.SecretKey,
	} {
		if v != "" {
			_ = os.Setenv(k, v)
		}
	}
	ln, err := net.FileListener(lfile)
	if err != nil {
		return fmt.Errorf("jm-engine: inherited socket: %w", err)
	}
	_ = lfile.Close()
	ul, ok := ln.(*net.UnixListener)
	if !ok {
		return fmt.Errorf("jm-engine: inherited socket is not a Unix socket")
	}
	ul.SetUnlinkOnClose(false) // the app owns the path

	// Step 1: read-only, no block cache, no background jobs.
	metaConf := getMetaConf(c, "jm-engine", true)
	metaConf.NoBGJob = true
	metaCli := meta.NewClient(addr, metaConf)
	format, err := metaCli.Load(true)
	if err != nil {
		return err
	}
	chunkConf := getChunkConf(c, format)
	chunkConf.CacheSize = 0
	chunkConf.CacheDir = "memory"
	chunkConf.Writeback = false
	chunkConf.Prefetch = 0
	vfsConf := getVfsConf(c, metaConf, format, chunkConf)
	blob, err := NewReloadableStorage(format, metaCli, updateFormat(c))
	if err != nil {
		return fmt.Errorf("object storage: %s", err)
	}
	if st := metaCli.Chroot(meta.Background(), metaConf.Subdir); st != 0 {
		return st
	}
	registry := prometheus.NewRegistry() // never exposed on a port
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_", registry)
	registerer.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerer.MustRegister(collectors.NewGoCollector())
	store := chunk.NewCachedStore(blob, *chunkConf, registerer)
	registerMetaMsg(metaCli, store, chunkConf)
	if g, ok := metaCli.(meta.JMReadCompactionGate); ok && c.Bool("no-read-compaction") {
		g.JMSetReadCompaction(false) // JuiceMount patch 0036, as in mount
	}
	if err = metaCli.NewSession(true); err != nil {
		return fmt.Errorf("new session: %s", err)
	}
	metaCli.OnReload(func(f *meta.Format) {
		updateFormat(c)(f)
		store.UpdateLimit(f.UploadLimit, f.DownloadLimit)
	})
	v := vfs.NewVFS(vfsConf, metaCli, store, registerer, registry)
	metaCli.InitMetrics(registerer)
	vfs.InitMetrics(registerer)

	var eb [8]byte
	if _, err := rand.Read(eb[:]); err != nil {
		return err
	}
	srv := jmengine.New(jmengine.Config{
		Listener: ul, Token: token, Version: version.Version(),
		Epoch:  binary.BigEndian.Uint64(eb[:]),
		Engine: &jmEngineVFS{v: v, format: format, reg: registry, ro: metaConf.ReadOnly},
		Logf:   logger.Infof,
	})
	srv.NoteMetaOK() // the volume settings just loaded
	logger.Infof("jm-engine ready: volume %s, version %s, read-only %v", format.Name, version.Version(), metaConf.ReadOnly)

	appGone := make(chan struct{})
	go func() {
		// The app holds the write end for its whole life. End of file (or
		// any error) means it is gone: no orphan helper stays behind.
		_, _ = io.Copy(io.Discard, start)
		close(appGone)
	}()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	var why string
	select {
	case <-srv.ShutdownRequested():
		why = "shutdown requested"
	case <-appGone:
		why = "the app is gone"
	case s := <-sigs:
		why = "signal " + s.String()
	case err := <-served:
		why = fmt.Sprintf("socket failed: %v", err)
	}
	logger.Infof("jm-engine stopping: %s", why)
	var done atomic.Bool
	timer := time.AfterFunc(jmEngineStopBound, func() {
		if !done.Load() {
			logger.Errorf("jm-engine: stop passed %s; exiting without a clean close", jmEngineStopBound)
			os.Exit(3)
		}
	})
	srv.Close(5 * time.Second)
	if err := v.FlushAll(""); err != nil {
		logger.Errorf("flush all delayed data: %s", err)
	}
	err = metaCli.CloseSession()
	object.Shutdown(blob)
	done.Store(true)
	timer.Stop()
	logger.Infof("jm-engine exit: %s", why)
	return err
}
