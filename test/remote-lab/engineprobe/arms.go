package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

// handle is an open file of either arm.
type handle interface {
	ReadAt(p []byte, off int64) (int, error)
	Close() error
}

// arm is one way to reach the volume.
type arm interface {
	Name() string
	// Resolve turns a volume path into an inode. The fuse arm has no inode
	// step (the kernel looks names up inside open); it returns 0.
	Resolve(path string) (uint64, error)
	// Open opens a file for reading, by inode when the arm has one.
	Open(path string, ino uint64) (handle, error)
	// WriteFile creates path (through a temporary name and a rename when
	// rename is set, as the app's drain does) with data.
	WriteFile(path string, data []byte, rename bool) error
	Mkdir(path string) error
	// Refresh tells the engine that another client changed the file (the
	// JMRefreshInode control message, op 1107, as the app sends it).
	Refresh(path string, ino uint64) (flags byte, err error)
	// Metrics returns the engine's operation counters: meta operations,
	// object requests and (fuse arm) FUSE operations, by method.
	Metrics() map[string]float64
	Close()
}

// ---------------------------------------------------------------- fuse arm

// fuseArm reads through a mount. With openat it opens each file relative to
// a descriptor of its folder that it keeps open (tuning arm a1(i) of the
// report: the kernel then looks up only the file's own name).
type fuseArm struct {
	mnt    string
	openat bool
	mu     sync.Mutex
	dirs   map[string]int
}

func (a *fuseArm) Name() string                   { return "fuse" }
func (a *fuseArm) Resolve(string) (uint64, error) { return 0, nil }
func (a *fuseArm) Mkdir(p string) error           { return os.Mkdir(filepath.Join(a.mnt, p), 0o755) }
func (a *fuseArm) Close()                         {}
func (a *fuseArm) full(p string) string           { return filepath.Join(a.mnt, p) }
func (a *fuseArm) Metrics() map[string]float64    { return readStats(filepath.Join(a.mnt, ".stats")) }
func (a *fuseArm) Open(p string, _ uint64) (handle, error) {
	var f *os.File
	var err error
	if a.openat {
		f, err = a.openAt(p)
	} else {
		f, err = os.Open(a.full(p))
	}
	if err != nil {
		return nil, err
	}
	// The kernel's own read-ahead would make one read fetch the next; the
	// two arms are compared on the reads the probe asks for.
	noReadahead(f)
	return f, nil
}

func (a *fuseArm) openAt(p string) (*os.File, error) {
	dir, name := filepath.Split(a.full(p))
	a.mu.Lock()
	fd, ok := a.dirs[dir]
	if !ok {
		var err error
		if fd, err = unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY, 0); err != nil {
			a.mu.Unlock()
			return nil, err
		}
		if a.dirs == nil {
			a.dirs = map[string]int{}
		}
		a.dirs[dir] = fd
	}
	a.mu.Unlock()
	nfd, err := unix.Openat(fd, name, unix.O_RDONLY, 0)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: p, Err: err}
	}
	return os.NewFile(uintptr(nfd), p), nil
}

func (a *fuseArm) WriteFile(p string, data []byte, rename bool) error {
	dst := a.full(p)
	tmp := dst
	if rename {
		tmp = filepath.Join(filepath.Dir(dst), ".tmp-"+filepath.Base(dst))
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if rename {
		return os.Rename(tmp, dst)
	}
	return nil
}

func (a *fuseArm) Refresh(p string, _ uint64) (byte, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(a.full(p), &st); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(filepath.Join(a.mnt, controlName()), os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if _, err := f.Write(refreshMsg(2, st.Ino)); err != nil {
		return 0, err
	}
	return readRefreshReply(f.Read)
}

// ---------------------------------------------------------------- direct arm

type directArm struct {
	v        *vfs.VFS
	m        meta.Meta
	blob     object.ObjectStorage
	registry *prometheus.Registry
	ctx      vfs.LogContext
	control  struct {
		ino uint64
		fh  uint64
	}
}

// engineFlags are the app's juicefs mount flags (health/fuse.go and the
// lab's MOUNT_FLAGS), as fields. Only flags the engine reads matter; the
// kernel-cache ones (attr/entry cache) have no meaning without a mount.
type engineFlags struct {
	cacheDir     string
	cacheSizeMiB int64
	bufferMiB    int64
	readahead    int64
	prefetch     int
	maxUploads   int
	demandFirst  bool
	readOnly     bool
	openCache    time.Duration
	bucket       string // overrides the bucket URL stored in the format, as the app's --bucket does
}

func openDirect(metaURL string, f engineFlags) (*directArm, error) {
	mc := meta.DefaultConf()
	mc.Retries = 3
	mc.MaxDeletes = 0
	mc.NoBGJob = true
	mc.Heartbeat = 60 * time.Second
	mc.ReadOnly = f.readOnly
	mc.OpenCache = f.openCache
	mc.OpenCacheLimit = 10000
	mc.SkipDirNlink = 20
	mc.SkipDirMtime = 100 * time.Millisecond
	mc.AtimeMode = meta.NoAtime
	mc.MountPoint = "/engineprobe"
	m := meta.NewClient(metaURL, mc)
	format, err := m.Load(true)
	if err != nil {
		return nil, fmt.Errorf("load format: %w", err)
	}
	if c := strings.ToLower(format.Compression); c != "" && c != "none" {
		return nil, fmt.Errorf("the volume is compressed (%s): this probe build has no zstd/lz4 (see build.sh)", c)
	}
	if f.bucket != "" {
		format.Bucket = f.bucket
	}
	blob, err := createStorage(*format)
	if err != nil {
		return nil, fmt.Errorf("object storage: %w", err)
	}
	cc := chunk.Config{
		BlockSize:         format.BlockSize * 1024,
		Compress:          format.Compression,
		HashPrefix:        format.HashPrefix,
		Encrypted:         format.EncryptKey != "",
		GetTimeout:        2 * time.Minute,
		PutTimeout:        3 * time.Minute,
		MaxUpload:         f.maxUploads,
		MaxStageWrite:     1000,
		MaxRetries:        3,
		Prefetch:          f.prefetch,
		DemandFirst:       f.demandFirst,
		BufferSize:        uint64(f.bufferMiB) << 20,
		Readahead:         int(f.readahead),
		CacheDir:          f.cacheDir,
		CacheSize:         uint64(f.cacheSizeMiB) << 20,
		FreeSpace:         0.1,
		CacheMode:         0o600,
		CacheFullBlock:    true,
		CacheChecksum:     "extend",
		CacheEviction:     "2-random",
		CacheScanInterval: time.Hour,
		OSCache:           true,
		AutoCreate:        true,
		UploadLimit:       format.UploadLimit * 1e6 / 8,
		DownloadLimit:     format.DownloadLimit * 1e6 / 8,
	}
	cc.SelfCheck(format.UUID)
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_", registry)
	store := chunk.NewCachedStore(blob, cc, registerer)
	m.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	m.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
		return vfs.Compact(cc, store, args[0].([]meta.Slice), args[1].(uint64))
	})
	if err := m.NewSession(true); err != nil {
		return nil, fmt.Errorf("new session: %w", err)
	}
	conf := &vfs.Config{
		Meta: mc, Format: *format, Version: "engineprobe", Chunk: &cc,
		Security: &vfs.SecurityConfig{}, Port: &vfs.Port{}, FuseOpts: &vfs.FuseOptions{},
		Pid: os.Getpid(), PPid: os.Getppid(), UMask: 0xFFFF,
	}
	v := vfs.NewVFS(conf, m, store, registerer, registry)
	m.InitMetrics(registerer)
	vfs.InitMetrics(registerer)
	a := &directArm{v: v, m: m, blob: blob, registry: registry}
	gids := []uint32{uint32(os.Getgid())}
	a.ctx = vfs.NewLogContext(meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), gids))
	return a, nil
}

func (a *directArm) Name() string { return "direct" }

func (a *directArm) Close() {
	_ = a.v.FlushAll("")
	_ = a.m.CloseSession()
	object.Shutdown(a.blob)
}

// Resolve looks up each part of the path, one metadata call per part, as a
// kernel without a name cache does. The product would know the inode from
// its mirror and skip this (see Open with ino != 0).
func (a *directArm) Resolve(p string) (uint64, error) {
	ino := meta.RootInode
	for _, part := range strings.Split(strings.Trim(p, "/"), "/") {
		if part == "" {
			continue
		}
		e, st := a.v.Lookup(a.ctx, ino, part)
		if st != 0 {
			return 0, fmt.Errorf("lookup %s: %w", part, st)
		}
		ino = e.Inode
	}
	return uint64(ino), nil
}

type directHandle struct {
	a   *directArm
	ino meta.Ino
	fh  uint64
}

func (h *directHandle) ReadAt(p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		n, st := h.a.v.Read(h.a.ctx, h.ino, p[total:], uint64(off)+uint64(total), h.fh)
		if st != 0 {
			return total, st
		}
		if n == 0 {
			return total, io.EOF
		}
		total += n
	}
	return total, nil
}

func (h *directHandle) Close() error { h.a.v.Release(h.a.ctx, h.ino, h.fh); return nil }

func (a *directArm) Open(p string, ino uint64) (handle, error) {
	if ino == 0 {
		var err error
		if ino, err = a.Resolve(p); err != nil {
			return nil, err
		}
	}
	_, fh, st := a.v.Open(a.ctx, meta.Ino(ino), syscall.O_RDONLY)
	if st != 0 {
		return nil, fmt.Errorf("open %s: %w", p, st)
	}
	return &directHandle{a: a, ino: meta.Ino(ino), fh: fh}, nil
}

func (a *directArm) parent(p string) (meta.Ino, string, error) {
	dir, name := filepath.Split(strings.Trim(p, "/"))
	pi, err := a.Resolve(dir)
	return meta.Ino(pi), name, err
}

func (a *directArm) Mkdir(p string) error {
	pi, name, err := a.parent(p)
	if err != nil {
		return err
	}
	if _, st := a.v.Mkdir(a.ctx, pi, name, 0o755, 0o022); st != 0 {
		return st
	}
	return nil
}

// WriteFile resolves the folder each time, as the fuse arm's kernel would
// with its name cache warm; the cost that matters here is create, write,
// flush and rename.
func (a *directArm) WriteFile(p string, data []byte, rename bool) error {
	pi, name, err := a.parent(p)
	if err != nil {
		return err
	}
	return a.writeIn(pi, name, data, rename)
}

func (a *directArm) writeIn(pi meta.Ino, name string, data []byte, rename bool) error {
	tmp := name
	if rename {
		tmp = ".tmp-" + name
	}
	e, fh, st := a.v.Create(a.ctx, pi, tmp, 0o644, 0o022, syscall.O_WRONLY)
	if st != 0 {
		return fmt.Errorf("create %s: %w", tmp, st)
	}
	if st = a.v.Write(a.ctx, e.Inode, data, 0, fh); st != 0 {
		a.v.Release(a.ctx, e.Inode, fh)
		return fmt.Errorf("write %s: %w", tmp, st)
	}
	st = a.v.Flush(a.ctx, e.Inode, fh, 0)
	a.v.Release(a.ctx, e.Inode, fh)
	if st != 0 {
		return fmt.Errorf("flush %s: %w", tmp, st)
	}
	if rename {
		if st = a.v.Rename(a.ctx, pi, tmp, pi, name, 0); st != 0 {
			return fmt.Errorf("rename %s: %w", name, st)
		}
	}
	return nil
}

func (a *directArm) Refresh(p string, ino uint64) (byte, error) {
	if ino == 0 {
		var err error
		if ino, err = a.Resolve(p); err != nil {
			return 0, err
		}
	}
	if a.control.fh == 0 {
		e, st := a.v.Lookup(a.ctx, meta.RootInode, ".control")
		if st != 0 {
			return 0, fmt.Errorf("lookup .control: %w", st)
		}
		_, fh, st := a.v.Open(a.ctx, e.Inode, syscall.O_RDWR)
		if st != 0 {
			return 0, fmt.Errorf("open .control: %w", st)
		}
		a.control.ino, a.control.fh = uint64(e.Inode), fh
	}
	ci := meta.Ino(a.control.ino)
	if st := a.v.Write(a.ctx, ci, refreshMsg(2, ino), 0, a.control.fh); st != 0 {
		return 0, fmt.Errorf("write .control: %w", st)
	}
	return readRefreshReply(func(b []byte) (int, error) {
		n, st := a.v.Read(a.ctx, ci, b, 0, a.control.fh)
		if st != 0 {
			return n, st
		}
		return n, nil
	})
}

func (a *directArm) Metrics() map[string]float64 {
	return parseStats(vfs.CollectMetrics(a.registry))
}

// ---------------------------------------------------------------- shared

// refreshMsg is the JMRefreshInode control message (patch 0031/0032):
// cmd(4) size(4) version(1) inode(8), big endian.
func refreshMsg(version byte, ino uint64) []byte {
	body := binary.BigEndian.AppendUint64([]byte{version}, ino)
	msg := binary.BigEndian.AppendUint32(nil, uint32(meta.JMRefreshInode))
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(body)))
	return append(msg, body...)
}

// readRefreshReply polls the control file until the 18-byte reply (or a
// one-byte errno) is there. Flag 16 (Pending: a kernel notify still waits)
// is returned as is; the fuse arm reports it, the direct arm has no kernel.
func readRefreshReply(read func([]byte) (int, error)) (byte, error) {
	var got []byte
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		b := make([]byte, 64)
		n, err := read(b)
		got = append(got, b[:n]...)
		if len(got) >= 1 && got[0] != 0 {
			return 0, fmt.Errorf("refresh: errno %d", got[0])
		}
		if len(got) >= 18 {
			return got[1], nil
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		time.Sleep(2 * time.Millisecond)
	}
	return 0, errors.New("refresh: no reply in 30 s")
}

// readStats reads a mount's .stats file (Prometheus text).
func readStats(p string) map[string]float64 {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil
	}
	return parseStats(b)
}

// parseStats keeps the per-method operation counts from vfs.CollectMetrics
// text: meta operations (juicefs_meta_ops_total_Lookup 3), FUSE operations
// (juicefs_fuse_ops_total_open), object requests (..._seconds_GET_total) and
// transactions.
func parseStats(b []byte) map[string]float64 {
	kinds := []struct{ prefix, kind string }{
		{"juicefs_meta_ops_total_", "meta"},
		{"juicefs_object_request_durations_histogram_seconds_", "object"},
		{"juicefs_fuse_ops_total_", "fuse"},
		{"juicefs_transaction_durations_histogram_seconds", "txn"},
		{"juicefs_object_request_data_bytes_", "bytes"},
	}
	out := map[string]float64{}
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		name, val, ok := strings.Cut(sc.Text(), " ")
		if !ok {
			continue
		}
		for _, k := range kinds {
			if rest, found := strings.CutPrefix(name, k.prefix); found {
				if k.kind == "object" || k.kind == "txn" {
					if !strings.HasSuffix(rest, "_total") {
						break
					}
					rest = strings.TrimSuffix(rest, "_total")
				}
				method := strings.TrimPrefix(rest, "_")
				if v, err := strconv.ParseFloat(val, 64); err == nil {
					out[k.kind+"."+method] += v
				}
				break
			}
		}
	}
	return out
}

func diffMetrics(before, after map[string]float64) map[string]float64 {
	d := map[string]float64{}
	for k, v := range after {
		if x := v - before[k]; x != 0 {
			d[k] = x
		}
	}
	return d
}

// createStorage is cmd/format.go's createStorage without the TLS client
// options: the volume's object store, with its prefix and encryption.
func createStorage(format meta.Format) (object.ObjectStorage, error) {
	if err := format.Decrypt(); err != nil {
		return nil, fmt.Errorf("format decrypt: %s", err)
	}
	var blob object.ObjectStorage
	var err error
	if format.Shards > 1 {
		blob, err = object.NewSharded(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.SessionToken, format.Shards)
	} else {
		blob, err = object.CreateStorage(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.SessionToken)
	}
	if err != nil {
		return nil, err
	}
	blob = object.WithPrefix(blob, format.Name+"/")
	if format.EncryptKey != "" {
		passphrase := os.Getenv("JFS_RSA_PASSPHRASE")
		if passphrase == "" {
			if block, _ := pem.Decode([]byte(format.EncryptKey)); block != nil && strings.Contains(block.Headers["Proc-Type"], "ENCRYPTED") {
				return nil, errors.New("the volume key needs JFS_RSA_PASSPHRASE")
			}
		}
		key, err := object.ParseRsaPrivateKeyFromPem([]byte(format.EncryptKey), []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("parse rsa: %s", err)
		}
		enc, err := object.NewDataEncryptor(object.NewRSAEncryptor(key), format.EncryptAlgo)
		if err != nil {
			return nil, err
		}
		blob = object.NewEncrypted(blob, enc)
	}
	return blob, nil
}

func metaIno(i uint64) meta.Ino { return meta.Ino(i) }
