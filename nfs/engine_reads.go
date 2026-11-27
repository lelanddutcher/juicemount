package nfs

import (
	"errors"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine/wire"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Engine-direct step 2 ("reads", JM_ENGINE=reads). A READ that misses the
// verified SSD route reads its bytes from the JuiceFS engine through the
// engine socket instead of the hidden FUSE mount. The socket is served by
// the same `juicefs mount` process ("both doors"): one engine, one block
// cache, one set of open files. Nothing here reads the engine's cache files
// or keeps file content: every byte comes from the engine's Read (design
// 5.2, rules 1-3).
//
// Every call names the file by inode, never by path. The handle pool keeps
// one engine handle per inode, so the engine's sequential-read detection
// sees one stream per file, and closes it after engineHandleIdle unused. An
// inode is identity in JuiceFS (a replaced file has a new one), so a pooled
// handle can never read "the new file with the old name".
//
// What stays as it is in this step: the refresh of a teammate's edit goes
// through the control file (message 1107). It refreshes every open reader of
// the inode inside the engine, the socket's handles included, and a READ
// still waits for a pending refresh before it reads (content_refresh.go).
// A handle is also reopened when the mirror's view of the file moved since
// the handle was opened (a fresh open reads fresh state).
//
// If the socket is not ready, or an open fails for a reason other than a
// timeout, the READ takes the FUSE path as before. Synthetic inodes (a file
// that exists only in the spool) never reach the engine (rule 6).

// EngineReadClient is one engine socket's read path (enginehost.Client).
type EngineReadClient interface {
	Open(ino uint64, timeout time.Duration) (wire.OpenReply, error)
	Read(ino, handle uint64, p []byte, off int64, timeout time.Duration) (int, error)
	Release(ino, handle uint64, timeout time.Duration) error
}

// EngineReadSource returns the current engine socket and its epoch; ok is
// false while none is ready.
type EngineReadSource func() (cl EngineReadClient, epoch uint64, ok bool)

// engineHandleIdle closes a pooled handle nobody used for this long
// (design 3.5).
var engineHandleIdle = 2 * time.Minute

// engineReadDeadline is the engine's own bound for one read. It is long on
// purpose: a read that outlives the NFS bound is not cancelled; the NFS
// request answers JUKEBOX and the Mac's retry joins the read still running
// (pendingFUSEReads, the same rule as the FUSE path).
var engineReadDeadline = pendingReadJoinMaxAge

// errEngineUnavailable: no engine socket is ready; read through FUSE.
var errEngineUnavailable = errors.New("engine socket not ready")

// engineReadStats counts the step-2 path for logs and the lab.
type engineReadStats struct {
	Opens, OpenFails, Fallbacks, Reopens, Reads, ReadBytes, ReadErrors, Releases, PrefetchReads atomic.Uint64
}

// EngineReads is the process-wide counter set.
var EngineReads engineReadStats

// EngineReadCounters is a snapshot for a log line.
func EngineReadCounters() map[string]uint64 {
	s := &EngineReads
	return map[string]uint64{
		"opens": s.Opens.Load(), "open_fails": s.OpenFails.Load(), "fallbacks": s.Fallbacks.Load(),
		"reopens": s.Reopens.Load(), "reads": s.Reads.Load(), "read_bytes": s.ReadBytes.Load(),
		"read_errors": s.ReadErrors.Load(), "releases": s.Releases.Load(), "prefetch_reads": s.PrefetchReads.Load(),
	}
}

// engineStamp is the mirror's view of a file when a READ began.
type engineStamp struct {
	size    int64
	mtime   int64
	version metadata.MirrorToken
}

type engineHandle struct {
	pool   *engineHandlePool
	ino    uint64
	cl     EngineReadClient
	epoch  uint64
	stamp  engineStamp
	opened time.Time // when the open was asked for

	ready chan struct{} // closed when the open finished
	fh    uint64
	size  int64 // the engine's length at open
	err   error

	// guarded by pool.mu
	refs    int
	retired bool
	broken  bool
	lastUse time.Time
}

type engineHandlePool struct {
	src  EngineReadSource
	mu   sync.Mutex
	m    map[uint64]*engineHandle
	stop chan struct{}
	once sync.Once
}

func newEngineHandlePool(src EngineReadSource) *engineHandlePool {
	p := &engineHandlePool{src: src, m: map[uint64]*engineHandle{}, stop: make(chan struct{})}
	go p.janitor()
	return p
}

// SetEngineReads turns the step-2 read path on (src non-nil) or off. Off is
// today's behaviour.
func (h *JuiceMountHandler) SetEngineReads(src EngineReadSource) {
	var p *engineHandlePool
	if src != nil {
		p = newEngineHandlePool(src)
	}
	if old := h.engineReads.Swap(p); old != nil {
		old.close()
	}
	if p != nil {
		// Read-ahead blocks stay on the mount door by default. Lab,
		// fast-remote: one 4 MiB block read through the socket is slower
		// than the mount's (the kernel splits it into parallel reads):
		// sequential 158-187 Mbit/s through the socket, 216-225 through the
		// mount, 226-230 with the level off; through the socket on the
		// demand reads' own handle it also delayed their first byte (0.45
		// vs 0.13-0.16 s). JM_ENGINE_PREFETCH=1 reads them through the
		// socket instead.
		prefetch := os.Getenv("JM_ENGINE_PREFETCH") == "1"
		jmlog.Info("engine reads on: READ misses read through the engine socket (JM_ENGINE=reads)",
			"readahead_through_engine", prefetch)
		if h.readahead != nil {
			// Rule 5 (read a block in pieces so a demand read of the file
			// does not queue behind it in macFUSE) has no purpose now:
			// demand reads do not go through macFUSE.
			h.readahead.demandOnEngine.Store(true)
			if prefetch {
				h.readahead.setEngineSource(p)
			}
		}
	} else if h.readahead != nil {
		h.readahead.demandOnEngine.Store(false)
		h.readahead.setEngineSource(nil)
	}
}

func (p *engineHandlePool) close() {
	p.once.Do(func() { close(p.stop) })
	p.mu.Lock()
	var idle []*engineHandle
	for ino, e := range p.m {
		delete(p.m, ino)
		e.retired = true
		if e.refs == 0 {
			idle = append(idle, e)
		}
	}
	p.mu.Unlock()
	for _, e := range idle {
		e.closeEngine()
	}
}

func (p *engineHandlePool) janitor() {
	t := time.NewTicker(engineHandleIdle / 4)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
		}
		now := time.Now()
		var idle []*engineHandle
		p.mu.Lock()
		for ino, e := range p.m {
			if e.refs == 0 && now.Sub(e.lastUse) >= engineHandleIdle {
				delete(p.m, ino)
				e.retired = true
				idle = append(idle, e)
			}
		}
		p.mu.Unlock()
		for _, e := range idle {
			e.closeEngine()
		}
	}
}

// acquire returns a ready handle of ino with one reference. stamp is the
// mirror's view at the READ; begun is when that view was taken. A pooled
// handle is reused unless the engine restarted (a new epoch), the handle
// broke, or the mirror's view moved and the handle was opened before the
// view was taken. anyStamp (read-ahead) reuses any live handle.
func (p *engineHandlePool) acquire(ino uint64, stamp engineStamp, begun time.Time, anyStamp bool, timeout time.Duration) (*engineHandle, error) {
	cl, epoch, ok := p.src()
	if !ok {
		return nil, errEngineUnavailable
	}
	p.mu.Lock()
	e := p.m[ino]
	if e != nil {
		stale := e.epoch != epoch || e.broken ||
			(!anyStamp && e.stamp != stamp && e.opened.Before(begun))
		if stale {
			EngineReads.Reopens.Add(1)
			p.retireLocked(e)
			e = nil
		}
	}
	creator := false
	if e == nil {
		e = &engineHandle{pool: p, ino: ino, cl: cl, epoch: epoch, stamp: stamp, opened: time.Now(),
			ready: make(chan struct{})}
		if anyStamp {
			e.stamp = engineStamp{size: -1}
		}
		p.m[ino] = e
		creator = true
	}
	e.refs++
	e.lastUse = time.Now()
	p.mu.Unlock()

	if creator {
		o, err := cl.Open(ino, timeout)
		if err != nil {
			EngineReads.OpenFails.Add(1)
			e.err = err
		} else {
			EngineReads.Opens.Add(1)
			e.fh, e.size = o.Handle, int64(o.Attr.Size)
		}
		close(e.ready)
		if err != nil {
			p.mu.Lock()
			if p.m[ino] == e {
				delete(p.m, ino)
			}
			e.retired = true
			e.refs--
			p.mu.Unlock()
			return nil, err
		}
		return e, nil
	}
	select {
	case <-e.ready:
	case <-time.After(timeout):
		p.release(e)
		return nil, os.ErrDeadlineExceeded
	}
	if e.err != nil {
		p.release(e)
		return nil, e.err
	}
	return e, nil
}

// retireLocked takes e out of the pool; the last reference closes it.
func (p *engineHandlePool) retireLocked(e *engineHandle) {
	if p.m[e.ino] == e {
		delete(p.m, e.ino)
	}
	if !e.retired {
		e.retired = true
		if e.refs == 0 {
			go e.closeEngine()
		}
	}
}

func (p *engineHandlePool) release(e *engineHandle) {
	p.mu.Lock()
	e.refs--
	e.lastUse = time.Now()
	last := e.retired && e.refs == 0
	p.mu.Unlock()
	if last {
		e.closeEngine()
	}
}

// markBroken retires e after a failure that may have left it unusable (a
// closed socket, an unknown handle).
func (p *engineHandlePool) markBroken(e *engineHandle) {
	p.mu.Lock()
	e.broken = true
	p.retireLocked(e)
	p.mu.Unlock()
}

// closeEngine releases the engine handle. An open that failed has none.
func (e *engineHandle) closeEngine() {
	select {
	case <-e.ready:
	default:
		return // still opening: its creator owns it and saw it retired
	}
	if e.err != nil {
		return
	}
	EngineReads.Releases.Add(1)
	// A failure here means the socket or the engine is gone, and the
	// handle with it.
	_ = e.cl.Release(e.ino, e.fh, 5*time.Second)
}

// readAt fills p from off through the engine, as os.File.ReadAt does: a
// short count comes with io.EOF. bound is the engine's deadline.
func (e *engineHandle) readAt(p []byte, off int64, bound time.Duration) (int, error) {
	got := 0
	for got < len(p) {
		end := len(p)
		if end-got > wire.MaxReadLen {
			end = got + wire.MaxReadLen
		}
		want := end - got
		n, err := e.cl.Read(e.ino, e.fh, p[got:end], off+int64(got), bound)
		got += n
		if err != nil {
			EngineReads.ReadErrors.Add(1)
			return got, engineReadError(e, err)
		}
		if n < want {
			EngineReads.Reads.Add(1)
			EngineReads.ReadBytes.Add(uint64(got))
			return got, io.EOF
		}
	}
	EngineReads.Reads.Add(1)
	EngineReads.ReadBytes.Add(uint64(got))
	return got, nil
}

// engineReadError maps a socket failure. An errno from the engine is
// returned as is (EIO, ENOENT...). A transport failure (closed socket,
// timeout of the call) marks the handle broken so the next READ reopens;
// it surfaces as EIO, which the cold-read retry repeats.
func engineReadError(e *engineHandle, err error) error {
	var se interface{ EngineStatus() syscall.Errno }
	if errors.As(err, &se) {
		errno := se.EngineStatus()
		if errno == syscall.EBADF {
			e.pool.markBroken(e)
			return syscall.EIO
		}
		return errno
	}
	var ne net.Error
	if (errors.As(err, &ne) && ne.Timeout()) || errors.Is(err, os.ErrDeadlineExceeded) {
		// The call's connection closed (that cancels the engine call);
		// the handle itself stays good. An offline READ keeps its pooled
		// handle this way: a reopen needs the NAS.
		return errFUSETimeout
	}
	e.pool.markBroken(e)
	jmlog.Debug("engine read failed on the socket; the handle is reopened on the next READ", "inode", e.ino, "error", err.Error())
	return syscall.EIO
}

// engineEligible: this READ may use the engine socket. Synthetic inodes
// (spool-only files), AppleDouble and the other sidecar names (their
// proofs are tied to a FUSE descriptor) stay on their present paths.
func (f *cachedFile) engineEligible() bool {
	if f.handler == nil || f.cachedInfo == nil || f.inode == 0 || f.inode&(1<<63) != 0 {
		return false
	}
	if isAppleDoubleNFSPath(f.name) || cacheableMetaName(path.Base(f.name)) {
		return false
	}
	return f.handler.engineReads.Load() != nil
}

func (f *cachedFile) engineStamp() engineStamp {
	return engineStamp{size: f.cachedInfo.size, mtime: f.cachedInfo.mtime.UnixNano(), version: f.cacheIdentity.Version}
}

// ensureReadSource prepares the source of a READ that left the cache
// routes: an engine handle when step 2 is on and the socket is ready, else
// the pooled FUSE descriptor as before.
func (f *cachedFile) ensureReadSource(trace *cachedReadTrace) error {
	if f.engineEligible() {
		start := time.Now()
		ok, err := f.ensureEngine()
		if trace != nil {
			trace.DescriptorAcquireNS += time.Since(start).Nanoseconds()
		}
		if ok || err != nil {
			return err
		}
	}
	return f.ensureFuseFDTraced(trace)
}

// ensureEngine acquires the pooled engine handle. ok false and a nil error:
// read through FUSE instead.
func (f *cachedFile) ensureEngine() (bool, error) {
	if err := f.handler.cacheBackendError(); err != nil {
		return false, err
	}
	f.fdMu.Lock()
	defer f.fdMu.Unlock()
	if f.closed {
		return false, os.ErrClosed
	}
	if f.eng != nil {
		return true, nil
	}
	pool := f.handler.engineReads.Load()
	if pool == nil {
		return false, nil
	}
	begun := f.openedAt
	if begun.IsZero() {
		begun = time.Now()
	}
	e, err := pool.acquire(f.inode, f.engineStamp(), begun, false, readOpenTimeout())
	if err == nil {
		f.eng = e
		return true, nil
	}
	var ne net.Error
	if errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) || errors.Is(err, syscall.ETIMEDOUT) {
		return false, errFUSETimeout // the Mac retries; as a FUSE open timeout
	}
	if !errors.Is(err, errEngineUnavailable) {
		jmlog.Debug("engine open failed; this READ reads through FUSE", "path", f.name, "inode", f.inode, "error", err.Error())
	}
	EngineReads.Fallbacks.Add(1)
	return false, nil
}

// engineReadAt is the engine twin of fuseFD.ReadAt, for the bounded read
// (boundedFUSEReadCall). It runs on the read goroutine.
func (f *cachedFile) engineReadAt(p []byte, off int64) (int, error) {
	return f.eng.readAt(p, off, engineReadDeadline)
}

// releaseEngine gives the pooled handle back (cachedFile.Close).
func (f *cachedFile) releaseEngine() {
	if f.eng != nil {
		f.eng.pool.release(f.eng)
		f.eng = nil
	}
}

// readSource is the descriptor or handle a READ reads through, for the
// JUKEBOX join key: a retry joins a read of the same source only.
func (f *cachedFile) readSource() any {
	if f.eng != nil {
		return f.eng
	}
	if f.fuseFD != nil {
		return f.fuseFD
	}
	return nil
}

// --- read-ahead through the engine -------------------------------------

// enginePrefetch reads speculative blocks of one inode through the engine.
// Reads of one file run side by side on the socket, so rule 5 of demand
// first (read a block in pieces so a demand read of the same file does not
// queue behind it in macFUSE) does not apply to it.
type enginePrefetch struct {
	e *engineHandle
}

func (s *enginePrefetch) ReadAt(p []byte, off int64) (int, error) {
	EngineReads.PrefetchReads.Add(1)
	return s.e.readAt(p, off, engineReadDeadline)
}

// Name names the source in read timeline lines.
func (s *enginePrefetch) Name() string { return "engine-inode-" + strconv.FormatUint(s.e.ino, 10) }

// Stat reports the length the engine had at open (a round's size bound).
func (s *enginePrefetch) Stat() (os.FileInfo, error) {
	return &snapshotFileInfo{size: s.e.size, mode: 0o644, inode: s.e.ino}, nil
}

// prefetchSource returns an engine source for ino, or nil.
func (p *engineHandlePool) prefetchSource(ino uint64) (*enginePrefetch, func()) {
	if ino == 0 || ino&(1<<63) != 0 {
		return nil, nil
	}
	e, err := p.acquire(ino, engineStamp{}, time.Time{}, true, readOpenTimeout())
	if err != nil {
		return nil, nil
	}
	return &enginePrefetch{e: e}, func() { p.release(e) }
}
