package nfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Peer edits on a Mac (laptop run 2026-11-14, bugs P1 and P2).
//
// When the metadata mirror learns that another client changed a file (the
// attr push, a folder reconcile, a juicemount:metadata update), the bytes
// this process reads through juicefs must change too. Three caches hold the
// old version: juicefs's slice list of the open file, juicefs's read buffers,
// and the kernel's pages of the FUSE file. #94 reopened the pooled descriptor
// to refresh them, which works on Linux. On macOS it does nothing while the
// file is being read: macFUSE gives all descriptors of a file one FUSE
// handle, so the second open() never reaches juicefs (12 of 12 lan trials).
//
// So the handler sends juicefs the JMRefreshInode control message (patch
// 0031, third_party/juicefs/pkg/vfs/jm_refresh.go), which drops juicefs's
// two caches of the inode for the descriptors that are already open and
// sends the kernel a notify for the whole file. The reply says whether the
// kernel accepted the notify (not, on macOS, whether macFUSE purged pages).
//
// A refresh that times out, fails, or whose reply lacks the kernel's notify
// acceptance (review C2) leaves the old bytes possibly cached, so the READs
// that waited for it answer EAGAIN (NFS JUKEBOX: the Mac retries) instead of
// reading on. The retry sends the refresh again. After refreshRetryBudget
// failed refreshes of one path, READs of it go on (and log), so a wedged
// juicefs or a kernel without notify cannot make a file unreadable.
//
// Ordering (P2). The NFS attributes of a READ come from the mirror when the
// READ starts (OpenFile's snapshot). The content hooks run BEFORE the mirror
// changes. A refresh is registered in the hook, and every READ of that path
// waits for the pending refresh before it reads any byte. Therefore a READ
// whose reply carries the new mtime or size started after the hook, and its
// bytes are read after the refresh: never old bytes under new attributes.
// The Mac keeps pages under the attributes they came with, so old bytes
// under the new mtime stayed on the Mac for good (20 of 24 trials). A READ
// that started before the hook carries the old attributes; whatever bytes it
// returns, the Mac drops them when it sees the new mtime.

// jfsMsgRefreshInode is meta.JMRefreshInode (patch 0031).
const jfsMsgRefreshInode = 1107

// The flags of a refresh reply (vfs.JMRefreshKernel*). Accepted means the
// kernel accepted the notify that drops the file's attributes and pages
// (offset 0, length 0: the whole file); whether macFUSE then purges its
// pages is the laptop's to confirm.
const (
	jfsRefreshKernelAccepted = 1
	jfsRefreshKernelNone     = 2
	jfsRefreshKernelTimeout  = 4
	jfsRefreshKernelFailed   = 8
)

const jfsRefreshReplyLen = 1 + 1 + 8 + 8

// refreshInode asks juicefs to drop its caches of inode. It returns the
// reply's kernel flags and the file's length and mtime as juicefs read them.
func (c *jfsControl) refreshInode(inode uint64, timeout time.Duration) (flags byte, length uint64, mtimeNS uint64, err error) {
	body := binary.BigEndian.AppendUint64([]byte{1}, inode)
	msg := append(controlHeader(jfsMsgRefreshInode, len(body)), body...)
	reply, err := c.exchangeUntil(msg, jfsRefreshReplyLen, func(r []byte) int {
		if len(r) > 0 && r[0] != 0 {
			return 1
		}
		return jfsRefreshReplyLen
	}, timeout)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(reply) == 0 {
		return 0, 0, 0, errors.New("juicefs refresh: empty reply")
	}
	if syscall.Errno(reply[0]) == syscall.EINVAL {
		// A juicefs without patch 0031 refuses the unknown message with one
		// EINVAL byte; a patched one refuses only a malformed body.
		return 0, 0, 0, errControlUnsupported
	}
	if err := controlErrno(reply[0]); err != nil {
		return 0, 0, 0, err
	}
	if len(reply) < jfsRefreshReplyLen {
		return 0, 0, 0, errors.New("juicefs refresh: short reply")
	}
	return reply[1], binary.BigEndian.Uint64(reply[2:10]), binary.BigEndian.Uint64(reply[10:18]), nil
}

// contentRefresher runs one refresh at a time per path and lets READs of
// that path wait for it.
type contentRefresher struct {
	mu      sync.Mutex
	pending map[string]*refreshFlight
	// waiting counts pending paths; READs check it without the lock.
	waiting atomic.Int64
	// run sends the refresh for one inode and reloads the kernel's
	// attributes of fusePath. Tests replace it.
	run func(inode uint64, fusePath string) error
	// unsupported: juicefs lacks patch 0031; stop sending.
	unsupported atomic.Bool

	// failedPaths holds the paths whose last refresh failed, for the READ
	// side to retry. Guarded by mu; failedCount mirrors its length.
	failedPaths map[string]*refreshFailure
	failedCount atomic.Int64

	refreshed, failed atomic.Int64
}

type refreshFlight struct {
	done     chan struct{}
	again    bool
	inode    uint64
	fusePath string
	err      error // the last run's result, set before done closes
}

type refreshFailure struct {
	inode    uint64
	fusePath string
	attempts int
}

// refreshRetryBudget is the number of failed refreshes in a row, for one
// edit of one path, after which its READs read on instead of answering EAGAIN.
const refreshRetryBudget = 3

// errRefreshPending: the READ waited past the bound. errRefreshFailed: the
// refresh ended without confirming the caches were dropped. Both become
// EAGAIN (JUKEBOX) for the READ.
var (
	errRefreshPending = errors.New("content refresh still pending")
	errRefreshFailed  = errors.New("content refresh not confirmed")
)

// errRefreshUnconfirmed: juicefs refreshed its own caches, but the kernel did
// not accept the notify that drops its pages of the file.
var errRefreshUnconfirmed = errors.New("kernel did not accept the page notify")

func newContentRefresher(run func(uint64, string) error) *contentRefresher {
	return &contentRefresher{pending: make(map[string]*refreshFlight), failedPaths: make(map[string]*refreshFailure), run: run}
}

// begin registers a refresh of name and starts it. It never blocks on FUSE:
// the content hooks call it while the metadata Store holds its locks. A
// second begin while one runs makes the flight run once more, and READs wait
// for the last run.
func (c *contentRefresher) begin(name string, inode uint64, fusePath string) {
	if c == nil || inode == 0 || c.unsupported.Load() {
		return
	}
	c.mu.Lock()
	if fail := c.failedPaths[name]; fail != nil {
		// A new edit: its refresh gets the whole retry budget again.
		fail.attempts = 0
	}
	if f := c.pending[name]; f != nil {
		f.again = true
		f.inode, f.fusePath = inode, fusePath
		c.mu.Unlock()
		return
	}
	c.startLocked(name, inode, fusePath)
	c.mu.Unlock()
}

func (c *contentRefresher) startLocked(name string, inode uint64, fusePath string) *refreshFlight {
	f := &refreshFlight{done: make(chan struct{}), inode: inode, fusePath: fusePath}
	c.pending[name] = f
	c.waiting.Add(1)
	go c.loop(name, f)
	return f
}

func (c *contentRefresher) loop(name string, f *refreshFlight) {
	c.mu.Lock()
	for {
		inode, fusePath := f.inode, f.fusePath
		f.again = false
		c.mu.Unlock()
		start := time.Now()
		err := c.run(inode, fusePath)
		switch {
		case errors.Is(err, errControlUnsupported):
			c.unsupported.Store(true)
			jmlog.Warn("content refresh: the bundled juicefs has no refresh message (patch 0031); a teammate's edit of a file this Mac holds open may stay old", "path", name)
			err = nil
		case errors.Is(err, syscall.ENOENT):
			// The inode is gone: there are no caches of it to keep.
			err = nil
		case err != nil:
			c.failed.Add(1)
			jmlog.Warn("content refresh failed; reads of the file answer JUKEBOX until a refresh succeeds", "path", name, "inode", inode, "ms", time.Since(start).Milliseconds(), "err", err)
		default:
			c.refreshed.Add(1)
		}
		c.mu.Lock()
		f.err = err
		if !f.again || c.unsupported.Load() {
			break
		}
	}
	prev := c.failedPaths[name]
	switch {
	case f.err == nil:
		if prev != nil {
			delete(c.failedPaths, name)
			c.failedCount.Add(-1)
		}
	case prev == nil:
		c.failedPaths[name] = &refreshFailure{inode: f.inode, fusePath: f.fusePath, attempts: 1}
		c.failedCount.Add(1)
	default:
		prev.inode, prev.fusePath = f.inode, f.fusePath
		prev.attempts++
	}
	delete(c.pending, name)
	c.waiting.Add(-1)
	c.mu.Unlock()
	close(f.done)
}

// wait blocks until no refresh of name is pending, at most bound. nil: the
// READ may read (no refresh pending, the last one succeeded, or the path's
// retry budget is spent). errRefreshPending: the bound passed.
// errRefreshFailed: the refresh did not confirm; a READ after a failure
// starts the refresh again and waits for that one.
func (c *contentRefresher) wait(name string, bound time.Duration) error {
	if c == nil || c.waiting.Load()+c.failedCount.Load() == 0 {
		return nil
	}
	c.mu.Lock()
	f := c.pending[name]
	if f == nil {
		fail := c.failedPaths[name]
		switch {
		case fail == nil || c.unsupported.Load():
			c.mu.Unlock()
			return nil
		case fail.attempts >= refreshRetryBudget:
			c.mu.Unlock()
			return nil
		}
		f = c.startLocked(name, fail.inode, fail.fusePath)
	}
	c.mu.Unlock()
	t := time.NewTimer(bound)
	defer t.Stop()
	select {
	case <-f.done:
	case <-t.C:
		return errRefreshPending
	}
	c.mu.Lock()
	err := f.err
	fail := c.failedPaths[name]
	c.mu.Unlock()
	if err != nil && (fail == nil || fail.attempts < refreshRetryBudget) {
		return errRefreshFailed
	}
	return nil
}

// contentRefreshBound bounds how long a READ waits for a pending refresh.
// The refresh is the shared control mutex (another control message may hold
// it), one local control exchange, one metadata round trip inside juicefs,
// and up to jmRefreshNotifyWait (2 s) for the kernel to accept the notify.
// The bound covers all of that with a margin; past it the READ answers
// JUKEBOX, never old bytes.
var contentRefreshBound = func() time.Duration {
	if netprofile.Default().HighLatency() {
		return 8 * time.Second
	}
	return 5 * time.Second
}

// contentRefreshTimeout bounds one refresh exchange.
const contentRefreshTimeout = 10 * time.Second

// refreshContentNow is the production refresh: the control message, then a
// stat of the FUSE path. The stat makes the kernel reload the attributes the
// message dropped, so a read through a descriptor that is already open sees
// an appended length (juicefs answers it from the refresh's own read, with
// no round trip, patch 0028).
func (h *JuiceMountHandler) refreshContentNow(inode uint64, fusePath string) error {
	start := time.Now()
	flags, length, _, err := sharedJFSControl(h.fusePath).refreshInode(inode, contentRefreshTimeout)
	if err != nil {
		return err
	}
	// One line per teammate edit of a file this process holds, as the attr
	// push logs "refreshed": the laptop check reads it beside juicefs's
	// access log (a new read of the inode after it).
	jmlog.Info("content refresh: juicefs dropped the open file's caches", "path", fusePath, "inode", inode,
		"length", length, "kernel_notify_accepted", flags&jfsRefreshKernelAccepted != 0, "ms", time.Since(start).Milliseconds())
	_, _ = os.Stat(fusePath)
	if flags&jfsRefreshKernelAccepted == 0 {
		jmlog.Warn("content refresh: the kernel did not accept the notify that drops the file's pages", "inode", inode,
			"no_notify", flags&jfsRefreshKernelNone != 0, "timeout", flags&jfsRefreshKernelTimeout != 0,
			"failed", flags&jfsRefreshKernelFailed != 0)
		return errRefreshUnconfirmed
	}
	return nil
}

// refreshContent starts a refresh of a file whose content another client
// changed, when this process may hold it open in juicefs. It must be called
// BEFORE the pooled descriptor is dropped (the deferred close would hide that
// a handle is still open) and before the mirror changes.
func (h *JuiceMountHandler) refreshContent(inMountPath string, inode uint64) {
	if h == nil || h.contentRefresh == nil || inMountPath == "" || inode == 0 || h.fdPool == nil {
		return
	}
	full := path.Join(h.fusePath, inMountPath)
	if !h.fdPool.RecentlyOpened(full) {
		return
	}
	h.contentRefresh.begin(inMountPath, inode, full)
}

// awaitContentRefresh is the READ side of the ordering above. A non-nil
// result is syscall.EAGAIN: the READ answers JUKEBOX and reads no byte.
func (h *JuiceMountHandler) awaitContentRefresh(name string) error {
	if h == nil || h.contentRefresh == nil {
		return nil
	}
	if err := h.contentRefresh.wait(name, contentRefreshBound()); err != nil {
		jmlog.Warn("content refresh: READ answers JUKEBOX", "path", name, "reason", err)
		return fmt.Errorf("%w: %w", syscall.EAGAIN, err)
	}
	return nil
}

// changedUnderRead reports that the file this READ started with was updated
// in place while it ran: the same mirror row (path, incarnation, store) and
// inode at a later revision, and no shorter (a teammate's in-place save or an
// append). The live FUSE path may serve that with the READ's attributes.
// Anything else keeps the established refusal: a replaced row (delete and
// create, even with the same inode number) must never let a retained handle
// read its successor, and a shorter file could end before the size the READ
// reports, which the premature-EOF guard turns into EIO.
func (f *cachedFile) changedUnderRead() bool {
	if f == nil || f.handler == nil || f.handler.store == nil || f.cacheIdentity.Inode == 0 {
		return false
	}
	old := f.cacheIdentity
	live, ok := f.handler.store.LookupCachedFileIdentity(f.name)
	if !ok || live == old {
		return false
	}
	return live.Path == old.Path && live.Inode == old.Inode && live.Inode == f.inode &&
		live.Version.Incarnation != "" && live.Version.Incarnation == old.Version.Incarnation &&
		live.Version.StoreID == old.Version.StoreID && live.Version.Revision > old.Version.Revision &&
		live.Size >= f.fileSize
}
