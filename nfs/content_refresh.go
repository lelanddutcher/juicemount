package nfs

import (
	"encoding/binary"
	"errors"
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
// 0031, third_party/juicefs/pkg/vfs/jm_refresh.go), which drops all three
// caches of the inode for the descriptors that are already open.
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

// The flags of a refresh reply (vfs.JMRefreshKernel*).
const (
	jfsRefreshKernelDropped = 1
	jfsRefreshKernelNone    = 2
	jfsRefreshKernelTimeout = 4
	jfsRefreshKernelFailed  = 8
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

	refreshed, failed atomic.Int64
}

type refreshFlight struct {
	done     chan struct{}
	again    bool
	inode    uint64
	fusePath string
}

func newContentRefresher(run func(uint64, string) error) *contentRefresher {
	return &contentRefresher{pending: make(map[string]*refreshFlight), run: run}
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
	if f := c.pending[name]; f != nil {
		f.again = true
		f.inode, f.fusePath = inode, fusePath
		c.mu.Unlock()
		return
	}
	f := &refreshFlight{done: make(chan struct{}), inode: inode, fusePath: fusePath}
	c.pending[name] = f
	c.waiting.Add(1)
	c.mu.Unlock()
	go c.loop(name, f)
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
		case err != nil:
			c.failed.Add(1)
			jmlog.Warn("content refresh failed", "path", name, "inode", inode, "ms", time.Since(start).Milliseconds(), "err", err)
		default:
			c.refreshed.Add(1)
		}
		c.mu.Lock()
		if !f.again || c.unsupported.Load() {
			break
		}
	}
	delete(c.pending, name)
	c.waiting.Add(-1)
	c.mu.Unlock()
	close(f.done)
}

// wait blocks until no refresh of name is pending, at most bound. It reports
// whether the read may go on with refreshed caches (false: the bound passed).
func (c *contentRefresher) wait(name string, bound time.Duration) bool {
	if c == nil || c.waiting.Load() == 0 {
		return true
	}
	c.mu.Lock()
	f := c.pending[name]
	c.mu.Unlock()
	if f == nil {
		return true
	}
	t := time.NewTimer(bound)
	defer t.Stop()
	select {
	case <-f.done:
		return true
	case <-t.C:
		return false
	}
}

// contentRefreshWait bounds how long a READ waits for a pending refresh: the
// refresh is one local control exchange and one metadata round trip inside
// juicefs. Past it the READ goes on (and logs), so a wedged juicefs cannot
// turn every READ of the file into a stall of its own.
func contentRefreshWait() time.Duration {
	if netprofile.Default().HighLatency() {
		return 4 * time.Second
	}
	return 2 * time.Second
}

// contentRefreshTimeout bounds one refresh exchange.
const contentRefreshTimeout = 10 * time.Second

// refreshContentNow is the production refresh: the control message, then a
// stat of the FUSE path. The stat makes the kernel reload the attributes the
// message dropped, so a read through a descriptor that is already open sees
// an appended length (juicefs answers it from the refresh's own read, with
// no round trip, patch 0028).
func (h *JuiceMountHandler) refreshContentNow(inode uint64, fusePath string) error {
	flags, _, _, err := sharedJFSControl(h.fusePath).refreshInode(inode, contentRefreshTimeout)
	if err != nil {
		return err
	}
	if flags&jfsRefreshKernelDropped == 0 {
		jmlog.Warn("content refresh: the kernel did not confirm it dropped the file's pages", "inode", inode,
			"no_notify", flags&jfsRefreshKernelNone != 0, "timeout", flags&jfsRefreshKernelTimeout != 0,
			"failed", flags&jfsRefreshKernelFailed != 0)
	}
	_, _ = os.Stat(fusePath)
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

// awaitContentRefresh is the READ side of the ordering above.
func (h *JuiceMountHandler) awaitContentRefresh(name string) {
	if h == nil || h.contentRefresh == nil {
		return
	}
	if !h.contentRefresh.wait(name, contentRefreshWait()) {
		jmlog.Warn("content refresh still pending; reading on", "path", name)
	}
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
