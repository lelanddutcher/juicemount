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
// changes. A hook registers a refresh, and a READ waits, before it reads any
// byte, for a refresh that started after every hook of its path that came
// before the READ (see Generations below). Therefore a READ whose reply
// carries the new mtime or size came after that edit's hook, and its bytes
// are read after a refresh that covers it: never old bytes under new
// attributes.
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
	flags, length, mtimeNS, _, err = c.refreshInodeTimed(inode, timeout)
	return flags, length, mtimeNS, err
}

// refreshInodeTimed is refreshInode that also reports how long it waited for
// the control mutex.
func (c *jfsControl) refreshInodeTimed(inode uint64, timeout time.Duration) (flags byte, length uint64, mtimeNS uint64, lockWait time.Duration, err error) {
	body := binary.BigEndian.AppendUint64([]byte{1}, inode)
	msg := append(controlHeader(jfsMsgRefreshInode, len(body)), body...)
	reply, lockWait, err := c.exchangeUntilTimed(msg, jfsRefreshReplyLen, func(r []byte) int {
		if len(r) > 0 && r[0] != 0 {
			return 1
		}
		return jfsRefreshReplyLen
	}, timeout)
	if err != nil {
		return 0, 0, 0, lockWait, err
	}
	if len(reply) == 0 {
		return 0, 0, 0, lockWait, errors.New("juicefs refresh: empty reply")
	}
	if syscall.Errno(reply[0]) == syscall.EINVAL {
		// A juicefs without patch 0031 refuses the unknown message with one
		// EINVAL byte; a patched one refuses only a malformed body.
		return 0, 0, 0, lockWait, errControlUnsupported
	}
	if err := controlErrno(reply[0]); err != nil {
		return 0, 0, 0, lockWait, err
	}
	if len(reply) < jfsRefreshReplyLen {
		return 0, 0, 0, lockWait, errors.New("juicefs refresh: short reply")
	}
	return reply[1], binary.BigEndian.Uint64(reply[2:10]), binary.BigEndian.Uint64(reply[10:18]), lockWait, nil
}

// contentRefresher runs the refreshes of each path, one at a time, and lets
// READs of the path wait for the refresh that covers them.
//
// Generations (bug F1, laptop run 2026-11-14). Each content hook of a path
// adds one to the path's want count. A refresh run takes the want count when
// it starts; when it succeeds, every hook up to that count is covered: the
// run dropped the caches after those hooks, so what juicefs reads after it is
// at least as new as each of their edits. A READ takes the want count when it
// reaches the gate, which is after OpenFile took the reply's attributes, and
// reads once a run covers that count. The attributes come from the mirror,
// and the mirror changes only after its hook (see Ordering above), so every
// edit the reply's attributes show is covered: never new attributes over
// old bytes. Hooks that come later do not hold the READ back.
//
// #96 made a READ wait until no refresh of the path was pending at all. A
// teammate's long save on a far link commits parts early, so the attr push
// reported the clip every 2 s, and a refresh was always pending: READs waited
// the whole bound, answered JUKEBOX, and the Mac backed off for up to 30 s
// (135 s frozen in one stretch).
//
// Pacing. Hooks that come while a run is going start one more run when it
// ends (coalesced), never one per hook. The next run then starts no sooner
// than the last run took (at most refreshMaxGap) after it ended, so refreshes
// of one path keep at most half of the time: each one costs metadata round
// trips and drops the kernel's pages of the file, which the player then
// reads again.
type contentRefresher struct {
	mu    sync.Mutex
	paths map[string]*refreshPath
	// waiting counts paths with a refresh pending or running; READs check it
	// (with failedCount) without the lock.
	waiting atomic.Int64
	// run sends the refresh for one inode and reloads the kernel's
	// attributes of fusePath. Tests replace it.
	run func(inode uint64, fusePath string) error
	// unsupported: juicefs lacks patch 0031; stop sending.
	unsupported atomic.Bool
	// maxGap is refreshMaxGap; tests set it.
	maxGap time.Duration

	// failedCount counts paths whose last refresh failed.
	failedCount atomic.Int64

	refreshed, failed atomic.Int64
}

// refreshPath is the refresh state of one path. Guarded by contentRefresher.mu.
type refreshPath struct {
	inode    uint64
	fusePath string
	want     uint64 // content hooks so far
	started  uint64 // the want count the current (or last) run took
	covered  uint64 // the want count of the last successful run
	// failedAt is the want count of the last run when it failed, 0 after a
	// success; attempts counts failed runs since the last success or hook.
	failedAt uint64
	attempts int
	running  bool // a goroutine owns the runs (pending or running)
	retry    bool // a READ asks for one more run after a failure
	lastEnd  time.Time
	lastRun  time.Duration // how long the last run took
	// ended is closed when a run ends, then replaced.
	ended chan struct{}
}

func (p *refreshPath) failed() bool { return p.failedAt != 0 && p.failedAt > p.covered }

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

// refreshMaxGap caps the pause between two refreshes of one path.
const refreshMaxGap = time.Second

func newContentRefresher(run func(uint64, string) error) *contentRefresher {
	return &contentRefresher{paths: make(map[string]*refreshPath), run: run, maxGap: refreshMaxGap}
}

// begin records a content hook of name and makes sure a run will cover it.
// It never blocks on FUSE: the content hooks call it while the metadata
// Store holds its locks.
func (c *contentRefresher) begin(name string, inode uint64, fusePath string) {
	if c == nil || inode == 0 || c.unsupported.Load() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.paths[name]
	if p == nil {
		p = &refreshPath{ended: make(chan struct{})}
		c.paths[name] = p
	}
	p.want++
	p.inode, p.fusePath = inode, fusePath
	// A new edit: its refresh gets the whole retry budget again.
	p.attempts = 0
	c.ensureRunningLocked(name, p)
}

func (c *contentRefresher) ensureRunningLocked(name string, p *refreshPath) {
	if p.running {
		return
	}
	p.running = true
	c.waiting.Add(1)
	go c.loop(name, p)
}

// loop runs refreshes of name until every hook is covered by a run that
// started after it (or a READ's retry is done).
func (c *contentRefresher) loop(name string, p *refreshPath) {
	c.mu.Lock()
	for (p.started < p.want || p.retry) && !c.unsupported.Load() {
		if gap := min(p.lastRun, c.maxGap); gap > 0 && !p.lastEnd.IsZero() {
			if d := gap - time.Since(p.lastEnd); d > 0 {
				c.mu.Unlock()
				time.Sleep(d)
				c.mu.Lock()
			}
		}
		p.retry = false
		target, inode, fusePath := p.want, p.inode, p.fusePath
		p.started = target
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
		wasFailed := p.failed()
		if err == nil {
			p.covered = max(p.covered, target)
			p.attempts = 0
		} else {
			p.failedAt = target
			p.attempts++
		}
		if nowFailed := p.failed(); nowFailed != wasFailed {
			if nowFailed {
				c.failedCount.Add(1)
			} else {
				c.failedCount.Add(-1)
			}
		}
		p.lastEnd = time.Now()
		p.lastRun = p.lastEnd.Sub(start)
		close(p.ended)
		p.ended = make(chan struct{})
	}
	p.running = false
	c.waiting.Add(-1)
	// Forget a path that is fully covered; a READ holding p still sees the
	// final state through its own pointer.
	if !p.failed() && p.covered >= p.want && c.paths[name] == p {
		delete(c.paths, name)
	}
	c.mu.Unlock()
}

// wait blocks until a refresh covers every content hook of name so far, at
// most bound. nil: the READ may read (covered, no hook ever, or the path's
// retry budget is spent). errRefreshPending: the bound passed.
// errRefreshFailed: the covering refresh did not confirm; the next READ
// starts it again and waits for that one.
func (c *contentRefresher) wait(name string, bound time.Duration) error {
	if c == nil || c.waiting.Load()+c.failedCount.Load() == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.paths[name]
	if p == nil {
		return nil
	}
	target := p.want
	var timer *time.Timer
	// tried: this READ has seen a run end, or has started one itself.
	tried := false
	for {
		if p.covered >= target || c.unsupported.Load() {
			return nil
		}
		if p.failedAt >= target {
			// A run that covers this READ failed, and none has succeeded since.
			switch {
			case p.attempts >= refreshRetryBudget:
				return nil
			case p.running && p.started > p.failedAt:
				// A newer run is already going: wait for it.
			case tried:
				return errRefreshFailed
			default:
				// Send it again and wait for that one (review C2).
				tried = true
				p.retry = true
				c.ensureRunningLocked(name, p)
			}
		}
		if timer == nil {
			timer = time.NewTimer(bound)
			defer timer.Stop()
		}
		ended := p.ended
		c.mu.Unlock()
		select {
		case <-ended:
			c.mu.Lock()
			tried = true
		case <-timer.C:
			c.mu.Lock()
			if p.covered >= target {
				return nil
			}
			return errRefreshPending
		}
	}
}

// contentRefreshBound bounds how long a READ waits for the refresh that
// covers it. That is at most the run going when the READ arrives, a pause no
// longer than that run, and one full run. A run is the shared control mutex
// (another control message may hold it), one local control exchange, two
// metadata round trips inside juicefs, and up to jmRefreshNotifyWait (6 s)
// for the kernel to accept the notify, which waits for the file's reads in
// flight. Past the bound the READ answers JUKEBOX, never old bytes. The bound
// is the same on every link: a JUKEBOX costs the Mac at least 4 s and then
// doubles, while the mount's own timeout (timeo=400) is 40 s.
var contentRefreshBound = func() time.Duration { return 12 * time.Second }

// contentRefreshTimeout bounds one refresh exchange.
const contentRefreshTimeout = 10 * time.Second

// refreshContentNow is the production refresh: the control message, then,
// when the length changed, a stat of the FUSE path. The stat makes the kernel
// reload the attributes the message dropped, so a read through a descriptor
// that is already open sees an appended length (juicefs answers it from the
// refresh's own read, with no round trip, patch 0028).
func (h *JuiceMountHandler) refreshContentNow(inode uint64, fusePath string) error {
	start := time.Now()
	flags, length, _, lockWait, err := sharedJFSControl(h.fusePath).refreshInodeTimed(inode, contentRefreshTimeout)
	exchanged := time.Now()
	if err != nil {
		jmlog.Warn("content refresh: exchange failed", "path", fusePath, "inode", inode,
			"control_wait_ms", lockWait.Milliseconds(), "ms", exchanged.Sub(start).Milliseconds(), "err", err)
		return err
	}
	// The stat makes the kernel reload the attributes the message dropped,
	// so a read through an open descriptor sees an appended length. Only a
	// changed length needs it (bug F1): on Linux the stat of a file whose
	// mtime changed drops all its pages and waits for every read of it in
	// flight (FUSE auto_inval_data), which took 3-20 s on the far lab link
	// during a teammate's long save.
	var statted bool
	if prev, seen := h.refreshLengths.Swap(inode, length); !seen || prev.(uint64) != length {
		_, _ = os.Stat(fusePath)
		statted = true
	}
	// One line per teammate edit of a file this process holds, as the attr
	// push logs "refreshed": the laptop check reads it beside juicefs's
	// access log (a new read of the inode after it). The phases (bug F1):
	// control_wait_ms waits for another control message, exchange_ms is
	// juicefs's work (its metadata round trip, the read buffers, the kernel
	// notify; juicefs's own log splits it when slow), stat_ms reloads the
	// kernel's attributes.
	jmlog.Info("content refresh: juicefs dropped the open file's caches", "path", fusePath, "inode", inode,
		"length", length, "kernel_notify_accepted", flags&jfsRefreshKernelAccepted != 0, "ms", time.Since(start).Milliseconds(),
		"control_wait_ms", lockWait.Milliseconds(), "exchange_ms", (exchanged.Sub(start) - lockWait).Milliseconds(),
		"stat_ms", time.Since(exchanged).Milliseconds(), "stat", statted)
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
