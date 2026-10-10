package nfs

// project_lease.go is the Mac side of the H-8B per-project writer lease.
//
// WHAT IT IS. The NAS runs a Redis proxy (jm-respproxy) in front of the
// JuiceFS metadata Redis. The proxy lets one principal (one Mac) write a
// project at a time. A project is the folder at depth 2 of the volume, for
// example "Film Projects/Scott Adams". The proxy answers four JM.LEASE
// commands on the same connection juicefs uses:
//
//	JM.LEASE ACQUIRE <proj> auto|checkout [ttl_s]
//	  -> [status GRANTED|RENEWED|HELD, :epoch, holder "name@host", :expires_ms, :foreign_since]
//	JM.LEASE HEARTBEAT [<proj-with-pending-spool> ...]
//	  -> for each lease: [:proj_ino, :epoch, held|lost, mode]
//	JM.LEASE RELEASE <proj> <epoch> -> [RELEASED|NOT_HOLDER|STALE_EPOCH, :epoch]
//	JM.LEASE STATUS [<proj>|*]      -> flat field/value record(s)
//
// <proj> is "path:<p>" or "ino:<n>". This client always names projects by
// path, because the spool knows paths, not backend inodes.
//
// WHY THE MAC NEEDS A CLIENT. Writes on this Mac land in the local spool and
// reach Redis only when the drainer copies them. Without a client the first
// time the proxy hears about an edit is the drain, minutes or hours later.
// The client:
//   - heartbeats every 20 s, listing the projects that still have pending
//     spool rows, so the proxy never idles out a lease with unsent changes;
//   - acquires (asynchronously) when a write first opens in a project, so a
//     second Mac sees "being edited" while the edit happens, not after;
//   - acquires synchronously before a drain, so a large ingest into a project
//     another Mac holds is parked locally instead of being uploaded and
//     refused at the last slice;
//   - tracks the epoch and foreign_since of every grant, which the drainer
//     uses to turn a stale edit into a conflict copy (drain_refusal.go);
//   - releases on an explicit "Done editing <project>" from the menu bar;
//   - retries parked rows ("waiting_project") once the project frees.
//
// Lease LIFETIME is the proxy's job (founder decision 2026-10-06): an auto
// lease is held while the Mac heartbeats, and is released after 4 h without
// writes and no pending spool, after 10 min without a heartbeat, or by an
// explicit release. A checkout lasts up to 72 h.
//
// Against a plain Redis (no proxy) every JM.LEASE fails "unknown command".
// The client then reports leases as unsupported and the drainer behaves
// exactly as before H-8B.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/redis/go-redis/v9"
)

// ProjectLeaseHeartbeatInterval is the heartbeat period (H8 plan §1.3).
const ProjectLeaseHeartbeatInterval = 20 * time.Second

// projectLeaseFresh is how long a confirmed lease is trusted without a new
// round trip. It is longer than one heartbeat so a drain storm in a held
// project costs no extra RTT, and far shorter than the proxy's 10 min orphan
// window.
const projectLeaseFresh = 45 * time.Second

// projectLeaseCheckoutMax is the longest checkout the proxy grants (72 h).
const projectLeaseCheckoutMax = 72 * time.Hour

// projectLeaseUnsupportedBackoff is how long the client stays quiet after the
// server answered "unknown command" (no proxy in front of Redis).
const projectLeaseUnsupportedBackoff = 5 * time.Minute

// Lease statuses from JM.LEASE ACQUIRE.
const (
	leaseGranted = "GRANTED"
	leaseRenewed = "RENEWED"
	leaseHeld    = "HELD"
)

// errLeasesUnsupported means the server has no JM.LEASE command.
var errLeasesUnsupported = errors.New("project leases not supported by the metadata server")

// projectOf returns the project path ("A/B") for an in-mount path, or "" when
// the path is not inside a project: files at depth 1-2 (namespace root), the
// derivative tree and the trash are not project-scoped.
func projectOf(nfsPath string) string {
	parts := strings.FieldsFunc(nfsPath, func(r rune) bool { return r == '/' })
	if len(parts) < 3 {
		return ""
	}
	switch parts[0] {
	case ".juicemount", ".trash", ".Trash", ".Trashes":
		return ""
	}
	return parts[0] + "/" + parts[1]
}

// projectDisplayName is the folder name shown in the menu bar.
func projectDisplayName(project string) string {
	if i := strings.LastIndexByte(project, '/'); i >= 0 {
		return project[i+1:]
	}
	return project
}

// holderHost returns the host part of "name@host".
func holderHost(holder string) string {
	if i := strings.LastIndexByte(holder, '@'); i >= 0 && i+1 < len(holder) {
		return holder[i+1:]
	}
	return holder
}

// leaseDoer is the part of a Redis client the lease client uses.
type leaseDoer interface {
	Do(ctx context.Context, args ...any) *redis.Cmd
}

// ProjectLeaseConfig configures NewProjectLeaseClient.
type ProjectLeaseConfig struct {
	// RedisURL is the metadata URL the mount uses (the proxy endpoint). Only
	// host:port is ever logged.
	RedisURL string
	// Password overrides the URL password (the Keychain secret the juicefs
	// child receives as META_PASSWORD). Never logged.
	Password string
	// Spool is the local spool. Required for heartbeats and waiting rows.
	Spool *SpoolStore
	// Interval overrides the heartbeat period (tests).
	Interval time.Duration
	// Timeout bounds one JM.LEASE round trip. 0 -> 5 s.
	Timeout time.Duration
	// Now is a test clock. nil -> time.Now.
	Now func() time.Time
}

// leaseDecision is what a drain needs to know about its project.
type leaseDecision struct {
	project string
	// held: this Mac holds the lease (GRANTED or RENEWED).
	held bool
	// heldBy is "name@host" when another principal holds it.
	heldBy string
	epoch  uint64
	// foreignBefore > 0 means another principal held the project after an
	// earlier epoch of ours: rows accepted under an epoch < foreignBefore may
	// be stale against a foreign edit.
	foreignBefore uint64
}

// taints reports whether a row accepted under acceptedEpoch predates a
// foreign holder.
func (d leaseDecision) taints(acceptedEpoch uint64) bool {
	return d.foreignBefore > 0 && acceptedEpoch < d.foreignBefore
}

type projectLeaseState struct {
	held          bool
	epoch         uint64
	mode          string
	inode         int64
	confirmedAt   time.Time
	foreignBefore uint64
	heldBy        string // other principal, when we do not hold it
	refusedAt     time.Time
	releaseWanted bool
	lastStatus    string
}

// ProjectLeaseClient is the Mac's lease client. All methods are safe for
// concurrent use; a nil *ProjectLeaseClient is a valid "no leases" client.
type ProjectLeaseClient struct {
	rdb       leaseDoer
	closer    func() error
	spool     *SpoolStore
	interval  time.Duration
	timeout   time.Duration
	now       func() time.Time
	addr      string // host:port only, for logs
	principal string // authenticated Redis username, never inferred from a receipt
	database  int    // selected Redis database

	mu          sync.Mutex
	projects    map[string]*projectLeaseState
	unsupported time.Time // until when JM.LEASE is assumed absent
	lastHBErr   string
	lastHBAt    time.Time

	touchCh  chan string
	stop     chan struct{}
	done     chan struct{}
	startMu  sync.Mutex
	started  bool
	stopOnce sync.Once
}

// NewProjectLeaseClient builds a client on its own Redis connection pool to
// the mount's metadata endpoint. It does not dial until first use.
func NewProjectLeaseClient(cfg ProjectLeaseConfig) (*ProjectLeaseClient, error) {
	opts, err := redisurl.Parse(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("project lease client: %w", err)
	}
	if cfg.Password != "" {
		opts.Password = cfg.Password
	}
	// JM.LEASE is RESP2-shaped; identity commands are noise to the proxy.
	opts.Protocol = 2
	opts.DisableIdentity = true
	opts.PoolSize = 2
	opts.MaxRetries = 1
	rdb := redis.NewClient(opts)
	c := newProjectLeaseClient(rdb, cfg)
	c.closer = rdb.Close
	c.addr = opts.Addr
	c.principal, c.database = opts.Username, opts.DB
	if c.principal == "" {
		c.principal = "default"
	}
	return c, nil
}

func newProjectLeaseClient(rdb leaseDoer, cfg ProjectLeaseConfig) *ProjectLeaseClient {
	c := &ProjectLeaseClient{
		rdb:      rdb,
		spool:    cfg.Spool,
		interval: cfg.Interval,
		timeout:  cfg.Timeout,
		now:      cfg.Now,
		projects: map[string]*projectLeaseState{},
		touchCh:  make(chan string, 64),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	if c.interval <= 0 {
		c.interval = ProjectLeaseHeartbeatInterval
	}
	if c.timeout <= 0 {
		c.timeout = 5 * time.Second
	}
	if c.now == nil {
		c.now = time.Now
	}
	return c
}

// Start launches the heartbeat loop. Safe to call once; later calls no-op.
func (c *ProjectLeaseClient) Start() {
	if c == nil {
		return
	}
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.started {
		return
	}
	c.started = true
	go c.loop()
}

// Stop ends the heartbeat loop and closes the connection pool. Leases are
// NOT released: an orderly quit with pending spool must keep the project, and
// the proxy frees an idle lease by itself.
func (c *ProjectLeaseClient) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		close(c.stop)
		c.startMu.Lock()
		started := c.started
		c.startMu.Unlock()
		if started {
			<-c.done
		}
		if c.closer != nil {
			_ = c.closer()
		}
	})
}

func (c *ProjectLeaseClient) loop() {
	defer close(c.done)
	t := time.NewTicker(c.interval)
	defer t.Stop()
	c.Tick(context.Background())
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			c.Tick(context.Background())
		case p := <-c.touchCh:
			c.acquireAsync(p)
		}
	}
}

func (c *ProjectLeaseClient) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *ProjectLeaseClient) stateLocked(project string) *projectLeaseState {
	st := c.projects[project]
	if st == nil {
		st = &projectLeaseState{}
		c.projects[project] = st
	}
	return st
}

func (c *ProjectLeaseClient) isUnsupported() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.unsupported.IsZero() && c.now().Before(c.unsupported)
}

// classifyErr maps a JM.LEASE error. "unknown command" means no proxy.
func (c *ProjectLeaseClient) classifyErr(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "unknown command") || strings.Contains(msg, "unknown subcommand") {
		c.mu.Lock()
		if c.unsupported.IsZero() || !c.now().Before(c.unsupported) {
			jmlog.Info("project leases: metadata server has no JM.LEASE; drains run without leases",
				"endpoint", c.addr)
		}
		c.unsupported = c.now().Add(projectLeaseUnsupportedBackoff)
		c.mu.Unlock()
		return errLeasesUnsupported
	}
	return err
}

// acquire sends JM.LEASE ACQUIRE and records the result.
func (c *ProjectLeaseClient) acquire(parent context.Context, project, mode string, ttl time.Duration) (leaseDecision, error) {
	dec := leaseDecision{project: project}
	if c.isUnsupported() {
		return dec, errLeasesUnsupported
	}
	args := []any{"JM.LEASE", "ACQUIRE", "path:" + project, mode}
	if mode == "checkout" {
		if ttl <= 0 || ttl > projectLeaseCheckoutMax {
			ttl = projectLeaseCheckoutMax
		}
		args = append(args, strconv.FormatInt(int64(ttl/time.Second), 10))
	}
	ctx, cancel := c.ctx(parent)
	defer cancel()
	v, err := c.rdb.Do(ctx, args...).Result()
	if err != nil {
		return dec, c.classifyErr(err)
	}
	arr, ok := v.([]any)
	if !ok || len(arr) != 5 {
		return dec, fmt.Errorf("JM.LEASE ACQUIRE: unexpected reply shape %T/%d", v, lenAny(v))
	}
	status, _ := arr[0].(string)
	epoch, _ := respUint(arr[1])
	holder, _ := arr[2].(string)
	foreign, _ := respUint(arr[4])
	dec.epoch = epoch
	now := c.now()
	c.mu.Lock()
	st := c.stateLocked(project)
	st.lastStatus = status
	switch status {
	case leaseGranted, leaseRenewed:
		dec.held = true
		st.held = true
		st.heldBy = ""
		st.epoch = epoch
		st.mode = mode
		st.confirmedAt = now
		if foreign == 1 && status == leaseGranted {
			// Another principal held the project after our last epoch. Every
			// row accepted under an earlier epoch may be stale against its
			// edits; remember the boundary for the whole lifetime of this
			// grant (a later RENEWED reports foreign_since=0).
			st.foreignBefore = epoch
		}
		dec.foreignBefore = st.foreignBefore
	case leaseHeld:
		st.held = false
		st.heldBy = holder
		st.epoch = epoch
		st.confirmedAt = time.Time{}
		dec.heldBy = holder
	default:
		c.mu.Unlock()
		return dec, fmt.Errorf("JM.LEASE ACQUIRE: unknown status %q", status)
	}
	c.mu.Unlock()
	return dec, nil
}

// EnsureForDrain returns the lease state a drain in project must act on,
// acquiring (or renewing) synchronously unless a recent confirmation is
// cached. An error means "unknown": the caller proceeds and relies on the
// proxy's refusal (EROFS) to stop a foreign write.
func (c *ProjectLeaseClient) EnsureForDrain(project string) (leaseDecision, error) {
	if c == nil || project == "" {
		return leaseDecision{project: project}, errLeasesUnsupported
	}
	c.mu.Lock()
	if st := c.projects[project]; st != nil && st.held && c.now().Sub(st.confirmedAt) < projectLeaseFresh {
		dec := leaseDecision{project: project, held: true, epoch: st.epoch, foreignBefore: st.foreignBefore}
		c.mu.Unlock()
		return dec, nil
	}
	c.mu.Unlock()
	return c.acquire(context.Background(), project, "auto", 0)
}

// Checkout takes a checkout lease (survives offline up to 72 h).
func (c *ProjectLeaseClient) Checkout(project string, ttl time.Duration) (leaseDecision, error) {
	if c == nil || project == "" {
		return leaseDecision{}, errLeasesUnsupported
	}
	return c.acquire(context.Background(), project, "checkout", ttl)
}

// EpochFor returns the epoch this Mac currently holds for the project of
// nfsPath, or 0. It never blocks on the network: the spool records it as the
// row's accepted_epoch at write-open time.
func (c *ProjectLeaseClient) EpochFor(nfsPath string) uint64 {
	if c == nil {
		return 0
	}
	p := projectOf(nfsPath)
	if p == "" {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if st := c.projects[p]; st != nil && st.held {
		return st.epoch
	}
	return 0
}

// Touch tells the client a write opened under nfsPath. If the project is not
// already held it is acquired in the background. Never blocks.
func (c *ProjectLeaseClient) Touch(nfsPath string) {
	if c == nil {
		return
	}
	p := projectOf(nfsPath)
	if p == "" {
		return
	}
	c.mu.Lock()
	st := c.projects[p]
	fresh := st != nil && st.held && c.now().Sub(st.confirmedAt) < projectLeaseFresh
	c.mu.Unlock()
	if fresh {
		return
	}
	select {
	case c.touchCh <- p:
	default: // the next heartbeat or drain acquires it
	}
}

func (c *ProjectLeaseClient) acquireAsync(project string) {
	c.mu.Lock()
	st := c.projects[project]
	fresh := st != nil && st.held && c.now().Sub(st.confirmedAt) < projectLeaseFresh
	c.mu.Unlock()
	if fresh {
		return
	}
	if _, err := c.acquire(context.Background(), project, "auto", 0); err != nil && !errors.Is(err, errLeasesUnsupported) {
		jmlog.Debug("project lease: background acquire failed", "project", project, "error", err.Error())
	}
}

// NoteRefused records that the proxy refused a drain write in project. The
// next tick asks who holds it.
func (c *ProjectLeaseClient) NoteRefused(project, holder string) {
	if c == nil || project == "" {
		return
	}
	c.mu.Lock()
	st := c.stateLocked(project)
	st.held = false
	st.confirmedAt = time.Time{}
	st.refusedAt = c.now()
	if holder != "" {
		st.heldBy = holder
	}
	c.mu.Unlock()
}

// Release asks the proxy to free project ("Done editing"). If this Mac still
// has unsent changes in the project the release is deferred until they are
// uploaded: releasing first would let another Mac take the project and turn
// every pending save into a conflict copy. Returns "released", "deferred",
// "not_holder" or "stale_epoch".
func (c *ProjectLeaseClient) Release(project string) (string, error) {
	if c == nil || project == "" {
		return "", errLeasesUnsupported
	}
	pending, err := c.pendingProjects()
	if err != nil {
		return "", err
	}
	if pending[project] > 0 {
		c.mu.Lock()
		c.stateLocked(project).releaseWanted = true
		c.mu.Unlock()
		return "deferred", nil
	}
	return c.releaseNow(context.Background(), project)
}

func (c *ProjectLeaseClient) releaseNow(parent context.Context, project string) (string, error) {
	c.mu.Lock()
	st := c.stateLocked(project)
	epoch := st.epoch
	c.mu.Unlock()
	ctx, cancel := c.ctx(parent)
	defer cancel()
	v, err := c.rdb.Do(ctx, "JM.LEASE", "RELEASE", "path:"+project, strconv.FormatUint(epoch, 10)).Result()
	if err != nil {
		return "", c.classifyErr(err)
	}
	arr, ok := v.([]any)
	if !ok || len(arr) != 2 {
		return "", fmt.Errorf("JM.LEASE RELEASE: unexpected reply shape %T/%d", v, lenAny(v))
	}
	status, _ := arr[0].(string)
	c.mu.Lock()
	st = c.stateLocked(project)
	st.releaseWanted = false
	st.held = false
	st.confirmedAt = time.Time{}
	st.lastStatus = status
	c.mu.Unlock()
	return strings.ToLower(status), nil
}

// pendingProjects counts active spool rows per project.
func (c *ProjectLeaseClient) pendingProjects() (map[string]int, error) {
	out := map[string]int{}
	if c.spool == nil {
		return out, nil
	}
	paths, err := c.spool.Meta().ActivePaths()
	if err != nil {
		return nil, err
	}
	for _, p := range paths {
		if proj := projectOf(p); proj != "" {
			out[proj]++
		}
	}
	return out, nil
}

// Tick runs one heartbeat, retries waiting rows whose project is free, and
// performs deferred releases. The loop calls it every interval; tests call it
// directly.
func (c *ProjectLeaseClient) Tick(parent context.Context) {
	if c == nil || c.isUnsupported() {
		return
	}
	pending, err := c.pendingProjects()
	if err != nil {
		jmlog.Debug("project lease: pending scan failed", "error", err.Error())
		pending = map[string]int{}
	}
	c.heartbeat(parent, pending)
	c.retryWaiting(parent)
	c.mu.Lock()
	var release []string
	for p, st := range c.projects {
		if st.releaseWanted && st.held && pending[p] == 0 {
			release = append(release, p)
		}
	}
	c.mu.Unlock()
	for _, p := range release {
		if status, err := c.releaseNow(parent, p); err != nil {
			jmlog.Debug("project lease: deferred release failed", "project", p, "error", err.Error())
		} else {
			jmlog.Info("project lease: released after uploads finished", "project", p, "status", status)
		}
	}
}

func (c *ProjectLeaseClient) heartbeat(parent context.Context, pending map[string]int) {
	args := []any{"JM.LEASE", "HEARTBEAT"}
	names := make([]string, 0, len(pending))
	for p := range pending {
		names = append(names, p)
	}
	sort.Strings(names)
	for _, p := range names {
		args = append(args, "path:"+p)
	}
	ctx, cancel := c.ctx(parent)
	defer cancel()
	v, err := c.rdb.Do(ctx, args...).Result()
	now := c.now()
	if err != nil {
		err = c.classifyErr(err)
		c.mu.Lock()
		c.lastHBErr = err.Error()
		c.mu.Unlock()
		return
	}
	arr, _ := v.([]any)
	heldIno := map[int64]uint64{}
	lostIno := map[int64]bool{}
	for _, rec := range arr {
		r, ok := rec.([]any)
		if !ok || len(r) != 4 {
			continue
		}
		ino, _ := respInt(r[0])
		ep, _ := respUint(r[1])
		state, _ := r[2].(string)
		if strings.EqualFold(state, "held") {
			heldIno[ino] = ep
		} else {
			lostIno[ino] = true
		}
	}
	// Learn project inodes we do not know yet (one STATUS per new project).
	c.mu.Lock()
	var unknown []string
	for p, st := range c.projects {
		if st.held && st.inode == 0 {
			unknown = append(unknown, p)
		}
	}
	c.mu.Unlock()
	for _, p := range unknown {
		if ino, err := c.statusInode(parent, p); err == nil {
			c.mu.Lock()
			c.stateLocked(p).inode = ino
			c.mu.Unlock()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastHBErr = ""
	c.lastHBAt = now
	for _, st := range c.projects {
		if st.inode == 0 {
			continue
		}
		if ep, ok := heldIno[st.inode]; ok {
			st.held = true
			st.epoch = ep
			st.confirmedAt = now
		} else if st.held {
			// Not in the held list: the proxy no longer counts it as ours
			// (expired, released or broken). The next drain re-acquires and
			// learns foreign_since.
			st.held = false
			st.confirmedAt = time.Time{}
		}
	}
}

// statusInode asks the proxy for the project's root inode.
func (c *ProjectLeaseClient) statusInode(parent context.Context, project string) (int64, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()
	v, err := c.rdb.Do(ctx, "JM.LEASE", "STATUS", "path:"+project).Result()
	if err != nil {
		return 0, c.classifyErr(err)
	}
	arr, ok := v.([]any)
	if !ok {
		return 0, fmt.Errorf("JM.LEASE STATUS: unexpected reply %T", v)
	}
	for i := 0; i+1 < len(arr); i += 2 {
		if k, _ := arr[i].(string); k == "proj" {
			if n, ok := respInt(arr[i+1]); ok && n > 0 {
				return n, nil
			}
			return 0, fmt.Errorf("JM.LEASE STATUS: bad proj field")
		}
	}
	return 0, fmt.Errorf("JM.LEASE STATUS: no proj field")
}

// retryWaiting re-acquires every project that has rows parked in
// waiting_project. A grant releases the rows and wakes the drainer.
func (c *ProjectLeaseClient) retryWaiting(parent context.Context) {
	if c.spool == nil {
		return
	}
	waiting, err := c.spool.Meta().WaitingProjects()
	if err != nil || len(waiting) == 0 {
		return
	}
	for p := range waiting {
		dec, err := c.acquire(parent, p, "auto", 0)
		if err != nil {
			if errors.Is(err, errLeasesUnsupported) {
				// No proxy any more (rollback): nothing will ever grant, so
				// let the rows drain the pre-H-8B way.
				_, _ = c.spool.Meta().ReleaseProjectHold(p)
				c.spool.signalReady()
			}
			continue
		}
		if dec.held {
			if n, err := c.spool.Meta().ReleaseProjectHold(p); err == nil && n > 0 {
				jmlog.Info("project lease: project is free; resuming held uploads",
					"project", p, "rows", n)
				c.spool.signalReady()
			}
		}
	}
}

// ProjectLeaseView is one project in the menu-bar JSON.
type ProjectLeaseView struct {
	Project          string `json:"project"`
	Name             string `json:"name"`
	State            string `json:"state"` // held | waiting | other | idle
	Holder           string `json:"holder,omitempty"`
	HolderHost       string `json:"holder_host,omitempty"`
	Epoch            uint64 `json:"epoch"`
	PendingRows      int    `json:"pending_rows"`
	WaitingRows      int    `json:"waiting_rows"`
	ReleaseRequested bool   `json:"release_requested"`
}

// ProjectLeaseStatus is the /project-leases JSON body.
type ProjectLeaseStatus struct {
	Enabled   bool               `json:"enabled"`
	Supported bool               `json:"supported"`
	Projects  []ProjectLeaseView `json:"projects"`
	LastError string             `json:"last_error,omitempty"`
}

// Snapshot returns the state the menu bar shows.
func (c *ProjectLeaseClient) Snapshot() ProjectLeaseStatus {
	out := ProjectLeaseStatus{Projects: []ProjectLeaseView{}}
	if c == nil {
		return out
	}
	out.Enabled = true
	out.Supported = !c.isUnsupported()
	pending, _ := c.pendingProjects()
	var waiting map[string]int
	if c.spool != nil {
		waiting, _ = c.spool.Meta().WaitingProjects()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out.LastError = c.lastHBErr
	seen := map[string]bool{}
	add := func(p string) {
		if seen[p] {
			return
		}
		seen[p] = true
		v := ProjectLeaseView{Project: p, Name: projectDisplayName(p), State: "idle",
			PendingRows: pending[p], WaitingRows: waiting[p]}
		if st := c.projects[p]; st != nil {
			v.Epoch = st.epoch
			v.ReleaseRequested = st.releaseWanted
			switch {
			case st.held:
				v.State = "held"
			case st.heldBy != "":
				v.State = "other"
				v.Holder = st.heldBy
				v.HolderHost = holderHost(st.heldBy)
			}
		}
		if v.WaitingRows > 0 {
			v.State = "waiting"
		}
		if v.State == "idle" && v.PendingRows == 0 {
			return
		}
		out.Projects = append(out.Projects, v)
	}
	for p := range c.projects {
		add(p)
	}
	for p := range waiting {
		add(p)
	}
	sort.Slice(out.Projects, func(i, j int) bool { return out.Projects[i].Project < out.Projects[j].Project })
	return out
}

// HTTPHandler serves the menu bar:
//
//	GET  /project-leases                         -> ProjectLeaseStatus JSON
//	POST /project-leases/release?project=<A/B>   -> {"status": "..."} (needs X-JM-Control: 1)
func (c *ProjectLeaseClient) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/project-leases", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(c.Snapshot())
	})
	mux.HandleFunc("/project-leases/release", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("X-JM-Control") != "1" {
			http.Error(w, "POST with X-JM-Control only", http.StatusForbidden)
			return
		}
		project := strings.Trim(r.URL.Query().Get("project"), "/")
		if projectOf(project+"/x") != project {
			http.Error(w, "project must be the depth-2 folder", http.StatusBadRequest)
			return
		}
		status, err := c.Release(project)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
	})
	return mux
}

func respInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	}
	return 0, false
}

func respUint(v any) (uint64, bool) {
	n, ok := respInt(v)
	if !ok || n < 0 {
		return 0, false
	}
	return uint64(n), true
}

func lenAny(v any) int {
	if a, ok := v.([]any); ok {
		return len(a)
	}
	return -1
}
