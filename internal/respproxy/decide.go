package respproxy

// decide.go is lane L2's Policy: the lease engine behind policy.go's frozen
// Decide (H8 plan §1.4 steps 3-7).
//
// Per unit:
//  1. analyse it (txnunit.go): anchors, edges, directory moves;
//  2. take the proxy-wide topology lock (write side for a unit that moves a
//     directory between parents, read side otherwise);
//  3. resolve every anchor to a namespace (ancestry.go);
//  4. apply the namespace rules (ns:root, ns:trash, ns:derivatives);
//  5. for project anchors, take each project's fence (read side, sorted
//     inode order) and check the holder; a free project is granted to the
//     writer (implicit grant), all-or-none across projects;
//  6. Done (from the reply goroutine) learns the unit's edges if it
//     committed, bumps the cache generation after a directory move, and
//     releases the locks. No unit decided under one holder can commit after
//     a grant, release, expiry or break, which all take the fence's write
//     side.
//
// Principals of kind admin and bg are not lease-checked (bg runs
// compaction, trash and gc on every project); they still take the locks so
// they are ordered against transfers, and their moves still bump the
// generation.

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LeaseConfig configures a LeaseEngine.
type LeaseConfig struct {
	Side        sideDoer // Proxy.Side()
	VolumeDB    int      // the JuiceFS volume DB (holds jmlease:*)
	Now         func() time.Time
	Logf        func(format string, args ...any)
	SweepEvery  time.Duration // expiry sweep and jmlease:v1:cfg re-read; default 5 s
	FlushEvery  time.Duration // persist touch/heartbeat fields; default 3 s
	ScanWorkers int           // background d* scan concurrency; default 4
	// FenceHoldMax bounds how long one unit may hold its project fences and
	// the topology lock waiting for its reply (default 15 s). See Decide.
	FenceHoldMax time.Duration
	NoScan       bool // skip the background scan (tests of the miss path)
	FileCache    int  // file -> parent cache entries; default 1M
}

// LeaseEngine implements Policy and LocalHandler.
type LeaseEngine struct {
	cfg LeaseConfig
	now func() time.Time
	anc *ancestry

	topo sync.RWMutex

	mu       sync.Mutex
	leases   map[uint64]*lease
	fences   map[uint64]*sync.RWMutex
	denyLast map[string]int64

	cfgMu      sync.RWMutex
	lcfg       leaseCfg
	depthFixed int

	logCh      chan logEntry
	logDropped atomic.Int64

	boot bootstrapState

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	started   bool

	// Counters (decisions that reached the engine).
	nAllow, nDeny, nGrant, nFenceTimeout, nLostKill atomic.Int64

	// Test hooks (same package only).
	hooks atomic.Pointer[engineHooks]
}

type engineHooks struct {
	admitted func(p Principal, u *Unit)
	done     func(p Principal, u *Unit, o Outcome, epochs map[uint64]uint64)
}

var (
	_ Policy       = (*LeaseEngine)(nil)
	_ LocalHandler = (*LeaseEngine)(nil)
)

// NewLeaseEngine builds the engine. Call Start before the proxy serves.
func NewLeaseEngine(cfg LeaseConfig) *LeaseEngine {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	if cfg.SweepEvery <= 0 {
		cfg.SweepEvery = 5 * time.Second
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = 3 * time.Second
	}
	if cfg.FenceHoldMax <= 0 {
		cfg.FenceHoldMax = 15 * time.Second
	}
	if cfg.ScanWorkers <= 0 {
		cfg.ScanWorkers = 4
	}
	if cfg.FileCache <= 0 {
		cfg.FileCache = 1 << 20
	}
	e := &LeaseEngine{
		cfg:      cfg,
		now:      cfg.Now,
		leases:   map[uint64]*lease{},
		fences:   map[uint64]*sync.RWMutex{},
		denyLast: map[string]int64{},
		lcfg:     defaultLeaseCfg(),
		logCh:    make(chan logEntry, 4096),
		stop:     make(chan struct{}),
	}
	e.anc = newAncestry(cfg.Side, cfg.VolumeDB, 2, cfg.FileCache)
	e.boot.doneC = make(chan struct{})
	return e
}

func (e *LeaseEngine) logf(format string, args ...any) { e.cfg.Logf(format, args...) }

// Start loads jmlease:v1:cfg and the lease table, reads the project roots
// synchronously, and starts the background scan, sweeper and log writer.
func (e *LeaseEngine) Start() error {
	if e.cfg.Side == nil {
		return fmt.Errorf("respproxy: lease engine has no side pool")
	}
	start := e.now()
	e.loadCfg()
	c := e.cfgNow()
	e.depthFixed = c.depth
	e.cfgMu.Lock()
	e.lcfg.depth = c.depth
	e.cfgMu.Unlock()
	e.anc = newAncestry(e.cfg.Side, e.cfg.VolumeDB, c.depth, e.cfg.FileCache)
	if err := e.loadLeases(ms(start)); err != nil {
		return fmt.Errorf("respproxy: load leases: %w", err)
	}
	t0 := time.Now()
	n, err := e.anc.bootstrapRoots()
	if err != nil {
		return fmt.Errorf("respproxy: bootstrap project roots: %w", err)
	}
	e.boot.mu.Lock()
	e.boot.st.SyncDur, e.boot.st.SyncDirs = time.Since(t0), n
	e.boot.mu.Unlock()
	e.mu.Lock()
	for _, l := range e.leases {
		if lbl := e.anc.label(l.proj); lbl != "" {
			l.label = lbl
		}
	}
	nl := len(e.leases)
	e.mu.Unlock()
	e.logf("respproxy: lease engine: %d lease records, %d project-depth dirs in %s, depth=%d idle=%s orphan=%s checkout_max=%s",
		nl, n, time.Since(t0).Round(time.Millisecond), c.depth, c.idle, c.orphan, c.checkoutMax)

	e.started = true
	e.wg.Add(2)
	go e.logWriter()
	go e.loop()
	if e.cfg.NoScan {
		e.boot.mu.Lock()
		e.boot.st.ScanErr = "disabled"
		e.boot.mu.Unlock()
		e.boot.done.Store(true)
		close(e.boot.doneC)
	} else {
		e.wg.Add(1)
		go e.scan()
	}
	return nil
}

func (e *LeaseEngine) scan() {
	defer e.wg.Done()
	t0 := time.Now()
	keys, added, err := e.anc.scanAll(e.cfg.ScanWorkers, e.stop)
	e.boot.mu.Lock()
	e.boot.st.ScanDur, e.boot.st.ScanKeys, e.boot.st.ScanEdges = time.Since(t0), keys, added
	if err != nil {
		e.boot.st.ScanErr = err.Error()
	}
	e.boot.st.ScanDone = err == nil
	e.boot.mu.Unlock()
	e.boot.done.Store(true)
	close(e.boot.doneC)
	e.logf("respproxy: lease engine: background scan read %d directories, added %d edges in %s (err=%v)",
		keys, added, time.Since(t0).Round(time.Millisecond), err)
}

// Bootstrap returns the bootstrap measurements.
func (e *LeaseEngine) Bootstrap() BootstrapStats {
	e.boot.mu.Lock()
	defer e.boot.mu.Unlock()
	return e.boot.st
}

// WaitScan blocks until the background scan has finished or d elapsed.
func (e *LeaseEngine) WaitScan(d time.Duration) bool {
	select {
	case <-e.boot.doneC:
		return true
	case <-time.After(d):
		return false
	}
}

func (e *LeaseEngine) loop() {
	defer e.wg.Done()
	sw := time.NewTicker(e.cfg.SweepEvery)
	fl := time.NewTicker(e.cfg.FlushEvery)
	defer sw.Stop()
	defer fl.Stop()
	for {
		select {
		case <-e.stop:
			e.flushDirty()
			return
		case <-sw.C:
			e.loadCfg()
			e.sweep()
		case <-fl.C:
			e.flushDirty()
		}
	}
}

// Close stops the background work and persists pending touches.
func (e *LeaseEngine) Close() {
	e.closeOnce.Do(func() {
		close(e.stop)
		if e.started {
			e.wg.Wait()
		}
	})
}

// ---------------------------------------------------------------------------
// Decide.

func (e *LeaseEngine) Decide(p Principal, u *Unit) Verdict {
	f := analyzeUnit(u)
	bypass := p.Kind == KindAdmin || p.Kind == KindBG || u.Mode == ModePassthrough
	hooks := e.hooks.Load()
	// upName is the unit's own upstream connection (L1 names each one
	// p:<principal>:<connID>); a Lost Outcome carries the same ConnID.
	upName := func(connID uint64) string {
		if connID == 0 || connID == u.ConnID {
			if u.UpstreamName != "" {
				return u.UpstreamName
			}
			connID = u.ConnID
		}
		return UpstreamName(p.Name, connID)
	}

	topoW := f.topology
	if topoW {
		e.topo.Lock()
	} else {
		e.topo.RLock()
	}
	var fences []*sync.RWMutex
	epochs := map[uint64]uint64{}
	// The locks are released exactly once: by Done when the reply arrives,
	// or by the fence-hold timeout (fenceTimeout) for a unit whose reply is
	// stuck behind a client that stopped reading.
	var relMu sync.Mutex
	released, timedOut := false, false
	var timer *time.Timer
	releaseLocked := func() {
		if released {
			return
		}
		released = true
		for i := len(fences) - 1; i >= 0; i-- {
			fences[i].RUnlock()
		}
		fences = nil
		if topoW {
			e.topo.Unlock()
		} else {
			e.topo.RUnlock()
		}
	}
	var onTimeout func()
	done := func(o Outcome) {
		relMu.Lock()
		defer relMu.Unlock()
		if timer != nil {
			timer.Stop()
		}
		if released && !timedOut {
			return // Done called twice
		}
		if o.Lost && !released {
			// The connection died with this unit in flight (including L1's
			// pending-reply cap disconnect). Bytes it already sent may
			// still reach Redis and commit, so make the outcome final the
			// same way the hold timeout does before releasing; on failure
			// keep the locks and let the timeout path retry.
			if err := e.killUpstream(upName(o.ConnID)); err != nil {
				e.logf("respproxy: unit of %s lost in flight; upstream kill failed (%v); keeping its locks", p.Name, err)
				timer = time.AfterFunc(e.cfg.FenceHoldMax, onTimeout)
				return
			}
			e.nLostKill.Add(1)
		}
		switch {
		case timedOut:
		case o.Lost:
			e.anc.forget(f) // outcome unknown: re-resolve from Redis
		case o.Committed:
			e.anc.learnCommitted(f)
		}
		if hooks != nil && hooks.done != nil && !released {
			hooks.done(p, u, o, epochs)
		}
		timedOut = false
		releaseLocked()
	}
	onTimeout = func() {
		relMu.Lock()
		defer relMu.Unlock()
		if released {
			return
		}
		// Make the outcome final before letting a transfer through: kill
		// this unit's own upstream connection. Once Redis has freed
		// a client, nothing it sent can still commit (commands Redis read
		// were executed when read; unread ones are dropped with the socket).
		if err := e.killUpstream(upName(0)); err != nil {
			e.logf("respproxy: fence hold for %s exceeded %s; upstream kill failed (%v); keeping the fence", p.Name, e.cfg.FenceHoldMax, err)
			timer = time.AfterFunc(e.cfg.FenceHoldMax, onTimeout)
			return
		}
		e.nFenceTimeout.Add(1)
		e.logf("respproxy: fence hold for %s exceeded %s: killed the unit's upstream connection and released its locks", p.Name, e.cfg.FenceHoldMax)
		e.queueLog(logEntry{"event", "fence_timeout", "actor", p.Name, "unit", summarizeUnit(u),
			"ts_ms", strconv.FormatInt(ms(e.now()), 10)})
		e.anc.forget(f) // outcome unknown: drop its edges, re-resolve from Redis
		timedOut = true
		releaseLocked()
	}
	arm := func(v Verdict) Verdict {
		relMu.Lock()
		if !released {
			timer = time.AfterFunc(e.cfg.FenceHoldMax, onTimeout)
		}
		relMu.Unlock()
		return v
	}
	admit := func() Verdict {
		e.nAllow.Add(1)
		if hooks != nil && hooks.admitted != nil {
			hooks.admitted(p, u)
		}
		return arm(Verdict{Done: done})
	}
	deny := func(d *Denial) Verdict {
		for i := len(fences) - 1; i >= 0; i-- {
			fences[i].RUnlock()
		}
		fences = nil
		e.nDeny.Add(1)
		e.logDeny(p, d, u.Mode, u)
		return arm(Verdict{Deny: d, Done: done})
	}

	if len(f.unresolvable) > 0 {
		return deny(&Denial{Reason: ReasonUnknownKey, Detail: "unanchored project write: " + f.unresolvable[0]})
	}

	if f.dirStatOnly() {
		projectSet := map[uint64]bool{}
		locations := make([]location, 0, len(f.dirStat))
		for _, dir := range f.dirStat {
			loc, err := e.anc.resolveDir(dir)
			if err != nil {
				return deny(&Denial{Reason: ReasonBGOnly, Detail: "dir-stat row of an unresolvable directory " + inoStr(dir)})
			}
			locations = append(locations, loc)
			if loc.kind == nsProject {
				projectSet[loc.proj] = true
			}
		}
		var projects []uint64
		for pr := range projectSet {
			projects = append(projects, pr)
		}
		sort.Slice(projects, func(i, j int) bool { return projects[i] < projects[j] })
		for _, pr := range projects {
			fm := e.fence(pr)
			fm.RLock()
			fences = append(fences, fm)
		}
		e.mu.Lock()
		var unresolved uint64
		for _, pr := range projects {
			if l := e.leases[pr]; l != nil && l.unresolved {
				unresolved = pr
				break
			}
		}
		e.mu.Unlock()
		if unresolved != 0 {
			return deny(&Denial{Reason: ReasonLeaseHeld, Proj: unresolved, Detail: "lease transition unresolved"})
		}
		if bypass {
			return admit()
		}
		for _, loc := range locations {
			switch loc.kind {
			case nsRoot, nsDerivatives:
				continue
			case nsProject:
				if e.heldWithin(p, loc.proj, DirStatGrace) {
					continue
				}
				d := e.leaseDenial(ReasonBGOnly, loc.proj)
				d.Detail = "dir-stat row of a project not held within " + DirStatGrace.String()
				return deny(d)
			default:
				return deny(&Denial{Reason: ReasonBGOnly, Detail: "dir-stat row in " + loc.kind.String()})
			}
		}
		return admit()
	}

	// Place the unit against the stored tree (txnunit.go).
	locs, pd := e.placeUnit(u, f, bypass)
	if pd != nil {
		if pd.Reason == ReasonXProject && pd.Proj != 0 {
			hd := e.leaseDenial(ReasonXProject, pd.Proj)
			hd.Detail = pd.Detail
			pd = hd
		}
		return deny(pd)
	}
	projSet := map[uint64]bool{}
	for _, l := range locs {
		switch l.kind {
		case nsProject:
			projSet[l.proj] = true
		case nsRoot, nsDerivatives:
		case nsTrash:
			if !bypass {
				return deny(&Denial{Reason: ReasonBGOnly, Detail: "write in ns:trash"})
			}
		default:
			return deny(&Denial{Reason: ReasonUnknownKey, Detail: "unresolved anchor"})
		}
	}

	// ns:root: creating a name is open; renaming or removing a directory
	// there (a project root or a top-level folder) needs admin.
	if p.Kind != KindAdmin {
		if d := e.rootEntryGate(f); d != nil {
			return deny(d)
		}
	}

	projs := make([]uint64, 0, len(projSet))
	for pr := range projSet {
		projs = append(projs, pr)
	}
	sort.Slice(projs, func(i, j int) bool { return projs[i] < projs[j] })
	if len(projs) == 0 {
		return admit()
	}

	var granted []uint64
	for attempt := 0; ; attempt++ {
		for _, pr := range projs {
			fm := e.fence(pr)
			fm.RLock()
			fences = append(fences, fm)
		}
		var other *lease
		var otherSnap leaseSnap
		var unresolved uint64
		var free []uint64
		e.mu.Lock()
		for _, pr := range projs {
			l := e.leases[pr]
			switch {
			case l != nil && l.unresolved:
				unresolved = pr
			case l == nil || !l.held():
				free = append(free, pr)
			case l.holder != p.Name:
				if other == nil {
					other, otherSnap = l, l.snap()
				}
			default:
				epochs[pr] = l.epoch
			}
		}
		e.mu.Unlock()
		if unresolved != 0 {
			v := deny(&Denial{Reason: ReasonLeaseHeld, Proj: unresolved, Detail: "lease transition unresolved"})
			e.rollback(p, granted)
			return v
		}
		if bypass {
			return admit()
		}
		if other != nil {
			reason := ReasonLeaseHeld
			if len(projs) > 1 {
				reason = ReasonXProject
			}
			d := &Denial{Reason: reason, Proj: otherSnap.Proj, Holder: otherSnap.Holder, Host: otherSnap.Host, Epoch: otherSnap.Epoch,
				Detail: fmt.Sprintf("project %d held by %s (%d project(s) in unit)", otherSnap.Proj, otherSnap.Holder, len(projs))}
			v := deny(d)
			e.rollback(p, granted)
			return v
		}
		if len(free) == 0 {
			e.touch(p, projs)
			return admit()
		}
		// Grant the free projects (write side), then re-check under the
		// read side.
		for i := len(fences) - 1; i >= 0; i-- {
			fences[i].RUnlock()
		}
		fences = nil
		if attempt >= 3 {
			d := &Denial{Reason: ReasonLeaseHeld, Proj: free[0], Detail: "lease grant raced repeatedly"}
			v := deny(d)
			e.rollback(p, granted)
			return v
		}
		for _, pr := range free {
			fm := e.fence(pr)
			fm.Lock()
			s, _, err := e.grantLocked(p, pr, LeaseAuto, 0, "implicit: first admitted write: "+summarizeUnit(u))
			fm.Unlock()
			if err != nil {
				v := deny(&Denial{Reason: ReasonLeaseHeld, Proj: pr, Detail: "lease store unavailable: " + err.Error()})
				e.rollback(p, granted)
				return v
			}
			if s.Holder == p.Name {
				granted = append(granted, pr)
				e.nGrant.Add(1)
			}
		}
	}
}

// rollback ends the grants a refused unit made (all-or-none).
func (e *LeaseEngine) rollback(p Principal, granted []uint64) {
	for _, pr := range granted {
		fm := e.fence(pr)
		fm.Lock()
		e.mu.Lock()
		mine := e.leases[pr] != nil && e.leases[pr].holder == p.Name
		e.mu.Unlock()
		if mine {
			if _, _, err := e.endLocked(pr, "release", p.Name, "rollback: unit refused (all-or-none)"); err != nil {
				e.logf("respproxy: lease rollback %d: %v", pr, err)
			}
		}
		fm.Unlock()
	}
}

// leaseDenial fills the holder fields of a refusal from the lease table.
func (e *LeaseEngine) leaseDenial(r Reason, proj uint64) *Denial {
	d := &Denial{Reason: r, Proj: proj}
	e.mu.Lock()
	if l := e.leases[proj]; l != nil && l.held() {
		d.Holder, d.Host, d.Epoch = l.holder, l.host, l.epoch
	}
	e.mu.Unlock()
	return d
}

// rootEntryGate refuses renaming or removing a directory entry of a
// directory above the project depth (plan founder default 5).
func (e *LeaseEngine) rootEntryGate(f *unitFacts) *Denial {
	for _, op := range f.dentries {
		if op.trash {
			continue
		}
		loc, err := e.anc.resolveDir(op.dir)
		if err != nil || loc.kind != nsRoot {
			continue
		}
		e.anc.sideReads.Add(1)
		v, err := e.cfg.Side.Do(e.cfg.VolumeDB, []byte("HGET"), []byte("d"+strconv.FormatUint(op.dir, 10)), []byte(op.name))
		if err != nil || v.Err() != nil {
			return &Denial{Reason: ReasonUnknownKey, Detail: "cannot read root entry " + op.name}
		}
		if v.Null {
			continue // a new name: open to every writer
		}
		if t, ok := entryType(v.Str); !ok || t != typeDir {
			continue // a file at the top of the tree
		}
		if op.del || !bytes.Equal(op.val, v.Str) {
			proj, _ := entryInode(v.Str)
			return &Denial{Reason: ReasonAdminOnly, Proj: proj,
				Detail: "renaming or removing directory " + strconv.Quote(op.name) + " above the project depth needs admin"}
		}
	}
	return nil
}

// Stats is a snapshot of the engine's counters.
type LeaseEngineStats struct {
	Allow, Deny, Grant int64
	FenceTimeouts      int64
	LostKills          int64
	Leases, Held       int
	Dirs, FileCache    int
	Memo               int
	Generation         uint64
	SideReads          int64
	LogDropped         int64
	Bootstrap          BootstrapStats
}

func (e *LeaseEngine) Stats() LeaseEngineStats {
	s := LeaseEngineStats{
		Allow: e.nAllow.Load(), Deny: e.nDeny.Load(), Grant: e.nGrant.Load(), FenceTimeouts: e.nFenceTimeout.Load(), LostKills: e.nLostKill.Load(),
		Dirs: e.anc.dirCount(), FileCache: e.anc.files.len(), Memo: e.anc.memo.size(),
		Generation: e.anc.memo.generation(), SideReads: e.anc.sideReads.Load(),
		LogDropped: e.logDropped.Load(), Bootstrap: e.Bootstrap(),
	}
	e.mu.Lock()
	s.Leases = len(e.leases)
	for _, l := range e.leases {
		if l.held() {
			s.Held++
		}
	}
	e.mu.Unlock()
	return s
}

// killUpstream closes, inside Redis, the one upstream connection named
// name (L1 names each one UpstreamName(principal, connID)) and returns only
// once CLIENT LIST no longer shows it. CLIENT KILL frees the client before
// it replies, so after this returns no command that connection sent can
// still execute. A name that is not listed is a connection already gone.
// Other connections of the same principal are untouched.
func (e *LeaseEngine) killUpstream(name string) error {
	if name == "" {
		return errors.New("respproxy: unit has no upstream name")
	}
	for attempt := 0; attempt < 5; attempt++ {
		ids, err := e.upstreamIDs(name)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil // confirmed absent
		}
		for _, id := range ids {
			if _, err := e.side("CLIENT", "KILL", "ID", id); err != nil && !strings.Contains(err.Error(), "No such client") {
				return err
			}
		}
	}
	return errors.New("respproxy: upstream " + name + " still listed after CLIENT KILL")
}

// upstreamIDs lists the Redis client ids whose name is exactly name.
func (e *LeaseEngine) upstreamIDs(name string) ([]string, error) {
	v, err := e.side("CLIENT", "LIST", "TYPE", "normal")
	if err != nil {
		return nil, err
	}
	want := "name=" + name
	var ids []string
	for _, line := range strings.Split(string(v.Str), "\n") {
		var id string
		named := false
		for _, f := range strings.Fields(line) {
			if strings.HasPrefix(f, "id=") {
				id = f[3:]
			}
			if f == want {
				named = true
			}
		}
		if named && id != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
