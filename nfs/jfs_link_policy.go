package nfs

import (
	"encoding/binary"
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// The live link policy of the bundled juicefs (patch 0027,
// third_party/juicefs/pkg/vfs/jm_link_policy.go).
//
// The app mounts juicefs with flags chosen from the link class at that
// moment (health/fuse.go): longer object timeouts with fewer retries on a
// slow or metered read policy, and --demand-first on every class but fast
// (slow and metered only until 2026-11-13). Before patch 0027 those flags
// held until the next remount. A
// volume mounted at home that woke abroad (laptop run 2026-11-12, 200 ms, 1%
// loss, server BBR) kept fetching whole 4 MiB blocks for every miss: first
// byte of an unpinned clip 6-7 s and 1 MiB scrubs 5-6 s (p95 12.2 s),
// against 3.7 s and 1.5 s (p95 4.3 s) on a volume mounted abroad.
//
// linkFollower watches the live class in this process (the one the NFS
// layer's own read-ahead and demand-first rules follow) and sends juicefs
// the policy the mount would have got on that class, with hysteresis. No
// remount: open files keep their handles and in-flight GETs keep their
// deadlines.
const jfsMsgLinkPolicy = 1106

// Version 2 of the message (patch 0035) adds the read-ahead of one
// sequential reader inside juicefs (netprofile.JuiceFSReadaheadMiB). A
// juicefs without patch 0035 refuses version 2; the follower then keeps
// version 1 and leaves the read-ahead at the mount's (off).
const (
	jfsLinkPolicyVersion   = 1
	jfsLinkPolicyVersion2  = 2
	jfsLinkFlagDemandFirst = 1
	jfsLinkReplyLen        = 1 + 1 + 4 + 4 + 1
	jfsLinkReplyLen2       = jfsLinkReplyLen + 4
	jfsLinkTimeout         = 10 * time.Second
)

// jfsLinkPolicy is the set of juicefs settings that follow the link.
type jfsLinkPolicy struct {
	DemandFirst bool
	GetTimeout  time.Duration
	PutTimeout  time.Duration
	IORetries   int
	// ReadaheadMiB is juicefs's read-ahead of one sequential reader (0 =
	// off); -1 when juicefs cannot set it (no patch 0035).
	ReadaheadMiB int
}

// The upstream defaults: what a fast mount runs with (a medium mount adds
// demand-first).
var jfsLinkUpstream = jfsLinkPolicy{GetTimeout: 60 * time.Second, PutTimeout: 60 * time.Second, IORetries: 10}

// jfsLinkPolicyFor is the policy health/fuse.go gives a mount on these
// classes: --demand-first on every class except fast
// (desktopJuiceFSDemandFirstArgs: measured better on lan 2026-11-13; fast
// is not measured yet), the object deadlines by ReadPolicyClass
// (desktopJuiceFSObjectIOArgs). health's
// TestJuiceFSLivePolicyMatchesMountArgs keeps the two equal. The rollback
// switches of those flags hold here too.
func jfsLinkPolicyFor(class, readClass netprofile.LinkClass) jfsLinkPolicy {
	p := jfsLinkUpstream
	p.DemandFirst = os.Getenv("JM_JFS_DEMAND_FIRST") != "0" && class != netprofile.ClassFast
	if os.Getenv("JM_JFS_OBJECT_TIMEOUTS") != "0" {
		switch readClass {
		case netprofile.ClassMetered:
			p.GetTimeout, p.PutTimeout, p.IORetries = 3*time.Minute, 5*time.Minute, 2
		case netprofile.ClassSlow:
			p.GetTimeout, p.PutTimeout, p.IORetries = 2*time.Minute, 3*time.Minute, 3
		}
	}
	return p
}

// encodeLinkPolicy is the body after the version byte: version 2 adds the
// read-ahead in KiB.
func encodeLinkPolicy(p jfsLinkPolicy, version byte) []byte {
	var flags byte
	if p.DemandFirst {
		flags |= jfsLinkFlagDemandFirst
	}
	b := []byte{flags}
	b = binary.BigEndian.AppendUint32(b, uint32(p.GetTimeout/time.Millisecond))
	b = binary.BigEndian.AppendUint32(b, uint32(p.PutTimeout/time.Millisecond))
	b = append(b, byte(p.IORetries))
	if version == jfsLinkPolicyVersion2 {
		b = binary.BigEndian.AppendUint32(b, uint32(max(p.ReadaheadMiB, 0))<<10)
	}
	return b
}

func decodeLinkPolicy(b []byte) jfsLinkPolicy {
	p := jfsLinkPolicy{
		DemandFirst:  b[0]&jfsLinkFlagDemandFirst != 0,
		GetTimeout:   time.Duration(binary.BigEndian.Uint32(b[1:5])) * time.Millisecond,
		PutTimeout:   time.Duration(binary.BigEndian.Uint32(b[5:9])) * time.Millisecond,
		IORetries:    int(b[9]),
		ReadaheadMiB: -1,
	}
	if len(b) >= jfsLinkReplyLen2-1 {
		p.ReadaheadMiB = int(binary.BigEndian.Uint32(b[10:14]) >> 10)
	}
	return p
}

// linkPolicy queries (set == nil) or sets the policy; it returns the policy
// in force afterwards. errControlUnsupported: a juicefs without patch 0027.
// It speaks version 2 until juicefs refuses the version 2 query and answers
// version 1 (no patch 0035); from then on it speaks version 1, and the
// policy it returns has ReadaheadMiB -1.
func (c *jfsControl) linkPolicy(set *jfsLinkPolicy) (jfsLinkPolicy, error) {
	if !c.linkV1Only.Load() {
		p, err := c.linkPolicyVersion(jfsLinkPolicyVersion2, set)
		if set != nil || !errors.Is(err, errControlUnsupported) {
			return p, err
		}
		if p, err = c.linkPolicyVersion(jfsLinkPolicyVersion, nil); err != nil {
			return p, err
		}
		c.linkV1Only.Store(true)
		return p, nil
	}
	return c.linkPolicyVersion(jfsLinkPolicyVersion, set)
}

func (c *jfsControl) linkPolicyVersion(version byte, set *jfsLinkPolicy) (jfsLinkPolicy, error) {
	replyLen := jfsLinkReplyLen
	if version == jfsLinkPolicyVersion2 {
		replyLen = jfsLinkReplyLen2
	}
	body := []byte{version}
	if set != nil {
		body = append(body, encodeLinkPolicy(*set, version)...)
	}
	msg := append(controlHeader(jfsMsgLinkPolicy, len(body)), body...)
	reply, err := c.exchangeUntil(msg, replyLen, func(r []byte) int {
		if len(r) > 0 && r[0] != 0 {
			return 1
		}
		return replyLen
	}, jfsLinkTimeout)
	if err != nil {
		return jfsLinkPolicy{}, err
	}
	if len(reply) == 0 {
		return jfsLinkPolicy{}, errors.New("juicefs link policy: empty reply")
	}
	if set == nil && syscall.Errno(reply[0]) == syscall.EINVAL {
		// A juicefs without patch 0027 answers any unknown message with one
		// EINVAL byte (pkg/vfs/internal.go default). A patched one never
		// refuses the bare version-1 query.
		return jfsLinkPolicy{}, errControlUnsupported
	}
	if err := controlErrno(reply[0]); err != nil {
		return jfsLinkPolicy{}, err
	}
	if len(reply) < replyLen {
		return jfsLinkPolicy{}, errors.New("juicefs link policy: short reply")
	}
	return decodeLinkPolicy(reply[1:replyLen]), nil
}

// Hysteresis. A move to a more cautious policy must hold for
// linkSlowerHold, a move back for linkFasterHold: the cautious policy costs
// little on a fast link (ranged GETs of 256 KiB parts), while the fast one
// on a slow link is the 3x regression above. A link that flaps faster than
// linkSlowerHold keeps the policy it has.
const (
	linkSlowerHold   = 10 * time.Second
	linkFasterHold   = 60 * time.Second
	linkFollowTick   = 2 * time.Second
	linkVerifyPeriod = time.Minute
	// linkVerifyReadahead: the check period while juicefs reads ahead. A
	// restarted juicefs comes back with read-ahead off while the NFS layer's
	// read-ahead still stands back for it (review L2).
	linkVerifyReadahead = 10 * time.Second
)

// linkHysteresis runs one hold for demand-first and one for the object
// deadlines. Abroad, ReadPolicyClass moves between slow and metered as the
// measured bandwidth crosses 3 MiB/s (netprofile has no hysteresis on
// bandwidth), so the requested deadlines can change every few seconds while
// demand-first stays on: one hold over the whole policy restarted on each
// change and kept a lan mount's whole-block reads abroad for minutes.
//
// The read-ahead has its own hold (readaheadHold).
type linkHysteresis struct {
	demand    linkHold[bool]
	timeouts  linkHold[jfsLinkTimeouts]
	readahead readaheadHold
}

// readaheadHold: juicefs read-ahead starts off, and a new size must be
// asked for readaheadHoldFor without a break before it applies, both ways.
// Not known yet (-1) asks for off. Unlike linkHold, the first request gets
// no shortcut: the first bandwidth readings of a session are often low (the
// open and the first GETs), so a near link reads as metered for a moment.
// With the linkHold rules that first reading became the decision and the
// read-ahead then waited linkFasterHold to come on (lab, 2026-11-27).
//
// Bounded on-time (review H1): while read-ahead is on, the bandwidth
// estimate reads low and may not mark the link metered (netprofile
// JuiceFSReadaheadMiB), so a near link that turns into an unflagged hotspot
// under the same tunnel would keep read-ahead for the whole session. After
// readaheadMaxOn on, the hold turns it off once; it comes back only after a
// measurement taken with it off says not metered for readaheadHoldFor.
type readaheadHold struct {
	stable, pending int
	pendingSet      bool
	since           time.Time
	onSince         time.Time
}

// readaheadMaxOn bounds one stretch of read-ahead. Each re-check costs a
// near link about 12 s without read-ahead (the estimate recovers in about a
// second of reading, then the 10 s hold); a link that turned thin stops
// after at most this long.
const readaheadMaxOn = 3 * time.Minute

// readaheadHoldFor is the read-ahead hold, both ways. Off waits too: one
// metered reading on a near link must not stop a playback's read-ahead, and
// on a link that really turned metered 10 s of read-ahead is at most what
// the reader asked for in that time plus one read-ahead window.
const readaheadHoldFor = 10 * time.Second

func (h *readaheadHold) observe(now time.Time, want int) int {
	want = max(want, 0)
	if h.stable > 0 && now.Sub(h.onSince) >= readaheadMaxOn {
		// Re-qualify: off now, and on again only through the hold, from a
		// request made while it is off (the follower passes on=false).
		h.stable, h.pendingSet = 0, false
		return 0
	}
	if want == h.stable {
		h.pendingSet = false
		return h.stable
	}
	if !h.pendingSet || want != h.pending {
		h.pending, h.since, h.pendingSet = want, now, true
	}
	if now.Sub(h.since) >= readaheadHoldFor {
		if h.stable == 0 && want > 0 {
			h.onSince = now
		}
		h.stable, h.pendingSet = want, false
	}
	return h.stable
}

type jfsLinkTimeouts struct {
	get, put time.Duration
	retries  int
}

// linkHold is the hold of one part of the policy. Its timer runs while
// requests differ from the stable value in one direction (all more
// cautious, or all less); it restarts only when that direction changes.
// When the hold ends the part takes the latest request.
type linkHold[T comparable] struct {
	stable T
	have   bool
	dir    int // 0 none, +1 more cautious, -1 less
	since  time.Time
}

func (h *linkHold[T]) observe(now time.Time, want T, slower bool) T {
	// The first decision has no hold: the first request becomes the stable
	// value at once, even when it is more cautious than the mount flags. The
	// mount flags came from an older sample (a single dial in the volume
	// owner, before the serving process measured anything), so there is no
	// settled state to protect yet. The holds apply from the second tick on.
	// That is why a lan mount whose live class is already slow switches at
	// the first successful tick, about 2 s after the NFS server starts, and
	// not after 10 s.
	if !h.have {
		h.stable, h.have = want, true
		return want
	}
	if want == h.stable {
		h.dir = 0
		return h.stable
	}
	dir, hold := -1, linkFasterHold
	if slower {
		dir, hold = 1, linkSlowerHold
	}
	if dir != h.dir {
		h.dir, h.since = dir, now
	}
	if now.Sub(h.since) >= hold {
		h.stable, h.dir = want, 0
	}
	return h.stable
}

// observe folds in the policy the classes ask for now and returns the
// policy to run.
func (h *linkHysteresis) observe(now time.Time, want jfsLinkPolicy) jfsLinkPolicy {
	d := h.demand.observe(now, want.DemandFirst, want.DemandFirst && !h.demand.stable)
	wt := jfsLinkTimeouts{want.GetTimeout, want.PutTimeout, want.IORetries}
	t := h.timeouts.observe(now, wt, wt.get > h.timeouts.stable.get)
	ra := h.readahead.observe(now, want.ReadaheadMiB)
	return jfsLinkPolicy{DemandFirst: d, GetTimeout: t.get, PutTimeout: t.put, IORetries: t.retries, ReadaheadMiB: ra}
}

// linkFollower keeps the juicefs policy equal to the hysteresis output.
type linkFollower struct {
	exchange func(set *jfsLinkPolicy) (jfsLinkPolicy, error)
	classes  func() (class, readClass netprofile.LinkClass)
	// readahead is the juicefs read-ahead the link asks for now (MiB),
	// given whether it is on now; nil asks for none.
	readahead func(on bool) int
	// noReadahead: juicefs cannot set its read-ahead (no patch 0035).
	noReadahead bool
	// engineRA reports juicefs read-ahead turning on or off to the link
	// estimator (netprofile SetEngineReadahead); raOn is the last report.
	engineRA func(on bool)
	raOn     bool

	hyst        linkHysteresis
	applied     jfsLinkPolicy
	haveApplied bool
	verifiedAt  time.Time
	unsupported bool
	lastErrLog  time.Time
	startedAt   time.Time // the first tick
	answered    bool      // juicefs has answered at least once
	warn        func(msg string, attrs ...any)
}

// linkStartupGrace: the follower starts with the NFS server, and the first
// ticks can come before juicefs serves its control file (laptop run
// 2026-11-13: "no such process" at the first tick of every session, success
// 2 s later). Until juicefs first answers, failures in this window are
// logged at debug level only. A juicefs that never answers is still warned
// about once the window ends.
const linkStartupGrace = 30 * time.Second

func newLinkFollower(ctl *jfsControl) *linkFollower {
	return &linkFollower{
		exchange: ctl.linkPolicy,
		classes: func() (netprofile.LinkClass, netprofile.LinkClass) {
			return jfsLinkClasses(netprofile.Default())
		},
		readahead: netprofile.Default().JuiceFSReadaheadMiB,
		engineRA: func(on bool) {
			netprofile.Default().SetEngineReadahead(on)
			setJFSReadaheadActive(ctl.root, on)
		},
	}
}

// jfsReadaheadOn holds, per FUSE root, whether juicefs reads ahead for its
// sequential readers now (patch 0035). The NFS layer's ReadaheadManager on
// that root then schedules nothing (readahead.go OnRead): juicefs already
// fetches whole blocks ahead of the reader, closer to the link. Both at once
// fetched twice: on lan a 1-4 MiB scrub jump made the NFS layer prefetch
// four blocks, and its sequential reads gave juicefs a run long enough to
// read ahead 16 MiB more (lab 2026-11-27: 125 -> 158 MiB for 81 MiB of
// scrub). On the Mac the NFS layer's reads also queue in macFUSE's one read
// per file in front of the reader's.
var jfsReadaheadOn sync.Map // root -> *atomic.Bool

func setJFSReadaheadActive(root string, on bool) {
	v, _ := jfsReadaheadOn.LoadOrStore(root, new(atomic.Bool))
	v.(*atomic.Bool).Store(on)
}

// jfsReadaheadActive reports whether juicefs at root reads ahead now.
func jfsReadaheadActive(root string) bool {
	v, ok := jfsReadaheadOn.Load(root)
	return ok && v.(*atomic.Bool).Load()
}

// reportReadahead tells the link estimator whether juicefs reads ahead now.
func (f *linkFollower) reportReadahead(on bool) {
	if on == f.raOn || f.engineRA == nil {
		return
	}
	f.raOn = on
	f.engineRA(on)
}

// tick runs one step at now. It talks to juicefs only when the target
// changed or the last check is older than linkVerifyPeriod (a remount
// starts juicefs again with its mount flags).
func (f *linkFollower) tick(now time.Time) {
	if f.unsupported {
		return
	}
	if f.startedAt.IsZero() {
		f.startedAt = now
	}
	class, readClass := f.classes()
	want := jfsLinkPolicyFor(class, readClass)
	if f.readahead != nil {
		want.ReadaheadMiB = f.readahead(f.hyst.readahead.stable > 0)
	}
	target := f.hyst.observe(now, want)
	if f.noReadahead {
		target.ReadaheadMiB = -1
	}
	verify := linkVerifyPeriod
	if f.raOn {
		verify = linkVerifyReadahead
	}
	if f.haveApplied && f.applied == target && now.Sub(f.verifiedAt) < verify {
		return
	}
	cur, err := f.exchange(nil)
	if err == nil && cur.ReadaheadMiB < 0 && !f.noReadahead {
		f.noReadahead, target.ReadaheadMiB = true, -1
		jmlog.Info("juicefs link policy: this juicefs cannot set its read-ahead (no patch 0035); it stays at the mount's")
	}
	if err == nil && cur != target {
		var set jfsLinkPolicy
		if set, err = f.exchange(&target); err == nil {
			jmlog.Info("juicefs link policy switched",
				"class", class.String(), "read_class", readClass.String(),
				"demand_first", set.DemandFirst, "was_demand_first", cur.DemandFirst,
				"get_timeout_s", int(set.GetTimeout.Seconds()), "was_get_timeout_s", int(cur.GetTimeout.Seconds()),
				"put_timeout_s", int(set.PutTimeout.Seconds()), "io_retries", set.IORetries,
				"readahead_mib", set.ReadaheadMiB, "was_readahead_mib", cur.ReadaheadMiB)
			cur = set
		}
	}
	if errors.Is(err, errControlUnsupported) {
		f.unsupported = true
		jmlog.Info("juicefs link policy: this juicefs has no live policy; its mount flags hold until a remount")
		return
	}
	if err != nil {
		f.haveApplied = false
		f.reportReadahead(false) // unknown: let the estimate count again
		if !f.answered && now.Sub(f.startedAt) < linkStartupGrace {
			jmlog.Debug("juicefs link policy: juicefs not answering yet; retrying", "error", err.Error())
			return
		}
		if now.Sub(f.lastErrLog) >= 10*time.Minute {
			f.lastErrLog = now
			warn := f.warn
			if warn == nil {
				warn = jmlog.Warn
			}
			warn("juicefs link policy: exchange failed; retrying", "error", err.Error())
		}
		return
	}
	f.answered = true
	f.applied, f.haveApplied, f.verifiedAt = cur, true, now
	f.reportReadahead(cur.ReadaheadMiB > 0)
}

// linkFollowers runs one follower per FUSE root while its NFS server serves.
var (
	linkFollowersMu sync.Mutex
	linkFollowers   = map[string]chan struct{}{}
)

// startLinkFollower starts the follower of the juicefs at fuseRoot, once.
// JM_JFS_LIVE_POLICY=0 is the field rollback: the mount flags then hold
// until the next remount, as before patch 0027.
func startLinkFollower(fuseRoot string) {
	if fuseRoot == "" || os.Getenv("JM_JFS_LIVE_POLICY") == "0" {
		return
	}
	linkFollowersMu.Lock()
	defer linkFollowersMu.Unlock()
	if _, ok := linkFollowers[fuseRoot]; ok {
		return
	}
	stop := make(chan struct{})
	linkFollowers[fuseRoot] = stop
	f := newLinkFollower(sharedJFSControl(fuseRoot))
	go func() {
		t := time.NewTicker(linkFollowTick)
		defer t.Stop()
		for {
			select {
			case <-stop:
				f.reportReadahead(false)
				return
			case now := <-t.C:
				f.tick(now)
			}
		}
	}()
}

// FollowJFSLinkPolicy runs the link follower of the juicefs at fuseRoot, as
// the NFS server does (lab probes). It returns the function that stops it.
func FollowJFSLinkPolicy(fuseRoot string) (stop func()) {
	startLinkFollower(fuseRoot)
	return func() { stopLinkFollower(fuseRoot) }
}

func stopLinkFollower(fuseRoot string) {
	linkFollowersMu.Lock()
	defer linkFollowersMu.Unlock()
	if stop, ok := linkFollowers[fuseRoot]; ok {
		close(stop)
		delete(linkFollowers, fuseRoot)
	}
}

// jfsLinkClasses reads the two classes the policy depends on, as
// health/fuse.go's desktopJuiceFSLinkArgs does at mount.
func jfsLinkClasses(p *netprofile.Profile) (class, readClass netprofile.LinkClass) {
	return p.Class(), p.ReadPolicyClass()
}

// JFSLinkPolicyFlags is the live policy for profile p, as the juicefs mount
// flags it stands for. health's tests check it against the flags a mount on
// the same profile gets, so the two cannot drift apart.
func JFSLinkPolicyFlags(prof *netprofile.Profile) []string {
	p := jfsLinkPolicyFor(jfsLinkClasses(prof))
	var args []string
	if p.GetTimeout != jfsLinkUpstream.GetTimeout || p.PutTimeout != jfsLinkUpstream.PutTimeout || p.IORetries != jfsLinkUpstream.IORetries {
		args = append(args, "--get-timeout", p.GetTimeout.String(), "--put-timeout", p.PutTimeout.String(),
			"--io-retries", strconv.Itoa(p.IORetries))
	}
	if p.DemandFirst {
		args = append(args, "--demand-first")
	}
	return args
}
