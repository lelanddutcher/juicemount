package nfs

import (
	"encoding/binary"
	"errors"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// The live link policy of the bundled juicefs (patch 0027,
// third_party/juicefs/pkg/vfs/jm_link_policy.go).
//
// The app mounts juicefs with flags chosen from the link class at that
// moment (health/fuse.go): --demand-first on a slow or metered link, and
// longer object timeouts with fewer retries on a slow or metered read
// policy. Before patch 0027 those flags held until the next remount. A
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

const (
	jfsLinkPolicyVersion   = 1
	jfsLinkFlagDemandFirst = 1
	jfsLinkReplyLen        = 1 + 1 + 4 + 4 + 1
	jfsLinkTimeout         = 10 * time.Second
)

// jfsLinkPolicy is the set of juicefs settings that follow the link.
type jfsLinkPolicy struct {
	DemandFirst bool
	GetTimeout  time.Duration
	PutTimeout  time.Duration
	IORetries   int
}

// The upstream defaults: what a fast or medium mount runs with.
var jfsLinkUpstream = jfsLinkPolicy{GetTimeout: 60 * time.Second, PutTimeout: 60 * time.Second, IORetries: 10}

// jfsLinkPolicyFor is the policy health/fuse.go gives a mount on these
// classes: --demand-first follows Class (desktopJuiceFSDemandFirstArgs),
// the object deadlines follow ReadPolicyClass (desktopJuiceFSObjectIOArgs).
// health's TestJuiceFSLivePolicyMatchesMountArgs keeps the two equal. The
// rollback switches of those flags hold here too.
func jfsLinkPolicyFor(class, readClass netprofile.LinkClass) jfsLinkPolicy {
	p := jfsLinkUpstream
	if os.Getenv("JM_JFS_DEMAND_FIRST") != "0" {
		p.DemandFirst = class == netprofile.ClassSlow || class == netprofile.ClassMetered
	}
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

func encodeLinkPolicy(p jfsLinkPolicy) []byte {
	var flags byte
	if p.DemandFirst {
		flags |= jfsLinkFlagDemandFirst
	}
	b := []byte{flags}
	b = binary.BigEndian.AppendUint32(b, uint32(p.GetTimeout/time.Millisecond))
	b = binary.BigEndian.AppendUint32(b, uint32(p.PutTimeout/time.Millisecond))
	return append(b, byte(p.IORetries))
}

func decodeLinkPolicy(b []byte) jfsLinkPolicy {
	return jfsLinkPolicy{
		DemandFirst: b[0]&jfsLinkFlagDemandFirst != 0,
		GetTimeout:  time.Duration(binary.BigEndian.Uint32(b[1:5])) * time.Millisecond,
		PutTimeout:  time.Duration(binary.BigEndian.Uint32(b[5:9])) * time.Millisecond,
		IORetries:   int(b[9]),
	}
}

// linkPolicy queries (set == nil) or sets the policy; it returns the policy
// in force afterwards. errControlUnsupported: a juicefs without patch 0027.
func (c *jfsControl) linkPolicy(set *jfsLinkPolicy) (jfsLinkPolicy, error) {
	body := []byte{jfsLinkPolicyVersion}
	if set != nil {
		body = append(body, encodeLinkPolicy(*set)...)
	}
	msg := append(controlHeader(jfsMsgLinkPolicy, len(body)), body...)
	reply, err := c.exchangeUntil(msg, jfsLinkReplyLen, func(r []byte) int {
		if len(r) > 0 && r[0] != 0 {
			return 1
		}
		return jfsLinkReplyLen
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
	if len(reply) < jfsLinkReplyLen {
		return jfsLinkPolicy{}, errors.New("juicefs link policy: short reply")
	}
	return decodeLinkPolicy(reply[1:]), nil
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
)

// linkHysteresis runs one hold for demand-first and one for the object
// deadlines. Abroad, ReadPolicyClass moves between slow and metered as the
// measured bandwidth crosses 3 MiB/s (netprofile has no hysteresis on
// bandwidth), so the requested deadlines can change every few seconds while
// demand-first stays on: one hold over the whole policy restarted on each
// change and kept a lan mount's whole-block reads abroad for minutes.
type linkHysteresis struct {
	demand   linkHold[bool]
	timeouts linkHold[jfsLinkTimeouts]
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
	return jfsLinkPolicy{DemandFirst: d, GetTimeout: t.get, PutTimeout: t.put, IORetries: t.retries}
}

// linkFollower keeps the juicefs policy equal to the hysteresis output.
type linkFollower struct {
	exchange func(set *jfsLinkPolicy) (jfsLinkPolicy, error)
	classes  func() (class, readClass netprofile.LinkClass)

	hyst        linkHysteresis
	applied     jfsLinkPolicy
	haveApplied bool
	verifiedAt  time.Time
	unsupported bool
	lastErrLog  time.Time
}

func newLinkFollower(ctl *jfsControl) *linkFollower {
	return &linkFollower{
		exchange: ctl.linkPolicy,
		classes: func() (netprofile.LinkClass, netprofile.LinkClass) {
			return jfsLinkClasses(netprofile.Default())
		},
	}
}

// tick runs one step at now. It talks to juicefs only when the target
// changed or the last check is older than linkVerifyPeriod (a remount
// starts juicefs again with its mount flags).
func (f *linkFollower) tick(now time.Time) {
	if f.unsupported {
		return
	}
	class, readClass := f.classes()
	target := f.hyst.observe(now, jfsLinkPolicyFor(class, readClass))
	if f.haveApplied && f.applied == target && now.Sub(f.verifiedAt) < linkVerifyPeriod {
		return
	}
	cur, err := f.exchange(nil)
	if err == nil && cur != target {
		var set jfsLinkPolicy
		if set, err = f.exchange(&target); err == nil {
			jmlog.Info("juicefs link policy switched",
				"class", class.String(), "read_class", readClass.String(),
				"demand_first", set.DemandFirst, "was_demand_first", cur.DemandFirst,
				"get_timeout_s", int(set.GetTimeout.Seconds()), "was_get_timeout_s", int(cur.GetTimeout.Seconds()),
				"put_timeout_s", int(set.PutTimeout.Seconds()), "io_retries", set.IORetries)
			cur = set
		}
	}
	if errors.Is(err, errControlUnsupported) {
		f.unsupported = true
		jmlog.Info("juicefs link policy: this juicefs has no live policy; its mount flags hold until a remount")
		return
	}
	if err != nil {
		if now.Sub(f.lastErrLog) >= 10*time.Minute {
			f.lastErrLog = now
			jmlog.Warn("juicefs link policy: exchange failed; retrying", "error", err.Error())
		}
		f.haveApplied = false
		return
	}
	f.applied, f.haveApplied, f.verifiedAt = cur, true, now
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
				return
			case now := <-t.C:
				f.tick(now)
			}
		}
	}()
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
