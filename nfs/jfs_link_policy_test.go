package nfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

func TestJFSLinkPolicyMessageMatchesJuiceFS(t *testing.T) {
	if jfsMsgLinkPolicy != meta.JMLinkPolicy {
		t.Fatalf("app message %d, juicefs %d", jfsMsgLinkPolicy, meta.JMLinkPolicy)
	}
}

var (
	lanPolicy    = jfsLinkPolicyFor(netprofile.ClassFast, netprofile.ClassFast)
	abroadPolicy = jfsLinkPolicyFor(netprofile.ClassSlow, netprofile.ClassMetered)
)

func TestJFSLinkPolicyForClasses(t *testing.T) {
	// Fast: the upstream defaults, demand-first off until a wired
	// multi-gigabit Mac is measured (PR #90 review MEDIUM-1).
	if lanPolicy != jfsLinkUpstream {
		t.Fatalf("fast policy %+v, want the upstream defaults", lanPolicy)
	}
	// Medium: the upstream deadlines with demand-first (measured on lan
	// 2026-11-13).
	if p, want := jfsLinkPolicyFor(netprofile.ClassMedium, netprofile.ClassMedium), (jfsLinkPolicy{DemandFirst: true, GetTimeout: time.Minute, PutTimeout: time.Minute, IORetries: 10}); p != want {
		t.Fatalf("medium policy %+v, want %+v", p, want)
	}
	want := jfsLinkPolicy{DemandFirst: true, GetTimeout: 3 * time.Minute, PutTimeout: 5 * time.Minute, IORetries: 2}
	if abroadPolicy != want {
		t.Fatalf("abroad (far, thin) policy %+v, want %+v", abroadPolicy, want)
	}
	t.Setenv("JM_JFS_DEMAND_FIRST", "0")
	t.Setenv("JM_JFS_OBJECT_TIMEOUTS", "0")
	if p := jfsLinkPolicyFor(netprofile.ClassMetered, netprofile.ClassMetered); p != jfsLinkUpstream {
		t.Fatalf("rollback switches ignored: %+v", p)
	}
}

// The wire: header, version, body; the reply's policy is decoded.
func TestJFSLinkPolicyExchange(t *testing.T) {
	var sent []byte
	reply := []byte{0}
	c := &jfsControl{testExchange: func(msg []byte, n int) ([]byte, error) {
		sent = append([]byte(nil), msg...)
		return reply, nil
	}}
	set := abroadPolicy
	set.ReadaheadMiB = 32
	reply = append([]byte{0}, encodeLinkPolicy(set, jfsLinkPolicyVersion2)...)
	got, err := c.linkPolicy(&set)
	if err != nil || got != set {
		t.Fatalf("set: %+v %v", got, err)
	}
	if binary.BigEndian.Uint32(sent) != 1106 || binary.BigEndian.Uint32(sent[4:]) != 15 {
		t.Fatalf("header % x", sent[:8])
	}
	// v2, demand, 180000 ms, 300000 ms, 2 retries, 32768 KiB read-ahead
	wantBody := []byte{2, 1, 0, 0x02, 0xbf, 0x20, 0, 0x04, 0x93, 0xe0, 2, 0, 0, 0x80, 0}
	if !bytes.Equal(sent[8:], wantBody) {
		t.Fatalf("body % x, want % x", sent[8:], wantBody)
	}
	if _, err := c.linkPolicy(nil); err != nil || len(sent) != 9 || sent[8] != 2 {
		t.Fatalf("query: % x %v", sent, err)
	}
	reply = []byte{byte(syscall.ENOTSUP)}
	if _, err := c.linkPolicy(nil); !errors.Is(err, errControlUnsupported) {
		t.Fatalf("ENOTSUP: %v", err)
	}
	reply = []byte{byte(syscall.EINVAL)}
	if _, err := c.linkPolicy(&abroadPolicy); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("EINVAL to a set: %v", err)
	}
	// A juicefs without patch 0027 answers the unknown message with EINVAL
	// (pkg/vfs/internal.go default); to the bare query that means unsupported.
	if _, err := c.linkPolicy(nil); !errors.Is(err, errControlUnsupported) {
		t.Fatalf("EINVAL to the query: %v, want unsupported", err)
	}
}

// A juicefs with patch 0027 but not 0035 refuses version 2 and answers
// version 1: the app keeps version 1 from then on, without read-ahead.
func TestJFSLinkPolicyFallsBackToVersion1(t *testing.T) {
	var sent [][]byte
	c := &jfsControl{testExchange: func(msg []byte, n int) ([]byte, error) {
		sent = append(sent, append([]byte(nil), msg...))
		if msg[8] != 1 {
			return []byte{byte(syscall.EINVAL)}, nil
		}
		if len(msg) == 9 {
			return append([]byte{0}, encodeLinkPolicy(lanPolicy, jfsLinkPolicyVersion)...), nil
		}
		return append([]byte{0}, msg[9:]...), nil
	}}
	got, err := c.linkPolicy(nil)
	want := lanPolicy
	want.ReadaheadMiB = -1
	if err != nil || got != want || len(sent) != 2 || sent[0][8] != 2 || sent[1][8] != 1 {
		t.Fatalf("query: %+v %v, %d messages", got, err, len(sent))
	}
	set := abroadPolicy
	set.ReadaheadMiB = 32
	got, err = c.linkPolicy(&set)
	want = abroadPolicy
	want.ReadaheadMiB = -1
	if err != nil || got != want || len(sent) != 3 || sent[2][8] != 1 || len(sent[2]) != 8+11 {
		t.Fatalf("set: %+v %v, last % x", got, err, sent[len(sent)-1])
	}
}

// juicefs read-ahead starts off and changes only after the same request
// held for readaheadHoldFor, both ways; a flicker changes nothing.
func TestLinkHysteresisReadahead(t *testing.T) {
	var h linkHysteresis
	t0 := time.Unix(1000, 0)
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	hold := readaheadHoldFor.Seconds()
	on, off, unknown := lanPolicy, lanPolicy, lanPolicy
	on.ReadaheadMiB, unknown.ReadaheadMiB = 16, -1
	if got := h.observe(at(0), on); got.ReadaheadMiB != 0 {
		t.Fatalf("first request applied at once: %d", got.ReadaheadMiB)
	}
	if got := h.observe(at(hold-2), on); got.ReadaheadMiB != 0 {
		t.Fatal("on before the hold")
	}
	if got := h.observe(at(hold), on); got.ReadaheadMiB != 16 {
		t.Fatalf("on after the hold: %d", got.ReadaheadMiB)
	}
	// One metered reading (off) on a near link changes nothing.
	h.observe(at(hold+2), off)
	if got := h.observe(at(hold+4), on); got.ReadaheadMiB != 16 {
		t.Fatalf("a flicker off turned read-ahead off: %d", got.ReadaheadMiB)
	}
	if got := h.observe(at(hold+6), off); got.ReadaheadMiB != 16 {
		t.Fatal("off at once")
	}
	if got := h.observe(at(2*hold+6), unknown); got.ReadaheadMiB != 0 {
		t.Fatalf("off (and not known counts as off) after the hold: %d", got.ReadaheadMiB)
	}
	// A link that is mostly metered with a near reading now and then stays off.
	for i := 0; i < 100; i++ {
		w := off
		if i%4 == 0 {
			w = on
		}
		if got := h.observe(at(100+float64(i)*2), w); got.ReadaheadMiB != 0 {
			t.Fatalf("flicker turned read-ahead on at tick %d", i)
		}
	}
}

// Not measured yet (-1) is off, and a first low reading does not delay the
// read-ahead beyond the hold (lab 2026-11-27: one early metered reading kept
// it off for linkFasterHold).
func TestLinkHysteresisReadaheadFirstReadingMetered(t *testing.T) {
	var h linkHysteresis
	t0 := time.Unix(1000, 0)
	unknown, metered, on := lanPolicy, lanPolicy, lanPolicy
	unknown.ReadaheadMiB, on.ReadaheadMiB = -1, 16
	tick := func(i int, p jfsLinkPolicy) int {
		return h.observe(t0.Add(time.Duration(i)*linkFollowTick), p).ReadaheadMiB
	}
	for i := 0; i < 5; i++ {
		if got := tick(i, unknown); got != 0 {
			t.Fatalf("not known: %d", got)
		}
	}
	tick(5, metered)
	i := 6
	for ; i < 40 && tick(i, on) == 0; i++ {
	}
	if waited := time.Duration(i-6) * linkFollowTick; waited != readaheadHoldFor {
		t.Fatalf("on after %s of near readings, want %s", waited, readaheadHoldFor)
	}
}

// The follower sends the read-ahead the link asks for, and stops asking a
// juicefs that cannot set it.
func TestLinkFollowerReadahead(t *testing.T) {
	j := &fakeJFS{cur: lanPolicy}
	class := [2]netprofile.LinkClass{netprofile.ClassFast, netprofile.ClassFast}
	ra := 32
	var sawOn bool
	var reports []bool
	f := newTestFollower(j, &class)
	f.readahead = func(on bool) int { sawOn = on; return ra }
	f.engineRA = func(on bool) { reports = append(reports, on) }
	t0 := time.Unix(1000, 0)
	now := t0
	run := func(d time.Duration) {
		for end := now.Add(d); now.Before(end); now = now.Add(linkFollowTick) {
			f.tick(now)
		}
	}
	run(readaheadHoldFor + linkFollowTick)
	if len(j.sets) != 1 || j.cur.ReadaheadMiB != 32 {
		t.Fatalf("on: sets %+v", j.sets)
	}
	f.tick(now)
	if !sawOn {
		t.Fatal("the follower did not tell the link rule read-ahead is on")
	}
	ra = 0
	run(readaheadHoldFor + linkFollowTick)
	if len(j.sets) != 2 || j.cur.ReadaheadMiB != 0 {
		t.Fatalf("off: sets %+v", j.sets)
	}
	if len(reports) != 2 || !reports[0] || reports[1] {
		t.Fatalf("reports to the estimator %v, want [true false]", reports)
	}

	old := &fakeJFS{cur: jfsLinkPolicy{GetTimeout: time.Minute, PutTimeout: time.Minute, IORetries: 10, ReadaheadMiB: -1}}
	old.cur.DemandFirst = false
	f = newTestFollower(old, &class)
	ra = 32
	f.readahead = func(bool) int { return ra }
	for i := 0; i < 40; i++ {
		f.tick(t0.Add(time.Duration(i) * linkFollowTick))
	}
	if len(old.sets) != 0 || old.queries != 2 {
		t.Fatalf("juicefs without 0035: %d sets %+v, %d queries (want none and 2)", len(old.sets), old.sets, old.queries)
	}
}

func TestLinkHysteresisBothWays(t *testing.T) {
	var h linkHysteresis
	t0 := time.Unix(1000, 0)
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	if got := h.observe(at(0), lanPolicy); got != lanPolicy {
		t.Fatal("first observation is not taken as is")
	}
	// lan -> abroad: held for linkSlowerHold.
	for s := 2.0; s < 12; s += 2 {
		got := h.observe(at(s), abroadPolicy)
		if want := map[bool]jfsLinkPolicy{true: abroadPolicy, false: lanPolicy}[s-2 >= linkSlowerHold.Seconds()]; got != want {
			t.Fatalf("t=%vs: %+v, want %+v", s, got, want)
		}
	}
	if got := h.observe(at(12), abroadPolicy); got != abroadPolicy {
		t.Fatalf("after %s of slow: still %+v", linkSlowerHold, got)
	}
	// abroad -> lan: held for linkFasterHold.
	if got := h.observe(at(20), lanPolicy); got != abroadPolicy {
		t.Fatal("switched back to fast at once")
	}
	if got := h.observe(at(20+linkFasterHold.Seconds()-1), lanPolicy); got != abroadPolicy {
		t.Fatal("switched back to fast before the hold")
	}
	if got := h.observe(at(20+linkFasterHold.Seconds()), lanPolicy); got != lanPolicy {
		t.Fatal("did not switch back to fast after the hold")
	}
}

// A link that flaps every 4 s never holds a class long enough: the policy
// stays where it is, in either starting state.
func TestLinkHysteresisFlappingDoesNotThrash(t *testing.T) {
	for _, start := range []jfsLinkPolicy{lanPolicy, abroadPolicy} {
		var h linkHysteresis
		t0 := time.Unix(1000, 0)
		h.observe(t0, start)
		other := map[bool]jfsLinkPolicy{true: abroadPolicy, false: lanPolicy}[start == lanPolicy]
		for i := 1; i < 300; i++ { // 10 min at one sample every 2 s
			want := start
			if (i/2)%2 == 1 {
				want = other
			}
			if got := h.observe(t0.Add(time.Duration(i)*2*time.Second), want); got != start {
				t.Fatalf("start %+v: switched at sample %d", start, i)
			}
		}
	}
	// A blip toward slow shorter than the hold is ignored too.
	var h linkHysteresis
	t0 := time.Unix(1000, 0)
	h.observe(t0, lanPolicy)
	h.observe(t0.Add(2*time.Second), abroadPolicy)
	h.observe(t0.Add(8*time.Second), abroadPolicy)
	if got := h.observe(t0.Add(10*time.Second), lanPolicy); got != lanPolicy {
		t.Fatal("blip switched the policy")
	}
	if got := h.observe(t0.Add(14*time.Second), abroadPolicy); got != lanPolicy {
		t.Fatal("a new slow run did not restart the hold")
	}
}

// fakeJFS is a juicefs that holds one policy.
type fakeJFS struct {
	cur     jfsLinkPolicy
	queries int
	sets    []jfsLinkPolicy
	err     error
}

func (j *fakeJFS) exchange(set *jfsLinkPolicy) (jfsLinkPolicy, error) {
	if j.err != nil {
		return jfsLinkPolicy{}, j.err
	}
	if set == nil {
		j.queries++
	} else {
		j.cur = *set
		j.sets = append(j.sets, *set)
	}
	return j.cur, nil
}

func newTestFollower(j *fakeJFS, class *[2]netprofile.LinkClass) *linkFollower {
	return &linkFollower{exchange: j.exchange, classes: func() (netprofile.LinkClass, netprofile.LinkClass) {
		return class[0], class[1]
	}}
}

// A lan mount moves abroad and back, with no remount.
func TestLinkFollowerSwitchesBothWays(t *testing.T) {
	j := &fakeJFS{cur: lanPolicy} // mounted on a fast link: no --demand-first
	class := [2]netprofile.LinkClass{netprofile.ClassFast, netprofile.ClassFast}
	f := newTestFollower(j, &class)
	t0 := time.Unix(1000, 0)
	now := t0
	run := func(d time.Duration) {
		for end := now.Add(d); now.Before(end); now = now.Add(linkFollowTick) {
			f.tick(now)
		}
	}
	run(30 * time.Second)
	if len(j.sets) != 0 || j.queries != 1 {
		t.Fatalf("steady lan: %d sets, %d queries (want 0 and 1)", len(j.sets), j.queries)
	}

	class = [2]netprofile.LinkClass{netprofile.ClassSlow, netprofile.ClassMetered} // abroad
	run(30 * time.Second)
	if len(j.sets) != 1 || j.sets[0] != abroadPolicy {
		t.Fatalf("abroad: sets %+v, want one %+v", j.sets, abroadPolicy)
	}

	class = [2]netprofile.LinkClass{netprofile.ClassFast, netprofile.ClassFast}
	run(linkFasterHold - 4*time.Second)
	if len(j.sets) != 1 {
		t.Fatalf("back to lan: switched before the hold (%+v)", j.sets)
	}
	run(10 * time.Second)
	if len(j.sets) != 2 || j.sets[1] != lanPolicy {
		t.Fatalf("back to lan: sets %+v", j.sets)
	}

	// A remount starts juicefs on its mount flags; the periodic check
	// finds it and sends the policy again.
	class = [2]netprofile.LinkClass{netprofile.ClassSlow, netprofile.ClassMetered}
	run(20 * time.Second)
	if j.cur != abroadPolicy {
		t.Fatal("abroad again: not applied")
	}
	j.cur = lanPolicy // remount on lan flags
	run(linkVerifyPeriod + 2*linkFollowTick)
	if j.cur != abroadPolicy {
		t.Fatal("after a remount the policy was not sent again")
	}
}

func TestLinkFollowerStopsOnOldJuiceFSAndRetriesErrors(t *testing.T) {
	class := [2]netprofile.LinkClass{netprofile.ClassSlow, netprofile.ClassMetered}
	j := &fakeJFS{err: errControlUnsupported}
	f := newTestFollower(j, &class)
	f.tick(time.Unix(1000, 0))
	j.err = nil
	f.tick(time.Unix(1002, 0))
	if !f.unsupported || len(j.sets) != 0 || j.queries != 0 {
		t.Fatalf("old juicefs: unsupported %v, %d sets, %d queries", f.unsupported, len(j.sets), j.queries)
	}

	j = &fakeJFS{err: &errControlNotSent{syscall.ENOENT}} // FUSE not mounted yet
	f = newTestFollower(j, &class)
	f.tick(time.Unix(1000, 0))
	j.err = nil
	f.tick(time.Unix(1002, 0))
	if f.unsupported || len(j.sets) != 1 || j.cur != abroadPolicy {
		t.Fatalf("after a transient error: unsupported %v, sets %+v", f.unsupported, j.sets)
	}
}

// An old juicefs answers EINVAL: the follower asks once in each version and
// stops, instead of one warning in its log every 2 s.
func TestLinkFollowerStopsOnEINVALFromOldJuiceFS(t *testing.T) {
	sent := 0
	c := &jfsControl{testExchange: func(msg []byte, n int) ([]byte, error) {
		sent++
		return []byte{byte(syscall.EINVAL)}, nil
	}}
	class := [2]netprofile.LinkClass{netprofile.ClassSlow, netprofile.ClassMetered}
	f := &linkFollower{exchange: c.linkPolicy, classes: func() (netprofile.LinkClass, netprofile.LinkClass) {
		return class[0], class[1]
	}}
	t0 := time.Unix(1000, 0)
	for i := 0; i < 600; i++ {
		f.tick(t0.Add(time.Duration(i) * linkFollowTick))
	}
	if !f.unsupported || sent != 2 {
		t.Fatalf("unsupported %v after %d messages, want true after 2 (version 2, then 1)", f.unsupported, sent)
	}
}

// Review case HIGH-1: mounted on lan, then abroad at about 30 Mbit/s, where
// ReadPolicyClass flips between slow and metered every 4 s (bandwidth around
// 3 MiB/s, no hysteresis in netprofile). Both ask for demand-first; only the
// deadlines differ. Demand-first must come on after linkSlowerHold, and the
// deadlines must move to the slow side too, not wait for the flapping to end.
func TestLinkHysteresisSlowMeteredFlapStillSwitches(t *testing.T) {
	slow := jfsLinkPolicyFor(netprofile.ClassSlow, netprofile.ClassSlow)
	metered := jfsLinkPolicyFor(netprofile.ClassSlow, netprofile.ClassMetered)
	if !slow.DemandFirst || !metered.DemandFirst || slow.GetTimeout == metered.GetTimeout {
		t.Fatalf("test premise: slow %+v, metered %+v", slow, metered)
	}
	var h linkHysteresis
	t0 := time.Unix(1000, 0)
	h.observe(t0, lanPolicy)
	var got jfsLinkPolicy
	for i := 1; i <= 15; i++ { // 30 s, one sample every 2 s, flipping every 4 s
		want := slow
		if (i/2)%2 == 1 {
			want = metered
		}
		got = h.observe(t0.Add(time.Duration(i)*2*time.Second), want)
		if s := float64(i * 2); s >= linkSlowerHold.Seconds()+2 && !got.DemandFirst {
			t.Fatalf("t=%vs: demand-first still off while every request wants it", s)
		}
	}
	if got.GetTimeout <= lanPolicy.GetTimeout {
		t.Fatalf("deadlines stayed at lan values %+v", got)
	}
	if got != slow && got != metered {
		t.Fatalf("policy %+v is neither request", got)
	}
}

// B3 (laptop run 2026-11-13): the first tick comes before juicefs serves its
// control file. Failures before juicefs first answers, inside the startup
// grace, are not warnings. A juicefs that never answers is still warned
// about after the grace, and a failure after it once answered (a remount)
// warns at once.
func TestLinkFollowerQuietBeforeJuiceFSAnswers(t *testing.T) {
	class := [2]netprofile.LinkClass{netprofile.ClassSlow, netprofile.ClassSlow}
	notReady := &errControlNotSent{syscall.ESRCH}
	t0 := time.Unix(1000, 0)

	j := &fakeJFS{cur: lanPolicy, err: notReady}
	f := newTestFollower(j, &class)
	warnings := 0
	f.warn = func(string, ...any) { warnings++ }
	f.tick(t0)
	j.err = nil
	f.tick(t0.Add(linkFollowTick))
	if warnings != 0 {
		t.Fatalf("start-up failure warned %d times", warnings)
	}
	if j.cur != jfsLinkPolicyFor(class[0], class[1]) {
		t.Fatalf("policy after start-up = %+v", j.cur)
	}
	j.err = notReady // a remount later: warn
	f.tick(t0.Add(linkVerifyPeriod + 2*linkFollowTick))
	if warnings != 1 {
		t.Fatalf("failure after juicefs answered: %d warnings, want 1", warnings)
	}

	j = &fakeJFS{err: notReady} // never answers
	f = newTestFollower(j, &class)
	warnings = 0
	f.warn = func(string, ...any) { warnings++ }
	for now := t0; now.Before(t0.Add(linkStartupGrace)); now = now.Add(linkFollowTick) {
		f.tick(now)
	}
	if warnings != 0 {
		t.Fatalf("%d warnings inside the start-up grace", warnings)
	}
	f.tick(t0.Add(linkStartupGrace))
	if warnings != 1 {
		t.Fatalf("juicefs never answered: %d warnings after the grace, want 1", warnings)
	}
}

// While juicefs reads ahead for a mount, the NFS layer's read-ahead on that
// mount schedules nothing; on another mount, or once juicefs stops, it does.
func TestNFSReadaheadYieldsToJuiceFS(t *testing.T) {
	root := t.TempDir()
	run := func() int64 {
		rm := NewReadaheadManager(root, nil, nil)
		defer rm.Stop()
		for i := 0; i < 10; i++ {
			rm.OnRead(42, int64(i)*readaheadBlockSize, readaheadBlockSize, "f.mov")
		}
		triggered, _ := rm.Stats()
		return triggered
	}
	if run() == 0 {
		t.Fatal("control: no read-ahead on a sequential pattern")
	}
	setJFSReadaheadActive(root, true)
	t.Cleanup(func() { setJFSReadaheadActive(root, false) })
	if got := run(); got != 0 {
		t.Fatalf("juicefs reads ahead: the NFS layer scheduled %d rounds, want 0", got)
	}
	if jfsReadaheadActive(root + "-other") {
		t.Fatal("the flag leaked to another mount")
	}
	setJFSReadaheadActive(root, false)
	if run() == 0 {
		t.Fatal("juicefs stopped: the NFS layer did not resume")
	}
}

// Review H1: read-ahead on for at most readaheadMaxOn at a stretch; then
// off once, and on again only through the hold.
func TestLinkHysteresisReadaheadBoundedOnTime(t *testing.T) {
	var h linkHysteresis
	t0 := time.Unix(1000, 0)
	on := lanPolicy
	on.ReadaheadMiB = 16
	var onAt, offAt, backAt time.Time
	for now := t0; now.Before(t0.Add(2 * readaheadMaxOn)); now = now.Add(linkFollowTick) {
		ra := h.observe(now, on).ReadaheadMiB
		switch {
		case ra > 0 && onAt.IsZero():
			onAt = now
		case ra == 0 && !onAt.IsZero() && offAt.IsZero():
			offAt = now
		case ra > 0 && !offAt.IsZero() && backAt.IsZero():
			backAt = now
		}
	}
	if onAt.IsZero() || offAt.Sub(onAt) != readaheadMaxOn {
		t.Fatalf("on at %s, off at %s: want off %s after on", onAt.Sub(t0), offAt.Sub(t0), readaheadMaxOn)
	}
	if backAt.Sub(offAt) < readaheadHoldFor || backAt.Sub(offAt) > readaheadHoldFor+2*linkFollowTick {
		t.Fatalf("back on %s after the re-check, want after the %s hold", backAt.Sub(offAt), readaheadHoldFor)
	}
}

// Review H1, end to end with the real link estimator: a Mac on fiber (12 ms,
// 40 MiB/s) gets read-ahead; the same tunnel then runs over an unflagged
// hotspot (70 ms, 0.5 MiB/s). While read-ahead is on the estimate cannot
// say metered, so read-ahead must still go off within readaheadMaxOn plus
// the hold and stay off. On fiber, where the estimate reads low only while
// read-ahead is on (as in the lab), read-ahead stays on most of the time.
func TestLinkFollowerReadaheadFiberThenUnflaggedHotspot(t *testing.T) {
	const mib = 1 << 20
	clock := time.Unix(5000, 0)
	p := netprofile.NewWithClock(func() time.Time { return clock })
	j := &fakeJFS{cur: lanPolicy}
	f := &linkFollower{exchange: j.exchange,
		classes:   func() (netprofile.LinkClass, netprofile.LinkClass) { return p.Class(), p.ReadPolicyClass() },
		readahead: p.JuiceFSReadaheadMiB, engineRA: p.SetEngineReadahead}
	// One tick: an RTT sample and one read at the given rate, then the
	// follower. biasedRate is what the estimator sees while read-ahead is on.
	tick := func(rtt time.Duration, rate, biasedRate float64) {
		p.ObserveRTT(rtt)
		if j.cur.ReadaheadMiB > 0 {
			rate = biasedRate
		}
		bytes := int64(max(rate/2, 256<<10))
		dur := time.Duration(float64(bytes) / rate * float64(time.Second))
		clock = clock.Add(dur)
		p.ObserveThroughput(bytes, dur)
		clock = clock.Add(linkFollowTick - dur%linkFollowTick)
		f.tick(clock)
	}
	onTicks, n := 0, 0
	for i := 0; i < 450; i++ { // 15 min on fiber
		tick(12*time.Millisecond, 40*mib, 1*mib)
		if i > 30 {
			n++
			if j.cur.ReadaheadMiB > 0 {
				onTicks++
			}
		}
	}
	if j.cur.ReadaheadMiB == 0 && onTicks == 0 {
		t.Fatal("fiber: read-ahead never came on")
	}
	if share := float64(onTicks) / float64(n); share < 0.85 {
		t.Fatalf("fiber: read-ahead on %.0f%% of the time, want at least 85%% (re-checks only)", share*100)
	}
	// Same tunnel, now an unflagged hotspot: no route or cost change.
	start := clock
	var offAfter time.Duration
	for i := 0; i < 300; i++ { // 10 min
		tick(70*time.Millisecond, 0.5*mib, 0.5*mib)
		if offAfter == 0 && j.cur.ReadaheadMiB == 0 {
			offAfter = clock.Sub(start)
		}
		if offAfter > 0 && j.cur.ReadaheadMiB > 0 {
			t.Fatalf("hotspot: read-ahead came back on %s after the switch", clock.Sub(start))
		}
	}
	t.Logf("fiber: read-ahead on %d of %d ticks; hotspot: off after %s", onTicks, n, offAfter)
	if offAfter == 0 || offAfter > readaheadMaxOn+readaheadHoldFor+2*linkFollowTick {
		t.Fatalf("hotspot: read-ahead off after %s, want within %s", offAfter, readaheadMaxOn+readaheadHoldFor)
	}
	if c := p.ReadPolicyClass(); c != netprofile.ClassMetered {
		t.Fatalf("hotspot, read-ahead off: read class %s, want metered", c)
	}
}
