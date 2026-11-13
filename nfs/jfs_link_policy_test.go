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
	if lanPolicy != jfsLinkUpstream {
		t.Fatalf("lan policy %+v, want the upstream defaults", lanPolicy)
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
	reply = append([]byte{0}, encodeLinkPolicy(abroadPolicy)...)
	got, err := c.linkPolicy(&abroadPolicy)
	if err != nil || got != abroadPolicy {
		t.Fatalf("set: %+v %v", got, err)
	}
	if binary.BigEndian.Uint32(sent) != 1106 || binary.BigEndian.Uint32(sent[4:]) != 11 {
		t.Fatalf("header % x", sent[:8])
	}
	wantBody := []byte{1, 1, 0, 0x02, 0xbf, 0x20, 0, 0x04, 0x93, 0xe0, 2} // v1, demand, 180000 ms, 300000 ms, 2
	if !bytes.Equal(sent[8:], wantBody) {
		t.Fatalf("body % x, want % x", sent[8:], wantBody)
	}
	if _, err := c.linkPolicy(nil); err != nil || len(sent) != 9 || sent[8] != 1 {
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
	j := &fakeJFS{cur: lanPolicy} // mounted on lan: no --demand-first
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

// An old juicefs answers EINVAL: the follower asks once and stops, instead of
// one warning in its log every 2 s.
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
	if !f.unsupported || sent != 1 {
		t.Fatalf("unsupported %v after %d messages, want true after 1", f.unsupported, sent)
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
