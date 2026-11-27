package nfs

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Bit 2 of the link policy carries --no-read-compaction (juicefs patch 0036)
// both ways, in both message versions (version 2 adds patch 0035's
// read-ahead after the version 1 body).
func TestJFSLinkPolicyReadCompactionWire(t *testing.T) {
	for _, v := range []byte{jfsLinkPolicyVersion, jfsLinkPolicyVersion2} {
		p := abroadPolicy
		p.NoReadCompaction = true
		p.ReadaheadMiB = -1 // version 1: not known
		if v == jfsLinkPolicyVersion2 {
			p.ReadaheadMiB = 16
		}
		b := encodeLinkPolicy(p, v)
		if b[0] != jfsLinkFlagDemandFirst|jfsLinkFlagNoReadComp {
			t.Fatalf("v%d flags %#x", v, b[0])
		}
		if got := decodeLinkPolicy(b); got != p {
			t.Fatalf("v%d decode %+v, want %+v", v, got, p)
		}
	}
}

// Read compaction and read-ahead hold separately: a link that turns far
// turns read-ahead off after readaheadHoldFor and read compaction off after
// linkSlowerHold, each on its own timer, and neither restarts the other's.
func TestLinkHysteresisReadCompactionAndReadahead(t *testing.T) {
	var h linkHysteresis
	t0 := time.Unix(1000, 0)
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	near := lanPolicy
	near.ReadaheadMiB = 16
	h.observe(at(0), near)
	if got := h.observe(at(readaheadHoldFor.Seconds()), near); got.ReadaheadMiB != 16 || got.NoReadCompaction {
		t.Fatalf("near after the hold: %+v", got)
	}
	far := near
	far.ReadaheadMiB, far.NoReadCompaction = 0, true
	start := readaheadHoldFor.Seconds() + 2
	h.observe(at(start), far)
	got := h.observe(at(start+readaheadHoldFor.Seconds()), far)
	if got.ReadaheadMiB != 0 {
		t.Fatalf("read-ahead still on after its hold: %+v", got)
	}
	if linkSlowerHold > readaheadHoldFor && got.NoReadCompaction {
		t.Fatalf("read compaction off before its own hold: %+v", got)
	}
	got = h.observe(at(start+linkSlowerHold.Seconds()), far)
	if !got.NoReadCompaction || got.ReadaheadMiB != 0 {
		t.Fatalf("far after both holds: %+v", got)
	}
}

// The follower turns read compaction off after linkSlowerHold when the link
// leaves the backend's neighbourhood, and on again after linkFasterHold.
func TestLinkFollowerReadCompactionBothWays(t *testing.T) {
	near := true
	j := &fakeJFS{cur: lanPolicy}
	class := [2]netprofile.LinkClass{netprofile.ClassFast, netprofile.ClassFast}
	f := newTestFollower(j, &class)
	f.readCompaction = func() bool { return near }
	now := time.Unix(1000, 0)
	run := func(d time.Duration) {
		for end := now.Add(d); now.Before(end); now = now.Add(linkFollowTick) {
			f.tick(now)
		}
	}
	run(10 * time.Second)
	if len(j.sets) != 0 {
		t.Fatalf("near: sets %+v", j.sets)
	}

	// Same class, but now through a tunnel: only read compaction changes.
	near = false
	run(linkSlowerHold - 2*linkFollowTick)
	if len(j.sets) != 0 {
		t.Fatalf("switched before the hold: %+v", j.sets)
	}
	run(4 * linkFollowTick)
	want := lanPolicy
	want.NoReadCompaction = true
	if len(j.sets) != 1 || j.sets[0] != want {
		t.Fatalf("far: sets %+v, want one %+v", j.sets, want)
	}

	near = true
	run(linkFasterHold - 2*linkFollowTick)
	if len(j.sets) != 1 {
		t.Fatalf("back on before the hold: %+v", j.sets)
	}
	run(4 * linkFollowTick)
	if len(j.sets) != 2 || j.sets[1] != lanPolicy {
		t.Fatalf("near again: sets %+v", j.sets)
	}
}

// A juicefs without patch 0036 ignores bit 2 and never reports it. The
// follower notices on the first set and stops asking, instead of sending
// the policy again on every tick.
func TestLinkFollowerOldJuiceFSIgnoresReadCompaction(t *testing.T) {
	old := &fakeJFS{cur: lanPolicy}
	exchange := func(set *jfsLinkPolicy) (jfsLinkPolicy, error) {
		if set != nil {
			s := *set
			s.NoReadCompaction = false
			set = &s
		}
		return old.exchange(set)
	}
	class := [2]netprofile.LinkClass{netprofile.ClassSlow, netprofile.ClassMetered}
	f := &linkFollower{exchange: exchange, classes: func() (netprofile.LinkClass, netprofile.LinkClass) {
		return class[0], class[1]
	}, readCompaction: func() bool { return false }}
	t0 := time.Unix(1000, 0)
	for i := 0; i < 300; i++ { // 10 min
		f.tick(t0.Add(time.Duration(i) * linkFollowTick))
	}
	if !f.noReadCompUnsupported || len(old.sets) != 1 || old.cur != abroadPolicy {
		t.Fatalf("unsupported %v, sets %+v", f.noReadCompUnsupported, old.sets)
	}
}
