package respproxy

import (
	"testing"
	"time"
)

// The ledger of quota usage above a person's grants: a claim within the
// reported usage plus the slack lands, one beyond it does not, a claim
// taken back (not committed) frees its room, and an excess left by a lost
// dir-stat flush decays instead of blocking the Mac's flushes for good.
func TestQuotaLedger(t *testing.T) {
	var l quotaLedger
	now := time.Unix(1_000_000, 0)
	const dir = 2
	s := ledgerSlack[0]
	move := func(d int64) []ledgerMove { return []ledgerMove{{dir: dir, idx: 0, delta: d}} }
	if _, bad, _ := l.apply("m", nil, move(s+1), now); bad == nil {
		t.Fatal("a claim beyond the slack with nothing reported landed")
	}
	undo, bad, _ := l.apply("m", move(3*s), move(3*s), now)
	if bad != nil {
		t.Fatalf("a claim of what was reported: refused (balance %+v)", l.m)
	}
	undo()
	undo() // once only
	if _, bad, _ := l.apply("m", nil, move(3*s), now); bad != nil {
		t.Fatal("a claim taken back still counted")
	}
	// Lost credit: s/2 over, then another s/2+1 does not fit until the
	// excess decays (s an hour).
	if _, bad, _ := l.apply("m", nil, move(s/2), now); bad != nil {
		t.Fatal(bad)
	}
	if _, bad, _ := l.apply("m", nil, move(s/2+1), now); bad == nil {
		t.Fatal("past the slack")
	}
	if _, bad, _ := l.apply("m", nil, move(s/2+1), now.Add(time.Hour/2)); bad != nil {
		t.Fatal("the excess did not decay")
	}
	// Another Mac has its own balance; a deficit (credit not yet
	// claimed) does not decay.
	l.apply("other", move(s), nil, now)
	if _, bad, _ := l.apply("other", nil, move(2*s), now.Add(10*time.Hour)); bad != nil {
		t.Fatal("the reported usage of another Mac decayed")
	}
	if satAdd(1<<62, 1<<62) != 1<<63-1 || satAdd(-1<<62, -1<<62-1) != -1<<63 {
		t.Error("satAdd wraps")
	}
}

func TestAccountingValueDenial(t *testing.T) {
	key := func(name string) KeyRef { return ParseKey(name) }
	for _, c := range []struct {
		args []string
		kind Kind
		bad  bool
	}{
		{[]string{"HINCRBY", "dirQuotaUsedSpace", "5", "4096"}, KindMac, false},
		{[]string{"HINCRBY", "dirQuotaUsedSpace", "5", "-4096"}, KindMac, false},
		{[]string{"HINCRBY", "dirQuotaUsedSpace", "5", "+4096"}, KindMac, true},
		{[]string{"HINCRBY", "dirQuotaUsedSpace", "5", "007"}, KindMac, true},
		{[]string{"HINCRBYFLOAT", "dirUsedSpace", "5", "1"}, KindAdmin, true},
		{[]string{"HINCRBYFLOAT", "dirUsedSpace", "5", "1"}, KindBG, false},
		{[]string{"HSET", "dirUsedInodes", "5", "0", "6", "1.5"}, KindMac, true},
		{[]string{"HSET", "dirUsedInodes", "5", "0", "6", "12"}, KindMac, false},
		{[]string{"HSET", "dirQuota", "5", "\x00\x01packed-limits"}, KindAdmin, false},
	} {
		args := make([][]byte, len(c.args))
		for i, a := range c.args {
			args[i] = []byte(a)
		}
		u := &Unit{Cmds: []Cmd{{Args: args, Info: Command{Name: c.args[0], Class: ClassWrite}}},
			Writes: []KeyWrite{{Cmd: 0, Key: key(c.args[1]), Raw: c.args[1]}}}
		if d := accountingValueDenial(Principal{Name: "x", Kind: c.kind}, u); (d != nil) != c.bad {
			t.Errorf("%q by %s: %v", c.args, c.kind, d)
		}
	}
}
