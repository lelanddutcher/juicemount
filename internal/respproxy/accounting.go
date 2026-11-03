package respproxy

// accounting.go: the volume-wide accounting hashes (dir stats and quota
// usage): the values a write may store in them, and how far a person
// limited to grants may move the quota usage of a folder above them.

import (
	"strconv"
	"sync"
	"time"
)

// counterHash: a dir-stat or quota-usage hash. juicefs stores a decimal
// int64 in every field and parses it back with strconv.ParseInt
// (doLoadQuotas, doGetDirStat).
func counterHash(shape string) bool {
	switch shape {
	case "dirDataLength", "dirUsedSpace", "dirUsedInodes", "dirQuotaUsedSpace", "dirQuotaUsedInodes":
		return true
	}
	return false
}

// canonicalInt: s is an int64 as strconv.FormatInt writes it (and as
// go-redis sends juicefs's int64 arguments).
func canonicalInt(s []byte) bool {
	v, err := strconv.ParseInt(string(s), 10, 64)
	return err == nil && strconv.FormatInt(v, 10) == string(s)
}

// accountingValueDenial refuses a write that would leave a value juicefs
// cannot parse in a dir-stat, quota-usage or quota hash: HINCRBYFLOAT on any
// of them, and an HINCRBY delta or an HSET / HSETNX / HMSET value on a
// counter that is not a decimal int64. juicefs never sends such a write
// (pkg/meta/redis.go: doFlushQuotas and doUpdateDirStat send HINCRBY with
// int64 deltas; doSyncDirStat, mknod and HandleQuota HSET int64 values). One
// float in dirQuotaUsedSpace made every new juicefs client fail "Load
// quotas" (no quota was enforced anywhere), and the flush MULTI of every
// running client partially applied and was sent again, counting the other
// quotas' usage several times. It holds for every principal but bg, in
// every mode, with or without people.
func accountingValueDenial(p Principal, u *Unit) *Denial {
	if p.Kind == KindBG {
		return nil
	}
	bad := func(w KeyWrite, what string) *Denial {
		return &Denial{Reason: ReasonBGOnly, Detail: what + " on " + w.Raw + ": juicefs stores only int64 values there"}
	}
	for _, w := range u.Writes {
		if w.Key.Group != "dirstat" && w.Key.Group != "quota" {
			continue
		}
		c := u.Cmds[w.Cmd]
		switch c.Info.Name {
		case "HINCRBYFLOAT":
			return bad(w, "HINCRBYFLOAT")
		case "HINCRBY":
			if len(c.Args) != 4 || !canonicalInt(c.Args[3]) {
				return bad(w, "HINCRBY with a delta that is not an int64")
			}
		case "HSET", "HMSET", "HSETNX":
			if !counterHash(w.Key.Shape) {
				continue // dirQuota holds packed limits (admin only)
			}
			for i := 3; i < len(c.Args); i += 2 {
				if !canonicalInt(c.Args[i]) {
					return bad(w, c.Info.Name+" of a value that is not an int64")
				}
			}
		}
	}
	return nil
}

// The quota-usage increments of a folder above a person's grants.
//
// juicefs counts every write against every quota on the way from the
// written directory to the root (updateDirQuota) and flushes the sums every
// 3 s, one MULTI of HINCRBY per quota folder (doFlushQuotas, which keeps a
// sum that failed and sends it again with the next flush). A Mac whose
// person is limited to grants writes only inside them, so the usage it
// flushes for a folder above them is the sum of its own writes below that
// folder. The same writes reach the dir stats of the written directories,
// inside the grants, with the same space and inode deltas (updateDirStat
// beside every updateDirQuota; flushed every second, doUpdateDirStat).
//
// The proxy keeps that sum per Mac and folder (quotaLedger): the dir-stat
// increments the Mac sends for directories inside its person's grants
// count for every folder above the grants that holds them, and an
// increment of such a folder's quota usage is admitted only while the
// usage the Mac flushed stays within ledgerSlack of what it reported for
// its own directories below. The slack covers the order of the two flushes
// (the quota flush can come up to about a second of writes before the
// dir-stat flush that reports them, or after); a single change larger than
// the slack (removing a file of 100 GB) waits for its dir-stat flush and
// lands with the next quota flush 3 s later. The excess decays at
// ledgerSlack an hour, so dir-stat flushes that were lost (juicefs drops a
// failed one) do not add up. Every admitted increment is in the audit log.
//
// What this bounds: a Mac can move the usage of a folder above its grants
// only by what it reports for its own folders, beyond that by at most the
// slack. A Mac that also forges its own folders' dir stats can move the
// folder above by as much, inflating its own folders' statistics equally;
// both are logged as that person's. The proxy does not recompute usage from
// file sizes. The ledger is in memory: a proxy restart starts it at zero.
type quotaLedger struct {
	mu sync.Mutex
	m  map[ledgerKey]*ledgerBal
}

type ledgerKey struct {
	principal string
	dir       uint64
}

// ledgerBal: per hash (0 space, 1 inodes) the usage flushed for the folder
// less the usage reported for directories below it.
type ledgerBal struct {
	v  [2]int64
	at time.Time // last decay
}

// ledgerSlack: how far the flushed usage may run ahead of, or behind, the
// reported usage: about a second of writes at 64 Gbit/s, and of creates at
// several thousand a second.
var ledgerSlack = [2]int64{8 << 30, 16384}

// maxLedgerEntries bounds the ledger; past it the oldest entries go.
const maxLedgerEntries = 1 << 16

// ledgerIndex maps a dir-stat or quota-usage hash to its ledger slot.
func ledgerIndex(hash string) (int, bool) {
	switch hash {
	case "dirUsedSpace", "dirQuotaUsedSpace":
		return 0, true
	case "dirUsedInodes", "dirQuotaUsedInodes":
		return 1, true
	}
	return 0, false
}

// ledgerMove is one change to a balance.
type ledgerMove struct {
	dir   uint64
	idx   int
	delta int64
}

func (b *ledgerBal) decay(now time.Time) {
	el := now.Sub(b.at)
	if el <= 0 {
		return
	}
	b.at = now
	for i := range b.v {
		// Only an excess decays: a deficit is a dir-stat increment whose
		// quota flush is still to come.
		if b.v[i] <= 0 {
			continue
		}
		d := int64(float64(ledgerSlack[i]) * el.Hours())
		if d >= b.v[i] || d < 0 {
			b.v[i] = 0
		} else {
			b.v[i] -= d
		}
	}
}

func (l *quotaLedger) bal(principal string, dir uint64, now time.Time) *ledgerBal {
	k := ledgerKey{principal, dir}
	b := l.m[k]
	if b == nil {
		if l.m == nil {
			l.m = map[ledgerKey]*ledgerBal{}
		}
		if len(l.m) >= maxLedgerEntries {
			var oldest ledgerKey
			first := true
			for k2, b2 := range l.m {
				if first || b2.at.Before(l.m[oldest].at) {
					oldest, first = k2, false
				}
			}
			delete(l.m, oldest)
		}
		b = &ledgerBal{at: now}
		l.m[k] = b
	}
	b.decay(now)
	return b
}

// apply books credits (dir-stat increments reported below the folders) and
// then claims (quota-usage increments of the folders). It refuses the
// first claim that leaves a balance outside the slack and books nothing of
// the claims then; credits are booked either way (the Mac reported them,
// and juicefs does not send a dir-stat flush twice). undo takes the claims
// back (the unit was not committed: juicefs sends the sum again).
func (l *quotaLedger) apply(principal string, credits, claims []ledgerMove, now time.Time) (undo func(), bad *ledgerMove, balance int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range credits {
		b := l.bal(principal, c.dir, now)
		b.v[c.idx] = satAdd(b.v[c.idx], -c.delta)
	}
	next := map[[2]uint64]int64{}
	for i := range claims {
		c := &claims[i]
		k := [2]uint64{c.dir, uint64(c.idx)}
		v, ok := next[k]
		if !ok {
			v = l.bal(principal, c.dir, now).v[c.idx]
		}
		v = satAdd(v, c.delta)
		if v > ledgerSlack[c.idx] || v < -ledgerSlack[c.idx] {
			return nil, c, v
		}
		next[k] = v
	}
	if len(claims) == 0 {
		return nil, nil, 0
	}
	for _, c := range claims {
		b := l.bal(principal, c.dir, now)
		b.v[c.idx] = satAdd(b.v[c.idx], c.delta)
	}
	once := sync.Once{}
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			for _, c := range claims {
				if b := l.m[ledgerKey{principal, c.dir}]; b != nil {
					b.v[c.idx] = satAdd(b.v[c.idx], -c.delta)
				}
			}
		})
	}, nil, 0
}

// satAdd adds without wrapping around.
func satAdd(a, b int64) int64 {
	s := a + b
	switch {
	case a > 0 && b > 0 && s < 0:
		return 1<<63 - 1
	case a < 0 && b < 0 && s >= 0:
		return -1 << 63
	}
	return s
}
