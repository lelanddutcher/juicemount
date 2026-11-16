package metadata

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// ============================================================================
// One content change per interval while a teammate's save runs (finding R1,
// laptop runs 2026-11-14 and 2026-11-16).
//
// THE PROBLEM. A teammate's long save commits a part every 2-4 s. The attr
// push publishes each part: the mirror row's mtime (or, for a late commit
// under the save's final mtime, the reported mtime: mtime_span.go) moves,
// and the content hook refreshes juicefs's caches. The macOS NFS client
// drops a file's pages on every mtime it has not seen, including pages it
// fetched a moment ago, and reads them again. Abroad one READ round takes
// 2-6 s, so one player read can need 3-4 rounds: 12-17 s freezes, ~20 % of
// READs repeated. A save dated before its close (cp -p, an app setting the
// mtime) made 82-88 refreshes per session.
//
// THE FIX. The attr push publishes another client's content change of a
// file at once when it published none for that inode in the last interval
// (peerRefreshInterval: by the measured round trip, 1 s up to 30 ms, 3 s up
// to 100 ms, 10 s above; by the class before the first sample). A
// later change within the interval is held: nothing is written, no
// observation is recorded, no hook runs, and the inode is read again when
// the interval ends. That read compares Redis with the row and observation
// the mirror last PUBLISHED, so every change held meanwhile (new mtimes, new
// sizes, late commits) is seen as one change and published once, through
// the same path as before. A save therefore shows the Mac its first part at
// once, at most one more change per interval while it runs, and one after
// its last part (at most one interval plus the push's own latency after
// it).
//
// P2 (new attributes never come with old bytes). Unchanged: the gate only
// decides WHEN a publication runs, never how. Every publication still runs
// the content hook before the row changes, and a READ whose reply carries
// the new attributes waits for a refresh that started after that hook
// (nfs/content_refresh.go, "Ordering"). Between two publications the bytes
// can be newer than the attributes (the Mac may cache new bytes under the
// previous mtime); the next publication reports an mtime the Mac has never
// seen (mtime_span.go never repeats one), so it drops them. A held change is
// never lost: its re-read goes through the client-lifetime deferral queue
// (owned_deferral.go), which survives a resubscribe, and a re-read that
// comes early is held again and re-armed.
//
// Only another client's changes that do not shorten the file are held (a
// row with the longer size would send READs past the new end, which the
// premature-EOF guard answers with EIO). The Mac's own bytes (exact own
// marks) and every other publication path (folder reconcile, SCAN,
// juicemount:metadata) are not gated. JM_PEER_REFRESH_INTERVAL sets the
// interval (a Go duration; 0 turns the gate off), capped at
// peerRefreshIntervalMax so the final bytes show within about 30 s.
//
// KNOWN GAP (review M2 of #102). A late commit (only the ctime changed) can
// be published only by the attr push: a SCAN reads mtime and size. Three
// paths already drop one: a restart (the gate, the armed re-read and the
// observations are in memory), a batch over attrBurstCeiling (promoted to a
// SCAN) and attrMaxRetries used up. A held late commit waits up to one
// interval, so those paths stay open up to 10 s instead of one push window
// (0.5-2 s). After a restart the next i-key event of the file publishes it
// (no observation: a late commit). Persisting the held set would close it.
// ============================================================================

const (
	// peerRefreshIntervalMax caps JM_PEER_REFRESH_INTERVAL.
	peerRefreshIntervalMax = 20 * time.Second
	// peerRefreshSlack lets a re-read that a shared deferral timer fires a
	// moment early through instead of holding it for one more round.
	peerRefreshSlack = 250 * time.Millisecond
	// peerGatePruneAt: past this many inodes, published() forgets the ones
	// whose interval has long ended.
	peerGatePruneAt = 4096
)

var attrPushHeld atomic.Int64 // changes held by the gate (a re-read was armed)

// The interval follows the measured round trip to the backend, not the
// bandwidth class: R1 costs one READ round per mtime change, and the round
// trip decides how long a round takes. The class is capped at slow whenever
// the backend route goes through a tunnel (utun), so a Mac at home reaching
// the NAS over Tailscale at a few ms would get the far-link interval
// (review M3 of #102).
const (
	peerRTTNear      = 30 * time.Millisecond  // at or under: peerIntervalNear
	peerRTTMid       = 100 * time.Millisecond // at or under: peerIntervalMid
	peerIntervalNear = time.Second
	peerIntervalMid  = 3 * time.Second
	peerIntervalFar  = 10 * time.Second
)

// peerIntervalInputs is what one interval choice was made from (logged).
type peerIntervalInputs struct {
	interval time.Duration
	source   string // env | rtt | class
	rtt      time.Duration
	class    netprofile.LinkClass
}

// peerIntervalFor picks the interval: JM_PEER_REFRESH_INTERVAL when set, else
// the smoothed RTT when one was measured, else the class (before the first
// RTT sample: slow or metered far, medium mid, fast near).
func peerIntervalFor(env string, snap netprofile.Snapshot) peerIntervalInputs {
	in := peerIntervalInputs{rtt: snap.RTT, class: snap.Class}
	if env != "" {
		if d, err := time.ParseDuration(env); err == nil && d >= 0 {
			if d > peerRefreshIntervalMax {
				d = peerRefreshIntervalMax
			}
			in.interval, in.source = d, "env"
			return in
		}
	}
	if snap.HaveRTT && snap.RTT > 0 {
		in.source = "rtt"
		switch {
		case snap.RTT <= peerRTTNear:
			in.interval = peerIntervalNear
		case snap.RTT <= peerRTTMid:
			in.interval = peerIntervalMid
		default:
			in.interval = peerIntervalFar
		}
		return in
	}
	in.source = "class"
	switch snap.Class {
	case netprofile.ClassSlow, netprofile.ClassMetered:
		in.interval = peerIntervalFar
	case netprofile.ClassMedium:
		in.interval = peerIntervalMid
	default:
		in.interval = peerIntervalNear
	}
	return in
}

// peerIntervalLogged is the last interval and source logged.
var peerIntervalLogged struct {
	mu       sync.Mutex
	interval time.Duration
	source   string
	done     bool
}

// peerRefreshInterval is the shortest time between two publications of
// another client's content changes of one file. A change of the interval or
// of its source is logged once, with its inputs.
func peerRefreshInterval() time.Duration {
	in := peerIntervalFor(os.Getenv("JM_PEER_REFRESH_INTERVAL"), netprofile.Default().Snapshot())
	l := &peerIntervalLogged
	l.mu.Lock()
	changed := !l.done || l.interval != in.interval || l.source != in.source
	l.interval, l.source, l.done = in.interval, in.source, true
	l.mu.Unlock()
	if changed {
		jmlog.Info("metadata attr push: teammate change interval", "interval_ms", in.interval.Milliseconds(),
			"source", in.source, "rtt_ms", in.rtt.Milliseconds(), "class", in.class.String())
	}
	return in.interval
}

// peerRefreshGate keeps, per inode, when another client's change was last
// published, and which inodes have a re-read armed.
type peerRefreshGate struct {
	mu        sync.Mutex
	last      map[uint64]time.Time
	armed     map[uint64]struct{}
	lastPrune time.Time
}

// hold reports whether a change of inode must wait, and until when.
func (g *peerRefreshGate) hold(inode uint64, now time.Time, interval time.Duration) (time.Time, bool) {
	if interval <= 0 {
		return time.Time{}, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	t, ok := g.last[inode]
	if !ok || now.Sub(t) >= interval-peerRefreshSlack {
		return time.Time{}, false
	}
	return t.Add(interval), true
}

// arm reports whether the caller must arm inode's re-read (none is armed).
func (g *peerRefreshGate) arm(inode uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.armed == nil {
		g.armed = make(map[uint64]struct{})
	}
	if _, ok := g.armed[inode]; ok {
		return false
	}
	g.armed[inode] = struct{}{}
	return true
}

func (g *peerRefreshGate) disarm(inode uint64) {
	g.mu.Lock()
	delete(g.armed, inode)
	g.mu.Unlock()
}

// published records that another client's change of inode was published.
func (g *peerRefreshGate) published(inode uint64, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.last == nil {
		g.last = make(map[uint64]time.Time)
	}
	g.last[inode] = now
	// At most one walk per second (review L3 of #102): a burst of more than
	// peerGatePruneAt recent inodes would otherwise walk the map on every
	// publication and delete nothing.
	if len(g.last) > peerGatePruneAt && now.Sub(g.lastPrune) >= time.Second {
		g.lastPrune = now
		for i, t := range g.last {
			if now.Sub(t) > peerRefreshIntervalMax {
				delete(g.last, i)
			}
		}
	}
}

// The gate's clock, interval and timer. A test sets them per client
// (RedisClient.testPeerGate*), so every other test keeps the gate on.
func (rc *RedisClient) peerGateNow() time.Time {
	if rc.testPeerGateNow != nil {
		return rc.testPeerGateNow()
	}
	return time.Now()
}

func (rc *RedisClient) peerGateInterval() time.Duration {
	if rc.testPeerGateInterval != nil {
		return rc.testPeerGateInterval()
	}
	return peerRefreshInterval()
}

func (rc *RedisClient) peerGateAfter(d time.Duration, f func()) {
	if rc.testPeerGateAfter != nil {
		rc.testPeerGateAfter(d, f)
		return
	}
	time.AfterFunc(d, f)
}

// holdPeerChanges splits the rows the attr push is about to publish: the
// Mac's own bytes and the first change of an inode in its interval go on;
// another client's later change is held, and its inode is read again when
// the interval ends (see the comment above).
//
// rowSize is the size the mirror row has now (the attr push read the row
// already; review L4 of #102).
func (rc *RedisClient) holdPeerChanges(rows []*Entry, rowSize map[uint64]int64) []*Entry {
	if len(rows) == 0 {
		return rows
	}
	interval := rc.peerGateInterval()
	if interval <= 0 {
		return rows
	}
	now := rc.peerGateNow()
	kept := rows[:0:0]
	for _, e := range rows {
		if e.span != nil && e.span.own {
			kept = append(kept, e)
			continue
		}
		// A shorter file publishes at once: a row that kept the longer size
		// would send READs past the new end, which the premature-EOF guard
		// answers with EIO.
		if size, ok := rowSize[e.Inode]; !ok || e.Size < size {
			kept = append(kept, e)
			continue
		}
		due, held := rc.peerGate.hold(e.Inode, now, interval)
		if !held {
			kept = append(kept, e)
			continue
		}
		if !rc.peerGate.arm(e.Inode) {
			continue // its re-read is armed already
		}
		attrPushHeld.Add(1)
		jmlog.Info("metadata attr push: holding a teammate's change until its interval ends", "path", e.Path,
			"inode", e.Inode, "due_ms", due.Sub(now).Milliseconds(), "interval_ms", interval.Milliseconds(),
			"late_commit", e.span != nil && e.span.lateCommit)
		inode := e.Inode
		// The deferral queue adds attrRetryBase before the re-read.
		wait := due.Sub(now) - attrRetryBase
		if wait < 0 {
			wait = 0
		}
		rc.peerGateAfter(wait, func() {
			rc.peerGate.disarm(inode)
			rc.deferOwnedFile(inode)
		})
	}
	return kept
}

// notePeerPublished records the publication of rows that were not the Mac's
// own bytes.
func (rc *RedisClient) notePeerPublished(e *Entry) {
	if e.span != nil && e.span.own {
		return
	}
	rc.peerGate.published(e.Inode, rc.peerGateNow())
}
