package metadata

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// KEYSPACE PUSH: IS IT ACTUALLY WORKING?
//
// This question has been unanswerable from the running process, which is why it
// keeps getting re-litigated. rc.engaged records the state the code INTENDED to
// be in; nothing counted whether a single event ever arrived. The 2026-07-10
// incident — notify-keyspace-events unset on the NAS, so push could never engage
// and the app fell back to a permanent 30s SCAN over the tunnel — was diagnosed
// by reading logs, not by asking.
//
// ── THE VALIDITY GATE, AND WHY IT IS NOT OPTIONAL ───────────────────────────
//
// "Zero events received" has two completely different meanings:
//
//	nothing changed on the volume  -> zero events is CORRECT
//	push is broken                 -> zero events is a FAILURE
//
// A status that printed "0 events" without distinguishing them would be exactly
// the harness this project has been burned by: a plausible number produced from
// no measurement. So the discriminator is our OWN writes.
//
// juicemount:metadata replays events for writes THIS client made — the publisher
// carries no origin field, so we receive our own mutations back. That is
// normally a trap (assuming remote-only cost an xattr-loss regression), but here
// it is the signal: if we published N mutations and received ZERO events, push
// is broken. If we published none, the answer is UNKNOWN and this refuses to
// render a verdict.

// KeyspaceVerdict is the answer to "is push working", including the honest
// refusal.
type KeyspaceVerdict string

const (
	// KeyspaceWorking — events observed. Push is delivering.
	KeyspaceWorking KeyspaceVerdict = "working"
	// KeyspaceBroken — we published mutations and received nothing.
	KeyspaceBroken KeyspaceVerdict = "broken"
	// KeyspaceUnknown — nothing happened to observe. NOT a pass and NOT a
	// failure; the check simply did not measure anything.
	KeyspaceUnknown KeyspaceVerdict = "unknown"
	// KeyspaceUnreachable — could not talk to Redis at all.
	KeyspaceUnreachable KeyspaceVerdict = "unreachable"
)

// KeyspaceStatus is a point-in-time answer about push health.
type KeyspaceStatus struct {
	Verdict KeyspaceVerdict `json:"verdict"`
	// Reason is human-readable and always populated — especially for unknown,
	// where the whole point is explaining what was NOT measured.
	Reason string `json:"reason"`

	// ConfigFlags is the live notify-keyspace-events value. Empty means unset,
	// which is the 2026-07-10 failure exactly.
	ConfigFlags string `json:"config_flags"`
	// ConfigSufficient reports whether those flags can carry what we subscribe to.
	ConfigSufficient bool `json:"config_sufficient"`

	// Engagement is the state the consumer believes it is in.
	Engagement string `json:"engagement"`

	// EventsApplied counts keyspace events applied to the mirror since boot.
	EventsApplied int64 `json:"events_applied"`
	// PublishedMutations counts mutations THIS client published since boot. The
	// validity denominator: without any of these, zero events proves nothing.
	PublishedMutations int64 `json:"published_mutations"`
	// SecondsSinceLastEvent is -1 when no event has ever arrived.
	SecondsSinceLastEvent float64 `json:"seconds_since_last_event"`
}

// Package-level counters. Package-level rather than per-client because the
// question is about the process's push health, and a status call should not need
// to find the one live RedisClient to answer it.
var (
	keyspaceEventsApplied  atomic.Int64
	keyspacePublished      atomic.Int64
	keyspaceLastEventNanos atomic.Int64
)

// noteKeyspaceEventApplied records one event applied to the mirror.
func noteKeyspaceEventApplied() {
	keyspaceEventsApplied.Add(1)
	keyspaceLastEventNanos.Store(time.Now().UnixNano())
}

// noteKeyspacePublished records one mutation this client published. This is the
// validity denominator — see the block comment.
func noteKeyspacePublished() { keyspacePublished.Add(1) }

// KeyspaceCounters exposes the raw counts (metrics, tests).
func KeyspaceCounters() (applied, published int64) {
	return keyspaceEventsApplied.Load(), keyspacePublished.Load()
}

// Status answers whether keyspace push is delivering, and REFUSES to answer when
// it has not measured anything.
func (rc *RedisClient) KeyspaceStatus(ctx context.Context) KeyspaceStatus {
	st := KeyspaceStatus{
		EventsApplied:         keyspaceEventsApplied.Load(),
		PublishedMutations:    keyspacePublished.Load(),
		SecondsSinceLastEvent: -1,
	}
	if last := keyspaceLastEventNanos.Load(); last > 0 {
		st.SecondsSinceLastEvent = time.Since(time.Unix(0, last)).Seconds()
	}
	st.Engagement = keyspaceEngagement(rc.engaged.Load()).String()

	sufficient, flags := rc.probeKeyspaceConfig(ctx)
	st.ConfigFlags = flags
	st.ConfigSufficient = sufficient

	v := keyspaceVerdictFor(st.EventsApplied, st.PublishedMutations, !(flags == "" && !sufficient))
	st.Verdict, st.Reason = v.Verdict, v.Reason
	if st.Verdict == KeyspaceWorking {
		st.Reason = fmt.Sprintf("%d events applied; last %.1fs ago",
			st.EventsApplied, st.SecondsSinceLastEvent)
	}
	return st
}

// keyspaceVerdictFor is the decision, split out so the validity gate is testable
// without a live Redis — the gate is the part that must not silently regress
// into reporting a pass it did not earn.
func keyspaceVerdictFor(eventsApplied, published int64, configReadable bool) KeyspaceStatus {
	st := KeyspaceStatus{EventsApplied: eventsApplied, PublishedMutations: published}
	switch {
	case !configReadable:
		// Either Redis is unreachable or the flag is genuinely unset. Both are
		// worth surfacing loudly; the config probe cannot tell them apart, so say
		// so rather than guessing.
		st.Verdict = KeyspaceUnreachable
		st.Reason = "could not read notify-keyspace-events (Redis unreachable, or the " +
			"flag is unset — the 2026-07-10 failure). Push cannot be confirmed."
	case eventsApplied > 0:
		st.Verdict = KeyspaceWorking
		st.Reason = fmt.Sprintf("%d events applied", eventsApplied)
	case published > 0:
		// We changed things and heard nothing back. Our own writes are replayed
		// to us, so this IS a failure rather than an absence of traffic.
		st.Verdict = KeyspaceBroken
		st.Reason = fmt.Sprintf("published %d mutations and received ZERO events — "+
			"this client's own writes are replayed to it, so silence here means push "+
			"is not delivering", published)
	default:
		// THE REFUSAL. Nothing was published and nothing arrived, so there was
		// nothing to observe. Reporting "working" here would be a number from no
		// measurement.
		st.Verdict = KeyspaceUnknown
		st.Reason = "no events received AND no mutations published — nothing happened " +
			"to observe. This is NOT a pass: write a file and re-check."
	}
	return st
}

// Register the keyspace verdict with /metrics at package init.
//
// From an init() rather than a wiring site, for the reason this sprint has kept
// re-learning: a status that nothing calls is indistinguishable from no status
// at all, and every forgotten Set*Provider found here was a mechanism that was
// correct and unreachable. An init cannot be forgotten.
//
// Reports the COUNTER-DERIVED verdict only — no Redis round trip on a scrape.
// The config-flag half needs a live client and is answered by
// RedisClient.KeyspaceStatus; the counters alone already separate "delivering"
// from "silent while we published", which is the question Leland actually asked.
func init() {
	metrics.Default().SetKeyspaceProvider(func() *metrics.KeyspaceSnapshot {
		applied, published := KeyspaceCounters()
		v := keyspaceVerdictFor(applied, published, true)
		return &metrics.KeyspaceSnapshot{
			Verdict:            string(v.Verdict),
			Reason:             v.Reason,
			EventsApplied:      applied,
			PublishedMutations: published,
		}
	})
}

// keyspaceShrinkReconciled counts events that would have SHRUNK a cached entry
// and were sent for reconcile instead of applied.
//
// Worth counting because the failure it guards is invisible: applying a
// reordered shrink produces short reads with no error anywhere. A rising count
// means push is delivering out of order often enough to matter.
var keyspaceShrinkReconciled atomic.Int64

func noteKeyspaceShrinkReconciled() { keyspaceShrinkReconciled.Add(1) }

// KeyspaceShrinkReconciles reports the count (metrics, tests).
func KeyspaceShrinkReconciles() int64 { return keyspaceShrinkReconciled.Load() }
