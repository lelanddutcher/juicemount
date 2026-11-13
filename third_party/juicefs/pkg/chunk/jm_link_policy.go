package chunk

// JuiceMount (patch 0027): the read and object settings that depend on the
// link, switchable while the volume is mounted.
//
// The app picks --demand-first, --get-timeout, --put-timeout and
// --io-retries from the link class when it mounts. A volume mounted on a
// fast link that later moves to a slow one kept the fast settings until the
// next remount: on the founder's laptop (2026-11-12, lan mount, then 200 ms
// and 1% loss) an unpinned clip's first byte took 6-7 s and a 1 MiB scrub
// 5-6 s, against 3.7 s and 1.5 s for a mount made on the slow link. The
// JMLinkPolicy control message (pkg/vfs/jm_link_policy.go) sets them here,
// and every later GET, PUT and read decision uses the new values. Nothing in
// flight is interrupted: a GET keeps the deadline it started with.
//
// --buffer-size, --prefetch, --max-uploads and --small-uploads stay fixed at
// mount: they size pools and goroutines that are made once, and demand-first
// already keeps the prefetcher off the link while a reader waits.

import (
	"sync/atomic"
	"time"
)

// JMLinkPolicy is the set of link settings the control message changes.
type JMLinkPolicy struct {
	DemandFirst bool
	GetTimeout  time.Duration
	PutTimeout  time.Duration
	MaxRetries  int
}

// Bounds of a policy the message may set; outside them it is refused.
const (
	JMLinkTimeoutMin = time.Second
	JMLinkTimeoutMax = 30 * time.Minute
	JMLinkRetriesMax = 50
)

// Valid reports whether p is inside the bounds.
func (p JMLinkPolicy) Valid() bool {
	ok := func(d time.Duration) bool { return d >= JMLinkTimeoutMin && d <= JMLinkTimeoutMax }
	return ok(p.GetTimeout) && ok(p.PutTimeout) && p.MaxRetries >= 0 && p.MaxRetries <= JMLinkRetriesMax
}

type jmLinkPolicy struct {
	getNS   atomic.Int64
	putNS   atomic.Int64
	retries atomic.Int32
	set     atomic.Bool // a JMLinkPolicy message has set the values
}

func (l *jmLinkPolicy) init(c Config) {
	l.getNS.Store(int64(c.GetTimeout))
	l.putNS.Store(int64(c.PutTimeout))
	l.retries.Store(int32(c.MaxRetries))
}

// getTimeout bounds one object GET; set at mount by --get-timeout.
func (store *cachedStore) getTimeout() time.Duration {
	if d := time.Duration(store.link.getNS.Load()); d > 0 {
		return d
	}
	return store.conf.GetTimeout // a store built without NewCachedStore (tests)
}

// putTimeout bounds one object PUT or DELETE; set at mount by --put-timeout.
func (store *cachedStore) putTimeout() time.Duration {
	if d := time.Duration(store.link.putNS.Load()); d > 0 {
		return d
	}
	return store.conf.PutTimeout
}

// maxRetries is --io-retries for a synchronous block upload.
func (store *cachedStore) maxRetries() int {
	if store.link.getNS.Load() > 0 { // initialized
		return int(store.link.retries.Load())
	}
	return store.conf.MaxRetries
}

// JMLinkPolicy reports the settings in force now.
func (store *cachedStore) JMLinkPolicy() JMLinkPolicy {
	return JMLinkPolicy{
		DemandFirst: store.demand.enabled(),
		GetTimeout:  store.getTimeout(),
		PutTimeout:  store.putTimeout(),
		MaxRetries:  store.maxRetries(),
	}
}

// JMSetLinkPolicy applies p and returns the settings in force afterwards. A
// policy outside the bounds changes nothing and returns false.
func (store *cachedStore) JMSetLinkPolicy(p JMLinkPolicy) (JMLinkPolicy, bool) {
	if !p.Valid() {
		return store.JMLinkPolicy(), false
	}
	old := store.JMLinkPolicy()
	store.link.getNS.Store(int64(p.GetTimeout))
	store.link.putNS.Store(int64(p.PutTimeout))
	store.link.retries.Store(int32(p.MaxRetries))
	store.link.set.Store(true)
	if store.demand != nil {
		store.demand.on.Store(p.DemandFirst)
	}
	now := store.JMLinkPolicy()
	if now != old {
		logger.Infof("link policy: demand-first %v -> %v, get-timeout %s -> %s, put-timeout %s -> %s, io-retries %d -> %d",
			old.DemandFirst, now.DemandFirst, old.GetTimeout, now.GetTimeout, old.PutTimeout, now.PutTimeout, old.MaxRetries, now.MaxRetries)
	}
	return now, true
}

// JMLinkOverride reports the policy in force and whether a JMLinkPolicy
// message set it. The vfs reader and writer take their retry budget and
// flush interrupt from it once it is set (pkg/vfs/jm_link_policy.go); until
// then they keep their mount values (meta --io-retries, which differs from
// the store's default of 10 when the flag is 0).
func (store *cachedStore) JMLinkOverride() (JMLinkPolicy, bool) {
	return store.JMLinkPolicy(), store.link.set.Load()
}
