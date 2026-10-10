package respproxy

// leases.go is the lease table (H8 plan §1.2-1.3) and the founder's
// lifetimes (2026-10-06):
//
//   - an auto lease is held while the Mac is online;
//   - it is released after idle_s (4 h) with no admitted write AND no
//     pending spool rows reported for the project by the last heartbeat;
//   - or after orphan_s (10 min) with no sign of life (no heartbeat and no
//     admitted write);
//   - a checkout survives offline up to checkout_max_s (72 h); only the
//     holder or an admin ends it early;
//   - only an admin breaks a lease. There is no anonymous principal.
//
// Known consequence (for the plan, L4): if the JuiceMount app crashes and
// stops heartbeating, its auto leases end after orphan_s (10 min) even when
// the Mac's spool still holds rows for those projects. If another principal
// writes the project meanwhile, the Mac's drains meet a foreign holder when
// the app restarts (ACQUIRE returns foreign_since=1) and those rows publish
// as conflict copies rather than over the other editor's work. A checkout
// (<= 72 h from its first grant; re-ACQUIRE cannot extend it past that)
// is the way to keep a project across a long offline period.
//
// State lives in Redis (DB = the volume DB; only the proxy writes it):
//
//	jmlease:v1:p:<root>     HASH  holder kind host epoch mode granted_ms
//	                              last_write_ms last_hb_ms checkout_until_ms
//	                              pending label e:<principal>=<last epoch held>
//	jmlease:v1:epoch:<root> STRING INCR on every grant and on every end
//	                              (release, expiry, break). Never reset.
//	jmlease:v1:log          STREAM grant/renew/release/expire/break/deny/would_deny
//
// Expiry is evaluated by the proxy from the hash fields, never by Redis
// TTLs, so state survives a proxy restart. After a restart every auto lease
// gets a fresh orphan window: the Macs could not heartbeat while the proxy
// was down.
//
// Fencing: every project has an RWMutex. A unit holds the read side from
// its decision until its reply (decide.go); grant, release, expiry and
// break take the write side, so no unit decided under the old holder can
// commit after a transfer.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	LeaseKeyPrefix = "jmlease:v1:p:"
	EpochKeyPrefix = "jmlease:v1:epoch:"
	LeaseLogKey    = "jmlease:v1:log"

	defaultIdle        = 4 * time.Hour
	defaultOrphan      = 10 * time.Minute
	defaultCheckoutMax = 72 * time.Hour
	// DirStatGrace: a principal that held a project within this window may
	// still flush its dir-stat rows for it (juicefs flushes every 1 s).
	DirStatGrace = 5 * time.Minute
	leaseLogMax  = 100000
)

const (
	LeaseAuto     = "auto"
	LeaseCheckout = "checkout"
)

type lease struct {
	proj            uint64
	holder          string
	kind            Kind
	host            string
	epoch           uint64
	mode            string
	grantedMs       int64
	lastWriteMs     int64
	lastHbMs        int64
	checkoutUntilMs int64
	pending         bool
	label           string
	hist            map[string]uint64 // principal -> last epoch it held
	recent          map[string]int64  // principal -> ms it stopped holding (dir-stat grace)
	dirty           bool              // touch fields not yet persisted
	unresolved      bool              // transition or reply outcome is not yet authoritative
}

func (l *lease) held() bool { return l.holder != "" }

// liveMs is the last sign of life of the holder.
func (l *lease) liveMs() int64 {
	if l.lastWriteMs > l.lastHbMs {
		return l.lastWriteMs
	}
	return l.lastHbMs
}

// leaseSnap is a copy of a lease for replies and logs.
type leaseSnap struct {
	Proj            uint64
	Holder          string
	Kind            Kind
	Host            string
	Epoch           uint64
	Mode            string
	GrantedMs       int64
	LastWriteMs     int64
	LastHbMs        int64
	CheckoutUntilMs int64
	Pending         bool
	Label           string
}

func (l *lease) snap() leaseSnap {
	return leaseSnap{
		Proj: l.proj, Holder: l.holder, Kind: l.kind, Host: l.host, Epoch: l.epoch, Mode: l.mode,
		GrantedMs: l.grantedMs, LastWriteMs: l.lastWriteMs, LastHbMs: l.lastHbMs,
		CheckoutUntilMs: l.checkoutUntilMs, Pending: l.pending, Label: l.label,
	}
}

// leaseCfg is jmlease:v1:cfg (plan §1.2), re-read by the sweeper.
type leaseCfg struct {
	idle, orphan, checkoutMax time.Duration
	depth                     int
}

func defaultLeaseCfg() leaseCfg {
	return leaseCfg{idle: defaultIdle, orphan: defaultOrphan, checkoutMax: defaultCheckoutMax, depth: 2}
}

func ms(t time.Time) int64 { return t.UnixMilli() }

// expiresMs is when the lease ends by itself if nothing happens (0 = never
// known: an auto lease with pending spool only ends by orphaning).
func (e *LeaseEngine) expiresMs(l *lease) int64 {
	if !l.held() {
		return 0
	}
	if l.mode == LeaseCheckout {
		return l.checkoutUntilMs
	}
	return l.liveMs() + e.cfgNow().orphan.Milliseconds()
}

// foreignSince: another principal held the project after p's last epoch.
func foreignSince(l *lease, principal string) bool {
	mine := l.hist[principal]
	for q, ep := range l.hist {
		if q != principal && ep > mine {
			return true
		}
	}
	return false
}

func (e *LeaseEngine) leaseLocked(proj uint64) *lease {
	l := e.leases[proj]
	if l == nil {
		l = &lease{proj: proj, hist: map[string]uint64{}, recent: map[string]int64{}}
		e.leases[proj] = l
	}
	return l
}

func (e *LeaseEngine) fence(proj uint64) *sync.RWMutex {
	e.mu.Lock()
	defer e.mu.Unlock()
	f := e.fences[proj]
	if f == nil {
		f = &sync.RWMutex{}
		e.fences[proj] = f
	}
	return f
}

// ---------------------------------------------------------------------------
// Persistence.

func leaseKey(proj uint64) string { return LeaseKeyPrefix + strconv.FormatUint(proj, 10) }
func epochKey(proj uint64) string { return EpochKeyPrefix + strconv.FormatUint(proj, 10) }

func (e *LeaseEngine) side(args ...string) (Value, error) {
	v, err := e.cfg.Side.Do(e.cfg.VolumeDB, strArgs(args...)...)
	if err == nil {
		err = v.Err()
	}
	return v, err
}

// leaseTransitionLua validates all expected state before writing. Redis Lua
// runs atomically but does not roll back earlier writes after a script error;
// callers quarantine the project on every uncertain reply. Epochs stay as
// decimal strings: Lua numbers cannot represent every Redis int64 exactly.
const leaseTransitionLua = `
local ct = redis.call('TYPE', KEYS[1]).ok
local ht = redis.call('TYPE', KEYS[2]).ok
if (ct ~= 'none' and ct ~= 'string') or (ht ~= 'none' and ht ~= 'hash') then
  return redis.error_reply('lease key type mismatch')
end
local function valid(s)
  return s and string.match(s, '^[1-9][0-9]*$') and
    (#s < 19 or (#s == 19 and s <= '9223372036854775807'))
end
local counter = redis.call('GET', KEYS[1])
if counter and not valid(counter) then return redis.error_reply('invalid lease counter') end
local hashEpoch = false
local hashHolder = false
if ht == 'hash' then
  hashEpoch = redis.call('HGET', KEYS[2], 'epoch')
  hashHolder = redis.call('HGET', KEYS[2], 'holder')
  if not valid(hashEpoch) or not counter or hashEpoch ~= counter or not hashHolder then
    return redis.error_reply('incoherent lease hash')
  end
end
local op, expectedEpoch, expectedHolder = ARGV[1], ARGV[2], ARGV[3]
if ht == 'hash' then
  if expectedEpoch ~= hashEpoch or expectedHolder ~= hashHolder then
    return redis.error_reply('lease changed')
  end
elseif expectedEpoch ~= '0' or expectedHolder ~= '' then
  return redis.error_reply('lease changed')
end
if op == 'grant' then
  if ht == 'hash' and hashHolder ~= '' then return redis.error_reply('lease held') end
elseif op == 'end' or op == 'renew' then
  if ht ~= 'hash' or hashHolder == '' then return redis.error_reply('lease not held') end
else
  return redis.error_reply('invalid lease transition')
end
if (op == 'grant' or op == 'end') and counter == '9223372036854775807' then
  return redis.error_reply('lease epoch overflow')
end
if op == 'grant' or op == 'end' then
  redis.call('INCR', KEYS[1])
  counter = redis.call('GET', KEYS[1])
end
local fields = {'holder', ARGV[4], 'kind', ARGV[5], 'host', ARGV[6],
  'epoch', counter, 'mode', ARGV[7], 'granted_ms', ARGV[8],
  'last_write_ms', ARGV[9], 'last_hb_ms', ARGV[10],
  'checkout_until_ms', ARGV[11], 'pending', ARGV[12], 'label', ARGV[13]}
if op == 'grant' then
  fields[#fields+1] = 'e:' .. ARGV[4]
  fields[#fields+1] = counter
end
redis.call('HSET', KEYS[2], unpack(fields))
return counter
`

func cloneLease(l *lease) *lease {
	c := *l
	c.hist = make(map[string]uint64, len(l.hist))
	for p, ep := range l.hist {
		c.hist[p] = ep
	}
	c.recent = make(map[string]int64, len(l.recent))
	for p, stopped := range l.recent {
		c.recent[p] = stopped
	}
	return &c
}

func (e *LeaseEngine) transition(op string, old, next *lease) (uint64, error) {
	v, err := e.side("EVAL", leaseTransitionLua, "2", epochKey(old.proj), leaseKey(old.proj),
		op, strconv.FormatUint(old.epoch, 10), old.holder,
		next.holder, string(next.kind), next.host, next.mode,
		strconv.FormatInt(next.grantedMs, 10), strconv.FormatInt(next.lastWriteMs, 10),
		strconv.FormatInt(next.lastHbMs, 10), strconv.FormatInt(next.checkoutUntilMs, 10),
		boolStr(next.pending), next.label)
	if err != nil {
		return 0, err
	}
	if v.Type != '$' || v.Null {
		return 0, errProtocol
	}
	ep, ok := persistedLeaseEpoch(string(v.Str))
	if !ok {
		return 0, errProtocol
	}
	switch op {
	case "renew":
		if ep != old.epoch {
			return 0, errProtocol
		}
	case "grant":
		if old.epoch != 0 && ep != old.epoch+1 {
			return 0, errProtocol
		}
	case "end":
		if ep != old.epoch+1 {
			return 0, errProtocol
		}
	default:
		return 0, errProtocol
	}
	return ep, nil
}

// persist writes the whole record of l (caller holds e.mu or owns l).
func persistArgs(l *lease) []string {
	a := []string{"HSET", leaseKey(l.proj),
		"holder", l.holder, "kind", string(l.kind), "host", l.host,
		"epoch", strconv.FormatUint(l.epoch, 10), "mode", l.mode,
		"granted_ms", strconv.FormatInt(l.grantedMs, 10),
		"last_write_ms", strconv.FormatInt(l.lastWriteMs, 10),
		"last_hb_ms", strconv.FormatInt(l.lastHbMs, 10),
		"checkout_until_ms", strconv.FormatInt(l.checkoutUntilMs, 10),
		"pending", boolStr(l.pending), "label", l.label,
	}
	for p, ep := range l.hist {
		a = append(a, "e:"+p, strconv.FormatUint(ep, 10))
	}
	return a
}

func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// loadLeaseRecord reads the epoch counter and its lease hash in one Redis
// snapshot. A failed INCR-followed-by-HSET can leave the old hash behind;
// loading that hash alone would revive a holder whose tenure has ended.
const loadLeaseRecordLua = `return {redis.call('GET', KEYS[1]), redis.call('HGETALL', KEYS[2])}`

func persistedLeaseEpoch(raw string) (uint64, bool) {
	n, err := strconv.ParseInt(raw, 10, 64) // Redis INCR is signed int64.
	return uint64(n), err == nil && n > 0 && strconv.FormatInt(n, 10) == raw
}

func (e *LeaseEngine) loadLeaseRecord(proj uint64) ([]Value, error) {
	v, err := e.side("EVAL_RO", loadLeaseRecordLua, "2", epochKey(proj), leaseKey(proj))
	if err != nil {
		return nil, err
	}
	if v.Type != '*' || len(v.Elems) != 2 || v.Elems[0].Type != '$' || v.Elems[0].Null ||
		v.Elems[1].Type != '*' || len(v.Elems[1].Elems) == 0 || len(v.Elems[1].Elems)%2 != 0 {
		return nil, fmt.Errorf("lease %d: missing or malformed persisted epoch/hash", proj)
	}
	counter, ok := persistedLeaseEpoch(string(v.Elems[0].Str))
	if !ok {
		return nil, fmt.Errorf("lease %d: invalid persisted epoch counter", proj)
	}
	var hashEpoch uint64
	seen := false
	for i := 0; i < len(v.Elems[1].Elems); i += 2 {
		field, value := v.Elems[1].Elems[i], v.Elems[1].Elems[i+1]
		if field.Type != '$' || field.Null || value.Type != '$' || value.Null {
			return nil, fmt.Errorf("lease %d: malformed persisted hash", proj)
		}
		if string(field.Str) == "epoch" {
			if seen {
				return nil, fmt.Errorf("lease %d: duplicate persisted epoch", proj)
			}
			hashEpoch, ok = persistedLeaseEpoch(string(value.Str))
			if !ok {
				return nil, fmt.Errorf("lease %d: invalid persisted hash epoch", proj)
			}
			seen = true
		}
	}
	if !seen || hashEpoch != counter {
		return nil, fmt.Errorf("lease %d: persisted hash/counter epoch mismatch", proj)
	}
	return v.Elems[1].Elems, nil
}

func (e *LeaseEngine) loadLeases(startMs int64) error {
	cursor := "0"
	candidates := make(map[uint64]*lease)
	for {
		v, err := e.side("SCAN", cursor, "MATCH", LeaseKeyPrefix+"*", "COUNT", "1000")
		if err != nil {
			return err
		}
		if len(v.Elems) != 2 {
			return errProtocol
		}
		cursor = string(v.Elems[0].Str)
		for _, k := range v.Elems[1].Elems {
			key := string(k.Str)
			proj, ok := digits(strings.TrimPrefix(key, LeaseKeyPrefix))
			if !ok || proj == 0 || leaseKey(proj) != key {
				return fmt.Errorf("malformed persisted lease key %q", key)
			}
			h, err := e.loadLeaseRecord(proj)
			if err != nil {
				return err
			}
			l := &lease{proj: proj, hist: map[string]uint64{}, recent: map[string]int64{}}
			for i := 0; i+1 < len(h); i += 2 {
				f, val := string(h[i].Str), string(h[i+1].Str)
				n, _ := strconv.ParseInt(val, 10, 64)
				switch {
				case f == "holder":
					l.holder = val
				case f == "kind":
					l.kind = Kind(val)
				case f == "host":
					l.host = val
				case f == "epoch":
					l.epoch = uint64(n)
				case f == "mode":
					l.mode = val
				case f == "granted_ms":
					l.grantedMs = n
				case f == "last_write_ms":
					l.lastWriteMs = n
				case f == "last_hb_ms":
					l.lastHbMs = n
				case f == "checkout_until_ms":
					l.checkoutUntilMs = n
				case f == "pending":
					l.pending = val == "1"
				case f == "label":
					l.label = val
				case strings.HasPrefix(f, "e:"):
					l.hist[f[2:]] = uint64(n)
				}
			}
			if l.held() && l.lastHbMs < startMs {
				// The proxy was down: nobody could heartbeat.
				l.lastHbMs = startMs
			}
			candidates[proj] = l
		}
		if cursor == "0" {
			e.mu.Lock()
			e.leases = candidates
			e.mu.Unlock()
			return nil
		}
	}
}

// loadCfg reads jmlease:v1:cfg (idle_s, orphan_s, checkout_max_s, depth).
// Missing or bad fields keep their defaults.
func (e *LeaseEngine) loadCfg() {
	v, err := e.side("HGETALL", ConfigKey)
	if err != nil {
		return
	}
	c := defaultLeaseCfg()
	for i := 0; i+1 < len(v.Elems); i += 2 {
		f := string(v.Elems[i].Str)
		n, err := strconv.ParseInt(string(v.Elems[i+1].Str), 10, 64)
		if err != nil || n <= 0 {
			continue
		}
		switch f {
		case "idle_s":
			c.idle = time.Duration(n) * time.Second
		case "orphan_s":
			c.orphan = time.Duration(n) * time.Second
		case "checkout_max_s":
			c.checkoutMax = time.Duration(n) * time.Second
		case "depth":
			if n <= 16 {
				c.depth = int(n)
			}
		}
	}
	e.cfgMu.Lock()
	if e.depthFixed != 0 {
		c.depth = e.depthFixed // depth is fixed for the life of the process
	}
	e.lcfg = c
	e.cfgMu.Unlock()
}

func (e *LeaseEngine) cfgNow() leaseCfg {
	e.cfgMu.RLock()
	defer e.cfgMu.RUnlock()
	return e.lcfg
}

// ---------------------------------------------------------------------------
// Transitions. Every one runs with the project's fence held for writing.

// grantLocked gives proj to p. The caller holds fence(proj).Lock().
func (e *LeaseEngine) grantLocked(p Principal, proj uint64, mode string, ttl time.Duration, why string) (leaseSnap, bool, error) {
	now := e.now()
	e.mu.Lock()
	l := e.leaseLocked(proj)
	if l.unresolved {
		e.mu.Unlock()
		return leaseSnap{}, false, fmt.Errorf("lease %d transition unresolved", proj)
	}
	if l.held() && l.holder != p.Name {
		s := l.snap()
		e.mu.Unlock()
		return s, false, nil
	}
	fs := foreignSince(l, p.Name)
	next := cloneLease(l)
	op, event := "grant", "grant"
	if l.held() { // renew: same holder, same epoch
		op, event = "renew", "renew"
		next.lastHbMs = ms(now)
		if mode == LeaseCheckout {
			// A re-ACQUIRE extends a checkout, but never past checkout_max_s
			// from the tenure's first grant (founder decision: <= 72 h).
			next.mode = LeaseCheckout
			until := ms(now.Add(ttl))
			if limit := l.grantedMs + e.cfgNow().checkoutMax.Milliseconds(); until > limit {
				until = limit
			}
			next.checkoutUntilMs = until
		}
	} else {
		next.holder, next.kind, next.host = p.Name, p.Kind, p.host()
		next.mode = mode
		next.grantedMs, next.lastWriteMs, next.lastHbMs = ms(now), ms(now), ms(now)
		next.checkoutUntilMs = 0
		if mode == LeaseCheckout {
			next.checkoutUntilMs = ms(now.Add(ttl))
		}
		next.pending = false
		if lbl := e.anc.label(proj); lbl != "" {
			next.label = lbl
		}
		delete(next.recent, p.Name)
	}
	l.unresolved = true
	e.mu.Unlock()
	ep, err := e.transition(op, l, next)
	if err != nil {
		return leaseSnap{}, false, err
	}
	next.epoch = ep
	if op == "grant" {
		next.hist[p.Name] = ep
	}
	next.unresolved = false
	s := next.snap()
	e.mu.Lock()
	e.leases[proj] = next
	e.mu.Unlock()
	e.logEvent(event, s, p.Name, why)
	return s, fs, nil
}

// endLocked frees proj (release, expiry, break). The caller holds
// fence(proj).Lock(). It returns the ended lease and the new epoch.
func (e *LeaseEngine) endLocked(proj uint64, event, actor, why string) (leaseSnap, uint64, error) {
	e.mu.Lock()
	l := e.leases[proj]
	if l != nil && l.unresolved {
		e.mu.Unlock()
		return leaseSnap{}, 0, fmt.Errorf("lease %d transition unresolved", proj)
	}
	if l == nil || !l.held() {
		e.mu.Unlock()
		return leaseSnap{}, 0, nil
	}
	old := l.snap()
	next := cloneLease(l)
	next.recent[l.holder] = ms(e.now())
	next.holder, next.kind, next.host, next.mode = "", "", "", ""
	next.checkoutUntilMs, next.pending = 0, false
	next.dirty = false
	l.unresolved = true
	e.mu.Unlock()
	ep, err := e.transition("end", l, next)
	if err != nil {
		return old, 0, err
	}
	next.epoch = ep
	next.unresolved = false
	e.mu.Lock()
	e.leases[proj] = next
	e.mu.Unlock()
	e.logEvent(event, old, actor, why)
	return old, ep, nil
}

// touch records an admitted write by p to projects it holds.
func (e *LeaseEngine) touch(p Principal, projs []uint64) {
	now := ms(e.now())
	e.mu.Lock()
	for _, pr := range projs {
		if l := e.leases[pr]; l != nil && !l.unresolved && l.holder == p.Name {
			l.lastWriteMs = now
			l.dirty = true
		}
	}
	e.mu.Unlock()
}

// heartbeat renews every lease p holds and records which of them have
// pending spool rows. It returns p's leases and the pending projects p no
// longer holds.
func (e *LeaseEngine) heartbeat(p Principal, pending map[uint64]bool) (held []leaseSnap, lost []uint64) {
	now := ms(e.now())
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, l := range e.leases {
		if l.holder != p.Name {
			continue
		}
		if l.unresolved {
			lost = append(lost, l.proj)
			continue
		}
		l.lastHbMs = now
		l.pending = pending[l.proj]
		l.dirty = true
		held = append(held, l.snap())
	}
	for pr := range pending {
		if l := e.leases[pr]; l == nil || l.holder != p.Name {
			lost = append(lost, pr)
		}
	}
	sort.Slice(held, func(i, j int) bool { return held[i].Proj < held[j].Proj })
	sort.Slice(lost, func(i, j int) bool { return lost[i] < lost[j] })
	return held, lost
}

// expiryReason says why l should end now ("" = it stays).
func (e *LeaseEngine) expiryReason(l *lease, now int64, c leaseCfg) string {
	if !l.held() || l.unresolved {
		return ""
	}
	if l.mode == LeaseCheckout {
		if l.checkoutUntilMs > 0 && now >= l.checkoutUntilMs {
			return "checkout ended"
		}
		return ""
	}
	if now-l.liveMs() >= c.orphan.Milliseconds() {
		return "orphaned: no heartbeat or write for " + c.orphan.String()
	}
	if now-l.lastWriteMs >= c.idle.Milliseconds() && !l.pending {
		return "idle " + c.idle.String() + " with no pending spool"
	}
	return ""
}

// sweep ends every lease whose lifetime is over.
func (e *LeaseEngine) sweep() {
	c := e.cfgNow()
	now := ms(e.now())
	var cands []uint64
	e.mu.Lock()
	for pr, l := range e.leases {
		if e.expiryReason(l, now, c) != "" {
			cands = append(cands, pr)
		}
	}
	e.mu.Unlock()
	sort.Slice(cands, func(i, j int) bool { return cands[i] < cands[j] })
	for _, pr := range cands {
		f := e.fence(pr)
		f.Lock()
		e.mu.Lock()
		why := ""
		if l := e.leases[pr]; l != nil {
			why = e.expiryReason(l, ms(e.now()), c)
		}
		e.mu.Unlock()
		if why != "" {
			if _, _, err := e.endLocked(pr, "expire", "-proxy", why); err != nil {
				e.logf("respproxy: lease expire %d: %v", pr, err)
			}
		}
		f.Unlock()
	}
}

// A flush is conditional on the same authoritative tenure. The project fence
// orders it against grant/end/renew; the Redis check protects against a stale
// hash or external mutation. A failed reply quarantines the project because
// the write may have executed.
const leaseFlushLua = `
local c = redis.call('GET', KEYS[1])
local h = redis.call('HGET', KEYS[2], 'epoch')
local holder = redis.call('HGET', KEYS[2], 'holder')
if c ~= ARGV[1] or h ~= ARGV[1] or holder ~= ARGV[2] then
  return redis.error_reply('lease flush tenure changed')
end
redis.call('HSET', KEYS[2], 'last_write_ms', ARGV[3],
  'last_hb_ms', ARGV[4], 'pending', ARGV[5])
return 1
`

func (e *LeaseEngine) flushDirty() {
	e.mu.Lock()
	var projects []uint64
	for pr, l := range e.leases {
		if l.dirty && l.held() && !l.unresolved {
			projects = append(projects, pr)
		}
	}
	e.mu.Unlock()
	sort.Slice(projects, func(i, j int) bool { return projects[i] < projects[j] })
	for _, pr := range projects {
		f := e.fence(pr)
		f.Lock()
		e.mu.Lock()
		l := e.leases[pr]
		if l == nil || !l.dirty || !l.held() || l.unresolved {
			e.mu.Unlock()
			f.Unlock()
			continue
		}
		ep, holder := l.epoch, l.holder
		write, hb, pending := l.lastWriteMs, l.lastHbMs, l.pending
		e.mu.Unlock()
		v, err := e.side("EVAL", leaseFlushLua, "2", epochKey(pr), leaseKey(pr),
			strconv.FormatUint(ep, 10), holder, strconv.FormatInt(write, 10),
			strconv.FormatInt(hb, 10), boolStr(pending))
		if err == nil && (v.Type != ':' || v.Int != 1) {
			err = errProtocol
		}
		e.mu.Lock()
		if err != nil {
			l.unresolved = true
		} else if l.lastWriteMs == write && l.lastHbMs == hb && l.pending == pending {
			l.dirty = false
		}
		e.mu.Unlock()
		f.Unlock()
		if err != nil {
			e.logf("respproxy: lease flush %d unresolved: %v", pr, err)
		}
	}
}

// heldWithin: p holds proj now, or stopped holding it within d.
func (e *LeaseEngine) heldWithin(p Principal, proj uint64, d time.Duration) bool {
	now := ms(e.now())
	e.mu.Lock()
	defer e.mu.Unlock()
	l := e.leases[proj]
	if l == nil || l.unresolved {
		return false
	}
	if l.holder == p.Name {
		return true
	}
	t, ok := l.recent[p.Name]
	return ok && now-t <= d.Milliseconds()
}

// ---------------------------------------------------------------------------
// The audit stream.

type logEntry []string

// logEvent queues one jmlease:v1:log entry (never blocks the caller).
func (e *LeaseEngine) logEvent(event string, s leaseSnap, actor, detail string) {
	ent := logEntry{"event", event, "proj", strconv.FormatUint(s.Proj, 10),
		"actor", actor, "holder", s.Holder, "host", s.Host,
		"epoch", strconv.FormatUint(s.Epoch, 10), "mode", s.Mode,
		"ts_ms", strconv.FormatInt(ms(e.now()), 10)}
	if detail != "" {
		ent = append(ent, "detail", detail)
	}
	e.queueLog(ent)
}

func (e *LeaseEngine) queueLog(ent logEntry) {
	select {
	case e.logCh <- ent:
	default:
		e.logDropped.Add(1)
	}
}

// logDeny logs a refusal (or, in observe mode, a would-be refusal), sampled
// to one per second per principal and project.
func (e *LeaseEngine) logDeny(p Principal, d *Denial, mode Mode, u *Unit) {
	event := "deny"
	if mode == ModeObserve {
		event = "would_deny"
	}
	key := event + "|" + p.Name + "|" + strconv.FormatUint(d.Proj, 10)
	now := ms(e.now())
	e.mu.Lock()
	last, ok := e.denyLast[key]
	if ok && now-last < 1000 {
		e.mu.Unlock()
		return
	}
	if len(e.denyLast) > 100000 {
		e.denyLast = map[string]int64{}
	}
	e.denyLast[key] = now
	e.mu.Unlock()
	e.queueLog(logEntry{"event", event, "reason", string(d.Reason),
		"proj", strconv.FormatUint(d.Proj, 10), "actor", p.Name, "kind", string(p.Kind),
		"holder", d.Holder, "host", d.Host, "epoch", strconv.FormatUint(d.Epoch, 10),
		"unit", summarizeUnit(u), "detail", d.Detail, "ts_ms", strconv.FormatInt(now, 10)})
}

func (e *LeaseEngine) logWriter() {
	defer e.wg.Done()
	for {
		select {
		case ent := <-e.logCh:
			e.writeLog(ent)
		case <-e.stop:
			for {
				select {
				case ent := <-e.logCh:
					e.writeLog(ent)
				default:
					return
				}
			}
		}
	}
}

func (e *LeaseEngine) writeLog(ent logEntry) {
	args := append([]string{"XADD", LeaseLogKey, "MAXLEN", "~", strconv.Itoa(leaseLogMax), "*"}, ent...)
	if _, err := e.side(args...); err != nil {
		e.logf("respproxy: lease log: %v", err)
	}
	if e.cfg.Logf != nil {
		e.cfg.Logf("respproxy: lease %s", strings.Join(ent, " "))
	}
}

func (s leaseSnap) String() string {
	return fmt.Sprintf("proj=%d holder=%s@%s epoch=%d mode=%s", s.Proj, s.Holder, s.Host, s.Epoch, s.Mode)
}
