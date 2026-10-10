package respproxy

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Brute-force throttle, keyed by (client IP, username): many principals can
// share one IP behind NAT or a Tailscale relay, and one principal's typos
// must not lock the others out (review H8B-L1 #8). After throttleFree
// failures inside throttleWindow the pair is locked out for
// 2^(n-throttleFree) seconds, capped at throttleMaxLock. While locked out
// every AUTH for that pair fails without checking the password, so a correct
// guess cannot be told from a wrong one.
//
// Unknown usernames share ONE global budget (unknownFree per
// throttleWindow): beyond it, every unknown-user attempt is refused without
// hashing, so name spraying cannot buy argon2 CPU or fill the failure table.
const (
	throttleFree    = 5
	throttleWindow  = 10 * time.Minute
	throttleMaxLock = 15 * time.Minute
	unknownFree     = 50
	verifyCacheMax  = 4096
	maxFailRecords  = 100000
	// argon2 is memory-hard; bound concurrent verifications so a flood of
	// AUTHs cannot exhaust the NAS's RAM.
	maxConcurrentVerify = 4
)

type failRec struct {
	n           int
	first       time.Time
	lockedUntil time.Time
}

// Authenticator checks AUTH credentials against the principal table.
type Authenticator struct {
	table atomic.Pointer[Principals]
	dummy atomic.Pointer[argon2Hash] // burned for unknown users: same cost as a real check
	Now   func() time.Time

	secret [32]byte // keys the verified-credential cache
	sem    chan struct{}

	// verifications counts argon2 evaluations (tests prove unknown users
	// pay one too).
	verifications atomic.Int64

	mu           sync.Mutex
	epoch        uint64                 // distinguishes A -> B -> the same A pointer
	fails        map[string]*failRec    // "ip\x00user"
	unknown      failRec                // global budget for unknown usernames
	verified     map[[32]byte]string    // H(secret, user, pass) -> principal name
	sessions     map[*conn]principalRec // authenticated sockets owned by this authenticator
	beforeCommit func()                 // deterministic AUTH-vs-Swap test seam; nil in production
}

// authTicket identifies the immutable table and record that an AUTH checked.
// Binding it under mu prevents an old Argon2 result from surviving a reload.
type authTicket struct {
	table *Principals
	epoch uint64
	rec   principalRec
}

// NewAuthenticator returns an authenticator over ps.
func NewAuthenticator(ps *Principals) *Authenticator {
	a := &Authenticator{
		Now:      time.Now,
		sem:      make(chan struct{}, maxConcurrentVerify),
		fails:    map[string]*failRec{},
		verified: map[[32]byte]string{},
		sessions: map[*conn]principalRec{},
	}
	rand.Read(a.secret[:])
	a.Swap(ps)
	return a
}

// Swap installs a new principal table (SIGHUP reload) and forgets every
// cached verification. The dummy hash copies the costliest parameters in
// the table, so an unknown name costs what a real one does.
func (a *Authenticator) Swap(ps *Principals) {
	if ps == nil {
		ps = &Principals{byName: map[string]principalRec{}}
	}
	d := argon2Hash{time: DefaultArgonTime, memory: DefaultArgonMemory, threads: DefaultArgonThreads, key: make([]byte, argonKeyLen)}
	first := true
	for _, r := range ps.byName {
		if first || uint64(r.hash.time)*uint64(r.hash.memory) > uint64(d.time)*uint64(d.memory) {
			d.time, d.memory, d.threads = r.hash.time, r.hash.memory, r.hash.threads
			d.key = make([]byte, len(r.hash.key))
			first = false
		}
	}
	d.salt = make([]byte, argonSaltLen)
	rand.Read(d.salt)
	a.mu.Lock()
	a.epoch++
	a.dummy.Store(&d)
	a.table.Store(ps)
	a.verified = map[[32]byte]string{}
	var revoked []*conn
	for c, old := range a.sessions {
		if c.revoked.Load() {
			// A concurrent Swap may still be retiring this transport.
			// Re-close it before this Swap returns; Unbind owns removal.
			revoked = append(revoked, c)
			continue
		}
		fresh, exists := ps.byName[old.p.Name]
		if !exists || !samePrincipalRecord(old, fresh) {
			// The mark and every Bind are ordered under mu. Once marked, a
			// socket cannot re-AUTH into a successor after its close is queued.
			c.revoked.Store(true)
			revoked = append(revoked, c)
		}
	}
	a.mu.Unlock()
	// Closing local transports wakes blocked reads and pub/sub streams. No
	// authenticator lock is held over Close, dial, Redis or policy work.
	// Work admitted before this Swap may already be in Redis or a policy
	// call and cannot be rolled back; later commands cannot use this socket.
	for _, c := range revoked {
		c.revokeTransport()
	}
}

func samePrincipalRecord(a, b principalRec) bool {
	return a.p == b.p && a.hash.time == b.hash.time &&
		a.hash.memory == b.hash.memory && a.hash.threads == b.hash.threads &&
		bytes.Equal(a.hash.salt, b.hash.salt) && bytes.Equal(a.hash.key, b.hash.key)
}

// bind registers the exact successful AUTH result. A replaced table, or a
// connection already marked revoked, requires a fresh AUTH on a fresh socket.
func (a *Authenticator) bind(c *conn, ticket authTicket) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	c.transportMu.Lock()
	defer c.transportMu.Unlock()
	if c.inactive || c.revoked.Load() || a.epoch != ticket.epoch || a.table.Load() != ticket.table ||
		(c.authOwner != nil && c.authOwner != a) {
		return false
	}
	pr := ticket.rec.p
	c.principal = &pr // command goroutine only
	c.authOwner = a
	a.sessions[c] = ticket.rec
	return true
}

func (a *Authenticator) unbind(c *conn) {
	a.mu.Lock()
	c.transportMu.Lock()
	c.inactive = true // the handle cannot re-register after cleanup begins
	c.transportMu.Unlock()
	delete(a.sessions, c)
	a.mu.Unlock()
}

// AuthResult is the outcome of one AUTH attempt.
type AuthResult int

const (
	AuthOK AuthResult = iota
	AuthBad
	AuthThrottled
)

func clientIP(remote string) string {
	if h, _, err := net.SplitHostPort(remote); err == nil {
		return h
	}
	return remote
}

// Authenticate checks user/pass for a client at remote (host:port).
func (a *Authenticator) Authenticate(remote, user, pass string) (Principal, AuthResult) {
	ticket, result := a.authenticateTicket(remote, user, pass)
	return ticket.rec.p, result
}

func (a *Authenticator) authenticateTicket(remote, user, pass string) (authTicket, AuthResult) {
	key := clientIP(remote) + "\x00" + user
	now := a.Now()
	a.mu.Lock()
	ps := a.table.Load()
	epoch := a.epoch
	rec, known := ps.byName[user]
	ticket := authTicket{table: ps, epoch: epoch, rec: rec}
	if known {
		if f := a.fails[key]; f != nil && now.Before(f.lockedUntil) {
			// An attempt during lockout counts as a failure and extends it.
			f.n++
			f.lockedUntil = now.Add(lockFor(f.n))
			a.mu.Unlock()
			return authTicket{}, AuthThrottled
		}
	} else {
		if now.Sub(a.unknown.first) > throttleWindow {
			a.unknown = failRec{first: now}
		}
		if a.unknown.n >= unknownFree {
			a.unknown.n++
			a.mu.Unlock()
			return authTicket{}, AuthThrottled
		}
	}
	a.mu.Unlock()

	ck := a.cacheKey(user, pass)
	a.mu.Lock()
	name, cached := a.verified[ck]
	a.mu.Unlock()
	ok := false
	if cached && known && name == user {
		ok = true
	} else {
		a.sem <- struct{}{}
		if known {
			ok = a.verify(rec.hash, pass)
		} else {
			a.verify(*a.dummy.Load(), pass)
		}
		<-a.sem
	}

	if a.beforeCommit != nil {
		a.beforeCommit()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.epoch != epoch || a.table.Load() != ps {
		// Swap won while Argon2 (or a cached result) was in flight.
		return authTicket{}, AuthBad
	}
	if ok {
		delete(a.fails, key)
		if len(a.verified) >= verifyCacheMax {
			a.verified = map[[32]byte]string{}
		}
		a.verified[ck] = user
		return ticket, AuthOK
	}
	if !known {
		a.unknown.n++
		return authTicket{}, AuthBad
	}
	f := a.fails[key]
	if f == nil || now.Sub(f.first) > throttleWindow {
		if f == nil && len(a.fails) >= maxFailRecords {
			for k, v := range a.fails {
				if now.After(v.lockedUntil) && now.Sub(v.first) > throttleWindow {
					delete(a.fails, k)
				}
			}
		}
		f = &failRec{first: now}
		a.fails[key] = f
	}
	f.n++
	if f.n >= throttleFree {
		f.lockedUntil = now.Add(lockFor(f.n))
	}
	return authTicket{}, AuthBad
}

func (a *Authenticator) verify(h argon2Hash, pass string) bool {
	a.verifications.Add(1)
	return h.verify(pass)
}

// lockFor is the lockout after n failures (n >= throttleFree): 1 s, doubling,
// capped at throttleMaxLock.
func lockFor(n int) time.Duration {
	lock := time.Second << uint(min(max(n-throttleFree, 0), 10))
	if lock > throttleMaxLock {
		lock = throttleMaxLock
	}
	return lock
}

func (a *Authenticator) cacheKey(user, pass string) [32]byte {
	h := sha256.New()
	h.Write(a.secret[:])
	h.Write([]byte(user))
	h.Write([]byte{0})
	h.Write([]byte(pass))
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
