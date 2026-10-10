package respproxy

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Proxy-owned keys in the volume DB (plan §1.2).
const (
	SingletonKey = "jmlease:v1:proxy" // SET NX PX 10000, renewed every 3 s
	ConfigKey    = "jmlease:v1:cfg"   // HASH; field "mode" overrides Config.Mode
)

const (
	singletonTTL   = 10 * time.Second
	singletonRenew = 3 * time.Second
	sideTimeout    = 5 * time.Second
)

// Config configures a Proxy.
//
// Upstream ACL (Redis 7): the jm-proxy user needs `on >pw ~* &* +@all`.
// The `&*` (allchannels) is REQUIRED: Redis 7 defaults new users to
// resetchannels (acl-pubsub-default), so `+@all ~*` alone makes every
// SUBSCRIBE/PSUBSCRIBE fail with -NOPERM and the keyspace push and
// juicemount:metadata consumers never receive a message.
type Config struct {
	Upstream         string // Redis host:port
	UpstreamUser     string // Redis ACL user the proxy authenticates as (jm-proxy); empty = none
	UpstreamPassword string
	Principals       *Principals
	Mode             Mode  // initial mode; jmlease:v1:cfg "mode" overrides it while set
	VolumeDB         int   // DB holding jmlease:* (the JuiceFS volume DB); default 1
	AllowedDBs       []int // DBs a non-admin principal may SELECT; default [VolumeDB]
	Singleton        bool  // hold jmlease:v1:proxy; refuse to start if another proxy does
	ModePoll         time.Duration
	// StageBirth explicitly enables custody receipts for recoverable stages.
	// It requires enforcing mode and singleton ownership; default false.
	StageBirth bool
	// SingletonTTL / SingletonRenew default to 10 s / 3 s (plan §1.2).
	SingletonTTL   time.Duration
	SingletonRenew time.Duration
	// PreAuthTimeout bounds how long a connection may stay
	// unauthenticated; default 10 s.
	PreAuthTimeout time.Duration
	// MaxConns caps client connections (default 4096).
	//
	// MaxPreAuthPerIP (default 16) bounds how many connections from one IP
	// may be AUTHENTICATING at once; further ones WAIT (up to
	// PreAuthTimeout) in a queue of MaxPreAuthQueuePerIP (default 1024)
	// and are refused only when that queue is full. A queue, not a
	// refusal: a juicefs mount or several containers behind one NAT IP open
	// their whole go-redis pool at once (default pool 10 x GOMAXPROCS), and
	// juicefs with io-retries=0 does not retry a refused dial (review
	// H8B-L1c #6: a burst of 64 lost 38). AUTH of a cached credential takes
	// microseconds, so a waiting connection is served almost at once; the
	// argon2 cost per unknown credential still bounds a flood.
	MaxConns             int
	MaxPreAuthPerIP      int
	MaxPreAuthQueuePerIP int
	// MaxPreAuthTotal (default 256) caps unauthenticated connections,
	// waiting or authenticating, across ALL IPs, so a few IPs cannot hold
	// the global MaxConns with idle unauthenticated sockets (review
	// H8B-L1d #7). 256 covers every server container plus a studio of Macs
	// reconnecting at once; each holds at most PreAuthTimeout.
	MaxPreAuthTotal int
	// MaxPendingBytes / MaxPendingReplies cap one connection's queue of
	// replies the client has not read yet (defaults 64 MiB / 1,000,000). A
	// client over either is disconnected (review H8B-L1b #4: a non-reading
	// client held 385 MB). Each slot is costed at slotCost plus the size of
	// a proxy-made reply; forwarded replies wait in Redis's output buffer.
	MaxPendingBytes   int64
	MaxPendingReplies int
	Policy            Policy // nil = AllowProjectWrites (L1)
	// BeforeSideClose retires caller-owned dependencies after all admitted
	// clients/background work join, while the side pool is still usable.
	// It runs once and must not wait for this proxy's Close or Done.
	BeforeSideClose func()
	Logf            func(format string, args ...any)
}

// Proxy is the enforcing RESP proxy. One upstream connection per client.
type Proxy struct {
	cfg   Config
	Logf  func(format string, args ...any)
	Stats *Stats
	Auth  *Authenticator
	Guard *Guard

	stageBirth atomic.Bool // initialized from Config; creation only
	policy     Policy
	mode       atomic.Value // Mode
	side       *SidePool

	mu           sync.Mutex
	preAuth      map[string]*ipGate // client IP -> unauthenticated connections
	preAuthTotal int                // unauthenticated connections, all IPs
	ln           net.Listener
	conns        map[net.Conn]struct{}
	wg           sync.WaitGroup
	closed       atomic.Bool
	stop         chan struct{}
	closeDone    chan struct{}
	connID       atomic.Uint64

	incarnation   string
	lastRenew     atomic.Pointer[time.Time] // request start, retaining its monotonic clock
	singletonLost atomic.Bool               // irreversible for this proxy instance
	started       atomic.Bool
	bgwg          sync.WaitGroup
}

// New builds a proxy. Call Start (singleton + mode poll), then Serve.
func New(cfg Config) (*Proxy, error) {
	if cfg.Upstream == "" {
		return nil, errors.New("respproxy: no upstream")
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeEnforce
	}
	if _, ok := ParseMode(string(cfg.Mode)); !ok {
		return nil, fmt.Errorf("respproxy: bad mode %q", cfg.Mode)
	}
	if cfg.StageBirth && (cfg.Mode != ModeEnforce || !cfg.Singleton) {
		return nil, errors.New("respproxy: stage_birth requires enforce mode and singleton ownership")
	}
	if cfg.ModePoll == 0 {
		cfg.ModePoll = 2 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	if cfg.Policy == nil {
		cfg.Policy = AllowProjectWrites{}
	}
	if cfg.SingletonTTL == 0 {
		cfg.SingletonTTL = singletonTTL
	}
	if cfg.SingletonRenew == 0 {
		cfg.SingletonRenew = singletonRenew
	}
	if cfg.PreAuthTimeout == 0 {
		cfg.PreAuthTimeout = preAuthTimeout
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = 4096
	}
	if cfg.MaxPreAuthPerIP == 0 {
		cfg.MaxPreAuthPerIP = 16
	}
	if cfg.MaxPreAuthQueuePerIP == 0 {
		cfg.MaxPreAuthQueuePerIP = 1024
	}
	if cfg.MaxPreAuthTotal == 0 {
		cfg.MaxPreAuthTotal = 256
	}
	if cfg.MaxPendingBytes == 0 {
		cfg.MaxPendingBytes = 64 << 20
	}
	if cfg.MaxPendingReplies == 0 {
		cfg.MaxPendingReplies = 1_000_000
	}
	if len(cfg.AllowedDBs) == 0 {
		cfg.AllowedDBs = []int{cfg.VolumeDB}
	}
	st := NewStats()
	st.Logf = cfg.Logf
	p := &Proxy{
		cfg:       cfg,
		Logf:      cfg.Logf,
		Stats:     st,
		Auth:      NewAuthenticator(cfg.Principals),
		policy:    cfg.Policy,
		conns:     map[net.Conn]struct{}{},
		preAuth:   map[string]*ipGate{},
		stop:      make(chan struct{}),
		closeDone: make(chan struct{}),
	}
	p.side = &SidePool{dial: p.dialUpstream, max: 8}
	p.Guard = NewGuard(&redisSessions{p: p, cache: map[uint64]string{}}, sideChunks{p})
	p.Guard.slices = &redisSlices{p: p}
	p.Guard.counters = sideCounters{p}
	p.stageBirth.Store(cfg.StageBirth)
	p.setMode(cfg.Mode, "config")
	return p, nil
}

// Side is the proxy-owned upstream pool for side reads (L2's ancestry and
// lease reads). Never use a client's connection for these.
func (p *Proxy) Side() *SidePool { return p.side }

// SetPolicy replaces the policy. Call before Serve.
func (p *Proxy) SetPolicy(pol Policy) { p.policy = pol }

// Mode is the current enforcement mode.
func (p *Proxy) Mode() Mode { return p.mode.Load().(Mode) }

func (p *Proxy) setMode(m Mode, source string) {
	old, _ := p.mode.Load().(Mode)
	if old == m {
		return
	}
	p.mode.Store(m)
	p.Stats.setMode(m)
	p.Logf("respproxy: mode %s -> %s (%s)", old, m, source)
}

// Incarnation is this proxy's singleton value ("" when not a singleton).
func (p *Proxy) Incarnation() string { return p.incarnation }

// Start acquires the singleton key (if configured) and starts the mode poll.
func (p *Proxy) Start() error {
	if p.closed.Load() || p.started.Swap(true) {
		return errors.New("respproxy: Start requires a fresh proxy instance")
	}
	if p.cfg.Singleton {
		host, _ := os.Hostname()
		var nonce [8]byte
		rand.Read(nonce[:])
		p.incarnation = fmt.Sprintf("%s:%d:%x:%d", host, os.Getpid(), nonce, time.Now().UnixMilli())
		start := time.Now()
		v, err := p.side.Do(p.cfg.VolumeDB, strArgs("SET", SingletonKey, p.incarnation, "NX", "PX", strconv.FormatInt(p.cfg.SingletonTTL.Milliseconds(), 10))...)
		if err != nil {
			return fmt.Errorf("respproxy: singleton: %w", err)
		}
		if v.Null {
			cur, _ := p.side.Do(p.cfg.VolumeDB, strArgs("GET", SingletonKey)...)
			return fmt.Errorf("respproxy: another proxy holds %s (incarnation %q); refusing to start", SingletonKey, cur.Str)
		}
		if err := v.Err(); err != nil {
			return fmt.Errorf("respproxy: singleton: %w", err)
		}
		if v.Type != '+' || string(v.Str) != "OK" {
			return fmt.Errorf("respproxy: singleton: %w", errProtocol)
		}
		p.lastRenew.Store(&start)
		if p.fenced() {
			if p.cfg.StageBirth {
				_ = p.Close()
			}
			return errors.New("respproxy: singleton acquisition exceeded ownership proof lifetime")
		}
		if !p.cfg.StageBirth {
			p.bgwg.Add(1)
			go p.renewLoop()
		}
	}
	p.pollMode()
	if p.cfg.StageBirth {
		// No background owner exists yet. Refuse an effective mode override
		// or a slow poll that exhausted the acquired ownership proof.
		if p.Mode() != ModeEnforce || p.fenced() {
			_ = p.Close()
			return errors.New("respproxy: stage_birth startup requires current enforcing singleton ownership")
		}
		p.bgwg.Add(1)
		go p.renewLoop()
	}
	p.bgwg.Add(1)
	go func() {
		defer p.bgwg.Done()
		t := time.NewTicker(p.cfg.ModePoll)
		defer t.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-t.C:
				p.pollMode()
			}
		}
	}()
	return nil
}

func (p *Proxy) pollMode() {
	v, err := p.side.Do(p.cfg.VolumeDB, strArgs("HGET", ConfigKey, "mode")...)
	if err != nil || v.Err() != nil || v.Null {
		return
	}
	m, ok := ParseMode(string(v.Str))
	if !ok {
		p.Logf("respproxy: ignoring bad %s mode %q", ConfigKey, v.Str)
		return
	}
	p.setMode(m, ConfigKey)
}

// renew returns 1 if renewed, 0 if the key is gone, -1 if another
// incarnation holds it.
const renewLua = `local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then redis.call('PEXPIRE', KEYS[1], ARGV[2]) return 1 end
if v == false then return 0 end
return -1`

const releaseLua = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`

// fenced refuses new write and lease admissions once ownership proof expires.
// Losing that proof is irreversible: a later successful renewal cannot make
// this instance's old in-memory leases authoritative again. A fresh proxy must
// acquire a new identity and reload coherent persisted state.
// This admission check does not revoke work already forwarded to Redis.
func (p *Proxy) fenced() bool {
	if !p.cfg.Singleton {
		return false
	}
	if p.singletonLost.Load() || p.closed.Load() {
		return true
	}
	last := p.lastRenew.Load()
	if last == nil {
		return true // Start has not established ownership yet.
	}
	// Include the complete RPC duration and use monotonic elapsed time.
	if time.Since(*last) >= p.cfg.SingletonTTL-p.cfg.SingletonTTL/5 {
		p.singletonLost.Store(true)
		return true
	}
	return false
}

func (p *Proxy) renewLoop() {
	defer p.bgwg.Done()
	t := time.NewTicker(p.cfg.SingletonRenew)
	defer t.Stop()
	ttl := strconv.FormatInt(p.cfg.SingletonTTL.Milliseconds(), 10)
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
		}
		if p.fenced() {
			break
		}
		start := time.Now()
		v, err := p.side.Do(p.cfg.VolumeDB, strArgs("EVAL", renewLua, "1", SingletonKey, p.incarnation, ttl)...)
		// Check the OLD proof before publishing a renewal. A delayed reply
		// cannot bridge an interval in which ownership was unproven.
		if p.fenced() {
			break
		}
		if err != nil || v.Err() != nil {
			p.Logf("respproxy: singleton renew failed: %v %v", err, v.Err())
			continue
		}
		if v.Type != ':' || v.Int != 1 {
			// Missing, foreign, or malformed is not permission to reacquire
			// the key with this identity and its old in-memory state.
			p.singletonLost.Store(true)
			break
		}
		p.lastRenew.Store(&start)
	}
	p.Logf("respproxy: singleton %s ownership proof lost; shutting down (fresh instance required)", SingletonKey)
	// Publish the fence before asynchronous Close retires the transports.
	p.singletonLost.Store(true)
	go p.Close()
}

// Serve accepts clients on ln until Close.
func (p *Proxy) Serve(ln net.Listener) error {
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	if p.closed.Load() {
		ln.Close()
		return nil
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			if p.closed.Load() {
				return nil
			}
			return err
		}
		if int(p.Stats.conns.Load()) >= p.cfg.MaxConns {
			p.Stats.connRefused.Add(1)
			c.Close()
			continue
		}
		ip := clientIP(c.RemoteAddr().String())
		p.mu.Lock()
		if p.closed.Load() {
			p.mu.Unlock()
			c.Close()
			return nil
		}
		gate := p.preAuth[ip]
		if gate == nil {
			gate = &ipGate{sem: make(chan struct{}, p.cfg.MaxPreAuthPerIP)}
			p.preAuth[ip] = gate
		}
		over := gate.queued >= p.cfg.MaxPreAuthQueuePerIP || p.preAuthTotal >= p.cfg.MaxPreAuthTotal
		if !over {
			gate.queued++
			p.preAuthTotal++
			p.Stats.conns.Add(1)
			p.wg.Add(1) // Admission and Close's snapshot share p.mu.
		} else if gate.queued == 0 {
			delete(p.preAuth, ip)
		}
		p.mu.Unlock()
		if over {
			p.Stats.connRefused.Add(1)
			c.Close()
			continue
		}
		go func() {
			defer p.wg.Done()
			p.handle(c, ip)
		}()
	}
}

// Closing reports admission shutdown; it is not completion.
func (p *Proxy) Closing() bool { return p.closed.Load() }

// Done closes only after the original Close finishes dependency retirement,
// singleton release and side-pool shutdown. Concurrent Close calls may return
// before that work is complete; callers that own the process must join Done.
func (p *Proxy) Done() <-chan struct{} { return p.closeDone }

// Close stops accepting, closes every connection, releases the singleton.
func (p *Proxy) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	defer close(p.closeDone)
	close(p.stop)
	var err error
	p.mu.Lock()
	if p.ln != nil {
		err = p.ln.Close()
	}
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
	p.bgwg.Wait()
	if p.cfg.BeforeSideClose != nil {
		p.cfg.BeforeSideClose()
	}
	if p.incarnation != "" {
		p.side.Do(p.cfg.VolumeDB, strArgs("EVAL", releaseLua, "1", SingletonKey, p.incarnation)...)
	}
	p.side.Close()
	return err
}

func (p *Proxy) track(c net.Conn, add bool) {
	p.mu.Lock()
	late := add && p.closed.Load()
	if add && !late {
		p.conns[c] = struct{}{}
	} else {
		delete(p.conns, c)
	}
	p.mu.Unlock()
	if late {
		_ = c.Close()
	}
}

// dialUpstream opens an authenticated upstream connection.
func (p *Proxy) dialUpstream() (net.Conn, *bufio.Reader, error) {
	up, err := net.DialTimeout("tcp", p.cfg.Upstream, sideTimeout)
	if err != nil {
		return nil, nil, err
	}
	if tc, ok := up.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	ur := bufio.NewReaderSize(up, 64<<10)
	if p.cfg.UpstreamPassword != "" {
		args := strArgs("AUTH", p.cfg.UpstreamPassword)
		if p.cfg.UpstreamUser != "" {
			args = strArgs("AUTH", p.cfg.UpstreamUser, p.cfg.UpstreamPassword)
		}
		up.SetDeadline(time.Now().Add(sideTimeout))
		w := bufio.NewWriter(up)
		WriteCommand(w, args)
		if err := w.Flush(); err != nil {
			up.Close()
			return nil, nil, err
		}
		v, err := ReadValue(ur)
		if err == nil {
			err = v.Err()
		}
		if err != nil {
			up.Close()
			return nil, nil, fmt.Errorf("upstream AUTH: %w", err)
		}
		up.SetDeadline(time.Time{})
	}
	return up, ur, nil
}

// ---------------------------------------------------------------------------
// Per-connection state machine.
//
// Before AUTH a connection has no upstream: commands are read under
// PreAuthLimits with a read deadline and answered by the command goroutine
// itself. After AUTH the proxy dials Redis and starts the reply goroutine,
// which is then the ONLY writer to the client. The command goroutine queues
// one slot per expected reply (forwarded command, proxy-made reply, or a
// proxy-issued command whose reply is dropped) and never blocks on the
// client: if the client stops reading, only the reply goroutine waits, and
// Redis buffers the rest, exactly as a direct connection would (review
// H8B-L1 #3).

// slot is one pending reply position on a connection.
type slot struct {
	synth     []byte        // a proxy-made reply (refusal, AUTH result, ...); nil otherwise
	drop      bool          // proxy-issued command: read and discard its reply
	exec      bool          // the reply is an EXEC reply
	execCount int           // protected EXEC: exact expected results, all must succeed
	strip     int           // EXEC: drop this many trailing results (proxy-appended commands)
	onExec    func(Value)   // EXEC: sees the parsed results (before stripping)
	toRaw     bool          // RESP2 SUBSCRIBE: the stream becomes pub/sub
	sub       string        // RESP3 subscribe family: confirmation kind
	subN      int           // confirmations still expected; -1 = every channel of the kind
	hook      func(Value)   // sees the parsed reply before the client does
	done      func(Outcome) // Verdict.Done
}

type txnState struct {
	cmds   []Cmd
	poison string // why the proxy cannot honour this transaction
}

type conn struct {
	p      *Proxy
	id     uint64
	remote string
	client net.Conn
	cr     *bufio.Reader
	cw     *bufio.Writer
	cs     *connStats

	up net.Conn
	ur *bufio.Reader
	uw *bufio.Writer
	// transportMu protects publication of up against a concurrent Swap
	// closing a revoked session. Never hold it across dial or network I/O.
	transportMu sync.Mutex
	inactive    bool // transportMu: shutdown or handle cleanup has begun
	revoked     atomic.Bool

	mu  sync.Mutex // guards q, fwd and raw only; never held across I/O
	q   []slot
	fwd int // slots in q that wait for an upstream frame (not synth)
	raw bool

	dead     chan struct{}
	deadOnce sync.Once
	replyEnd chan struct{}

	// reply goroutine only
	subs    map[string]map[string]bool
	teeBuf  bytes.Buffer
	discard *bufio.Writer

	qBytes   int64 // estimated bytes held by q (guarded by mu)
	overflow bool  // q exceeded its caps (guarded by mu)

	// command goroutine only
	preAuthIP   string
	preAuthSlot bool
	principal   *Principal
	authOwner   *Authenticator // fixed at first successful AUTH; handle defer unbinds
	resp3       bool
	subscribed  bool // RESP2 pub/sub: only (P|S)(UN)SUBSCRIBE, PING, QUIT
	db          int
	txn         *txnState
	watched     []string
	watchAck    chan struct{} // closed when the last WATCH's reply was read
}

var errClose = errors.New("respproxy: closing connection")

// preAuthTimeout bounds how long a connection may stay unauthenticated.
const preAuthTimeout = 10 * time.Second

// selectTimeout bounds the wait for a SELECT reply.
const selectTimeout = 10 * time.Second

// ipGate admits MaxPreAuthPerIP authenticating connections per IP at once
// (sem) and counts every unauthenticated connection, waiting or active
// (queued, guarded by Proxy.mu).
type ipGate struct {
	sem    chan struct{}
	queued int
}

// enterPreAuth waits for the IP's authentication slot. False: timed out or
// the proxy is closing.
func (c *conn) enterPreAuth() bool {
	gate := c.gate()
	t := time.NewTimer(c.p.cfg.PreAuthTimeout)
	defer t.Stop()
	select {
	case gate.sem <- struct{}{}:
		c.preAuthSlot = true
		return true
	case <-t.C:
		c.p.Stats.connRefused.Add(1)
		return false
	case <-c.p.stop:
		return false
	}
}

func (c *conn) gate() *ipGate {
	c.p.mu.Lock()
	defer c.p.mu.Unlock()
	return c.p.preAuth[c.preAuthIP]
}

// leavePreAuth releases the connection's slot and queue place (once).
func (c *conn) leavePreAuth() {
	if c.preAuthIP == "" {
		return
	}
	c.p.mu.Lock()
	g := c.p.preAuth[c.preAuthIP]
	if c.preAuthSlot {
		<-g.sem
		c.preAuthSlot = false
	}
	c.p.preAuthTotal--
	if g.queued--; g.queued <= 0 {
		delete(c.p.preAuth, c.preAuthIP)
	}
	c.p.mu.Unlock()
	c.preAuthIP = ""
}

func (p *Proxy) handle(client net.Conn, ip string) {
	if tc, ok := client.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	c := &conn{
		p: p, id: p.connID.Add(1), remote: client.RemoteAddr().String(),
		client:    client,
		cr:        bufio.NewReaderSize(client, 16<<10),
		cw:        bufio.NewWriterSize(client, 64<<10),
		dead:      make(chan struct{}),
		discard:   bufio.NewWriter(io.Discard),
		subs:      map[string]map[string]bool{},
		db:        0,
		preAuthIP: ip,
	}
	p.track(client, true)
	c.cs = p.Stats.newConnStats()
	defer func() {
		c.leavePreAuth()
		c.shutdown() // close first; Swap must not miss an unclosed retired socket
		if c.authOwner != nil {
			c.authOwner.unbind(c)
		}
		if c.replyEnd != nil {
			<-c.replyEnd
		}
		p.track(client, false)
		if c.up != nil {
			p.track(c.up, false)
		}
		p.Stats.closeConnStats(c.cs)
		p.Stats.conns.Add(-1)
		// Any unit still waiting for a reply is lost: report it.
		c.mu.Lock()
		q := c.q
		c.q = nil
		c.mu.Unlock()
		for _, s := range q {
			if s.done != nil {
				s.done(Outcome{Forwarded: true, Lost: true, ConnID: c.id})
			}
		}
	}()
	if p.closed.Load() {
		return
	}
	if !c.enterPreAuth() || !c.preAuth() {
		return
	}
	c.commandLoop()
	c.flushUpstream()
}

func (c *conn) shutdown() {
	c.deadOnce.Do(func() {
		c.transportMu.Lock()
		c.inactive = true
		up := c.up
		close(c.dead)
		c.transportMu.Unlock()
		c.client.Close()
		if up != nil {
			up.Close()
		}
	})
}

// Swap calls this after marking revoked. Closing both transports wakes idle,
// blocked and pub/sub sessions without waiting for a Redis or policy call.
func (c *conn) revokeTransport() { c.shutdown() }

// preAuth serves the connection until it authenticates. It returns false if
// the connection should close.
func (c *conn) preAuth() bool {
	c.client.SetReadDeadline(time.Now().Add(c.p.cfg.PreAuthTimeout))
	for c.principal == nil {
		args, err := ReadCommandLimits(c.cr, PreAuthLimits)
		if err != nil {
			if errors.Is(err, errProtocol) {
				c.p.Stats.protoErrors.Add(1)
				c.cw.WriteString("-ERR Protocol error: unauthenticated request too large\r\n")
				c.cw.Flush()
			}
			return false
		}
		if len(args) == 0 {
			continue
		}
		info := Classify(args)
		c.cs.command(info, "preauth")
		var ok bool
		switch info.Name {
		case "AUTH":
			ok = c.auth(args)
		case "HELLO":
			ok = c.hello(args)
		case "QUIT":
			c.reply([]byte("+OK\r\n"))
			return false
		default:
			c.p.Stats.unauthRefused.Add(1)
			c.reply((&Denial{Reason: ReasonAuthRequired}).Reply())
			continue
		}
		if !ok {
			return false
		}
	}
	c.client.SetReadDeadline(time.Time{})
	c.leavePreAuth()
	return c.flushUpstream() == nil // the SETNAME/HELLO that completes the login
}

// startUpstream dials Redis (authenticated as the proxy) and starts the
// reply goroutine. Called once, on the first successful AUTH.
func (c *conn) startUpstream() error {
	up, ur, err := c.p.dialUpstream()
	if err != nil {
		return err
	}
	c.p.track(up, true)
	c.transportMu.Lock()
	if c.inactive || c.revoked.Load() {
		c.transportMu.Unlock()
		c.p.track(up, false)
		up.Close()
		return errClose
	}
	c.up, c.ur = up, ur
	c.uw = bufio.NewWriterSize(up, 64<<10)
	c.replyEnd = make(chan struct{})
	c.transportMu.Unlock()
	go func() {
		defer close(c.replyEnd)
		defer c.shutdown()
		c.replyLoop()
	}()
	return nil
}

// reply puts a proxy-made reply into the next client slot. Only the reply
// goroutine writes to the client, and it wakes only on upstream data; so
// when no forwarded reply is pending, the proxy sends its own PING (reply
// dropped) ahead of the synthetic reply to wake it in order. That costs one
// Redis round trip per refusal on an idle connection, and nothing on the
// hot path.
func (c *conn) reply(b []byte) {
	c.p.Stats.synthetic.Add(1)
	if c.up == nil { // pre-auth: no reply goroutine yet
		c.cw.Write(b)
		c.cw.Flush()
		return
	}
	c.mu.Lock()
	wake := c.fwd == 0
	if wake {
		c.q = append(c.q, slot{drop: true})
		c.fwd++
		c.account(slotCost)
	}
	c.q = append(c.q, slot{synth: b})
	c.account(slotCost + int64(len(b)))
	c.mu.Unlock()
	if wake {
		WriteCommand(c.uw, pingArgs)
	}
}

var pingArgs = strArgs("PING")

// forward queues a reply slot and writes args upstream (buffered).
func (c *conn) forward(args [][]byte, s slot) error {
	if c.revoked.Load() {
		if s.done != nil {
			s.done(Outcome{ConnID: c.id})
		}
		return errClose
	}
	c.mu.Lock()
	c.q = append(c.q, s)
	c.fwd++
	c.account(slotCost)
	over := c.overflow
	c.mu.Unlock()
	if over {
		return errOverflow
	}
	if c.revoked.Load() {
		return errClose // handle defer retires the queued slot as lost
	}
	return WriteCommand(c.uw, args)
}

func (c *conn) flushUpstream() error {
	if c.revoked.Load() {
		return errClose
	}
	return c.uw.Flush()
}

// slotCost is the estimated memory of one queued reply slot.
const slotCost = 96

var errOverflow = errors.New("respproxy: client is not reading its replies; pending-reply cap exceeded")

// account adds n bytes to the queue estimate (mu held). Negative n releases.
func (c *conn) account(n int64) {
	c.qBytes += n
	if n > 0 && (c.qBytes > c.p.cfg.MaxPendingBytes || len(c.q) > c.p.cfg.MaxPendingReplies) {
		c.overflow = true
	}
}

func (c *conn) overflowed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.overflow
}

// writeNoSlot writes a command whose replies are raw pub/sub messages
// (RESP2 subscribed mode).
func (c *conn) writeNoSlot(args [][]byte) error {
	if c.revoked.Load() {
		return errClose
	}
	return WriteCommand(c.uw, args)
}

func (c *conn) replyLoop() {
	for {
		if err := c.frame(); err != nil {
			if !isClosed(err) {
				c.p.Stats.protoErrors.Add(1)
				c.p.Logf("respproxy: conn %d: %v", c.id, err)
			}
			c.cw.Flush()
			return
		}
		if err := c.writeSynth(false); err != nil {
			return
		}
		if c.ur.Buffered() == 0 {
			if err := c.cw.Flush(); err != nil {
				return
			}
		}
	}
}

// writeSynth writes the proxy-made replies at the head of the queue.
func (c *conn) writeSynth(flush bool) error {
	wrote := false
	for {
		c.mu.Lock()
		if len(c.q) == 0 || c.q[0].synth == nil {
			c.mu.Unlock()
			break
		}
		b := c.q[0].synth
		c.q = c.q[1:]
		c.account(-(slotCost + int64(len(b))))
		c.mu.Unlock()
		if _, err := c.cw.Write(b); err != nil {
			return err
		}
		wrote = true
	}
	if wrote && flush {
		return c.cw.Flush()
	}
	return nil
}

var subKinds = map[string]string{
	"subscribe": "subscribe", "psubscribe": "psubscribe", "ssubscribe": "ssubscribe",
	"unsubscribe": "subscribe", "punsubscribe": "psubscribe", "sunsubscribe": "ssubscribe",
}

// tee copies one frame into c.teeBuf and parses it.
func (c *conn) tee() (FrameInfo, Value, error) {
	c.teeBuf.Reset()
	w := bufio.NewWriter(&c.teeBuf)
	info, err := CopyFrame(c.ur, w)
	if err != nil {
		return info, Value{}, err
	}
	w.Flush()
	v, err := ReadValue(bufio.NewReader(bytes.NewReader(c.teeBuf.Bytes())))
	return info, v, err
}

// frame relays one upstream frame to its slot.
func (c *conn) frame() error {
	b, err := c.ur.Peek(1)
	if err != nil {
		return err
	}
	c.mu.Lock()
	raw := c.raw
	c.mu.Unlock()
	if raw {
		_, err := CopyFrame(c.ur, c.cw)
		return err
	}
	if b[0] == '>' {
		_, v, err := c.tee()
		if err != nil {
			return err
		}
		c.pushFrame(v)
		c.p.Stats.replies.Add(1)
		_, err = c.cw.Write(c.teeBuf.Bytes())
		return err
	}
	c.mu.Lock()
	if len(c.q) == 0 || c.q[0].synth != nil {
		c.mu.Unlock()
		return fmt.Errorf("reply with no pending command (desync); closing")
	}
	s := c.q[0]
	c.q = c.q[1:]
	c.fwd--
	c.account(-slotCost)
	if s.toRaw {
		c.raw = true
	}
	c.mu.Unlock()
	dst := c.cw
	if s.drop {
		dst = c.discard
	}
	var info FrameInfo
	if s.strip > 0 || s.onExec != nil {
		var v Value
		info, v, err = c.tee()
		if err == nil && s.execCount > 0 && !((v.Type == '*' || v.Type == '_') && v.Null) {
			if v.Type != '*' || len(v.Elems) != s.execCount {
				err = errors.New("protected EXEC reply incomplete")
			} else {
				for _, item := range v.Elems {
					if item.Type == '-' {
						err = errors.New("protected EXEC had a command error")
					}
				}
				last := v.Elems[len(v.Elems)-1]
				if last.Type != '+' || string(last.Str) != "OK" {
					err = errors.New("protected EXEC receipt unconfirmed")
				}
			}
		}
		if err == nil && s.onExec != nil {
			s.onExec(v)
		}
		if err == nil {
			if s.strip > 0 && v.Type == '*' && !v.Null && len(v.Elems) >= s.strip {
				v.Elems = v.Elems[:len(v.Elems)-s.strip]
				err = EncodeValue(dst, v)
			} else {
				_, err = dst.Write(c.teeBuf.Bytes())
			}
		}
	} else if s.hook != nil {
		var v Value
		info, v, err = c.tee()
		if err == nil {
			s.hook(v)
			_, err = dst.Write(c.teeBuf.Bytes())
		}
	} else {
		info, err = CopyFrame(c.ur, dst)
	}
	if err != nil {
		if s.done != nil {
			s.done(Outcome{Forwarded: true, Lost: true, ConnID: c.id})
		}
		return err
	}
	c.p.Stats.replies.Add(1)
	if s.done != nil {
		committed := !info.IsError()
		if s.exec {
			committed = committed && !info.Null && info.Type == '*'
		}
		s.done(Outcome{Forwarded: true, Committed: committed, ConnID: c.id})
	}
	return nil
}

// pushFrame accounts a RESP3 push. A subscribe-family confirmation answers
// the SUBSCRIBE-family slot at the head of the queue; any other push is a
// message and owns no slot (review H8B-L1 #4: an error to SUBSCRIBE is a
// normal reply and lands in that slot, not the next command's).
func (c *conn) pushFrame(v Value) {
	if len(v.Elems) == 0 {
		return
	}
	kind := strings.ToLower(string(v.Elems[0].Str))
	base, ok := subKinds[kind]
	if !ok {
		return
	}
	set := c.subs[base]
	c.mu.Lock()
	if len(c.q) > 0 && c.q[0].synth == nil && c.q[0].sub == kind {
		h := &c.q[0]
		if h.subN < 0 {
			h.subN = max(len(set), 1)
		}
		h.subN--
		if h.subN <= 0 {
			c.q = c.q[1:]
			c.fwd--
			c.account(-slotCost)
		}
	}
	c.mu.Unlock()
	if len(v.Elems) < 2 || v.Elems[1].Null {
		return
	}
	ch := string(v.Elems[1].Str)
	if kind == base {
		if set == nil {
			set = map[string]bool{}
			c.subs[base] = set
		}
		set[ch] = true
	} else {
		delete(set, ch)
	}
}

func isClosed(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
		strings.Contains(err.Error(), "connection reset") || strings.Contains(err.Error(), "broken pipe")
}

func errReply(msg string) []byte { return []byte("-" + msg + "\r\n") }

const wrongPass = "WRONGPASS invalid username-password pair or user is disabled."

func (c *conn) commandLoop() {
	for {
		args, err := ReadCommand(c.cr)
		if err != nil {
			if !isClosed(err) {
				c.p.Stats.protoErrors.Add(1)
				c.p.Logf("respproxy: conn %d command stream: %v", c.id, err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		if c.revoked.Load() {
			return // also catches commands already buffered before socket Close
		}
		if err := c.command(args); err != nil {
			if err != errClose {
				c.p.Logf("respproxy: conn %d: %v", c.id, err)
			}
			if err == errOverflow {
				c.p.Stats.overflowClosed.Add(1)
			}
			return
		}
		if c.overflowed() {
			c.p.Stats.overflowClosed.Add(1)
			c.p.Logf("respproxy: conn %d (%s): %v", c.id, c.remote, errOverflow)
			return
		}
		if c.cr.Buffered() == 0 {
			c.p.Stats.flushes.Add(1)
			if err := c.flushUpstream(); err != nil {
				return
			}
		}
	}
}

// roArgs forwards an allow-listed script as EVAL_RO / EVALSHA_RO so Redis
// itself refuses any write the body might attempt.
func roArgs(args [][]byte) [][]byte {
	out := append([][]byte(nil), args...)
	switch strings.ToUpper(string(args[0])) {
	case "EVAL":
		out[0] = []byte("EVAL_RO")
	case "EVALSHA":
		out[0] = []byte("EVALSHA_RO")
	}
	return out
}

func (c *conn) command(args [][]byte) error {
	if c.revoked.Load() {
		return errClose
	}
	p := c.p
	info := Classify(args)
	ctx := "bare"
	if c.txn != nil {
		ctx = "txn"
	}
	c.cs.command(info, ctx)
	p.Stats.recordUnknown(info)
	name := info.Name
	if info.Script != "" {
		args = roArgs(args)
	}

	if c.subscribed {
		switch name {
		case "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE", "PING", "QUIT":
			return c.writeNoSlot(args)
		}
		p.Logf("respproxy: conn %d: %s on a RESP2 pub/sub connection; closing", c.id, name)
		return errClose
	}

	if c.txn != nil {
		return c.inTxn(args, info)
	}

	switch name {
	case "AUTH":
		c.auth(args)
		return nil
	case "HELLO":
		c.hello(args)
		return nil
	case "QUIT":
		return c.forward(args, slot{})
	case "SELECT":
		return c.selectDB(args, info)
	case "MULTI":
		c.txn = &txnState{}
		return c.forward(args, slot{})
	case "EXEC", "DISCARD": // without MULTI: Redis answers the error
		c.watched = nil
		return c.forward(args, slot{})
	case "WATCH":
		for _, a := range args[1:] {
			if len(c.watched) < 8192 {
				c.watched = append(c.watched, string(a))
			}
		}
		ack := make(chan struct{})
		c.watchAck = ack
		return c.forward(args, slot{hook: func(Value) { close(ack) }})
	case "UNWATCH":
		c.watched = nil
		return c.forward(args, slot{})
	case "CLIENT":
		if len(args) >= 3 && strings.EqualFold(string(args[1]), "SETNAME") {
			// The upstream name always identifies the principal.
			return c.forward(strArgs("CLIENT", "SETNAME", c.upstreamName()), slot{})
		}
	}
	switch info.Class {
	case ClassForbidden:
		// Refused in every mode: forwarding would break reply accounting
		// (CLIENT REPLY, MONITOR) or upstream identity (RESET).
		d := &Denial{Reason: ReasonAdminOnly, Detail: name + " breaks the proxy's reply stream"}
		p.Stats.recordDecision(DecDeny, d, *c.principal, c.newUnit(UnitBare, []Cmd{{Args: args, Info: info}}))
		c.reply(d.Reply())
		return nil
	case ClassLocal:
		c.local(args)
		return nil
	case ClassPubSub:
		return c.subscribe(args, name)
	}
	if !needsGate(info) {
		if info.Class == ClassRead {
			p.Stats.seen(info.Keys)
		}
		return c.forward(args, slot{})
	}
	return c.gateBare(args, info)
}

// selectDB admits SELECT only to a configured DB (review H8B-L1 #7) and
// records the DB only once Redis answered +OK. The command goroutine waits
// for that reply so no later unit is decided under a DB the connection is
// not in.
func (c *conn) selectDB(args [][]byte, info Command) error {
	n := -1
	if len(args) == 2 {
		if v, err := strconv.Atoi(string(args[1])); err == nil && v >= 0 {
			n = v
		}
	}
	if n < 0 {
		return c.forward(args, slot{}) // Redis answers the syntax error
	}
	if c.principal.Kind != KindAdmin && !c.p.dbAllowed(n) { // every mode
		d := &Denial{Reason: ReasonAdminOnly, Detail: "SELECT " + strconv.Itoa(n) + ": not a configured DB"}
		c.p.Stats.recordDecision(DecDeny, d, *c.principal, c.newUnit(UnitBare, []Cmd{{Args: args, Info: info}}))
		c.reply(d.Reply())
		return nil
	}
	got := make(chan Value, 1)
	if err := c.forward(args, slot{hook: func(v Value) { got <- v }}); err != nil {
		return err
	}
	if err := c.flushUpstream(); err != nil {
		return err
	}
	t := time.NewTimer(selectTimeout)
	defer t.Stop()
	select {
	case v := <-got:
		if v.Type == '+' {
			c.db = n
		}
		return nil
	case <-t.C:
		c.p.Logf("respproxy: conn %d: no SELECT reply in %v; closing", c.id, selectTimeout)
		return errClose
	case <-c.dead:
		return errClose
	}
}

func (p *Proxy) dbAllowed(n int) bool {
	for _, d := range p.cfg.AllowedDBs {
		if d == n {
			return true
		}
	}
	return false
}

func (c *conn) inTxn(args [][]byte, info Command) error {
	name := info.Name
	switch name {
	case "EXEC":
		return c.exec()
	case "DISCARD":
		c.txn = nil
		c.watched = nil
		return c.forward(args, slot{})
	case "MULTI", "WATCH": // Redis answers the error itself
		return c.forward(args, slot{})
	}
	switch {
	case name == "AUTH", name == "HELLO", name == "SELECT", name == "CLIENT", name == "QUIT",
		info.Class == ClassLocal, info.Class == ClassPubSub, info.Class == ClassForbidden:
		// These change connection state the proxy tracks, or are served
		// by the proxy; Redis would apply them at EXEC time, out of order
		// with the proxy's view. The transaction is refused at EXEC.
		if c.txn.poison == "" {
			c.txn.poison = name + " inside MULTI"
		}
		c.reply(errReply("ERR proxy: " + name + " is not allowed inside MULTI"))
		return nil
	}
	c.txn.cmds = append(c.txn.cmds, Cmd{Args: args, Info: info})
	return c.forward(args, slot{})
}

// UpstreamName is the CLIENT SETNAME the proxy gives the upstream Redis
// connection of a client connection: "p:<principal>:<connID>". It is unique
// per client connection, so `CLIENT KILL ID`-free targeted kills
// (`CLIENT KILL ... ` after a CLIENT LIST lookup by this name) hit exactly one
// connection. Side connections are "p:-proxy-side".
func UpstreamName(principal string, connID uint64) string {
	return "p:" + principal + ":" + strconv.FormatUint(connID, 10)
}

func (c *conn) upstreamName() string { return UpstreamName(c.principal.Name, c.id) }

func (c *conn) newUnit(kind UnitKind, cmds []Cmd) *Unit {
	u := &Unit{Kind: kind, DB: c.db, Cmds: cmds, Mode: c.p.Mode(), ConnID: c.id, Remote: c.remote}
	if c.principal != nil {
		u.UpstreamName = c.upstreamName()
	}
	if kind == UnitTxn {
		u.beforeSide = c.confirmWatch
	}
	return u
}

// confirmWatch makes the connection's last WATCH reach Redis and waits for
// its reply, so a side read that follows is covered by the WATCH: any change
// after the read aborts the EXEC (review H8B-L1d #4).
func (c *conn) confirmWatch() error {
	ack := c.watchAck
	if ack == nil {
		return errors.New("no WATCH on this connection")
	}
	if err := c.flushUpstream(); err != nil {
		return err
	}
	t := time.NewTimer(selectTimeout)
	defer t.Stop()
	select {
	case <-ack:
		return nil
	case <-t.C:
		return errors.New("WATCH reply timed out")
	case <-c.dead:
		return errClose
	}
}

func writesOf(cmds []Cmd) []writeRec {
	var out []writeRec
	for _, cm := range cmds {
		if cm.Info.Class == ClassWrite {
			out = append(out, writeRec{cmd: cm.Info, args: cm.Args})
		}
	}
	return out
}

// decide runs the fence, the guard and, for project-scoped writes, the
// policy. It returns the refusal to apply (nil in observe mode, except for
// the fence), the verdict (whose Done must be honoured) and the guard
// effects to commit on admit.
func (c *conn) decide(u *Unit) (*Denial, Verdict, []guardEffect) {
	pr := *c.principal
	if c.p.fenced() {
		d := &Denial{Reason: ReasonProxyFenced, Detail: "singleton lease not renewed within its TTL"}
		c.p.Stats.recordDecision(DecDeny, d, pr, u)
		return d, Verdict{}, nil
	}
	// The command gate and key-class gate (Guard) refuse in EVERY mode:
	// passthrough and observe relax only the project-lease decision
	// (L5 review: in observe/passthrough a webdav principal ran FLUSHDB and
	// a Mac rewrote jmlease:v1:cfg).
	d, effects := c.p.Guard.Check(pr, u)
	if d != nil {
		c.p.Stats.recordDecision(DecDeny, d, pr, u)
		return d, Verdict{}, nil
	}
	var v Verdict
	_, leaseEngine := c.p.policy.(*LeaseEngine)
	if len(u.Project) > 0 && (u.Mode != ModePassthrough || leaseEngine) {
		if !c.revoked.Load() {
			c.uw.Flush() // Decide may wait on an earlier unit's Done
		}
		v = c.p.policy.Decide(pr, u)
		d = v.Deny
	}
	switch {
	case d == nil:
		c.cs.allow(pr)
	case u.Mode == ModeObserve || u.Mode == ModePassthrough:
		c.p.Stats.recordDecision(DecWouldDeny, d, pr, u)
		d = nil
	default:
		c.p.Stats.recordDecision(DecDeny, d, pr, u)
	}
	if d == nil {
		d = c.checkReplacements(u)
		if d != nil {
			c.p.Stats.recordDecision(DecDeny, d, pr, u)
		}
	}
	return d, v, effects
}

func (c *conn) gateBare(args [][]byte, info Command) error {
	cmds := []Cmd{{Args: args, Info: info}}
	c.p.Stats.analyse("bare", writesOf(cmds))
	u := c.newUnit(UnitBare, cmds)
	d, v, effects := c.decide(u)
	if d != nil {
		if v.Done != nil {
			v.Done(Outcome{ConnID: c.id})
		}
		c.reply(d.Reply())
		return nil
	}
	s := slot{done: c.p.commitOnSuccess(effects, v.Done)}
	switch counter, n := Allocation(u); counter {
	case "nextsession":
		s.hook = c.p.sessionHook(c.principal.Name, n)
	case "nextchunk":
		s.hook = c.p.sliceHook(c.principal.Name, n)
	}
	if u.rewrite != nil {
		args = u.rewrite
	}
	return c.forward(args, s)
}

// commitOnSuccess applies the guard's learned state (own deleted-file and
// zero-ref records, ended sessions) only once Redis confirmed the unit: a
// WATCH-aborted, failed or lost EXEC leaves nothing recorded (review
// H8B-L1b).
func (p *Proxy) commitOnSuccess(effects []guardEffect, done func(Outcome)) func(Outcome) {
	if len(effects) == 0 {
		return done
	}
	return func(o Outcome) {
		if o.Committed && !o.Lost {
			p.Guard.Commit(effects)
		}
		if done != nil {
			done(o)
		}
	}
}

// sliceHook records the slice ids a principal's own INCRBY nextchunk
// allocated: (R-n, R] for reply R (juicefs base.go:1658-1663).
func (p *Proxy) sliceHook(owner string, n int64) func(Value) {
	return func(v Value) {
		if v.Type != ':' || v.Int < n {
			return
		}
		if err := p.Guard.slices.Record(uint64(v.Int-n+1), uint64(v.Int), owner); err != nil {
			p.Logf("respproxy: slice ids %d..%d owner record for %s failed: %v", v.Int-n+1, v.Int, owner, err)
		}
	}
}

// sessionHook records the owner of the session ids a principal's own
// INCRBY nextsession allocated, before the client sees them.
func (p *Proxy) sessionHook(owner string, n int64) func(Value) {
	return func(v Value) {
		if v.Type != ':' {
			return
		}
		for sid := v.Int - n + 1; sid <= v.Int; sid++ {
			if sid <= 0 {
				continue
			}
			got, err := p.Guard.sessions.Claim(uint64(sid), owner)
			switch {
			case err != nil:
				p.Logf("respproxy: session %d owner record for %s failed: %v", sid, owner, err)
			case got != owner:
				p.Logf("respproxy: session %d already owned by %s; %s cannot use it", sid, got, owner)
			}
		}
	}
}

// exec decides a whole MULTI body. EXEC was held back; on refusal the proxy
// sends DISCARD upstream (its +OK is dropped) and puts the refusal in the
// EXEC slot, so the WATCHed keys and every queued write stay untouched.
func (c *conn) exec() error {
	t := c.txn
	c.txn = nil
	watched := c.watched
	c.watched = nil
	c.p.Stats.analyse("txn", writesOf(t.cmds))
	u := c.newUnit(UnitTxn, t.cmds)
	u.Watched = watched

	var d *Denial
	var v Verdict
	var effects []guardEffect
	if t.poison != "" {
		d = &Denial{Reason: ReasonUnknownKey, Detail: t.poison}
		c.p.Stats.recordDecision(DecDeny, d, *c.principal, u)
	} else {
		d, v, effects = c.decide(u)
	}
	if d != nil {
		if v.Done != nil {
			v.Done(Outcome{ConnID: c.id})
		}
		c.p.Stats.discards.Add(1)
		if err := c.forward(strArgs("DISCARD"), slot{drop: true}); err != nil {
			return err
		}
		c.reply(d.Reply())
		return nil
	}
	// First links of self-allocated slices: set their used bits in the SAME
	// MULTI, so the bit is set exactly when the link commits. The appended
	// commands' QUEUED replies are dropped and their EXEC results stripped.
	var extra int
	for _, e := range effects {
		for _, id := range e.sliceUsed {
			if err := c.forward(strArgs("SETBIT", UsedBitKey(id), strconv.FormatUint(UsedBitOffset(id), 10), "1"), slot{drop: true}); err != nil {
				return err
			}
			extra++
		}
	}
	if u.replacementReceipt != nil {
		if err := c.forward(u.replacementReceipt, slot{drop: true}); err != nil {
			if v.Done != nil {
				v.Done(Outcome{Forwarded: true, Lost: true, ConnID: c.id})
			}
			return err
		}
		extra++
	}
	es := slot{exec: true, strip: extra, done: c.p.commitOnSuccess(effects, v.Done)}
	if u.replacementReceipt != nil {
		es.execCount = len(u.Cmds) + extra
	}
	// Remember sliceRef fields this txn takes below zero (from the EXEC
	// results), so juicefs's following bare HDEL of them is the
	// principal's own drop (Guard.ownSliceDrop).
	type decr struct {
		i int
		f string
	}
	var decs []decr
	for i, cm := range u.Cmds {
		if cm.Info.Name == "HINCRBY" && len(cm.Args) == 4 && string(cm.Args[1]) == "sliceRef" {
			if n, ok := RedisInt(cm.Args[3]); ok && n < 0 {
				decs = append(decs, decr{i, string(cm.Args[2])})
			}
		}
	}
	if len(decs) > 0 && c.principal.Kind != KindBG && c.principal.Kind != KindAdmin {
		pr := *c.principal
		es.onExec = func(v Value) {
			if v.Type != '*' || v.Null {
				return
			}
			for _, d := range decs {
				if d.i < len(v.Elems) && v.Elems[d.i].Type == ':' && v.Elems[d.i].Int < 0 {
					c.p.Guard.NoteDropped(pr, d.f)
				}
			}
		}
	}
	return c.forward(strArgs("EXEC"), es)
}

// auth handles AUTH [user] pass. It returns false only when the connection
// must close (upstream unavailable on first login).
func (c *conn) auth(args [][]byte) bool {
	var user, pass string
	switch len(args) {
	case 2:
		user, pass = "default", string(args[1])
	case 3:
		user, pass = string(args[1]), string(args[2])
	default:
		c.reply(errReply("ERR wrong number of arguments for 'auth' command"))
		return true
	}
	if !c.authenticate(user, pass) {
		return true
	}
	if !c.ensureUpstream() {
		return false
	}
	if err := c.forward(strArgs("CLIENT", "SETNAME", c.upstreamName()), slot{drop: true}); err != nil {
		return false
	}
	c.reply([]byte("+OK\r\n"))
	return true
}

func (c *conn) ensureUpstream() bool {
	if c.up != nil {
		return true
	}
	if err := c.startUpstream(); err != nil {
		c.p.Logf("respproxy: conn %d: dial upstream: %v", c.id, err)
		c.cw.WriteString("-ERR proxy: metadata server unavailable\r\n")
		c.cw.Flush()
		return false
	}
	return true
}

// authenticate checks credentials and, on failure, replies WRONGPASS. A
// failed re-AUTH keeps the connection's current principal.
func (c *conn) authenticate(user, pass string) bool {
	a := c.authOwner
	if a == nil {
		a = c.p.Auth
	}
	ticket, res := a.authenticateTicket(c.remote, user, pass)
	switch res {
	case AuthOK:
		if a.bind(c, ticket) {
			c.p.Stats.authOK.Add(1)
			return true
		}
		c.p.Stats.authBad.Add(1)
	case AuthThrottled:
		c.p.Stats.authThrottled.Add(1)
		c.p.Logf("respproxy: AUTH throttled user=%q remote=%s", sanitizeToken(user), c.remote)
	default:
		c.p.Stats.authBad.Add(1)
		c.p.Logf("respproxy: AUTH failed user=%q remote=%s", sanitizeToken(user), c.remote)
	}
	c.reply(errReply(wrongPass))
	return false
}

// hello handles HELLO [protover [AUTH user pass] [SETNAME name]]. AUTH is
// checked by the proxy and never forwarded; the upstream connection is
// already authenticated as the proxy. The client's SETNAME is replaced by
// p:<principal>. Returns false only when the connection must close.
func (c *conn) hello(args [][]byte) bool {
	ver := ""
	i := 1
	if len(args) > 1 {
		ver = string(args[1])
		i = 2
		if ver != "2" && ver != "3" {
			c.reply(errReply("NOPROTO unsupported protocol version"))
			return true
		}
	}
	var user, pass string
	hasAuth := false
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "AUTH":
			if i+2 >= len(args) {
				c.reply(errReply("ERR Syntax error in HELLO option 'auth'"))
				return true
			}
			user, pass, hasAuth = string(args[i+1]), string(args[i+2]), true
			i += 3
		case "SETNAME":
			if i+1 >= len(args) {
				c.reply(errReply("ERR Syntax error in HELLO option 'setname'"))
				return true
			}
			i += 2
		default:
			c.reply(errReply("ERR Syntax error in HELLO option"))
			return true
		}
	}
	if hasAuth {
		if !c.authenticate(user, pass) {
			return true
		}
	} else if c.principal == nil {
		c.p.Stats.unauthRefused.Add(1)
		c.reply((&Denial{Reason: ReasonAuthRequired}).Reply())
		return true
	}
	if !c.ensureUpstream() {
		return false
	}
	if ver == "" {
		return c.forward(strArgs("HELLO"), slot{}) == nil
	}
	c.resp3 = ver == "3"
	return c.forward(strArgs("HELLO", ver, "SETNAME", c.upstreamName()), slot{}) == nil
}

func (c *conn) subscribe(args [][]byte, name string) error {
	if name == "PUBSUB" {
		return c.forward(args, slot{})
	}
	if c.resp3 {
		// RESP3: confirmations arrive as push frames (pushFrame pairs them
		// with this slot); an error arrives as this slot's normal reply.
		kind := strings.ToLower(name)
		n := len(args) - 1
		if n == 0 && strings.Contains(kind, "unsubscribe") {
			n = -1
		}
		return c.forward(args, slot{sub: kind, subN: n})
	}
	// RESP2: the connection becomes a pub/sub stream for good. (If Redis
	// refuses the SUBSCRIBE, any later non-pub/sub command closes it.)
	c.subscribed = true
	return c.forward(args, slot{toRaw: true})
}

func (c *conn) local(args [][]byte) {
	if c.p.fenced() {
		c.reply((&Denial{Reason: ReasonProxyFenced, Detail: "singleton ownership proof lost"}).Reply())
		return
	}
	if len(args) >= 2 && strings.EqualFold(string(args[0]), "JM.LEASE") && strings.EqualFold(string(args[1]), "REPLACEMENT_CAPABILITIES") {
		if len(args) != 2 {
			c.reply(errFrame("REPLACEMENT_CAPABILITIES takes no arguments"))
			return
		}
		if err := c.flushUpstream(); err != nil {
			c.reply(errFrame("upstream unavailable"))
			return
		}
		c.reply(c.replacementCapabilities())
		return
	}
	if len(args) >= 2 && strings.EqualFold(string(args[0]), "JM.LEASE") && strings.EqualFold(string(args[1]), "CHECK_REPLACE") {
		e, ok := c.p.policy.(*LeaseEngine)
		if !ok || c.p.Mode() != ModeEnforce || !c.p.cfg.Singleton || len(args) != 5 {
			c.reply(errFrame("CHECK_REPLACE requires enforcing singleton lease service and project/accepted/current epochs"))
			return
		}
		if err := c.flushUpstream(); err != nil {
			c.reply(errFrame("upstream unavailable"))
			return
		}
		c.reply(e.checkReplacementHistory(*c.principal, c.db, c.p.Incarnation(), args[2:]))
		return
	}
	if len(args) >= 2 && strings.EqualFold(string(args[0]), "JM.LEASE") && strings.EqualFold(string(args[1]), "ARM_REPLACE") {
		e, ok := c.p.policy.(*LeaseEngine)
		if !ok || c.p.Mode() != ModeEnforce || !c.p.cfg.Singleton || (len(args) != 3 && len(args) != 4) {
			c.reply(errFrame("ARM_REPLACE requires enforcing singleton lease service, one record and optional exact prior record"))
			return
		}
		if err := c.flushUpstream(); err != nil {
			c.reply(errFrame("upstream unavailable"))
			return
		}
		var required []byte
		if len(args) == 4 {
			required = args[3]
			if len(required) == 0 {
				c.reply(errFrame("expected replacement record must be nonempty"))
				return
			}
		}
		c.reply(e.armReplacement(*c.principal, c.db, c.p.Incarnation(), args[2], required))
		return
	}
	if h, ok := c.p.policy.(LocalHandler); ok {
		c.reply(h.HandleLocal(*c.principal, c.db, args))
		return
	}
	c.reply(errReply("ERR unknown command '" + sanitizeToken(string(args[0])) + "' (no lease service in this proxy)"))
}

// ---------------------------------------------------------------------------
// Proxy-owned stores on side connections.

// SessionOwnersKey maps juicefs session id -> principal (plan §1.2 table,
// proxy-owned; clients cannot write jmlease:*).
const SessionOwnersKey = "jmlease:v1:sess"

type redisSessions struct {
	p     *Proxy
	mu    sync.Mutex
	cache map[uint64]string
}

func (s *redisSessions) Owner(sid uint64) (string, bool, error) {
	s.mu.Lock()
	o, ok := s.cache[sid]
	s.mu.Unlock()
	if ok {
		return o, true, nil
	}
	v, err := s.p.side.Do(s.p.cfg.VolumeDB, strArgs("HGET", SessionOwnersKey, strconv.FormatUint(sid, 10))...)
	if err == nil {
		err = v.Err()
	}
	if err != nil {
		return "", false, err
	}
	if v.Null {
		return "", false, nil
	}
	s.remember(sid, string(v.Str))
	return string(v.Str), true, nil
}

func (s *redisSessions) remember(sid uint64, owner string) {
	s.mu.Lock()
	if len(s.cache) > 100000 {
		s.cache = map[uint64]string{}
	}
	s.cache[sid] = owner
	s.mu.Unlock()
}

func (s *redisSessions) Claim(sid uint64, owner string) (string, error) {
	f := strconv.FormatUint(sid, 10)
	v, err := s.p.side.Do(s.p.cfg.VolumeDB, strArgs("HSETNX", SessionOwnersKey, f, owner)...)
	if err == nil {
		err = v.Err()
	}
	if err != nil {
		return "", err
	}
	got, ok, err := s.Owner(sid)
	if err != nil || !ok {
		return "", fmt.Errorf("session %d: owner unreadable after claim: %v", sid, err)
	}
	return got, nil
}

func (s *redisSessions) Release(sids []uint64) error {
	args := strArgs("HDEL", SessionOwnersKey)
	s.mu.Lock()
	for _, sid := range sids {
		delete(s.cache, sid)
		args = append(args, []byte(strconv.FormatUint(sid, 10)))
	}
	s.mu.Unlock()
	v, err := s.p.side.Do(s.p.cfg.VolumeDB, args...)
	if err == nil {
		err = v.Err()
	}
	return err
}

// SliceOwnersKey is a ZSET of allocated slice-id ranges: score = last id
// of the range, member = "<first>:<last>:<principal>". Ranges are disjoint
// because nextchunk only increases. One entry per 4096 slices written.
const SliceOwnersKey = "jmlease:v1:slices"

// redisSlices persists slice-id ownership; memory holds only a bounded
// cache of recent ranges (a principal writes from its current batch).
type redisSlices struct {
	p     *Proxy
	mu    sync.Mutex
	cache []sliceRange // most recent last; bounded
	used  map[uint64]bool
}

const sliceCacheMax = 1024

// mergeRangeLua: ARGV lo, hi, owner. If the range ending at lo-1 belongs to
// owner, replace it with lo'..hi; else add lo..hi.
const mergeRangeLua = `local lo, hi, owner = ARGV[1], ARGV[2], ARGV[3]
local prev = redis.call('ZRANGE', KEYS[1], tonumber(lo) - 1, tonumber(lo) - 1, 'BYSCORE')
for _, m in ipairs(prev) do
  local plo, phi, pown = string.match(m, '^(%d+):(%d+):(.+)$')
  if pown == owner and tonumber(phi) + 1 == tonumber(lo) then
    redis.call('ZREM', KEYS[1], m)
    lo = plo
  end
end
return redis.call('ZADD', KEYS[1], hi, lo .. ':' .. hi .. ':' .. owner)`

func (s *redisSlices) remember(r sliceRange) {
	s.mu.Lock()
	if len(s.cache) >= sliceCacheMax {
		s.cache = append(s.cache[:0], s.cache[len(s.cache)/2:]...)
	}
	s.cache = append(s.cache, r)
	s.mu.Unlock()
}

func (s *redisSlices) Owner(id uint64) (string, bool, error) {
	s.mu.Lock()
	for i := len(s.cache) - 1; i >= 0; i-- {
		if r := s.cache[i]; id >= r.lo && id <= r.hi {
			s.mu.Unlock()
			return r.owner, true, nil
		}
	}
	s.mu.Unlock()
	v, err := s.p.side.Do(s.p.cfg.VolumeDB, strArgs("ZRANGE", SliceOwnersKey, strconv.FormatUint(id, 10), "+inf", "BYSCORE", "LIMIT", "0", "1")...)
	if err == nil {
		err = v.Err()
	}
	if err != nil {
		return "", false, err
	}
	if len(v.Elems) == 0 {
		return "", false, nil
	}
	parts := strings.SplitN(string(v.Elems[0].Str), ":", 3)
	if len(parts) != 3 {
		return "", false, fmt.Errorf("bad %s member %q", SliceOwnersKey, v.Elems[0].Str)
	}
	lo, e1 := strconv.ParseUint(parts[0], 10, 64)
	hi, e2 := strconv.ParseUint(parts[1], 10, 64)
	if e1 != nil || e2 != nil || id < lo || id > hi {
		return "", false, nil
	}
	s.remember(sliceRange{lo, hi, parts[2]})
	return parts[2], true, nil
}

func (s *redisSlices) Record(lo, hi uint64, owner string) error {
	s.remember(sliceRange{lo, hi, owner})
	// Merge with the owner's range ending at lo-1 (consecutive batches of
	// one writer), so a busy single-writer volume stays one entry.
	v, err := s.p.side.Do(s.p.cfg.VolumeDB, strArgs("EVAL", mergeRangeLua, "1", SliceOwnersKey,
		strconv.FormatUint(lo, 10), strconv.FormatUint(hi, 10), owner)...)
	if err == nil {
		err = v.Err()
	}
	return err
}

func (s *redisSlices) Used(id uint64) (bool, error) {
	s.mu.Lock()
	if v, ok := s.used[id]; ok {
		s.mu.Unlock()
		return v, nil
	}
	s.mu.Unlock()
	v, err := s.p.side.Do(s.p.cfg.VolumeDB, strArgs("GETBIT", UsedBitKey(id), strconv.FormatUint(UsedBitOffset(id), 10))...)
	if err == nil {
		err = v.Err()
	}
	if err != nil {
		return false, err
	}
	used := v.Int == 1
	if used { // only the proxy sets bits, and it updates this cache when it does
		s.cacheUsed(id)
	}
	return used, nil
}

func (s *redisSlices) cacheUsed(id uint64) {
	s.mu.Lock()
	if s.used == nil || len(s.used) > 1_000_000 {
		s.used = map[uint64]bool{}
	}
	s.used[id] = true
	s.mu.Unlock()
}

func (s *redisSlices) MarkUsed(ids []uint64, persist bool) error {
	for _, id := range ids {
		s.cacheUsed(id)
		if persist {
			v, err := s.p.side.Do(s.p.cfg.VolumeDB, strArgs("SETBIT", UsedBitKey(id), strconv.FormatUint(UsedBitOffset(id), 10), "1")...)
			if err == nil {
				err = v.Err()
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type sideCounters struct{ p *Proxy }

func (s sideCounters) Counter(db int, key string) (int64, error) {
	v, err := s.p.side.Do(db, strArgs("GET", key)...)
	if err == nil {
		err = v.Err()
	}
	if err != nil || v.Null {
		return 0, err
	}
	return strconv.ParseInt(string(v.Str), 10, 64)
}

type sideChunks struct{ p *Proxy }

func (s sideChunks) ChunkElems(db int, key string) ([][]byte, error) {
	v, err := s.p.side.Do(db, strArgs("LRANGE", key, "0", "-1")...)
	if err == nil {
		err = v.Err()
	}
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(v.Elems))
	for _, e := range v.Elems {
		out = append(out, e.Str)
	}
	return out, nil
}

// SidePool is a small pool of proxy-owned, authenticated upstream
// connections for side reads and proxy bookkeeping. It never shares a
// connection with a client.
type SidePool struct {
	dial func() (net.Conn, *bufio.Reader, error)
	max  int

	mu     sync.Mutex
	idle   []*sideConn
	closed bool
}

type sideConn struct {
	c  net.Conn
	r  *bufio.Reader
	w  *bufio.Writer
	db int
}

func (sp *SidePool) get() (*sideConn, error) {
	sp.mu.Lock()
	if sp.closed {
		sp.mu.Unlock()
		return nil, errors.New("respproxy: side pool closed")
	}
	if n := len(sp.idle); n > 0 {
		sc := sp.idle[n-1]
		sp.idle = sp.idle[:n-1]
		sp.mu.Unlock()
		return sc, nil
	}
	sp.mu.Unlock()
	c, r, err := sp.dial()
	if err != nil {
		return nil, err
	}
	sc := &sideConn{c: c, r: r, w: bufio.NewWriter(c)}
	if err := sc.roundTrip(strArgs("CLIENT", "SETNAME", "p:-proxy-side"), nil); err != nil {
		c.Close()
		return nil, err
	}
	return sc, nil
}

func (sp *SidePool) put(sc *sideConn) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.closed || len(sp.idle) >= sp.max {
		sc.c.Close()
		return
	}
	sp.idle = append(sp.idle, sc)
}

func (sc *sideConn) roundTrip(args [][]byte, out *Value) error {
	sc.c.SetDeadline(time.Now().Add(sideTimeout))
	if err := WriteCommand(sc.w, args); err != nil {
		return err
	}
	if err := sc.w.Flush(); err != nil {
		return err
	}
	v, err := ReadValue(sc.r)
	if err != nil {
		return err
	}
	if out != nil {
		*out = v
	}
	return nil
}

// Do runs one command on DB db and returns its reply. A Redis error reply is
// returned as a Value (check Value.Err); err is a transport failure.
func (sp *SidePool) Do(db int, args ...[]byte) (Value, error) {
	sc, err := sp.get()
	if err != nil {
		return Value{}, err
	}
	if sc.db != db {
		var v Value
		if err := sc.roundTrip(strArgs("SELECT", strconv.Itoa(db)), &v); err != nil || v.Err() != nil {
			sc.c.Close()
			if err == nil {
				err = v.Err()
			}
			return Value{}, err
		}
		sc.db = db
	}
	var v Value
	if err := sc.roundTrip(args, &v); err != nil {
		sc.c.Close()
		return Value{}, err
	}
	sp.put(sc)
	return v, nil
}

// Close closes idle side connections.
func (sp *SidePool) Close() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.closed = true
	for _, sc := range sp.idle {
		sc.c.Close()
	}
	sp.idle = nil
}
