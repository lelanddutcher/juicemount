package respproxy

// policy.go is the FROZEN seam between the proxy core (lane L1) and the
// lease decision (lane L2). Do not change these types without updating both
// lanes and H8_OPTION_B_PLAN.md.
//
// Order of evaluation for every unit (H8 plan §1.4):
//
//	step 0  reads, PUBLISH juicemount:metadata and JM.* commands: no check.
//	step 1  command gate (guard.go): admin-only commands.
//	step 2  key-class gate (guard.go): allow-list, bg-only, farm-only,
//	        admin-only and unknown shapes.
//	step 3+ every remaining write key is "project-scoped" and is listed in
//	        Unit.Project. If that list is not empty the proxy calls
//	        Policy.Decide exactly once for the unit.
//
// Decide is never called for a unit the guard already refused, and never for
// a unit with no project-scoped write.

import (
	"fmt"
	"strings"
)

// Kind is the role of an authenticated principal.
type Kind string

const (
	KindMac  Kind = "mac"  // an editor's Mac (JuiceMount app + its juicefs child)
	KindFarm Kind = "farm" // derivative farm workers
	// KindManager is the server manager's own principal: farm keys and farm
	// scripts. Its maintenance jobs do NOT run under it (decision
	// 2026-10-06, for L5): scheduled gc / compact / fsck connect with the
	// bg principal's credentials (they write delSlices, sliceRef, delfiles
	// and trash, which are bg-only), and `juicefs config` (writes
	// `setting`) connects as admin. The manager kind gets no bg rights, so
	// a compromised manager API cannot drop other projects' slice refs.
	KindManager Kind = "manager"
	KindBG      Kind = "bg"     // the server's background-job juicefs mount (gc, trash, sessions)
	KindWebDAV  Kind = "webdav" // read-only gateway: no write rights
	KindAdmin   Kind = "admin"  // operator: may do anything; every action is logged
)

// Principal is who is on the other end of a client connection. There is no
// anonymous principal: an unauthenticated connection is refused before any
// unit is built (AUTH_REQUIRED).
type Principal struct {
	Name string // unique, from the principals file
	Kind Kind
	Host string // the machine's hostname; owns jum:presence:<Host>. Empty means Name.
}

func (p Principal) host() string {
	if p.Host != "" {
		return p.Host
	}
	return p.Name
}

// UnitKind says how the commands of a unit reached the proxy.
type UnitKind string

const (
	UnitBare UnitKind = "bare" // one command outside MULTI (pipelined or not)
	UnitTxn  UnitKind = "txn"  // the commands queued between MULTI and EXEC
)

// Cmd is one command of a unit.
type Cmd struct {
	Args [][]byte // the raw argv, as forwarded upstream
	Info Command  // classification (classify.go)
}

// KeyWrite is one key written by one command of the unit.
type KeyWrite struct {
	Cmd   int      // index into Unit.Cmds
	Key   KeyRef   // parsed JuiceFS / JuiceMount key shape
	Raw   string   // the raw key
	Class KeyClass // the guard's class for this write
}

// Unit is one decision unit (plan §1.4): a bare command, or a
// WATCH…MULTI…EXEC body. Decide must treat it as read-only input.
type Unit struct {
	Kind    UnitKind
	DB      int      // the Redis DB the client had SELECTed
	Cmds    []Cmd    // bare: exactly one; txn: the queued commands (MULTI/EXEC excluded)
	Watched []string // keys WATCHed on this connection before MULTI
	Writes  []KeyWrite
	// Project is the subset of Writes the guard left for the lease check.
	// It includes delfiles/sliceRef writes that inherit the txn's anchors,
	// trash writes paired with a non-trash dentry write, and dir-stat writes.
	Project []KeyWrite
	// RefInodes (added 2026-10-06, additive) lists inodes that shared
	// cleanup records in this unit refer to: delfiles/detachedNodes members,
	// and the chunk inode owning every slice a sliceRef/delSlices record
	// names (or a zero-ref drop's recorded inode). The guard already refused
	// any record naming an inode or slice the unit does not itself write;
	// Decide MUST check the principal may write every RefInode, exactly as
	// for the inodes of Project.
	RefInodes []uint64

	// beforeSide, set by the proxy, makes the connection's buffered WATCH
	// reach Redis (and waits for its +OK) before the guard's first side read.
	beforeSide func() error
	// rewrite, set by the guard, replaces the forwarded command (a bare
	// own sliceRef drop runs as the proxy's conditional SliceDropScript).
	rewrite [][]byte
	// replacementReceipt is a trusted append, never a client command.
	replacementReceipt [][]byte
	Mode               Mode // the proxy mode at decision time
	// ConnID is the proxy's id of the client connection. Its upstream
	// Redis connection is named UpstreamName = "p:<principal>:<ConnID>"
	// (UpstreamName()), so L2 can CLIENT LIST / kill exactly that one.
	ConnID       uint64
	UpstreamName string
	Remote       string // client address
}

// Reason is the second token of a refusal reply.
type Reason string

const (
	ReasonLeaseHeld         Reason = "LEASE_HELD"         // L2: another principal holds the project
	ReasonAdminOnly         Reason = "ADMIN_ONLY"         // command or key needs the admin principal
	ReasonBGOnly            Reason = "BG_ONLY"            // background-job keys: bg principal only
	ReasonFarmOnly          Reason = "FARM_ONLY"          // juicefarm:/jmfarm:/jmctl: keys: farm or manager only
	ReasonUnknownKey        Reason = "UNKNOWN_KEY"        // a key shape that is neither JuiceFS's nor JuiceMount's
	ReasonPrincipalReadonly Reason = "PRINCIPAL_READONLY" // a principal with no write rights (webdav)
	ReasonXProject          Reason = "XPROJECT"           // L2: a unit spanning projects the principal cannot hold together
	ReasonAuthRequired      Reason = "AUTH_REQUIRED"      // the connection has not authenticated
	ReasonProxyFenced       Reason = "PROXY_FENCED"       // this proxy lost its singleton lease; no writes
)

// AllReasons lists every reason the proxy can emit (tests fuzz over it).
var AllReasons = []Reason{
	ReasonLeaseHeld, ReasonAdminOnly, ReasonBGOnly, ReasonFarmOnly,
	ReasonUnknownKey, ReasonPrincipalReadonly, ReasonXProject, ReasonAuthRequired, ReasonProxyFenced,
}

// Denial is a refusal. The proxy renders it as
//
//	-JMDENY <REASON> proj=<rootIno> holder=<principal>@<host> epoch=<n>
//
// (plan §1.5). Proj/Holder/Host/Epoch are zero/"-" for non-lease reasons.
type Denial struct {
	Reason Reason
	Proj   uint64
	Holder string
	Host   string
	Epoch  uint64
	Detail string // logged only, never sent to the client
}

// Outcome is what happened to an admitted unit, reported to Verdict.Done.
type Outcome struct {
	// Forwarded is false when the unit never reached Redis (refused, or the
	// connection died first).
	Forwarded bool
	// Committed is true when Redis applied the unit: a non-error reply for a
	// bare command; a non-nil array reply for EXEC (a WATCH abort is false).
	Committed bool
	// Lost is true when the unit was sent but its reply never arrived (the
	// upstream or client connection died). Redis may or may not have
	// applied it; L2 must treat the unit as possibly committed.
	Lost bool
	// ConnID is the client connection the unit ran on (= Unit.ConnID);
	// its upstream is named UpstreamName(principal, ConnID).
	ConnID uint64
}

// Verdict is Decide's answer.
type Verdict struct {
	// Deny refuses the unit. nil admits it.
	Deny *Denial
	// Done, if not nil, is called exactly once: after the unit's reply has
	// been read from Redis (for a txn, the EXEC reply), or with
	// Outcome{Forwarded:false} if the unit was refused or never sent. It is
	// called from the connection's reply goroutine and must not block on
	// that connection. L2 releases its per-project read lock here (§1.4
	// step 6) and updates caches on Committed (step 7).
	Done func(Outcome)
}

// Policy rules on project-scoped writes (§1.4 steps 3-6). L1 ships
// AllowProjectWrites; L2 replaces it with the lease engine.
//
// Decide is called from the client connection's command goroutine, once per
// unit, and may block (side reads, locks). It must be safe for concurrent
// use across connections. Before calling Decide the proxy flushes every
// earlier command of the same connection upstream, so Decide may wait for
// the Done of an earlier unit on that connection without deadlocking. Side
// reads must use the proxy's SidePool (Proxy.Side), never the client's
// upstream connection.
type Policy interface {
	Decide(p Principal, u *Unit) Verdict
}

// LocalHandler is optionally implemented by a Policy to serve JM.* commands
// (JM.LEASE ...). The proxy never forwards JM.* upstream. The returned bytes
// must be exactly one RESP reply frame.
type LocalHandler interface {
	HandleLocal(p Principal, db int, args [][]byte) []byte
}

// AllowProjectWrites is the L1 policy: every project-scoped write is
// admitted. L2 implements leases behind the same interface.
type AllowProjectWrites struct{}

func (AllowProjectWrites) Decide(Principal, *Unit) Verdict { return Verdict{} }

// Mode is the proxy's enforcement mode.
type Mode string

const (
	// Modes relax ONLY the project-lease decision (Policy.Decide). AUTH,
	// the command gate and the key-class gate (guard.go: ADMIN_ONLY, BG_ONLY,
	// FARM_ONLY, UNKNOWN_KEY, PRINCIPAL_READONLY, jmlease:*, setting/acl,
	// unpinned scripts, cross-DB) refuse in every mode.
	ModePassthrough Mode = "passthrough" // Decide is not called
	ModeObserve     Mode = "observe"     // Decide is called; its refusals are admitted and logged WOULD_DENY
	ModeEnforce     Mode = "enforce"     // Decide's refusals are enforced
)

// ParseMode validates a mode string.
func ParseMode(s string) (Mode, bool) {
	switch Mode(strings.ToLower(strings.TrimSpace(s))) {
	case ModePassthrough:
		return ModePassthrough, true
	case ModeObserve:
		return ModeObserve, true
	case ModeEnforce:
		return ModeEnforce, true
	}
	return "", false
}

// denyToken is the first token of every refusal. It must never be one that
// juicefs or go-redis retries (TestH8BNeverRetryablePrefix).
const denyToken = "JMDENY"

// Error renders the refusal's error text (without the leading '-').
func (d *Denial) Error() string {
	holder, host := sanitizeToken(d.Holder), sanitizeToken(d.Host)
	msg := fmt.Sprintf("%s %s proj=%d holder=%s@%s epoch=%d",
		denyToken, sanitizeToken(string(d.Reason)), d.Proj, holder, host, d.Epoch)
	// juicefs's shouldRetry retries any error that merely CONTAINS
	// "EXECABORT" (redis.go:979), so a principal or host name carrying it
	// must not reach the wire intact.
	for strings.Contains(msg, "EXECABORT") {
		msg = strings.ReplaceAll(msg, "EXECABORT", "EXEC_ABORT")
	}
	return msg
}

// Reply renders the refusal as one RESP error frame.
func (d *Denial) Reply() []byte { return []byte("-" + d.Error() + "\r\n") }

// sanitizeToken keeps a field to one printable token: no spaces, no CR/LF,
// so a hostile principal or host name cannot inject a frame or a token.
func sanitizeToken(s string) string {
	if s == "" {
		return "-"
	}
	b := []byte(s)
	for i, c := range b {
		if c <= ' ' || c >= 0x7f {
			b[i] = '_'
		}
	}
	if len(b) > 128 {
		b = b[:128]
	}
	return string(b)
}
