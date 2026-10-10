package respproxy

// leasecmd.go serves JM.LEASE (H8 plan §1.3). The proxy never forwards it.
//
// Wire format (RESP2-compatible; every reply is one frame):
//
//	JM.LEASE ACQUIRE <proj> <auto|checkout> [ttl_s]
//	  -> *5 [+status :epoch $holder :expires_ms :foreign_since]
//	     status GRANTED (new epoch) | RENEWED (caller already held it) |
//	            HELD (another principal holds it; nothing changed)
//	     holder "<principal>@<host>" of the current holder ("" if free)
//	     expires_ms: when the lease ends by itself (checkout end, or last
//	            sign of life + orphan_s); 0 when free
//	     foreign_since: 1 when another principal held the project after the
//	            caller's last epoch (spool rows accepted under an older
//	            epoch must be published as conflict copies on mismatch)
//	JM.LEASE CHECK_REPLACE <proj> <accepted_epoch> <current_epoch>
//	  -> *4 [+CLEAN|FOREIGN :project :current_epoch :max_foreign_epoch]
//	     Read-only, authenticated Mac/enforcing singleton/volume DB only.
//	     Unknown or unconfirmed replies must hold replacement uploads.
//	     CLEAN is a snapshot; ARM/rename still enforce publication authority.
//
//	JM.LEASE HEARTBEAT [<proj-with-pending-spool> ...]
//	  -> *N of *4 [:proj :epoch +state $mode]
//	     one "held" entry per lease the caller holds (all are renewed), and
//	     one "lost" entry per listed project the caller does not hold
//	JM.LEASE RELEASE <proj> <epoch>
//	  -> *2 [+status :epoch]  RELEASED | NOT_HOLDER | STALE_EPOCH
//	JM.LEASE STATUS [<proj>|*]
//	  -> one record, or *N of records; a record is a flat field/value array:
//	     proj epoch state(held|free) holder host kind mode granted_ms
//	     last_write_ms last_hb_ms checkout_until_ms expires_ms pending label
//	JM.LEASE BREAK <proj>          (admin only)
//	  -> *3 [+BROKEN|+FREE :epoch $previous_holder]
//
// <proj> is ino:<n> (any inode inside the project) or path:<p> (a path from
// the volume root; components beyond the project depth are ignored).
// Errors: -ERR for syntax and unknown projects; -JMDENY ADMIN_ONLY for a
// BREAK by a non-admin; -JMDENY PRINCIPAL_READONLY for an ACQUIRE by a
// principal with no write rights.

import (
	"bytes"
	"strconv"
	"strings"
	"time"
)

type respBuf struct{ bytes.Buffer }

func (b *respBuf) arr(n int)       { b.WriteString("*" + strconv.Itoa(n) + "\r\n") }
func (b *respBuf) simple(s string) { b.WriteString("+" + s + "\r\n") }
func (b *respBuf) integer(n int64) { b.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n") }
func (b *respBuf) bulk(s string)   { b.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n") }
func errFrame(msg string) []byte {
	return []byte("-ERR " + strings.NewReplacer("\r", " ", "\n", " ").Replace(msg) + "\r\n")
}
func holderStr(name, host string) string {
	if name == "" {
		return ""
	}
	return name + "@" + host
}

// HandleLocal implements LocalHandler.
func (e *LeaseEngine) HandleLocal(p Principal, db int, args [][]byte) []byte {
	if len(args) < 2 || !strings.EqualFold(string(args[0]), "JM.LEASE") {
		return errFrame("unknown command '" + sanitizeToken(string(args[0])) + "'")
	}
	sub := strings.ToUpper(string(args[1]))
	rest := args[2:]
	switch sub {
	case "ACQUIRE":
		return e.cmdAcquire(p, rest)
	case "HEARTBEAT":
		return e.cmdHeartbeat(p, rest)
	case "RELEASE":
		return e.cmdRelease(p, rest)
	case "STATUS":
		return e.cmdStatus(rest)
	case "BREAK":
		return e.cmdBreak(p, rest)
	}
	return errFrame("unknown JM.LEASE subcommand '" + sanitizeToken(sub) + "'")
}

// resolveProj turns ino:<n> / path:<p> into a project root inode.
func (e *LeaseEngine) resolveProj(spec string) (uint64, error) {
	switch {
	case strings.HasPrefix(spec, "ino:"):
		n, ok := digits(spec[4:])
		if !ok || n == 0 {
			return 0, errBadProj("bad inode " + strconv.Quote(spec))
		}
		if isTrashIno(n) {
			return 0, errBadProj("inode is in the trash")
		}
		locs, err := e.anc.resolveInode(n)
		if err != nil {
			return 0, errBadProj("cannot resolve " + spec + ": " + err.Error())
		}
		if len(locs) == 0 || locs[0].kind != nsProject {
			kind := nsUnknown
			if len(locs) > 0 {
				kind = locs[0].kind
			}
			return 0, errBadProj(spec + " is not inside a project (" + kind.String() + ")")
		}
		return locs[0].proj, nil
	case strings.HasPrefix(spec, "path:"):
		parts := strings.FieldsFunc(spec[5:], func(r rune) bool { return r == '/' })
		depth := e.anc.depth
		if len(parts) < depth {
			return 0, errBadProj(spec + " is above the project depth")
		}
		cur := uint64(rootIno)
		for _, name := range parts[:depth] {
			e.anc.sideReads.Add(1)
			v, err := e.cfg.Side.Do(e.cfg.VolumeDB, []byte("HGET"), []byte("d"+strconv.FormatUint(cur, 10)), []byte(name))
			if err != nil || v.Err() != nil {
				return 0, errBadProj("cannot read " + spec)
			}
			if v.Null {
				return 0, errBadProj(spec + ": no such directory " + strconv.Quote(name))
			}
			t, _ := entryType(v.Str)
			n, ok := entryInode(v.Str)
			if !ok || t != typeDir {
				return 0, errBadProj(spec + ": " + strconv.Quote(name) + " is not a directory")
			}
			cur = n
		}
		loc, err := e.anc.resolveDir(cur)
		if err != nil || loc.kind != nsProject {
			return 0, errBadProj(spec + " is not a project")
		}
		return loc.proj, nil
	}
	return 0, errBadProj("project must be ino:<n> or path:<p>")
}

type errBadProj string

func (e errBadProj) Error() string { return string(e) }

func (e *LeaseEngine) cmdAcquire(p Principal, a [][]byte) []byte {
	if len(a) < 2 || len(a) > 3 {
		return errFrame("usage: JM.LEASE ACQUIRE <proj> <auto|checkout> [ttl_s]")
	}
	if p.Kind == KindWebDAV {
		return (&Denial{Reason: ReasonPrincipalReadonly, Detail: "acquire"}).Reply()
	}
	mode := strings.ToLower(string(a[1]))
	if mode != LeaseAuto && mode != LeaseCheckout {
		return errFrame("mode must be auto or checkout")
	}
	c := e.cfgNow()
	ttl := c.checkoutMax
	if len(a) == 3 {
		n, err := strconv.ParseInt(string(a[2]), 10, 64)
		if err != nil || n <= 0 {
			return errFrame("ttl_s must be a positive integer")
		}
		if d := time.Duration(n) * time.Second; d < ttl {
			ttl = d
		}
	}
	proj, err := e.resolveProj(string(a[0]))
	if err != nil {
		return errFrame(err.Error())
	}
	fm := e.fence(proj)
	fm.Lock()
	e.mu.Lock()
	l := e.leaseLocked(proj)
	wasMine := l.held() && l.holder == p.Name
	e.mu.Unlock()
	s, fs, err := e.grantLocked(p, proj, mode, ttl, "JM.LEASE ACQUIRE "+mode)
	var exp int64
	if err == nil {
		e.mu.Lock()
		exp = e.expiresMs(e.leases[proj])
		e.mu.Unlock()
	}
	fm.Unlock()
	if err != nil {
		return errFrame("lease store unavailable: " + err.Error())
	}
	status := "GRANTED"
	switch {
	case s.Holder != p.Name:
		status = "HELD"
	case wasMine:
		status = "RENEWED"
	default:
		e.nGrant.Add(1)
	}
	var b respBuf
	b.arr(5)
	b.simple(status)
	b.integer(int64(s.Epoch))
	b.bulk(holderStr(s.Holder, s.Host))
	b.integer(exp)
	b.integer(map[bool]int64{true: 1}[fs])
	return b.Bytes()
}

func (e *LeaseEngine) cmdHeartbeat(p Principal, a [][]byte) []byte {
	pending := map[uint64]bool{}
	for _, spec := range a {
		proj, err := e.resolveProj(string(spec))
		if err != nil {
			return errFrame(err.Error())
		}
		pending[proj] = true
	}
	held, lost := e.heartbeat(p, pending)
	var b respBuf
	b.arr(len(held) + len(lost))
	for _, s := range held {
		b.arr(4)
		b.integer(int64(s.Proj))
		b.integer(int64(s.Epoch))
		b.simple("held")
		b.bulk(s.Mode)
	}
	for _, pr := range lost {
		e.mu.Lock()
		var ep uint64
		if l := e.leases[pr]; l != nil {
			ep = l.epoch
		}
		e.mu.Unlock()
		b.arr(4)
		b.integer(int64(pr))
		b.integer(int64(ep))
		b.simple("lost")
		b.bulk("")
	}
	return b.Bytes()
}

func (e *LeaseEngine) cmdRelease(p Principal, a [][]byte) []byte {
	if len(a) != 2 {
		return errFrame("usage: JM.LEASE RELEASE <proj> <epoch>")
	}
	ep, ok := digits(string(a[1]))
	if !ok {
		return errFrame("epoch must be an integer")
	}
	proj, err := e.resolveProj(string(a[0]))
	if err != nil {
		return errFrame(err.Error())
	}
	fm := e.fence(proj)
	fm.Lock()
	defer fm.Unlock()
	e.mu.Lock()
	l := e.leases[proj]
	if l != nil && l.unresolved {
		e.mu.Unlock()
		return errFrame("lease transition unresolved")
	}
	status := "RELEASED"
	var cur uint64
	switch {
	case l == nil || !l.held() || l.holder != p.Name:
		status = "NOT_HOLDER"
	case l.epoch != ep:
		status = "STALE_EPOCH"
	}
	if l != nil {
		cur = l.epoch
	}
	e.mu.Unlock()
	if status == "RELEASED" {
		_, ne, err := e.endLocked(proj, "release", p.Name, "JM.LEASE RELEASE")
		if err != nil {
			return errFrame("lease store unavailable: " + err.Error())
		}
		cur = ne
	}
	var b respBuf
	b.arr(2)
	b.simple(status)
	b.integer(int64(cur))
	return b.Bytes()
}

func (e *LeaseEngine) writeRecord(b *respBuf, proj uint64) bool {
	e.mu.Lock()
	l := e.leases[proj]
	if l != nil && l.unresolved {
		e.mu.Unlock()
		return false
	}
	var s leaseSnap
	var exp int64
	if l != nil {
		s, exp = l.snap(), e.expiresMs(l)
	} else {
		s.Proj = proj
	}
	e.mu.Unlock()
	if s.Label == "" {
		s.Label = e.anc.label(proj)
	}
	state := "free"
	if s.Holder != "" {
		state = "held"
	}
	b.arr(30)
	b.bulk("proj")
	b.integer(int64(s.Proj))
	b.bulk("epoch")
	b.integer(int64(s.Epoch))
	b.bulk("state")
	b.bulk(state)
	b.bulk("holder")
	b.bulk(s.Holder)
	b.bulk("host")
	b.bulk(s.Host)
	b.bulk("kind")
	b.bulk(string(s.Kind))
	b.bulk("mode")
	b.bulk(s.Mode)
	b.bulk("granted_ms")
	b.integer(s.GrantedMs)
	b.bulk("last_write_ms")
	b.integer(s.LastWriteMs)
	b.bulk("last_hb_ms")
	b.integer(s.LastHbMs)
	b.bulk("checkout_until_ms")
	b.integer(s.CheckoutUntilMs)
	b.bulk("expires_ms")
	b.integer(exp)
	b.bulk("pending")
	b.integer(map[bool]int64{true: 1}[s.Pending])
	b.bulk("label")
	b.bulk(s.Label)
	b.bulk("held")
	b.integer(map[bool]int64{true: 1}[s.Holder != ""])
	return true
}

func (e *LeaseEngine) cmdStatus(a [][]byte) []byte {
	if len(a) > 1 {
		return errFrame("usage: JM.LEASE STATUS [<proj>|*]")
	}
	var b respBuf
	if len(a) == 1 && string(a[0]) != "*" {
		proj, err := e.resolveProj(string(a[0]))
		if err != nil {
			return errFrame(err.Error())
		}
		f := e.fence(proj)
		f.RLock()
		ok := e.writeRecord(&b, proj)
		f.RUnlock()
		if !ok {
			return errFrame("lease transition unresolved")
		}
		return b.Bytes()
	}
	e.mu.Lock()
	var projs []uint64
	for pr := range e.leases {
		projs = append(projs, pr)
	}
	e.mu.Unlock()
	sortU64(projs)
	b.arr(len(projs))
	for _, pr := range projs {
		f := e.fence(pr)
		f.RLock()
		ok := e.writeRecord(&b, pr)
		f.RUnlock()
		if !ok {
			return errFrame("lease transition unresolved")
		}
	}
	return b.Bytes()
}

func (e *LeaseEngine) cmdBreak(p Principal, a [][]byte) []byte {
	if p.Kind != KindAdmin {
		return (&Denial{Reason: ReasonAdminOnly, Detail: "JM.LEASE BREAK"}).Reply()
	}
	if len(a) != 1 {
		return errFrame("usage: JM.LEASE BREAK <proj>")
	}
	proj, err := e.resolveProj(string(a[0]))
	if err != nil {
		return errFrame(err.Error())
	}
	fm := e.fence(proj)
	fm.Lock()
	old, ep, err := e.endLocked(proj, "break", p.Name, "JM.LEASE BREAK by "+p.Name)
	fm.Unlock()
	if err != nil {
		return errFrame("lease store unavailable: " + err.Error())
	}
	var b respBuf
	b.arr(3)
	if old.Holder == "" {
		b.simple("FREE")
		e.mu.Lock()
		if l := e.leases[proj]; l != nil {
			ep = l.epoch
		}
		e.mu.Unlock()
	} else {
		b.simple("BROKEN")
	}
	b.integer(int64(ep))
	b.bulk(holderStr(old.Holder, old.Host))
	return b.Bytes()
}

func sortU64(s []uint64) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
