package respproxy

// audit.go feeds the per-person mutation log (internal/auditlog).
//
// The log is on once the writer is active. The jm-respproxy entrypoint
// opens it Lazy (JM_AUDIT unset or "auto"): nothing is written until the
// principals table names a person (or grants are in force); JM_AUDIT=on
// logs from the start, JM_AUDIT=off never.
//
// For every admitted unit with a namespace or attribute change the proxy
// appends an INTENT record before it forwards the unit, and an OUTCOME
// record (committed / refused / unknown) once Redis answered or the reply
// was lost. A crash between the two leaves an intent without an outcome:
// the change may or may not have been applied. Units that only write file
// contents (chunk lists, and the file's own length and times) are collapsed
// into one MODIFIED record per (principal, inode, window). A unit that
// writes contents AND anything else, an inode, xattr, symlink or link
// parent of an inode whose contents it does not write, or the mode, owner,
// group, flags or ACL of the inode it does write (compared with the stored
// value by a side read), is a namespace unit with an intent; its attribute
// changes are listed in the intent (Record.Attrs). Refused units get one
// REFUSED record. Dir-stat flushes, locks and cleanup records are not
// logged: they change no name, attribute or content.
//
// Durability. While grants are in force, a Mac's intent is on disk before
// its unit is forwarded: the proxy waits for the fsync of the batch that
// carries it (auditlog group commit: intents of every connection queued
// meanwhile share one write and one fsync), at most AuditSyncTimeout, and
// refuses the unit (AUDIT_UNAVAILABLE) if it is not durable by then. Every
// other record (intents of service principals, every intent while no
// grants are in force, outcomes, refusals, modified records) is queued
// without waiting: a crash can lose the last fraction of a second of them,
// so a change can be in Redis with no record of it.
//
// If the log cannot take a record, the unit is refused (AUDIT_UNAVAILABLE)
// only when grants are in force and the writer is a Mac; otherwise the
// failure is counted and logged and the write goes ahead.

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
)

// unitAudit is the lease engine's description of a unit for the log,
// computed from the placement it already did.
type unitAudit struct {
	action   string
	projects []auditlog.Project
	ops      []auditlog.Op
	inodes   []uint64
}

// describe builds the log description of a placed unit.
func (e *LeaseEngine) describe(f *unitFacts, locs []location) *unitAudit {
	a := describeFacts(f, e.anc.label)
	seen := map[uint64]bool{}
	for _, l := range locs {
		if l.kind != nsProject || seen[l.proj] {
			continue
		}
		seen[l.proj] = true
		a.projects = append(a.projects, auditlog.Project{Ino: l.proj, Path: e.anc.label(l.proj)})
	}
	sort.Slice(a.projects, func(i, j int) bool { return a.projects[i].Ino < a.projects[j].Ino })
	return a
}

// describeFacts describes a unit from its own commands (no placement).
// label returns a directory's display path, or "".
func describeFacts(f *unitFacts, label func(uint64) string) *unitAudit {
	a := &unitAudit{inodes: f.inodes}
	var adds, removes, trashAdds, trashRemoves int
	for _, op := range f.dentries {
		o := auditlog.Op{Op: "add", Parent: op.dir, Name: op.name, Ino: op.ino, Dir: op.isDir, Trash: op.trash}
		if op.del {
			o.Op, o.Ino, o.Dir = "remove", 0, false
		}
		if !op.trash && label != nil {
			if p := label(op.dir); p != "" {
				o.Path = joinPath(p, op.name)
			}
		}
		a.ops = append(a.ops, o)
		switch {
		case op.trash && op.del:
			trashRemoves++
		case op.trash:
			trashAdds++
		case op.del:
			removes++
		default:
			adds++
		}
	}
	switch {
	case len(f.dentries) == 0:
		a.action = "setattr"
	case trashAdds > 0 && removes > 0:
		a.action = "trash"
	case trashRemoves > 0 && adds > 0:
		a.action = "restore"
	case adds > 0 && removes > 0:
		a.action = "rename"
	case adds > 0 && len(f.links) > 0:
		a.action = "link"
	case adds > 0:
		a.action = "create"
	case removes > 0:
		a.action = "remove"
	default:
		a.action = "other"
	}
	return a
}

func joinPath(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}

// auditClass is how a unit is logged.
type auditClass int

const (
	auditNone      auditClass = iota // stats, locks, cleanup records
	auditData                        // file contents only: collapsed
	auditNamespace                   // names or attributes: intent + outcome
)

// classifyAudit classifies a unit from its keys alone. A unit that writes
// contents and an inode's own key of one of those inodes (juicefs stores
// the new length with the slice) is auditData only if that inode's
// attributes do not change: attrCheck lists those writes for the side read
// (auditAttrs).
func classifyAudit(u *Unit) (cls auditClass, attrCheck []KeyWrite) {
	chunks := map[uint64]bool{}
	var dentry, other, inode bool
	for _, w := range u.Project {
		switch w.Key.Shape {
		case ShapeDentry:
			dentry = true
		case ShapeChunk:
			chunks[w.Key.Ino] = true
		case ShapeXattr, ShapeSymlink, ShapeParent:
			other = true
		case ShapeInode:
			inode = true
		}
	}
	switch {
	case dentry || other:
		return auditNamespace, nil
	case len(chunks) == 0 && inode:
		return auditNamespace, nil
	case len(chunks) == 0:
		return auditNone, nil
	}
	for _, w := range u.Project {
		if w.Key.Shape != ShapeInode {
			continue
		}
		if !chunks[w.Key.Ino] {
			return auditNamespace, nil // another inode's attributes
		}
		attrCheck = append(attrCheck, w)
	}
	return auditData, attrCheck
}

// auditAttrs compares the attributes the unit's SET i<n> writes with the
// stored ones (one MGET side read). For a data unit (all) is false and
// writes holds the inode writes classifyAudit found; anything it cannot
// prove unchanged (another command, an unreadable or missing value) is a
// change. For a namespace unit (all) every SET i<n> of an existing inode is
// compared and only proven changes are listed.
func (c *conn) auditAttrs(u *Unit, writes []KeyWrite, all bool) []auditlog.AttrChange {
	if all {
		for _, w := range u.Project {
			if w.Key.Shape == ShapeInode {
				writes = append(writes, w)
			}
		}
	}
	var out []auditlog.AttrChange
	type cmp struct {
		ino uint64
		val []byte
	}
	var todo []cmp
	args := [][]byte{[]byte("MGET")}
	for _, w := range writes {
		cm := u.Cmds[w.Cmd]
		name := cm.Info.Name
		if (name != "SET" && name != "SETNX") || len(cm.Args) < 3 || string(cm.Args[1]) != w.Raw {
			if !all {
				out = append(out, auditlog.AttrChange{Ino: w.Key.Ino, Field: "attr", New: "written by " + name})
			}
			continue
		}
		todo = append(todo, cmp{w.Key.Ino, cm.Args[2]})
		args = append(args, []byte(w.Raw))
	}
	if len(todo) == 0 {
		return out
	}
	v, err := c.p.side.Do(u.DB, args...)
	if err == nil {
		err = v.Err()
	}
	if err == nil && len(v.Elems) != len(todo) {
		err = fmt.Errorf("MGET returned %d values for %d keys", len(v.Elems), len(todo))
	}
	if err != nil {
		if !all {
			for _, t := range todo {
				out = append(out, auditlog.AttrChange{Ino: t.ino, Field: "attr", New: "stored value unreadable: " + err.Error()})
			}
		}
		return out
	}
	for i, t := range todo {
		old := v.Elems[i]
		if old.Null {
			if !all {
				out = append(out, auditlog.AttrChange{Ino: t.ino, Field: "attr", New: "set on an inode that does not exist"})
			}
			continue
		}
		out = append(out, attrDiff(t.ino, old.Str, t.val)...)
	}
	return out
}

// attrDiff lists the security-relevant attributes that differ between two
// marshalled JuiceFS v1.3.1 attrs (pkg/meta/interface.go Attr.Marshal:
// flags at 0, type<<12|mode at 1, uid at 3, gid at 7, and the access and
// default ACL ids at 72 when the value is 80 bytes long).
func attrDiff(ino uint64, a, b []byte) []auditlog.AttrChange {
	okLen := func(v []byte) bool { return len(v) == 72 || len(v) == 80 }
	if !okLen(a) || !okLen(b) {
		return []auditlog.AttrChange{{Ino: ino, Field: "attr", Old: fmt.Sprintf("%d bytes", len(a)), New: fmt.Sprintf("%d bytes", len(b))}}
	}
	var out []auditlog.AttrChange
	add := func(field, o, n string) {
		if o != n {
			out = append(out, auditlog.AttrChange{Ino: ino, Field: field, Old: o, New: n})
		}
	}
	ma, mb := binary.BigEndian.Uint16(a[1:3]), binary.BigEndian.Uint16(b[1:3])
	add("type", fmt.Sprint(ma>>12), fmt.Sprint(mb>>12))
	add("mode", fmt.Sprintf("%04o", ma&0o7777), fmt.Sprintf("%04o", mb&0o7777))
	add("uid", fmt.Sprint(binary.BigEndian.Uint32(a[3:7])), fmt.Sprint(binary.BigEndian.Uint32(b[3:7])))
	add("gid", fmt.Sprint(binary.BigEndian.Uint32(a[7:11])), fmt.Sprint(binary.BigEndian.Uint32(b[7:11])))
	add("flags", fmt.Sprintf("%#x", a[0]), fmt.Sprintf("%#x", b[0]))
	acl := func(v []byte) string {
		if len(v) < 80 {
			return "none"
		}
		return fmt.Sprintf("%d/%d", binary.BigEndian.Uint32(v[72:76]), binary.BigEndian.Uint32(v[76:80]))
	}
	add("acl", acl(a), acl(b))
	return out
}

// chunkInodes lists the inodes whose chunk lists a unit writes.
func chunkInodes(u *Unit) []uint64 {
	var out []uint64
	seen := map[uint64]bool{}
	for _, w := range u.Project {
		if w.Key.Shape == ShapeChunk && !seen[w.Key.Ino] {
			seen[w.Key.Ino] = true
			out = append(out, w.Key.Ino)
		}
	}
	return out
}

// auditSink is the proxy's side of the log: the writer and the collapse
// of content writes.
type auditSink struct {
	w      *auditlog.Writer
	window time.Duration
	logf   func(string, ...any)
	now    func() time.Time

	mu  sync.Mutex
	agg map[aggKey]*aggEnt

	errs    atomic.Int64
	lastLog atomic.Int64 // unix ms of the last error log line
	active  atomic.Bool  // the writer was activated (auditOn)
	// want: the principals table asks for the log (a person, or grants
	// in force); the loop activates a Lazy writer once it does.
	want func() bool
	// syncTimeout bounds the wait for a durable intent.
	syncTimeout time.Duration

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

type aggKey struct {
	principal string
	ino       uint64
}

type aggEnt struct {
	rec auditlog.Record
}

// maxAgg bounds the collapse map; past it everything is flushed early.
const maxAgg = 1 << 16

func newAuditSink(w *auditlog.Writer, window, syncTimeout time.Duration, want func() bool, logf func(string, ...any)) *auditSink {
	if window <= 0 {
		window = 10 * time.Second
	}
	if syncTimeout <= 0 {
		syncTimeout = 2 * time.Second
	}
	s := &auditSink{w: w, window: window, syncTimeout: syncTimeout, want: want, logf: logf, now: time.Now, agg: map[aggKey]*aggEnt{},
		stop: make(chan struct{}), done: make(chan struct{})}
	s.active.Store(w.Active())
	go s.loop()
	return s
}

// auditOn returns the sink while the log is on (nil: no log).
func (p *Proxy) auditOn() *auditSink {
	s := p.audit
	if s == nil || !s.activate() {
		return nil
	}
	return s
}

// activate reports whether the log is on; a Lazy writer is activated by
// the first unit (or loop tick) that sees a table with a person or with
// grants in force, and stays on from then.
func (s *auditSink) activate() bool {
	if s.active.Load() {
		return true
	}
	if s.want == nil || !s.want() {
		return false
	}
	if err := s.w.Activate(); err != nil {
		s.failed(err)
	}
	s.active.Store(true)
	return true
}

func (s *auditSink) loop() {
	defer close(s.done)
	t := time.NewTicker(s.window / 2)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			s.flush(true)
			return
		case <-t.C:
			s.activate()
			s.flush(false)
		}
	}
}

// close flushes every open window. Call after every connection ended.
func (s *auditSink) close() {
	s.once.Do(func() { close(s.stop) })
	<-s.done
}

// flush appends the windows that are older than the window length (all of
// them when all is set).
func (s *auditSink) flush(all bool) {
	cut := s.now().Add(-s.window).UnixMilli()
	s.mu.Lock()
	var out []*auditlog.Record
	for k, e := range s.agg {
		if all || e.rec.First <= cut {
			r := e.rec
			out = append(out, &r)
			delete(s.agg, k)
		}
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].First < out[j].First })
	for _, r := range out {
		if _, err := s.w.Append(r); err != nil {
			s.failed(err)
		}
	}
}

// failed counts a record the log did not take, logging at most once a
// minute.
func (s *auditSink) failed(err error) {
	s.errs.Add(1)
	now := time.Now().UnixMilli()
	if last := s.lastLog.Load(); now-last > 60_000 && s.lastLog.CompareAndSwap(last, now) {
		s.logf("respproxy: audit log did not take a record (%d so far): %v", s.errs.Load(), err)
	}
}

// noteData adds one content write of each inode to its window.
func (s *auditSink) noteData(base auditlog.Record, inodes []uint64, o Outcome) {
	now := s.now().UnixMilli()
	full := false
	s.mu.Lock()
	for _, ino := range inodes {
		k := aggKey{base.Principal, ino}
		e := s.agg[k]
		if e == nil {
			e = &aggEnt{rec: base}
			e.rec.Type, e.rec.Inode, e.rec.First = auditlog.TypeModified, ino, now
			s.agg[k] = e
		}
		e.rec.Last = now
		e.rec.Count++
		switch outcomeOf(o) {
		case auditlog.OutcomeCommitted:
			e.rec.Committed++
		case auditlog.OutcomeUnknown:
			e.rec.Unknown++
		default:
			e.rec.NotCommitted++
		}
	}
	full = len(s.agg) >= maxAgg
	s.mu.Unlock()
	if full {
		s.flush(true)
	}
}

func outcomeOf(o Outcome) string {
	switch {
	case o.Lost:
		return auditlog.OutcomeUnknown
	case o.Committed:
		return auditlog.OutcomeCommitted
	}
	return auditlog.OutcomeRefused
}

// baseRecord carries who wrote a unit.
func (c *conn) baseRecord(u *Unit) auditlog.Record {
	pr := c.principal
	r := auditlog.Record{Principal: pr.Name, Kind: string(pr.Kind), Host: pr.Host}
	if u.Person != nil {
		r.Person, r.PersonName = u.Person.ID, u.Person.Name
	}
	return r
}

func (c *conn) unitDescription(u *Unit) *unitAudit {
	if u.audit != nil {
		return u.audit
	}
	return describeFacts(analyzeUnit(u), nil)
}

// auditStart logs an admitted unit before it is forwarded. It returns the
// Done to install in place of next, or a refusal when the log cannot take
// the intent and grants are in force (the unit is then not forwarded).
func (c *conn) auditStart(u *Unit, next func(Outcome)) (func(Outcome), *Denial) {
	s := c.p.auditOn()
	if s == nil || len(u.Project) == 0 {
		return next, nil
	}
	cls, attrCheck := classifyAudit(u)
	if cls == auditNone {
		return next, nil
	}
	failClosed := c.principal.Kind == KindMac && c.p.Auth.table.Load().GrantsInForce()
	unavailable := func(err error) *Denial {
		return &Denial{Reason: ReasonAuditUnavailable, Detail: "audit log unavailable while grants are in force: " + err.Error()}
	}
	base := c.baseRecord(u)
	var attrs []auditlog.AttrChange
	if cls == auditData {
		if attrs = c.auditAttrs(u, attrCheck, false); len(attrs) > 0 {
			cls = auditNamespace // contents and attributes: an intent
		}
	} else {
		attrs = c.auditAttrs(u, nil, true)
	}
	if cls == auditData {
		if failClosed && !s.w.Healthy() {
			return nil, unavailable(auditlog.ErrUnavailable)
		}
		base.Projects = c.unitDescription(u).projects
		inodes := chunkInodes(u)
		return func(o Outcome) {
			s.noteData(base, inodes, o)
			if next != nil {
				next(o)
			}
		}, nil
	}
	d := c.unitDescription(u)
	rec := base
	rec.Type, rec.Action, rec.Projects, rec.Ops, rec.Attrs = auditlog.TypeIntent, d.action, d.projects, d.ops, attrs
	if len(d.ops) == 0 {
		rec.Inodes = d.inodes
	}
	var seq uint64
	var err error
	if failClosed {
		// The intent is on disk before the unit reaches Redis.
		seq, err = s.w.AppendDurable(&rec, s.syncTimeout)
	} else {
		seq, err = s.w.Append(&rec)
	}
	if err != nil {
		s.failed(err)
		if !failClosed {
			return next, nil
		}
		if seq != 0 {
			// Queued but not durable in time: it may still be written,
			// so say what became of it.
			out := c.baseOutcome(base, seq, Outcome{})
			out.Reason = "not forwarded: the intent was not on disk in time"
			if _, err := s.w.Append(out); err != nil {
				s.failed(err)
			}
		}
		return nil, unavailable(err)
	}
	return func(o Outcome) {
		out := c.baseOutcome(base, seq, o)
		if _, err := s.w.Append(out); err != nil {
			s.failed(err)
		}
		if next != nil {
			next(o)
		}
	}, nil
}

func (c *conn) baseOutcome(base auditlog.Record, seq uint64, o Outcome) *auditlog.Record {
	r := auditlog.Record{Type: auditlog.TypeOutcome, Unit: seq, Principal: base.Principal, Person: base.Person,
		Outcome: outcomeOf(o)}
	switch {
	case r.Outcome == auditlog.OutcomeRefused && !o.Forwarded:
		r.Reason = "not forwarded"
	case r.Outcome == auditlog.OutcomeRefused:
		r.Reason = "not applied (error reply or WATCH abort)"
	}
	return &r
}

// auditRefused logs a refused unit that writes anything.
func (c *conn) auditRefused(u *Unit, d *Denial) {
	s := c.p.auditOn()
	if s == nil || len(u.Writes) == 0 || d.Reason == ReasonAuditUnavailable {
		return
	}
	r := c.baseRecord(u)
	r.Type, r.Outcome, r.Reason, r.Detail = auditlog.TypeRefused, auditlog.OutcomeRefused, string(d.Reason), d.Detail
	if len(u.Project) > 0 {
		desc := c.unitDescription(u)
		r.Action, r.Projects, r.Ops = desc.action, desc.projects, desc.ops
		if len(desc.ops) == 0 {
			r.Inodes = desc.inodes
		}
	}
	if d.Proj != 0 && len(r.Projects) == 0 {
		r.Projects = []auditlog.Project{{Ino: d.Proj}}
	}
	if _, err := s.w.Append(&r); err != nil {
		s.failed(err)
	}
}

// AuditErrors is how many records the audit log did not take.
func (p *Proxy) AuditErrors() int64 {
	if p.audit == nil {
		return 0
	}
	return p.audit.errs.Load()
}
