package respproxy

// audit.go feeds the per-person mutation log (internal/auditlog).
//
// For every admitted unit with a namespace or attribute change the proxy
// appends an INTENT record before it forwards the unit, and an OUTCOME
// record (committed / refused / unknown) once Redis answered or the reply
// was lost. A crash between the two leaves an intent without an outcome:
// the change may or may not have been applied. Units that only write file
// contents (chunk lists) are collapsed into one MODIFIED record per
// (principal, inode, window). Refused units get one REFUSED record.
// Dir-stat flushes, locks and cleanup records are not logged: they change
// no name, attribute or content.
//
// Appending never waits for the disk (auditlog.Writer batches the fsync).
// If the log cannot take a record, the unit is refused (AUDIT_UNAVAILABLE)
// only when grants are in force and the writer is a Mac; otherwise the
// failure is counted and logged and the write goes ahead.

import (
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

func classifyAudit(u *Unit) auditClass {
	var dentry, chunk, attrs bool
	for _, w := range u.Project {
		switch w.Key.Shape {
		case ShapeDentry:
			dentry = true
		case ShapeChunk:
			chunk = true
		case ShapeInode, ShapeXattr, ShapeSymlink, ShapeParent:
			attrs = true
		}
	}
	switch {
	case dentry:
		return auditNamespace
	case chunk:
		return auditData // juicefs stores the new length (i<n>) with the slice
	case attrs:
		return auditNamespace
	}
	return auditNone
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

func newAuditSink(w *auditlog.Writer, window time.Duration, logf func(string, ...any)) *auditSink {
	if window <= 0 {
		window = 10 * time.Second
	}
	s := &auditSink{w: w, window: window, logf: logf, now: time.Now, agg: map[aggKey]*aggEnt{},
		stop: make(chan struct{}), done: make(chan struct{})}
	go s.loop()
	return s
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
	s := c.p.audit
	if s == nil || len(u.Project) == 0 {
		return next, nil
	}
	cls := classifyAudit(u)
	if cls == auditNone {
		return next, nil
	}
	failClosed := c.principal.Kind == KindMac && c.p.Auth.table.Load().GrantsInForce()
	unavailable := func(err error) *Denial {
		return &Denial{Reason: ReasonAuditUnavailable, Detail: "audit log unavailable while grants are in force: " + err.Error()}
	}
	base := c.baseRecord(u)
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
	rec.Type, rec.Action, rec.Projects, rec.Ops = auditlog.TypeIntent, d.action, d.projects, d.ops
	if len(d.ops) == 0 {
		rec.Inodes = d.inodes
	}
	seq, err := s.w.Append(&rec)
	if err != nil {
		s.failed(err)
		if failClosed {
			return nil, unavailable(err)
		}
		return next, nil
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
	s := c.p.audit
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
