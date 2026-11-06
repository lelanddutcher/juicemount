package teamstest

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

var bothModes = []respproxy.Mode{respproxy.ModePassthrough, respproxy.ModeEnforce}

// intentFor returns the intent whose ops add or remove name, and its
// outcome ("" when none was recorded).
func intentFor(t *testing.T, recs []auditlog.Record, name string) (auditlog.Record, string) {
	t.Helper()
	for _, r := range recs {
		if r.Type != auditlog.TypeIntent {
			continue
		}
		for _, o := range r.Ops {
			if o.Name == name && !o.Trash {
				for _, x := range recs {
					if x.Type == auditlog.TypeOutcome && x.Unit == r.Seq {
						return r, x.Outcome
					}
				}
				return r, ""
			}
		}
	}
	t.Fatalf("no intent for %q in %d records", name, len(recs))
	return auditlog.Record{}, ""
}

func refusalFor(recs []auditlog.Record, name string) *auditlog.Record {
	for i, r := range recs {
		if r.Type != auditlog.TypeRefused {
			continue
		}
		for _, o := range r.Ops {
			if o.Name == name {
				return &recs[i]
			}
		}
	}
	return nil
}

func hasProject(r auditlog.Record, ino meta.Ino, path string) bool {
	for _, p := range r.Projects {
		if p.Ino == uint64(ino) && p.Path == path {
			return true
		}
	}
	return false
}

// TestPersonGrantsThroughProxy: John may write only in /Clients/A. In both
// passthrough and enforce mode his creates, renames, attribute changes,
// writes and deletes in A are admitted and audited; the same in B, a move
// between A and B, and top-level or project creation are refused with
// nothing committed and an audited refusal. Revoking the grant applies to
// his next write without a reconnect; a client-folder grant opens every
// project under it. A Mac without a person may read but not write while
// grants are in force; the service principals keep their rights.
func TestPersonGrantsThroughProxy(t *testing.T) {
	for _, mode := range bothModes {
		t.Run(string(mode), func(t *testing.T) { personGrants(t, mode) })
	}
}

func personGrants(t *testing.T, mode respproxy.Mode) {
	e := newEnv(t, mode, johnGranted("/Clients/A"))
	john := e.client("mac-john")
	ctx := meta.Background()
	A, B, C, D, clients := e.ino["/Clients/A"], e.ino["/Clients/B"], e.ino["/Clients/C"], e.ino["/Clients/D"], e.ino["/Clients"]
	var ino, fino meta.Ino
	var attr meta.Attr
	ok := func(st syscall.Errno, what string) {
		t.Helper()
		if st != 0 {
			t.Fatalf("%s: %v, want success", what, st)
		}
	}
	refused := func(st syscall.Errno, what string) {
		t.Helper()
		if st != syscall.EROFS {
			t.Fatalf("%s: %v, want EROFS (JMDENY NOT_GRANTED)", what, st)
		}
	}

	// In A: everything works.
	ok(john.Mkdir(ctx, A, "d1", 0o755, 0o022, 0, &ino, &attr), "mkdir A/d1")
	ok(john.Create(ctx, A, "f1", 0o644, 0o022, 0, &fino, &attr), "create A/f1")
	john.Close(ctx, fino)
	ok(john.Rename(ctx, A, "f1", A, "f2", 0, &ino, &attr), "rename A/f1 -> f2")
	attr.Mode = 0o600
	ok(john.SetAttr(ctx, fino, meta.SetAttrMode, 0, &attr), "chmod A/f2")
	var sid uint64
	ok(john.NewSlice(ctx, &sid), "new slice")
	ok(john.Write(ctx, fino, 0, 0, meta.Slice{Id: sid, Size: 4, Len: 4}, time.Now()), "write A/f2")
	ok(john.Unlink(ctx, A, "f2"), "unlink A/f2")
	ok(john.Rmdir(ctx, A, "d1"), "rmdir A/d1")
	ok(john.Mkdir(ctx, A, "keep", 0o755, 0o022, 0, &ino, &attr), "mkdir A/keep")

	// In B, between A and B, and above the projects: refused, nothing
	// committed.
	b, h, g := e.ino["/Clients/B/b.txt"], e.ino["/Clients/B/h.txt"], e.ino["/Clients/A/g.txt"]
	refused(john.Mkdir(ctx, B, "x", 0o755, 0o022, 0, &ino, &attr), "mkdir B/x")
	refused(john.Create(ctx, B, "y", 0o644, 0o022, 0, &ino, &attr), "create B/y")
	refused(john.Rename(ctx, B, "b.txt", B, "b2.txt", 0, &ino, &attr), "rename in B")
	var battr meta.Attr
	if st := john.GetAttr(ctx, b, &battr); st != 0 {
		t.Fatal(st)
	}
	battr.Mode = 0o600
	refused(john.SetAttr(ctx, b, meta.SetAttrMode, 0, &battr), "chmod B/b.txt")
	ok(john.NewSlice(ctx, &sid), "new slice")
	refused(john.Write(ctx, b, 0, 0, meta.Slice{Id: sid, Size: 4, Len: 4}, time.Now()), "write B/b.txt")
	refused(john.Unlink(ctx, B, "b.txt"), "unlink B/b.txt")
	refused(john.Rename(ctx, A, "g.txt", B, "g.txt", 0, &ino, &attr), "move A -> B")
	refused(john.Rename(ctx, B, "h.txt", A, "h.txt", 0, &ino, &attr), "move B -> A")
	refused(john.Mkdir(ctx, meta.RootInode, "Top", 0o755, 0o022, 0, &ino, &attr), "mkdir /Top")
	refused(john.Mkdir(ctx, clients, "E", 0o755, 0o022, 0, &ino, &attr), "mkdir /Clients/E")
	for _, c := range []struct {
		parent meta.Ino
		name   string
		want   bool
	}{{B, "x", false}, {B, "y", false}, {B, "b2.txt", false}, {B, "b.txt", true}, {B, "h.txt", true}, {B, "g.txt", false},
		{A, "g.txt", true}, {A, "h.txt", false}, {meta.RootInode, "Top", false}, {clients, "E", false}, {A, "keep", true}} {
		if got := e.exists(c.parent, c.name); got != c.want {
			t.Errorf("after refusals: %d/%s exists = %v, want %v", c.parent, c.name, got, c.want)
		}
	}
	var after meta.Attr
	if st := e.admin.GetAttr(ctx, b, &after); st != 0 || after.Mode&0o777 != 0o644 || after.Length != 0 {
		t.Errorf("B/b.txt changed by refused writes: mode %o length %d (%v)", after.Mode, after.Length, st)
	}
	_ = h
	_ = g

	// The log: every admitted namespace change is an intent with a
	// committed outcome, carrying John and project A; every refusal is a
	// refused record; the contents write is one modified record.
	recs := e.records(auditlog.Filter{Person: "p-john"})
	for _, name := range []string{"d1", "f1", "f2", "keep"} {
		r, out := intentFor(t, recs, name)
		if out != auditlog.OutcomeCommitted || r.Principal != "mac-john" || r.PersonName != "John" || !hasProject(r, A, "/Clients/A") {
			t.Errorf("intent for %s: %+v outcome %q", name, r, out)
		}
	}
	if r, _ := intentFor(t, recs, "f2"); r.Action != "rename" {
		t.Errorf("rename recorded as %q", r.Action)
	}
	if r, _ := intentFor(t, recs, "d1"); r.Ops[0].Path != "/Clients/A/d1" || r.Action != "create" {
		t.Errorf("mkdir A/d1 recorded as %+v", r)
	}
	setattr := false
	for _, r := range recs {
		if r.Type == auditlog.TypeIntent && r.Action == "setattr" && len(r.Inodes) == 1 && r.Inodes[0] == uint64(fino) {
			setattr = true
		}
	}
	if !setattr {
		t.Error("chmod of A/f2 not recorded")
	}
	for _, name := range []string{"x", "y", "Top", "E", "g.txt", "h.txt", "b2.txt"} {
		r := refusalFor(recs, name)
		if r == nil || r.Reason != string(respproxy.ReasonNotGranted) || r.Person != "p-john" {
			t.Errorf("refusal for %s: %+v", name, r)
		}
	}
	if r := refusalFor(recs, "g.txt"); r == nil || !hasProject(*r, A, "/Clients/A") || !hasProject(*r, B, "/Clients/B") {
		t.Errorf("the refused move A -> B does not name both projects: %+v", r)
	}
	deadline := time.Now().Add(5 * time.Second)
	modified := false
	for !modified && time.Now().Before(deadline) {
		for _, r := range e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeModified}}) {
			if r.Inode == uint64(fino) && r.Committed == 1 && hasProject(r, A, "/Clients/A") {
				modified = true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !modified {
		t.Error("the contents write of A/f2 has no modified record")
	}

	// A connection John opened before the grant change.
	raw, err := net.Dial("tcp", e.paddr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	rr := bufio.NewReader(raw)
	rawDo := func(cmd string) string {
		raw.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := raw.Write([]byte(cmd + "\r\n")); err != nil {
			t.Fatal(err)
		}
		l, err := rr.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(l)
	}
	if got := rawDo("AUTH mac-john " + passOf("mac-john")); got != "+OK" {
		t.Fatalf("AUTH = %q", got)
	}

	// Revoke: the next write is refused; the connection stays.
	e.setPeople(johnGranted())
	refused(john.Mkdir(ctx, A, "after-revoke", 0o755, 0o022, 0, &ino, &attr), "mkdir A after revoke")
	if e.exists(A, "after-revoke") {
		t.Fatal("write committed after revoke")
	}
	if got := rawDo("PING"); got != "+PONG" {
		t.Fatalf("John's connection after a grant change: %q, want +PONG (not revoked)", got)
	}

	// A client-folder grant: creating a project and writing in any project.
	e.setPeople(johnGranted("/Clients"))
	ok(john.Mkdir(ctx, clients, "E", 0o755, 0o022, 0, &ino, &attr), "mkdir /Clients/E with /Clients granted")
	ok(john.Mkdir(ctx, B, "z", 0o755, 0o022, 0, &ino, &attr), "mkdir B/z with /Clients granted")
	refused(john.Mkdir(ctx, meta.RootInode, "Top", 0o755, 0o022, 0, &ino, &attr), "mkdir /Top with /Clients granted")

	// No person while grants are in force: reads, but no writes.
	anon := e.client("mac-anon")
	refused(anon.Mkdir(ctx, meta.RootInode, "AnonTop", 0o755, 0o022, 0, &ino, &attr), "anon mkdir /AnonTop")
	refused(anon.Mkdir(ctx, C, "anon", 0o755, 0o022, 0, &ino, &attr), "anon mkdir C/anon")
	if st := anon.Lookup(ctx, A, "g.txt", &ino, &attr, false); st != 0 {
		t.Fatalf("anon lookup A/g.txt: %v", st)
	}

	// Service principals keep their rights.
	ok(e.client("admin1").Mkdir(ctx, meta.RootInode, "AdminTop", 0o755, 0o022, 0, &ino, &attr), "admin mkdir /AdminTop")
	ok(e.client("mgr1").Mkdir(ctx, D, "m", 0o755, 0o022, 0, &ino, &attr), "manager mkdir D/m")
	ok(e.client("bg1").Mkdir(ctx, D, "bgdir", 0o755, 0o022, 0, &ino, &attr), "bg mkdir D/bgdir")

	recs = e.records(auditlog.Filter{})
	if r, out := intentFor(t, recs, "AdminTop"); out != auditlog.OutcomeCommitted || r.Principal != "admin1" || r.Person != "" {
		t.Errorf("admin mkdir recorded as %+v / %q", r, out)
	}
	if r := refusalFor(recs, "AnonTop"); r == nil || r.Principal != "mac-anon" || r.Person != "" || r.Reason != string(respproxy.ReasonNotGranted) {
		t.Errorf("anon mkdir recorded as %+v", r)
	}

	// The chain verifies, and a changed line is detected.
	e.proxy.Close()
	<-e.proxy.Done()
	e.audit.Close()
	rep, err := auditlog.Verify(e.auditDir)
	if err != nil || !rep.OK || rep.Records < 20 {
		t.Fatalf("verify: %+v, %v", rep, err)
	}
	segs, _ := filepath.Glob(filepath.Join(e.auditDir, "audit-*.jsonl"))
	body, err := os.ReadFile(segs[len(segs)-1])
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(body), `"name":"x"`, `"name":"X"`, 1)
	if tampered == string(body) {
		t.Fatal("no refused mkdir of x to tamper with")
	}
	if err := os.WriteFile(segs[len(segs)-1], []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	if rep, err := auditlog.Verify(e.auditDir); err != nil || rep.OK || rep.BadSeq == 0 {
		t.Fatalf("tampered log verifies: %+v, %v", rep, err)
	}
}

// TestNoPolicyKeepsTodaysBehaviour: with a version-1 principals file (no
// people) every Mac writes everywhere, as before Teams.
func TestNoPolicyKeepsTodaysBehaviour(t *testing.T) {
	for _, mode := range bothModes {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnv(t, mode, nil)
			ctx := meta.Background()
			var ino meta.Ino
			var attr meta.Attr
			for _, c := range []struct {
				who    string
				parent meta.Ino
				name   string
			}{{"mac-john", meta.RootInode, "JohnTop"}, {"mac-john", e.ino["/Clients/A"], "j"}, {"mac-john", e.ino["/Clients"], "NewJ"},
				{"mac-anon", meta.RootInode, "AnonTop"}, {"mac-anon", e.ino["/Clients/B"], "a"}} {
				if st := e.client(c.who).Mkdir(ctx, c.parent, c.name, 0o755, 0o022, 0, &ino, &attr); st != 0 {
					t.Fatalf("%s mkdir %s: %v", c.who, c.name, st)
				}
			}
		})
	}
}

// TestAuditSinkFailure: while the log cannot take records, Mac writes are
// refused (AUDIT_UNAVAILABLE) only when grants are in force; service
// principals and installs without grants keep writing. After recovery a gap
// record counts the loss and the chain verifies.
func TestAuditSinkFailure(t *testing.T) {
	t.Run("grants in force: fail closed", func(t *testing.T) {
		e := newEnv(t, respproxy.ModePassthrough, johnGranted("/Clients/A"))
		john, anon, admin := e.client("mac-john"), e.client("mac-anon"), e.client("admin1")
		ctx := meta.Background()
		var ino meta.Ino
		var attr meta.Attr
		errsBefore := e.proxy.AuditErrors()
		e.failSink.Store(true)
		// The first record after the failure is the one that finds it.
		admin.Mkdir(ctx, e.ino["/Clients/D"], "probe", 0o755, 0o022, 0, &ino, &attr)
		waitUnhealthy(t, e.audit)
		if st := john.Mkdir(ctx, e.ino["/Clients/A"], "j1", 0o755, 0o022, 0, &ino, &attr); st != syscall.EROFS {
			t.Fatalf("john mkdir with the log down: %v, want EROFS", st)
		}
		if st := anon.Mkdir(ctx, e.ino["/Clients/C"], "a1", 0o755, 0o022, 0, &ino, &attr); st != syscall.EROFS {
			t.Fatalf("anon mkdir with the log down and grants in force: %v, want EROFS", st)
		}
		if st := admin.Mkdir(ctx, e.ino["/Clients/D"], "a2", 0o755, 0o022, 0, &ino, &attr); st != 0 {
			t.Fatalf("admin mkdir with the log down: %v", st)
		}
		if e.exists(e.ino["/Clients/A"], "j1") || e.exists(e.ino["/Clients/C"], "a1") {
			t.Fatal("a refused write committed")
		}
		e.healSink.Store(true)
		waitHealthy(t, e.audit)
		refused := e.proxy.AuditErrors() - errsBefore
		if st := john.Mkdir(ctx, e.ino["/Clients/A"], "j2", 0o755, 0o022, 0, &ino, &attr); st != 0 {
			t.Fatalf("john mkdir after recovery: %v", st)
		}
		// One gap record counts the failure. A record the proxy's flush
		// appends while that gap is being written is refused too, and at
		// most one more gap record, written right after, counts it. Every
		// record the proxy saw refused is counted; the gaps count more only
		// for records the log took without a wait and then lost.
		gaps := e.records(auditlog.Filter{Types: []string{auditlog.TypeGap}})
		failures := 0
		var dropped uint64
		for _, g := range gaps {
			if g.Dropped == 0 {
				t.Fatalf("empty gap record: %+v", gaps)
			}
			if !strings.HasSuffix(g.Detail, "; refused while the log was recovering") {
				failures++
			}
			dropped += g.Dropped
		}
		if failures != 1 || len(gaps) > 2 || refused <= 0 || dropped < uint64(refused) {
			t.Fatalf("gap records = %+v (counting %d), want one for the failure and at most one more, counting at least the %d refused records", gaps, dropped, refused)
		}
		if rep, err := auditlog.Verify(e.auditDir); err != nil || !rep.OK {
			t.Fatalf("verify: %+v %v", rep, err)
		}
	})
	t.Run("no grants: log and continue", func(t *testing.T) {
		e := newEnv(t, respproxy.ModePassthrough, []respproxy.PersonEntry{{ID: "p-john", Name: "John",
			Access: respproxy.AccessAll, Devices: []string{"mac-john"}}})
		john := e.client("mac-john")
		ctx := meta.Background()
		var ino meta.Ino
		var attr meta.Attr
		e.failSink.Store(true)
		john.Mkdir(ctx, e.ino["/Clients/A"], "probe", 0o755, 0o022, 0, &ino, &attr)
		waitUnhealthy(t, e.audit)
		before := e.proxy.AuditErrors()
		if st := john.Mkdir(ctx, e.ino["/Clients/A"], "j1", 0o755, 0o022, 0, &ino, &attr); st != 0 {
			t.Fatalf("mkdir with the log down and no grants: %v, want success", st)
		}
		if !e.exists(e.ino["/Clients/A"], "j1") || e.proxy.AuditErrors() <= before {
			t.Fatalf("write not committed (%v) or the lost record not counted (%d)", e.exists(e.ino["/Clients/A"], "j1"), e.proxy.AuditErrors())
		}
	})
}

func waitUnhealthy(t *testing.T, w *auditlog.Writer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for w.Healthy() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if w.Healthy() {
		t.Fatal("the injected sink failure was not noticed")
	}
}

func waitHealthy(t *testing.T, w *auditlog.Writer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !w.Healthy() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !w.Healthy() {
		t.Fatal("the log did not recover")
	}
}
