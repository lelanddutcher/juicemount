package teamstest

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

// stallEnv is an env whose audit writer stalls every write while stalled
// is set (a disk that stopped answering).
func stallEnv(t *testing.T, people []respproxy.PersonEntry) (*env, *atomic.Bool, string) {
	t.Helper()
	v := newVolume(t)
	e := &env{volume: v, mode: respproxy.ModeEnforce}
	stalled := &atomic.Bool{}
	dir := filepath.Join(v.dir, "audit-stall")
	e.startProxy(people, func(c *respproxy.Config) {
		w, err := auditlog.Open(dir, auditlog.Options{Logf: logf, BeforeWrite: func() error {
			for stalled.Load() {
				time.Sleep(time.Millisecond)
			}
			return nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		c.Audit, c.AuditSyncTimeout = w, 300*time.Millisecond
		t.Cleanup(func() { stalled.Store(false); w.Close() })
	})
	t.Cleanup(func() { stalled.Store(false) })
	return e, stalled, dir
}

// onDisk reports whether a segment file already holds an intent adding
// name (read straight from the files, no Sync).
func onDisk(t *testing.T, dir, name string) bool {
	t.Helper()
	segs, _ := filepath.Glob(filepath.Join(dir, "audit-*.jsonl"))
	for _, s := range segs {
		b, err := os.ReadFile(s)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range strings.Split(string(b), "\n") {
			if strings.Contains(l, `"type":"intent"`) && strings.Contains(l, `"name":"`+name+`"`) {
				return true
			}
		}
	}
	return false
}

// TestIntentDurableBeforeForward: while grants are in force a Mac's intent
// is on disk when its change is acknowledged; with the disk stalled the
// change is refused (AUDIT_UNAVAILABLE) after AuditSyncTimeout and nothing
// is committed, and the late intent gets a "not forwarded" outcome. A
// service principal's intent, and every intent while no grants are in
// force, is not waited for.
func TestIntentDurableBeforeForward(t *testing.T) {
	t.Run("grants in force", func(t *testing.T) {
		e, stalled, dir := stallEnv(t, johnGranted("/Clients/A"))
		john, admin := e.client("mac-john"), e.client("admin1")
		ctx := meta.Background()
		A := e.ino["/Clients/A"]
		var ino meta.Ino
		var attr meta.Attr
		if st := john.Mkdir(ctx, A, "durable", 0o755, 0o022, 0, &ino, &attr); st != 0 {
			t.Fatal(st)
		}
		if !onDisk(t, dir, "durable") {
			t.Fatal("the change was acknowledged before its intent was on disk")
		}
		stalled.Store(true)
		t0 := time.Now()
		st := john.Mkdir(ctx, A, "undurable", 0o755, 0o022, 0, &ino, &attr)
		if st != syscall.EROFS || e.exists(A, "undurable") {
			t.Fatalf("mkdir with the disk stalled: %v (committed %v), want EROFS and nothing committed", st, e.exists(A, "undurable"))
		}
		if d := time.Since(t0); d > 5*time.Second {
			t.Fatalf("the refusal took %s", d)
		}
		if st := admin.Mkdir(ctx, e.ino["/Clients/D"], "adm", 0o755, 0o022, 0, &ino, &attr); st != 0 {
			t.Fatalf("admin mkdir with the disk stalled: %v (not waited for)", st)
		}
		stalled.Store(false)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			p, err := auditlog.Query(dir, auditlog.Filter{Person: "p-john"}, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range p.Records {
				if r.Type != auditlog.TypeIntent || len(r.Ops) == 0 || r.Ops[0].Name != "undurable" {
					continue
				}
				for _, o := range p.Records {
					if o.Type == auditlog.TypeOutcome && o.Unit == r.Seq && o.Outcome == auditlog.OutcomeRefused &&
						strings.Contains(o.Reason, "not forwarded") {
						return
					}
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("the late intent has no \"not forwarded\" outcome")
	})
	t.Run("no grants", func(t *testing.T) {
		e, stalled, _ := stallEnv(t, []respproxy.PersonEntry{{ID: "p-john", Name: "John", Access: respproxy.AccessAll,
			Devices: []string{"mac-john"}}})
		john := e.client("mac-john")
		var ino meta.Ino
		var attr meta.Attr
		stalled.Store(true)
		if st := john.Mkdir(meta.Background(), e.ino["/Clients/A"], "async", 0o755, 0o022, 0, &ino, &attr); st != 0 {
			t.Fatalf("mkdir with the disk stalled and no grants: %v, want success (not waited for)", st)
		}
	})
}

// TestAuditLogStartsWithFirstPerson: a Lazy log writes nothing (not even
// its directory) while the principals table names nobody, and starts with
// the first unit after a person exists.
func TestAuditLogStartsWithFirstPerson(t *testing.T) {
	v := newVolume(t)
	e := &env{volume: v, mode: respproxy.ModePassthrough}
	dir := filepath.Join(v.dir, "audit-lazy")
	var lazy *auditlog.Writer
	e.startProxy(nil, func(c *respproxy.Config) {
		var err error
		lazy, err = auditlog.Open(dir, auditlog.Options{Logf: logf, Lazy: true, HealthPath: dir + ".health"})
		if err != nil {
			t.Fatal(err)
		}
		c.Audit = lazy
		t.Cleanup(func() { lazy.Close() })
	})
	ctx := meta.Background()
	var ino meta.Ino
	var attr meta.Attr
	if st := e.client("mac-john").Mkdir(ctx, e.ino["/Clients/A"], "before", 0o755, 0o022, 0, &ino, &attr); st != 0 {
		t.Fatal(st)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("the audit directory exists with no person: %v", err)
	}
	if h, err := auditlog.ReadHealth(dir + ".health"); err != nil || h.Active {
		t.Fatalf("health with no person: %+v, %v", h, err)
	}
	e.setPeople([]respproxy.PersonEntry{{ID: "p-john", Name: "John", Access: respproxy.AccessAll, Devices: []string{"mac-john"}}})
	if st := e.client("mac-john").Mkdir(ctx, e.ino["/Clients/A"], "after", 0o755, 0o022, 0, &ino, &attr); st != 0 {
		t.Fatal(st)
	}
	if err := lazy.Sync(); err != nil {
		t.Fatal(err)
	}
	if !onDisk(t, dir, "after") || onDisk(t, dir, "before") {
		t.Fatal("the log did not start with the first person")
	}
	if !lazy.Active() {
		t.Fatal("the writer is not active")
	}
}
