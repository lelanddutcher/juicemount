package respproxy

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
)

// TestSingleUserVolumeHasNoAuditWork: with the log Lazy (JM_AUDIT=auto)
// and no person in the principals table, as on every single-user volume,
// the proxy writes nothing for the log (no directory, no health file) and
// asks the lease engine for no per-unit description. Both start with the
// first person.
func TestSingleUserVolumeHasNoAuditWork(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "audit")
	w, err := auditlog.Open(dir, auditlog.Options{Lazy: true, HealthPath: dir + ".health", Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	v1, err := ParsePrincipals(principalsBody(t, 0, ""))
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(Config{Upstream: "127.0.0.1:1", Principals: v1, Mode: ModePassthrough, VolumeDB: testDB,
		Logf: func(string, ...any) {}, Audit: w})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c := &conn{p: p, principal: &Principal{Name: "mac1", Kind: KindMac}}
	if u := c.newUnit(UnitBare, nil); u.wantAudit {
		t.Error("a unit asks for an audit description while the log is off")
	}
	p.audit.flush(true)
	for _, path := range []string{dir, dir + ".health"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("%s exists on a volume with no person: %v", path, err)
		}
	}
	v2, err := ParsePrincipals(principalsBody(t, 2, `[{"id":"p-a","access":"all","devices":["mac1"]}]`))
	if err != nil {
		t.Fatal(err)
	}
	p.Auth.table.Store(v2)
	if u := c.newUnit(UnitBare, nil); !u.wantAudit || !w.Active() {
		t.Errorf("with a person: wantAudit %v, log active %v", u.wantAudit, w.Active())
	}
	if _, err := os.Stat(dir + ".health"); err != nil {
		t.Errorf("no health file once the log is on: %v", err)
	}
}
