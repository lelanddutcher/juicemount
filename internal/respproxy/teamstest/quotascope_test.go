package teamstest

// Review round 4: quotas and per-person grants. A quota above the project
// folders sums the writes of several projects; a person limited to one of
// them may not move its usage at all (any bound tied to what their Mac
// reports could be gamed or stalled real flushes), and while grants are in
// force no such quota is created. Per-project quotas work as before.

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/redis/go-redis/v9"
)

const gib = int64(1 << 30)

func used(e *env, hash string, ino meta.Ino) int64 {
	v, _ := e.rdb.HGet(context.Background(), hash, istr(ino)).Int64()
	return v
}

// waitUsed waits until cond holds for dirQuotaUsedSpace of ino.
func waitUsed(e *env, ino meta.Ino, d time.Duration, cond func(int64) bool) (int64, bool) {
	deadline := time.Now().Add(d)
	for {
		v := used(e, "dirQuotaUsedSpace", ino)
		if cond(v) {
			return v, true
		}
		if time.Now().After(deadline) {
			return v, false
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func johnMount(t *testing.T, e *env) meta.Meta {
	t.Helper()
	jm := meta.NewClient("redis://mac-john:"+passOf("mac-john")+"@"+e.paddr+"/"+strconv.Itoa(testDB), metaConf())
	if _, err := jm.Load(true); err != nil {
		t.Fatal(err)
	}
	if err := jm.NewSession(true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { jm.CloseSession() })
	return jm
}

// TestQuotaUsageAboveGrantsNeverMoves: with a quota on /Clients (created
// before grants, straight in Redis), John (granted /Clients/A) moves its
// usage by no write at all: not alone, not riding along his own dir stats,
// not after a refused or WATCH-aborted unit, an HSET reset of his own
// stats, or a negative bait (each of which bought him room in the round-4
// ledger). B stays writable.
func TestQuotaUsageAboveGrantsNeverMoves(t *testing.T) {
	for _, mode := range bothModes {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnv(t, mode, johnGranted("/Clients/A"))
			ctx := context.Background()
			clients, A := e.ino["/Clients"], e.ino["/Clients/A"]
			setQuota(t, e, "/Clients", 100*gib, 100000)
			var x meta.Ino
			var attr meta.Attr
			if st := e.client("mac-john").Create(meta.Background(), A, "own.txt", 0o644, 0o022, 0, &x, &attr); st != 0 {
				t.Fatal(st)
			}
			john := e.rawClient("mac-john")
			s0, i0 := used(e, "dirQuotaUsedSpace", clients), used(e, "dirQuotaUsedInodes", clients)
			refused := func(what string, err error) {
				t.Helper()
				if err == nil || !strings.Contains(err.Error(), string(respproxy.ReasonNotGranted)) {
					t.Errorf("%s: %v, want NOT_GRANTED", what, err)
				}
			}
			incr := func(hash string, d int64) error { return john.HIncrBy(ctx, hash, istr(clients), d).Err() }
			refused("+2^40 inodes", incr("dirQuotaUsedInodes", 1<<40))
			refused("-2^41 inodes", incr("dirQuotaUsedInodes", -(1<<41)))
			refused("+1 byte", incr("dirQuotaUsedSpace", 1))
			_, err := john.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.HIncrBy(ctx, "dirUsedSpace", istr(A), 40*gib)
				p.HIncrBy(ctx, "dirQuotaUsedSpace", istr(clients), 49*gib)
				return nil
			})
			refused("own dir stat and /Clients in one unit", err)
			refused("after the refused unit", incr("dirQuotaUsedSpace", 40*gib))
			a0 := e.rdb.HGet(ctx, "dirUsedSpace", istr(A)).Val()
			if err := john.HIncrBy(ctx, "dirUsedSpace", istr(A), 500*gib).Err(); err == nil {
				john.HSet(ctx, "dirUsedSpace", istr(A), a0)
			}
			refused("after an inflated and reset own dir stat", incr("dirQuotaUsedSpace", 500*gib))
			_, err = john.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.HIncrBy(ctx, "dirUsedInodes", istr(A), -1_000_000)
				p.HIncrBy(ctx, "dirQuotaUsedInodes", istr(clients), -1_100_000)
				return nil
			})
			refused("negative bait", err)
			refused("negative after the bait", incr("dirQuotaUsedInodes", -1_000_000))
			refused("within the old slack", incr("dirQuotaUsedInodes", 16384))
			if s, i := used(e, "dirQuotaUsedSpace", clients), used(e, "dirQuotaUsedInodes", clients); s != s0 || i != i0 {
				t.Errorf("/Clients usage %d/%d -> %d/%d", s0, i0, s, i)
			}
			m := meta.NewClient("redis://"+e.addr+"/"+strconv.Itoa(testDB), metaConf())
			if _, err := m.Load(true); err != nil {
				t.Fatal(err)
			}
			defer m.Shutdown()
			if st := m.Create(meta.Background(), e.ino["/Clients/B"], "new.txt", 0o644, 0o022, 0, &x, &attr); st != 0 {
				t.Errorf("create in B: %v", st)
			}
		})
	}
}

// TestNoQuotaAboveProjectsWhileGrantsInForce: while grants are in force,
// juicefs quota set (doSetQuota: HSET dirQuota and its usage) on the root
// or a client folder is refused for every principal but bg, admin
// included; on a project it works, and quota delete of the folder above
// (HDEL) works. Without people, quota set on a client folder works as
// before, through the proxy, with real juicefs writes flushing it.
func TestNoQuotaAboveProjectsWhileGrantsInForce(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A"))
	adm := e.client("admin1")
	set := func(path string) error {
		return adm.HandleQuota(meta.Background(), meta.QuotaSet, path, map[string]*meta.Quota{path: {MaxSpace: 1 << 40, MaxInodes: 1000}}, false, false, false)
	}
	for _, p := range []string{"/Clients", "/"} {
		if err := set(p); err == nil || !strings.Contains(err.Error(), string(respproxy.ReasonNotGranted)) {
			t.Errorf("quota set %s by admin while grants are in force: %v", p, err)
		}
	}
	if v := e.rdb.HExists(context.Background(), "dirQuota", istr(e.ino["/Clients"])).Val(); v {
		t.Error("a quota on /Clients was created")
	}
	if err := set("/Clients/A"); err != nil {
		t.Errorf("quota set on a project: %v", err)
	}
	setQuota(t, e, "/Clients", 1<<40, 1000) // created before grants
	if err := adm.HandleQuota(meta.Background(), meta.QuotaDel, "/Clients", nil, false, false, false); err != nil {
		t.Errorf("quota delete /Clients: %v", err)
	}
	for _, mode := range bothModes {
		t.Run("no people "+string(mode), func(t *testing.T) {
			e := newEnv(t, mode, nil)
			clients, A := e.ino["/Clients"], e.ino["/Clients/A"]
			adm := e.client("admin1")
			if err := adm.HandleQuota(meta.Background(), meta.QuotaSet, "/Clients", map[string]*meta.Quota{"/Clients": {MaxSpace: 1 << 50, MaxInodes: 1 << 20}}, false, false, false); err != nil {
				t.Fatal("quota set via the proxy:", err)
			}
			jm := meta.NewClient("redis://mac-anon:"+passOf("mac-anon")+"@"+e.paddr+"/"+strconv.Itoa(testDB), metaConf())
			if _, err := jm.Load(true); err != nil {
				t.Fatal(err)
			}
			if err := jm.NewSession(true); err != nil {
				t.Fatal(err)
			}
			defer jm.CloseSession()
			c := meta.Background()
			var attr meta.Attr
			var f meta.Ino
			if st := jm.Create(c, A, "big.mov", 0o644, 0o022, 0, &f, &attr); st != 0 {
				t.Fatal(st)
			}
			if st := jm.Truncate(c, f, 0, uint64(100*gib), &attr, false); st != 0 {
				t.Fatal("truncate", st)
			}
			jm.Close(c, f)
			if v, ok := waitUsed(e, clients, 10*time.Second, func(v int64) bool { return v >= 100*gib }); !ok {
				t.Errorf("after a 100 GiB truncate: /Clients %d", v)
			}
			if st := jm.Unlink(c, A, "big.mov"); st != 0 {
				t.Fatal("unlink", st)
			}
			if v, ok := waitUsed(e, clients, 10*time.Second, func(v int64) bool { return v < gib }); !ok {
				t.Errorf("after the delete: /Clients %d", v)
			}
		})
	}
}

// TestLimitedFlushStallsOnQuotaAboveProjects documents why the Manager
// does not put grants in force with a quota above the project folders, and
// reports one: juicefs flushes every quota in one transaction, so the
// proxy refuses all of John's flush, his project's quota usage included,
// for as long as the quota on /Clients exists; deleting it lets the flush
// through.
func TestLimitedFlushStallsOnQuotaAboveProjects(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A"))
	A, clients := e.ino["/Clients/A"], e.ino["/Clients"]
	setQuota(t, e, "/Clients", 1<<50, 1<<20)
	setQuota(t, e, "/Clients/A", 1<<50, 1<<20)
	jm := johnMount(t, e)
	c := meta.Background()
	var attr meta.Attr
	var f meta.Ino
	if st := jm.Create(c, A, "clip.mov", 0o644, 0o022, 0, &f, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := jm.Truncate(c, f, 0, uint64(gib), &attr, false); st != 0 {
		t.Fatal(st)
	}
	jm.Close(c, f)
	if v, ok := waitUsed(e, A, 7*time.Second, func(v int64) bool { return v >= gib }); ok {
		t.Fatalf("A's usage landed (%d) although the flush names /Clients", v)
	}
	named := false
	for _, r := range e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeRefused}}) {
		named = named || hasRow(r, "dirQuotaUsedSpace", clients)
	}
	if !named {
		t.Error("no refusal names the quota on /Clients")
	}
	if err := e.admin.HandleQuota(c, meta.QuotaDel, "/Clients", nil, false, false, false); err != nil {
		t.Fatal(err)
	}
	// juicefs loads the quotas again every heartbeat (12 s): from then on
	// its flush no longer names /Clients.
	if v, ok := waitUsed(e, A, 40*time.Second, func(v int64) bool { return v >= gib }); !ok {
		t.Errorf("A's usage after the quota on /Clients was deleted: %d", v)
	}
}

// TestPerProjectQuotasConvergeForLimitedPerson: the real juicefs cases the
// round-4 ledger refused for good now converge with per-project quotas:
// two mounts on one Mac with 100 GiB and 60 GiB in one step each and a
// delete from the other mount, and a 20 GiB folder moved between two
// granted projects with quotas.
func TestPerProjectQuotasConvergeForLimitedPerson(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A", "/Clients/B"))
	A, B := e.ino["/Clients/A"], e.ino["/Clients/B"]
	setQuota(t, e, "/Clients/A", 1<<50, 1<<20)
	setQuota(t, e, "/Clients/B", 1<<50, 1<<20)
	b0 := used(e, "dirQuotaUsedSpace", B)
	m1, m2 := johnMount(t, e), johnMount(t, e)
	c := meta.Background()
	var attr meta.Attr
	var f1, f2, dir, f3 meta.Ino
	if st := m1.Create(c, A, "one.mov", 0o644, 0o022, 0, &f1, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m2.Create(c, A, "two.mov", 0o644, 0o022, 0, &f2, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m1.Truncate(c, f1, 0, uint64(100*gib), &attr, false); st != 0 {
		t.Fatal(st)
	}
	if st := m2.Truncate(c, f2, 0, uint64(60*gib), &attr, false); st != 0 {
		t.Fatal(st)
	}
	m1.Close(c, f1)
	m2.Close(c, f2)
	if v, ok := waitUsed(e, A, 10*time.Second, func(v int64) bool { return v >= 160*gib }); !ok {
		t.Fatalf("after 160 GiB in two mounts: A %d", v)
	}
	if st := m2.Unlink(c, A, "one.mov"); st != 0 {
		t.Fatal(st)
	}
	if v, ok := waitUsed(e, A, 10*time.Second, func(v int64) bool { return v >= 60*gib && v < 61*gib }); !ok {
		t.Errorf("after deleting 100 GiB from the other mount: A %d", v)
	}
	if st := m1.Mkdir(c, A, "big", 0o755, 0o022, 0, &dir, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m1.Create(c, dir, "clip.mov", 0o644, 0o022, 0, &f3, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m1.Truncate(c, f3, 0, uint64(20*gib), &attr, false); st != 0 {
		t.Fatal(st)
	}
	m1.Close(c, f3)
	if v, ok := waitUsed(e, A, 10*time.Second, func(v int64) bool { return v >= 80*gib }); !ok {
		t.Fatalf("before the move: A %d", v)
	}
	if st := m1.Rename(c, A, "big", B, "big", 0, &dir, &attr); st != 0 {
		t.Fatal("rename", st)
	}
	if v, ok := waitUsed(e, B, 10*time.Second, func(v int64) bool { return v >= b0+20*gib }); !ok {
		t.Errorf("B after the move: %d", v)
	}
	if v, ok := waitUsed(e, A, 10*time.Second, func(v int64) bool { return v < 61*gib }); !ok {
		t.Errorf("A after the move: %d", v)
	}
	if recs := e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeRefused}}); len(recs) > 0 {
		t.Errorf("refusals: %+v", recs)
	}
}

// TestFlushAfterProxyRestartLands: a proxy restart between a delete's
// dir-stat flush and its quota flush kept the round-4 ledger refusing the
// quota flush for good. With a per-project quota it lands.
func TestFlushAfterProxyRestartLands(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A"))
	ctx := context.Background()
	A := e.ino["/Clients/A"]
	setQuota(t, e, "/Clients/A", 1<<50, 100000)
	jm := e.client("mac-john")
	var x meta.Ino
	var attr meta.Attr
	if st := jm.Create(meta.Background(), A, "own.txt", 0o644, 0o022, 0, &x, &attr); st != 0 {
		t.Fatal(st)
	}
	john := e.rawClient("mac-john")
	for _, c := range []struct {
		hash string
		d    int64
	}{{"dirUsedSpace", 20 * gib}, {"dirQuotaUsedSpace", 20 * gib}, {"dirUsedSpace", -20 * gib}} {
		if err := john.HIncrBy(ctx, c.hash, istr(A), c.d).Err(); err != nil {
			t.Fatal(c.hash, err)
		}
	}
	q0 := used(e, "dirQuotaUsedSpace", A)
	e.proxy.Close()
	<-e.proxy.Done()
	e.auditDir = e.dir + "/audit2"
	e.startProxy(johnGranted("/Clients/A"))
	if st := e.client("mac-john").Create(meta.Background(), A, "own2.txt", 0o644, 0o022, 0, &x, &attr); st != 0 {
		t.Fatal(st)
	}
	_, err := e.rawClient("mac-john").TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HIncrBy(ctx, "dirQuotaUsedSpace", istr(A), -20*gib)
		p.HIncrBy(ctx, "dirQuotaUsedInodes", istr(A), 0)
		return nil
	})
	if err != nil || used(e, "dirQuotaUsedSpace", A) != q0-20*gib {
		t.Errorf("quota flush after a proxy restart: %v (A %d -> %d)", err, q0, used(e, "dirQuotaUsedSpace", A))
	}
}
