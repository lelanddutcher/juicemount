package teamstest

// Review round 3: dir-stat and quota-usage rows that ride along a granted
// write, and the audit records of units that write such rows.

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

func hasRow(r auditlog.Record, hash string, ino meta.Ino) bool {
	for _, x := range r.Rows {
		if x.Hash == hash && x.Ino == uint64(ino) {
			return true
		}
	}
	return false
}

// TestGrantChecksStatRowsRidingAlong: John (granted /Clients/A) writes
// another project's dir-stat or quota-usage rows, or a quota-usage row of
// a folder above the projects, in a transaction that also writes his own
// file in A. The lease rules let rows ride along a unit that writes an
// inode, and the grant check did too, so John could set B's quota usage
// (B then refuses every create with EDQUOT). Every such row is now placed:
// refused, nothing written, and the refusal names the row. Rows of A and
// the dir-stat counters above the projects pass; the quota usage of the
// folder that holds A does not (review round 4: quotascope_test.go).
func TestGrantChecksStatRowsRidingAlong(t *testing.T) {
	for _, mode := range bothModes {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnv(t, mode, johnGranted("/Clients/A"))
			ctx := context.Background()
			other := e.mustMkdir(e.admin, "/", "Other")
			A, B, clients := e.ino["/Clients/A"], e.ino["/Clients/B"], e.ino["/Clients"]
			g := e.ino["/Clients/A/g.txt"]
			s := func(i meta.Ino) string { return strconv.FormatUint(uint64(i), 10) }
			john := e.rawClient("mac-john")
			graw, err := e.rdb.Get(ctx, inoKey("i", g)).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			along := func(f func(p redis.Pipeliner)) error {
				_, err := john.TxPipelined(ctx, func(p redis.Pipeliner) error {
					p.Set(ctx, inoKey("i", g), graw, 0) // unchanged attr of John's own file
					f(p)
					return nil
				})
				return err
			}
			alone := func(f func(p redis.Pipeliner)) error {
				_, err := john.TxPipelined(ctx, func(p redis.Pipeliner) error { f(p); return nil })
				return err
			}
			for _, c := range []struct {
				what  string
				run   func(func(redis.Pipeliner)) error
				write func(p redis.Pipeliner)
				hash  string
				dir   meta.Ino
				ok    bool
			}{
				{"HSET dirQuotaUsedSpace B along A", along, func(p redis.Pipeliner) { p.HSet(ctx, "dirQuotaUsedSpace", s(B), 1<<50) }, "dirQuotaUsedSpace", B, false},
				{"HINCRBY dirUsedInodes B along A", along, func(p redis.Pipeliner) { p.HIncrBy(ctx, "dirUsedInodes", s(B), 1<<40) }, "dirUsedInodes", B, false},
				{"HSET dirQuotaUsedInodes B alone", alone, func(p redis.Pipeliner) { p.HSet(ctx, "dirQuotaUsedInodes", s(B), 1<<40) }, "dirQuotaUsedInodes", B, false},
				{"HSET dirQuotaUsedInodes /Clients along A", along, func(p redis.Pipeliner) { p.HSet(ctx, "dirQuotaUsedInodes", s(clients), 1<<40) }, "dirQuotaUsedInodes", clients, false},
				{"HDEL dirQuotaUsedSpace /Clients alone", alone, func(p redis.Pipeliner) { p.HDel(ctx, "dirQuotaUsedSpace", s(clients)) }, "dirQuotaUsedSpace", clients, false},
				{"HINCRBY dirQuotaUsedSpace /Other alone", alone, func(p redis.Pipeliner) { p.HIncrBy(ctx, "dirQuotaUsedSpace", s(other), 1<<40) }, "dirQuotaUsedSpace", other, false},
				{"HINCRBY dirQuotaUsedSpace /Other along A", along, func(p redis.Pipeliner) { p.HIncrBy(ctx, "dirQuotaUsedSpace", s(other), 1<<40) }, "dirQuotaUsedSpace", other, false},
				{"HINCRBY dirQuotaUsedSpace A along A", along, func(p redis.Pipeliner) { p.HIncrBy(ctx, "dirQuotaUsedSpace", s(A), 0) }, "dirQuotaUsedSpace", A, true},
				{"HINCRBY dirQuotaUsedInodes /Clients alone", alone, func(p redis.Pipeliner) { p.HIncrBy(ctx, "dirQuotaUsedInodes", s(clients), 0) }, "dirQuotaUsedInodes", clients, false},
				{"HINCRBY dirUsedSpace /Other alone", alone, func(p redis.Pipeliner) { p.HIncrBy(ctx, "dirUsedSpace", s(other), 0) }, "dirUsedSpace", other, true},
			} {
				before, _ := e.rdb.HGet(ctx, c.hash, s(c.dir)).Result()
				err := c.run(c.write)
				after, _ := e.rdb.HGet(ctx, c.hash, s(c.dir)).Result()
				notGranted := err != nil && strings.Contains(err.Error(), string(respproxy.ReasonNotGranted))
				switch {
				case !c.ok && (!notGranted || after != before):
					t.Errorf("%s: err %v, row %q -> %q; want NOT_GRANTED and the row unchanged", c.what, err, before, after)
				case c.ok && err != nil:
					t.Errorf("%s: %v, want it admitted", c.what, err)
				}
			}
			recs := e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeRefused}})
			found := false
			for _, r := range recs {
				found = found || hasRow(r, "dirQuotaUsedSpace", B)
			}
			if !found {
				t.Errorf("no refusal names B's dirQuotaUsedSpace row: %+v", recs)
			}
		})
	}
}

// TestQuotaUsageOfAnotherProjectStaysWritable: with a quota on
// /Clients/B, John (granted A) cannot inflate B's quota usage riding along
// a write in A, so a client can still create in B; his own writes in A
// still flush the usage of the quota on A. (A quota above the projects is
// not supported while grants are in force: quotascope_test.go.)
func TestQuotaUsageOfAnotherProjectStaysWritable(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A"))
	ctx := context.Background()
	A, B := e.ino["/Clients/A"], e.ino["/Clients/B"]
	for _, q := range []string{"/Clients/B", "/Clients/A"} {
		if err := e.admin.HandleQuota(meta.Background(), meta.QuotaSet, q,
			map[string]*meta.Quota{q: {MaxSpace: 1 << 30, MaxInodes: 1000}}, false, false, false); err != nil {
			t.Fatal(err)
		}
	}
	g := e.ino["/Clients/A/g.txt"]
	john := e.rawClient("mac-john")
	graw, _ := e.rdb.Get(ctx, inoKey("i", g)).Bytes()
	_, err := john.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, inoKey("i", g), graw, 0)
		p.HSet(ctx, "dirQuotaUsedInodes", strconv.FormatUint(uint64(B), 10), 1<<40)
		return nil
	})
	if err == nil {
		t.Error("John's transaction inflating B's quota usage was admitted")
	}
	m := meta.NewClient("redis://"+e.addr+"/"+strconv.Itoa(testDB), metaConf())
	if _, err := m.Load(true); err != nil {
		t.Fatal(err)
	}
	if err := m.NewSession(false); err != nil {
		t.Fatal(err)
	}
	defer m.CloseSession()
	var ino meta.Ino
	var attr meta.Attr
	if st := m.Create(meta.Background(), B, "new.txt", 0o644, 0o022, 0, &ino, &attr); st != 0 {
		t.Errorf("create in B after John's transaction: %v", st)
	}

	// John's own create in A: juicefs flushes the quota usage of A
	// (doFlushQuotas, every 3 s), through the proxy.
	jm := meta.NewClient("redis://mac-john:"+passOf("mac-john")+"@"+e.paddr+"/"+strconv.Itoa(testDB), metaConf())
	if _, err := jm.Load(true); err != nil {
		t.Fatal(err)
	}
	if err := jm.NewSession(false); err != nil {
		t.Fatal(err)
	}
	defer jm.CloseSession()
	if st := jm.Create(meta.Background(), A, "mine.txt", 0o644, 0o022, 0, &ino, &attr); st != 0 {
		t.Fatalf("John's create in A: %v", st)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		a, _ := e.rdb.HGet(ctx, "dirQuotaUsedInodes", strconv.FormatUint(uint64(A), 10)).Int64()
		if a == 2 { // g.txt, mine.txt
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("quota usage of A after John's create: %d (the flush was refused?)", a)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestAuditLogsStatAndQuotaRows: a unit that writes a file's contents and
// also a quota, dir-stat or deferred-delete row was one "modified" record
// of the file; a bare removal of a quota inside a grant was not logged at
// all. Both are intents now, and every intent lists the rows it writes.
func TestAuditLogsStatAndQuotaRows(t *testing.T) {
	for _, mode := range bothModes {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnv(t, mode, johnGranted("/Clients/A"))
			ctx := context.Background()
			A, g := e.ino["/Clients/A"], e.ino["/Clients/A/g.txt"]
			var sub, sub2 meta.Ino
			var attr meta.Attr
			if st := e.admin.Mkdir(meta.Background(), A, "sub", 0o755, 0o022, 0, &sub, &attr); st != 0 {
				t.Fatal(st)
			}
			if st := e.admin.Mkdir(meta.Background(), A, "sub2", 0o755, 0o022, 0, &sub2, &attr); st != 0 {
				t.Fatal(st)
			}
			row := string(make([]byte, 16))
			for _, d := range []meta.Ino{sub, sub2} {
				e.rdb.HSet(ctx, "dirQuota", strconv.FormatUint(uint64(d), 10), row)
			}
			john := e.rawClient("mac-john")
			s := func(i meta.Ino) string { return strconv.FormatUint(uint64(i), 10) }
			for _, c := range []struct {
				what  string
				write func(p redis.Pipeliner)
				hash  string
				ino   meta.Ino
			}{
				{"contents + HDEL dirQuota", func(p redis.Pipeliner) {
					p.Del(ctx, inoKey("c", g)+"_7")
					p.HDel(ctx, "dirQuota", s(sub))
				}, "dirQuota", sub},
				{"contents + dir-stat", func(p redis.Pipeliner) {
					p.Del(ctx, inoKey("c", g)+"_8")
					p.HIncrBy(ctx, "dirUsedSpace", s(A), 0)
				}, "dirUsedSpace", A},
				{"contents + delfiles", func(p redis.Pipeliner) {
					p.Del(ctx, inoKey("c", g)+"_9")
					p.ZAdd(ctx, "delfiles", redis.Z{Score: 1, Member: s(g) + ":0"})
					p.ZRem(ctx, "delfiles", s(g)+":0")
				}, "delfiles", g},
				{"bare HDEL dirQuota", func(p redis.Pipeliner) { p.HDel(ctx, "dirQuota", s(sub2)) }, "dirQuota", sub2},
			} {
				if _, err := john.TxPipelined(ctx, func(p redis.Pipeliner) error { c.write(p); return nil }); err != nil {
					t.Fatalf("%s: %v", c.what, err)
				}
				recs := e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeIntent}})
				found := false
				for _, r := range recs {
					found = found || hasRow(r, c.hash, c.ino)
				}
				if !found {
					t.Errorf("%s: no intent lists the %s row of %d: %+v", c.what, c.hash, c.ino, recs)
				}
			}
			// A plain mkdir lists the new directory's dir-stat rows.
			jm := e.client("mac-john")
			var d meta.Ino
			if st := jm.Mkdir(meta.Background(), A, "fresh", 0o755, 0o022, 0, &d, &attr); st != 0 {
				t.Fatal(st)
			}
			recs := e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeIntent}})
			found := false
			for _, r := range recs {
				found = found || (r.Action == "create" && hasRow(r, "dirUsedInodes", d))
			}
			if !found {
				t.Errorf("the mkdir intent does not list the new directory's rows")
			}
		})
	}
}
