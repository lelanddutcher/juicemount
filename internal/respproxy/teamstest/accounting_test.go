package teamstest

// Review round 3: the quota usage of folders above a person's grants, the
// values the accounting hashes take, the size of one unit's record, and
// refusals in one place hiding an attempt on another.

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

func setQuota(t *testing.T, e *env, path string, space, inodes int64) {
	t.Helper()
	if err := e.admin.HandleQuota(meta.Background(), meta.QuotaSet, path,
		map[string]*meta.Quota{path: {MaxSpace: space, MaxInodes: inodes}}, false, false, false); err != nil {
		t.Fatal(err)
	}
}

func istr(i meta.Ino) string { return strconv.FormatUint(uint64(i), 10) }

// TestQuotaUsageAboveGrantsMovesOnlyWithOwnUsage: John (granted
// /Clients/A) could add any amount to the quota usage of /Clients, the
// folder that holds A and B: 2^40 made every create in B fail with EDQUOT,
// -2^41 lifted the /Clients quota, and nothing was logged. Such an
// increment is now admitted only within what John's Mac reported for his
// own folders below /Clients (dir-stat increments), plus a slack for the
// order of juicefs's two flushes; a unit that was not committed does not
// count; and the admitted sums are in the audit log.
func TestQuotaUsageAboveGrantsMovesOnlyWithOwnUsage(t *testing.T) {
	for _, mode := range bothModes {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnv(t, mode, johnGranted("/Clients/A"))
			ctx := context.Background()
			clients, A, B := e.ino["/Clients"], e.ino["/Clients/A"], e.ino["/Clients/B"]
			setQuota(t, e, "/Clients", 1<<40, 100000)
			john := e.rawClient("mac-john")
			used := func(hash string) string { return e.rdb.HGet(ctx, hash, istr(clients)).Val() }
			refused := func(what string, err error) {
				t.Helper()
				if err == nil || !strings.Contains(err.Error(), string(respproxy.ReasonNotGranted)) {
					t.Errorf("%s: %v, want NOT_GRANTED", what, err)
				}
			}
			inodes0, space0 := used("dirQuotaUsedInodes"), used("dirQuotaUsedSpace")
			refused("+2^40 inodes", john.HIncrBy(ctx, "dirQuotaUsedInodes", istr(clients), 1<<40).Err())
			refused("-2^41 inodes", john.HIncrBy(ctx, "dirQuotaUsedInodes", istr(clients), -(1<<41)).Err())
			refused("+16 GiB space", john.HIncrBy(ctx, "dirQuotaUsedSpace", istr(clients), 16<<30).Err())
			if a, b := used("dirQuotaUsedInodes"), used("dirQuotaUsedSpace"); a != inodes0 || b != space0 {
				t.Fatalf("/Clients usage %s/%s -> %s/%s", inodes0, space0, a, b)
			}
			m := meta.NewClient("redis://"+e.addr+"/"+strconv.Itoa(testDB), metaConf())
			if _, err := m.Load(true); err != nil {
				t.Fatal(err)
			}
			var ino meta.Ino
			var attr meta.Attr
			if st := m.Create(meta.Background(), B, "new.txt", 0o644, 0o022, 0, &ino, &attr); st != 0 {
				t.Errorf("create in B: %v", st)
			}
			m.Shutdown()

			// 20 GiB reported for A (a dir-stat flush) lets 20 GiB move
			// /Clients, in two flushes; one more GiB beyond the slack not.
			// (A juicefs create first: in enforce John then holds A.)
			var x meta.Ino
			if st := e.client("mac-john").Create(meta.Background(), A, "own.txt", 0o644, 0o022, 0, &x, &attr); st != 0 {
				t.Fatal("create in A:", st)
			}
			const gib = int64(1 << 30)
			if err := john.HIncrBy(ctx, "dirUsedSpace", istr(A), 20*gib).Err(); err != nil {
				t.Fatal(err)
			}
			// A flush that is not committed (WATCH abort) books nothing.
			watched := inoKey("i", A)
			raw, _ := e.rdb.Get(ctx, watched).Bytes()
			werr := john.Watch(ctx, func(tx *redis.Tx) error {
				if err := e.rdb.Set(ctx, watched, raw, 0).Err(); err != nil {
					return err
				}
				_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
					p.HIncrBy(ctx, "dirQuotaUsedSpace", istr(clients), 20*gib)
					return nil
				})
				return err
			}, watched)
			if werr != redis.TxFailedErr {
				t.Fatalf("watched flush: %v, want a WATCH abort", werr)
			}
			for i, d := range []int64{12 * gib, 8 * gib} {
				if err := john.HIncrBy(ctx, "dirQuotaUsedSpace", istr(clients), d).Err(); err != nil {
					t.Fatalf("flush %d of John's reported usage: %v", i, err)
				}
			}
			refused("9 GiB unreported", john.HIncrBy(ctx, "dirQuotaUsedSpace", istr(clients), 9*gib).Err())
			s0, _ := strconv.ParseInt(space0, 10, 64)
			if got := used("dirQuotaUsedSpace"); got != strconv.FormatInt(s0+20*gib, 10) {
				t.Errorf("/Clients space usage %s, want %d", got, s0+20*gib)
			}

			found := false
			deadline := time.Now().Add(5 * time.Second)
			for !found && time.Now().Before(deadline) {
				time.Sleep(300 * time.Millisecond) // the window closes
				for _, r := range e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeModified}}) {
					for _, row := range r.Rows {
						found = found || (r.Action == "quota" && row.Hash == "dirQuotaUsedSpace" && row.Ino == uint64(clients) &&
							row.Value == strconv.FormatInt(20*gib, 10) && row.Path == "/Clients" && r.Committed == 2)
					}
				}
			}
			if !found {
				t.Errorf("no quota record with John's committed 20 GiB on /Clients: %+v",
					e.records(auditlog.Filter{Person: "p-john"}))
			}
		})
	}
}

// TestAccountingCountersTakeOnlyIntegers: HINCRBYFLOAT on a quota-usage
// hash left a float there, after which every new juicefs client failed to
// load quotas (none enforced) and the flush MULTI of every running client
// partially applied and was sent again. Every principal but bg, in every
// mode and also without people (the lease engine let a Mac do it to any
// folder above the project depth), is refused a non-integer value there.
func TestAccountingCountersTakeOnlyIntegers(t *testing.T) {
	for _, c := range []struct {
		name   string
		mode   respproxy.Mode
		people []respproxy.PersonEntry
		who    string
	}{
		{"granted person", respproxy.ModeEnforce, johnGranted("/Clients/A"), "mac-john"},
		{"no people enforce", respproxy.ModeEnforce, nil, "mac-anon"},
		{"no people passthrough", respproxy.ModePassthrough, nil, "mac-anon"},
		{"admin", respproxy.ModeEnforce, nil, "admin1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t, c.mode, c.people)
			ctx := context.Background()
			clients, A := istr(e.ino["/Clients"]), istr(e.ino["/Clients/A"])
			setQuota(t, e, "/Clients", 1<<40, 100000)
			cl := e.rawClient(c.who)
			for _, w := range []struct {
				what string
				args []any
			}{
				{"HINCRBYFLOAT quota usage", []any{"HINCRBYFLOAT", "dirQuotaUsedSpace", clients, "0.5"}},
				{"HINCRBYFLOAT dir stat", []any{"HINCRBYFLOAT", "dirUsedSpace", A, "1"}},
				{"HINCRBY 1e3", []any{"HINCRBY", "dirQuotaUsedInodes", clients, "1e3"}},
				{"HSET float", []any{"HSET", "dirQuotaUsedSpace", clients, "4096.5"}},
				{"HSET text", []any{"HSET", "dirUsedInodes", A, "x"}},
			} {
				if err := cl.Do(ctx, w.args...).Err(); err == nil || !strings.HasPrefix(err.Error(), "JMDENY") {
					t.Errorf("%s by %s: %v, want a refusal", w.what, c.who, err)
				}
			}
			for _, k := range [][2]string{{"dirQuotaUsedSpace", clients}, {"dirQuotaUsedInodes", clients}, {"dirUsedSpace", A}, {"dirUsedInodes", A}} {
				if v := e.rdb.HGet(ctx, k[0], k[1]).Val(); v != "" {
					if _, err := strconv.ParseInt(v, 10, 64); err != nil {
						t.Errorf("%s[%s] = %q", k[0], k[1], v)
					}
				}
			}
			// An integer increment of an unscoped writer still lands.
			if c.people == nil {
				if err := cl.HIncrBy(ctx, "dirUsedSpace", clients, 4096).Err(); err != nil {
					t.Errorf("HINCRBY of an int64: %v", err)
				}
			}
		})
	}
}

// TestAccountingHashesRefuseWholeKeyWrites: John cannot expire, remove,
// rename or overwrite the accounting hashes as a whole, or write a quota
// definition.
func TestAccountingHashesRefuseWholeKeyWrites(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A"))
	ctx := context.Background()
	B := e.ino["/Clients/B"]
	setQuota(t, e, "/Clients/B", 1<<40, 1000)
	sb := istr(B)
	q0, u0 := e.rdb.HGet(ctx, "dirQuota", sb).Val(), e.rdb.HGet(ctx, "dirQuotaUsedInodes", sb).Val()
	john := e.rawClient("mac-john")
	for _, c := range []struct {
		what string
		args []any
	}{
		{"HEXPIRE B usage", []any{"HEXPIRE", "dirQuotaUsedInodes", 1, "FIELDS", 1, sb}},
		{"HPEXPIRE B quota", []any{"HPEXPIRE", "dirQuota", 1, "FIELDS", 1, sb}},
		{"HPERSIST", []any{"HPERSIST", "dirQuota", "FIELDS", 1, sb}},
		{"EXPIRE dirQuota", []any{"EXPIRE", "dirQuota", 100000}},
		{"DEL dirQuotaUsedInodes", []any{"DEL", "dirQuotaUsedInodes"}},
		{"UNLINK dirQuota", []any{"UNLINK", "dirQuota"}},
		{"RENAME dirQuota", []any{"RENAME", "dirQuota", "x"}},
		{"COPY over dirQuota", []any{"COPY", "nosuch", "dirQuota", "REPLACE"}},
		{"HSET field with suffix", []any{"HSET", "dirQuota", sb + "x", "0123456789abcdef"}},
		{"HINCRBY dirQuota B", []any{"HINCRBY", "dirQuota", sb, 1}},
	} {
		if err := john.Do(ctx, c.args...).Err(); err == nil || !strings.HasPrefix(err.Error(), "JMDENY") {
			t.Errorf("%s: %v, want a refusal", c.what, err)
		}
	}
	time.Sleep(1100 * time.Millisecond) // past any 1 s expiry
	if q, u, ttl := e.rdb.HGet(ctx, "dirQuota", sb).Val(), e.rdb.HGet(ctx, "dirQuotaUsedInodes", sb).Val(),
		e.rdb.TTL(ctx, "dirQuota").Val(); q != q0 || u != u0 || ttl != -1 {
		t.Errorf("dirQuota[B] %q -> %q, usedInodes[B] %q -> %q, ttl %v", q0, q, u0, u, ttl)
	}
}

// TestRefusedFloodKeepsEachPlace: 20 refused writes in C fill the window's
// one-by-one budget; the attempt on B that follows was only a count in
// the collapsed record. The first refusal in each place is now logged one
// by one, and the collapsed record names the places it counts.
func TestRefusedFloodKeepsEachPlace(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A"))
	ctx := context.Background()
	C, B := e.ino["/Clients/C"], e.ino["/Clients/B"]
	craw, _ := e.rdb.Get(ctx, inoKey("i", C)).Bytes()
	john := e.rawClient("mac-john")
	for i := 0; i < 25; i++ {
		if err := john.Set(ctx, inoKey("i", C), craw, 0).Err(); err == nil {
			t.Fatal("write to C admitted")
		}
	}
	if err := john.HDel(ctx, inoKey("d", B), "b.txt").Err(); err == nil {
		t.Fatal("removal in B admitted")
	}
	time.Sleep(1500 * time.Millisecond) // past the refused window (6 x 200 ms)
	e.proxy.Close()
	<-e.proxy.Done()
	seenB, collapsedC := false, false
	for _, r := range e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeRefused}}) {
		if r.Action == "collapsed" {
			collapsedC = collapsedC || (r.Count == 5 && hasProject(r, C, "/Clients/C"))
			continue
		}
		for _, o := range r.Ops {
			seenB = seenB || (o.Name == "b.txt" && hasProject(r, B, "/Clients/B"))
		}
	}
	if !seenB || !collapsedC {
		t.Errorf("attempt on B logged one by one: %v; collapsed record names C with 5: %v; records %+v", seenB, collapsedC,
			e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeRefused}}))
	}
}

// TestGrantedJuiceFSWorkflowWithQuotas: ordinary juicefs work in a granted
// project, with quotas on A and on /Clients above it: nothing is refused,
// the quota usage of both converges, and the usage John's Mac flushed for
// /Clients is in the audit log.
func TestGrantedJuiceFSWorkflowWithQuotas(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A"))
	ctx := context.Background()
	A, clients := e.ino["/Clients/A"], e.ino["/Clients"]
	setQuota(t, e, "/Clients", 1<<40, 100000)
	setQuota(t, e, "/Clients/A", 1<<40, 100000)
	jm := meta.NewClient("redis://mac-john:"+passOf("mac-john")+"@"+e.paddr+"/"+strconv.Itoa(testDB), metaConf())
	if _, err := jm.Load(true); err != nil {
		t.Fatal(err)
	}
	if err := jm.NewSession(true); err != nil {
		t.Fatal(err)
	}
	defer jm.CloseSession()
	c := meta.Background()
	var attr meta.Attr
	var sub, f1, f2 meta.Ino
	if st := jm.Mkdir(c, A, "sub", 0o755, 0o022, 0, &sub, &attr); st != 0 {
		t.Fatal("mkdir", st)
	}
	if st := jm.Create(c, sub, "a.mov", 0o644, 0o022, 0, &f1, &attr); st != 0 {
		t.Fatal("create", st)
	}
	var sid uint64
	if st := jm.NewSlice(c, &sid); st != 0 {
		t.Fatal(st)
	}
	if st := jm.Write(c, f1, 0, 0, meta.Slice{Id: sid, Size: 1 << 20, Len: 1 << 20}, time.Now()); st != 0 {
		t.Fatal("write", st)
	}
	jm.Close(c, f1)
	if st := jm.Rename(c, sub, "a.mov", A, "b.mov", 0, &f1, &attr); st != 0 {
		t.Fatal("rename", st)
	}
	if st := jm.Create(c, A, "tmp", 0o644, 0o022, 0, &f2, &attr); st != 0 {
		t.Fatal("create2", st)
	}
	jm.Close(c, f2)
	if st := jm.Unlink(c, A, "tmp"); st != 0 {
		t.Fatal("unlink", st)
	}
	if st := jm.Rmdir(c, A, "sub"); st != 0 {
		t.Fatal("rmdir", st)
	}
	if st := jm.SetXattr(c, f1, "user.x", []byte("v"), 0); st != 0 {
		t.Fatal("setxattr", st)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		qa, _ := e.rdb.HGet(ctx, "dirQuotaUsedSpace", istr(A)).Int64()
		qc, _ := e.rdb.HGet(ctx, "dirQuotaUsedSpace", istr(clients)).Int64()
		if qa >= 1<<20 && qc >= 1<<20 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("quota usage A %d /Clients %d: the flush did not land", qa, qc)
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // the quota window closes
	if recs := e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeRefused}}); len(recs) > 0 {
		t.Errorf("refusals: %+v", recs)
	}
	logged := false
	for _, r := range e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeModified}}) {
		logged = logged || (r.Action == "quota" && hasRow(r, "dirQuotaUsedSpace", clients))
	}
	if !logged {
		t.Error("no audit record of the usage John's Mac flushed for /Clients")
	}
}

// TestOversizedUnitKeepsLogReadable: one admitted MULTI in John's own
// project (12,000 entries with 240-byte names, ~3 MB on the wire) made a
// line longer than the readers accept: Query, Verify and the Manager's
// Follow failed for the whole log. Its intent now lists the first entries,
// how many it leaves out and a digest of them all.
func TestOversizedUnitKeepsLogReadable(t *testing.T) {
	e := newEnv(t, respproxy.ModeEnforce, johnGranted("/Clients/A"))
	ctx := context.Background()
	A, g := e.ino["/Clients/A"], e.ino["/Clients/A/g.txt"]
	val := make([]byte, 9)
	val[0] = 1 // a file entry naming g.txt's inode
	for i := 0; i < 8; i++ {
		val[1+i] = byte(uint64(g) >> (56 - 8*i))
	}
	john := redis.NewClient(&redis.Options{Addr: e.paddr, Username: "mac-john", Password: passOf("mac-john"), DB: testDB, Protocol: 2,
		DisableIdentity: true, MaxRetries: -1, ReadTimeout: 2 * time.Minute, WriteTimeout: time.Minute})
	defer john.Close()
	jm := e.client("mac-john")
	var x meta.Ino
	var at meta.Attr
	if st := jm.Create(meta.Background(), A, "before.txt", 0o644, 0o022, 0, &x, &at); st != 0 {
		t.Fatal(st)
	}
	e.audit.Sync()
	_, anchor, err := auditlog.VerifyAnchors(e.auditDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	args := []any{"HSET", inoKey("d", A)}
	name := strings.Repeat("\x01", 240)
	const n = 12000
	for i := 0; i < n; i++ {
		args = append(args, name+strconv.Itoa(i), string(val))
	}
	if _, err := john.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Do(ctx, args...); return nil }); err != nil {
		t.Fatalf("HSET of %d entries: %v", n, err)
	}
	if err := e.audit.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := auditlog.Query(e.auditDir, auditlog.Filter{}, 0, 10); err != nil {
		t.Errorf("Query: %v", err)
	}
	if rep, err := auditlog.Verify(e.auditDir); err != nil || !rep.OK {
		t.Errorf("Verify: %+v, %v", rep, err)
	}
	if _, rep, err := auditlog.Follow(e.auditDir, anchor, 0); err != nil || !rep.OK {
		t.Errorf("Follow: %+v, %v", rep, err)
	}
	var big *auditlog.Record
	for _, r := range e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeIntent}}) {
		if r.Omitted > 0 {
			r := r
			big = &r
		}
	}
	if big == nil || len(big.Ops) > 256 || big.Omitted != n-len(big.Ops) || !strings.HasPrefix(big.Digest, "sha256:") ||
		!strings.Contains(big.Detail, "more entries") {
		t.Errorf("the large unit's intent: %+v", big)
	}
}
