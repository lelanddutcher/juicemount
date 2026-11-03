package teamstest

// Round-2 review findings, through a real proxy in passthrough and enforce:
// attribute changes hidden in content units, the trash, quota rows above
// the project depth, and Macs with no person while grants are in force.

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/redis/go-redis/v9"
)

// rawClient is a plain Redis client through the proxy as principal name.
func (e *env) rawClient(name string) *redis.Client {
	c := redis.NewClient(&redis.Options{Addr: e.paddr, Username: name, Password: passOf(name), DB: testDB, Protocol: 2,
		DisableIdentity: true, MaxRetries: -1})
	e.t.Cleanup(func() { c.Close() })
	return c
}

func inoKey(prefix string, i meta.Ino) string { return prefix + strconv.FormatUint(uint64(i), 10) }

// withMode returns a copy of a marshalled attr with mode bits m.
func withMode(raw []byte, m uint16) []byte {
	out := append([]byte(nil), raw...)
	cur := binary.BigEndian.Uint16(out[1:3])
	binary.BigEndian.PutUint16(out[1:3], (cur&0xF000)|m)
	return out
}

// intentNaming returns the intents of person whose Inodes or Attrs name
// inode n, with their outcomes.
func intentNaming(recs []auditlog.Record, person string, n meta.Ino) (out []auditlog.Record, outcomes []string) {
	for _, r := range recs {
		if r.Type != auditlog.TypeIntent || r.Person != person {
			continue
		}
		hit := false
		for _, x := range r.Inodes {
			hit = hit || x == uint64(n)
		}
		for _, a := range r.Attrs {
			hit = hit || a.Ino == uint64(n)
		}
		if !hit {
			continue
		}
		oc := ""
		for _, x := range recs {
			if x.Type == auditlog.TypeOutcome && x.Unit == r.Seq {
				oc = x.Outcome
			}
		}
		out, outcomes = append(out, r), append(outcomes, oc)
	}
	return out, outcomes
}

func hasAttr(r auditlog.Record, n meta.Ino, field, old, new string) bool {
	for _, a := range r.Attrs {
		if a.Ino == uint64(n) && a.Field == field && a.Old == old && a.New == new {
			return true
		}
	}
	return false
}

// TestAuditAttrChangeInContentUnit: a unit that writes a file's contents
// and also changes another inode's attributes, or the mode of the file it
// writes, is logged as an intent naming the inode and the change (it was a
// "modified" record of the written file only). A plain contents write
// stays one "modified" record, without an intent.
func TestAuditAttrChangeInContentUnit(t *testing.T) {
	for _, mode := range bothModes {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnv(t, mode, johnGranted("/Clients/A"))
			ctx := context.Background()
			A, g := e.ino["/Clients/A"], e.ino["/Clients/A/g.txt"]
			var other meta.Ino
			var attr meta.Attr
			if st := e.admin.Create(meta.Background(), A, "other.txt", 0o644, 0o022, 0, &other, &attr); st != 0 {
				t.Fatal(st)
			}
			john := e.rawClient("mac-john")

			// Another inode's mode, with a chunk write of g.
			raw, err := e.rdb.Get(ctx, inoKey("i", other)).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			setuid := withMode(raw, 0o4777)
			if _, err := john.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, inoKey("i", other), setuid, 0)
				p.Del(ctx, inoKey("c", g)+"_7")
				return nil
			}); err != nil {
				t.Fatalf("txn: %v", err)
			}
			// The written file's own mode, with its chunk write.
			graw, err := e.rdb.Get(ctx, inoKey("i", g)).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := john.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, inoKey("i", g), withMode(graw, 0o777), 0)
				p.Del(ctx, inoKey("c", g)+"_9")
				return nil
			}); err != nil {
				t.Fatalf("txn: %v", err)
			}
			// A plain contents write through juicefs.
			jm := e.client("mac-john")
			var sid uint64
			if st := jm.NewSlice(meta.Background(), &sid); st != 0 {
				t.Fatal(st)
			}
			if st := jm.Write(meta.Background(), g, 0, 0, meta.Slice{Id: sid, Size: 4, Len: 4}, time.Now()); st != 0 {
				t.Fatal(st)
			}

			recs := e.records(auditlog.Filter{Person: "p-john"})
			rs, ocs := intentNaming(recs, "p-john", other)
			if len(rs) != 1 || ocs[0] != auditlog.OutcomeCommitted || !hasAttr(rs[0], other, "mode", "0644", "4777") {
				t.Fatalf("setuid of inode %d with a chunk write of %d: intents %+v outcomes %v", other, g, rs, ocs)
			}
			rs, ocs = intentNaming(recs, "p-john", g)
			found := false
			for i, r := range rs {
				found = found || (hasAttr(r, g, "mode", "0644", "0777") && ocs[i] == auditlog.OutcomeCommitted)
			}
			if !found {
				t.Fatalf("chmod of %d hidden in its own chunk write: intents %+v outcomes %v", g, rs, ocs)
			}
			if len(rs) != 2 { // the two transactions above, not the plain write
				t.Errorf("intents naming %d: %+v; the plain contents write must not add one", g, rs)
			}
		})
	}
}

// TestGrantsKeepTheTrash: in passthrough as in enforce, a person cannot
// write contents into, or change the attributes of, a file trashed from a
// project they have no grant on, cannot create entries in a directory
// trashed from one, and cannot restore such a file into their own project.
// Restoring a file trashed from their own project works.
func TestGrantsKeepTheTrash(t *testing.T) {
	for _, mode := range bothModes {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnv(t, mode, johnGranted("/Clients/A"))
			ctx := meta.Background()
			A, B := e.ino["/Clients/A"], e.ino["/Clients/B"]
			h, g := e.ino["/Clients/B/h.txt"], e.ino["/Clients/A/g.txt"]
			jm := e.client("mac-john")
			raw := e.rawClient("mac-john")
			if st := e.admin.Unlink(ctx, B, "h.txt"); st != 0 {
				t.Fatal(st)
			}
			var ha meta.Attr
			if st := e.admin.GetAttr(ctx, h, &ha); st != 0 || ha.Parent < meta.TrashInode {
				t.Fatalf("B/h.txt not in the trash: parent %d (%v)", ha.Parent, st)
			}
			trash := ha.Parent
			before, _ := e.rdb.Get(context.Background(), inoKey("i", h)).Bytes()

			var sid uint64
			if st := jm.NewSlice(ctx, &sid); st != 0 {
				t.Fatal(st)
			}
			if st := jm.Write(ctx, h, 0, 0, meta.Slice{Id: sid, Size: 4, Len: 4}, time.Now()); st != syscall.EROFS {
				t.Errorf("write into B/h.txt in the trash: %v, want EROFS", st)
			}
			if err := raw.Set(context.Background(), inoKey("i", h), withMode(before, 0o777), 0).Err(); err == nil {
				t.Error("chmod of B/h.txt in the trash admitted")
			}
			var ri meta.Ino
			var attr meta.Attr
			if st := jm.Rename(ctx, trash, fmt.Sprintf("%d-%d-h.txt", B, h), A, "stolen", 0, &ri, &attr); st != syscall.EROFS {
				t.Errorf("restore of B/h.txt into A: %v, want EROFS", st)
			}
			if e.exists(A, "stolen") {
				t.Error("B/h.txt was restored into A")
			}
			after, _ := e.rdb.Get(context.Background(), inoKey("i", h)).Bytes()
			if string(after) != string(before) {
				t.Error("B/h.txt in the trash changed")
			}

			// A directory trashed from B: no new entry in it.
			var od meta.Ino
			if st := e.admin.Mkdir(ctx, B, "olddir", 0o755, 0o022, 0, &od, &attr); st != 0 {
				t.Fatal(st)
			}
			if st := e.admin.Rmdir(ctx, B, "olddir"); st != 0 {
				t.Fatal(st)
			}
			var oa meta.Attr
			if st := e.admin.GetAttr(ctx, od, &oa); st != 0 || oa.Parent < meta.TrashInode {
				t.Skipf("B/olddir not kept in the trash (parent %d, %v)", oa.Parent, st)
			}
			nino := meta.Ino(1 << 40)
			na := meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Nlink: 1, Parent: od}
			entry := append([]byte{meta.TypeFile}, make([]byte, 8)...)
			binary.BigEndian.PutUint64(entry[1:], uint64(nino))
			_, err := raw.TxPipelined(context.Background(), func(p redis.Pipeliner) error {
				p.HSet(context.Background(), inoKey("d", od), "planted", entry)
				p.Set(context.Background(), inoKey("i", nino), na.Marshal(), 0)
				return nil
			})
			if err == nil {
				t.Error("a new entry in B/olddir (in the trash) was admitted")
			}
			if n, _ := e.rdb.HExists(context.Background(), inoKey("d", od), "planted").Result(); n {
				t.Error("an entry was created in B/olddir in the trash")
			}

			// John's own deleted file comes back.
			if st := jm.Unlink(ctx, A, "g.txt"); st != 0 {
				t.Fatal(st)
			}
			var ga meta.Attr
			if st := e.admin.GetAttr(ctx, g, &ga); st != 0 || ga.Parent < meta.TrashInode {
				t.Fatalf("A/g.txt not in the trash: parent %d (%v)", ga.Parent, st)
			}
			if st := jm.Rename(ctx, ga.Parent, fmt.Sprintf("%d-%d-g.txt", A, g), A, "g-back", 0, &ri, &attr); st != 0 {
				t.Errorf("restore of John's own A/g.txt: %v", st)
			}
			if !e.exists(A, "g-back") {
				t.Error("A/g.txt was not restored")
			}
			recs := e.records(auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeRefused}})
			if r := refusalFor(recs, "stolen"); r == nil || r.Reason != string(respproxy.ReasonNotGranted) {
				t.Errorf("the refused restore was not recorded: %+v", r)
			}
		})
	}
}

// TestQuotaRowsNeedAGrant: a scoped person cannot remove the quota of a
// folder above the project depth (or of the root, or of another project),
// alone or riding along a write they may make; the quota of a folder in
// their project, and the usage counters above it, stay open.
func TestQuotaRowsNeedAGrant(t *testing.T) {
	for _, mode := range bothModes {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnv(t, mode, johnGranted("/Clients/A"))
			ctx := context.Background()
			var sub meta.Ino
			var attr meta.Attr
			if st := e.admin.Mkdir(meta.Background(), e.ino["/Clients/A"], "sub", 0o755, 0o022, 0, &sub, &attr); st != 0 {
				t.Fatal(st)
			}
			row := string(make([]byte, 16))
			john := e.rawClient("mac-john")
			for _, c := range []struct {
				what string
				dir  meta.Ino
				ok   bool
			}{{"/Clients", e.ino["/Clients"], false}, {"/", meta.RootInode, false}, {"/Clients/B", e.ino["/Clients/B"], false},
				{"/Clients/A/sub", sub, true}} {
				f := strconv.FormatUint(uint64(c.dir), 10)
				if err := e.rdb.HSet(ctx, "dirQuota", f, row).Err(); err != nil {
					t.Fatal(err)
				}
				err := john.HDel(ctx, "dirQuota", f).Err()
				kept, _ := e.rdb.HExists(ctx, "dirQuota", f).Result()
				notGranted := err != nil && strings.Contains(err.Error(), string(respproxy.ReasonNotGranted))
				switch {
				case !c.ok && (err == nil || !notGranted || !kept):
					t.Errorf("HDEL dirQuota %s: err %v, row kept %v; want NOT_GRANTED", c.what, err, kept)
				case c.ok && notGranted:
					// (enforce may refuse it for the lease John does not hold)
					t.Errorf("HDEL dirQuota %s inside the grant: %v", c.what, err)
				}
			}
			// Riding along a write in A.
			clients := strconv.FormatUint(uint64(e.ino["/Clients"]), 10)
			e.rdb.HSet(ctx, "dirQuota", clients, row)
			graw, _ := e.rdb.Get(ctx, inoKey("i", e.ino["/Clients/A/g.txt"])).Bytes()
			_, err := john.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, inoKey("i", e.ino["/Clients/A/g.txt"]), graw, 0)
				p.HDel(ctx, "dirQuota", clients)
				return nil
			})
			if kept, _ := e.rdb.HExists(ctx, "dirQuota", clients).Result(); err == nil || !kept {
				t.Errorf("HDEL dirQuota /Clients along a write in A: err %v, row kept %v", err, kept)
			}
			// Usage counters above the project depth stay open.
			if err := john.HIncrBy(ctx, "dirUsedSpace", clients, 0).Err(); err != nil {
				t.Errorf("dir-stat counter of /Clients: %v", err)
			}
		})
	}
}

// TestUnassignedMacReadOnly: while grants are in force (a scoped person,
// or restrict_unassigned with nobody scoped) a Mac with no person may read
// but not write, and its refusals are logged; without either it writes as
// before.
func TestUnassignedMacReadOnly(t *testing.T) {
	cases := []struct {
		name   string
		people []respproxy.PersonEntry
		restrict,
		refused bool
	}{
		{"scoped person", johnGranted("/Clients/A"), false, true},
		{"restrict_unassigned", []respproxy.PersonEntry{{ID: "p-john", Name: "John", Access: respproxy.AccessAll, Devices: []string{"mac-john"}}}, true, true},
		{"people, no grants", []respproxy.PersonEntry{{ID: "p-john", Name: "John", Access: respproxy.AccessAll, Devices: []string{"mac-john"}}}, false, false},
	}
	for _, mode := range bothModes {
		for _, c := range cases {
			t.Run(string(mode)+"/"+c.name, func(t *testing.T) {
				v := newVolume(t)
				e := &env{volume: v, mode: mode}
				e.restrict = c.restrict
				e.startProxy(c.people)
				anon := e.client("mac-anon")
				ctx := meta.Background()
				var ino meta.Ino
				var attr meta.Attr
				st := anon.Mkdir(ctx, e.ino["/Clients/C"], "anon", 0o755, 0o022, 0, &ino, &attr)
				if c.refused != (st == syscall.EROFS) || (!c.refused && st != 0) {
					t.Fatalf("mkdir by a Mac with no person: %v, want refused=%v", st, c.refused)
				}
				if st := anon.Lookup(ctx, e.ino["/Clients/A"], "g.txt", &ino, &attr, false); st != 0 {
					t.Fatalf("lookup by a Mac with no person: %v", st)
				}
				if c.refused {
					r := refusalFor(e.records(auditlog.Filter{Principal: "mac-anon"}), "anon")
					if r == nil || r.Reason != string(respproxy.ReasonNotGranted) || r.Person != "" {
						t.Fatalf("refusal of mac-anon: %+v", r)
					}
					// The person with access "all" still writes.
					if !c.restrict {
						return
					}
					if st := e.client("mac-john").Mkdir(ctx, e.ino["/Clients/C"], "john", 0o755, 0o022, 0, &ino, &attr); st != 0 {
						t.Fatalf("mkdir by John (access all): %v", st)
					}
				}
			})
		}
	}
}
