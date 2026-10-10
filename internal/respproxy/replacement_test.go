package respproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/juicedata/juicefs/pkg/slicegraph"
	"github.com/redis/go-redis/v9"
)

type replacementRig struct {
	*leaseRig
	db   *redis.Client
	rec  replaceguard.Record
	attr []byte
}

func startReplacementRig(t *testing.T, mut ...func(*LeaseConfig)) *replacementRig {
	t.Helper()
	_ = redisAddr(t, true)
	r := startLeaseRigP(t, ModeEnforce, func(c *Config) { c.Singleton = true; c.ModePoll = time.Hour }, func(c *LeaseConfig) {
		c.NoScan = true
		c.FlushEvery = time.Hour
		for _, configure := range mut {
			configure(c)
		}
	})
	t.Cleanup(func() { singletonCloseJoined(t, r.tp.Proxy) })
	db := directLeaseView(t, r)
	ctx := context.Background()
	mac := r.conn(t, "mac1")
	v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto")
	if v.Type != '*' || v.Elems[1].Int != 1 {
		t.Fatalf("grant: %+v", v)
	}
	format := meta.Format{Name: "replacement-fixture", UUID: "replacement-fixture-volume", BlockSize: 4096}
	setting, _ := json.Marshal(format)
	graph := slicegraph.Graph{Version: slicegraph.Version, Length: 0}
	canon, err := graph.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canon)
	rec := replaceguard.Record{Version: 1, OperationID: strings.Repeat("a", 32), Owner: "untrusted-owner", Proxy: "untrusted-proxy",
		BackendUUID: format.UUID, BackendDigest: strings.Repeat("b", 64), Project: 3, AcceptedEpoch: 1, Epoch: 1,
		Parents:     []replaceguard.Parent{{Inode: 1}, {Inode: 2, Name: "Projects"}, {Inode: 3, Name: "A"}},
		Stage:       replaceguard.File{Inode: 21, Parent: 3, Name: ".juicemount-streaming-drain-fixture", Length: 5, MtimeNS: 10_000_000_101, CtimeNS: 10_000_000_102},
		Destination: replaceguard.File{Inode: 10, Parent: 3, Name: "f", Length: 0, MtimeNS: 11_000_000_101, CtimeNS: 11_000_000_102},
		GraphDigest: hex.EncodeToString(digest[:]), ImageSHA256: strings.Repeat("c", 64), Phase: "prepared"}
	stage := (&meta.Attr{Typ: meta.TypeFile, Mode: 0666, Nlink: 1, Parent: 3, Length: 5, Mtime: 10, Mtimensec: 101, Ctime: 10, Ctimensec: 102}).Marshal()
	old := (&meta.Attr{Typ: meta.TypeFile, Mode: 0666, Nlink: 1, Parent: 3, Length: 0, Mtime: 11, Mtimensec: 101, Ctime: 11, Ctimensec: 102}).Marshal()
	pipe := db.Pipeline()
	pipe.Set(ctx, "setting", setting, 0)
	pipe.Set(ctx, "i21", stage, 0)
	pipe.Set(ctx, "i10", old, 0)
	for _, parent := range []struct{ n, p uint64 }{{1, 1}, {2, 1}, {3, 2}} {
		pipe.Set(ctx, "i"+inoStr(parent.n), (&meta.Attr{Typ: meta.TypeDirectory, Mode: 0777, Nlink: 2, Parent: meta.Ino(parent.p), Length: 4096}).Marshal(), 0)
	}
	pipe.HSet(ctx, "d3", rec.Stage.Name, mkEntry(typeFile, 21))
	pipe.Set(ctx, "usedSpace", "1048576", 0)
	pipe.Set(ctx, "totalInodes", "100", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	return &replacementRig{r, db, rec, stage}
}
func (r *replacementRig) arm(t *testing.T) *rawConn {
	t.Helper()
	mac := r.conn(t, "mac1")
	raw, err := replaceguard.Encode(r.rec)
	if err != nil {
		t.Fatal(err)
	}
	v := mac.do("JM.LEASE", "ARM_REPLACE", string(raw))
	if v.Type != '$' || v.Null {
		t.Fatalf("arm: %+v", v)
	}
	got, err := replaceguard.Decode(v.Str)
	if err != nil {
		t.Fatal(err)
	}
	if got.Owner != "mac1" || got.Proxy != r.tp.Incarnation() {
		t.Fatalf("request supplied authority: %+v", got)
	}
	r.rec = got
	return mac
}
func (r *replacementRig) unchanged(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if got := r.db.HGet(ctx, "d3", r.rec.Stage.Name).Val(); got != mkEntry(typeFile, 21) {
		t.Fatalf("stage name changed: %x", got)
	}
	if got := r.db.Get(ctx, "i21").Val(); got != string(r.attr) {
		t.Fatalf("sealed attr changed: %x", got)
	}
	if got := r.db.HGet(ctx, "d3", "f").Val(); got != mkEntry(typeFile, 10) {
		t.Fatalf("original replaced: %x", got)
	}
}

func TestReplacementAuthenticatedEnrollment(t *testing.T) {
	r := startReplacementRig(t)
	mac := r.arm(t)
	raw, _ := replaceguard.Encode(r.rec)
	if v := mac.do("JM.LEASE", "ARM_REPLACE", string(raw)); v.Type != '$' || string(v.Str) != string(raw) {
		t.Fatalf("idempotent retry: %+v", v)
	}
	changed := r.rec
	changed.OperationID = strings.Repeat("d", 32)
	raw, _ = replaceguard.Encode(changed)
	if v := mac.do("JM.LEASE", "ARM_REPLACE", string(raw)); v.Type != '-' {
		t.Fatalf("rebound operation: %+v", v)
	}
	if v := r.conn(t, "mac2").do("JM.LEASE", "ARM_REPLACE", string(raw)); v.Type != '-' {
		t.Fatalf("foreign enrollment: %+v", v)
	}
	r.unchanged(t)
}

func TestReplacementSealedStageCannotBeMutated(t *testing.T) {
	for _, mode := range []Mode{ModeEnforce, ModeObserve, ModePassthrough} {
		t.Run(string(mode), func(t *testing.T) {
			r := startReplacementRig(t)
			mac := r.arm(t)
			r.tp.setMode(mode, "test")
			for _, a := range [][]string{
				{"SET", "i21", string(r.attr)}, {"RPUSH", "c21_0", "new mapping"},
				{"HDEL", "d3", r.rec.Stage.Name}, {"HSET", "d3", "alias", mkEntry(typeFile, 21)},
				{"HSET", "d3", r.rec.Stage.Name, mkEntry(typeFile, 99)},
				{"SET", replaceguard.Key(21), "tampered"},
			} {
				for _, multi := range []bool{false, true} {
					var v Value
					if multi {
						v = txn(mac, a)
					} else {
						v = mac.do(a...)
					}
					if v.Type != '-' {
						t.Fatalf("mode=%s multi=%v cmd=%s unexpectedly accepted: %+v", mode, multi, a[0], v)
					}
					r.unchanged(t)
				}
			}
			if v := mac.do("HGET", "d3", r.rec.Stage.Name); v.Type != '$' {
				t.Fatalf("stage read denied: %+v", v)
			}
		})
	}
}

func TestReplacementArmRejectsChangedAuthority(t *testing.T) {
	for _, which := range []string{"foreign-history", "epoch", "singleton", "stage-length", "stage-entry", "setting"} {
		t.Run(which, func(t *testing.T) {
			r := startReplacementRig(t)
			ctx := context.Background()
			switch which {
			case "foreign-history":
				r.rec.AcceptedEpoch = 0
				r.db.HSet(ctx, leaseKey(3), "e:other", "1")
			case "epoch":
				r.db.Set(ctx, epochKey(3), "2", 0)
			case "singleton":
				r.db.Set(ctx, SingletonKey, "other-proxy", 0)
			case "stage-length":
				r.db.Set(ctx, "i21", mkAttr(typeFile, 3), 0)
			case "stage-entry":
				r.db.HSet(ctx, "d3", r.rec.Stage.Name, mkEntry(typeFile, 99))
			case "setting":
				r.db.Set(ctx, "setting", `{"UUID":"other-volume"}`, 0)
			}
			raw, _ := replaceguard.Encode(r.rec)
			if v := r.conn(t, "mac1").do("JM.LEASE", "ARM_REPLACE", string(raw)); v.Type != '-' {
				t.Fatalf("invalid arm: %+v", v)
			}
			if n := r.db.Exists(ctx, replaceguard.Key(21)).Val(); n != 0 {
				t.Fatal("rejected arm wrote record")
			}
		})
	}
}

// This is the production metadata Rename call through the real authenticated
// proxy. It does not mount a filesystem or claim media-byte upload acceptance.
func TestReplacementActualMetaRenameThroughProxy(t *testing.T) {
	for _, trash := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_trash", true: "empty_trash"}[trash], func(t *testing.T) {
			r := startReplacementRig(t)
			if trash {
				ctx := context.Background()
				format := meta.Format{Name: "replacement-fixture", UUID: r.rec.BackendUUID, BlockSize: 4096, TrashDays: 1}
				raw, _ := json.Marshal(format)
				if err := r.db.Set(ctx, "setting", raw, 0).Err(); err != nil {
					t.Fatal(err)
				}
				tr := uint64(meta.TrashInode)
				if err := r.db.Set(ctx, "i"+inoStr(tr), (&meta.Attr{Typ: meta.TypeDirectory, Mode: 0555, Nlink: 2, Parent: 1, Length: 4096}).Marshal(), 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			r.arm(t)
			conf := meta.DefaultConf()
			conf.Retries = 0
			conf.MaxDeletes = 0
			m := meta.NewClient("redis://mac1:mac1-pass@"+r.tp.addr+"/15", conf)
			t.Cleanup(func() { _ = m.Shutdown() })
			if _, err := m.Load(false); err != nil {
				t.Fatal(err)
			}
			var ino meta.Ino
			var attr meta.Attr
			if st := m.Rename(meta.Background(), 3, r.rec.Stage.Name, 3, "f", 0, &ino, &attr); st != 0 {
				t.Fatalf("actual metadata rename: %v", st)
			}
			if ino != 21 {
				t.Fatalf("published inode=%d", ino)
			}
			raw, err := r.db.Get(context.Background(), replaceguard.Key(21)).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			got, err := replaceguard.Decode(raw)
			if err != nil || got.Phase != "committed" {
				t.Fatalf("receipt: %+v %v", got, err)
			}
			if r.db.HExists(context.Background(), "d3", r.rec.Stage.Name).Val() || r.db.HGet(context.Background(), "d3", "f").Val() != mkEntry(typeFile, 21) {
				t.Fatal("namespace not replaced")
			}
			if st := m.Rename(meta.Background(), 3, "f", 3, "renamed.mov", 0, &ino, &attr); st != 0 {
				t.Fatalf("published file cannot be renamed normally: %v", st)
			}
		})
	}
}

func TestReplacementLostObserveUnitFinalizedBeforeArm(t *testing.T) {
	r := startReplacementRig(t)
	before := upstreamNames(t, r.db, "p:mac2:")
	other := r.conn(t, "mac2")
	name := newName(t, before, upstreamNames(t, r.db, "p:mac2:"))
	args := strArgs("HSET", "x21", "field", "late")
	u := &Unit{Kind: UnitBare, Mode: ModeObserve, DB: testDB, UpstreamName: name, Cmds: []Cmd{{Args: args, Info: Classify(args)}}}
	p := Principal{Name: "mac2", Kind: KindMac}
	if d, _ := r.tp.Guard.Check(p, u); d != nil {
		t.Fatal(d)
	}
	v := r.eng.Decide(p, u)
	if v.Deny == nil || v.Done == nil {
		t.Fatal("fixture did not hold a denied observe unit")
	}
	v.Done(Outcome{Forwarded: true, Lost: true})
	if got := r.eng.nLostKill.Load(); got != 1 {
		t.Fatalf("lost read-topology unit was not finalized: kills=%d", got)
	}
	if !r.eng.topo.TryLock() {
		t.Fatal("topology custody retained after confirmed kill")
	}
	r.eng.topo.Unlock()
	_ = other
	r.arm(t)
}

func TestReplacementReceiptDoesNotClaimPartialOrAbortedEXEC(t *testing.T) {
	for _, fault := range []string{"command-error", "watch-abort"} {
		t.Run(fault, func(t *testing.T) {
			r := startReplacementRig(t)
			r.arm(t)
			ctx := context.Background()
			var fired atomic.Int32
			if fault == "command-error" {
				if err := r.db.Set(ctx, "x10", "wrong-type", 0).Err(); err != nil {
					t.Fatal(err)
				}
			} else {
				r.eng.hooks.Store(&engineHooks{admitted: func(p Principal, u *Unit) {
					if u.Kind == UnitTxn && len(u.Watched) > 0 && fired.CompareAndSwap(0, 1) {
						if err := r.db.Set(ctx, "i10", r.db.Get(ctx, "i10").Val(), 0).Err(); err != nil {
							panic(err)
						}
					}
				}})
			}
			client := r.tp.client(t, "mac1")
			err := client.Watch(ctx, func(tx *redis.Tx) error {
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.HDel(ctx, "d3", r.rec.Stage.Name)
					pipe.Set(ctx, "i21", r.attr, 0)
					pipe.HSet(ctx, "d3", "f", mkEntry(typeFile, 21))
					if fault == "command-error" {
						pipe.HSet(ctx, "x10", "field", "bad")
					}
					return nil
				})
				return err
			}, replaceguard.RequiredKeys(r.rec)...)
			if err == nil {
				t.Fatal("faulted EXEC reported success")
			}
			if fault == "watch-abort" && (!errors.Is(err, redis.TxFailedErr) || fired.Load() != 1) {
				t.Fatalf("WATCH fault did not fire: %v fired=%d", err, fired.Load())
			}
			raw, err := r.db.Get(ctx, replaceguard.Key(21)).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			record, err := replaceguard.Decode(raw)
			if err != nil || record.Phase != "prepared" {
				t.Fatalf("false success receipt: %+v %v", record, err)
			}
			if fault == "watch-abort" {
				r.unchanged(t)
			} else {
				if r.db.HGet(ctx, "d3", "f").Val() != mkEntry(typeFile, 21) {
					t.Fatal("fixture did not actually execute partial rename")
				}
				if v := r.conn(t, "mac1").do("SET", "i21", string(r.attr)); v.Type != '-' {
					t.Fatalf("partial operation lost stage protection: %+v", v)
				}
			}
		})
	}
}
