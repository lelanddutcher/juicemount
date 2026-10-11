package respproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
)

func startBirthRig(t *testing.T) *replacementRig {
	t.Helper()
	r := startReplacementRig(t)
	// Creation opt-in is set before any birth client exists and never changed
	// concurrently. Existing record protection is independent of this switch.
	r.tp.stageBirth.Store(true)
	format := meta.Format{Name: "birth-fixture", UUID: "12345678-1234-1234-1234-123456789abc", BlockSize: 4096}
	raw, _ := json.Marshal(format)
	for key, value := range map[string]interface{}{"setting": raw, "nextinode": 1000} {
		if err := r.db.Set(context.Background(), key, value, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func birthSDK(t *testing.T, r *replacementRig) meta.Meta {
	t.Helper()
	conf := meta.DefaultConf()
	conf.Retries = 0
	conf.MaxDeletes = 0
	conf.NoBGJob = true
	m := meta.NewClient("redis://mac1:mac1-pass@"+r.tp.addr+"/15", conf)
	t.Cleanup(func() { _ = m.Shutdown() })
	if _, err := m.Load(false); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestBirthActualSDKCreateThroughProxy(t *testing.T) {
	r := startBirthRig(t)
	m := birthSDK(t, r)
	ctx := meta.Background()
	op := strings.Repeat("c", 32)
	name := replaceguard.BirthStageName(op)
	var ino meta.Ino
	var attr meta.Attr
	if st := m.Create(ctx, 3, name, 0600, 0, syscall.O_EXCL, &ino, &attr); st != 0 {
		t.Fatalf("real SDK create: %v", st)
	}
	raw, err := r.db.Get(ctx, replaceguard.BirthOperationKey(op)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	b, err := replaceguard.DecodeBirth(raw)
	if err != nil || b.Stage.Inode != uint64(ino) || b.Owner != "mac1" || b.Proxy != r.tp.Incarnation() || b.Database != testDB || b.Project != 3 || b.Epoch != 1 || len(b.Parents) != 3 || b.Parents[1].Name != "Projects" || b.Parents[2].Name != "A" {
		t.Fatalf("birth: %+v %v", b, err)
	}
	if r.db.Get(ctx, replaceguard.BirthInodeKey(uint64(ino))).Val() != string(raw) {
		t.Fatal("reciprocal birth differs")
	}
	actual := r.db.Get(ctx, "i"+ino.String()).Val()
	digest := sha256.Sum256([]byte(actual))
	if b.AttrSHA256 != hex.EncodeToString(digest[:]) || b.Mode != 0600 || b.Stage.Length != 0 {
		t.Fatal("raw attributes not bound")
	}
	if r.db.HGet(ctx, "d3", name).Val() != mkEntry(typeFile, uint64(ino)) || r.db.Get(ctx, "usedSpace").Val() != "1052672" || r.db.Get(ctx, "totalInodes").Val() != "101" {
		t.Fatal("create postconditions differ")
	}
	if st := m.Close(ctx, ino); st != 0 {
		t.Fatal(st)
	}
	// A second create cannot retarget the immutable operation, even O_CREAT
	// without O_EXCL. It must not turn a failed owned create into adoption.
	var retry meta.Ino
	var retryAttr meta.Attr
	if st := m.Create(ctx, 3, name, 0600, 0, 0, &retry, &retryAttr); st == 0 {
		t.Fatal("operation was adopted")
	}
	// A normal name remains usable; no birth lookup is inferred from its name.
	if st := m.Create(ctx, 3, "ordinary.mov", 0600, 0, syscall.O_EXCL, &retry, &retryAttr); st != 0 {
		t.Fatalf("ordinary create: %v", st)
	}
	_ = m.Close(ctx, retry)
	r.unchanged(t)
}

func rawBirth(t *testing.T, r *replacementRig) (replaceguard.Birth, [][]string) {
	t.Helper()
	attr := (&meta.Attr{Typ: meta.TypeFile, Mode: 0600, Nlink: 1, Parent: 3, Mtime: 10, Mtimensec: 1, Ctime: 10, Ctimensec: 2}).Marshal()
	hash := sha256.Sum256(attr)
	op := strings.Repeat("d", 32)
	b := replaceguard.Birth{Version: 1, OperationID: op, Owner: "mac1", Proxy: r.tp.Incarnation(), Database: testDB, BackendUUID: "12345678-1234-1234-1234-123456789abc", Project: 3, Epoch: 1, Parents: []replaceguard.Parent{{Inode: 1}, {Inode: 2, Name: "Projects"}, {Inode: 3, Name: "A"}}, Stage: replaceguard.File{Inode: 101, Parent: 3, Name: replaceguard.BirthStageName(op), MtimeNS: 10000000001, CtimeNS: 10000000002}, Type: 1, Nlink: 1, Mode: 0600, AttrSHA256: hex.EncodeToString(hash[:])}
	return b, [][]string{{"SET", "i101", string(attr)}, {"HSET", "d3", b.Stage.Name, mkEntry(typeFile, 101)}, {"INCRBY", "usedSpace", "4096"}, {"INCR", "totalInodes"}}
}

func TestBirthCreateRefusesUnsupportedUnit(t *testing.T) {
	for _, which := range []string{"missing-watch", "existing-op", "existing-inode-index", "existing-inode", "existing-name", "counter-type", "counter-overflow", "counter-noncanonical", "inode-counter-overflow", "alias", "chunk", "parent-mode", "bad-uuid", "ancestry-alias", "observe"} {
		t.Run(which, func(t *testing.T) {
			r := startBirthRig(t)
			b, commands := rawBirth(t, r)
			ctx := context.Background()
			keys := replaceguard.BirthRequiredKeys(b)
			set := func(k string, v interface{}) {
				t.Helper()
				if err := r.db.Set(ctx, k, v, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			switch which {
			case "missing-watch":
				keys = keys[1:]
			case "existing-op":
				set(replaceguard.BirthOperationKey(b.OperationID), "occupied")
			case "existing-inode-index":
				set(replaceguard.BirthInodeKey(b.Stage.Inode), "occupied")
			case "existing-inode":
				set("i101", commands[0][2])
			case "existing-name":
				if err := r.db.HSet(ctx, "d3", b.Stage.Name, mkEntry(typeFile, 10)).Err(); err != nil {
					t.Fatal(err)
				}
			case "counter-type":
				if err := r.db.Del(ctx, "usedSpace").Err(); err != nil {
					t.Fatal(err)
				}
				if err := r.db.HSet(ctx, "usedSpace", "bad", "1").Err(); err != nil {
					t.Fatal(err)
				}
			case "counter-overflow":
				set("usedSpace", "9223372036854771712")
			case "counter-noncanonical":
				set("usedSpace", "01")
			case "inode-counter-overflow":
				set("totalInodes", "9223372036854775807")
			case "alias":
				commands = append(commands, []string{"HSET", "d3", "alias", mkEntry(typeFile, 101)})
			case "chunk":
				commands = append(commands, []string{"RPUSH", "c101_0", "invalid"})
			case "parent-mode":
				a := (&meta.Attr{Typ: meta.TypeDirectory, Mode: 0, Nlink: 2, Parent: 2, Length: 4096}).Marshal()
				commands = append(commands, []string{"SET", "i3", string(a)})
			case "bad-uuid":
				set("setting", `{"UUID":"bad"}`)
			case "ancestry-alias":
				if err := r.db.HSet(ctx, "d2", "alias", mkEntry(typeDir, 3)).Err(); err != nil {
					t.Fatal(err)
				}
			case "observe":
				r.tp.setMode(ModeObserve, "test")
			}
			mac := r.conn(t, "mac1")
			watch := append([]string{"WATCH"}, keys...)
			if v := mac.do(watch...); v.Type == '-' {
				t.Fatalf("watch: %+v", v)
			}
			if v := txn(mac, commands...); v.Type != '-' {
				t.Fatalf("unsupported create accepted: %+v", v)
			}
			if which != "existing-op" && r.db.Exists(ctx, replaceguard.BirthOperationKey(b.OperationID)).Val() != 0 {
				t.Fatal("failed create gained receipt")
			}
			if which != "existing-inode" && r.db.Exists(ctx, "i101").Val() != 0 {
				t.Fatal("failed create installed inode")
			}
			r.unchanged(t)
		})
	}
}

func TestBirthRemainsProtectedWithoutCreationGate(t *testing.T) {
	for _, mode := range []Mode{ModeEnforce, ModeObserve, ModePassthrough} {
		t.Run(string(mode), func(t *testing.T) {
			r := startBirthRig(t)
			b, cmds := rawBirth(t, r)
			mac := r.conn(t, "mac1")
			if v := mac.do(append([]string{"WATCH"}, replaceguard.BirthRequiredKeys(b)...)...); v.Type == '-' {
				t.Fatal(v)
			}
			if v := txn(mac, cmds...); v.Type != '*' || len(v.Elems) != len(cmds) {
				t.Fatalf("create result slots: %+v", v)
			}
			// New writes are serialized after the first EXEC response.
			r.tp.stageBirth.Store(false)
			r.tp.setMode(mode, "test")
			for _, who := range []string{"mac1", "mac2"} {
				client := r.conn(t, who)
				for _, command := range [][]string{{"SET", "i101", cmds[0][2]}, {"DEL", "i101"}, {"RPUSH", "c101_0", "data"}, {"HDEL", "d3", b.Stage.Name}, {"HSET", "d3", "alias", mkEntry(typeFile, 101)}, {"HSET", "d3", b.Stage.Name, mkEntry(typeFile, 10)}, {"DEL", replaceguard.BirthOperationKey(b.OperationID)}, {"SET", replaceguard.BirthInodeKey(101), "tamper"}} {
					for _, multi := range []bool{false, true} {
						var v Value
						if multi {
							v = txn(client, command)
						} else {
							v = client.do(command...)
						}
						if v.Type != '-' {
							t.Fatalf("%s multi=%v %s accepted %+v", who, multi, command[0], v)
						}
					}
				}
			}
			if r.db.HGet(context.Background(), "d3", b.Stage.Name).Val() != mkEntry(typeFile, 101) || r.db.Get(context.Background(), "i101").Val() != cmds[0][2] {
				t.Fatal("born stage changed")
			}
			r.unchanged(t)
		})
	}
}

func TestBirthReceiptPostconditionsPrecedeMutation(t *testing.T) {
	for _, which := range []string{"inode", "dentry", "counter", "existing-index"} {
		t.Run(which, func(t *testing.T) {
			r := startBirthRig(t)
			b, cmds := rawBirth(t, r)
			ctx := context.Background()
			raw, _ := replaceguard.EncodeBirth(b)
			// Exercise the exact trusted script against simulated partial transaction state.
			if err := r.db.Set(ctx, "i101", cmds[0][2], 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := r.db.HSet(ctx, "d3", b.Stage.Name, mkEntry(typeFile, 101)).Err(); err != nil {
				t.Fatal(err)
			}
			args := []interface{}{string(raw), "GET", "i101", "", cmds[0][2], "HGET", "d3", b.Stage.Name, mkEntry(typeFile, 101), "GET", "usedSpace", "", "1048576"}
			switch which {
			case "inode":
				args[4] = "changed"
			case "dentry":
				args[8] = "changed"
			case "counter":
				args[12] = "wrong"
			case "existing-index":
				if err := r.db.Set(ctx, replaceguard.BirthInodeKey(101), "occupied", 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.db.Eval(ctx, birthCommitLua, []string{replaceguard.BirthOperationKey(b.OperationID), replaceguard.BirthInodeKey(101), replaceguard.Key(101)}, args...).Err(); err == nil {
				t.Fatal("partial create received receipt")
			}
			if r.db.Exists(ctx, replaceguard.BirthOperationKey(b.OperationID)).Val() != 0 {
				t.Fatal("receipt written before final predicate")
			}
		})
	}
}

func TestBirthLegacyStageCannotBeAdopted(t *testing.T) {
	r := startBirthRig(t)
	m := birthSDK(t, r)
	ctx := meta.Background()
	name := replaceguard.BirthStageName(strings.Repeat("e", 32))
	if err := r.db.HSet(ctx, "d3", name, mkEntry(typeFile, 10)).Err(); err != nil {
		t.Fatal(err)
	}
	for _, flags := range []uint32{0, syscall.O_EXCL} {
		var ino meta.Ino
		var attr meta.Attr
		if st := m.Create(ctx, 3, name, 0600, 0, flags, &ino, &attr); st != syscall.EEXIST || ino != 0 || attr.Typ != 0 {
			t.Fatalf("legacy collision flags=%d got=%v inode=%d type=%d", flags, st, ino, attr.Typ)
		}
	}
	r.unchanged(t)
}

func TestBirthLostSDKCreateReplyRetainsCustody(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		name := "ordinary"
		if delayed {
			name = "delayed-bookkeeping"
		}
		t.Run(name, func(t *testing.T) {
			r := startBirthRig(t)
			m := birthSDK(t, r)
			type closeResult struct {
				matched bool
				err     error
			}
			hookDone := make(chan closeResult, 1)
			closed := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce, hookOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			// Run before the rig's cleanup even when an assertion fails while the
			// hook is paused outside tp.mu.
			t.Cleanup(unblock)
			var lost atomic.Bool
			r.eng.hooks.Store(&engineHooks{done: func(p Principal, u *Unit, o Outcome, _ map[uint64]uint64) {
				if u.replacementReceipt == nil || !o.Committed {
					return
				}
				hookOnce.Do(func() {
					r.tp.mu.Lock()
					result := closeResult{}
					for client := range r.tp.conns {
						if client.RemoteAddr().String() == u.Remote {
							result.matched = true
							result.err = client.Close()
							break
						}
					}
					r.tp.mu.Unlock()
					if result.matched && result.err == nil {
						close(closed)
						if delayed {
							<-release
						}
						lost.Store(true)
					}
					hookDone <- result
				})
			}})
			op := strings.Repeat("f", 32)
			createCtx, cancelCreate := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelCreate()
			sdkCtx := meta.WrapContext(createCtx)
			defer sdkCtx.Cancel()
			var ino meta.Ino
			var attr meta.Attr
			st := m.Create(sdkCtx, 3, replaceguard.BirthStageName(op), 0600, 0, syscall.O_EXCL, &ino, &attr)
			if createCtx.Err() != nil {
				t.Fatalf("SDK create deadline is not a lost-reply witness: %v", createCtx.Err())
			}
			if st == 0 {
				t.Fatal("injected lost acknowledgement reported success")
			}
			if delayed {
				select {
				case <-closed:
				case result := <-hookDone:
					if !result.matched {
						t.Fatal("committed hook found no matching SDK client")
					}
					t.Fatalf("committed hook could not close SDK client: %v", result.err)
				case <-time.After(5 * time.Second):
					t.Fatal("committed hook did not close SDK connection")
				}
				if lost.Load() {
					t.Fatal("hook bookkeeping completed before release")
				}
				unblock()
			}
			select {
			case result := <-hookDone:
				if !result.matched {
					t.Fatal("committed hook found no matching SDK client")
				}
				if result.err != nil {
					t.Fatalf("committed hook could not close SDK client: %v", result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("committed hook did not complete")
			}
			if !lost.Load() {
				t.Fatal("did not interrupt actual committed create reply")
			}
			raw, err := r.db.Get(context.Background(), replaceguard.BirthOperationKey(op)).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			b, err := replaceguard.DecodeBirth(raw)
			if err != nil {
				t.Fatal(err)
			}
			if r.db.Get(context.Background(), replaceguard.BirthInodeKey(b.Stage.Inode)).Val() != string(raw) || r.db.HGet(context.Background(), "d3", b.Stage.Name).Val() != mkEntry(typeFile, b.Stage.Inode) {
				t.Fatal("lost create custody differs")
			}
			r.unchanged(t)
		})
	}
}

func TestBirthEveryWatchedPredicateAbortsCreate(t *testing.T) {
	keys := []string{replaceguard.BirthOperationKey(strings.Repeat("d", 32)), replaceguard.BirthInodeKey(101), replaceguard.Key(101), "setting", SingletonKey, leaseKey(3), epochKey(3), "usedSpace", "totalInodes", "i1", "d1", "i2", "d2", "i3", "d3", "i101", "d101"}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			r := startBirthRig(t)
			b, cmds := rawBirth(t, r)
			mac := r.conn(t, "mac1")
			ctx := context.Background()
			if v := mac.do(append([]string{"WATCH"}, replaceguard.BirthRequiredKeys(b)...)...); v.Type == '-' {
				t.Fatal(v)
			}
			// Change and restore the key to prove Redis WATCH, not only a value check,
			// prevents creation. DUMP/RESTORE preserves arbitrary string/hash types.
			dump, err := r.db.Dump(ctx, key).Bytes()
			if err != nil && !strings.Contains(err.Error(), "redis: nil") {
				t.Fatal(err)
			}
			if err := r.db.Set(ctx, key, "changed", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if dump == nil {
				if err := r.db.Del(ctx, key).Err(); err != nil {
					t.Fatal(err)
				}
			} else if err := r.db.RestoreReplace(ctx, key, 0, string(dump)).Err(); err != nil {
				t.Fatal(err)
			}
			if v := txn(mac, cmds...); v.Type != '*' || !v.Null {
				t.Fatalf("WATCH change failed to abort: %+v", v)
			}
			if r.db.Exists(ctx, "i101", replaceguard.BirthOperationKey(b.OperationID), replaceguard.BirthInodeKey(101)).Val() != 0 {
				t.Fatal("aborted create gained state")
			}
			r.unchanged(t)
		})
	}
}
