package respproxy

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
)

func createBornSDK(t *testing.T) (*replacementRig, meta.Meta, replaceguard.Birth) {
	t.Helper()
	r := startBirthRig(t)
	m := birthSDK(t, r)
	op := strings.Repeat("9", 32)
	var ino meta.Ino
	var attr meta.Attr
	if st := m.Create(meta.Background(), 3, replaceguard.BirthStageName(op), 0600, 0, syscall.O_EXCL, &ino, &attr); st != 0 {
		t.Fatal(st)
	}
	raw, err := r.db.Get(context.Background(), replaceguard.BirthOperationKey(op)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	b, err := replaceguard.DecodeBirth(raw)
	if err != nil {
		t.Fatal(err)
	}
	return r, m, b
}
func birthSlice(t *testing.T, m meta.Meta, n uint32) meta.Slice {
	t.Helper()
	var id uint64
	if st := m.NewSlice(meta.Background(), &id); st != 0 {
		t.Fatal(st)
	}
	return meta.Slice{Id: id, Size: n, Len: n}
}
func bornArmRecord(r *replacementRig, b replaceguard.Birth, attr []byte, epoch uint64) replaceguard.Record {
	rec := r.rec
	rec.OperationID = b.OperationID
	rec.BackendUUID = b.BackendUUID
	rec.Epoch = epoch
	rec.Parents = b.Parents
	var a meta.Attr
	a.Unmarshal(attr)
	rec.Stage = replaceguard.File{Name: b.Stage.Name, Inode: b.Stage.Inode, Parent: b.Stage.Parent, Length: a.Length, MtimeNS: a.Mtime*1e9 + int64(a.Mtimensec), CtimeNS: a.Ctime*1e9 + int64(a.Ctimensec)}
	return rec
}
func enrollBorn(t *testing.T, r *replacementRig, b replaceguard.Birth, epoch uint64) replaceguard.Record {
	t.Helper()
	raw := r.db.Get(context.Background(), "i"+inoStr(b.Stage.Inode)).Val()
	rec := bornArmRecord(r, b, []byte(raw), epoch)
	encoded, err := replaceguard.Encode(rec)
	if err != nil {
		t.Fatal(err)
	}
	reply := r.conn(t, "mac1").do("JM.LEASE", "ARM_REPLACE", string(encoded))
	if reply.Type != '$' || reply.Null {
		t.Fatalf("birth ARM: %+v", reply)
	}
	result, err := replaceguard.Decode(reply.Str)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestBirthUploadActualSDKAndCurrentLease(t *testing.T) {
	r, m, b := createBornSDK(t)
	ctx := meta.Background()
	inode := meta.Ino(b.Stage.Inode)
	for _, length := range []uint32{5001, 23} {
		slice := birthSlice(t, m, length)
		if st := m.Write(ctx, inode, 0, 0, slice, time.Unix(20, 1)); st != 0 {
			t.Fatalf("actual SDK write: %v", st)
		}
	}
	if r.db.LLen(ctx, "c"+inoStr(b.Stage.Inode)+"_0").Val() != 2 || r.db.Get(ctx, "usedSpace").Val() != "1056768" {
		t.Fatal("write mapping or allocation differs")
	}
	attr := meta.Attr{Mode: 0600, Mtime: 30, Mtimensec: 2}
	if st := m.SetAttr(ctx, inode, meta.SetAttrMode|meta.SetAttrMtime, 0, &attr); st != 0 {
		t.Fatalf("timestamp/same-mode: %v", st)
	}
	oldBirth := r.db.Get(ctx, replaceguard.BirthInodeKey(b.Stage.Inode)).Val()
	epoch := replaceLeaseEpoch(t, r)
	if epoch == b.Epoch {
		t.Fatal("no new tenure")
	}
	if st := m.Write(ctx, inode, 0, 0, birthSlice(t, m, 31), time.Unix(40, 3)); st != 0 {
		t.Fatalf("same owner fresh lease: %v", st)
	}
	if r.db.Get(ctx, replaceguard.BirthInodeKey(b.Stage.Inode)).Val() != oldBirth {
		t.Fatal("historical birth refreshed")
	}
	r.unchanged(t)
}

func TestBirthUploadSameOwnerProxyRestart(t *testing.T) {
	r, m, b := createBornSDK(t)
	if st := m.Close(meta.Background(), meta.Ino(b.Stage.Inode)); st != 0 {
		t.Fatal(st)
	}
	_ = m.Shutdown()
	old := r.tp.Incarnation()
	tp := restartReplacementProxy(t, r)
	if tp.Incarnation() == old {
		t.Fatal("same incarnation")
	}
	// Separate adapter points at the fresh proxy; do not mutate the old rig
	// captured by its cleanup callbacks.
	resumed := *r
	copyRig := *r.leaseRig
	resumed.leaseRig = &copyRig
	resumed.tp = tp
	next := birthSDK(t, &resumed)
	if st := next.Write(meta.Background(), meta.Ino(b.Stage.Inode), 0, 0, birthSlice(t, next, 64), time.Unix(50, 4)); st != 0 {
		t.Fatalf("historical birth blocks fresh proxy: %v", st)
	}
	r.unchanged(t)
}

func TestBirthSealThenPublishUsesFinalGuard(t *testing.T) {
	r, m, b := createBornSDK(t)
	ctx := meta.Background()
	inode := meta.Ino(b.Stage.Inode)
	if st := m.Write(ctx, inode, 0, 0, birthSlice(t, m, 5), time.Unix(20, 0)); st != 0 {
		t.Fatal(st)
	}
	before := r.db.Get(ctx, "i"+inoStr(b.Stage.Inode)).Val()
	enrollBorn(t, r, b, 1)
	if st := m.Write(ctx, inode, 0, 0, birthSlice(t, m, 6), time.Unix(30, 0)); st == 0 {
		t.Fatal("write after ARM accepted")
	}
	attrs := meta.Attr{Mtime: 40}
	if st := m.SetAttr(ctx, inode, meta.SetAttrMtime, 0, &attrs); st == 0 {
		t.Fatal("setattr after ARM accepted")
	}
	if r.db.Get(ctx, "i"+inoStr(b.Stage.Inode)).Val() != before {
		t.Fatal("sealed image changed")
	}
	var published meta.Ino
	var attr meta.Attr
	if st := m.Rename(ctx, 3, b.Stage.Name, 3, "f", 0, &published, &attr); st != 0 || published != inode {
		t.Fatalf("born publication: %v %d", st, published)
	}
	if r.db.HExists(ctx, "d3", b.Stage.Name).Val() {
		t.Fatal("private name survived publication")
	}
	if st := m.Write(ctx, inode, 0, 0, birthSlice(t, m, 7), time.Unix(50, 0)); st != 0 {
		t.Fatalf("committed destination ordinary write: %v", st)
	}
	if r.db.HGet(ctx, "d3", "f").Val() != mkEntry(typeFile, b.Stage.Inode) {
		t.Fatal("published identity differs")
	}
}

func TestBirthUploadRefusesWatchedUnsupportedChanges(t *testing.T) {
	for _, which := range []string{"length-without-chunk", "shrink", "mode", "uid", "gid", "atime", "nlink", "parent", "flags", "rdev", "invalid-time", "wrong-op", "wrong-uuid", "prepared-other-operation", "missing-watch", "renamed-stage"} {
		t.Run(which, func(t *testing.T) {
			r, m, b := createBornSDK(t)
			ctx := context.Background()
			key := "i" + inoStr(b.Stage.Inode)
			if st := m.Write(meta.Background(), meta.Ino(b.Stage.Inode), 0, 0, birthSlice(t, m, 5), time.Unix(20, 0)); st != 0 {
				t.Fatal(st)
			}
			old := r.db.Get(ctx, key).Val()
			next := []byte(old)
			keys := replaceguard.BirthRequiredKeys(b)
			switch which {
			case "length-without-chunk":
				binary.BigEndian.PutUint64(next[51:59], 6)
			case "shrink":
				binary.BigEndian.PutUint64(next[51:59], 4)
			case "mode":
				next[2] |= 4
			case "uid":
				next[6]++
			case "gid":
				next[10]++
			case "atime":
				next[18]++
			case "nlink":
				next[50]++
			case "parent":
				next[70]++
			case "flags":
				next[0] = 1
			case "rdev":
				next[62] = 1
			case "invalid-time":
				binary.BigEndian.PutUint32(next[31:35], 1000000000)
			case "wrong-op":
				if err := r.db.Set(ctx, replaceguard.BirthOperationKey(b.OperationID), "wrong", 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "wrong-uuid":
				if err := r.db.Set(ctx, "setting", `{"UUID":"different"}`, 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "prepared-other-operation":
				rec := bornArmRecord(r, b, []byte(old), 1)
				rec.OperationID = strings.Repeat("8", 32)
				raw, _ := replaceguard.Encode(rec)
				if err := r.db.Set(ctx, replaceguard.Key(b.Stage.Inode), raw, 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "missing-watch":
				keys = keys[1:]
			case "renamed-stage":
				if err := r.db.HDel(ctx, "d3", b.Stage.Name).Err(); err != nil {
					t.Fatal(err)
				}
			}
			mac := r.conn(t, "mac1")
			if v := mac.do(append([]string{"WATCH"}, keys...)...); v.Type == '-' {
				t.Fatal(v)
			}
			if v := txn(mac, []string{"SET", key, string(next)}); v.Type != '-' {
				t.Fatalf("%s accepted %+v", which, v)
			}
			if r.db.Get(ctx, key).Val() != old {
				t.Fatal("refused update changed inode")
			}
			r.unchanged(t)
		})
	}
}

func TestBirthPeerCannotUseCurrentLeaseToMutateStage(t *testing.T) {
	r, m, b := createBornSDK(t)
	ctx := context.Background()
	mac := r.conn(t, "mac1")
	if v := mac.do("JM.LEASE", "RELEASE", "ino:3", "1"); v.Type != '*' {
		t.Fatal(v)
	}
	peer := r.conn(t, "mac2")
	if v := peer.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' || string(v.Elems[0].Str) != "GRANTED" {
		t.Fatal(v)
	}
	key := "i" + inoStr(b.Stage.Inode)
	old := r.db.Get(ctx, key).Val()
	next := []byte(old)
	binary.BigEndian.PutUint64(next[23:31], 77)
	if v := peer.do(append([]string{"WATCH"}, replaceguard.BirthRequiredKeys(b)...)...); v.Type == '-' {
		t.Fatal(v)
	}
	if v := txn(peer, []string{"SET", key, string(next)}); v.Type != '-' {
		t.Fatalf("current foreign holder mutated private stage: %+v", v)
	}
	if st := m.Write(meta.Background(), meta.Ino(b.Stage.Inode), 0, 0, birthSlice(t, m, 5), time.Unix(20, 0)); st == 0 {
		t.Fatal("original owner wrote without lease")
	}
	if r.db.Get(ctx, key).Val() != old {
		t.Fatal("stage changed")
	}
	r.unchanged(t)
}

func TestBirthARMRejectsMissingOrDisagreeingPair(t *testing.T) {
	for _, which := range []string{"missing-inode", "missing-operation", "mismatched-operation"} {
		t.Run(which, func(t *testing.T) {
			r, _, b := createBornSDK(t)
			ctx := context.Background()
			key := replaceguard.BirthInodeKey(b.Stage.Inode)
			if which != "missing-inode" {
				key = replaceguard.BirthOperationKey(b.OperationID)
			}
			if which == "mismatched-operation" {
				if err := r.db.Set(ctx, key, "corrupt", 0).Err(); err != nil {
					t.Fatal(err)
				}
			} else if err := r.db.Del(ctx, key).Err(); err != nil {
				t.Fatal(err)
			}
			rec := bornArmRecord(r, b, []byte(r.db.Get(ctx, "i"+inoStr(b.Stage.Inode)).Val()), 1)
			raw, _ := replaceguard.Encode(rec)
			if v := r.conn(t, "mac1").do("JM.LEASE", "ARM_REPLACE", string(raw)); v.Type != '-' {
				t.Fatalf("bad pair ARM: %+v", v)
			}
			if r.db.Exists(ctx, replaceguard.Key(b.Stage.Inode)).Val() != 0 {
				t.Fatal("bad pair sealed")
			}
			r.unchanged(t)
		})
	}
}

func TestBirthARMWaitsForAdmittedUpload(t *testing.T) {
	r, m, b := createBornSDK(t)
	slice := birthSlice(t, m, 5)
	admitted := make(chan []byte, 1)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	r.eng.hooks.Store(&engineHooks{admitted: func(_ Principal, u *Unit) {
		for _, cm := range u.Cmds {
			if cm.Info.Name == "RPUSH" && string(cm.Args[1]) == "c"+inoStr(b.Stage.Inode)+"_0" {
				for _, set := range u.Cmds {
					if set.Info.Name == "SET" && string(set.Args[1]) == "i"+inoStr(b.Stage.Inode) {
						admitted <- append([]byte(nil), set.Args[2]...)
						<-release
						return
					}
				}
			}
		}
	}})
	written := make(chan syscall.Errno, 1)
	go func() { written <- m.Write(meta.Background(), meta.Ino(b.Stage.Inode), 0, 0, slice, time.Unix(30, 0)) }()
	var expected []byte
	select {
	case expected = <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("write not admitted")
	}
	rec := bornArmRecord(r, b, expected, 1)
	raw, _ := replaceguard.Encode(rec)
	client := r.conn(t, "mac1")
	armed := make(chan Value, 1)
	go func() { armed <- client.do("JM.LEASE", "ARM_REPLACE", string(raw)) }()
	select {
	case got := <-armed:
		t.Fatalf("ARM passed admitted upload: %+v", got)
	case <-time.After(25 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	select {
	case st := <-written:
		if st != 0 {
			t.Fatal(st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not join")
	}
	select {
	case got := <-armed:
		if got.Type != '$' {
			t.Fatalf("seal after upload: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ARM did not join")
	}
	if got := r.db.Get(context.Background(), "i"+inoStr(b.Stage.Inode)).Val(); got != string(expected) {
		t.Fatal("seal did not bind completed upload")
	}
}
