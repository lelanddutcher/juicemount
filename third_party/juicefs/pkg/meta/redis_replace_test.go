//go:build !noredis

package meta

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/juicedata/juicefs/pkg/slicegraph"
)

// This suite requires an explicitly allocated, empty Redis DB. It never uses
// the inherited localhost test target and never invokes FLUSHDB.
func guardedRedis(t *testing.T) *redisMeta {
	t.Helper()
	address := os.Getenv("JM_REPLACE_TEST_REDIS_URL")
	if address == "" {
		t.Skip("JM_REPLACE_TEST_REDIS_URL is required")
	}
	if !strings.HasPrefix(address, "redis://") || !strings.Contains(strings.TrimPrefix(address, "redis://"), "/") {
		t.Fatal("JM_REPLACE_TEST_REDIS_URL must be an explicit redis://host:port/db URL")
	}
	store, err := newRedisMeta("redis", strings.TrimPrefix(address, "redis://"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	m := store.(*redisMeta)
	t.Cleanup(func() { _ = m.Shutdown() })
	count, err := m.rdb.DBSize(Background()).Result()
	if err != nil || count != 0 {
		t.Fatalf("replacement Redis test DB must be empty and owned by the runner: size=%d err=%v", count, err)
	}
	return m
}

type guardedFixture struct {
	m     *redisMeta
	r     replaceguard.Record
	stage Ino
	dest  Ino
	trash Ino
	chunk []byte
}

func seedGuardedRename(t *testing.T, m *redisMeta) guardedFixture {
	t.Helper()
	ctx := Background()
	stage, dest, trash := Ino(73001), Ino(73002), Ino(73003)
	format := &Format{UUID: "replacement-fixture-volume", TrashDays: 1}
	m.fmt = format
	m.subTrash.name = time.Now().UTC().Format("2006-01-02-15")
	m.subTrash.inode = trash
	setting, _ := json.Marshal(format)
	old := slicegraph.Graph{Version: slicegraph.Version, Length: slicegraph.ChunkSize + 3,
		Chunks: []slicegraph.Chunk{{Index: 0, Extents: []slicegraph.Extent{
			{Pos: 1, SliceID: 91, Size: 4, Len: 4}, {Pos: 8, Len: 2},
		}}, {Index: 1}}}
	canon, err := old.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canon)
	r := replaceguard.Record{Version: 1, OperationID: strings.Repeat("a", 32), Owner: "mac-a", Proxy: "proxy-a",
		BackendUUID: format.UUID, BackendDigest: strings.Repeat("b", 64), Project: 3,
		AcceptedEpoch: 4, Epoch: 5, Parents: []replaceguard.Parent{{Inode: 1}, {Name: "Projects", Inode: 2}, {Name: "P", Inode: 3}},
		Stage: replaceguard.File{Name: ".juicemount-streaming-drain-fixture", Parent: 3, Inode: uint64(stage), Length: 5,
			MtimeNS: 10_000_000_101, CtimeNS: 10_000_000_102},
		Destination: replaceguard.File{Name: "clip.mov", Parent: 3, Inode: uint64(dest), Length: old.Length,
			MtimeNS: 11_000_000_101, CtimeNS: 11_000_000_102},
		GraphDigest: hex.EncodeToString(digest[:]), ImageSHA256: strings.Repeat("c", 64), Phase: "prepared"}
	record, err := replaceguard.Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	root := &Attr{Typ: TypeDirectory, Mode: 0777, Nlink: 2, Parent: 1, Length: 4096}
	projects := &Attr{Typ: TypeDirectory, Mode: 0777, Nlink: 2, Parent: 1, Length: 4096}
	project := &Attr{Typ: TypeDirectory, Mode: 0777, Nlink: 2, Parent: 2, Length: 4096}
	stageAttr := &Attr{Typ: TypeFile, Mode: 0666, Nlink: 1, Parent: 3, Length: r.Stage.Length,
		Mtime: 10, Mtimensec: 101, Ctime: 10, Ctimensec: 102}
	destAttr := &Attr{Typ: TypeFile, Mode: 0666, Nlink: 1, Parent: 3, Length: r.Destination.Length,
		Mtime: 11, Mtimensec: 101, Ctime: 11, Ctimensec: 102}
	chunk := make([]byte, 24)
	binary.BigEndian.PutUint32(chunk[0:4], 1)
	binary.BigEndian.PutUint64(chunk[4:12], 91)
	binary.BigEndian.PutUint32(chunk[12:16], 4)
	binary.BigEndian.PutUint32(chunk[20:24], 4)
	zero := make([]byte, 24)
	binary.BigEndian.PutUint32(zero[0:4], 8)
	binary.BigEndian.PutUint32(zero[20:24], 2)
	commands := []error{
		m.rdb.Set(ctx, m.setting(), setting, 0).Err(),
		m.rdb.Set(ctx, "jmlease:v1:proxy", r.Proxy, 0).Err(),
		m.rdb.Set(ctx, "jmlease:v1:epoch:3", "5", 0).Err(),
		m.rdb.HSet(ctx, "jmlease:v1:p:3", "holder", r.Owner, "epoch", "5", "e:foreign", "4").Err(),
		m.rdb.Set(ctx, replaceguard.Key(uint64(stage)), record, 0).Err(),
		m.rdb.Set(ctx, m.inodeKey(1), m.marshal(root), 0).Err(),
		m.rdb.Set(ctx, m.inodeKey(2), m.marshal(projects), 0).Err(),
		m.rdb.Set(ctx, m.inodeKey(3), m.marshal(project), 0).Err(),
		m.rdb.Set(ctx, m.inodeKey(stage), m.marshal(stageAttr), 0).Err(),
		m.rdb.Set(ctx, m.inodeKey(dest), m.marshal(destAttr), 0).Err(),
		m.rdb.HSet(ctx, m.entryKey(1), "Projects", m.packEntry(TypeDirectory, 2)).Err(),
		m.rdb.HSet(ctx, m.entryKey(2), "P", m.packEntry(TypeDirectory, 3)).Err(),
		m.rdb.HSet(ctx, m.entryKey(3), r.Stage.Name, m.packEntry(TypeFile, stage), r.Destination.Name, m.packEntry(TypeFile, dest)).Err(),
		m.rdb.HSet(ctx, m.entryKey(trash), "fixture", "placeholder").Err(),
		m.rdb.RPush(ctx, m.chunkKey(dest, 0), chunk, zero).Err(),
	}
	for _, err := range commands {
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_ = m.rdb.Del(ctx, m.setting(), "jmlease:v1:proxy", "jmlease:v1:epoch:3", "jmlease:v1:p:3",
			replaceguard.Key(uint64(stage)), m.inodeKey(1), m.inodeKey(2), m.inodeKey(3), m.inodeKey(stage), m.inodeKey(dest),
			m.entryKey(1), m.entryKey(2), m.entryKey(3), m.entryKey(trash), m.chunkKey(dest, 0), m.chunkKey(dest, 1)).Err()
	})
	return guardedFixture{m: m, r: r, stage: stage, dest: dest, trash: trash, chunk: chunk}
}

func (f guardedFixture) rename() syscall.Errno {
	var ino, previous Ino
	var attr, previousAttr Attr
	return f.m.doRename(Background(), 3, f.r.Stage.Name, 3, f.r.Destination.Name, 0,
		&ino, &previous, &attr, &previousAttr)
}

func TestProtectedRenameSparseMultichunkTrash(t *testing.T) {
	m := guardedRedis(t)
	f := seedGuardedRename(t, m)
	if got := f.rename(); got != 0 {
		t.Fatalf("guarded rename: %v", got)
	}
	ctx := Background()
	got, err := m.rdb.HGet(ctx, m.entryKey(3), f.r.Destination.Name).Bytes()
	if err != nil || string(got) != string(m.packEntry(TypeFile, f.stage)) {
		t.Fatalf("destination entry: %x, %v", got, err)
	}
	if _, err := m.rdb.HGet(ctx, m.entryKey(3), f.r.Stage.Name).Result(); err == nil {
		t.Fatal("stage entry remained after rename")
	}
	if ok, err := m.rdb.Exists(ctx, replaceguard.Key(uint64(f.stage))).Result(); err != nil || ok != 1 {
		t.Fatalf("proxy record changed: %d, %v", ok, err)
	}
}

func TestOrdinaryRenameWithoutReplacementRecord(t *testing.T) {
	m := guardedRedis(t)
	f := seedGuardedRename(t, m)
	if err := m.rdb.Del(Background(), replaceguard.Key(uint64(f.stage))).Err(); err != nil {
		t.Fatal(err)
	}
	if got := f.rename(); got != 0 {
		t.Fatalf("ordinary rename after absent guard: %v", got)
	}
}

func TestProtectedRenameRefusesChangedGraphAndAuthority(t *testing.T) {
	mutations := map[string]func(t *testing.T, f guardedFixture){
		"same-attrs-changed-chunk": func(t *testing.T, f guardedFixture) {
			b := append([]byte(nil), f.chunk...)
			binary.BigEndian.PutUint64(b[4:12], 92)
			if err := f.m.rdb.LSet(Background(), f.m.chunkKey(f.dest, 0), 0, b).Err(); err != nil {
				t.Fatal(err)
			}
		},
		"foreign-tenure": func(t *testing.T, f guardedFixture) {
			if err := f.m.rdb.HSet(Background(), "jmlease:v1:p:3", "e:foreign", "5").Err(); err != nil {
				t.Fatal(err)
			}
		},
		"counter": func(t *testing.T, f guardedFixture) {
			if err := f.m.rdb.Set(Background(), "jmlease:v1:epoch:3", "6", 0).Err(); err != nil {
				t.Fatal(err)
			}
		},
		"singleton": func(t *testing.T, f guardedFixture) {
			if err := f.m.rdb.Set(Background(), "jmlease:v1:proxy", "proxy-b", 0).Err(); err != nil {
				t.Fatal(err)
			}
		},
		"sealed-stage": func(t *testing.T, f guardedFixture) {
			a := &Attr{Typ: TypeFile, Mode: 0666, Nlink: 1, Parent: 3, Length: f.r.Stage.Length + 1,
				Mtime: 10, Mtimensec: 101, Ctime: 10, Ctimensec: 102}
			if err := f.m.rdb.Set(Background(), f.m.inodeKey(f.stage), f.m.marshal(a), 0).Err(); err != nil {
				t.Fatal(err)
			}
		},
		"ancestor-edge": func(t *testing.T, f guardedFixture) {
			if err := f.m.rdb.HSet(Background(), f.m.entryKey(2), "P", f.m.packEntry(TypeDirectory, 9)).Err(); err != nil {
				t.Fatal(err)
			}
		},
		"malformed-record": func(t *testing.T, f guardedFixture) {
			if err := f.m.rdb.Set(Background(), replaceguard.Key(uint64(f.stage)), "{bad", 0).Err(); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			m := guardedRedis(t)
			f := seedGuardedRename(t, m)
			mutate(t, f)
			if got := f.rename(); got == 0 {
				t.Fatal("accepted stale or malformed guard")
			}
			ctx := Background()
			for name, want := range map[string]Ino{f.r.Stage.Name: f.stage, f.r.Destination.Name: f.dest} {
				entry, err := m.rdb.HGet(ctx, m.entryKey(3), name).Bytes()
				if err != nil || string(entry) != string(m.packEntry(TypeFile, want)) {
					t.Fatalf("entry %s changed after refusal: %x %v", name, entry, err)
				}
			}
		})
	}
}

func TestProtectedRenameRefusesCommittedAndRepeatedName(t *testing.T) {
	m := guardedRedis(t)
	f := seedGuardedRename(t, m)
	r := f.r
	r.Phase = "committed"
	encoded, err := replaceguard.Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.rdb.Set(Background(), replaceguard.Key(uint64(f.stage)), encoded, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := f.rename(); got == 0 {
		t.Fatal("committed record was accepted")
	}
	var ino Ino
	if got := m.doRename(Background(), 3, r.Stage.Name, 3, r.Stage.Name, 0, &ino, nil, nil, nil); got == 0 {
		t.Fatal("protected same-name shortcut was accepted")
	}
}

func TestCommittedRecordAllowsPublishedFileMove(t *testing.T) {
	m := guardedRedis(t)
	f := seedGuardedRename(t, m)
	if got := f.rename(); got != 0 {
		t.Fatalf("publish: %v", got)
	}
	r := f.r
	r.Phase = "committed"
	encoded, err := replaceguard.Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.rdb.Set(Background(), replaceguard.Key(uint64(f.stage)), encoded, 0).Err(); err != nil {
		t.Fatal(err)
	}
	var ino, previous Ino
	var attr, previousAttr Attr
	if got := m.doRename(Background(), 3, r.Destination.Name, 3, "moved.mov", 0,
		&ino, &previous, &attr, &previousAttr); got != 0 {
		t.Fatalf("move published file: %v", got)
	}
	entry, err := m.rdb.HGet(Background(), m.entryKey(3), "moved.mov").Bytes()
	if err != nil || string(entry) != string(m.packEntry(TypeFile, f.stage)) {
		t.Fatalf("moved entry: %x, %v", entry, err)
	}
}

func TestProtectedRenamePrevalidatesSelectedNoTrashKeys(t *testing.T) {
	for name, keyAndValue := range map[string]struct{ key, value string }{
		"wrong-type-deletion-index": {"delfiles", "wrong-type"},
		"overflowed-inode-count":    {"totalInodes", "9223372036854775808"},
	} {
		t.Run(name, func(t *testing.T) {
			m := guardedRedis(t)
			f := seedGuardedRename(t, m)
			m.fmt.TrashDays = 0
			ctx := Background()
			if err := m.rdb.Set(ctx, m.usedSpaceKey(), "67112960", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := m.rdb.Set(ctx, m.totalInodesKey(), "3", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := m.rdb.Set(ctx, keyAndValue.key, keyAndValue.value, 0).Err(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.rdb.Del(ctx, m.usedSpaceKey(), m.totalInodesKey(), m.delfiles()).Err() })
			if got := f.rename(); got == 0 {
				t.Fatal("rename executed with invalid selected-branch key")
			}
			for entryName, want := range map[string]Ino{f.r.Stage.Name: f.stage, f.r.Destination.Name: f.dest} {
				entry, err := m.rdb.HGet(ctx, m.entryKey(3), entryName).Bytes()
				if err != nil || string(entry) != string(m.packEntry(TypeFile, want)) {
					t.Fatalf("entry %s changed: %x, %v", entryName, entry, err)
				}
			}
		})
	}
}
