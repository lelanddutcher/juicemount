//go:build !noredis

package meta

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/redis/go-redis/v9"
)

// Uses the explicitly allocated empty DB required by guardedRedis; never
// touches the inherited Redis target or flushes a database.
func TestBirthCreateWatchAndFreshness(t *testing.T) {
	m := guardedRedis(t)
	ctx := Background()
	t.Cleanup(func() {
		_ = m.rdb.Del(ctx, "i1", "i3", "setting", "jmlease:v1:proxy", "usedSpace", "totalInodes", "jmlease:v1:epoch:1", "birth-test-marker", replaceguard.BirthOperationKey(strings.Repeat("a", 32))).Err()
	})
	parent, fresh := Ino(3), Ino(73001)
	op := strings.Repeat("a", 32)
	root := &Attr{Typ: TypeDirectory, Mode: 0777, Nlink: 2, Parent: 1, Length: 4096}
	dir := &Attr{Typ: TypeDirectory, Mode: 0777, Nlink: 2, Parent: 1, Length: 4096}
	for _, item := range []struct {
		key   string
		value interface{}
	}{
		{m.inodeKey(1), m.marshal(root)}, {m.inodeKey(parent), m.marshal(dir)},
		{m.setting(), `{}`}, {"jmlease:v1:proxy", "proxy-a"},
		{m.usedSpaceKey(), "0"}, {m.totalInodesKey(), "0"},
	} {
		if err := m.rdb.Set(ctx, item.key, item.value, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	stage := &Attr{Typ: TypeFile, Mode: 0600, Nlink: 1, Parent: parent, Length: 0}
	prepare := func() error {
		return m.rdb.Watch(ctx, func(tx *redis.Tx) error {
			return m.prepareBirthCreate(ctx, tx, op, parent, fresh, TypeFile, stage)
		}, m.inodeKey(parent), m.entryKey(parent))
	}
	if err := prepare(); err != nil {
		t.Fatalf("fresh create refused: %v", err)
	}
	if err := m.rdb.Set(ctx, replaceguard.BirthOperationKey(op), "occupied", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(prepare(), syscall.EIO) {
		t.Fatal("accepted occupied operation receipt")
	}
	if err := m.rdb.Del(ctx, replaceguard.BirthOperationKey(op)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := m.rdb.Set(ctx, m.usedSpaceKey(), "9223372036854771712", 0).Err(); err != nil { // MaxInt64 - 4095
		t.Fatal(err)
	}
	if !errors.Is(prepare(), syscall.EIO) {
		t.Fatal("accepted overflowing usedSpace")
	}
	if err := m.rdb.Set(ctx, m.usedSpaceKey(), "0", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := m.rdb.Set(ctx, m.totalInodesKey(), math.MaxInt64, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(prepare(), syscall.EIO) {
		t.Fatal("accepted overflowing totalInodes")
	}
	if err := m.rdb.Set(ctx, m.totalInodesKey(), "0", 0).Err(); err != nil {
		t.Fatal(err)
	}
	// Mutating a watched ancestor lease after preparation aborts EXEC.
	err := m.rdb.Watch(ctx, func(tx *redis.Tx) error {
		if err := m.prepareBirthCreate(ctx, tx, op, parent, fresh, TypeFile, stage); err != nil {
			return err
		}
		if err := m.rdb.Set(ctx, "jmlease:v1:epoch:1", "1", 0).Err(); err != nil {
			return err
		}
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "birth-test-marker", "unexpected", 0)
			return nil
		})
		return err
	}, m.inodeKey(parent), m.entryKey(parent))
	if !errors.Is(err, redis.TxFailedErr) {
		t.Fatalf("ancestor WATCH did not abort EXEC: %v", err)
	}
}

func TestBirthMutationReceiptAndNamespace(t *testing.T) {
	m := guardedRedis(t)
	ctx := Background()
	t.Cleanup(func() {
		_ = m.rdb.Del(ctx, "i1", "i3", "i73001", "d1", "d3", "setting", replaceguard.BirthOperationKey(strings.Repeat("b", 32)), replaceguard.BirthInodeKey(73001)).Err()
	})
	stage := Ino(73001)
	op := strings.Repeat("b", 32)
	uuid := "12345678-1234-abcd-5678-123456789abc"
	b := replaceguard.Birth{
		Version: 1, OperationID: op, Owner: "mac-a", Proxy: "proxy-old",
		Database: int64(m.rdb.(*redis.Client).Options().DB), BackendUUID: uuid,
		Project: 3, Epoch: 4, Parents: []replaceguard.Parent{{Inode: 1}, {Name: "P", Inode: 3}},
		Stage: replaceguard.File{Name: replaceguard.BirthStageName(op), Parent: 3, Inode: uint64(stage)},
		Type:  1, Nlink: 1, Mode: 0600, AttrSHA256: strings.Repeat("a", 64),
	}
	raw, err := replaceguard.EncodeBirth(b)
	if err != nil {
		t.Fatal(err)
	}
	setting, _ := json.Marshal(&Format{UUID: uuid})
	for _, item := range []struct {
		key   string
		value interface{}
	}{
		{m.setting(), setting},
		{m.inodeKey(1), m.marshal(&Attr{Typ: TypeDirectory, Mode: 0777, Nlink: 2, Parent: 1})},
		{m.inodeKey(3), m.marshal(&Attr{Typ: TypeDirectory, Mode: 0777, Nlink: 2, Parent: 1})},
		{m.inodeKey(stage), m.marshal(&Attr{Typ: TypeFile, Mode: 0600, Nlink: 1, Parent: 3, Length: 5})},
		{replaceguard.BirthOperationKey(op), raw}, {replaceguard.BirthInodeKey(uint64(stage)), raw},
	} {
		if err := m.rdb.Set(ctx, item.key, item.value, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.rdb.HSet(ctx, m.entryKey(1), "P", m.packEntry(TypeDirectory, 3)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := m.rdb.HSet(ctx, m.entryKey(3), b.Stage.Name, m.packEntry(TypeFile, stage)).Err(); err != nil {
		t.Fatal(err)
	}
	check := func() error {
		return m.rdb.Watch(ctx, func(tx *redis.Tx) error {
			_, err := m.prepareBirthMutation(ctx, tx, stage)
			return err
		}, m.inodeKey(stage))
	}
	if err := check(); err != nil {
		t.Fatalf("valid born stage refused: %v", err)
	}
	if err := m.rdb.Set(ctx, replaceguard.BirthOperationKey(op), "corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(check(), syscall.EIO) {
		t.Fatal("accepted disagreeing birth lookups")
	}
	if err := m.rdb.Set(ctx, replaceguard.BirthOperationKey(op), raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := m.rdb.HSet(ctx, m.entryKey(3), b.Stage.Name, m.packEntry(TypeFile, 73002)).Err(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(check(), syscall.EIO) {
		t.Fatal("accepted swapped stage dentry")
	}
}

func TestBirthMutationPrefixedVolumeDoesNotTouchUnprefixedKeys(t *testing.T) {
	m := &redisMeta{prefix: "{3}"}
	// A nil transaction is intentional: unsupported birth namespaces must
	// return before any Redis command, including WATCH across cluster slots.
	b, err := m.prepareBirthMutation(Background(), nil, 73001)
	if err != nil || b != nil {
		t.Fatalf("prefixed ordinary mutation: %+v %v", b, err)
	}
}
