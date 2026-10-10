//go:build !noredis

package meta

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"syscall"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/redis/go-redis/v9"
)

// prepareBirthCreate runs before doMknod reads or queues any create command.
// It supplies WATCH coverage, not authority: the proxy must resolve the named
// ancestry and verify current ownership, raw creation bytes and postconditions.
func (m *redisMeta) prepareBirthCreate(ctx Context, tx *redis.Tx, operationID string,
	parent, inode Ino, typ uint8, attr *Attr) error {
	if m.prefix != "" || m.conf.CaseInsensi {
		return syscall.ENOTSUP
	}
	if parent < RootInode || parent >= TrashInode || inode <= RootInode || inode >= TrashInode ||
		typ != TypeFile || attr == nil || attr.Typ != TypeFile || attr.Parent != parent ||
		attr.Nlink != 1 || attr.Length != 0 || attr.Rdev != 0 || attr.Flags != 0 ||
		attr.AccessACL != 0 || attr.DefaultACL != 0 {
		return syscall.EIO
	}
	// The initial txn WATCH covers the parent inode and dentry. Add every
	// remaining fixed predicate before reading anything, including the exact
	// candidate inode and both immutable receipt lookup directions.
	fixed := []string{
		replaceguard.BirthOperationKey(operationID), replaceguard.BirthInodeKey(uint64(inode)),
		replaceguard.Key(uint64(inode)), m.inodeKey(inode), m.entryKey(inode),
		m.setting(), "jmlease:v1:proxy", m.usedSpaceKey(), m.totalInodesKey(),
	}
	if err := tx.Watch(ctx, fixed...).Err(); err != nil {
		return err
	}
	for _, key := range []string{fixed[0], fixed[1], fixed[2], fixed[3], fixed[4]} {
		if err := guardedType(tx, ctx, key, "none"); err != nil {
			return syscall.EIO
		}
	}
	if err := guardedType(tx, ctx, m.setting(), "string"); err != nil {
		return syscall.EIO
	}
	if err := guardedType(tx, ctx, "jmlease:v1:proxy", "string"); err != nil {
		return syscall.EIO
	}
	for _, counter := range []struct {
		key   string
		added uint64
	}{{m.usedSpaceKey(), uint64(align4K(0))}, {m.totalInodesKey(), 1}} {
		raw, err := tx.Get(ctx, counter.key).Result()
		if err != nil {
			return syscall.EIO
		}
		n, ok := guardedDecimal(raw, false)
		if !ok || n > math.MaxInt64-counter.added {
			return syscall.EIO
		}
	}
	seen := make(map[Ino]bool)
	for current := parent; ; {
		if current < RootInode || current >= TrashInode || seen[current] || len(seen) >= 256 {
			return syscall.EIO
		}
		seen[current] = true
		id := strconv.FormatUint(uint64(current), 10)
		keys := []string{m.inodeKey(current), m.entryKey(current),
			"jmlease:v1:p:" + id, "jmlease:v1:epoch:" + id}
		if err := tx.Watch(ctx, keys...).Err(); err != nil {
			return err
		}
		if err := guardedType(tx, ctx, m.entryKey(current), "hash", "none"); err != nil {
			return syscall.EIO
		}
		a, err := guardedAttr(tx, ctx, m.inodeKey(current)) // exact 72/80-byte Attr
		if err != nil || a.Typ != TypeDirectory {
			return syscall.EIO
		}
		if current == RootInode {
			if a.Parent != RootInode {
				return syscall.EIO
			}
			return nil
		}
		if a.Parent < RootInode || a.Parent >= TrashInode || a.Parent == current {
			return syscall.EIO
		}
		current = a.Parent
	}
}

// prepareBirthMutation watches both immutable lookup directions before any
// inode read. A birth is custody evidence only: the proxy checks the current
// authenticated lease and supported transaction shape independently.
func (m *redisMeta) prepareBirthMutation(ctx Context, tx *redis.Tx, inode Ino) (*replaceguard.Birth, error) {
	// Reserved birth creation is unsupported in prefixed/cluster volumes.
	// Ordinary inode writes must not WATCH another hash slot.
	if m.prefix != "" {
		return nil, nil
	}
	if inode <= RootInode || inode >= TrashInode {
		return nil, nil
	}
	birthKey, finalKey := replaceguard.BirthInodeKey(uint64(inode)), replaceguard.Key(uint64(inode))
	if err := tx.Watch(ctx, birthKey, finalKey).Err(); err != nil {
		return nil, err
	}
	birthRaw, birthErr := tx.Get(ctx, birthKey).Bytes()
	finalRaw, finalErr := tx.Get(ctx, finalKey).Bytes()
	if birthErr != nil && birthErr != redis.Nil || finalErr != nil && finalErr != redis.Nil {
		return nil, syscall.EIO
	}
	var final *replaceguard.Record
	if finalErr == nil {
		r, err := replaceguard.Decode(finalRaw)
		if err != nil || r.Stage.Inode != uint64(inode) {
			return nil, syscall.EIO
		}
		final = &r
	}
	if birthErr == redis.Nil {
		if final != nil && final.Phase == "prepared" {
			return nil, syscall.EPERM
		}
		return nil, nil
	}
	b, err := replaceguard.DecodeBirth(birthRaw)
	if err != nil || b.Stage.Inode != uint64(inode) || m.prefix != "" {
		return nil, syscall.EIO
	}
	client, ok := m.rdb.(*redis.Client)
	if !ok || b.Database != int64(client.Options().DB) {
		return nil, syscall.EIO
	}
	opKey := replaceguard.BirthOperationKey(b.OperationID)
	if err := tx.Watch(ctx, opKey).Err(); err != nil {
		return nil, err
	}
	opRaw, err := tx.Get(ctx, opKey).Bytes()
	if err != nil || !bytes.Equal(opRaw, birthRaw) {
		return nil, syscall.EIO
	}
	if final != nil {
		if !b.MatchesReplacement(*final) {
			return nil, syscall.EIO
		}
		if final.Phase == "prepared" {
			return nil, syscall.EPERM
		}
		return nil, nil // committed destination follows ordinary inode behavior
	}
	if err := tx.Watch(ctx, replaceguard.BirthRequiredKeys(b)...).Err(); err != nil {
		return nil, err
	}
	setting, err := tx.Get(ctx, m.setting()).Bytes()
	if err != nil {
		return nil, syscall.EIO
	}
	var format Format
	if json.Unmarshal(setting, &format) != nil || format.UUID != b.BackendUUID {
		return nil, syscall.EIO
	}
	for i, p := range b.Parents {
		current := Ino(p.Inode)
		a, err := guardedAttr(tx, ctx, m.inodeKey(current))
		if err != nil || a.Typ != TypeDirectory {
			return nil, syscall.EIO
		}
		if i == 0 {
			if a.Parent != RootInode {
				return nil, syscall.EIO
			}
			continue
		}
		previous := Ino(b.Parents[i-1].Inode)
		entry, err := tx.HGet(ctx, m.entryKey(previous), p.Name).Bytes()
		if err != nil || !bytes.Equal(entry, m.packEntry(TypeDirectory, current)) || a.Parent != previous {
			return nil, syscall.EIO
		}
	}
	stage, err := guardedAttr(tx, ctx, m.inodeKey(inode))
	if err != nil || stage.Typ != TypeFile || stage.Nlink != 1 || stage.Parent != Ino(b.Stage.Parent) ||
		stage.AccessACL != 0 || stage.DefaultACL != 0 || stage.Length > math.MaxInt64 ||
		stage.Mode != b.Mode || stage.Flags != 0 {
		return nil, syscall.EIO
	}
	if err := guardedType(tx, ctx, m.entryKey(inode), "none"); err != nil {
		return nil, syscall.EIO
	}
	entry, err := tx.HGet(ctx, m.entryKey(Ino(b.Stage.Parent)), b.Stage.Name).Bytes()
	if err != nil || !bytes.Equal(entry, m.packEntry(TypeFile, inode)) {
		return nil, syscall.EIO
	}
	return &b, nil
}
