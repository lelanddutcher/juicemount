//go:build !noredis

package meta

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/juicedata/juicefs/pkg/slicegraph"
	"github.com/redis/go-redis/v9"
)

func guardedDecimal(raw string, positive bool) (uint64, bool) {
	n, err := strconv.ParseUint(raw, 10, 64)
	return n, err == nil && n <= math.MaxInt64 && (!positive || n > 0) && strconv.FormatUint(n, 10) == raw
}

func guardedTime(sec int64, ns uint32, expected int64) bool {
	return expected >= 0 && sec == expected/1e9 && int64(ns) == expected%1e9
}

func guardedAttr(tx *redis.Tx, ctx context.Context, key string) (Attr, error) {
	var a Attr
	raw, err := tx.Get(ctx, key).Bytes()
	if err != nil {
		return a, err
	}
	if len(raw) != 72 && len(raw) != 80 {
		return a, syscall.EIO
	}
	a.Unmarshal(raw)
	return a, nil
}

func guardedType(tx *redis.Tx, ctx context.Context, key string, allowed ...string) error {
	kind, err := tx.Type(ctx, key).Result()
	if err != nil {
		return err
	}
	for _, a := range allowed {
		if kind == a {
			return nil
		}
	}
	return syscall.EIO
}

// checkProtectedRename runs after every RequiredKeys WATCH and before any
// protected rename pipeline is queued. It compares the old graph's visible
// slices; no old media bytes or new private image bytes are read here.
func (m *redisMeta) checkProtectedRename(ctx Context, tx *redis.Tx, r replaceguard.Record,
	parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, flags uint32, ino, dino, trash Ino) error {
	if r.Phase != "prepared" || flags != 0 || parentSrc != parentDst ||
		uint64(parentSrc) != r.Stage.Parent || uint64(parentDst) != r.Destination.Parent ||
		nameSrc != r.Stage.Name || nameDst != r.Destination.Name ||
		uint64(ino) != r.Stage.Inode || uint64(dino) != r.Destination.Inode || dino == 0 {
		return syscall.EIO
	}
	// This guarded wave supports one volume DB with unprefixed proxy keys.
	if m.prefix != "" {
		return syscall.ENOTSUP
	}
	setting, err := tx.Get(ctx, m.setting()).Bytes()
	if err != nil {
		return err
	}
	var format Format
	if json.Unmarshal(setting, &format) != nil || format.UUID != r.BackendUUID {
		return syscall.EIO
	}
	proxy, err := tx.Get(ctx, "jmlease:v1:proxy").Result()
	if err != nil || proxy != r.Proxy {
		return syscall.EIO
	}
	project := strconv.FormatUint(r.Project, 10)
	counter, err := tx.Get(ctx, "jmlease:v1:epoch:"+project).Result()
	if err != nil {
		return err
	}
	current, ok := guardedDecimal(counter, true)
	if !ok || current != r.Epoch {
		return syscall.EIO
	}
	lease, err := tx.HGetAll(ctx, "jmlease:v1:p:"+project).Result()
	if err != nil {
		return err
	}
	hashEpoch, ok := guardedDecimal(lease["epoch"], true)
	if !ok || hashEpoch != current || lease["holder"] != r.Owner {
		return syscall.EIO
	}
	for k, v := range lease {
		if !strings.HasPrefix(k, "e:") {
			continue
		}
		if len(k) <= 2 {
			return syscall.EIO
		}
		epoch, valid := guardedDecimal(v, true)
		if !valid || k[2:] != r.Owner && epoch > r.AcceptedEpoch {
			return syscall.EIO
		}
	}
	for i, p := range r.Parents {
		inode := Ino(p.Inode)
		a, err := guardedAttr(tx, ctx, m.inodeKey(inode))
		if err != nil || a.Typ != TypeDirectory {
			return syscall.EIO
		}
		if i == 0 {
			if a.Parent != inode {
				return syscall.ESTALE
			}
			continue
		}
		previous := Ino(r.Parents[i-1].Inode)
		entry, err := tx.HGet(ctx, m.entryKey(previous), p.Name).Bytes()
		if err != nil || string(entry) != string(m.packEntry(TypeDirectory, inode)) || a.Parent != previous {
			return syscall.ESTALE
		}
	}
	stage, err := guardedAttr(tx, ctx, m.inodeKey(ino))
	if err != nil {
		return syscall.EIO
	}
	destination, err := guardedAttr(tx, ctx, m.inodeKey(dino))
	if err != nil {
		return syscall.EIO
	}
	for _, item := range []struct {
		got  Attr
		want replaceguard.File
	}{{stage, r.Stage}, {destination, r.Destination}} {
		a, w := item.got, item.want
		if a.Typ != TypeFile || a.Nlink != 1 || a.Parent != Ino(w.Parent) || a.Length != w.Length ||
			!guardedTime(a.Mtime, a.Mtimensec, w.MtimeNS) || !guardedTime(a.Ctime, a.Ctimensec, w.CtimeNS) {
			return syscall.ESTALE
		}
	}
	for _, f := range []replaceguard.File{r.Stage, r.Destination} {
		entry, err := tx.HGet(ctx, m.entryKey(Ino(f.Parent)), f.Name).Bytes()
		if err != nil || string(entry) != string(m.packEntry(TypeFile, Ino(f.Inode))) {
			return syscall.ESTALE
		}
	}
	// WATCH also covers each absent chunk key. Empty Redis lists are observed
	// holes; a missing key cannot be distinguished here but is canonical empty.
	graph := slicegraph.Graph{Version: slicegraph.Version, Length: r.Destination.Length}
	count := (r.Destination.Length + slicegraph.ChunkSize - 1) / slicegraph.ChunkSize
	graph.Chunks = make([]slicegraph.Chunk, 0, count)
	var history uint64
	for i := uint64(0); i < count; i++ {
		key := m.chunkKey(dino, uint32(i))
		n, err := tx.LLen(ctx, key).Result()
		if err != nil || n < 0 || n > 1<<16 || history+uint64(n) > 1<<17 {
			return syscall.EIO
		}
		history += uint64(n)
		vals, err := tx.LRange(ctx, key, 0, -1).Result()
		if err != nil || int64(len(vals)) != n {
			return syscall.EIO
		}
		raw := make([]slicegraph.Extent, 0, len(vals))
		for _, val := range vals {
			if len(val) != sliceBytes {
				return syscall.EIO
			}
			b := []byte(val)
			raw = append(raw, slicegraph.Extent{Pos: binary.BigEndian.Uint32(b[0:4]), SliceID: binary.BigEndian.Uint64(b[4:12]), Size: binary.BigEndian.Uint32(b[12:16]), Off: binary.BigEndian.Uint32(b[16:20]), Len: binary.BigEndian.Uint32(b[20:24])})
		}
		visible, err := slicegraph.Resolve(raw)
		if err != nil {
			return syscall.EIO
		}
		graph.Chunks = append(graph.Chunks, slicegraph.Chunk{Index: uint32(i), Extents: visible})
	}
	canon, err := graph.Canonical()
	if err != nil {
		return syscall.EIO
	}
	digest := sha256.Sum256(canon)
	if hex.EncodeToString(digest[:]) != r.GraphDigest {
		return syscall.ESTALE
	}
	// Selected doRename branch: both files are single-link and share a parent.
	// Validate every key type and integer command before EXEC; Redis does not
	// roll back commands already applied in an EXEC that later hits WRONGTYPE.
	if trash > 0 {
		key := m.entryKey(trash)
		if err := tx.Watch(ctx, key).Err(); err != nil {
			return err
		}
		return guardedType(tx, ctx, key, "none", "hash")
	}
	counterKeys := []string{m.usedSpaceKey(), m.totalInodesKey(), m.delfiles(), m.sustained(m.sid)}
	if err := tx.Watch(ctx, counterKeys...).Err(); err != nil {
		return err
	}
	if err := guardedType(tx, ctx, m.delfiles(), "none", "zset"); err != nil {
		return err
	}
	if err := guardedType(tx, ctx, m.sustained(m.sid), "none", "set"); err != nil {
		return err
	}
	used, err := tx.Get(ctx, m.usedSpaceKey()).Result()
	if err != nil {
		return err
	}
	usedValue, ok := guardedDecimal(used, false)
	if !ok || uint64(align4K(destination.Length)) > usedValue {
		return syscall.EIO
	}
	inodes, err := tx.Get(ctx, m.totalInodesKey()).Result()
	if err != nil {
		return err
	}
	inodeValue, ok := guardedDecimal(inodes, true)
	if !ok || inodeValue == 0 {
		return syscall.EIO
	}
	return nil
}
