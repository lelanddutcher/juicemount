package metadata

import (
	"context"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/redis/go-redis/v9"
)

//go:embed parent_read_proof.lua
var parentReadProofLua string

const (
	maxReadProofDepth      = 32
	maxReadProofPathBytes  = 4096
	maxReadProofReplyBytes = 8192
	maxReadProofDuration   = 2 * time.Second
)

var ErrReadPathProof = errors.New("authoritative Redis path proof refused")

// ErrReadProofVolume preserves a positive format refusal from the atomic
// proof even if the connection or setting changes before the caller returns.
var ErrReadProofVolume = errors.New("authoritative Redis format refused")

// RedisPathNode is a full-width JuiceFS 1.3.1 dentry and inode-attr observation.
type RedisPathNode struct {
	Path                     string
	Inode, ParentInode, Size uint64
	Type                     uint8
	MtimeSec                 int64
	MtimeNsec                uint32
	RawEdge, RawAttr         []byte // exact Redis bytes for before/after comparison
}

// RedisPathProof is one atomic Redis observation. It is NOT a backing-FD,
// mount-generation, or dataset-incarnation proof and grants no serving right.
// The caller must separately bind Redis DB/prefix, dataset run, and mount;
// a same-UUID reset/restore can reproduce this exact tuple.
type RedisPathProof struct {
	FormatUUID, FormatSHA1, Path string
	Root                         RedisPathNode
	Nodes                        []RedisPathNode // full ancestry, leaf last
	RequestBytes, ResponseBytes  int
	RedisOps                     int // TYPE/STRLEN/GET setting and STRLEN/GET i1; HSTRLEN/HGET and STRLEN/GET per edge
	Elapsed                      time.Duration
}

func readProofParts(name string, ids []uint64) ([]string, error) {
	if name == "" || len(name) > maxReadProofPathBytes || path.IsAbs(name) || path.Clean(name) != name || name == "." || !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return nil, ErrReadPathProof
	}
	parts := strings.Split(name, "/")
	if len(parts) == 0 || len(parts) > maxReadProofDepth || len(parts) != len(ids) {
		return nil, ErrReadPathProof
	}
	seen := map[uint64]struct{}{uint64(meta.RootInode): {}}
	for i, part := range parts {
		if part == "" || part == "." || part == ".." || len(part) > 255 || ids[i] <= uint64(meta.RootInode) || ids[i] >= uint64(meta.TrashInode) {
			return nil, ErrReadPathProof
		}
		if _, exists := seen[ids[i]]; exists {
			return nil, ErrReadPathProof
		}
		seen[ids[i]] = struct{}{}
	}
	return parts, nil
}

func decodeReadProofAttr(raw string, wantType uint8, wantParent uint64) (RedisPathNode, error) {
	b := []byte(raw)
	if len(b) != 72 && len(b) != 80 {
		return RedisPathNode{}, ErrReadPathProof
	}
	typ := uint8(binary.BigEndian.Uint16(b[1:3]) >> 12)
	if typ != wantType {
		return RedisPathNode{}, ErrReadPathProof
	}
	sec := binary.BigEndian.Uint64(b[23:31])
	ns := binary.BigEndian.Uint32(b[31:35])
	if sec > math.MaxInt64 || ns >= 1e9 {
		return RedisPathNode{}, ErrReadPathProof
	}
	parent := binary.BigEndian.Uint64(b[63:71])
	// JuiceFS hardlinked regular files may carry Parent=0. Deliberately
	// refuse them here until a separate multi-parent read contract exists.
	if parent != wantParent {
		return RedisPathNode{}, ErrReadPathProof
	}
	return RedisPathNode{ParentInode: parent, Size: binary.BigEndian.Uint64(b[51:59]), Type: typ,
		MtimeSec: int64(sec), MtimeNsec: ns, RawAttr: b}, nil
}

// ReadParentPathProof validates every expected root-to-leaf edge in ONE Redis
// EVAL. The caller must supply full uint64 inode IDs captured independently;
// it refuses a replaced parent even if the new leaf has equal size/mtime.
// A deadline is mandatory. The proof has no TTL and never consults SQLite.
func (rc *RedisClient) ReadParentPathProof(ctx context.Context, formatUUID, name string, expectedInodes []uint64) (RedisPathProof, error) {
	parts, err := readProofParts(name, expectedInodes)
	if err != nil || len(formatUUID) == 0 || len(formatUUID) > 128 || strings.ContainsRune(formatUUID, 0) {
		return RedisPathProof{}, ErrReadPathProof
	}
	if _, ok := ctx.Deadline(); !ok || rc == nil || rc.redisDB() == nil {
		return RedisPathProof{}, ErrReadPathProof
	}
	ctx, cancel := context.WithTimeout(ctx, maxReadProofDuration)
	defer cancel()
	args := make([]interface{}, 0, 2+3*len(parts))
	args = append(args, formatUUID, strconv.Itoa(len(parts)))
	requestBytes := len(formatUUID) + len(strconv.Itoa(len(parts)))
	for i, part := range parts {
		typ := byte(jfsTypeDir)
		if i == len(parts)-1 {
			typ = jfsTypeFile
		}
		var packed [9]byte
		packed[0] = typ
		binary.BigEndian.PutUint64(packed[1:], expectedInodes[i])
		decimal := strconv.FormatUint(expectedInodes[i], 10)
		args = append(args, part, decimal, string(packed[:]))
		requestBytes += len(part) + len(decimal) + len(packed)
	}
	started := time.Now()
	rows, err := rc.redisDB().Eval(ctx, parentReadProofLua, nil, args...).Slice()
	if err != nil {
		var serverErr redis.Error
		if errors.As(err, &serverErr) {
			reason := strings.TrimPrefix(serverErr.Error(), "ERR ")
			if reason == "read proof: format UUID mismatch" || reason == "read proof: missing or oversized format" || reason == "read proof: invalid format type" {
				return RedisPathProof{}, fmt.Errorf("%w: %w: %v", ErrReadPathProof, ErrReadProofVolume, err)
			}
		}
		return RedisPathProof{}, fmt.Errorf("%w: %v", ErrReadPathProof, err)
	}
	if len(rows) != 3+2*len(parts) {
		return RedisPathProof{}, ErrReadPathProof
	}
	get := func(i int) (string, bool) { v, ok := rows[i].(string); return v, ok }
	responseBytes := 0
	for _, row := range rows {
		value, ok := row.(string)
		if !ok {
			return RedisPathProof{}, ErrReadPathProof
		}
		responseBytes += len(value)
		if responseBytes > maxReadProofReplyBytes {
			return RedisPathProof{}, ErrReadPathProof
		}
	}
	version, ok := get(0)
	if !ok || version != "v1" {
		return RedisPathProof{}, ErrReadPathProof
	}
	sha, ok := get(1)
	if !ok || len(sha) != 40 {
		return RedisPathProof{}, ErrReadPathProof
	}
	rootRaw, ok := get(2)
	if !ok {
		return RedisPathProof{}, ErrReadPathProof
	}
	root, err := decodeReadProofAttr(rootRaw, jfsTypeDir, uint64(meta.RootInode))
	if err != nil {
		return RedisPathProof{}, err
	}
	root.Path, root.Inode = ".", 1
	proof := RedisPathProof{FormatUUID: formatUUID, FormatSHA1: sha, Path: name, Root: root,
		Nodes: make([]RedisPathNode, 0, len(parts)), RequestBytes: requestBytes,
		ResponseBytes: responseBytes, RedisOps: 5 + 4*len(parts), Elapsed: time.Since(started)}
	parent, prefix := uint64(1), ""
	for i, part := range parts {
		typ := byte(jfsTypeDir)
		if i == len(parts)-1 {
			typ = jfsTypeFile
		}
		edge, ok := get(3 + 2*i)
		if !ok || len(edge) != 9 || edge[0] != typ || binary.BigEndian.Uint64([]byte(edge)[1:]) != expectedInodes[i] {
			return RedisPathProof{}, ErrReadPathProof
		}
		attr, ok := get(4 + 2*i)
		if !ok {
			return RedisPathProof{}, ErrReadPathProof
		}
		node, err := decodeReadProofAttr(attr, typ, parent)
		if err != nil {
			return RedisPathProof{}, err
		}
		if prefix == "" {
			prefix = part
		} else {
			prefix += "/" + part
		}
		node.Path, node.Inode = prefix, expectedInodes[i]
		node.RawEdge = []byte(edge)
		proof.Nodes = append(proof.Nodes, node)
		parent = node.Inode
	}
	return proof, nil
}
