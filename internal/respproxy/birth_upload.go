package respproxy

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/juicedata/juicefs/pkg/replaceguard"
)

func (c *conn) guardBornStages(u *Unit, ids map[uint64]bool) error {
	active := 0
	for n := range ids {
		if n <= 1 {
			continue
		}
		v, err := c.p.side.Do(u.DB, strArgs("GET", replaceguard.BirthInodeKey(n))...)
		if err != nil || v.Err() != nil {
			return errors.New("birth lookup unavailable")
		}
		if v.Null {
			continue
		}
		b, err := replaceguard.DecodeBirth(v.Str)
		if err != nil || b.Stage.Inode != n || b.Database != int64(u.DB) {
			return errors.New("invalid born inode")
		}
		op, err := c.p.side.Do(u.DB, strArgs("GET", replaceguard.BirthOperationKey(b.OperationID))...)
		if err != nil || op.Err() != nil || op.Null || !bytes.Equal(op.Str, v.Str) {
			return errors.New("birth indexes differ")
		}
		final, err := c.p.side.Do(u.DB, strArgs("GET", replaceguard.Key(n))...)
		if err != nil || final.Err() != nil {
			return errors.New("birth publication lookup unavailable")
		}
		if !final.Null {
			r, err := replaceguard.Decode(final.Str)
			if err != nil || !b.MatchesReplacement(r) {
				return errors.New("birth publication differs")
			}
			// Prepared uses the existing rename guard; committed is an ordinary
			// published inode under current lease policy. Birth grants neither path.
			continue
		}
		active++
		if active > 1 {
			return errors.New("multiple uploading stages")
		}
		if err := c.guardBirthUpload(u, b); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) guardBirthUpload(u *Unit, b replaceguard.Birth) error {
	fail := func() error { return errors.New("unsupported or changed born-stage upload") }
	e, ok := c.p.policy.(*LeaseEngine)
	if !ok || u.Kind != UnitTxn || u.Mode != ModeEnforce || c.principal.Kind != KindMac || c.principal.Name != b.Owner || !c.p.cfg.Singleton || c.p.fenced() {
		return fail()
	}
	watched := map[string]bool{}
	for _, key := range u.Watched {
		watched[key] = true
	}
	for _, key := range replaceguard.BirthRequiredKeys(b) {
		if !watched[key] {
			return errors.New("missing born upload WATCH")
		}
	}
	if u.beforeSide == nil || u.beforeSide() != nil {
		return fail()
	}
	read := func(args ...string) (Value, error) {
		if !watched[args[1]] {
			return Value{}, fail()
		}
		v, err := c.p.side.Do(u.DB, strArgs(args...)...)
		if err == nil {
			err = v.Err()
		}
		return v, err
	}
	location, err := e.anc.resolveDir(b.Stage.Parent)
	if err != nil || location.kind != nsProject || location.proj != b.Project || len(b.Parents) <= e.anc.depth || b.Parents[e.anc.depth].Inode != b.Project {
		return fail()
	}
	e.mu.Lock()
	l := e.leases[b.Project]
	valid := l != nil && !l.unresolved && l.held() && l.holder == b.Owner
	var epoch uint64
	if valid {
		epoch = l.epoch
	}
	e.mu.Unlock()
	if !valid {
		return fail()
	}
	if err := c.replacementAuthority(replaceguard.Record{Owner: b.Owner, Proxy: c.p.Incarnation(), Project: b.Project, Epoch: epoch, AcceptedEpoch: epoch}); err != nil {
		return err
	}
	setting, err := read("GET", "setting")
	var format struct{ UUID string }
	if err != nil || setting.Null || json.Unmarshal(setting.Str, &format) != nil || format.UUID != b.BackendUUID {
		return fail()
	}
	for i, p := range b.Parents {
		v, err := read("GET", "i"+inoStr(p.Inode))
		a, ok := parseAttr(v.Str)
		if err != nil || v.Null || !ok || a.typ != typeDir {
			return fail()
		}
		parent := p.Inode
		if i > 0 {
			parent = b.Parents[i-1].Inode
			v, err = read("HGET", "d"+inoStr(parent), p.Name)
			if err != nil || v.Null || string(v.Str) != mkDirectoryEntry(p.Inode) {
				return fail()
			}
		}
		if a.parent != parent {
			return fail()
		}
	}
	v, err := read("HGET", "d"+inoStr(b.Stage.Parent), b.Stage.Name)
	if err != nil || v.Null || string(v.Str) != mkBirthEntry(b.Stage.Inode) {
		return fail()
	}
	inodeKey := "i" + inoStr(b.Stage.Inode)
	old, err := read("GET", inodeKey)
	if err != nil || old.Null || len(old.Str) != 72 {
		return fail()
	}
	a, ok := parseAttr(old.Str)
	if !ok || a.typ != typeFile || a.parent != b.Stage.Parent || binary.BigEndian.Uint32(old.Str[47:51]) != 1 || binary.BigEndian.Uint16(old.Str[1:3])&07777 != b.Mode {
		return fail()
	}
	oldLength := binary.BigEndian.Uint64(old.Str[51:59])
	if oldLength > math.MaxInt64-4096 {
		return fail()
	}
	var next []byte
	var chunk []byte
	var index uint64
	var delta int64
	sets, pushes, increments := 0, 0, 0
	for _, cm := range u.Cmds {
		args := cm.Args
		if len(args) < 2 {
			return fail()
		}
		key := string(args[1])
		switch {
		case cm.Info.Name == "SET" && key == inodeKey && len(args) == 3:
			sets++
			next = args[2]
		case cm.Info.Name == "RPUSH" && strings.HasPrefix(key, "c"+inoStr(b.Stage.Inode)+"_") && len(args) == 3:
			pushes++
			suffix := strings.TrimPrefix(key, "c"+inoStr(b.Stage.Inode)+"_")
			index, err = strconv.ParseUint(suffix, 10, 32)
			if err != nil || inoStr(index) != suffix || !watched[key] {
				return fail()
			}
			kind, err := read("TYPE", key)
			if err != nil || string(kind.Str) != "none" && string(kind.Str) != "list" {
				return fail()
			}
			chunk = args[2]
		case cm.Info.Name == "INCRBY" && key == "usedSpace" && len(args) == 3:
			increments++
			var valid bool
			delta, valid = RedisInt(args[2])
			if !valid || delta <= 0 || strconv.FormatInt(delta, 10) != string(args[2]) {
				return fail()
			}
		default:
			return fail()
		}
	}
	if sets != 1 || pushes > 1 || increments > 1 || len(next) != len(old.Str) {
		return fail()
	}
	fixed := append([]byte(nil), next...)
	copy(fixed[23:47], old.Str[23:47])
	copy(fixed[51:59], old.Str[51:59])
	if !bytes.Equal(fixed, old.Str) {
		return fail()
	}
	for _, off := range []int{23, 35} {
		sec := binary.BigEndian.Uint64(next[off : off+8])
		sub := binary.BigEndian.Uint32(next[off+8 : off+12])
		if sub >= 1e9 || sec > uint64((math.MaxInt64-int64(sub))/1e9) {
			return fail()
		}
	}
	newLength := binary.BigEndian.Uint64(next[51:59])
	if newLength < oldLength || newLength > math.MaxInt64-4096 {
		return fail()
	}
	if pushes == 0 {
		if newLength != oldLength || increments != 0 {
			return fail()
		}
		return nil
	}
	if len(chunk) != 24 {
		return fail()
	}
	pos := uint64(binary.BigEndian.Uint32(chunk[:4]))
	id := binary.BigEndian.Uint64(chunk[4:12])
	size := uint64(binary.BigEndian.Uint32(chunk[12:16]))
	off := uint64(binary.BigEndian.Uint32(chunk[16:20]))
	length := uint64(binary.BigEndian.Uint32(chunk[20:24]))
	if id == 0 || length == 0 || pos+length > 64<<20 || off+length > size || newLength != max(oldLength, index*(64<<20)+pos+length) {
		return fail()
	}
	align := func(n uint64) uint64 { return (n + 4095) / 4096 * 4096 }
	// JuiceFS charges one 4KiB allocation even for an empty file.
	if delta != int64(max(uint64(4096), align(newLength))-max(uint64(4096), align(oldLength))) {
		return fail()
	}
	if delta > 0 {
		counter, err := read("GET", "usedSpace")
		value, ok := RedisInt(counter.Str)
		if err != nil || counter.Null || !ok || value < 0 || value > math.MaxInt64-delta || strconv.FormatInt(value, 10) != string(counter.Str) {
			return fail()
		}
	}
	return nil
}

func mkDirectoryEntry(n uint64) string {
	b := []byte(mkBirthEntry(n))
	b[0] = typeDir
	return string(b)
}

func (e *LeaseEngine) birthForArm(db int, r replaceguard.Record) ([]byte, error) {
	a, err := e.replacementRead(db, "GET", replaceguard.BirthInodeKey(r.Stage.Inode))
	if err != nil {
		return nil, err
	}
	b, err := e.replacementRead(db, "GET", replaceguard.BirthOperationKey(r.OperationID))
	if err != nil {
		return nil, err
	}
	if a.Null && b.Null {
		return nil, nil
	}
	born, err := replaceguard.DecodeBirth(a.Str)
	if err != nil || a.Null || b.Null || !bytes.Equal(a.Str, b.Str) || born.Database != int64(db) || !born.MatchesReplacement(r) {
		return nil, errors.New("birth custody differs at sealing")
	}
	return a.Str, nil
}
