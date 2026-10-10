package respproxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/juicedata/juicefs/pkg/replaceguard"
)

// Birth creation is gated until upload guards and owned recovery are qualified.
// Persisted births are always protected, including after the gate is disabled.
func (c *conn) checkStageBirth(u *Unit, ids map[uint64]bool) *Denial {
	if err := c.guardBornStages(u, ids); err != nil {
		return replacementDeny(err.Error())
	}
	if !c.p.stageBirth.Load() {
		return nil
	}
	var birth *replaceguard.Birth
	for _, w := range u.Writes {
		if w.Key.Shape != ShapeDentry {
			continue
		}
		cm := u.Cmds[w.Cmd]
		if !isHSet(cm.Info.Name) {
			continue
		}
		for j := 2; j+1 < len(cm.Args); j += 2 {
			name := string(cm.Args[j])
			if !strings.HasPrefix(name, ".juicemount-streaming-drain-replacement-") {
				continue
			}
			op, ok := replaceguard.ParseBirthStageName(name)
			ino, iok := entryInode(cm.Args[j+1])
			typ, tok := entryType(cm.Args[j+1])
			if !ok || !iok || !tok || typ != typeFile || birth != nil {
				return replacementDeny("invalid birth placement")
			}
			birth = &replaceguard.Birth{Version: 1, OperationID: op, Stage: replaceguard.File{Name: name, Parent: w.Key.Ino, Inode: ino}, Type: 1, Nlink: 1}
		}
	}
	if birth == nil {
		return nil
	}
	if err := c.prepareStageBirth(u, birth); err != nil {
		return replacementDeny(err.Error())
	}
	return nil
}

// No mutations precede MSET. A receipt proves all supported create commands
// applied, not merely that EXEC ran (Redis transactions do not roll back).
const birthCommitLua = `
if redis.call('EXISTS',KEYS[1],KEYS[2],KEYS[3]) ~= 0 then return redis.error_reply('birth already enrolled') end
for i=2,#ARGV,4 do
 local op,key,field,want=ARGV[i],ARGV[i+1],ARGV[i+2],ARGV[i+3]
 local got
 if op == 'GET' then got=redis.call('GET',key)
 else got=redis.call('HGET',key,field) end
 if got ~= want then return redis.error_reply('birth postcondition failed') end
end
return redis.call('MSET',KEYS[1],ARGV[1],KEYS[2],ARGV[1])
`

func (c *conn) prepareStageBirth(u *Unit, b *replaceguard.Birth) error {
	fail := func() error { return errors.New("unsupported or changed birth create") }
	e, ok := c.p.policy.(*LeaseEngine)
	if !ok || u.Kind != UnitTxn || u.Mode != ModeEnforce || c.principal.Kind != KindMac || !c.p.cfg.Singleton || c.p.fenced() || u.replacementReceipt != nil {
		return fail()
	}
	watched := map[string]bool{}
	for _, k := range u.Watched {
		watched[k] = true
	}
	read := func(args ...string) (Value, error) {
		if len(args) < 2 || !watched[args[1]] {
			return Value{}, errors.New("birth key not watched")
		}
		v, err := c.p.side.Do(u.DB, strArgs(args...)...)
		if err == nil {
			err = v.Err()
		}
		return v, err
	}
	if u.beforeSide == nil || u.beforeSide() != nil {
		return fail()
	}
	b.Owner, b.Proxy, b.Database = c.principal.Name, c.p.Incarnation(), int64(u.DB)
	// Read the exact path, under WATCH, instead of trusting cached ancestry names.
	seen := map[uint64]bool{}
	for n := b.Stage.Parent; ; {
		if n == 0 || n > math.MaxInt64 || seen[n] || len(b.Parents) >= 256 {
			return fail()
		}
		seen[n] = true
		v, err := read("GET", "i"+inoStr(n))
		if err != nil || v.Null {
			return fail()
		}
		a, ok := parseAttr(v.Str)
		if !ok || a.typ != typeDir {
			return fail()
		}
		if n == 1 {
			if a.parent != 1 {
				return fail()
			}
			b.Parents = append(b.Parents, replaceguard.Parent{Inode: 1})
			break
		}
		// Bound the reverse directory lookup. Unsupported huge parents HOLD.
		key := "d" + inoStr(a.parent)
		size, err := read("HLEN", key)
		if err != nil || size.Type != ':' || size.Int < 1 || size.Int > 65536 {
			return fail()
		}
		entries, err := read("HGETALL", key)
		if err != nil || entries.Type != '*' || len(entries.Elems) != int(size.Int)*2 {
			return fail()
		}
		name := ""
		for j := 0; j < len(entries.Elems); j += 2 {
			ino, valid := entryInode(entries.Elems[j+1].Str)
			typ, tok := entryType(entries.Elems[j+1].Str)
			if valid && ino == n {
				if !tok || typ != typeDir || name != "" {
					return fail()
				}
				name = string(entries.Elems[j].Str)
			}
		}
		if name == "" {
			return fail()
		}
		b.Parents = append(b.Parents, replaceguard.Parent{Name: name, Inode: n})
		n = a.parent
	}
	for i, j := 0, len(b.Parents)-1; i < j; i, j = i+1, j-1 {
		b.Parents[i], b.Parents[j] = b.Parents[j], b.Parents[i]
	}
	if len(b.Parents) <= e.anc.depth {
		return fail()
	}
	b.Project = b.Parents[e.anc.depth].Inode
	loc, err := e.anc.resolveDir(b.Stage.Parent)
	if err != nil || loc.kind != nsProject || loc.proj != b.Project {
		return fail()
	}
	e.mu.Lock()
	l := e.leases[b.Project]
	valid := l != nil && !l.unresolved && l.held() && l.holder == b.Owner
	if valid {
		b.Epoch = l.epoch
	}
	e.mu.Unlock()
	if !valid {
		return fail()
	}
	// Birth is custody only: existing foreign history still fences publication.
	if err := c.replacementAuthority(replaceguard.Record{Owner: b.Owner, Proxy: b.Proxy, Project: b.Project, Epoch: b.Epoch, AcceptedEpoch: b.Epoch}); err != nil {
		return err
	}
	setting, err := read("GET", "setting")
	if err != nil || setting.Null {
		return fail()
	}
	var format struct{ UUID string }
	if json.Unmarshal(setting.Str, &format) != nil {
		return fail()
	}
	b.BackendUUID = format.UUID
	inodeKey, parentKey := "i"+inoStr(b.Stage.Inode), "i"+inoStr(b.Stage.Parent)
	var rawAttr []byte
	for _, cm := range u.Cmds {
		if cm.Info.Name == "SET" && len(cm.Args) == 3 && string(cm.Args[1]) == inodeKey {
			if rawAttr != nil {
				return fail()
			}
			rawAttr = cm.Args[2]
		}
	}
	if len(rawAttr) != 72 || rawAttr[0] != 0 || binary.BigEndian.Uint32(rawAttr[59:63]) != 0 {
		return fail()
	}
	ns := func(rawAttr []byte, off int) (int64, bool) {
		sec := binary.BigEndian.Uint64(rawAttr[off : off+8])
		sub := binary.BigEndian.Uint32(rawAttr[off+8 : off+12])
		if sub >= 1e9 || sec > uint64((math.MaxInt64-int64(sub))/1e9) {
			return 0, false
		}
		return int64(sec)*1e9 + int64(sub), true
	}
	b.Stage.MtimeNS, ok = ns(rawAttr, 23)
	if !ok {
		return fail()
	}
	b.Stage.CtimeNS, ok = ns(rawAttr, 35)
	if !ok {
		return fail()
	}
	if _, ok = ns(rawAttr, 11); !ok || !replacementAttr(rawAttr, b.Stage) {
		return fail()
	}
	b.Mode = binary.BigEndian.Uint16(rawAttr[1:3]) & 07777
	digest := sha256.Sum256(rawAttr)
	b.AttrSHA256 = hex.EncodeToString(digest[:])
	raw, err := replaceguard.EncodeBirth(*b)
	if err != nil {
		return fail()
	}
	for _, key := range replaceguard.BirthRequiredKeys(*b) {
		if !watched[key] {
			return errors.New("missing birth WATCH")
		}
	}
	for _, key := range []string{replaceguard.BirthOperationKey(b.OperationID), replaceguard.BirthInodeKey(b.Stage.Inode), replaceguard.Key(b.Stage.Inode), inodeKey, "d" + inoStr(b.Stage.Inode)} {
		v, err := read("EXISTS", key)
		if err != nil || v.Type != ':' || v.Int != 0 {
			return fail()
		}
	}
	v, err := read("HGET", "d"+inoStr(b.Stage.Parent), b.Stage.Name)
	if err != nil || !v.Null {
		return fail()
	}
	parent, err := read("GET", parentKey)
	if err != nil || len(parent.Str) != 72 {
		return fail()
	}
	args := strArgs("EVAL", birthCommitLua, "3", replaceguard.BirthOperationKey(b.OperationID), replaceguard.BirthInodeKey(b.Stage.Inode), replaceguard.Key(b.Stage.Inode), string(raw))
	add := func(op, key, field, want string) { args = append(args, strArgs(op, key, field, want)...) }
	counts := map[string]int{}
	for _, cm := range u.Cmds {
		a := cm.Args
		if len(a) < 2 {
			return fail()
		}
		key := string(a[1])
		counts[key]++
		if counts[key] > 1 {
			return fail()
		}
		switch key {
		case inodeKey:
			if cm.Info.Name != "SET" || len(a) != 3 || !bytes.Equal(a[2], rawAttr) {
				return fail()
			}
			add("GET", key, "", string(a[2]))
		case parentKey:
			if cm.Info.Name != "SET" || len(a) != 3 || len(a[2]) != len(parent.Str) {
				return fail()
			}
			if _, ok := ns(a[2], 23); !ok {
				return fail()
			}
			if _, ok := ns(a[2], 35); !ok {
				return fail()
			}
			next := append([]byte(nil), a[2]...)
			copy(next[23:47], parent.Str[23:47])
			if !bytes.Equal(next, parent.Str) {
				return fail()
			}
			add("GET", key, "", string(a[2]))
		case "d" + inoStr(b.Stage.Parent):
			if cm.Info.Name != "HSET" || len(a) != 4 || string(a[2]) != b.Stage.Name || string(a[3]) != mkBirthEntry(b.Stage.Inode) {
				return fail()
			}
			add("HGET", key, b.Stage.Name, string(a[3]))
		case "usedSpace", "totalInodes":
			delta := int64(1)
			if key == "usedSpace" {
				delta = 4096
				if cm.Info.Name != "INCRBY" || len(a) != 3 || string(a[2]) != "4096" {
					return fail()
				}
			} else if cm.Info.Name != "INCR" || len(a) != 2 {
				return fail()
			}
			value, err := read("GET", key)
			n, valid := RedisInt(value.Str)
			if err != nil || value.Null || !valid || n < 0 || strconv.FormatInt(n, 10) != string(value.Str) || n > math.MaxInt64-delta {
				return fail()
			}
			add("GET", key, "", strconv.FormatInt(n+delta, 10))
		default:
			return fail()
		}
	}
	for _, key := range []string{inodeKey, "d" + inoStr(b.Stage.Parent), "usedSpace", "totalInodes"} {
		if counts[key] != 1 {
			return fail()
		}
	}
	u.replacementReceipt = args
	return nil
}

func mkBirthEntry(ino uint64) string {
	b := make([]byte, 9)
	b[0] = typeFile
	binary.BigEndian.PutUint64(b[1:], ino)
	return string(b)
}
