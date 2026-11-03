package respproxy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
)

func TestAccountingValueDenial(t *testing.T) {
	key := func(name string) KeyRef { return ParseKey(name) }
	for _, c := range []struct {
		args []string
		kind Kind
		bad  bool
	}{
		{[]string{"HINCRBY", "dirQuotaUsedSpace", "5", "4096"}, KindMac, false},
		{[]string{"HINCRBY", "dirQuotaUsedSpace", "5", "-4096"}, KindMac, false},
		{[]string{"HINCRBY", "dirQuotaUsedSpace", "5", "+4096"}, KindMac, true},
		{[]string{"HINCRBY", "dirQuotaUsedSpace", "5", "007"}, KindMac, true},
		{[]string{"HINCRBYFLOAT", "dirUsedSpace", "5", "1"}, KindAdmin, true},
		{[]string{"HINCRBYFLOAT", "dirUsedSpace", "5", "1"}, KindBG, false},
		{[]string{"HSET", "dirUsedInodes", "5", "0", "6", "1.5"}, KindMac, true},
		{[]string{"HSET", "dirUsedInodes", "5", "0", "6", "12"}, KindMac, false},
		{[]string{"HSET", "dirQuota", "5", "\x00\x01packed-limits"}, KindAdmin, false},
	} {
		args := make([][]byte, len(c.args))
		for i, a := range c.args {
			args[i] = []byte(a)
		}
		u := &Unit{Cmds: []Cmd{{Args: args, Info: Command{Name: c.args[0], Class: ClassWrite}}},
			Writes: []KeyWrite{{Cmd: 0, Key: key(c.args[1]), Raw: c.args[1]}}}
		if d := accountingValueDenial(Principal{Name: "x", Kind: c.kind}, u); (d != nil) != c.bad {
			t.Errorf("%q by %s: %v", c.args, c.kind, d)
		}
	}
}

// A capped intent stays within its encoded bound whatever its names hold:
// JSON escapes a byte to up to six, so a byte limit on names and paths
// alone let one reach 4.3 MB (review round 4). The digest is that of the
// unit's commands in RESP, as an AOF holds them.
func TestCapRecordBoundsEncodedSize(t *testing.T) {
	cmds := []Cmd{{Args: [][]byte{[]byte("HSET"), []byte("d5"), []byte("a"), []byte("\x01b")}}, {Args: [][]byte{[]byte("SET"), []byte("i6"), {}}}}
	for _, ch := range []string{"a", "<", "\x01"} {
		n, p := strings.Repeat(ch, 300), strings.Repeat(ch, 5000)
		r := auditlog.Record{Type: auditlog.TypeIntent, Detail: strings.Repeat(ch, 512), Principal: "mac-x", Person: "p-x"}
		for i := 0; i < 1000; i++ {
			r.Ops = append(r.Ops, auditlog.Op{Op: "remove", Parent: 1 << 60, Name: n, Ino: 1 << 60, Dir: true, Trash: true, Path: p})
			r.Rows = append(r.Rows, auditlog.Row{Hash: "dirQuotaUsedInodes", Ino: 1 << 60, Op: "incr", Value: "-9223372036854775808", Path: p})
			r.Inodes = append(r.Inodes, 1<<60)
			r.Attrs = append(r.Attrs, auditlog.AttrChange{Ino: 1 << 60, Field: "attr", Old: p, New: p})
			r.Projects = append(r.Projects, auditlog.Project{Ino: 1 << 60, Path: p})
		}
		capRecord(&r, intentLimits, cmds)
		b, _ := json.Marshal(r)
		if len(b) > intentLimits.encoded || r.Omitted == 0 || len(r.Ops) == 0 {
			t.Errorf("%q: %d bytes (bound %d), omitted %d, %d ops", ch, len(b), intentLimits.encoded, r.Omitted, len(r.Ops))
		}
		want := sha256.Sum256([]byte("*4\r\n$4\r\nHSET\r\n$2\r\nd5\r\n$1\r\na\r\n$2\r\n\x01b\r\n*3\r\n$3\r\nSET\r\n$2\r\ni6\r\n$0\r\n\r\n"))
		if r.Digest != "sha256:"+hex.EncodeToString(want[:]) {
			t.Errorf("digest %s", r.Digest)
		}
	}
	small := auditlog.Record{Type: auditlog.TypeIntent, Ops: []auditlog.Op{{Op: "add", Name: "x"}}}
	capRecord(&small, intentLimits, cmds)
	if small.Omitted != 0 || small.Digest != "" || len(small.Ops) != 1 {
		t.Errorf("an intent within the bounds was changed: %+v", small)
	}
}
