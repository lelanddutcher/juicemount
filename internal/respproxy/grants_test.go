package respproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// principalsBody renders a principals file over the test credentials with
// an optional version and people section.
func principalsBody(t testing.TB, version int, people string) []byte {
	t.Helper()
	h, err := HashPassword("x-pass", 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.WriteString("{")
	if version != 0 {
		fmt.Fprintf(&b, `"version":%d,`, version)
	}
	fmt.Fprintf(&b, `"principals":[{"name":"mac1","kind":"mac","argon2id":%q},{"name":"mac2","kind":"mac","argon2id":%q},`+
		`{"name":"bg1","kind":"bg","argon2id":%q}]`, h, h, h)
	if people != "" {
		b.WriteString(`,"people":` + people)
	}
	b.WriteString("}")
	return []byte(b.String())
}

func TestPrincipalsFormatVersions(t *testing.T) {
	john := `[{"id":"p-john","name":"John","access":"granted","grants":["/Clients/A/","/Clients/A"],"devices":["mac1","mac-gone"]}]`
	for _, c := range []struct {
		name    string
		body    []byte
		wantErr string
	}{
		{"v1 without version", principalsBody(t, 0, ""), ""},
		{"v1 explicit", principalsBody(t, 1, ""), ""},
		{"v2 without people", principalsBody(t, 2, ""), ""},
		{"v2 with people", principalsBody(t, 2, john), ""},
		{"future version", principalsBody(t, 3, ""), "format version 3"},
		{"people without v2", principalsBody(t, 0, john), "needs format version 2"},
		{"service principal as device", principalsBody(t, 2, `[{"id":"p-x","access":"all","devices":["bg1"]}]`), "only Macs"},
		{"device of two people", principalsBody(t, 2,
			`[{"id":"p-a","access":"all","devices":["mac1"]},{"id":"p-b","access":"all","devices":["mac1"]}]`), "already belongs"},
		{"duplicate id", principalsBody(t, 2, `[{"id":"p-a","access":"all"},{"id":"p-a","access":"all"}]`), "duplicate id"},
		{"bad access", principalsBody(t, 2, `[{"id":"p-a","access":"some"}]`), "access must be"},
		{"relative grant", principalsBody(t, 2, `[{"id":"p-a","access":"granted","grants":["Clients"]}]`), "not an absolute path"},
		{"dotdot grant", principalsBody(t, 2, `[{"id":"p-a","access":"granted","grants":["/Clients/../B"]}]`), `".."`},
		{"root grant", principalsBody(t, 2, `[{"id":"p-a","access":"granted","grants":["/"]}]`), "root"},
		{"unknown person field", principalsBody(t, 2, `[{"id":"p-a","access":"all","role":"x"}]`), "unknown field"},
	} {
		ps, err := ParsePrincipals(c.body)
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
			t.Errorf("%s: err = %v, want %q", c.name, err, c.wantErr)
		case c.wantErr == "" && c.name == "v2 with people":
			p := ps.PersonOf("mac1")
			if p == nil || p.ID != "p-john" || p.Name != "John" || !p.Scoped || len(p.Grants) != 1 || p.Grants[0] != "/Clients/A" {
				t.Errorf("person of mac1 = %+v", p)
			}
			if ps.PersonOf("mac2") != nil || ps.PersonOf("bg1") != nil || !ps.GrantsInForce() {
				t.Errorf("mac2/bg1 have a person, or grants not in force")
			}
		case c.wantErr == "":
			if ps.PersonOf("mac1") != nil || ps.GrantsInForce() {
				t.Errorf("%s: people where there are none", c.name)
			}
		}
	}
}

// oldParsePrincipals is the parser of a proxy built before format version
// 2 (principals.go before Teams slice 1): strict, principals only.
func oldParsePrincipals(data []byte) error {
	var f struct {
		Principals []PrincipalEntry `json:"principals"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing data")
	}
	return nil
}

// What an old proxy does with each file: it reads every version-1 file
// and refuses every version-2 file. The refusal keeps its old table (the
// reloader does not acknowledge, the Manager sees a stalled proxy); this is
// why the Manager writes version 2 only for a proxy that advertises it.
func TestOldProxyRefusesVersion2KeepsVersion1(t *testing.T) {
	if err := oldParsePrincipals(principalsBody(t, 0, "")); err != nil {
		t.Fatalf("old proxy refuses a v1 file: %v", err)
	}
	for _, body := range [][]byte{principalsBody(t, 2, ""), principalsBody(t, 2, `[{"id":"p-a","access":"all"}]`)} {
		if err := oldParsePrincipals(body); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("old proxy on a v2 file: %v, want an unknown-field refusal", err)
		}
	}
}

// The reloader's heartbeat advertises the newest format this proxy reads.
func TestReloadHeartbeatAdvertisesFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "principals.json")
	if err := os.WriteFile(path, principalsBody(t, 2, ""), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := NewPrincipalReloader(path, NewAuthenticator(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Tick(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "reload.heartbeat"))
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Fields(string(b))
	if len(f) != 2 || f[1] != HeartbeatFormatToken+"2" {
		t.Fatalf("heartbeat = %q, want \"<time> principals-format=2\"", b)
	}
}

func TestCleanGrantPath(t *testing.T) {
	for in, want := range map[string]string{
		"/Clients": "/Clients", "/Clients/": "/Clients", "/Clients/A": "/Clients/A", "/a b/c d": "/a b/c d",
	} {
		if got, err := CleanGrantPath(in); err != nil || got != want {
			t.Errorf("CleanGrantPath(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "/", "Clients", "//Clients", "/Clients//A", "/./A", "/A/..", "/A\x00", "/A\nB"} {
		if got, err := CleanGrantPath(in); err == nil {
			t.Errorf("CleanGrantPath(%q) = %q, want an error", in, got)
		}
	}
}

func TestRestrictUnassigned(t *testing.T) {
	v1 := strings.Replace(string(principalsBody(t, 0, "")), `{"principals"`, `{"restrict_unassigned":true,"principals"`, 1)
	if _, err := ParsePrincipals([]byte(v1)); err == nil || !strings.Contains(err.Error(), "needs format version 2") {
		t.Fatalf("restrict_unassigned in a version-1 file: %v", err)
	}
	v2 := strings.Replace(string(principalsBody(t, 2, "")), `"version":2,`, `"version":2,"restrict_unassigned":true,`, 1)
	ps, err := ParsePrincipals([]byte(v2))
	if err != nil || !ps.GrantsInForce() || ps.HasPeople() {
		t.Fatalf("restrict_unassigned without people: in force %v, people %v, %v", ps.GrantsInForce(), ps.HasPeople(), err)
	}
}

func TestTrashEntryParent(t *testing.T) {
	for name, want := range map[string]uint64{
		"12-345-clip.mov": 12, "12-345-a-b.mov": 12, "7-8-": 7,
		"0-1-x": 0, "x-1-y": 0, "12-x-y": 0, "12": 0, "-1-y": 0, "12--y": 0, "012-1-y": 0,
	} {
		got, ok := trashEntryParent(name)
		if ok != (want != 0) || got != want {
			t.Errorf("trashEntryParent(%q) = %d, %v; want %d", name, got, ok, want)
		}
	}
}

func TestClassifyAudit(t *testing.T) {
	unit := func(keys ...string) *Unit {
		u := &Unit{}
		for i, k := range keys {
			u.Cmds = append(u.Cmds, Cmd{Args: [][]byte{[]byte("SET"), []byte(k), []byte("v")}, Info: Command{Name: "SET"}})
			u.Project = append(u.Project, KeyWrite{Cmd: i, Key: ParseKey(k), Raw: k})
		}
		return u
	}
	for _, c := range []struct {
		keys  []string
		want  auditClass
		check int
	}{
		{[]string{"c7_0", "i7"}, auditData, 1},      // contents and the file's own length: compare its attrs
		{[]string{"c7_0"}, auditData, 0},            // contents only
		{[]string{"c7_0", "i8"}, auditNamespace, 0}, // another inode's attributes
		{[]string{"c7_0", "x7"}, auditNamespace, 0}, // an xattr, even of the written file
		{[]string{"c7_0", "p7"}, auditNamespace, 0}, // a link parent
		{[]string{"c7_0", "s9"}, auditNamespace, 0}, // a symlink target
		{[]string{"c7_0", "d3"}, auditNamespace, 0}, // an entry
		{[]string{"i7"}, auditNamespace, 0},         // setattr
		{[]string{"lockf7"}, auditNone, 0},          // a lock
		{[]string{"c7_0", "lockf8"}, auditData, 0},  // contents and a lock
	} {
		got, check := classifyAudit(unit(c.keys...))
		if got != c.want || len(check) != c.check {
			t.Errorf("%v: class %d with %d attr checks, want %d with %d", c.keys, got, len(check), c.want, c.check)
		}
	}
}

func TestAttrDiff(t *testing.T) {
	mk := func(flags byte, mode uint16, uid, gid uint32, acl bool) []byte {
		n := 72
		if acl {
			n = 80
		}
		b := make([]byte, n)
		b[0] = flags
		b[1], b[2] = byte(mode>>8), byte(mode)
		b[3], b[4], b[5], b[6] = byte(uid>>24), byte(uid>>16), byte(uid>>8), byte(uid)
		b[7], b[8], b[9], b[10] = byte(gid>>24), byte(gid>>16), byte(gid>>8), byte(gid)
		if acl {
			b[75], b[79] = 3, 4
		}
		return b
	}
	base := mk(0, 1<<12|0o644, 501, 20, false)
	if d := attrDiff(7, base, mk(0, 1<<12|0o644, 501, 20, false)); len(d) != 0 {
		t.Fatalf("no change reported as %+v", d)
	}
	d := attrDiff(7, base, mk(1, 1<<12|0o4755, 0, 0, true))
	got := map[string]string{}
	for _, c := range d {
		got[c.Field] = c.Old + ">" + c.New
	}
	want := map[string]string{"mode": "0644>4755", "uid": "501>0", "gid": "20>0", "flags": "0x0>0x1", "acl": "none>3/4"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("changes %v, want %v", got, want)
	}
	if d := attrDiff(7, base, []byte("short")); len(d) != 1 || d[0].Field != "attr" {
		t.Fatalf("unparsable value: %+v", d)
	}
}
