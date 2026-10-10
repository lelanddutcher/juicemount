package respproxy

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseKeyShapes(t *testing.T) {
	cases := map[string]struct {
		shape string
		ino   uint64
		trash bool
	}{
		"i12":                  {ShapeInode, 12, false},
		"d1":                   {ShapeDentry, 1, false},
		"p77":                  {ShapeParent, 77, false},
		"x5":                   {ShapeXattr, 5, false},
		"s9":                   {ShapeSymlink, 9, false},
		"c42_3":                {ShapeChunk, 42, false},
		"lockf8":               {ShapeFlock, 8, false},
		"lockp8":               {ShapePlock, 8, false},
		"locked3":              {ShapeLocked, 0, false},
		"session3":             {ShapeSession, 0, false},
		"sessionInfos":         {"sessionInfos", 0, false},
		"nextinode":            {"nextinode", 0, false},
		"sliceRef":             {"sliceRef", 0, false},
		"d9223372032828243968": {ShapeDentry, TrashInode, true},
		"juicemount:metadata":  {ShapeChannel, 0, false},
		"jum:presence:h":       {ShapePresence, 0, false},
		"juicefarm:queue":      {ShapeFarm, 0, false},
		"jmlease:v1:cfg":       {ShapeLease, 0, false},
		"ifoo":                 {ShapeUnknown, 0, false},
		"i+5":                  {ShapeUnknown, 0, false},
		"c42":                  {ShapeUnknown, 0, false},
		"jum:presence:":        {ShapeUnknown, 0, false},
	}
	for k, want := range cases {
		got := ParseKey(k)
		if got.Shape != want.shape || got.Ino != want.ino || got.Trash != want.trash {
			t.Errorf("ParseKey(%q) = %+v, want %+v", k, got, want)
		}
	}
}

func TestClassifyCommands(t *testing.T) {
	for _, c := range []struct {
		args []string
		want Class
	}{
		{[]string{"MSET", "i1", "a", "d1", "b"}, ClassWrite},
		{[]string{"CONFIG", "SET", "maxmemory-policy", "noeviction"}, ClassAdminW},
		{[]string{"CONFIG", "GET", "maxmemory-policy"}, ClassAdmin},
		{[]string{"FOO.BAR", "x"}, ClassUnknown},
		{[]string{"JM.LEASE", "STATUS"}, ClassLocal},
		{[]string{"CLIENT", "REPLY", "OFF"}, ClassForbidden},
		{[]string{"CLIENT", "KILL", "ID", "1"}, ClassAdminW},
		{[]string{"CLIENT", "SETINFO", "LIB-NAME", "x"}, ClassConn},
		{[]string{"RESET"}, ClassForbidden},
		{[]string{"MONITOR"}, ClassForbidden},
		{[]string{"BRPOP", "juicefarm:queue", "5"}, ClassWrite},
		{[]string{"SCRIPT", "FLUSH"}, ClassAdminW},
		{[]string{"EVAL", "return redis.call('GET', KEYS[1])", "1", "i1"}, ClassScript}, // not allow-listed
		{[]string{"EVALSHA", "abc", "0"}, ClassScript},
		{[]string{"EVALSHA", "8526c64984678fc27f67f44755c8ba7dcdf7ea60", "2", "d1", "name"}, ClassRead},
		{[]string{"EVALSHA", "8526C64984678FC27F67F44755C8BA7DCDF7EA60", "2", "d1", "name"}, ClassRead},
		{[]string{"SCRIPT", "LOAD", "return 1"}, ClassScript},
		{[]string{"COPY", "i1", "i2", "DB", "0"}, ClassWrite},
	} {
		got := Classify(bytesArgs(c.args...))
		if got.Class != c.want {
			t.Errorf("Classify(%q) = %s, want %s", c.args, got.Class, c.want)
		}
	}
	c := Classify(bytesArgs("BRPOP", "juicefarm:a", "juicefarm:b", "5"))
	if len(c.Keys) != 2 {
		t.Fatalf("BRPOP keys: %+v", c.Raw)
	}
}

// AllowedScripts must pin exactly the bodies in the tree: recompute every
// SHA1 from source (raw-string literals and the embedded .lua file), so an
// edited script fails here instead of silently losing or gaining access.
func TestScriptAllowListPinned(t *testing.T) {
	type src struct {
		file string
		re   string
	}
	srcs := []src{
		{"../../third_party/juicefs/pkg/meta/lua_scripts.go", "(?s)const (?:scriptLookup|scriptResolve) = `(.*?)`"},
		{"../../metadata/redis.go", "(?s)const luaScanBatch = `(.*?)`"},
		{"../../cache/volume.go", "(?s)const \\w+Lua = `(.*?)`"},
		{"../../cache/volume.go", "(?s)rdb\\.Eval\\(ctx, `(.*?)`"},
	}
	got := map[string]bool{}
	for _, sr := range srcs {
		b, err := os.ReadFile(sr.file)
		if err != nil {
			t.Fatal(err)
		}
		ms := regexp.MustCompile(sr.re).FindAllSubmatch(b, -1)
		if len(ms) == 0 {
			t.Fatalf("%s: no script matched %s", sr.file, sr.re)
		}
		for _, m := range ms {
			got[ScriptSHA(m[1])] = true
		}
	}
	lua, err := os.ReadFile("../../metadata/parent_read_proof.lua")
	if err != nil {
		t.Fatal(err)
	}
	got[ScriptSHA(lua)] = true
	for sha := range got {
		if AllowedScripts[sha] == "" {
			t.Errorf("tree script %s is not on the allow-list (edited? re-review and re-pin it)", sha)
		}
	}
	for sha, what := range AllowedScripts {
		if !got[sha] {
			t.Errorf("allow-listed %s (%s) no longer exists in the tree", sha, what)
		}
	}
	// Every Mac-side Eval call site in cache/ and metadata/ must use a pinned
	// body: count raw-string bodies that call redis and compare.
	raw := regexp.MustCompile("(?s)`([^`]*redis\\.(?:p?call)[^`]*)`")
	n := 0
	for _, dir := range []string{"../../cache", "../../metadata"} {
		files, _ := filepath.Glob(filepath.Join(dir, "*.go"))
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			b, _ := os.ReadFile(f)
			for _, m := range raw.FindAllSubmatch(b, -1) {
				n++
				if AllowedScripts[ScriptSHA(m[1])] == "" {
					t.Errorf("%s: Mac-side script not pinned:\n%.200s", f, m[1])
				}
			}
		}
	}
	if n < 5 {
		t.Fatalf("found only %d Mac-side scripts; the scan is broken", n)
	}
}

func TestReadCommandAndCopyFrame(t *testing.T) {
	in := "*3\r\n$3\r\nSET\r\n$2\r\ni1\r\n$3\r\na\r\n\r\nPING hello\r\n"
	r := bufio.NewReader(strings.NewReader(in))
	args, err := ReadCommand(r)
	if err != nil || len(args) != 3 || string(args[2]) != "a\r\n" {
		t.Fatalf("array cmd: %q %v", args, err)
	}
	args, err = ReadCommand(r)
	if err != nil || len(args) != 2 || string(args[0]) != "PING" {
		t.Fatalf("inline cmd: %q %v", args, err)
	}
	// RESP3 map with nested array, a push frame, attribute, bulk, null array.
	replies := "%2\r\n+a\r\n*2\r\n:1\r\n$-1\r\n+b\r\n_\r\n" +
		">3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$2\r\nhi\r\n" +
		"|1\r\n+k\r\n+v\r\n:5\r\n" +
		"$5\r\nhe\r\nl\r\n" + "*-1\r\n" + "-ERR x\r\n"
	rr := bufio.NewReader(strings.NewReader(replies))
	var out bytes.Buffer
	w := bufio.NewWriter(&out)
	var types []byte
	var nulls []bool
	for i := 0; i < 6; i++ {
		info, err := CopyFrame(rr, w)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		types = append(types, info.Type)
		nulls = append(nulls, info.Null)
	}
	w.Flush()
	if string(types) != "%>:$*-" {
		t.Fatalf("frame types %q", types)
	}
	if !nulls[4] || nulls[0] || nulls[3] {
		t.Fatalf("null flags %v", nulls)
	}
	if out.String() != replies {
		t.Fatalf("copy not byte-exact:\n%q\n%q", out.String(), replies)
	}
	v, err := ReadValue(bufio.NewReader(strings.NewReader(replies)))
	if err != nil || v.Type != '%' || len(v.Elems) != 4 {
		t.Fatalf("ReadValue map: %+v %v", v, err)
	}
}

// A create-like txn: HSET d<P> name entry(ino) + SET i<ino> attr. The new
// inode is anchored by the dentry; a bare chunk write on it is anchored by
// the parent learned from the earlier txn.
func TestAnalyseAnchors(t *testing.T) {
	s := NewStats()
	s.TrackAnchors = true
	entry := append([]byte{1}, be64(20)...)
	attr := make([]byte, 72)
	copy(attr[63:71], be64(1))
	var ws []writeRec
	for _, a := range [][][]byte{
		append(bytesArgs("HSET", "d1", "f"), entry), append(bytesArgs("SET", "i20"), attr), bytesArgs("SET", "i1", "x"),
	} {
		ws = append(ws, writeRec{cmd: Classify(a), args: a})
	}
	s.analyse("txn", ws)
	a := s.Snapshot().Anchors[ShapeInode]
	if a[AnchorDentryInTxn] != 1 || a[AnchorDirOfDentry] != 1 {
		t.Fatalf("create anchors: %+v", a)
	}
	if s.Snapshot().NewInodeNoDentry != 0 {
		t.Fatal("create counted as unanchored new inode")
	}
	args := bytesArgs("RPUSH", "c20_0", "x")
	s.analyse("bare", []writeRec{{cmd: Classify(args), args: args}})
	if got := s.Snapshot().Anchors[ShapeChunk][AnchorLearned]; got != 1 {
		t.Fatalf("chunk anchor learned = %d", got)
	}
	args = bytesArgs("SET", "i99", "x")
	s.analyse("txn", []writeRec{{cmd: Classify(args), args: args}})
	if s.Snapshot().NewInodeNoDentry != 1 {
		t.Fatal("unanchored new inode not flagged")
	}
}

func TestPrincipalsParse(t *testing.T) {
	h, err := HashPassword("pw", 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	good := `{"principals":[{"name":"a","kind":"mac","argon2id":"` + h + `"}]}`
	if ps, err := ParsePrincipals([]byte(good)); err != nil || ps.Len() != 1 {
		t.Fatalf("good file: %v", err)
	}
	for name, body := range map[string]string{
		"anon kind":     `{"principals":[{"name":"a","kind":"anon","argon2id":"` + h + `"}]}`,
		"duplicate":     `{"principals":[{"name":"a","kind":"mac","argon2id":"` + h + `"},{"name":"a","kind":"bg","argon2id":"` + h + `"}]}`,
		"bad hash":      `{"principals":[{"name":"a","kind":"mac","argon2id":"plaintext"}]}`,
		"huge memory":   `{"principals":[{"name":"a","kind":"mac","argon2id":"$argon2id$v=19$m=99999999,t=1,p=1$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaA"}]}`,
		"space in name": `{"principals":[{"name":"a b","kind":"mac","argon2id":"` + h + `"}]}`,
		"unknown field": `{"principals":[{"name":"a","kind":"mac","argon2id":"` + h + `","password":"x"}]}`,
	} {
		if _, err := ParsePrincipals([]byte(body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestAuthThrottleLocksOutAfterFailures(t *testing.T) {
	a := NewAuthenticator(principalsForTest(t))
	now := time.Unix(1_000_000, 0)
	a.Now = func() time.Time { return now }
	if _, r := a.Authenticate("10.0.0.1:5", "mac1", "mac1-pass"); r != AuthOK {
		t.Fatalf("correct password: %v", r)
	}
	for i := 0; i < throttleFree; i++ {
		if _, r := a.Authenticate("10.0.0.1:5", "mac1", "guess"); r != AuthBad {
			t.Fatalf("failure %d: %v", i, r)
		}
	}
	// Locked out: even the right password is refused, from any port.
	if _, r := a.Authenticate("10.0.0.1:6", "mac1", "mac1-pass"); r != AuthThrottled {
		t.Fatalf("during lockout: %v", r)
	}
	// Another IP is not affected.
	if _, r := a.Authenticate("10.0.0.2:5", "mac1", "mac1-pass"); r != AuthOK {
		t.Fatalf("other IP: %v", r)
	}
	now = now.Add(2 * time.Second)
	if p, r := a.Authenticate("10.0.0.1:5", "mac1", "mac1-pass"); r != AuthOK || p.Name != "mac1" || p.Kind != KindMac {
		t.Fatalf("after lockout: %v %+v", r, p)
	}
	// Lockouts grow with repeated failures.
	for i := 0; i < throttleFree+3; i++ {
		a.Authenticate("10.0.0.3:1", "mac1", "guess")
		now = now.Add(time.Millisecond)
	}
	now = now.Add(4 * time.Second)
	if _, r := a.Authenticate("10.0.0.3:1", "mac1", "mac1-pass"); r != AuthThrottled {
		t.Fatalf("escalated lockout expired too early: %v", r)
	}
}

// fakeChunks serves side reads of chunk lists for the guard.
type fakeChunks map[string][]string

func (f fakeChunks) ChunkElems(db int, key string) ([][]byte, error) { return strArgs(f[key]...), nil }

// mslice marshals one juicefs slice (pos, id, size, off, len).
func mslice(id uint64, size uint32) string {
	b := make([]byte, 24)
	copy(b[4:12], be64(id))
	b[12], b[13], b[14], b[15] = byte(size>>24), byte(size>>16), byte(size>>8), byte(size)
	b[23] = byte(size)
	return string(b)
}

func delayed(ids ...uint64) string {
	var b []byte
	for _, id := range ids {
		b = append(b, be64(id)...)
		b = append(b, 0, 0, 16, 0)
	}
	return string(b)
}

// guardRun runs one unit through the guard.
func guardRun(g *Guard, p Principal, kind UnitKind, cmds ...[]string) (*Denial, *Unit, []guardEffect) {
	return guardRunW(g, p, kind, nil, cmds...)
}

func guardRunW(g *Guard, p Principal, kind UnitKind, watched []string, cmds ...[]string) (*Denial, *Unit, []guardEffect) {
	u := &Unit{Kind: kind, Watched: watched}
	for _, c := range cmds {
		args := bytesArgs(c...)
		u.Cmds = append(u.Cmds, Cmd{Args: args, Info: Classify(args)})
	}
	d, effects := g.Check(p, u)
	return d, u, effects
}

func TestGuardKeyClasses(t *testing.T) {
	mac := Principal{Name: "mac1", Kind: KindMac, Host: "mac1-host"}
	bg := Principal{Name: "bg1", Kind: KindBG}
	farm := Principal{Name: "farm1", Kind: KindFarm}
	dav := Principal{Name: "dav1", Kind: KindWebDAV}
	admin := Principal{Name: "admin1", Kind: KindAdmin}
	trash := func(n uint64) string { return "d" + itoa(TrashInode+n) }
	trashI := func(n uint64) string { return "i" + itoa(TrashInode+n) }

	cases := []struct {
		name    string
		p       Principal
		kind    UnitKind
		cmds    [][]string
		want    Reason // "" = admitted
		project int    // expected len(u.Project) when admitted
	}{
		{"create txn", mac, UnitTxn, [][]string{{"HSET", "d5", "f", "x"}, {"SET", "i9", "a"}, {"INCRBY", "usedSpace", "4096"}, {"INCR", "totalInodes"}}, "", 2},
		{"unlink to trash is project", mac, UnitTxn, [][]string{{"HDEL", "d5", "f"}, {"HSET", trash(1), "5-9-f", "x"}, {"SET", "i9", "a"}, {"ZADD", "delfiles", "1", "9:0"}}, "", 4},
		{"delfiles names another inode", mac, UnitTxn, [][]string{{"HSET", "d5", "f", "x"}, {"ZADD", "delfiles", "1", "777777:999"}}, ReasonBGOnly, 0},
		{"delfiles with options", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"ZADD", "delfiles", "NX", "1", "9:0"}}, ReasonBGOnly, 0},
		{"bare trash cleanup is bg", mac, UnitBare, [][]string{{"HDEL", trash(1), "5-9-f"}}, ReasonBGOnly, 0},
		{"bg trash cleanup", bg, UnitTxn, [][]string{{"HDEL", trash(1), "5-9-f"}, {"DEL", "i9"}}, "", 1},
		{"hourly trash dir create", mac, UnitTxn, [][]string{{"HSET", trash(0), "2026-10-06-13", "x"}, {"SET", trashI(3), "a"}, {"SET", trashI(0), "a"}}, "", 0},
		{"bare trash SETNX", mac, UnitBare, [][]string{{"SETNX", trashI(0), "a"}}, "", 0},
		{"bare delfiles ZREM", mac, UnitBare, [][]string{{"ZREM", "delfiles", "9:0"}}, ReasonBGOnly, 0},
		{"bg delfiles ZREM", bg, UnitBare, [][]string{{"ZREM", "delfiles", "9:0"}}, "", 0},
		{"bare sliceRef HDEL", mac, UnitBare, [][]string{{"HDEL", "sliceRef", "k1_4096"}}, ReasonBGOnly, 0},
		{"clone: sliceRef of a pushed slice", mac, UnitTxn, [][]string{{"RPUSH", "c9_0", mslice(1, 4096)}, {"HINCRBY", "sliceRef", "k1_4096", "1"}}, "", 2},
		{"sliceRef of a slice the txn does not push", mac, UnitTxn, [][]string{{"RPUSH", "c9_0", mslice(1, 4096)}, {"HINCRBY", "sliceRef", "k1_4096", "1"}, {"HINCRBY", "sliceRef", "k123_4096", "-1"}}, ReasonBGOnly, 0},
		// Review H8B-L1b #1: a forged chunk element naming a victim's slice.
		{"forged push + decrement", mac, UnitTxn, [][]string{{"RPUSH", "c100_0", mslice(555555, 4096)}, {"HINCRBY", "sliceRef", "k555555_4096", "-1"}}, ReasonAdminOnly, 0},
		{"forged push + delSlices", mac, UnitTxn, [][]string{{"RPUSH", "c100_0", mslice(555555, 4096)}, {"HSET", "delSlices", "555555_17", delayed(555555)}}, ReasonAdminOnly, 0},
		{"forged push alone", mac, UnitTxn, [][]string{{"RPUSH", "c100_0", mslice(555555, 4096)}, {"SET", "i100", "a"}}, ReasonAdminOnly, 0},
		{"hole push", mac, UnitTxn, [][]string{{"RPUSH", "c100_0", mslice(0, 4096)}, {"SET", "i100", "a"}}, "", 2},
		{"push of a non-slice value", mac, UnitTxn, [][]string{{"RPUSH", "c100_0", "garbage"}}, ReasonUnknownKey, 0},
		{"nextinode over batch", mac, UnitBare, [][]string{{"INCRBY", "nextinode", "9223372032828244968"}}, ReasonAdminOnly, 0},
		{"nextinode batch", mac, UnitBare, [][]string{{"INCRBY", "nextinode", "1024"}}, "", 0},
		{"nextinode 1025", mac, UnitBare, [][]string{{"INCRBY", "nextinode", "1025"}}, ReasonAdminOnly, 0},
		{"nextchunk batch", mac, UnitBare, [][]string{{"INCRBY", "nextchunk", "4096"}}, "", 0},
		{"nextchunk 4097", mac, UnitBare, [][]string{{"INCRBY", "nextchunk", "4097"}}, ReasonAdminOnly, 0},
		{"nextchunk in txn", mac, UnitTxn, [][]string{{"INCRBY", "nextchunk", "4096"}}, ReasonAdminOnly, 0},
		{"bare usedSpace (not a file op)", mac, UnitBare, [][]string{{"INCRBY", "usedSpace", "4096"}}, ReasonBGOnly, 0},
		{"usedSpace 1e18 in txn", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"INCRBY", "usedSpace", "1000000000000000000"}}, ReasonAdminOnly, 0},
		{"usedSpace 17 TiB in txn", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"INCRBY", "usedSpace", "18691697672192"}}, ReasonAdminOnly, 0},
		// bg is exempt from the per-minute rate bound, not from the per-txn bound.
		{"bg usedSpace 2 TiB in txn", bg, UnitTxn, [][]string{{"DEL", "i9"}, {"INCRBY", "usedSpace", "2199023255552"}}, ReasonAdminOnly, 0},
		{"nextchunk not a full batch", mac, UnitBare, [][]string{{"INCRBY", "nextchunk", "1"}}, ReasonAdminOnly, 0},
		{"INCR nextchunk", mac, UnitBare, [][]string{{"INCR", "nextchunk"}}, ReasonAdminOnly, 0},
		{"usedSpace -1e18", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"INCRBY", "usedSpace", "-1000000000000000000"}}, ReasonAdminOnly, 0},
		{"usedSpace 1 TiB truncate", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"INCRBY", "usedSpace", "1099511627776"}}, "", 1},
		{"totalInodes DECRBY 1e15", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"DECRBY", "totalInodes", "1000000000000000"}}, ReasonAdminOnly, 0},
		{"totalInodes INCRBY 2", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"INCRBY", "totalInodes", "2"}}, ReasonAdminOnly, 0},
		{"bare totalInodes", mac, UnitBare, [][]string{{"DECR", "totalInodes"}}, ReasonBGOnly, 0},
		{"sliceRef garbage field", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"HINCRBY", "sliceRef", "zz", "-1"}}, ReasonBGOnly, 0},
		{"gc-only txn", mac, UnitTxn, [][]string{{"HINCRBY", "sliceRef", "k1_4096", "-1"}, {"ZADD", "delfiles", "1", "9:0"}}, ReasonBGOnly, 0},
		{"delSlices bare", mac, UnitBare, [][]string{{"HSET", "delSlices", "1_2", "x"}}, ReasonBGOnly, 0},
		{"lastCleanupTrash", mac, UnitBare, [][]string{{"SET", "lastCleanupTrash", "1"}}, ReasonBGOnly, 0},
		{"lastCleanupSessions allowed", mac, UnitBare, [][]string{{"SET", "lastCleanupSessions", "1"}}, "", 0},
		{"rewind nextsession", mac, UnitBare, [][]string{{"SET", "nextsession", "41"}}, ReasonAdminOnly, 0},
		{"decrement nextinode", mac, UnitBare, [][]string{{"INCRBY", "nextinode", "-100"}}, ReasonAdminOnly, 0},
		{"DEL nextchunk", mac, UnitBare, [][]string{{"DEL", "nextchunk"}}, ReasonAdminOnly, 0},
		{"nextsession in txn", mac, UnitTxn, [][]string{{"INCRBY", "nextsession", "1"}}, ReasonAdminOnly, 0},
		{"nextsession huge", mac, UnitBare, [][]string{{"INCRBY", "nextsession", "1000"}}, ReasonAdminOnly, 0},
		{"SET usedSpace", mac, UnitBare, [][]string{{"SET", "usedSpace", "0"}}, ReasonBGOnly, 0},
		{"dirstat flush", mac, UnitBare, [][]string{{"HINCRBY", "dirUsedSpace", "5", "4096"}}, "", 1},
		{"DEL whole dirstat hash", mac, UnitBare, [][]string{{"DEL", "dirUsedSpace", "dirUsedInodes"}}, ReasonAdminOnly, 0},
		{"final unlink deletes", mac, UnitTxn, [][]string{{"HDEL", "d5", "f"}, {"DEL", "i9"}, {"DEL", "s9"}, {"DEL", "x9"}, {"DEL", "p9"}, {"HDEL", "lockf9", "3_1"}, {"HDEL", "lockp9", "3_1"}}, "", 7},
		{"rmdir drops quota row", mac, UnitTxn, [][]string{{"HDEL", "d1", "dir"}, {"DEL", "d5"}, {"HDEL", "dirQuota", "5"}}, "", 3},
		{"quota set", mac, UnitBare, [][]string{{"HSET", "dirQuota", "5", "x"}}, ReasonAdminOnly, 0},
		{"setting", mac, UnitBare, [][]string{{"SET", "setting", "{}"}}, ReasonAdminOnly, 0},
		{"acl", mac, UnitBare, [][]string{{"HSET", "acl", "1", "x"}}, ReasonAdminOnly, 0},
		{"admin setting", admin, UnitBare, [][]string{{"SET", "setting", "{}"}}, "", 0},
		{"own presence", mac, UnitBare, [][]string{{"HSET", "jum:presence:mac1-host", "p", "1"}}, "", 0},
		{"other presence", mac, UnitBare, [][]string{{"EXPIRE", "jum:presence:mac2-host", "45"}}, ReasonAdminOnly, 0},
		{"link bench EX 60", mac, UnitBare, [][]string{{"SET", "jm:link-bench:ab", "x", "EX", "60"}}, "", 0},
		{"link bench EX 61", mac, UnitBare, [][]string{{"SET", "jm:link-bench:ab", "x", "EX", "61"}}, ReasonUnknownKey, 0},
		{"link bench PX", mac, UnitBare, [][]string{{"SET", "jm:link-bench:ab", "x", "PX", "500"}}, "", 0},
		{"link bench DEL", mac, UnitBare, [][]string{{"DEL", "jm:link-bench:ab"}}, "", 0},
		{"farm keys from mac", mac, UnitBare, [][]string{{"LPUSH", "juicefarm:queue", "j"}}, ReasonFarmOnly, 0},
		{"farm keys from farm", farm, UnitBare, [][]string{{"BRPOP", "juicefarm:queue", "5"}}, "", 0},
		{"farm script (trusted server principal)", farm, UnitBare, [][]string{{"EVAL", "redis.call('HSET', 'juicefarm:job:' .. ARGV[1], 'a', 'b')", "1", "juicefarm:queue", "x"}}, "", 0},
		{"unpinned script from mac", mac, UnitBare, [][]string{{"EVAL", "return 1", "0"}}, ReasonAdminOnly, 0},
		{"unpinned script from webdav", dav, UnitBare, [][]string{{"EVAL", "return _G['re'..'dis'].call('FLUSHDB')", "0"}}, ReasonAdminOnly, 0},
		{"SCRIPT LOAD unpinned", mac, UnitBare, [][]string{{"SCRIPT", "LOAD", "return server.call('FLUSHDB')"}}, ReasonAdminOnly, 0},
		{"lease keys", farm, UnitBare, [][]string{{"HSET", "jmlease:v1:cfg", "mode", "passthrough"}}, ReasonAdminOnly, 0},
		{"session owner table", mac, UnitBare, [][]string{{"HSET", "jmlease:v1:sess", "42", "mac1"}}, ReasonAdminOnly, 0},
		{"webdav project write", dav, UnitBare, [][]string{{"SET", "i5", "a"}}, ReasonPrincipalReadonly, 0},
		{"webdav counter", dav, UnitBare, [][]string{{"INCR", "nextsession"}}, "", 0},
		{"publish metadata", mac, UnitBare, [][]string{{"PUBLISH", "juicemount:metadata", "x"}}, "", 0},
		{"publish keyspace spoof", mac, UnitBare, [][]string{{"PUBLISH", "__keyspace@1__:i5", "set"}}, ReasonAdminOnly, 0},
		{"config get maxmemory", mac, UnitBare, [][]string{{"CONFIG", "GET", "maxmemory-policy"}}, "", 0},
		{"config get requirepass", mac, UnitBare, [][]string{{"CONFIG", "GET", "requirepass"}}, ReasonAdminOnly, 0},
		{"config get glob", mac, UnitBare, [][]string{{"CONFIG", "GET", "*"}}, ReasonAdminOnly, 0},
		{"client reply off even admin", admin, UnitBare, [][]string{{"CLIENT", "REPLY", "OFF"}}, ReasonAdminOnly, 0},
		{"DEL allSessions", mac, UnitBare, [][]string{{"DEL", "allSessions"}}, ReasonBGOnly, 0},
		{"COPY into another DB", mac, UnitBare, [][]string{{"COPY", "i5", "i6", "DB", "0", "REPLACE"}}, ReasonAdminOnly, 0},
		{"COPY same DB", mac, UnitBare, [][]string{{"COPY", "i5", "i6"}}, "", 2},
		{"MOVE", mac, UnitBare, [][]string{{"MOVE", "i5", "0"}}, ReasonAdminOnly, 0},
		{"admin unknown key", admin, UnitBare, [][]string{{"SET", "whatever", "x"}}, "", 0},
		{"hard link parent", mac, UnitTxn, [][]string{{"HINCRBY", "p9", "5", "1"}, {"HSET", "d6", "l", "x"}}, "", 2},
		{"locks", mac, UnitBare, [][]string{{"HSET", "lockf9", "3_1", "W"}}, "", 1},
	}
	for _, c := range cases {
		g := NewGuard(NewMemSessionStore(), fakeChunks{})
		d, u, _ := guardRun(g, c.p, c.kind, c.cmds...)
		switch {
		case c.want == "" && d != nil:
			t.Errorf("%s: refused %s (%s)", c.name, d.Reason, d.Detail)
		case c.want != "" && (d == nil || d.Reason != c.want):
			t.Errorf("%s: got %+v, want %s", c.name, d, c.want)
		case c.want == "" && len(u.Project) != c.project:
			t.Errorf("%s: %d project-scoped writes, want %d (%+v)", c.name, len(u.Project), c.project, u.Writes)
		}
	}
}

// Shared cleanup records carry the inodes they reference to Decide
// (Unit.RefInodes), and compaction records are verified against the chunk
// the txn rewrites (review H8B-L1 #6).
func TestGuardCleanupRecordsAnchored(t *testing.T) {
	mac := Principal{Name: "mac1", Kind: KindMac}
	chunks := fakeChunks{"c9_0": {mslice(3, 4096), mslice(4, 4096)}}
	g := NewGuard(NewMemSessionStore(), chunks)
	g.slices.Record(5, 5, "mac1") // the proxy relayed slice id 5 to mac1
	// doCompactChunk with trash: LTRIM+LPUSH the WATCHed chunk, track the
	// new slice with "0", delay the old ones.
	compaction := [][]string{{"LTRIM", "c9_0", "2", "-1"}, {"LPUSH", "c9_0", mslice(5, 8192)},
		{"HSET", "sliceRef", "k5_8192", "0"}, {"HSET", "delSlices", "5_17", delayed(3, 4)}}
	d, u, _ := guardRunW(g, mac, UnitTxn, []string{"c9_0"}, compaction...)
	if d != nil {
		t.Fatalf("compaction refused: %+v", d)
	}
	if len(u.RefInodes) != 1 || u.RefInodes[0] != 9 || len(u.Project) != 4 {
		t.Fatalf("RefInodes %v project %d", u.RefInodes, len(u.Project))
	}
	// Compaction re-pushing a head slice already in the same chunk (c).
	if d, _, _ := guardRunW(g, mac, UnitTxn, []string{"c9_0"}, []string{"LTRIM", "c9_0", "2", "-1"},
		[]string{"LPUSH", "c9_0", mslice(5, 8192)}, []string{"LPUSH", "c9_0", mslice(3, 4096)}); d != nil {
		t.Fatalf("re-push of the chunk's own slice: %+v", d)
	}
	// ...but not into ANOTHER chunk.
	if d, _, _ := guardRunW(g, mac, UnitTxn, []string{"c9_0"}, []string{"LTRIM", "c9_0", "2", "-1"},
		[]string{"RPUSH", "c10_0", mslice(3, 4096)}); d == nil {
		t.Fatal("slice of c9_0 pushed into c10_0 admitted")
	}
	// mac2 may not push mac1's allocated slice.
	if d, _, _ := guardRun(g, Principal{Name: "mac2", Kind: KindMac}, UnitTxn, []string{"RPUSH", "c11_0", mslice(5, 8192)}, []string{"SET", "i11", "a"}); d == nil {
		t.Fatal("mac2 pushed a slice allocated to mac1")
	}
	// Same txn without the WATCH: the side read would be unsound.
	if d, _, _ := guardRunW(g, mac, UnitTxn, nil, compaction...); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("unwatched compaction: %+v", d)
	}
	// A delayed record naming another file's slice (777) is refused.
	bad := [][]string{{"LTRIM", "c9_0", "2", "-1"}, {"LPUSH", "c9_0", mslice(5, 8192)},
		{"HSET", "delSlices", "5_17", delayed(3, 777)}}
	if d, _, _ := guardRunW(g, mac, UnitTxn, []string{"c9_0"}, bad...); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("victim slice in delSlices: %+v", d)
	}
	// delfiles anchors to the unlinked inode.
	d, u, _ = guardRun(g, mac, UnitTxn, []string{"HDEL", "d5", "f"}, []string{"SET", "i9", "a"}, []string{"ZADD", "delfiles", "1", "9:4096"})
	if d != nil || len(u.RefInodes) != 1 || u.RefInodes[0] != 9 {
		t.Fatalf("delfiles: %+v %v", d, u.RefInodes)
	}
}

func TestGuardSessionOwnership(t *testing.T) {
	store := NewMemSessionStore()
	g := NewGuard(store, nil)
	mac1 := Principal{Name: "mac1", Kind: KindMac}
	mac2 := Principal{Name: "mac2", Kind: KindMac}
	bg := Principal{Name: "bg1", Kind: KindBG}
	// Writing a session nobody was allocated: refused (no first-writer claim).
	if d, _, _ := guardRun(g, mac1, UnitBare, []string{"ZADD", "allSessions", "100", "7"}); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("unallocated session: %+v", d)
	}
	// Allocation (what the proxy records from mac1's INCRBY nextsession reply).
	u := &Unit{Kind: UnitBare, Cmds: []Cmd{{Args: bytesArgs("INCRBY", "nextsession", "1"), Info: Classify(bytesArgs("INCRBY", "nextsession", "1"))}}}
	if k, n := Allocation(u); k != "nextsession" || n != 1 {
		t.Fatalf("Allocation = %s %d", k, n)
	}
	store.Claim(7, "mac1")
	if d, _, _ := guardRun(g, mac1, UnitBare, []string{"HSET", "sessionInfos", "7", "x"}); d != nil {
		t.Fatalf("own session refused: %+v", d)
	}
	for _, cmd := range [][]string{{"HSET", "sessionInfos", "7", "x"}, {"ZADD", "allSessions", "0", "7"}, {"ZREM", "allSessions", "7"}, {"DEL", "session7"}, {"SADD", "locked7", "lockf9"}} {
		if d, _, _ := guardRun(g, mac2, UnitBare, cmd); d == nil || d.Reason != ReasonBGOnly {
			t.Fatalf("%v from mac2 on mac1's session: %+v", cmd, d)
		}
	}
	// A second claim never moves ownership.
	if got, _ := store.Claim(7, "mac2"); got != "mac1" {
		t.Fatalf("claim moved ownership to %s", got)
	}
	d, _, eff := guardRun(g, bg, UnitBare, []string{"ZREM", "allSessions", "7"})
	if d != nil {
		t.Fatalf("bg session cleanup refused: %+v", d)
	}
	g.Commit(eff)
	if _, ok, _ := store.Owner(7); ok {
		t.Fatal("ended session still owned")
	}
}

// cleanupZeroRef after a principal's own verified compaction is admitted
// and carries the chunk inode; anyone else's (or an untracked) drop stays
// bg-only.
func TestGuardOwnZeroRefDrop(t *testing.T) {
	g := NewGuard(NewMemSessionStore(), fakeChunks{"c9_0": {mslice(3, 4096)}})
	g.slices.Record(5, 5, "mac1")
	mac1 := Principal{Name: "mac1", Kind: KindMac}
	mac2 := Principal{Name: "mac2", Kind: KindMac}
	d, _, eff := guardRunW(g, mac1, UnitTxn, []string{"c9_0"}, []string{"LTRIM", "c9_0", "1", "-1"}, []string{"LPUSH", "c9_0", mslice(5, 4096)},
		[]string{"HSET", "sliceRef", "k5_4096", "0"}, []string{"HSET", "delSlices", "5_17", delayed(3)})
	if d != nil {
		t.Fatalf("compaction refused: %+v", d)
	}
	drop := []string{"HDEL", "sliceRef", "k5_4096"}
	if d, _, _ := guardRun(g, mac1, UnitTxn, drop); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("drop before the compaction committed: %+v", d)
	}
	g.Commit(eff)
	if d, _, _ := guardRun(g, mac2, UnitTxn, drop); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("mac2 dropping mac1's zero ref: %+v", d)
	}
	if d, _, _ := guardRun(g, mac1, UnitTxn, []string{"HDEL", "sliceRef", "k6_4096"}); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("untracked sliceRef drop: %+v", d)
	}
	// A bare drop of a slice mac1 allocated is its own drop (L5 finding),
	// run as the conditional SliceDropScript; mac2's is refused.
	if d, u, _ := guardRun(g, mac1, UnitBare, drop); d != nil || u.rewrite == nil {
		t.Fatalf("bare own drop: %+v", d)
	}
	if d, _, _ := guardRun(g, mac2, UnitBare, drop); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("bare drop by mac2: %+v", d)
	}
	d, u, eff := guardRun(g, mac1, UnitTxn, drop)
	if d != nil || len(u.RefInodes) != 1 || u.RefInodes[0] != 9 || len(u.Project) != 1 {
		t.Fatalf("own zero-ref drop: %+v refs=%v project=%d", d, u.RefInodes, len(u.Project))
	}
	g.Commit(eff)
	if d, _, _ := guardRun(g, mac1, UnitTxn, drop); d == nil {
		t.Fatal("zero-ref drop admitted twice")
	}
}

func TestReadCommandLimits(t *testing.T) {
	for name, in := range map[string]string{
		"too many args pre-auth": "*11\r\n",
		"huge bulk pre-auth":     "*1\r\n$400000000\r\n",
		"total over 16KB":        "*2\r\n$9000\r\n" + strings.Repeat("a", 9000) + "\r\n$9000\r\n",
		"long inline pre-auth":   strings.Repeat("a", 17<<10) + "\r\n",
	} {
		if _, err := ReadCommandLimits(bufio.NewReader(strings.NewReader(in)), PreAuthLimits); err != errProtocol {
			t.Errorf("%s: err=%v, want protocol error", name, err)
		}
	}
	if _, err := ReadCommandLimits(bufio.NewReader(strings.NewReader("*2000000\r\n")), AuthLimits); err != errProtocol {
		t.Errorf("2M args after auth: %v", err)
	}
	if _, err := ReadCommandLimits(bufio.NewReader(strings.NewReader("*1\r\n$20000000\r\n")), AuthLimits); err != errProtocol {
		t.Errorf("20 MB bulk after auth: %v", err)
	}
	args, err := ReadCommandLimits(bufio.NewReader(strings.NewReader("*3\r\n$4\r\nAUTH\r\n$4\r\nmac1\r\n$4\r\npass\r\n")), PreAuthLimits)
	if err != nil || len(args) != 3 {
		t.Fatalf("AUTH pre-auth: %q %v", args, err)
	}
	// A declared-but-unsent bulk allocates only what arrives.
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	ReadCommandLimits(bufio.NewReader(strings.NewReader("*1\r\n$16000000\r\nabc")), AuthLimits)
	runtime.ReadMemStats(&m1)
	if d := m1.TotalAlloc - m0.TotalAlloc; d > 1<<20 {
		t.Fatalf("declared 16 MB bulk with 3 bytes sent allocated %d bytes", d)
	}
}

func TestLoadPrincipalsRefusesLoosePermissions(t *testing.T) {
	h, _ := HashPassword("pw", 1, 64, 1)
	body := []byte(`{"principals":[{"name":"a","kind":"mac","argon2id":"` + h + `"}]}`)
	dir := t.TempDir()
	f := filepath.Join(dir, "principals.json")
	if err := os.WriteFile(f, body, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrincipals(f); err == nil {
		t.Fatal("0644 principals file accepted")
	}
	os.Chmod(f, 0o640)
	if _, err := LoadPrincipals(f); err == nil {
		t.Fatal("0640 principals file accepted")
	}
	os.Chmod(f, 0o600)
	if ps, err := LoadPrincipals(f); err != nil || ps.Len() != 1 {
		t.Fatalf("0600: %v", err)
	}
}

// An unknown username costs one argon2 evaluation, like a real one, so
// timing does not reveal which names exist.
func TestAuthUnknownUserBurnsAHash(t *testing.T) {
	a := NewAuthenticator(principalsForTest(t))
	before := a.verifications.Load()
	if _, r := a.Authenticate("10.0.0.9:1", "nobody", "x"); r != AuthBad {
		t.Fatalf("unknown user: %v", r)
	}
	if a.verifications.Load() != before+1 {
		t.Fatal("unknown user skipped the dummy argon2 evaluation")
	}
	d := a.dummy.Load()
	if d.memory != 64 || d.time != 1 {
		t.Fatalf("dummy params m=%d t=%d do not match the table's (m=64 t=1)", d.memory, d.time)
	}
}

// One principal's typos behind a shared IP (NAT, Tailscale relay) do not
// lock out another principal; unknown names share one global budget.
func TestAuthThrottlePerPrincipal(t *testing.T) {
	a := NewAuthenticator(principalsForTest(t))
	now := time.Unix(2_000_000, 0)
	a.Now = func() time.Time { return now }
	for i := 0; i < throttleFree+1; i++ {
		a.Authenticate("10.0.0.1:5", "mac2", "wrong")
	}
	if _, r := a.Authenticate("10.0.0.1:6", "mac2", "mac2-pass"); r != AuthThrottled {
		t.Fatalf("mac2 not locked: %v", r)
	}
	if _, r := a.Authenticate("10.0.0.1:7", "farm1", "farm1-pass"); r != AuthOK {
		t.Fatalf("farm1 behind the same IP: %v", r)
	}
	for i := 0; i < unknownFree; i++ {
		a.Authenticate("10.0.0."+itoa(uint64(i%200))+":1", "ghost"+itoa(uint64(i)), "x")
	}
	before := a.verifications.Load()
	if _, r := a.Authenticate("10.9.9.9:1", "ghostX", "x"); r != AuthThrottled {
		t.Fatalf("unknown-user budget not enforced: %v", r)
	}
	if a.verifications.Load() != before {
		t.Fatal("throttled unknown user still cost a hash")
	}
	if _, r := a.Authenticate("10.9.9.9:2", "mac1", "mac1-pass"); r != AuthOK {
		t.Fatalf("known user blocked by the unknown-user budget: %v", r)
	}
}

func itoa(n uint64) string { return strconv.FormatUint(n, 10) }

// Lane L3: juicefs drops a deleted file's delfiles entry with a bare ZREM
// even under --no-bgjob --max-deletes 0. A principal may drop exactly the
// entries it queued (unlink, or close of a file it unlinked while open).
func TestGuardOwnDelfilesDrop(t *testing.T) {
	g := NewGuard(NewMemSessionStore(), fakeChunks{})
	mac1 := Principal{Name: "mac1", Kind: KindMac}
	mac2 := Principal{Name: "mac2", Kind: KindMac}
	drop := []string{"ZREM", "delfiles", "9:4096"}
	if d, _, _ := guardRun(g, mac1, UnitBare, drop); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("drop of an entry nobody queued: %+v", d)
	}
	// doDeleteSustainedInode: ZADD delfiles + DEL i<ino> in one txn.
	d, _, eff := guardRun(g, mac1, UnitTxn, []string{"ZADD", "delfiles", "1", "9:4096"}, []string{"DEL", "i9"},
		[]string{"INCRBY", "usedSpace", "-4096"}, []string{"DECR", "totalInodes"})
	if d != nil {
		t.Fatalf("close of unlinked file refused: %+v", d)
	}
	g.Commit(eff)
	if d, _, _ := guardRun(g, mac2, UnitBare, drop); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("mac2 dropping mac1's entry: %+v", d)
	}
	if d, _, _ := guardRun(g, mac1, UnitBare, []string{"ZREM", "delfiles", "9:4096", "10:1"}); d == nil {
		t.Fatal("drop including an entry mac1 never queued admitted")
	}
	d, u, eff := guardRun(g, mac1, UnitBare, drop)
	if d != nil || len(u.Project) != 0 {
		t.Fatalf("own entry drop: %+v project=%d", d, len(u.Project))
	}
	g.Commit(eff)
	if d, _, _ := guardRun(g, mac1, UnitBare, drop); d == nil {
		t.Fatal("entry dropped twice")
	}
}
