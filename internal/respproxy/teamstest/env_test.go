package teamstest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/redis/go-redis/v9"
)

const testDB = 10

type cred struct{ name, kind, pass string }

// Synthetic principals; low-cost hashes keep the suite fast.
var creds = []cred{
	{"mac-john", "mac", "john-synthetic-pass"},
	{"mac-anon", "mac", "anon-synthetic-pass"},
	{"admin1", "admin", "admin-synthetic-pass"},
	{"bg1", "bg", "bg-synthetic-pass"},
	{"mgr1", "manager", "mgr-synthetic-pass"},
}

func passOf(name string) string {
	for _, c := range creds {
		if c.name == name {
			return c.pass
		}
	}
	return ""
}

func johnGranted(grants ...string) []respproxy.PersonEntry {
	return []respproxy.PersonEntry{{ID: "p-john", Name: "John", Access: respproxy.AccessGranted,
		Grants: grants, Devices: []string{"mac-john"}}}
}

// logf is quiet unless TEAMSTEST_VERBOSE is set: background goroutines of
// the proxy may log after a test ended, where t.Logf panics.
func logf(format string, args ...any) {
	if os.Getenv("TEAMSTEST_VERBOSE") != "" {
		log.Printf(format, args...)
	}
}

func redisAddr(t testing.TB) string {
	a := os.Getenv("JM_RESPPROXY_TEST_REDIS")
	if a == "" {
		t.Skip("JM_RESPPROXY_TEST_REDIS not set")
	}
	return a
}

// volume is a formatted JuiceFS volume in DB 10 with
//
//	/Clients/{A,B,C,D}, /Clients/A/g.txt, /Clients/B/{b.txt,h.txt}
type volume struct {
	t     testing.TB
	addr  string
	rdb   *redis.Client
	admin meta.Meta // straight to Redis: setup and ground truth
	ino   map[string]meta.Ino
	dir   string // principals.json, reload files, audit/
	// restrict: principals.json says restrict_unassigned.
	restrict bool
}

func metaConf() *meta.Config {
	conf := meta.DefaultConf()
	conf.NoBGJob, conf.MaxDeletes, conf.Retries = true, 0, 1
	return conf
}

func newVolume(t testing.TB) *volume {
	t.Helper()
	addr := redisAddr(t)
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: testDB})
	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.FlushDB(context.Background()); rdb.Close() })
	m := meta.NewClient(fmt.Sprintf("redis://%s/%d", addr, testDB), metaConf())
	if err := m.Init(&meta.Format{Name: "teams", UUID: "00000000-0000-4000-8000-0000000000ab", Storage: "file",
		Bucket: t.TempDir() + "/", BlockSize: 4096, TrashDays: 1}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Load(true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Shutdown() })
	v := &volume{t: t, addr: addr, rdb: rdb, admin: m, ino: map[string]meta.Ino{"/": meta.RootInode}, dir: t.TempDir()}
	for _, d := range []string{"/Clients", "/Clients/A", "/Clients/B", "/Clients/C", "/Clients/D"} {
		v.ino[d] = v.mustMkdir(m, filepath.Dir(d), filepath.Base(d))
	}
	for _, f := range []string{"/Clients/A/g.txt", "/Clients/B/b.txt", "/Clients/B/h.txt"} {
		var ino meta.Ino
		var attr meta.Attr
		if st := m.Create(meta.Background(), v.ino[filepath.Dir(f)], filepath.Base(f), 0o644, 0o022, 0, &ino, &attr); st != 0 {
			t.Fatalf("create %s: %v", f, st)
		}
		m.Close(meta.Background(), ino)
		v.ino[f] = ino
	}
	return v
}

func (v *volume) mustMkdir(m meta.Meta, parent, name string) meta.Ino {
	v.t.Helper()
	var ino meta.Ino
	var attr meta.Attr
	if st := m.Mkdir(meta.Background(), v.ino[parent], name, 0o755, 0o022, 0, &ino, &attr); st != 0 {
		v.t.Fatalf("mkdir %s/%s: %v", parent, name, st)
	}
	return ino
}

// exists looks name up in parent, straight in Redis.
func (v *volume) exists(parent meta.Ino, name string) bool {
	var ino meta.Ino
	var attr meta.Attr
	st := v.admin.Lookup(meta.Background(), parent, name, &ino, &attr, false)
	if st != 0 && st != syscall.ENOENT {
		v.t.Fatalf("lookup %s: %v", name, st)
	}
	return st == 0
}

var hashCache = map[string]string{}

func hashOf(t testing.TB, name string) string {
	if h, ok := hashCache[name]; ok {
		return h
	}
	h, err := respproxy.HashPassword(passOf(name), 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	hashCache[name] = h
	return h
}

// writePrincipals writes principals.json: version 1 when people is nil
// (and not restrict).
func (v *volume) writePrincipals(people []respproxy.PersonEntry) []byte {
	v.t.Helper()
	f := struct {
		Version            int                        `json:"version,omitempty"`
		Principals         []respproxy.PrincipalEntry `json:"principals"`
		People             []respproxy.PersonEntry    `json:"people,omitempty"`
		RestrictUnassigned bool                       `json:"restrict_unassigned,omitempty"`
	}{People: people, RestrictUnassigned: v.restrict}
	if people != nil || v.restrict {
		f.Version = 2
	}
	for _, c := range creds {
		f.Principals = append(f.Principals, respproxy.PrincipalEntry{Name: c.name, Kind: respproxy.Kind(c.kind),
			Host: c.name + "-host", Argon2id: hashOf(v.t, c.name)})
	}
	b, err := json.Marshal(f)
	if err != nil {
		v.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(v.dir, "principals.json"), b, 0o600); err != nil {
		v.t.Fatal(err)
	}
	return b
}

// env is a volume behind a running proxy with the lease engine and the
// audit log.
type env struct {
	*volume
	mode     respproxy.Mode
	proxy    *respproxy.Proxy
	reloader *respproxy.PrincipalReloader
	audit    *auditlog.Writer
	auditDir string
	paddr    string
	failSink atomic.Bool
	// healSink ends an injected failure from inside the next failing write,
	// so the write that succeeds starts after every refusal made before it
	// was set (see auditlog TestSinkFailureRefusesThenRecordsGap).
	healSink atomic.Bool
	// smartRoots: LeaseConfig.SmartRoots (none unless a test sets it).
	smartRoots []string
}

func newEnv(t testing.TB, mode respproxy.Mode, people []respproxy.PersonEntry, mut ...func(*respproxy.Config)) *env {
	t.Helper()
	e := &env{volume: newVolume(t), mode: mode}
	e.startProxy(people, mut...)
	return e
}

func (e *env) startProxy(people []respproxy.PersonEntry, mut ...func(*respproxy.Config)) {
	t := e.t
	t.Helper()
	e.writePrincipals(people)
	path := filepath.Join(e.dir, "principals.json")
	ps, err := respproxy.LoadPrincipals(path)
	if err != nil {
		t.Fatal(err)
	}
	if e.auditDir == "" {
		e.auditDir = filepath.Join(e.dir, "audit")
	}
	e.audit, err = auditlog.Open(e.auditDir, auditlog.Options{Logf: logf, RetryEvery: 20 * time.Millisecond,
		BeforeWrite: func() error {
			if e.failSink.Load() {
				if e.healSink.CompareAndSwap(true, false) {
					e.failSink.Store(false)
				}
				return errors.New("injected sink failure")
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	var eng *respproxy.LeaseEngine
	cfg := respproxy.Config{Upstream: e.addr, Principals: ps, Mode: e.mode, VolumeDB: testDB, Logf: logf,
		ModePoll: time.Hour, Audit: e.audit, AuditWindow: 200 * time.Millisecond,
		BeforeSideClose: func() {
			if eng != nil {
				eng.Close()
			}
		}}
	for _, m := range mut {
		m(&cfg)
	}
	p, err := respproxy.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	eng = respproxy.NewLeaseEngine(respproxy.LeaseConfig{Side: p.Side(), VolumeDB: testDB, Logf: logf,
		SweepEvery: time.Hour, FlushEvery: 50 * time.Millisecond, SmartRoots: e.smartRoots})
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}
	p.SetPolicy(eng)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go p.Serve(ln)
	e.proxy, e.paddr = p, ln.Addr().String()
	t.Cleanup(func() {
		p.Close()
		<-p.Done()
		e.audit.Close()
	})
	if e.reloader, err = respproxy.NewPrincipalReloader(path, p.Auth); err != nil {
		t.Fatal(err)
	}
}

// setPeople installs a new people section through the reload handshake,
// exactly as the Manager and the proxy's reloader do.
func (e *env) setPeople(people []respproxy.PersonEntry) {
	e.t.Helper()
	body := e.writePrincipals(people)
	sum := sha256.Sum256(body)
	nonce := make([]byte, 8)
	rand.Read(nonce)
	line := hex.EncodeToString(sum[:]) + " " + strconv.FormatInt(time.Now().UnixMilli(), 10) + " " + hex.EncodeToString(nonce)
	if err := os.WriteFile(filepath.Join(e.dir, "reload.request"), []byte(line+"\n"), 0o600); err != nil {
		e.t.Fatal(err)
	}
	if err := e.reloader.Tick(); err != nil {
		e.t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(e.dir, "reload.applied")); string(b) != line+"\n" {
		e.t.Fatalf("reload.applied = %q, want %q", b, line)
	}
}

// client is a JuiceFS metadata client through the proxy as principal name,
// with the Mac's flags (no background jobs, no deletes of its own).
func (e *env) client(name string) meta.Meta {
	e.t.Helper()
	m := meta.NewClient(fmt.Sprintf("redis://%s:%s@%s/%d", name, passOf(name), e.paddr, testDB), metaConf())
	if _, err := m.Load(true); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { m.Shutdown() })
	return m
}

// records syncs the log and returns every record matching f, oldest first.
func (e *env) records(f auditlog.Filter) []auditlog.Record {
	e.t.Helper()
	if err := e.audit.Sync(); err != nil {
		e.t.Fatal(err)
	}
	var out []auditlog.Record
	var before uint64
	for {
		p, err := auditlog.Query(e.auditDir, f, before, 1000)
		if err != nil {
			e.t.Fatal(err)
		}
		out = append(out, p.Records...) // newest first
		if p.Next == 0 {
			break
		}
		before = p.Next
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
