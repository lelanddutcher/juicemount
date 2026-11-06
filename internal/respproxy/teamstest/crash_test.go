package teamstest

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

// holdRelay is a TCP relay between the proxy and Redis that, once armed,
// holds the EXEC of any transaction naming marker: the proxy has decided,
// logged its intent and sent EXEC, and Redis never sees it.
type holdRelay struct {
	ln       net.Listener
	up       string
	marker   string
	mu       sync.Mutex
	armed    bool
	conns    []net.Conn
	held     chan struct{}
	heldOnce sync.Once
}

func newHoldRelay(t *testing.T, upstream, marker string) *holdRelay {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &holdRelay{ln: ln, up: upstream, marker: marker, held: make(chan struct{})}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go r.pipe(c)
		}
	}()
	t.Cleanup(r.close)
	return r
}

func (r *holdRelay) arm() { r.mu.Lock(); r.armed = true; r.mu.Unlock() }

func (r *holdRelay) close() {
	r.ln.Close()
	r.mu.Lock()
	for _, c := range r.conns {
		c.Close()
	}
	r.mu.Unlock()
}

func (r *holdRelay) pipe(c net.Conn) {
	u, err := net.Dial("tcp", r.up)
	if err != nil {
		c.Close()
		return
	}
	r.mu.Lock()
	r.conns = append(r.conns, c, u)
	r.mu.Unlock()
	go func() { io.Copy(c, u); c.Close() }()
	br, bw := bufio.NewReader(c), bufio.NewWriter(u)
	inTxn, marked := false, false
	for {
		args, err := respproxy.ReadCommand(br)
		if err != nil {
			u.Close()
			return
		}
		if len(args) == 0 {
			continue
		}
		switch strings.ToUpper(string(args[0])) {
		case "MULTI":
			inTxn, marked = true, false
		case "EXEC":
			r.mu.Lock()
			hold := r.armed && inTxn && marked
			r.mu.Unlock()
			if hold {
				r.heldOnce.Do(func() { close(r.held) })
				bw.Flush()
				io.Copy(io.Discard, br) // never forward; wait for the close
				u.Close()
				return
			}
			inTxn = false
		case "DISCARD":
			inTxn = false
		}
		if inTxn {
			for _, a := range args {
				marked = marked || string(a) == r.marker
			}
		}
		respproxy.WriteCommand(bw, args)
		if br.Buffered() == 0 {
			if bw.Flush() != nil {
				return
			}
		}
	}
}

// childConfig is what the crash child needs to run a proxy.
type childConfig struct {
	Upstream, Principals, AuditDir, AddrFile string
}

// TestCrashChild is the proxy process TestCrashBetweenIntentAndOutcome
// kills. It runs only as that test's child.
func TestCrashChild(t *testing.T) {
	raw := os.Getenv("TEAMSTEST_CRASH_CHILD")
	if raw == "" {
		t.Skip("child process of TestCrashBetweenIntentAndOutcome")
	}
	var cfg childConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	ps, err := respproxy.LoadPrincipals(cfg.Principals)
	if err != nil {
		t.Fatal(err)
	}
	w, err := auditlog.Open(cfg.AuditDir, auditlog.Options{Logf: logf})
	if err != nil {
		t.Fatal(err)
	}
	p, err := respproxy.New(respproxy.Config{Upstream: cfg.Upstream, Principals: ps, Mode: respproxy.ModePassthrough,
		VolumeDB: testDB, Logf: logf, ModePoll: time.Hour, Audit: w})
	if err != nil {
		t.Fatal(err)
	}
	eng := respproxy.NewLeaseEngine(respproxy.LeaseConfig{Side: p.Side(), VolumeDB: testDB, Logf: logf, SweepEvery: time.Hour})
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
	if err := os.WriteFile(cfg.AddrFile, []byte(ln.Addr().String()), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Second) // the parent kills this process long before
	t.Fatal("not killed")
}

// TestCrashBetweenIntentAndOutcome kills the proxy process after it logged
// the intent of John's mkdir and sent its EXEC, before Redis answered. The
// log keeps the intent with no outcome; the chain verifies, and a reopened
// log continues it.
func TestCrashBetweenIntentAndOutcome(t *testing.T) {
	v := newVolume(t)
	v.writePrincipals(johnGranted("/Clients/A"))
	relay := newHoldRelay(t, v.addr, "crashdir")
	auditDir := filepath.Join(v.dir, "audit")
	addrFile := filepath.Join(v.dir, "addr")
	cfg, _ := json.Marshal(childConfig{Upstream: relay.ln.Addr().String(), Principals: filepath.Join(v.dir, "principals.json"),
		AuditDir: auditDir, AddrFile: addrFile})
	child := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1")
	child.Env = append(os.Environ(), "TEAMSTEST_CRASH_CHILD="+string(cfg))
	child.Stdout, child.Stderr = os.Stderr, os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			child.Process.Kill()
			child.Wait()
		}
	}()
	var paddr string
	for deadline := time.Now().Add(30 * time.Second); paddr == "" && time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		b, _ := os.ReadFile(addrFile)
		paddr = string(b)
	}
	if paddr == "" {
		t.Fatal("the child proxy did not start")
	}
	e := &env{volume: v, paddr: paddr}
	john := e.client("mac-john")
	ctx := meta.Background()
	var ino meta.Ino
	var attr meta.Attr
	A := v.ino["/Clients/A"]
	if st := john.Mkdir(ctx, A, "before", 0o755, 0o022, 0, &ino, &attr); st != 0 {
		t.Fatalf("mkdir before the crash: %v", st)
	}

	relay.arm()
	go john.Mkdir(ctx, A, "crashdir", 0o755, 0o022, 0, &ino, &attr) // never answered
	select {
	case <-relay.held:
	case <-time.After(10 * time.Second):
		t.Fatal("the mkdir's EXEC never reached the relay")
	}
	// The intent is fsynced in the background; wait until it is on disk.
	var intent *auditlog.Record
	for deadline := time.Now().Add(10 * time.Second); intent == nil && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		p, err := auditlog.Query(auditDir, auditlog.Filter{Person: "p-john", Types: []string{auditlog.TypeIntent}}, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		for i, r := range p.Records {
			for _, o := range r.Ops {
				if o.Name == "crashdir" {
					intent = &p.Records[i]
				}
			}
		}
	}
	if intent == nil {
		t.Fatal("no intent for the held mkdir on disk")
	}
	child.Process.Kill()
	child.Wait()
	waited = true
	relay.close()

	if v.exists(A, "crashdir") {
		t.Fatal("the held EXEC reached Redis")
	}
	outs, err := auditlog.Query(auditDir, auditlog.Filter{Types: []string{auditlog.TypeOutcome}}, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	committedBefore := false
	for _, r := range outs.Records {
		if r.Unit == intent.Seq {
			t.Fatalf("an outcome for the crashed unit: %+v", r)
		}
		committedBefore = committedBefore || r.Outcome == auditlog.OutcomeCommitted
	}
	if !committedBefore {
		t.Fatal("the mkdir before the crash has no committed outcome")
	}
	if rep, err := auditlog.Verify(auditDir); err != nil || !rep.OK {
		t.Fatalf("verify after the crash: %+v %v", rep, err)
	}
	w, err := auditlog.Open(auditDir, auditlog.Options{Logf: logf})
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	rep, err := auditlog.Verify(auditDir)
	if err != nil || !rep.OK || rep.LastSeq <= intent.Seq {
		t.Fatalf("verify after reopening: %+v %v", rep, err)
	}
}
