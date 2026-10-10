package nfs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeLeaseServer is a minimal RESP2 server that speaks the JM.LEASE wire of
// internal/respproxy/leasecmd.go (lane L2). It mirrors only what the Mac
// client uses; it is NOT the proxy.
type fakeLeaseServer struct {
	t  *testing.T
	ln net.Listener

	mu       sync.Mutex
	me       string // principal@host of the client under test
	projects map[string]*fakeLeaseProject
	calls    []string
	// unknown makes every JM.LEASE reply "unknown command" (plain Redis).
	unknown bool
}

type fakeLeaseProject struct {
	holder string
	epoch  uint64
	ino    int64
	// lastMine is the client's last epoch; a grant after a foreign epoch
	// reports foreign_since=1.
	lastMine uint64
}

func newFakeLeaseServer(t *testing.T) *fakeLeaseServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeLeaseServer{t: t, ln: ln, me: "mac-a@Mac-A", projects: map[string]*fakeLeaseProject{}}
	go f.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeLeaseServer) url() string { return "redis://" + f.ln.Addr().String() + "/1" }

func (f *fakeLeaseServer) project(p string) *fakeLeaseProject {
	pr := f.projects[p]
	if pr == nil {
		pr = &fakeLeaseProject{ino: int64(1000 + len(f.projects))}
		f.projects[p] = pr
	}
	return pr
}

// setHolder makes another principal hold p (a new epoch).
func (f *fakeLeaseServer) setHolder(p, holder string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pr := f.project(p)
	pr.epoch++
	pr.holder = holder
}

func (f *fakeLeaseServer) free(p string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.project(p).holder = ""
}

func (f *fakeLeaseServer) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeLeaseServer) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.conn(c)
	}
}

func (f *fakeLeaseServer) conn(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		args, err := readRESPArray(r)
		if err != nil {
			return
		}
		if _, err := io.WriteString(c, f.reply(args)); err != nil {
			return
		}
	}
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("not an array: %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		h, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		l, err := strconv.Atoi(strings.TrimRight(h, "\r\n")[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out = append(out, string(buf[:l]))
	}
	return out, nil
}

func respBulk(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }

func (f *fakeLeaseServer) reply(args []string) string {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "HELLO":
		return "-ERR unknown command 'HELLO'\r\n"
	case "AUTH", "SELECT", "CLIENT", "PING":
		return "+OK\r\n"
	case "JM.LEASE":
	default:
		return "-ERR unknown command '" + cmd + "'\r\n"
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, strings.Join(args[1:], " "))
	if f.unknown {
		return "-ERR unknown command 'JM.LEASE'\r\n"
	}
	sub := strings.ToUpper(args[1])
	proj := func(spec string) string { return strings.Trim(strings.TrimPrefix(spec, "path:"), "/") }
	switch sub {
	case "ACQUIRE":
		pr := f.project(proj(args[2]))
		status := "GRANTED"
		foreign := 0
		switch pr.holder {
		case f.me:
			status = "RENEWED"
		case "":
			if pr.lastMine > 0 && pr.epoch > pr.lastMine {
				foreign = 1
			}
			pr.epoch++
			pr.holder = f.me
			pr.lastMine = pr.epoch
		default:
			status = "HELD"
		}
		if status == "RENEWED" {
			pr.lastMine = pr.epoch
		}
		return fmt.Sprintf("*5\r\n+%s\r\n:%d\r\n%s:%d\r\n:%d\r\n", status, pr.epoch, respBulk(pr.holder), 0, foreign)
	case "HEARTBEAT":
		var b strings.Builder
		var recs []string
		for name, pr := range f.projects {
			if pr.holder == f.me {
				recs = append(recs, fmt.Sprintf("*4\r\n:%d\r\n:%d\r\n+held\r\n%s", pr.ino, pr.epoch, respBulk("auto")))
				continue
			}
			for _, a := range args[2:] {
				if proj(a) == name {
					recs = append(recs, fmt.Sprintf("*4\r\n:%d\r\n:%d\r\n+lost\r\n%s", pr.ino, pr.epoch, respBulk("")))
				}
			}
		}
		fmt.Fprintf(&b, "*%d\r\n", len(recs))
		for _, r := range recs {
			b.WriteString(r)
		}
		return b.String()
	case "RELEASE":
		pr := f.project(proj(args[2]))
		ep, _ := strconv.ParseUint(args[3], 10, 64)
		switch {
		case pr.holder != f.me:
			return fmt.Sprintf("*2\r\n+NOT_HOLDER\r\n:%d\r\n", pr.epoch)
		case pr.epoch != ep:
			return fmt.Sprintf("*2\r\n+STALE_EPOCH\r\n:%d\r\n", pr.epoch)
		}
		pr.holder = ""
		return fmt.Sprintf("*2\r\n+RELEASED\r\n:%d\r\n", pr.epoch)
	case "STATUS":
		pr := f.project(proj(args[2]))
		return fmt.Sprintf("*4\r\n%s:%d\r\n%s%s", respBulk("proj"), pr.ino, respBulk("holder"), respBulk(pr.holder))
	}
	return "-ERR unknown JM.LEASE subcommand\r\n"
}

func newTestLeaseClient(t *testing.T, f *fakeLeaseServer, spool *SpoolStore, now func() time.Time) *ProjectLeaseClient {
	t.Helper()
	c, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: f.url(), Spool: spool, Timeout: 2 * time.Second, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return c
}

func TestProjectOf(t *testing.T) {
	for in, want := range map[string]string{
		"Film Projects/Scott Adams/edit.prproj": "Film Projects/Scott Adams",
		"/Film Projects/Scott Adams/a/b/c.mov":  "Film Projects/Scott Adams",
		"Film Projects/loose.mov":               "",
		"top.mov":                               "",
		".juicemount/derivatives/x/y/z.png":     "",
		"Film Projects/Scott Adams":             "",
		"Clients/Vidéos Été/A001/clip.mov":      "Clients/Vidéos Été",
	} {
		if got := projectOf(in); got != want {
			t.Errorf("projectOf(%q)=%q want %q", in, got, want)
		}
	}
}

func TestProjectLeaseAcquireTracksEpochAndForeignSince(t *testing.T) {
	f := newFakeLeaseServer(t)
	now := time.Unix(1_800_000_000, 0)
	clock := func() time.Time { return now }
	c := newTestLeaseClient(t, f, nil, clock)
	const p = "Film/ProjA"
	dec, err := c.EnsureForDrain(p)
	if err != nil || !dec.held || dec.epoch != 1 || dec.foreignBefore != 0 {
		t.Fatalf("first grant dec=%+v err=%v", dec, err)
	}
	if got := c.EpochFor(p + "/x.mov"); got != 1 {
		t.Fatalf("EpochFor=%d want 1", got)
	}
	// Cached: no second round trip within the freshness window.
	before := len(f.callLog())
	if _, err := c.EnsureForDrain(p); err != nil || len(f.callLog()) != before {
		t.Fatalf("fresh lease re-acquired: calls %d -> %d err=%v", before, len(f.callLog()), err)
	}
	// Another Mac takes it (epoch 2), then frees it; our re-grant is foreign.
	f.setHolder(p, "mac-b@Edit-Bay-2")
	now = now.Add(2 * projectLeaseFresh)
	dec, err = c.EnsureForDrain(p)
	if err != nil || dec.held || dec.heldBy != "mac-b@Edit-Bay-2" {
		t.Fatalf("held-by-other dec=%+v err=%v", dec, err)
	}
	f.free(p)
	dec, err = c.EnsureForDrain(p)
	if err != nil || !dec.held || dec.epoch != 3 || dec.foreignBefore != 3 {
		t.Fatalf("foreign re-grant dec=%+v err=%v", dec, err)
	}
	if !dec.taints(1) || dec.taints(3) {
		t.Fatalf("taints: epoch1=%v epoch3=%v", dec.taints(1), dec.taints(3))
	}
	// A later renew keeps the boundary.
	now = now.Add(2 * projectLeaseFresh)
	dec, _ = c.EnsureForDrain(p)
	if dec.foreignBefore != 3 {
		t.Fatalf("renew lost the foreign boundary: %+v", dec)
	}
}

func TestProjectLeaseUnknownCommandMeansUnsupported(t *testing.T) {
	f := newFakeLeaseServer(t)
	f.unknown = true
	c := newTestLeaseClient(t, f, nil, nil)
	if _, err := c.EnsureForDrain("Film/ProjA"); err != errLeasesUnsupported {
		t.Fatalf("err=%v want errLeasesUnsupported", err)
	}
	n := len(f.callLog())
	if _, err := c.EnsureForDrain("Film/ProjA"); err != errLeasesUnsupported || len(f.callLog()) != n {
		t.Fatalf("unsupported server was asked again: %d -> %d", n, len(f.callLog()))
	}
	if s := c.Snapshot(); s.Supported {
		t.Fatal("snapshot reports leases supported against plain Redis")
	}
}

func TestProjectLeaseHeartbeatListsPendingProjectsAndDetectsLoss(t *testing.T) {
	spool, _ := newTestDrainer(t, DrainerConfig{})
	f := newFakeLeaseServer(t)
	now := time.Unix(1_800_000_000, 0)
	c := newTestLeaseClient(t, f, spool, func() time.Time { return now })
	writeSpoolEntry(t, spool, "Film/ProjA/a.mov", []byte("a"))
	writeSpoolEntry(t, spool, "Film/ProjB/sub/b.mov", []byte("b"))
	writeSpoolEntry(t, spool, "Film/loose.mov", []byte("c"))
	if _, err := c.EnsureForDrain("Film/ProjA"); err != nil {
		t.Fatal(err)
	}
	c.Tick(context.Background())
	var hb string
	for _, call := range f.callLog() {
		if strings.HasPrefix(call, "HEARTBEAT") {
			hb = call
		}
	}
	if hb != "HEARTBEAT path:Film/ProjA path:Film/ProjB" {
		t.Fatalf("heartbeat args %q", hb)
	}
	// The proxy drops our lease (expired/broken): the next heartbeat marks it
	// lost so the next drain re-acquires.
	f.setHolder("Film/ProjA", "mac-b@Edit-Bay-2")
	c.Tick(context.Background())
	if got := c.EpochFor("Film/ProjA/a.mov"); got != 0 {
		t.Fatalf("lost lease still reports epoch %d", got)
	}
}

func TestProjectLeaseReleaseDefersWhilePending(t *testing.T) {
	spool, _ := newTestDrainer(t, DrainerConfig{})
	f := newFakeLeaseServer(t)
	c := newTestLeaseClient(t, f, spool, nil)
	e := writeSpoolEntry(t, spool, "Film/ProjA/a.mov", []byte("a"))
	if _, err := c.EnsureForDrain("Film/ProjA"); err != nil {
		t.Fatal(err)
	}
	if st, err := c.Release("Film/ProjA"); err != nil || st != "deferred" {
		t.Fatalf("release with pending rows: %q %v", st, err)
	}
	f.mu.Lock()
	early := f.projects["Film/ProjA"].holder
	f.mu.Unlock()
	if early != f.me {
		t.Fatal("released while changes were pending")
	}
	if _, err := spool.Meta().MarkDone(e.ID()); err != nil {
		t.Fatal(err)
	}
	c.Tick(context.Background())
	f.mu.Lock()
	holder := f.projects["Film/ProjA"].holder
	f.mu.Unlock()
	if holder != "" {
		t.Fatalf("deferred release did not happen after uploads finished: holder=%q", holder)
	}
	// Nothing pending: Done editing releases immediately.
	if _, err := c.EnsureForDrain("Film/ProjB"); err != nil {
		t.Fatal(err)
	}
	if st, err := c.Release("Film/ProjB"); err != nil || st != "released" {
		t.Fatalf("release: %q %v", st, err)
	}
}

func TestProjectLeaseHTTPHandler(t *testing.T) {
	spool, _ := newTestDrainer(t, DrainerConfig{})
	f := newFakeLeaseServer(t)
	c := newTestLeaseClient(t, f, spool, nil)
	f.setHolder("Film/ProjA", "mac-b@Edit-Bay-2")
	if _, err := c.EnsureForDrain("Film/ProjA"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(c.HTTPHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/project-leases")
	if err != nil {
		t.Fatal(err)
	}
	var st ProjectLeaseStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(st.Projects) != 1 || st.Projects[0].State != "other" || st.Projects[0].HolderHost != "Edit-Bay-2" ||
		st.Projects[0].Name != "ProjA" {
		t.Fatalf("status %+v", st)
	}
	// Release needs POST + the control header.
	resp, err = http.Post(srv.URL+"/project-leases/release?project=Film/ProjA", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("release without control header: %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/project-leases/release?project=Film", nil)
	req.Header.Set("X-JM-Control", "1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("release of a non-project: %d", resp.StatusCode)
	}
}

func TestProjectLeaseURLSecretNeverInErrors(t *testing.T) {
	_, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: "redis://user:s3cret@:bad"})
	if err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("err=%v", err)
	}
}
