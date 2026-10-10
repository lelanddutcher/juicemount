package manager

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingSignaler struct {
	mu   sync.Mutex
	shas []string
}

func (r *recordingSignaler) Reload(sha string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shas = append(r.shas, sha)
	return nil
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// fakeLeaseClient stands in for the proxy's JM.LEASE surface (L2) so the
// manager's behaviour is testable before that lane merges.
type fakeLeaseClient struct {
	mu       sync.Mutex
	held     map[string]string // proj -> holder@host of ANOTHER principal
	absent   map[string]bool
	acquired []string
	released []string
	breaks   []string
	records  []LeaseRecord
	mode     string
	fail     error // every Acquire fails with this (lease server trouble)
	calls    int
	heldFrom int // Acquire calls numbered >= heldFrom (1-based) answer HELD
}

func (f *fakeLeaseClient) Acquire(_ context.Context, proj, mode string, ttl time.Duration) (LeaseGrant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail != nil {
		return LeaseGrant{}, f.fail
	}
	if f.heldFrom > 0 && f.calls >= f.heldFrom {
		return LeaseGrant{Status: "HELD", Epoch: 9, Holder: "thief@Other"}, nil
	}
	if f.absent[proj] {
		return LeaseGrant{}, ErrProjectAbsent
	}
	if h, ok := f.held[proj]; ok {
		return LeaseGrant{Status: "HELD", Epoch: 7, Holder: h}, nil
	}
	f.acquired = append(f.acquired, proj+" "+mode)
	return LeaseGrant{Status: "GRANTED", Epoch: 8, Holder: "manager@nas"}, nil
}

func (f *fakeLeaseClient) Release(_ context.Context, proj string, epoch int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, proj)
	return "RELEASED", nil
}

func (f *fakeLeaseClient) Status(context.Context, string) ([]LeaseRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]LeaseRecord(nil), f.records...), nil
}

func (f *fakeLeaseClient) Break(_ context.Context, proj string) (BreakResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.breaks = append(f.breaks, proj)
	return BreakResult{Status: "BROKEN", Epoch: 9, PreviousHolder: "edit-bay-2@Edit-Bay-2"}, nil
}

func (f *fakeLeaseClient) GetMode(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mode, nil
}

func (f *fakeLeaseClient) SetMode(_ context.Context, m string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = m
	return nil
}

// fakeJuicefs writes a marker when executed, so a test can prove the sync
// subprocess never started.
func fakeJuicefs(t *testing.T) (bin, marker string) {
	t.Helper()
	dir := t.TempDir()
	marker = filepath.Join(dir, "juicefs-ran")
	bin = filepath.Join(dir, "juicefs")
	script := "#!/bin/sh\ntouch '" + marker + "'\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, marker
}

// TestH8BManagerSyncRefusedOnLeasedProject: the manager takes a checkout
// lease on every project a sync or trash restore writes into BEFORE it
// writes anything. A project another principal holds refuses the whole
// operation: no juicefs sync process starts, no file moves.
func TestH8BManagerSyncRefusedOnLeasedProject(t *testing.T) {
	t.Run("sync into a held project", func(t *testing.T) {
		bin, marker := fakeJuicefs(t)
		fake := &fakeLeaseClient{held: map[string]string{"path:/data/Scott Adams": "edit-bay-2@Edit-Bay-2"}}
		spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: "redis://manager@respproxy:6379/1", VolName: "zpool",
			Leases: NewProjectLeaser(fake)}
		progress := make(chan ProgressEvent, 64)
		err := RunSync(context.Background(), bin, spec, t.TempDir(), "/jfs/data/Scott Adams/ingest",
			DefaultSyncOptions(), nil, progress)
		var le *ProjectLeasedError
		if !errors.As(err, &le) {
			t.Fatalf("RunSync into a held project: err = %v, want ProjectLeasedError", err)
		}
		if le.Project != "path:/data/Scott Adams" || !strings.Contains(le.Holder, "edit-bay-2") {
			t.Fatalf("refusal names %+v, want the project and its holder", le)
		}
		if _, err := os.Stat(marker); err == nil {
			t.Fatal("juicefs sync ran although the project is leased by another principal")
		}
	})

	t.Run("sync above project depth checks every project it would write", func(t *testing.T) {
		bin, marker := fakeJuicefs(t)
		src := t.TempDir()
		for _, d := range []string{"Free Project", "Scott Adams"} {
			if err := os.MkdirAll(filepath.Join(src, d), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		fake := &fakeLeaseClient{held: map[string]string{"path:/data/Scott Adams": "edit-bay-2@Edit-Bay-2"}}
		spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: "redis://m@p:6379/1", VolName: "zpool", Leases: NewProjectLeaser(fake)}
		err := RunSync(context.Background(), bin, spec, src, "/jfs/data", DefaultSyncOptions(), nil, make(chan ProgressEvent, 64))
		var le *ProjectLeasedError
		if !errors.As(err, &le) {
			t.Fatalf("err = %v, want ProjectLeasedError for the held child project", err)
		}
		if _, err := os.Stat(marker); err == nil {
			t.Fatal("juicefs sync ran")
		}
		// All-or-none: the free project's checkout was given back.
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if len(fake.acquired) != len(fake.released) {
			t.Fatalf("partial checkouts leaked: acquired %v released %v", fake.acquired, fake.released)
		}
	})

	t.Run("sync into a free project checks out, runs, releases", func(t *testing.T) {
		bin, marker := fakeJuicefs(t)
		fake := &fakeLeaseClient{held: map[string]string{}}
		spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: "redis://m@p:6379/1", VolName: "zpool", Leases: NewProjectLeaser(fake)}
		_ = RunSync(context.Background(), bin, spec, t.TempDir(), "/jfs/data/Free/x", DefaultSyncOptions(), nil, make(chan ProgressEvent, 64))
		if _, err := os.Stat(marker); err != nil {
			t.Fatal("juicefs sync did not run for a free project")
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if len(fake.acquired) != 1 || fake.acquired[0] != "path:/data/Free checkout" {
			t.Fatalf("acquired %v, want one checkout of path:/data/Free", fake.acquired)
		}
		if len(fake.released) != 1 {
			t.Fatalf("released %v, want the checkout given back", fake.released)
		}
	})

	t.Run("trash restore into a held project", func(t *testing.T) {
		fm := t.TempDir()
		bucket := filepath.Join(fm, ".trash", "2026-11-03-09")
		if err := os.MkdirAll(bucket, 0o755); err != nil {
			t.Fatal(err)
		}
		entry := filepath.Join(bucket, "1-2-clip.mov")
		if err := os.WriteFile(entry, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		fake := &fakeLeaseClient{held: map[string]string{"path:/data/Scott Adams": "edit-bay-2@Edit-Bay-2"}}
		mgr := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: fm})
		mgr.SetProjectLeaser(NewProjectLeaser(fake))
		a := &API{jobs: mgr, fuseMount: fm, destMount: "/jfs"}
		rr := doJSONHandler(t, a.handleTrashRestore, map[string]string{
			"path": "/jfs/.trash/2026-11-03-09/1-2-clip.mov", "target_path": "/jfs/data/Scott Adams/clip.mov"})
		if rr.Code != http.StatusConflict {
			t.Fatalf("restore into held project: %d %s, want 409", rr.Code, rr.Body)
		}
		if !strings.Contains(rr.Body.String(), "edit-bay-2") {
			t.Fatalf("refusal does not name the holder: %s", rr.Body)
		}
		if _, err := os.Stat(entry); err != nil {
			t.Fatal("trash entry moved although the project is leased")
		}
	})
}

// TestH8BLeaseUnsupportedProceeds: before the lease engine (L2) is
// deployed, the proxy answers JM.LEASE with "unknown command". Sync must
// still run then (passthrough), not wedge every migration.
func TestH8BLeaseUnsupportedProceeds(t *testing.T) {
	bin, marker := fakeJuicefs(t)
	spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: "redis://m@p:6379/1", VolName: "zpool",
		Leases: NewProjectLeaser(&unsupportedLeases{})}
	_ = RunSync(context.Background(), bin, spec, t.TempDir(), "/jfs/data/P/x", DefaultSyncOptions(), nil, make(chan ProgressEvent, 64))
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("sync did not run when the proxy has no lease service")
	}
}

type unsupportedLeases struct{ fakeLeaseClient }

func (*unsupportedLeases) Acquire(context.Context, string, string, time.Duration) (LeaseGrant, error) {
	return LeaseGrant{}, ErrLeasesUnsupported
}

func TestH8BProjectsForSync(t *testing.T) {
	src := t.TempDir()
	_ = os.MkdirAll(filepath.Join(src, "B"), 0o755)
	_ = os.MkdirAll(filepath.Join(src, "A"), 0o755)
	_ = os.WriteFile(filepath.Join(src, "loose.txt"), nil, 0o644)
	cases := []struct {
		dest     string
		preserve bool
		want     []string
	}{
		{"/jfs/data/P/sub/deeper", true, []string{"path:/data/P"}},
		{"/jfs/data/P", true, []string{"path:/data/P"}},
		{"/jfs/data", true, []string{"path:/data/A", "path:/data/B"}},
		{"/jfs/data", false, []string{"path:/data/" + filepath.Base(src)}},
		{"/external/backup", true, nil},
	}
	for _, c := range cases {
		got, err := projectsForSync(src, c.dest, c.preserve)
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)
		if strings.Join(got, "|") != strings.Join(c.want, "|") {
			t.Errorf("projectsForSync(%q, preserve=%v) = %v, want %v", c.dest, c.preserve, got, c.want)
		}
	}
}

func TestH8BLeasesAdminBreakAndList(t *testing.T) {
	fake := &fakeLeaseClient{records: []LeaseRecord{{Proj: 42, State: "held", Holder: "edit-bay-2", Host: "Edit-Bay-2", Label: "/data/Scott Adams", Mode: "auto"}}}
	a := &leasesAdmin{client: fake}
	rr := doJSONHandler(t, a.handleLeases, nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "Scott Adams") {
		t.Fatalf("list: %d %s", rr.Code, rr.Body)
	}
	rr = doJSONHandler(t, a.handleLeaseBreak, map[string]string{"project": "ino:42"})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("break without confirm: %d, want 400", rr.Code)
	}
	rr = doJSONHandler(t, a.handleLeaseBreak, map[string]string{"project": "ino:42", "confirm": "ino:42"})
	if rr.Code != http.StatusOK || len(fake.breaks) != 1 || fake.breaks[0] != "ino:42" {
		t.Fatalf("break: %d %s breaks=%v", rr.Code, rr.Body, fake.breaks)
	}
}

// fakeRESP is a minimal RESP2 server speaking the L2 JM.LEASE wire format
// (internal/respproxy/leasecmd.go on feat/h8b-leases) so the real client's
// parsing is exercised without the proxy.
func fakeRESP(t *testing.T, handle func(args []string) string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						return
					}
					n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
					args := make([]string, n)
					for i := 0; i < n; i++ {
						l, _ := br.ReadString('\n')
						sz, _ := strconv.Atoi(strings.TrimSpace(l[1:]))
						buf := make([]byte, sz+2)
						if _, err := io.ReadFull(br, buf); err != nil {
							return
						}
						args[i] = string(buf[:sz])
					}
					var reply string
					switch strings.ToUpper(args[0]) {
					case "AUTH", "SELECT":
						reply = "+OK\r\n"
					case "PING":
						reply = "+PONG\r\n"
					default:
						reply = handle(args)
					}
					if _, err := c.Write([]byte(reply)); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

func bulk(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }

func TestH8BRedisLeaseClientWireFormat(t *testing.T) {
	var mu sync.Mutex
	var seen [][]string
	addr := fakeRESP(t, func(a []string) string {
		mu.Lock()
		seen = append(seen, a)
		mu.Unlock()
		if strings.ToUpper(a[0]) == "HGET" {
			return bulk("observe")
		}
		if strings.ToUpper(a[0]) == "HSET" {
			return ":0\r\n"
		}
		switch strings.ToUpper(a[1]) {
		case "ACQUIRE":
			if a[2] == "path:/data/Missing" {
				return "-ERR path:/data/Missing: no such directory \"Missing\"\r\n"
			}
			if a[2] == "path:/data/Held" {
				return "*5\r\n+HELD\r\n:7\r\n" + bulk("edit-bay-2@Edit-Bay-2") + ":1700000000000\r\n:0\r\n"
			}
			return "*5\r\n+GRANTED\r\n:8\r\n" + bulk("manager@nas") + ":1700000000000\r\n:1\r\n"
		case "RELEASE":
			return "*2\r\n+RELEASED\r\n:9\r\n"
		case "STATUS":
			rec := "*30\r\n" + bulk("proj") + ":42\r\n" + bulk("epoch") + ":3\r\n" + bulk("state") + bulk("held") +
				bulk("holder") + bulk("edit-bay-2") + bulk("host") + bulk("Edit-Bay-2") + bulk("kind") + bulk("mac") +
				bulk("mode") + bulk("auto") + bulk("granted_ms") + ":1\r\n" + bulk("last_write_ms") + ":2\r\n" +
				bulk("last_hb_ms") + ":3\r\n" + bulk("checkout_until_ms") + ":0\r\n" + bulk("expires_ms") + ":4\r\n" +
				bulk("pending") + ":1\r\n" + bulk("label") + bulk("/data/Scott Adams") + bulk("held") + ":1\r\n"
			return "*1\r\n" + rec
		case "BREAK":
			return "*3\r\n+BROKEN\r\n:4\r\n" + bulk("edit-bay-2@Edit-Bay-2")
		}
		return "-ERR unknown command 'X'\r\n"
	})
	c, err := NewRedisLeaseClientAs("redis://"+addr+"/1", "admin", "pw-not-a-secret")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	g, err := c.Acquire(ctx, "path:/data/Free", "checkout", time.Hour)
	if err != nil || g.Status != "GRANTED" || g.Epoch != 8 || g.Holder != "manager@nas" || !g.ForeignSince {
		t.Fatalf("acquire = %+v, %v", g, err)
	}
	if g, err := c.Acquire(ctx, "path:/data/Held", "checkout", time.Hour); err != nil || g.Status != "HELD" || g.Holder != "edit-bay-2@Edit-Bay-2" {
		t.Fatalf("held acquire = %+v, %v", g, err)
	}
	if _, err := c.Acquire(ctx, "path:/data/Missing", "checkout", time.Hour); !errors.Is(err, ErrProjectAbsent) {
		t.Fatalf("missing project err = %v, want ErrProjectAbsent", err)
	}
	if st, err := c.Release(ctx, "path:/data/Free", 8); err != nil || st != "RELEASED" {
		t.Fatalf("release = %q, %v", st, err)
	}
	recs, err := c.Status(ctx, "")
	if err != nil || len(recs) != 1 || recs[0].Proj != 42 || recs[0].Label != "/data/Scott Adams" || !recs[0].Pending || recs[0].Holder != "edit-bay-2" {
		t.Fatalf("status = %+v, %v", recs, err)
	}
	if b, err := c.Break(ctx, "ino:42"); err != nil || b.Status != "BROKEN" || b.PreviousHolder != "edit-bay-2@Edit-Bay-2" {
		t.Fatalf("break = %+v, %v", b, err)
	}
	if m, err := c.GetMode(ctx); err != nil || m != "observe" {
		t.Fatalf("mode = %q, %v", m, err)
	}
	if err := c.SetMode(ctx, "enforce"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	var acquireTTL, hset []string
	for _, a := range seen {
		if len(a) > 1 && a[1] == "ACQUIRE" && a[2] == "path:/data/Free" {
			acquireTTL = a
		}
		if a[0] == "HSET" || a[0] == "hset" {
			hset = a
		}
	}
	if strings.Join(acquireTTL, " ") != "JM.LEASE ACQUIRE path:/data/Free checkout 3600" {
		t.Fatalf("acquire wire = %q", acquireTTL)
	}
	if strings.Join(hset, " ") != "hset jmlease:v1:cfg mode enforce" && strings.Join(hset, " ") != "HSET jmlease:v1:cfg mode enforce" {
		t.Fatalf("mode write wire = %q", hset)
	}

	// A proxy without the lease engine (L1) or plain Redis.
	plain := fakeRESP(t, func(a []string) string {
		return "-ERR unknown command 'JM.LEASE' (no lease service in this proxy)\r\n"
	})
	pc, _ := NewRedisLeaseClient("redis://"+plain+"/1", "")
	if _, err := pc.Acquire(ctx, "path:/data/P", "checkout", time.Hour); !errors.Is(err, ErrLeasesUnsupported) {
		t.Fatalf("unknown command err = %v, want ErrLeasesUnsupported", err)
	}
}

// TestH8BCheckoutFailsClosed: when the lease answer is unknown (server
// unreachable, or the sync source cannot be listed to find its projects),
// nothing is written; only an explicit "no lease service" proceeds.
func TestH8BCheckoutFailsClosed(t *testing.T) {
	t.Run("lease server unreachable", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		ln.Close() // nothing listens here now
		c, err := NewRedisLeaseClient("redis://manager:pw-not-a-secret@"+addr+"/1", "")
		if err != nil {
			t.Fatal(err)
		}
		bin, marker := fakeJuicefs(t)
		spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: "redis://m@p:6379/1", VolName: "zpool", Leases: NewProjectLeaser(c)}
		err = RunSync(context.Background(), bin, spec, t.TempDir(), "/jfs/data/P/x", DefaultSyncOptions(), nil, make(chan ProgressEvent, 64))
		if err == nil {
			t.Fatal("sync ran although the lease server was unreachable")
		}
		if _, serr := os.Stat(marker); serr == nil {
			t.Fatal("juicefs sync started without a checkout")
		}
	})
	t.Run("unreadable source above project depth", func(t *testing.T) {
		bin, marker := fakeJuicefs(t)
		fake := &fakeLeaseClient{held: map[string]string{}}
		spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: "redis://m@p:6379/1", VolName: "zpool", Leases: NewProjectLeaser(fake)}
		missing := filepath.Join(t.TempDir(), "gone")
		if err := RunSync(context.Background(), bin, spec, missing, "/jfs/data", DefaultSyncOptions(), nil, make(chan ProgressEvent, 64)); err == nil {
			t.Fatal("sync into /jfs/data with an unlistable source ran with no checkout")
		}
		if _, err := os.Stat(marker); err == nil {
			t.Fatal("juicefs sync started")
		}
	})
	t.Run("trash restore answers 503 when the lease cannot be checked", func(t *testing.T) {
		fm := t.TempDir()
		bucket := filepath.Join(fm, ".trash", "2026-11-03-09")
		_ = os.MkdirAll(bucket, 0o755)
		entry := filepath.Join(bucket, "1-2-clip.mov")
		_ = os.WriteFile(entry, []byte("x"), 0o644)
		mgr := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: fm})
		mgr.SetProjectLeaser(NewProjectLeaser(&fakeLeaseClient{fail: errors.New("dial tcp 10.0.0.1:30179: connect: connection refused")}))
		a := &API{jobs: mgr, fuseMount: fm, destMount: "/jfs"}
		rr := doJSONHandler(t, a.handleTrashRestore, map[string]string{
			"path": "/jfs/.trash/2026-11-03-09/1-2-clip.mov", "target_path": "/jfs/data/P/clip.mov"})
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("restore with the lease server down: %d %s, want 503", rr.Code, rr.Body)
		}
		if _, err := os.Stat(entry); err != nil {
			t.Fatal("trash entry moved although the lease could not be checked")
		}
	})
}

// TestH8BCheckoutLossStopsTheWrite: a checkout lost during a long sync
// (an admin broke it and someone else took the project) closes the lost
// channel, and RunSync's context is cancelled so juicefs stops.
func TestH8BCheckoutLossStopsTheWrite(t *testing.T) {
	fake := &fakeLeaseClient{held: map[string]string{}, heldFrom: 2} // granted, then HELD on renewal
	l := &checkoutLeaser{client: fake, ttl: 60 * time.Millisecond}
	release, lost, err := l.Checkout(context.Background(), []string{"path:/data/P"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	select {
	case <-lost:
	case <-time.After(3 * time.Second):
		t.Fatal("renewal saw HELD but the lost signal never fired")
	}

	fake2 := &fakeLeaseClient{held: map[string]string{}, heldFrom: 2}
	spec := RunSyncSpec{Leases: &checkoutLeaser{client: fake2, ttl: 60 * time.Millisecond}}
	ctx, rel, err := checkoutForSync(context.Background(), spec, t.TempDir(), "/jfs/data/P/x", DefaultSyncOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the sync context was not cancelled when the checkout was lost")
	}
}
