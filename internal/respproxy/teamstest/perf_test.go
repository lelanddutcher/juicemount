package teamstest

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

// TestAuditOverhead measures proxy write latency on a burst of small writes
// (creates, and 4-byte contents writes) through three proxies on one
// volume: no people and no audit log (as before Teams), grants in force
// without the log, and grants with the log; the operations alternate
// between them so machine noise hits all alike. It only reports: run it
// with JM_TEAMS_PERF=1.
func TestAuditOverhead(t *testing.T) {
	if os.Getenv("JM_TEAMS_PERF") != "1" {
		t.Skip("set JM_TEAMS_PERF=1 to measure")
	}
	const n = 3000
	v := newVolume(t)
	base := &env{volume: v, mode: respproxy.ModePassthrough, auditDir: filepath.Join(v.dir, "audit-base")}
	base.startProxy(nil, func(c *respproxy.Config) { c.Audit = nil })
	off := &env{volume: v, mode: respproxy.ModePassthrough, auditDir: filepath.Join(v.dir, "audit-off")}
	off.startProxy(johnGranted("/Clients"), func(c *respproxy.Config) { c.Audit = nil })
	on := &env{volume: v, mode: respproxy.ModePassthrough, auditDir: filepath.Join(v.dir, "audit-on")}
	on.startProxy(johnGranted("/Clients"))
	ctx := meta.Background()
	type side struct {
		name          string
		m             meta.Meta
		dir           meta.Ino
		file          meta.Ino
		create, write []time.Duration
	}
	sides := []*side{{name: "no grants, no audit", m: base.client("mac-john"), dir: v.ino["/Clients/C"]},
		{name: "grants, no audit   ", m: off.client("mac-john"), dir: v.ino["/Clients/A"]},
		{name: "grants + audit     ", m: on.client("mac-john"), dir: v.ino["/Clients/B"]}}
	var attr meta.Attr
	for i := 0; i < n; i++ {
		for _, s := range sides {
			t0 := time.Now()
			if st := s.m.Create(ctx, s.dir, fmt.Sprintf("f%05d", i), 0o644, 0o022, 0, &s.file, &attr); st != 0 {
				t.Fatal(st)
			}
			s.create = append(s.create, time.Since(t0))
			s.m.Close(ctx, s.file)
		}
	}
	for i := 0; i < n; i++ {
		for _, s := range sides {
			var sid uint64
			if st := s.m.NewSlice(ctx, &sid); st != 0 {
				t.Fatal(st)
			}
			t0 := time.Now()
			if st := s.m.Write(ctx, s.file, 0, uint32(i*4), meta.Slice{Id: sid, Size: 4, Len: 4}, time.Now()); st != 0 {
				t.Fatal(st)
			}
			s.write = append(s.write, time.Since(t0))
		}
	}
	stats := func(d []time.Duration) string {
		s := append([]time.Duration(nil), d...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		var sum time.Duration
		for _, x := range s {
			sum += x
		}
		us := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e3 }
		return fmt.Sprintf("mean %6.1fµs p50 %6.1fµs p90 %6.1fµs p99 %7.1fµs", us(sum)/float64(len(s)),
			us(s[len(s)/2]), us(s[len(s)*9/10]), us(s[len(s)*99/100]))
	}
	for _, s := range sides {
		t.Logf("%s create: %s", s.name, stats(s.create))
		t.Logf("%s write:  %s", s.name, stats(s.write))
	}
	// Concurrent creates: with grants in force every Mac intent waits for
	// its fsync; group commit lets concurrent connections share one.
	const workers, each = 8, 200
	for _, sd := range []struct {
		name string
		e    *env
		dir  meta.Ino
	}{{"grants, no audit   ", off, v.ino["/Clients/A"]}, {"grants + audit     ", on, v.ino["/Clients/B"]}} {
		var mu sync.Mutex
		var lat []time.Duration
		var wg sync.WaitGroup
		start := time.Now()
		for w := 0; w < workers; w++ {
			m := sd.e.client("mac-john")
			// One directory per worker: the latency of the log, not of
			// juicefs retrying conflicting transactions on one directory.
			wdir := v.mustMkdir(v.admin, "/Clients/"+map[meta.Ino]string{v.ino["/Clients/A"]: "A", v.ino["/Clients/B"]: "B"}[sd.dir], fmt.Sprintf("w%d", w))
			wg.Add(1)
			go func(w int, wdir meta.Ino) {
				defer wg.Done()
				var ino meta.Ino
				var a meta.Attr
				for i := 0; i < each; i++ {
					t0 := time.Now()
					if st := m.Create(ctx, wdir, fmt.Sprintf("c%d-%05d", w, i), 0o644, 0o022, 0, &ino, &a); st != 0 {
						t.Error(st)
						return
					}
					d := time.Since(t0)
					m.Close(ctx, ino)
					mu.Lock()
					lat = append(lat, d)
					mu.Unlock()
				}
			}(w, wdir)
		}
		wg.Wait()
		t.Logf("%s %d concurrent creates: %s; %.0f creates/s", sd.name, workers, stats(lat),
			float64(workers*each)/time.Since(start).Seconds())
	}
	if err := on.audit.Sync(); err != nil {
		t.Fatal(err)
	}
	t.Logf("audit errors: %d", on.proxy.AuditErrors())
}
