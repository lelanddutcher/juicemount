package main

// roam: what a running session does while the network changes under it.
//
// experiments/roam-lab.sh starts this probe, changes the network (a new
// client address, a link drop, a path change) and stops it. Every worker
// logs one JSON line per operation to -out, with the wall-clock time in
// milliseconds, so the script can line the timeline up with its own event
// marks. The workers:
//
//	read    a sequential read of one cold file over NFS, 1 MiB per line,
//	        paced at -read-mibs like a player; an error reopens the file
//	        and resumes at the same offset
//	write   one new 64 KiB file per -write-every over NFS (every 10th is
//	        8 MiB), retried until it succeeds; each success goes to
//	        <out>.manifest with its SHA-256
//	meta    an NFS LOOKUP of the run folder and a READDIRPLUS of it
//	fuse    a stat of an absent name straight on the JuiceFS mount (each
//	        one misses the entry cache, so it asks Redis)
//	reach   the app's reachability monitor (health.Reachability) on the
//	        metadata address, with the app's failure threshold
//	spool   jm5's /spool and /project-leases, once per second
//
// An operation that is still running at the end is logged with
// "stuck": true and how long it has been running.
//
// `drainprobe verify` then checks the manifest against the JuiceFS mount:
// every acknowledged file is there with the same bytes, once.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/health"
)

type roamLog struct {
	mu sync.Mutex
	f  *os.File
}

func (l *roamLog) put(v map[string]any) {
	v["t"] = time.Now().UnixMilli()
	b, _ := json.Marshal(v)
	l.mu.Lock()
	_, _ = l.f.Write(append(b, '\n'))
	l.mu.Unlock()
}

// opClock remembers when the running operation of one worker started, so a
// worker that never returns is still reported.
type opClock struct {
	name  string
	start atomic.Int64 // unix ms; 0 = idle
	op    atomic.Value // string
}

func (c *opClock) begin(op string) { c.op.Store(op); c.start.Store(time.Now().UnixMilli()) }
func (c *opClock) end()            { c.start.Store(0) }

func roamContent(i int) []byte {
	n := 64 << 10
	if i%10 == 9 {
		n = 8 << 20
	}
	b := make([]byte, n)
	rand.New(rand.NewSource(int64(i) + 1)).Read(b)
	return b
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}

func roam(args []string) error {
	fs := flag.NewFlagSet("roam", flag.ExitOnError)
	addr := fs.String("nfs", "127.0.0.1:21049", "NFS server address")
	export := fs.String("export", "", "export path")
	dir := fs.String("dir", "", "run folder, relative to the export root (created)")
	readFile := fs.String("read", "", "file to read sequentially, relative to the export root")
	readRate := fs.Float64("read-mibs", 0, "pace the read at this many MiB/s, as a player does (0 = as fast as it goes)")
	fuse := fs.String("fuse", "/jfs", "the JuiceFS mount")
	reach := fs.String("reach", "", "metadata host:port for the reachability monitor")
	metrics := fs.String("metrics", "http://127.0.0.1:21050", "jm5 metrics base URL")
	out := fs.String("out", "", "JSONL timeline")
	every := fs.Duration("write-every", time.Second, "one new file per interval")
	duration := fs.Duration("duration", 10*time.Minute, "stop after this long (or on SIGTERM)")
	_ = fs.Parse(args)
	if *export == "" || *dir == "" || *out == "" {
		return errors.New("-export, -dir and -out are required")
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	manifest, err := os.Create(*out + ".manifest")
	if err != nil {
		return err
	}
	defer manifest.Close()
	lg := &roamLog{f: f}

	setup, err := dial(*addr, *export)
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	if _, err := mkdirLikeMacOS(setup, *dir); err != nil {
		return fmt.Errorf("mkdir %s: %w", *dir, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() { <-sig; cancel() }()

	var clocks []*opClock
	clock := func(name string) *opClock {
		c := &opClock{name: name}
		c.op.Store("")
		clocks = append(clocks, c)
		return c
	}
	var wg sync.WaitGroup
	run := func(fn func()) { wg.Add(1); go func() { defer wg.Done(); fn() }() }

	if *readFile != "" {
		c := clock("read")
		run(func() { roamRead(ctx, lg, c, *addr, *export, *readFile, *readRate) })
	}
	{
		c := clock("write")
		run(func() { roamWrite(ctx, lg, c, manifest, *addr, *export, *dir, *every) })
	}
	{
		c := clock("meta")
		run(func() { roamMeta(ctx, lg, c, *addr, *export, *dir) })
	}
	{
		c := clock("fuse")
		run(func() { roamFUSE(ctx, lg, c, *fuse) })
	}
	if *reach != "" {
		r := health.NewReachability(*reach, health.WithFailureThreshold(2))
		r.OnChange(func(up bool, why string) { lg.put(map[string]any{"w": "reach", "up": up, "why": why}) })
		r.Start()
		defer r.Stop()
	}
	run(func() { roamSpool(ctx, lg, *metrics) })

	<-ctx.Done()
	// Workers stuck in a call cannot see ctx; report them and leave.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	now := time.Now().UnixMilli()
	for _, c := range clocks {
		if s := c.start.Load(); s != 0 {
			lg.put(map[string]any{"w": c.name, "op": c.op.Load(), "stuck": true, "since": s, "ms": now - s})
		}
	}
	lg.put(map[string]any{"w": "probe", "end": true})
	return nil
}

func roamRead(ctx context.Context, lg *roamLog, c *opClock, addr, export, file string, mibs float64) {
	buf := make([]byte, 1<<20)
	var off int64
	// Pacing: the read may not get ahead of start + off/rate (a player at a
	// fixed bitrate). After a stall it catches up as fast as it can.
	t0 := time.Now()
	pace := func() {
		if mibs <= 0 {
			return
		}
		due := t0.Add(time.Duration(float64(off) / (mibs * (1 << 20)) * float64(time.Second)))
		if d := time.Until(due); d > 0 {
			sleepCtx(ctx, d)
		}
	}
	for ctx.Err() == nil {
		c.begin("open")
		t := time.Now()
		v, err := dial(addr, export)
		var size int64
		var rf interface {
			ReadAt([]byte, int64) (int, error)
		}
		if err == nil {
			var fi os.FileInfo
			fi, _, err = v.Lookup(file)
			if err == nil {
				size = fi.Size()
				rf, err = v.Open(file)
			}
		}
		c.end()
		if err != nil {
			lg.put(map[string]any{"w": "read", "op": "open", "ok": false, "ms": time.Since(t).Milliseconds(), "err": errText(err)})
			sleepCtx(ctx, time.Second)
			continue
		}
		lg.put(map[string]any{"w": "read", "op": "open", "ok": true, "off": off, "ms": time.Since(t).Milliseconds()})
		for ctx.Err() == nil && off < size {
			pace()
			c.begin("read")
			t := time.Now()
			n, err := rf.ReadAt(buf, off)
			c.end()
			if n > 0 {
				off += int64(n)
			}
			if err != nil && !(errors.Is(err, io.EOF) || err.Error() == "EOF") {
				lg.put(map[string]any{"w": "read", "op": "read", "ok": false, "off": off, "ms": time.Since(t).Milliseconds(), "err": errText(err)})
				break // reopen and resume at off
			}
			lg.put(map[string]any{"w": "read", "op": "read", "ok": true, "off": off, "n": n, "ms": time.Since(t).Milliseconds()})
		}
		if off >= size && size > 0 {
			lg.put(map[string]any{"w": "read", "op": "done", "ok": true, "off": off})
			<-ctx.Done()
			return
		}
		sleepCtx(ctx, time.Second)
	}
}

func roamWrite(ctx context.Context, lg *roamLog, c *opClock, manifest *os.File, addr, export, dir string, every time.Duration) {
	target, err := dial(addr, export)
	for i := 0; ctx.Err() == nil; i++ {
		name := fmt.Sprintf("%s/w%05d.bin", dir, i)
		data := roamContent(i)
		sum := sha256.Sum256(data)
		for try := 0; ctx.Err() == nil; try++ {
			t := time.Now()
			if target == nil || err != nil {
				c.begin("dial")
				target, err = dial(addr, export)
				c.end()
				if err != nil {
					lg.put(map[string]any{"w": "write", "op": "dial", "ok": false, "err": errText(err)})
					sleepCtx(ctx, time.Second)
					continue
				}
			}
			c.begin("create")
			wf, werr := target.OpenFile(name, 0o644)
			if werr == nil {
				c.begin("write")
				_, werr = wf.Write(data)
				c.begin("commit")
				if cerr := wf.Close(); werr == nil {
					werr = cerr
				}
			}
			c.end()
			ms := time.Since(t).Milliseconds()
			if werr != nil {
				lg.put(map[string]any{"w": "write", "op": "file", "i": i, "try": try, "ok": false, "ms": ms, "err": errText(werr)})
				if strings.Contains(werr.Error(), "connection") || strings.Contains(werr.Error(), "EOF") {
					err = werr // redial
				}
				sleepCtx(ctx, time.Second)
				continue
			}
			lg.put(map[string]any{"w": "write", "op": "file", "i": i, "try": try, "ok": true, "ms": ms, "bytes": len(data)})
			fmt.Fprintf(manifest, "%s %d %s\n", filepath.Base(name), len(data), hex.EncodeToString(sum[:]))
			break
		}
		if !sleepCtx(ctx, every) {
			return
		}
	}
}

func roamMeta(ctx context.Context, lg *roamLog, c *opClock, addr, export, dir string) {
	v, err := dial(addr, export)
	for ctx.Err() == nil {
		t := time.Now()
		if err != nil {
			c.begin("dial")
			v, err = dial(addr, export)
			c.end()
		}
		if err == nil {
			c.begin("lookup")
			_, _, err = v.Lookup(dir)
			if err == nil {
				c.begin("readdirplus")
				_, err = v.ReadDirPlus(dir)
			}
			c.end()
		}
		lg.put(map[string]any{"w": "meta", "ok": err == nil, "ms": time.Since(t).Milliseconds(), "err": errText(err)})
		if err != nil && !strings.Contains(err.Error(), "connection") {
			err = nil // an NFS error, not a dead connection: keep the target
		}
		if !sleepCtx(ctx, time.Second) {
			return
		}
	}
}

func roamFUSE(ctx context.Context, lg *roamLog, c *opClock, fuse string) {
	for i := 0; ctx.Err() == nil; i++ {
		t := time.Now()
		c.begin("stat")
		_, err := os.Stat(filepath.Join(fuse, fmt.Sprintf(".roam-absent-%d-%d", os.Getpid(), i)))
		c.end()
		ok := errors.Is(err, os.ErrNotExist)
		e := ""
		if !ok {
			e = errText(err)
		}
		lg.put(map[string]any{"w": "fuse", "ok": ok, "ms": time.Since(t).Milliseconds(), "err": e})
		if !sleepCtx(ctx, time.Second) {
			return
		}
	}
}

func roamSpool(ctx context.Context, lg *roamLog, base string) {
	hc := &http.Client{Timeout: 2 * time.Second}
	get := func(path string) any {
		r, err := hc.Get(base + path)
		if err != nil {
			return map[string]any{"error": errText(err)}
		}
		defer r.Body.Close()
		var v any
		if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&v); err != nil {
			return map[string]any{"error": errText(err), "status": r.StatusCode}
		}
		if m, ok := v.(map[string]any); ok {
			delete(m, "entries") // one row per spooled file: too much per second
		}
		return v
	}
	for ctx.Err() == nil {
		lg.put(map[string]any{"w": "spool", "spool": get("/spool"), "leases": get("/project-leases")})
		if !sleepCtx(ctx, time.Second) {
			return
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// verify checks a roam manifest against the JuiceFS mount: every
// acknowledged file is in the folder with the acknowledged bytes, and the
// folder holds nothing else (no duplicates, no leftovers).
func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	manifest := fs.String("manifest", "", "the roam manifest")
	dir := fs.String("dir", "", "the run folder on the JuiceFS mount")
	_ = fs.Parse(args)
	b, err := os.ReadFile(*manifest)
	if err != nil {
		return err
	}
	want := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var name, sum string
		var n int
		if _, err := fmt.Sscanf(line, "%s %d %s", &name, &n, &sum); err == nil {
			want[name] = sum
		}
	}
	ents, err := os.ReadDir(*dir)
	if err != nil {
		return err
	}
	var missing, wrong, extra []string
	have := map[string]bool{}
	for _, e := range ents {
		have[e.Name()] = true
		sum, ok := want[e.Name()]
		if !ok {
			extra = append(extra, e.Name())
			continue
		}
		data, err := os.ReadFile(filepath.Join(*dir, e.Name()))
		got := sha256.Sum256(data)
		if err != nil || hex.EncodeToString(got[:]) != sum {
			wrong = append(wrong, e.Name())
		}
	}
	for name := range want {
		if !have[name] {
			missing = append(missing, name)
		}
	}
	emit(map[string]any{"step": "verify", "acknowledged": len(want), "on_mount": len(ents),
		"missing": missing, "wrong": wrong, "extra": extra,
		"ok": len(missing) == 0 && len(wrong) == 0 && len(extra) == 0})
	return nil
}
