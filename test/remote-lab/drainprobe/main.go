// drainprobe drives the write path of a JuiceMount NFS server (cmd/jm5 with
// the write spool on) from inside the remote lab's client container, and
// watches the spool drain land on the JuiceFS mount.
//
// The remote lab measures JuiceFS alone. The spool, the drainer and the
// folder-creation path live in the NFS layer, so this probe serves that layer
// with jm5 on the lab client and talks NFSv3 to it over loopback. The link
// between the client and the server stays the lab's emulated WAN.
//
//	drainprobe write -nfs 127.0.0.1:21049 -dir w/run1 -small 200 -big-mb 500
//	drainprobe wait  -fuse /jfs -dir w/run1 -count 201 -timeout 600s
//
// Both print one JSON object on stdout. The workload copies the laptop lab's
// (orchestrator laptop-lab-2026-11-11): one mkdir, 200 files of 2–63 KiB
// (seeded), then one 500 MB file.
package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	mrand "math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	_ "modernc.org/sqlite"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: drainprobe write|wait [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "write":
		err = write(os.Args[2:])
	case "wait":
		err = wait(os.Args[2:])
	case "read":
		err = read(os.Args[2:])
	default:
		err = fmt.Errorf("unknown step %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "drainprobe:", err)
		os.Exit(1)
	}
}

func emit(v any) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func dial(addr, export string) (*nfsc.Target, error) {
	c, err := rpc.DialTCP("tcp", addr, false)
	if err != nil {
		return nil, err
	}
	m := &nfsc.Mount{Client: c}
	return m.Mount(export, rpc.NewAuthUnix("drainprobe", 0, 0).Auth())
}

func secs(d time.Duration) float64 { return float64(d.Microseconds()) / 1e6 }

// mkdirLikeMacOS sends MKDIR and, on NFS3ERR_JUKEBOX (which this client
// reports as os.ErrInvalid), waits 4 s and sends it again, as the macOS NFS
// client does (its first "try later" delay). A resend that finds the folder
// counts as success: the first attempt created it.
func mkdirLikeMacOS(v *nfsc.Target, name string) (retries int, err error) {
	for {
		_, err = v.Mkdir(name, 0o755)
		if err == nil || (retries > 0 && errors.Is(err, os.ErrExist)) {
			return retries, nil
		}
		if !errors.Is(err, os.ErrInvalid) || retries >= 5 {
			return retries, err
		}
		retries++
		time.Sleep(4 * time.Second)
	}
}

func write(args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	addr := fs.String("nfs", "127.0.0.1:21049", "NFS server address")
	export := fs.String("export", "/Volumes/zpool", "export path (jm5 --mount)")
	dir := fs.String("dir", "", "folder to create, relative to the export root")
	mkdirs := fs.Int("mkdirs", 1, "extra nested folders created after the first (each timed)")
	small := fs.Int("small", 200, "small files")
	bigMB := fs.Int("big-mb", 500, "size of the one large file in MB (0 = none)")
	_ = fs.Parse(args)
	if *dir == "" {
		return fmt.Errorf("-dir is required")
	}
	v, err := dial(*addr, *export)
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	// The parent must already exist; resolve it first so the timed mkdir
	// is the MKDIR call alone, as for an app that already browses there.
	if _, _, err := v.Lookup(filepath.Dir(*dir)); err != nil {
		return fmt.Errorf("lookup parent: %w", err)
	}
	var mk []float64
	jukebox := 0
	for i := 0; i < *mkdirs; i++ {
		name := *dir
		if i > 0 {
			name = fmt.Sprintf("%s/sub%02d", *dir, i)
		}
		t := time.Now()
		retries, err := mkdirLikeMacOS(v, name)
		if err != nil {
			return fmt.Errorf("mkdir %s: %w", name, err)
		}
		jukebox += retries
		mk = append(mk, secs(time.Since(t)))
	}

	rnd := mrand.New(mrand.NewSource(7))
	buf := make([]byte, 8<<20)
	_, _ = rand.Read(buf)
	per := make([]float64, 0, *small)
	t1 := time.Now()
	for i := 0; i < *small; i++ {
		s := time.Now()
		f, err := v.OpenFile(fmt.Sprintf("%s/small_%03d.xmp", *dir, i), 0o644)
		if err != nil {
			return fmt.Errorf("create small %d: %w", i, err)
		}
		n := (rnd.Intn(62) + 2) * 1024
		if _, err := f.Write(buf[:n]); err != nil {
			return fmt.Errorf("write small %d: %w", i, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("commit small %d: %w", i, err)
		}
		per = append(per, secs(time.Since(s)))
	}
	smallTotal := time.Since(t1)

	var bigTotal time.Duration
	if *bigMB > 0 {
		s := time.Now()
		f, err := v.OpenFile(*dir+"/render.mov", 0o644)
		if err != nil {
			return fmt.Errorf("create big: %w", err)
		}
		left := *bigMB << 20
		for left > 0 {
			n := min(left, len(buf))
			if _, err := f.Write(buf[:n]); err != nil {
				return fmt.Errorf("write big: %w", err)
			}
			left -= n
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("commit big: %w", err)
		}
		bigTotal = time.Since(s)
	}
	sort.Float64s(per)
	q := func(p float64) float64 {
		if len(per) == 0 {
			return 0
		}
		return per[int(p*float64(len(per)-1))]
	}
	emit(map[string]any{
		"step": "write", "dir": *dir, "mkdir_s": mk, "mkdir_jukebox": jukebox,
		"small": *small, "small_total_s": secs(smallTotal), "small_p50_s": q(0.5), "small_p95_s": q(0.95),
		"big_mb": *bigMB, "big_total_s": secs(bigTotal),
	})
	return nil
}

// wait polls jm5's spool database (read-only, local) until no saved file
// or folder is left to send, then checks once that every file is on the
// JuiceFS mount at its final size. Polling the database costs the link
// nothing, so the round-trip counts stay the drain's own.
func wait(args []string) error {
	fs := flag.NewFlagSet("wait", flag.ExitOnError)
	db := fs.String("db", "/var/jm/metadata.db", "jm5 metadata database")
	fuse := fs.String("fuse", "/jfs", "JuiceFS mount, for the final check")
	dir := fs.String("dir", "", "folder, relative to the mount")
	count := fs.Int("count", 201, "files expected on the mount")
	big := fs.String("big", "render.mov", "name of the large file")
	timeout := fs.Duration("timeout", 600*time.Second, "give up after")
	every := fs.Duration("every", time.Second, "poll interval")
	metricsURL := fs.String("metrics", "", "jm5 /metrics URL, to sample the FUSE data gate")
	jfsStats := fs.String("jfsstats", "", "JuiceFS .stats file, to sample uploaded bytes")
	_ = fs.Parse(args)
	conn, err := sql.Open("sqlite", "file:"+*db+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	defer conn.Close()
	hasDirs := false
	_ = conn.QueryRow(`SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='pending_dirs'`).Scan(&hasDirs)
	t0 := time.Now()
	var trace [][5]float64
	marks := map[string]float64{}
	gateMax, gateSum, gateN := 0, 0, 0
	put0 := putBytes(*jfsStats)
	var putTrace [][2]float64
	var small, bigLeft, dirs int
	for {
		small, bigLeft, dirs = 0, 0, 0
		err := conn.QueryRow(`SELECT
			COALESCE(SUM(CASE WHEN nfs_path NOT LIKE ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN nfs_path LIKE ? THEN 1 ELSE 0 END), 0)
			FROM spool_entries WHERE drain_state <> 'done'`, "%/"+*big, "%/"+*big).Scan(&small, &bigLeft)
		if err != nil {
			return fmt.Errorf("query spool: %w", err)
		}
		if hasDirs {
			if err := conn.QueryRow(`SELECT COUNT(*) FROM pending_dirs`).Scan(&dirs); err != nil {
				return fmt.Errorf("query pending folders: %w", err)
			}
		}
		el := secs(time.Since(t0))
		gate := -1
		if *metricsURL != "" {
			if g, ok := gateInUse(*metricsURL); ok {
				gate = g
				gateMax = max(gateMax, g)
				gateSum += g
				gateN++
			}
		}
		trace = append(trace, [5]float64{el, float64(small), float64(bigLeft), float64(dirs), float64(gate)})
		if *jfsStats != "" {
			putTrace = append(putTrace, [2]float64{el, float64(putBytes(*jfsStats) - put0)})
		}
		if _, ok := marks["folders"]; !ok && dirs == 0 {
			marks["folders"] = el
		}
		if _, ok := marks["small"]; !ok && small == 0 {
			marks["small"] = el
		}
		if _, ok := marks["big"]; !ok && bigLeft == 0 {
			marks["big"] = el
		}
		if (small == 0 && bigLeft == 0 && dirs == 0) || time.Since(t0) > *timeout {
			break
		}
		time.Sleep(*every)
	}
	drained := secs(time.Since(t0))
	put := putBytes(*jfsStats) - put0
	onMount := 0
	_ = filepath.Walk(filepath.Join(*fuse, *dir), func(p string, fi os.FileInfo, err error) error {
		if err == nil && fi.Mode().IsRegular() && !strings.HasPrefix(filepath.Base(p), ".") {
			onMount++
		}
		return nil
	})
	step := max(1, len(trace)/40)
	var short [][5]float64
	for i := 0; i < len(trace); i += step {
		short = append(short, trace[i])
	}
	short = append(short, trace[len(trace)-1])
	emit(map[string]any{
		"step": "wait", "dir": *dir, "seconds": drained, "done": small == 0 && bigLeft == 0 && dirs == 0,
		"small_left": small, "big_left": bigLeft, "folders_left": dirs, "marks_s": marks,
		"on_mount": onMount, "want_on_mount": *count,
		"put_bytes": put, "put_mbit_s": float64(put) * 8 / 1e6 / max(drained, 1),
		"put_trace":       sample(putTrace),
		"gate_in_use_max": gateMax, "gate_in_use_mean": float64(gateSum) / float64(max(gateN, 1)),
		"trace_cols": "seconds,small_left,big_left,folders_left,gate_in_use", "trace": short,
	})
	return nil
}

// putBytes reads JuiceFS's uploaded object bytes from its .stats file.
func putBytes(path string) int64 {
	if path == "" {
		return 0
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	for _, l := range strings.Split(string(b), "\n") {
		if f := strings.Fields(l); len(f) == 2 && f[0] == "juicefs_object_request_data_bytes_PUT" {
			var v float64
			_, _ = fmt.Sscan(f[1], &v)
			return int64(v)
		}
	}
	return 0
}

func sample(t [][2]float64) [][2]float64 {
	step := max(1, len(t)/30)
	var out [][2]float64
	for i := 0; i < len(t); i += step {
		out = append(out, t[i])
	}
	if len(t) > 0 {
		out = append(out, t[len(t)-1])
	}
	return out
}

// gateInUse reads fuse_data_gate.in_use from jm5's JSON /metrics.
func gateInUse(url string) (int, bool) {
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	var m struct {
		Gate *struct {
			InUse int `json:"in_use"`
		} `json:"fuse_data_gate"`
	}
	if json.NewDecoder(resp.Body).Decode(&m) != nil || m.Gate == nil {
		return 0, false
	}
	return m.Gate.InUse, true
}

// read reads a file over NFS for a time budget, like an editor: random
// 1 MiB scrub reads first, then a sequential read. It exercises the NFS read
// path (read-ahead, data gate, descriptor pool) before the writes.
func read(args []string) error {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	addr := fs.String("nfs", "127.0.0.1:21049", "NFS server address")
	export := fs.String("export", "/Volumes/zpool", "export path")
	file := fs.String("file", "", "file to read, relative to the export root")
	scrub := fs.Int("scrub", 15, "random 1 MiB reads")
	budget := fs.Duration("budget", 300*time.Second, "total time budget")
	_ = fs.Parse(args)
	v, err := dial(*addr, *export)
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	fi, _, err := v.Lookup(*file)
	if err != nil {
		return fmt.Errorf("lookup: %w", err)
	}
	size := fi.Size()
	f, err := v.Open(*file)
	if err != nil {
		return err
	}
	t0 := time.Now()
	buf := make([]byte, 1<<20)
	rnd := mrand.New(mrand.NewSource(11))
	var lat []float64
	errs := 0
	for i := 0; i < *scrub && time.Since(t0) < *budget; i++ {
		off := rnd.Int63n(max(size-int64(len(buf)), 1))
		s := time.Now()
		if _, err := f.ReadAt(buf, off); err != nil && err.Error() != "EOF" {
			errs++
		}
		lat = append(lat, secs(time.Since(s)))
	}
	var seq int64
	for off := int64(0); off < size && time.Since(t0) < *budget; {
		n, err := f.ReadAt(buf, off)
		if n <= 0 || err != nil && err.Error() != "EOF" {
			if err != nil {
				errs++
			}
			break
		}
		off += int64(n)
		seq += int64(n)
	}
	emit(map[string]any{"step": "read", "file": *file, "scrub_s": lat, "seq_bytes": seq,
		"seconds": secs(time.Since(t0)), "errors": errs})
	return nil
}
