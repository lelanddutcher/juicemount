// linkclassprobe: measure how the app classifies the lab link.
//
// It runs inside the lab client container and uses the app's own
// classification code on the real (shaped, optionally tunnelled) link:
//
//   - the route ceiling: health.ResolveRouteInterface on the proxy address,
//     then health.DefaultRouteCeiling().Apply, as the serving process does
//     at startup (bridge/cbridge.go) and the mount does (health/fuse.go);
//   - the mount's one link sample (health.ObserveMountLinkSample);
//   - the reachability monitor with the app's RTT observer
//     (health.NewReachability + WithRTTObserver(netprofile ObserveRTT));
//   - the read path: the NFS layer's ReadaheadManager on the juicefs mount,
//     and foreground reads of -rsize bytes, timed and fed to
//     ObserveThroughput as nfs/handler.go does for each FUSE subread.
//
// Every second it records the profile snapshot, the high-latency flag and
// the juicefs live policy flags (nfs.JFSLinkPolicyFlags). Phase "idle" is
// the reachability monitor alone (-idle); phase "read" is a sequential read
// of -file for -read (or to its end). One JSON object goes to stdout.
//
// The foreground reads model the macOS NFS client: -inflight READs of
// -rsize (1 MiB, the app's rsize) at a time, in file order.
//
// -serial models macFUSE, which runs one read of a file at a time in
// 128 KiB requests (nfs/demand_first.go rule 5): each foreground READ is
// issued as -piece sized preads, and only one pread of the file runs at a
// time across every READ in flight and the NFS layer's read-ahead (which
// macFUSE serializes with them too); -no-server-readahead turns that
// read-ahead off for a pure foreground measurement. read_get_bytes is the juicefs
// client's object GET bytes during the read (/jfs/.stats).
//
// -follow runs the app's juicefs link follower on the mount
// (nfs.FollowJFSLinkPolicy), as the NFS server does: juicefs's timeouts,
// demand-first and read-ahead (patch 0035) follow the live class.
// -jfs-readahead-mb sets JM_JFS_READAHEAD_MB for it (0: the rollback).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/nfs"
)

type sample struct {
	T           float64  `json:"t"`
	Phase       string   `json:"phase"`
	Class       string   `json:"class"`
	ReadClass   string   `json:"read_class"`
	RTTms       float64  `json:"rtt_ms"`
	BWMbit      float64  `json:"bw_mbit"`
	HaveBW      bool     `json:"have_bw"`
	HighLatency bool     `json:"high_latency"`
	LowBW       bool     `json:"low_bandwidth"`
	JFSFlags    []string `json:"jfs_flags"`
	JFSRAMiB    int      `json:"jfs_readahead_mib"`
	ReadMiB     float64  `json:"read_mib"`
}

type result struct {
	StartUnix        float64   `json:"start_unix"`
	RouteInterface   string    `json:"route_interface"`
	RouteErr         string    `json:"route_error,omitempty"`
	Tunneled         bool      `json:"route_ceiling"`
	ForceClass       string    `json:"force_class,omitempty"`
	IdleS            float64   `json:"idle_s"`
	ReadS            float64   `json:"read_s"`
	ReadBytes        int64     `json:"read_bytes"`
	ReadMbit         float64   `json:"read_mbit"`
	ReadErr          string    `json:"read_error,omitempty"`
	ReadGetBytes     int64     `json:"read_get_bytes"`
	ReadTailGetBytes int64     `json:"read_tail_get_bytes"`
	Inflight         int       `json:"inflight"`
	Piece            int       `json:"piece"`
	Serial           bool      `json:"serial"`
	ServerRA         bool      `json:"server_readahead"`
	TTFBms           float64   `json:"ttfb_ms,omitempty"`
	TTFBGetBytes     int64     `json:"ttfb_get_bytes,omitempty"`
	TTFBGetBytes3s   int64     `json:"ttfb_get_bytes_3s,omitempty"`
	ScrubMs          []float64 `json:"scrub_ms,omitempty"`
	ScrubP50         float64   `json:"scrub_p50_ms,omitempty"`
	ScrubP95         float64   `json:"scrub_p95_ms,omitempty"`
	ScrubBytes       int64     `json:"scrub_bytes,omitempty"`
	ScrubGetBytes    int64     `json:"scrub_get_bytes,omitempty"`
	ScrubGets        int64     `json:"scrub_gets,omitempty"`
	Read2S           float64   `json:"read2_s,omitempty"`
	Read2Bytes       int64     `json:"read2_bytes,omitempty"`
	Read2GetBytes    int64     `json:"read2_get_bytes,omitempty"`
	Read2Mbit        float64   `json:"read2_mbit,omitempty"`
	Samples          []sample  `json:"samples"`
	Summary          summary   `json:"summary"`
}

type summary struct {
	Classes        map[string]int `json:"classes"`
	ReadClasses    map[string]int `json:"read_classes"`
	HighLatencyN   int            `json:"high_latency_samples"`
	N              int            `json:"samples"`
	RTTMedianMs    float64        `json:"rtt_median_ms"`
	BWMedianMbit   float64        `json:"bw_median_mbit"`
	BWLastMbit     float64        `json:"bw_last_mbit"`
	FinalClass     string         `json:"final_class"`
	FinalReadClass string         `json:"final_read_class"`
	FinalJFSFlags  []string       `json:"final_jfs_flags"`
}

func main() {
	meta := flag.String("meta", "10.231.10.20:6379", "the RESP proxy address (host:port)")
	file := flag.String("file", "", "a cold file on the juicefs mount to read")
	root := flag.String("root", "/jfs", "the juicefs mount")
	idle := flag.Duration("idle", 20*time.Second, "reachability-only phase")
	readFor := flag.Duration("read", 25*time.Second, "sequential read budget")
	rsize := flag.Int("rsize", 1<<20, "foreground read size")
	inflight := flag.Int("inflight", 2, "foreground reads in flight")
	interval := flag.Duration("probe-interval", 2*time.Second, "reachability base interval (the app's default)")
	force := flag.String("force-class", "", "pin the class (diagnostic): metered|slow|medium|fast")
	piece := flag.Int("piece", 0, "split each foreground read into preads of this size (0: one pread)")
	serial := flag.Bool("serial", false, "one foreground pread of the file at a time (models macFUSE)")
	noServerRA := flag.Bool("no-server-readahead", false, "do not run the NFS layer's read-ahead")
	follow := flag.Bool("follow", true, "run the app's juicefs link follower on the mount")
	jfsRA := flag.String("jfs-readahead-mb", "", "JM_JFS_READAHEAD_MB for the follower (empty: the app's default)")
	ttfbFile := flag.String("ttfb-file", "", "after the read: open this cold file and time its first 64 KiB")
	scrubFile := flag.String("scrub-file", "", "after the read: random 1-4 MiB jumps in this cold file")
	scrubN := flag.Int("scrub", 30, "scrub jumps")
	scrubBudget := flag.Duration("scrub-budget", 90*time.Second, "scrub time budget")
	settle := flag.Duration("settle", 5*time.Second, "wait after the read before the next phase")
	read2File := flag.String("read2-file", "", "last: a sequential read of this cold file (the next clip of the session)")
	read2For := flag.Duration("read2", 20*time.Second, "second sequential read budget")
	flag.Parse()
	if *jfsRA != "" {
		_ = os.Setenv("JM_JFS_READAHEAD_MB", *jfsRA)
	}
	if *file == "" {
		fmt.Fprintln(os.Stderr, "linkclassprobe: -file is required")
		os.Exit(2)
	}

	p := netprofile.Default()
	res := result{ForceClass: *force, Inflight: *inflight, Piece: *piece, Serial: *serial, ServerRA: !*noServerRA}
	if *force != "" {
		c := map[string]netprofile.LinkClass{"metered": netprofile.ClassMetered, "slow": netprofile.ClassSlow,
			"medium": netprofile.ClassMedium, "fast": netprofile.ClassFast}[*force]
		p.ForceClass(&c)
	}
	iface, err := health.ResolveBackendRoute(*meta, health.ResolveRouteInterface)
	res.RouteInterface = iface
	if err != nil {
		res.RouteErr = err.Error()
	}
	res.Tunneled, _ = health.DefaultRouteCeiling().Apply(p, iface, err)
	health.ObserveMountLinkSample(p, "redis://"+*meta+"/1", nil)

	reach := health.NewReachability(*meta, health.WithFailureThreshold(2),
		health.WithBaseInterval(*interval), health.WithRTTObserver(p.ObserveRTT))
	reach.Start()
	defer reach.Stop()
	if *follow {
		defer nfs.FollowJFSLinkPolicy(*root)()
	}

	var readBytes atomic.Int64
	var mu sync.Mutex
	phase := "idle"
	t0 := time.Now()
	res.StartUnix = float64(t0.UnixNano()) / 1e9
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			s := p.Snapshot()
			mu.Lock()
			ph := phase
			mu.Unlock()
			res.Samples = append(res.Samples, sample{
				T: time.Since(t0).Seconds(), Phase: ph, Class: s.Class.String(), ReadClass: s.ReadClass.String(),
				RTTms: float64(s.RTT) / 1e6, BWMbit: s.BytesPerSec * 8 / 1e6, HaveBW: s.HaveBW,
				HighLatency: p.HighLatency(), LowBW: s.LowBandwidth, JFSFlags: nfs.JFSLinkPolicyFlags(p),
				JFSRAMiB: p.JuiceFSReadaheadMiB(false),
				ReadMiB:  float64(readBytes.Load()) / (1 << 20),
			})
		}
	}()

	time.Sleep(*idle)
	res.IdleS = idle.Seconds()
	mu.Lock()
	phase = "read"
	mu.Unlock()

	nr := newNFSReader(p, *root, *rsize, *inflight, *piece, *serial, !*noServerRA)
	defer nr.stop()
	get0 := jfsGetBytes(*root)
	rs, rerr := nr.seqRead(*file, *readFor, &readBytes)
	res.ReadGetBytes = jfsGetBytes(*root) - get0
	if *settle > 0 {
		// Read-ahead still in flight when the reader stopped: what a
		// stopped playback costs.
		g := jfsGetBytes(*root)
		time.Sleep(*settle)
		res.ReadTailGetBytes = jfsGetBytes(*root) - g
	}
	res.ReadS = rs.Seconds()
	res.ReadBytes = readBytes.Load()
	if rs > 0 {
		res.ReadMbit = float64(res.ReadBytes) * 8 / rs.Seconds() / 1e6
	}
	if rerr != nil {
		res.ReadErr = rerr.Error()
	}
	if *ttfbFile != "" {
		g := jfsGetBytes(*root)
		d, err := nr.ttfb(*ttfbFile)
		res.TTFBms, res.TTFBGetBytes = d.Seconds()*1000, jfsGetBytes(*root)-g
		if err != nil {
			res.ReadErr += " ttfb: " + err.Error()
		}
		time.Sleep(3 * time.Second) // what read-ahead the first byte started
		res.TTFBGetBytes3s = jfsGetBytes(*root) - g
	}
	if *scrubFile != "" && *scrubN > 0 {
		mu.Lock()
		phase = "scrub"
		mu.Unlock()
		g := jfsGetBytes(*root)
		n0 := jfsCounter(*root, "juicefs_object_request_durations_histogram_seconds_GET_total")
		lat, b, err := nr.scrub(*scrubFile, *scrubN, *scrubBudget)
		time.Sleep(3 * time.Second) // read-ahead a jump started still lands
		res.ScrubMs, res.ScrubBytes, res.ScrubGetBytes = lat, b, jfsGetBytes(*root)-g
		res.ScrubGets = jfsCounter(*root, "juicefs_object_request_durations_histogram_seconds_GET_total") - n0
		res.ScrubP50, res.ScrubP95 = pct(lat, 0.5), pct(lat, 0.95)
		if err != nil {
			res.ReadErr += " scrub: " + err.Error()
		}
	}
	if *read2File != "" {
		mu.Lock()
		phase = "read2"
		mu.Unlock()
		var n atomic.Int64
		g := jfsGetBytes(*root)
		d, err := nr.seqRead(*read2File, *read2For, &n)
		res.Read2S, res.Read2Bytes, res.Read2GetBytes = d.Seconds(), n.Load(), jfsGetBytes(*root)-g
		if d > 0 {
			res.Read2Mbit = float64(n.Load()) * 8 / d.Seconds() / 1e6
		}
		if err != nil {
			res.ReadErr += " read2: " + err.Error()
		}
	}
	time.Sleep(1100 * time.Millisecond) // one more sample after the read
	close(stop)
	<-done
	res.Summary = summarize(res.Samples)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	_ = enc.Encode(res)
}

// nfsReader models the NFS layer reading the juicefs mount for one Mac
// client: READs of rsize, inflight at a time, each timed into the link
// estimator and reported to the NFS layer's read-ahead (nil: off).
type nfsReader struct {
	p        *netprofile.Profile
	root     string
	pool     *nfs.FDPool
	rm       *nfs.ReadaheadManager
	rsize    int
	inflight int
	piece    int
	serial   bool
	mu       sync.Mutex // -serial: one pread of a file at a time
}

func newNFSReader(p *netprofile.Profile, root string, rsize, inflight, piece int, serial, serverRA bool) *nfsReader {
	r := &nfsReader{p: p, root: root, pool: nfs.NewFDPool(), rsize: rsize, inflight: inflight, piece: piece, serial: serial}
	if r.piece <= 0 || r.piece > rsize {
		r.piece = rsize
	}
	if serverRA {
		r.rm = nfs.NewReadaheadManager(root, r.pool, p)
		if serial {
			// macFUSE serializes the NFS layer's read-ahead with the
			// foreground reads of the same file.
			r.rm.SetPrefetchReadAt(func(f *os.File, b []byte, off int64) (int, error) {
				return readPieces(f, b, off, r.piece, true, &r.mu)
			})
		}
	}
	return r
}

func (r *nfsReader) stop() {
	if r.rm != nil {
		r.rm.Stop()
	}
}

func (r *nfsReader) open(name string) (*os.File, int64, uint64, error) {
	f, err := r.pool.Get(r.root + "/" + name)
	if err != nil {
		return nil, 0, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, 0, 0, err
	}
	var ino uint64
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		ino = sys.Ino
	}
	return f, st.Size(), ino, nil
}

// readRange reads [from, to) of name in rsize READs, inflight at a time,
// until the deadline.
func (r *nfsReader) readRange(f *os.File, ino uint64, name string, from, to int64, deadline time.Time, total *atomic.Int64) error {
	next := atomic.Int64{}
	next.Store(from)
	var firstErr error
	var errMu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < r.inflight; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, r.rsize)
			for time.Now().Before(deadline) {
				off := next.Add(int64(r.rsize)) - int64(r.rsize)
				if off >= to {
					return
				}
				b := buf[:min(int64(r.rsize), to-off)]
				t := time.Now()
				n, rerr := readPieces(f, b, off, r.piece, r.serial, &r.mu)
				el := time.Since(t)
				if n > 0 {
					r.p.ObserveThroughput(int64(n), el)
					if r.rm != nil {
						r.rm.OnRead(ino, off, n, name)
					}
					if total != nil {
						total.Add(int64(n))
					}
				}
				if rerr != nil && n == 0 {
					errMu.Lock()
					if firstErr == nil {
						firstErr = rerr
					}
					errMu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// seqRead reads name from its start for budget (or to its end).
func (r *nfsReader) seqRead(name string, budget time.Duration, total *atomic.Int64) (time.Duration, error) {
	f, size, ino, err := r.open(name)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	err = r.readRange(f, ino, name, 0, size, start.Add(budget), total)
	return time.Since(start), err
}

// ttfb opens name and reads its first 64 KiB: the time to the first byte.
func (r *nfsReader) ttfb(name string) (time.Duration, error) {
	start := time.Now()
	f, _, ino, err := r.open(name)
	if err != nil {
		return 0, err
	}
	err = r.readRange(f, ino, name, 0, 64<<10, start.Add(time.Minute), nil)
	return time.Since(start), err
}

// scrub makes n random jumps of 1-4 MiB in name (seeded, as probe.py
// scrub) and returns each jump's time, until budget.
func (r *nfsReader) scrub(name string, n int, budget time.Duration) ([]float64, int64, error) {
	f, size, ino, err := r.open(name)
	if err != nil {
		return nil, 0, err
	}
	rng := rand.New(rand.NewSource(20261127))
	var lat []float64
	var bytes int64
	end := time.Now().Add(budget)
	for i := 0; i < n && time.Now().Before(end); i++ {
		ln := int64(1<<20 + rng.Intn(3<<20))
		off := rng.Int63n(size-ln) &^ (4<<10 - 1)
		t := time.Now()
		if err := r.readRange(f, ino, name, off, off+ln, t.Add(2*time.Minute), nil); err != nil {
			return lat, bytes, err
		}
		lat = append(lat, time.Since(t).Seconds()*1000)
		bytes += ln
	}
	return lat, bytes, nil
}

// readPieces reads buf at off in piece-sized preads, each under mu when
// serial is set, and stops at the first short or failed pread.
func readPieces(f *os.File, buf []byte, off int64, piece int, serial bool, mu *sync.Mutex) (int, error) {
	got := 0
	for got < len(buf) {
		end := got + piece
		if end > len(buf) {
			end = len(buf)
		}
		if serial {
			mu.Lock()
		}
		n, err := f.ReadAt(buf[got:end], off+int64(got))
		if serial {
			mu.Unlock()
		}
		want := end - got
		got += n
		if err != nil {
			return got, err
		}
		if n < want {
			return got, nil
		}
	}
	return got, nil
}

// jfsGetBytes is the juicefs client's object GET bytes so far (0 if the
// counter is missing).
func jfsGetBytes(root string) int64 {
	b, err := os.ReadFile(root + "/.stats")
	if err != nil {
		return 0
	}
	var total int64
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && strings.HasPrefix(f[0], "juicefs_object_request_data_bytes") && strings.Contains(f[0], "GET") {
			if v, err := strconv.ParseFloat(f[1], 64); err == nil {
				total += int64(v)
			}
		}
	}
	return total
}

// jfsCounter is one counter of the juicefs client (0 if missing).
func jfsCounter(root, name string) int64 {
	b, err := os.ReadFile(root + "/.stats")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == name {
			v, _ := strconv.ParseFloat(f[1], 64)
			return int64(v)
		}
	}
	return 0
}

func summarize(ss []sample) summary {
	sum := summary{Classes: map[string]int{}, ReadClasses: map[string]int{}, N: len(ss)}
	var rtts, bws []float64
	for _, s := range ss {
		sum.Classes[s.Class]++
		sum.ReadClasses[s.ReadClass]++
		if s.HighLatency {
			sum.HighLatencyN++
		}
		if s.RTTms > 0 {
			rtts = append(rtts, s.RTTms)
		}
		if s.HaveBW {
			bws = append(bws, s.BWMbit)
		}
	}
	sum.RTTMedianMs = median(rtts)
	sum.BWMedianMbit = median(bws)
	if len(bws) > 0 {
		sum.BWLastMbit = bws[len(bws)-1]
	}
	if n := len(ss); n > 0 {
		sum.FinalClass, sum.FinalReadClass, sum.FinalJFSFlags = ss[n-1].Class, ss[n-1].ReadClass, ss[n-1].JFSFlags
	}
	return sum
}

// pct is the q quantile of v (nearest rank).
func pct(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	return c[min(len(c)-1, int(q*float64(len(c))))]
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	return c[len(c)/2]
}
