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
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
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
	ReadMiB     float64  `json:"read_mib"`
}

type result struct {
	RouteInterface string   `json:"route_interface"`
	RouteErr       string   `json:"route_error,omitempty"`
	Tunneled       bool     `json:"route_ceiling"`
	ForceClass     string   `json:"force_class,omitempty"`
	IdleS          float64  `json:"idle_s"`
	ReadS          float64  `json:"read_s"`
	ReadBytes      int64    `json:"read_bytes"`
	ReadMbit       float64  `json:"read_mbit"`
	ReadErr        string   `json:"read_error,omitempty"`
	Samples        []sample `json:"samples"`
	Summary        summary  `json:"summary"`
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
	flag.Parse()
	if *file == "" {
		fmt.Fprintln(os.Stderr, "linkclassprobe: -file is required")
		os.Exit(2)
	}

	p := netprofile.Default()
	res := result{ForceClass: *force}
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

	var readBytes atomic.Int64
	var mu sync.Mutex
	phase := "idle"
	t0 := time.Now()
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
				ReadMiB: float64(readBytes.Load()) / (1 << 20),
			})
		}
	}()

	time.Sleep(*idle)
	res.IdleS = idle.Seconds()
	mu.Lock()
	phase = "read"
	mu.Unlock()

	rs, rerr := seqRead(p, *root, *file, *rsize, *inflight, *readFor, &readBytes)
	res.ReadS = rs.Seconds()
	res.ReadBytes = readBytes.Load()
	if rs > 0 {
		res.ReadMbit = float64(res.ReadBytes) * 8 / rs.Seconds() / 1e6
	}
	if rerr != nil {
		res.ReadErr = rerr.Error()
	}
	time.Sleep(1100 * time.Millisecond) // one more sample after the read
	close(stop)
	<-done
	res.Summary = summarize(res.Samples)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	_ = enc.Encode(res)
}

// seqRead reads name sequentially as the NFS layer does: rsize reads,
// inflight at a time, each timed into the link estimator and reported to
// the readahead manager.
func seqRead(p *netprofile.Profile, root, name string, rsize, inflight int, budget time.Duration, total *atomic.Int64) (time.Duration, error) {
	pool := nfs.NewFDPool()
	rm := nfs.NewReadaheadManager(root, pool, p)
	defer rm.Stop()
	path := root + "/" + name
	f, err := pool.Get(path)
	if err != nil {
		return 0, err
	}
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := st.Size()
	var ino uint64
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		ino = sys.Ino
	}
	start := time.Now()
	deadline := start.Add(budget)
	var next atomic.Int64
	var firstErr error
	var errMu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < inflight; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, rsize)
			for time.Now().Before(deadline) {
				off := next.Add(int64(rsize)) - int64(rsize)
				if off >= size {
					return
				}
				t := time.Now()
				n, rerr := f.ReadAt(buf, off)
				el := time.Since(t)
				if n > 0 {
					p.ObserveThroughput(int64(n), el)
					rm.OnRead(ino, off, n, name)
					total.Add(int64(n))
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
	return time.Since(start), firstErr
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

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	return c[len(c)/2]
}
