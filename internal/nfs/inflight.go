package nfs

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// In-flight RPC tracking. The completed-RPC latency metrics (max_us per op)
// CANNOT see a HUNG RPC — one that never returns never updates its max. That
// blind spot is exactly what hid the "error 100060" cause: the kernel NFS soft
// mount (timeo≈40s) gives up on an RPC that hasn't completed, the client
// surfaces ETIMEDOUT, and Finder aborts the copy — but server-side every
// *completed* op still looks fast.
//
// This registry tracks each RPC from dispatch to completion. A watchdog
// samples the oldest in-flight RPC; when one crosses inflightDumpAfter (well
// before the 40s client timeout) it logs the op+age and writes a full
// goroutine dump, capturing the wedge in the act so the exact blocked code
// path is known on the next reproduction.

type inflightEntry struct {
	op    string
	start time.Time
}

var (
	inflightMu     sync.Mutex
	inflightMap    = make(map[uint64]inflightEntry)
	inflightNextID uint64
	inflightWDOnce sync.Once
)

// inflightDumpAfter: an RPC in-flight longer than this is treated as a stall
// and triggers a goroutine dump. 22s leaves margin before the ~40s soft-mount
// timeout so we capture the blocked stack BEFORE the client aborts with
// ETIMEDOUT ("error 100060"). Tunable via JM_INFLIGHT_DUMP_SEC.
var inflightDumpAfter = func() time.Duration {
	if v := os.Getenv("JM_INFLIGHT_DUMP_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 22 * time.Second
}()

var inflightDumpDir = func() string {
	if d := os.Getenv("JM_INFLIGHT_DUMP_DIR"); d != "" {
		return d
	}
	return "/tmp/jm_dumps"
}()

// inflightRegister records an in-flight RPC and returns its id. The watchdog is
// started lazily on first use so no explicit wiring is needed.
func inflightRegister(op string) uint64 {
	inflightWDOnce.Do(func() { go inflightWatchdog() })
	inflightMu.Lock()
	inflightNextID++
	id := inflightNextID
	inflightMap[id] = inflightEntry{op: op, start: time.Now()}
	inflightMu.Unlock()
	return id
}

func inflightDone(id uint64) {
	inflightMu.Lock()
	delete(inflightMap, id)
	inflightMu.Unlock()
}

// InflightStats reports the current in-flight RPC count and the oldest one's
// op + age. Exposed for the metrics endpoint so external monitors can watch
// for a hang live (oldest_age climbing toward the client timeout).
func InflightStats() (count int, oldestOp string, oldestAge time.Duration) {
	now := time.Now()
	inflightMu.Lock()
	defer inflightMu.Unlock()
	count = len(inflightMap)
	for _, e := range inflightMap {
		if age := now.Sub(e.start); age > oldestAge {
			oldestAge = age
			oldestOp = e.op
		}
	}
	return
}

// JUKEBOX tracking. The completed-RPC metrics count a JUKEBOX reply as a
// SUCCESS (it's a valid NFS status, not an rpc_error), so a JUKEBOX storm — the
// mechanism behind "error 100060" when the client retries the same logical op
// until its ~40s soft-mount timeout — is invisible. We count JUKEBOX replies
// per op and the watchdog logs the per-op rate periodically, so a storm names
// its own op (e.g. "nfs.LOOKUP" retrying thousands of times) instead of forcing
// another guess.
var jukeboxByOp sync.Map // op string -> *atomic.Int64

func recordJukebox(op string) {
	v, _ := jukeboxByOp.LoadOrStore(op, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

// snapshotJukebox returns the current per-op JUKEBOX totals.
func snapshotJukebox() map[string]int64 {
	out := map[string]int64{}
	jukeboxByOp.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

func inflightWatchdog() {
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	var lastDump time.Time
	prevJuke := map[string]int64{}
	var sinceJukeLog int
	for range tick.C {
		// Every ~15s, report any op whose JUKEBOX count grew — a storming op
		// is the "error 100060" signature.
		sinceJukeLog++
		if sinceJukeLog >= 5 {
			sinceJukeLog = 0
			cur := snapshotJukebox()
			type kv struct {
				op string
				d  int64
			}
			var grew []kv
			for op, n := range cur {
				if d := n - prevJuke[op]; d > 0 {
					grew = append(grew, kv{op, d})
				}
			}
			prevJuke = cur
			if len(grew) > 0 {
				sort.Slice(grew, func(i, j int) bool { return grew[i].d > grew[j].d })
				parts := ""
				for _, g := range grew {
					parts += fmt.Sprintf(" %s=%d", g.op, g.d)
				}
				Log.Errorf("JUKEBOX-RATE (per ~15s):%s — a high per-op rate is the 'error 100060' retry-storm signature", parts)
			}
		}
		count, op, age := InflightStats()
		if age < inflightDumpAfter {
			continue
		}
		Log.Errorf("INFLIGHT-STALL: %s in-flight %.1fs (inflight=%d) — RPC approaching soft-mount timeout (error 100060)",
			op, age.Seconds(), count)
		// Debounce: at most one dump per 30s so a sustained stall doesn't
		// spew hundreds of files.
		if time.Since(lastDump) < 30*time.Second {
			continue
		}
		lastDump = time.Now()
		if err := os.MkdirAll(inflightDumpDir, 0o755); err != nil {
			continue
		}
		fn := filepath.Join(inflightDumpDir, fmt.Sprintf("inflight_stall_%d.txt", time.Now().Unix()))
		f, err := os.Create(fn)
		if err != nil {
			continue
		}
		fmt.Fprintf(f, "INFLIGHT STALL: op=%s age=%.1fs inflight=%d at %s\n\n",
			op, age.Seconds(), count, time.Now().Format(time.RFC3339))
		_ = pprof.Lookup("goroutine").WriteTo(f, 2)
		_ = f.Close()
		Log.Errorf("INFLIGHT-STALL: goroutine dump written to %s", fn)
	}
}

// inflightOpName builds a short op label for a request (e.g. "nfs.WRITE").
func inflightOpName(r *request) string {
	switch r.Header.Prog {
	case nfsServiceID:
		return "nfs." + NFSProcedure(r.Header.Proc).String()
	case mountServiceID:
		return "mount." + MountProcedure(r.Header.Proc).String()
	default:
		return fmt.Sprintf("%d.%d", r.Header.Prog, r.Header.Proc)
	}
}
