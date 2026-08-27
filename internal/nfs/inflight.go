package nfs

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
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

// inflightDumpDir must OUTLIVE A REBOOT.
//
// This defaulted to /tmp/jm_dumps, which macOS clears on boot. The dump is
// written precisely when the mount is wedging — i.e. in the minutes before the
// machine may panic or be force-restarted — so the one moment the stack traces
// matter is the one moment they are guaranteed to be gone.
//
// That is not hypothetical: on 2026-08-05 twelve dumps were written while NFS
// reads sat in-flight for 592s, the box panicked, and every one of them
// evaporated in the reboot. The stalled-goroutine stacks — the only direct
// evidence of WHERE the read was blocked — were lost.
//
// ~/Library/Logs/JuiceMount sits beside juicemount.log, which is where an
// operator already looks and which survives a restart.
var inflightDumpDir = func() string {
	if d := os.Getenv("JM_INFLIGHT_DUMP_DIR"); d != "" {
		return d
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, "Library", "Logs", "JuiceMount", "stall-dumps")
	}
	return "/tmp/jm_dumps" // last resort: better than not dumping at all
}()

// pruneStallDumps keeps the dump directory bounded. Persisting dumps across
// reboots means they accumulate, and an unbounded debug directory on the boot
// disk is its own failure — this system already fights for free space (the
// spool and the JuiceFS cache share the SSD). Keeps the newest maxStallDumps.
const maxStallDumps = 40

func pruneStallDumps(dir string) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type fi struct {
		name string
		mod  time.Time
	}
	var files []fi
	for _, e := range ents {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "inflight_stall_") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fi{e.Name(), info.ModTime()})
	}
	if len(files) <= maxStallDumps {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod.After(files[j].mod) })
	for _, f := range files[maxStallDumps:] {
		_ = os.Remove(filepath.Join(dir, f.name))
	}
}

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

// A single JUKEBOX reply is an expected, invisible recovery mechanism for an
// idempotent RPC crossing a bounded FUSE blip. Finder's error 100060 signature
// is a retry storm, not one successful retry. Keep every reply in metrics, but
// reserve the release-gating error log for three or more replies in one ~15s
// window. Lower rates remain visible as warnings without the 100060 signature.
const jukeboxStormMinReplies int64 = 3

func isJukeboxStormWindow(total int64) bool { return total >= jukeboxStormMinReplies }

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
		// Every ~15s, report every op whose JUKEBOX count grew. Isolated retries
		// stay diagnostic; only a high-rate window carries the release-gating
		// "error 100060" signature.
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
				var total int64
				for _, g := range grew {
					parts += fmt.Sprintf(" %s=%d", g.op, g.d)
					total += g.d
				}
				if isJukeboxStormWindow(total) {
					Log.Errorf("JUKEBOX-RATE (per ~15s):%s — a high retry rate is the 'error 100060' retry-storm signature", parts)
				} else {
					Log.Warnf("JUKEBOX-RETRY (per ~15s):%s — isolated idempotent retry recovered", parts)
				}
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
		pruneStallDumps(inflightDumpDir)
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

// Wire live stall telemetry into /metrics at package init.
//
// InflightStats' doc comment above says it is "Exposed for the metrics
// endpoint so external monitors can watch for a hang live". That was false
// until this init() existed: its only caller was inflightWatchdog, which does
// not speak until >=22s. snapshotJukebox was in the same state — one caller, a
// ~15s watchdog log line — and this mount runs with jukebox logging muted, so
// a JUKEBOX storm reached neither the user nor /metrics.
//
// Registering from init() rather than a wiring site is deliberate: the failure
// mode being fixed IS the forgotten wiring site. See nfs/fusedatagate.go for
// the same reasoning and [[feedback_architect_role]] "dead knobs".
func init() {
	metrics.Default().SetStallProvider(func() *metrics.StallSnapshot {
		count, oldestOp, oldestAge := InflightStats()
		jb := snapshotJukebox()
		var total int64
		for _, v := range jb {
			total += v
		}
		if len(jb) == 0 {
			jb = nil // omitempty: an empty map is noise, not a reading
		}
		return &metrics.StallSnapshot{
			Inflight:     count,
			OldestOp:     oldestOp,
			OldestAgeMs:  oldestAge.Milliseconds(),
			JukeboxByOp:  jb,
			JukeboxTotal: total,
		}
	})
}
