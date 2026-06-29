// Package metrics implements zero-dependency in-process metrics for the
// NFS server: per-RPC latency histograms, a few global counters, and a
// tiny HTTP server that exposes /metrics and /health as JSON.
//
// The histogram is a fixed bucketed log/linear approximation. It avoids
// pulling in Prometheus or HDR — totals and percentiles are derived from
// the bucket counts. Buckets are spaced to cover sub-microsecond fast
// paths up through multi-second slow RPCs.
package metrics

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// RPCType is the canonical name of an NFS procedure that we time.
type RPCType string

// Known RPC types. Keep this list in sync with the dispatch table in
// internal/nfs/nfs.go — anything not listed here will be silently
// recorded under "OTHER".
const (
	RPCGetAttr     RPCType = "GETATTR"
	RPCSetAttr     RPCType = "SETATTR"
	RPCLookup      RPCType = "LOOKUP"
	RPCAccess      RPCType = "ACCESS"
	RPCRead        RPCType = "READ"
	RPCWrite       RPCType = "WRITE"
	RPCCreate      RPCType = "CREATE"
	RPCRemove      RPCType = "REMOVE"
	RPCRename      RPCType = "RENAME"
	RPCMkdir       RPCType = "MKDIR"
	RPCRmdir       RPCType = "RMDIR"
	RPCReadDir     RPCType = "READDIR"
	RPCReadDirPlus RPCType = "READDIRPLUS"
	RPCFSStat      RPCType = "FSSTAT"
	RPCFSInfo      RPCType = "FSINFO"
	RPCPathConf    RPCType = "PATHCONF"
	RPCCommit      RPCType = "COMMIT"
	RPCOther       RPCType = "OTHER"
)

// trackedTypes is the ordered list of RPC types we expose, even when
// they have zero samples. Keeping the order stable makes JSON output
// pleasant to read.
var trackedTypes = []RPCType{
	RPCGetAttr, RPCSetAttr, RPCLookup, RPCAccess,
	RPCRead, RPCWrite, RPCCreate, RPCRemove, RPCRename,
	RPCMkdir, RPCRmdir, RPCReadDir, RPCReadDirPlus,
	RPCFSStat, RPCFSInfo, RPCPathConf, RPCCommit,
	RPCOther,
}

// numBuckets is the fixed number of latency buckets. Keeping this a
// const lets histogram.buckets be a stack-allocated array.
const numBuckets = 22

// histBuckets are bucket upper bounds in microseconds. The pattern is
// roughly 1, 2, 5 × 10^k from 1us to 10s. Anything slower lands in the
// final overflow bucket.
var histBuckets = [numBuckets]float64{
	1, 2, 5,
	10, 20, 50,
	100, 200, 500,
	1_000, 2_000, 5_000,
	10_000, 20_000, 50_000,
	100_000, 200_000, 500_000,
	1_000_000, 2_000_000, 5_000_000,
	10_000_000,
}

// histogram is a simple bucketed histogram with running totals.
// All fields are guarded by an atomic-only contract: the buckets array
// is updated via atomic.AddUint64 so there is no per-record lock.
type histogram struct {
	buckets [numBuckets]uint64
	count   atomic.Uint64
	sumUs   atomic.Uint64 // running sum of latencies in microseconds
	maxUs   atomic.Uint64 // monotonically rising max
}

func (h *histogram) record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	uus := uint64(us)
	h.count.Add(1)
	h.sumUs.Add(uus)
	for {
		cur := h.maxUs.Load()
		if uus <= cur {
			break
		}
		if h.maxUs.CompareAndSwap(cur, uus) {
			break
		}
	}

	// Find the first bucket whose upper bound >= us.
	idx := sort.SearchFloat64s(histBuckets[:], float64(us))
	if idx >= len(h.buckets) {
		idx = len(h.buckets) - 1
	}
	atomic.AddUint64(&h.buckets[idx], 1)
}

// snapshot returns a stable copy of the histogram state.
func (h *histogram) snapshot() histogramSnapshot {
	var out histogramSnapshot
	out.Count = h.count.Load()
	out.SumUs = h.sumUs.Load()
	out.MaxUs = h.maxUs.Load()
	for i := range h.buckets {
		out.Buckets[i] = atomic.LoadUint64(&h.buckets[i])
	}
	return out
}

type histogramSnapshot struct {
	Count   uint64
	SumUs   uint64
	MaxUs   uint64
	Buckets [numBuckets]uint64
}

// percentileUs returns an approximate percentile (0..1) in microseconds.
// We linearly interpolate inside the matching bucket to avoid a
// step-function look on small sample counts.
func (s histogramSnapshot) percentileUs(p float64) float64 {
	if s.Count == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(s.Count) * p))
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i, c := range s.Buckets {
		next := cum + c
		if target <= next {
			lo := 0.0
			if i > 0 {
				lo = histBuckets[i-1]
			}
			hi := histBuckets[i]
			if c == 0 {
				return hi
			}
			frac := float64(target-cum) / float64(c)
			return lo + frac*(hi-lo)
		}
		cum = next
	}
	// Should be unreachable, but fall back to the top bucket bound.
	return histBuckets[len(histBuckets)-1]
}

// Registry holds all metrics for a process. Tests and embedded uses can
// create their own; the server uses a global one through Default().
type Registry struct {
	startedAt time.Time

	histsMu sync.RWMutex
	hists   map[RPCType]*histogram

	// Global counters
	rpcTotal   atomic.Uint64
	rpcErrors  atomic.Uint64
	bytesRead  atomic.Uint64
	bytesWrite atomic.Uint64

	// Read-path resilience counters. A cold JuiceFS/MinIO chunk fetch under
	// heavy concurrent read load can return a TRANSIENT EIO that the read
	// path now retries (cachedFile.ReadAt) instead of surfacing as NFS3ERR_IO
	// — which the kernel turns into EIO for read() consumers and SIGBUS for
	// mmap consumers (many NLEs mmap media → crash), with no data lost.
	// readRetries counts retried subreads; readFails counts the ones that
	// still failed after exhausting retries (the genuinely-bad case worth an
	// alert). Both were SILENT before (rpc_errors stayed 0), which made the
	// concurrent-readback SIGBUS hard to diagnose (2026-06-14).
	readRetries atomic.Uint64
	readFails   atomic.Uint64

	// Health hook — set by main.go so /health can answer accurately.
	healthMu sync.RWMutex
	healthFn func() HealthSnapshot

	// Network hook — set by the bridge so /metrics can report the adaptive
	// link estimate (class, RTT, bandwidth, live readahead policy). Decoupled
	// via a provider so this package needn't import netprofile.
	netMu sync.RWMutex
	netFn func() *NetworkSnapshot
}

// NetworkSnapshot mirrors the netprofile link estimate for /metrics.
type NetworkSnapshot struct {
	Class           string  `json:"class"`
	RTTMs           float64 `json:"rtt_ms"`
	BandwidthMBps   float64 `json:"bandwidth_mbps"`
	HaveRTT         bool    `json:"have_rtt"`
	HaveBandwidth   bool    `json:"have_bandwidth"`
	ThroughputN     int64   `json:"throughput_samples"`
	BootstrappedRTT bool    `json:"bootstrapped_from_rtt"`
	// Live readahead policy derived from the class.
	ReadaheadEnabled bool `json:"readahead_enabled"`
	ReadaheadSeq     int  `json:"readahead_seq_threshold"`
	ReadaheadBlocks  int  `json:"readahead_blocks_ahead"`
	ReadaheadWorkers int  `json:"readahead_workers"`
}

// HealthSnapshot is the JSON-friendly payload returned by /health.
//
// `components` deliberately has NO `,omitempty`: handleHealth normalizes a nil
// map to {} before encoding, so the field is ALWAYS a JSON object — never null
// and never absent. A null or missing `components` makes the Swift HealthProbe
// decoder throw and abort the entire decode (the same class of bug as
// CacheStatus roots:null — the stuck offline toggle).
type HealthSnapshot struct {
	Healthy    bool              `json:"healthy"`
	Components map[string]string `json:"components"`
	Reason     string            `json:"reason,omitempty"`
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		startedAt: time.Now(),
		hists:     make(map[RPCType]*histogram),
	}
}

var defaultRegistry = NewRegistry()

// Default returns the process-wide registry.
func Default() *Registry { return defaultRegistry }

// SetHealthProvider registers a callback used by /health.
func (r *Registry) SetHealthProvider(fn func() HealthSnapshot) {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	r.healthFn = fn
}

// SetNetworkProvider registers a callback used by /metrics to report the
// adaptive link estimate. Safe to leave unset (the field is then omitted).
func (r *Registry) SetNetworkProvider(fn func() *NetworkSnapshot) {
	r.netMu.Lock()
	defer r.netMu.Unlock()
	r.netFn = fn
}

// histFor returns (and lazily creates) the histogram for an RPC type.
func (r *Registry) histFor(t RPCType) *histogram {
	r.histsMu.RLock()
	h := r.hists[t]
	r.histsMu.RUnlock()
	if h != nil {
		return h
	}
	r.histsMu.Lock()
	defer r.histsMu.Unlock()
	if h = r.hists[t]; h != nil {
		return h
	}
	h = &histogram{}
	r.hists[t] = h
	return h
}

// Observe records a single RPC's outcome. Pass err != nil on failure.
func (r *Registry) Observe(t RPCType, d time.Duration, err error) {
	r.histFor(t).record(d)
	r.rpcTotal.Add(1)
	if err != nil {
		r.rpcErrors.Add(1)
	}
}

// AddBytesRead increments the read-bytes counter.
func (r *Registry) AddBytesRead(n int64) {
	if n > 0 {
		r.bytesRead.Add(uint64(n))
	}
}

// AddBytesWritten increments the written-bytes counter.
func (r *Registry) AddBytesWritten(n int64) {
	if n > 0 {
		r.bytesWrite.Add(uint64(n))
	}
}

// IncReadRetry records that a transient cold-read EIO was retried.
func (r *Registry) IncReadRetry() { r.readRetries.Add(1) }

// IncReadFail records that a cold read still failed after exhausting retries
// (the genuinely-bad case — bytes were surfaced to the client as an error).
func (r *Registry) IncReadFail() { r.readFails.Add(1) }

// Snapshot is the JSON shape returned by /metrics.
type Snapshot struct {
	UptimeSec    int64                  `json:"uptime_sec"`
	RPCTotal     uint64                 `json:"rpc_total"`
	RPCErrors    uint64                 `json:"rpc_errors"`
	BytesRead    uint64                 `json:"bytes_read"`
	BytesWritten uint64                 `json:"bytes_written"`
	ReadRetries  uint64                 `json:"read_retries"`
	ReadFails    uint64                 `json:"read_fails"`
	RPCs         map[string]RPCSnapshot `json:"rpcs"`
	Network      *NetworkSnapshot       `json:"network,omitempty"`
}

// RPCSnapshot is the per-RPC JSON shape.
type RPCSnapshot struct {
	Count  uint64  `json:"count"`
	MeanUs float64 `json:"mean_us"`
	MaxUs  uint64  `json:"max_us"`
	P50Us  float64 `json:"p50_us"`
	P95Us  float64 `json:"p95_us"`
	P99Us  float64 `json:"p99_us"`
}

// Snapshot builds a self-contained metrics view.
func (r *Registry) Snapshot() Snapshot {
	out := Snapshot{
		UptimeSec:    int64(time.Since(r.startedAt).Seconds()),
		RPCTotal:     r.rpcTotal.Load(),
		RPCErrors:    r.rpcErrors.Load(),
		BytesRead:    r.bytesRead.Load(),
		BytesWritten: r.bytesWrite.Load(),
		ReadRetries:  r.readRetries.Load(),
		ReadFails:    r.readFails.Load(),
		RPCs:         make(map[string]RPCSnapshot, len(trackedTypes)),
	}

	r.netMu.RLock()
	netFn := r.netFn
	r.netMu.RUnlock()
	if netFn != nil {
		out.Network = netFn()
	}

	r.histsMu.RLock()
	defer r.histsMu.RUnlock()

	// Always emit the canonical types so the JSON shape is stable, even
	// before the first sample arrives.
	emitted := make(map[RPCType]struct{}, len(trackedTypes))
	for _, t := range trackedTypes {
		emitted[t] = struct{}{}
		h := r.hists[t]
		if h == nil {
			out.RPCs[string(t)] = RPCSnapshot{}
			continue
		}
		out.RPCs[string(t)] = makeRPCSnapshot(h.snapshot())
	}
	// Anything else seen at runtime that's not in the canonical list
	// (defensive — should not happen with the current dispatch).
	for t, h := range r.hists {
		if _, ok := emitted[t]; ok {
			continue
		}
		out.RPCs[string(t)] = makeRPCSnapshot(h.snapshot())
	}
	return out
}

func makeRPCSnapshot(s histogramSnapshot) RPCSnapshot {
	if s.Count == 0 {
		return RPCSnapshot{}
	}
	mean := float64(s.SumUs) / float64(s.Count)
	return RPCSnapshot{
		Count:  s.Count,
		MeanUs: mean,
		MaxUs:  s.MaxUs,
		P50Us:  s.percentileUs(0.50),
		P95Us:  s.percentileUs(0.95),
		P99Us:  s.percentileUs(0.99),
	}
}

// Server runs an HTTP listener that exposes /metrics and /health.
type Server struct {
	registry *Registry
	addr     string
	listener net.Listener
	httpSrv  *http.Server

	// ExtraRoutes lets callers (e.g. cbridge) register additional handlers
	// on the same listener — Pin/Unpin/CacheStatus/Offline endpoints live
	// here. Set BEFORE calling Start().
	ExtraRoutes map[string]http.HandlerFunc
}

// NewServer creates a metrics HTTP server bound to addr (e.g. 127.0.0.1:11050).
// The listener is opened by Start().
func NewServer(addr string, reg *Registry) *Server {
	if reg == nil {
		reg = Default()
	}
	return &Server{addr: addr, registry: reg}
}

// Addr returns the actual listening address (after Start).
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Start binds the listener and serves on a background goroutine.
func (s *Server) Start() error {
	if s.addr == "" {
		return fmt.Errorf("metrics: empty address")
	}
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("metrics listen %s: %w", s.addr, err)
	}
	s.listener = l

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/health", s.handleHealth)
	for path, h := range s.ExtraRoutes {
		mux.HandleFunc(path, h)
	}
	mux.HandleFunc("/", s.handleIndex) // catch-all last

	s.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = s.httpSrv.Serve(l)
	}()
	return nil
}

// Stop closes the HTTP server and listener.
func (s *Server) Stop() {
	if s.httpSrv != nil {
		_ = s.httpSrv.Close()
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(s.registry.Snapshot())
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.registry.healthMu.RLock()
	fn := s.registry.healthFn
	s.registry.healthMu.RUnlock()

	var snap HealthSnapshot
	if fn != nil {
		snap = fn()
	} else {
		// No provider yet — assume healthy if the server is up.
		snap = HealthSnapshot{Healthy: true}
	}
	// Never emit a null or absent `components`: a nil Go map marshals to JSON
	// null, which makes the Swift HealthProbe decoder throw valueNotFound and
	// abort the whole decode (same root cause as CacheStatus roots:null — the
	// stuck offline toggle). Normalize to an empty object so the JSON is always
	// `"components": {}` for the fn==nil and monitor-stopped paths.
	if snap.Components == nil {
		snap.Components = map[string]string{}
	}

	w.Header().Set("Content-Type", "application/json")
	if !snap.Healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(snap)
}

// handleIndex serves the human-readable route banner at the exact root path
// and returns 404 for anything else. This handler is registered against the
// ServeMux subtree pattern "/", so it receives every request that no
// longer-prefix exact route (/metrics, /health, /whoami, /residency, the
// /manager and /debug/pprof subtrees, …) already claimed. Without the
// path guard below, a truly-unknown route such as GET /definitely-not-a-real-
// route would fall through here and get an HTTP 200 — the "catch-all 200 trap"
// OpenLoupe flagged: capability probing can't distinguish a served route from
// an unserved one. Registered routes are unaffected because ServeMux always
// dispatches the longest matching pattern, so they never reach this function.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = fmt.Fprint(w, "JuiceMount metrics\n  /metrics\n  /health\n")
}
