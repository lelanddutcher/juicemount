package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Status reports the proxy state for a given source file.
type Status int

const (
	StatusUnknown    Status = iota
	StatusReady             // proxy file exists and is usable
	StatusGenerating        // a worker is currently producing the proxy
	StatusFailed            // last attempt failed; will retry on next Get
	StatusNotProxyable      // codec doesn't qualify for proxying
)

func (s Status) String() string {
	switch s {
	case StatusReady:
		return "ready"
	case StatusGenerating:
		return "generating"
	case StatusFailed:
		return "failed"
	case StatusNotProxyable:
		return "not-proxyable"
	default:
		return "unknown"
	}
}

// Result is what Get returns. Callers inspect ProxyPath if Status == Ready.
type Result struct {
	Status      Status
	ProxyPath   string        // populated when Status == Ready
	Codec       Codec         // detected codec
	Duration    time.Duration // last generation duration (0 if cached)
	Err         error         // populated when Status == Failed
}

// Manager owns the proxy cache directory and the worker pool.
// Safe for concurrent use.
type Manager struct {
	cacheDir string
	workers  int
	jobs     chan jobRequest

	// in-flight tracking so concurrent Gets for the same file coalesce
	mu          sync.Mutex
	inFlight    map[string]*sync.WaitGroup // cacheKey -> wg
	recentFails map[string]time.Time       // cacheKey -> when it last failed (to back off retries)

	// stats — read via Stats()
	statsMu      sync.Mutex
	totalGen     int64
	totalCached  int64
	totalFailed  int64
	totalDurNs   int64

	closeOnce sync.Once
	closed    chan struct{}
}

type jobRequest struct {
	spec  ProxySpec
	done  chan jobResult
	cacheKey string
}

type jobResult struct {
	dur time.Duration
	err error
}

// NewManager creates a proxy manager. Pass an empty cacheDir to use the
// default `~/Library/Caches/JuiceMount/proxies/`. Workers defaults to a
// hardware-friendly value if 0.
func NewManager(cacheDir string, workers int) (*Manager, error) {
	if cacheDir == "" {
		cacheDir = defaultCacheDir()
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir cache: %w", err)
	}
	if workers <= 0 {
		workers = recommendedWorkerCount()
	}

	m := &Manager{
		cacheDir:    cacheDir,
		workers:     workers,
		jobs:        make(chan jobRequest, 64),
		inFlight:    make(map[string]*sync.WaitGroup),
		recentFails: make(map[string]time.Time),
		closed:      make(chan struct{}),
	}

	// Spawn worker goroutines
	for i := 0; i < workers; i++ {
		go m.workerLoop()
	}

	return m, nil
}

// Stop closes the worker pool. Pending jobs in the channel are dropped.
// In-flight ffmpeg processes are NOT killed (caller must context-cancel).
func (m *Manager) Stop() {
	m.closeOnce.Do(func() {
		close(m.closed)
		close(m.jobs)
	})
}

func defaultCacheDir() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, "Library", "Caches", "JuiceMount", "proxies")
	}
	return "/tmp/juicemount-proxies"
}

// CacheKeyFor computes the cache key for a source file. Includes path +
// size + mtime so an in-place overwrite invalidates the cached proxy.
func (m *Manager) CacheKeyFor(srcPath string) (string, error) {
	info, err := os.Stat(srcPath)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%d\x00%d", srcPath, info.Size(), info.ModTime().UnixNano())
	return hex.EncodeToString(h.Sum(nil)[:16]), nil // 32-char hex prefix is plenty
}

// proxyPathFor returns the on-disk path for a given cache key.
func (m *Manager) proxyPathFor(cacheKey string) string {
	// Shard by first 2 chars to keep any single dir under ~256 entries
	return filepath.Join(m.cacheDir, cacheKey[:2], cacheKey+".mp4")
}

// Get returns a Result for the given source file. Behavior:
//   - If the file isn't a proxyable codec → StatusNotProxyable.
//   - If a proxy exists on disk for the current (path,size,mtime) → StatusReady (instant).
//   - If a proxy is currently being generated → StatusGenerating.
//   - Otherwise: enqueues a generation job and returns StatusGenerating.
//
// This is non-blocking. To wait for generation to complete, call GetBlocking.
func (m *Manager) Get(srcPath string) Result {
	codec := DetectByExtension(srcPath)
	if codec == CodecUnknown {
		// Try the slower path that opens the file
		if c, err := DetectByMagic(srcPath); err == nil {
			codec = c
		}
	}
	if codec == CodecUnknown {
		return Result{Status: StatusNotProxyable, Codec: codec}
	}

	cacheKey, err := m.CacheKeyFor(srcPath)
	if err != nil {
		return Result{Status: StatusFailed, Codec: codec, Err: err}
	}

	proxyPath := m.proxyPathFor(cacheKey)
	if info, err := os.Stat(proxyPath); err == nil && info.Size() > 0 {
		m.bumpStats(0, true, false)
		return Result{Status: StatusReady, Codec: codec, ProxyPath: proxyPath}
	}

	// Not cached — check if already in flight, or recently failed (back off
	// to avoid retry storms when ffmpeg can't decode a malformed source)
	m.mu.Lock()
	if _, busy := m.inFlight[cacheKey]; busy {
		m.mu.Unlock()
		return Result{Status: StatusGenerating, Codec: codec, ProxyPath: proxyPath}
	}
	if failedAt, ok := m.recentFails[cacheKey]; ok && time.Since(failedAt) < failureBackoff {
		m.mu.Unlock()
		return Result{Status: StatusFailed, Codec: codec, ProxyPath: proxyPath,
			Err: fmt.Errorf("recent failure; backoff until %v", failedAt.Add(failureBackoff))}
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	m.inFlight[cacheKey] = wg
	m.mu.Unlock()

	// Enqueue. Non-blocking; if the queue is full, fall back to synchronous
	// — this shouldn't happen in practice (queue is 64 deep, workers drain).
	spec := DefaultSpec(srcPath, proxyPath, codec)
	req := jobRequest{spec: spec, cacheKey: cacheKey}
	select {
	case m.jobs <- req:
		// queued — worker will signal completion via the inFlight WG
	default:
		// queue full — clear the in-flight marker and return generating
		// (caller will retry; load-shed scenario)
		m.mu.Lock()
		delete(m.inFlight, cacheKey)
		m.mu.Unlock()
		wg.Done()
	}

	return Result{Status: StatusGenerating, Codec: codec, ProxyPath: proxyPath}
}

// GetBlocking is Get but waits for any in-flight generation to complete.
// Useful in tests; not recommended for the NFS hot path (use Get + retry).
func (m *Manager) GetBlocking(ctx context.Context, srcPath string) Result {
	for {
		r := m.Get(srcPath)
		if r.Status != StatusGenerating {
			return r
		}
		// Wait briefly and re-check
		select {
		case <-ctx.Done():
			return Result{Status: StatusFailed, Codec: r.Codec, Err: ctx.Err()}
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// workerLoop pulls jobs and runs them. Exits when m.jobs is closed.
func (m *Manager) workerLoop() {
	for req := range m.jobs {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		dur, err := req.spec.Generate(ctx)
		cancel()

		// Mark in-flight done; if it failed, record for backoff
		m.mu.Lock()
		if wg, ok := m.inFlight[req.cacheKey]; ok {
			wg.Done()
			delete(m.inFlight, req.cacheKey)
		}
		if err != nil {
			m.recentFails[req.cacheKey] = time.Now()
		} else {
			delete(m.recentFails, req.cacheKey) // success clears any prior failure
		}
		m.mu.Unlock()

		m.bumpStats(dur, false, err != nil)
	}
}

// failureBackoff is how long we suppress retries after a failed proxy
// generation. Prevents retry storms when GetBlocking polls every 100ms
// and ffmpeg fails fast (e.g. malformed source — the policy is "give the
// user a chance to replace the file before we waste cycles").
const failureBackoff = 30 * time.Second

func (m *Manager) bumpStats(dur time.Duration, cached bool, failed bool) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	if cached {
		m.totalCached++
	} else if failed {
		m.totalFailed++
	} else {
		m.totalGen++
		m.totalDurNs += dur.Nanoseconds()
	}
}

// Stats returns counter snapshots.
type Stats struct {
	TotalGenerated int64
	TotalCached    int64
	TotalFailed    int64
	AvgGenMs       float64
	Workers        int
	CacheDir       string
}

func (m *Manager) Stats() Stats {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	avg := 0.0
	if m.totalGen > 0 {
		avg = float64(m.totalDurNs) / float64(m.totalGen) / 1e6
	}
	return Stats{
		TotalGenerated: m.totalGen,
		TotalCached:    m.totalCached,
		TotalFailed:    m.totalFailed,
		AvgGenMs:       avg,
		Workers:        m.workers,
		CacheDir:       m.cacheDir,
	}
}
