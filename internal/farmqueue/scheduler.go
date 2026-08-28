package farmqueue

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strings"
)

const (
	QueueClassServer = "server"
	QueueClassRender = "render"
	QueueClassCPU    = "cpu"
)

func classQueue(kind, class string) string {
	return QueueKey + ":" + kind + ":" + class
}

// RouteInitialJob separates recursive discovery from accelerated execution.
// A fresh derivative/proxy/transcript request has not yet proven whether Path
// is one file or a directory containing thousands, so it first goes to the server's
// metadata lane. Bounded children (ShardCount > 0), exact retry subsets, and
// explicit fallback jobs bypass this planner and are routed for execution.
func (c *Client) RouteInitialJob(ctx context.Context, j *Job) {
	if j == nil {
		return
	}
	if j.PlanOnly || shouldPlanOnServer(*j) {
		j.PlanOnly = true
		j.QueueClass = QueueClassServer
		j.SelectedBackend = "server-dispatch"
		j.SelectedWorker = ""
		j.RequiredCapabilities = []string{"metadata"}
		return
	}
	c.RouteJob(ctx, j)
}

func shouldPlanOnServer(j Job) bool {
	if len(j.Kinds) != 1 || j.ShardCount > 0 || len(j.RetryTargets) > 0 {
		return false
	}
	if j.Kinds[0] == KindDerivatives && j.DerivativePass != "" {
		return false
	}
	return j.Kinds[0] == KindDerivatives || j.Kinds[0] == KindProxy || j.Kinds[0] == KindTranscript
}

// RouteJob selects a concrete execution lane from verified live worker
// profiles. Hardware HEVC wins, then hardware H.264; software H.264 is the
// fallback only when no render worker reports a working encoder.
func (c *Client) RouteJob(ctx context.Context, j *Job) {
	if j == nil || len(j.Kinds) != 1 {
		return
	}
	route, err := c.SnapshotRouter(ctx)
	if err != nil {
		// Direct legacy callers cannot return a routing error. Keep their old
		// explicit CPU behavior, but make the uncertainty visible. Server planner
		// paths use SnapshotRouter directly and retry instead of mass-demoting.
		routeJobWithWorkers(j, nil, nil)
		j.RoutingReason = "worker discovery unavailable while routing: " + err.Error()
		return
	}
	route(j)
}

// SnapshotRouter reads live worker profiles and queued load once, then returns
// a plan-local router. Directory planning may create thousands of one-file
// children; re-reading Redis for every source would make the metadata lane the
// bottleneck. The closure increments its private load estimate after each
// assignment so measured batching still spreads work across eligible nodes.
func (c *Client) SnapshotRouter(ctx context.Context) (func(*Job), error) {
	workers, err := c.ActiveWorkers(ctx)
	if err != nil {
		return nil, err
	}
	loads := c.queuedWorkerLoads(ctx, workers)
	return func(j *Job) {
		if j == nil || len(j.Kinds) != 1 {
			return
		}
		routeJobWithWorkers(j, workers, loads)
		for _, capability := range j.RequiredCapabilities {
			if id, ok := strings.CutPrefix(capability, "worker:"); ok && id != "" {
				loads[id]++
				break
			}
		}
	}, nil
}

func routeJobWithWorkers(j *Job, workers []Worker, loads map[string]int) {
	kind := j.Kinds[0]
	switch kind {
	case KindDerivatives:
		if j.DerivativePass == DerivativePassPreviews {
			if selected, decoder, ok := preferredHardwareDecoderWorkerWithLoad(workers, j.SourceVideoCodec, loads); ok {
				j.QueueClass = QueueClassRender
				j.SelectedBackend = decoder
				j.SelectedWorker = workerDisplayName(selected)
				j.RequiredCapabilities = []string{"decoder:" + decoder, "worker:" + selected.ID}
				j.RoutingReason = ""
				return
			}
			j.QueueClass = QueueClassCPU
			j.SelectedBackend = "cpu-decode"
			j.SelectedWorker = ""
			j.RequiredCapabilities = []string{"cpu"}
			if j.RoutingReason == "" {
				j.RoutingReason = "no verified hardware decoder is online for " + displayCodec(j.SourceVideoCodec)
			}
			return
		}
		j.QueueClass = QueueClassServer
		j.SelectedBackend = "server-metadata"
		j.RequiredCapabilities = []string{"metadata"}
	case KindProxy:
		if selected, enc, ok := preferredHardwareWorkerWithLoad(workers, loads); ok {
			j.QueueClass = QueueClassRender
			j.VCodec = enc
			j.SelectedBackend = enc
			j.SelectedWorker = workerDisplayName(selected)
			j.RequiredCapabilities = []string{"encoder:" + enc, "worker:" + selected.ID}
			return
		}
		j.QueueClass = QueueClassCPU
		j.VCodec = "libx264"
		j.SelectedBackend = "libx264"
		j.SelectedWorker = ""
		j.RequiredCapabilities = []string{"cpu"}
	case KindTranscript:
		if selected, backend, ok := preferredTranscriptWorkerWithLoad(workers, loads); ok {
			j.QueueClass = QueueClassRender
			j.SelectedBackend = backend
			j.SelectedWorker = workerDisplayName(selected)
			j.RequiredCapabilities = []string{"transcript:" + backend, "worker:" + selected.ID}
			return
		}
		j.QueueClass = QueueClassCPU
		j.SelectedBackend = "cpu"
		j.SelectedWorker = ""
		j.RequiredCapabilities = []string{"cpu"}
	}
}

func preferredHardwareDecoderWorkerWithLoad(workers []Worker, codec string, loads map[string]int) (Worker, string, bool) {
	codec = strings.ToLower(strings.TrimSpace(codec))
	if codec != "h264" && codec != "hevc" {
		return Worker{}, "", false
	}
	type candidate struct {
		worker  Worker
		decoder string
		cost    float64
	}
	var candidates []candidate
	for _, w := range workers {
		if w.Role != QueueClassRender {
			continue
		}
		for _, decoder := range w.Decoders {
			if strings.HasPrefix(decoder, codec+"_") {
				candidates = append(candidates, candidate{
					worker: w, decoder: decoder,
					cost: workerQueueCost(w, KindDerivatives, loads[w.ID]),
				})
			}
		}
	}
	if len(candidates) == 0 {
		return Worker{}, "", false
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].cost < candidates[j].cost })
	return candidates[0].worker, candidates[0].decoder, true
}

func displayCodec(codec string) string {
	if codec = strings.TrimSpace(codec); codec != "" {
		return codec
	}
	return "this source codec"
}

func preferredHardwareWorker(workers []Worker) (Worker, string, bool) {
	return preferredHardwareWorkerWithLoad(workers, nil)
}

func preferredHardwareWorkerWithLoad(workers []Worker, loads map[string]int) (Worker, string, bool) {
	for _, family := range []string{"hevc_", "h264_"} {
		type candidate struct {
			worker  Worker
			encoder string
			cost    float64
		}
		var candidates []candidate
		for _, w := range workers {
			if w.Role != QueueClassRender {
				continue
			}
			for _, enc := range w.Encoders {
				if strings.HasPrefix(enc, family) && enc != "libx265" && enc != "libx264" {
					candidates = append(candidates, candidate{
						worker: w, encoder: enc,
						cost: workerQueueCost(w, KindProxy, loads[w.ID]),
					})
					break
				}
			}
		}
		if len(candidates) > 0 {
			sort.SliceStable(candidates, func(i, j int) bool {
				return candidates[i].cost < candidates[j].cost
			})
			return candidates[0].worker, candidates[0].encoder, true
		}
	}
	return Worker{}, "", false
}

func preferredHardwareEncoder(workers []Worker) (string, bool) {
	_, encoder, ok := preferredHardwareWorker(workers)
	return encoder, ok
}

func preferredTranscriptWorker(workers []Worker) (Worker, string, bool) {
	return preferredTranscriptWorkerWithLoad(workers, nil)
}

func preferredTranscriptWorkerWithLoad(workers []Worker, loads map[string]int) (Worker, string, bool) {
	type candidate struct {
		worker  Worker
		backend string
		cost    float64
	}
	var candidates []candidate
	for _, w := range workers {
		if w.Role != QueueClassRender {
			continue
		}
		for _, backend := range w.TranscriptBackends {
			if backend != "" && backend != "cpu" {
				candidates = append(candidates, candidate{
					worker: w, backend: backend,
					cost: workerQueueCost(w, KindTranscript, loads[w.ID]),
				})
				break
			}
		}
	}
	if len(candidates) > 0 {
		sort.SliceStable(candidates, func(i, j int) bool {
			return candidates[i].cost < candidates[j].cost
		})
		return candidates[0].worker, candidates[0].backend, true
	}
	return Worker{}, "", false
}

// workerQueueCost estimates time-to-finish for one more job on a worker. The
// codec family is selected before this function is used, so measured capacity
// only balances jobs among equally preferred HEVC (or H.264) nodes; load can
// never downgrade a job from HEVC merely because an H.264 device is idle.
func workerQueueCost(w Worker, kind string, queued int) float64 {
	if queued < 0 {
		queued = 0
	}
	if w.CurrentJob != "" {
		queued++
	}

	seconds := 0.0
	switch kind {
	case KindProxy:
		if w.Benchmarks.ProxyJobsCompleted > 0 && w.Benchmarks.LastProxySeconds > 0 {
			seconds = w.Benchmarks.LastProxySeconds
		} else {
			throughput := positiveMin(w.Benchmarks.EncodeFPS, w.Benchmarks.DecodeFPS)
			if throughput <= 0 {
				throughput = positiveMax(w.Benchmarks.EncodeFPS, w.Benchmarks.DecodeFPS)
			}
			if throughput <= 0 {
				throughput = 1
			}
			// A 300-frame reference clip turns FPS into an estimated duration.
			seconds = 300 / throughput
		}
	case KindTranscript:
		if w.Benchmarks.TranscriptJobsCompleted > 0 && w.Benchmarks.LastTranscriptSeconds > 0 {
			seconds = w.Benchmarks.LastTranscriptSeconds
		} else {
			rate := w.Benchmarks.TranscriptXReal
			if rate <= 0 {
				rate = 0.1
			}
			seconds = 60 / rate // one reference minute of source audio
		}
	case KindDerivatives:
		throughput := w.Benchmarks.DecodeFPS
		if throughput <= 0 {
			throughput = 1
		}
		// A 300-frame reference strip turns measured decoder FPS into time.
		seconds = 300 / throughput
	default:
		seconds = w.Benchmarks.LastJobSeconds
		if seconds <= 0 {
			seconds = 1
		}
	}
	// Real mount throughput is a shared bottleneck for decode, encode, and AI.
	// Penalize slow/unknown access without letting a synthetic cache-speed spike
	// dominate the actual codec benchmark.
	if w.Benchmarks.AccessMBps > 0 {
		seconds *= 1 + 100/math.Min(w.Benchmarks.AccessMBps, 1000)
	} else {
		seconds *= 2
	}

	// Synthetic speed alone can make a flaky accelerator look attractive. Fold
	// observed file-level reliability into the same measured cost so a slightly
	// slower node that consistently publishes valid output wins over a fast node
	// that repeatedly fails admissions or encodes. Unknown history is neutral.
	processed, failed := w.Benchmarks.FilesProcessed, w.Benchmarks.FilesFailed
	switch kind {
	case KindProxy:
		processed, failed = w.Benchmarks.ProxyFilesProcessed, w.Benchmarks.ProxyFilesFailed
	case KindTranscript:
		processed, failed = w.Benchmarks.TranscriptFilesProcessed, w.Benchmarks.TranscriptFilesFailed
	}
	if attempted := processed + failed; attempted > 0 && failed > 0 {
		failureRate := float64(failed) / float64(attempted)
		seconds *= 1 + 4*failureRate
	}
	return float64(queued+1) * seconds
}

func positiveMin(values ...float64) float64 {
	min := 0.0
	for _, value := range values {
		if value > 0 && (min == 0 || value < min) {
			min = value
		}
	}
	return min
}

func positiveMax(values ...float64) float64 {
	max := 0.0
	for _, value := range values {
		if value > max {
			max = value
		}
	}
	return max
}

// queuedWorkerLoads counts ready jobs already pinned to each exact worker.
// Running work is represented by Worker.CurrentJob in workerQueueCost. The
// count is best-effort: a Redis read error falls back to live benchmark-only
// routing, never to an unverified worker or a different codec class.
func (c *Client) queuedWorkerLoads(ctx context.Context, workers []Worker) map[string]int {
	loads := make(map[string]int, len(workers))
	for _, key := range []string{
		classQueue(KindDerivatives, QueueClassRender),
		classQueue(KindProxy, QueueClassRender),
		classQueue(KindTranscript, QueueClassRender),
	} {
		raws, err := c.rdb.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			continue
		}
		for _, raw := range raws {
			var job Job
			if json.Unmarshal([]byte(raw), &job) != nil {
				continue
			}
			for _, required := range job.RequiredCapabilities {
				if id, ok := strings.CutPrefix(required, "worker:"); ok && id != "" {
					loads[id]++
					break
				}
			}
		}
	}
	return loads
}

func preferredTranscriptBackend(workers []Worker) (string, bool) {
	_, backend, ok := preferredTranscriptWorker(workers)
	return backend, ok
}

func workerDisplayName(w Worker) string {
	if w.Name != "" {
		return w.Name
	}
	return w.ID
}

// WorkerQueueKeys returns only lanes this worker is allowed to execute. A
// render node never drains the CPU fallback lane, and a server node never
// races a render node for accelerator work.
func WorkerQueueKeys(w Worker) []string {
	var keys []string
	switch w.Role {
	case QueueClassServer:
		keys = append(keys,
			classQueue(KindDerivatives, QueueClassServer),
			classQueue(KindDerivatives, QueueClassCPU),
			classQueue(KindProxy, QueueClassServer),
			classQueue(KindTranscript, QueueClassServer),
			classQueue(KindProxy, QueueClassCPU),
			classQueue(KindTranscript, QueueClassCPU))
	case QueueClassRender:
		if len(w.Decoders) > 0 {
			keys = append(keys, classQueue(KindDerivatives, QueueClassRender))
		}
		if len(w.Encoders) > 0 {
			keys = append(keys, classQueue(KindProxy, QueueClassRender))
		}
		if backend, ok := preferredTranscriptBackend([]Worker{w}); ok && backend != "" {
			keys = append(keys, classQueue(KindTranscript, QueueClassRender))
		}
	default:
		// Legacy workers retain the v1 subscription contract.
		keys = append(keys, QueueKeysForKinds(w.Kinds)...)
		return uniqueStrings(keys)
	}
	// Legacy catch-all remains a compatibility lane. Server owns it so an old
	// multi-kind producer cannot send a render-only job to an arbitrary node.
	if w.Role == QueueClassServer {
		keys = append(keys, QueueKey)
	}
	return uniqueStrings(keys)
}

func workerCapabilitySet(w Worker) map[string]bool {
	set := map[string]bool{"cpu": true}
	if w.ID != "" {
		set["worker:"+w.ID] = true
	}
	for _, cap := range w.Capabilities {
		set[cap] = true
	}
	for _, enc := range w.Encoders {
		set["encoder:"+enc] = true
	}
	for _, decoder := range w.Decoders {
		set["decoder:"+decoder] = true
	}
	for _, backend := range w.TranscriptBackends {
		set["transcript:"+backend] = true
	}
	if w.Role == QueueClassServer {
		set["metadata"] = true
	}
	return set
}

func WorkerSupports(w Worker, required []string) bool {
	set := workerCapabilitySet(w)
	for _, req := range required {
		if !set[req] {
			return false
		}
	}
	return true
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
