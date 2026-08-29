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
			if id := workerIDForPin(workers, capability); id != "" {
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
			if selected, decoder, ok := preferredHardwareDecoderWorkerForSourceWithLoad(
				workers, j.SourceVideoCodec, j.SourceVideoProfile, j.SourcePixelFormat, j.SourceVideoWidth, j.SourceVideoHeight, loads,
			); ok {
				j.QueueClass = QueueClassRender
				j.SelectedBackend = decoder
				j.SelectedWorker = workerDisplayName(selected)
				j.RequiredCapabilities = append(
					DecoderSourceRequirements(decoder, j.SourceVideoCodec, j.SourceVideoProfile, j.SourcePixelFormat),
					workerPinCapability(selected),
				)
				j.RoutingReason = ""
				return
			}
			j.QueueClass = QueueClassCPU
			j.SelectedBackend = "cpu-decode"
			j.SelectedWorker = ""
			j.RequiredCapabilities = []string{"cpu"}
			if j.RoutingReason == "" {
				j.RoutingReason = "no verified hardware decoder is online for " + displayCodecProfile(j.SourceVideoCodec, j.SourceVideoProfile)
			}
			return
		}
		j.QueueClass = QueueClassServer
		j.SelectedBackend = "server-metadata"
		j.RequiredCapabilities = []string{"metadata"}
	case KindProxy:
		// Source-aware proxy children are planned from live ffprobe data on the
		// server. Admit them only when one worker has proved both halves of the
		// hardware path: decoding this source codec/profile and encoding the
		// preferred HEVC (or, if unavailable, H.264) output. Legacy bounded jobs
		// without source metadata retain encoder-only routing and are rechecked by
		// the render worker's live admission probe before user media is touched.
		if strings.TrimSpace(j.SourceVideoCodec) != "" {
			if selected, enc, decoder, ok := preferredProxyWorkerForSourceWithLoad(
				workers, j.SourceVideoCodec, j.SourceVideoProfile, j.SourcePixelFormat, j.SourceVideoWidth, j.SourceVideoHeight, loads,
			); ok {
				j.QueueClass = QueueClassRender
				j.VCodec = enc
				j.SelectedBackend = enc
				j.SelectedWorker = workerDisplayName(selected)
				j.RequiredCapabilities = append([]string{"encoder:" + enc}, DecoderSourceRequirements(decoder, j.SourceVideoCodec, j.SourceVideoProfile, j.SourcePixelFormat)...)
				j.RequiredCapabilities = append(j.RequiredCapabilities, workerPinCapability(selected))
				j.RoutingReason = ""
				return
			}
			j.QueueClass = QueueClassCPU
			j.VCodec = "libx264"
			j.SelectedBackend = "libx264"
			j.SelectedWorker = ""
			j.RequiredCapabilities = []string{"cpu"}
			if j.RoutingReason == "" {
				j.RoutingReason = "no verified hardware decode+encode path is online for " + displayCodecProfile(j.SourceVideoCodec, j.SourceVideoProfile)
			}
			return
		}
		if selected, enc, ok := preferredHardwareWorkerWithLoad(workers, loads); ok {
			j.QueueClass = QueueClassRender
			j.VCodec = enc
			j.SelectedBackend = enc
			j.SelectedWorker = workerDisplayName(selected)
			j.RequiredCapabilities = []string{"encoder:" + enc, workerPinCapability(selected)}
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
			j.RequiredCapabilities = []string{"transcript:" + backend, workerPinCapability(selected)}
			return
		}
		j.QueueClass = QueueClassCPU
		j.SelectedBackend = "cpu"
		j.SelectedWorker = ""
		j.RequiredCapabilities = []string{"cpu"}
	}
}

// preferredProxyWorkerWithLoad selects one end-to-end hardware path. Encoder
// preference remains HEVC then H.264, but an encoder cannot admit a source by
// itself: the same worker must expose the matching-family decoder and the exact
// measured source profile capability.
func preferredProxyWorkerWithLoad(workers []Worker, codec, profile string, loads map[string]int) (Worker, string, string, bool) {
	return preferredProxyWorkerForSourceWithLoad(workers, codec, profile, "", 0, 0, loads)
}

func preferredProxyWorkerForSourceWithLoad(workers []Worker, codec, profile, pixelFormat string, width, height int, loads map[string]int) (Worker, string, string, bool) {
	codec = strings.ToLower(strings.TrimSpace(codec))
	switch codec {
	case "avc1":
		codec = "h264"
	case "h265", "hev1", "hvc1":
		codec = "hevc"
	case "av01":
		codec = "av1"
	}
	if codec != "h264" && codec != "hevc" && codec != "av1" {
		return Worker{}, "", "", false
	}
	type candidate struct {
		worker  Worker
		encoder string
		decoder string
		cost    float64
	}
	for _, encoderPrefix := range []string{"hevc_", "h264_"} {
		var candidates []candidate
		for _, w := range workers {
			if w.Role != QueueClassRender {
				continue
			}
			for _, encoder := range w.Encoders {
				if !strings.HasPrefix(encoder, encoderPrefix) || encoder == "libx265" || encoder == "libx264" {
					continue
				}
				_, family, ok := strings.Cut(encoder, "_")
				if !ok || family == "" {
					continue
				}
				decoder := codec + "_" + family
				required := append([]string{"encoder:" + encoder}, DecoderSourceRequirements(decoder, codec, profile, pixelFormat)...)
				if !WorkerSupports(w, required) || !WorkerSupportsDecodeSize(w, decoder, width, height) {
					continue
				}
				candidates = append(candidates, candidate{
					worker: w, encoder: encoder, decoder: decoder,
					cost: workerQueueCost(w, KindProxy, loads[w.ID]),
				})
			}
		}
		if len(candidates) > 0 {
			sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].cost < candidates[j].cost })
			selected := candidates[0]
			return selected.worker, selected.encoder, selected.decoder, true
		}
	}
	return Worker{}, "", "", false
}

func preferredHardwareDecoderWorkerWithLoad(workers []Worker, codec, profile string, loads map[string]int) (Worker, string, bool) {
	return preferredHardwareDecoderWorkerForSourceWithLoad(workers, codec, profile, "", 0, 0, loads)
}

func preferredHardwareDecoderWorkerForSourceWithLoad(workers []Worker, codec, profile, pixelFormat string, width, height int, loads map[string]int) (Worker, string, bool) {
	codec = strings.ToLower(strings.TrimSpace(codec))
	if codec == "av01" {
		codec = "av1"
	}
	if codec != "h264" && codec != "hevc" && codec != "av1" {
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
			if strings.HasPrefix(decoder, codec+"_") && WorkerSupports(w, DecoderSourceRequirements(decoder, codec, profile, pixelFormat)) &&
				WorkerSupportsDecodeSize(w, decoder, width, height) {
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

// WorkerSupportsDecodeSize fails closed for newly planned media with known
// geometry. A legacy job without dimensions retains its live worker-side
// admission check, but a source-aware planner cannot schedule 6K/8K work from
// an unmeasured device inventory claim.
func WorkerSupportsDecodeSize(worker Worker, decoder string, width, height int) bool {
	if width <= 0 || height <= 0 {
		return true
	}
	limit, ok := worker.DecodeLimits[decoder]
	return ok && limit.MaxWidth >= width && limit.MaxHeight >= height
}

// workerCanClaimJob applies the portable capability contract and the measured
// decoder envelope together. The same predicate is mirrored inside the atomic
// Redis claim script so a differently sized GPU cannot race a job after the
// scheduler deliberately removes the original worker pin.
func workerCanClaimJob(worker Worker, job Job) bool {
	if !WorkerSupports(worker, job.RequiredCapabilities) {
		return false
	}
	decoder := requiredDecoder(job.RequiredCapabilities)
	if decoder == "" {
		return true
	}
	return WorkerSupportsDecodeSize(worker, decoder, job.SourceVideoWidth, job.SourceVideoHeight)
}

// workerCanClaimOrAdoptStaleRuntimePin is used by ready-lane maintenance.
// Before stable-name pins shipped, queued work targeted a random process ID.
// A replacement process may adopt that work only after the old heartbeat is
// gone and only when every portable codec/profile/geometry requirement still
// passes. A live worker's pin and every stable-name pin remain strict.
func workerCanClaimOrAdoptStaleRuntimePin(worker Worker, job Job, activeWorkerIDs map[string]bool) bool {
	if workerCanClaimJob(worker, job) {
		return true
	}
	required := make([]string, 0, len(job.RequiredCapabilities))
	adopted := false
	for _, capability := range job.RequiredCapabilities {
		if id, ok := strings.CutPrefix(capability, "worker:"); ok && id != "" && id != worker.ID {
			if activeWorkerIDs[id] {
				return false
			}
			adopted = true
			continue
		}
		required = append(required, capability)
	}
	if !adopted || !WorkerSupports(worker, required) {
		return false
	}
	decoder := requiredDecoder(required)
	return decoder == "" || WorkerSupportsDecodeSize(worker, decoder, job.SourceVideoWidth, job.SourceVideoHeight)
}

func requiredDecoder(capabilities []string) string {
	for _, capability := range capabilities {
		if !strings.HasPrefix(capability, "decoder:") || strings.Count(capability, ":") != 1 {
			continue
		}
		return strings.TrimPrefix(capability, "decoder:")
	}
	return ""
}

func displayCodec(codec string) string {
	if codec = strings.TrimSpace(codec); codec != "" {
		return codec
	}
	return "this source codec"
}

func displayCodecProfile(codec, profile string) string {
	codec = displayCodec(codec)
	if profile = NormalizeVideoProfile(codec, profile); profile != "" {
		return codec + " profile " + profile
	}
	return codec
}

// NormalizeVideoProfile converts ffprobe's display labels into stable queue
// capability terms. Unknown non-empty profiles remain a normalized token so
// they fail closed unless a worker has explicitly proved that exact profile.
func NormalizeVideoProfile(codec, profile string) string {
	codec = strings.ToLower(strings.TrimSpace(codec))
	profile = strings.ToLower(strings.TrimSpace(profile))
	profile = strings.NewReplacer("_", " ", "-", " ").Replace(profile)
	profile = strings.Join(strings.Fields(profile), " ")
	if profile == "" {
		return ""
	}
	switch codec {
	case "h264", "avc1":
		switch {
		case profile == "constrained baseline":
			return "constrained_baseline"
		case profile == "baseline":
			return "baseline"
		case profile == "main":
			return "main"
		case strings.HasPrefix(profile, "high"):
			return "high"
		}
	case "hevc", "h265", "hev1", "hvc1":
		switch strings.ReplaceAll(profile, " ", "") {
		case "main":
			return "main"
		case "main10":
			return "main10"
		}
	case "av1", "av01":
		switch strings.ReplaceAll(profile, " ", "") {
		case "main", "profile0":
			return "main"
		case "high", "profile1":
			return "high"
		case "professional", "profile2":
			return "professional"
		}
	}
	return strings.ReplaceAll(profile, " ", "_")
}

// DecoderRequirements is the durable, portable capability contract for one
// source. A profile token is additive: legacy jobs without source profile keep
// their codec-level behavior, while newly probed jobs fail closed on a worker
// that never completed that exact hardware decode probe.
func DecoderRequirements(decoder, codec, profile string) []string {
	required := []string{"decoder:" + decoder}
	if profile = NormalizeVideoProfile(codec, profile); profile != "" {
		required = append(required, "decoder:"+decoder+":profile:"+profile)
	}
	return required
}

func NormalizePixelFormat(pixelFormat string) string {
	return strings.ToLower(strings.TrimSpace(pixelFormat))
}

func DecoderPixelFormatRequirement(decoder, pixelFormat string) string {
	return "decoder:" + decoder + ":pixfmt:" + NormalizePixelFormat(pixelFormat)
}

// DecoderSourceRequirements is the exact source admission contract used by
// newly planned video work. DecoderRequirements remains the compatibility
// helper for older jobs that predate pixel-format probing.
func DecoderSourceRequirements(decoder, codec, profile, pixelFormat string) []string {
	required := DecoderRequirements(decoder, codec, profile)
	if pixelFormat = NormalizePixelFormat(pixelFormat); pixelFormat != "" {
		required = append(required, DecoderPixelFormatRequirement(decoder, pixelFormat))
	}
	return required
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
// New jobs use the stable Manager worker name, while legacy ready work can
// still carry the process-local runtime ID used before RC 0.5. Both map back
// to the current runtime ID consumed by workerQueueCost.
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
			counted := false
			for _, required := range job.RequiredCapabilities {
				if id := workerIDForPin(workers, required); id != "" {
					loads[id]++
					counted = true
					break
				}
			}
			// A pre-stable-pin job names the durable node separately from its
			// stale runtime capability. Count it against the restarted process so
			// measured scheduling does not treat a large inherited backlog as zero.
			if !counted && strings.TrimSpace(job.SelectedWorker) != "" {
				for _, worker := range workers {
					if strings.EqualFold(strings.TrimSpace(job.SelectedWorker), strings.TrimSpace(worker.Name)) {
						loads[worker.ID]++
						break
					}
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
		// Planning is intentionally ahead of every CPU execution lane. A fresh
		// Manager request only needs a short filesystem expansion before its
		// bounded children can run on independent render/CPU workers. Putting a
		// large CPU derivative backlog between planning kinds leaves an idle GPU
		// even though compatible proxy/transcript work is waiting to be routed.
		keys = append(keys,
			classQueue(KindDerivatives, QueueClassServer),
			classQueue(KindProxy, QueueClassServer),
			classQueue(KindTranscript, QueueClassServer),
			classQueue(KindDerivatives, QueueClassCPU),
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
	if pin := stableWorkerPinCapability(w); pin != "" {
		set[pin] = true
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

const stableWorkerPinPrefix = "worker-name:"

// workerPinCapability returns the durable scheduling identity for a node.
// Manager-controlled workers have a stable, unique name that survives a
// container restart; legacy unnamed workers retain their process-local ID.
func workerPinCapability(w Worker) string {
	if pin := stableWorkerPinCapability(w); pin != "" {
		return pin
	}
	if w.ID != "" {
		return "worker:" + w.ID
	}
	return ""
}

func stableWorkerPinCapability(w Worker) string {
	name := strings.ToLower(strings.TrimSpace(w.Name))
	if !ValidWorkerControlName(name) {
		return ""
	}
	return stableWorkerPinPrefix + name
}

// IsWorkerPinCapability lets execution planners deliberately make a bounded
// child portable while retaining its codec/profile/geometry contract.
func IsWorkerPinCapability(capability string) bool {
	if id, ok := strings.CutPrefix(capability, "worker:"); ok && id != "" {
		return true
	}
	name, ok := strings.CutPrefix(capability, stableWorkerPinPrefix)
	return ok && name != ""
}

func workerIDForPin(workers []Worker, capability string) string {
	for _, worker := range workers {
		if capability == "worker:"+worker.ID || capability == stableWorkerPinCapability(worker) {
			return worker.ID
		}
	}
	return ""
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
