// Package farmqueue is the shared Redis job-queue contract between the producers
// (the JuiceMount Manager, and later OpenLoupe) that ENQUEUE server-side
// generation work and the farm worker(s) that DRAIN it. It is deliberately tiny
// and dependency-light (just go-redis, already in the tree for JuiceFS metadata)
// so both the CGO-free manager and the farm binary can import it.
//
// Wire contract — keep field names + Redis keys in sync with the contract repo's
// FARM_QUEUE_PROTOCOL.md. The same Redis that holds the JuiceFS volume metadata
// is reused; all queue state lives under the `juicefarm:` keyspace so it never
// collides with JuiceFS keys.
//
// Lifecycle:
//
//	producer:  Enqueue(job)                        → route + LPUSH lane + HSET queued
//	worker:    ClaimForWorker() → run              → durable processing list + lease
//	           MarkDone/MarkFailed → AckClaim       → terminal status + remove receipt
//	           MarkDispatched      → AckClaim       → bounded children own the work
//	worker:    Heartbeat() every loop              → SET worker:<id> EX 30
//	reader:    ListJobs() / ActiveWorkers()        → ZREVRANGE + HGETALL / SCAN
package farmqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis keys + tunables. All under the juicefarm: namespace.
const (
	QueueKey       = "juicefarm:queue"        // LIST: marshaled Jobs (LPUSH producer → BRPOP worker, FIFO)
	JobHashPrefix  = "juicefarm:job:"         // HASH per job id: the JobStatus fields
	JobIndexKey    = "juicefarm:jobs"         // ZSET of job ids scored by enqueue unix-ts (recent-first listing)
	WorkerPrefix   = "juicefarm:worker:"      // STRING per worker: JSON Worker heartbeat, short TTL
	WorkerIndexKey = "juicefarm:workers"      // SET of worker ids; stale ids are pruned by ActiveWorkers
	ConfigKey      = "juicefarm:config"       // STRING: JSON FarmConfig — manager-owned desired state
	ControlKey     = "juicefarm:control"      // STRING: JSON FarmControl — global play/pause + discovery state
	WatchLeaderKey = "juicefarm:watch:leader" // STRING: short lease held by one discovery watcher
	WatchCursorKey = "juicefarm:watch:cursor" // STRING: recursive backstop's last completed scan time

	ProcessingPrefix   = "juicefarm:processing:" // LIST per worker: atomically claimed raw jobs
	ProcessingIndexKey = "juicefarm:processing"  // SET of worker ids with a processing list
	LeaseIndexKey      = "juicefarm:leases"      // ZSET: job id scored by lease expiry unix seconds

	// JobTTL keeps finished job records around for a week so the UI can show
	// recent history; the queue LIST entries are consumed immediately.
	JobTTL = 7 * 24 * time.Hour
	// WorkerTTL: a worker counts as alive only while its heartbeat key exists.
	// The worker must refresh well within this window (we use ~1/3).
	WorkerTTL = 30 * time.Second
)

// ConfigKey is the manager-owned farm config document (STRING, JSON FarmConfig).
const ConfigKeyName = "juicefarm:config"

// QueueKeyFor maps a Job to the queue it should be pushed onto. Jobs that ask
// for a single supported kind go to that kind's dedicated queue; multi-kind,
// all/empty, and unknown future kinds stay on the shared catch-all queue so a
// producer can never strand work on a queue that no shipped worker drains.
func QueueKeyFor(j *Job) string {
	if len(j.Kinds) == 1 && isDedicatedKind(j.Kinds[0]) {
		if j.QueueClass != "" {
			return classQueue(j.Kinds[0], j.QueueClass)
		}
		return QueueKey + ":" + j.Kinds[0]
	}
	return QueueKey
}

func allQueueKeys() []string {
	keys := QueueKeysForKinds([]string{KindAll})
	for _, kind := range AllKinds() {
		for _, class := range []string{QueueClassServer, QueueClassRender, QueueClassCPU} {
			keys = append(keys, classQueue(kind, class))
		}
	}
	return uniqueStrings(keys)
}

// Job lifecycle status values.
const (
	StatusQueued     = "queued"
	StatusRunning    = "running"
	StatusDispatched = "dispatched"
	StatusDone       = "done"
	StatusFailed     = "failed"
)

// Kinds the worker understands. "all" expands to the full pipeline.
const (
	KindDerivatives = "derivatives" // public composite; planner splits metadata from video previews
	KindProxy       = "proxy"       // faststart playback proxy (HEVC-first GPU, H.264 fallback)
	KindTranscript  = "transcript"  // whisper.cpp speech-to-text
	KindAll         = "all"
)

// DerivativePass splits the historical composite "derivatives" kind at the
// scheduling boundary without changing the Manager/watch wire vocabulary.
// Empty marks the composite planning request, including legacy queued work;
// workers never execute that shape directly.
const (
	DerivativePassMetadata = "metadata" // tech probe + audio waveform on the server
	DerivativePassPreviews = "previews" // poster + filmstrip on a verified video decoder
)

// AllKinds returns the dedicated job kinds supported by this worker protocol.
// Return a fresh slice so callers cannot mutate package state.
func AllKinds() []string {
	return []string{KindDerivatives, KindProxy, KindTranscript}
}

func isDedicatedKind(kind string) bool {
	switch kind {
	case KindDerivatives, KindProxy, KindTranscript:
		return true
	default:
		return false
	}
}

// DrainKinds normalizes a worker subscription. Empty subscriptions used to
// mean the catch-all queue only, which made the stock worker silently ignore
// Manager's default single-kind job. Empty and "all" now mean every supported
// dedicated queue plus the catch-all queue, preserving compatibility for
// existing deployments while making the safe generic-worker behavior explicit.
func DrainKinds(kinds []string) []string {
	if len(kinds) == 0 {
		return AllKinds()
	}
	for _, kind := range kinds {
		if kind == KindAll {
			return AllKinds()
		}
	}
	seen := make(map[string]bool, len(kinds))
	out := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		if isDedicatedKind(kind) && !seen[kind] {
			out = append(out, kind)
			seen[kind] = true
		}
	}
	return out
}

// QueueKeysForKinds returns the dedicated queues a worker must poll followed
// by the legacy catch-all queue. A generic worker (empty or "all") polls every
// known dedicated queue, which prevents Manager defaults from being orphaned.
func QueueKeysForKinds(kinds []string) []string {
	drained := DrainKinds(kinds)
	keys := make([]string, 0, len(drained)+1)
	for _, kind := range drained {
		keys = append(keys, QueueKey+":"+kind)
	}
	return append(keys, QueueKey)
}

// Job is the unit a producer enqueues and the worker drains. It is marshaled
// onto the queue LIST verbatim; field names are the wire contract.
type Job struct {
	ID         string   `json:"id"`
	Path       string   `json:"path"`        // path UNDER the volume mount to process (a dir or a single file)
	Kinds      []string `json:"kinds"`       // subset of {derivatives,proxy,transcript} or ["all"]
	Producer   string   `json:"producer"`    // "manager" | "openloupe"
	EnqueuedAt string   `json:"enqueued_at"` // ISO8601 (RFC3339, UTC)

	// Options — all optional; a zero value means "use the worker's env/flag
	// default" so a producer only overrides what it cares about.
	CRF          int    `json:"crf,omitempty"`
	Preset       string `json:"preset,omitempty"`
	Model        string `json:"model,omitempty"` // whisper model id or bare name
	VCodec       string `json:"vcodec,omitempty"`
	Workers      int    `json:"workers,omitempty"`
	ProxyWorkers int    `json:"proxy_workers,omitempty"`

	// Scheduler annotations are additive wire fields. QueueClass is chosen from
	// verified live worker profiles; RequiredCapabilities makes the decision
	// inspectable in Manager and prevents a generic CPU worker from racing a GPU
	// for accelerator-only work.
	QueueClass           string   `json:"queue_class,omitempty"`
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
	SelectedBackend      string   `json:"selected_backend,omitempty"`
	SelectedWorker       string   `json:"selected_worker,omitempty"`
	Attempts             int      `json:"attempts,omitempty"`
	// HardwareFailures counts completed render executions that failed on a
	// verified accelerator. Attempts remains delivery/recovery telemetry and may
	// increase when a claimed worker disappears; availability churn must never
	// consume the separate hardware retry budget.
	HardwareFailures int    `json:"hardware_failures,omitempty"`
	ParentID         string `json:"parent_id,omitempty"`
	// ProcessedOffset carries successful work across a narrowed retry. The
	// terminal status adds the retry's results so partial GPU success is not
	// erased when only failed targets move to another worker or the CPU lane.
	ProcessedOffset int `json:"processed_offset,omitempty"`
	// ShardIndex/ShardCount mark a bounded child created from a directory-sized
	// claim. Workers never shard an already-bounded child, so one slow file can
	// hold at most one small work set instead of an entire directory lease.
	ShardIndex int `json:"shard_index,omitempty"`
	ShardCount int `json:"shard_count,omitempty"`
	// PlanOnly keeps recursive filesystem discovery on the server worker. Fresh
	// proxy/transcript requests first enter a metadata-capable server lane; that
	// worker expands the path into deterministic bounded children, then routes
	// only those children to measured render/CPU execution lanes. This prevents
	// a GPU from spending an hour walking a directory before it can accelerate a
	// single file.
	PlanOnly bool `json:"plan_only,omitempty"`
	// DerivativePass is set only on bounded children emitted by the server
	// planner. Keeping Kinds=["derivatives"] preserves API compatibility while
	// preventing a metadata-capable server from silently full-decoding video.
	DerivativePass string `json:"derivative_pass,omitempty"`
	// SourceVideoCodec/SourceVideoProfile/SourceBitDepth carry the server's live ffprobe result into
	// a bounded preview child. They let recovery reroute the child to another
	// verified decoder family/profile without rescanning a directory.
	SourceVideoCodec   string `json:"source_video_codec,omitempty"`
	SourceVideoProfile string `json:"source_video_profile,omitempty"`
	SourceBitDepth     int    `json:"source_bit_depth,omitempty"`
	SourcePixelFormat  string `json:"source_pixel_format,omitempty"`
	SourceVideoWidth   int    `json:"source_video_width,omitempty"`
	SourceVideoHeight  int    `json:"source_video_height,omitempty"`
	// RoutingReason makes an intentional CPU decode fallback inspectable in the
	// same Recent Jobs error/note surface used by retry recovery.
	RoutingReason string `json:"routing_reason,omitempty"`
	// RetryTargets is worker-authored after a partially successful batch. It
	// narrows the next hardware retry or CPU fallback to the exact files that
	// failed, so a directory-shaped job can never recompute successful GPU
	// outputs. Workers validate every path remains under Path and the mount.
	RetryTargets []string `json:"retry_targets,omitempty"`
	// CPUFallbackLocked distinguishes a terminal, deliberate CPU route (source
	// incompatibility or exhausted hardware retries) from a temporary CPU route
	// selected only because no compatible accelerator was online. Maintenance may
	// promote only the latter when verified hardware returns.
	CPUFallbackLocked bool `json:"cpu_fallback_locked,omitempty"`
}

// JobStatus is the worker-maintained record a producer reads back. Stored as a
// flat Redis HASH (all string fields) so HGETALL round-trips without a codec.
type JobStatus struct {
	ID               string `json:"id"`
	Status           string `json:"status"` // queued|running|dispatched|done|failed
	Path             string `json:"path"`
	Kinds            string `json:"kinds"` // comma-joined for display
	Producer         string `json:"producer"`
	EnqueuedAt       string `json:"enqueued_at"`
	StartedAt        string `json:"started_at,omitempty"`
	FinishedAt       string `json:"finished_at,omitempty"`
	Processed        int    `json:"processed"`
	Failed           int    `json:"failed"`
	Error            string `json:"error,omitempty"`
	Worker           string `json:"worker,omitempty"`
	TargetWorker     string `json:"target_worker,omitempty"`
	Backend          string `json:"backend,omitempty"`
	QueueClass       string `json:"queue_class,omitempty"`
	Attempts         int    `json:"attempts"`
	HardwareFailures int    `json:"hardware_failures"`
	ParentID         string `json:"parent_id,omitempty"`
	DerivativePass   string `json:"derivative_pass,omitempty"`
}

// WorkerBenchmarks records observed rather than advertised capability. Startup
// probes populate the synthetic fields; LastJobSeconds/JobsCompleted are fed by
// real queue work and give the scheduler an honest moving signal over time.
type WorkerBenchmarks struct {
	EncodeFPS                float64 `json:"encode_fps,omitempty"`
	DecodeFPS                float64 `json:"decode_fps,omitempty"`
	TranscriptXReal          float64 `json:"transcript_x_realtime,omitempty"`
	AccessMBps               float64 `json:"access_mbps,omitempty"`
	LastJobSeconds           float64 `json:"last_job_seconds,omitempty"`
	JobsCompleted            int64   `json:"jobs_completed,omitempty"`
	FilesProcessed           int64   `json:"files_processed,omitempty"`
	FilesFailed              int64   `json:"files_failed,omitempty"`
	LastProxySeconds         float64 `json:"last_proxy_seconds,omitempty"`
	ProxyJobsCompleted       int64   `json:"proxy_jobs_completed,omitempty"`
	ProxyFilesProcessed      int64   `json:"proxy_files_processed,omitempty"`
	ProxyFilesFailed         int64   `json:"proxy_files_failed,omitempty"`
	LastTranscriptSeconds    float64 `json:"last_transcript_seconds,omitempty"`
	TranscriptJobsCompleted  int64   `json:"transcript_jobs_completed,omitempty"`
	TranscriptFilesProcessed int64   `json:"transcript_files_processed,omitempty"`
	TranscriptFilesFailed    int64   `json:"transcript_files_failed,omitempty"`
	ProbedAt                 string  `json:"probed_at,omitempty"`
	ProbeError               string  `json:"probe_error,omitempty"`
}

// VideoDecodeLimit is the largest frame geometry a decoder completed during
// the worker's startup probes. It is measured from real encoded bitstreams and
// hardware-frame output; inventory strings alone never populate this field.
type VideoDecodeLimit struct {
	MaxWidth  int `json:"max_width"`
	MaxHeight int `json:"max_height"`
}

// Worker is the heartbeat a draining worker publishes so producers can tell the
// farm is alive + accepting work (the `farm-queue` capability signal).
// Extra fields (all optional/omitted when empty) carry the manager-config
// feedback loop: which config revision the worker last applied, its effective
// settings, hardware capabilities, and anything it could NOT apply without a
// restart. Additive only — older readers ignore unknown JSON keys.
type Worker struct {
	ID         string `json:"id"`
	StartedAt  string `json:"started_at"`
	LastSeen   string `json:"last_seen"`
	CurrentJob string `json:"current_job,omitempty"`
	// BuildVersion/BuildCommit make release admission observable from Manager.
	// A worker can report perfect benchmark numbers while still running an old
	// scheduler or fallback implementation; exact provenance prevents that node
	// from being mistaken for the artifact under test.
	BuildVersion string `json:"build_version,omitempty"`
	BuildCommit  string `json:"build_commit,omitempty"`

	// Manager-config feedback (FARM-NODE-CONFIG spec):
	Name               string                      `json:"name,omitempty"`            // JM_WORKER_NAME (stable identity)
	Kinds              []string                    `json:"kinds,omitempty"`           // dedicated queues this worker drains
	ConfigRevision     int64                       `json:"config_revision,omitempty"` // 0 = unmanaged
	PendingRestart     []string                    `json:"pending_restart,omitempty"` // knobs needing container restart
	Capabilities       []string                    `json:"capabilities,omitempty"`    // e.g. ["vulkan","vaapi","cuda"]
	Effective          map[string]string           `json:"effective,omitempty"`       // resolved hot settings
	Role               string                      `json:"role,omitempty"`            // server|render
	Encoders           []string                    `json:"encoders,omitempty"`        // successfully probed ffmpeg encoders
	Decoders           []string                    `json:"decoders,omitempty"`        // verified/listed hardware decoders
	DecodeLimits       map[string]VideoDecodeLimit `json:"decode_limits,omitempty"`
	TranscriptBackends []string                    `json:"transcript_backends,omitempty"`
	Benchmarks         WorkerBenchmarks            `json:"benchmarks,omitempty"`
	State              string                      `json:"state,omitempty"` // idle|working|paused|disabled
}

// FarmConfig is the manager-owned desired state published at ConfigKey. The
// worker deep-merges overrides[<its name>] over defaults; unknown fields are
// ignored by the worker (forward compat) but REJECTED by the manager's PUT
// validation (no free-form strings reach argv/env).
// Secrets are FORBIDDEN here by design — this Redis doubles as the volume
// metadata store, so anyone who can mount the volume can read this key.
type FarmConfig struct {
	Revision  int64                     `json:"revision"`
	UpdatedAt string                    `json:"updated_at,omitempty"`
	UpdatedBy string                    `json:"updated_by,omitempty"`
	Defaults  map[string]any            `json:"defaults,omitempty"`
	Overrides map[string]map[string]any `json:"overrides,omitempty"` // worker name → patch
}

// nowISO returns the current UTC time in the wire format.
func nowISO() string { return time.Now().UTC().Format(time.RFC3339) }

// NewID returns a short random hex id for a job or worker.
func NewID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// NewJob builds a ready-to-enqueue Job with a fresh id + timestamp.
func NewJob(path string, kinds []string, producer string) Job {
	return Job{ID: NewID(), Path: path, Kinds: kinds, Producer: producer, EnqueuedAt: nowISO()}
}

// Client wraps a *redis.Client. Dial with Open; both producers and the worker
// use the same constructor against the JuiceFS meta URL.
type Client struct{ rdb *redis.Client }

// Open dials Redis from a redis:// URL (the same JM_META the volume uses).
func Open(metaURL string) (*Client, error) {
	opt, err := redis.ParseURL(metaURL)
	if err != nil {
		return nil, err
	}
	return &Client{rdb: redis.NewClient(opt)}, nil
}

// Wrap adapts an already-constructed *redis.Client (e.g. one the manager holds).
func Wrap(rdb *redis.Client) *Client { return &Client{rdb: rdb} }

// Close releases the underlying connection pool.
func (c *Client) Close() error { return c.rdb.Close() }

// Ping verifies connectivity.
func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

// ---- producer side -------------------------------------------------------

// Enqueue pushes a job onto the queue and records its initial queued status +
// index entry in one round-trip.
func (c *Client) Enqueue(ctx context.Context, j Job) error {
	if j.ID == "" {
		j.ID = NewID()
	}
	if j.EnqueuedAt == "" {
		j.EnqueuedAt = nowISO()
	}
	if j.QueueClass == "" {
		c.RouteInitialJob(ctx, &j)
	}
	raw, err := json.Marshal(j)
	if err != nil {
		return err
	}
	st := JobStatus{
		ID: j.ID, Status: StatusQueued, Path: j.Path, Kinds: strings.Join(j.Kinds, ","),
		Producer: j.Producer, EnqueuedAt: j.EnqueuedAt, Backend: j.SelectedBackend,
		TargetWorker: j.SelectedWorker, QueueClass: j.QueueClass, Attempts: j.Attempts,
		HardwareFailures: j.HardwareFailures, ParentID: j.ParentID,
	}
	pipe := c.rdb.TxPipeline()
	pipe.LPush(ctx, QueueKeyFor(&j), raw)
	pipe.HSet(ctx, JobHashPrefix+j.ID, st.toMap())
	pipe.Expire(ctx, JobHashPrefix+j.ID, JobTTL)
	// Unix-second scores make every burst of jobs tie. Redis is then free to
	// return tied members lexicographically, so a just-completed job can be
	// absent from ListJobs(n) even though older jobs from the same second are
	// shown. Unix microseconds are still exactly representable by float64 at the
	// current epoch and preserve enqueue order under Manager-sized bursts.
	pipe.ZAdd(ctx, JobIndexKey, redis.Z{Score: float64(time.Now().UnixMicro()), Member: j.ID})
	_, err = pipe.Exec(ctx)
	return err
}

// ---- worker side ---------------------------------------------------------

// Dequeue blocks up to timeout for the next job. ok=false means the wait timed
// out with nothing available (the worker should loop + heartbeat again).
func (c *Client) Dequeue(ctx context.Context, timeout time.Duration) (job Job, ok bool, err error) {
	res, err := c.rdb.BRPop(ctx, timeout, QueueKey).Result()
	if err == redis.Nil {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	// res = [key, value]
	if len(res) != 2 {
		return Job{}, false, nil
	}
	if err := json.Unmarshal([]byte(res[1]), &job); err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

// DequeueKinds blocks up to timeout for the next job from any of the worker's
// declared kind queues, falling back to the catch-all QueueKey last so
// un-routed (all/legacy) jobs still get drained by whoever is free. Empty and
// "all" subscriptions drain every known kind, so a generic stock worker always
// consumes the Manager's default single-kind jobs.
func (c *Client) DequeueKinds(ctx context.Context, timeout time.Duration, kinds []string) (job Job, ok bool, err error) {
	keys := QueueKeysForKinds(kinds)
	res, err := c.rdb.BRPop(ctx, timeout, keys...).Result()
	if err == redis.Nil {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	if len(res) != 2 {
		return Job{}, false, nil
	}
	if err := json.Unmarshal([]byte(res[1]), &job); err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

// MarkRunning flips a job to running and stamps started_at.
func (c *Client) MarkRunning(ctx context.Context, id string) error {
	return c.rdb.HSet(ctx, JobHashPrefix+id, map[string]any{
		"status": StatusRunning, "started_at": nowISO(),
	}).Err()
}

// MarkDispatched records that a directory-sized parent released its durable
// claim after atomically enqueueing bounded children. It deliberately has no
// finished_at: the parent did not process the media and its children may still
// be queued or running. ParentID on each child keeps the relationship visible.
func (c *Client) MarkDispatched(ctx context.Context, id string) error {
	return c.rdb.HSet(ctx, JobHashPrefix+id, map[string]any{
		"status": StatusDispatched,
	}).Err()
}

// MarkDone records a successful finish + counts.
func (c *Client) MarkDone(ctx context.Context, id string, processed, failed int) error {
	st := StatusDone
	if failed > 0 && processed == 0 {
		st = StatusFailed
	}
	return c.rdb.HSet(ctx, JobHashPrefix+id, map[string]any{
		"status": st, "finished_at": nowISO(),
		"processed": strconv.Itoa(processed), "failed": strconv.Itoa(failed),
	}).Err()
}

// MarkFailed records a hard failure (the worker couldn't run the job at all).
func (c *Client) MarkFailed(ctx context.Context, id, errMsg string) error {
	return c.rdb.HSet(ctx, JobHashPrefix+id, map[string]any{
		"status": StatusFailed, "finished_at": nowISO(), "error": errMsg,
	}).Err()
}

// MarkFailedWithCounts retains partial success when the final narrowed retry
// still fails. Without this, a large GPU batch could publish thousands of
// proxies and then appear as 0/0 merely because its small CPU subset failed.
func (c *Client) MarkFailedWithCounts(ctx context.Context, id string, processed, failed int, errMsg string) error {
	return c.rdb.HSet(ctx, JobHashPrefix+id, map[string]any{
		"status": StatusFailed, "finished_at": nowISO(), "error": errMsg,
		"processed": strconv.Itoa(processed), "failed": strconv.Itoa(failed),
	}).Err()
}

// Heartbeat publishes/refreshes the worker's liveness key (TTL WorkerTTL) and
// records its ID in a compact index. Using an index keeps worker discovery off
// the full JuiceFS metadata keyspace: Redis SCAN with a MATCH pattern still
// walks every key and can time out on production-sized volumes.
func (c *Client) Heartbeat(ctx context.Context, w Worker) error {
	w.LastSeen = nowISO()
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	pipe := c.rdb.TxPipeline()
	pipe.Set(ctx, WorkerPrefix+w.ID, raw, WorkerTTL)
	if w.ID != "" {
		pipe.SAdd(ctx, WorkerIndexKey, w.ID)
	}
	_, err = pipe.Exec(ctx)
	return err
}

// ---- reader side ---------------------------------------------------------

// ListJobs returns the most-recently-enqueued n jobs (status records), newest
// first. Missing/expired records are skipped.
func (c *Client) ListJobs(ctx context.Context, n int) ([]JobStatus, error) {
	if n <= 0 {
		n = 50
	}
	ids, err := c.rdb.ZRevRange(ctx, JobIndexKey, 0, int64(n-1)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]JobStatus, 0, len(ids))
	for _, id := range ids {
		m, err := c.rdb.HGetAll(ctx, JobHashPrefix+id).Result()
		if err != nil || len(m) == 0 {
			continue
		}
		out = append(out, jobStatusFromMap(m))
	}
	return out, nil
}

// ClearFinished removes terminal (done/failed) job records — and any
// leaked index entries whose hash has already expired — from Redis, so
// the Recent-jobs list can be cleared and the JobIndexKey ZSET does not
// grow without bound over the lifetime of the shared metadata Redis.
//
// Queued/running/dispatched jobs are NEVER removed: dropping a job a worker is
// about to process, is actively processing, or whose children are still active
// would orphan or hide work. Returns the number of index entries removed.
func (c *Client) ClearFinished(ctx context.Context) (int, error) {
	ids, err := c.rdb.ZRange(ctx, JobIndexKey, 0, -1).Result()
	if err != nil {
		return 0, err
	}
	pipe := c.rdb.TxPipeline()
	removed := 0
	for _, id := range ids {
		m, err := c.rdb.HGetAll(ctx, JobHashPrefix+id).Result()
		if err != nil {
			continue
		}
		// len(m)==0 => the hash expired but the index entry leaked; prune
		// it. Otherwise only prune terminal jobs. queued/running stay.
		if st := m["status"]; len(m) == 0 || st == StatusDone || st == StatusFailed {
			pipe.ZRem(ctx, JobIndexKey, id)
			pipe.Del(ctx, JobHashPrefix+id)
			removed++
		}
	}
	if removed == 0 {
		return 0, nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return removed, nil
}

// ActiveWorkers returns the currently-heartbeating workers (their keys are
// alive). Stale IDs are pruned from the compact index as workers expire. An
// empty slice means the farm is NOT draining — the producer should surface
// "farm offline / not accepting work."
func (c *Client) ActiveWorkers(ctx context.Context) ([]Worker, error) {
	ids, err := c.rdb.SMembers(ctx, WorkerIndexKey).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Worker, 0, len(ids))
	stale := make([]any, 0)
	for _, id := range ids {
		raw, err := c.rdb.Get(ctx, WorkerPrefix+id).Result()
		if err != nil {
			if err == redis.Nil {
				stale = append(stale, id)
			}
			continue
		}
		var w Worker
		if json.Unmarshal([]byte(raw), &w) == nil {
			out = append(out, w)
		} else {
			stale = append(stale, id)
		}
	}
	if len(stale) > 0 {
		// Best-effort hygiene: a failed prune must not hide the live workers
		// already read above.
		_ = c.rdb.SRem(ctx, WorkerIndexKey, stale...).Err()
	}
	return out, nil
}

// QueueDepth is the aggregate number of jobs waiting (not yet popped) across
// every shipped dedicated queue and the legacy catch-all queue.
func (c *Client) QueueDepth(ctx context.Context) (int64, error) {
	keys := allQueueKeys()
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, pipe.LLen(ctx, key))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	var total int64
	for _, cmd := range cmds {
		total += cmd.Val()
	}
	return total, nil
}

// ---- HASH <-> struct helpers --------------------------------------------

func (s JobStatus) toMap() map[string]any {
	m := map[string]any{
		"id": s.ID, "status": s.Status, "path": s.Path, "kinds": s.Kinds,
		"producer": s.Producer, "enqueued_at": s.EnqueuedAt,
		"processed": strconv.Itoa(s.Processed), "failed": strconv.Itoa(s.Failed),
		"attempts":          strconv.Itoa(s.Attempts),
		"hardware_failures": strconv.Itoa(s.HardwareFailures),
	}
	if s.DerivativePass != "" {
		m["derivative_pass"] = s.DerivativePass
	}
	if s.StartedAt != "" {
		m["started_at"] = s.StartedAt
	}
	if s.FinishedAt != "" {
		m["finished_at"] = s.FinishedAt
	}
	if s.Error != "" {
		m["error"] = s.Error
	}
	if s.Worker != "" {
		m["worker"] = s.Worker
	}
	if s.TargetWorker != "" {
		m["target_worker"] = s.TargetWorker
	}
	if s.Backend != "" {
		m["backend"] = s.Backend
	}
	if s.QueueClass != "" {
		m["queue_class"] = s.QueueClass
	}
	if s.ParentID != "" {
		m["parent_id"] = s.ParentID
	}
	return m
}

func jobStatusFromMap(m map[string]string) JobStatus {
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	return JobStatus{
		ID: m["id"], Status: m["status"], Path: m["path"], Kinds: m["kinds"],
		Producer: m["producer"], EnqueuedAt: m["enqueued_at"],
		StartedAt: m["started_at"], FinishedAt: m["finished_at"],
		Processed: atoi(m["processed"]), Failed: atoi(m["failed"]), Error: m["error"],
		Worker: m["worker"], TargetWorker: m["target_worker"], Backend: m["backend"], QueueClass: m["queue_class"], Attempts: atoi(m["attempts"]), HardwareFailures: atoi(m["hardware_failures"]), ParentID: m["parent_id"], DerivativePass: m["derivative_pass"],
	}
}
