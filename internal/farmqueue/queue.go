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
//	producer:  Enqueue(job)                       → LPUSH queue + HSET status=queued
//	worker:    Dequeue() → MarkRunning → run →     BRPOP queue, HSET status=running
//	           MarkDone / MarkFailed               → HSET status=done|failed
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
	QueueKey      = "juicefarm:queue"   // LIST: marshaled Jobs (LPUSH producer → BRPOP worker, FIFO)
	JobHashPrefix = "juicefarm:job:"    // HASH per job id: the JobStatus fields
	JobIndexKey   = "juicefarm:jobs"    // ZSET of job ids scored by enqueue unix-ts (recent-first listing)
	WorkerPrefix  = "juicefarm:worker:" // STRING per worker: JSON Worker heartbeat, short TTL

	// JobTTL keeps finished job records around for a week so the UI can show
	// recent history; the queue LIST entries are consumed immediately.
	JobTTL = 7 * 24 * time.Hour
	// WorkerTTL: a worker counts as alive only while its heartbeat key exists.
	// The worker must refresh well within this window (we use ~1/3).
	WorkerTTL = 30 * time.Second
)

// Job lifecycle status values.
const (
	StatusQueued  = "queued"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
)

// Kinds the worker understands. "all" expands to the full pipeline.
const (
	KindDerivatives = "derivatives" // tech + thumbnail/poster + filmstrip + waveform (the cheap passes)
	KindProxy       = "proxy"       // the H.264 faststart playback proxy (CPU-heavy)
	KindTranscript  = "transcript"  // whisper.cpp speech-to-text
	KindAll         = "all"
)

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
}

// JobStatus is the worker-maintained record a producer reads back. Stored as a
// flat Redis HASH (all string fields) so HGETALL round-trips without a codec.
type JobStatus struct {
	ID         string `json:"id"`
	Status     string `json:"status"` // queued|running|done|failed
	Path       string `json:"path"`
	Kinds      string `json:"kinds"` // comma-joined for display
	Producer   string `json:"producer"`
	EnqueuedAt string `json:"enqueued_at"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	Processed  int    `json:"processed"`
	Failed     int    `json:"failed"`
	Error      string `json:"error,omitempty"`
}

// Worker is the heartbeat a draining worker publishes so producers can tell the
// farm is alive + accepting work (the `farm-queue` capability signal).
type Worker struct {
	ID         string `json:"id"`
	StartedAt  string `json:"started_at"`
	LastSeen   string `json:"last_seen"`
	CurrentJob string `json:"current_job,omitempty"`
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
	raw, err := json.Marshal(j)
	if err != nil {
		return err
	}
	st := JobStatus{
		ID: j.ID, Status: StatusQueued, Path: j.Path, Kinds: strings.Join(j.Kinds, ","),
		Producer: j.Producer, EnqueuedAt: j.EnqueuedAt,
	}
	pipe := c.rdb.TxPipeline()
	pipe.LPush(ctx, QueueKey, raw)
	pipe.HSet(ctx, JobHashPrefix+j.ID, st.toMap())
	pipe.Expire(ctx, JobHashPrefix+j.ID, JobTTL)
	pipe.ZAdd(ctx, JobIndexKey, redis.Z{Score: float64(time.Now().Unix()), Member: j.ID})
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

// MarkRunning flips a job to running and stamps started_at.
func (c *Client) MarkRunning(ctx context.Context, id string) error {
	return c.rdb.HSet(ctx, JobHashPrefix+id, map[string]any{
		"status": StatusRunning, "started_at": nowISO(),
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

// Heartbeat publishes/refreshes the worker's liveness key (TTL WorkerTTL).
func (c *Client) Heartbeat(ctx context.Context, w Worker) error {
	w.LastSeen = nowISO()
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, WorkerPrefix+w.ID, raw, WorkerTTL).Err()
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

// ActiveWorkers returns the currently-heartbeating workers (their keys are
// alive). An empty slice means the farm is NOT draining — the producer should
// surface "farm offline / not accepting work."
func (c *Client) ActiveWorkers(ctx context.Context) ([]Worker, error) {
	var (
		out    []Worker
		cursor uint64
	)
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, WorkerPrefix+"*", 50).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			raw, err := c.rdb.Get(ctx, k).Result()
			if err != nil {
				continue
			}
			var w Worker
			if json.Unmarshal([]byte(raw), &w) == nil {
				out = append(out, w)
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	return out, nil
}

// QueueDepth is the number of jobs waiting (not yet popped).
func (c *Client) QueueDepth(ctx context.Context) (int64, error) {
	return c.rdb.LLen(ctx, QueueKey).Result()
}

// ---- HASH <-> struct helpers --------------------------------------------

func (s JobStatus) toMap() map[string]any {
	m := map[string]any{
		"id": s.ID, "status": s.Status, "path": s.Path, "kinds": s.Kinds,
		"producer": s.Producer, "enqueued_at": s.EnqueuedAt,
		"processed": strconv.Itoa(s.Processed), "failed": strconv.Itoa(s.Failed),
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
	return m
}

func jobStatusFromMap(m map[string]string) JobStatus {
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	return JobStatus{
		ID: m["id"], Status: m["status"], Path: m["path"], Kinds: m["kinds"],
		Producer: m["producer"], EnqueuedAt: m["enqueued_at"],
		StartedAt: m["started_at"], FinishedAt: m["finished_at"],
		Processed: atoi(m["processed"]), Failed: atoi(m["failed"]), Error: m["error"],
	}
}
