package farmqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const h1TransportAckMatch = "tracked acknowledgement lacks an open coverage ledger"

type h1TransportReplyLossConn struct {
	net.Conn
	request bytes.Buffer
	target  bool
	armed   *atomic.Bool
	lost    *atomic.Bool
}

func (c *h1TransportReplyLossConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 && !c.target {
		_, _ = c.request.Write(p[:n])
		if strings.Contains(c.request.String(), h1TransportAckMatch) && c.armed.CompareAndSwap(true, false) {
			c.target = true
		}
	}
	return n, err
}

func (c *h1TransportReplyLossConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.target && n > 0 {
		c.lost.Store(true)
		_ = c.Conn.Close()
		return 0, io.EOF
	}
	return n, err
}

type h1TransportEvalReplyLossConn struct {
	net.Conn
	request bytes.Buffer
	script  []byte
	target  bool
	armed   *atomic.Bool
	lost    *atomic.Bool
}

func h1EvalRequestFrame(script string) []byte {
	return []byte("$4\r\neval\r\n$" + strconv.Itoa(len(script)) + "\r\n" + script + "\r\n")
}

func (c *h1TransportEvalReplyLossConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 && !c.target {
		_, _ = c.request.Write(p[:n])
		if bytes.Contains(c.request.Bytes(), h1EvalRequestFrame(string(c.script))) && c.armed.CompareAndSwap(true, false) {
			c.target = true
		}
	}
	return n, err
}

func (c *h1TransportEvalReplyLossConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.target && n > 0 {
		c.lost.Store(true)
		_ = c.Conn.Close()
		return 0, io.EOF
	}
	return n, err
}

func h1InstallEvalTransportReplyLoss(t *testing.T, q *Client, script string) *atomic.Bool {
	t.Helper()
	options := *q.rdb.Options()
	options.MaxRetries = -1
	var armed atomic.Bool
	armed.Store(true)
	var lost atomic.Bool
	baseDialer := &net.Dialer{}
	options.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := baseDialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &h1TransportEvalReplyLossConn{Conn: conn, script: []byte(script), armed: &armed, lost: &lost}, nil
	}
	if err := q.rdb.Close(); err != nil {
		t.Fatalf("close fixture Redis client before installing transport: %v", err)
	}
	q.rdb = redis.NewClient(&options)
	return &lost
}

func TestH1TrackedClaimTransportReplyLossIsRecoveredWithoutExecution(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	plannerWorker := watchPlannerWorker()
	parent, child := h1ReviewTrackedChild(t, q, ctx, plannerWorker)
	if committed, err := q.CommitWatchEpoch(ctx, parent.Job.Path, "leader", parent.Job.TrackedEpoch); err != nil || !committed {
		t.Fatalf("commit planner coverage: committed=%v err=%v", committed, err)
	}
	if err := q.AckClaim(ctx, parent); err != nil {
		t.Fatalf("acknowledge planner: %v", err)
	}
	worker := Worker{ID: "h1-claim-socket-reply-loss-worker", Name: "h1-claim-socket-reply-loss-worker", Role: QueueClassServer,
		Capabilities: []string{"cpu"}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	lost := h1InstallEvalTransportReplyLoss(t, q, claimForWorkerScript)
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err == nil || ok || claim.Token != "" || claim.Job.ID != "" {
		t.Fatalf("claim after committed reply loss: claim=%+v ok=%v err=%v; want no returned claim and an error", claim, ok, err)
	}
	if !lost.Load() {
		t.Fatal("test did not drop the target tracked-claim EVAL response at the socket")
	}

	statusKey := JobHashPrefix + child.ID
	status, err := q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["claim_phase"] != "reserved" || status["lease_owner"] != worker.ID || status["claim_token"] == "" {
		t.Fatalf("committed claim reservation = %+v err=%v; want durable reservation", status, err)
	}
	processingKey := TrackedProcessingPrefix + worker.ID
	processing, err := q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil || len(processing) != 1 || processing[0] == "" {
		t.Fatalf("committed claim processing receipt = %q err=%v; want exactly one receipt", processing, err)
	}
	var received Job
	if err := json.Unmarshal([]byte(processing[0]), &received); err != nil || received.ID != child.ID {
		t.Fatalf("processing receipt job = %+v err=%v; want %q", received, err, child.ID)
	}
	if ready, err := q.rdb.LLen(ctx, QueueKeyFor(&child)).Result(); err != nil || ready != 0 {
		t.Fatalf("ready lane while claim is reserved = %d err=%v; want no duplicate delivery", ready, err)
	}
	if err := q.rdb.HSet(ctx, statusKey, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil || recovered != 1 {
		t.Fatalf("recover committed but unobserved claim: recovered=%d err=%v; want one", recovered, err)
	}
	processing, err = q.rdb.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil || len(processing) != 0 {
		t.Fatalf("old processing custody after recovery = %q err=%v; want none", processing, err)
	}
	ready, err := q.rdb.LRange(ctx, QueueKeyFor(&child), 0, -1).Result()
	if err != nil || len(ready) != 1 {
		t.Fatalf("ready child after recovery = %q err=%v; want exactly one", ready, err)
	}
	var restored Job
	if err := json.Unmarshal([]byte(ready[0]), &restored); err != nil || restored.ID != child.ID {
		t.Fatalf("restored ready job = %+v err=%v; want %q", restored, err, child.ID)
	}
	status, err = q.rdb.HGetAll(ctx, statusKey).Result()
	if err != nil || status["status"] != StatusQueued || status["claim_token"] != "" || status["claim_phase"] != "" {
		t.Fatalf("recovered child state = %+v err=%v; want unclaimed queued state", status, err)
	}
}

func TestH1TrackedCPUFallbackTransportReplyLossReplaysImmutableChild(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	plannerWorker := watchPlannerWorker()
	_, plannedChild := h1ReviewTrackedChild(t, q, ctx, plannerWorker)
	parentClaim, _ := h1ReplyLossClaimTrackedChild(t, q, ctx, plannedChild)
	targets := []string{plannedChild.RetryTargets[0]}
	childIdentity, err := newCPUFallbackSubset(parentClaim.Job, targets)
	if err != nil {
		t.Fatal(err)
	}
	lost := h1InstallEvalTransportReplyLoss(t, q, enqueueCPUFallbackSubsetScript)
	if _, _, err := q.EnqueueCPUFallbackSubsetForClaim(ctx, parentClaim, targets, "decoder unavailable"); err == nil {
		t.Fatal("tracked fallback publish returned success after its socket response was dropped")
	}
	if !lost.Load() {
		t.Fatal("test did not drop the target tracked-fallback EVAL response at the socket")
	}
	firstPayload, err := q.rdb.HGet(ctx, JobHashPrefix+childIdentity.ID, "payload").Result()
	if err != nil {
		t.Fatalf("read committed fallback payload: %v", err)
	}
	replayed, created, err := q.EnqueueCPUFallbackSubsetForClaim(ctx, parentClaim, targets, "decoder unavailable")
	if err != nil || created {
		t.Fatalf("same-claim fallback replay: created=%v err=%v; want existing immutable child", created, err)
	}
	replayPayload, err := json.Marshal(replayed)
	if err != nil || string(replayPayload) != firstPayload {
		t.Fatalf("fallback replay payload differs: replay=%s stored=%s err=%v", replayPayload, firstPayload, err)
	}
	if published, err := q.rdb.HGet(ctx, JobHashPrefix+childIdentity.ID, "cpu_fallback_published").Result(); err != nil || published != "1" {
		t.Fatalf("fallback publication receipt = %q err=%v; want durable publication", published, err)
	}
	queued, err := q.rdb.LRange(ctx, QueueKeyFor(&replayed), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	occurrences := 0
	for _, raw := range queued {
		var job Job
		if json.Unmarshal([]byte(raw), &job) == nil && job.ID == replayed.ID {
			occurrences++
		}
	}
	if occurrences != 1 {
		t.Fatalf("fallback queue contains %d copies of %s in %q; want one", occurrences, replayed.ID, queued)
	}
}

func TestH1TrackedAckTransportReplyLossIsRecoverable(t *testing.T) {
	q, ctx := openWatchEpochTestQueue(t)
	plannerWorker := watchPlannerWorker()
	_, child := h1ReviewTrackedChild(t, q, ctx, plannerWorker)
	claim, worker := h1ReplyLossClaimTrackedChild(t, q, ctx, child)
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatalf("complete tracked child: %v", err)
	}

	options := *q.rdb.Options()
	options.MaxRetries = -1
	var armed atomic.Bool
	armed.Store(true)
	var lost atomic.Bool
	baseDialer := &net.Dialer{}
	options.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := baseDialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &h1TransportReplyLossConn{Conn: conn, armed: &armed, lost: &lost}, nil
	}
	if err := q.rdb.Close(); err != nil {
		t.Fatalf("close fixture Redis client before installing transport: %v", err)
	}
	q.rdb = redis.NewClient(&options)

	if err := q.AckClaim(ctx, claim); err == nil {
		t.Fatal("ACK returned success after its Redis response was dropped at the transport")
	}
	if !lost.Load() {
		t.Fatal("test did not drop a Redis response for the targeted tracked ACK")
	}

	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+claim.Job.ID).Result()
	if err != nil || status["ack_token"] != claim.Token || status["ack_owner"] != worker.ID {
		t.Fatalf("Redis did not commit tracked ACK before reply loss: status=%+v err=%v", status, err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatalf("retry committed ACK after transport reply loss: %v", err)
	}
	processing, err := q.rdb.LRange(ctx, claimProcessingKey(claim), 0, -1).Result()
	if err != nil || len(processing) != 0 {
		t.Fatalf("processing custody after ACK replay = %q err=%v; want settled", processing, err)
	}
}
