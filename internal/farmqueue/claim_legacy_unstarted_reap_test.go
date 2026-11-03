package farmqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// delayedEvalConn holds the first write carrying match, reports it written (as
// the kernel does once bytes are queued) and delivers it only after release is
// closed and the client has closed the connection. That is a request go-redis
// gave up on (read timeout, then resent elsewhere) that still reaches Redis
// later: TCP keeps retransmitting queued bytes after close().
type delayedEvalConn struct {
	net.Conn
	match   []byte
	armed   *atomic.Bool
	release chan struct{}
	mu      sync.Mutex
	held    []byte
}

func (c *delayedEvalConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, c.match) && c.armed.CompareAndSwap(true, false) {
		c.mu.Lock()
		c.held = append([]byte(nil), p...)
		c.mu.Unlock()
		return len(p), nil
	}
	return c.Conn.Write(p)
}

func (c *delayedEvalConn) Close() error {
	c.mu.Lock()
	held := c.held
	c.mu.Unlock()
	if held == nil {
		return c.Conn.Close()
	}
	go func() {
		<-c.release
		_, _ = c.Conn.Write(held)
		time.Sleep(200 * time.Millisecond)
		_ = c.Conn.Close()
	}()
	return nil
}

// requeueReplyLossConn drops the reply to the first request carrying match.
type requeueReplyLossConn struct {
	net.Conn
	match  []byte
	armed  *atomic.Bool
	lost   *atomic.Bool
	buf    bytes.Buffer
	target bool
}

func (c *requeueReplyLossConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if !c.target {
		c.buf.Write(p[:n])
		if bytes.Contains(c.buf.Bytes(), c.match) && c.armed.CompareAndSwap(true, false) {
			c.target = true
		}
	}
	return n, err
}

func (c *requeueReplyLossConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.target && n > 0 {
		c.lost.Store(true)
		_ = c.Conn.Close()
		return 0, io.EOF
	}
	return n, err
}

// faultClient is a second client to q's Redis whose connections wrap does
// what it likes with.
func faultClient(t *testing.T, q *Client, wrap func(net.Conn) net.Conn, mutate func(*redis.Options)) *Client {
	t.Helper()
	options := *q.rdb.Options()
	base := &net.Dialer{}
	options.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := base.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return wrap(conn), nil
	}
	if mutate != nil {
		mutate(&options)
	}
	c := Wrap(redis.NewClient(&options))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ageLegacyClaim moves job's claim time past legacyUnstartedClaimGrace, the
// state Redis reaches once that time has passed: a test cannot wait a minute.
func ageLegacyClaim(t *testing.T, q *Client, ctx context.Context, jobID string) {
	t.Helper()
	fields, err := q.rdb.HMGet(ctx, JobHashPrefix+jobID, "delivery_token", "delivery_claimed").Result()
	if err != nil {
		t.Fatal(err)
	}
	token, _ := fields[0].(string)
	stamp, _ := fields[1].(string)
	if token == "" || stamp == "" || stamp[:len(token)+1] != token+":" {
		t.Fatalf("job %s delivery_token=%q delivery_claimed=%q; want a claim time bound to the token", jobID, token, stamp)
	}
	now, err := q.rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	aged := now.Add(-legacyUnstartedClaimGrace - time.Second).Unix()
	if err := q.rdb.HSet(ctx, JobHashPrefix+jobID, "delivery_claimed", token+":"+strconv.FormatInt(aged, 10)).Err(); err != nil {
		t.Fatal(err)
	}
}

func receiptsHold(t *testing.T, q *Client, ctx context.Context, workerID, jobID string) bool {
	t.Helper()
	raws, err := q.rdb.LRange(ctx, ProcessingPrefix+workerID, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range raws {
		if receiptJobID(raw) == jobID {
			return true
		}
	}
	return false
}

func laneHolds(t *testing.T, q *Client, ctx context.Context, job Job) int {
	t.Helper()
	raws, err := q.rdb.LRange(ctx, routedLane(t, q, ctx, job), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, raw := range raws {
		if receiptJobID(raw) == job.ID {
			n++
		}
	}
	return n
}

func receiptJobID(raw string) string {
	var job Job
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		return ""
	}
	return job.ID
}

// A claim EVAL go-redis gave up on (read timeout, then resent) that reaches
// Redis only after the resend's claim ran and was acknowledged claims a second
// job under a token the worker has spent. Nothing in the worker returns it;
// the reaper must, once the claim has gone unstarted past the grace, while the
// worker lives and works on something else.
func TestLegacyClaimUnderSpentTokenIsReturnedByTheReaper(t *testing.T) {
	base, ctx, worker, first, second := legacyReplyLossFixture(t)
	third := NewJob("/mnt/third.mov", []string{KindDerivatives}, "test")
	if err := base.Enqueue(ctx, third); err != nil {
		t.Fatal(err)
	}
	var armed atomic.Bool
	armed.Store(true)
	release := make(chan struct{})
	q := faultClient(t, base, func(c net.Conn) net.Conn {
		return &delayedEvalConn{Conn: c, match: h1EvalRequestFrame(claimForWorkerScript), armed: &armed, release: release}
	}, func(o *redis.Options) { o.ReadTimeout = 300 * time.Millisecond })
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != first.ID {
		t.Fatalf("claim: %q ok=%v err=%v; want %s", claim.Job.ID, ok, err, first.ID)
	}
	runLegacyClaimToAck(t, q, ctx, claim, worker)
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for !receiptsHold(t, base, ctx, worker.ID, second.ID) {
		if time.Now().After(deadline) {
			t.Fatal("the delayed claim EVAL never claimed the second job")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if token, _ := base.rdb.HGet(ctx, JobHashPrefix+second.ID, "delivery_token").Result(); token != claim.Token {
		t.Fatalf("%s delivery_token = %q; want the spent token %q", second.ID, token, claim.Token)
	}

	// The worker goes on to its next job, under a new token.
	next, ok, err := base.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || next.Job.ID != third.ID {
		t.Fatalf("next claim: %q ok=%v err=%v; want %s", next.Job.ID, ok, err, third.ID)
	}
	worker.CurrentJob = third.ID
	if err := base.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if err := base.MarkClaimRunning(ctx, next, worker); err != nil {
		t.Fatal(err)
	}

	// Within the grace the reaper leaves it: it could be a claim on its way to start.
	if n, err := base.RecoverAbandonedWorkers(ctx); err != nil || n != 0 {
		t.Fatalf("reaper within the grace: recovered=%d err=%v; want nothing", n, err)
	}
	if !receiptsHold(t, base, ctx, worker.ID, second.ID) {
		t.Fatal("the reaper took a fresh unstarted claim")
	}
	ageLegacyClaim(t, base, ctx, second.ID)
	if n, err := base.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("reaper past the grace: recovered=%d err=%v; want the stranded claim", n, err)
	}
	if receiptsHold(t, base, ctx, worker.ID, second.ID) || !receiptsHold(t, base, ctx, worker.ID, third.ID) {
		t.Fatal("want only the stranded receipt released, the running one kept")
	}
	fields, err := base.rdb.HGetAll(ctx, JobHashPrefix+second.ID).Result()
	if err != nil || fields["status"] != StatusQueued || fields["delivery_token"] != "" ||
		fields["delivery_claimed"] != "" || fields["attempts"] != "0" {
		t.Fatalf("returned record = %+v err=%v; want queued, undelivered, no attempt spent", fields, err)
	}
	if n := laneHolds(t, base, ctx, second); n != 1 {
		t.Fatalf("%s is in its lane %d time(s); want once", second.ID, n)
	}
	if err := base.CompleteClaim(ctx, next, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := base.AckClaim(ctx, next); err != nil {
		t.Fatal(err)
	}
	worker.CurrentJob = ""
	again, ok, err := base.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || again.Job.ID != second.ID || again.Token == claim.Token {
		t.Fatalf("claim after recovery: %q ok=%v err=%v; want %s under a new token", again.Job.ID, ok, err, second.ID)
	}
	runLegacyClaimToAck(t, base, ctx, again, worker)
	if n, err := base.rdb.LLen(ctx, ProcessingPrefix+worker.ID).Result(); err != nil || n != 0 {
		t.Fatalf("receipts left = %d err=%v", n, err)
	}
}

// The reaper never returns a delivery the live worker has started, is about
// to start (its heartbeat names the job), or claimed within the grace; and a
// claim from a worker whose claim script records no claim time (before this
// change) gets one when the reaper first sees it, so the grace runs from then.
func TestLegacyUnstartedReapSparesLiveDeliveries(t *testing.T) {
	q, ctx, worker, first, second := legacyReplyLossFixture(t)
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != first.ID {
		t.Fatalf("claim: %q ok=%v err=%v", claim.Job.ID, ok, err)
	}
	ageLegacyClaim(t, q, ctx, first.ID)
	worker.CurrentJob = first.ID
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 0 || !receiptsHold(t, q, ctx, worker.ID, first.ID) {
		t.Fatalf("heartbeat names the job: recovered=%d err=%v; want it left to start", n, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatal(err)
	}
	worker.CurrentJob = ""
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 0 || !receiptsHold(t, q, ctx, worker.ID, first.ID) {
		t.Fatalf("started delivery: recovered=%d err=%v; want it kept", n, err)
	}
	if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if stamp, _ := q.rdb.HGet(ctx, JobHashPrefix+first.ID, "delivery_claimed").Result(); stamp != "" {
		t.Fatalf("acknowledged record keeps delivery_claimed=%q", stamp)
	}

	// A claim made by an older claim script: no claim time.
	old, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || old.Job.ID != second.ID {
		t.Fatalf("claim: %q ok=%v err=%v", old.Job.ID, ok, err)
	}
	if err := q.rdb.HDel(ctx, JobHashPrefix+second.ID, "delivery_claimed").Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 0 || !receiptsHold(t, q, ctx, worker.ID, second.ID) {
		t.Fatalf("first sight of an unstamped claim: recovered=%d err=%v; want it stamped and kept", n, err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 0 {
		t.Fatalf("stamped claim within the grace: recovered=%d err=%v; want it kept", n, err)
	}
	ageLegacyClaim(t, q, ctx, second.ID)
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 || receiptsHold(t, q, ctx, worker.ID, second.ID) {
		t.Fatalf("unstamped claim past the grace: recovered=%d err=%v; want it returned", n, err)
	}
	if err := q.MarkClaimRunning(ctx, old, worker); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("start after the reaper returned it: %v; want ErrClaimLost", err)
	}
}

// The reaper's requeue and the worker's start race for one unstarted
// delivery: exactly one lands, every time, so the job never runs twice.
func TestLegacyUnstartedReapRacesStartExactlyOnce(t *testing.T) {
	q, ctx, worker, _, _ := legacyReplyLossFixture(t)
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	options := *q.rdb.Options()
	reaper := Wrap(redis.NewClient(&options))
	defer reaper.Close()
	started, reaped := 0, 0
	for i := 0; i < 60; i++ {
		if err := q.Heartbeat(ctx, worker); err != nil {
			t.Fatal(err)
		}
		job := NewJob("/mnt/race-"+strconv.Itoa(i)+".mov", []string{KindDerivatives}, "test")
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
		if err != nil || !ok || claim.Job.ID != job.ID {
			t.Fatalf("iteration %d claim: %q ok=%v err=%v", i, claim.Job.ID, ok, err)
		}
		ageLegacyClaim(t, q, ctx, job.ID)
		var startErr error
		var recovered int
		var reapErr error
		gate := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		// The reaper makes several reads before its requeue; sweep the start
		// across that window so both orders occur.
		delay := time.Duration(i%30) * 50 * time.Microsecond
		go func() {
			defer wg.Done()
			<-gate
			time.Sleep(delay)
			startErr = q.MarkClaimRunning(ctx, claim, worker)
		}()
		go func() { defer wg.Done(); <-gate; recovered, reapErr = reaper.RecoverAbandonedWorkers(ctx) }()
		close(gate)
		wg.Wait()
		if reapErr != nil || startErr != nil && !errors.Is(startErr, ErrClaimLost) {
			t.Fatalf("iteration %d: start=%v reap=%v", i, startErr, reapErr)
		}
		status, _ := q.rdb.HGet(ctx, JobHashPrefix+job.ID, "status").Result()
		held, inLane := receiptsHold(t, q, ctx, worker.ID, job.ID), laneHolds(t, q, ctx, job)
		switch {
		case startErr == nil && recovered == 0:
			if status != StatusRunning || !held || inLane != 0 {
				t.Fatalf("iteration %d: start won but status=%q receipt=%v lane=%d", i, status, held, inLane)
			}
			started++
			if err := q.CompleteClaim(ctx, claim, StatusDone, 1, 0, ""); err != nil {
				t.Fatal(err)
			}
			if err := q.AckClaim(ctx, claim); err != nil {
				t.Fatal(err)
			}
		case startErr != nil && recovered == 1:
			if status != StatusQueued || held || inLane != 1 {
				t.Fatalf("iteration %d: reaper won but status=%q receipt=%v lane=%d", i, status, held, inLane)
			}
			reaped++
			again, ok, err := q.ClaimForWorker(ctx, 0, worker)
			if err != nil || !ok || again.Job.ID != job.ID {
				t.Fatalf("iteration %d reclaim: %q ok=%v err=%v", i, again.Job.ID, ok, err)
			}
			runLegacyClaimToAck(t, q, ctx, again, worker)
		default:
			t.Fatalf("iteration %d: start=%v and reaper recovered %d: want exactly one to land", i, startErr, recovered)
		}
	}
	t.Logf("start won %d, reaper won %d", started, reaped)
	if started == 0 || reaped == 0 {
		t.Logf("only one order occurred; the exactly-once check still held each time")
	}
}

// go-redis resends a requeue EVAL whose reply was lost. The resend finds its
// own requeue landed and must say so, not report a lost claim (jmfarm stops on
// one). It must not answer for a later delivery under the same token.
func TestLegacyRequeueResendAfterSuccessIsNotClaimLost(t *testing.T) {
	base, ctx, worker, first, second := legacyReplyLossFixture(t)
	claim, ok, err := base.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != first.ID {
		t.Fatalf("claim: %q ok=%v err=%v", claim.Job.ID, ok, err)
	}
	if err := base.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatal(err)
	}
	var armed, lost atomic.Bool
	armed.Store(true)
	q := faultClient(t, base, func(c net.Conn) net.Conn {
		return &requeueReplyLossConn{Conn: c, match: []byte("if ARGV[14] == '1' then\n"), armed: &armed, lost: &lost}
	}, nil)
	err = q.RequeueClaimSameRoute(ctx, claim, "dispatch retry")
	if !lost.Load() {
		t.Fatal("test did not drop the requeue reply")
	}
	if err != nil {
		t.Fatalf("resent requeue: %v; want success, the first run landed", err)
	}
	status, _ := base.rdb.HGet(ctx, JobHashPrefix+first.ID, "status").Result()
	if n := laneHolds(t, base, ctx, first); status != StatusQueued || n != 1 || receiptsHold(t, base, ctx, worker.ID, first.ID) {
		t.Fatalf("after the resend: status=%q in lane %d time(s); want queued once, no receipt", status, n)
	}
	// A delayed claim replaying the spent token delivers the job again under
	// it; a requeue with that token must move this delivery, not stop at the
	// marker of the earlier one.
	other, ok, err := base.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || other.Job.ID != second.ID {
		t.Fatalf("claim: %q ok=%v err=%v; want %s", other.Job.ID, ok, err, second.ID)
	}
	runLegacyClaimToAck(t, base, ctx, other, worker)
	base.claimScanFor(worker.ID).keepToken(claim.Token, true)
	replay, ok, err := base.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || replay.Job.ID != first.ID || replay.Token != claim.Token {
		t.Fatalf("claim under the spent token: %q ok=%v err=%v", replay.Job.ID, ok, err)
	}
	if err := base.RequeueClaimSameRoute(ctx, replay, "again"); err != nil {
		t.Fatal(err)
	}
	if receiptsHold(t, base, ctx, worker.ID, first.ID) || laneHolds(t, base, ctx, first) != 1 {
		t.Fatal("requeue under a reused token returned success without moving the delivery")
	}
}
