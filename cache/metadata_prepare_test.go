//go:build redisoptionsacceptance

package cache

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type preparationHook struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	bounded atomic.Int32
	normal  atomic.Int32
	onBound func() error
}

type preparationRedisError string

func (e preparationRedisError) Error() string { return string(e) }
func (preparationRedisError) RedisError()     {}

func (h *preparationHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *preparationHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *preparationHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" && len(cmd.Args()) > 1 {
			if script, ok := cmd.Args()[1].(string); ok && strings.Contains(script, "JM_CACHE_PREPARE_LIMIT") {
				h.bounded.Add(1)
				h.once.Do(func() { close(h.entered) })
				select {
				case <-h.release:
				case <-ctx.Done():
					return ctx.Err()
				}
				if h.onBound != nil {
					if err := h.onBound(); err != nil {
						return err
					}
				}
			} else if ok && strings.Contains(script, "LRANGE") {
				h.normal.Add(1)
			}
		}
		return next(ctx, cmd)
	}
}

func TestPrepareChunkMappingBoundedRefusalDoesNotPublish(t *testing.T) {
	r, rdb, _ := residentReaderFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name      string
		records   int
		malformed bool
	}{
		{"too-many-records", prepareMappingMaxRecords + 1, false},
		{"malformed-record", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.InvalidateSliceCache(3)
			if err := rdb.Del(ctx, "c3_0").Err(); err != nil {
				t.Fatal(err)
			}
			record := make([]byte, 24)
			if tc.malformed {
				record = []byte("bad")
			}
			items := make([]any, tc.records)
			for i := range items {
				items[i] = string(record)
			}
			if err := rdb.RPush(ctx, "c3_0", items...).Err(); err != nil {
				t.Fatal(err)
			}
			if err := r.PrepareChunkMapping(ctx, 3, 0); err == nil {
				t.Fatal("invalid bounded response accepted")
			}
			r.sliceMu.RLock()
			_, present := r.sliceCache["3_0"]
			_, witnessed := r.attrWitness["3_0"]
			r.sliceMu.RUnlock()
			if present || witnessed {
				t.Fatal("partial preparation published")
			}
		})
	}
}

func TestPrepareChunkMappingJoinsForegroundWithoutDuplicateFetch(t *testing.T) {
	source, direct, binding := residentReaderFixture(t)
	_ = source
	hook := &preparationHook{entered: make(chan struct{}), release: make(chan struct{})}
	opts := *direct.Options()
	client := redis.NewClient(&opts)
	client.AddHook(hook)
	defer client.Close()
	r, err := NewVolumeReader(binding, client)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	joined := make(chan struct{})
	r.testJoinPreparationFlight = func() { close(joined) }
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	prepared := make(chan error, 1)
	read := make(chan error, 1)
	var releaseOnce sync.Once
	var workers sync.WaitGroup
	workers.Add(1)
	defer func() { releaseOnce.Do(func() { close(hook.release) }); cancel(); workers.Wait() }()
	go func() { defer workers.Done(); prepared <- r.PrepareChunkMapping(ctx, 3, 0) }()
	select {
	case <-hook.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		body := make([]byte, 4096)
		_, err := r.ReadBlock(ctx, 3, 0, body)
		if err == nil && !bytes.Equal(body, bytes.Repeat([]byte{'B'}, 4096)) {
			err = errors.New("wrong resident bytes")
		}
		read <- err
	}()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	releaseOnce.Do(func() { close(hook.release) })
	if err := <-prepared; err != nil {
		t.Fatal(err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if hook.bounded.Load() != 1 || hook.normal.Load() != 0 {
		t.Fatalf("mapping flights bounded=%d normal=%d", hook.bounded.Load(), hook.normal.Load())
	}
}

func TestPrepareChunkMappingResidentRAMFastPath(t *testing.T) {
	r, _, _ := residentReaderFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.PrepareChunkMapping(context.Background(), 3, 0); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("unbounded preparation: %v", err)
	}
	if err := r.PrepareChunkMapping(ctx, 3, 0); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	got := make([]byte, 4096)
	if n, err := r.ReadBlock(canceled, 3, 0, got); err != nil || n != len(got) || !bytes.Equal(got, bytes.Repeat([]byte{'B'}, len(got))) {
		t.Fatalf("resident RAM mapping depended on canceled network context: %d %v", n, err)
	}
}

func TestPrepareChunkMappingBudgetRefusalRetriesForeground(t *testing.T) {
	_, direct, binding := residentReaderFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	record, err := direct.LIndex(ctx, "c3_0", 0).Result()
	if err != nil {
		t.Fatal(err)
	}
	extra := make([]any, prepareMappingMaxRecords)
	for i := range extra {
		extra[i] = record
	}
	if err := direct.RPush(ctx, "c3_0", extra...).Err(); err != nil {
		t.Fatal(err)
	}
	hook := &preparationHook{entered: make(chan struct{}), release: make(chan struct{})}
	opts := *direct.Options()
	client := redis.NewClient(&opts)
	client.AddHook(hook)
	defer client.Close()
	r, err := NewVolumeReader(binding, client)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	joined := make(chan struct{})
	r.testJoinPreparationFlight = func() { close(joined) }
	prepared := make(chan error, 1)
	read := make(chan error, 1)
	var workers sync.WaitGroup
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(hook.release) }); cancel(); workers.Wait() }()
	workers.Add(1)
	go func() { defer workers.Done(); prepared <- r.PrepareChunkMapping(ctx, 3, 0) }()
	select {
	case <-hook.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		body := make([]byte, 4096)
		_, err := r.ReadBlock(ctx, 3, 0, body)
		if err == nil && !bytes.Equal(body, bytes.Repeat([]byte{'B'}, len(body))) {
			err = errors.New("wrong fallback bytes")
		}
		read <- err
	}()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	releaseOnce.Do(func() { close(hook.release) })
	if err := <-prepared; err == nil {
		t.Fatal("oversized preparation accepted")
	}
	if err := <-read; err != nil {
		t.Fatalf("foreground inherited preparation refusal: %v", err)
	}
	if hook.bounded.Load() != 1 || hook.normal.Load() != 1 {
		t.Fatalf("mapping flights bounded=%d normal=%d", hook.bounded.Load(), hook.normal.Load())
	}
}

func TestPrepareChunkMappingInvalidatedWhileHeld(t *testing.T) {
	_, direct, binding := residentReaderFixture(t)
	hook := &preparationHook{entered: make(chan struct{}), release: make(chan struct{})}
	opts := *direct.Options()
	client := redis.NewClient(&opts)
	client.AddHook(hook)
	defer client.Close()
	r, err := NewVolumeReader(binding, client)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	prepared := make(chan error, 1)
	var worker sync.WaitGroup
	worker.Add(1)
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(hook.release) }); cancel(); worker.Wait() }()
	go func() { defer worker.Done(); prepared <- r.PrepareChunkMapping(ctx, 3, 0) }()
	select {
	case <-hook.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	r.InvalidateSliceCache(3)
	releaseOnce.Do(func() { close(hook.release) })
	if err := <-prepared; err == nil {
		t.Fatal("in-flight preparation published after invalidation")
	}
	if _, err := r.ResidentEOF(3, residentWant(4096)); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("invalidated witness = %v", err)
	}
}

func TestPrepareChunkMappingVolumeRefusalSurvivesTransportReplacement(t *testing.T) {
	_, direct, binding := residentReaderFixture(t)
	hook := &preparationHook{entered: make(chan struct{}), release: make(chan struct{})}
	close(hook.release)
	opts := *direct.Options()
	client := redis.NewClient(&opts)
	client.AddHook(hook)
	defer client.Close()
	replacement := redis.NewClient(&opts)
	defer replacement.Close()
	r, err := NewVolumeReader(binding, client)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	hook.onBound = func() error { r.rdb.Store(replacement); return preparationRedisError("JM_CACHE_VOLUME_MISMATCH") }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.PrepareChunkMapping(ctx, 3, 0); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("replacement erased binding refusal: %v", err)
	}
	if _, err := r.ResidentEOF(3, residentWant(4096)); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("refused witness published: %v", err)
	}
}
