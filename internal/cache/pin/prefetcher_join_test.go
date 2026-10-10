package pin

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type waitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestStopWithinRetainsActualTrackedWork(t *testing.T) {
	p := NewPrefetcher(newTestStore(t), t.TempDir(), "/Volumes/zpool", 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if !p.StopWithin(time.Second) {
			t.Error("prefetcher did not join before store cleanup")
		}
	})
	entered := make(chan struct{})
	if !p.TryGo(func() {
		close(entered)
		<-release
	}) {
		t.Fatal("live prefetcher refused tracked work")
	}
	<-entered
	if p.StopWithin(10 * time.Millisecond) {
		t.Fatal("stop reported completion while admitted work was held")
	}
	if p.StopWithin(0) {
		t.Fatal("retry reported completion before held work returned")
	}
	stops := make(chan bool, 8)
	for i := 0; i < cap(stops); i++ {
		go func() { stops <- p.StopWithin(10 * time.Millisecond) }()
	}
	for i := 0; i < cap(stops); i++ {
		if <-stops {
			t.Fatal("concurrent stop reported completion before held work returned")
		}
	}
	if p.Enqueue(Entry{Path: "/Volumes/zpool/late"}) {
		t.Fatal("enqueue accepted after stop")
	}
	late := make(chan struct{})
	if p.TryGo(func() { close(late) }) {
		t.Fatal("TryGo reported admission after Stop")
	}
	p.Go(func() { close(late) })
	select {
	case <-late:
		t.Fatal("Go accepted work after stop")
	default:
	}
	if err := p.EnqueueWait(context.Background(), Entry{}); !errors.Is(err, ErrPrefetcherStopped) {
		t.Fatalf("EnqueueWait after stop = %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	if !p.StopWithin(time.Second) || !p.StopWithin(0) {
		t.Fatal("retained join did not complete idempotently after release")
	}
	p.Stop()
}

func TestEnqueueWaitUnblocksWhenFullQueueStops(t *testing.T) {
	// No worker: the queue stays full, so the caller must be released by
	// stop itself rather than by a dequeue or a context deadline.
	p := &Prefetcher{
		jobs:       make(chan jobReq, 1),
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
		queueSpace: make(chan struct{}, 1),
	}
	p.jobs <- jobReq{}
	ctx := &waitingContext{Context: context.Background(), waiting: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		result <- p.EnqueueWait(ctx, Entry{Path: "/Volumes/zpool/waiting.mov", Generation: "captured-generation"})
	}()
	<-ctx.waiting // EnqueueWait reached the full-queue wait branch.
	p.StopWithin(0)
	select {
	case err := <-result:
		if !errors.Is(err, ErrPrefetcherStopped) {
			t.Fatalf("waiting enqueue = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting enqueue did not return on stop")
	}
}

func TestConcurrentStopAndGoAdmission(t *testing.T) {
	for attempt := 0; attempt < 5; attempt++ {
		p := NewPrefetcher(newTestStore(t), t.TempDir(), "/Volumes/zpool", 1)
		release := make(chan struct{})
		start := make(chan struct{})
		var producers sync.WaitGroup
		for i := 0; i < 64; i++ {
			producers.Add(1)
			go func() {
				defer producers.Done()
				<-start
				p.Go(func() { <-release })
			}()
		}
		stopResult := make(chan bool, 1)
		go func() {
			<-start
			stopResult <- p.StopWithin(10 * time.Millisecond)
		}()
		close(start)
		producers.Wait()
		<-stopResult
		<-p.stopCh
		late := make(chan struct{})
		p.Go(func() { close(late) })
		select {
		case <-late:
			t.Fatal("Go admitted after stop")
		default:
		}
		close(release)
		if !p.StopWithin(time.Second) {
			t.Fatal("concurrent stop lost an admitted Go task")
		}
	}
}
