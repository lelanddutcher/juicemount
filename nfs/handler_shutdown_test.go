package nfs

import (
	"sync"
	"testing"
	"time"
)

func TestStopHandlerIsIdempotentUnderConcurrentCalls(t *testing.T) {
	h := NewHandler(nil, t.TempDir())
	const callers = 24
	start := make(chan struct{})
	results := make(chan bool, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- h.stopHandlerWithDeadline(time.Second)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for ok := range results {
		if !ok {
			t.Fatal("concurrent stop reported failure")
		}
	}
	if !h.stopHandlerWithDeadline(time.Second) {
		t.Fatal("repeated stop failed")
	}
}

func TestStopHandlerRetainsSpoolWhileSweeperIsBlocked(t *testing.T) {
	spool := newTestSpoolStore(t, 0)
	h := &JuiceMountHandler{}
	h.spool.Store(spool)
	entered := make(chan struct{})
	release := make(chan struct{})
	h.spoolSweeperStop = func() {
		close(entered)
		<-release
	}
	if h.stopHandlerWithDeadline(20 * time.Millisecond) {
		t.Fatal("stop reported success while sweeper still owned the spool")
	}
	<-entered
	if entry, err := spool.OpenWrite("/still-open.bin"); err != nil {
		t.Fatalf("spool closed before sweeper exited: %v", err)
	} else {
		_ = entry.Close()
	}
	close(release)
	if !h.stopHandlerWithDeadline(time.Second) {
		t.Fatal("retry did not finish after sweeper exited")
	}
	if _, err := spool.OpenWrite("/after-stop.bin"); err == nil {
		t.Fatal("spool remained open after completed stop")
	}
}
