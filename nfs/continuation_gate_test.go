package nfs

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

func TestContinuationGateFIFOAndStop(t *testing.T) {
	var gate continuationGate
	first, err := gate.acquire("clip.mov", time.Now().Add(5*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		id  int
		err error
	}
	queued := make(chan int, 3)
	got := make(chan outcome, 3)
	release := make([]chan struct{}, 3)
	var joined sync.WaitGroup
	defer func() {
		gate.stop(10 * time.Millisecond)
		if first != nil {
			first()
		}
		for _, ch := range release {
			if ch != nil {
				select {
				case <-ch:
				default:
					close(ch)
				}
			}
		}
		joined.Wait()
	}()
	for i := range release {
		release[i] = make(chan struct{})
		joined.Add(1)
		go func(i int) {
			defer joined.Done()
			done, err := gate.acquire("clip.mov", time.Now().Add(5*time.Second), func() { queued <- i })
			if err != nil {
				got <- outcome{i, err}
				return
			}
			got <- outcome{id: i}
			<-release[i]
			done()
		}(i)
		select {
		case id := <-queued:
			if id != i {
				t.Fatalf("queued %d, want %d", id, i)
			}
		case <-time.After(time.Second):
			t.Fatal("queue admission did not publish")
		}
	}
	first()
	first = nil
	for i := range release {
		select {
		case result := <-got:
			if result.id != i || result.err != nil {
				t.Fatalf("FIFO result %+v, want holder %d", result, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("holder %d did not run", i)
		}
		close(release[i])
	}
	joined.Wait()
	if !gate.stop(time.Second) {
		t.Fatal("gate did not join after all holders released")
	}
	if _, err := gate.acquire("clip.mov", time.Now().Add(time.Second), nil); !errors.Is(err, errFUSETimeout) {
		t.Fatalf("post-stop admission: %v", err)
	}
}

func TestContinuationGateStopRetainsHolderAndWakesWaiter(t *testing.T) {
	var gate continuationGate
	holder, err := gate.acquire("clip.mov", time.Now().Add(5*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	queued := make(chan struct{})
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := gate.acquire("clip.mov", time.Now().Add(5*time.Second), func() { close(queued) })
		result <- err
	}()
	defer func() {
		gate.stop(10 * time.Millisecond)
		if holder != nil {
			holder()
		}
		<-done
	}()
	select {
	case <-queued:
	case <-time.After(time.Second):
		t.Fatal("waiter did not queue")
	}
	if gate.stop(10 * time.Millisecond) {
		t.Fatal("stop completed with original holder still live")
	}
	select {
	case err := <-result:
		if !errors.Is(err, errFUSETimeout) {
			t.Fatalf("waiter stop error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stop did not wake queued waiter")
	}
	holder()
	holder = nil
	if !gate.stop(time.Second) {
		t.Fatal("retry stop did not join")
	}
}

func TestContinuationGateSharedDeadlineAndCapacity(t *testing.T) {
	const expectedWaiters = 95 // independently bound to one 96-WRITE managed pool minus its holder
	if outer := nfslib.DefaultRPCSemaphoreSize - nfslib.WriteRPCReserve; outer != 96 {
		t.Fatalf("managed WRITE pool=%d, want 96", outer)
	}
	if continuationWaiterCap != expectedWaiters {
		t.Fatalf("continuation waiters=%d, want %d", continuationWaiterCap, expectedWaiters)
	}
	var gate continuationGate
	holder, err := gate.acquire("clip.mov", time.Now().Add(12*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	queued := make(chan struct{}, expectedWaiters)
	results := make(chan error, expectedWaiters)
	var joined sync.WaitGroup
	defer func() {
		gate.stop(10 * time.Millisecond)
		if holder != nil {
			holder()
		}
		joined.Wait()
	}()
	for i := 0; i < expectedWaiters; i++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			_, err := gate.acquire("clip.mov", time.Now().Add(10*time.Second), func() { queued <- struct{}{} })
			results <- err
		}()
	}
	queueDeadline := time.NewTimer(4 * time.Second)
	defer queueDeadline.Stop()
	for i := 0; i < expectedWaiters; i++ {
		select {
		case <-queued:
		case <-queueDeadline.C:
			t.Fatal("waiter did not reach bounded queue")
		}
	}
	gate.mu.Lock()
	queuedCount := gate.waiters
	gate.mu.Unlock()
	if queuedCount != expectedWaiters {
		t.Fatalf("queued waiters=%d, want %d", queuedCount, expectedWaiters)
	}
	capBefore := metrics.Default().AdmissionRefusals().ContinuationWaiterCap
	if _, err := gate.acquire("clip.mov", time.Now().Add(time.Second), nil); !errors.Is(err, errFUSETimeout) {
		t.Fatalf("overflow admission = %v", err)
	}
	if got := metrics.Default().AdmissionRefusals().ContinuationWaiterCap; got != capBefore+1 {
		t.Fatalf("one overflow cap counter=%d, want %d", got, capBefore+1)
	}
	if _, err := gate.acquire("other.mov", time.Now().Add(time.Second), nil); err != nil {
		t.Fatalf("unrelated path blocked by full queue: %v", err)
	} else {
		// The unrelated holder is released separately below.
	}
	gate.release("other.mov")
	if !gate.stop(10 * time.Millisecond) {
		for i := 0; i < expectedWaiters; i++ {
			if err := <-results; !errors.Is(err, errFUSETimeout) {
				t.Fatalf("woken waiter = %v", err)
			}
		}
	} else {
		t.Fatal("stop completed while first holder is live")
	}
	holder()
	holder = nil
	if !gate.stop(time.Second) {
		t.Fatal("holder did not join")
	}
}

func TestContinuationGateQueuedDeadlineRemovesWaiter(t *testing.T) {
	var gate continuationGate
	holder, err := gate.acquire("clip.mov", time.Now().Add(time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder()
	queued := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := gate.acquire("clip.mov", time.Now().Add(50*time.Millisecond), func() { close(queued) })
		done <- err
	}()
	select {
	case <-queued:
	case <-time.After(time.Second):
		t.Fatal("deadline waiter did not queue")
	}
	select {
	case err := <-done:
		if !errors.Is(err, errFUSETimeout) {
			t.Fatalf("deadline result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued deadline did not fire")
	}
	if gate.waiters != 0 {
		t.Fatalf("expired waiter retained: %d", gate.waiters)
	}
}

func TestContinuationNamespaceDeadlineRetainsWorker(t *testing.T) {
	h := &JuiceMountHandler{}
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	defer func() {
		close(release)
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("timed-out namespace worker did not join")
		}
		if !h.namespace.stop(time.Second) {
			t.Fatal("timed-out namespace authority did not retire")
		}
	}()
	deadline := time.Now().Add(50 * time.Millisecond)
	_, err := runNamespaceOperationUntil(h, []string{"clip.mov"}, deadline, func(_ *namespaceOperation) (struct{}, error) {
		close(entered)
		<-release
		close(finished)
		return struct{}{}, nil
	}, nil)
	if !errors.Is(err, errFUSETimeout) {
		t.Fatalf("shared deadline result = %v", err)
	}
	select {
	case <-entered:
	default:
		t.Fatal("worker never entered")
	}
	if op := h.namespace.begin([]string{"clip.mov"}); op != nil {
		op.finish(nil)
		t.Fatal("timed-out worker lost namespace custody")
	}
}
