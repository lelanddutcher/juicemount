//go:build darwin && latency_acceptance

package nfs

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

type kernelPressureStatus struct {
	StartedNS       int64 `json:"started_ns"`
	FinishedNS      int64 `json:"finished_ns"`
	Completed       bool  `json:"completed"`
	Before          int   `json:"before"`
	After           int   `json:"after"`
	Budget          int   `json:"budget"`
	EvictedChildren int   `json:"evicted_children"`
	MissingPins     int   `json:"missing_pins"`
}

type kernelCachePressure struct {
	mu     sync.Mutex
	status kernelPressureStatus
	done   chan struct{}
	pins   map[string]struct{}
}

func prepareKernelCachePressure(store *metadata.Store, count, budget int) *kernelCachePressure {
	pins := map[string]struct{}{"slow.bin": {}}
	for _, phase := range []string{"quiet", "held", "post"} {
		for i := 0; i < 4; i++ {
			for _, kind := range []string{"attr", "lookup"} {
				pins[fmt.Sprintf("%s/%s-%d.bin", phase, kind, i)] = struct{}{}
			}
		}
	}
	store.SetPinChecker(navigationPinnedPaths(pins))
	stamp := time.Now().Add(time.Hour)
	for i := 0; i < count; i++ {
		store.InsertToCache(metadata.MakeEntry(fmt.Sprintf("__pressure/file-%06d", i), false, 1,
			stamp.Add(time.Duration(i)), uint64(10_000_000+i)))
	}
	store.SetCacheBudgetForLatencyAcceptance(budget)
	return &kernelCachePressure{pins: pins, status: kernelPressureStatus{Budget: budget}}
}

func (p *kernelCachePressure) snapshot() *kernelPressureStatus {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	copy := p.status
	return &copy
}

// now shares the transport trace's monotonic clock. Start and finish bracket
// the actual production eviction, not the subsequent verification work.
func (p *kernelCachePressure) start(store *metadata.Store, now func() int64) error {
	if p == nil {
		return errors.New("cache pressure was not enabled")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done != nil {
		return errors.New("cache eviction already admitted")
	}
	p.done = make(chan struct{})
	p.status.Before, _ = store.CacheStats()
	go func() {
		p.mu.Lock()
		p.status.StartedNS = now()
		p.mu.Unlock()
		store.EvictForLatencyAcceptance()
		finished := now()
		after, _ := store.CacheStats()
		missing, removed := 0, 0
		for path := range p.pins {
			if !store.PathCachedForLatencyAcceptance(path) {
				missing++
			}
		}
		for _, phase := range []string{"quiet", "held", "post"} {
			for i := 0; i < 4; i++ {
				for j := 0; j < 100; j++ {
					if !store.PathCachedForLatencyAcceptance(fmt.Sprintf("%s/list-%d/item-%03d.bin", phase, i, j)) {
						removed++
					}
				}
			}
		}
		p.mu.Lock()
		p.status.FinishedNS, p.status.Completed = finished, true
		p.status.After, p.status.MissingPins, p.status.EvictedChildren = after, missing, removed
		p.mu.Unlock()
		close(p.done)
	}()
	return nil
}

func (p *kernelCachePressure) join(timeout time.Duration) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	done := p.done
	p.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errors.New("cache eviction did not join; serving dependencies retained")
	}
}

func TestKernelCachePressureLifecycle(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	root := t.TempDir()
	store, err := metadata.Open(filepath.Join(root, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := kernelFixtureSeed(root, store, "quiet", "held", "post"); err != nil {
		t.Fatal(err)
	}
	p := prepareKernelCachePressure(store, 2300, 2048)
	if err := p.join(time.Second); err != nil {
		t.Fatal(err)
	}
	clock := func() int64 { return time.Now().UnixNano() }
	if err := p.start(store, clock); err != nil {
		t.Fatal(err)
	}
	if err := p.start(store, clock); err == nil {
		t.Fatal("duplicate eviction admitted")
	}
	if err := p.join(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	status := p.snapshot()
	if !status.Completed || status.StartedNS <= 0 || status.FinishedNS < status.StartedNS || status.After != 2048 || status.MissingPins != 0 || status.EvictedChildren != 1200 {
		t.Fatalf("incomplete pressure proof: %+v", status)
	}
	for _, phase := range []string{"quiet", "held", "post"} {
		for i := 0; i < 4; i++ {
			rows, err := store.ListChildrenForReadDir(fmt.Sprintf("%s/list-%d", phase, i))
			if err != nil || len(rows) != 100 {
				t.Fatalf("incomplete directory: %d %v", len(rows), err)
			}
		}
	}
}

func TestKernelCachePressureJoinRetainsOnTimeout(t *testing.T) {
	p := &kernelCachePressure{done: make(chan struct{})}
	if err := p.join(time.Millisecond); err == nil {
		t.Fatal("unfinished worker accepted")
	}
	close(p.done)
	if err := p.join(time.Second); err != nil {
		t.Fatal(err)
	}
}
