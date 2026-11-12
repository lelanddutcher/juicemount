package pin

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fakePinner scripts juicefs's pin message.
type fakePinner struct {
	mu       sync.Mutex
	size     int64
	steps    int // Status polls before the fill ends
	polls    int
	failWith syscall.Errno
	fetched  int64
	startErr error
	started  []uint64
	known    map[uint64]bool
	missing  map[uint64]int64
	kept     []uint64
	onPoll   func(poll int)
}

func (f *fakePinner) Start(inode uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return f.startErr
	}
	f.started = append(f.started, inode)
	f.polls = 0
	return nil
}

func (f *fakePinner) Status(inode uint64) (BlockFillStatus, error) {
	f.mu.Lock()
	f.polls++
	poll, steps, size := f.polls, f.steps, f.size
	hook := f.onPoll
	f.mu.Unlock()
	if hook != nil {
		hook(poll)
	}
	st := BlockFillStatus{Total: size, State: BlockFillRunning}
	if poll < steps {
		st.Done = size * int64(poll) / int64(steps)
		return st, nil
	}
	if f.failWith != 0 {
		st.State, st.Errno, st.Done = BlockFillFailed, f.failWith, size/2
		return st, nil
	}
	f.mu.Lock()
	if f.known == nil {
		f.known = map[uint64]bool{}
	}
	f.known[inode] = true
	f.mu.Unlock()
	st.State, st.Done, st.Fetched = BlockFillDone, size, f.fetched
	return st, nil
}

func (f *fakePinner) Check(inode uint64) (bool, int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.known[inode], f.size, f.missing[inode], nil
}

func (f *fakePinner) Sweep(keep []uint64) (int, int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kept = append([]uint64(nil), keep...)
	return 1, f.size, 9 << 30, nil
}

// guardProvider adds the witness inode lookup the nfs provider has.
type guardProvider struct {
	*testOwnedPinProvider
	inode uint64
}

func (g *guardProvider) ReadyInode(e Entry) (uint64, bool) { return g.inode, len(e.Authority) != 0 }

func fastPolls(t *testing.T) {
	oldPoll, oldSave := blockFillPoll, progressSaveEvery
	blockFillPoll, progressSaveEvery = time.Millisecond, 0
	t.Cleanup(func() { blockFillPoll, progressSaveEvery = oldPoll, oldSave })
}

func claimAndPrefetch(t *testing.T, s *Store, p *Prefetcher, path string) error {
	t.Helper()
	claimed, ok, err := s.ClaimPrefetch(pinEntry(t, s, path))
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	return p.prefetch(claimed)
}

func TestBlockPinFillsWholeFileThenHashesWithProgress(t *testing.T) {
	fastPolls(t)
	data := []byte(strings.Repeat("pinned clip bytes ", 1000))
	s, p, path := pinnedFixture(t, "clip.mov", data)
	f := &fakePinner{size: int64(len(data)), steps: 4}
	var midway []Entry
	f.onPoll = func(poll int) {
		if poll == 3 {
			midway = append(midway, pinEntry(t, s, path))
		}
	}
	p.SetBlockPinner(f)
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	e := pinEntry(t, s, path)
	if e.Status != StatusReady || len(f.started) != 1 || f.started[0] == 0 {
		t.Fatalf("after fill: %+v, starts %v", e, f.started)
	}
	if len(midway) != 1 || midway[0].Status != StatusPrefetching || midway[0].BytesCached == 0 || midway[0].BytesCached >= int64(len(data)) {
		t.Fatalf("no byte progress while the fill ran: %+v", midway)
	}
	if got := p.LiveStats().BytesPrefetched; got != int64(len(data)) {
		t.Fatalf("live bytes %d, want %d (the local hash read must not count again)", got, len(data))
	}
	if a, _ := s.AggregateStats(); a.CachedBytes != int64(len(data)) || a.ReadyFiles != 1 {
		t.Fatalf("aggregate %+v", a)
	}
}

func TestBlockPinUnsupportedKeepsReadThrough(t *testing.T) {
	fastPolls(t)
	data := []byte("old juicefs")
	s, p, path := pinnedFixture(t, "clip.mov", data)
	p.SetBlockPinner(&fakePinner{startErr: ErrBlockPinUnsupported})
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	if e := pinEntry(t, s, path); e.Status != StatusReady {
		t.Fatalf("fallback: %+v", e)
	}
	if got := p.LiveStats().BytesPrefetched; got != int64(len(data)) {
		t.Fatalf("read-through bytes %d", got)
	}
}

func TestBlockPinFailureIsRetriedWithBackoff(t *testing.T) {
	fastPolls(t)
	data := []byte(strings.Repeat("x", 4096))
	s, p, path := pinnedFixture(t, "clip.mov", data)
	f := &fakePinner{size: int64(len(data)), steps: 2, failWith: syscall.EIO}
	p.SetBlockPinner(f)
	if err := claimAndPrefetch(t, s, p, path); !errors.Is(err, syscall.EIO) {
		t.Fatalf("fill failure: %v", err)
	}
	e := pinEntry(t, s, path)
	if e.Status != StatusFailed || !strings.Contains(e.LastError, "juicefs pin fill") || e.BytesCached != int64(len(data))/2 {
		t.Fatalf("failed entry: %+v", e)
	}
	if n := p.retryFailed(time.Now()); n != 0 {
		t.Fatal("retried before the first backoff")
	}
	if n := p.retryFailed(time.Now().Add(31 * time.Second)); n != 1 {
		t.Fatal("not retried after 30 s")
	}
	if e := pinEntry(t, s, path); e.Status != StatusPending {
		t.Fatalf("retry did not requeue: %+v", e)
	}
	// It fails again; the next retry waits 60 s.
	if err := claimAndPrefetch(t, s, p, path); err == nil {
		t.Fatal("second attempt did not fail")
	}
	if n := p.retryFailed(time.Now().Add(31 * time.Second)); n != 0 {
		t.Fatal("second retry ignored the doubled backoff")
	}
	if n := p.retryFailed(time.Now().Add(61 * time.Second)); n != 1 {
		t.Fatal("second retry never came")
	}
	f.mu.Lock()
	f.failWith = 0
	f.mu.Unlock()
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	if p.retries[path] != 0 {
		t.Fatal("success did not reset the backoff")
	}
}

func TestBlockPinShareFullIsReportedAsFailed(t *testing.T) {
	fastPolls(t)
	data := []byte("too big for the pin share")
	s, p, path := pinnedFixture(t, "clip.mov", data)
	p.SetBlockPinner(&fakePinner{size: int64(len(data)), steps: 1, failWith: syscall.ENOSPC})
	err := claimAndPrefetch(t, s, p, path)
	if !errors.Is(err, ErrPinnedShareFull) || !errors.Is(err, ErrInsufficientCacheCapacity) {
		t.Fatalf("cap refusal: %v", err)
	}
	if e := pinEntry(t, s, path); e.Status != StatusFailed || !strings.Contains(e.LastError, "90%") {
		t.Fatalf("cap refusal must be Failed with the reason, not silently pending: %+v", e)
	}
}

func TestPinGuardRepinsAfterRelaunchAndRequeuesLostBlocks(t *testing.T) {
	fastPolls(t)
	data := []byte(strings.Repeat("ready", 100))
	s, p, path := pinnedFixture(t, "clip.mov", data)
	gp := &guardProvider{testOwnedPinProvider: p.provider.(*testOwnedPinProvider), inode: 4242}
	p.provider = gp
	f := &fakePinner{size: int64(len(data)), steps: 1}
	p.SetBlockPinner(f)
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}

	// A relaunched juicefs holds no pins: the guard pins again; present
	// blocks download nothing, so Ready stays.
	f.mu.Lock()
	f.known, f.started = nil, nil
	f.mu.Unlock()
	rep := p.GuardPins(context.Background())
	if rep.Repinned != 1 || rep.Requeued != 0 || len(f.started) != 1 || f.started[0] != 4242 {
		t.Fatalf("relaunch guard: %+v starts %v", rep, f.started)
	}
	if !slices.Contains(f.kept, 4242) || !rep.Supported || rep.Pinned != int64(len(data)) {
		t.Fatalf("sweep kept %v, report %+v", f.kept, rep)
	}
	if e := pinEntry(t, s, path); e.Status != StatusReady {
		t.Fatalf("re-pin of present blocks revoked Ready: %+v", e)
	}
	if p.LiveStats().PinLimitBytes != 9<<30 {
		t.Fatal("pin cap not reported")
	}

	// Blocks lost while pinned: Ready goes back to the workers.
	f.mu.Lock()
	f.missing = map[uint64]int64{4242: 100}
	f.mu.Unlock()
	if rep := p.GuardPins(context.Background()); rep.Requeued != 1 {
		t.Fatalf("lost blocks not requeued: %+v", rep)
	}
	if e := pinEntry(t, s, path); e.Status != StatusPending {
		t.Fatalf("entry with lost blocks still %v", e.Status)
	}

	// A re-pin after relaunch that had to download: also requeued.
	f.mu.Lock()
	f.missing, f.known = nil, nil
	f.mu.Unlock()
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.known, f.fetched = nil, 10
	f.mu.Unlock()
	if rep := p.GuardPins(context.Background()); rep.Repinned != 1 || rep.Requeued != 1 {
		t.Fatalf("download during re-pin: %+v", rep)
	}
}

func TestPinGuardSweepDropsUnpinnedFiles(t *testing.T) {
	fastPolls(t)
	s, p, path := pinnedFixture(t, "clip.mov", []byte("bytes"))
	p.provider = &guardProvider{testOwnedPinProvider: p.provider.(*testOwnedPinProvider), inode: 7}
	f := &fakePinner{size: 5, steps: 1}
	p.SetBlockPinner(f)
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	if err := s.UnpinPath(path); err != nil {
		t.Fatal(err)
	}
	p.GuardPins(context.Background())
	if len(f.kept) != 0 {
		t.Fatalf("sweep kept %v after unpin", f.kept)
	}
}

func TestProgressRateAndETA(t *testing.T) {
	var m progressMeter
	t0 := time.Unix(1000, 0)
	for i := 0; i <= 10; i++ {
		m.add(t0.Add(time.Duration(i)*time.Second), int64(i)*10<<20)
	}
	r := m.rate(t0.Add(10 * time.Second))
	if r < 9.9*(1<<20) || r > 10.1*(1<<20) {
		t.Fatalf("rate %.0f, want 10 MiB/s", r)
	}
	if m.rate(t0.Add(20*time.Second)) != 0 {
		t.Fatal("a stalled pass still has a rate")
	}
	a := AggregateStats{TotalBytes: 300 << 20, CachedBytes: 100 << 20, PrefetchingFiles: 1}
	if eta := ETASeconds(a, 10<<20); eta != 20 {
		t.Fatalf("eta %d, want 20", eta)
	}
	if ETASeconds(AggregateStats{TotalBytes: 1, ReadyFiles: 1}, 1) != 0 {
		t.Fatal("eta with nothing left to fetch")
	}
	for n, want := range map[int]time.Duration{1: 30 * time.Second, 2: time.Minute, 3: 2 * time.Minute, 10: 30 * time.Minute} {
		if got := retryBackoff(n); got != want {
			t.Fatalf("backoff %d = %v, want %v", n, got, want)
		}
	}
}
