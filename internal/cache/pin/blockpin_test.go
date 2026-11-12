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
	changed  map[uint64]bool
	kept     []uint64
	onPoll   func(poll int)
	hang     bool // fills never end (no server)
	canceled []uint64
	capacity int64
	workers  []int
	// compacted inodes fail only a Verify with slices.
	compacted    map[uint64]bool
	sliceChecked []uint64
	sweepErr     error
	sweeps       int
}

func (f *fakePinner) Cancel(inode uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.canceled = append(f.canceled, inode)
	return nil
}

func (f *fakePinner) SetCapacity(bytes int64) (int64, int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.capacity = bytes
	return bytes, f.size, bytes * 9 / 10, nil
}

func (f *fakePinner) Verify(items []PinVerifyItem, slices bool) ([]PinVerifyResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []PinVerifyResult
	for _, it := range items {
		if slices {
			f.sliceChecked = append(f.sliceChecked, it.Inode)
		}
		switch {
		case slices && f.compacted[it.Inode]:
			out = append(out, PinVerifyResult{Inode: it.Inode, Reason: PinChanged})
		case !f.known[it.Inode]:
			out = append(out, PinVerifyResult{Inode: it.Inode, Reason: PinUnknown})
		case f.changed[it.Inode]:
			out = append(out, PinVerifyResult{Inode: it.Inode, Reason: PinChanged})
		case f.missing[it.Inode] > 0:
			out = append(out, PinVerifyResult{Inode: it.Inode, Reason: PinIncomplete, Missing: f.missing[it.Inode]})
		}
	}
	return out, nil
}

func (f *fakePinner) Start(inode uint64, workers int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return f.startErr
	}
	f.started = append(f.started, inode)
	f.workers = append(f.workers, workers)
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
	f.mu.Lock()
	hang := f.hang
	f.mu.Unlock()
	if hang {
		return st, nil
	}
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
	f.sweeps++
	if f.sweepErr != nil {
		return 0, 0, 0, f.sweepErr
	}
	f.kept = append([]uint64(nil), keep...)
	return 1, f.size, 9 << 30, nil
}

// guardProvider adds the witness inode lookup the nfs provider has.
type guardProvider struct {
	*testOwnedPinProvider
	inode   uint64
	current ExpectedFile // the mirror now; zero means same as the witness
}

func (g *guardProvider) PinIdentity(e Entry) (uint64, ExpectedFile, bool) {
	cur := g.current
	if cur.Inode == 0 {
		cur = ExpectedFile{Inode: g.inode, Size: e.Size, MtimeNS: 1}
	}
	return g.inode, cur, len(e.Authority) != 0
}

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
	if e := pinEntry(t, s, path); e.Status != StatusFailed || !strings.Contains(e.LastError, "unpin a folder") {
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

// M1: a Ready pin made from another version of the file goes back to the
// workers; so does one whose file was replaced (another inode).
func TestPinGuardRequeuesChangedFiles(t *testing.T) {
	fastPolls(t)
	s, p, path := pinnedFixture(t, "clip.mov", []byte("version one"))
	gp := &guardProvider{testOwnedPinProvider: p.provider.(*testOwnedPinProvider), inode: 11}
	p.provider = gp
	f := &fakePinner{size: 11, steps: 1}
	p.SetBlockPinner(f)
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.known = map[uint64]bool{11: true}
	f.changed = map[uint64]bool{11: true}
	f.mu.Unlock()
	if rep := p.GuardPins(context.Background()); rep.Requeued != 1 || rep.Checked != 1 {
		t.Fatalf("changed file: %+v", rep)
	}
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	gp.current = ExpectedFile{Inode: 12, Size: 11, MtimeNS: 1}
	if rep := p.GuardPins(context.Background()); rep.Requeued != 1 {
		t.Fatalf("replaced file: %+v", rep)
	}
}

// H1: with no server, a re-pin never ends; the guard gives up after its
// timeout and cancels the fill instead of blocking forever.
func TestPinGuardRepinTimesOutOffline(t *testing.T) {
	fastPolls(t)
	old := guardFillTimeout
	guardFillTimeout = 50 * time.Millisecond
	defer func() { guardFillTimeout = old }()
	s, p, path := pinnedFixture(t, "clip.mov", []byte("abroad"))
	p.provider = &guardProvider{testOwnedPinProvider: p.provider.(*testOwnedPinProvider), inode: 21}
	f := &fakePinner{size: 6, steps: 1}
	p.SetBlockPinner(f)
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.known, f.hang = nil, true
	f.mu.Unlock()
	done := make(chan GuardReport, 1)
	go func() { done <- p.GuardPins(context.Background()) }()
	select {
	case rep := <-done:
		if rep.Repinned != 0 || rep.Requeued != 0 || !rep.Supported {
			t.Fatalf("offline pass: %+v", rep)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("guard blocked on an offline re-pin")
	}
	if !slices.Contains(f.canceled, 21) {
		t.Fatal("the timed-out fill was not canceled")
	}
	if e := pinEntry(t, s, path); e.Status != StatusReady {
		t.Fatalf("offline pass revoked Ready: %+v", e)
	}
}

// H3: unpinning a file while its fill runs cancels the fill in juicefs.
func TestUnpinDuringFillCancels(t *testing.T) {
	fastPolls(t)
	old := liveCheckEvery
	liveCheckEvery = time.Millisecond
	defer func() { liveCheckEvery = old }()
	s, p, path := pinnedFixture(t, "clip.mov", []byte("large file"))
	f := &fakePinner{size: 10, hang: true}
	p.SetBlockPinner(f)
	p.SetFillWorkers(func() int { return 1 })
	claimed, ok, err := s.ClaimPrefetch(pinEntry(t, s, path))
	if err != nil || !ok {
		t.Fatal(err)
	}
	res := make(chan error, 1)
	go func() { res <- p.prefetch(claimed) }()
	time.Sleep(20 * time.Millisecond)
	if err := s.UnpinPath(path); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-res:
		if !errors.Is(err, ErrPinSuperseded) {
			t.Fatalf("fill after unpin: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fill kept running after unpin")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.canceled) != 1 || len(f.workers) != 1 || f.workers[0] != 1 {
		t.Fatalf("cancel %v, workers %v", f.canceled, f.workers)
	}
}

// M7: a pin removed while the guard runs is neither re-pinned nor kept.
func TestPinGuardSkipsPinRemovedDuringPass(t *testing.T) {
	fastPolls(t)
	s, p, path := pinnedFixture(t, "clip.mov", []byte("bytes"))
	p.provider = &guardProvider{testOwnedPinProvider: p.provider.(*testOwnedPinProvider), inode: 31}
	f := &fakePinner{size: 5, steps: 1}
	p.SetBlockPinner(f)
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.known, f.started = nil, nil
	f.mu.Unlock()
	if err := s.UnpinPath(path); err != nil {
		t.Fatal(err)
	}
	p.GuardPins(context.Background())
	if len(f.started) != 0 || slices.Contains(f.kept, 31) {
		t.Fatalf("removed pin re-pinned %v or kept %v", f.started, f.kept)
	}
}

// H2: a refusal at the cap grows the cache for the whole pinned set, then
// the fill is tried again.
func TestShareFullGrowsCacheThenRetries(t *testing.T) {
	fastPolls(t)
	data := []byte("needs a bigger cache")
	s, p, path := pinnedFixture(t, "clip.mov", data)
	f := &fakePinner{size: int64(len(data)), steps: 1, failWith: syscall.ENOSPC}
	f.onPoll = func(int) {
		f.mu.Lock()
		if f.capacity > 0 {
			f.failWith = 0
		}
		f.mu.Unlock()
	}
	p.SetBlockPinner(f)
	cacheVolumeMu.RLock()
	dir := cacheVolumePath
	cacheVolumeMu.RUnlock()
	p.capacityDir = dir
	SetCacheBudgetBytes(int64(len(data)) + 1)
	ComputeCapacity(s, dir)
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatalf("after growing: %v", err)
	}
	if f.capacity != CacheBytesForPins(int64(len(data))) || CacheBudgetBytes() != f.capacity {
		t.Fatalf("capacity %d, budget %d", f.capacity, CacheBudgetBytes())
	}
	if want := int64(200<<30)*10/9 + int64(200<<30)/1024 + 1<<30; CacheBytesForPins(200<<30) != want {
		t.Fatal("cache size for pins")
	}
}

// L3: failed pins rotate oldest first; a deleted file stops after a few
// retries; maps drop unpinned paths.
func TestRetryStopsForMissingFiles(t *testing.T) {
	fastPolls(t)
	s, p, path := pinnedFixture(t, "clip.mov", []byte("x"))
	e := pinEntry(t, s, path)
	if _, err := s.UpdateStatusForEntry(e, StatusFailed, 0, "open: no such file or directory"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i := 1; i <= maxMissingRetries; i++ {
		now = now.Add(time.Hour)
		if p.retryFailed(now) != 1 {
			t.Fatalf("retry %d of a missing file did not run", i)
		}
		e = pinEntry(t, s, path)
		if _, err := s.UpdateStatusForEntry(e, StatusFailed, 0, "open: no such file or directory"); err != nil {
			t.Fatal(err)
		}
	}
	if p.retryFailed(now.Add(time.Hour)) != 0 {
		t.Fatal("a missing file is retried forever")
	}
	p.SetBlockPinner(&fakePinner{})
	_ = s.UnpinPath(path)
	p.GuardPins(context.Background())
	if len(p.retries) != 0 {
		t.Fatalf("retries kept for an unpinned path: %v", p.retries)
	}
}

// Re-review HIGH-2: a pin the guard requeues (authority cleared) keeps its
// juicefs pins, also after a relaunch offline, where no fill can learn the
// inode again.
func TestPinGuardKeepsRequeuedPinsOfflineAcrossRelaunch(t *testing.T) {
	fastPolls(t)
	data := []byte("trip clip")
	s, p, path := pinnedFixture(t, "clip.mov", data)
	gp := &guardProvider{testOwnedPinProvider: p.provider.(*testOwnedPinProvider), inode: 5150}
	p.provider = gp
	f := &fakePinner{size: int64(len(data)), steps: 1}
	p.SetBlockPinner(f)
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	// A pin made before the column existed: only the witness has the inode.
	if _, err := s.db.Exec(`UPDATE pinned_files SET pin_inode=0`); err != nil {
		t.Fatal(err)
	}
	// Offline: juicefs still holds the pin but a block went missing, so the
	// guard requeues the entry; the sweep of the same pass keeps it.
	f.mu.Lock()
	f.known = map[uint64]bool{5150: true}
	f.missing = map[uint64]int64{5150: 1}
	f.mu.Unlock()
	if rep := p.GuardPins(context.Background()); rep.Requeued != 1 || !slices.Contains(f.kept, 5150) {
		t.Fatalf("requeue pass: %+v kept %v", rep, f.kept)
	}
	if e := pinEntry(t, s, path); e.Status != StatusPending || len(e.Authority) != 0 {
		t.Fatalf("entry after requeue: %+v", e)
	}
	// Relaunch, still offline: a new prefetcher with no inode memory, and
	// the entry fails (no FUSE mount); every later pass keeps its pins.
	p2 := NewPrefetcher(s, t.TempDir(), "/Volumes/zpool", 1, gp)
	t.Cleanup(func() { p2.StopWithin(5 * time.Second) })
	p2.SetBlockPinner(f)
	e := pinEntry(t, s, path)
	if _, err := s.UpdateStatusForEntry(e, StatusFailed, 0, "no server"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		f.mu.Lock()
		f.kept = nil
		f.mu.Unlock()
		p2.GuardPins(context.Background())
		if !slices.Contains(f.kept, 5150) {
			t.Fatalf("pass %d after relaunch dropped the pins of a failed pin: kept %v", i, f.kept)
		}
	}
	// Unpinning still lets them go.
	if err := s.UnpinPath(path); err != nil {
		t.Fatal(err)
	}
	p2.GuardPins(context.Background())
	if slices.Contains(f.kept, 5150) {
		t.Fatalf("pins kept after unpin: %v", f.kept)
	}
}

// Re-review MEDIUM-5: the guard also checks Ready pins against their slices
// in metadata, a rotating batch per pass, and requeues a compacted file.
func TestPinGuardRequeuesCompactedFiles(t *testing.T) {
	fastPolls(t)
	s, p, path := pinnedFixture(t, "clip.mov", []byte("compact me"))
	p.provider = &guardProvider{testOwnedPinProvider: p.provider.(*testOwnedPinProvider), inode: 31}
	f := &fakePinner{size: 10, steps: 1}
	p.SetBlockPinner(f)
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.known = map[uint64]bool{31: true}
	f.mu.Unlock()
	if rep := p.GuardPins(context.Background()); rep.SliceChecked != 1 || rep.Requeued != 0 {
		t.Fatalf("current file: %+v", rep)
	}
	f.mu.Lock()
	f.compacted = map[uint64]bool{31: true}
	f.mu.Unlock()
	if rep := p.GuardPins(context.Background()); rep.Requeued != 1 {
		t.Fatalf("compacted file: %+v", rep)
	}
	if e := pinEntry(t, s, path); e.Status != StatusPending {
		t.Fatalf("compacted entry %v", e.Status)
	}
}

// Re-review MEDIUM-3: the guard's re-pins pass the same download width as
// the workers' fills, so a slow link's one-download limit covers both.
func TestPinGuardRepinUsesFillWidth(t *testing.T) {
	fastPolls(t)
	s, p, path := pinnedFixture(t, "clip.mov", []byte("slow link"))
	p.provider = &guardProvider{testOwnedPinProvider: p.provider.(*testOwnedPinProvider), inode: 41}
	f := &fakePinner{size: 9, steps: 1}
	p.SetBlockPinner(f)
	p.SetFillWorkers(func() int { return 1 })
	if err := claimAndPrefetch(t, s, p, path); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.known = nil
	f.mu.Unlock()
	if rep := p.GuardPins(context.Background()); rep.Repinned != 1 {
		t.Fatalf("re-pin: %+v", rep)
	}
	if len(f.workers) != 2 || f.workers[0] != 1 || f.workers[1] != 1 {
		t.Fatalf("fill widths %v, want 1 for the worker and the guard", f.workers)
	}
}

// Re-review MEDIUM-2: with whole-block pins off, one empty SWEEP clears the
// pins an earlier run persisted; it retries until juicefs answers.
func TestClearBlockPinsSweepsEverything(t *testing.T) {
	f := &fakePinner{sweepErr: errors.New("not mounted yet"), kept: []uint64{1}}
	go func() {
		time.Sleep(20 * time.Millisecond)
		f.mu.Lock()
		f.sweepErr = nil
		f.mu.Unlock()
	}()
	if err := ClearBlockPins(context.Background(), f, 5*time.Millisecond, 1000); err != nil {
		t.Fatal(err)
	}
	if len(f.kept) != 0 || f.sweeps < 2 {
		t.Fatalf("kept %v after %d sweeps", f.kept, f.sweeps)
	}
	old := &fakePinner{sweepErr: syscall.EINVAL}
	if err := ClearBlockPins(context.Background(), old, time.Millisecond, 1000); !errors.Is(err, syscall.EINVAL) || old.sweeps != 1 {
		t.Fatalf("old juicefs: %v after %d sweeps", err, old.sweeps)
	}
}
