package farm

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// testWatcher builds a Watcher over recorders: resolves come from a map,
// enqueues append to a slice. Settle 60s, cap injectable.
func testWatcher(resolves map[uint64]string, cap int) (*Watcher, *[]string) {
	enqueued := &[]string{}
	w := NewWatcher(WatchConfig{
		Mount: "/vol",
		Resolve: func(_ context.Context, ino uint64) (string, bool) {
			p, ok := resolves[ino]
			return p, ok
		},
		Enqueue: func(_ context.Context, rel string) error {
			*enqueued = append(*enqueued, rel)
			return nil
		},
		Settle:     60 * time.Second,
		Tick:       15 * time.Second,
		MaxPerTick: cap,
	})
	return w, enqueued
}

func TestWatchSettleGate(t *testing.T) {
	w, got := testWatcher(map[uint64]string{42: "clips/reel1"}, 200)
	t0 := time.Now()
	w.Note(42, t0)

	// 30s after the event: NOT settled — nothing may be enqueued.
	if n := w.Tick(context.Background(), t0.Add(30*time.Second)); n != 0 || len(*got) != 0 {
		t.Fatalf("unsettled dir processed: n=%d got=%v", n, *got)
	}
	// A fresh event re-arms the window.
	w.Note(42, t0.Add(45*time.Second))
	if n := w.Tick(context.Background(), t0.Add(70*time.Second)); n != 0 {
		t.Fatalf("re-dirtied dir processed before new settle window elapsed: n=%d", n)
	}
	// Quiet past the window: processed exactly once.
	if n := w.Tick(context.Background(), t0.Add(120*time.Second)); n != 1 || len(*got) != 1 || (*got)[0] != "clips/reel1" {
		t.Fatalf("settled dir not enqueued once: n=%d got=%v", n, *got)
	}
}

func TestWatchFeedbackLoopGuard(t *testing.T) {
	cases := map[string]bool{
		"clips/reel1":                        true,
		"Film Projects/Job A":                true,
		".juicemount/derivatives/42":         false,
		".trash/2026/clip.mov":               false,
		"a/.juicemount/x":                    false, // dot-dir at any depth
		".Spotlight-V100/Store":              false,
		"":                                   false,
		".":                                  false,
		"/":                                  false,
		"clips/.hidden/inner":                false,
		"clips/reel.with.dots":               true, // dots INSIDE a name are fine
		"logged footage/A001_0101/A001.braw": true,
	}
	for rel, want := range cases {
		if got := WatchPathAllowed(rel); got != want {
			t.Errorf("WatchPathAllowed(%q) = %v, want %v", rel, got, want)
		}
	}

	// End-to-end: a settled dir resolving into .juicemount must not enqueue.
	w, got := testWatcher(map[uint64]string{7: ".juicemount/derivatives/99"}, 200)
	t0 := time.Now()
	w.Note(7, t0)
	if n := w.Tick(context.Background(), t0.Add(2*time.Minute)); n != 0 || len(*got) != 0 {
		t.Fatalf("derivative-namespace dir was enqueued: %v", *got)
	}
}

func TestWatchRootIgnored(t *testing.T) {
	w, got := testWatcher(map[uint64]string{1: ""}, 200)
	w.Note(1, time.Now())
	w.Note(0, time.Now())
	if n := w.Tick(context.Background(), time.Now().Add(5*time.Minute)); n != 0 || len(*got) != 0 {
		t.Fatalf("root inode produced work: %v", *got)
	}
}

func TestWatchPerTickCapAndCarryover(t *testing.T) {
	resolves := map[uint64]string{}
	for i := uint64(10); i < 20; i++ {
		resolves[i] = fmt.Sprintf("dir%d", i)
	}
	w, got := testWatcher(resolves, 3)
	t0 := time.Now()
	for i := uint64(10); i < 20; i++ {
		w.Note(i, t0)
	}
	late := t0.Add(5 * time.Minute)
	if n := w.Tick(context.Background(), late); n != 3 {
		t.Fatalf("tick 1 enqueued %d, want cap 3", n)
	}
	if n := w.Tick(context.Background(), late); n != 3 {
		t.Fatalf("tick 2 enqueued %d, want 3 (carryover)", n)
	}
	total := len(*got)
	w.Tick(context.Background(), late)
	w.Tick(context.Background(), late)
	if len(*got) != 10 {
		t.Fatalf("after draining, enqueued %d (was %d after 2 ticks), want all 10", len(*got), total)
	}
}

func TestWatchDedupeTTL(t *testing.T) {
	w, got := testWatcher(map[uint64]string{42: "clips/reel1"}, 200)
	t0 := time.Now()
	w.Note(42, t0)
	late := t0.Add(2 * time.Minute)
	w.Tick(context.Background(), late)
	// Same dir dirtied again, settles again inside the dedupe TTL: suppressed.
	w.Note(42, late)
	if n := w.Tick(context.Background(), late.Add(2*time.Minute)); n != 0 {
		t.Fatalf("dedupe TTL failed: re-enqueued within window")
	}
	// Past the TTL: allowed again.
	w.Note(42, late.Add(11*time.Minute))
	if n := w.Tick(context.Background(), late.Add(13*time.Minute)); n != 1 {
		t.Fatalf("post-TTL re-enqueue blocked")
	}
	if len(*got) != 2 {
		t.Fatalf("total enqueues = %d, want 2", len(*got))
	}
}

func TestWatchPrunesOverlappingAncestors(t *testing.T) {
	w, got := testWatcher(map[uint64]string{
		10: "Projects",
		11: "Projects/Film A",
		12: "Projects/Film A/Reel 1",
		13: "Projects/Film B/Reel 2",
	}, 200)
	t0 := time.Now()
	for _, inode := range []uint64{10, 11, 12, 13} {
		w.Note(inode, t0)
	}
	if n := w.Tick(context.Background(), t0.Add(2*time.Minute)); n != 2 {
		t.Fatalf("enqueued %d overlapping targets, want 2 leaf batches (%v)", n, *got)
	}
	want := map[string]bool{"Projects/Film A/Reel 1": true, "Projects/Film B/Reel 2": true}
	for _, path := range *got {
		if !want[path] {
			t.Fatalf("unexpected overlapping batch %q (all=%v)", path, *got)
		}
	}
}

func TestWatchEnqueueFailureRetries(t *testing.T) {
	fail := true
	var got []string
	w := NewWatcher(WatchConfig{
		Mount:   "/vol",
		Resolve: func(_ context.Context, ino uint64) (string, bool) { return "clips/x", true },
		Enqueue: func(_ context.Context, rel string) error {
			if fail {
				return errors.New("redis down")
			}
			got = append(got, rel)
			return nil
		},
		Settle: 60 * time.Second, MaxPerTick: 10,
	})
	t0 := time.Now()
	w.Note(42, t0)
	late := t0.Add(2 * time.Minute)
	if n := w.Tick(context.Background(), late); n != 0 {
		t.Fatal("failed enqueue counted as success")
	}
	fail = false
	// The dir was re-dirtied at `late`; settle again and retry succeeds.
	if n := w.Tick(context.Background(), late.Add(2*time.Minute)); n != 1 || len(got) != 1 {
		t.Fatalf("retry after enqueue failure didn't happen: n=%d got=%v", n, got)
	}
}

func TestWatchDirtyOverflow(t *testing.T) {
	resolves := map[uint64]string{}
	w, got := testWatcher(resolves, 10)
	w.cfg.MaxDirty = 5
	t0 := time.Now()
	for i := uint64(10); i < 30; i++ {
		w.Note(i, t0)
	}
	w.mu.Lock()
	dirtyLen, overflow := len(w.dirty), w.overflow
	w.mu.Unlock()
	if dirtyLen != 5 || overflow != 15 {
		t.Fatalf("dirty=%d overflow=%d, want 5/15", dirtyLen, overflow)
	}
	// Known inodes still update their timestamps at capacity.
	w.Note(10, t0.Add(time.Second))
	w.mu.Lock()
	if !w.dirty[10].After(t0) {
		t.Fatal("existing inode timestamp not refreshed at capacity")
	}
	w.mu.Unlock()
	_ = got
}

func TestWatchKindsFromEnv(t *testing.T) {
	t.Setenv("JM_FARM_WATCH_KINDS", "")
	if k := WatchKindsFromEnv(); len(k) != 3 || k[0] != "derivatives" || k[1] != "proxy" || k[2] != "transcript" {
		t.Fatalf("default kinds = %v", k)
	}
	t.Setenv("JM_FARM_WATCH_KINDS", "derivatives, transcript")
	if k := WatchKindsFromEnv(); len(k) != 2 || k[1] != "transcript" {
		t.Fatalf("parsed kinds = %v", k)
	}
	t.Setenv("JM_FARM_WATCH", "0")
	if WatchEnabled() {
		t.Fatal("kill switch ignored")
	}
	t.Setenv("JM_FARM_WATCH", "")
	if !WatchEnabled() {
		t.Fatal("default should be enabled")
	}
}

func TestWatchPausePreservesDirtyWork(t *testing.T) {
	enabled := false
	var got []string
	w := NewWatcher(WatchConfig{
		Settle:  time.Second,
		Tick:    time.Second,
		Enabled: func(context.Context) bool { return enabled },
		Resolve: func(context.Context, uint64) (string, bool) { return "ingest/reel-1", true },
		Enqueue: func(_ context.Context, path string) error {
			got = append(got, path)
			return nil
		},
	})
	t0 := time.Now()
	w.Note(42, t0)
	if n := w.Tick(context.Background(), t0.Add(2*time.Second)); n != 0 {
		t.Fatalf("paused watcher enqueued %d jobs", n)
	}
	enabled = true
	if n := w.Tick(context.Background(), t0.Add(3*time.Second)); n != 1 || len(got) != 1 {
		t.Fatalf("resume enqueued %d jobs, got %v", n, got)
	}
}
