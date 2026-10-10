//go:build redisoptionsacceptance

package nfs

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Admission observations are not source reads, cache publication, or Finder
// readiness. This test uses the supported, name-ordered paginated ReadDir mode.
func TestP97RepeatedReadDirEventuallyExaminesLaterVideo(t *testing.T) {
	for _, tc := range []struct {
		name       string
		videos     int
		text       int
		visits     int
		expireSeen bool
		wantIndex  int
	}{
		{"inside-127-positive-control", 0, 127, 1, false, 127},
		{"sparse-outside-128", 0, 128, 3, false, 128},
		{"expired-dedup-early-candidates", 8, 120, 3, true, 128},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, jobs := p93AdmissionFixture(t)
			h := x.jfs.handler
			mtime := time.Unix(1700000000, 123)
			for i := 0; i < tc.videos; i++ {
				p97Insert(t, x, fmt.Sprintf("a%03d.mp4", i), 4096, mtime, uint64(1000+i))
			}
			for i := 0; i < tc.text; i++ {
				p97Insert(t, x, fmt.Sprintf("b%03d.txt", i), 1, mtime, uint64(2000+i))
			}
			order := p93OrderedListing(t, x)
			position := -1
			for i, entry := range order {
				if entry.Name == "clip.mov" {
					position = i
				}
			}
			if len(order) != tc.wantIndex+1 || position != tc.wantIndex {
				t.Fatalf("fixture source order count=%d target_index=%d want=%d", len(order), position, tc.wantIndex)
			}
			target, resident := x.store.LookupCachedFileIdentity("media/clip.mov")
			binding, selected := x.session.SnapshotBinding()
			readerBinding, bound := x.session.Reader().VolumeBinding()
			if !resident || !selected || !bound || binding != readerBinding || !x.session.Qualified() || !h.readahead.previewAllowed() {
				t.Fatal("target, selected binding, or preview admission guard unqualified")
			}
			p93Log(t, struct {
				Case        string            `json:"case"`
				SourceOrder []p93ListingEntry `json:"measured_source_order"`
				TargetIndex int               `json:"target_index_zero_based"`
				Visits      int               `json:"predeclared_visits"`
			}{tc.name, order, position, tc.visits})
			var firstJobs []string
			targetObserved := false
			for visit := 1; visit <= tc.visits; visit++ {
				if tc.expireSeen && visit > 1 {
					// Model cooldown expiry without wall sleeps or clearing dedup.
					rm := h.readahead
					rm.mu.Lock()
					old := time.Now().Add(-previewCooldown - time.Second)
					for key := range rm.previewSeen {
						rm.previewSeen[key] = old
					}
					rm.mu.Unlock()
				}
				if got := p93OrderedListing(t, x); !reflect.DeepEqual(got, order) {
					t.Fatal("source children/order/metadata changed between visits")
				}
				current, resident := x.store.LookupCachedFileIdentity("media/clip.mov")
				currentBinding, selected := x.session.SnapshotBinding()
				readerBinding, bound := x.session.Reader().VolumeBinding()
				if !resident || current != target || !selected || !bound || currentBinding != binding || readerBinding != binding || !x.session.Qualified() {
					t.Fatal("target identity or selected/read binding changed between visits")
				}
				if _, queued := p93SchedulerState(h.readahead); queued != 0 || len(jobs) != 0 {
					t.Fatalf("previous visit not drained: queue=%d callbacks=%d", queued, len(jobs))
				}
				start := time.Now()
				p93ActualReadDir(t, x, order) // complete name, size, mtime, mode, inode oracle
				observed := p97ObservedSince(t, h.readahead, jobs, start, target)
				if tc.name == "sparse-outside-128" && visit == 1 && len(observed) != 0 {
					t.Fatalf("initial scan exceeded 128 examined children: %v", observed)
				}
				if visit == 1 {
					firstJobs = append([]string(nil), observed...)
				}
				for _, path := range observed {
					if path == target.Path {
						targetObserved = true
					}
				}
				seen, queued := p93SchedulerState(h.readahead)
				p93Log(t, struct {
					Case            string   `json:"case"`
					Visit           int      `json:"visit"`
					TargetIndex     int      `json:"target_index_zero_based"`
					ObservedJobs    []string `json:"admitted_observer_paths"`
					TargetObserved  bool     `json:"target_observed_cumulative"`
					Seen            int      `json:"dedup_size"`
					QueueAfterDrain int      `json:"queue_after_drain"`
				}{tc.name, visit, position, observed, targetObserved, seen, queued})
				if queued != 0 {
					t.Fatal("preview queue remained after observer drain")
				}
			}
			if !targetObserved {
				t.Fatalf("eligible target at index %d never admitted in %d unchanged ReadDir visits; first observed=%v; dedup_expired_between_visits=%t", position, tc.visits, firstJobs, tc.expireSeen)
			}
		})
	}
}

// Default RAM iteration is intentionally unordered. ReadDir must reuse its
// sorted children snapshot for both the full listing and cursor scan.
func TestP97DensePreviewProgressWithExpiredDedupOnDefaultRAM(t *testing.T) {
	x, jobs := p93AdmissionFixture(t)
	t.Setenv("JM_READDIR_PAGINATED", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	mtime := time.Unix(1700000000, 123)
	for i := 0; i < previewChildrenLimit; i++ {
		p97Insert(t, x, fmt.Sprintf("a%03d.mp4", i), 4096, mtime, uint64(1000+i))
	}
	order := p97SortedOrder(t, x)
	if len(order) != previewChildrenLimit+1 || order[len(order)-1].Name != "clip.mov" {
		t.Fatalf("dense source order count=%d last=%q", len(order), order[len(order)-1].Name)
	}
	target, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok || !x.session.Qualified() {
		t.Fatal("dense target/session not qualified")
	}
	var first []string
	targetObserved := false
	covered := make(map[string]bool, len(order))
	for visit := 1; visit <= 17; visit++ {
		if visit > 1 {
			rm := x.jfs.handler.readahead
			rm.mu.Lock()
			old := time.Now().Add(-previewCooldown - time.Second)
			for key := range rm.previewSeen {
				rm.previewSeen[key] = old
			}
			rm.mu.Unlock()
		}
		start := time.Now()
		p97ActualSortedReadDir(t, x, order)
		observed := p97ObservedSince(t, x.jfs.handler.readahead, jobs, start, target)
		if visit == 1 {
			first = append([]string(nil), observed...)
			if len(first) != previewCandidatesLimit {
				t.Fatalf("first dense scan admitted %d; want %d", len(first), previewCandidatesLimit)
			}
		}
		if visit == 2 && reflect.DeepEqual(observed, first) {
			t.Fatal("expired dedup restarted dense scan at the first eight")
		}
		for _, path := range observed {
			covered[path] = true
			if path == target.Path {
				targetObserved = true
			}
		}
		p93Log(t, struct {
			Visit          int      `json:"visit"`
			Observed       []string `json:"admitted_observer_paths"`
			TargetObserved bool     `json:"target_observed_cumulative"`
		}{visit, observed, targetObserved})
	}
	if !targetObserved {
		t.Fatal("dense eligible target beyond initial 128 never reached in 17 bounded visits")
	}
	if len(covered) != len(order) {
		t.Fatalf("dense cursor skipped examined-position progress: unique=%d want=%d", len(covered), len(order))
	}
	for _, entry := range order {
		if !covered["media/"+entry.Name] {
			t.Fatalf("dense eligible entry never examined: %s", entry.Name)
		}
	}
}

func p97SortedOrder(t *testing.T, x *boundCacheFixture) []p93ListingEntry {
	t.Helper()
	order := p93OrderedListing(t, x)
	sort.Slice(order, func(i, j int) bool { return order[i].Name < order[j].Name })
	return order
}

func p97ActualSortedReadDir(t *testing.T, x *boundCacheFixture, order []p93ListingEntry) {
	t.Helper()
	if before := p97SortedOrder(t, x); !reflect.DeepEqual(before, order) {
		t.Fatal("default RAM source set/metadata changed before ReadDir")
	}
	infos := p86ReadDirPrompt(t, x)
	if got := p93FileInfoListing(t, infos); !reflect.DeepEqual(got, order) {
		t.Fatalf("default RAM ReadDir listing or metadata differs: got=%+v want=%+v", got, order)
	}
	if after := p97SortedOrder(t, x); !reflect.DeepEqual(after, order) {
		t.Fatal("default RAM source set/metadata changed after ReadDir")
	}
}

func TestP97PreviewCursorCapEmptyAndStop(t *testing.T) {
	x, jobs := p93AdmissionFixture(t)
	rm := x.jfs.handler.readahead
	binding := p97SelectedBinding(t, x)
	for i := 0; i < previewCursorLimit+1; i++ {
		x.jfs.handler.scheduleNativePreviews(fmt.Sprintf("dir-%03d", i), []*metadata.Entry{nil})
	}
	rm.previewScanMu.Lock()
	count := len(rm.previewCursors)
	_, firstPresent := rm.previewCursors[previewCursorKey{dir: "dir-000", binding: binding}]
	_, lastPresent := rm.previewCursors[previewCursorKey{dir: fmt.Sprintf("dir-%03d", previewCursorLimit), binding: binding}]
	rm.previewScanMu.Unlock()
	if count != previewCursorLimit || firstPresent || !lastPresent || len(jobs) != 0 {
		t.Fatalf("cursor bound/eviction invalid: count=%d first=%t last=%t jobs=%d", count, firstPresent, lastPresent, len(jobs))
	}
	rm.clearPreviewCursors(fmt.Sprintf("dir-%03d", previewCursorLimit))
	rm.previewScanMu.Lock()
	cleared := len(rm.previewCursors)
	rm.previewScanMu.Unlock()
	if cleared != previewCursorLimit-1 {
		t.Fatalf("empty directory did not remove cursor: %d", cleared)
	}
	if !x.jfs.handler.StopHandler() {
		t.Fatal("handler did not stop")
	}
	x.jfs.handler.scheduleNativePreviews("after-stop", []*metadata.Entry{nil})
	rm.previewScanMu.Lock()
	afterStop := len(rm.previewCursors)
	rm.previewScanMu.Unlock()
	if afterStop != cleared {
		t.Fatalf("stopped handler admitted cursor state: before=%d after=%d", cleared, afterStop)
	}
}

func p97SelectedBinding(t *testing.T, x *boundCacheFixture) cache.VolumeBinding {
	t.Helper()
	b, ok := x.session.SnapshotBinding()
	if !ok {
		t.Fatal("selected binding absent")
	}
	return b
}

func p97Insert(t *testing.T, x *boundCacheFixture, name string, size int64, mtime time.Time, inode uint64) {
	t.Helper()
	e := metadata.MakeEntry("media/"+name, false, size, mtime, inode)
	e.MtimeNsecKnown = true
	if err := x.store.Insert(e); err != nil {
		t.Fatal(err)
	}
}

func p97ObservedSince(t *testing.T, rm *ReadaheadManager, jobs <-chan previewJob, start time.Time, target metadata.CachedFileIdentity) []string {
	t.Helper()
	rm.mu.Lock()
	want := make([]string, 0, previewCandidatesLimit)
	for key, at := range rm.previewSeen {
		if !at.Before(start) {
			want = append(want, key.identity.Path)
		}
	}
	rm.mu.Unlock()
	if len(want) > previewCandidatesLimit {
		t.Fatalf("admissions exceed per-listing candidate budget: %d", len(want))
	}
	observed := make([]string, 0, len(want))
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for range want {
		select {
		case job := <-jobs:
			if job.key.identity.Path == target.Path && job.key.identity != target {
				t.Fatal("target observer identity differs from resident target")
			}
			observed = append(observed, job.key.identity.Path)
		case <-deadline.C:
			t.Fatalf("preview observer did not drain: observed=%v want=%v", observed, want)
		}
	}
	sort.Strings(want)
	sort.Strings(observed)
	if !reflect.DeepEqual(observed, want) {
		t.Fatalf("observed jobs differ from admission map: observed=%v want=%v", observed, want)
	}
	return observed
}
