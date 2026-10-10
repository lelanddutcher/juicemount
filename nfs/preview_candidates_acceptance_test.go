//go:build redisoptionsacceptance

package nfs

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

// This overlay observes scheduler admission only. A previewRun observer does
// not prove a FUSE source read, block publication, or offline cache readiness.
func p93AdmissionFixture(t *testing.T) (*boundCacheFixture, chan previewJob) {
	t.Helper()
	t.Setenv("JM_NATIVE_PREVIEW_WARM", "1")
	t.Setenv("JM_JFS_MAX_READAHEAD", "1M")
	// The production SQLite ReadDir path pages by name. The RAM substrate's
	// map iteration cannot establish a stable position at the 128-child edge.
	t.Setenv("JM_READDIR_PAGINATED", "1")
	x := newBoundCacheFixture(t)
	h := x.jfs.handler
	h.sidecar.enabled = false
	h.prefetchMu.Lock()
	h.prefetched["media"] = time.Now()
	h.prefetchMu.Unlock()
	profile := netprofile.New()
	profile.ObserveRTT(5 * time.Millisecond)
	h.readahead.profile = profile
	jobs := make(chan previewJob, 16)
	h.readahead.previewRun = func(job previewJob) { jobs <- job }
	t.Cleanup(func() {
		if !h.StopHandler() {
			t.Error("preview fixture handler did not stop")
			return
		}
		select {
		case <-h.readahead.previewDone:
		default:
			t.Error("preview worker was not joined")
		}
	})
	if !x.session.Qualified() {
		t.Fatal("selected session is not qualified")
	}
	selected, ok := x.session.SnapshotBinding()
	readerBinding, bound := x.session.Reader().VolumeBinding()
	_, remoteReady := x.session.RemoteObservation()
	if !ok || !bound || !remoteReady || selected != readerBinding || selected.BlockSize != previewBlockBytes {
		t.Fatalf("selected/read binding or remote observation invalid: snapshot=%t reader=%t remote=%t match=%t block=%d", ok, bound, remoteReady, selected == readerBinding, selected.BlockSize)
	}
	if !h.readahead.previewAllowed() {
		t.Fatal("preview health/opt-in guard refused fixture")
	}
	if seen, queued := p93SchedulerState(h.readahead); seen != 0 || queued != 0 {
		t.Fatalf("fixture admission state not empty: seen=%d queued=%d", seen, queued)
	}
	return x, jobs
}

func p93SchedulerState(rm *ReadaheadManager) (seen, queued int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return len(rm.previewSeen), len(rm.previewQueue)
}

type p93ListingEntry struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
	Mode    uint32 `json:"mode"`
	Inode   uint64 `json:"inode"`
}

func p93OrderedListing(t *testing.T, x *boundCacheFixture) []p93ListingEntry {
	t.Helper()
	children, err := x.store.ListChildrenForReadDir("media")
	if err != nil {
		t.Fatal(err)
	}
	listing := make([]p93ListingEntry, len(children))
	for i, child := range children {
		listing[i] = p93ListingEntry{child.Name, child.Size, child.Mtime.UnixNano(), uint32(child.Mode), child.Inode}
	}
	return listing
}

func p93FileInfoListing(t *testing.T, infos []os.FileInfo) []p93ListingEntry {
	t.Helper()
	listing := make([]p93ListingEntry, len(infos))
	for i, info := range infos {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatalf("ReadDir entry %q lacks inode Stat_t: %T", info.Name(), info.Sys())
		}
		listing[i] = p93ListingEntry{info.Name(), info.Size(), info.ModTime().UnixNano(), uint32(info.Mode()), stat.Ino}
	}
	return listing
}

func p93ActualReadDir(t *testing.T, x *boundCacheFixture, wantOrder []p93ListingEntry) {
	t.Helper()
	infos := p86ReadDirPrompt(t, x)
	gotInfos := p93FileInfoListing(t, infos)
	if after := p93OrderedListing(t, x); !reflect.DeepEqual(after, wantOrder) {
		t.Fatalf("source listing changed across ReadDir: before=%+v after=%+v", wantOrder, after)
	}
	if !reflect.DeepEqual(gotInfos, wantOrder) {
		t.Fatalf("actual ReadDir listing differs from measured source: got=%+v want=%+v", gotInfos, wantOrder)
	}
}

func p93Log(t *testing.T, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(encoded))
}

// AppleDouble companion files are not video preview candidates. Eight names
// ending in .mp4 must leave admission capacity for the real clip. A fresh
// target-only fixture independently proves the guard and observer path healthy.
func TestP93RegressionAppleDoubleDoesNotSpendPreviewCandidates(t *testing.T) {
	x, jobs := p93AdmissionFixture(t)
	mtime := time.Unix(1700000000, 123)
	for i := 0; i < previewCandidatesLimit; i++ {
		name := fmt.Sprintf("._a%02d.mp4", i)
		entry := metadata.MakeEntry("media/"+name, false, 4096, mtime, uint64(2000+i))
		entry.MtimeNsecKnown = true
		if err := x.store.Insert(entry); err != nil {
			t.Fatal(err)
		}
	}
	order := p93OrderedListing(t, x)
	if len(order) != previewCandidatesLimit+1 || order[len(order)-1].Name != "clip.mov" {
		t.Fatalf("bad AppleDouble source order: %+v", order)
	}
	for _, entry := range order[:len(order)-1] {
		if !strings.HasPrefix(entry.Name, "._") || !strings.HasSuffix(entry.Name, ".mp4") {
			t.Fatalf("non-AppleDouble filler in source order: %+v", entry)
		}
	}
	p93ActualReadDir(t, x, order)
	firstSeen, firstQueued := p93SchedulerState(x.jfs.handler.readahead)
	firstJobs := make([]string, 0, firstSeen)
	for i := 0; i < firstSeen; i++ {
		select {
		case job := <-jobs:
			firstJobs = append(firstJobs, job.key.identity.Path)
		case <-time.After(3 * time.Second):
			t.Fatal("first-listing observer did not drain")
		}
	}
	if _, queued := p93SchedulerState(x.jfs.handler.readahead); queued != 0 {
		t.Fatal("first-listing queue did not drain")
	}
	firstQualified := x.session.Qualified()
	controlX, controlJobs := p93AdmissionFixture(t)
	controlOrder := p93OrderedListing(t, controlX)
	if len(controlOrder) != 1 || controlOrder[0].Name != "clip.mov" {
		t.Fatalf("independent target-only control listing: %+v", controlOrder)
	}
	p93ActualReadDir(t, controlX, controlOrder)
	var control previewJob
	select {
	case control = <-controlJobs:
	case <-time.After(3 * time.Second):
		t.Fatal("target-only positive control did not reach observer")
	}
	controlSeen, controlQueued := p93SchedulerState(controlX.jfs.handler.readahead)
	p93Log(t, struct {
		Case             string            `json:"case"`
		SourceOrder      []p93ListingEntry `json:"source_order"`
		FirstJobs        []string          `json:"first_listing_observed_jobs"`
		FirstSeen        int               `json:"first_listing_seen"`
		FirstQueue       int               `json:"first_listing_queue_after_readdir"`
		ControlOrder     []p93ListingEntry `json:"independent_control_source_order"`
		ControlTarget    string            `json:"independent_control_observed_job"`
		ControlSeen      int               `json:"independent_control_seen"`
		ControlQueue     int               `json:"independent_control_queue_after_observer"`
		FirstQualified   bool              `json:"first_session_qualified"`
		ControlQualified bool              `json:"control_session_qualified"`
	}{"appledouble-candidate-budget", order, firstJobs, firstSeen, firstQueued, controlOrder, control.key.identity.Path, controlSeen, controlQueued, firstQualified, controlX.session.Qualified()})
	if control.key.identity.Path != "media/clip.mov" || controlSeen != 1 || controlQueued != 0 || !firstQualified || !controlX.session.Qualified() {
		t.Fatal("target-only positive control invalid")
	}
	if len(firstJobs) != 1 || firstJobs[0] != "media/clip.mov" {
		t.Fatalf("AppleDouble entries consumed candidate budget: observed first-listing jobs=%q; want only media/clip.mov", firstJobs)
	}
}
