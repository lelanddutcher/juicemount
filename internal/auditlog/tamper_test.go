package auditlog

// Review round 3: a prefix cut with a forged retention record, a removed
// log directory, and a log that filled its disk.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// anchoredLog writes 200 records in 2 KiB segments with no retention and
// anchors the head every 40 records, as the Manager does.
func anchoredLog(t *testing.T, dir string) []Anchor {
	t.Helper()
	w := mustOpen(t, dir, Options{SegmentBytes: 2048, RetainBytes: -1, RetainAge: -1})
	var anchors []Anchor
	for i := 0; i < 200; i++ {
		if _, err := w.Append(&Record{Type: TypeIntent, Principal: "mac-x", Action: "remove", Detail: "evidence"}); err != nil {
			t.Fatal(err)
		}
		if i%40 == 39 {
			if err := w.Sync(); err != nil {
				t.Fatal(err)
			}
			var head Anchor
			if len(anchors) == 0 {
				_, head, _ = VerifyAnchors(dir, nil)
			} else {
				var rep Report
				head, rep, _ = Follow(dir, anchors[len(anchors)-1], 0)
				if !rep.OK {
					t.Fatal(rep.Problem)
				}
			}
			head.TS = time.Now().UnixMilli()
			anchors = append(anchors, head)
		}
	}
	w.Close()
	return anchors
}

// forgeRetention removes the oldest half of the segments and appends a
// retention record naming the new start, chained to the head. It returns
// the new start.
func forgeRetention(t *testing.T, dir string) uint64 {
	t.Helper()
	segs, _ := segments(dir)
	if len(segs) < 6 {
		t.Fatalf("only %d segments", len(segs))
	}
	cut := len(segs) / 2
	for _, s := range segs[:cut] {
		os.Remove(s)
	}
	startSeq, startPrev, _ := firstRecord(segs[cut])
	_, head, _ := VerifyAnchors(dir, nil)
	forged := Record{Seq: head.Seq + 1, TS: time.Now().UnixMilli(), Type: TypeRetention, StartSeq: startSeq, StartPrev: startPrev,
		Detail: "removing old segments (size limit)", Prev: head.Hash}
	line, _ := json.Marshal(forged)
	f, err := os.OpenFile(segs[len(segs)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(append(line, '\n'))
	f.Close()
	return startSeq
}

// TestForgedRetentionIsJudgedAgainstThePolicy: someone who can write the
// audit directory (not the Manager's anchors) removes the oldest segments,
// with records the Manager anchored, and appends a retention record naming
// the new start. The chain verifies, so VerifyAnchors and Follow hand the
// record to the caller; CheckRetention finds that no policy explains it
// (no limit, the size kept is far below the limit, the removed records are
// younger than the age limit), and Uncovered names the anchors it removed.
// A removal the writer's own retention made fits its policy.
func TestForgedRetentionIsJudgedAgainstThePolicy(t *testing.T) {
	dir := t.TempDir()
	anchors := anchoredLog(t, dir)
	start := forgeRetention(t, dir)

	rep, _, err := VerifyAnchors(dir, anchors)
	if err != nil || !rep.OK || rep.StartSeq != start || len(rep.Retentions) != 1 || rep.Retentions[0].StartSeq != start {
		t.Fatalf("VerifyAnchors: %+v, %v (the chain itself verifies and reports the retention record)", rep, err)
	}
	_, frep, _ := Follow(dir, anchors[len(anchors)-1], 0)
	if !frep.OK || len(frep.Retentions) != 1 {
		t.Fatalf("Follow from the last anchor: %+v", frep)
	}
	forged := rep.Retentions[0]
	now := time.Now()
	for _, p := range []Policy{{}, {RetainBytes: 1 << 30}, {RetainAge: 24 * time.Hour}, {RetainBytes: 1 << 30, RetainAge: 24 * time.Hour}} {
		if msg := CheckRetention(dir, forged, anchors, p, now); msg == "" {
			t.Errorf("policy %+v: the forged retention fits", p)
		}
	}
	// It fits a policy that could have made it: a size limit the kept
	// segments are close to, or removed records older than the age limit.
	if msg := CheckRetention(dir, forged, anchors, Policy{RetainBytes: 8 << 10, SegmentBytes: 2048}, now); msg != "" {
		t.Errorf("size policy that explains it: %s", msg)
	}
	if msg := CheckRetention(dir, forged, anchors, Policy{RetainAge: time.Hour}, now.Add(2*time.Hour)); msg != "" {
		t.Errorf("age policy that explains it: %s", msg)
	}
	var removed []uint64
	for _, a := range anchors {
		if a.Seq < start {
			removed = append(removed, a.Seq)
		}
	}
	if len(removed) == 0 {
		t.Fatal("the cut removed no anchor")
	}
	forged.Problem = CheckRetention(dir, forged, anchors, Policy{}, now)
	if unc := Uncovered(anchors, start, []Retention{forged}); len(unc) != len(removed) {
		t.Fatalf("Uncovered = %v, want the removed anchors %v", unc, removed)
	}
	if unc := Uncovered(anchors, start, nil); len(unc) != len(removed) {
		t.Fatalf("Uncovered with no retention seen = %v", unc)
	}
	forged.Problem = ""
	if unc := Uncovered(anchors, start, []Retention{forged}); len(unc) != 0 {
		t.Fatalf("Uncovered with a retention that fits = %v", unc)
	}

	// The writer's own retention fits its policy.
	dir2 := t.TempDir()
	w := mustOpen(t, dir2, Options{SegmentBytes: 2048, RetainBytes: 8 << 10, RetainAge: -1})
	for i := 0; i < 200; i++ {
		appendN(t, w, 1, "p-1")
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	rep2, _, _ := VerifyAnchors(dir2, nil)
	if !rep2.OK || len(rep2.Retentions) == 0 {
		t.Fatalf("own retention: %+v", rep2)
	}
	last := rep2.Retentions[len(rep2.Retentions)-1]
	if msg := CheckRetention(dir2, last, nil, Policy{RetainBytes: 8 << 10, SegmentBytes: 2048}, time.Now()); msg != "" {
		t.Errorf("the writer's own retention does not fit its policy: %s", msg)
	}
}

// TestRemovedDirectoryFailsTheWriter: the audit directory is removed while
// the writer runs. The writer notices before its next write (it fsynced
// into an unlinked file and stayed healthy), refuses, and recovers into a
// new directory whose first records are the gap and its continuation; the
// old records are gone, which the Manager's anchors then report.
func TestRemovedDirectoryFailsTheWriter(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "audit")
	w := mustOpen(t, dir, Options{RetryEvery: 10 * time.Millisecond})
	defer w.Close()
	appendN(t, w, 10, "p-1")
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	appendN(t, w, 1, "p-1")
	if err := w.Sync(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Sync after the directory was removed = %v, want ErrUnavailable", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !w.Healthy() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !w.Healthy() {
		t.Fatal("the writer did not recover")
	}
	appendN(t, w, 1, "p-1")
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	p, err := Query(dir, Filter{Types: []string{TypeGap}}, 0, 10)
	if err != nil || len(p.Records) != 1 || !strings.Contains(p.Records[0].Detail, "gone") {
		t.Fatalf("gap record in the new directory: %+v, %v", p.Records, err)
	}
	rep := mustVerify(t, dir)
	if rep.StartSeq <= 11 {
		t.Fatalf("the new log starts at seq %d; its records continue the sequence", rep.StartSeq)
	}
}

// TestRetentionRunsWhileFailed: a log over its size limit fills its disk
// (every write fails). Before, retention ran only while healthy, so the
// writer stayed failed for good; now recovery removes what the policy
// removes, names the new start in a retention record written after the
// removal, and the log verifies.
func TestRetentionRunsWhileFailed(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{SegmentBytes: 2048, RetainBytes: -1, RetainAge: -1})
	for i := 0; i < 100; i++ {
		appendN(t, w, 1, "p-1")
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	size := func() int64 {
		segs, _ := segments(dir)
		var n int64
		for _, s := range segs {
			if st, err := os.Stat(s); err == nil {
				n += st.Size()
			}
		}
		return n
	}
	full := size() // the disk: nothing more fits
	health := filepath.Join(t.TempDir(), "audit.health")
	w = mustOpenFailed(t, dir, Options{SegmentBytes: 2048, RetainBytes: 8 << 10, RetainAge: -1, RetryEvery: 10 * time.Millisecond,
		HealthPath: health, BeforeWrite: func() error {
			if size() >= full {
				return errors.New("no space left on device")
			}
			return nil
		}})
	defer w.Close()
	deadline := time.Now().Add(5 * time.Second)
	for !w.Healthy() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !w.Healthy() {
		h, _ := ReadHealth(health)
		t.Fatalf("a full log over its size limit stays failed: %+v", h)
	}
	appendN(t, w, 1, "p-1")
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	rep := mustVerify(t, dir)
	if rep.StartSeq <= 1 || size() > 8<<10+2048 {
		t.Fatalf("after recovery: %+v, %d bytes", rep, size())
	}
	p, _ := Query(dir, Filter{Types: []string{TypeRetention}}, 0, 10)
	if len(p.Records) == 0 || !strings.Contains(p.Records[0].Detail, "removed before this record was written") {
		t.Fatalf("retention record: %+v", p.Records)
	}

	// Under the limit there is nothing to remove: it stays failed and the
	// health file says why.
	dir2 := t.TempDir()
	w2 := mustOpenFailed(t, dir2, Options{RetainBytes: 1 << 30, RetryEvery: 10 * time.Millisecond, HealthPath: health,
		BeforeWrite: func() error { return errors.New("no space left on device") }})
	defer w2.Close()
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h, _ := ReadHealth(health); strings.Contains(h.Note, "nothing more to remove") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h, _ := ReadHealth(health)
	t.Fatalf("health of a full log under its limit: %+v", h)
}

// mustOpenFailed opens a writer whose first write fails.
func mustOpenFailed(t *testing.T, dir string, o Options) *Writer {
	t.Helper()
	if o.Logf == nil {
		o.Logf = quiet
	}
	w, err := Open(dir, o)
	if err == nil {
		w.Close()
		t.Fatal("Open succeeded on a full disk")
	}
	return w
}
