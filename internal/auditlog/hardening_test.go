package auditlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// rechain rewrites dir the way someone with write access to it could:
// edit applies to every record, and every prev is recomputed, so the
// result verifies on its own.
func rechain(t *testing.T, dir string, edit func(*Record) bool) {
	t.Helper()
	segs, _ := segments(dir)
	prev := zeroHash
	for _, s := range segs {
		var out bytes.Buffer
		if err := eachLine(s, func(_ int, line []byte) error {
			var r Record
			if err := json.Unmarshal(line, &r); err != nil {
				return err
			}
			if !edit(&r) {
				return nil
			}
			r.Prev = prev
			b, _ := json.Marshal(&r)
			sum := sha256.Sum256(b)
			prev = hex.EncodeToString(sum[:])
			out.Write(b)
			out.WriteByte('\n')
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(s, out.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// Records queued while a write fails keep their place in the chain: the
// gap record is written after them (it was written before them, so the
// log had a seq going backwards and did not verify).
func TestRecordsQueuedDuringFailureKeepSeqOrder(t *testing.T) {
	dir := t.TempDir()
	release := make(chan struct{})
	var calls atomic.Int32
	w := mustOpen(t, dir, Options{RetryEvery: 10 * time.Millisecond, BeforeWrite: func() error {
		switch calls.Add(1) {
		case 1:
			return nil // the open record
		case 2:
			<-release // the batch being written while more is queued
			return errors.New("injected failure")
		}
		return nil
	}})
	appendN(t, w, 1, "p-1")
	for calls.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	appendN(t, w, 3, "p-1") // queued behind the failing batch
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	for !w.Healthy() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	w.Close()
	rep := mustVerify(t, dir)
	if rep.Records != 5 { // open, 3 queued, gap
		t.Fatalf("records = %d, want 5: %+v", rep.Records, rep)
	}
}

func TestAppendDurable(t *testing.T) {
	dir := t.TempDir()
	var mode atomic.Int32 // 0 ok, 1 fail, 2 stall
	w := mustOpen(t, dir, Options{RetryEvery: time.Hour, BeforeWrite: func() error {
		switch mode.Load() {
		case 1:
			return errors.New("injected failure")
		case 2:
			for mode.Load() == 2 {
				time.Sleep(time.Millisecond)
			}
		}
		return nil
	}})
	seq, err := w.AppendDurable(&Record{Type: TypeIntent, Detail: "durable"}, time.Second)
	if err != nil || seq == 0 {
		t.Fatalf("AppendDurable = %d, %v", seq, err)
	}
	b, _ := os.ReadFile(segName(dir, 1))
	if !bytes.Contains(b, []byte(`"detail":"durable"`)) {
		t.Fatal("AppendDurable returned before the record was in the file")
	}
	mode.Store(2)
	t0 := time.Now()
	if _, err := w.AppendDurable(&Record{Type: TypeIntent}, 50*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("AppendDurable on a stalled disk = %v, want ErrTimeout", err)
	}
	if d := time.Since(t0); d > 2*time.Second {
		t.Fatalf("the timeout took %s", d)
	}
	mode.Store(1) // the stalled batch now fails
	if _, err := w.AppendDurable(&Record{Type: TypeIntent}, 2*time.Second); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("AppendDurable on a failing disk = %v, want ErrUnavailable", err)
	}
}

func TestLazyWriterTouchesNothingUntilActivated(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "audit")
	health := dir + ".health"
	w := mustOpen(t, dir, Options{Lazy: true, HealthPath: health})
	if _, err := w.Append(&Record{Type: TypeIntent}); !errors.Is(err, ErrInactive) {
		t.Fatalf("Append before Activate = %v, want ErrInactive", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("the directory exists before Activate: %v", err)
	}
	if _, err := os.Stat(health); !os.IsNotExist(err) {
		t.Fatalf("a health file exists before Activate: %v", err)
	}
	if err := w.Activate(); err != nil {
		t.Fatal(err)
	}
	if h, err := ReadHealth(health); err != nil || !h.Active || !h.Healthy {
		t.Fatalf("health after Activate: %+v, %v", h, err)
	}
	if _, err := w.Append(&Record{Type: TypeIntent}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if rep := mustVerify(t, dir); rep.Records != 2 {
		t.Fatalf("records = %d, want open + 1", rep.Records)
	}
	// Close of a writer never activated.
	w2, err := Open(filepath.Join(base, "never"), Options{Lazy: true, Logf: quiet})
	if err != nil {
		t.Fatal(err)
	}
	w2.Close()
	// A health file of an earlier run with the log on is rewritten as
	// off, so the Manager does not report it stale.
	w3 := mustOpen(t, dir, Options{Lazy: true, HealthPath: health})
	if h, err := ReadHealth(health); err != nil || h.Active || h.Healthy || time.Since(time.UnixMilli(h.TS)) > time.Minute {
		t.Fatalf("leftover health file after a Lazy open: %+v, %v", h, err)
	}
	w3.Close()
}

// Retention removes the oldest segments past the size limit, first
// writing a record that names the new start; the retained log verifies,
// and one whose start was cut without such a record does not.
func TestRetentionBySizeKeepsTheChainVerifiable(t *testing.T) {
	dir := t.TempDir()
	health := filepath.Join(t.TempDir(), "audit.health")
	w := mustOpen(t, dir, Options{SegmentBytes: 2048, RetainBytes: 8 << 10, RetainAge: -1, HealthPath: health})
	for i := 0; i < 200; i++ {
		appendN(t, w, 1, "p-1")
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	segs, _ := segments(dir)
	var total int64
	for _, s := range segs {
		st, _ := os.Stat(s)
		total += st.Size()
	}
	if total > 8<<10+2048 {
		t.Fatalf("%d bytes in %d segments kept, limit 8 KiB", total, len(segs))
	}
	rep := mustVerify(t, dir)
	if rep.StartSeq <= 1 {
		t.Fatalf("nothing was removed: %+v", rep)
	}
	p, _ := Query(dir, Filter{Types: []string{TypeRetention}}, 0, 100)
	if len(p.Records) == 0 || p.Records[0].StartSeq > rep.StartSeq {
		t.Fatalf("retention records: %+v (log starts at %d)", p.Records, rep.StartSeq)
	}
	if h, err := ReadHealth(health); err != nil || h.Removed == 0 || h.RetainBytes != 8<<10 {
		t.Fatalf("health: %+v, %v", h, err)
	}
	// Cutting one more segment by hand is a break.
	os.Remove(segs[0])
	if rep, err := Verify(dir); err != nil || rep.OK || !strings.Contains(rep.Problem, "no retention record") {
		t.Fatalf("a log cut without a retention record verifies: %+v, %v", rep, err)
	}
}

func TestRetentionByAge(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{SegmentBytes: 2048, RetainBytes: -1, RetainAge: time.Hour, MaintainEvery: 10 * time.Millisecond})
	for i := 0; i < 60; i++ {
		appendN(t, w, 1, "p-1")
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	segs, _ := segments(dir)
	if len(segs) < 3 {
		t.Fatalf("%d segments", len(segs))
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, s := range segs[:len(segs)-1] {
		os.Chtimes(s, old, old)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if now, _ := segments(dir); len(now) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	w.Close()
	if now, _ := segments(dir); len(now) != 1 {
		t.Fatalf("%d segments left, want only the newest", len(now))
	}
	mustVerify(t, dir)
}

// A page reads at most QueryScanBytes: a filter that matches nothing
// returns empty pages with a cursor until the oldest segment was read.
func TestQueryIsPagedBySegment(t *testing.T) {
	defer func(v int64) { QueryScanBytes = v }(QueryScanBytes)
	QueryScanBytes = 1
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{SegmentBytes: 2048})
	for i := 0; i < 60; i++ {
		appendN(t, w, 1, "p-1")
		w.Sync()
	}
	w.Close()
	segs, _ := segments(dir)
	if len(segs) < 3 {
		t.Fatalf("%d segments", len(segs))
	}
	calls, before := 0, uint64(0)
	for {
		p, err := Query(dir, Filter{Person: "nobody"}, before, 100)
		if err != nil {
			t.Fatal(err)
		}
		calls++
		if len(p.Records) != 0 {
			t.Fatalf("records for nobody: %+v", p.Records)
		}
		if p.Next == 0 {
			break
		}
		before = p.Next
	}
	if calls != len(segs) {
		t.Fatalf("%d calls for %d segments, want one segment per call", calls, len(segs))
	}
	// A matching filter still returns every record once.
	n, before := 0, uint64(0)
	for {
		p, _ := Query(dir, Filter{Person: "p-1"}, before, 7)
		n += len(p.Records)
		if p.Next == 0 {
			break
		}
		before = p.Next
	}
	if n != 60 {
		t.Fatalf("paged %d records, want 60", n)
	}
}

// Anchors catch what the chain alone cannot: a rewrite with recomputed
// hashes, and a cut of the newest records to before the anchor. A cut
// after the last anchor stays undetectable (the documented limit).
func TestAnchorsDetectRewriteAndTruncation(t *testing.T) {
	setup := func(t *testing.T) (string, Anchor) {
		dir := t.TempDir()
		w := mustOpen(t, dir, Options{SegmentBytes: 4096})
		appendN(t, w, 30, "p-john")
		w.Sync()
		_, a1, err := VerifyAnchors(dir, nil)
		if err != nil || a1.Seq == 0 {
			t.Fatalf("first anchor: %+v %v", a1, err)
		}
		appendN(t, w, 30, "p-john")
		w.Sync()
		a2, rep, err := Follow(dir, a1, 0)
		if err != nil || !rep.OK || a2.Seq <= a1.Seq {
			t.Fatalf("Follow: %+v %+v %v", a2, rep, err)
		}
		appendN(t, w, 5, "p-john")
		w.Close()
		return dir, a2
	}
	t.Run("rewrite", func(t *testing.T) {
		dir, a := setup(t)
		rechain(t, dir, func(r *Record) bool {
			if r.Seq == 10 {
				r.Person = "p-jane"
			}
			return true
		})
		mustVerify(t, dir) // the chain alone cannot tell
		if rep, _, _ := VerifyAnchors(dir, []Anchor{a}); rep.OK {
			t.Fatal("a rewritten log matches the anchor")
		}
		if _, rep, _ := Follow(dir, a, 0); rep.OK {
			t.Fatal("Follow accepts a rewritten log")
		}
	})
	t.Run("truncate before the anchor", func(t *testing.T) {
		dir, a := setup(t)
		rechain(t, dir, func(r *Record) bool { return r.Seq < a.Seq-3 })
		mustVerify(t, dir)
		rep, _, _ := VerifyAnchors(dir, []Anchor{a})
		if rep.OK || !strings.Contains(rep.Problem, "newer records were removed") {
			t.Fatalf("a truncated log matches the anchor: %+v", rep)
		}
		if _, rep, _ := Follow(dir, a, 0); rep.OK {
			t.Fatal("Follow accepts a truncated log")
		}
	})
	t.Run("truncate after the anchor", func(t *testing.T) {
		dir, a := setup(t)
		rechain(t, dir, func(r *Record) bool { return r.Seq <= a.Seq })
		if rep, _, _ := VerifyAnchors(dir, []Anchor{a}); !rep.OK {
			t.Fatalf("a cut after the last anchor is reported: %+v (the limit changed: update the docs)", rep)
		}
	})
	t.Run("Follow respects maxSeq", func(t *testing.T) {
		dir, a := setup(t)
		head, rep, err := Follow(dir, a, a.Seq+2)
		if err != nil || !rep.OK || head.Seq != a.Seq+2 {
			t.Fatalf("Follow with maxSeq: head %+v %+v %v", head, rep, err)
		}
	})
}
